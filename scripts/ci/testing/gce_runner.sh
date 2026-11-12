#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Manage the per-PR GCE VMs that run Google system tests as GitHub self-hosted runners.
# Runs on the "manager" runner, a GCE VM whose service account may create VMs.
#
# Usage:
#   gce_runner.sh ensure <vm-name> [<pr-number>]   create, start or reuse the VM; wait until its runner is online
#   gce_runner.sh stop <vm-name>                   stop the VM unless a newer run of the same branch is pending
#   gce_runner.sh delete <vm-name>                 delete the VM and unregister its runner
#   gce_runner.sh cleanup                          delete VMs of closed PRs and old manual-run VMs
#
# Environment:
#   SYSTEM_TESTS_GCP_PROJECT  project of the VMs (required)
#   GITHUB_REPOSITORY         owner/repo (set by GitHub Actions)
#   RUNNER_ADMIN_TOKEN        token with "Administration: write" on the repo (ensure, delete)
#   GITHUB_TOKEN              workflow token (stop, cleanup)
#   RUNNER_ZONE, RUNNER_MACHINE_TYPE, RUNNER_DISK_SIZE, RUNNER_SERVICE_ACCOUNT  optional overrides;
#   zone and service account default to the manager VM's own.
#   MAX_RUNNING_TEST_VMS      test VMs running at the same time (default: 4)
set -euo pipefail

: "${SYSTEM_TESTS_GCP_PROJECT:?Set SYSTEM_TESTS_GCP_PROJECT}"
: "${GITHUB_REPOSITORY:?Set GITHUB_REPOSITORY}"
PROJECT="${SYSTEM_TESTS_GCP_PROJECT}"
MY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ONLINE_TIMEOUT_SECONDS=1500
BRANCH_VM_MAX_AGE_DAYS=3
# Test VMs running at the same time; more wait for a free slot (stopped VMs do not count).
MAX_RUNNING_VMS="${MAX_RUNNING_TEST_VMS:-4}"
SLOT_WAIT_SECONDS=7200

# On Ubuntu GCE images gcloud is a snap in /snap/bin, which is not on the PATH of the runner service.
export PATH="${PATH}:/snap/bin:/usr/lib/google-cloud-sdk/bin"
if ! command -v gcloud >/dev/null; then
    echo "gcloud is not installed on this runner: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

get_vm_metadata() {
    curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/$1"
}

ZONE="${RUNNER_ZONE:-$(get_vm_metadata instance/zone | awk -F/ '{print $NF}')}"
MACHINE_TYPE="${RUNNER_MACHINE_TYPE:-e2-standard-8}"
DISK_SIZE="${RUNNER_DISK_SIZE:-200GB}"
SERVICE_ACCOUNT="${RUNNER_SERVICE_ACCOUNT:-$(get_vm_metadata instance/service-accounts/default/email)}"

github_api() {
    local method=$1 path=$2 token=$3
    curl -sSf -X "${method}" -H "Authorization: Bearer ${token}" -H "Accept: application/vnd.github+json" \
        "https://api.github.com${path}"
}

get_vm_status() {
    gcloud compute instances describe "$1" --project "${PROJECT}" --zone "${ZONE}" \
        --format='value(status)' 2>/dev/null || true
}

get_runner_field() {
    local name=$1 field=$2
    github_api GET "/repos/${GITHUB_REPOSITORY}/actions/runners?name=${name}" "${RUNNER_ADMIN_TOKEN}" |
        python3 -c "import json, sys; r = json.load(sys.stdin)['runners']; print(r[0]['${field}'] if r else '')"
}

get_registration_token() {
    github_api POST "/repos/${GITHUB_REPOSITORY}/actions/runners/registration-token" "${RUNNER_ADMIN_TOKEN}" |
        python3 -c 'import json, sys; print(json.load(sys.stdin)["token"])'
}

wait_until_runner_online() {
    local name=$1
    local deadline=$((SECONDS + ONLINE_TIMEOUT_SECONDS))
    while ((SECONDS < deadline)); do
        if [[ "$(get_runner_field "${name}" status)" == "online" ]]; then
            echo "Runner ${name} is online"
            return 0
        fi
        echo "Waiting for runner ${name} to come online..."
        sleep 20
    done
    echo "Runner ${name} did not come online in ${ONLINE_TIMEOUT_SECONDS}s; see the VM serial console:"
    echo "  gcloud compute instances get-serial-port-output ${name} --project ${PROJECT} --zone ${ZONE}"
    return 1
}

count_busy_vms() {
    # A running test VM holds a slot only while its run's tests are not finished: once "Run system tests
    # in Airflow" completed, the VM is about to be stopped, and waiting for its stop job could deadlock
    # (the stop job may be queued behind this waiting job on the manager runner).
    local name=$1
    gcloud compute instances list --project "${PROJECT}" \
        --filter "labels.gst-managed=true AND status:(RUNNING PROVISIONING STAGING) AND name!=${name}" \
        --format json | python3 -c '
import json, os, sys, urllib.request

def run_tests_finished(run_id):
    token, repo = os.environ.get("GITHUB_TOKEN"), os.environ.get("GITHUB_REPOSITORY")
    if not (token and repo and run_id):
        return False
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/actions/runs/{run_id}/jobs",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
    )
    try:
        jobs = json.load(urllib.request.urlopen(request, timeout=30))["jobs"]
    except Exception:
        return False
    return any(j["name"].startswith("Run system tests") and j["status"] == "completed" for j in jobs)

vms = json.load(sys.stdin)
print(sum(1 for vm in vms if not run_tests_finished((vm.get("labels") or {}).get("gst-run"))))
'
}

report_queue_position() {
    # Show the run's place in the queue in the PR status comment (only for PR runs in GitHub Actions).
    [[ -n "${GST_PR:-}" && -n "${GITHUB_TOKEN:-}" && -n "${GITHUB_RUN_ID:-}" ]] || return 0
    local position waiting
    read -r position waiting < <(python3 "${MY_DIR}/pr_status_comment.py" position --run-id "${GITHUB_RUN_ID}") || return 0
    echo "Position in the queue: ${position} of ${waiting}"
    python3 "${MY_DIR}/pr_status_comment.py" update --pr "${GST_PR}" --run-id "${GITHUB_RUN_ID}" \
        --state queued --position "${position}" --waiting "${waiting}" || true
}

wait_for_free_slot() {
    local name=$1 running
    local deadline=$((SECONDS + SLOT_WAIT_SECONDS))
    while true; do
        running=$(count_busy_vms "${name}")
        if ((running < MAX_RUNNING_VMS)); then
            echo "${running} of ${MAX_RUNNING_VMS} test VMs are running; starting ${name}"
            return 0
        fi
        if ((SECONDS >= deadline)); then
            echo "No free test VM slot after $((SLOT_WAIT_SECONDS / 60)) minutes; raise MAX_RUNNING_TEST_VMS or retry"
            return 1
        fi
        echo "${running} of ${MAX_RUNNING_VMS} test VMs are busy; waiting for a free slot..."
        report_queue_position
        sleep 60
    done
}

ensure_vm() {
    local name=$1 pr=${2:-}
    local status labels="purpose=airflow-system-tests,gst-managed=true"
    [[ -n "${pr}" ]] && labels="${labels},gst-pr=${pr}"
    status=$(get_vm_status "${name}")
    echo "VM ${name}: ${status:-does not exist}"
    if [[ "${status}" != "RUNNING" ]]; then
        wait_for_free_slot "${name}"
    fi
    case "${status}" in
        "")
            gcloud compute instances create "${name}" --project "${PROJECT}" --zone "${ZONE}" \
                --machine-type "${MACHINE_TYPE}" --boot-disk-size "${DISK_SIZE}" \
                --boot-disk-type pd-balanced --image-family ubuntu-2404-lts-amd64 \
                --image-project ubuntu-os-cloud --no-address \
                --service-account "${SERVICE_ACCOUNT}" --scopes cloud-platform \
                --labels "${labels}" \
                --metadata "github-repo=${GITHUB_REPOSITORY},runner-labels=${name},runner-registration-token=$(get_registration_token)" \
                --metadata-from-file "startup-script=${MY_DIR}/gce_runner_startup.sh"
            ;;
        TERMINATED | STOPPING)
            # The VM keeps the startup script it was created with; refresh it so changes apply on boot.
            gcloud compute instances add-metadata "${name}" --project "${PROJECT}" --zone "${ZONE}" \
                --metadata-from-file "startup-script=${MY_DIR}/gce_runner_startup.sh"
            gcloud compute instances start "${name}" --project "${PROJECT}" --zone "${ZONE}"
            ;;
        SUSPENDED | SUSPENDING)
            gcloud compute instances resume "${name}" --project "${PROJECT}" --zone "${ZONE}"
            ;;
    esac
    if [[ -n "${status}" && -z "$(get_runner_field "${name}" id)" ]]; then
        # GitHub removes runners that were offline for a long time; register the existing VM again,
        # with the current startup script (the VM keeps the one it was created with).
        echo "Runner ${name} is not registered in GitHub; registering it again"
        gcloud compute instances add-metadata "${name}" --project "${PROJECT}" --zone "${ZONE}" \
            --metadata "runner-force-register=true,runner-registration-token=$(get_registration_token)" \
            --metadata-from-file "startup-script=${MY_DIR}/gce_runner_startup.sh"
        gcloud compute instances reset "${name}" --project "${PROJECT}" --zone "${ZONE}"
    fi
    if [[ -n "${GITHUB_RUN_ID:-}" ]]; then
        # Lets the emergency stop find the VM of a run.
        gcloud compute instances add-labels "${name}" --project "${PROJECT}" --zone "${ZONE}" \
            --labels "gst-run=${GITHUB_RUN_ID}" || true
    fi
    wait_until_runner_online "${name}"
    gcloud compute instances remove-metadata "${name}" --project "${PROJECT}" --zone "${ZONE}" \
        --keys runner-registration-token,runner-force-register
}

has_newer_pending_run() {
    : "${GITHUB_RUN_ID:?}" "${GITHUB_TOKEN:?}" "${BRANCH:?Set BRANCH}"
    local workflow_path=".github/workflows/google-system-tests.yml"
    # "waiting" (for approval) is not counted: an approved run starts the VM again itself.
    github_api GET "/repos/${GITHUB_REPOSITORY}/actions/workflows/google-system-tests.yml/runs?branch=${BRANCH}&per_page=20" \
        "${GITHUB_TOKEN}" | python3 -c "
import json, sys
runs = json.load(sys.stdin)['workflow_runs']
pending = [r for r in runs if r['status'] in ('queued', 'in_progress', 'pending', 'requested')
           and r['id'] > ${GITHUB_RUN_ID} and r['path'] == '${workflow_path}']
sys.exit(0 if pending else 1)
"
}

stop_vm() {
    local name=$1
    if has_newer_pending_run; then
        echo "A newer run of this branch is pending; leaving ${name} running for it"
        return 0
    fi
    if [[ "$(get_vm_status "${name}")" == "RUNNING" ]]; then
        gcloud compute instances stop "${name}" --project "${PROJECT}" --zone "${ZONE}" --async
        echo "Stopping ${name}; it starts again on the next run"
    fi
}

delete_vm() {
    local name=$1 runner_id
    runner_id=$(get_runner_field "${name}" id)
    if [[ -n "${runner_id}" ]]; then
        github_api DELETE "/repos/${GITHUB_REPOSITORY}/actions/runners/${runner_id}" "${RUNNER_ADMIN_TOKEN}" >/dev/null
        echo "Unregistered runner ${name}"
    fi
    if [[ -n "$(get_vm_status "${name}")" ]]; then
        gcloud compute instances delete "${name}" --project "${PROJECT}" --zone "${ZONE}" --quiet
    fi
}

cleanup_vms() {
    local name pr created status state
    local now
    now=$(date +%s)
    gcloud compute instances list --project "${PROJECT}" --filter "labels.gst-managed=true" \
        --format 'value(name,labels.gst-pr,creationTimestamp,status)' |
        while read -r name pr created status; do
            if [[ -n "${pr}" ]]; then
                state=$(github_api GET "/repos/${GITHUB_REPOSITORY}/pulls/${pr}" "${GITHUB_TOKEN}" |
                    python3 -c 'import json, sys; print(json.load(sys.stdin)["state"])')
                [[ "${state}" == "closed" ]] && echo "PR ${pr} is closed" && delete_vm "${name}"
            elif [[ "${status}" == "TERMINATED" ]] &&
                (((now - $(date -d "${created}" +%s)) > BRANCH_VM_MAX_AGE_DAYS * 86400)); then
                echo "${name} is older than ${BRANCH_VM_MAX_AGE_DAYS} days" && delete_vm "${name}"
            fi
        done
}

case "${1:-}" in
    ensure) ensure_vm "${2:?VM name}" "${3:-}" ;;
    stop) stop_vm "${2:?VM name}" ;;
    delete) delete_vm "${2:?VM name}" ;;
    cleanup) cleanup_vms ;;
    *)
        echo "Usage: $0 ensure|stop|delete <vm-name> [<pr-number>] | cleanup"
        exit 2
        ;;
esac
