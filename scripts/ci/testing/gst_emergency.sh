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
# Emergency stop ("red button") for Google system tests on PRs.
#
# Usage:
#   gst_emergency.sh stop-run <github-run-id> [<pr-number>]   Tier 1: stop one run and clean up after it
#   gst_emergency.sh stop-all                                  Tier 2: stop everything, revoke cloud access
#   gst_emergency.sh restore                                   undo Tier 2 after the incident
#
# Tier 1: force-cancel the GitHub run, delete its test VM and runner, then list and delete every cloud
#         resource whose name or labels contain the run's env id (gh<run id>).
# Tier 2: disable the workflow (no new runs), force-cancel all runs, delete all test VMs and runners,
#         disable the test service account (revokes Google Cloud access of anything still running) and
#         delete every resource the cleanup tool supports in the dedicated test project.
#
# Environment: SYSTEM_TESTS_GCP_PROJECT, RUNNER_SERVICE_ACCOUNT, GITHUB_TOKEN (actions: write),
# RUNNER_ADMIN_TOKEN, GITHUB_REPOSITORY.
set -uo pipefail

: "${SYSTEM_TESTS_GCP_PROJECT:?}" "${GITHUB_REPOSITORY:?}" "${GITHUB_TOKEN:?}"
PROJECT="${SYSTEM_TESTS_GCP_PROJECT}"
MY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AIRFLOW_ROOT="$(cd "${MY_DIR}/../../.." && pwd)"
WORKFLOW_FILE="google-system-tests.yml"
WORK_DIR="$(mktemp -d)"
export PATH="${PATH}:/snap/bin:/usr/lib/google-cloud-sdk/bin:${HOME}/.local/bin"

github() {
    local method=$1 path=$2
    curl -sS -X "${method}" -H "Authorization: Bearer ${GITHUB_TOKEN}" -H "Accept: application/vnd.github+json" \
        "https://api.github.com/repos/${GITHUB_REPOSITORY}${path}"
}

report() {
    echo "$*"
    [[ -n "${GITHUB_STEP_SUMMARY:-}" ]] && echo "- $*" >>"${GITHUB_STEP_SUMMARY}"
    return 0
}

cancel_run() {
    local run_id=$1
    github POST "/actions/runs/${run_id}/force-cancel" >/dev/null && report "Force-cancelled GitHub run ${run_id}"
}

delete_vm() {
    local vm=$1
    if "${MY_DIR}/gce_runner.sh" delete "${vm}"; then
        report "Deleted test VM ${vm} and unregistered its runner"
    else
        report "WARNING: could not delete VM ${vm}"
    fi
}

list_project_resources() {
    gcloud asset search-all-resources --scope "projects/${PROJECT}" --format json >"${WORK_DIR}/all.json"
}

run_cleanup_tool() {
    local resources_file=$1
    cat >"${WORK_DIR}/config.json" <<EOF
{"project_id": "${PROJECT}", "protected_resources": {}, "default_location": "us-central1"}
EOF
    PYTHONPATH="${AIRFLOW_ROOT}/providers/google/tests/system/google/resources_cleanup" \
        uv run --no-project --with google-cloud-aiplatform --with jinja2 \
        python -m airflow_google_provider_resource_cleanup --config-path "${WORK_DIR}/config.json" \
        --resources-file-path "${resources_file}" delete --project-id "${PROJECT}"
}

cleanup_by_env_id() {
    local env_id=$1
    list_project_resources || {
        report "WARNING: could not list resources (Cloud Asset Inventory)"
        return 1
    }
    python3 - "${WORK_DIR}/all.json" "${WORK_DIR}/run.json" "${env_id}" <<'PYTHON'
import json, sys
src, dst, env_id = sys.argv[1:]
needle = env_id.lower().replace("_", "-")
def matches(resource):
    text = " ".join([resource.get("name", ""), resource.get("displayName", ""), json.dumps(resource.get("labels", {}))])
    return needle in text.lower().replace("_", "-")
resources = [r for r in json.load(open(src)) if matches(r)]
json.dump(resources, open(dst, "w"))
print(f"{len(resources)} resource(s) of this run found:")
for r in resources:
    print(f"  {r.get('assetType')}  {r.get('name')}")
PYTHON
    report "Listed resources containing ${env_id} (Cloud Asset Inventory can lag a few minutes behind)"
    run_cleanup_tool "${WORK_DIR}/run.json" && report "Cleanup of ${env_id} resources finished"
}

stop_run() {
    local run_id=$1 pr=${2:-}
    report "Tier 1 emergency stop of run ${run_id}${pr:+ (PR #${pr})}"
    cancel_run "${run_id}"
    local vm=""
    if [[ -n "${pr}" ]]; then
        vm="gst-pr-${pr}"
    else
        vm=$(gcloud compute instances list --project "${PROJECT}" \
            --filter "labels.gst-run=${run_id}" --format "value(name)" | head -1)
    fi
    [[ -n "${vm}" ]] && delete_vm "${vm}"
    cleanup_by_env_id "gh${run_id}"
    if [[ -n "${pr}" ]]; then
        python3 "${MY_DIR}/pr_status_comment.py" update --pr "${pr}" --run-id "${run_id}" --state stopped \
            --note "Stopped by ${GITHUB_ACTOR:-an operator}; VM deleted and run resources cleaned up" || true
    fi
}

stop_all() {
    report "Tier 2 emergency stop of ALL Google system tests runs in ${PROJECT}"
    github PUT "/actions/workflows/${WORKFLOW_FILE}/disable" >/dev/null && report "Disabled workflow ${WORKFLOW_FILE}"
    for status in queued in_progress waiting; do
        for run_id in $(github GET "/actions/workflows/${WORKFLOW_FILE}/runs?status=${status}&per_page=100" |
            python3 -c 'import json, sys; print(" ".join(str(r["id"]) for r in json.load(sys.stdin)["workflow_runs"]))'); do
            cancel_run "${run_id}"
        done
    done
    if [[ -n "${RUNNER_SERVICE_ACCOUNT:-}" ]]; then
        gcloud iam service-accounts disable "${RUNNER_SERVICE_ACCOUNT}" --project "${PROJECT}" --quiet &&
            report "Disabled the test service account ${RUNNER_SERVICE_ACCOUNT} (no more Google Cloud API access)"
    fi
    for vm in $(gcloud compute instances list --project "${PROJECT}" --filter "labels.gst-managed=true" \
        --format "value(name)"); do
        delete_vm "${vm}"
    done
    list_project_resources && run_cleanup_tool "${WORK_DIR}/all.json" &&
        report "Cleanup of all supported resources in ${PROJECT} finished"
    report "Run 'restore' after the incident is resolved"
}

restore() {
    if [[ -n "${RUNNER_SERVICE_ACCOUNT:-}" ]]; then
        gcloud iam service-accounts enable "${RUNNER_SERVICE_ACCOUNT}" --project "${PROJECT}" --quiet &&
            report "Enabled the test service account ${RUNNER_SERVICE_ACCOUNT}"
    fi
    github PUT "/actions/workflows/${WORKFLOW_FILE}/enable" >/dev/null && report "Enabled workflow ${WORKFLOW_FILE}"
}

case "${1:-}" in
    stop-run) stop_run "${2:?GitHub run id}" "${3:-}" ;;
    stop-all) stop_all ;;
    restore) restore ;;
    *)
        echo "Usage: $0 stop-run <github-run-id> [<pr-number>] | stop-all | restore"
        exit 2
        ;;
esac
