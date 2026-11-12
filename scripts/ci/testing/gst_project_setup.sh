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
# One-time setup of a dedicated Google Cloud project for Google system tests on PRs (proof of concept).
# Idempotent: safe to run again. Run it as a project owner of the new project.
#
# Usage: gst_project_setup.sh <project-id> <manager-service-account> <alert-email> [region]
#
# Sets up: APIs, the test identity (service account gst-runner), permissions for the manager runner,
# network (Cloud Router + NAT, IAP SSH firewall), Data Access audit logs for IAM, project Org Policies
# that are safe for the current system tests, GPU quota 0, log-based anomaly alerts sent by email, and a
# Cloud Monitoring dashboard. Steps that need more rights than available are reported and skipped.
set -uo pipefail

PROJECT="${1:?project id}"
MANAGER_SA="${2:?manager service account email (the VM running the manager runner)}"
ALERT_EMAIL="${3:?email address for alerts}"
REGION="${4:-us-central1}"
RUNNER_SA="gst-runner@${PROJECT}.iam.gserviceaccount.com"
MY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FAILED_STEPS=()

step() {
    local name=$1
    shift
    echo "==> ${name}"
    if ! "$@"; then
        echo "    !! ${name} failed (see above); continuing"
        FAILED_STEPS+=("${name}")
    fi
}

enable_apis() {
    gcloud services enable --project "${PROJECT}" \
        compute.googleapis.com iam.googleapis.com iamcredentials.googleapis.com logging.googleapis.com \
        monitoring.googleapis.com cloudasset.googleapis.com orgpolicy.googleapis.com \
        cloudquotas.googleapis.com aiplatform.googleapis.com bigquery.googleapis.com \
        pubsub.googleapis.com storage.googleapis.com
}

create_runner_identity() {
    gcloud iam service-accounts describe "${RUNNER_SA}" --project "${PROJECT}" &>/dev/null ||
        gcloud iam service-accounts create gst-runner --project "${PROJECT}" \
            --display-name "Google system tests on PRs (test VMs)"
    # Broad role for the proof of concept, so all system tests can run; production uses the narrow set of
    # roles the selected tests need.
    for role in roles/editor roles/logging.logWriter roles/monitoring.metricWriter; do
        gcloud projects add-iam-policy-binding "${PROJECT}" --quiet --condition=None \
            --member "serviceAccount:${RUNNER_SA}" --role "${role}" >/dev/null
    done
}

grant_manager() {
    # The manager creates/starts/stops/deletes test VMs, attaches gst-runner to them, lists resources for
    # cleanup and can disable gst-runner in an emergency (Tier 2).
    for role in roles/compute.instanceAdmin.v1 roles/cloudasset.viewer roles/editor; do
        gcloud projects add-iam-policy-binding "${PROJECT}" --quiet --condition=None \
            --member "serviceAccount:${MANAGER_SA}" --role "${role}" >/dev/null
    done
    gcloud iam service-accounts add-iam-policy-binding "${RUNNER_SA}" --project "${PROJECT}" --quiet \
        --member "serviceAccount:${MANAGER_SA}" --role roles/iam.serviceAccountUser >/dev/null
    gcloud iam service-accounts add-iam-policy-binding "${RUNNER_SA}" --project "${PROJECT}" --quiet \
        --member "serviceAccount:${MANAGER_SA}" --role roles/iam.serviceAccountAdmin >/dev/null
}

create_network() {
    gcloud compute routers describe gst-router --project "${PROJECT}" --region "${REGION}" &>/dev/null ||
        gcloud compute routers create gst-router --project "${PROJECT}" --region "${REGION}" --network default
    gcloud compute routers nats describe gst-nat --router gst-router --project "${PROJECT}" \
        --region "${REGION}" &>/dev/null ||
        gcloud compute routers nats create gst-nat --router gst-router --project "${PROJECT}" \
            --region "${REGION}" --auto-allocate-nat-external-ips --nat-all-subnet-ip-ranges \
            --enable-logging --log-filter ERRORS_ONLY
    gcloud compute firewall-rules describe gst-allow-iap-ssh --project "${PROJECT}" &>/dev/null ||
        gcloud compute firewall-rules create gst-allow-iap-ssh --project "${PROJECT}" --network default \
            --direction INGRESS --allow tcp:22 --source-ranges 35.235.240.0/20
}

enable_iam_data_access_logs() {
    local policy
    policy=$(mktemp)
    gcloud projects get-iam-policy "${PROJECT}" --format json >"${policy}" &&
        python3 - "${policy}" <<'PYTHON' &&
import json, sys
path = sys.argv[1]
policy = json.load(open(path))
configs = [c for c in policy.get("auditConfigs", []) if c.get("service") != "iam.googleapis.com"]
configs.append({"service": "iam.googleapis.com", "auditLogConfigs": [{"logType": "ADMIN_READ"}, {"logType": "DATA_WRITE"}]})
policy["auditConfigs"] = configs
json.dump(policy, open(path, "w"))
PYTHON
        gcloud projects set-iam-policy "${PROJECT}" "${policy}" --quiet >/dev/null
}

set_org_policies() {
    # Only policies that do not break the current system tests (checked against the test code):
    # no test creates service-account keys, and tests only use US and EU locations.
    # Not enforced, because existing tests need them: no external VM IPs (compute tests), no public
    # Cloud SQL IP (Cloud SQL tests), public access prevention (GCS ACL test). Those are detected instead.
    local tmp
    tmp=$(mktemp)
    cat >"${tmp}" <<EOF
name: projects/${PROJECT}/policies/iam.disableServiceAccountKeyCreation
spec:
  rules:
    - enforce: true
EOF
    gcloud org-policies set-policy "${tmp}" || return 1
    cat >"${tmp}" <<EOF
name: projects/${PROJECT}/policies/gcp.resourceLocations
spec:
  rules:
    - values:
        allowedValues: ["in:us-locations", "in:eu-locations", "global"]
EOF
    gcloud org-policies set-policy "${tmp}"
}

set_gpu_quota_zero() {
    gcloud beta quotas preferences create --project "${PROJECT}" --service compute.googleapis.com \
        --quota-id GPUS-ALL-REGIONS-per-project --preferred-value 0 --preference-id gst-no-gpus \
        --justification "System tests do not use GPUs" --quiet 2>&1 | grep -v "already exists" || true
}

create_alert_channel() {
    CHANNEL=$(gcloud beta monitoring channels list --project "${PROJECT}" \
        --filter "labels.email_address=${ALERT_EMAIL}" --format "value(name)" | head -1)
    if [[ -z "${CHANNEL}" ]]; then
        CHANNEL=$(gcloud beta monitoring channels create --project "${PROJECT}" --type email \
            --display-name "Google system tests alerts" --channel-labels "email_address=${ALERT_EMAIL}" \
            --format "value(name)")
    fi
    echo "    notification channel: ${CHANNEL}"
}

create_log_alert() {
    local id=$1 title=$2 filter=$3 doc=$4 tmp
    tmp=$(mktemp)
    python3 - "${tmp}" "${title}" "${filter}" "${doc}" "${CHANNEL}" <<'PYTHON'
import json, sys
path, title, log_filter, doc, channel = sys.argv[1:]
json.dump({
    "displayName": f"[gst] {title}",
    "combiner": "OR",
    "conditions": [{"displayName": title, "conditionMatchedLog": {"filter": log_filter}}],
    "alertStrategy": {"notificationRateLimit": {"period": "300s"}, "autoClose": "1800s"},
    "notificationChannels": [channel],
    "documentation": {"mimeType": "text/markdown", "content": doc},
}, open(path, "w"))
PYTHON
    local existing
    existing=$(gcloud alpha monitoring policies list --project "${PROJECT}" \
        --filter "displayName=\"[gst] ${title}\"" --format "value(name)" | head -1)
    if [[ -n "${existing}" ]]; then
        gcloud alpha monitoring policies update "${existing}" --project "${PROJECT}" --policy-from-file "${tmp}" >/dev/null
    else
        gcloud alpha monitoring policies create --project "${PROJECT}" --policy-from-file "${tmp}" >/dev/null
    fi
    echo "    alert: ${title} (${id})"
}

create_anomaly_alerts() {
    local stop="If this is not expected, open the 'Google system tests emergency stop' workflow in GitHub."
    create_log_alert gpu "GPU, TPU or large VM created" \
        'protoPayload.methodName=~"compute.instances.insert" AND (protoPayload.request.guestAccelerators:* OR protoPayload.request.machineType=~"(a2|a3|g2|m1|m2|m3|n2-highmem-(64|80|96|128)|n2-standard-(64|80|96|128)|c2-standard-60)")' \
        "A VM with accelerators or a very large machine type was created. System tests do not need them. ${stop}"
    create_log_alert iam "IAM policy change or service-account key creation" \
        'protoPayload.methodName=("SetIamPolicy" OR "google.iam.admin.v1.CreateServiceAccountKey" OR "google.iam.admin.v1.CreateServiceAccount" OR "google.iam.admin.v1.CreateRole") AND NOT protoPayload.authenticationInfo.principalEmail=~"^(user:)?[^@]+@(google\.com)$"' \
        "IAM was changed by a non-human identity (possibly a test run). ${stop}"
    create_log_alert public_ip "Public IP assigned to a VM" \
        'protoPayload.methodName=~"compute.instances.(insert|addAccessConfig)" AND protoPayload.request.networkInterfaces.accessConfigs:*' \
        "A VM got a public IP. Expected only for the compute system tests (example_compute*.py); otherwise investigate. ${stop}"
    create_log_alert firewall "Firewall rule created or changed" \
        'protoPayload.methodName=~"compute.firewalls.(insert|patch|update)" AND NOT protoPayload.request.name="gst-allow-iap-ssh"' \
        "A firewall rule was created or changed in the test project. ${stop}"
    create_log_alert sensitive_api "Sensitive API used (Secret Manager, KMS, Billing, Org Policy)" \
        'protoPayload.serviceName=("secretmanager.googleapis.com" OR "cloudkms.googleapis.com" OR "cloudbilling.googleapis.com" OR "orgpolicy.googleapis.com") AND protoPayload.authenticationInfo.principalEmail="'"${RUNNER_SA}"'"' \
        "A test VM called a sensitive API. ${stop}"
}

create_dashboard() {
    local tmp
    tmp=$(mktemp)
    sed "s/__PROJECT__/${PROJECT}/g" "${MY_DIR}/gst_dashboard.json" >"${tmp}"
    local existing
    existing=$(gcloud monitoring dashboards list --project "${PROJECT}" \
        --filter 'displayName="Google system tests on PRs"' --format "value(name)" | head -1)
    if [[ -z "${existing}" ]]; then
        gcloud monitoring dashboards create --project "${PROJECT}" --config-from-file "${tmp}" >/dev/null
    fi
    echo "    https://console.cloud.google.com/monitoring/dashboards?project=${PROJECT}"
}

step "Enable APIs" enable_apis
step "Create the test identity gst-runner" create_runner_identity
step "Grant the manager runner access" grant_manager
step "Create Cloud Router, NAT and IAP firewall" create_network
step "Enable Data Access audit logs for IAM" enable_iam_data_access_logs
step "Set Org Policies (needs Org Policy Administrator)" set_org_policies
step "Set GPU quota to 0" set_gpu_quota_zero
step "Create the email notification channel" create_alert_channel
step "Create anomaly alerts" create_anomaly_alerts
step "Create the monitoring dashboard" create_dashboard

echo
echo "Then set in GitHub (Settings -> Secrets and variables -> Actions -> Variables):"
echo "  SYSTEM_TESTS_GCP_PROJECT = ${PROJECT}"
echo "  RUNNER_SERVICE_ACCOUNT   = ${RUNNER_SA}"
echo "  RUNNER_ZONE              = ${REGION}-a"
if ((${#FAILED_STEPS[@]})); then
    echo
    echo "Steps that failed (usually missing permissions, e.g. Org Policies need an organization admin):"
    printf '  - %s\n' "${FAILED_STEPS[@]}"
fi
