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
# Run Google system tests in a full Airflow started with breeze, on the machine running this script.
#
# Usage: run_google_system_tests.sh <system test file> [<system test file> ...]
#
# Environment:
#   SYSTEM_TESTS_GCP_PROJECT  Google Cloud project the tests create resources in (required)
#   SYSTEM_TESTS_ENV_ID       Unique suffix for created resources (default: random)
#   TIMEOUT_MINUTES           Per-test timeout (default: 180)
#   KEEP_AIRFLOW_MINUTES      Keep Airflow (and its UI) running after the tests (default: 0)
#   PYTHON_VERSION            Breeze Python version (default: 3.11)
#
# Google credentials come from the machine: on a GCE VM, the VM's service account.
set -euo pipefail

if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <system test file> [<system test file> ...]"
    exit 2
fi
: "${SYSTEM_TESTS_GCP_PROJECT:?Set SYSTEM_TESTS_GCP_PROJECT}"
SYSTEM_TESTS_ENV_ID="${SYSTEM_TESTS_ENV_ID:-st$(date +%s)}"
TIMEOUT_MINUTES="${TIMEOUT_MINUTES:-180}"
KEEP_AIRFLOW_MINUTES="${KEEP_AIRFLOW_MINUTES:-0}"
PYTHON_VERSION="${PYTHON_VERSION:-3.11}"
# Forks have no CI image cache of their own; apache/airflow's cache avoids building the image from scratch.
export GITHUB_REPOSITORY=apache/airflow

AIRFLOW_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "${AIRFLOW_ROOT}"

# Fresh runner VMs (gce_runner_startup.sh) have uv but not breeze.
export PATH="${HOME}/.local/bin:${PATH}"
if ! command -v breeze >/dev/null; then
    scripts/tools/setup_breeze
fi

# breeze mounts files/ as /files in the container; /files/dags is Airflow's Dags folder.
rm -rf files/dags files/system-tests-results
mkdir -p files/dags files/airflow-breeze-config
for test_file in "$@"; do
    cp "${test_file}" files/dags/
    test_dir="$(dirname "${test_file}")"
    if [[ -d "${test_dir}/resources" ]]; then
        cp -r "${test_dir}/resources" files/dags/
    fi
done

# Sourced inside the container by scripts/in_container/run_system_tests_in_airflow.sh.
# The Google connection has no key, so the machine's credentials are used.
cat >files/airflow-breeze-config/environment_variables.env <<EOF
SYSTEM_TESTS_GCP_PROJECT=${SYSTEM_TESTS_GCP_PROJECT}
SYSTEM_TESTS_ENV_ID=${SYSTEM_TESTS_ENV_ID}
AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT='{"conn_type": "google_cloud_platform", "extra": {"project": "${SYSTEM_TESTS_GCP_PROJECT}"}}'
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
AIRFLOW__CORE__LOAD_EXAMPLES=False
EOF

echo "Running $# system test(s) in project ${SYSTEM_TESTS_GCP_PROJECT} with env id ${SYSTEM_TESTS_ENV_ID}"

# Building the image downloads OS packages from external mirrors that sometimes rate-limit; retry those.
for attempt in 1 2 3; do
    if breeze ci-image build --python "${PYTHON_VERSION}"; then
        break
    fi
    if [[ ${attempt} == 3 ]]; then
        echo "CI image build failed 3 times, giving up"
        exit 1
    fi
    echo "CI image build failed (attempt ${attempt}), retrying in 60 seconds"
    sleep 60
done
# `breeze start-airflow` builds the UI from sources before starting; `breeze run` does not, and without the
# built UI the api-server answers every page with "Internal Server Error".
breeze ui compile-assets
breeze down || true

set +e
breeze run --python "${PYTHON_VERSION}" --backend postgres --forward-ports --answer no \
    bash /opt/airflow/scripts/in_container/run_system_tests_in_airflow.sh \
    --timeout-minutes "${TIMEOUT_MINUTES}" --keep-airflow-minutes "${KEEP_AIRFLOW_MINUTES}"
result=$?
set -e

breeze down || true
exit "${result}"
