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
# Runs inside the breeze container: start a full Airflow, then run the system tests in /files/dags.
set -euo pipefail

# `breeze run` skips the breeze entrypoint initialization (configure_environment.sh), which normally sets
# the Dags folder and sources the variables file, so both are done here.
export AIRFLOW__CORE__DAGS_FOLDER=/files/dags
set -o allexport
# shellcheck source=/dev/null
source /files/airflow-breeze-config/environment_variables.env
set +o allexport
# The system tests import helpers from the "system" package of the Google provider tests.
export PYTHONPATH="${PYTHONPATH:-}:/opt/airflow/providers/google/tests"
echo "Dags folder: ${AIRFLOW__CORE__DAGS_FOLDER}, project: ${SYSTEM_TESTS_GCP_PROJECT}, env id: ${SYSTEM_TESTS_ENV_ID}"

# One folder per run: the VM's logging agent ships it to Cloud Logging, filtered by this path.
RUN_LOGS_DIR="/files/system-tests-results/${SYSTEM_TESTS_ENV_ID}"
LOGS_DIR="${RUN_LOGS_DIR}/components"
mkdir -p "${LOGS_DIR}" "${AIRFLOW__LOGGING__BASE_LOG_FOLDER}"
export SYSTEM_TESTS_EVENTS_FILE="${RUN_LOGS_DIR}/events.jsonl"

airflow db migrate

# The same components as `breeze start-airflow`, started without the interactive terminal.
airflow api-server --port 8080 >"${LOGS_DIR}/api-server.log" 2>&1 &
airflow scheduler >"${LOGS_DIR}/scheduler.log" 2>&1 &
airflow dag-processor >"${LOGS_DIR}/dag-processor.log" 2>&1 &
airflow triggerer >"${LOGS_DIR}/triggerer.log" 2>&1 &

KEEP_AIRFLOW_MINUTES=0
DRIVER_ARGS=()
while [[ $# -gt 0 ]]; do
    if [[ $1 == "--keep-airflow-minutes" ]]; then
        KEEP_AIRFLOW_MINUTES=$2
        shift 2
    else
        DRIVER_ARGS+=("$1")
        shift
    fi
done

set +e
python /opt/airflow/scripts/in_container/run_system_tests_in_airflow.py "${DRIVER_ARGS[@]}"
result=$?
set -e

if [[ ${result} != 0 ]]; then
    echo "Import errors:"
    airflow dags list-import-errors || true
    echo "Last lines of the Dag processor log:"
    tail -50 "${LOGS_DIR}/dag-processor.log" || true
fi

if [[ ${KEEP_AIRFLOW_MINUTES} -gt 0 && (${result} != 0 || ${KEEP_AIRFLOW_ON_SUCCESS:-false} == "true") ]]; then
    echo "Keeping Airflow running for ${KEEP_AIRFLOW_MINUTES} minutes so the UI can be inspected;" \
        "cancel the workflow run to stop earlier"
    sleep $((KEEP_AIRFLOW_MINUTES * 60))
fi
exit "${result}"
