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

LOGS_DIR=/files/system-tests-results/components
mkdir -p "${LOGS_DIR}"

airflow db migrate

# The same components as `breeze start-airflow`, started without the interactive terminal.
airflow api-server --port 8080 >"${LOGS_DIR}/api-server.log" 2>&1 &
airflow scheduler >"${LOGS_DIR}/scheduler.log" 2>&1 &
airflow dag-processor >"${LOGS_DIR}/dag-processor.log" 2>&1 &
airflow triggerer >"${LOGS_DIR}/triggerer.log" 2>&1 &

python /opt/airflow/scripts/in_container/run_system_tests_in_airflow.py "$@"
