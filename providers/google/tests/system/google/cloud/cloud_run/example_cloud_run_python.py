#
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
"""
Example DAG demonstrating the usage of the classic Python operators to execute Python functions natively and
within a virtual environment.
"""

from __future__ import annotations

import os

import pendulum

from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunExecutePythonJobOperator,
)
from airflow.providers.standard.operators.python import (
    PythonVirtualenvOperator,
)
from airflow.sdk import DAG

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
LOCATION = "europe-west1"


with DAG(
    dag_id="example_python_operator_test",
    schedule="@once",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    options = ["a", "b", "c", "d"]

    # [START howto_operator_python_venv]
    def callable_virtualenv(choices):
        """
        Example function that will be performed in a virtual environment.

        Importing at the function level ensures that it will not attempt to import the
        library before it is installed.
        """
        from time import sleep

        from colorama import Back, Fore, Style

        print(Fore.RED + "some red text")
        print(Back.GREEN + "and with a green background")
        print(Style.DIM + "and in dim text")
        print(Style.RESET_ALL)
        for _ in range(4):
            print(Style.DIM + "Please wait...", flush=True)
            sleep(1)

        for letter in choices:
            print(f"letter: {letter}")
        print("Finished")
        # return "HELLO_WORLD"

    virtualenv_task = PythonVirtualenvOperator(
        task_id="virtualenv_python",
        python_callable=callable_virtualenv,
        requirements=["colorama==0.4.0"],
        system_site_packages=False,
    )
    # [END howto_operator_python_venv]

    cloud_python_task = CloudRunExecutePythonJobOperator(
        task_id="cloud_python_task",
        project_id=PROJECT_ID,
        location=LOCATION,
        python_callable=callable_virtualenv,
        requirements=["colorama==0.4.0"],
        image_repository="us-central1-docker.pkg.dev/airflow-system-tests-303516/system-tests/sample-python",
        op_args=[options],
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
