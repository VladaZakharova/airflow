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
Example Airflow DAG showing how to run Python code on Google Cloud Run jobs.
"""

from __future__ import annotations

import os
from datetime import datetime

from google.cloud.run_v2.types import k8s_min

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecutePythonJobOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSDeleteObjectsOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "cloud_run_python"
LOCATION = "europe-west1"
ARTIFACT_REGISTRY_REPOSITORY = os.environ.get(
    "SYSTEM_TESTS_CLOUD_RUN_ARTIFACT_REPOSITORY",
    f"{LOCATION}-docker.pkg.dev/{PROJECT_ID}/airflow-system-tests",
)
IMAGE_REPOSITORY = os.environ.get(
    "SYSTEM_TESTS_CLOUD_RUN_IMAGE_REPOSITORY",
    f"{ARTIFACT_REGISTRY_REPOSITORY}/cloud-run-python",
)
CACHE_IMAGE_REPOSITORY = os.environ.get(
    "SYSTEM_TESTS_CLOUD_RUN_CACHE_IMAGE_REPOSITORY",
    f"{ARTIFACT_REGISTRY_REPOSITORY}/cloud-run-python-cache",
)
BUCKET_NAME = f"bucket-{DAG_ID}-{ENV_ID}".replace("_", "-")
PYTHON_FILE_URI = os.environ.get(
    "SYSTEM_TESTS_CLOUD_RUN_PYTHON_FILE_URI",
    "gs://airflow-system-tests-resources/cloud-build/cloud_run_user_code.py",
)
XCOM_VOLUME = f"gs://{BUCKET_NAME}/cloud-run-results"
LOGS_VOLUME = f"gs://{BUCKET_NAME}/cloud-run-logs"


def callable_with_return(*, choices: list[str], prefix: str) -> dict[str, str | int]:
    """
    Example Python function executed on Cloud Run and returned through XCom.
    """
    from colorama import Fore, Style

    print(Fore.GREEN + f"Processing {len(choices)} choices")
    print(Style.RESET_ALL)
    joined = ",".join(choices)
    return {"prefix": prefix, "joined": joined, "count": len(choices)}


def callable_with_logs(*, choices: list[str]) -> None:
    """
    Example Python function that writes logs and uses an environment variable.
    """
    from os import getenv

    print(f"Environment marker: {getenv('DEMO_ENV')}")
    for letter in choices:
        print(f"log-choice: {letter}")


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "cloud", "run", "python"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        project_id=PROJECT_ID,
    )

    # [START howto_operator_cloud_run_execute_python_job]
    cloud_python_callable_return = CloudRunExecutePythonJobOperator(
        task_id="cloud_python_callable_return",
        project_id=PROJECT_ID,
        location=LOCATION,
        python_callable=callable_with_return,
        image_repository=IMAGE_REPOSITORY,
        requirements=["colorama==0.4.0"],
        op_kwargs={"choices": ["a", "b", "c", "d"], "prefix": "letters"},
        python_version="3.11",
        xcom_volume=XCOM_VOLUME,
    )
    # [END howto_operator_cloud_run_execute_python_job]

    # [START howto_operator_cloud_run_execute_python_job_logs_only]
    cloud_python_callable_logs = CloudRunExecutePythonJobOperator(
        task_id="cloud_python_callable_logs",
        project_id=PROJECT_ID,
        location=LOCATION,
        python_callable=callable_with_logs,
        image_repository=IMAGE_REPOSITORY,
        op_kwargs={"choices": ["x", "y"]},
        env_vars=[k8s_min.EnvVar(name="DEMO_ENV", value="cloud-run-system-test")],
        do_xcom_push=False,
        logs_volume=LOGS_VOLUME,
        cache_image_repository=CACHE_IMAGE_REPOSITORY,
    )
    # [END howto_operator_cloud_run_execute_python_job_logs_only]

    # [START howto_operator_cloud_run_execute_python_job_from_python_file]
    cloud_python_file = CloudRunExecutePythonJobOperator(
        task_id="cloud_python_file",
        project_id=PROJECT_ID,
        location=LOCATION,
        python_file=PYTHON_FILE_URI,
        entry_point="run_from_file",
        image_repository=IMAGE_REPOSITORY,
        op_kwargs={"choices": ["red", "blue"], "prefix": "from-file"},
        python_version="3.11",
        xcom_volume=XCOM_VOLUME,
    )
    # [END howto_operator_cloud_run_execute_python_job_from_python_file]

    delete_test_objects = GCSDeleteObjectsOperator(
        task_id="delete_test_objects",
        bucket_name=BUCKET_NAME,
        prefix=["cloud-run/", "cloud-run-results/"],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_bucket >> cloud_python_callable_return >> delete_test_objects
    create_bucket >> cloud_python_callable_logs >> delete_test_objects
    create_bucket >> cloud_python_file >> delete_test_objects
    delete_test_objects >> delete_bucket

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
