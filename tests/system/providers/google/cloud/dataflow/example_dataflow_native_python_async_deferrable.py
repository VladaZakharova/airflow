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

"""Example Airflow DAG for testing Google Dataflow Beam Pipeline Operator with Asynchronous Python in the deferrable mode."""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.sensors.dataflow import (
    DataflowJobAutoScalingEventsSensor,
    DataflowJobMessagesSensor,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "dataflow_native_python_async_deferrable"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"

PYTHON_FILE_NAME = "wordcount_debugging.py"
GCS_TMP = f"gs://{BUCKET_NAME}/temp/"
GCS_STAGING = f"gs://{BUCKET_NAME}/staging/"
GCS_OUTPUT = f"gs://{BUCKET_NAME}/output"
GCS_PYTHON_SCRIPT = f"gs://{BUCKET_NAME}/{PYTHON_FILE_NAME}"
PYTHON_FILE_LOCAL_PATH = str(Path(__file__).parent / "resources" / PYTHON_FILE_NAME)
LOCATION = "europe-west1"

default_args = {
    "dataflow_default_options": {
        "tempLocation": GCS_TMP,
        "stagingLocation": GCS_STAGING,
    }
}

with DAG(
    DAG_ID,
    default_args=default_args,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataflow"],
) as dag:
    create_bucket = GCSCreateBucketOperator(task_id="create_bucket", bucket_name=BUCKET_NAME)

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_bucket",
        src=PYTHON_FILE_LOCAL_PATH,
        dst=PYTHON_FILE_NAME,
        bucket=BUCKET_NAME,
    )

    # [START howto_operator_start_python_job_async]
    start_python_job_async = BeamRunPythonPipelineOperator(
        task_id="start_python_job_async",
        runner=BeamRunnerType.DataflowRunner,
        py_file=GCS_PYTHON_SCRIPT,
        py_options=[],
        pipeline_options={
            "output": GCS_OUTPUT,
        },
        py_requirements=["apache-beam[gcp]==2.46.0"],
        py_interpreter="python3",
        py_system_site_packages=False,
        dataflow_config={
            "job_name": "start_python_job_async",
            "location": LOCATION,
            "wait_until_finished": False,
        },
    )
    # [END howto_operator_start_python_job_async]

    # [START howto_sensor_wait_for_job_status]
    # wait_for_python_job_async_done = DataflowJobStatusSensor(
    #     task_id="wait_for_python_job_async_done",
    #     job_id="{{task_instance.xcom_pull('start_python_job_async')['dataflow_job_id']}}",
    #     expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
    #     location=LOCATION,
    # )
    # [END howto_sensor_wait_for_job_status]

    # [START howto_sensor_wait_for_job_metric]
    # def check_metric_scalar_gte(metric_name: str, value: int) -> Callable:
    #     """Check is metric greater than equals to given value."""

    #     def callback(metrics: list[dict]) -> bool:
    #         dag.log.info("Looking for '%s' >= %d", metric_name, value)
    #         for metric in metrics:
    #             context = metric.get("name", {}).get("context", {})
    #             original_name = context.get("original_name", "")
    #             tentative = context.get("tentative", "")
    #             if original_name == "Service-cpu_num_seconds" and not tentative:
    #                 return metric["scalar"] >= value
    #         raise AirflowException(f"Metric '{metric_name}' not found in metrics")

    #     return callback

    # wait_for_python_job_async_metric = DataflowJobMetricsSensor(
    #     task_id="wait_for_python_job_async_metric",
    #     job_id="{{task_instance.xcom_pull('start_python_job_async')['dataflow_job_id']}}",
    #     location=LOCATION,
    #     callback=check_metric_scalar_gte(metric_name="Service-cpu_num_seconds", value=100),
    #     fail_on_terminal_state=False,
    # )
    # [END howto_sensor_wait_for_job_metric]

    # [START howto_sensor_wait_for_job_message_deferrable]
    def check_job_message(messages: list[dict]) -> bool:
        """Check message"""
        for message in messages:
            if "Adding workflow start and stop steps." in message.get("messageText", ""):
                return True
        return False

    wait_for_python_job_async_job_message_deferrable = DataflowJobMessagesSensor(
        task_id="wait_for_python_job_async_job_message_deferrable",
        job_id="{{task_instance.xcom_pull('start_python_job_async')['dataflow_job_id']}}",
        location=LOCATION,
        callback=check_job_message,
        fail_on_terminal_state=False,
        deferrable=True,
    )
    # [END howto_sensor_wait_for_job_message_deferrable]

    # [START howto_sensor_wait_for_job_autoscaling_event_deferrable]
    def check_autoscaling_event(autoscaling_events: list[dict]) -> bool:
        """Check autoscaling event"""
        for autoscaling_event in autoscaling_events:
            if "Worker pool started." in autoscaling_event.get("description", {}).get("messageText", ""):
                return True
        return False

    wait_for_python_job_async_autoscaling_event_deferrable = DataflowJobAutoScalingEventsSensor(
        task_id="wait_for_python_job_async_autoscaling_event_deferrable",
        job_id="{{task_instance.xcom_pull('start_python_job_async')['dataflow_job_id']}}",
        location=LOCATION,
        callback=check_autoscaling_event,
        fail_on_terminal_state=False,
        deferrable=True,
    )
    # [END howto_sensor_wait_for_job_autoscaling_event_deferrable]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        >> upload_file
        # TEST BODY
        >> start_python_job_async
        >> [
            # wait_for_python_job_async_done,
            # wait_for_python_job_async_metric,
            wait_for_python_job_async_job_message_deferrable,
            wait_for_python_job_async_autoscaling_event_deferrable,
        ]
        # TEST TEARDOWN
        >> delete_bucket
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
