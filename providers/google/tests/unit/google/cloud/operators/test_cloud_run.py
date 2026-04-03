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
This module contains various unit tests for GCP Cloud Build Operators
"""

from __future__ import annotations

from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import datetime
from unittest import mock

import pytest
from google.api_core.exceptions import AlreadyExists
from google.cloud.exceptions import GoogleCloudError
from google.cloud.run_v2 import Job, Service

from airflow.models.dag import DAG
from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunCreateJobOperator,
    CloudRunCreateServiceOperator,
    CloudRunDeleteJobOperator,
    CloudRunDeleteServiceOperator,
    CloudRunExecuteJobOperator,
    CloudRunExecutePythonJobOperator,
    CloudRunInfrastructureExecutionError,
    CloudRunJobTimeoutError,
    CloudRunListJobsOperator,
    CloudRunUpdateJobOperator,
    CloudRunUserCodeExecutionError,
)
from airflow.providers.google.cloud.triggers.cloud_run import RunJobStatus

CLOUD_RUN_HOOK_PATH = "airflow.providers.google.cloud.operators.cloud_run.CloudRunHook"
CLOUD_RUN_SERVICE_HOOK_PATH = "airflow.providers.google.cloud.operators.cloud_run.CloudRunServiceHook"
TASK_ID = "test"
PROJECT_ID = "testproject"
REGION = "us-central1"
JOB_NAME = "jobname"
SERVICE_NAME = "servicename"
OVERRIDES = {
    "container_overrides": [{"args": ["python", "main.py"]}],
    "task_count": 1,
    "timeout": "60s",
}

JOB = Job()
JOB.name = JOB_NAME

SERVICE = Service()
SERVICE.name = SERVICE_NAME


def create_context(task):
    dag = task.dag if task.has_dag() else DAG(dag_id="dag", schedule=None)
    if not task.has_dag():
        task.dag = dag
    logical_date = datetime(2022, 1, 1, 0, 0, 0)
    task_instance = mock.Mock()
    task_instance.try_number = 1
    task_instance.xcom_push = mock.Mock()
    return {
        "dag": dag,
        "run_id": "manual__2022-01-01T00:00:00+00:00",
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
        "logical_date": logical_date,
        "params": {"message": "hello"},
        "ds": "2022-01-01",
    }


def _assert_common_template_fields(template_fields):
    assert "project_id" in template_fields
    assert "region" in template_fields
    assert "gcp_conn_id" in template_fields
    assert "impersonation_chain" in template_fields


class TestCloudRunCreateJobOperator:
    def test_template_fields(self):
        operator = CloudRunCreateJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, job=JOB
        )

        _assert_common_template_fields(operator.template_fields)
        assert "job_name" in operator.template_fields

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_create(self, hook_mock):
        hook_mock.return_value.create_job.return_value = JOB

        operator = CloudRunCreateJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, job=JOB
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.create_job.assert_called_once_with(
            job_name=JOB_NAME,
            region=REGION,
            project_id=PROJECT_ID,
            job=JOB,
            use_regional_endpoint=False,
        )


class TestCloudRunExecuteJobOperator:
    def test_template_fields(self):
        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, overrides=OVERRIDES
        )

        _assert_common_template_fields(operator.template_fields)
        assert "job_name" in operator.template_fields
        assert "overrides" in operator.template_fields
        assert "polling_period_seconds" in operator.template_fields
        assert "timeout_seconds" in operator.template_fields
        assert "transport" in operator.template_fields

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_with_transport(self, hook_mock):
        """Test that transport parameter is passed to CloudRunHook."""
        hook_mock.return_value.get_job.return_value = JOB
        hook_mock.return_value.execute_job.return_value = self._mock_operation(3, 3, 0)

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            transport="rest",
        )

        operator.execute(context=mock.MagicMock())

        # Verify that CloudRunHook was instantiated with transport parameter
        hook_mock.assert_called_once()
        call_kwargs = hook_mock.call_args[1]
        assert call_kwargs["transport"] == "rest"

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_success(self, hook_mock):
        hook_mock.return_value.get_job.return_value = JOB
        hook_mock.return_value.execute_job.return_value = self._mock_operation(3, 3, 0)

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.get_job.assert_called_once_with(
            job_name=mock.ANY,
            region=REGION,
            project_id=PROJECT_ID,
            use_regional_endpoint=False,
        )

        hook_mock.return_value.execute_job.assert_called_once_with(
            job_name=JOB_NAME,
            region=REGION,
            project_id=PROJECT_ID,
            overrides=None,
            use_regional_endpoint=False,
        )

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_fail_one_failed_task(self, hook_mock):
        hook_mock.return_value.execute_job.return_value = self._mock_operation(3, 2, 1)

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME
        )

        with pytest.raises(AirflowException) as exception:
            operator.execute(context=mock.MagicMock())

        assert "Some tasks failed execution" in str(exception.value)

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_fail_all_failed_tasks(self, hook_mock):
        hook_mock.return_value.execute_job.return_value = self._mock_operation(3, 0, 3)

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME
        )

        with pytest.raises(AirflowException) as exception:
            operator.execute(context=mock.MagicMock())

        assert "Some tasks failed execution" in str(exception.value)

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_fail_incomplete_failed_tasks(self, hook_mock):
        hook_mock.return_value.execute_job.return_value = self._mock_operation(3, 2, 0)

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME
        )

        with pytest.raises(AirflowException) as exception:
            operator.execute(context=mock.MagicMock())

        assert "Not all tasks finished execution" in str(exception.value)

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_fail_incomplete_succeeded_tasks(self, hook_mock):
        hook_mock.return_value.execute_job.return_value = self._mock_operation(3, 0, 2)

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME
        )

        with pytest.raises(AirflowException) as exception:
            operator.execute(context=mock.MagicMock())

        assert "Not all tasks finished execution" in str(exception.value)

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_deferrable(self, hook_mock):
        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, deferrable=True
        )

        with pytest.raises(TaskDeferred):
            operator.execute(mock.MagicMock())

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_deferrable_execute_complete_method_timeout(self, hook_mock):
        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, deferrable=True
        )

        event = {"status": RunJobStatus.TIMEOUT.value, "job_name": JOB_NAME}

        with pytest.raises(AirflowException) as e:
            operator.execute_complete(mock.MagicMock(), event)

        assert "Operation timed out" in str(e.value)

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_deferrable_execute_complete_method_fail(self, hook_mock):
        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, deferrable=True
        )

        error_code = 10
        error_message = "error message"

        event = {
            "status": RunJobStatus.FAIL.value,
            "operation_error_code": error_code,
            "operation_error_message": error_message,
            "job_name": JOB_NAME,
        }

        with pytest.raises(AirflowException) as e:
            operator.execute_complete(mock.MagicMock(), event)

        assert f"Operation failed with error code [{error_code}] and error message [{error_message}]" in str(
            e.value
        )

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_deferrable_execute_complete_method_success(self, hook_mock):
        hook_mock.return_value.get_job.return_value = JOB

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, deferrable=True
        )

        event = {"status": RunJobStatus.SUCCESS.value, "job_name": JOB_NAME}

        result = operator.execute_complete(mock.MagicMock(), event)

        hook_mock.return_value.get_job.assert_called_once_with(
            job_name=mock.ANY,
            region=REGION,
            project_id=PROJECT_ID,
            use_regional_endpoint=False,
        )
        assert result["name"] == JOB_NAME

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_overrides(self, hook_mock):
        hook_mock.return_value.get_job.return_value = JOB
        hook_mock.return_value.execute_job.return_value = self._mock_operation(3, 3, 0)

        overrides = {
            "container_overrides": [{"args": ["python", "main.py"]}],
            "task_count": 1,
            "timeout": "60s",
        }

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, overrides=overrides
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.get_job.assert_called_once_with(
            job_name=mock.ANY,
            region=REGION,
            project_id=PROJECT_ID,
            use_regional_endpoint=False,
        )

        hook_mock.return_value.execute_job.assert_called_once_with(
            job_name=JOB_NAME,
            region=REGION,
            project_id=PROJECT_ID,
            overrides=overrides,
            use_regional_endpoint=False,
        )

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_overrides_with_invalid_task_count(self, hook_mock):
        overrides = {
            "container_overrides": [{"args": ["python", "main.py"]}],
            "task_count": -1,
            "timeout": "60s",
        }

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, overrides=overrides
        )

        with pytest.raises(AirflowException):
            operator.execute(context=mock.MagicMock())

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_overrides_with_invalid_timeout(self, hook_mock):
        overrides = {
            "container_overrides": [{"args": ["python", "main.py"]}],
            "task_count": 1,
            "timeout": "60",
        }

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, overrides=overrides
        )

        with pytest.raises(AirflowException):
            operator.execute(context=mock.MagicMock())

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_overrides_with_invalid_container_args(self, hook_mock):
        overrides = {
            "container_overrides": [{"name": "job", "args": "python main.py"}],
            "task_count": 1,
            "timeout": "60s",
        }

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, overrides=overrides
        )

        with pytest.raises(AirflowException):
            operator.execute(context=mock.MagicMock())

    def _mock_operation(self, task_count, succeeded_count, failed_count):
        operation = mock.MagicMock()
        operation.result.return_value = self._mock_execution(task_count, succeeded_count, failed_count)
        return operation

    def _mock_execution(self, task_count, succeeded_count, failed_count):
        execution = mock.MagicMock()
        execution.task_count = task_count
        execution.succeeded_count = succeeded_count
        execution.failed_count = failed_count
        return execution


class TestCloudRunDeleteJobOperator:
    def test_template_fields(self):
        operator = CloudRunDeleteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME
        )

        _assert_common_template_fields(operator.template_fields)
        assert "job_name" in operator.template_fields

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute(self, hook_mock):
        hook_mock.return_value.delete_job.return_value = JOB

        operator = CloudRunDeleteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME
        )

        deleted_job = operator.execute(context=mock.MagicMock())

        assert deleted_job["name"] == JOB.name

        hook_mock.return_value.delete_job.assert_called_once_with(
            job_name=JOB_NAME,
            region=REGION,
            project_id=PROJECT_ID,
            use_regional_endpoint=False,
        )


class TestCloudRunUpdateJobOperator:
    def test_template_fields(self):
        operator = CloudRunUpdateJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, job=JOB
        )

        _assert_common_template_fields(operator.template_fields)
        assert "job_name" in operator.template_fields

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute(self, hook_mock):
        hook_mock.return_value.update_job.return_value = JOB

        operator = CloudRunUpdateJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, job=JOB
        )

        updated_job = operator.execute(context=mock.MagicMock())

        assert updated_job["name"] == JOB.name

        hook_mock.return_value.update_job.assert_called_once_with(
            job_name=JOB_NAME,
            job=JOB,
            region=REGION,
            project_id=PROJECT_ID,
            use_regional_endpoint=False,
        )


class TestCloudRunListJobsOperator:
    def test_template_fields(self):
        operator = CloudRunListJobsOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, limit=2, show_deleted=False
        )

        _assert_common_template_fields(operator.template_fields)

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute(self, hook_mock):
        limit = 2
        show_deleted = True
        operator = CloudRunListJobsOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, limit=limit, show_deleted=show_deleted
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.list_jobs.assert_called_once_with(
            region=REGION,
            project_id=PROJECT_ID,
            limit=limit,
            show_deleted=show_deleted,
            use_regional_endpoint=False,
        )

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_with_invalid_limit(self, hook_mock):
        limit = -1
        with pytest.raises(expected_exception=AirflowException):
            CloudRunListJobsOperator(task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, limit=limit)


class TestCloudRunExecutePythonJobOperator:
    def test_template_fields(self):
        operator = CloudRunExecutePythonJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            location=REGION,
            image_repository="repo/image",
            python_file="gs://bucket/{{ ds }}/code.py",
            entry_point="main",
            xcom_volume="gs://bucket/results/{{ ds }}",
            logs_volume="gs://bucket/logs/{{ params.message }}",
            requirements=["package==1.0"],
            op_kwargs={"message": "{{ params.message }}"},
            timeout="{{ params.timeout_seconds }}",
        )

        assert "image_repository" in operator.template_fields
        assert "python_file" in operator.template_fields
        assert "entry_point" in operator.template_fields
        assert "xcom_volume" in operator.template_fields
        assert "logs_volume" in operator.template_fields
        assert "requirements" in operator.template_fields
        assert "op_args" in operator.template_fields
        assert "op_kwargs" in operator.template_fields
        assert "labels" in operator.template_fields
        assert "network" in operator.template_fields
        assert "subnet" in operator.template_fields
        assert "python_version" in operator.template_fields
        assert "cache_image_repository" in operator.template_fields

    def test_render_template_fields(self):
        with DAG(dag_id="dag", schedule=None):
            operator = CloudRunExecutePythonJobOperator(
                task_id=TASK_ID,
                project_id=PROJECT_ID,
                location=REGION,
                image_repository="repo/image",
                python_file="gs://bucket/{{ ds }}/code.py",
                entry_point="run_{{ params.message }}",
                xcom_volume="gs://bucket/results/{{ ds }}",
                logs_volume="gs://bucket/logs/{{ params.message }}",
                requirements=["package-{{ ds }}"],
                op_kwargs={"message": "{{ params.message }}"},
                timeout="{{ params.timeout_seconds }}",
                params={"message": "hello", "timeout_seconds": 30},
            )

        context = create_context(operator)
        operator.render_template_fields(context)

        assert operator.python_file == "gs://bucket/2022-01-01/code.py"
        assert operator.entry_point == "run_hello"
        assert operator.xcom_volume == "gs://bucket/results/2022-01-01"
        assert operator.logs_volume == "gs://bucket/logs/hello"
        assert operator.requirements == ["package-2022-01-01"]
        assert operator.op_kwargs == {"message": "hello"}
        assert operator.timeout == "{{ params.timeout_seconds }}"

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_run.CloudRunPythonJobImageBuilder.prepare_image"
    )
    def test_execute_uses_timeout_override_and_wait_timeout(self, prepare_image_mock, hook_mock):
        job_operation = mock.MagicMock()
        job_execution = mock.MagicMock()
        job_execution.failed_count = 0
        job_operation.result.return_value = job_execution
        hook_mock.return_value.execute_job.return_value = job_operation
        cloud_run_job = Job()
        cloud_run_job.name = "projects/x/locations/y/jobs/test-job"
        hook_mock.return_value.create_job.return_value = cloud_run_job

        operator = CloudRunExecutePythonJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            location=REGION,
            image_repository="repo/image",
            python_file="gs://bucket/code.py",
            entry_point="main",
            do_xcom_push=False,
            timeout=30,
        )

        operator.execute(context=create_context(operator))

        hook_mock.return_value.execute_job.assert_called_once_with(
            job_name="test-job",
            project_id=PROJECT_ID,
            region=REGION,
            overrides={"timeout": "30s"},
        )
        job_operation.result.assert_called_once_with(timeout=30)

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_run.CloudRunPythonJobImageBuilder.prepare_image"
    )
    def test_execute_timeout_raises_clean_error(self, prepare_image_mock, hook_mock):
        job_operation = mock.MagicMock()
        job_operation.result.side_effect = FutureTimeoutError
        hook_mock.return_value.execute_job.return_value = job_operation
        cloud_run_job = Job()
        cloud_run_job.name = "projects/x/locations/y/jobs/test-job"
        hook_mock.return_value.create_job.return_value = cloud_run_job

        operator = CloudRunExecutePythonJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            location=REGION,
            image_repository="repo/image",
            python_file="gs://bucket/code.py",
            entry_point="main",
            do_xcom_push=False,
            timeout=30,
        )

        with pytest.raises(CloudRunJobTimeoutError):
            operator.execute(context=create_context(operator))

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_run.CloudRunPythonJobImageBuilder.prepare_image"
    )
    def test_execute_user_code_error(self, prepare_image_mock, hook_mock):
        job_operation = mock.MagicMock()
        job_execution = mock.MagicMock()
        job_execution.failed_count = 1
        job_operation.result.return_value = job_execution
        hook_mock.return_value.execute_job.return_value = job_operation
        cloud_run_job = Job()
        cloud_run_job.name = "projects/x/locations/y/jobs/test-job"
        hook_mock.return_value.create_job.return_value = cloud_run_job

        operator = CloudRunExecutePythonJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            location=REGION,
            image_repository="repo/image",
            python_file="gs://bucket/code.py",
            entry_point="main",
            xcom_volume="gs://bucket/results",
            logs_volume="gs://bucket/logs",
        )
        operator._read_logs_from_gcs = mock.Mock(
            return_value="AIRFLOW_USER_CODE_ERROR: ValueError: bad input\nTraceback..."
        )
        operator._emit_collected_logs = mock.Mock()

        with pytest.raises(CloudRunUserCodeExecutionError) as exc_info:
            operator.execute(context=create_context(operator))

        assert "ValueError" in str(exc_info.value)
        assert "bad input" in str(exc_info.value)

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_run.CloudRunPythonJobImageBuilder.prepare_image"
    )
    def test_execute_infrastructure_error(self, prepare_image_mock, hook_mock):
        job_operation = mock.MagicMock()
        job_execution = mock.MagicMock()
        job_execution.failed_count = 1
        job_operation.result.return_value = job_execution
        hook_mock.return_value.execute_job.return_value = job_operation
        cloud_run_job = Job()
        cloud_run_job.name = "projects/x/locations/y/jobs/test-job"
        hook_mock.return_value.create_job.return_value = cloud_run_job

        operator = CloudRunExecutePythonJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            location=REGION,
            image_repository="repo/image",
            python_file="gs://bucket/code.py",
            entry_point="main",
            xcom_volume="gs://bucket/results",
            logs_volume="gs://bucket/logs",
        )
        operator._read_logs_from_gcs = mock.Mock(return_value=None)
        operator._emit_collected_logs = mock.Mock()

        with pytest.raises(CloudRunInfrastructureExecutionError):
            operator.execute(context=create_context(operator))


class TestCloudRunCreateServiceOperator:
    def test_template_fields(self):
        operator = CloudRunCreateServiceOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            service=SERVICE,
            service_name=SERVICE_NAME,
        )

        _assert_common_template_fields(operator.template_fields)
        assert "service_name" in operator.template_fields

    @mock.patch(CLOUD_RUN_SERVICE_HOOK_PATH)
    def test_execute(self, hook_mock):
        hook_mock.return_value.create_service.return_value = SERVICE

        operator = CloudRunCreateServiceOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            service=SERVICE,
            service_name=SERVICE_NAME,
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.create_service.assert_called_once_with(
            service=SERVICE,
            service_name=SERVICE_NAME,
            region=REGION,
            project_id=PROJECT_ID,
            use_regional_endpoint=False,
        )

    @mock.patch(CLOUD_RUN_SERVICE_HOOK_PATH)
    def test_execute_already_exists(self, hook_mock):
        hook_mock.return_value.create_service.side_effect = AlreadyExists("Service already exists")
        hook_mock.return_value.get_service.return_value = SERVICE

        operator = CloudRunCreateServiceOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            service=SERVICE,
            service_name=SERVICE_NAME,
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.create_service.assert_called_once_with(
            service=SERVICE,
            service_name=SERVICE_NAME,
            region=REGION,
            project_id=PROJECT_ID,
            use_regional_endpoint=False,
        )
        hook_mock.return_value.get_service.assert_called_once_with(
            service_name=SERVICE_NAME,
            region=REGION,
            project_id=PROJECT_ID,
            use_regional_endpoint=False,
        )

    @mock.patch(CLOUD_RUN_SERVICE_HOOK_PATH)
    def test_execute_when_other_error(self, hook_mock):
        error_message = "An error occurred. Exiting."
        hook_mock.return_value.create_service.side_effect = GoogleCloudError(error_message, errors=None)

        operator = CloudRunCreateServiceOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            service=SERVICE,
            service_name=SERVICE_NAME,
        )

        with pytest.raises(expected_exception=GoogleCloudError) as context:
            operator.execute(context=mock.MagicMock())

        assert error_message == context.value.message

        hook_mock.return_value.create_service.assert_called_once_with(
            service=SERVICE,
            service_name=SERVICE_NAME,
            region=REGION,
            project_id=PROJECT_ID,
            use_regional_endpoint=False,
        )


class TestCloudRunDeleteServiceOperator:
    def test_template_fields(self):
        operator = CloudRunDeleteServiceOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            service_name=SERVICE_NAME,
        )

        _assert_common_template_fields(operator.template_fields)
        assert "service_name" in operator.template_fields

    @mock.patch(CLOUD_RUN_SERVICE_HOOK_PATH)
    def test_execute(self, hook_mock):
        hook_mock.return_value.delete_service.return_value = SERVICE

        operator = CloudRunDeleteServiceOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            service_name=SERVICE_NAME,
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.delete_service.assert_called_once_with(
            service_name=SERVICE_NAME,
            region=REGION,
            project_id=PROJECT_ID,
            use_regional_endpoint=False,
        )
