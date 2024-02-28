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
from __future__ import annotations

import asyncio
from unittest import mock

import pytest
from google.cloud.aiplatform_v1 import JobState, PipelineState, types

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.triggers.vertex_ai import (
    CreateHyperparameterTuningJobTrigger,
    RunPipelineJobTrigger,
)
from airflow.triggers.base import TriggerEvent

TEST_CONN_ID = "test_connection"
TEST_PROJECT_ID = "test_propject_id"
TEST_LOCATION = "us-central-1"
TEST_HPT_JOB_ID = "test_job_id"
TEST_POLL_INTERVAL = 20
TEST_IMPERSONATION_CHAIN = "test_chain"
TEST_HPT_JOB_NAME = (
    f"projects/{TEST_PROJECT_ID}/locations/{TEST_LOCATION}/hyperparameterTuningJobs/{TEST_HPT_JOB_ID}"
)
VERTEX_AI_TRIGGER_PATH = "airflow.providers.google.cloud.triggers.vertex_ai.{}"


@pytest.fixture
def create_hyperparameter_tuning_job_trigger():
    return CreateHyperparameterTuningJobTrigger(
        conn_id=TEST_CONN_ID,
        project_id=TEST_PROJECT_ID,
        location=TEST_LOCATION,
        job_id=TEST_HPT_JOB_ID,
        poll_interval=TEST_POLL_INTERVAL,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
    )


@pytest.fixture
def run_pipeline_job_trigger():
    return RunPipelineJobTrigger(
        conn_id=TEST_CONN_ID,
        project_id=TEST_PROJECT_ID,
        location=TEST_LOCATION,
        job_id=TEST_HPT_JOB_ID,
        poll_interval=TEST_POLL_INTERVAL,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
    )


class TestCreateHyperparameterTuningJobTrigger:
    def test_serialize(self, create_hyperparameter_tuning_job_trigger):
        classpath, kwargs = create_hyperparameter_tuning_job_trigger.serialize()
        assert (
            classpath
            == "airflow.providers.google.cloud.triggers.vertex_ai.CreateHyperparameterTuningJobTrigger"
        )
        assert kwargs == dict(
            conn_id=TEST_CONN_ID,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            job_id=TEST_HPT_JOB_ID,
            poll_interval=TEST_POLL_INTERVAL,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )

    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("HyperparameterTuningJobAsyncHook"))
    def test_get_async_hook(self, mock_async_hook, create_hyperparameter_tuning_job_trigger):
        hook_expected = mock_async_hook.return_value

        hook_created = create_hyperparameter_tuning_job_trigger._get_async_hook()

        mock_async_hook.assert_called_once_with(
            gcp_conn_id=TEST_CONN_ID, impersonation_chain=TEST_IMPERSONATION_CHAIN
        )
        assert hook_created == hook_expected

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "state, status",
        [
            (JobState.JOB_STATE_CANCELLED, "error"),
            (JobState.JOB_STATE_FAILED, "error"),
            (JobState.JOB_STATE_PAUSED, "success"),
            (JobState.JOB_STATE_SUCCEEDED, "success"),
        ],
    )
    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("HyperparameterTuningJobAsyncHook"))
    @mock.patch("google.cloud.aiplatform_v1.types.HyperparameterTuningJob")
    async def test_run(
        self, mock_hpt_job, mock_async_hook, state, status, create_hyperparameter_tuning_job_trigger
    ):
        mock_job = mock.MagicMock(
            status="success",
            state=state,
        )
        mock_job.name = TEST_HPT_JOB_NAME
        mock_async_wait_hyperparameter_tuning_job = mock.AsyncMock(return_value=mock_job)
        mock_async_hook.return_value.wait_hyperparameter_tuning_job.side_effect = mock.MagicMock(
            side_effect=mock_async_wait_hyperparameter_tuning_job
        )
        mock_dict_job = mock.MagicMock()
        mock_hpt_job.to_dict.return_value = mock_dict_job

        generator = create_hyperparameter_tuning_job_trigger.run()
        event_actual = await generator.asend(None)  # type:ignore[attr-defined]

        mock_async_wait_hyperparameter_tuning_job.assert_awaited_once_with(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            job_id=TEST_HPT_JOB_ID,
            poll_interval=TEST_POLL_INTERVAL,
        )
        assert event_actual == TriggerEvent(
            {
                "status": status,
                "message": f"Hyperparameter tuning job {TEST_HPT_JOB_NAME} completed with status {state.name}",
                "job": mock_dict_job,
            }
        )

    @pytest.mark.asyncio
    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("HyperparameterTuningJobAsyncHook"))
    async def test_run_exception(self, mock_async_hook, create_hyperparameter_tuning_job_trigger):
        mock_async_hook.return_value.wait_hyperparameter_tuning_job.side_effect = AirflowException(
            "test error"
        )

        generator = create_hyperparameter_tuning_job_trigger.run()
        event_actual = await generator.asend(None)  # type:ignore[attr-defined]

        assert event_actual == TriggerEvent(
            {
                "status": "error",
                "message": "test error",
            }
        )


class TestRunPipelineJobTrigger:
    def test_serialize(self, run_pipeline_job_trigger):
        actual_data = run_pipeline_job_trigger.serialize()
        expected_data = (
            "airflow.providers.google.cloud.triggers.vertex_ai.RunPipelineJobTrigger",
            {
                "conn_id": TEST_CONN_ID,
                "project_id": TEST_PROJECT_ID,
                "location": TEST_LOCATION,
                "job_id": TEST_HPT_JOB_ID,
                "poll_interval": TEST_POLL_INTERVAL,
                "impersonation_chain": TEST_IMPERSONATION_CHAIN,
            },
        )
        actual_data == expected_data

    @pytest.mark.asyncio
    async def test_get_async_hook(self, run_pipeline_job_trigger):
        hook = await run_pipeline_job_trigger._get_async_hook()
        actual_conn_id = hook._hook_kwargs.get("gcp_conn_id")
        actual_imp_chain = hook._hook_kwargs.get("impersonation_chain")
        assert (actual_conn_id, actual_imp_chain) == (TEST_CONN_ID, TEST_IMPERSONATION_CHAIN)

    @pytest.mark.parametrize(
        "pipeline_state_value",
        [
            PipelineState.PIPELINE_STATE_SUCCEEDED,
            PipelineState.PIPELINE_STATE_PAUSED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch("google.cloud.aiplatform_v1.types.PipelineJob.to_dict")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.vertex_ai.pipeline_job.PipelineJobAsyncHook.get_pipeline_job"
    )
    async def test_run_yields_success_event_on_successful_pipeline_state(
        self,
        mock_get_pipeline_job,
        mock_pipeline_job_dict,
        run_pipeline_job_trigger,
        pipeline_state_value,
    ):
        mock_get_pipeline_job.return_value = types.PipelineJob(state=pipeline_state_value)
        mock_pipeline_job_dict.return_value = {}
        expected_event = TriggerEvent(
            {
                "status": "success",
                "message": f"Pipeline job '{TEST_HPT_JOB_ID}' has completed with status {pipeline_state_value.name}.",
                "job": {},
            }
        )
        actual_event = await run_pipeline_job_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.parametrize(
        "pipeline_state_value",
        [
            PipelineState.PIPELINE_STATE_FAILED,
            PipelineState.PIPELINE_STATE_CANCELLED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch("google.cloud.aiplatform_v1.types.PipelineJob.to_dict")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.vertex_ai.pipeline_job.PipelineJobAsyncHook.get_pipeline_job"
    )
    async def test_run_yields_error_event_on_failed_pipeline_state(
        self, mock_get_pipeline_job, mock_pipeline_job_dict, pipeline_state_value, run_pipeline_job_trigger
    ):
        mock_get_pipeline_job.return_value = types.PipelineJob(state=pipeline_state_value)
        mock_pipeline_job_dict.return_value = {}
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": f"Pipeline job '{TEST_HPT_JOB_ID}' has completed with status {pipeline_state_value.name}.",
                "job": {},
            }
        )
        actual_event = await run_pipeline_job_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.parametrize(
        "pipeline_state_value",
        [
            PipelineState.PIPELINE_STATE_CANCELLING,
            PipelineState.PIPELINE_STATE_PENDING,
            PipelineState.PIPELINE_STATE_QUEUED,
            PipelineState.PIPELINE_STATE_RUNNING,
            PipelineState.PIPELINE_STATE_UNSPECIFIED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.google.cloud.hooks.vertex_ai.pipeline_job.PipelineJobAsyncHook.get_pipeline_job"
    )
    async def test_run_test_run_loop_is_still_running_if_pipeline_is_running(
        self, mock_get_pipeline_job, pipeline_state_value, run_pipeline_job_trigger
    ):
        mock_get_pipeline_job.return_value = types.PipelineJob(state=pipeline_state_value)
        task = asyncio.create_task(run_pipeline_job_trigger.run().__anext__())
        await asyncio.sleep(0.5)
        assert task.done() is False
        task.cancel()

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.google.cloud.hooks.vertex_ai.pipeline_job.PipelineJobAsyncHook.get_pipeline_job"
    )
    async def test_run_raises_exception(self, mock_get_pipeline_job, run_pipeline_job_trigger):
        """
        Tests the DataflowJobAutoScalingEventTrigger does fire if there is an exception.
        """
        mock_get_pipeline_job.side_effect = mock.AsyncMock(side_effect=Exception("Test exception"))
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": f"Exception occurred when trying to run pipeline job {TEST_HPT_JOB_ID}: Test exception",
                "job": None,
            }
        )
        actual_event = await run_pipeline_job_trigger.run().asend(None)
        assert expected_event == actual_event
