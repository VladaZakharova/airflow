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
import logging
from unittest import mock

import pytest
from google.cloud.dataflow_v1beta3 import JobState

from airflow.providers.google.cloud.triggers.dataflow import (
    DataflowJobAutoScalingEventTrigger,
    DataflowJobMessagesTrigger,
    TemplateJobStartTrigger,
)
from airflow.triggers.base import TriggerEvent

PROJECT_ID = "test-project-id"
JOB_ID = "test_job_id_2012-12-23-10:00"
LOCATION = "us-central1"
GCP_CONN_ID = "test_gcp_conn_id"
POLL_SLEEP = 20
IMPERSONATION_CHAIN = ["impersonate", "this"]
CANCEL_TIMEOUT = 10 * 420


@pytest.fixture
def template_job_start_trigger():
    return TemplateJobStartTrigger(
        project_id=PROJECT_ID,
        job_id=JOB_ID,
        location=LOCATION,
        gcp_conn_id=GCP_CONN_ID,
        poll_sleep=POLL_SLEEP,
        impersonation_chain=IMPERSONATION_CHAIN,
        cancel_timeout=CANCEL_TIMEOUT,
    )


@pytest.fixture
def dataflow_job_autoscaling_event_trigger():
    return DataflowJobAutoScalingEventTrigger(
        project_id=PROJECT_ID,
        job_id=JOB_ID,
        location=LOCATION,
        gcp_conn_id=GCP_CONN_ID,
        poll_sleep=POLL_SLEEP,
        impersonation_chain=IMPERSONATION_CHAIN,
        cancel_timeout=CANCEL_TIMEOUT,
        fail_on_terminal_state=False,
    )


@pytest.fixture
def dataflow_job_messages_trigger():
    return DataflowJobMessagesTrigger(
        project_id=PROJECT_ID,
        job_id=JOB_ID,
        location=LOCATION,
        gcp_conn_id=GCP_CONN_ID,
        poll_sleep=POLL_SLEEP,
        impersonation_chain=IMPERSONATION_CHAIN,
        cancel_timeout=CANCEL_TIMEOUT,
        fail_on_terminal_state=False,
    )


class TestTemplateJobStartTrigger:
    def test_serialize(self, template_job_start_trigger):
        actual_data = template_job_start_trigger.serialize()
        expected_data = (
            "airflow.providers.google.cloud.triggers.dataflow.TemplateJobStartTrigger",
            {
                "project_id": PROJECT_ID,
                "job_id": JOB_ID,
                "location": LOCATION,
                "gcp_conn_id": GCP_CONN_ID,
                "poll_sleep": POLL_SLEEP,
                "impersonation_chain": IMPERSONATION_CHAIN,
                "cancel_timeout": CANCEL_TIMEOUT,
            },
        )
        assert actual_data == expected_data

    @pytest.mark.parametrize(
        "attr, expected",
        [
            ("gcp_conn_id", GCP_CONN_ID),
            ("poll_sleep", POLL_SLEEP),
            ("impersonation_chain", IMPERSONATION_CHAIN),
            ("cancel_timeout", CANCEL_TIMEOUT),
        ],
    )
    def test_get_async_hook(self, template_job_start_trigger, attr, expected):
        hook = template_job_start_trigger._get_async_hook()
        actual = hook._hook_kwargs.get(attr)
        assert actual is not None
        assert actual == expected

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_loop_return_success_event(self, mock_job_status, template_job_start_trigger):
        mock_job_status.return_value = JobState.JOB_STATE_DONE

        expected_event = TriggerEvent(
            {
                "job_id": JOB_ID,
                "status": "success",
                "message": "Job completed",
            }
        )
        actual_event = await template_job_start_trigger.run().asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_loop_return_failed_event(self, mock_job_status, template_job_start_trigger):
        mock_job_status.return_value = JobState.JOB_STATE_FAILED

        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": f"Dataflow job with id {JOB_ID} has failed its execution",
            }
        )
        actual_event = await template_job_start_trigger.run().asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_loop_return_stopped_event(self, mock_job_status, template_job_start_trigger):
        mock_job_status.return_value = JobState.JOB_STATE_STOPPED
        expected_event = TriggerEvent(
            {
                "status": "stopped",
                "message": f"Dataflow job with id {JOB_ID} was stopped",
            }
        )
        actual_event = await template_job_start_trigger.run().asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_loop_is_still_running(self, mock_job_status, template_job_start_trigger, caplog):
        mock_job_status.return_value = JobState.JOB_STATE_RUNNING
        caplog.set_level(logging.INFO)

        task = asyncio.create_task(template_job_start_trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert not task.done()
        assert f"Current job status is: {JobState.JOB_STATE_RUNNING}"
        assert f"Sleeping for {POLL_SLEEP} seconds."
        # cancel the task to suppress test warnings
        task.cancel()


# LO!
class TestDataflowJobAutoScalingEventTrigger:

    def test_serialize(self, dataflow_job_autoscaling_event_trigger):
        expected_data = (
            "airflow.providers.google.cloud.triggers.dataflow.DataflowJobAutoScalingEventTrigger",
            {
                "project_id": PROJECT_ID,
                "job_id": JOB_ID,
                "location": LOCATION,
                "gcp_conn_id": GCP_CONN_ID,
                "poll_sleep": POLL_SLEEP,
                "impersonation_chain": IMPERSONATION_CHAIN,
                "cancel_timeout": CANCEL_TIMEOUT,
                "fail_on_terminal_state": False,
            },
        )
        actual_data = dataflow_job_autoscaling_event_trigger.serialize()
        assert actual_data == expected_data

    @pytest.mark.parametrize(
    "attr, expected",
    [
        ("gcp_conn_id", GCP_CONN_ID),
        ("poll_sleep", POLL_SLEEP),
        ("impersonation_chain", IMPERSONATION_CHAIN),
        ("cancel_timeout", CANCEL_TIMEOUT),
    ],
    )
    def test_get_async_hook(self, dataflow_job_autoscaling_event_trigger, attr, expected):
        hook = dataflow_job_autoscaling_event_trigger._get_async_hook()
        actual = hook._hook_kwargs.get(attr)
        assert actual is not None
        assert actual == expected

    @pytest.mark.parametrize(
        "job_status_value",
        [
            JobState.JOB_STATE_DONE.value,
            JobState.JOB_STATE_FAILED.value,
            JobState.JOB_STATE_CANCELLED.value,
            JobState.JOB_STATE_UPDATED.value,
            JobState.JOB_STATE_DRAINED.value,
        ]
    )
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    @mock.patch("airflow.providers.google.cloud.triggers.dataflow.DataflowJobAutoScalingEventTrigger.list_job_autoscaling_events")
    async def test_run_yields_terminal_state_event_if_fail_on_terminal_state(
        self,
        mock_list_job_autoscaling_events,
        mock_job_status,
        job_status_value,
        dataflow_job_autoscaling_event_trigger,
    ):
        dataflow_job_autoscaling_event_trigger.fail_on_terminal_state = True
        mock_list_job_autoscaling_events.return_value = []
        mock_job_status.return_value = job_status_value
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": f"Job with id '{JOB_ID}' is already in terminal state: {job_status_value}",
                "result": None,
            }
        )
        actual_event = await dataflow_job_autoscaling_event_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    @mock.patch("airflow.providers.google.cloud.triggers.dataflow.DataflowJobAutoScalingEventTrigger.list_job_autoscaling_events")
    async def test_run_loop_is_still_running_if_fail_on_terminal_state(
        self,
        mock_list_job_autoscaling_events,
        mock_job_status,
        dataflow_job_autoscaling_event_trigger,
        caplog,
    ):
        """Test that DataflowJobAutoScalingEventTrigger is still in loop if the job status is RUNNING."""
        dataflow_job_autoscaling_event_trigger.fail_on_terminal_state = True
        mock_job_status.return_value = "not a terminal state"
        mock_list_job_autoscaling_events.return_value = []
        caplog.set_level(logging.INFO)
        task = asyncio.create_task(dataflow_job_autoscaling_event_trigger.run().__anext__())
        await asyncio.sleep(0.5)
        assert task.done() is False
        # cancel the task to suppress test warnings
        task.cancel()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    @mock.patch("airflow.providers.google.cloud.triggers.dataflow.DataflowJobAutoScalingEventTrigger.list_job_autoscaling_events")
    async def test_run_yields_autoscaling_events(self, mock_list_job_autoscaling_events, mock_job_status, dataflow_job_autoscaling_event_trigger):
        mock_job_status.return_value = JobState.JOB_STATE_DONE.value
        test_autoscaling_events = [
            {
                'event_type': 2,
                'description': {},
                'time': '2024-02-05T13:43:31.066611771Z',
                'worker_pool': 'Regular',
                'current_num_workers': '0',
                'target_num_workers': '0',
            },
            {
                'target_num_workers': '1',
                'event_type': 1, 'description': {},
                'time': '2024-02-05T13:43:31.066611771Z',
                'worker_pool': 'Regular',
                'current_num_workers': '0',
            },
        ]
        mock_list_job_autoscaling_events.return_value = test_autoscaling_events
        expected_event = TriggerEvent(
            {
                "status": "success",
                "message": f"Detected 2 autoscaling events for job '{JOB_ID}'",
                "result": test_autoscaling_events,
            }
        )
        actual_event = await dataflow_job_autoscaling_event_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_raises_exception(self, mock_job_status, dataflow_job_autoscaling_event_trigger):
        """
        Tests the DataflowJobAutoScalingEventTrigger does fire if there is an exception.
        """
        mock_job_status.side_effect = mock.AsyncMock(side_effect=Exception("Test exception"))
        expected_event = TriggerEvent({"status": "error", "message": "Test exception", "result": None})
        actual_event = await dataflow_job_autoscaling_event_trigger.run().asend(None)
        assert expected_event == actual_event

    def test_mapped_terminal_job_statuses_are_mapped_correctly(self, dataflow_job_autoscaling_event_trigger):
        """
        Tests the DataflowJobAutoScalingEventTrigger does fire if there is an exception.
        """
        expected_job_status_values = [
            JobState.JOB_STATE_DONE.value,
            JobState.JOB_STATE_FAILED.value,
            JobState.JOB_STATE_CANCELLED.value,
            JobState.JOB_STATE_UPDATED.value,
            JobState.JOB_STATE_DRAINED.value,
        ]
        actual_job_status_values = sorted(dataflow_job_autoscaling_event_trigger.mapped_terminal_job_statuses)
        assert expected_job_status_values == actual_job_status_values


class TestDataflowJobMessagesTrigger:
    """Test case for DataflowJobMessagesTrigger"""

    def test_serialize(self, dataflow_job_messages_trigger):
        expected_data = (
            "airflow.providers.google.cloud.triggers.dataflow.DataflowJobMessagesTrigger",
            {
                "project_id": PROJECT_ID,
                "job_id": JOB_ID,
                "location": LOCATION,
                "gcp_conn_id": GCP_CONN_ID,
                "poll_sleep": POLL_SLEEP,
                "impersonation_chain": IMPERSONATION_CHAIN,
                "cancel_timeout": CANCEL_TIMEOUT,
                "fail_on_terminal_state": False,
            },
        )
        actual_data = dataflow_job_messages_trigger.serialize()
        assert actual_data == expected_data

    @pytest.mark.parametrize(
    "attr, expected",
    [
        ("gcp_conn_id", GCP_CONN_ID),
        ("poll_sleep", POLL_SLEEP),
        ("impersonation_chain", IMPERSONATION_CHAIN),
        ("cancel_timeout", CANCEL_TIMEOUT),
    ],
    )
    def test_get_async_hook(self, dataflow_job_messages_trigger, attr, expected):
        hook = dataflow_job_messages_trigger._get_async_hook()
        actual = hook._hook_kwargs.get(attr)
        assert actual is not None
        assert actual == expected

    @pytest.mark.parametrize(
        "job_status_value",
        [
            JobState.JOB_STATE_DONE.value,
            JobState.JOB_STATE_FAILED.value,
            JobState.JOB_STATE_CANCELLED.value,
            JobState.JOB_STATE_UPDATED.value,
            JobState.JOB_STATE_DRAINED.value,
        ]
    )
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    @mock.patch("airflow.providers.google.cloud.triggers.dataflow.DataflowJobMessagesTrigger.list_job_messages")
    async def test_run_yields_terminal_state_event_if_fail_on_terminal_state(
        self,
        mock_list_job_messages,
        mock_job_status,
        job_status_value,
        dataflow_job_messages_trigger,
    ):
        dataflow_job_messages_trigger.fail_on_terminal_state = True
        mock_list_job_messages.return_value = []
        mock_job_status.return_value = job_status_value
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": f"Job with id '{JOB_ID}' is already in terminal state: {job_status_value}",
                "result": None,
            }
        )
        actual_event = await dataflow_job_messages_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    @mock.patch("airflow.providers.google.cloud.triggers.dataflow.DataflowJobMessagesTrigger.list_job_messages")
    async def test_run_loop_is_still_running_if_fail_on_terminal_state(
        self,
        mock_list_job_messages,
        mock_job_status,
        dataflow_job_messages_trigger,
        caplog,
    ):
        """Test that DataflowJobMessagesTrigger is still in loop if the job status is RUNNING."""
        dataflow_job_messages_trigger.fail_on_terminal_state = True
        mock_job_status.return_value = "not a terminal state"
        mock_list_job_messages.return_value = []
        caplog.set_level(logging.INFO)
        task = asyncio.create_task(dataflow_job_messages_trigger.run().__anext__())
        await asyncio.sleep(0.5)
        assert task.done() is False
        # cancel the task to suppress test warnings
        task.cancel()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    @mock.patch("airflow.providers.google.cloud.triggers.dataflow.DataflowJobMessagesTrigger.list_job_messages")
    async def test_run_yields_job_messages(self, mock_list_job_messages, mock_job_status, dataflow_job_messages_trigger):
        mock_job_status.return_value = JobState.JOB_STATE_DONE.value
        test_job_messages = [
            {'id': '1707695235850', 'time': '2024-02-06T23:47:15.850Z', 'message_text': 'Dataflow Runner V2 auto-enabled.', 'message_importance': 5},
            {'id': '1707695635401', 'time': '2024-02-06T23:53:55.401Z', 'message_text': 'Worker configuration: n1-standard-1 in europe-west1-d.', 'message_importance': 5},
        ]
        mock_list_job_messages.return_value = test_job_messages
        expected_event = TriggerEvent(
            {
                "status": "success",
                "message": f"Detected 2 job messages for job '{JOB_ID}'",
                "result": test_job_messages,
            }
        )
        actual_event = await dataflow_job_messages_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_raises_exception(self, mock_job_status, dataflow_job_messages_trigger):
        """
        Tests the DataflowJobMessagesTrigger does fire if there is an exception.
        """
        mock_job_status.side_effect = mock.AsyncMock(side_effect=Exception("Test exception"))
        expected_event = TriggerEvent({"status": "error", "message": "Test exception", "result": None})
        actual_event = await dataflow_job_messages_trigger.run().asend(None)
        assert expected_event == actual_event

    def test_mapped_terminal_job_statuses_are_mapped_correctly(self, dataflow_job_messages_trigger):
        """
        Tests the DataflowJobMessagesTrigger does fire if there is an exception.
        """
        expected_job_status_values = [
            JobState.JOB_STATE_DONE.value,
            JobState.JOB_STATE_FAILED.value,
            JobState.JOB_STATE_CANCELLED.value,
            JobState.JOB_STATE_UPDATED.value,
            JobState.JOB_STATE_DRAINED.value,
        ]
        actual_job_status_values = sorted(dataflow_job_messages_trigger.mapped_terminal_job_statuses)
        assert expected_job_status_values == actual_job_status_values
