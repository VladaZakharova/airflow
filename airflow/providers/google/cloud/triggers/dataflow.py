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
from typing import TYPE_CHECKING, Any, Sequence

from google.cloud.dataflow_v1beta3 import JobState
from google.cloud.dataflow_v1beta3.types import AutoscalingEvent, JobMessage

from airflow.providers.google.cloud.hooks.dataflow import AsyncDataflowHook, DataflowJobStatus
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from google.cloud.dataflow_v1beta3.services.messages_v1_beta3.pagers import ListJobMessagesAsyncPager


DEFAULT_DATAFLOW_LOCATION = "us-central1"


class TemplateJobStartTrigger(BaseTrigger):
    """Dataflow trigger to check if templated job has been finished.

    :param project_id: Required. the Google Cloud project ID in which the job was started.
    :param job_id: Required. ID of the job.
    :param location: Optional. the location where job is executed. If set to None then
        the value of DEFAULT_DATAFLOW_LOCATION will be used
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param cancel_timeout: Optional. How long (in seconds) operator should wait for the pipeline to be
        successfully cancelled when task is being killed.
    """

    def __init__(
        self,
        job_id: str,
        project_id: str | None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        poll_sleep: int = 10,
        impersonation_chain: str | Sequence[str] | None = None,
        cancel_timeout: int | None = 5 * 60,
    ):
        super().__init__()
        self.project_id = project_id
        self.job_id = job_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.poll_sleep = poll_sleep
        self.impersonation_chain = impersonation_chain
        self.cancel_timeout = cancel_timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize class arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.dataflow.TemplateJobStartTrigger",
            {
                "project_id": self.project_id,
                "job_id": self.job_id,
                "location": self.location,
                "gcp_conn_id": self.gcp_conn_id,
                "poll_sleep": self.poll_sleep,
                "impersonation_chain": self.impersonation_chain,
                "cancel_timeout": self.cancel_timeout,
            },
        )

    async def run(self):
        """
        Fetch job status or yield certain Events.

        Main loop of the class in where it is fetching the job status and yields certain Event.

        If the job has status success then it yields TriggerEvent with success status, if job has
        status failed - with error status. In any other case Trigger will wait for specified
        amount of time stored in self.poll_sleep variable.
        """
        hook = self._get_async_hook()
        try:
            while True:
                status = await hook.get_job_status(
                    project_id=self.project_id,
                    job_id=self.job_id,
                    location=self.location,
                )
                if status == JobState.JOB_STATE_DONE:
                    yield TriggerEvent(
                        {
                            "job_id": self.job_id,
                            "status": "success",
                            "message": "Job completed",
                        }
                    )
                    return
                elif status == JobState.JOB_STATE_FAILED:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": f"Dataflow job with id {self.job_id} has failed its execution",
                        }
                    )
                    return
                elif status == JobState.JOB_STATE_STOPPED:
                    yield TriggerEvent(
                        {
                            "status": "stopped",
                            "message": f"Dataflow job with id {self.job_id} was stopped",
                        }
                    )
                    return
                else:
                    self.log.info("Job is still running...")
                    self.log.info("Current job status is: %s", status)
                    self.log.info("Sleeping for %s seconds.", self.poll_sleep)
                    await asyncio.sleep(self.poll_sleep)
        except Exception as e:
            self.log.exception("Exception occurred while checking for job completion.")
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> AsyncDataflowHook:
        return AsyncDataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            poll_sleep=self.poll_sleep,
            impersonation_chain=self.impersonation_chain,
            cancel_timeout=self.cancel_timeout,
        )


class DataflowJobAutoScalingEventTrigger(BaseTrigger):
    """
    Trigger that checks for autoscaling events associated with a Dataflow job.

    :param job_id: Required. ID of the job.
    :param project_id: Required. The Google Cloud project ID in which the job was started.
    :param location: Optional. The location where the job is executed. If set to None then
        the value of DEFAULT_DATAFLOW_LOCATION will be used.
    :param gcp_conn_id: The connection ID to use for connecting to Google Cloud.
    :param poll_sleep: Time (seconds) to wait between two consecutive calls to check the job.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param cancel_timeout: Optional. How long (in seconds) trigger should wait for the pipeline to be
        successfully cancelled when task is being killed. Default value is 300s.
    :param fail_on_terminal_state: If set to True the trigger will yield a TriggerEvent with
        error status if the job is in a terminal state. Default is True.
    """

    def __init__(
        self,
        job_id: str,
        project_id: str | None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        poll_sleep: int = 10,
        impersonation_chain: str | Sequence[str] | None = None,
        cancel_timeout: int | None = 5 * 60,
        fail_on_terminal_state: bool = True,
    ):
        super().__init__()
        self.project_id = project_id
        self.job_id = job_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.poll_sleep = poll_sleep
        self.impersonation_chain = impersonation_chain
        self.cancel_timeout = cancel_timeout
        self.fail_on_terminal_state = fail_on_terminal_state

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize class arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.dataflow.DataflowJobAutoScalingEventTrigger",
            {
                "project_id": self.project_id,
                "job_id": self.job_id,
                "location": self.location,
                "gcp_conn_id": self.gcp_conn_id,
                "poll_sleep": self.poll_sleep,
                "impersonation_chain": self.impersonation_chain,
                "cancel_timeout": self.cancel_timeout,
                "fail_on_terminal_state": self.fail_on_terminal_state,
            },
        )

    async def run(self):
        """
        Loop until a terminal job status or any autoscaling events are returned.

        Yields a TriggerEvent with success status, if the client returns any autoscaling events
        and fail_on_terminal_state attribute is False.

        Yields a TriggerEvent with error status, if the client returns a job status with
        a terminal state value and fail_on_terminal_state attribute is True.

        Yields a TriggerEvent with error status, if any exception is raised while looping.

        In any other case the Trigger will wait for a specified amount of time
        stored in self.poll_sleep variable.
        """
        hook = self._get_async_hook()
        while True:
            try:
                job_status = await hook.get_job_status(
                    job_id=self.job_id,
                    project_id=self.project_id,
                    location=self.location,
                )
                autoscaling_events = await self.list_job_autoscaling_events(hook=hook)
                if self.fail_on_terminal_state:
                    if job_status in self.mapped_terminal_job_statuses:
                        yield TriggerEvent(
                            {
                                "status": "error",
                                "message": f"Job with id '{self.job_id}' is already in terminal state: {job_status}",
                                "result": None,
                            }
                        )
                        return
                if autoscaling_events:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": f"Detected {len(autoscaling_events)} autoscaling events for job '{self.job_id}'",
                            "result": autoscaling_events,
                        }
                    )
                    return
                self.log.info("Sleeping for %s seconds.", self.poll_sleep)
                await asyncio.sleep(self.poll_sleep)
            except Exception as e:
                self.log.error("Exception occurred while checking for job's autoscaling events!")
                yield TriggerEvent({"status": "error", "message": str(e), "result": None})

    def _get_async_hook(self) -> AsyncDataflowHook:
        return AsyncDataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            poll_sleep=self.poll_sleep,
            impersonation_chain=self.impersonation_chain,
            cancel_timeout=self.cancel_timeout,
        )

    async def list_job_autoscaling_events(
        self, hook: AsyncDataflowHook, **kwargs
    ) -> list[dict[str, str | dict]]:
        """Wait for the Dataflow client response and then return it in a serialized list."""
        job_response: ListJobMessagesAsyncPager = await hook.list_job_messages(
            job_id=self.job_id,
            project_id=self.project_id,
            location=self.location,
            **kwargs,
        )
        return self._get_autoscaling_events_from_job_response(job_response)

    def _get_autoscaling_events_from_job_response(
        self, job_response: ListJobMessagesAsyncPager
    ) -> list[dict[str, str | dict]]:
        """Return a list of serialized AutoscalingEvent objects."""
        return [AutoscalingEvent.to_dict(event) for event in job_response.autoscaling_events]

    @property
    def mapped_terminal_job_statuses(self) -> list[int]:
        return [getattr(JobState, state).value for state in DataflowJobStatus.TERMINAL_STATES]


class DataflowJobMessagesTrigger(BaseTrigger):
    """
    Trigger that checks for job messages associated with a Dataflow job.

    :param job_id: Required. ID of the job.
    :param project_id: Required. The Google Cloud project ID in which the job was started.
    :param location: Optional. The location where the job is executed. If set to None then
        the value of DEFAULT_DATAFLOW_LOCATION will be used.
    :param gcp_conn_id: The connection ID to use for connecting to Google Cloud.
    :param poll_sleep: Time (seconds) to wait between two consecutive calls to check the job.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param cancel_timeout: Optional. How long (in seconds) trigger should wait for the pipeline to be
        successfully cancelled when task is being killed. Default value is 300s.
    :param fail_on_terminal_state: If set to True the trigger will yield a TriggerEvent with
        error status if the job is in a terminal state. Default is True.
    """

    def __init__(
        self,
        job_id: str,
        project_id: str | None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        poll_sleep: int = 10,
        impersonation_chain: str | Sequence[str] | None = None,
        cancel_timeout: int | None = 5 * 60,
        fail_on_terminal_state: bool = True,
    ):
        super().__init__()
        self.project_id = project_id
        self.job_id = job_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.poll_sleep = poll_sleep
        self.impersonation_chain = impersonation_chain
        self.cancel_timeout = cancel_timeout
        self.fail_on_terminal_state = fail_on_terminal_state

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize class arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.dataflow.DataflowJobMessagesTrigger",
            {
                "project_id": self.project_id,
                "job_id": self.job_id,
                "location": self.location,
                "gcp_conn_id": self.gcp_conn_id,
                "poll_sleep": self.poll_sleep,
                "impersonation_chain": self.impersonation_chain,
                "cancel_timeout": self.cancel_timeout,
                "fail_on_terminal_state": self.fail_on_terminal_state,
            },
        )

    async def run(self):
        """
        Loop until a terminal job status or any job messages are returned.

        Yields a TriggerEvent with success status, if the client returns any job messages
        and fail_on_terminal_state attribute is False.

        Yields a TriggerEvent with error status, if the client returns a job status with
        a terminal state value and fail_on_terminal_state attribute is True.

        Yields a TriggerEvent with error status, if any exception is raised while looping.

        In any other case the Trigger will wait for a specified amount of time
        stored in self.poll_sleep variable.
        """
        hook = self._get_async_hook()
        while True:
            try:
                job_status = await hook.get_job_status(
                    job_id=self.job_id,
                    project_id=self.project_id,
                    location=self.location,
                )
                job_messages = await self.list_job_messages(hook=hook)
                if self.fail_on_terminal_state:
                    if job_status in self.mapped_terminal_job_statuses:
                        yield TriggerEvent(
                            {
                                "status": "error",
                                "message": f"Job with id '{self.job_id}' is already in terminal state: {job_status}",
                                "result": None,
                            }
                        )
                        return
                if job_messages:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": f"Detected {len(job_messages)} job messages for job '{self.job_id}'",
                            "result": job_messages,
                        }
                    )
                    return
                self.log.info("Sleeping for %s seconds.", self.poll_sleep)
                await asyncio.sleep(self.poll_sleep)
            except Exception as e:
                self.log.error("Exception occurred while checking for job's messages!")
                yield TriggerEvent({"status": "error", "message": str(e), "result": None})

    def _get_async_hook(self) -> AsyncDataflowHook:
        return AsyncDataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            poll_sleep=self.poll_sleep,
            impersonation_chain=self.impersonation_chain,
            cancel_timeout=self.cancel_timeout,
        )

    async def list_job_messages(self, hook: AsyncDataflowHook, **kwargs) -> list[dict[str, str | dict]]:
        """Wait for the Dataflow client response and then return it in a serialized list."""
        job_response: ListJobMessagesAsyncPager = await hook.list_job_messages(
            job_id=self.job_id,
            project_id=self.project_id,
            location=self.location,
            **kwargs,
        )
        return self._get_job_messages_from_job_response(job_response)

    def _get_job_messages_from_job_response(
        self, job_response: ListJobMessagesAsyncPager
    ) -> list[dict[str, str | dict]]:
        """Return a list of serialized JobMessage objects."""
        return [JobMessage.to_dict(message) for message in job_response.job_messages]

    @property
    def mapped_terminal_job_statuses(self) -> list[int]:
        return [getattr(JobState, state).value for state in DataflowJobStatus.TERMINAL_STATES]
