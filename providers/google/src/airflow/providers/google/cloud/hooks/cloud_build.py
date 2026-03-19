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
"""Hook for Google Cloud Build service."""

from __future__ import annotations

import shlex
import subprocess
from collections.abc import Sequence
from typing import TYPE_CHECKING

from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import AlreadyExists
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.devtools.cloudbuild_v1 import CloudBuildAsyncClient, CloudBuildClient, GetBuildRequest

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook
from airflow.providers.google.common.hooks.operation_helpers import OperationHelper

if TYPE_CHECKING:
    from google.api_core.operation import Operation
    from google.api_core.retry import Retry
    from google.api_core.retry_async import AsyncRetry
    from google.cloud.devtools.cloudbuild_v1.types import Build, BuildTrigger, RepoSource

# Time to sleep between active checks of the operation results
TIME_TO_SLEEP_IN_SECONDS = 5


class CloudBuildHook(GoogleBaseHook, OperationHelper):
    """
    Hook for the Google Cloud Build Service.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain, **kwargs)
        self._client: dict[str, CloudBuildClient] = {}

    def _get_build_id_from_operation(self, operation: Operation) -> str:
        """
        Retrieve Cloud Build ID from Operation Object.

        :param operation: The proto to append resource_label airflow
            version to

        :return: Cloud Build ID
        """
        try:
            return operation.metadata.build.id
        except Exception:
            raise AirflowException("Could not retrieve Build ID from Operation.")

    def get_conn(self, location: str = "global") -> CloudBuildClient:
        """
        Retrieve the connection to Google Cloud Build.

        :param location: The location of the project.

        :return: Google Cloud Build client object.
        """
        if location not in self._client:
            client_options = None
            if location != "global":
                client_options = ClientOptions(api_endpoint=f"{location}-cloudbuild.googleapis.com:443")
            self._client[location] = CloudBuildClient(
                credentials=self.get_credentials(),
                client_info=CLIENT_INFO,
                client_options=client_options,
            )
        return self._client[location]

    def cloud_build_options_to_args(self, options: dict) -> list[str]:
        """
        Return a formatted builds parameters from a dictionary of arguments.

        :param options: Dictionary with options
        :return: List of arguments
        """
        if not options:
            return []

        args: list[str] = []
        for attr, value in options.items():
            if value is None or (isinstance(value, bool) and value):
                args.append(f"--{attr}")
            elif isinstance(value, bool) and not value:
                continue
            elif isinstance(value, list):
                args.extend([f"--{attr}={v}" for v in value])
            elif isinstance(value, dict):
                args.append(f"--{attr}")
                args.extend([f"{k}={v}" for k, v in value.items()])
            else:
                args.append(f"--{attr}={value}")
        return args

    def _build_gcloud_command(self, command: list[str], parameters: dict) -> list[str]:
        return [*command, *(self.cloud_build_options_to_args(parameters))]

    @GoogleBaseHook.fallback_to_default_project_id
    def cancel_build(
        self,
        id_: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str = "global",
    ) -> Build:
        """
        Cancel a build in progress.

        :param id_: The ID of the build.
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.
        :param location: The location of the project.
        """
        client = self.get_conn(location=location)

        self.log.info("Start cancelling build: %s.", id_)

        build = client.cancel_build(
            request={"project_id": project_id, "id": id_},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info("Build has been cancelled: %s.", id_)

        return build

    @GoogleBaseHook.fallback_to_default_project_id
    def create_build_without_waiting_for_result(
        self,
        build: dict | Build,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str = "global",
    ) -> tuple[Operation, str]:
        """
        Start a build with the specified configuration without waiting for it to finish.

        :param build: The build resource to create. If a dict is provided, it must be of the same form
            as the protobuf message `google.cloud.devtools.cloudbuild_v1.types.Build`
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.
        :param location: The location of the project.
        """
        client = self.get_conn(location=location)

        parent = f"projects/{project_id}/locations/{location}"

        self.log.info("Start creating build...")

        operation = client.create_build(
            request={"parent": parent, "project_id": project_id, "build": build},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        id_ = self._get_build_id_from_operation(operation)
        self.log.info("Build has been created: %s.", id_)

        return operation, id_

    @GoogleBaseHook.fallback_to_default_project_id
    def create_build_trigger(
        self,
        trigger: dict | BuildTrigger,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str = "global",
    ) -> BuildTrigger:
        """
        Create a new BuildTrigger.

        :param trigger: The BuildTrigger to create. If a dict is provided, it must be of the same form
            as the protobuf message `google.cloud.devtools.cloudbuild_v1.types.BuildTrigger`
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.
        :param location: The location of the project.
        """
        client = self.get_conn(location=location)

        self.log.info("Start creating build trigger...")

        try:
            trigger = client.create_build_trigger(
                request={"project_id": project_id, "trigger": trigger},
                retry=retry,
                timeout=timeout,
                metadata=metadata,
            )
        except AlreadyExists:
            raise AirflowException("Cloud Build Trigger with such parameters already exists.")

        self.log.info("Build trigger has been created.")

        return trigger

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_build_trigger(
        self,
        trigger_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str = "global",
    ) -> None:
        """
        Delete a BuildTrigger by its project ID and trigger ID.

        :param trigger_id: The ID of the BuildTrigger to delete.
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.
        :param location: The location of the project.
        """
        client = self.get_conn(location=location)

        self.log.info("Start deleting build trigger: %s.", trigger_id)

        client.delete_build_trigger(
            request={"project_id": project_id, "trigger_id": trigger_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info("Build trigger has been deleted: %s.", trigger_id)

    @GoogleBaseHook.fallback_to_default_project_id
    def get_build(
        self,
        id_: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str = "global",
    ) -> Build:
        """
        Return information about a previously requested build.

        :param id_: The ID of the build.
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.
        :param location: The location of the project.
        """
        client = self.get_conn(location=location)

        self.log.info("Start retrieving build: %s.", id_)

        build = client.get_build(
            request={"project_id": project_id, "id": id_},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info("Build has been retrieved: %s.", id_)

        return build

    @GoogleBaseHook.fallback_to_default_project_id
    def get_build_trigger(
        self,
        trigger_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str = "global",
    ) -> BuildTrigger:
        """
        Return information about a BuildTrigger.

        :param trigger_id: The ID of the BuildTrigger to get.
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.
        :param location: The location of the project.
        """
        client = self.get_conn(location=location)

        self.log.info("Start retrieving build trigger: %s.", trigger_id)

        trigger = client.get_build_trigger(
            request={"project_id": project_id, "trigger_id": trigger_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info("Build trigger has been retrieved: %s.", trigger_id)

        return trigger

    @GoogleBaseHook.fallback_to_default_project_id
    def list_build_triggers(
        self,
        location: str = "global",
        project_id: str = PROVIDE_PROJECT_ID,
        page_size: int | None = None,
        page_token: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> list[BuildTrigger]:
        """
        List existing BuildTriggers.

        :param project_id: Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param location: The location of the project.
        :param page_size: Optional, number of results to return in the list.
        :param page_token: Optional, token to provide to skip to a particular spot in the list.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.

        """
        client = self.get_conn(location=location)

        parent = f"projects/{project_id}/locations/{location}"

        self.log.info("Start retrieving build triggers.")

        response = client.list_build_triggers(
            request={
                "parent": parent,
                "project_id": project_id,
                "page_size": page_size,
                "page_token": page_token,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info("Build triggers have been retrieved.")

        return list(response.triggers)

    @GoogleBaseHook.fallback_to_default_project_id
    def list_builds(
        self,
        location: str = "global",
        project_id: str = PROVIDE_PROJECT_ID,
        page_size: int | None = None,
        page_token: int | None = None,
        filter_: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> list[Build]:
        """
        List previously requested builds.

        :param project_id: Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param location: The location of the project.
        :param page_size: Optional, number of results to return in the list.
        :param page_token: Optional, token to provide to skip to a particular spot in the list.
        :param filter_: Optional, the raw filter text to constrain the results.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.

        """
        client = self.get_conn(location=location)

        parent = f"projects/{project_id}/locations/{location}"

        self.log.info("Start retrieving builds.")

        response = client.list_builds(
            request={
                "parent": parent,
                "project_id": project_id,
                "page_size": page_size,
                "page_token": page_token,
                "filter": filter_,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info("Builds have been retrieved.")

        return list(response.builds)

    @GoogleBaseHook.fallback_to_default_project_id
    def retry_build(
        self,
        id_: str,
        project_id: str = PROVIDE_PROJECT_ID,
        wait: bool = True,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str = "global",
    ) -> Build:
        """
        Create a new build using the original build request; may or may not result in an identical build.

        :param id_: Build ID of the original build.
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param wait: Optional, wait for operation to finish.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.
        :param location: The location of the project.
        """
        client = self.get_conn(location=location)

        self.log.info("Start retrying build: %s.", id_)

        operation = client.retry_build(
            request={"project_id": project_id, "id": id_},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        id_ = self._get_build_id_from_operation(operation)
        self.log.info("Build has been retried: %s.", id_)

        if not wait:
            return self.get_build(id_=id_, project_id=project_id, location=location)

        self.wait_for_operation(operation, timeout)

        return self.get_build(id_=id_, project_id=project_id, location=location)

    @GoogleBaseHook.fallback_to_default_project_id
    def run_build_trigger(
        self,
        trigger_id: str,
        source: dict | RepoSource,
        project_id: str = PROVIDE_PROJECT_ID,
        wait: bool = True,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str = "global",
    ) -> Build:
        """
        Run a BuildTrigger at a particular source revision.

        :param trigger_id: The ID of the trigger.
        :param source: Source to build against this trigger. If a dict is provided, it must be of the
            same form as the protobuf message `google.cloud.devtools.cloudbuild_v1.types.RepoSource`
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param wait: Optional, wait for operation to finish.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.
        :param location: The location of the project.
        """
        client = self.get_conn(location=location)

        self.log.info("Start running build trigger: %s.", trigger_id)
        operation = client.run_build_trigger(
            request={"project_id": project_id, "trigger_id": trigger_id, "source": source},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info("Build trigger has been run: %s.", trigger_id)

        id_ = self._get_build_id_from_operation(operation)
        self.log.info("Build has been created: %s.", id_)

        if not wait:
            return self.get_build(id_=id_, project_id=project_id, location=location)

        self.wait_for_operation(operation, timeout)

        return self.get_build(id_=id_, project_id=project_id, location=location)

    @GoogleBaseHook.fallback_to_default_project_id
    def update_build_trigger(
        self,
        trigger_id: str,
        trigger: dict | BuildTrigger,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str = "global",
    ) -> BuildTrigger:
        """
        Update a BuildTrigger by its project ID and trigger ID.

        :param trigger_id: The ID of the trigger.
        :param trigger: The BuildTrigger to create. If a dict is provided, it must be of the same form
            as the protobuf message `google.cloud.devtools.cloudbuild_v1.types.BuildTrigger`
        :param project_id: Optional, Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
            Note that if `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Optional, additional metadata that is provided to the method.
        :param location: The location of the project.
        """
        client = self.get_conn(location=location)

        self.log.info("Start updating build trigger: %s.", trigger_id)

        trigger = client.update_build_trigger(
            request={"project_id": project_id, "trigger_id": trigger_id, "trigger": trigger},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info("Build trigger has been updated: %s.", trigger_id)

        return trigger

    @GoogleBaseHook.fallback_to_default_project_id
    def submit_build(
        self,
        source: str | None = None,
        submit_flags: dict | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> str:
        """
        Submit a build using Cloud Build.

        :param source: The location of the source to build.
        :param submit_flags: Optional. The dictionary of flags which can be used for Submit command.
        :param project_id: Google Cloud Project project_id where the function belongs.
            If set to None or missing, the default project_id from the GCP connection is used.
        """
        submit_command = ["gcloud", "builds", "submit"]
        if source:
            submit_command.append(source)

        if project_id:
            if submit_flags:
                submit_flags["project"] = project_id
            else:
                submit_flags = {"project": project_id}

        cmd = self._build_gcloud_command(
            command=submit_command,
            parameters=submit_flags if submit_flags else {},
        )

        self.log.info("Executing command: %s", " ".join(shlex.quote(c) for c in cmd))
        success_code = 0

        with self.provide_authorized_gcloud():
            proc = subprocess.run(cmd, check=False, capture_output=True)

        if proc.returncode != success_code:
            stderr_last_20_lines = "\n".join(proc.stderr.decode().strip().splitlines()[-20:])
            raise AirflowException(
                f"Process exit with non-zero exit code. Exit code: {proc.returncode}. Error Details : "
                f"{stderr_last_20_lines}"
            )

        response = proc.stdout.decode().strip()
        return response


class CloudBuildAsyncHook(GoogleBaseHook):
    """Asynchronous Hook for the Google Cloud Build Service."""

    @GoogleBaseHook.fallback_to_default_project_id
    async def get_cloud_build(
        self,
        id_: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str = "global",
    ) -> Build:
        """Retrieve a Cloud Build with a specified id."""
        if not id_:
            raise AirflowException("Google Cloud Build id is required.")

        client_options = None
        if location != "global":
            client_options = ClientOptions(api_endpoint=f"{location}-cloudbuild.googleapis.com:443")
        client = CloudBuildAsyncClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

        request = GetBuildRequest(
            project_id=project_id,
            id=id_,
        )
        build_instance = await client.get_build(
            request=request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return build_instance
