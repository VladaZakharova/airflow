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
from __future__ import annotations

import asyncio
import inspect
import itertools
import logging
import textwrap
import types
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from functools import cached_property
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Literal

from google.api_core.client_options import ClientOptions
from google.cloud.run_v2 import (
    CreateJobRequest,
    CreateServiceRequest,
    DeleteJobRequest,
    DeleteServiceRequest,
    GetJobRequest,
    GetServiceRequest,
    Job,
    JobsAsyncClient,
    JobsClient,
    ListJobsRequest,
    RunJobRequest,
    Service,
    ServicesAsyncClient,
    ServicesClient,
    UpdateJobRequest,
)
from google.longrunning import operations_pb2

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.cloud_build import CloudBuildHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseAsyncHook,
    GoogleBaseHook,
)

if TYPE_CHECKING:
    from google.api_core import operation
    from google.api_core.operation_async import AsyncOperation
    from google.cloud.run_v2.services.jobs import pagers

logger = logging.getLogger(__name__)


class NoLocationSpecifiedException(Exception):
    """Custom exception to catch error when location is not specified."""

    pass


class CloudRunPythonJobImageBuilderException(Exception):
    """Base exception for CloudRunPythonJobImageBuilder validation/runtime errors."""


class CloudRunPythonSourceSelectionError(CloudRunPythonJobImageBuilderException):
    """Custom exception to catch error when python source inputs are invalid."""


class CloudRunPythonEntryPointRequiredError(CloudRunPythonJobImageBuilderException):
    """Custom exception to catch error when entry_point is missing."""


class CloudRunPythonFileNotSetError(CloudRunPythonJobImageBuilderException):
    """Custom exception to catch error when python_file is not set."""


class CloudRunInvalidPythonFileUriError(CloudRunPythonJobImageBuilderException):
    """Custom exception to catch error when python_file URI is invalid."""


class CloudRunPythonCallableValidationError(CloudRunPythonJobImageBuilderException):
    """Raised when python_callable cannot be serialized safely."""


class CloudRunPythonJobImageBuilder:
    """
    Helper for building an image from Python source code for a Cloud Run Job.

    :param python_callable: Callable object to execute in generated ``main.py``.
        Must be set when ``python_file`` is not set.
    :param image_repository: Target image reference for the built container image.
        Typical value is an Artifact Registry path such as
        ``<region>-docker.pkg.dev/<project>/<repo>/<image>:<tag>``.
    :param requirements: Optional package requirements.
        You can pass one string, a list of strings, or ``None``.
    :param op_args: Positional arguments for the function.
    :param op_kwargs: Keyword arguments for the function.
    :param python_file: Path to a Python file in GCS.
        Should be in format ``gs://bucket/path/to/file.py``.
        Use this, or ``python_callable``.
    :param python_version: Optional Python version string to write to ``.python-version``.
    :param cache_image_repository: Optional image path used for build cache.
    :param entry_point: Function name to call from ``python_file``.
        Required when ``python_file`` is used.
    :param gcp_conn_id: Airflow connection id for Google Cloud.
    :param impersonation_chain: Optional service account impersonation chain.
    """

    def __init__(
        self,
        python_callable: Callable | None,
        image_repository: str,
        requirements: None | Iterable[str] | str = None,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        python_file: str | None = None,
        python_version: str | None = None,
        cache_image_repository: str | None = None,
        entry_point: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        if bool(python_callable) == bool(python_file):
            raise CloudRunPythonSourceSelectionError(
                "Choose one code source: set either 'python_callable' or 'python_file'."
            )

        if python_file and not entry_point:
            raise CloudRunPythonEntryPointRequiredError(
                "When using 'python_file', provide 'entry_point' with the function name to run."
            )
        if python_callable:
            self._validate_python_callable(python_callable)
        self.python_callable = python_callable
        self.image_repository = image_repository

        if not requirements:
            self.requirements: list[str] = []
        elif isinstance(requirements, str):
            self.requirements = [requirements]
        else:
            self.requirements = list(requirements)

        self.op_args = op_args or ()
        self.op_kwargs = op_kwargs or {}

        self.python_file = python_file
        self.python_version = python_version
        self.entry_point = entry_point
        self.cache_image_repository = cache_image_repository

        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def _validate_python_callable(self, python_callable: Callable) -> None:
        if (
            not isinstance(python_callable, types.FunctionType)
            or isinstance(python_callable, types.LambdaType)
            and python_callable.__name__ == "<lambda>"
        ):
            raise CloudRunPythonCallableValidationError(
                "CloudRunPythonJobImageBuilder only supports functions for 'python_callable'."
            )

        if inspect.isgeneratorfunction(python_callable):
            raise CloudRunPythonCallableValidationError(
                "CloudRunPythonJobImageBuilder does not support using 'yield' in 'python_callable'."
            )

    @cached_property
    def cloud_build_hook(self) -> CloudBuildHook:
        return CloudBuildHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    @cached_property
    def gcs_hook(self) -> GCSHook:
        return GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    def _download_python_file(self, destination_path: Path) -> None:
        """Download python source file from GCS into build context."""
        if not self.python_file.startswith("gs://"):
            raise CloudRunInvalidPythonFileUriError(
                f"Python file path should be in format gs://bucket/path/to/file.py. Got: {self.python_file}"
            )

        without_scheme = self.python_file[len("gs://") :]
        try:
            bucket_name, object_name = without_scheme.split("/", 1)
        except ValueError as e:
            raise CloudRunInvalidPythonFileUriError(
                f"Python file path should be in format gs://bucket/path/to/file.py. Got: {self.python_file}"
            ) from e

        self.gcs_hook.download(
            bucket_name=bucket_name,
            object_name=object_name,
            filename=str(destination_path),
        )

    def prepare_image(self) -> None:
        """Prepare python code, build cloud run Job image and upload it to registry."""
        with TemporaryDirectory(prefix="cloud-run-job-image") as tmp:
            tmp_dir = Path(tmp)

            main_path = tmp_dir / "main.py"
            procfile_path = tmp_dir / "Procfile"
            requirements_path = tmp_dir / "requirements.txt"
            user_code_path = tmp_dir / "user_code.py"
            python_version_path = tmp_dir / ".python-version"
            cloudbuild_path = tmp_dir / "cloudbuild.yaml"

            main_content = self._generate_main_content()
            requirements_content = self._generate_requirements_content()
            procfile_content = self._generate_procfile_content()
            cloudbuild_content = self._generate_cloudbuild_content()

            with (
                open(main_path, "w") as main_file,
                open(procfile_path, "w") as procfile_file,
                open(requirements_path, "w") as requirements_file,
                open(cloudbuild_path, "w") as cloudbuild_file,
            ):
                main_file.write(main_content)
                procfile_file.write(procfile_content)
                requirements_file.write(requirements_content)
                cloudbuild_file.write(cloudbuild_content)

            if self.python_file:
                self._download_python_file(destination_path=user_code_path)

            if self.python_version:
                with open(python_version_path, "w") as python_version_file:
                    python_version_file.write(self.python_version)

            if self.cache_image_repository:
                logger.info(
                    "Using buildpacks cache image %s.",
                    self.cache_image_repository,
                )

            self.cloud_build_hook.submit_build(
                source=str(tmp_dir),
                config=str(cloudbuild_path),
            )

    def get_python_source(self) -> str:
        """Return the source of self.python_callable."""
        return textwrap.dedent(inspect.getsource(self.python_callable))

    def _generate_cloudbuild_content(self) -> str:
        """Return generated content for cloudbuild.yaml file."""
        args = [
            "build",
            self.image_repository,
            "--builder",
            "gcr.io/buildpacks/builder:latest",
            "--publish",
        ]

        if self.cache_image_repository:
            args.extend(["--cache-image", self.cache_image_repository])

        args_yaml = "\n".join(f"      - {arg}" for arg in args)

        return f"""
steps:
  - name: gcr.io/k8s-skaffold/pack
    entrypoint: pack
    args:
{args_yaml}
"""

    def _generate_main_content(self) -> str:
        """Return generated content for main.py file."""
        if self.python_callable:
            python_callable_src = self.get_python_source()
            python_callable_name = self.python_callable.__name__

            code_block = python_callable_src
            invocation = f"{python_callable_name}(*args, **kwargs)"
        else:
            code_block = f"""
import user_code

if not hasattr(user_code, "{self.entry_point}"):
    available = [name for name in dir(user_code) if not name.startswith("_")]
    raise AttributeError(
        "Entry point '{self.entry_point}' not found in python_file. "
        f"Available attributes: {{available}}"
    )

if not callable(getattr(user_code, "{self.entry_point}")):
    raise TypeError(
        "Entry point '{self.entry_point}' exists in python_file, "
        "but it is not callable."
    )
"""
            invocation = f"user_code.{self.entry_point}(*args, **kwargs)"

        return f"""
import contextlib
import io
import json
import os
import sys
import traceback

from google.cloud import storage

{code_block}


_LOG_FILE_PATH = "/tmp/airflow-cloud-run-task.log"


class _DualWriter(io.TextIOBase):
    # Mirror output to the original stream and to the local log file.
    def __init__(self, original_stream, log_file):
        self.original_stream = original_stream
        self.log_file = log_file

    @property
    def encoding(self):
        return getattr(self.original_stream, "encoding", "utf-8")

    def write(self, data):
        self.original_stream.write(data)
        self.log_file.write(data)
        return len(data)

    def flush(self):
        self.original_stream.flush()
        self.log_file.flush()

    # Some code checks whether stdout/stderr is a terminal.
    # Keep this standard stream method and forward the answer from the original stream.
    def isatty(self):
        return getattr(self.original_stream, "isatty", lambda: False)()


def _write_result_to_gcs(payload):
    # Return values are optional, so skip upload when no destination is configured.
    uri = os.environ.get("AIRFLOW_RETURN_VALUE_GCS_URI")
    if not uri:
        return

    if not uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {{uri}}")

    without_scheme = uri[len("gs://"):]
    bucket_name, object_name = without_scheme.split("/", 1)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    blob.upload_from_string(
        json.dumps(payload),
        content_type="application/json",
    )


def _upload_log_file_to_gcs():
    # Logs are optional too, so only upload when the operator asked for them.
    uri = os.environ.get("AIRFLOW_LOG_OUTPUT_GCS_URI")
    if not uri or not os.path.exists(_LOG_FILE_PATH):
        return

    if not uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {{uri}}")

    without_scheme = uri[len("gs://"):]
    bucket_name, object_name = without_scheme.split("/", 1)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(_LOG_FILE_PATH)


def _write_success_result(result):
    # Prefer JSON so XCom can preserve the original value when possible.
    try:
        payload = {{
            "status": "success",
            "serialization_format": "json",
            "return_value": result,
        }}
        json.dumps(payload)
    except TypeError:
        payload = {{
            "status": "success",
            "serialization_format": "string_repr",
            "return_value": str(result),
        }}

    _write_result_to_gcs(payload)


def _write_user_error(exc):
    # Keep the traceback in the logs.
    formatted_traceback = traceback.format_exc()
    print(f"AIRFLOW_USER_CODE_ERROR: {{type(exc).__name__}}: {{exc}}", file=sys.stderr)
    print(formatted_traceback, file=sys.stderr, end="")
    _write_result_to_gcs(
        {{
            "status": "user_error",
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "traceback": formatted_traceback,
            "return_value": None,
        }}
    )


@contextlib.contextmanager
def _capture_logs():
    # Stream container output to both Cloud Run logs and a local file for later upload to GCS.
    try:
        with open(_LOG_FILE_PATH, "a", encoding="utf-8", buffering=1) as log_file:
            stdout = _DualWriter(sys.stdout, log_file)
            stderr = _DualWriter(sys.stderr, log_file)
            try:
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    yield
            finally:
                log_file.flush()
    finally:
        _upload_log_file_to_gcs()

if __name__ == "__main__":
    args = tuple({self.op_args})
    kwargs = {self.op_kwargs}
    with _capture_logs():
        try:
            result = {invocation}
            _write_success_result(result)
        except Exception as exc:
            _write_user_error(exc)
            raise
"""

    def _generate_requirements_content(self) -> str:
        """Return generated content for requirements.txt file."""
        reqs = list(self.requirements or [])
        # Ensure the generated runtime can upload logs and results to GCS.
        if "google-cloud-storage" not in reqs:
            reqs.append("google-cloud-storage")

        return "\n".join(reqs)

    def _generate_procfile_content(self) -> str:
        """Return generated content for Procfile file."""
        return """
web: python3 main.py
"""


class CloudRunHook(GoogleBaseHook):
    """
    Hook for the Google Cloud Run service.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    :param transport: Optional. The transport to use for API requests. Can be 'rest' or 'grpc'.
        If set to None, a transport is chosen automatically. Use 'rest' if gRPC is not available
        or fails in your environment (e.g., Docker containers with certain network configurations).
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        transport: Literal["rest", "grpc"] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain, **kwargs)
        self._client: JobsClient | None = None
        self.transport = transport

    def get_conn(self, location: str | None = None, use_regional_endpoint: bool | None = False) -> JobsClient:
        """
        Retrieve the connection to Google Cloud Run.

        :param location: The location of the project.
        :param use_regional_endpoint: If set to True, regional endpoint will be used while creating Client.
            If not provided, the default one is global endpoint.

        :return: Cloud Run Jobs client object.
        """
        if self._client is None:
            client_kwargs = {
                "credentials": self.get_credentials(),
                "client_info": CLIENT_INFO,
            }
            if self.transport:
                client_kwargs["transport"] = self.transport
            if use_regional_endpoint:
                if not location:
                    raise NoLocationSpecifiedException(
                        "No location was specified while using use_regional_endpoint parameter"
                    )
                client_kwargs["client_options"] = ClientOptions(
                    api_endpoint=f"{location}-run.googleapis.com:443"
                )
            self._client = JobsClient(**client_kwargs)  # type: ignore[arg-type]
        return self._client

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_job(
        self,
        job_name: str,
        region: str,
        use_regional_endpoint: bool | None = False,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> Job:
        delete_request = DeleteJobRequest()
        delete_request.name = f"projects/{project_id}/locations/{region}/jobs/{job_name}"

        operation = self.get_conn(location=region, use_regional_endpoint=use_regional_endpoint).delete_job(
            delete_request
        )
        return operation.result()

    @GoogleBaseHook.fallback_to_default_project_id
    def create_job(
        self,
        job_name: str,
        job: Job | dict,
        region: str,
        use_regional_endpoint: bool | None = False,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> Job:
        if isinstance(job, dict):
            job = Job(job)

        create_request = CreateJobRequest()
        create_request.job = job
        create_request.job_id = job_name
        create_request.parent = f"projects/{project_id}/locations/{region}"

        operation = self.get_conn(location=region, use_regional_endpoint=use_regional_endpoint).create_job(
            create_request
        )
        return operation.result()

    @GoogleBaseHook.fallback_to_default_project_id
    def update_job(
        self,
        job_name: str,
        job: Job | dict,
        region: str,
        use_regional_endpoint: bool | None = False,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> Job:
        if isinstance(job, dict):
            job = Job(job)

        update_request = UpdateJobRequest()
        job.name = f"projects/{project_id}/locations/{region}/jobs/{job_name}"
        update_request.job = job
        operation = self.get_conn(location=region, use_regional_endpoint=use_regional_endpoint).update_job(
            update_request
        )
        return operation.result()

    @GoogleBaseHook.fallback_to_default_project_id
    def execute_job(
        self,
        job_name: str,
        region: str,
        use_regional_endpoint: bool | None = False,
        project_id: str = PROVIDE_PROJECT_ID,
        overrides: dict[str, Any] | None = None,
    ) -> operation.Operation:
        run_job_request = RunJobRequest(
            name=f"projects/{project_id}/locations/{region}/jobs/{job_name}", overrides=overrides
        )
        operation = self.get_conn(location=region, use_regional_endpoint=use_regional_endpoint).run_job(
            request=run_job_request
        )
        return operation

    @GoogleBaseHook.fallback_to_default_project_id
    def get_job(
        self,
        job_name: str,
        region: str,
        use_regional_endpoint: bool | None = False,
        project_id: str = PROVIDE_PROJECT_ID,
    ):
        get_job_request = GetJobRequest(name=f"projects/{project_id}/locations/{region}/jobs/{job_name}")
        return self.get_conn(location=region, use_regional_endpoint=use_regional_endpoint).get_job(
            get_job_request
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def list_jobs(
        self,
        region: str,
        use_regional_endpoint: bool | None = False,
        project_id: str = PROVIDE_PROJECT_ID,
        show_deleted: bool = False,
        limit: int | None = None,
    ) -> Iterable[Job]:
        if limit is not None and limit < 0:
            raise AirflowException("The limit for the list jobs request should be greater or equal to zero")

        list_jobs_request: ListJobsRequest = ListJobsRequest(
            parent=f"projects/{project_id}/locations/{region}", show_deleted=show_deleted
        )

        jobs: pagers.ListJobsPager = self.get_conn(
            location=region, use_regional_endpoint=use_regional_endpoint
        ).list_jobs(request=list_jobs_request)

        return list(itertools.islice(jobs, limit))


class CloudRunAsyncHook(GoogleBaseAsyncHook):
    """
    Async hook for the Google Cloud Run service.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    :param transport: Optional. The transport to use for API requests. Can be 'rest' or 'grpc'.
        When set to 'rest', uses the synchronous REST client wrapped with
        ``asyncio.to_thread()`` for compatibility with async triggers.
        When None or 'grpc', uses the native async gRPC transport (grpc_asyncio).
    """

    sync_hook_class = CloudRunHook

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        transport: Literal["rest", "grpc"] | None = None,
        **kwargs,
    ):
        self._client: JobsAsyncClient | JobsClient | None = None
        self.transport = transport
        super().__init__(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain, **kwargs)

    async def get_conn(
        self, location: str | None = None, use_regional_endpoint: bool | None = False
    ) -> JobsAsyncClient | JobsClient:
        """
        Retrieve the connection to Google Cloud Run.

        :param location: The location of the project.
        :param use_regional_endpoint: If set to True, regional endpoint will be used while creating Client.
            If not provided, the default one is global endpoint.

        :return: Cloud Run Jobs client object.
        """
        if self._client is None:
            sync_hook = await self.get_sync_hook()
            credentials = sync_hook.get_credentials()
            common_kwargs = {
                "credentials": credentials,
                "client_info": CLIENT_INFO,
            }
            if use_regional_endpoint:
                if not location:
                    raise NoLocationSpecifiedException(
                        "No location was specified while using use_regional_endpoint parameter"
                    )
                common_kwargs["client_options"] = ClientOptions(
                    api_endpoint=f"{location}-run.googleapis.com:443"
                )
            if self.transport == "rest":
                # REST transport is synchronous-only. Use the sync JobsClient here;
                # get_operation() wraps calls with asyncio.to_thread() for async compat.
                self._client = JobsClient(
                    transport="rest",
                    **common_kwargs,
                )
            else:
                # Default: use JobsAsyncClient which picks grpc_asyncio transport.
                self._client = JobsAsyncClient(
                    **common_kwargs,
                )

        return self._client

    async def get_operation(
        self, operation_name: str, location: str | None = None, use_regional_endpoint: bool | None = False
    ) -> operations_pb2.Operation:
        conn = await self.get_conn(location=location, use_regional_endpoint=use_regional_endpoint)
        request = operations_pb2.GetOperationRequest(name=operation_name)
        if self.transport == "rest":
            # REST client is synchronous — run in a thread to avoid blocking the event loop.
            return await asyncio.to_thread(conn.get_operation, request, timeout=120)
        return await conn.get_operation(request, timeout=120)


class CloudRunServiceHook(GoogleBaseHook):
    """
    Hook for the Google Cloud Run services.

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
    ):
        self._client: ServicesClient | None = None
        super().__init__(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain, **kwargs)

    def get_conn(
        self, location: str | None = None, use_regional_endpoint: bool | None = False
    ) -> ServicesClient:
        """
        Retrieve the connection to Google Cloud Run.

        :param location: The location of the project.
        :param use_regional_endpoint: If set to True, regional endpoint will be used while creating Client.
            If not provided, the default one is global endpoint.

        :return: Google Cloud Run client object.
        """
        if self._client is None:
            client_kwargs = {
                "credentials": self.get_credentials(),
                "client_info": CLIENT_INFO,
            }
            if use_regional_endpoint:
                if not location:
                    raise NoLocationSpecifiedException(
                        "No location was specified while using use_regional_endpoint parameter"
                    )
                client_kwargs["client_options"] = ClientOptions(
                    api_endpoint=f"{location}-run.googleapis.com:443"
                )
            self._client = ServicesClient(**client_kwargs)  # type: ignore[arg-type]
        return self._client

    @GoogleBaseHook.fallback_to_default_project_id
    def get_service(
        self,
        service_name: str,
        region: str,
        use_regional_endpoint: bool | None = False,
        project_id: str = PROVIDE_PROJECT_ID,
    ):
        get_service_request = GetServiceRequest(
            name=f"projects/{project_id}/locations/{region}/services/{service_name}"
        )
        return self.get_conn(location=region, use_regional_endpoint=use_regional_endpoint).get_service(
            get_service_request
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_service(
        self,
        service_name: str,
        service: Service | dict,
        region: str,
        use_regional_endpoint: bool | None = False,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> Service:
        if isinstance(service, dict):
            service = Service(service)

        create_request = CreateServiceRequest(
            parent=f"projects/{project_id}/locations/{region}",
            service=service,
            service_id=service_name,
        )

        operation = self.get_conn(
            location=region, use_regional_endpoint=use_regional_endpoint
        ).create_service(create_request)
        return operation.result()

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_service(
        self,
        service_name: str,
        region: str,
        use_regional_endpoint: bool | None = False,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> Service:
        delete_request = DeleteServiceRequest(
            name=f"projects/{project_id}/locations/{region}/services/{service_name}"
        )

        operation = self.get_conn(
            location=region, use_regional_endpoint=use_regional_endpoint
        ).delete_service(delete_request)
        return operation.result()


class CloudRunServiceAsyncHook(GoogleBaseAsyncHook):
    """
    Async hook for the Google Cloud Run services.

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

    sync_hook_class = CloudRunServiceHook

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        self._client: ServicesClient | ServicesAsyncClient | None = None
        super().__init__(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain, **kwargs)

    async def get_conn(
        self, location: str | None = None, use_regional_endpoint: bool | None = False
    ) -> ServicesClient | ServicesAsyncClient:
        """
        Retrieve the connection to Google Cloud Run.

        :param location: The location of the project.
        :param use_regional_endpoint: If set to True, regional endpoint will be used while creating Client.
            If not provided, the default one is global endpoint.
        """
        if self._client is None:
            sync_hook = await self.get_sync_hook()
            client_kwargs = {
                "credentials": sync_hook.get_credentials(),
                "client_info": CLIENT_INFO,
            }
            if use_regional_endpoint:
                if not location:
                    raise NoLocationSpecifiedException(
                        "No location was specified while using use_regional_endpoint parameter"
                    )
                client_kwargs["client_options"] = ClientOptions(
                    api_endpoint=f"{location}-run.googleapis.com:443"
                )
            self._client = ServicesAsyncClient(**client_kwargs)
        return self._client

    @GoogleBaseHook.fallback_to_default_project_id
    async def create_service(
        self,
        service_name: str,
        service: Service | dict,
        region: str,
        use_regional_endpoint: bool | None = False,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> AsyncOperation:
        if isinstance(service, dict):
            service = Service(service)

        create_request = CreateServiceRequest(
            parent=f"projects/{project_id}/locations/{region}",
            service=service,
            service_id=service_name,
        )
        client = await self.get_conn(location=region, use_regional_endpoint=use_regional_endpoint)

        return await client.create_service(create_request)  # type: ignore[misc]

    @GoogleBaseHook.fallback_to_default_project_id
    async def delete_service(
        self,
        service_name: str,
        region: str,
        use_regional_endpoint: bool | None = False,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> AsyncOperation:
        delete_request = DeleteServiceRequest(
            name=f"projects/{project_id}/locations/{region}/services/{service_name}"
        )
        client = await self.get_conn(location=region, use_regional_endpoint=use_regional_endpoint)

        return await client.delete_service(delete_request)  # type: ignore[misc]
