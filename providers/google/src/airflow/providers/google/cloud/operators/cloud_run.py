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

import re
import time
import uuid
from collections.abc import Callable, Collection, Iterable, Mapping, MutableMapping, MutableSequence, Sequence
from concurrent.futures import TimeoutError as FutureTimeoutError
from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal

import google.cloud.exceptions
from google.api_core.exceptions import AlreadyExists
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.run_v2 import Job, Service
from google.cloud.run_v2.types import k8s_min, vendor_settings

from airflow.providers.common.compat.sdk import AirflowException, conf
from airflow.providers.google.cloud.hooks.cloud_run import (
    CloudRunHook,
    CloudRunPythonJobImageBuilder,
    CloudRunServiceHook,
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.links.cloud_run import CloudRunJobLoggingLink
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.cloud.triggers.cloud_run import CloudRunJobFinishedTrigger, RunJobStatus

if TYPE_CHECKING:
    from google.api_core import operation
    from google.api_core.retry import Retry
    from google.cloud.run_v2.types import Execution

    from airflow.providers.common.compat.sdk import Context


class CloudRunExecutePythonJobOperatorException(Exception):
    """Base exception for CloudRunExecutePythonJobOperator errors."""


class CloudRunPythonSourceConfigurationError(CloudRunExecutePythonJobOperatorException):
    """Raised when python_callable/python_file configuration is invalid."""


class CloudRunXComConfigurationError(CloudRunExecutePythonJobOperatorException):
    """Raised when XCom return-value transport configuration is invalid."""


class CloudRunLogsConfigurationError(CloudRunExecutePythonJobOperatorException):
    """Raised when execution log transport configuration is invalid."""


class CloudRunCacheImageRepositoryValidationError(CloudRunExecutePythonJobOperatorException):
    """Raised when cache_image_repository value is invalid."""


class CloudRunPythonVersionValidationError(CloudRunExecutePythonJobOperatorException):
    """Raised when python_version has an invalid format or unsupported version."""


class CloudRunInvalidGcsUriError(CloudRunExecutePythonJobOperatorException):
    """Raised when a GCS URI does not match expected format."""


class CloudRunResultPayloadError(CloudRunExecutePythonJobOperatorException):
    """Raised when Cloud Run return-value payload is invalid."""


class CloudRunPythonJobExecutionError(CloudRunExecutePythonJobOperatorException):
    """Raised when Cloud Run reports failed task executions."""


class CloudRunUserCodeExecutionError(CloudRunPythonJobExecutionError):
    """Raised when remote user code fails during Cloud Run execution."""


class CloudRunInfrastructureExecutionError(CloudRunPythonJobExecutionError):
    """Raised when Cloud Run infrastructure fails before user code completes."""


class CloudRunJobTimeoutError(CloudRunExecutePythonJobOperatorException):
    """Raised when Cloud Run job execution exceeds the configured timeout."""


class CloudRunCreateJobOperator(GoogleCloudBaseOperator):
    """
    Creates a job without executing it. Pushes the created job to xcom.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param job_name: Required. The name of the job to create.
    :param job: Required. The job descriptor containing the configuration of the job to submit.
    :param use_regional_endpoint: If set to True, regional endpoint will be used while creating Client.
        If not provided, the default one is global endpoint.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("project_id", "region", "gcp_conn_id", "impersonation_chain", "job_name")

    def __init__(
        self,
        project_id: str,
        region: str,
        job_name: str,
        job: dict | Job,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        use_regional_endpoint: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.job_name = job_name
        self.job = job
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.use_regional_endpoint = use_regional_endpoint

    def execute(self, context: Context):
        hook: CloudRunHook = CloudRunHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        job = hook.create_job(
            job_name=self.job_name,
            job=self.job,
            region=self.region,
            project_id=self.project_id,
            use_regional_endpoint=self.use_regional_endpoint,
        )
        self.log.info("Job created")

        return Job.to_dict(job)


class CloudRunUpdateJobOperator(GoogleCloudBaseOperator):
    """
    Updates a job and wait for the operation to be completed. Pushes the updated job to xcom.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param job_name: Required. The name of the job to update.
    :param job: Required. The job descriptor containing the new configuration of the job to update.
        The name field will be replaced by job_name
    :param use_regional_endpoint: If set to True, regional endpoint will be used while creating Client.
        If not provided, the default one is global endpoint.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("project_id", "region", "gcp_conn_id", "impersonation_chain", "job_name")

    def __init__(
        self,
        project_id: str,
        region: str,
        job_name: str,
        job: dict | Job,
        gcp_conn_id: str = "google_cloud_default",
        use_regional_endpoint: bool = False,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.job_name = job_name
        self.job = job
        self.gcp_conn_id = gcp_conn_id
        self.use_regional_endpoint = use_regional_endpoint
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook: CloudRunHook = CloudRunHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        job = hook.update_job(
            job_name=self.job_name,
            job=self.job,
            region=self.region,
            project_id=self.project_id,
            use_regional_endpoint=self.use_regional_endpoint,
        )

        return Job.to_dict(job)


class CloudRunDeleteJobOperator(GoogleCloudBaseOperator):
    """
    Deletes a job and wait for the operation to be completed. Pushes the deleted job to xcom.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param job_name: Required. The name of the job to delete.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param use_regional_endpoint: If set to True, regional endpoint will be used while creating Client.
        If not provided, the default one is global endpoint.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("project_id", "region", "gcp_conn_id", "impersonation_chain", "job_name")

    def __init__(
        self,
        project_id: str,
        region: str,
        job_name: str,
        gcp_conn_id: str = "google_cloud_default",
        use_regional_endpoint: bool = False,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.job_name = job_name
        self.gcp_conn_id = gcp_conn_id
        self.use_regional_endpoint = use_regional_endpoint
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook: CloudRunHook = CloudRunHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        job = hook.delete_job(
            job_name=self.job_name,
            region=self.region,
            project_id=self.project_id,
            use_regional_endpoint=self.use_regional_endpoint,
        )

        return Job.to_dict(job)


class CloudRunListJobsOperator(GoogleCloudBaseOperator):
    """
    Lists jobs.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param show_deleted: If true, returns deleted (but unexpired)
        resources along with active ones.
    :param limit: The number of jobs to list. If left empty,
        all the jobs will be returned.
    :param use_regional_endpoint: If set to True, regional endpoint will be used while creating Client.
        If not provided, the default one is global endpoint.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = (
        "project_id",
        "region",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str,
        region: str,
        show_deleted: bool = False,
        limit: int | None = None,
        use_regional_endpoint: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.show_deleted = show_deleted
        self.limit = limit
        self.use_regional_endpoint = use_regional_endpoint
        if limit is not None and limit < 0:
            raise AirflowException("The limit for the list jobs request should be greater or equal to zero")

    def execute(self, context: Context):
        hook: CloudRunHook = CloudRunHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        jobs = hook.list_jobs(
            region=self.region,
            project_id=self.project_id,
            show_deleted=self.show_deleted,
            limit=self.limit,
            use_regional_endpoint=self.use_regional_endpoint,
        )

        return [Job.to_dict(job) for job in jobs]


class CloudRunExecuteJobOperator(GoogleCloudBaseOperator):
    """
    Executes a job and waits for the operation to be completed. Pushes the executed job to xcom.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param job_name: Required. The name of the job to execute.
    :param overrides: Optional map of override values.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param polling_period_seconds: Optional. Control the rate of the poll for the result of deferrable run.
        By default, the trigger will poll every 10 seconds.
    :param timeout_seconds: Optional. The timeout for this request, in seconds.
    :param use_regional_endpoint: If set to True, regional endpoint will be used while creating Client.
        If not provided, the default one is global endpoint.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: Run the operator in deferrable mode.
    :param transport: Optional. The transport to use for API requests. Can be 'rest' or 'grpc'.
        If set to None, a transport is chosen automatically. Use 'rest' if gRPC is not available
        or fails in your environment (e.g., Docker containers with certain network configurations).
    """

    operator_extra_links = (CloudRunJobLoggingLink(),)
    template_fields = (
        "project_id",
        "region",
        "gcp_conn_id",
        "impersonation_chain",
        "job_name",
        "overrides",
        "polling_period_seconds",
        "timeout_seconds",
        "transport",
    )

    def __init__(
        self,
        project_id: str,
        region: str,
        job_name: str,
        overrides: dict[str, Any] | None = None,
        polling_period_seconds: float = 10,
        timeout_seconds: float | None = None,
        use_regional_endpoint: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        transport: Literal["rest", "grpc"] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.job_name = job_name
        self.overrides = overrides
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.polling_period_seconds = polling_period_seconds
        self.timeout_seconds = timeout_seconds
        self.deferrable = deferrable
        self.use_regional_endpoint = use_regional_endpoint
        self.transport = transport
        self.operation: operation.Operation | None = None

    def execute(self, context: Context):
        hook: CloudRunHook = CloudRunHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            transport=self.transport,
        )
        self.operation = hook.execute_job(
            region=self.region,
            project_id=self.project_id,
            job_name=self.job_name,
            overrides=self.overrides,
            use_regional_endpoint=self.use_regional_endpoint,
        )

        if self.operation is None:
            raise AirflowException("Operation is None")

        if self.operation.metadata.log_uri:
            CloudRunJobLoggingLink.persist(
                context=context,
                log_uri=self.operation.metadata.log_uri,
            )

        if not self.deferrable:
            result: Execution = self._wait_for_operation(self.operation)
            self._fail_if_execution_failed(result)
            job = hook.get_job(
                job_name=result.job,
                region=self.region,
                project_id=self.project_id,
                use_regional_endpoint=self.use_regional_endpoint,
            )
            return Job.to_dict(job)
        self.defer(
            trigger=CloudRunJobFinishedTrigger(
                operation_name=self.operation.operation.name,
                job_name=self.job_name,
                project_id=self.project_id,
                location=self.region,
                use_regional_endpoint=self.use_regional_endpoint,
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
                polling_period_seconds=self.polling_period_seconds,
                transport=self.transport,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict):
        status = event["status"]

        if status == RunJobStatus.TIMEOUT.value:
            raise AirflowException("Operation timed out")

        if status == RunJobStatus.FAIL.value:
            error_code = event["operation_error_code"]
            error_message = event["operation_error_message"]
            raise AirflowException(
                f"Operation failed with error code [{error_code}] and error message [{error_message}]"
            )

        hook: CloudRunHook = CloudRunHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            transport=self.transport,
        )

        job = hook.get_job(
            job_name=event["job_name"],
            region=self.region,
            project_id=self.project_id,
            use_regional_endpoint=self.use_regional_endpoint,
        )
        return Job.to_dict(job)

    def _fail_if_execution_failed(self, execution: Execution):
        task_count = execution.task_count
        succeeded_count = execution.succeeded_count
        failed_count = execution.failed_count

        if succeeded_count + failed_count != task_count:
            raise AirflowException("Not all tasks finished execution")

        if failed_count > 0:
            raise AirflowException("Some tasks failed execution")

    def _wait_for_operation(self, operation: operation.Operation):
        try:
            return operation.result(timeout=self.timeout_seconds)
        except Exception:
            error = operation.exception(timeout=self.timeout_seconds)
            raise AirflowException(error)


class CloudRunCreateServiceOperator(GoogleCloudBaseOperator):
    """
    Creates a Service without executing it. Pushes the created service to xcom.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param service_name: Required. The name of the service to create.
    :param service: The service descriptor containing the configuration of the service to submit.
    :param use_regional_endpoint: If set to True, regional endpoint will be used while creating Client.
        If not provided, the default one is global endpoint.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("project_id", "region", "gcp_conn_id", "impersonation_chain", "service_name")

    def __init__(
        self,
        project_id: str,
        region: str,
        service_name: str,
        service: dict | Service,
        use_regional_endpoint: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.service = service
        self.service_name = service_name
        self.use_regional_endpoint = use_regional_endpoint
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self._validate_inputs()

    def _validate_inputs(self):
        missing_fields = [k for k in ["project_id", "region", "service_name"] if not getattr(self, k)]
        if not self.project_id or not self.region or not self.service_name:
            raise AirflowException(
                f"Required parameters are missing: {missing_fields}. These parameters be passed either as "
                "keyword parameter or as extra field in Airflow connection definition. Both are not set!"
            )

    def execute(self, context: Context):
        hook: CloudRunServiceHook = CloudRunServiceHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )

        try:
            service = hook.create_service(
                service=self.service,
                service_name=self.service_name,
                region=self.region,
                project_id=self.project_id,
                use_regional_endpoint=self.use_regional_endpoint,
            )
        except AlreadyExists:
            self.log.info(
                "Already existed Cloud run service, service_name=%s, region=%s",
                self.service_name,
                self.region,
            )
            service = hook.get_service(
                service_name=self.service_name,
                region=self.region,
                project_id=self.project_id,
                use_regional_endpoint=self.use_regional_endpoint,
            )
            return Service.to_dict(service)
        except google.cloud.exceptions.GoogleCloudError as e:
            self.log.error("An error occurred. Exiting.")
            raise e

        return Service.to_dict(service)


class CloudRunDeleteServiceOperator(GoogleCloudBaseOperator):
    """
    Deletes a Service without executing it. Pushes the deleted service to xcom.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param service_name: Required. The name of the service to create.
    :param use_regional_endpoint: If set to True, regional endpoint will be used while creating Client.
        If not provided, the default one is global endpoint.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("project_id", "region", "gcp_conn_id", "impersonation_chain", "service_name")

    def __init__(
        self,
        project_id: str,
        region: str,
        service_name: str,
        use_regional_endpoint: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.service_name = service_name
        self.use_regional_endpoint = use_regional_endpoint
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self._validate_inputs()

    def _validate_inputs(self):
        missing_fields = [k for k in ["project_id", "region", "service_name"] if not getattr(self, k)]
        if not self.project_id or not self.region or not self.service_name:
            raise AirflowException(
                f"Required parameters are missing: {missing_fields}. These parameters be passed either as "
                "keyword parameter or as extra field in Airflow connection definition. Both are not set!"
            )

    def execute(self, context: Context):
        hook: CloudRunServiceHook = CloudRunServiceHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )

        try:
            service = hook.delete_service(
                service_name=self.service_name,
                region=self.region,
                project_id=self.project_id,
                use_regional_endpoint=self.use_regional_endpoint,
            )
        except google.cloud.exceptions.NotFound as e:
            self.log.error("An error occurred. Not Found.")
            raise e

        return Service.to_dict(service)


class CloudRunExecutePythonJobOperator(GoogleCloudBaseOperator):
    """
    Executes a Python callable or script on a Cloud Run job.

    :param project_id: Google Cloud project id where the job runs.
    :param location: Region where the job runs, for example ``us-central1``.
    :param python_callable: Python function to run.
        Use this, or ``python_file``.
    :param image_repository: Container image path where the built image will be pushed.
    :param labels: GCP resource labels for cost attribution.
    :param env_vars: Environment variables for the execution environment.
        Pass a list of environment variable items.
    :param memory: Memory allocation (e.g., '512Mi', '2Gi'). Default '512Mi'.
    :param cpu: CPU allocation (e.g., '1', '2'). Default '1'.
    :param extra_gcs_mounts: GCS folders to mounted to the Cloud Run job execution.
    :param network: VPC network for execution environment connectivity.
    :param subnet: VPC subnet for execution environment connectivity.
    :param requirements: Python dependencies for building the remote execution image.
    :param python_file: GCS URI to a Python file to execute remotely.
        Mutually exclusive with python_callable.
    :param entry_point: Function name to call when using python_file mode.
    :param python_version: Python version for the image build process.
    :param cache_image_repository: Buildpacks cache image repository.
    :param do_xcom_push: If True, fetch return value from GCS and push it to XCom.
    :param xcom_volume: Base GCS URI where return-value payloads are stored.
        The operator writes the function return payload to a separate object under this path.
    :param logs_volume: Base GCS URI where execution logs are stored.
        The operator writes ordered execution logs to a separate object under this path.
        If not provided, ``xcom_volume`` is used for logs too.
    :param timeout: Max execution seconds.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or ordered list of service accounts forming a delegation chain.
        If set as a string, that service account is used for Cloud Build, Cloud Run,
        and related Google Cloud API calls made by this operator.
        If set as a sequence, the last account in the list is the one used for execution,
        and the earlier accounts are delegates required to reach it.
        The same impersonation target is applied to the Cloud Build submission step used
        to build the execution image.
    """

    template_fields = (
        "image_repository",
        "xcom_volume",
        "logs_volume",
        "python_file",
        "entry_point",
        "requirements",
        "op_args",
        "op_kwargs",
        "labels",
        "network",
        "subnet",
        "python_version",
        "cache_image_repository",
    )

    def __init__(
        self,
        project_id: str,
        location: str,
        image_repository: str,
        xcom_volume: str | None = None,
        logs_volume: str | None = None,
        do_xcom_push: bool = True,
        python_callable: Callable | None = None,
        python_file: str | None = None,
        entry_point: str | None = None,
        cache_image_repository: str | None = None,
        requirements: None | Iterable[str] | str = None,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        labels: MutableMapping[str, str] | None = None,
        env_vars: MutableSequence[k8s_min.EnvVar] | None = None,
        memory: str = "512Mi",
        cpu: str = "1",
        extra_gcs_mounts: MutableSequence[k8s_min.VolumeMount] | None = None,
        network: str | None = None,
        subnet: str | None = None,
        python_version: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if bool(python_callable) == bool(python_file):
            raise CloudRunPythonSourceConfigurationError(
                "Exactly one of 'python_callable' or 'python_file' must be provided."
            )

        if python_file and not entry_point:
            raise CloudRunPythonSourceConfigurationError(
                "When using 'python_file', provide 'entry_point' with the function name to run."
            )

        if do_xcom_push and not xcom_volume:
            raise CloudRunXComConfigurationError(
                "Set 'xcom_volume' in format gs://bucket/path when 'do_xcom_push=True'."
            )

        self.project_id = project_id
        self.location = location
        self.python_callable = python_callable
        self.image_repository = image_repository
        self.requirements = requirements
        self.op_args = op_args
        self.op_kwargs = op_kwargs
        self.labels = labels
        self.env_vars = env_vars
        self.memory = memory
        self.cpu = cpu
        self.extra_gcs_mounts = extra_gcs_mounts
        self.network = network
        self.subnet = subnet
        self.python_file = python_file
        self.python_version = python_version
        self._validate_python_version()
        self.cache_image_repository = cache_image_repository
        self._validate_cache_image_repository()

        self.entry_point = entry_point
        self.gcp_conn_id = gcp_conn_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.impersonation_chain = impersonation_chain
        self.xcom_volume = xcom_volume
        self.logs_volume = logs_volume or xcom_volume
        self.do_xcom_push = do_xcom_push
        self._execution_image_repository: str | None = None

    @cached_property
    def hook(self) -> CloudRunHook:
        return CloudRunHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    @cached_property
    def gcs_hook(self) -> GCSHook:
        return GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    def _validate_cache_image_repository(self) -> None:
        if self.cache_image_repository is not None and not str(self.cache_image_repository).strip():
            raise CloudRunCacheImageRepositoryValidationError("Cache image repository is empty.")

        if self.cache_image_repository and self.cache_image_repository == self.image_repository:
            raise CloudRunCacheImageRepositoryValidationError(
                "Cache image path should be different from the main image path."
            )

    def _validate_python_version(self) -> None:
        if self.python_version is None:
            return

        version = str(self.python_version).strip()
        if not version:
            raise CloudRunPythonVersionValidationError("Python version is empty.")

        if not re.fullmatch(r"\d+\.\d+(\.\d+)?", version):
            raise CloudRunPythonVersionValidationError(
                f"Python version '{version}' is not valid. Use format X.Y or X.Y.Z, for example 3.11."
            )

        major_minor = ".".join(version.split(".")[:2])
        supported = {"3.7", "3.8", "3.9", "3.10", "3.11", "3.12", "3.13", "3.14"}

        if major_minor not in supported:
            raise CloudRunPythonVersionValidationError(
                f"Python version '{version}' is not supported. Supported versions are: {', '.join(sorted(supported))}."
            )

    def _build_result_gcs_uri(self, context: Context) -> str:
        if not self.xcom_volume:
            raise CloudRunXComConfigurationError(
                "Result storage path is missing. Set 'xcom_volume' in format gs://bucket/path."
            )

        base = self.xcom_volume.rstrip("/")
        object_path = (
            f"cloud_run_python_results/"
            f"{context['dag'].dag_id}/"
            f"{context['task'].task_id}/"
            f"{context['run_id']}/"
            f"{context['ti'].try_number}/"
            f"return.json"
        )
        return f"{base}/{object_path}"

    def _build_log_gcs_uri(self, context: Context) -> str:
        if not self.logs_volume:
            raise CloudRunLogsConfigurationError(
                "Log storage path is missing. Set 'logs_volume' in format gs://bucket/path."
            )

        base = self.logs_volume.rstrip("/")
        object_path = (
            f"cloud_run_python_logs/"
            f"{context['dag'].dag_id}/"
            f"{context['task'].task_id}/"
            f"{context['run_id']}/"
            f"{context['ti'].try_number}/"
            f"stdout.log"
        )
        return f"{base}/{object_path}"

    def _split_gcs_uri(self, gcs_uri: str) -> tuple[str, str]:
        """Split a GCS URI into bucket and object name."""
        if not gcs_uri.startswith("gs://"):
            raise CloudRunInvalidGcsUriError(f"Path '{gcs_uri}' is not valid. Use format gs://bucket/path.")

        without_scheme = gcs_uri[len("gs://") :]
        try:
            return without_scheme.split("/", 1)
        except ValueError as e:
            raise CloudRunInvalidGcsUriError(
                f"Path '{gcs_uri}' is not valid. Use format gs://bucket/path."
            ) from e

    def _read_result_from_gcs(self, result_gcs_uri: str) -> Any:
        """Read a successful result payload from GCS."""
        payload = self._read_result_payload_from_gcs(result_gcs_uri)
        if payload is None:
            return None
        if payload.get("status") != "success":
            raise CloudRunResultPayloadError(f"Unexpected result payload at '{result_gcs_uri}': {payload}")

        return payload.get("return_value")

    def _read_result_payload_from_gcs(self, result_gcs_uri: str) -> dict[str, Any] | None:
        """Read the raw result payload from GCS."""
        import json

        bucket_name, object_name = self._split_gcs_uri(result_gcs_uri)
        deadline = time.monotonic() + 5

        while True:
            if self.gcs_hook.exists(bucket_name=bucket_name, object_name=object_name):
                return json.loads(
                    self.gcs_hook.download(bucket_name=bucket_name, object_name=object_name).decode("utf-8")
                )

            if time.monotonic() >= deadline:
                break

            time.sleep(1)

        return None

    def _read_logs_from_gcs(self, log_gcs_uri: str) -> str | None:
        """Read collected task logs from GCS."""
        bucket_name, object_name = self._split_gcs_uri(log_gcs_uri)
        deadline = time.monotonic() + 10

        while True:
            if self.gcs_hook.exists(bucket_name=bucket_name, object_name=object_name):
                return self.gcs_hook.download(bucket_name=bucket_name, object_name=object_name).decode(
                    "utf-8"
                )

            if time.monotonic() >= deadline:
                break

            time.sleep(1)

        return None

    def _emit_collected_logs(self, collected_logs: str | None) -> None:
        """Replay collected Cloud Run logs into the Airflow task log."""
        if not collected_logs:
            return

        self.log.info("Cloud Run output:")
        # The marker is only for failure classification. Users should just see the actual traceback lines.
        for line in collected_logs.splitlines():
            if line.strip().startswith("AIRFLOW_USER_CODE_ERROR:"):
                continue
            self.log.info("%s", line)

    def _build_execution_image_repository(self) -> str:
        """Build a unique image reference for this task run."""
        if "@" in self.image_repository:
            return self.image_repository

        # Each run gets its own image tag so parallel tasks do not race on a shared mutable tag.
        unique_suffix = uuid.uuid4().hex[:12]
        repository_name, separator, tag = self.image_repository.rpartition(":")
        if separator and "/" not in tag:
            return f"{repository_name}:{tag}-{unique_suffix}"
        return f"{self.image_repository}:{unique_suffix}"

    def _raise_for_failed_execution(
        self,
        *,
        collected_logs: str | None,
    ) -> None:
        """Raise a user or infrastructure error for a failed execution."""
        if collected_logs:
            for line in collected_logs.splitlines():
                stripped_line = line.strip()
                if stripped_line.startswith("AIRFLOW_USER_CODE_ERROR:"):
                    error_message = stripped_line.removeprefix("AIRFLOW_USER_CODE_ERROR:").strip()
                    if not error_message:
                        error_message = "User code execution failed."
                    raise CloudRunUserCodeExecutionError(f"User code failed: {error_message}")

        raise CloudRunInfrastructureExecutionError(
            "Cloud Run infrastructure execution failed before user code completed successfully."
        )

    def execute(self, context: Context):
        self._execution_image_repository = self._build_execution_image_repository()

        # Build an execution image from user code and dependencies.
        python_job_image_builder = CloudRunPythonJobImageBuilder(
            python_callable=self.python_callable,
            image_repository=self._execution_image_repository,
            requirements=self.requirements,
            op_args=self.op_args,
            op_kwargs=self.op_kwargs,
            python_file=self.python_file,
            python_version=self.python_version,
            entry_point=self.entry_point,
            cache_image_repository=self.cache_image_repository,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        if self.python_version:
            self.log.info("Using Python version %s for Cloud Run job image build.", self.python_version)
        else:
            self.log.info("Using default Python version for Cloud Run job image build.")
        self.log.info(
            "Using execution image %s for this Cloud Run job run.", self._execution_image_repository
        )
        self.log.info("Start building process for Python image for Cloud Run Job...")
        python_job_image_builder.prepare_image()
        self.log.info("Python image for Cloud Run Job built successfully.")

        # Pass per-run GCS destinations into the container so it can upload logs and results.
        env_vars = list(self.env_vars or [])
        if self.logs_volume:
            env_vars.append(
                k8s_min.EnvVar(
                    name="AIRFLOW_LOG_OUTPUT_GCS_URI",
                    value=self._build_log_gcs_uri(context),
                )
            )

        if self.do_xcom_push:
            env_vars.append(
                k8s_min.EnvVar(
                    name="AIRFLOW_RETURN_VALUE_GCS_URI",
                    value=self._build_result_gcs_uri(context),
                )
            )
        self.env_vars = env_vars

        # Deploy and run the Cloud Run job
        cloud_run_job = self._deploy_cloud_run_job_image()
        cloud_run_job_name = cloud_run_job["name"].split("/")[-1]
        try:
            job_execution = self._execute_cloud_run_job(job_name=cloud_run_job_name)
        except Exception as execution_error:
            if isinstance(execution_error, CloudRunJobTimeoutError):
                raise execution_error
            if self.logs_volume:
                log_gcs_uri = self._build_log_gcs_uri(context)
                collected_logs = self._read_logs_from_gcs(log_gcs_uri)
                if collected_logs is None:
                    self.log.warning("Cloud Run log file was not found at '%s'.", log_gcs_uri)
                self._emit_collected_logs(collected_logs)
            else:
                collected_logs = None
            try:
                self._raise_for_failed_execution(
                    collected_logs=collected_logs,
                )
            except CloudRunPythonJobExecutionError as classified_error:
                raise classified_error from execution_error
            raise execution_error

        if self.logs_volume:
            log_gcs_uri = self._build_log_gcs_uri(context)
            collected_logs = self._read_logs_from_gcs(log_gcs_uri)
            if collected_logs is None:
                self.log.warning("Cloud Run log file was not found at '%s'.", log_gcs_uri)
            self._emit_collected_logs(collected_logs)
        else:
            collected_logs = None

        if job_execution.failed_count > 0:
            self._raise_for_failed_execution(
                collected_logs=collected_logs,
            )
        self.log.info("Cloud Run Job finished successfully.")

        # XCom path: read and return task result payload from GCS
        if self.do_xcom_push:
            return self._read_result_from_gcs(self._build_result_gcs_uri(context))

        return None

    def _deploy_cloud_run_job_image(self):
        """Create Job instance and deploy it on Cloud Run."""
        cloud_run_job_obj = Job()
        container = k8s_min.Container()
        container.image = self._execution_image_repository or self.image_repository
        container.resources.limits = {"cpu": self.cpu, "memory": self.memory}
        if self.env_vars:
            container.env = self.env_vars
        if self.extra_gcs_mounts:
            container.volume_mounts = self.extra_gcs_mounts
        if self.labels:
            cloud_run_job_obj.template.labels = self.labels
        cloud_run_job_obj.template.template.containers.append(container)
        if self.network or self.subnet:
            network_interface = vendor_settings.VpcAccess.NetworkInterface(
                network=self.network,
                subnetwork=self.subnet,
            )
            cloud_run_job_obj.template.template.vpc_access = vendor_settings.VpcAccess(
                network_interfaces=[network_interface],
            )

        uniqueness_suffix = uuid.uuid4().hex

        cloud_run_job = self.hook.create_job(
            job_name=f"cloud-python-{uniqueness_suffix}",
            job=cloud_run_job_obj,
            region=self.location,
            project_id=self.project_id,
        )

        self.log.info("Cloud Run Job created")

        return Job.to_dict(cloud_run_job)

    def _execute_cloud_run_job(self, job_name: str) -> Execution:
        self.log.info("Executing Cloud Run Job: %s", job_name)
        execution_overrides = None
        if self.timeout is not None:
            execution_overrides = {"timeout": f"{self.timeout}s"}
        job_operation = self.hook.execute_job(
            region=self.location,
            project_id=self.project_id,
            job_name=job_name,
            overrides=execution_overrides,
        )
        try:
            return job_operation.result(timeout=self.timeout)
        except FutureTimeoutError as e:
            raise CloudRunJobTimeoutError(
                f"Cloud Run job execution timed out after {self.timeout} seconds."
            ) from e
