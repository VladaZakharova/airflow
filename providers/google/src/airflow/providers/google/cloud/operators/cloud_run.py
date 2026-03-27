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

import time
import uuid
from collections.abc import Callable, Collection, Iterable, Mapping, MutableMapping, MutableSequence, Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal

import google.cloud.exceptions
from google.api_core.exceptions import AlreadyExists
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud import logging as gcp_logging
from google.cloud.run_v2 import Job, Service
from google.cloud.run_v2.types import k8s_min, vendor_settings

from airflow.providers.common.compat.sdk import AirflowException, conf
from airflow.providers.google.cloud.hooks.cloud_run import (
    CloudRunHook,
    CloudRunPythonJobImageBuilder,
    CloudRunServiceHook,
)
from airflow.providers.google.cloud.links.cloud_run import CloudRunJobLoggingLink
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.cloud.triggers.cloud_run import CloudRunJobFinishedTrigger, RunJobStatus
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.utils.hashlib_wrapper import md5

if TYPE_CHECKING:
    from google.api_core import operation
    from google.api_core.retry import Retry
    from google.cloud.run_v2.types import Execution

    from airflow.providers.common.compat.sdk import Context


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

    :param project_id: GCP project ID where the serverless execution
        will be provisioned.
    :param location: GCP location for execution.
    :param python_callable: A Python callable to execute remotely.
        Mutually exclusive with python_file.
    :param image_repository: Repository where the image will be built.
    :param op_args: Positional arguments for the callable/entry_point.
    :param op_kwargs: Keyword arguments for the callable/entry_point.
    :param labels: GCP resource labels for cost attribution.
    :param env_vars: Environment variables for the execution environment.
    :param memory: Memory allocation (e.g., '512Mi', '2Gi'). Default '512Mi'.
    :param cpu: CPU allocation (e.g., '1', '2'). Default '1'.
    :param extra_gcs_mounts: GCS folders to mounted to the Cloud Run job execution.
    :param network: VPC network for execution environment connectivity.
    :param subnet: VPC subnet for execution environment connectivity.



    TODO(ADD SUPPORT FOR GCS DOWNLOADING):param requirements: Python dependencies. Either a list of pip
        requirement strings or a GCS URI to a requirements.txt file.
    TODO(DOWNLOAD FILE FROM GCS, PARCE IT AND UPDATE CODE IN CloudRunPythonJobImageBuilder):param python_file: GCS URI to a Python file to execute remotely.
        Mutually exclusive with python_callable.
    TODO(BIND WITH python_file UPDATE CODE IN CloudRunPythonJobImageBuilder):param entry_point: Function name to call when using python_file mode.
        If not specified, the file is executed as __main__.
    TODO(CHECK gcloud build MAYBE WE CAN USE A .python-version FILE):param python_version: Python version for the execution environment.
        Defaults to the Composer environment's Python version.
    TODO(CLARIFY WHAT IT IS WE HAVE THE SAME FOR PythonVirtualenvOperator BUT MAYBE WE DO NOT NEED IT HERE):param string_args: Positional arguments for the callable/entry_point.

    TODO(RELATED TO op_kwargs CLARIFY WITH AUGUSTO MAYBE WE DO NOT NEED IT BECAUSE op_kwargs CAN BE PART OF template_fields AND THIS FUNCTIONALYTY WILL WORK WITHOUT ADDITIONAL IMPLEMENTATION):param xcom_inputs: Dict of {key: Jinja template} that resolves
        upstream XCom values and passes them as kwargs.
    TODO(IMPLEMENT return VALUE FOR python_callable. IT IS IMPOSSIBLE TO GET RETURN VALUE FROM Cloud Run Job. WE NEED TO PRINT RETURN VALUE INSIDE python_callable FUNCTION, AND THEN PARCE LOGS AND STORE IT IN XCOM):param do_xcom_push: Push the return value to XCom. Default True.

    TODO(I DO NOT KNOW WHAT IS IT):param base_image: Custom container base image for native dependencies.
    TODO(NEED CLARIFICATION WITH AUGUSTO):param cache_image_repository: Repository where the image will be built.

    :param timeout: Max execution seconds.
    :param impersonation_chain: Ordered list of service accounts to impersonate. The last account is used for execution.
    """

    template_fields = (
        "project_id",
        "location",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str,
        location: str,
        python_callable: Callable,
        image_repository: str,
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
        python_file: str | None = None,  # TODO
        python_version: str | None = None,  # TODO
        string_args: None = None,  # TODO
        entry_point: None = None,  # TODO
        gcp_conn_id: str = "google_cloud_default",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
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
        self.python_file = python_file  # TODO
        self.python_version = python_version  # TODO
        self.string_args = string_args  # TODO
        self.entry_point = entry_point  # TODO
        self.gcp_conn_id = gcp_conn_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.impersonation_chain = impersonation_chain

    @cached_property
    def hook(self) -> CloudRunHook:
        return CloudRunHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    @property
    def gcp_logging_client(self) -> gcp_logging.Client:
        """The Cloud Library API client."""
        client = gcp_logging.Client(
            credentials=self.hook.get_credentials(),
            project=self.project_id,
            client_info=CLIENT_INFO,
        )
        return client

    def execute(self, context: Context):
        python_job_image_builder = CloudRunPythonJobImageBuilder(
            python_callable=self.python_callable,
            image_repository=self.image_repository,
            requirements=self.requirements,
            op_args=self.op_args,
            op_kwargs=self.op_kwargs,
            python_file=self.python_file,  # TODO
            python_version=self.python_version,  # TODO
            string_args=self.string_args,  # TODO
            entry_point=self.entry_point,  # TODO
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Start building process for Python image for Cloud Run Job...")
        python_job_image_builder.prepare_image()
        self.log.info("Python image for Cloud Run Job built successfully.")

        cloud_run_job = self._deploy_cloud_run_job_image()
        cloud_run_job_name = cloud_run_job["name"].split("/")[-1]

        job_logs = self._execute_cloud_run_job(
            job_name=cloud_run_job_name,
        )

        return job_logs

    def _deploy_cloud_run_job_image(self):
        """Create Job instance and deploy it on Cloud Run."""
        cloud_run_job_obj = Job()
        container = k8s_min.Container()
        container.image = self.image_repository
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

        hash_base = str(uuid.uuid4())
        uniqueness_suffix = md5(hash_base.encode()).hexdigest()

        cloud_run_job = self.hook.create_job(
            job_name=f"cloud-python-{uniqueness_suffix}",
            job=cloud_run_job_obj,
            region=self.location,
            project_id=self.project_id,
        )

        self.log.info("Cloud Run Job created")

        return Job.to_dict(cloud_run_job)

    def _execute_cloud_run_job(self, job_name: str):
        self.log.info("Executing Cloud Run Job: %s", job_name)
        job_operation = self.hook.execute_job(
            region=self.location,
            project_id=self.project_id,
            job_name=job_name,
        )

        self.log.info("Output:")
        self._stream_cloud_run_job_logs(job_name, job_operation)
        job_execution = job_operation.result()

        if job_execution.failed_count > 0:
            raise AirflowException("Cloud Python Task failed execution.")

    def _stream_cloud_run_job_logs(self, job_name: str, job_operation: operation.Operation):
        execution_id = job_operation.metadata.name.split("/")[-1]

        def _print_cloud_run_job_log(entry):
            payload = entry.payload
            self.log.info("%s", payload)

        log_filter = (
            f'resource.type="cloud_run_job" '
            f'AND resource.labels.job_name="{job_name}" '
            f'AND labels."run.googleapis.com/execution_name"="{execution_id}"'
        )
        last_timestamp = None
        while not job_operation.done():
            current_filter = log_filter
            if last_timestamp:
                current_filter += f' AND timestamp > "{last_timestamp.isoformat()}"'
            entries = self.gcp_logging_client.list_entries(
                filter_=current_filter, order_by=gcp_logging.ASCENDING
            )
            for entry in entries:
                _print_cloud_run_job_log(entry)
                last_timestamp = entry.timestamp
            time.sleep(2)

        final_entries = self.gcp_logging_client.list_entries(
            filter_=f'{log_filter} AND timestamp > "{last_timestamp.isoformat()}"'
            if last_timestamp
            else log_filter,
            order_by=gcp_logging.ASCENDING,
        )
        for entry in final_entries:
            _print_cloud_run_job_log(entry)

    def _delete_all_resources(self):
        pass
