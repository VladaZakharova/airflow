from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from google.api_core import exceptions

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.vertex_ai.experiment_service import (
    ExperimentHook,
    ExperimentRunHook,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CreateExperimentOperator(GoogleCloudBaseOperator):
    """
    Use the Vertex AI SDK to create experiment.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param experiment_name: Required. The name of the evaluation experiment.
    :param experiment_description: Optional. Description of the evaluation experiment.
    :param experiment_tensorboard: Optional. The Vertex TensorBoard instance to use as a backing
        TensorBoard for the provided experiment. If no TensorBoard is provided, a default TensorBoard
        instance is created and used by this experiment.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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
        "location",
        "project_id",
        "impersonation_chain",
        "experiment_name",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        experiment_name: str,
        experiment_description: str = "",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        experiment_tensorboard: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.experiment_name = experiment_name
        self.experiment_description = experiment_description
        self.experiment_tensorboard = experiment_tensorboard
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        self.hook = ExperimentHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            self.hook.create_experiment(
                project_id=self.project_id,
                location=self.location,
                experiment_name=self.experiment_name,
                experiment_description=self.experiment_description,
                experiment_tensorboard=self.experiment_tensorboard,
            )
        except exceptions.AlreadyExists:
            raise AirflowException(f"Experiment with name {self.experiment_name} already exist")

        self.log.info("Created experiment: %s", self.experiment_name)


class DeleteExperimentOperator(GoogleCloudBaseOperator):
    """
    Use the Vertex AI SDK to delete experiment.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param experiment_name: Required. The name of the evaluation experiment.
    :param delete_backing_tensorboard_runs: Optional. If True will also delete the Vertex AI TensorBoard
            runs associated with the experiment runs under this experiment that we used to store time series
            metrics.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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
        "location",
        "project_id",
        "impersonation_chain",
        "experiment_name",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        experiment_name: str,
        delete_backing_tensorboard_runs: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.experiment_name = experiment_name
        self.delete_backing_tensorboard_runs = delete_backing_tensorboard_runs
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        self.hook = ExperimentHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            self.hook.delete_experiment(
                project_id=self.project_id,
                location=self.location,
                experiment_name=self.experiment_name,
                delete_backing_tensorboard_runs=self.delete_backing_tensorboard_runs,
            )
        except exceptions.NotFound:
            raise AirflowException(f"Experiment with name {self.experiment_name} not found")

        self.log.info("Deleted experiment: %s", self.experiment_name)


class CreateExperimentRunOperator(GoogleCloudBaseOperator):
    """
    Use the Vertex AI SDK to create experiment run.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param experiment_name: Required. The name of the evaluation experiment.
    :param experiment_run_name: Required. The specific run name or ID for this experiment.
    :param experiment_run_tensorboard: Optional. A backing TensorBoard resource to enable and store time series
        metrics logged to this experiment run using log_time_series_metrics.
    :param run_after_creation: Optional. If True experiment run will be created with state running.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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
        "location",
        "project_id",
        "impersonation_chain",
        "experiment_name",
        "experiment_run_name",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        experiment_name: str,
        experiment_run_name: str,
        experiment_run_tensorboard: str | None = None,
        run_after_creation: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.experiment_name = experiment_name
        self.experiment_run_name = experiment_run_name
        self.experiment_run_tensorboard = experiment_run_tensorboard
        self.run_after_creation = run_after_creation
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        self.hook = ExperimentRunHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            self.hook.create_experiment_run(
                project_id=self.project_id,
                location=self.location,
                experiment_name=self.experiment_name,
                experiment_run_name=self.experiment_run_name,
                experiment_run_tensorboard=self.experiment_run_tensorboard,
                run_after_creation=self.run_after_creation,
            )
        except exceptions.AlreadyExists:
            raise AirflowException(f"Experiment Run with name {self.experiment_run_name} not found")

        self.log.info("Created experiment run: %s", self.experiment_run_name)


class DeleteExperimentRunOperator(GoogleCloudBaseOperator):
    """
    Use the Vertex AI SDK to delete experiment run.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param experiment_name: Required. The name of the evaluation experiment.
    :param experiment_run_name: Required. The specific run name or ID for this experiment.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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
        "location",
        "project_id",
        "impersonation_chain",
        "experiment_name",
        "experiment_run_name",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        experiment_name: str,
        experiment_run_name: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.experiment_name = experiment_name
        self.experiment_run_name = experiment_run_name
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        self.hook = ExperimentRunHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            self.hook.delete_experiment_run(
                project_id=self.project_id,
                location=self.location,
                experiment_name=self.experiment_name,
                experiment_run_name=self.experiment_run_name,
            )
        except exceptions.NotFound:
            raise AirflowException(f"Experiment Run with name {self.experiment_run_name} not found")

        self.log.info("Deleted experiment run: %s", self.experiment_run_name)
