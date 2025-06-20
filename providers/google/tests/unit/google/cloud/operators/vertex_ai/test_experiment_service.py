from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.operators.vertex_ai.experiment_service import (
    CreateExperimentOperator,
    CreateExperimentRunOperator,
    DeleteExperimentOperator,
    DeleteExperimentRunOperator,
)

VERTEX_AI_PATH = "airflow.providers.google.cloud.operators.vertex_ai.experiment_service.{}"

TASK_ID = "test_task_id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "test-location"
GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


class TestVertexAICreateExperimentOperator:
    @mock.patch(VERTEX_AI_PATH.format("ExperimentHook"))
    def test_execute(self, mock_hook):
        test_experiment_name = "test_experiment_name"
        test_experiment_description = "test-description"
        test_tensorboard = None

        op = CreateExperimentOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            experiment_name=test_experiment_name,
            experiment_description=test_experiment_description,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_experiment.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            experiment_name=test_experiment_name,
            experiment_description=test_experiment_description,
            experiment_tensorboard=test_tensorboard,
        )


class TestVertexAIDeleteExperimentOperator:
    @mock.patch(VERTEX_AI_PATH.format("ExperimentHook"))
    def test_execute(self, mock_hook):
        test_experiment_name = "test_experiment_name"
        test_delete_backing_tensorboard_runs = True

        op = DeleteExperimentOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            experiment_name=test_experiment_name,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            delete_backing_tensorboard_runs=test_delete_backing_tensorboard_runs,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_experiment.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            experiment_name=test_experiment_name,
            delete_backing_tensorboard_runs=test_delete_backing_tensorboard_runs,
        )


class TestVertexAICreateExperimentRunOperator:
    @mock.patch(VERTEX_AI_PATH.format("ExperimentRunHook"))
    def test_execute(self, mock_hook):
        test_experiment_name = "test_experiment_name"
        test_experiment_run_name = "test_experiment_run_name"
        test_experiment_run_tensorboard = None
        test_run_after_creation = False

        op = CreateExperimentRunOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            experiment_name=test_experiment_name,
            experiment_run_name=test_experiment_run_name,
            experiment_run_tensorboard=test_experiment_run_tensorboard,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_experiment_run.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            experiment_name=test_experiment_name,
            experiment_run_name=test_experiment_run_name,
            experiment_run_tensorboard=test_experiment_run_tensorboard,
            run_after_creation=test_run_after_creation,
        )


class TestVertexAIDeleteExperimentRunOperator:
    @mock.patch(VERTEX_AI_PATH.format("ExperimentRunHook"))
    def test_execute(self, mock_hook):
        test_experiment_name = "test_experiment_name"
        test_experiment_run_name = "test_experiment_run_name"

        op = DeleteExperimentRunOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            experiment_name=test_experiment_name,
            experiment_run_name=test_experiment_run_name,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.delete_experiment_run.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            experiment_name=test_experiment_name,
            experiment_run_name=test_experiment_run_name,
        )
