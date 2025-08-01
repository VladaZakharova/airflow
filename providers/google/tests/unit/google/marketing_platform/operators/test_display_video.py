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

import json
from tempfile import NamedTemporaryFile
from unittest import mock

import pytest

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models import TaskInstance as TI
from airflow.providers.google.marketing_platform.operators.display_video import (
    GoogleDisplayVideo360CreateQueryOperator,
    GoogleDisplayVideo360CreateSDFDownloadTaskOperator,
    GoogleDisplayVideo360DeleteReportOperator,
    GoogleDisplayVideo360DownloadLineItemsOperator,
    GoogleDisplayVideo360DownloadReportV2Operator,
    GoogleDisplayVideo360RunQueryOperator,
    GoogleDisplayVideo360SDFtoGCSOperator,
    GoogleDisplayVideo360UploadLineItemsOperator,
)
from airflow.utils import timezone
from airflow.utils.session import create_session

API_VERSION = "v4"
GCP_CONN_ID = "google_cloud_default"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]

DEFAULT_DATE = timezone.datetime(2021, 1, 1)
REPORT_ID = "report_id"
BUCKET_NAME = "test_bucket"
REPORT_NAME = "test_report.csv"
QUERY_ID = FILENAME = "test.csv"
OBJECT_NAME = "object_name"
OPERATION_NAME = "test_operation"
RESOURCE_NAME = "resource/path/to/media"


class TestGoogleDisplayVideo360DeleteReportOperator:
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360Hook"
    )
    def test_execute(self, hook_mock):
        with pytest.warns(AirflowProviderDeprecationWarning):  # noqa: PT031
            op = GoogleDisplayVideo360DeleteReportOperator(
                report_id=QUERY_ID, api_version=API_VERSION, task_id="test_task"
            )
            op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=None,
        )
        hook_mock.return_value.delete_query.assert_called_once_with(query_id=QUERY_ID)


@pytest.mark.db_test
class TestGoogleDisplayVideo360DownloadReportV2Operator:
    def setup_method(self):
        with create_session() as session:
            session.query(TI).delete()

    def teardown_method(self):
        with create_session() as session:
            session.query(TI).delete()

    @pytest.mark.parametrize(
        "file_path, should_except", [("https://host/path", False), ("file:/path/to/file", True)]
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.shutil")
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.urllib.request")
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.tempfile")
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.GCSHook")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360Hook"
    )
    def test_execute(
        self,
        mock_hook,
        mock_gcs_hook,
        mock_temp,
        mock_request,
        mock_shutil,
        file_path,
        should_except,
    ):
        mock_temp.NamedTemporaryFile.return_value.__enter__.return_value.name = FILENAME
        mock_hook.return_value.get_report.return_value = {
            "metadata": {
                "status": {"state": "DONE"},
                "googleCloudStoragePath": file_path,
            }
        }
        # Create mock context with task_instance
        mock_context = {"task_instance": mock.Mock()}

        with pytest.warns(AirflowProviderDeprecationWarning):  # noqa: PT031
            op = GoogleDisplayVideo360DownloadReportV2Operator(
                query_id=QUERY_ID,
                report_id=REPORT_ID,
                bucket_name=BUCKET_NAME,
                report_name=REPORT_NAME,
                task_id="test_task",
            )
            if should_except:
                with pytest.raises(AirflowException):
                    op.execute(context=mock_context)
                return
            op.execute(context=mock_context)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version="v2",
            impersonation_chain=None,
        )
        mock_hook.return_value.get_report.assert_called_once_with(report_id=REPORT_ID, query_id=QUERY_ID)

        mock_gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_gcs_hook.return_value.upload.assert_called_once_with(
            bucket_name=BUCKET_NAME,
            filename=FILENAME,
            gzip=True,
            mime_type="text/csv",
            object_name=REPORT_NAME + ".gz",
        )
        mock_context["task_instance"].xcom_push.assert_called_once_with(
            key="report_name", value=REPORT_NAME + ".gz"
        )

    @pytest.mark.parametrize(
        "test_bucket_name",
        [BUCKET_NAME, f"gs://{BUCKET_NAME}", "XComArg", "{{ ti.xcom_pull(task_ids='taskflow_op') }}"],
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.shutil")
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.urllib.request")
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.tempfile")
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.GCSHook")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360Hook"
    )
    def test_set_bucket_name(
        self,
        mock_hook,
        mock_gcs_hook,
        mock_temp,
        mock_request,
        mock_shutil,
        test_bucket_name,
        dag_maker,
    ):
        mock_temp.NamedTemporaryFile.return_value.__enter__.return_value.name = FILENAME
        mock_hook.return_value.get_report.return_value = {
            "metadata": {"status": {"state": "DONE"}, "googleCloudStoragePath": "TEST"}
        }
        with pytest.warns(AirflowProviderDeprecationWarning):  # noqa: PT031
            with dag_maker(dag_id="test_set_bucket_name", start_date=DEFAULT_DATE) as dag:
                if BUCKET_NAME not in test_bucket_name:

                    @dag.task(task_id="taskflow_op")
                    def f():
                        return BUCKET_NAME

                    taskflow_op = f()

                GoogleDisplayVideo360DownloadReportV2Operator(
                    query_id=QUERY_ID,
                    report_id=REPORT_ID,
                    bucket_name=test_bucket_name if test_bucket_name != "XComArg" else taskflow_op,
                    report_name=REPORT_NAME,
                    task_id="test_task",
                )

            dr = dag_maker.create_dagrun()

            for ti in dr.get_task_instances():
                ti.run()

        mock_gcs_hook.return_value.upload.assert_called_once_with(
            bucket_name=BUCKET_NAME,
            filename=FILENAME,
            gzip=True,
            mime_type="text/csv",
            object_name=REPORT_NAME + ".gz",
        )


class TestGoogleDisplayVideo360RunQueryOperator:
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360Hook"
    )
    def test_execute(self, hook_mock):
        parameters = {"param": "test"}

        # Create mock context with task_instance
        mock_context = {"task_instance": mock.Mock()}

        hook_mock.return_value.run_query.return_value = {
            "key": {
                "queryId": QUERY_ID,
                "reportId": REPORT_ID,
            }
        }
        with pytest.warns(AirflowProviderDeprecationWarning):  # noqa: PT031
            op = GoogleDisplayVideo360RunQueryOperator(
                query_id=QUERY_ID,
                parameters=parameters,
                api_version=API_VERSION,
                task_id="test_task",
            )
            op.execute(context=mock_context)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=None,
        )

        mock_context["task_instance"].xcom_push.assert_any_call(key="query_id", value=QUERY_ID)
        mock_context["task_instance"].xcom_push.assert_any_call(key="report_id", value=REPORT_ID)
        hook_mock.return_value.run_query.assert_called_once_with(query_id=QUERY_ID, params=parameters)


class TestGoogleDisplayVideo360DownloadLineItemsOperator:
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360Hook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.GCSHook")
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.tempfile")
    def test_execute(self, mock_temp, gcs_hook_mock, hook_mock):
        request_body = {
            "filterType": "filter_type",
            "filterIds": [],
            "format": "format",
            "fileSpec": "file_spec",
        }
        mock_temp.NamedTemporaryFile.return_value.__enter__.return_value.name = FILENAME
        gzip = False
        with pytest.warns(AirflowProviderDeprecationWarning):  # noqa: PT031
            op = GoogleDisplayVideo360DownloadLineItemsOperator(
                request_body=request_body,
                bucket_name=BUCKET_NAME,
                object_name=OBJECT_NAME,
                gzip=gzip,
                api_version=API_VERSION,
                gcp_conn_id=GCP_CONN_ID,
                task_id="test_task",
                impersonation_chain=IMPERSONATION_CHAIN,
            )
            op.execute(context=None)

        gcs_hook_mock.return_value.upload.assert_called_with(
            bucket_name=BUCKET_NAME,
            object_name=OBJECT_NAME,
            filename=FILENAME,
            gzip=gzip,
            mime_type="text/csv",
        )

        gcs_hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.download_line_items.assert_called_once_with(request_body=request_body)


class TestGoogleDisplayVideo360UploadLineItemsOperator:
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.tempfile")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360Hook"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.GCSHook")
    def test_execute(self, gcs_hook_mock, hook_mock, mock_tempfile):
        line_items = "holy_hand_grenade"
        gcs_hook_mock.return_value.download.return_value = line_items
        mock_tempfile.NamedTemporaryFile.return_value.__enter__.return_value.name = FILENAME
        with pytest.warns(AirflowProviderDeprecationWarning):  # noqa: PT031
            op = GoogleDisplayVideo360UploadLineItemsOperator(
                bucket_name=BUCKET_NAME,
                object_name=OBJECT_NAME,
                api_version=API_VERSION,
                gcp_conn_id=GCP_CONN_ID,
                task_id="test_task",
            )
            op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=None,
        )

        gcs_hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )

        gcs_hook_mock.return_value.download.assert_called_once_with(
            bucket_name=BUCKET_NAME,
            object_name=OBJECT_NAME,
            filename=FILENAME,
        )
        hook_mock.return_value.upload_line_items.assert_called_once()
        hook_mock.return_value.upload_line_items.assert_called_once_with(line_items=line_items)


class TestGoogleDisplayVideo360SDFtoGCSOperator:
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.zipfile")
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.os")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.tempfile.TemporaryDirectory"
    )
    @mock.patch("airflow.providers.google.marketing_platform.operators.display_video.GCSHook")
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360Hook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.open",
        new_callable=mock.mock_open,
    )
    def test_execute(self, mock_open, mock_hook, gcs_hook_mock, temp_dir_mock, os_mock, zipfile_mock):
        operation = {"response": {"resourceName": RESOURCE_NAME}}
        media = mock.Mock()

        mock_hook.return_value.get_sdf_download_operation.return_value = operation
        mock_hook.return_value.download_media.return_value = media

        tmp_dir = "/tmp/mock_dir"
        temp_dir_mock.return_value.__enter__.return_value = tmp_dir

        # Mock os behavior
        os_mock.path.join.side_effect = lambda *args: "/".join(args)
        os_mock.listdir.return_value = [FILENAME]

        # Mock zipfile behavior
        zipfile_mock.ZipFile.return_value.__enter__.return_value.extractall.return_value = None

        op = GoogleDisplayVideo360SDFtoGCSOperator(
            operation_name=OPERATION_NAME,
            bucket_name=BUCKET_NAME,
            object_name=OBJECT_NAME,
            gzip=False,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            task_id="test_task",
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        result = op.execute(context=None)

        # Assertions
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.get_sdf_download_operation.assert_called_once_with(
            operation_name=OPERATION_NAME
        )
        mock_hook.return_value.download_media.assert_called_once_with(resource_name=RESOURCE_NAME)
        mock_hook.return_value.download_content_from_request.assert_called_once()

        gcs_hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        gcs_hook_mock.return_value.upload.assert_called_once_with(
            bucket_name=BUCKET_NAME,
            object_name=OBJECT_NAME,
            filename=f"{tmp_dir}/{FILENAME}",
            gzip=False,
        )

        assert result == f"{BUCKET_NAME}/{OBJECT_NAME}"


class TestGoogleDisplayVideo360CreateSDFDownloadTaskOperator:
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360Hook"
    )
    def test_execute(self, mock_hook):
        body_request = {
            "version": "1",
            "id": "id",
            "filter": {"id": []},
        }
        test_name = "test_task"

        # Create mock context with task_instance
        mock_context = {"task_instance": mock.Mock()}

        mock_hook.return_value.create_sdf_download_operation.return_value = {"name": test_name}

        op = GoogleDisplayVideo360CreateSDFDownloadTaskOperator(
            body_request=body_request,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            task_id="test_task",
        )

        op.execute(context=mock_context)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=None,
        )

        mock_hook.return_value.create_sdf_download_operation.assert_called_once()
        mock_hook.return_value.create_sdf_download_operation.assert_called_once_with(
            body_request=body_request
        )
        mock_context["task_instance"].xcom_push.assert_called_once_with(key="name", value=test_name)


class TestGoogleDisplayVideo360CreateQueryOperator:
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360Hook"
    )
    def test_execute(self, hook_mock):
        body = {"body": "test"}

        # Create mock context with task_instance
        mock_context = {"task_instance": mock.Mock()}

        hook_mock.return_value.create_query.return_value = {"queryId": QUERY_ID}
        with pytest.warns(AirflowProviderDeprecationWarning):  # noqa: PT031
            op = GoogleDisplayVideo360CreateQueryOperator(body=body, task_id="test_task")
            op.execute(context=mock_context)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version="v2",
            impersonation_chain=None,
        )
        hook_mock.return_value.create_query.assert_called_once_with(query=body)
        mock_context["task_instance"].xcom_push.assert_called_once_with(key="query_id", value=QUERY_ID)

    def test_prepare_template(self):
        body = {"key": "value"}
        with NamedTemporaryFile("w+", suffix=".json") as f:
            f.write(json.dumps(body))
            f.flush()
            with pytest.warns(AirflowProviderDeprecationWarning):  # noqa: PT031
                op = GoogleDisplayVideo360CreateQueryOperator(body=body, task_id="test_task")
                op.prepare_template()

            assert isinstance(op.body, dict)
            assert op.body == body
