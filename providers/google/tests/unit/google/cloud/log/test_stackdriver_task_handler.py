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

import logging
from contextlib import nullcontext
from unittest import mock
from urllib.parse import parse_qs, urlsplit

import pytest
from google.cloud.logging import Resource
from google.cloud.logging_v2.types import ListLogEntriesRequest, ListLogEntriesResponse, LogEntry

from airflow.providers.google.cloud.log.stackdriver_task_handler import StackdriverTaskHandler
from airflow.utils import timezone
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS


def _create_list_log_entries_response_mock(messages, token):
    return ListLogEntriesResponse(
        entries=[LogEntry(json_payload={"message": message}) for message in messages], next_page_token=token
    )


@pytest.fixture
def clean_stackdriver_handlers():
    yield
    for handler_ref in reversed(logging._handlerList[:]):
        handler = handler_ref()
        if isinstance(handler, StackdriverTaskHandler):
            logging._removeHandlerRef(handler_ref)
            del handler


@pytest.mark.usefixtures("clean_stackdriver_handlers")
@mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
@mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.gcp_logging.Client")
def test_should_pass_message_to_client(mock_client, mock_get_creds_and_project_id):
    mock_get_creds_and_project_id.return_value = ("creds", "project_id")

    transport_type = mock.MagicMock()
    stackdriver_task_handler = StackdriverTaskHandler(transport=transport_type, labels={"key": "value"})
    logger = logging.getLogger("logger")
    logger.setLevel(logging.INFO)
    logger.addHandler(stackdriver_task_handler)

    logger.info("test-message")
    stackdriver_task_handler.flush()

    transport_type.assert_called_once_with(mock_client.return_value, "airflow")
    transport_type.return_value.send.assert_called_once_with(
        mock.ANY, "test-message", labels={"key": "value"}, resource=Resource(type="global", labels={})
    )
    mock_client.assert_called_once_with(credentials="creds", client_info=mock.ANY, project="project_id")


@pytest.mark.usefixtures("clean_stackdriver_handlers")
@mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
@mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.gcp_logging.Client")
def test_should_use_configured_log_name(mock_client, mock_get_creds_and_project_id):
    import importlib
    import logging

    from airflow import settings
    from airflow.config_templates import airflow_local_settings

    mock_get_creds_and_project_id.return_value = ("creds", "project_id")

    try:
        # this is needed for Airflow 2.8 and below where default settings are triggering warning on
        # extra "name" in the configuration of stackdriver handler. As of Airflow 2.9 this warning is not
        # emitted.
        context_manager = nullcontext()
        with context_manager:
            with conf_vars(
                {
                    ("logging", "remote_logging"): "True",
                    ("logging", "remote_base_log_folder"): "stackdriver://host/path",
                }
            ):
                importlib.reload(airflow_local_settings)
                settings.configure_logging()

                logger = logging.getLogger("airflow.task")
                handler = logger.handlers[0]
                assert isinstance(handler, StackdriverTaskHandler)
                with mock.patch.object(handler, "transport_type") as transport_type_mock:
                    logger.error("foo")
                    transport_type_mock.assert_called_once_with(mock_client.return_value, "path")
    finally:
        importlib.reload(airflow_local_settings)
        settings.configure_logging()


@pytest.mark.db_test
class TestStackdriverLoggingHandlerTask:
    DAG_ID = "dag_for_testing_stackdriver_file_task_handler"
    TASK_ID = "task_for_testing_stackdriver_task_handler"

    @pytest.fixture(autouse=True)
    def task_instance(self, create_task_instance, clean_stackdriver_handlers):
        self.ti = create_task_instance(
            dag_id=self.DAG_ID,
            task_id=self.TASK_ID,
            logical_date=timezone.datetime(2016, 1, 1),
            state=TaskInstanceState.RUNNING,
        )
        self.ti.try_number = 1
        self.ti.raw = False
        yield
        clear_db_runs()
        clear_db_dags()

    def _setup_handler(self, **handler_kwargs):
        self.transport_mock = mock.MagicMock()
        handler_kwargs = {"transport": self.transport_mock, **handler_kwargs}
        stackdriver_task_handler = StackdriverTaskHandler(**handler_kwargs)
        self.logger = logging.getLogger("logger")
        self.logger.addHandler(stackdriver_task_handler)
        return stackdriver_task_handler

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.gcp_logging.Client")
    def test_should_set_labels(self, mock_client, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        stackdriver_task_handler = self._setup_handler()
        stackdriver_task_handler.set_context(self.ti)

        self.logger.info("test-message")
        stackdriver_task_handler.flush()

        date_key = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
        labels = {
            "task_id": self.TASK_ID,
            "dag_id": self.DAG_ID,
            date_key: "2016-01-01T00:00:00+00:00",
            "try_number": "1",
        }
        resource = Resource(type="global", labels={})
        self.transport_mock.return_value.send.assert_called_once_with(
            mock.ANY, "test-message", labels=labels, resource=resource
        )

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.gcp_logging.Client")
    def test_should_append_labels(self, mock_client, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        stackdriver_task_handler = self._setup_handler(
            labels={"product.googleapis.com/task_id": "test-value"},
        )
        stackdriver_task_handler.set_context(self.ti)

        self.logger.info("test-message")
        stackdriver_task_handler.flush()

        date_key = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
        labels = {
            "task_id": self.TASK_ID,
            "dag_id": self.DAG_ID,
            date_key: "2016-01-01T00:00:00+00:00",
            "try_number": "1",
            "product.googleapis.com/task_id": "test-value",
        }
        resource = Resource(type="global", labels={})
        self.transport_mock.return_value.send.assert_called_once_with(
            mock.ANY, "test-message", labels=labels, resource=resource
        )

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client")
    def test_should_read_logs_for_all_try(self, mock_client, mock_get_creds_and_project_id):
        mock_client.return_value.list_log_entries.return_value.pages = iter(
            [_create_list_log_entries_response_mock(["MSG1", "MSG2"], None)]
        )
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        stackdriver_task_handler = self._setup_handler()
        logs, metadata = stackdriver_task_handler.read(self.ti)

        date_label = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"

        filter_str = (
            'resource.type="global"\n'
            'logName="projects/project_id/logs/airflow"\n'
            'labels.task_id="task_for_testing_stackdriver_task_handler"\n'
            'labels.dag_id="dag_for_testing_stackdriver_file_task_handler"\n'
            f'labels.{date_label}="2016-01-01T00:00:00+00:00"'
        )
        mock_client.return_value.list_log_entries.assert_called_once_with(
            request=ListLogEntriesRequest(
                resource_names=["projects/project_id"],
                filter=filter_str,
                order_by="timestamp asc",
                page_size=1000,
                page_token=None,
            )
        )
        assert logs == [(("default-hostname", "MSG1\nMSG2"),)]
        assert metadata == [{"end_of_log": True}]

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client")
    def test_should_read_logs_for_task_with_quote(self, mock_client, mock_get_creds_and_project_id):
        mock_client.return_value.list_log_entries.return_value.pages = iter(
            [_create_list_log_entries_response_mock(["MSG1", "MSG2"], None)]
        )
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        self.ti.task_id = 'K"OT'
        stackdriver_task_handler = self._setup_handler()

        logs, metadata = stackdriver_task_handler.read(self.ti)
        date_label = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
        filter_str = (
            'resource.type="global"\n'
            'logName="projects/project_id/logs/airflow"\n'
            'labels.task_id="K\\"OT"\n'
            'labels.dag_id="dag_for_testing_stackdriver_file_task_handler"\n'
            f'labels.{date_label}="2016-01-01T00:00:00+00:00"'
        )
        mock_client.return_value.list_log_entries.assert_called_once_with(
            request=ListLogEntriesRequest(
                resource_names=["projects/project_id"],
                filter=filter_str,
                order_by="timestamp asc",
                page_size=1000,
                page_token=None,
            )
        )
        assert logs == [(("default-hostname", "MSG1\nMSG2"),)]
        assert metadata == [{"end_of_log": True}]

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client")
    def test_should_read_logs_for_single_try(self, mock_client, mock_get_creds_and_project_id):
        mock_client.return_value.list_log_entries.return_value.pages = iter(
            [_create_list_log_entries_response_mock(["MSG1", "MSG2"], None)]
        )
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")
        stackdriver_task_handler = self._setup_handler()

        logs, metadata = stackdriver_task_handler.read(self.ti, 3)
        date_label = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
        filter_str = (
            'resource.type="global"\n'
            'logName="projects/project_id/logs/airflow"\n'
            'labels.task_id="task_for_testing_stackdriver_task_handler"\n'
            'labels.dag_id="dag_for_testing_stackdriver_file_task_handler"\n'
            f'labels.{date_label}="2016-01-01T00:00:00+00:00"\n'
            'labels.try_number="3"'
        )
        mock_client.return_value.list_log_entries.assert_called_once_with(
            request=ListLogEntriesRequest(
                resource_names=["projects/project_id"],
                filter=filter_str,
                order_by="timestamp asc",
                page_size=1000,
                page_token=None,
            )
        )
        assert logs == [(("default-hostname", "MSG1\nMSG2"),)]
        assert metadata == [{"end_of_log": True}]

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client")
    def test_should_read_logs_with_pagination(self, mock_client, mock_get_creds_and_project_id):
        mock_client.return_value.list_log_entries.side_effect = [
            mock.MagicMock(pages=iter([_create_list_log_entries_response_mock(["MSG1", "MSG2"], "TOKEN1")])),
            mock.MagicMock(pages=iter([_create_list_log_entries_response_mock(["MSG3", "MSG4"], None)])),
        ]
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")
        stackdriver_task_handler = self._setup_handler()

        logs, metadata1 = stackdriver_task_handler.read(self.ti, 3)
        date_label = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
        filter_str = (
            'resource.type="global"\n'
            'logName="projects/project_id/logs/airflow"\n'
            'labels.task_id="task_for_testing_stackdriver_task_handler"\n'
            'labels.dag_id="dag_for_testing_stackdriver_file_task_handler"\n'
            f'labels.{date_label}="2016-01-01T00:00:00+00:00"\n'
            'labels.try_number="3"'
        )
        mock_client.return_value.list_log_entries.assert_called_once_with(
            request=ListLogEntriesRequest(
                resource_names=["projects/project_id"],
                filter=filter_str,
                order_by="timestamp asc",
                page_size=1000,
                page_token=None,
            )
        )
        assert logs == [(("default-hostname", "MSG1\nMSG2"),)]
        assert metadata1 == [{"end_of_log": False, "next_page_token": "TOKEN1"}]

        mock_client.return_value.list_log_entries.return_value.next_page_token = None
        logs, metadata2 = stackdriver_task_handler.read(self.ti, 3, metadata1[0])

        mock_client.return_value.list_log_entries.assert_called_with(
            request=ListLogEntriesRequest(
                resource_names=["projects/project_id"],
                filter=(
                    'resource.type="global"\n'
                    'logName="projects/project_id/logs/airflow"\n'
                    'labels.task_id="task_for_testing_stackdriver_task_handler"\n'
                    'labels.dag_id="dag_for_testing_stackdriver_file_task_handler"\n'
                    f'labels.{date_label}="2016-01-01T00:00:00+00:00"\n'
                    'labels.try_number="3"'
                ),
                order_by="timestamp asc",
                page_size=1000,
                page_token="TOKEN1",
            )
        )
        assert logs == [(("default-hostname", "MSG3\nMSG4"),)]
        assert metadata2 == [{"end_of_log": True}]

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client")
    def test_should_read_logs_with_download(self, mock_client, mock_get_creds_and_project_id):
        mock_client.return_value.list_log_entries.side_effect = [
            mock.MagicMock(pages=iter([_create_list_log_entries_response_mock(["MSG1", "MSG2"], "TOKEN1")])),
            mock.MagicMock(pages=iter([_create_list_log_entries_response_mock(["MSG3", "MSG4"], None)])),
        ]
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        stackdriver_task_handler = self._setup_handler()
        logs, metadata1 = stackdriver_task_handler.read(self.ti, 3, {"download_logs": True})

        assert logs == [(("default-hostname", "MSG1\nMSG2\nMSG3\nMSG4"),)]
        assert metadata1 == [{"end_of_log": True}]

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client")
    def test_should_read_logs_with_custom_resources(self, mock_client, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")
        resource = Resource(
            type="cloud_composer_environment",
            labels={
                "environment.name": "test-instance",
                "location": "europe-west-3",
                "project_id": "project_id",
            },
        )
        stackdriver_task_handler = self._setup_handler(resource=resource)

        entry = mock.MagicMock(json_payload={"message": "TEXT"})
        page = mock.MagicMock(entries=[entry, entry], next_page_token=None)
        mock_client.return_value.list_log_entries.return_value.pages = iter([page])

        logs, metadata = stackdriver_task_handler.read(self.ti)
        date_label = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
        filter_str = (
            'resource.type="cloud_composer_environment"\n'
            'logName="projects/project_id/logs/airflow"\n'
            'resource.labels."environment.name"="test-instance"\n'
            'resource.labels.location="europe-west-3"\n'
            'resource.labels.project_id="project_id"\n'
            'labels.task_id="task_for_testing_stackdriver_task_handler"\n'
            'labels.dag_id="dag_for_testing_stackdriver_file_task_handler"\n'
            f'labels.{date_label}="2016-01-01T00:00:00+00:00"'
        )
        mock_client.return_value.list_log_entries.assert_called_once_with(
            request=ListLogEntriesRequest(
                resource_names=["projects/project_id"],
                filter=filter_str,
                order_by="timestamp asc",
                page_size=1000,
                page_token=None,
            )
        )
        assert logs == [(("default-hostname", "TEXT\nTEXT"),)]
        assert metadata == [{"end_of_log": True}]

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.gcp_logging.Client")
    def test_should_use_credentials(self, mock_client, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        stackdriver_task_handler = StackdriverTaskHandler(gcp_key_path="KEY_PATH")
        client = stackdriver_task_handler._client

        mock_get_creds_and_project_id.assert_called_once_with(
            disable_logging=True,
            key_path="KEY_PATH",
            scopes=frozenset(
                {
                    "https://www.googleapis.com/auth/logging.write",
                    "https://www.googleapis.com/auth/logging.read",
                }
            ),
        )
        mock_client.assert_called_once_with(credentials="creds", client_info=mock.ANY, project="project_id")
        assert mock_client.return_value == client

    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.get_credentials_and_project_id")
    @mock.patch("airflow.providers.google.cloud.log.stackdriver_task_handler.LoggingServiceV2Client")
    def test_should_return_valid_external_url(self, mock_client, mock_get_creds_and_project_id):
        mock_get_creds_and_project_id.return_value = ("creds", "project_id")

        stackdriver_task_handler = StackdriverTaskHandler(gcp_key_path="KEY_PATH")
        url = stackdriver_task_handler.get_external_log_url(self.ti, self.ti.try_number)

        parsed_url = urlsplit(url)
        parsed_qs = parse_qs(parsed_url.query)
        assert parsed_url.scheme == "https"
        assert parsed_url.netloc == "console.cloud.google.com"
        assert parsed_url.path == "/logs/viewer"
        assert {"project", "interval", "resource", "advancedFilter"} == set(parsed_qs.keys())
        assert "global" in parsed_qs["resource"]

        filter_params = parsed_qs["advancedFilter"][0].splitlines()
        date_label = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
        expected_filter = [
            'resource.type="global"',
            'logName="projects/project_id/logs/airflow"',
            f'labels.task_id="{self.ti.task_id}"',
            f'labels.dag_id="{self.DAG_ID}"',
            f'labels.{date_label}="{self.ti.logical_date.isoformat() if AIRFLOW_V_3_0_PLUS else self.ti.execution_date.isoformat()}"',
            f'labels.try_number="{self.ti.try_number}"',
        ]
        assert set(expected_filter) == set(filter_params)
