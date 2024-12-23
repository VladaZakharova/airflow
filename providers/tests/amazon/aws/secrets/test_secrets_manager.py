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

from unittest import mock

import pytest
from moto import mock_aws

from airflow.providers.amazon.aws.secrets.secrets_manager import SecretsManagerBackend


class TestSecretsManagerBackend:
    @mock.patch("airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend.get_conn_value")
    def test_aws_secrets_manager_get_connection(self, mock_get_value):
        mock_get_value.return_value = "scheme://user:pass@host:100"
        conn = SecretsManagerBackend().get_connection("fake_conn")
        assert conn.host == "host"

    @mock_aws
    def test_get_conn_value_full_url_mode(self):
        secret_id = "airflow/connections/test_postgres"
        create_param = {
            "Name": secret_id,
            "SecretString": "postgresql://airflow:airflow@host:5432/airflow",
        }

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.create_secret(**create_param)

        returned_uri = secrets_manager_backend.get_conn_value(conn_id="test_postgres")
        assert returned_uri == "postgresql://airflow:airflow@host:5432/airflow"

    @mock_aws
    def test_get_conn_value_non_existent_key(self):
        """
        Test that if the key with connection ID is not present,
        SecretsManagerBackend.get_connection should return None
        """
        conn_id = "test_mysql"

        secret_id = "airflow/connections/test_postgres"
        create_param = {
            "Name": secret_id,
            "SecretString": "postgresql://airflow:airflow@host:5432/airflow",
        }

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.create_secret(**create_param)

        assert secrets_manager_backend.get_conn_value(conn_id=conn_id) is None
        assert secrets_manager_backend.get_connection(conn_id=conn_id) is None

    @mock_aws
    def test_get_variable(self):
        secret_id = "airflow/variables/hello"
        create_param = {"Name": secret_id, "SecretString": "world"}

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.create_secret(**create_param)

        returned_uri = secrets_manager_backend.get_variable("hello")
        assert returned_uri == "world"

    @mock_aws
    def test_get_variable_non_existent_key(self):
        """
        Test that if Variable key is not present,
        SystemsManagerParameterStoreBackend.get_variables should return None
        """
        secret_id = "airflow/variables/hello"
        create_param = {"Name": secret_id, "SecretString": "world"}

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.create_secret(**create_param)

        assert secrets_manager_backend.get_variable("test_mysql") is None

    @mock_aws
    def test_get_config_non_existent_key(self):
        """
        Test that if Config key is not present,
        SystemsManagerParameterStoreBackend.get_config should return None
        """
        secret_id = "airflow/config/hello"
        create_param = {"Name": secret_id, "SecretString": "world"}

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.create_secret(**create_param)

        assert secrets_manager_backend.get_config("test") is None

    @mock.patch("airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend._get_secret")
    def test_connection_prefix_none_value(self, mock_get_secret):
        """
        Test that if Connection ID is not present in AWS Secrets Manager,
        SecretsManagerBackend.get_conn_value should return None,
        SecretsManagerBackend._get_secret should not be called
        """
        kwargs = {"connections_prefix": None}

        secrets_manager_backend = SecretsManagerBackend(**kwargs)

        assert secrets_manager_backend.get_conn_value("test_mysql") is None
        mock_get_secret.assert_not_called()

    @mock.patch("airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend._get_secret")
    def test_variable_prefix_none_value(self, mock_get_secret):
        """
        Test that if Variable key is not present in AWS Secrets Manager,
        SecretsManagerBackend.get_variables should return None,
        SecretsManagerBackend._get_secret should not be called
        """
        kwargs = {"variables_prefix": None}

        secrets_manager_backend = SecretsManagerBackend(**kwargs)

        assert secrets_manager_backend.get_variable("hello") is None
        mock_get_secret.assert_not_called()

    @mock.patch("airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend._get_secret")
    def test_config_prefix_none_value(self, mock_get_secret):
        """
        Test that if Config key is not present in AWS Secrets Manager,
        SecretsManagerBackend.get_config should return None,
        SecretsManagerBackend._get_secret should not be called
        """
        kwargs = {"config_prefix": None}

        secrets_manager_backend = SecretsManagerBackend(**kwargs)

        assert secrets_manager_backend.get_config("config") is None
        mock_get_secret.assert_not_called()

    @mock.patch(
        "airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend.client",
        new_callable=mock.PropertyMock,
    )
    @pytest.mark.parametrize(
        "connection_id, connections_lookup_pattern, num_client_calls",
        [
            ("test", "test", 1),
            ("test", ".*", 1),
            ("test", "T.*", 1),
            ("test", "dummy-pattern", 0),
            ("test", None, 1),
        ],
    )
    def test_connection_lookup_pattern(
        self, mock_client, connection_id, connections_lookup_pattern, num_client_calls
    ):
        """
        Test that if Connection ID is looked up in AWS Secrets Manager
        """
        mock_client().get_secret_value.return_value = {"SecretString": None}
        kwargs = {"connections_lookup_pattern": connections_lookup_pattern}

        secrets_manager_backend = SecretsManagerBackend(**kwargs)
        secrets_manager_backend.get_conn_value(connection_id)
        assert mock_client().get_secret_value.call_count == num_client_calls

    @mock.patch(
        "airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend.client",
        new_callable=mock.PropertyMock,
    )
    @pytest.mark.parametrize(
        "variable_key, variables_lookup_pattern, num_client_calls",
        [
            ("test", "test", 1),
            ("test", ".*", 1),
            ("test", "T.*", 1),
            ("test", "dummy-pattern", 0),
            ("test", None, 1),
        ],
    )
    def test_variable_lookup_pattern(
        self, mock_client, variable_key, variables_lookup_pattern, num_client_calls
    ):
        """
        Test that if Variable key is looked up in AWS Secrets Manager
        """
        mock_client().get_secret_value.return_value = {"SecretString": None}
        kwargs = {"variables_lookup_pattern": variables_lookup_pattern}

        secrets_manager_backend = SecretsManagerBackend(**kwargs)
        secrets_manager_backend.get_variable(variable_key)
        assert mock_client().get_secret_value.call_count == num_client_calls

    @mock.patch(
        "airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend.client",
        new_callable=mock.PropertyMock,
    )
    @pytest.mark.parametrize(
        "config_key, config_lookup_pattern, num_client_calls",
        [
            ("test", "test", 1),
            ("test", ".*", 1),
            ("test", "T.*", 1),
            ("test", "dummy-pattern", 0),
            ("test", None, 1),
        ],
    )
    def test_config_lookup_pattern(self, mock_client, config_key, config_lookup_pattern, num_client_calls):
        """
        Test that if Variable key is looked up in AWS Secrets Manager
        """
        mock_client().get_secret_value.return_value = {"SecretString": None}
        kwargs = {"config_lookup_pattern": config_lookup_pattern}

        secrets_manager_backend = SecretsManagerBackend(**kwargs)
        secrets_manager_backend.get_config(config_key)
        assert mock_client().get_secret_value.call_count == num_client_calls

    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.SessionFactory")
    def test_passing_client_kwargs(self, mock_session_factory):
        secrets_manager_backend = SecretsManagerBackend(
            use_ssl=False, role_arn="arn:aws:iam::222222222222:role/awesome-role", region_name="eu-central-1"
        )

        # Mock SessionFactory, session and client
        mock_session_factory_instance = mock_session_factory.return_value
        mock_ssm_client = mock.MagicMock(return_value="mock-secretsmanager-client")
        mock_session = mock.MagicMock()
        mock_session.client = mock_ssm_client
        mock_create_session = mock.MagicMock(return_value=mock_session)
        mock_session_factory_instance.create_session = mock_create_session

        secrets_manager_backend.client
        assert mock_session_factory.call_count == 1
        mock_session_factory_call_kwargs = mock_session_factory.call_args.kwargs
        assert "conn" in mock_session_factory_call_kwargs
        conn_wrapper = mock_session_factory_call_kwargs["conn"]

        assert conn_wrapper.conn_id == "SecretsManagerBackend__connection"
        assert conn_wrapper.role_arn == "arn:aws:iam::222222222222:role/awesome-role"
        assert conn_wrapper.region_name == "eu-central-1"

        mock_ssm_client.assert_called_once_with(
            service_name="secretsmanager", region_name="eu-central-1", use_ssl=False
        )
