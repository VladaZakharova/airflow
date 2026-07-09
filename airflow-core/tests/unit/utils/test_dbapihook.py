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

import pytest

from airflow.models.connection import Connection
from airflow.providers.common.sql.hooks.sql import DbApiHook


@pytest.fixture
def mock_connection() -> Connection:
    conn = Connection(
        conn_id="test_conn",
        conn_type="postgres",
        host="my_host.com",
        login="my_user",
        password="my_password",
        port=5432,
        schema="my_schema",
    )
    # Intentionally remove get_uri if it exists to simulate the breaking change in Airflow 3
    if hasattr(conn, "get_uri"):
        delattr(Connection, "get_uri")
    return conn


@pytest.fixture
def dbapi_hook(mock_connection: Connection) -> DbApiHook:
    class TestDbApiHook(DbApiHook):
        conn_name_attr = "test_conn_id"
        default_conn_name = "test_conn"
        conn_type = "postgres"
        hook_name = "Test"

        def get_connection(self, conn_id):
            return mock_connection

    return TestDbApiHook(test_conn_id="test_conn")


def test_get_uri(dbapi_hook: DbApiHook):
    uri = dbapi_hook.get_uri()
    assert uri == "postgres://my_user:my_password@my_host.com:5432/my_schema"


def test_get_openlineage_authority_part(dbapi_hook: DbApiHook, mock_connection: Connection):
    authority = dbapi_hook.get_openlineage_authority_part(mock_connection, default_port=5432)
    assert authority == "my_host.com:5432"
