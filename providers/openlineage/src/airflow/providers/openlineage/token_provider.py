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

from typing import Any

from airflow.providers.common.compat.sdk import AirflowException, BaseHook

AIRFLOW_CONNECTION_API_KEY_AUTH_TYPE = "airflow_connection_api_key"
_DEFAULT_EXTRA_KEYS = ("apiKey", "api_key", "apikey", "token", "access_token")


class OpenLineageAirflowConnectionAuthError(AirflowException):
    """Raised when OpenLineage API key auth cannot be resolved from an Airflow connection."""


class AirflowConnectionTokenProvider:
    """
    Resolve an OpenLineage API key from an Airflow connection.

    The connection password is preferred. If it is empty, the provider falls back to common keys in
    the connection ``extra`` JSON, or to the user-specified ``extra_key``.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self.conn_id = config.get("conn_id", "")
        self.extra_key = config.get("extra_key")
        if not self.conn_id:
            raise OpenLineageAirflowConnectionAuthError(
                "OpenLineage `airflow_connection_api_key` auth requires a non-empty `conn_id`."
            )

    def get_api_key(self) -> str:
        connection = BaseHook.get_connection(self.conn_id)
        if connection.password:
            return connection.password.strip()
        api_key = self._get_api_key_from_extra(connection.extra_dejson)
        if api_key:
            return api_key

        raise OpenLineageAirflowConnectionAuthError(
            "OpenLineage `airflow_connection_api_key` auth could not find a token in connection "
            f"`{self.conn_id}`. Expected connection password or token in connection extra."
        )

    def _get_api_key_from_extra(self, extra: dict[str, Any]) -> str | None:
        if self.extra_key:
            value = extra.get(self.extra_key)
            return str(value).strip() if value else None

        for key in _DEFAULT_EXTRA_KEYS:
            value = extra.get(key)
            if value:
                return str(value).strip()
        return None

    def get_bearer(self) -> str:
        return f"Bearer {self.get_api_key()}"
