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

from typing import TYPE_CHECKING

from airflow_google_provider_resource_cleanup.handlers._base import BaseDeleteHandler
from airflow_google_provider_resource_cleanup.helpers import curl, get_resource_path

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

API_BASE = "https://dlp.googleapis.com/v2/"


def _resolve_dlp_resource_path(resource: dict, project_id: str | None = None) -> str:
    path = get_resource_path(resource)
    path_parts = path.split("/")
    # Cloud Asset Inventory returns numeric project numbers for DLP assets, which the DLP v2 API rejects.
    if (
        len(path_parts) >= 2
        and path_parts[0] == "projects"
        and path_parts[1].isdigit()
        and isinstance(project_id, str)
        and project_id
        and not project_id.isdigit()
    ):
        path_parts[1] = project_id
        return "/".join(path_parts)
    return path


async def _delete_dlp_resource(
    resource: dict,
    log_prefix: str,
    *,
    project_id: str | None = None,
):
    name = _resolve_dlp_resource_path(resource, project_id=project_id)
    return await curl(f"{API_BASE}{name}", log_prefix=log_prefix)


class DLPDeleteHandler(BaseDeleteHandler):
    DELETERS: dict[str, Callable[[dict, str], Awaitable[bool | None]]] = {
        "dlp.googleapis.com/JobTrigger": _delete_dlp_resource,
        "dlp.googleapis.com/DiscoveryConfig": _delete_dlp_resource,
        "dlp.googleapis.com/DlpJob": _delete_dlp_resource,
        "dlp.googleapis.com/InspectTemplate": _delete_dlp_resource,
        "dlp.googleapis.com/DeidentifyTemplate": _delete_dlp_resource,
        "dlp.googleapis.com/StoredInfoType": _delete_dlp_resource,
    }

    DELETION_ORDER = [
        "dlp.googleapis.com/JobTrigger",
        "dlp.googleapis.com/DiscoveryConfig",
        "dlp.googleapis.com/DlpJob",
        "dlp.googleapis.com/InspectTemplate",
        "dlp.googleapis.com/DeidentifyTemplate",
        "dlp.googleapis.com/StoredInfoType",
    ]

    def __init__(self, project_id: str | None = None):
        super().__init__(project_id=project_id)
        if self.project_id:

            async def delete_dlp_resource(resource: dict, log_prefix: str) -> bool | None:
                return await _delete_dlp_resource(resource, log_prefix, project_id=self.project_id)

            self.DELETERS = dict.fromkeys(self.DELETERS, delete_dlp_resource)
