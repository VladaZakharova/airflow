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

from typing import Any

from airflow_google_provider_resource_cleanup.handlers._base import BaseDeleteHandler
from airflow_google_provider_resource_cleanup.helpers import get_resource_path, run_command_async


def _extract_project_id(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip().split("/")[-1]
    # gcloud transfer jobs delete rejects numeric project numbers and requires a project ID.
    if not candidate or candidate.isdigit():
        return None
    return candidate


def _extract_transfer_job_params(
    resource: dict,
    project_id: str | None = None,
) -> tuple[str, str | None]:
    path_parts = get_resource_path(resource).split("/")
    transfer_job_name = path_parts[-1]
    if path_parts[0] == "projects" and len(path_parts) >= 4:
        extracted_project = _extract_project_id(path_parts[1])
        if extracted_project:
            return transfer_job_name, extracted_project
    extracted_project = _extract_project_id(resource.get("project")) or _extract_project_id(project_id)
    return transfer_job_name, extracted_project


async def _delete_bucket(resource: dict, log_prefix: str):
    name = get_resource_path(resource).split("/")[-1]
    cmd = f"gcloud storage rm --recursive gs://{name} --quiet"
    return await run_command_async(cmd, log_prefix, check=True, ignore_not_found=True)


async def _delete_transfer_job(
    resource: dict,
    log_prefix: str,
    *,
    project_id: str | None = None,
):
    if resource.get("state") == "DELETED":
        print(f"{log_prefix}Transfer job {get_resource_path(resource)} is already deleted. Skipping.")
        return False
    transfer_job_name, resolved_project_id = _extract_transfer_job_params(resource, project_id=project_id)
    project_flag = f" --project={resolved_project_id}" if resolved_project_id else ""
    cmd = f"gcloud transfer jobs delete {transfer_job_name}{project_flag} --quiet"
    return await run_command_async(cmd, log_prefix, check=True, ignore_not_found=True)


class StorageDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "storage.googleapis.com/Bucket": _delete_bucket,
        "storagetransfer.googleapis.com/TransferJob": _delete_transfer_job,
    }

    DELETION_ORDER = [
        "storagetransfer.googleapis.com/TransferJob",
        "storage.googleapis.com/Bucket",
    ]

    def __init__(self, project_id: str | None = None):
        super().__init__(project_id=project_id)
        if self.project_id:

            async def delete_transfer_job(resource: dict, log_prefix: str) -> bool | None:
                return await _delete_transfer_job(resource, log_prefix, project_id=self.project_id)

            self.DELETERS = {
                **self.DELETERS,
                "storagetransfer.googleapis.com/TransferJob": delete_transfer_job,
            }
