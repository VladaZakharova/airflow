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

from airflow_google_provider_resource_cleanup.handlers._base import BaseDeleteHandler
from airflow_google_provider_resource_cleanup.helpers import get_resource_path, run_command_async


def _extract_instance_params(resource: dict) -> tuple[str, str]:
    path_parts = get_resource_path(resource).split("/")
    project_id = path_parts[1]
    instance = path_parts[-1]
    return instance, project_id


def _extract_backup_params(resource: dict) -> tuple[str, str, str]:
    path_parts = get_resource_path(resource).split("/")
    project_id = path_parts[1]
    instance = path_parts[3]
    backup_id = path_parts[-1]
    return backup_id, instance, project_id


async def _delete_instance(resource: dict, log_prefix: str):
    instance, project_id = _extract_instance_params(resource)
    cmd = f"gcloud sql instances delete {instance} --project={project_id} --quiet"
    return await run_command_async(cmd, log_prefix, check=True, ignore_not_found=True)


async def _delete_backup(resource: dict, log_prefix: str):
    path = get_resource_path(resource)
    path_parts = path.split("/")
    project_id = path_parts[1]
    if "instances" in path_parts:
        backup_id, instance, project_id = _extract_backup_params(resource)
        cmd = f"gcloud sql backups delete {backup_id} --instance={instance} --project={project_id} --quiet"
    else:
        cmd = f"gcloud sql backups delete {path} --project={project_id} --quiet"
    return await run_command_async(cmd, log_prefix, check=True, ignore_not_found=True)


async def _delete_backup_run(resource: dict, log_prefix: str):
    backup_id, instance, project_id = _extract_backup_params(resource)
    cmd = f"gcloud sql backups delete {backup_id} --instance={instance} --project={project_id} --quiet"
    return await run_command_async(cmd, log_prefix, check=True, ignore_not_found=True)


class CloudSQLDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "sqladmin.googleapis.com/Backup": _delete_backup,
        "sqladmin.googleapis.com/BackupRun": _delete_backup_run,
        "sqladmin.googleapis.com/Instance": _delete_instance,
    }
    DELETION_ORDER = [
        "sqladmin.googleapis.com/BackupRun",
        "sqladmin.googleapis.com/Backup",
        "sqladmin.googleapis.com/Instance",
    ]
