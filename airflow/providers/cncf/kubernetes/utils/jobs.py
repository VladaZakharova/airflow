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
"""Launches JOBs."""
from __future__ import annotations

import enum

JOB_FINAL_STATUS_CONDITION_TYPES = {
    "Complete",
    "Failed",
}

JOB_STATUS_CONDITION_TYPES = JOB_FINAL_STATUS_CONDITION_TYPES | {"Suspended"}


class OnJobFinishAction(str, enum.Enum):
    """Action to take when the job finishes."""

    KEEP_JOB = "keep_job"
    DELETE_JOB = "delete_job"
    DELETE_SUCCEEDED_JOB = "delete_succeeded_job"
    DELETE_FAILED_JOB = "delete_failed_job"
