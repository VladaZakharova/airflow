#!/usr/bin/env python3
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
"""
Print links to the Cloud Logging views of one Google system tests run and add them to the job summary.

The VM's logging agent ships everything under files/system-tests-results/<env id>/ (Airflow components,
task logs, test events), so filtering on that path shows exactly one run.
"""

from __future__ import annotations

import argparse
import datetime
import os
from urllib.parse import quote

COMPONENTS = ["scheduler", "dag-processor", "triggerer", "api-server"]


def build_logs_url(project: str, query: str, start: datetime.datetime, hours: int) -> str:
    end = start + datetime.timedelta(hours=hours)
    time_range = quote(f"{start:%Y-%m-%dT%H:%M:%SZ}/{end:%Y-%m-%dT%H:%M:%SZ}", safe="")
    return (
        f"https://console.cloud.google.com/logs/query;query={quote(query, safe='')};"
        f"timeRange={time_range}?project={project}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--env-id", required=True, help="SYSTEM_TESTS_ENV_ID of the run")
    parser.add_argument("--run-id", required=True, help="GitHub run id")
    parser.add_argument("--pr", default="")
    parser.add_argument("--vm", required=True)
    parser.add_argument("--zone", required=True)
    parser.add_argument("--hours", type=int, default=12, help="Length of the time range in the links")
    args = parser.parse_args()

    start = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=5)
    logs = f"projects/{args.project}/logs"
    run_path = f'labels."agent.googleapis.com/log_file_path"=~"system-tests-results/{args.env_id}/"'
    links = [
        ("Everything of this run", f'logName=~"{logs}/gst-" AND {run_path}'),
        (
            "Test results (one event per test, with failed tasks)",
            f'logName="{logs}/gst-events" AND jsonPayload.run_id="{args.run_id}"',
        ),
        ("Task logs (all Dags of this run)", f'logName="{logs}/gst-task-logs" AND {run_path}'),
    ]
    links += [
        (
            f"Airflow {component}",
            f'logName="{logs}/gst-components" AND {run_path} '
            f'AND labels."agent.googleapis.com/log_file_path"=~"components/{component}.log"',
        )
        for component in COMPONENTS
    ]
    links.append(("Errors only", f'logName=~"{logs}/gst-" AND {run_path} AND severity>=ERROR'))

    vm_url = (
        f"https://console.cloud.google.com/compute/instancesDetail/zones/{args.zone}/instances/"
        f"{args.vm}?project={args.project}&tab=monitoring"
    )
    title = f"PR #{args.pr}, run {args.run_id}" if args.pr else f"run {args.run_id}"
    lines = [
        f"## Logs and monitoring of this run ({title})",
        "",
        "Opens Google Cloud console pages (requires access to the project).",
        "",
    ]
    lines += [
        f"- [{name}]({build_logs_url(args.project, query, start, args.hours)})" for name, query in links
    ]
    lines += [f"- [VM {args.vm}: CPU, memory, disk]({vm_url})", ""]
    text = "\n".join(lines) + "\n"
    print(text)
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(text)


if __name__ == "__main__":
    main()
