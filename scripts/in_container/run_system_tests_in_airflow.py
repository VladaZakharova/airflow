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
Run system test Dags in a running Airflow and report which of them failed.

Runs inside the breeze container while the scheduler, dag-processor and triggerer are running.
For every system test file in the Dags folder it waits until the Dag is parsed, starts one Dag run,
waits until the run finishes, and writes a summary. The watcher task in each system test turns any
failed task into a failed Dag run, so the Dag run state is the test result.

Exit code 1 when any test failed, timed out or could not be loaded.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from sqlalchemy import select

from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.errors import ParseImportError
from airflow.models.taskinstance import TaskInstance
from airflow.utils.session import create_session

POLL_SECONDS = 30
SCHEDULED_RUN_WAIT_SECONDS = 90


@dataclass
class TestResult:
    file: str
    dag_id: str | None = None
    run_id: str | None = None
    status: str = "PENDING"  # PENDING, RUNNING, SUCCESS, FAILED, TIMED_OUT, IMPORT_ERROR
    started_at: float | None = None
    duration_minutes: float | None = None
    failed_tasks: list[str] = field(default_factory=list)
    details: str = ""


def run_airflow_cli(*args: str) -> None:
    subprocess.run(["airflow", *args], check=True, capture_output=True, text=True)


def wait_until_parsed(results: list[TestResult], timeout_seconds: int) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        with create_session() as session:
            for result in results:
                if result.dag_id or result.status == "IMPORT_ERROR":
                    continue
                dag_model = session.scalar(
                    select(DagModel).where(DagModel.fileloc.endswith(f"/{result.file}"))
                )
                if dag_model:
                    result.dag_id = dag_model.dag_id
                    continue
                import_error = session.scalar(
                    select(ParseImportError).where(ParseImportError.filename.endswith(result.file))
                )
                if import_error:
                    result.status = "IMPORT_ERROR"
                    result.details = (import_error.stacktrace or "Import error").strip().splitlines()[-1]
        if all(r.dag_id or r.status == "IMPORT_ERROR" for r in results):
            return
        time.sleep(5)
    for result in results:
        if not result.dag_id and result.status != "IMPORT_ERROR":
            result.status = "IMPORT_ERROR"
            result.details = "Dag was not parsed in time"


def start_dag_run(result: TestResult) -> None:
    """Unpause the Dag; system tests use ``@once`` so the scheduler creates the run. Trigger otherwise."""
    run_airflow_cli("dags", "unpause", result.dag_id)
    deadline = time.monotonic() + SCHEDULED_RUN_WAIT_SECONDS
    while time.monotonic() < deadline:
        if find_first_run(result.dag_id):
            break
        time.sleep(5)
    else:
        run_airflow_cli("dags", "trigger", result.dag_id)
    result.status = "RUNNING"
    result.started_at = time.monotonic()


def find_first_run(dag_id: str) -> DagRun | None:
    with create_session() as session:
        return session.scalar(select(DagRun).where(DagRun.dag_id == dag_id).order_by(DagRun.id).limit(1))


def update_running(result: TestResult, timeout_seconds: int) -> None:
    dag_run = find_first_run(result.dag_id)
    elapsed = time.monotonic() - result.started_at
    result.duration_minutes = round(elapsed / 60, 1)
    if dag_run is None:
        if elapsed > timeout_seconds:
            result.status = "TIMED_OUT"
        return
    result.run_id = dag_run.run_id
    if dag_run.state in ("success", "failed"):
        result.status = "SUCCESS" if dag_run.state == "success" else "FAILED"
        with create_session() as session:
            result.failed_tasks = sorted(
                session.scalars(
                    select(TaskInstance.task_id).where(
                        TaskInstance.dag_id == result.dag_id,
                        TaskInstance.run_id == dag_run.run_id,
                        TaskInstance.state.in_(["failed", "upstream_failed"]),
                        TaskInstance.task_id != "watcher",
                    )
                )
            )
    elif elapsed > timeout_seconds:
        result.status = "TIMED_OUT"
        result.details = f"still {dag_run.state} after {result.duration_minutes} minutes"


def write_summary(results: list[TestResult], output_dir: Path) -> None:
    icons = {"SUCCESS": "✅", "FAILED": "❌", "TIMED_OUT": "⏱️", "IMPORT_ERROR": "💥"}
    failed = [r for r in results if r.status != "SUCCESS"]
    lines = [
        "## Google system tests",
        "",
        f"**{len(results) - len(failed)} of {len(results)} passed**",
        "",
        "| System test | Dag | Result | Minutes | Failed tasks / details |",
        "|---|---|---|---|---|",
    ]
    for r in results:
        problems = ", ".join(r.failed_tasks) or r.details or "–"
        lines.append(
            f"| `{r.file}` | `{r.dag_id or '–'}` | {icons.get(r.status, '')} {r.status} "
            f"| {r.duration_minutes or '–'} | {problems} |"
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.md").write_text("\n".join(lines) + "\n")
    (output_dir / "results.json").write_text(json.dumps([asdict(r) for r in results], indent=2))
    print("\n".join(lines))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dags-folder", type=Path, default=Path("/files/dags"))
    parser.add_argument("--output-dir", type=Path, default=Path("/files/system-tests-results"))
    parser.add_argument("--timeout-minutes", type=int, default=180, help="Per-test timeout")
    parser.add_argument("--max-parallel", type=int, default=5, help="Dag runs at the same time")
    parser.add_argument("--parse-timeout-minutes", type=int, default=10)
    args = parser.parse_args()

    results = [TestResult(file=path.name) for path in sorted(args.dags_folder.glob("example_*.py"))]
    if not results:
        print(f"No system tests found in {args.dags_folder}")
        return 1

    wait_until_parsed(results, args.parse_timeout_minutes * 60)
    timeout_seconds = args.timeout_minutes * 60
    while True:
        running = [r for r in results if r.status == "RUNNING"]
        for result in running:
            update_running(result, timeout_seconds)
        running = [r for r in results if r.status == "RUNNING"]
        for result in [r for r in results if r.status == "PENDING"][: args.max_parallel - len(running)]:
            print(f"Starting {result.dag_id} ({result.file})")
            start_dag_run(result)
        if all(r.status not in ("PENDING", "RUNNING") for r in results):
            break
        print(", ".join(f"{r.dag_id or r.file}: {r.status}" for r in results), flush=True)
        time.sleep(POLL_SECONDS)

    write_summary(results, args.output_dir)
    return 0 if all(r.status == "SUCCESS" for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
