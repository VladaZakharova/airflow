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
Keep one status comment per PR for its Google system tests run, and compute queue positions.

Commands:
  update   create or edit the PR's status comment (state, risk, queue position, links)
  position print "<position> <waiting>" of a run among runs waiting for a test VM

The comment carries its data in a hidden JSON block, so each update keeps what earlier steps wrote
(e.g. the risk level from the security review).

Environment: GITHUB_TOKEN, GITHUB_REPOSITORY, GITHUB_SERVER_URL (set by GitHub Actions).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.request

MARKER = "<!-- google-system-tests-status -->"
DATA_RE = re.compile(r"<!-- gst-data (\{.*?\}) -->", re.S)
PREPARE_JOB_PREFIX = "Start the test VM"
WORKFLOW_FILE = "google-system-tests.yml"

STATES = {
    "waiting-approval": ("⏸️", "Waiting for approval by a trusted approver"),
    "queued": ("⏳", "Approved, waiting for a free test VM"),
    "starting": ("🚀", "Starting the test VM"),
    "running": ("🏃", "Running the system tests"),
    "passed": ("✅", "All system tests passed"),
    "failed": ("❌", "System tests failed"),
    "cancelled": ("⚪", "Run cancelled"),
    "stopped": ("🛑", "Stopped by the emergency stop"),
}
RISK_BADGES = {
    "LOW": "🟢 LOW",
    "MEDIUM": "🟡 MEDIUM",
    "HIGH": "🔴 HIGH (see the security review before approving)",
}


def call_github(method: str, path: str, body: dict | None = None):
    url = path if path.startswith("http") else f"https://api.github.com{path}"
    request = urllib.request.Request(
        url,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read() or b"null")


def find_status_comment(repo: str, pr: str) -> dict | None:
    page = 1
    while True:
        comments = call_github("GET", f"/repos/{repo}/issues/{pr}/comments?per_page=100&page={page}")
        for comment in comments:
            if MARKER in (comment.get("body") or ""):
                return comment
        if len(comments) < 100:
            return None
        page += 1


def render(data: dict) -> str:
    emoji, text = STATES.get(data.get("state", ""), ("ℹ️", data.get("state", "")))
    if data.get("state") == "queued" and data.get("position"):
        text += f": position {data['position']} of {data['waiting']}"
    lines = [MARKER, "### Google system tests", "", f"{emoji} **{text}**", ""]
    rows = []
    if data.get("risk"):
        rows.append(("Security review", RISK_BADGES.get(data["risk"], data["risk"])))
    if data.get("tests"):
        rows.append(("Selected tests", ", ".join(f"`{t.rsplit('/', 1)[-1]}`" for t in data["tests"])))
    if data.get("run_url"):
        rows.append(("Workflow run", f"[run {data.get('run_id', '')}]({data['run_url']})"))
    if data.get("vm"):
        rows.append(("Test VM", f"`{data['vm']}`"))
    if data.get("note"):
        rows.append(("Note", data["note"]))
    if rows:
        lines += ["| | |", "|---|---|"] + [f"| {k} | {v} |" for k, v in rows] + [""]
    if data.get("links"):
        lines += [
            "<details><summary>Logs and monitoring of this run</summary>",
            "",
            data["links"],
            "</details>",
            "",
        ]
    if data.get("run_url"):
        server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
        repo = os.environ["GITHUB_REPOSITORY"]
        lines.append(
            f"[All queued runs]({server}/{repo}/actions/workflows/{WORKFLOW_FILE}?query=is%3Ain_progress) · "
            f"[Waiting for approval]({server}/{repo}/actions/workflows/{WORKFLOW_FILE}?query=is%3Awaiting) · "
            f"[Emergency stop runs]({server}/{repo}/actions/workflows/google-system-tests-emergency.yml)"
        )
        lines.append("")
        lines.append(
            "🛑 Emergency stop: add the label `gst-stop` to this PR (or `gst-stop-all` to stop all runs)"
        )
    lines.append(f"<!-- gst-data {json.dumps(data)} -->")
    return "\n".join(lines) + "\n"


def update(args: argparse.Namespace) -> None:
    repo = os.environ["GITHUB_REPOSITORY"]
    comment = find_status_comment(repo, args.pr)
    data: dict = {}
    if comment:
        match = DATA_RE.search(comment["body"])
        if match:
            data = json.loads(match.group(1))
    if data.get("run_id") and args.run_id and str(data["run_id"]) != str(args.run_id):
        data = {}  # a new run of the PR starts a fresh status
    data["state"] = args.state
    for key in ("risk", "run_id", "run_url", "vm", "note"):
        if getattr(args, key) is not None:
            data[key] = getattr(args, key)
    if args.tests is not None:
        data["tests"] = args.tests.split()
    if args.position is not None:
        data["position"], data["waiting"] = args.position, args.waiting
    if args.links_file and os.path.exists(args.links_file):
        data["links"] = open(args.links_file).read().strip()
    body = render(data)
    if comment:
        call_github("PATCH", f"/repos/{repo}/issues/comments/{comment['id']}", {"body": body})
    else:
        call_github("POST", f"/repos/{repo}/issues/{args.pr}/comments", {"body": body})


def position(args: argparse.Namespace) -> None:
    """Runs waiting for a VM have their prepare job in progress; order them by when that job started."""
    repo = os.environ["GITHUB_REPOSITORY"]
    runs = call_github(
        "GET", f"/repos/{repo}/actions/workflows/{WORKFLOW_FILE}/runs?status=in_progress&per_page=50"
    )["workflow_runs"]
    waiting = []
    for run in runs:
        for job in call_github("GET", f"/repos/{repo}/actions/runs/{run['id']}/jobs")["jobs"]:
            if job["name"].startswith(PREPARE_JOB_PREFIX) and job["status"] == "in_progress":
                waiting.append((job["started_at"], run["id"]))
    waiting.sort()
    ids = [run_id for _, run_id in waiting]
    pos = ids.index(int(args.run_id)) + 1 if int(args.run_id) in ids else 0
    print(pos, len(ids))


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)
    p_update = sub.add_parser("update")
    p_update.add_argument("--pr", required=True)
    p_update.add_argument("--state", required=True, choices=list(STATES))
    p_update.add_argument("--risk")
    p_update.add_argument("--run-id")
    p_update.add_argument("--run-url")
    p_update.add_argument("--vm")
    p_update.add_argument("--note")
    p_update.add_argument("--tests")
    p_update.add_argument("--position", type=int)
    p_update.add_argument("--waiting", type=int)
    p_update.add_argument("--links-file")
    p_position = sub.add_parser("position")
    p_position.add_argument("--run-id", required=True)
    args = parser.parse_args()
    if args.command == "update":
        if not args.pr:
            return 0  # manual runs without a PR have no comment
        update(args)
    else:
        position(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
