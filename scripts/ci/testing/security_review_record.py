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
Print the Cloud Logging record of a PR's security review, and decide whether it sends the HIGH-risk email.

The HIGH-risk email alert matches only records with ``send_alert: true``: every new run that reviews a
HIGH-risk commit (the PR was opened, or new code was pushed after an earlier approval) sends one email.
A re-run of the same run reviews the same commit again and does not email again. The PR label
``gst-high-risk`` shows the latest result: it is set while the PR is HIGH and removed when it is not.

Usage: security_review_record.py <review result JSON written by review_pr_security.py --record-file>
Environment: GITHUB_TOKEN, GITHUB_REPOSITORY, GITHUB_SERVER_URL, GITHUB_RUN_ID, GITHUB_RUN_ATTEMPT,
GITHUB_SHA, HEAD_SHA (the PR's head commit), GST_PR.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

LABEL = "gst-high-risk"
MAX_FINDINGS = 6  # each finding is one label of the alert email
MAX_FINDING_CHARS = 300
MAX_SUMMARY_CHARS = 900  # extracted alert labels are limited in size
LEVEL_ORDER = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}


def format_findings(items: list[dict]) -> list[str]:
    """
    Return one short line per finding, most severe first, at most ``MAX_FINDINGS`` lines.

    The email shows each line as its own alert label, because labels are shown on one line each. Findings
    with the same level, file and reason are merged into one line with all their line numbers.
    """
    groups: dict[tuple[str, str, str, str], list[str]] = {}
    for item in items:
        if item["location"] == "-":  # the overall AI verdict; its text is in ai_summary
            continue
        path, _, line = item["location"].rpartition(":")
        if not line.isdigit():
            path, line = item["location"], ""
        source = "AI review" if item["check"] == "AI review" else os.path.basename(path)
        lines = groups.setdefault((item["level"], source, item["reason"], path), [])
        if line:
            lines.append(line)
    rows = []
    for (level, source, reason, _), lines in sorted(
        groups.items(), key=lambda g: LEVEL_ORDER.get(g[0][0], 3)
    ):
        where = f"{source}, line{'s' if len(lines) > 1 else ''} {', '.join(lines)}" if lines else source
        rows.append(f"{level} | {where} | {reason}"[:MAX_FINDING_CHARS])
    if len(rows) > MAX_FINDINGS:
        rows = rows[: MAX_FINDINGS - 1] + [f"... and {len(rows) - MAX_FINDINGS + 1} more, see review_run"]
    return rows


def call_github(method: str, path: str, body: dict | None = None):
    request = urllib.request.Request(
        f"https://api.github.com/repos/{os.environ['GITHUB_REPOSITORY']}{path}",
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


def sync_label(pr: str, risk: str) -> None:
    """Show the latest review result on the PR: the label ``gst-high-risk`` is set only while it is HIGH."""
    if not pr or not os.environ.get("GITHUB_TOKEN"):
        return
    labels = {label["name"] for label in call_github("GET", f"/issues/{pr}/labels")}
    if risk == "HIGH" and LABEL not in labels:
        try:
            call_github(
                "POST",
                "/labels",
                {
                    "name": LABEL,
                    "color": "b60205",
                    "description": "Google system tests security review: HIGH risk",
                },
            )
        except urllib.error.HTTPError as e:
            if e.code != 422:  # 422: the label already exists in the repository
                raise
        call_github("POST", f"/issues/{pr}/labels", {"labels": [LABEL]})
    elif risk != "HIGH" and LABEL in labels:
        call_github("DELETE", f"/issues/{pr}/labels/{LABEL}")


def main() -> int:
    record = json.load(open(sys.argv[1]))
    env = os.environ
    repo_url = f"{env['GITHUB_SERVER_URL']}/{env['GITHUB_REPOSITORY']}"
    pr = env.get("GST_PR", "")
    head_sha = env.get("HEAD_SHA") or env["GITHUB_SHA"]
    items = record.pop("finding_items", [])
    levels = [item["level"] for item in items if item["location"] != "-"]
    record.update(
        pr=pr,
        pr_url=f"{repo_url}/pull/{pr}" if pr else "",
        run_id=env["GITHUB_RUN_ID"],
        run_url=f"{repo_url}/actions/runs/{env['GITHUB_RUN_ID']}",
        repository=env["GITHUB_REPOSITORY"],
        commit=head_sha,
        commit_short=head_sha[:10],
        commit_url=f"{repo_url}/pull/{pr}/commits/{head_sha}" if pr else f"{repo_url}/commit/{head_sha}",
        finding_counts=", ".join(f"{levels.count(lvl)} {lvl}" for lvl in LEVEL_ORDER if lvl in levels)
        or "none",
        ai_summary=(record.get("ai_summary") or "not available")[:MAX_SUMMARY_CHARS],
        # A re-run of the same run reviews the same commit again, so it must not email again.
        send_alert=record.get("risk") == "HIGH" and env.get("GITHUB_RUN_ATTEMPT", "1") == "1",
    )
    for number, row in enumerate(format_findings(items), start=1):
        record[f"finding_{number}"] = row
    try:
        sync_label(pr, record.get("risk", ""))
    except Exception as e:  # The record must still be written if GitHub is unreachable.
        print(f"Could not update the PR label: {e}", file=sys.stderr)
    print(json.dumps(record))
    return 0


if __name__ == "__main__":
    sys.exit(main())
