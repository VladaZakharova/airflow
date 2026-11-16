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
Print the Cloud Logging record of a PR's security review, and mark the first HIGH risk of the PR.

The HIGH-risk email alert matches only records with ``first_alert: true``. A PR gets the label
``gst-high-risk`` the first time it is HIGH, so later runs of the same PR (re-runs, new pushes) do not
email again. When a later review is no longer HIGH, the label is removed, so a new HIGH emails again.

Usage: security_review_record.py <review result JSON written by review_pr_security.py --record-file>
Environment: GITHUB_TOKEN, GITHUB_REPOSITORY, GITHUB_SERVER_URL, GITHUB_RUN_ID, GITHUB_SHA, GST_PR.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

LABEL = "gst-high-risk"
MAX_FINDINGS_CHARS = 900  # extracted alert labels are limited in size
LEVEL_ORDER = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}


def format_findings(items: list[dict]) -> str:
    """
    Return the findings as a short Markdown list for the email, most severe first.

    Findings with the same level, file and reason are merged into one line with all their line numbers.
    """
    groups: dict[tuple[str, str, str, str], list[str]] = {}
    for item in items:
        path, _, line = item["location"].rpartition(":")
        if not line.isdigit():
            path, line = item["location"], ""
        source = " (AI review)" if item["check"] == "AI review" else ""
        key = (item["level"], os.path.basename(path) or path, item["reason"], source)
        lines = groups.setdefault(key, [])
        if line:
            lines.append(line)
    rows = []
    for (level, file, reason, source), lines in sorted(
        groups.items(), key=lambda g: LEVEL_ORDER.get(g[0][0], 3)
    ):
        where = f"`{file}`" if file not in ("", "-") else "whole PR"
        if lines:
            where += f" line{'s' if len(lines) > 1 else ''} {', '.join(lines)}"
        rows.append(f"- **{level}**: {where}: {reason}{source}")
    text = ""
    for count, row in enumerate(rows):
        more = f"\n- … and {len(rows) - count} more, see the run"
        if len(text) + len(row) + len(more) + 1 > MAX_FINDINGS_CHARS:
            return text + more
        text += ("\n" if text else "") + row
    return text or "none"


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


def is_first_high_risk(pr: str, risk: str) -> bool:
    """Return True only for the first HIGH risk of a PR, and keep the PR's label in sync."""
    if not pr or not os.environ.get("GITHUB_TOKEN"):
        return risk == "HIGH"
    labels = {label["name"] for label in call_github("GET", f"/issues/{pr}/labels")}
    if risk == "HIGH":
        if LABEL in labels:
            return False
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
        return True
    if LABEL in labels:
        call_github("DELETE", f"/issues/{pr}/labels/{LABEL}")
    return False


def main() -> int:
    record = json.load(open(sys.argv[1]))
    env = os.environ
    repo_url = f"{env['GITHUB_SERVER_URL']}/{env['GITHUB_REPOSITORY']}"
    pr = env.get("GST_PR", "")
    items = record.pop("finding_items", [])
    levels = [item["level"] for item in items]
    record.update(
        pr=pr,
        pr_url=f"{repo_url}/pull/{pr}" if pr else "",
        run_id=env["GITHUB_RUN_ID"],
        run_url=f"{repo_url}/actions/runs/{env['GITHUB_RUN_ID']}",
        repository=env["GITHUB_REPOSITORY"],
        commit=env["GITHUB_SHA"],
        finding_counts=", ".join(f"{levels.count(lvl)} {lvl}" for lvl in LEVEL_ORDER if lvl in levels)
        or "none",
        top_findings=format_findings(items),
        ai_summary=(record.get("ai_summary") or "not available")[:MAX_FINDINGS_CHARS],
    )
    try:
        record["first_alert"] = is_first_high_risk(pr, record.get("risk", ""))
    except Exception as e:  # The record must still be written if GitHub is unreachable.
        print(f"Could not check the PR label: {e}", file=sys.stderr)
        record["first_alert"] = record.get("risk") == "HIGH"
    print(json.dumps(record))
    return 0


if __name__ == "__main__":
    sys.exit(main())
