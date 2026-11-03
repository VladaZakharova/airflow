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
Find the Google system tests related to the files changed between two commits.

A system test is related to a changed provider module when it imports that module. When nothing
imports the changed modules directly, modules importing them are followed (hook -> operator -> test).

Usage::

    find_google_system_tests.py --base <commit> --head <commit> [--max-tests 10]

Prints a JSON list of system test paths (relative to the repository root).
"""

from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
PROVIDER_ROOT = ROOT / "providers" / "google"
SRC_ROOT = PROVIDER_ROOT / "src"
SYSTEM_TESTS_ROOT = PROVIDER_ROOT / "tests" / "system"
IMPORT_DEPTH = 2


def get_module_name(path: Path) -> str:
    return ".".join(path.relative_to(SRC_ROOT).with_suffix("").parts)


def get_imported_modules(path: Path) -> set[str]:
    modules = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
        elif isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
    return modules


def build_import_index() -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """Return (module -> provider modules importing it, module -> system tests importing it)."""
    importers: dict[str, set[str]] = defaultdict(set)
    for path in SRC_ROOT.rglob("*.py"):
        for imported in get_imported_modules(path):
            importers[imported].add(get_module_name(path))
    system_tests: dict[str, set[str]] = defaultdict(set)
    for path in SYSTEM_TESTS_ROOT.rglob("example_*.py"):
        for imported in get_imported_modules(path):
            system_tests[imported].add(str(path.relative_to(ROOT)))
    return importers, system_tests


def find_tests_for_file(changed_file: str, importers, system_tests, depth: int) -> set[str]:
    """Return system tests related to one changed file, looking at most ``depth`` import levels away."""
    path = ROOT / changed_file
    if not path.exists() or path.suffix != ".py":
        return set()
    if path.is_relative_to(SYSTEM_TESTS_ROOT):
        if path.name.startswith("example_"):
            return {changed_file}
        # A changed helper next to system tests: run the tests in the same folder.
        return {str(p.relative_to(ROOT)) for p in path.parent.glob("example_*.py")}
    if not path.is_relative_to(SRC_ROOT):
        return set()
    modules = {get_module_name(path)}
    seen = set(modules)
    for _ in range(depth + 1):
        # Stop at the closest level that has tests.
        found = {test for module in modules for test in system_tests.get(module, set())}
        if found:
            return found
        modules = {importer for module in modules for importer in importers.get(module, set())} - seen
        seen |= modules
    return set()


def find_tests(changed_files: list[str], importers, system_tests) -> set[str]:
    """
    Select the system tests for a change.

    First only direct matches: changed system tests and tests importing a changed module. Indirect
    matches (via operators importing a changed hook or trigger) are used only when there are no
    direct ones, because shared modules are imported by dozens of operators.
    """
    direct = set()
    for changed_file in changed_files:
        direct |= find_tests_for_file(changed_file, importers, system_tests, depth=0)
    if direct:
        return direct
    indirect = set()
    for changed_file in changed_files:
        indirect |= find_tests_for_file(changed_file, importers, system_tests, depth=IMPORT_DEPTH)
    return indirect


def get_changed_files(base: str, head: str) -> list[str]:
    output = subprocess.check_output(["git", "diff", "--name-only", f"{base}...{head}"], cwd=ROOT, text=True)
    return [line for line in output.splitlines() if line.startswith("providers/google/")]


def is_manual_only(test_file: str) -> bool:
    """Tests skipped by pytest unless RUN_MANUAL_GOOGLE_SYSTEM_TESTS is set, e.g. because they run for hours."""
    return "RUN_MANUAL_GOOGLE_SYSTEM_TESTS" in (ROOT / test_file).read_text()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--base", required=True, help="Base commit, for example the merge base with main")
    parser.add_argument("--head", default="HEAD", help="Head commit")
    parser.add_argument("--max-tests", type=int, default=10, help="Fail if more tests are selected")
    args = parser.parse_args()

    importers, system_tests = build_import_index()
    tests = find_tests(get_changed_files(args.base, args.head), importers, system_tests)
    manual_only = sorted(test for test in tests if is_manual_only(test))
    if manual_only:
        print(
            "Skipping manual-only system tests (run them by naming them in a manual workflow run): "
            + " ".join(manual_only),
            file=sys.stderr,
        )
        tests -= set(manual_only)

    if len(tests) > args.max_tests:
        print(
            f"{len(tests)} system tests are related to this change (limit {args.max_tests}). "
            "The change probably touches shared code; run selected tests manually instead.",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(sorted(tests)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
