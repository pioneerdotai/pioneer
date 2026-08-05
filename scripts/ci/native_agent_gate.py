#!/usr/bin/env python3
"""Run and attest the mandatory native-agent lifecycle release gate.

The runner deliberately uses only Python's standard library so the exact same
contract can run on GitHub's Linux, macOS and Windows hosted runners.  It never
uses a shell to execute manifest commands.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import time
from typing import Any


SCHEMA_VERSION = 1
REPORT_KIND = "native-agent-suite-report"
ATTESTATION_KIND = "native-agent-gate-attestation"
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
GIT_SHA_PATTERN = re.compile(r"[0-9a-f]{40}")


class GateError(RuntimeError):
    pass


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GateError(f"cannot read JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise GateError(f"JSON root must be an object: {path}")
    return value


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def current_platform() -> str:
    value = platform.system().lower()
    if value == "darwin":
        return "macos"
    if value == "windows":
        return "windows"
    if value == "linux":
        return "linux"
    return value


def git_output(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        raise GateError(f"git {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def validate_manifest(manifest: dict[str, Any]) -> None:
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise GateError(f"unsupported manifest schema_version: {manifest.get('schema_version')!r}")

    suites = manifest.get("suites")
    required_reports = manifest.get("required_reports")
    required_categories = manifest.get("required_categories")
    category_required_tests = manifest.get("category_required_tests")
    ignored = manifest.get("allowed_ignored_tests")
    if not isinstance(suites, list) or not suites:
        raise GateError("manifest suites must be a non-empty list")
    if not isinstance(required_reports, list) or not required_reports:
        raise GateError("manifest required_reports must be a non-empty list")
    if not isinstance(required_categories, list) or not required_categories:
        raise GateError("manifest required_categories must be a non-empty list")
    if not isinstance(category_required_tests, dict):
        raise GateError("manifest category_required_tests must be an object")
    if not isinstance(ignored, list):
        raise GateError("manifest allowed_ignored_tests must be a list")

    suite_ids: set[str] = set()
    category_coverage: set[str] = set()
    for suite in suites:
        if not isinstance(suite, dict):
            raise GateError("every suite must be an object")
        suite_id = suite.get("id")
        if not isinstance(suite_id, str) or not suite_id:
            raise GateError("every suite must have a non-empty id")
        if suite_id in suite_ids:
            raise GateError(f"duplicate suite id: {suite_id}")
        suite_ids.add(suite_id)
        platforms = suite.get("platforms")
        if (
            not isinstance(platforms, list)
            or not platforms
            or not all(isinstance(item, str) and item for item in platforms)
            or len(set(platforms)) != len(platforms)
        ):
            raise GateError(f"suite {suite_id} has no valid platforms")
        cases = suite.get("cases")
        if not isinstance(cases, list) or not cases:
            raise GateError(f"suite {suite_id} has no cases")
        case_ids: set[str] = set()
        for case in cases:
            if not isinstance(case, dict):
                raise GateError(f"suite {suite_id} contains a non-object case")
            case_id = case.get("id")
            if not isinstance(case_id, str) or not case_id:
                raise GateError(f"suite {suite_id} contains a case without an id")
            if case_id in case_ids:
                raise GateError(f"duplicate case id {suite_id}/{case_id}")
            case_ids.add(case_id)
            command = case.get("command")
            if not isinstance(command, list) or not command or not all(isinstance(item, str) and item for item in command):
                raise GateError(f"case {suite_id}/{case_id} has an invalid command")
            if command[:2] != ["cargo", "test"]:
                raise GateError(f"case {suite_id}/{case_id} is not an explicit cargo test command")
            if "--locked" not in command:
                raise GateError(f"case {suite_id}/{case_id} is not locked")
            if "--" in command or "--no-run" in command:
                raise GateError(f"case {suite_id}/{case_id} may not filter or skip test execution")
            if command.count("-p") != 1:
                raise GateError(f"case {suite_id}/{case_id} must select exactly one package")
            command_features(command)
            categories = case.get("categories")
            if (
                not isinstance(categories, list)
                or not categories
                or not all(isinstance(item, str) and item for item in categories)
                or len(set(categories)) != len(categories)
            ):
                raise GateError(f"case {suite_id}/{case_id} has no categories")
            category_coverage.update(categories)

    required_set = set(required_reports)
    if len(required_set) != len(required_reports):
        raise GateError("required_reports contains duplicates")
    if required_set != suite_ids:
        missing = sorted(suite_ids - required_set)
        unknown = sorted(required_set - suite_ids)
        raise GateError(f"required_reports must name every suite; missing={missing}, unknown={unknown}")

    category_set = set(required_categories)
    if len(category_set) != len(required_categories):
        raise GateError("required_categories contains duplicates")
    missing_categories = sorted(category_set - category_coverage)
    if missing_categories:
        raise GateError(f"required categories have no executable case: {missing_categories}")
    unknown_categories = sorted(category_coverage - category_set)
    if unknown_categories:
        raise GateError(f"suite cases reference unknown categories: {unknown_categories}")
    if set(category_required_tests) != category_set:
        raise GateError("category_required_tests must define every and only required category")
    for category, tests in category_required_tests.items():
        if (
            not isinstance(tests, list)
            or not tests
            or not all(
                isinstance(item, str)
                and item
                and (item.endswith(": test") or item.endswith(": benchmark"))
                for item in tests
            )
            or len(set(tests)) != len(tests)
        ):
            raise GateError(f"category {category} has no valid exact required test identities")

    case_keys = {
        (suite["id"], case["id"])
        for suite in suites
        for case in suite["cases"]
    }
    ignored_keys: set[tuple[str, str, str]] = set()
    for entry in ignored:
        if not isinstance(entry, dict):
            raise GateError("every ignored-test allowance must be an object")
        key = (entry.get("suite"), entry.get("case"), entry.get("test"))
        reason = entry.get("reason")
        if not all(isinstance(item, str) and item for item in key) or not isinstance(reason, str) or not reason.strip():
            raise GateError("ignored-test allowances require suite, case, test and a non-empty reason")
        if not (key[2].endswith(": test") or key[2].endswith(": benchmark")):
            raise GateError(f"ignored-test allowance must use an exact test identity: {key[2]!r}")
        if key in ignored_keys:
            raise GateError(f"duplicate ignored-test allowance: {key}")
        ignored_keys.add(key)
        if key[0] not in suite_ids:
            raise GateError(f"ignored-test allowance references unknown suite: {key[0]}")
        if (key[0], key[1]) not in case_keys:
            raise GateError(f"ignored-test allowance references unknown case: {key[0]}/{key[1]}")


def find_suite(manifest: dict[str, Any], suite_id: str) -> dict[str, Any]:
    for suite in manifest["suites"]:
        if suite["id"] == suite_id:
            return suite
    raise GateError(f"unknown suite: {suite_id}")


def command_features(command: list[str]) -> list[str]:
    if "--all-features" in command:
        return ["*"]
    if "--features" not in command:
        return []
    index = command.index("--features")
    if index + 1 >= len(command):
        raise GateError("cargo test --features is missing its value")
    features = [
        feature
        for value in command[index + 1].split(",")
        for feature in value.split()
        if feature
    ]
    if not features or len(set(features)) != len(features):
        raise GateError("cargo test --features must name unique non-empty features")
    return sorted(features)


def deterministic_numeric_seed(seed: str) -> str:
    return str(int(hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16], 16))


def run_command(
    argv: list[str], env: dict[str, str], *, collect_tests: bool = False
) -> dict[str, Any]:
    started = time.monotonic()
    digest = hashlib.sha256()
    tests: list[str] = []
    try:
        process = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
        )
    except OSError as exc:
        raw = str(exc).encode("utf-8", errors="replace")
        return {
            "argv": argv,
            "exit_code": 127,
            "duration_seconds": round(time.monotonic() - started, 3),
            "output_sha256": hashlib.sha256(raw).hexdigest(),
            "listed_tests": [],
            "spawn_error": str(exc),
        }
    assert process.stdout is not None
    for raw in iter(process.stdout.readline, b""):
        digest.update(raw)
        decoded = raw.decode("utf-8", errors="replace")
        sys.stdout.write(decoded)
        sys.stdout.flush()
        if collect_tests:
            stripped = decoded.strip()
            if stripped.endswith(": test") or stripped.endswith(": benchmark"):
                tests.append(stripped)
    returncode = process.wait()
    return {
        "argv": argv,
        "exit_code": returncode,
        "duration_seconds": round(time.monotonic() - started, 3),
        "output_sha256": digest.hexdigest(),
        "listed_tests": tests,
    }


def allowed_ignored(manifest: dict[str, Any], suite_id: str, case_id: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for entry in manifest["allowed_ignored_tests"]:
        if entry["suite"] == suite_id and entry["case"] == case_id:
            result[entry["test"]] = entry["reason"]
    return result


def run_suite(args: argparse.Namespace) -> int:
    manifest_path = Path(args.manifest).resolve()
    manifest = load_json(manifest_path)
    validate_manifest(manifest)
    suite = find_suite(manifest, args.suite)
    host_platform = current_platform()
    if host_platform not in suite["platforms"]:
        raise GateError(
            f"suite {args.suite} does not support platform {host_platform}; expected {suite['platforms']}"
        )

    if GIT_SHA_PATTERN.fullmatch(args.expected_sha) is None:
        raise GateError(f"expected SHA is not a full lowercase Git SHA: {args.expected_sha!r}")
    if args.seed != args.expected_sha:
        raise GateError("fault seed must equal the exact commit SHA")
    actual_sha = git_output("rev-parse", "HEAD")
    if actual_sha != args.expected_sha:
        raise GateError(f"exact SHA mismatch: expected {args.expected_sha}, got {actual_sha}")
    dirty_paths = git_output("status", "--porcelain").splitlines()
    if dirty_paths:
        raise GateError(f"source working tree is dirty: {dirty_paths}")

    numeric_seed = deterministic_numeric_seed(args.seed)
    environment = os.environ.copy()
    environment["PIONEER_FAULT_SEED"] = args.seed
    environment["PROPTEST_RNG_SEED"] = numeric_seed
    environment["QUICKCHECK_SEED"] = numeric_seed

    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "kind": REPORT_KIND,
        "suite_id": args.suite,
        "platform": host_platform,
        "architecture": platform.machine(),
        "commit_sha": actual_sha,
        "expected_sha": args.expected_sha,
        "clean": not dirty_paths,
        "dirty_paths": dirty_paths,
        "seed": args.seed,
        "numeric_seed": numeric_seed,
        "started_at": utc_now(),
        "cases": [],
        "outcome": "running",
    }
    failed = False
    started = time.monotonic()

    for case in suite["cases"]:
        case_id = case["id"]
        command = list(case["command"])
        print(f"::group::{args.suite}/{case_id} enumerate")
        enumeration = run_command(
            command + ["--", "--list", "--format", "terse"],
            environment,
            collect_tests=True,
        )
        print("::endgroup::")
        print(f"::group::{args.suite}/{case_id} ignored")
        ignored_run = run_command(
            command + ["--", "--ignored", "--list", "--format", "terse"],
            environment,
            collect_tests=True,
        )
        print("::endgroup::")

        tests = enumeration.pop("listed_tests")
        ignored_tests = ignored_run.pop("listed_tests")
        allowance = allowed_ignored(manifest, args.suite, case_id)
        unexpected_ignored = [name for name in ignored_tests if name not in allowance]
        stale_allowances = [name for name in allowance if name not in ignored_tests]
        ignored_not_enumerated = [name for name in ignored_tests if name not in tests]
        ignored_names = set(ignored_tests)
        executed_tests = [name for name in tests if name not in ignored_names]

        case_report: dict[str, Any] = {
            "id": case_id,
            "categories": case["categories"],
            "features": command_features(command),
            "enumeration": enumeration,
            "ignored_enumeration": ignored_run,
            "enumerated_tests": tests,
            "executed_tests": executed_tests,
            "ignored_tests": [
                {"name": name, "reason": allowance.get(name)} for name in ignored_tests
            ],
            "unexpected_ignored": unexpected_ignored,
            "stale_ignored_allowances": stale_allowances,
            "ignored_not_enumerated": ignored_not_enumerated,
            "outcome": "pending",
        }

        preflight_ok = (
            enumeration["exit_code"] == 0
            and ignored_run["exit_code"] == 0
            and bool(executed_tests)
            and not unexpected_ignored
            and not stale_allowances
            and not ignored_not_enumerated
        )
        if preflight_ok:
            print(f"::group::{args.suite}/{case_id} execute")
            execution = run_command(command, environment)
            print("::endgroup::")
            execution.pop("listed_tests")
            case_report["execution"] = execution
            case_report["outcome"] = "passed" if execution["exit_code"] == 0 else "failed"
        else:
            case_report["execution"] = None
            case_report["outcome"] = "failed-preflight"

        if case_report["outcome"] != "passed":
            failed = True
        report["cases"].append(case_report)
        write_json(Path(args.output), report)

    report["duration_seconds"] = round(time.monotonic() - started, 3)
    report["finished_at"] = utc_now()
    dirty_after = git_output("status", "--porcelain").splitlines()
    report["clean_after"] = not dirty_after
    report["dirty_paths_after"] = dirty_after
    if dirty_after:
        failed = True
    report["outcome"] = "failed" if failed else "passed"
    report["enumerated_test_count"] = sum(
        len(case["enumerated_tests"]) for case in report["cases"]
    )
    report["test_count"] = sum(len(case["executed_tests"]) for case in report["cases"])
    write_json(Path(args.output), report)
    return 1 if failed else 0


def report_paths(directory: Path) -> list[Path]:
    return sorted(path for path in directory.rglob("*.json") if path.is_file())


def validate_reports(args: argparse.Namespace) -> int:
    manifest = load_json(Path(args.manifest))
    validate_manifest(manifest)
    if GIT_SHA_PATTERN.fullmatch(args.expected_sha) is None:
        raise GateError(f"expected SHA is not a full lowercase Git SHA: {args.expected_sha!r}")
    reports: dict[str, tuple[Path, dict[str, Any]]] = {}
    for path in report_paths(Path(args.reports_dir)):
        value = load_json(path)
        if value.get("kind") != REPORT_KIND:
            continue
        suite_id = value.get("suite_id")
        if not isinstance(suite_id, str):
            raise GateError(f"report has no suite_id: {path}")
        if suite_id in reports:
            raise GateError(f"duplicate report for suite {suite_id}: {reports[suite_id][0]} and {path}")
        reports[suite_id] = (path, value)

    required = set(manifest["required_reports"])
    present = set(reports)
    if required != present:
        raise GateError(
            f"report set mismatch: missing={sorted(required - present)}, unknown={sorted(present - required)}"
        )

    merged: list[dict[str, Any]] = []
    seeds: set[str] = set()
    numeric_seeds: set[str] = set()
    categories: set[str] = set()
    tests_by_category: dict[str, list[str]] = {}
    total_tests = 0
    total_enumerated_tests = 0
    total_ignored_tests = 0
    total_duration = 0.0
    case_counts_by_outcome: dict[str, int] = {}
    for suite_id in manifest["required_reports"]:
        path, report = reports[suite_id]
        suite_definition = find_suite(manifest, suite_id)
        if report.get("schema_version") != SCHEMA_VERSION:
            raise GateError(f"report {suite_id} has unsupported schema")
        if report.get("commit_sha") != args.expected_sha or report.get("expected_sha") != args.expected_sha:
            raise GateError(f"report {suite_id} does not attest exact SHA {args.expected_sha}")
        if report.get("outcome") != "passed":
            raise GateError(f"report {suite_id} outcome is not passed")
        if report.get("clean") is not True:
            raise GateError(f"report {suite_id} was produced from a dirty checkout")
        if report.get("dirty_paths") != []:
            raise GateError(f"report {suite_id} has inconsistent initial cleanliness evidence")
        if report.get("clean_after") is not True:
            raise GateError(f"report {suite_id} left tracked checkout changes")
        if report.get("dirty_paths_after") != []:
            raise GateError(f"report {suite_id} has inconsistent final cleanliness evidence")
        if report.get("platform") not in suite_definition["platforms"]:
            raise GateError(f"report {suite_id} used an undeclared platform")
        cases = report.get("cases")
        if not isinstance(cases, list) or not cases:
            raise GateError(f"report {suite_id} has no cases")
        expected_cases = {case["id"]: case for case in suite_definition["cases"]}
        actual_cases = {case.get("id"): case for case in cases if isinstance(case, dict)}
        if len(actual_cases) != len(cases) or set(actual_cases) != set(expected_cases):
            raise GateError(
                f"report {suite_id} case set mismatch: expected={sorted(expected_cases)}, "
                f"actual={sorted(str(key) for key in actual_cases)}"
            )
        suite_test_count = 0
        for case_id, case in actual_cases.items():
            definition = expected_cases[case_id]
            if case.get("outcome") != "passed":
                raise GateError(f"report {suite_id} has non-passing case {case.get('id')}")
            outcome = str(case["outcome"])
            case_counts_by_outcome[outcome] = case_counts_by_outcome.get(outcome, 0) + 1
            if case.get("categories") != definition["categories"]:
                raise GateError(f"report {suite_id}/{case_id} category contract changed")
            tests = case.get("enumerated_tests")
            if (
                not isinstance(tests, list)
                or not tests
                or not all(
                    isinstance(test, str)
                    and (test.endswith(": test") or test.endswith(": benchmark"))
                    for test in tests
                )
            ):
                raise GateError(f"report {suite_id}/{case.get('id')} enumerated no tests")
            for evidence_field in (
                "unexpected_ignored",
                "stale_ignored_allowances",
                "ignored_not_enumerated",
            ):
                if case.get(evidence_field) != []:
                    raise GateError(
                        f"report {suite_id}/{case.get('id')} has unresolved or missing "
                        f"ignored-test evidence: {evidence_field}"
                    )
            ignored_tests = case.get("ignored_tests")
            if not isinstance(ignored_tests, list) or not all(
                isinstance(item, dict)
                and isinstance(item.get("name"), str)
                and isinstance(item.get("reason"), str)
                for item in ignored_tests
            ):
                raise GateError(f"report {suite_id}/{case_id} has invalid ignored-test evidence")
            expected_ignored = allowed_ignored(manifest, suite_id, case_id)
            actual_ignored = {
                item["name"]: item["reason"]
                for item in ignored_tests
            }
            total_ignored_tests += len(actual_ignored)
            if len(actual_ignored) != len(ignored_tests) or actual_ignored != expected_ignored:
                raise GateError(
                    f"report {suite_id}/{case_id} ignored-test evidence does not match manifest"
                )
            ignored_names = set(actual_ignored)
            if not ignored_names.issubset(tests):
                raise GateError(
                    f"report {suite_id}/{case_id} ignored tests were not enumerated"
                )
            expected_executed_tests = [name for name in tests if name not in ignored_names]
            executed_tests = case.get("executed_tests")
            if executed_tests != expected_executed_tests or not expected_executed_tests:
                raise GateError(
                    f"report {suite_id}/{case_id} executed-test evidence is incomplete"
                )
            expected_features = command_features(definition["command"])
            if case.get("features") != expected_features:
                raise GateError(f"report {suite_id}/{case_id} feature profile changed")
            enumeration = case.get("enumeration")
            ignored_enumeration = case.get("ignored_enumeration")
            execution = case.get("execution")
            if not all(isinstance(item, dict) for item in (enumeration, ignored_enumeration, execution)):
                raise GateError(f"report {suite_id}/{case_id} is missing command evidence")
            command = definition["command"]
            if enumeration.get("argv") != command + ["--", "--list", "--format", "terse"]:
                raise GateError(f"report {suite_id}/{case_id} enumeration command changed")
            if ignored_enumeration.get("argv") != command + [
                "--",
                "--ignored",
                "--list",
                "--format",
                "terse",
            ]:
                raise GateError(f"report {suite_id}/{case_id} ignored command changed")
            if execution.get("argv") != command:
                raise GateError(f"report {suite_id}/{case_id} execution command changed")
            if any(
                item.get("exit_code") != 0
                or not isinstance(item.get("output_sha256"), str)
                or SHA256_PATTERN.fullmatch(item["output_sha256"]) is None
                for item in (enumeration, ignored_enumeration, execution)
            ):
                raise GateError(f"report {suite_id}/{case_id} has unsuccessful command evidence")
            categories.update(case.get("categories", []))
            for category in case.get("categories", []):
                tests_by_category.setdefault(category, []).extend(expected_executed_tests)
            suite_test_count += len(expected_executed_tests)
            total_enumerated_tests += len(tests)
        if report.get("test_count") != suite_test_count:
            raise GateError(f"report {suite_id} test_count does not match executed tests")
        suite_enumerated_test_count = sum(len(case["enumerated_tests"]) for case in cases)
        if report.get("enumerated_test_count") != suite_enumerated_test_count:
            raise GateError(
                f"report {suite_id} enumerated_test_count does not match enumerated tests"
            )
        total_tests += suite_test_count
        seed = report.get("seed")
        if seed != args.expected_sha:
            raise GateError(f"report {suite_id} fault seed does not match exact SHA")
        seeds.add(seed)
        numeric_seed = report.get("numeric_seed")
        if numeric_seed != deterministic_numeric_seed(args.expected_sha):
            raise GateError(f"report {suite_id} has the wrong numeric property-test seed")
        numeric_seeds.add(numeric_seed)
        total_duration += float(report.get("duration_seconds", 0.0))
        merged.append(
            {
                "suite_id": suite_id,
                "platform": report.get("platform"),
                "report_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "test_count": report.get("test_count"),
                "duration_seconds": report.get("duration_seconds"),
            }
        )

    if len(seeds) != 1:
        raise GateError(f"reports do not share one fault seed: {sorted(seeds)}")
    if len(numeric_seeds) != 1:
        raise GateError(f"reports do not share one numeric seed: {sorted(numeric_seeds)}")
    missing_categories = sorted(set(manifest["required_categories"]) - categories)
    if missing_categories:
        raise GateError(f"reports did not execute required categories: {missing_categories}")
    uncovered: dict[str, list[str]] = {}
    for category, required_tests in manifest["category_required_tests"].items():
        candidates = set(tests_by_category.get(category, []))
        missing_tests = [test for test in required_tests if test not in candidates]
        if missing_tests:
            uncovered[category] = missing_tests
    if uncovered:
        raise GateError(f"required category tests were not executed: {uncovered}")

    category_test_counts = {
        category: len(set(tests_by_category.get(category, [])))
        for category in manifest["required_categories"]
    }

    attestation = {
        "schema_version": SCHEMA_VERSION,
        "kind": ATTESTATION_KIND,
        "commit_sha": args.expected_sha,
        "seed": next(iter(seeds)),
        "numeric_seed": next(iter(numeric_seeds)),
        "generated_at": utc_now(),
        "outcome": "passed",
        "suite_count": len(merged),
        "enumerated_test_count": total_enumerated_tests,
        "test_count": total_tests,
        "ignored_test_count": total_ignored_tests,
        "case_counts_by_outcome": case_counts_by_outcome,
        "category_test_counts": category_test_counts,
        "required_test_identities": manifest["category_required_tests"],
        "duration_seconds": round(total_duration, 3),
        "required_categories": manifest["required_categories"],
        "feature_matrix": [
            {
                "suite_id": suite["id"],
                "cases": [
                    {
                        "case_id": case["id"],
                        "features": command_features(case["command"]),
                    }
                    for case in suite["cases"]
                ],
            }
            for suite in manifest["suites"]
        ],
        "reports": merged,
    }
    write_json(Path(args.output), attestation)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with Path(summary_path).open("a", encoding="utf-8") as summary:
            summary.write("## Native-agent lifecycle gate\n\n")
            summary.write(f"- Commit: `{args.expected_sha}`\n")
            summary.write(f"- Fault seed: `{attestation['seed']}`\n")
            summary.write(f"- Suites: {attestation['suite_count']}\n")
            summary.write(f"- Enumerated tests: {attestation['enumerated_test_count']}\n")
            summary.write(f"- Executed tests: {attestation['test_count']}\n")
            summary.write(f"- Explicitly allowlisted manual tests: {attestation['ignored_test_count']}\n")
            summary.write("- Outcome: passed\n")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate-manifest")
    validate.add_argument("--manifest", required=True)

    run = subparsers.add_parser("run")
    run.add_argument("--manifest", required=True)
    run.add_argument("--suite", required=True)
    run.add_argument("--expected-sha", required=True)
    run.add_argument("--seed", required=True)
    run.add_argument("--output", required=True)

    aggregate = subparsers.add_parser("validate-reports")
    aggregate.add_argument("--manifest", required=True)
    aggregate.add_argument("--reports-dir", required=True)
    aggregate.add_argument("--expected-sha", required=True)
    aggregate.add_argument("--output", required=True)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        if args.command == "validate-manifest":
            manifest = load_json(Path(args.manifest))
            validate_manifest(manifest)
            return 0
        if args.command == "run":
            return run_suite(args)
        if args.command == "validate-reports":
            return validate_reports(args)
        raise GateError(f"unknown command: {args.command}")
    except GateError as exc:
        print(f"native-agent gate failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
