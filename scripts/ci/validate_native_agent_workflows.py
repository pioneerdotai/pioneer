#!/usr/bin/env python3
"""Fail-closed structural checks for native-agent CI/release wiring."""

from __future__ import annotations

import json
from pathlib import Path
import re
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[2]

EXPECTED_CATEGORIES = {
    "normal-final",
    "parallel-tools",
    "local-tool-rejection",
    "provider-failure",
    "provider-conformance",
    "cancellation",
    "restart-recovery",
    "blocked-resume",
    "durable-events",
    "scheduled-parent-child",
    "migration-restart",
    "path-attachments",
    "process-cancellation",
    "long-running-progress",
    "long-running-no-progress",
}

EXPECTED_CATEGORY_TESTS = {
    "normal-final": (
        "manager_tests::turn_completes_when_tool_event_trace_outlives_tool_runtime: test",
        "tests::computer_use_remains_registered_and_reaches_handler_when_visible: test",
    ),
    "parallel-tools": (
        "manager_tests::completed_parallel_tool_result_is_journaled_before_the_batch_finishes: test",
    ),
    "local-tool-rejection": (
        "manager_tests::rejected_capability_emits_event_warning_and_blocks_before_prompt_compile: test",
    ),
    "provider-failure": (
        "manager_tests::provider_failure_checkpoints_window_and_recovery_continues_in_next_window: test",
    ),
    "provider-conformance": (
        "providers::compatible::tests::compatible_stream_completion_captures_absent_reasoning_losslessly: test",
    ),
    "cancellation": ("manager_tests::cancel_turn_emits_interrupted_durable_event: test",),
    "restart-recovery": (
        "manager_tests::execution_window_continuation_restarts_same_turn_and_completes_next_window: test",
    ),
    "blocked-resume": (
        "message::tests::blocked_execution_window_recovery_blocks_child_task_run_without_failure: test",
    ),
    "durable-events": (
        "tests::commit_waiter_completes_only_after_consumer_finishes_event: test",
    ),
    "scheduled-parent-child": ("tests::scheduled_trigger_fires_when_due: test",),
    "migration-restart": (
        "migration_fresh_schema_is_idempotent_across_database_restart: test",
        "migration_previous_release_upgrades_after_database_restart: test",
    ),
    "path-attachments": (
        "context::tests::llm_context_attachment_becomes_structured_tool_message: test",
    ),
    "process-cancellation": (
        "handlers::shell::tests::exec_command_cancellation_terminates_one_shot_process: test",
    ),
    "long-running-progress": (
        "resilience::recovery::tests::virtual_week_with_causal_progress_survives_windows_and_coordinator_restart: test",
    ),
    "long-running-no-progress": (
        "resilience::recovery::tests::persisted_no_progress_windows_trip_recovery_circuit_breaker_after_coordinator_restart: test",
    ),
}


def cargo_test(package: str, *extra: str) -> tuple[str, ...]:
    return ("cargo", "test", "--locked", "-p", package, *extra)


EXPECTED_SUITES = {
    "linux-native-lifecycle": {
        "platforms": ("linux",),
        "cases": {
            "agent": cargo_test("pioneer-agent"),
            "provider": cargo_test("pioneer-provider"),
            "tools": cargo_test("pioneer-tools"),
            "tools-computer-use": cargo_test(
                "pioneer-tools", "--features", "computer-use"
            ),
            "runtime-events": cargo_test("pioneer-runtime-events"),
            "crud": cargo_test("pioneer-crud"),
            "gateway": cargo_test("pioneer-gateway"),
            "tasks": cargo_test("pioneer-tasks"),
            "skills": cargo_test("pioneer-skills"),
            "hooks": cargo_test("pioneer-hooks"),
            "memory": cargo_test("pioneer-memory"),
            "protocol": cargo_test("pioneer-protocol"),
            "migration": cargo_test("pioneer-migration"),
            "sqlite": cargo_test("pioneer-sqlite"),
        },
    },
    "macos-native-runtime": {
        "platforms": ("macos",),
        "cases": {
            "agent": cargo_test("pioneer-agent"),
            "provider": cargo_test("pioneer-provider"),
            "tools": cargo_test("pioneer-tools"),
            "tools-computer-use": cargo_test(
                "pioneer-tools", "--features", "computer-use"
            ),
            "runtime-events": cargo_test("pioneer-runtime-events"),
            "gateway": cargo_test("pioneer-gateway"),
        },
    },
    "windows-native-runtime": {
        "platforms": ("windows",),
        "cases": {
            "agent": cargo_test("pioneer-agent"),
            "provider": cargo_test("pioneer-provider"),
            "tools": cargo_test("pioneer-tools"),
            "tools-computer-use": cargo_test(
                "pioneer-tools", "--features", "computer-use"
            ),
            "runtime-events": cargo_test("pioneer-runtime-events"),
            "gateway": cargo_test("pioneer-gateway"),
        },
    },
}


class ContractError(RuntimeError):
    pass


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def require_line(text: str, line: str, source: str) -> None:
    """Require an exact, correctly indented line; comments cannot satisfy it."""

    if line not in text.splitlines():
        raise ContractError(f"{source} is missing required active line: {line!r}")


def require_fragment(text: str, fragment: str, source: str) -> None:
    """Require an exact active YAML fragment whose ordering is security-relevant."""

    if fragment not in text:
        raise ContractError(f"{source} is missing required active workflow fragment")


def job_block(text: str, job: str, source: str) -> str:
    match = re.search(
        rf"(?ms)^  {re.escape(job)}:\n(.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)",
        text,
    )
    if match is None:
        raise ContractError(f"{source} has no job {job}")
    return match.group(1)


def validate_calling_workflow(
    relative: str,
    *,
    gated_job: str,
    require_ci_triggers: bool = False,
    require_exact_checkout: bool = False,
    require_gate_dependency: bool = True,
    publish_job: str | None = None,
    publish_dependency: str | None = None,
) -> None:
    text = read(relative)
    gate = job_block(text, "native-agent-gate", relative)
    require_line(gate, "    uses: ./.github/workflows/native-agent-gate.yml", relative)
    require_line(gate, "      expected_sha: ${{ github.sha }}", relative)

    gated = job_block(text, gated_job, relative)
    if require_gate_dependency:
        require_line(gated, "    needs: native-agent-gate", relative)
    if require_exact_checkout:
        require_line(gated, "          ref: ${{ github.sha }}", relative)

    if (publish_job is None) != (publish_dependency is None):
        raise ContractError("publish_job and publish_dependency must be configured together")
    if publish_job is not None and publish_dependency is not None:
        publisher = job_block(text, publish_job, relative)
        require_line(publisher, f"    needs: {publish_dependency}", relative)
        if require_exact_checkout:
            require_line(publisher, "          ref: ${{ github.sha }}", relative)

    if require_ci_triggers:
        for trigger in ("  pull_request:", "  merge_group:", "  push:"):
            require_line(text, trigger, relative)
        require_line(text, "      - main", relative)


def validate_release_ci_wait(relative: str, *, allow_non_tag_runs: bool) -> None:
    """Require releases to wait for successful main CI on their exact tag SHA."""

    text = read(relative)
    require_fragment(
        text,
        "permissions:\n  actions: read\n  contents: write",
        relative,
    )
    for trigger_line in ("  push:", "    tags:", '      - "v*"'):
        require_line(text, trigger_line, relative)

    wait_job = job_block(text, "wait-for-ci", relative)
    for line in (
        "    name: wait-for-ci",
        "    runs-on: ubuntu-latest",
        "    timeout-minutes: 155",
        "          CI_WAIT_REPOSITORY: ${{ github.repository }}",
        "          CI_WAIT_SHA: ${{ github.sha }}",
        "          GITHUB_TOKEN: ${{ github.token }}",
        "          python3 scripts/ci/wait_for_ci.py",
        '          --repository "$CI_WAIT_REPOSITORY"',
        "          --workflow ci.yml",
        '          --sha "$CI_WAIT_SHA"',
        "          --branch main",
        "          --event push",
        "          --timeout-seconds 9000",
        "          --poll-seconds 30",
    ):
        require_line(wait_job, line, relative)

    if any(
        line.startswith("    if:") or line.lstrip().startswith("continue-on-error:")
        for line in wait_job.splitlines()
    ):
        raise ContractError(f"{relative} wait-for-ci job may not be skipped or tolerated")

    release_condition = (
        "        if: ${{ github.event_name == 'push' && "
        "startsWith(github.ref, 'refs/tags/v') }}"
    )
    if allow_non_tag_runs:
        expected_step_conditions = [
            "        if: ${{ github.event_name != 'push' || "
            "!startsWith(github.ref, 'refs/tags/v') }}",
            release_condition,
            release_condition,
        ]
        actual_step_conditions = [
            line for line in wait_job.splitlines() if line.startswith("        if:")
        ]
        if actual_step_conditions != expected_step_conditions:
            raise ContractError(f"{relative} has an unsafe non-tag CI-wait condition")
        require_fragment(
            wait_job,
            "      - name: skip-ci-wait-for-non-tag-run\n"
            "        if: ${{ github.event_name != 'push' || "
            "!startsWith(github.ref, 'refs/tags/v') }}\n"
            '        run: echo "Exact-SHA CI wait is required only for v* tag releases."',
            relative,
        )
        require_fragment(
            wait_job,
            "      - uses: actions/checkout@v5\n"
            f"{release_condition}\n"
            "        with:\n"
            "          ref: ${{ github.sha }}\n"
            "          persist-credentials: false",
            relative,
        )
        require_fragment(
            wait_job,
            "      - name: wait-for-successful-exact-sha-ci\n"
            f"{release_condition}\n"
            "        env:",
            relative,
        )
    else:
        if any(line.startswith("        if:") for line in wait_job.splitlines()):
            raise ContractError(f"{relative} tag-only CI wait may not be conditional")
        require_fragment(
            wait_job,
            "      - uses: actions/checkout@v5\n"
            "        with:\n"
            "          ref: ${{ github.sha }}\n"
            "          persist-credentials: false",
            relative,
        )
        require_fragment(
            wait_job,
            "      - name: wait-for-successful-exact-sha-ci\n        env:",
            relative,
        )

    gate = job_block(text, "native-agent-gate", relative)
    require_line(gate, "    needs: wait-for-ci", relative)
    if any(
        line.startswith("    if:") or line.lstrip().startswith("continue-on-error:")
        for line in gate.splitlines()
    ):
        raise ContractError(f"{relative} native-agent gate may not bypass failed CI wait")


def validate_reusable_gate() -> None:
    relative = ".github/workflows/native-agent-gate.yml"
    text = read(relative)
    for line in (
        "  workflow_call:",
        "      expected_sha:",
        "  NATIVE_AGENT_GATE_SHA: ${{ inputs.expected_sha || github.sha }}",
        "  PIONEER_FAULT_SEED: ${{ inputs.expected_sha || github.sha }}",
        "          test \"$(git rev-parse HEAD)\" = \"$NATIVE_AGENT_GATE_SHA\"",
        "          test -z \"$(git status --porcelain)\"",
        "          python scripts/ci/native_agent_gate.py validate-manifest",
        "        run: python -m unittest discover -s scripts/ci/tests -p 'test_*.py' -v",
        "        run: python scripts/ci/validate_native_agent_workflows.py",
        "          --suite linux-native-lifecycle",
        "            suite: macos-native-runtime",
        "            suite: windows-native-runtime",
        "          --expected-sha \"$NATIVE_AGENT_GATE_SHA\"",
        "          --seed \"$PIONEER_FAULT_SEED\"",
        "          --reports-dir native-agent-results",
        "          if-no-files-found: error",
    ):
        require_line(text, line, relative)

    for job in (
        "validate-contract",
        "linux-native-lifecycle",
        "platform-native-runtime",
        "attest-native-agent-gate",
    ):
        require_line(
            job_block(text, job, relative),
            "          ref: ${{ env.NATIVE_AGENT_GATE_SHA }}",
            relative,
        )

    for job in ("linux-native-lifecycle", "platform-native-runtime"):
        require_line(
            job_block(text, job, relative),
            "    needs: validate-contract",
            relative,
        )

    final_job = job_block(text, "attest-native-agent-gate", relative)
    for predecessor in (
        "validate-contract",
        "linux-native-lifecycle",
        "platform-native-runtime",
    ):
        require_line(final_job, f"      - {predecessor}", relative)
    require_line(final_job, "    if: always()", relative)


def object_list_by_id(value: Any, label: str) -> dict[str, dict[str, Any]]:
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ContractError(f"{label} must be a list of objects")
    result: dict[str, dict[str, Any]] = {}
    for item in value:
        item_id = item.get("id")
        if not isinstance(item_id, str) or not item_id or item_id in result:
            raise ContractError(f"{label} has a missing or duplicate id: {item_id!r}")
        result[item_id] = item
    return result


def validate_manifest_alignment() -> None:
    manifest = json.loads(read("ci/native-agent-gate.json"))
    if not isinstance(manifest, dict) or manifest.get("schema_version") != 1:
        raise ContractError("native-agent manifest must use schema version 1")

    required_reports = manifest.get("required_reports")
    expected_suite_ids = set(EXPECTED_SUITES)
    if (
        not isinstance(required_reports, list)
        or len(required_reports) != len(set(required_reports))
        or set(required_reports) != expected_suite_ids
    ):
        raise ContractError("manifest required report set is not exact")

    required_categories = manifest.get("required_categories")
    if (
        not isinstance(required_categories, list)
        or len(required_categories) != len(set(required_categories))
        or set(required_categories) != EXPECTED_CATEGORIES
    ):
        raise ContractError("manifest required category set is not exact")

    actual_category_tests = manifest.get("category_required_tests")
    if not isinstance(actual_category_tests, dict):
        raise ContractError("manifest exact category-test contract is missing")
    normalized_category_tests = {
        category: tuple(tests) if isinstance(tests, list) else ()
        for category, tests in actual_category_tests.items()
    }
    if normalized_category_tests != EXPECTED_CATEGORY_TESTS:
        raise ContractError("manifest exact category-test contract changed")

    suites = object_list_by_id(manifest.get("suites"), "manifest suites")
    if set(suites) != expected_suite_ids:
        raise ContractError(f"manifest suite set is not exact: {sorted(suites)}")

    for suite_id, expected in EXPECTED_SUITES.items():
        suite = suites[suite_id]
        if tuple(suite.get("platforms", ())) != expected["platforms"]:
            raise ContractError(f"manifest platform contract changed for {suite_id}")
        cases = object_list_by_id(suite.get("cases"), f"manifest suite {suite_id} cases")
        expected_cases = expected["cases"]
        if set(cases) != set(expected_cases):
            raise ContractError(f"manifest case set changed for {suite_id}")
        for case_id, expected_command in expected_cases.items():
            if tuple(cases[case_id].get("command", ())) != expected_command:
                raise ContractError(f"manifest command changed for {suite_id}/{case_id}")


def main() -> int:
    try:
        validate_reusable_gate()
        validate_calling_workflow(
            ".github/workflows/ci.yml",
            gated_job="lint-test-build",
            require_ci_triggers=True,
            require_gate_dependency=False,
        )
        validate_calling_workflow(
            ".github/workflows/release-gateway.yml",
            gated_job="build-gateway",
            require_exact_checkout=True,
            publish_job="publish-release",
            publish_dependency="build-gateway",
        )
        validate_release_ci_wait(
            ".github/workflows/release-gateway.yml", allow_non_tag_runs=False
        )
        validate_calling_workflow(
            ".github/workflows/release-app.yml",
            gated_job="package-desktop",
            require_exact_checkout=True,
            publish_job="publish-desktop-release",
            publish_dependency="package-desktop",
        )
        validate_release_ci_wait(
            ".github/workflows/release-app.yml", allow_non_tag_runs=True
        )
        validate_manifest_alignment()
    except (ContractError, OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
        print(f"native-agent workflow contract failed: {exc}", file=sys.stderr)
        return 1
    print("native-agent workflow contract is complete and release-gated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
