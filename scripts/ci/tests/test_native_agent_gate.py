from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import tempfile
import unittest


CI_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CI_DIR))

import native_agent_gate as gate  # noqa: E402


def minimal_manifest() -> dict:
    return {
        "schema_version": 1,
        "required_categories": ["normal-final"],
        "category_required_tests": {"normal-final": ["tests::normal_final: test"]},
        "required_reports": ["linux"],
        "suites": [
            {
                "id": "linux",
                "platforms": ["linux"],
                "cases": [
                    {
                        "id": "agent",
                        "command": ["cargo", "test", "--locked", "-p", "pioneer-agent"],
                        "categories": ["normal-final"],
                    }
                ],
            }
        ],
        "allowed_ignored_tests": [],
    }


def passing_report(sha: str = "a" * 40) -> dict:
    command = ["cargo", "test", "--locked", "-p", "pioneer-agent"]
    successful = {"exit_code": 0, "duration_seconds": 0.1, "output_sha256": "1" * 64}
    return {
        "schema_version": 1,
        "kind": gate.REPORT_KIND,
        "suite_id": "linux",
        "platform": "linux",
        "architecture": "x86_64",
        "commit_sha": sha,
        "expected_sha": sha,
        "clean": True,
        "clean_after": True,
        "dirty_paths": [],
        "dirty_paths_after": [],
        "seed": sha,
        "numeric_seed": gate.deterministic_numeric_seed(sha),
        "started_at": "2026-01-01T00:00:00Z",
        "finished_at": "2026-01-01T00:00:01Z",
        "duration_seconds": 1.0,
        "enumerated_test_count": 1,
        "test_count": 1,
        "outcome": "passed",
        "cases": [
            {
                "id": "agent",
                "categories": ["normal-final"],
                "features": [],
                "enumeration": {
                    **successful,
                    "argv": command + ["--", "--list", "--format", "terse"],
                },
                "ignored_enumeration": {
                    **successful,
                    "argv": command + ["--", "--ignored", "--list", "--format", "terse"],
                },
                "execution": {**successful, "argv": command},
                "enumerated_tests": ["tests::normal_final: test"],
                "executed_tests": ["tests::normal_final: test"],
                "ignored_tests": [],
                "unexpected_ignored": [],
                "stale_ignored_allowances": [],
                "ignored_not_enumerated": [],
                "ambiguous_pinned_test_identities": [],
                "outcome": "passed",
            }
        ],
    }


class ManifestTests(unittest.TestCase):
    def test_minimal_manifest_is_valid(self) -> None:
        gate.validate_manifest(minimal_manifest())

    def test_locked_workspace_command_is_valid(self) -> None:
        manifest = minimal_manifest()
        manifest["suites"][0]["cases"][0]["command"] = [
            "cargo",
            "test",
            "--locked",
            "--workspace",
        ]
        gate.validate_manifest(manifest)

    def test_missing_required_category_is_rejected(self) -> None:
        manifest = minimal_manifest()
        manifest["required_categories"].append("restart-recovery")
        with self.assertRaisesRegex(gate.GateError, "no executable case"):
            gate.validate_manifest(manifest)

    def test_empty_suite_is_rejected(self) -> None:
        manifest = minimal_manifest()
        manifest["suites"][0]["cases"] = []
        with self.assertRaisesRegex(gate.GateError, "has no cases"):
            gate.validate_manifest(manifest)

    def test_non_test_command_is_rejected(self) -> None:
        manifest = minimal_manifest()
        manifest["suites"][0]["cases"][0]["command"] = ["bash", "test.sh"]
        with self.assertRaisesRegex(gate.GateError, "not an explicit cargo test"):
            gate.validate_manifest(manifest)

    def test_unlocked_test_command_is_rejected(self) -> None:
        manifest = minimal_manifest()
        manifest["suites"][0]["cases"][0]["command"].remove("--locked")
        with self.assertRaisesRegex(gate.GateError, "is not locked"):
            gate.validate_manifest(manifest)

    def test_unscoped_test_command_is_rejected(self) -> None:
        manifest = minimal_manifest()
        manifest["suites"][0]["cases"][0]["command"] = [
            "cargo",
            "test",
            "--locked",
        ]
        with self.assertRaisesRegex(gate.GateError, "package or the workspace"):
            gate.validate_manifest(manifest)

    def test_filtered_test_command_is_rejected(self) -> None:
        manifest = minimal_manifest()
        manifest["suites"][0]["cases"][0]["command"].extend(["--", "normal_final"])
        with self.assertRaisesRegex(gate.GateError, "may not filter"):
            gate.validate_manifest(manifest)

    def test_unknown_case_category_is_rejected(self) -> None:
        manifest = minimal_manifest()
        manifest["suites"][0]["cases"][0]["categories"].append("invented")
        with self.assertRaisesRegex(gate.GateError, "unknown categories"):
            gate.validate_manifest(manifest)

    def test_required_test_identity_must_be_exact(self) -> None:
        manifest = minimal_manifest()
        manifest["category_required_tests"]["normal-final"] = ["normal_final"]
        with self.assertRaisesRegex(gate.GateError, "exact required test identities"):
            gate.validate_manifest(manifest)

    def test_unreasoned_ignore_allowance_is_rejected(self) -> None:
        manifest = minimal_manifest()
        manifest["allowed_ignored_tests"] = [
            {"suite": "linux", "case": "agent", "test": "slow", "reason": ""}
        ]
        with self.assertRaisesRegex(gate.GateError, "non-empty reason"):
            gate.validate_manifest(manifest)

    def test_ignore_allowance_requires_exact_test_identity(self) -> None:
        manifest = minimal_manifest()
        manifest["allowed_ignored_tests"] = [
            {
                "suite": "linux",
                "case": "agent",
                "test": "manual_smoke",
                "reason": "manual fixture",
            }
        ]
        with self.assertRaisesRegex(gate.GateError, "exact test identity"):
            gate.validate_manifest(manifest)


class ReportTests(unittest.TestCase):
    def validate(self, report: dict) -> dict:
        sha = "a" * 40
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest_path = root / "manifest.json"
            report_path = root / "reports" / "linux.json"
            output_path = root / "attestation.json"
            gate.write_json(manifest_path, minimal_manifest())
            gate.write_json(report_path, report)
            args = argparse.Namespace(
                manifest=str(manifest_path),
                reports_dir=str(report_path.parent),
                expected_sha=sha,
                output=str(output_path),
            )
            gate.validate_reports(args)
            return json.loads(output_path.read_text(encoding="utf-8"))

    def test_passing_exact_sha_report_produces_attestation(self) -> None:
        attestation = self.validate(passing_report())
        self.assertEqual(attestation["outcome"], "passed")
        self.assertEqual(attestation["enumerated_test_count"], 1)
        self.assertEqual(attestation["test_count"], 1)
        self.assertEqual(attestation["commit_sha"], "a" * 40)
        self.assertEqual(attestation["ignored_test_count"], 0)
        self.assertEqual(attestation["case_counts_by_outcome"], {"passed": 1})
        self.assertEqual(attestation["category_test_counts"], {"normal-final": 1})
        self.assertEqual(
            attestation["required_test_identities"],
            {"normal-final": ["tests::normal_final: test"]},
        )
        self.assertEqual(
            attestation["feature_matrix"],
            [{"suite_id": "linux", "cases": [{"case_id": "agent", "features": []}]}],
        )

    def test_intentionally_failed_case_blocks_attestation(self) -> None:
        report = passing_report()
        report["outcome"] = "failed"
        report["cases"][0]["outcome"] = "failed"
        with self.assertRaisesRegex(gate.GateError, "outcome is not passed"):
            self.validate(report)

    def test_wrong_sha_blocks_attestation(self) -> None:
        report = passing_report("b" * 40)
        with self.assertRaisesRegex(gate.GateError, "exact SHA"):
            self.validate(report)

    def test_dirty_checkout_blocks_attestation(self) -> None:
        report = passing_report()
        report["clean"] = False
        report["dirty_paths"] = [" M crates/agent/src/lib.rs"]
        with self.assertRaisesRegex(gate.GateError, "dirty checkout"):
            self.validate(report)

    def test_duplicate_pinned_workspace_identity_blocks_attestation(self) -> None:
        report = passing_report()
        duplicate = "tests::normal_final: test"
        report["cases"][0]["enumerated_tests"] = [duplicate, duplicate]
        report["cases"][0]["executed_tests"] = [duplicate, duplicate]
        report["cases"][0]["ambiguous_pinned_test_identities"] = [duplicate]
        report["enumerated_test_count"] = 2
        report["test_count"] = 2
        with self.assertRaisesRegex(gate.GateError, "ambiguous pinned"):
            self.validate(report)

    def test_unexpected_ignored_test_blocks_attestation(self) -> None:
        report = passing_report()
        report["cases"][0]["unexpected_ignored"] = ["tests::hidden: test"]
        with self.assertRaisesRegex(gate.GateError, "ignored-test evidence"):
            self.validate(report)

    def test_missing_ignored_test_evidence_blocks_attestation(self) -> None:
        report = passing_report()
        del report["cases"][0]["ignored_not_enumerated"]
        with self.assertRaisesRegex(gate.GateError, "missing ignored-test evidence"):
            self.validate(report)

    def test_inconsistent_clean_paths_block_attestation(self) -> None:
        report = passing_report()
        report["dirty_paths_after"] = ["?? forged"]
        with self.assertRaisesRegex(gate.GateError, "final cleanliness evidence"):
            self.validate(report)

    def test_unattested_ignored_test_blocks_attestation_even_if_summary_is_forged(self) -> None:
        report = passing_report()
        report["cases"][0]["ignored_tests"] = [
            {"name": "tests::hidden: test", "reason": "forged report reason"}
        ]
        with self.assertRaisesRegex(gate.GateError, "does not match manifest"):
            self.validate(report)

    def test_exact_reasoned_ignored_allowance_is_accepted(self) -> None:
        manifest = minimal_manifest()
        manifest["allowed_ignored_tests"] = [
            {
                "suite": "linux",
                "case": "agent",
                "test": "tests::manual_external_smoke: test",
                "reason": "manual smoke requires an external test credential",
            }
        ]
        report = passing_report()
        report["cases"][0]["enumerated_tests"].append(
            "tests::manual_external_smoke: test"
        )
        report["enumerated_test_count"] = 2
        report["cases"][0]["ignored_tests"] = [
            {
                "name": "tests::manual_external_smoke: test",
                "reason": "manual smoke requires an external test credential",
            }
        ]
        sha = "a" * 40
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest_path = root / "manifest.json"
            report_path = root / "reports" / "linux.json"
            output_path = root / "attestation.json"
            gate.write_json(manifest_path, manifest)
            gate.write_json(report_path, report)
            args = argparse.Namespace(
                manifest=str(manifest_path),
                reports_dir=str(report_path.parent),
                expected_sha=sha,
                output=str(output_path),
            )
            gate.validate_reports(args)
            self.assertTrue(output_path.exists())

    def test_empty_executed_test_list_blocks_attestation(self) -> None:
        report = passing_report()
        report["cases"][0]["executed_tests"] = []
        with self.assertRaisesRegex(gate.GateError, "executed-test evidence is incomplete"):
            self.validate(report)

    def test_ignored_required_test_does_not_count_as_executed(self) -> None:
        manifest = minimal_manifest()
        manifest["allowed_ignored_tests"] = [
            {
                "suite": "linux",
                "case": "agent",
                "test": "tests::normal_final: test",
                "reason": "fixture proves ignored tests cannot satisfy release coverage",
            }
        ]
        report = passing_report()
        report["cases"][0]["executed_tests"] = []
        report["cases"][0]["ignored_tests"] = [
            {
                "name": "tests::normal_final: test",
                "reason": "fixture proves ignored tests cannot satisfy release coverage",
            }
        ]
        report["test_count"] = 0
        sha = "a" * 40
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest_path = root / "manifest.json"
            report_path = root / "reports" / "linux.json"
            gate.write_json(manifest_path, manifest)
            gate.write_json(report_path, report)
            args = argparse.Namespace(
                manifest=str(manifest_path),
                reports_dir=str(report_path.parent),
                expected_sha=sha,
                output=str(root / "attestation.json"),
            )
            with self.assertRaisesRegex(gate.GateError, "executed-test evidence is incomplete"):
                gate.validate_reports(args)

    def test_wrong_fault_seed_blocks_attestation(self) -> None:
        report = passing_report()
        report["seed"] = "b" * 40
        with self.assertRaisesRegex(gate.GateError, "fault seed does not match"):
            self.validate(report)

    def test_wrong_numeric_seed_blocks_attestation(self) -> None:
        report = passing_report()
        report["numeric_seed"] = "1234"
        with self.assertRaisesRegex(gate.GateError, "wrong numeric property-test seed"):
            self.validate(report)

    def test_malformed_output_hash_blocks_attestation(self) -> None:
        report = passing_report()
        report["cases"][0]["execution"]["output_sha256"] = "truthy-but-not-a-hash"
        with self.assertRaisesRegex(gate.GateError, "unsuccessful command evidence"):
            self.validate(report)

    def test_declared_category_without_matching_executed_test_blocks_attestation(self) -> None:
        report = passing_report()
        report["cases"][0]["enumerated_tests"] = ["tests::unrelated_case: test"]
        report["cases"][0]["executed_tests"] = ["tests::unrelated_case: test"]
        with self.assertRaisesRegex(gate.GateError, "required category tests were not executed"):
            self.validate(report)

    def test_every_exact_test_declared_for_a_category_is_required(self) -> None:
        manifest = minimal_manifest()
        manifest["category_required_tests"]["normal-final"].append(
            "tests::second_required_regression: test"
        )
        report = passing_report()
        sha = "a" * 40
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest_path = root / "manifest.json"
            report_path = root / "reports" / "linux.json"
            gate.write_json(manifest_path, manifest)
            gate.write_json(report_path, report)
            args = argparse.Namespace(
                manifest=str(manifest_path),
                reports_dir=str(report_path.parent),
                expected_sha=sha,
                output=str(root / "attestation.json"),
            )
            with self.assertRaisesRegex(
                gate.GateError, "tests::second_required_regression: test"
            ):
                gate.validate_reports(args)


if __name__ == "__main__":
    unittest.main()
