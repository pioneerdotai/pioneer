from __future__ import annotations

import json
from pathlib import Path
from unittest import mock
import sys
import tempfile
import unittest


CI_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CI_DIR))

import validate_native_agent_workflows as workflows  # noqa: E402


class WorkflowContractTests(unittest.TestCase):
    def test_repository_workflows_are_release_gated(self) -> None:
        workflows.validate_reusable_gate()
        workflows.validate_calling_workflow(
            ".github/workflows/ci.yml",
            gated_job="lint-test-build",
            require_ci_triggers=True,
            require_gate_dependency=False,
        )
        workflows.validate_calling_workflow(
            ".github/workflows/release-gateway.yml",
            gated_job="build-gateway",
            require_exact_checkout=True,
            publish_job="publish-release",
            publish_dependency="build-gateway",
        )
        workflows.validate_calling_workflow(
            ".github/workflows/release-app.yml",
            gated_job="package-desktop",
            require_exact_checkout=True,
            publish_job="publish-desktop-release",
            publish_dependency="package-desktop",
        )
        workflows.validate_manifest_alignment()

    def test_pre_fix_manual_only_ci_fixture_is_rejected(self) -> None:
        baseline = """name: CI

on:
  workflow_dispatch:

jobs:
  lint-test-build:
    runs-on: ubuntu-latest
    steps:
      - run: cargo test -p pioneer-cli
"""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / ".github/workflows/ci.yml"
            path.parent.mkdir(parents=True)
            path.write_text(baseline, encoding="utf-8")
            with mock.patch.object(workflows, "ROOT", root):
                with self.assertRaisesRegex(workflows.ContractError, "no job native-agent-gate"):
                    workflows.validate_calling_workflow(
                        ".github/workflows/ci.yml",
                        gated_job="lint-test-build",
                        require_ci_triggers=True,
                    )

    def test_release_build_without_needs_is_rejected(self) -> None:
        broken = """name: Release

jobs:
  native-agent-gate:
    uses: ./.github/workflows/native-agent-gate.yml
    with:
      expected_sha: ${{ github.sha }}
  build-gateway:
    runs-on: ubuntu-latest
"""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / ".github/workflows/release.yml"
            path.parent.mkdir(parents=True)
            path.write_text(broken, encoding="utf-8")
            with mock.patch.object(workflows, "ROOT", root):
                with self.assertRaisesRegex(workflows.ContractError, "needs: native-agent-gate"):
                    workflows.validate_calling_workflow(
                        ".github/workflows/release.yml", gated_job="build-gateway"
                    )

    def test_commented_gate_dependency_does_not_satisfy_contract(self) -> None:
        broken = """name: Release

jobs:
  native-agent-gate:
    uses: ./.github/workflows/native-agent-gate.yml
    with:
      expected_sha: ${{ github.sha }}
  build-gateway:
    # needs: native-agent-gate
    runs-on: ubuntu-latest
"""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / ".github/workflows/release.yml"
            path.parent.mkdir(parents=True)
            path.write_text(broken, encoding="utf-8")
            with mock.patch.object(workflows, "ROOT", root):
                with self.assertRaisesRegex(workflows.ContractError, "required active line"):
                    workflows.validate_calling_workflow(
                        ".github/workflows/release.yml", gated_job="build-gateway"
                    )

    def test_publisher_cannot_bypass_gated_build(self) -> None:
        broken = """name: Release

jobs:
  native-agent-gate:
    uses: ./.github/workflows/native-agent-gate.yml
    with:
      expected_sha: ${{ github.sha }}
  build-gateway:
    needs: native-agent-gate
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
        with:
          ref: ${{ github.sha }}
  publish-release:
    needs: native-agent-gate
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
        with:
          ref: ${{ github.sha }}
"""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / ".github/workflows/release.yml"
            path.parent.mkdir(parents=True)
            path.write_text(broken, encoding="utf-8")
            with mock.patch.object(workflows, "ROOT", root):
                with self.assertRaisesRegex(workflows.ContractError, "needs: build-gateway"):
                    workflows.validate_calling_workflow(
                        ".github/workflows/release.yml",
                        gated_job="build-gateway",
                        require_exact_checkout=True,
                        publish_job="publish-release",
                        publish_dependency="build-gateway",
                    )

    def test_manifest_cannot_drop_feature_profile_case(self) -> None:
        manifest = json.loads(
            (workflows.ROOT / "ci/native-agent-gate.json").read_text(encoding="utf-8")
        )
        manifest["suites"][0]["cases"] = [
            case
            for case in manifest["suites"][0]["cases"]
            if case["id"] != "tools-computer-use"
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "ci/native-agent-gate.json"
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(manifest), encoding="utf-8")
            with mock.patch.object(workflows, "ROOT", root):
                with self.assertRaisesRegex(workflows.ContractError, "case set changed"):
                    workflows.validate_manifest_alignment()

    def test_manifest_cannot_replace_required_regression_identity(self) -> None:
        manifest = json.loads(
            (workflows.ROOT / "ci/native-agent-gate.json").read_text(encoding="utf-8")
        )
        manifest["category_required_tests"]["normal-final"] = [
            "tests::unrelated_green_test: test"
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "ci/native-agent-gate.json"
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(manifest), encoding="utf-8")
            with mock.patch.object(workflows, "ROOT", root):
                with self.assertRaisesRegex(workflows.ContractError, "category-test contract changed"):
                    workflows.validate_manifest_alignment()


if __name__ == "__main__":
    unittest.main()
