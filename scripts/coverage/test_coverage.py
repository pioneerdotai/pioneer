"""Infrastructure regression tests. Do not run before external acceptance."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from coverage import (POLICY, inventory, junit_cases, measured_configuration, merge_shards,
                      read_lcov, relative_sources, sha, totals, workspace_packages, write_lcov)
from rust_sources import CoverageError, RustSources


LINUX_CFG = ['target_arch="x86_64"', 'target_os="linux"', "unix"]


def configuration(variants=None, member="crates/sample"):
    return {"target_cfg": LINUX_CFG, "package_features": {member: [[]] if variants is None else variants}}


class ReportTests(unittest.TestCase):
    def make_shards(self, root, zero=False):
        identity = {"checkout_sha": "fixture-sha", "run_id": "fixture-run", "run_attempt": "2"}
        common = {"ci": identity, "schema_version": POLICY["schema_version"],
                  "policy_version": POLICY["policy_version"], "policy_fingerprint": "fixture-policy",
                  "measurement": POLICY["measurement"], "metric": POLICY["metric"],
                  "platform": {"os": "linux", "arch": "x86_64", "target": POLICY["target"]},
                  "tools": {"rustc": f"release: {POLICY['rust']}\nhost: {POLICY['target']}",
                            "nextest": POLICY["nextest"], "cargo_llvm_cov": POLICY["cargo_llvm_cov"]},
                  "build_environment": {}, "manifest_hashes": {}, "packages": {"crates/sample": "sample"},
                  "source_roots": ["crates/sample/src/lib.rs"], "measurement_paths": ["crates/sample/src/lib.rs"],
                  "configuration": configuration(),
                  "source_inventory_sha256": "fixture-source-inventory"}
        for n in range(1, 5):
            folder = root / f"shard-{n}"
            folder.mkdir()
            # Shared zero-hit line, shared covered line, and a distinct line per shard.
            write_lcov(folder / "shard.lcov", {"crates/example/src/lib.rs":
                       {1: 0, 2: 0 if zero else n, 10 + n: 0}})
            junit = (f'<testsuites tests="1" failures="0" errors="0"><testsuite name="binary" '
                     f'tests="1" failures="0" errors="0"><testcase name="test_{n}"/>'
                     '</testsuite></testsuites>').encode()
            (folder / "junit.xml").write_bytes(junit)
            data = {"common": common, "shard": n, "partition": f"hash:{n}/4", "tests_executed": 1,
                    "checks": {"nextest": "success", "fmt": "success" if n == 1 else "skipped",
                               "doctests": "success" if n == 1 else "skipped"},
                    "lcov_sha256": sha((folder / "shard.lcov").read_bytes()), "junit_sha256": sha(junit)}
            (folder / "metadata.json").write_text(json.dumps(data))
        return identity

    def test_union_preserves_zero_lines_and_sums_overlap(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            identity = self.make_shards(root)
            files, _, _, count = merge_shards(root, identity, "fixture-policy")
            lines = files["crates/example/src/lib.rs"]
            self.assertEqual(lines, {1: 0, 2: 10, 11: 0, 12: 0, 13: 0, 14: 0})
            self.assertEqual(count, 4)
            self.assertEqual(totals(files)["covered_lines"], 1)
            self.assertEqual(totals(files)["measurable_lines"], 6)

    def test_measured_zero_is_not_missing_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            identity = self.make_shards(root, zero=True)
            files, _, _, _ = merge_shards(root, identity, "fixture-policy")
            self.assertEqual(totals(files)["percent"], 0)
        self.assertIsNone(totals({})["percent"])
        with self.assertRaises(CoverageError):
            read_lcov("")

    def test_missing_shard_is_fatal(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            identity = self.make_shards(root)
            for path in (root / "shard-4").iterdir():
                path.unlink()
            (root / "shard-4").rmdir()
            with self.assertRaisesRegex(CoverageError, "exactly four"):
                merge_shards(root, identity, "fixture-policy")

    def test_incompatible_attempt_tools_or_required_checks_are_fatal(self):
        for mutation in ("attempt", "tools", "doctests", "features", "missing_cfg", "roots", "scope"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                identity = self.make_shards(root)
                path = root / "shard-1/metadata.json"
                item = json.loads(path.read_text())
                if mutation == "attempt":
                    item["common"]["ci"]["run_attempt"] = "1"
                elif mutation == "tools":
                    item["common"]["tools"] = {"rustc": "different compiler"}
                elif mutation == "doctests":
                    item["checks"]["doctests"] = "skipped"
                elif mutation == "features":
                    item["common"]["configuration"]["package_features"]["crates/sample"] = [["schema"]]
                elif mutation == "missing_cfg":
                    del item["common"]["configuration"]
                elif mutation == "roots":
                    item["common"]["source_roots"] = []
                else:
                    item["common"]["measurement_paths"] = []
                path.write_text(json.dumps(item))
                with self.assertRaises(CoverageError):
                    merge_shards(root, identity, "fixture-policy")

    def test_corrupt_lcov_and_duplicate_tests_are_fatal(self):
        for mutation in ("corruption", "duplicate"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                identity = self.make_shards(root)
                folder = root / "shard-4"
                if mutation == "corruption":
                    (folder / "shard.lcov").write_text("truncated")
                else:
                    junit = (root / "shard-1/junit.xml").read_bytes()
                    (folder / "junit.xml").write_bytes(junit)
                    path = folder / "metadata.json"
                    item = json.loads(path.read_text())
                    item["junit_sha256"] = sha(junit)
                    path.write_text(json.dumps(item))
                with self.assertRaises(CoverageError):
                    merge_shards(root, identity, "fixture-policy")

    def test_automatic_path_dependency_workspace_members_are_included(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "Cargo.toml").write_text('[workspace]\nmembers = ["a"]\n[workspace.dependencies]\nb = { path = "b" }\n')
            for name in ("a", "b"):
                (root / name).mkdir()
            (root / "a/Cargo.toml").write_text('[package]\nname = "a"\n[dependencies]\nb.workspace = true\n')
            (root / "b/Cargo.toml").write_text('[package]\nname = "b"\n')
            packages, _ = workspace_packages(root)
            self.assertEqual(packages, {"a": "a", "b": "b"})

    def test_nonstandard_root_walks_path_modules_without_expanding_scope(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "Cargo.toml").write_text('[workspace]\nmembers = ["crates/sample"]\n')
            member = root / "crates/sample"
            (member / "src").mkdir(parents=True)
            (member / "Cargo.toml").write_text('[package]\nname = "sample"\n[lib]\npath = "lib.rs"\n')
            data = {
                "crates/sample/lib.rs": '#[path = "src/lib.rs"]\nmod generated;\n',
                "crates/sample/src/lib.rs": "//! @generated module list\nmod authored;\n",
                "crates/sample/src/authored.rs": "pub fn production() {}\n",
            }
            for path, source in data.items():
                (root / path).write_text(source)
            with patch("coverage.ROOT", root), patch("coverage.command", return_value="\0".join(data)):
                packages, roots, sources, measured = inventory()
                self.assertEqual(roots, {"crates/sample/lib.rs"})
                self.assertIn("crates/sample/lib.rs", sources)  # Included in inventory hashes.
                self.assertNotIn("crates/sample/lib.rs", measured)
                files = relative_sources({p: {1: 1} for p in sources}, measured)
                self.assertNotIn("crates/sample/lib.rs", files)
                parsed = RustSources(sources, roots, POLICY, measured, packages, configuration())
                self.assertEqual(parsed.filter(files), {"crates/sample/src/authored.rs": {1: 1}})

    def test_lcov_source_aliases_normalize_before_scope_filtering(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            source = "crates/desktop/src/main.rs"
            files = {
                source: {1: 0, 2: 0},
                str(root / "crates/desktop/src/module/../main.rs"): {1: 3, 3: 0},
                "crates/desktop/src/module/../main.rs": {1: 2},
                str(root / "crates/desktop/src/../tests/desktop_architecture.rs"): {1: 7},
                "crates/desktop/src/../tests/desktop_architecture.rs": {1: 11},
                str(root / "../external.rs"): {1: 13},
                str(root.parent / (root.name + "-sibling") / source): {1: 17},
            }
            with patch("coverage.ROOT", root):
                self.assertEqual(relative_sources(files, {source}),
                                 {source: {1: 5, 2: 0, 3: 0}})

    def test_relative_lcov_paths_cannot_escape_checkout(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            source = "crates/desktop/src/main.rs"
            files = {source: {1: 1}, "../external.rs": {1: 3}}
            with patch("coverage.ROOT", root), self.assertRaisesRegex(CoverageError, "escapes repository"):
                relative_sources(files, {source})

    def test_compiler_artifacts_capture_actual_feature_variants(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = (root / "crates/sample/Cargo.toml").as_posix()
            artifact = {"reason": "compiler-artifact", "manifest_path": manifest,
                        "target": {"kind": ["lib"]}, "features": ["schema"]}
            cached = dict(artifact, features=[], fresh=True)
            build_script = dict(artifact, target={"kind": ["custom-build"]}, features=["host-only"])
            messages = [artifact, cached, build_script, {"reason": "build-finished", "success": True}]
            (root / "build.jsonl").write_text("\n".join(json.dumps(m) for m in messages))
            (root / "target-cfg.txt").write_text("\n".join(LINUX_CFG))
            with patch("coverage.ROOT", root):
                self.assertEqual(measured_configuration(root, {"crates/sample": "sample"}),
                                 configuration([[], ["schema"]]))
                # Neither missing facts nor a truncated/failed build is accepted as feature-off.
                with self.assertRaises(CoverageError):
                    measured_configuration(root, {"crates/missing": "missing"})
                (root / "build.jsonl").write_text(json.dumps(artifact))
                with self.assertRaises(CoverageError):
                    measured_configuration(root, {"crates/sample": "sample"})
                (root / "build.jsonl").write_text(json.dumps({"reason": "build-finished", "success": False}))
                with self.assertRaises(CoverageError):
                    measured_configuration(root, {"crates/sample": "sample"})

    def test_llvm_function_totals_normalize_to_unique_da_lines(self):
        # Functions/closures may overlap; LF/LH describe function summaries,
        # while DA is the file-level line inventory used by our metric.
        for found, hit in ((4, 2), (2, 2)):
            with self.subTest(found=found, hit=hit), tempfile.TemporaryDirectory() as tmp:
                raw = ("SF:crates/example/src/lib.rs\nFN:1,outer\nFN:1,closure\n"
                       "FNDA:3,outer\nFNDA:1,closure\nFNF:2\nFNH:2\n"
                       f"DA:1,3\nDA:2,0\nLF:{found}\nLH:{hit}\nend_of_record\n")
                files = read_lcov(raw, llvm_export=True)
                self.assertEqual(files, {"crates/example/src/lib.rs": {1: 3, 2: 0}})
                self.assertEqual(totals(files), {"covered_lines": 1, "measurable_lines": 2, "percent": 50.0})
                output = Path(tmp) / "shard.lcov"
                write_lcov(output, files)
                self.assertEqual(output.read_text(), "TN:\nSF:crates/example/src/lib.rs\n"
                                 "DA:1,3\nDA:2,0\nLF:2\nLH:1\nend_of_record\n")
                self.assertEqual(read_lcov(output.read_text()), files)
                # LLVM summary compatibility must not weaken normalized artifacts.
                with self.assertRaisesRegex(CoverageError, "inconsistent LCOV LF/LH"):
                    read_lcov(raw)

    def test_bad_line_totals_and_failed_junit_are_fatal(self):
        malformed = (
            "SF:src/lib.rs\nDA:1,-1\nLF:1\nLH:0\nend_of_record\n",
            "SF:src/lib.rs\nDA:1,0\n",  # Truncated record.
            "SF:src/lib.rs\nDA:1,0\nLF:1\nLH:2\nend_of_record\n",
            "SF:src/lib.rs\nDA:1,0\nLF:1\nend_of_record\n",  # Missing LH.
            "SF:src/lib.rs\nDA:1,0\nLH:0\nend_of_record\n",  # Missing LF.
            "SF:src/lib.rs\nLF:1\nLH:0\nend_of_record\n",  # Summary-only data.
            "SF:src/lib.rs\nDA:1,0\nDA:1,1\nLF:1\nLH:1\nend_of_record\n",
            "SF:src/lib.rs\nDA:1,0\nLF:1\nLF:1\nLH:0\nend_of_record\n",
        )
        for data in malformed:
            for llvm_export in (False, True):
                with self.subTest(data=data, llvm_export=llvm_export), self.assertRaises(CoverageError):
                    read_lcov(data, llvm_export=llvm_export)
        with self.assertRaisesRegex(CoverageError, "inconsistent LCOV LF/LH"):
            read_lcov("SF:src/lib.rs\nDA:1,0\nLF:2\nLH:0\nend_of_record\n")
        with self.assertRaises(CoverageError):
            junit_cases(b'<testsuites tests="1" failures="1" errors="0"/>')


class RustPolicyTests(unittest.TestCase):
    def filtered(self, sources, variants=None):
        data = {p: source.encode() for p, source in sources.items()}
        options = {} if variants is None else {
            "packages": {"crates/sample": "sample"}, "configuration": configuration(variants)}
        parser = RustSources(data, {"crates/sample/src/lib.rs"}, POLICY, **options)
        return parser.filter({p: {n: 0 for n in range(1, len(s.splitlines()) + 1)} for p, s in sources.items()})

    def test_service_adapters_keep_separate_normal_and_cross_platform_contexts(self):
        base = "crates/sample/src/"
        normal = ('#[cfg(target_os = "linux")]\nmod linux;\n'
                  '#[cfg(target_os = "windows")]\nmod windows;\n')
        cross_platform = ('#[cfg(all(test, not(target_os = "linux")))]\n'
                          '#[path = "linux.rs"]\nmod linux_cross_platform;\n'
                          '#[cfg(all(test, not(target_os = "windows")))]\n'
                          '#[path = "windows.rs"]\nmod windows_cross_platform;\n')
        for declarations in (normal + cross_platform, cross_platform + normal):
            with self.subTest(declarations=declarations):
                sources = {
                    base + "lib.rs": "mod service;\n",
                    base + "service/mod.rs": declarations,
                    base + "service/linux.rs": 'mod child;\ninclude!("shared.rs");\npub fn linux_service() {}\n',
                    base + "service/windows.rs": "mod child;\npub fn windows_service() {}\n",
                    base + "service/linux/child.rs": 'pub fn production() {}\n#[test]\nfn only_test() {}\n',
                    base + "service/windows/child.rs": "fn utility() {}\n",
                    base + "service/child.rs": 'include!("detail.rs");\n',
                    base + "service/detail.rs": "fn utility() {}\n",
                    base + "service/shared.rs": "mod shared_child;\npub fn included_production() {}\n",
                    base + "service/shared_child.rs": "pub fn production() {}\n",
                }
                result = self.filtered(sources, [[]])
                self.assertEqual(set(result), {base + p for p in (
                    "lib.rs", "service/mod.rs", "service/linux.rs", "service/linux/child.rs",
                    "service/shared.rs", "service/shared_child.rs")})
                self.assertEqual(set(result[base + "service/linux.rs"]), {1, 2, 3})
                self.assertEqual(set(result[base + "service/linux/child.rs"]), {1})
                self.assertEqual(set(result[base + "service/shared.rs"]), {1, 2})

    def test_two_production_inclusions_resolve_both_child_directories(self):
        base = "crates/sample/src/"
        sources = {
            base + "lib.rs": ('#[cfg(unix)]\nmod adapter;\n'
                              '#[cfg(target_os = "linux")]\n#[path = "adapter.rs"]\nmod alias;\n'),
            base + "adapter.rs": 'mod child;\npub fn production() {}\n#[cfg(test)]\nfn only_test() {}\n',
            base + "adapter/child.rs": "pub fn normal_child() {}\n",
            base + "child.rs": "pub fn path_child() {}\n",
        }
        result = self.filtered(sources, [[]])
        self.assertEqual(set(result), set(sources))
        self.assertEqual(set(result[base + "adapter.rs"]), {1, 2})
        self.assertEqual(set(result[base + "adapter/child.rs"]), {1})
        self.assertEqual(set(result[base + "child.rs"]), {1})

    def test_external_test_scope_propagates_to_literal_include_with_test_macro(self):
        base = "crates/sample/src/"
        sources = {
            base + "lib.rs": ('#[cfg(test)]\n#[path = "helper.rs"]\nmod checks;\n'
                              'pub fn production() {}\n'),
            base + "helper.rs": 'include!("macros.rs");\n',
            base + "macros.rs": 'macro_rules! emit { () => { #[test] fn hidden() {} }; }\n',
        }
        result = self.filtered(sources, [[]])
        self.assertEqual(result, {base + "lib.rs": {4: 0}})
        # Sharing the same file/directory with production must restore the guard.
        sources[base + "lib.rs"] += '#[path = "helper.rs"]\nmod shared;\n'
        with self.assertRaisesRegex(CoverageError, "test-generating macro outside test scope"):
            self.filtered(sources, [[]])

    def test_production_macros_allow_feature_and_platform_cfg_with_feature_on_or_off(self):
        path = "crates/sample/src/lib.rs"
        source = """macro_rules! record_qualification_diagnostic {
    ($function:ident($($argument:expr),* $(,)?)) => {
        #[cfg(feature = "qualification-diagnostics")]
        { $function($($argument),*); }
        #[cfg(not(feature = "qualification-diagnostics"))]
        { () }
        #[cfg(windows)]
        { windows_diagnostic(); }
        #[cfg(not(test))]
        { production(); }
        #[cfg_attr(unix, cfg(feature = "qualification-diagnostics"))]
        { another_diagnostic(); }
    };
}
ordinary! {
    #[cfg(feature = "qualification-diagnostics")]
    fn production_item() {}
}
pub fn keep() {}
"""
        for features in ([], ["qualification-diagnostics"]):
            with self.subTest(features=features):
                result = self.filtered({path: source}, [features])
                self.assertEqual(set(result[path]), set(range(1, len(source.splitlines()) + 1)))

    def test_test_generating_macros_still_require_an_enclosing_test_scope(self):
        path = "crates/sample/src/lib.rs"
        attributes = (
            "#[test]", "#[tokio::test]", "#[cfg(test)]",
            '#[cfg(feature = "test-support")]',
            '#[cfg(any(test, feature = "disabled"))]',
            "#[cfg_attr(unix, cfg_attr(test, test))]",
            '#[cfg_attr(feature = "disabled", cfg_attr(unix, cfg(test)))]',
        )
        for attr in attributes:
            for macro in ('macro_rules! emit { () => { ATTR fn hidden() {} }; }\n',
                          'ordinary! { ATTR fn hidden() {} }\n'):
                with self.subTest(attribute=attr, macro=macro), self.assertRaisesRegex(
                        CoverageError, "test-generating macro outside test scope"):
                    self.filtered({path: macro.replace("ATTR", attr)}, [[]])

    def test_numeric_schemars_arguments_are_opaque_with_schema_on_or_off(self):
        path = "crates/sample/src/lib.rs"
        source = ('#[cfg_attr(feature = "schema", schemars(length(min = 1, max = 128)))]\n'
                  'pub struct Files(pub Vec<String>);\n')
        for features in ([], ["schema"]):
            with self.subTest(features=features):
                self.assertEqual(set(self.filtered({path: source}, [features])[path]), {1, 2})
        # Unknown ordinary attributes also need no feature facts.
        self.assertEqual(set(self.filtered({path: source})[path]), {1, 2})

    def test_nested_cfg_attr_keeps_test_attributes_and_module_paths_semantic(self):
        path = "crates/sample/src/lib.rs"
        sources = {path: ('#[cfg_attr(feature = "schema", cfg_attr(unix, test))]\n'
                          'fn conditional_test() {}\n'
                          '#[cfg_attr(unix, path = "helper.rs")]\nmod chosen;\n'),
                   "crates/sample/src/helper.rs": "pub fn production() {}\n"}
        self.assertEqual(set(self.filtered(sources, [["schema"]])[path]), {3, 4})
        self.assertEqual(set(self.filtered(sources, [[]])[path]), {1, 2, 3, 4})
        self.assertIn("crates/sample/src/helper.rs", self.filtered(sources, [["schema"]]))

    def test_mixed_feature_test_uses_actual_features_and_preserves_not_test(self):
        path = "crates/sample/src/lib.rs"
        source = ('#[cfg(any(feature = "schema", test))]\npub fn schema() {}\n'
                  '#[cfg(not(test))]\npub fn production() {}\n')
        self.assertEqual(set(self.filtered({path: source}, [["schema"]])[path]), {1, 2, 3, 4})
        self.assertEqual(set(self.filtered({path: source}, [[]])[path]), {3, 4})
        self.assertEqual(set(self.filtered({path: source}, [[], ["schema"]])[path]), {1, 2, 3, 4})

    def test_feature_variants_do_not_create_a_fictional_combined_configuration(self):
        path = "crates/sample/src/lib.rs"
        source = ('#[cfg(feature = "schema")]\nmod outer {\n'
                  '    #[cfg(any(not(feature = "schema"), test))]\n    fn only_test() {}\n}\n'
                  'pub fn keep() {}\n')
        self.assertFalse({3, 4} & set(self.filtered({path: source}, [[], ["schema"]])[path]))

    def test_variant_specific_test_module_paths_keep_excluded_descendants(self):
        path = "crates/sample/src/lib.rs"
        sources = {
            path: ('#[cfg(test)]\n#[cfg_attr(feature = "schema", path = "alternate.rs")]\n'
                   'mod helper;\npub fn keep() {}\n'),
            "crates/sample/src/helper.rs": 'include!("nested.rs");\n',
            "crates/sample/src/alternate.rs": 'include!("other.rs");\n',
            "crates/sample/src/nested.rs": "fn utility() {}\n",
            "crates/sample/src/other.rs": "fn utility() {}\n",
        }
        result = self.filtered(sources, [[], ["schema"]])
        self.assertEqual(set(result), {path})
        self.assertEqual(set(result[path]), {4})

    def test_linux_mixed_platform_cfg_propagates_through_excluded_modules_and_include(self):
        path = "crates/sample/src/lib.rs"
        sources = {
            path: ('#[cfg(any(not(unix), test))]\n#[path = "helper.rs"]\nmod secure;\n'
                   '#[cfg(any(unix, test))]\npub fn linux_production() {}\n'
                   '#[cfg(not(test))]\npub fn production() {}\n'),
            "crates/sample/src/helper.rs": 'include!("nested.rs");\n',
            "crates/sample/src/nested.rs": "fn utility() {}\n",
        }
        filtered = self.filtered(sources, [[]])
        self.assertEqual(set(filtered), {path})
        self.assertEqual(set(filtered[path]), {4, 5, 6, 7})

    def test_inline_test_range_handles_strings_comments_and_nested_blocks(self):
        source = '''pub fn production() -> &'static str { r#"{ #[cfg(test)] }"# }
#[cfg(test)]
mod odd_name {
    // } should not end the test module.
    #[test]
    fn braces() { let _ = "}"; if true { assert!(true); } }
}
pub fn after() {}
'''
        path = "crates/sample/src/lib.rs"
        self.assertEqual(set(self.filtered({path: source})[path]), {1, 8})

    def test_standalone_test_and_test_support_cfg_are_excluded(self):
        path = "crates/sample/src/lib.rs"
        source = '''pub fn keep() {}
#[tokio::test]
async fn test_one() {}
#[cfg(any(test, feature = "test-support"))]
fn helper() {}
#[cfg(not(test))]
pub fn keep_too() {}
'''
        self.assertEqual(set(self.filtered({path: source})[path]), {1, 6, 7})

    def test_external_test_modules_and_literal_includes_inherit_scope(self):
        sources = {
            "crates/sample/src/lib.rs": '''pub fn production() {}
#[cfg(test)]
#[path = "helper.rs"]
mod unusual_name;
''',
            "crates/sample/src/helper.rs": '''include!("nested.rs");
''',
            "crates/sample/src/nested.rs": '''fn utility() {}
''',
        }
        self.assertEqual(set(self.filtered(sources)), {"crates/sample/src/lib.rs"})

    def test_mixed_line_ambiguous_cfg_macro_and_parse_errors_fail_closed(self):
        path = "crates/sample/src/lib.rs"
        for source in ("pub fn keep() {} #[test] fn bad() {}\n",
                       '#[cfg(any(test, feature = "unresolved"))]\nfn unclear() {}\n',
                       'macro_rules! emit { () => { #[test] fn hidden() {} }; }\n',
                       'pub fn incomplete( {\n'):
            with self.subTest(source=source), self.assertRaises(CoverageError):
                self.filtered({path: source})

    def test_cfg_on_struct_initializer_and_match_arm_owns_entire_expression(self):
        path = "crates/sample/src/lib.rs"
        source = """pub fn example() {
    let _ = Sample {
        #[cfg(test)]
        helper: expensive_helper(),
        real: 0,
    };
    match 0 {
        #[cfg(test)]
        1 => expensive_helper(),
        _ => (),
    }
}
"""
        lines = set(self.filtered({path: source})[path])
        self.assertFalse(lines & {3, 4, 8, 9})
        self.assertTrue({1, 2, 5, 6, 7, 10, 11, 12} <= lines)

    def test_dynamic_include_requires_real_out_dir_tokens(self):
        path = "crates/sample/src/lib.rs"
        self.filtered({path: 'include!(concat!(env!("OUT_DIR"), "/generated.rs"));\n'})
        with self.assertRaises(CoverageError):
            self.filtered({path: 'include!(concat!("OUT_DIR", "/workspace.rs"));\n'})

    def test_inner_test_attribute_excludes_whole_module(self):
        path = "crates/sample/src/lib.rs"
        source = "mod testing {\n    #![cfg(test)]\n    fn helper() {}\n}\npub fn keep() {}\n"
        self.assertEqual(set(self.filtered({path: source})[path]), {5})

    def test_generated_parent_does_not_exclude_authored_child(self):
        sources = {
            "crates/sample/src/lib.rs": "//! @generated module list\nmod authored;\n",
            "crates/sample/src/authored.rs": "pub fn production() {}\n",
        }
        self.assertEqual(set(self.filtered(sources)), {"crates/sample/src/authored.rs"})

    def test_test_only_field_with_trailing_comma_and_comment(self):
        path = "crates/sample/src/lib.rs"
        source = "struct Sample {\n    #[cfg(test)]\n    helper: u8, // test-only field\n    real: u8,\n}\n"
        self.assertEqual(set(self.filtered({path: source})[path]), {1, 4, 5})

    def test_generated_and_fixture_files_do_not_enter_denominator(self):
        sources = {
            "crates/sample/src/lib.rs": "mod generated_model;\nmod fixtures;\npub fn keep() {}\n",
            "crates/sample/src/generated_model.rs": "//! @generated by an external tool\npub fn generated() {}\n",
            "crates/sample/src/fixtures/mod.rs": "pub fn fixture() {}\n",
        }
        self.assertEqual(set(self.filtered(sources)), {"crates/sample/src/lib.rs"})


if __name__ == "__main__":
    unittest.main()
