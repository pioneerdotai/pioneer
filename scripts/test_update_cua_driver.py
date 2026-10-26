"""Release selection and failure boundaries of the CUA source updater."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("cua_updater", Path(__file__).with_name("update-cua-driver.py"))
updater = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(updater)

TAG = "nightly-cua-driver-rs-v0.38.1-nightly.20261026.40269720879"
REVISION = "a" * 40


class ReleaseUpdateTests(unittest.TestCase):
    def test_latest_is_driver_specific_and_accepts_published_nightlies(self):
        releases = [
            {"tag_name": TAG, "published_at": "2026-10-26T05:25:46Z", "prerelease": True},
            {"tag_name": "cua-driver-rs-v0.38.0", "published_at": "2026-10-21T05:26:13Z", "prerelease": True},
            {"tag_name": "cua-spaces-v99.0.0", "published_at": "2026-10-27T00:00:00Z"},
            {"tag_name": "cua-driver-rs-v9.0.0", "published_at": "2026-10-28T00:00:00Z", "draft": True},
            {"tag_name": "cua-driver-rs-v9.0.0", "published_at": None},
        ]
        self.assertEqual(updater.select_release(releases)["tag_name"], TAG)
        with self.assertRaisesRegex(ValueError, "No published CUA driver"):
            updater.select_release(releases[2:])

    def test_schema_copy_preserves_nested_delimiters_and_string_contents(self):
        expression = '({"description": "literal } and \\\"quoted\\\"", "nested": [json!({"ref": r#"a)"#})]})'
        self.assertEqual(updater.fragment(expression + ", ignored()", 0), expression)
        with self.assertRaisesRegex(ValueError, "Unbalanced|Unterminated"):
            updater.fragment('({"schema": [}', 0)

    def test_missing_browser_schema_cannot_take_the_next_tools_schema(self):
        source = 'name: "get_browser_state".into(),\nname: "browser_click".into(),\ninput_schema: json!({})'
        module = 'pub(crate) fn session_schema() -> serde_json::Value { serde_json::json!({"type": "string"}) }'
        native = {"tools": [{"input_schema": {"properties": {"session": {"description": "Owner session"}}}}]}
        with self.assertRaisesRegex(ValueError, "Cannot extract schema for get_browser_state"):
            updater.browser_catalog(source, module, ["get_browser_state"], native, TAG)

    def test_moved_existing_tag_does_not_replace_trusted_source(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / updater.METADATA
            path.parent.mkdir()
            original = json.dumps({"release": TAG, "revision": "b" * 40}).encode()
            path.write_bytes(original)

            def github(path, raw=False):
                if path.startswith("releases/tags/"):
                    return {"published_at": "2026-10-26T05:25:46Z", "draft": False}
                if path.startswith("commits/"):
                    return {"sha": REVISION}
                self.fail("A moved release must be rejected before reading its source")

            with patch.object(updater, "github", side_effect=github):
                with self.assertRaisesRegex(ValueError, "moved an existing release tag"):
                    updater.update(root, TAG)
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual([p.relative_to(root) for p in root.rglob("*") if p.is_file()], [updater.METADATA])

    def test_resolver_failure_restores_all_files_and_removes_new_outputs(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            originals = {
                updater.GATEWAY: b'[dependencies]\ncua-driver-sdk = { git = "https://github.com/trycua/cua", rev = "old", optional = true }\n',
                updater.CATALOG / "native-contract.json": b"old native catalog\n",
                updater.CATALOG / "browser_catalog.rs": b"old browser catalog\n",
                Path("Cargo.lock"): b"original locked graph\n",
                Path("crates/protocol/src/turn.rs"): b'pub const CUA_TOOL_NAMES: &[&str] = &["cua_click"];\n',
            }
            for path, content in originals.items():
                (root / path).parent.mkdir(parents=True, exist_ok=True)
                (root / path).write_bytes(content)
            (root / "config").mkdir()
            upstream = {
                updater.WORKSPACE: '[workspace.package]\nversion = "0.38.0"\n',
                updater.NATIVE: json.dumps({"contract_version": "0.8.0", "tools": [
                    {"name": "click", "description": "Click", "input_schema": {
                        "properties": {"session": {"description": "Owner session"}}}},
                ]}),
                updater.BROWSER: "",
                updater.BROWSER_MODULE: 'pub(crate) fn session_schema() -> serde_json::Value { serde_json::json!({"type": "string"}) }',
            }

            def github(path, raw=False):
                if path.startswith("releases/tags/"):
                    return {"published_at": "2026-10-26T05:25:46Z", "draft": False}
                if path.startswith("commits/"):
                    return {"sha": REVISION}
                return upstream[path.removeprefix("contents/").split("?", 1)[0]]

            def fail_resolution(command, cwd, check):
                self.assertEqual(command, ["cargo", "update", "--workspace"])
                self.assertEqual(cwd, root)
                # The real update path has written a coherent candidate before
                # calling Cargo. A failed resolver may also have touched its lock.
                self.assertEqual(json.loads((root / updater.METADATA).read_text())["release"], TAG)
                self.assertIn('tag = "' + TAG + '"', (root / updater.GATEWAY).read_text())
                self.assertEqual(json.loads((root / updater.CATALOG / "native-contract.json").read_text())["revision"], REVISION)
                (root / "Cargo.lock").write_text("partial resolver output\n")
                raise subprocess.CalledProcessError(101, command)

            with patch.object(updater, "github", side_effect=github), \
                 patch.object(updater.subprocess, "check_output", side_effect=lambda command, input, text: input), \
                 patch.object(updater.subprocess, "run", side_effect=fail_resolution):
                with self.assertRaises(subprocess.CalledProcessError):
                    updater.update(root, TAG)
            actual = {p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()}
            self.assertEqual(actual, originals)


if __name__ == "__main__":
    unittest.main()
