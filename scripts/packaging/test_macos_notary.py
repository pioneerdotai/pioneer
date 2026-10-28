"""Author-only notary boundary regressions; transport responses are fixtures.

These tests never contact Apple or run signing tools. The real notary owner
interprets external CLI responses and writes its own state, reports and logs.
"""
from contextlib import redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import runpy
import subprocess
import tempfile
import unittest
from unittest.mock import patch

NOTARY = runpy.run_path(str(Path(__file__).with_name("macos-notary.py")))
SUBMISSION_ID = "11111111-2222-3333-4444-555555555555"
CREDENTIALS = dict(APPLE_NOTARIZATION_KEY_ID="fixture-key-id",
                   APPLE_NOTARIZATION_ISSUER_ID="fixture-issuer-id",
                   APPLE_NOTARIZATION_KEY="fixture-notary-key")


class NotaryTransport:
    """Fixture for only xcrun's external wire/file responses, not owner outcomes."""
    def __init__(self, report_status="Accepted", issues=None, staple_failures=0):
        self.report_status = report_status
        self.issues = issues
        self.staple_failures = staple_failures
        self.actions = []
        self.staple_attempts = 0

    def run(self, arguments, **options):
        if arguments[0] != "xcrun":
            raise AssertionError(f"Unexpected external command: {arguments[0]}")
        action = tuple(arguments[1:3])
        self.actions.append(action)
        status, output, error = 0, "", ""
        if action == ("notarytool", "submit"):
            output = json.dumps(dict(id=SUBMISSION_ID, status="In Progress"))
        elif action == ("notarytool", "wait"):
            output = json.dumps(dict(id=SUBMISSION_ID, status=self.report_status))
            status = 65 if self.report_status == "Invalid" else 0
        elif action == ("notarytool", "log"):
            Path(arguments[-1]).write_text(json.dumps(dict(
                jobId=SUBMISSION_ID, status=self.report_status, issues=self.issues)))
        elif action == ("stapler", "staple"):
            self.staple_attempts += 1
            if self.staple_attempts > 4:
                raise AssertionError("Stapler retries exceeded their external attempt budget")
            if self.staple_attempts <= self.staple_failures:
                status, error = 65, "CloudKit network connection timed out\n"
            else:
                (Path(arguments[-1]) / "Contents/CodeResources").write_bytes(b"fixture Apple ticket")
        elif action == ("stapler", "validate"):
            if not (Path(arguments[-1]) / "Contents/CodeResources").is_file():
                status, error = 66, "No stapled ticket\n"
        else:
            raise AssertionError(f"Unexpected xcrun operation: {action}")
        return subprocess.CompletedProcess(arguments, status, output, error)


def inputs(root, retained=False):
    submit, app, logs = root / "payload.zip", root / "Gateway.app", root / "notary"
    submit.write_bytes(b"notarization container fixture")
    (app / "Contents").mkdir(parents=True)
    if retained:
        logs.mkdir()
        (logs / "submission.json").write_text(json.dumps(dict(
            sha256=hashlib.sha256(submit.read_bytes()).hexdigest(),
            id=SUBMISSION_ID, status="Accepted")))
    return submit, app, logs


class MacosNotaryTests(unittest.TestCase):
    def test_invalid_submission_or_report_error_retains_report_and_never_staples(self):
        for status, issues in (("Invalid", [dict(severity="error", message="Invalid code signature")]),
                               ("Accepted", [dict(severity="error", message="Invalid nested signature")])):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                submit, app, logs = inputs(Path(directory))
                transport = NotaryTransport(status, issues)
                with patch.dict(os.environ, CREDENTIALS), patch.object(subprocess, "run", transport.run):
                    with self.assertRaisesRegex(ValueError, "Notarization"):
                        NOTARY["notarize"](submit, [app], logs)
                state = json.loads((logs / "submission.json").read_text())
                self.assertEqual((state["id"], state["status"]), (SUBMISSION_ID, status))
                report = json.loads((logs / "apple-report.json").read_text())
                self.assertEqual(report["issues"][0]["severity"], "error")
                self.assertFalse(any(tool == "stapler" for tool, action in transport.actions))
                self.assertFalse((app / "Contents/CodeResources").exists())
                self.assertNotIn("final_sha256", state)

    def test_retained_accepted_submission_retries_stapler_without_new_submit(self):
        for failures in (1, 4):
            with self.subTest(failures=failures), tempfile.TemporaryDirectory() as directory:
                submit, app, logs = inputs(Path(directory), retained=True)
                transport, output = NotaryTransport(staple_failures=failures), io.StringIO()
                with (patch.dict(os.environ, CREDENTIALS), patch.object(subprocess, "run", transport.run),
                      patch("time.sleep", return_value=None), redirect_stdout(output)):
                    if failures == 4:
                        with self.assertRaisesRegex(RuntimeError, "failed; see"):
                            NOTARY["notarize"](submit, [app], logs)
                    else:
                        NOTARY["notarize"](submit, [app], logs)
                self.assertNotIn(("notarytool", "submit"), transport.actions)
                self.assertNotIn(("notarytool", "wait"), transport.actions)
                state = json.loads((logs / "submission.json").read_text())
                self.assertEqual((state["id"], state["status"]), (SUBMISSION_ID, "Accepted"))
                self.assertIn("Transient Apple error", output.getvalue())
                if failures == 1:
                    self.assertEqual(transport.staple_attempts, 2)
                    self.assertTrue((app / "Contents/CodeResources").is_file())
                    self.assertIn(("stapler", "validate"), transport.actions)
                    self.assertIn("attempt 2, exit 0", (logs / "staple-0.log").read_text())
                    self.assertTrue((logs / "validate-0.log").is_file())
                    self.assertEqual(state["final_sha256"], hashlib.sha256(submit.read_bytes()).hexdigest())
                else:
                    self.assertEqual(transport.staple_attempts, 4)
                    self.assertFalse((app / "Contents/CodeResources").exists())
                    self.assertNotIn(("stapler", "validate"), transport.actions)
                    self.assertIn("attempt 4, exit 65", (logs / "staple-0.log").read_text())
                    self.assertNotIn("final_sha256", state)


if __name__ == "__main__":
    unittest.main()
