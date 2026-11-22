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
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile

NOTARY = runpy.run_path(str(Path(__file__).with_name("macos-notary.py")))
GATEWAY = runpy.run_path(str(Path(__file__).with_name("package-gateway.py")))
SUBMISSION_ID = "11111111-2222-3333-4444-555555555555"
CREDENTIALS = dict(APPLE_NOTARIZATION_KEY_ID="fixture-key-id",
                   APPLE_NOTARIZATION_ISSUER_ID="fixture-issuer-id",
                   APPLE_NOTARIZATION_KEY="fixture-notary-key")


class NotaryTransport:
    """Fixture for only xcrun's external wire/file responses, not owner outcomes."""
    def __init__(self, report_status="Accepted", issues=None, staple_failures=0, validation_errors=()):
        self.report_status = report_status
        self.issues = issues
        self.staple_failures = staple_failures
        self.actions = []
        self.staple_attempts = 0
        self.validation_errors = iter(validation_errors)
        self.validation_attempts = 0

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
            self.validation_attempts += 1
            failure = next(self.validation_errors, None)
            if failure is not None:
                status, error = failure
            elif not (Path(arguments[-1]) / "Contents/CodeResources").is_file():
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
    def test_default_gateway_delivers_exact_signed_notarized_archive(self):
        identity = "Developer ID Application: Fixture"
        expected_code = {
            "pioneer", "libonnxruntime.dylib", "cua/postgres/lib/vector.so",
            *(f"cua/postgres/bin/{name}" for name in ("postgres", "initdb", "pg_ctl", "psql")),
        }
        extension = "cua/postgres/share/extension/vector.control"
        signature = b"fixture Developer ID signature"
        transport = NotaryTransport()

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = SimpleNamespace(target="x86_64-apple-darwin", inputs=root/"inputs",
                                   postgres=root/"inputs/cua/postgres", notary_log_dir=root/"notary")
            for entry in expected_code | {extension}:
                path = args.inputs / entry
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"\xcf\xfa\xed\xfe" if entry in expected_code else b"extension")
            binary = args.inputs / "pioneer"
            binary.chmod(0o755)
            submitted = []

            def run(arguments, **options):
                if arguments[0] == "codesign":
                    path = Path(arguments[-1])
                    if "--sign" in arguments:
                        self.assertIn("--timestamp", arguments)
                        self.assertEqual(arguments[arguments.index("--sign") + 1], identity)
                        path.write_bytes(path.read_bytes() + signature)
                    else:
                        self.assertTrue(path.read_bytes().endswith(signature))
                    return subprocess.CompletedProcess(arguments, 0, "",
                                                       f"Authority={identity}\nTeamIdentifier=TESTTEAM\n")
                if arguments[1:3] == ["notarytool", "submit"]:
                    archive = Path(arguments[3])
                    with zipfile.ZipFile(archive) as payload:
                        self.assertEqual(set(payload.namelist()), expected_code | {extension})
                        for entry in expected_code:
                            self.assertTrue(payload.read(entry).endswith(signature), entry)
                        self.assertEqual(payload.read(extension), b"extension")
                        mode = payload.getinfo("cua/postgres/bin/postgres").external_attr >> 16
                        self.assertEqual(mode & 0o777, 0o755)
                    submitted.append(hashlib.sha256(archive.read_bytes()).hexdigest())
                return transport.run(arguments, **options)

            with (patch.dict(os.environ, CREDENTIALS, MACOS_DESKTOP_SIGN_IDENTITY=identity),
                  patch.object(subprocess, "run", run)):
                # Rejection blocks distribution and retains the exact submission.
                archive = root / "published.zip"
                transport.report_status = "Rejected"
                with self.assertRaisesRegex(ValueError, "Notarization Rejected"):
                    GATEWAY["package_macos_default"](args, binary, archive)
                self.assertFalse(archive.exists())

                # A separate accepted release publishes the submitted bytes.
                args.notary_log_dir = root / "accepted"
                transport.report_status = "Accepted"
                receipt = GATEWAY["package_macos_default"](args, binary, archive)
                resumed = root / "resumed.zip"
                self.assertEqual(GATEWAY["package_macos_default"](args, binary, resumed), receipt)
                self.assertEqual(resumed.read_bytes(), archive.read_bytes())

            self.assertEqual(receipt, dict(id=SUBMISSION_ID, status="Accepted", sha256=submitted[-1]))
            self.assertEqual(hashlib.sha256(archive.read_bytes()).hexdigest(), submitted[-1])
            self.assertEqual(len(submitted), 2)
            self.assertFalse(any(tool == "stapler" for tool, _ in transport.actions))

    def test_delivered_ticket_validation_recovers_network_errors_but_rejects_invalid_tickets(self):
        network = (68, 'Error Domain=NSURLErrorDomain Code=-1005 "The network connection was lost."\n'
                       'CloudKit\'s response is inconsistent with expections: (null)\n'
                       'The validate action failed! Error 68.\n')
        invalid = (65, "The ticket data is invalid.\n")
        for errors, attempts, failure in (((network,), 2, None),
                                          ((network,) * 4, 4, "network connection was lost"),
                                          ((invalid,), 1, "ticket data is invalid")):
            with self.subTest(errors=errors), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                _, app, logs = inputs(root)
                (app / "Contents/CodeResources").write_bytes(b"delivered ticket fixture")
                transport = NotaryTransport(validation_errors=errors)
                with (patch.dict(os.environ, PIONEER_NOTARY_LOG_DIR=str(logs)),
                      patch.object(subprocess, "run", transport.run),
                      patch("time.sleep", return_value=None), redirect_stdout(io.StringIO())):
                    if failure:
                        with self.assertRaisesRegex(RuntimeError, failure):
                            NOTARY["validate_ticket"](app)
                    else:
                        NOTARY["validate_ticket"](app)
                self.assertEqual(transport.validation_attempts, attempts)
                validation_logs = list((logs / "validation").glob("*.log"))
                self.assertEqual(len(validation_logs), 1)
                log = validation_logs[0].read_text()
                self.assertIn(errors[0][1], log)
                if failure is None:
                    self.assertIn("attempt 2, exit 0", log)
                self.assertEqual((app / "Contents/CodeResources").read_bytes(), b"delivered ticket fixture")
                self.assertFalse((logs / "submission.json").exists())

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
