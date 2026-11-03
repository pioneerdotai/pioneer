#!/usr/bin/env python3
"""Notarize one complete container; retain submission state and staple its objects.

Apple documents nested tickets and the ZIP/standalone stapling limitation at
https://developer.apple.com/documentation/security/customizing-the-notarization-workflow
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def transient(result):
    output = (result.stdout + result.stderr).lower()
    if any(marker in output for marker in ("revoked", "invalid signature", "not code-signed", "ticket data is invalid")):
        return False
    return any(marker in output for marker in (
        "timed out", "timeout", "connection", "network", "service unavailable",
        "temporarily unavailable", "http 503", "http 502", "cloudkit", "error 65",
        "could not find ticket", "record not found"))


def command(arguments, log, retry=False, terminal_json=False):
    for attempt in range(1, 5 if retry else 2):
        result = subprocess.run(arguments, capture_output=True, text=True)
        with log.open("a") as output:
            output.write(f"attempt {attempt}, exit {result.returncode}\n")
            output.write(result.stdout + result.stderr + "\n")
        if result.returncode == 0:
            return result.stdout
        if terminal_json:
            try:
                if json.loads(result.stdout).get("status") in ("Accepted", "Invalid", "Rejected"):
                    return result.stdout
            except (ValueError, AttributeError):
                pass
        if not retry or attempt == 4 or not transient(result):
            details = (result.stdout + result.stderr).strip()
            raise RuntimeError(f"{arguments[1:3]} failed; see {log}\n{details}")
        delay = attempt * 5
        print(f"Transient Apple error; attempt {attempt}/4, retry in {delay}s", flush=True)
        time.sleep(delay)


def validate_ticket(path, log=None):
    """Validate delivered tickets with the same bounded network retries as stapling."""
    if log is None:
        identity = hashlib.sha256(str(Path(path).resolve()).encode()).hexdigest()[:16]
        log = Path(os.environ.get("PIONEER_NOTARY_LOG_DIR", "notary-logs")) / "validation" / f"{identity}.log"
    log = Path(log)
    log.parent.mkdir(parents=True, exist_ok=True)
    command(["xcrun", "stapler", "validate", str(path)], log, retry=True)


def notarize(submit, staples, log_dir):
    submit, log_dir = Path(submit), Path(log_dir)
    staples = [Path(path) for path in staples]
    if not submit.is_file() or not staples:
        raise ValueError("Notarization requires a complete container and staple objects")
    if any(path.suffix not in (".app", ".dmg", ".pkg") or not path.exists() for path in staples):
        raise ValueError("Only existing Apple-supported objects may be stapled")
    log_dir.mkdir(parents=True, exist_ok=True)
    state_path = log_dir / "submission.json"
    fingerprint = digest(submit)
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    if state and fingerprint not in (state.get("sha256"), state.get("final_sha256")):
        raise ValueError("Retained notary submission belongs to different input bytes")
    key_id = os.environ.get("APPLE_NOTARIZATION_KEY_ID", "")
    issuer = os.environ.get("APPLE_NOTARIZATION_ISSUER_ID", "")
    key = os.environ.get("APPLE_NOTARIZATION_KEY", "")
    if not key and os.environ.get("APPLE_NOTARIZATION_KEY_BASE64"):
        encoded = "".join(os.environ["APPLE_NOTARIZATION_KEY_BASE64"].split()).rstrip("%")
        key = base64.b64decode(encoded, validate=True).decode()
    if not key_id or not issuer or not key:
        raise ValueError("Notarization requires existing Apple API credentials")
    with tempfile.TemporaryDirectory(prefix="pioneer-notary-key-") as temporary:
        key_file = Path(temporary) / "key.p8"
        key_file.write_text(key)
        key_file.chmod(0o600)
        credentials = ["--key", str(key_file), "--key-id", key_id, "--issuer", issuer]
        if not state.get("id"):
            # Never retry submit blindly: a lost response may already have created
            # a submission. Keep its raw response for operator recovery instead.
            if state.get("submission_started"):
                raise ValueError("Prior submit has an uncertain outcome; recover its ID from submit.log before retrying")
            state = dict(sha256=fingerprint, submission_started=True)
            write_json(state_path, state)
            submitted = subprocess.run(["xcrun", "notarytool", "submit", str(submit),
                                       *credentials, "--output-format", "json"], capture_output=True, text=True)
            (log_dir / "submit.log").write_text(submitted.stdout + submitted.stderr)
            result = json.loads(submitted.stdout)
            if not result.get("id"):
                raise ValueError("Apple submit response has no submission ID")
            state.update(id=result["id"], status=result.get("status"))
            write_json(state_path, state)
        if state.get("status") not in ("Accepted", "Invalid", "Rejected"):
            response = command(["xcrun", "notarytool", "wait", state["id"],
                                *credentials, "--output-format", "json"], log_dir / "wait.log", retry=True, terminal_json=True)
            result = json.loads(response)
            state["status"] = result.get("status")
            write_json(state_path, state)
        command(["xcrun", "notarytool", "log", state["id"], *credentials,
                 str(log_dir / "apple-report.json")], log_dir / "log.log", retry=True)
        report = json.loads((log_dir / "apple-report.json").read_text())
        if (state.get("status") != "Accepted" or report.get("status") != "Accepted"
                or str(report.get("jobId", "")).lower() != str(state["id"]).lower()
                or any(issue.get("severity") == "error" for issue in report.get("issues") or [])):
            raise ValueError(f"Notarization {state.get('status')}; see {log_dir / 'apple-report.json'}")
    for index, path in enumerate(staples):
        command(["xcrun", "stapler", "staple", str(path)], log_dir / f"staple-{index}.log", retry=True)
        state["final_sha256"] = digest(submit)
        write_json(state_path, state)
        validate_ticket(path, log_dir / f"validate-{index}.log")
    state["final_sha256"] = digest(submit)
    write_json(state_path, state)
    return state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--submit", required=True, type=Path)
    parser.add_argument("--staple", action="append", required=True, type=Path)
    parser.add_argument("--log-dir", required=True, type=Path)
    args = parser.parse_args()
    notarize(args.submit, args.staple, args.log_dir)


if __name__ == "__main__":
    main()
