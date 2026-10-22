#!/usr/bin/env python3
"""Reuse artifacts only from the same repository/ref/commit's Release Gateway run."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tarfile
import time
from urllib.parse import urlencode

SYMBOLS = runpy.run_path(str(Path(__file__).with_name("symbol-inputs.py")))
WARMUP_RUN = "Warm Gateway release cache"


def api(path):
    return json.loads(subprocess.check_output(["gh", "api", path], text=True))


def verify_run(run, commit, ref):
    repo = os.environ["GITHUB_REPOSITORY"]
    if (run["head_repository"]["full_name"] != repo or run["head_sha"] != commit
            or run["head_branch"] != ref or run["event"] not in ("push", "workflow_dispatch")
            or run.get("display_title") == WARMUP_RUN
            or run["path"].split("@")[0] != ".github/workflows/release-gateway.yml"):
        raise ValueError("Gateway producer must match repository, ref, commit and trusted workflow")
    return run


def select_run(explicit):
    repo = os.environ["GITHUB_REPOSITORY"]
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    ref = os.environ["GITHUB_REF_NAME"]
    if explicit:
        return verify_run(api(f"repos/{repo}/actions/runs/{int(explicit)}"), commit, ref)
    query = urlencode(dict(head_sha=commit, branch=ref, per_page=100))
    candidates = api(f"repos/{repo}/actions/workflows/release-gateway.yml/runs?{query}")["workflow_runs"]
    for run in candidates:
        if (run["conclusion"] not in ("cancelled", "failure", "timed_out", "skipped", "action_required")
                and run.get("display_title") != WARMUP_RUN):
            return verify_run(run, commit, ref)
    raise ValueError("No matching Gateway run; start Release Gateway on this ref first, then select its run ID")


def wait_artifact(run, name):
    repo = os.environ["GITHUB_REPOSITORY"]
    while True:
        # Check artifacts before terminal state: another platform may fail after
        # this platform's exact inputs were successfully retained.
        rows = json.loads(subprocess.check_output([
            "gh", "api", "--paginate", "--slurp",
            f"repos/{repo}/actions/runs/{run['id']}/artifacts?per_page=100"], text=True))
        matches = [a for page in rows for a in page["artifacts"] if a["name"] == name and not a["expired"]]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise ValueError("Ambiguous Gateway artifact")
        current = api(f"repos/{repo}/actions/runs/{run['id']}")
        if current["status"] == "completed":
            raise ValueError(f"Gateway run {run['id']} ended ({current['conclusion']}) without {name}")
        print(f"Waiting for {name} from Gateway run {run['id']} ({current['status']})", flush=True)
        time.sleep(30)  # No build deadline: a cold release can legitimately take hours.


def download(run, name, out):
    artifact = wait_artifact(run, name)
    subprocess.run(["gh", "run", "download", str(run["id"]), "--repo", os.environ["GITHUB_REPOSITORY"],
                    "--name", name, "--dir", str(out)], check=True)
    return artifact


def restore_gateway(inputs, binary, target, run_id, commit, catalog):
    metadata = json.loads((inputs / "inputs.json").read_text())
    if (metadata["commit"] != commit or str(metadata["run_id"]) != str(run_id)
            or metadata["target"] != target or metadata.get("catalog_sha256") != SYMBOLS["digest"](catalog)
            or len(metadata["binaries"]) != 1):
        raise ValueError("Gateway input provenance/catalog mismatch")
    entry = metadata["binaries"][0]
    expected_name = "pioneer.exe" if target.endswith("windows-msvc") else "pioneer"
    if entry["filename"] != expected_name:
        raise ValueError("Unexpected Gateway executable")
    wanted = "binaries/" + expected_name
    records = [row for row in metadata["files"] if row["path"] == wanted]
    if len(records) != 1 or records[0]["sha256"] != entry["sha256"]:
        raise ValueError("Gateway binary inventory mismatch")
    binary.parent.mkdir(parents=True, exist_ok=True)
    try:
        with tarfile.open(inputs / "inputs.tar.gz", "r|gz") as archive:
            for member in archive:
                if member.name != wanted:
                    continue
                if not member.isfile() or member.size != records[0]["bytes"] or member.size > 2 * 1024**3:
                    raise ValueError("Invalid Gateway binary member")
                digest = hashlib.sha256()
                with archive.extractfile(member) as source, binary.open("wb") as output:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        output.write(chunk)
                        digest.update(chunk)
                if binary.stat().st_size != member.size or digest.hexdigest() != entry["sha256"]:
                    raise ValueError("Gateway binary checksum mismatch")
                break
            else:
                raise ValueError("Gateway binary missing from input archive")
        data = binary.read_bytes()
        if SYMBOLS["native"]["identity"](data) != (entry["format"], entry["module_id"]):
            raise ValueError("Gateway module mismatch")
        build = SYMBOLS["native"]["build_manifest"](data)
        if build["revision"] != commit or build["target"] != target or "COMPUTER_USE" not in build["features"]:
            raise ValueError("Desktop requires the exact computer-use Gateway")
        binary.chmod(0o755)
    except BaseException:
        binary.unlink(missing_ok=True)
        raise
    (inputs / "reuse.json").write_text(json.dumps(dict(schema_version=1, run_id=str(run_id),
        commit=commit, target=target, variant="computer-use", module_id=entry["module_id"],
        binary_sha256=entry["sha256"]), indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("catalog", "gateway"))
    parser.add_argument("--run-id", default="")
    parser.add_argument("--target")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--binary", type=Path)
    args = parser.parse_args()
    run = select_run(args.run_id)
    if args.mode == "catalog":
        download(run, "pioneer-release-model-catalog", args.out)
        json.loads((args.out / "catalog.json").read_text())
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            output.write(f"run_id={run['id']}\n")
    else:
        if not args.target or not args.binary:
            parser.error("gateway requires --target and --binary")
        download(run, f"symbol-inputs-gateway-{args.target}-computer-use", args.out)
        restore_gateway(args.out, args.binary, args.target, run["id"], run["head_sha"],
                        Path(os.environ["PIONEER_BUNDLED_MODEL_CATALOG"]))


if __name__ == "__main__":
    main()
