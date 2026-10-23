#!/usr/bin/env python3
"""Restore completed Desktop/Gateway inputs from this release run; never poll or build."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import runpy
import subprocess
import tarfile

SYMBOLS = runpy.run_path(str(Path(__file__).with_name("symbol-inputs.py")))


def restore(inputs, names, target, commit, catalog):
    metadata = json.loads((inputs / "inputs.json").read_text())
    if (metadata["commit"] != commit or str(metadata["run_id"]) != os.environ["GITHUB_RUN_ID"]
            or metadata["target"] != target
            or metadata.get("catalog_sha256") != SYMBOLS["digest"](catalog)
            or {row["filename"] for row in metadata["binaries"]} != set(names)
            or len(metadata["binaries"]) != len(names)):
        raise ValueError("Release inputs must match this run, commit, target, catalog and binary inventory")
    binaries = {"binaries/" + row["filename"]: row for row in metadata["binaries"]}
    records = {row["path"]: row for row in metadata["files"]}
    output = Path("target") / target / "release"
    output.mkdir(parents=True, exist_ok=True)
    created = []
    seen = set()
    try:
        with tarfile.open(inputs / "inputs.tar.gz", "r|gz") as archive:
            for member in archive:
                if member.name not in binaries:
                    continue
                entry = binaries[member.name]
                row = records[member.name]
                if (member.name in seen or not member.isfile() or member.size != row["bytes"]
                        or row["sha256"] != entry["sha256"]):
                    raise ValueError("Invalid release executable member")
                binary = output / entry["filename"]
                digest = hashlib.sha256()
                with binary.open("xb") as destination:
                    created.append(binary)
                    with archive.extractfile(member) as source:
                        for chunk in iter(lambda: source.read(1024 * 1024), b""):
                            destination.write(chunk)
                            digest.update(chunk)
                if binary.stat().st_size != member.size or digest.hexdigest() != entry["sha256"]:
                    raise ValueError("Release executable checksum mismatch")
                data = binary.read_bytes()
                if SYMBOLS["native"]["identity"](data) != (entry["format"], entry["module_id"]):
                    raise ValueError("Release executable module mismatch")
                if entry["filename"] in ("pioneer", "pioneer.exe"):
                    build = SYMBOLS["native"]["build_manifest"](data)
                    if (build["revision"] != commit or build["target"] != target
                            or "COMPUTER_USE" not in build["features"]):
                        raise ValueError("Desktop requires this release's computer-use Gateway")
                binary.chmod(0o755)
                seen.add(member.name)
                # Binaries sort before objects; do not decompress multi-GiB symbol objects.
                if seen == set(binaries):
                    break
        if seen != set(binaries):
            raise ValueError("Release executable missing from input archive")
    except BaseException:
        for binary in created:
            binary.unlink(missing_ok=True)
        raise
    return metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    args = parser.parse_args()
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    catalog = Path(os.environ["PIONEER_BUNDLED_MODEL_CATALOG"])
    suffix = ".exe" if args.target.endswith("windows-msvc") else ""
    restore(args.inputs, [name + suffix for name in ("pioneer-app", "pioneer-app-updater")],
            args.target, commit, catalog)
    gateway = args.inputs / "gateway"
    metadata = restore(gateway, ["pioneer" + suffix], args.target, commit, catalog)
    entry = metadata["binaries"][0]
    (gateway / "reuse.json").write_text(json.dumps(dict(schema_version=1,
        run_id=str(metadata["run_id"]), commit=commit, target=args.target,
        variant="computer-use", module_id=entry["module_id"], binary_sha256=entry["sha256"]),
        indent=2) + "\n")


if __name__ == "__main__":
    main()
