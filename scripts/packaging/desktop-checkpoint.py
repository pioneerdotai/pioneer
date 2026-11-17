#!/usr/bin/env python3
"""Retain exact signed Desktop composition for Apple retries, without rebuilding."""
import argparse
import json
import os
from pathlib import Path
import runpy
import shutil

RUNTIME = runpy.run_path(str(Path(__file__).with_name("runtime-inputs.py")))


def checkpoint(target, work, mode):
    suffix = ".exe" if "windows" in target else ""
    compiled = Path("target") / target / "release"
    runtime = Path(os.environ["PIONEER_GATEWAY_RUNTIME_DIR"])
    inputs = dict(context=RUNTIME["context"](target, "desktop"),
                  binaries={name: RUNTIME["digest"](compiled / (name + suffix))
                            for name in ("pioneer-app", "pioneer-app-updater")},
                  gateway=RUNTIME["digest"](runtime / "provenance.json"),
                  signing_identity=os.environ.get("MACOS_DESKTOP_SIGN_IDENTITY"),
                  dmg_identity=os.environ.get("MACOS_DMG_SIGN_IDENTITY"),
                  url_scheme=os.environ.get("PIONEER_APP_URL_SCHEME", "pioneer"))
    # Consumer retries can belong to a later attempt of the same release run.
    inputs["context"].pop("run_attempt")
    if mode == "prepare":
        marker = work / "building-inputs.json"
        if marker.exists():
            if (json.loads(marker.read_text()) != inputs
                    or (work.parent / "desktop/submission.json").exists()):
                raise ValueError("Incomplete Desktop staging belongs to other inputs or an Apple submission")
            # Only our marked, pre-submit staging can be discarded. Accepted or
            # uncertain submissions always retain their exact signed checkpoint.
            shutil.rmtree(work)
            work.mkdir()
        elif any(work.iterdir()):
            raise ValueError("Unowned Desktop staging cannot be reused")
        marker.write_text(json.dumps(inputs, indent=2) + "\n")
        return
    app = work / "Pioneer.app"
    files = {path.relative_to(app).as_posix(): RUNTIME["digest"](path)
             for path in sorted(app.rglob("*")) if path.is_file()
             and path.relative_to(app).as_posix() != "Contents/CodeResources"}
    if not files or any(p.is_symlink() for p in app.rglob("*")):
        raise ValueError("Signed Desktop checkpoint is empty or contains links")
    value = dict(schema_version=1, inputs=inputs, signed_files=files,
                 dmg_sha256=RUNTIME["digest"](work / "signed.dmg"))
    receipt = work / "checkpoint.json"
    if mode == "verify":
        if json.loads(receipt.read_text()) != value:
            raise ValueError("Signed Desktop checkpoint differs from this release's exact inputs")
    else:
        with receipt.open("x") as output:
            output.write(json.dumps(value, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "record", "verify"))
    parser.add_argument("--target", required=True)
    parser.add_argument("--work", required=True, type=Path)
    args = parser.parse_args()
    checkpoint(args.target, args.work, args.mode)
