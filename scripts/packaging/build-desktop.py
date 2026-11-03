#!/usr/bin/env python3
"""Compile Desktop/updater independently and retain inputs before signing."""
import argparse
from pathlib import Path
import runpy
import subprocess
import sys

RUNTIME = runpy.run_path(str(Path(__file__).with_name("runtime-inputs.py")))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True, choices=sorted(RUNTIME["PLATFORMS"]))
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.out.exists():
        raise ValueError(f"Build output already exists: {args.out}")
    for package in ("pioneer-desktop", "pioneer-app-updater"):
        subprocess.run(["cargo", "build", "--locked", "--release", "-p", package,
                        "--target", args.target], check=True)
    suffix = ".exe" if args.target.endswith("windows-msvc") else ""
    binaries = [Path("target") / args.target / "release" / (name + suffix)
                for name in ("pioneer-app", "pioneer-app-updater")]
    args.out.mkdir(parents=True, exist_ok=False)
    subprocess.run([sys.executable, str(Path(__file__).with_name("symbol-inputs.py")),
                    "export", "--target", args.target, str(args.out / "symbol-inputs"),
                    *map(str, binaries)], check=True)
    RUNTIME["export"](args.out / "runtime", args.target, "desktop", binaries,
                      args.out / "symbol-inputs/inputs.json")
