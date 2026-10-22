#!/usr/bin/env python3
"""Compile Desktop and its updater independently of the Gateway build."""
import argparse
import os
from pathlib import Path
import subprocess
import sys


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True)
    args = parser.parse_args()
    for package in ("pioneer-desktop", "pioneer-app-updater"):
        subprocess.run(["cargo", "build", "--locked", "--release", "-p", package,
                        "--target", args.target], check=True)
    suffix = ".exe" if args.target.endswith("windows-msvc") else ""
    binaries = [str(Path("target") / args.target / "release" / (name + suffix))
                for name in ("pioneer-app", "pioneer-app-updater")]
    subprocess.run([sys.executable, str(Path(__file__).with_name("symbol-inputs.py")),
                    "export", "--target", args.target,
                    os.environ["PIONEER_SYMBOL_INPUTS_DIR"], *binaries], check=True)
