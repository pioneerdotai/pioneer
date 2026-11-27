#!/usr/bin/env python3
"""Prepare the stock, pinned LiveKit server for the Gateway runtime."""
import argparse
import os
import platform
from pathlib import Path
import runpy
import shutil
import subprocess
import tempfile

VERSION = "v1.13.11"
REVISION = "385aa40f9292fe1db8caad87301e4b285b7c198a"
UPSTREAM = "https://github.com/livekit/livekit.git"
PLATFORMS = {
    "aarch64-apple-darwin": ("darwin", "arm64"),
    "x86_64-apple-darwin": ("darwin", "amd64"),
    "aarch64-unknown-linux-gnu": ("linux", "arm64"),
    "x86_64-unknown-linux-gnu": ("linux", "amd64"),
    "aarch64-pc-windows-msvc": ("windows", "arm64"),
    "x86_64-pc-windows-msvc": ("windows", "amd64"),
}
NATIVE = runpy.run_path(str(Path(__file__).with_name("cua-driver.py")))


def executable(target):
    return "livekit-server.exe" if PLATFORMS[target][0] == "windows" else "livekit-server"


def validate(target, path):
    program = path / executable(target)
    license_path = path / "LICENSE"
    if {item.name for item in path.iterdir()} != {program.name, license_path.name}:
        raise ValueError("Unexpected LiveKit runtime inventory")
    for item in (program, license_path):
        if not item.is_file() or item.is_symlink():
            raise ValueError("LiveKit runtime requires regular files")
    NATIVE["architecture"](program, target)


def prepare(target, out):
    goos, goarch = PLATFORMS[target]
    if goos == "darwin" and platform.system() != "Darwin":
        raise ValueError("LiveKit's macOS CPU statistics require CGO and the macOS SDK")
    out = out.resolve()
    with tempfile.TemporaryDirectory(prefix="pioneer-livekit-") as directory:
        source = Path(directory) / "source"
        subprocess.run(["git", "clone", "--depth=1", "--branch", VERSION, UPSTREAM, source], check=True)
        revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip()
        if revision != REVISION:
            raise ValueError("Unexpected LiveKit source revision")
        staging = Path(directory) / "livekit"
        staging.mkdir()
        env = dict(os.environ, CGO_ENABLED="1" if goos == "darwin" else "0",
                   GOOS=goos, GOARCH=goarch, GOTOOLCHAIN="local")
        subprocess.run(["go", "build", "-mod=readonly", "-trimpath", "-buildvcs=false",
                        "-ldflags=-s -w", "-o", staging / executable(target), "./cmd/server"],
                       cwd=source, env=env, check=True)
        shutil.copy2(source / "LICENSE", staging / "LICENSE")
        (staging / executable(target)).chmod(0o755)
        validate(target, staging)
        shutil.copytree(staging, out)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True, choices=sorted(PLATFORMS))
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    prepare(args.target, args.out)
