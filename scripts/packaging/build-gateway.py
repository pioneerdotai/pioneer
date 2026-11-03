#!/usr/bin/env python3
"""Compile one Pioneer Gateway configuration and retain exact inputs before signing."""
import argparse
import os
from pathlib import Path
import runpy
import subprocess
import sys
import time

RUNTIME = runpy.run_path(str(Path(__file__).with_name("runtime-inputs.py")))
SYMBOLS = Path(__file__).with_name("symbol-inputs.py")


def configure(target):
    values = {}
    if target.endswith("windows-msvc"):
        values["BINDGEN_EXTRA_CLANG_ARGS"] = "--target=" + target
        if target.startswith("aarch64-"):
            values.update(CMAKE_GENERATOR="Ninja", CMAKE_C_COMPILER="clang-cl",
                          CMAKE_CXX_COMPILER="clang-cl", CMAKE_C_COMPILER_TARGET=target,
                          CMAKE_CXX_COMPILER_TARGET=target, CL="/EHsc", GGML_CCACHE="OFF")
    with open(os.environ["GITHUB_ENV"], "a") as output:
        for key, value in values.items():
            output.write(f"{key}={value}\n")


def build(args):
    expected = RUNTIME["asset_name"](args.target, args.variant)
    if args.asset != expected:
        raise ValueError(f"Gateway variant requires asset {expected}")
    if args.out.exists():
        raise ValueError(f"Build output already exists: {args.out}")
    command = ["cargo", "build", "--locked", "--release", "-p", "pioneer-cli",
               "--target", args.target, "--bin", "pioneer"]
    if args.variant == "computer-use":
        command += ["--features", "computer-use"]
    environment = os.environ.copy()
    if args.variant == "computer-use" and "apple-darwin" in args.target:
        environment["MACOSX_DEPLOYMENT_TARGET"] = "13.0"
    started = time.monotonic()
    subprocess.run(command, env=environment, check=True)
    print(f"Gateway compilation completed in {time.monotonic() - started:.1f}s", flush=True)
    suffix = ".exe" if args.target.endswith("windows-msvc") else ""
    source = Path("target") / args.target / "release" / ("pioneer" + suffix)
    args.out.mkdir(parents=True, exist_ok=False)
    subprocess.run([sys.executable, str(SYMBOLS), "export", "--target", args.target,
                    "--variant", args.variant, str(args.out / "symbol-inputs"), str(source)], check=True)
    RUNTIME["export"](args.out / "runtime", args.target, args.variant, [source],
                      args.out / "symbol-inputs/inputs.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True, choices=sorted(RUNTIME["PLATFORMS"]))
    parser.add_argument("--configure", action="store_true")
    parser.add_argument("--variant", choices=("default", "computer-use"), default="default")
    parser.add_argument("--asset")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    if args.configure:
        configure(args.target)
    else:
        if not args.asset or not args.out:
            parser.error("build requires --asset and --out")
        build(args)
