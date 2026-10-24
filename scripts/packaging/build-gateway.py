#!/usr/bin/env python3
"""Build and retain the Gateway without mutating Cargo's cached executable."""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

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
    command = ["cargo", "build", "--locked", "--release", "-p", "pioneer-cli",
               "--target", args.target]
    started = time.monotonic()
    subprocess.run(command, check=True)
    print(f"Gateway compilation completed in {time.monotonic() - started:.1f}s", flush=True)
    suffix = ".exe" if args.target.endswith("windows-msvc") else ""
    source = Path("target") / args.target / "release" / ("pioneer" + suffix)
    args.out.mkdir(parents=True, exist_ok=False)
    inputs = args.out / "symbol-inputs"
    started = time.monotonic()
    subprocess.run([sys.executable, str(SYMBOLS), "export", "--target", args.target,
                    str(inputs), str(source)], check=True)
    print(f"Gateway symbol input export completed in {time.monotonic() - started:.1f}s", flush=True)
    started = time.monotonic()
    binary = args.out / source.name
    shutil.copy2(source, binary)
    if "apple-darwin" in args.target:
        subprocess.run(["strip", "-S", str(binary)], check=True)
    elif "linux" in args.target:
        subprocess.run(["strip", "--strip-debug", str(binary)], check=True)
    subprocess.run([sys.executable, str(SYMBOLS), "record", str(binary),
                    str(inputs / "packaged.json")], check=True)
    assets = args.out / "assets"
    assets.mkdir()
    packaging_environment = os.environ.copy()
    packaging_environment["PIONEER_SYMBOL_INPUTS_DIR"] = str(inputs.resolve())
    subprocess.run([sys.executable, str(Path(__file__).with_name("cua-driver.py")),
                    "--target", args.target, "--binary", str(binary),
                    "--archive", str(assets / args.asset)], env=packaging_environment, check=True)
    print(f"Gateway packaging completed in {time.monotonic() - started:.1f}s", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True)
    parser.add_argument("--configure", action="store_true")
    parser.add_argument("--asset")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    if args.configure:
        configure(args.target)
    else:
        if not args.asset or not args.out:
            parser.error("build requires --asset and --out")
        build(args)
