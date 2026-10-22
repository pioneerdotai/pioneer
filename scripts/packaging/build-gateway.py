#!/usr/bin/env python3
"""Build and retain one Gateway variant without mutating Cargo's cached executable."""
import argparse
import gzip
import os
from pathlib import Path
import shutil
import subprocess
import sys
import zipfile

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
    if args.variant == "computer-use":
        command += ["--features", "computer-use"]
    subprocess.run(command, check=True)
    suffix = ".exe" if args.target.endswith("windows-msvc") else ""
    source = Path("target") / args.target / "release" / ("pioneer" + suffix)
    args.out.mkdir(parents=True, exist_ok=False)
    inputs = args.out / "symbol-inputs"
    subprocess.run([sys.executable, str(SYMBOLS), "export", "--target", args.target,
                    str(inputs), str(source)], check=True)
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
    if args.asset.endswith(".gz"):
        with binary.open("rb") as source_file, gzip.open(assets / args.asset, "wb", compresslevel=9) as archive:
            shutil.copyfileobj(source_file, archive)
    else:
        with zipfile.ZipFile(assets / args.asset, "w", compression=zipfile.ZIP_DEFLATED,
                             compresslevel=9) as archive:
            archive.write(binary, binary.name)
            if args.target == "x86_64-apple-darwin":
                libs = list(Path(os.environ["PIONEER_ONNXRUNTIME_DYLIB_DIR"]).glob("libonnxruntime*.dylib"))
                if not libs:
                    raise ValueError("ONNX Runtime dylibs missing")
                for lib in libs:
                    archive.write(lib, lib.name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True)
    parser.add_argument("--configure", action="store_true")
    parser.add_argument("--variant", choices=("default", "computer-use"))
    parser.add_argument("--asset")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    if args.configure:
        configure(args.target)
    else:
        if not args.variant or not args.asset or not args.out:
            parser.error("build requires --variant, --asset and --out")
        build(args)
