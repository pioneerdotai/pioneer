#!/usr/bin/env python3
"""Build and retain the Gateway without mutating Cargo's cached executable."""
import argparse
import gzip
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
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


def compatibility_archive(target, bridge, package, archive):
    """The real pre-migration extractor understands only these outer members."""
    if bridge.stat().st_size > 512 * 1024 * 1024:
        raise ValueError("Legacy CU executable exceeds installer binary budget")
    if archive.suffix == ".gz":
        with bridge.open("rb") as source_file, gzip.open(archive, "wb", compresslevel=9) as output:
            shutil.copyfileobj(source_file, output)
    elif archive.suffix == ".zip":
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED,
                             compresslevel=9) as output:
            # Exactly one executable basename: legacy extraction flattens paths.
            output.write(bridge, "pioneer.exe" if "windows" in target else "pioneer")
            if target == "x86_64-apple-darwin":
                # The old extractor requires a genuine ONNX companion, but writes
                # it into live bin before admission. Never overwrite a live name.
                with zipfile.ZipFile(package) as complete:
                    libraries = [name for name in complete.namelist()
                                 if "/" not in name and name.startswith("libonnxruntime")
                                 and name.endswith(".dylib")]
                    if not libraries:
                        raise ValueError("Legacy Intel CU bridge requires ONNX companion")
                    library = complete.read(libraries[0])
                    output.writestr("libonnxruntime.pioneer-cu-" +
                                    hashlib.sha256(library).hexdigest() + ".dylib", library)
    else:
        raise ValueError("Legacy CU bridge requires a gzip executable or ZIP")


def build(args):
    platforms = {
        "x86_64-unknown-linux-gnu": ("linux-x86_64", "gz"),
        "aarch64-unknown-linux-gnu": ("linux-aarch64", "gz"),
        "aarch64-apple-darwin": ("macos-aarch64", "gz"),
        "x86_64-apple-darwin": ("macos-x86_64", "zip"),
        "x86_64-pc-windows-msvc": ("windows-x86_64", "zip"),
        "aarch64-pc-windows-msvc": ("windows-aarch64", "zip"),
    }
    platform, extension = platforms[args.target]
    expected = (f"pioneer-gateway-{platform}-computer-use-package.zip" if args.variant == "computer-use"
                else f"pioneer-gateway-{platform}.{extension}")
    if args.asset != expected:
        raise ValueError(f"Gateway variant requires asset {expected}")
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
    inputs = args.out / "symbol-inputs"
    started = time.monotonic()
    subprocess.run([sys.executable, str(SYMBOLS), "export", "--target", args.target, "--variant", args.variant,
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
    if args.variant == "computer-use":
        packaging_environment = os.environ.copy()
        packaging_environment["PIONEER_SYMBOL_INPUTS_DIR"] = str(inputs.resolve())
        subprocess.run([sys.executable, str(Path(__file__).with_name("cua-driver.py")),
                        "--target", args.target, "--binary", str(binary),
                        "--archive", str(assets / args.asset)], env=packaging_environment, check=True)
        # Compile the bridge only after the complete signed package exists.
        # Embedding before signing avoids executable overlays and hash cycles.
        bridge_environment = environment.copy()
        bridge_environment["PIONEER_LEGACY_CU_PACKAGE"] = str((assets / args.asset).resolve())
        subprocess.run(["cargo", "build", "--locked", "--release", "-p", "pioneer-cli",
                        "--target", args.target, "--bin", "pioneer-cu-update",
                        "--features", "legacy-cu-bootstrap"], env=bridge_environment, check=True)
        bridge_source = source.with_name("pioneer-cu-update" + suffix)
        bridge_inputs = inputs / "compatibility"
        subprocess.run([sys.executable, str(SYMBOLS), "export", "--target", args.target,
                        "--variant", args.variant, str(bridge_inputs), str(bridge_source)], check=True)
        bridge = args.out / bridge_source.name
        shutil.copy2(bridge_source, bridge)
        if "apple-darwin" in args.target:
            subprocess.run(["strip", "-S", str(bridge)], check=True)
            subprocess.run([sys.executable, str(Path(__file__).with_name("cua-driver.py")),
                            "--target", args.target, "--binary", str(bridge),
                            "--sign-bootstrap"], check=True)
        elif "linux" in args.target:
            subprocess.run(["strip", "--strip-debug", str(bridge)], check=True)
        subprocess.run([sys.executable, str(SYMBOLS), "record", str(bridge),
                        str(bridge_inputs / "packaged.json")], check=True)
        compatibility_archive(args.target, bridge, assets / args.asset,
                              assets / f"pioneer-gateway-{platform}-computer-use.{extension}")

    elif args.asset.endswith(".gz"):
        with binary.open("rb") as source_file, gzip.open(assets / args.asset, "wb", compresslevel=9) as archive:
            shutil.copyfileobj(source_file, archive)
    else:
        with zipfile.ZipFile(assets / args.asset, "w", compression=zipfile.ZIP_DEFLATED,
                             compresslevel=9) as archive:
            archive.write(binary, binary.name)
            if args.target == "x86_64-apple-darwin":
                libraries = list(Path(os.environ["PIONEER_ONNXRUNTIME_DYLIB_DIR"]).glob("libonnxruntime*.dylib"))
                if not libraries:
                    raise ValueError("Gateway ONNX Runtime dylibs missing")
                for library in libraries:
                    archive.write(library, library.name)
    print(f"Gateway packaging completed in {time.monotonic() - started:.1f}s", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True)
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
