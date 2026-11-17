#!/usr/bin/env python3
"""Assemble Gateway assets from retained inputs; never compile or download."""
import argparse
import gzip
import json
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import sys
import tempfile
import zipfile

RUNTIME = runpy.run_path(str(Path(__file__).with_name("runtime-inputs.py")))
CUA = runpy.run_path(str(Path(__file__).with_name("cua-driver.py")))
SYMBOLS = Path(__file__).with_name("symbol-inputs.py")


def package(args):
    if args.asset != RUNTIME["asset_name"](args.target, args.variant):
        raise ValueError("Gateway asset name does not match target and variant")
    suffix = ".exe" if "windows" in args.target else ""
    name = "pioneer" + suffix
    metadata = RUNTIME["read"](args.inputs, args.target, args.variant, [name])
    if args.variant == "computer-use" and args.prepared is None:
        raise ValueError("CU packaging requires --prepared verified CUA inputs")
    args.out.mkdir(parents=True, exist_ok=False)
    assets = args.out / "assets"
    assets.mkdir()
    symbols = args.out / "symbols"
    symbols.mkdir()
    shutil.copy2(args.inputs / "inputs.json", symbols / "inputs.json")
    archive = assets / args.asset
    with tempfile.TemporaryDirectory(prefix="pioneer-gateway-") as temporary:
        work = Path(temporary)
        binary = work / name
        shutil.copy2(args.inputs / name, binary)
        binary.chmod(0o755)
        if "apple-darwin" in args.target:
            subprocess.run(["strip", "-S", str(binary)], check=True)
        elif "linux" in args.target:
            subprocess.run(["strip", "--strip-debug", str(binary)], check=True)
        elif "windows" in args.target:
            subprocess.run(["pwsh", "-NoProfile", "-File",
                            str(Path(__file__).with_name("sign-windows.ps1")),
                            "-Path", str(binary)], check=True)
        if args.variant == "computer-use":
            # Intel libraries come from the exact compile producer, never from
            # the signing runner's independently selected installation.
            if args.target == "x86_64-apple-darwin":
                os.environ["PIONEER_ONNXRUNTIME_DYLIB_DIR"] = str(args.inputs.resolve())
            CUA["package"](args.target, binary, args.prepared, archive, args.notary_log_dir)
            CUA["validate_archive"](args.target, archive,
                                     verify_signatures="apple-darwin" in args.target)
            runtime = args.out / "runtime"
            runtime.mkdir()
            shutil.copy2(archive, runtime / archive.name)
            # Record the executable actually delivered inside the final archive.
            # Desktop receives only that archive and its hash-bound metadata.
            entry = ("cua/Pioneer Gateway.app/Contents/MacOS/pioneer"
                     if "apple-darwin" in args.target else name)
            with zipfile.ZipFile(archive) as complete:
                binary.write_bytes(complete.read(entry))
            data = binary.read_bytes()
            if (RUNTIME["NATIVE"]["identity"](data) !=
                    (metadata["binaries"][0]["format"], metadata["binaries"][0]["module_id"])
                    or RUNTIME["NATIVE"]["build_manifest"](data) !=
                    RUNTIME["NATIVE"]["build_manifest"]((args.inputs / name).read_bytes())):
                raise ValueError("Packaged Gateway differs from the retained compiler module/build")
            RUNTIME["check_build"](binary, data, metadata)
            (runtime / "SHA256SUMS").write_text(
                f"sha256:{RUNTIME['digest'](archive)} {archive.name}\n")
            shutil.copy2(args.inputs / "inputs.json", runtime / "inputs.json")
            RUNTIME["write"](runtime, args.target, args.variant, [],
                             stage="gateway-final", source=metadata,
                             gateway_asset=archive.name, gateway_sha256=RUNTIME["digest"](archive))
            packaged_binary = binary
        else:
            if archive.suffix == ".gz":
                with binary.open("rb") as source, gzip.open(archive, "wb", compresslevel=9) as destination:
                    shutil.copyfileobj(source, destination)
            else:
                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED,
                                     compresslevel=9) as destination:
                    destination.write(binary, binary.name)
                    if args.target == "x86_64-apple-darwin":
                        libraries = sorted(args.inputs.glob("libonnxruntime*.dylib"))
                        if not libraries:
                            raise ValueError("Gateway Intel ONNX Runtime inputs missing")
                        for library in libraries:
                            destination.write(library, library.name)
            packaged_binary = binary
        subprocess.run([sys.executable, str(SYMBOLS), "record", str(packaged_binary),
                        str(symbols / "packaged.json")], check=True)
    # Publication verifies the exact asset inventory before opening the draft.
    (assets / (args.asset + ".provenance.json")).write_text(json.dumps(dict(
        schema_version=1, source=metadata, asset=args.asset,
        sha256=RUNTIME["digest"](archive)), indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True, choices=sorted(RUNTIME["PLATFORMS"]))
    parser.add_argument("--variant", required=True, choices=("default", "computer-use"))
    parser.add_argument("--asset", required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--prepared", type=Path)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--notary-log-dir", type=Path, default=Path("notary-logs/gateway"))
    package(parser.parse_args())
