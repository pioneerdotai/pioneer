#!/usr/bin/env python3
"""Assemble Gateway assets from retained inputs; never compile or download."""
import argparse
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
POSTGRES = runpy.run_path(str(Path(__file__).with_name("postgres-runtime.py")))
LIVEKIT = runpy.run_path(str(Path(__file__).with_name("livekit-runtime.py")))
NOTARY = runpy.run_path(str(Path(__file__).with_name("macos-notary.py")))


def package_macos_default(args, binary, archive):
    identity = os.environ.get("MACOS_DESKTOP_SIGN_IDENTITY")
    if not identity:
        raise ValueError("macOS Gateway requires a Developer ID signing identity")
    libraries = sorted(args.inputs.glob("libonnxruntime*.dylib")) if args.target.startswith("x86_64") else []
    if args.target.startswith("x86_64") and not libraries:
        raise ValueError("Gateway Intel ONNX Runtime inputs missing")
    inputs = dict(target=args.target, signing_identity=identity,
                  gateway_sha256=RUNTIME["digest"](args.inputs / binary.name),
                  postgres=RUNTIME["file_inventory"](args.postgres),
                  livekit=RUNTIME["file_inventory"](args.livekit),
                  onnx={path.name: RUNTIME["digest"](path) for path in libraries})
    logs = args.notary_log_dir
    logs.mkdir(parents=True, exist_ok=True)
    retained = logs / "signed-payload.zip"
    state_path = logs / "package-inputs.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    if state and state.get("inputs") != inputs:
        raise ValueError("Retained Gateway package belongs to different signing inputs")
    if retained.exists():
        if state.get("sha256") != RUNTIME["digest"](retained):
            raise ValueError("Retained signed Gateway ZIP differs from its recorded checksum")
    else:
        if (logs / "submission.json").exists():
            raise ValueError("Retained notarization submission requires its original signed Gateway ZIP")
        with tempfile.TemporaryDirectory(prefix="gateway-default-", dir=logs) as temporary:
            work = Path(temporary)
            root = work / "payload"
            root.mkdir()
            shutil.copy2(binary, root / binary.name)
            postgres = root / "runtime/postgres"
            shutil.copytree(args.postgres, postgres)
            for program in POSTGRES["PROGRAMS"]:
                (postgres / "bin" / program).chmod(0o755)
            livekit = root / "runtime/livekit"
            shutil.copytree(args.livekit, livekit)
            (livekit / LIVEKIT["executable"](args.target)).chmod(0o755)
            for library in libraries:
                shutil.copy2(library, root / library.name)
            for executable in NOTARY["mach_o_files"](root):
                subprocess.run(["codesign", "--force", "--timestamp", "--options", "runtime",
                                "--sign", identity, str(executable)], check=True)
            NOTARY["verify_code_signatures"](root)
            staged_zip = work / "payload.zip"
            CUA["zip_tree"](root, staged_zip)
            NOTARY["write_json"](state_path, dict(inputs=inputs, sha256=RUNTIME["digest"](staged_zip)))
            staged_zip.replace(retained)
    accepted = NOTARY["notarize"](retained, [], logs, standalone_zip=True)
    shutil.copy2(retained, archive)
    return dict(id=accepted["id"], status=accepted["status"], sha256=accepted["final_sha256"])


def package(args):
    if args.asset != RUNTIME["asset_name"](args.target, args.variant):
        raise ValueError("Gateway asset name does not match target and variant")
    suffix = ".exe" if "windows" in args.target else ""
    name = "pioneer" + suffix
    metadata = RUNTIME["read"](args.inputs, args.target, args.variant, [name])
    if args.variant == "computer-use" and args.prepared is None:
        raise ValueError("CU packaging requires --prepared verified CUA inputs")
    POSTGRES["validate"](args.target, args.postgres)
    LIVEKIT["validate"](args.target, args.livekit)
    args.out.mkdir(parents=True, exist_ok=False)
    assets = args.out / "assets"
    assets.mkdir()
    symbols = args.out / "symbols"
    symbols.mkdir()
    shutil.copy2(args.inputs / "inputs.json", symbols / "inputs.json")
    archive = assets / args.asset
    notarization = None
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
        livekit = work / "livekit"
        shutil.copytree(args.livekit, livekit)
        livekit_executable = livekit / LIVEKIT["executable"](args.target)
        livekit_executable.chmod(0o755)
        if "windows" in args.target:
            subprocess.run(["pwsh", "-NoProfile", "-File",
                            str(Path(__file__).with_name("sign-windows.ps1")),
                            "-Path", str(livekit_executable)], check=True)
        if args.variant == "computer-use":
            # Intel libraries come from the exact compile producer, never from
            # the signing runner's independently selected installation.
            if args.target == "x86_64-apple-darwin":
                os.environ["PIONEER_ONNXRUNTIME_DYLIB_DIR"] = str(args.inputs.resolve())
            CUA["package"](args.target, binary, args.prepared, archive, args.notary_log_dir,
                           postgres=args.postgres, livekit=livekit)
            CUA["validate_archive"](args.target, archive,
                                     verify_signatures="apple-darwin" in args.target)
            runtime = args.out / "runtime"
            runtime.mkdir()
            shutil.copy2(archive, runtime / archive.name)
            # Record the executable actually delivered inside the final archive.
            # Desktop receives only that archive and its hash-bound metadata.
            entry = ("runtime/Pioneer Gateway.app/Contents/MacOS/pioneer"
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
        elif "apple-darwin" in args.target:
            notarization = package_macos_default(args, binary, archive)
            # Record the actual distribution-signed binary's native identity.
            with zipfile.ZipFile(archive) as complete:
                binary.write_bytes(complete.read(binary.name))
            data = binary.read_bytes()
            if RUNTIME["NATIVE"]["identity"](data) != (
                    metadata["binaries"][0]["format"], metadata["binaries"][0]["module_id"]):
                raise ValueError("Packaged Gateway differs from the retained compiler module")
            RUNTIME["check_build"](binary, data, metadata)
            packaged_binary = binary
        else:
            with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED,
                                 compresslevel=9) as destination:
                destination.write(binary, binary.name)
                for entry in sorted(livekit.iterdir()):
                    destination.write(entry, (Path("runtime/livekit") / entry.name).as_posix())
                for path in sorted(args.postgres.rglob("*")):
                    if path.is_symlink():
                        raise ValueError("PostgreSQL runtime archive requires regular files")
                    if path.is_file():
                        member = Path("runtime/postgres") / path.relative_to(args.postgres)
                        # GitHub artifact transfers flatten Unix modes.
                        entry = zipfile.ZipInfo.from_file(path, member.as_posix())
                        if path.parent == args.postgres / "bin":
                            entry.external_attr = (0o100755 << 16)
                        entry.compress_type = zipfile.ZIP_DEFLATED
                        with path.open("rb") as source, destination.open(entry, "w") as output:
                            shutil.copyfileobj(source, output)
            packaged_binary = binary
        subprocess.run([sys.executable, str(SYMBOLS), "record", str(packaged_binary),
                        str(symbols / "packaged.json")], check=True)
    # Publication verifies the exact asset inventory before opening the draft.
    receipt = dict(schema_version=1, source=metadata, asset=args.asset, sha256=RUNTIME["digest"](archive))
    if notarization is not None:
        receipt["notarization"] = notarization
    (assets / (args.asset + ".provenance.json")).write_text(json.dumps(receipt, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True, choices=sorted(RUNTIME["PLATFORMS"]))
    parser.add_argument("--variant", required=True, choices=("default", "computer-use"))
    parser.add_argument("--asset", required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--prepared", type=Path)
    parser.add_argument("--postgres", type=Path, required=True)
    parser.add_argument("--livekit", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--notary-log-dir", type=Path, default=Path("notary-logs/gateway"))
    package(parser.parse_args())
