#!/usr/bin/env python3
"""Embed/check an approved Gateway runtime without changing its final ZIP bytes."""
import argparse
import json
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import tempfile
import zipfile

RUNTIME = runpy.run_path(str(Path(__file__).with_name("runtime-inputs.py")))
CUA = runpy.run_path(str(Path(__file__).with_name("cua-driver.py")))


def gateway(directory, target):
    suffix = ".exe" if "windows" in target else ""
    metadata = RUNTIME["read"](directory, target, "computer-use", [])
    asset = RUNTIME["asset_name"](target, "computer-use")
    if (metadata.get("stage") != "gateway-final" or metadata.get("gateway_asset") != asset
            or metadata.get("gateway_sha256") != RUNTIME["digest"](directory / asset)):
        raise ValueError("Desktop requires the approved final Gateway package")
    CUA["validate_archive"](target, directory / asset,
                            verify_signatures="apple-darwin" in target)
    expected = f"sha256:{metadata['gateway_sha256']} {asset}\n"
    if (directory / "SHA256SUMS").read_text() != expected:
        raise ValueError("Gateway runtime checksums differ from final package")
    with zipfile.ZipFile(directory / asset) as archive:
        entry = ("cua/Pioneer Gateway.app/Contents/MacOS/pioneer"
                 if "apple-darwin" in target else "pioneer" + suffix)
        data = archive.read(entry)
        originals = metadata["source"]["binaries"]
        if (len(originals) != 1 or originals[0]["filename"] != "pioneer" + suffix
                or RUNTIME["NATIVE"]["identity"](data) !=
                (originals[0]["format"], originals[0]["module_id"])):
            raise ValueError("Archived Gateway differs from the approved compiler module")
        RUNTIME["check_build"](Path("pioneer" + suffix), data, metadata)
    return metadata


def bundle_files(directory, target):
    return [RUNTIME["asset_name"](target, "computer-use"), "SHA256SUMS", "provenance.json"]


def verify_bundle(approved, destination, target):
    gateway(approved, target)
    names = bundle_files(approved, target)
    if set(names) != {p.name for p in destination.iterdir()}:
        raise ValueError("Desktop Gateway bundle inventory differs from approved inputs")
    for name in names:
        path = destination / name
        if path.is_symlink() or not path.is_file() or RUNTIME["digest"](path) != RUNTIME["digest"](approved / name):
            raise ValueError(f"Desktop changed approved Gateway payload: {name}")


def own_binaries(target):
    suffix = ".exe" if "windows" in target else ""
    output = Path("target") / target / "release"
    receipt = output / "desktop-inputs.json"
    if not receipt.is_file():
        raise ValueError("Missing prepared Desktop inputs; run build-desktop.py then release-inputs.py explicitly")
    metadata = json.loads(receipt.read_text())
    expected = RUNTIME["context"](target, "desktop")
    for key in ("schema_version", "commit", "run_id", "version", "target", "variant", "profile", "catalog_sha256"):
        if metadata.get(key) != expected[key]:
            raise ValueError(f"Desktop inputs have a different {key}")
    names = {name + suffix for name in ("pioneer-app", "pioneer-app-updater")}
    if {row["filename"] for row in metadata["binaries"]} != names or len(metadata["binaries"]) != 2:
        raise ValueError("Prepared Desktop executable inventory mismatch")
    for row in metadata["binaries"]:
        binary = output / row["filename"]
        if (binary.is_symlink() or not binary.is_file()
                or RUNTIME["digest"](binary) != row["sha256"]
                or RUNTIME["NATIVE"]["identity"](binary.read_bytes()) != (row["format"], row["module_id"])):
            raise ValueError(f"Prepared Desktop executable checksum/identity mismatch: {binary}")
        RUNTIME["check_build"](binary, binary.read_bytes(), metadata)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("preflight", "embed", "verify", "macos-app-zip"))
    parser.add_argument("--target", required=True, choices=sorted(RUNTIME["PLATFORMS"]))
    parser.add_argument("--runtime", type=Path, default=os.environ.get("PIONEER_GATEWAY_RUNTIME_DIR"))
    parser.add_argument("--destination", type=Path)
    args = parser.parse_args()
    if args.runtime is None:
        parser.error("--runtime or PIONEER_GATEWAY_RUNTIME_DIR with approved ready Gateway runtime is required")
    if args.mode == "preflight":
        own_binaries(args.target)
        gateway(args.runtime, args.target)
        return
    if args.destination is None:
        parser.error("--destination is required")
    if args.mode == "embed":
        gateway(args.runtime, args.target)
        args.destination.mkdir(parents=True, exist_ok=True)
        if any(args.destination.iterdir()):
            raise ValueError("Gateway destination must be empty")
        for name in bundle_files(args.runtime, args.target):
            shutil.copy2(args.runtime / name, args.destination / name)
        verify_bundle(args.runtime, args.destination, args.target)
    elif args.mode == "verify":
        verify_bundle(args.runtime, args.destination, args.target)
    else:
        # Validate delivered app.zip bytes independently of the staging check.
        gateway(args.runtime, args.target)
        prefix = "Pioneer.app/Contents/Resources/gateway/"
        with zipfile.ZipFile(args.destination) as archive:
            entries = archive.namelist()
            if len(entries) != len(set(entries)):
                raise ValueError("Duplicate Desktop app ZIP entries")
            for name in bundle_files(args.runtime, args.target):
                if archive.read(prefix + name) != (args.runtime / name).read_bytes():
                    raise ValueError("Delivered Desktop app ZIP changed approved Gateway bytes")
            # Read signatures from the delivered archive too; staging checks
            # alone do not establish the outer app's final signature or ticket.
            for name in entries:
                CUA["safe_name"](name)
        with tempfile.TemporaryDirectory(prefix="pioneer-app-verify-") as temporary:
            unpacked = Path(temporary) / "app"
            CUA["extract"](args.destination, unpacked)
            app = unpacked / "Pioneer.app"
            verify_bundle(args.runtime, app / "Contents/Resources/gateway", args.target)
            if os.environ.get("MACOS_DESKTOP_SIGN_IDENTITY"):
                for name in ("pioneer-app", "pioneer-app-updater"):
                    subprocess.run(["codesign", "--verify", "--strict",
                                    str(app / "Contents/MacOS" / name)], check=True)
                subprocess.run(["codesign", "--verify", "--deep", "--strict", str(app)], check=True)
            if os.environ.get("APPLE_NOTARIZATION_KEY_ID"):
                notary = runpy.run_path(str(Path(__file__).with_name("macos-notary.py")))
                notary["validate_ticket"](app)


if __name__ == "__main__":
    main()
