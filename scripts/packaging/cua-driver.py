#!/usr/bin/env python3
"""Build the pinned local daemon and package the Gateway-owned CUA payload.

This script is a release producer, never an installer or a daemon launcher.
The separate upstream workspace keeps its original lockfile and relative deps.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import plistlib
import shutil
import subprocess
import sys
import tempfile
import zipfile

REVISION = "688c5fd62a9f13dddc309970c948df494e7de33f"
VERSION = "0.38.0"
BUNDLE_ID = "ai.pioneer.gateway"
TARGETS = {
    "x86_64-apple-darwin", "aarch64-apple-darwin",
    "x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu",
    "x86_64-pc-windows-msvc", "aarch64-pc-windows-msvc",
}


def build(target):
    source = Path("target/cua-source") / REVISION
    if not source.exists():
        source.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(["git", "init", str(source)], check=True)
        subprocess.run(["git", "-C", str(source), "remote", "add", "origin",
                        "https://github.com/trycua/cua.git"], check=True)
        subprocess.run(["git", "-C", str(source), "fetch", "--depth=1", "origin", REVISION], check=True)
        subprocess.run(["git", "-C", str(source), "checkout", "--detach", REVISION], check=True)
    actual = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    dirty = subprocess.check_output(["git", "-C", str(source), "status", "--porcelain"], text=True)
    if actual != REVISION or dirty:
        raise ValueError("CUA source must be the clean immutable pinned revision")
    workspace = source / "libs/cua-driver/rust"
    command = ["cargo", "build", "--locked", "--release", "-p", "cua-driver", "--target", target]
    if "linux" in target:
        command += ["--features", "portal-input,portal-capture"]
    if "windows" in target:
        command += ["-p", "cua-driver-uia"]
    environment = os.environ.copy()
    environment["CARGO_TARGET_DIR"] = str(Path("target/cua-build").resolve())
    # Pioneer owns its telemetry. Never compile a daemon with an inherited DSN.
    for name in ("PIONEER_SENTRY_DSN", "PIONEER_DESKTOP_SENTRY_DSN"):
        environment.pop(name, None)
    subprocess.run(command, cwd=workspace, env=environment, check=True)
    suffix = ".exe" if "windows" in target else ""
    return source, Path(environment["CARGO_TARGET_DIR"]) / target / "release" / ("cua-driver" + suffix)


def package(target, binary, archive):
    if target not in TARGETS or archive.suffix != ".zip":
        raise ValueError("A supported target and a .zip Gateway asset are required")
    source, driver = build(target)
    with tempfile.TemporaryDirectory(prefix="pioneer-cua-package-") as temporary:
        root = Path(temporary)
        suffix = ".exe" if "windows" in target else ""
        shutil.copy2(binary, root / ("pioneer" + suffix))
        payload = root / "cua"
        payload.mkdir()
        if "apple-darwin" in target:
            app = payload / "Pioneer Gateway.app"
            contents = app / "Contents"
            (contents / "MacOS").mkdir(parents=True)
            (contents / "Helpers").mkdir()
            gateway = contents / "MacOS/pioneer"
            daemon = contents / "Helpers/cua-driver"
            shutil.copy2(binary, gateway)
            with (contents / "Info.plist").open("wb") as output:
                plistlib.dump(dict(CFBundleIdentifier=BUNDLE_ID, CFBundleName="Pioneer Gateway",
                    CFBundleExecutable="pioneer", CFBundlePackageType="APPL",
                    CFBundleVersion=VERSION, LSUIElement=True, LSMinimumSystemVersion="13.0"), output)
            if target == "x86_64-apple-darwin":
                libraries = list(Path(os.environ["PIONEER_ONNXRUNTIME_DYLIB_DIR"]).glob("libonnxruntime*.dylib"))
                if not libraries:
                    raise ValueError("Gateway ONNX Runtime dylibs missing")
                for library in libraries:
                    shutil.copy2(library, root / library.name)
                    shutil.copy2(library, gateway.parent / library.name)
        else:
            daemon = payload / ("cua-driver" + suffix)
        shutil.copy2(driver, daemon)
        daemon.chmod(0o755)
        if "windows" in target:
            shutil.copy2(driver.with_name("cua-driver-uia.exe"), payload / "cua-driver-uia.exe")
        if "linux" in target:
            # Upstream's optional compositor extensions are installed by the
            # operator in the Gateway session; no privileged plugin is installed.
            shutil.copytree(source / "libs/cua-driver/wayland-helper", payload / "wayland-helper")
        shutil.copy2(source / "LICENSE.md", payload / "LICENSE.cua-driver.md")
        shutil.copy2(source / "libs/cua-driver/rust/THIRD_PARTY_NOTICES.md", payload / "THIRD_PARTY_NOTICES.md")
        manifest = dict(schema_version=1, repository="https://github.com/trycua/cua",
            revision=REVISION, version=VERSION, target=target, permission_owner="gateway",
            upstream_lock_sha256=hashlib.sha256((source / "libs/cua-driver/rust/Cargo.lock").read_bytes()).hexdigest(),
            daemon=daemon.relative_to(payload).as_posix(),
            sha256=hashlib.sha256(daemon.read_bytes()).hexdigest())
        (payload / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        if "apple-darwin" in target:
            identity = os.environ.get("MACOS_DESKTOP_SIGN_IDENTITY", "")
            if not identity:
                raise ValueError("Gateway CUA app requires a stable macOS signing identity")
            for executable in [daemon, *gateway.parent.glob("*.dylib"), gateway]:
                subprocess.run(["codesign", "--force", "--timestamp", "--options", "runtime",
                                "--sign", identity, str(executable)], check=True)
            subprocess.run(["codesign", "--force", "--timestamp", "--options", "runtime",
                            "--sign", identity, str(app)], check=True)
            subprocess.run(["codesign", "--verify", "--deep", "--strict", str(app)], check=True)
            key_id = os.environ.get("APPLE_NOTARIZATION_KEY_ID", "")
            issuer = os.environ.get("APPLE_NOTARIZATION_ISSUER_ID", "")
            key = os.environ.get("APPLE_NOTARIZATION_KEY", "")
            if not key and os.environ.get("APPLE_NOTARIZATION_KEY_BASE64"):
                key = base64.b64decode(os.environ["APPLE_NOTARIZATION_KEY_BASE64"], validate=True).decode()
            if not key_id or not issuer or not key:
                raise ValueError("Standalone Gateway app requires notarization credentials")
            key_file = root / "notary-key.p8"
            key_file.write_text(key)
            key_file.chmod(0o600)
            notary_archive = root / "gateway-notary.zip"
            try:
                subprocess.run(["ditto", "-c", "-k", "--keepParent", str(app), str(notary_archive)], check=True)
                response = subprocess.check_output(["xcrun", "notarytool", "submit", str(notary_archive),
                    "--key", str(key_file), "--key-id", key_id, "--issuer", issuer,
                    "--wait", "--output-format", "json"], text=True)
                if json.loads(response).get("status") != "Accepted":
                    raise ValueError("Gateway app notarization was not accepted")
                subprocess.run(["xcrun", "stapler", "staple", str(app)], check=True)
            finally:
                key_file.unlink(missing_ok=True)
                notary_archive.unlink(missing_ok=True)
            if os.environ.get("PIONEER_SYMBOL_INPUTS_DIR"):
                subprocess.run([sys.executable, str(Path(__file__).with_name("symbol-inputs.py")),
                    "record", str(gateway),
                    str(Path(os.environ["PIONEER_SYMBOL_INPUTS_DIR"]) / "packaged.json")], check=True)
            # Record the final signed daemon bytes, not its pre-signing digest.
            manifest["sha256"] = hashlib.sha256(daemon.read_bytes()).hexdigest()
            (payload / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        archive.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as output:
            for entry in sorted(root.rglob("*")):
                if entry.is_file():
                    output.write(entry, entry.relative_to(root).as_posix())
        with zipfile.ZipFile(archive) as check:
            required = {"pioneer" + suffix, "cua/manifest.json", "cua/LICENSE.cua-driver.md",
                        "cua/THIRD_PARTY_NOTICES.md", "cua/" + manifest["daemon"]}
            if not required.issubset(check.namelist()):
                raise ValueError("Gateway archive is missing the local CUA payload")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True, choices=sorted(TARGETS))
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--archive", required=True, type=Path)
    arguments = parser.parse_args()
    package(arguments.target, arguments.binary, arguments.archive)
