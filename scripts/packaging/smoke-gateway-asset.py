"""Installer fixture only: complete payload layout, deliberately unavailable CUA.

This is not a release producer or a signed/notarized macOS package. Smoke jobs
exercise installation and Gateway health, not driver readiness or OS grants.
"""
import argparse
import hashlib
import json
from pathlib import Path
import plistlib
import zipfile


def fixture(binary, archive, target):
    windows = "windows" in target
    macos = "apple-darwin" in target
    executable = "pioneer.exe" if windows else "pioneer"
    daemon = ("Pioneer Gateway.app/Contents/Helpers/cua-driver" if macos else
              "cua-driver.exe" if windows else "cua-driver")
    # Invalid executable on Windows; explicit non-ready executable on Unix.
    driver = b"CUA unavailable in installer fixture\n" if windows else b"#!/bin/sh\nexit 1\n"
    members = {
        executable: (binary.read_bytes(), 0o755),
        "cua/" + daemon: (driver, 0o755),
        "cua/LICENSE.cua-driver.md": (b"Synthetic installer fixture; no upstream binary.\n", 0o644),
        "cua/THIRD_PARTY_NOTICES.md": (b"Synthetic installer fixture.\n", 0o644),
    }
    if windows:
        members["cua/cua-driver-uia.exe"] = (driver, 0o755)
    if macos:
        app = "cua/Pioneer Gateway.app/Contents/"
        members[app + "MacOS/pioneer"] = (binary.read_bytes(), 0o755)
        members[app + "Info.plist"] = (plistlib.dumps(dict(
            CFBundleIdentifier="ai.pioneer.gateway", CFBundleExecutable="pioneer",
            CFBundleName="Pioneer Gateway", CFBundlePackageType="APPL")), 0o644)
    manifest = dict(schema_version=1, repository="https://github.com/trycua/cua",
                    revision="688c5fd62a9f13dddc309970c948df494e7de33f",
                    version="0.38.0", target=target, permission_owner="gateway",
                    daemon=daemon, sha256=hashlib.sha256(driver).hexdigest())
    members["cua/manifest.json"] = ((json.dumps(manifest) + "\n").encode(), 0o644)
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as output:
        for name, (data, mode) in members.items():
            info = zipfile.ZipInfo(name)
            info.create_system = 3
            info.external_attr = (0o100000 | mode) << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            output.writestr(info, data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--target", required=True)
    args = parser.parse_args()
    fixture(args.binary, args.archive, args.target)
