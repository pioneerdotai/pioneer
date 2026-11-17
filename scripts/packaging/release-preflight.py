#!/usr/bin/env python3
"""Reject inconsistent release inputs before compiling any release binaries."""
import argparse
import os
from pathlib import Path, PurePosixPath
import re
import runpy
import tomllib
from urllib.parse import quote

TARGETS = {
    "x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu",
    "x86_64-apple-darwin", "aarch64-apple-darwin",
    "x86_64-pc-windows-msvc", "aarch64-pc-windows-msvc",
}
SEMVER = r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=sorted(TARGETS))
    parser.add_argument("--variant", choices=("default", "computer-use"))
    parser.add_argument("--signing", action="store_true")
    args = parser.parse_args()
    if args.variant and not args.target:
        parser.error("--variant requires --target")
    workspace = tomllib.loads(Path("Cargo.toml").read_text())
    version = workspace["workspace"]["package"]["version"]
    tag = os.environ["GITHUB_REF_NAME"]
    tagged_version = tag[1:] if tag.startswith(("v", "V")) else tag
    if not re.fullmatch(SEMVER, tagged_version) or tagged_version != version:
        raise ValueError(f"Release tag {tag!r} must match workspace version {version!r}")
    declaration = runpy.run_path("scripts/packaging/cua-driver.py")["release"]()
    if declaration.get("schema_version") != 1 or declaration.get("repository") != "https://github.com/trycua/cua":
        raise ValueError("Unsupported CUA release declaration/repository")
    release_tag = declaration["tag"]
    revision = declaration["revision"]
    if (release_tag != "cua-driver-rs-v" + declaration["version"]
            or not re.fullmatch(SEMVER, declaration["version"])
            or not re.fullmatch(r"[0-9a-f]{40}", revision)):
        raise ValueError("Invalid CUA release declaration")
    platforms = {"darwin-universal": "tar.gz", "linux-arm64": "tar.gz", "linux-x86_64": "tar.gz",
                 "windows-arm64": "zip", "windows-x86_64": "zip"}
    if set(declaration["assets"]) != set(platforms):
        raise ValueError("CUA declaration must cover exactly all five archives for six targets")
    for platform, extension in platforms.items():
        asset = declaration["assets"][platform]
        expected = f"cua-driver-rs-{declaration['version']}-{platform}-binary.{extension}"
        if asset.get("name") != expected or not re.fullmatch(r"[0-9a-f]{64}", asset.get("sha256", "")):
            raise ValueError("CUA release asset name/hash is inconsistent")
    resources = declaration["resources"]
    if not {"LICENSE.cua-driver.md", "THIRD_PARTY_NOTICES.md"}.issubset(resources):
        raise ValueError("CUA release declaration must include licenses and notices")
    for destination, resource in resources.items():
        for name in (destination, resource["path"]):
            path = PurePosixPath(name)
            if (not name or not path.parts or path.is_absolute() or ".." in path.parts or "\\" in name or ":" in name
                    or path.as_posix() != name):
                raise ValueError("CUA release resource path is unsafe")
        if not re.fullmatch(r"[0-9a-f]{64}", resource.get("sha256", "")):
            raise ValueError("CUA release resource must have a trusted hash")
    sdk = workspace["workspace"]["dependencies"]["cua-driver-sdk"]
    if (sdk.get("git") != declaration["repository"] or sdk.get("tag") != release_tag
            or any(key in sdk for key in ("rev", "branch"))):
        raise ValueError("CUA SDK must use the declared stable release tag")
    lock = tomllib.loads(Path("Cargo.lock").read_text())
    packages = [entry for entry in lock["package"] if entry["name"] == "cua-driver-sdk"]
    source = f"git+{sdk['git']}?tag={quote(release_tag, safe='')}#{revision}"
    if len(packages) != 1 or packages[0]["source"] != source or packages[0]["version"] != declaration["version"]:
        raise ValueError("CUA SDK lock source/version must match the declared release")
    if any(entry.get("source", "").startswith("git+" + sdk["git"])
           and entry["source"] != source for entry in lock["package"]):
        raise ValueError("CUA SDK Git packages must share the declared source identity")
    # The producer also validates assets/resources against the declaration. This
    # guard calls only the pure declaration reader; it never prepares a payload or runs a driver/compiler.
    if args.signing:
        required = ("MACOS_CERTIFICATE_P12_BASE64", "MACOS_CERTIFICATE_PASSWORD",
                    "MACOS_DESKTOP_SIGN_IDENTITY",
                    "APPLE_NOTARIZATION_KEY_ID", "APPLE_NOTARIZATION_ISSUER_ID")
        missing = [key for key in required if not os.environ.get(key)]
        if not (os.environ.get("APPLE_NOTARIZATION_KEY") or os.environ.get("APPLE_NOTARIZATION_KEY_BASE64")):
            missing.append("APPLE_NOTARIZATION_KEY or APPLE_NOTARIZATION_KEY_BASE64")
        if missing:
            raise ValueError("Missing required macOS signing parameters: " + ", ".join(missing))
        if (os.environ.get("WINDOWS_SIGNING_CERT_BASE64") or os.environ.get("WINDOWS_SIGNING_CERT_PATH")) and not os.environ.get("WINDOWS_SIGNING_CERT_PASSWORD"):
            raise ValueError("Configured Windows certificate requires WINDOWS_SIGNING_CERT_PASSWORD")
        if os.environ.get("WINDOWS_SIGNING_REQUIRED", "false").strip().lower() in ("true", "1", "yes"):
            if not (os.environ.get("WINDOWS_SIGNING_CERT_BASE64") or os.environ.get("WINDOWS_SIGNING_CERT_PATH")
                    or os.environ.get("WINDOWS_SIGNING_SUBJECT_NAME")):
                raise ValueError("Windows signing is required but no existing certificate is configured")
    print(f"Release inputs agree: {tag}; CUA {release_tag}; target {args.target or 'all six'}")


if __name__ == "__main__":
    main()
