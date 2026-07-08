#!/usr/bin/env python3
"""Create tiny local release fixtures for desktop auto-update checks."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


ASSETS = [
    ("macos", "x86_64", "macos_app_zip", "Pioneer-x86_64.app.zip"),
    ("macos", "aarch64", "macos_app_zip", "Pioneer-aarch64.app.zip"),
    ("linux", "x86_64", "appimage", "pioneer-linux-x86_64.AppImage"),
    ("linux", "aarch64", "appimage", "pioneer-linux-aarch64.AppImage"),
    ("windows", "x86_64", "wix_bundle_exe", "Pioneer-x86_64.exe"),
    ("windows", "aarch64", "wix_bundle_exe", "Pioneer-aarch64.exe"),
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="target/desktop-update-fixtures")
    parser.add_argument("--newer-version", default="9.9.9")
    parser.add_argument("--current-version", default="0.25.0")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    create_fixture(out_dir / "newer", args.newer_version, "valid")
    create_fixture(out_dir / "no-update", args.current_version, "valid")
    create_fixture(out_dir / "missing-asset", args.newer_version, "missing")
    create_fixture(out_dir / "sha-mismatch", args.newer_version, "mismatch")

    print(f"Created desktop update fixtures under {out_dir}")
    print()
    print("Serve one variant with:")
    print(f"  cd {out_dir / 'newer'} && python3 -m http.server 8765")
    print()
    print("Point Pioneer at it with:")
    print("  PIONEER_RELEASE_API_BASE=http://127.0.0.1:8765/releases")
    print("  PIONEER_RELEASE_DOWNLOAD_BASE=http://127.0.0.1:8765/releases/download")
    print("  PIONEER_RELEASE_REPO=local/pioneer")
    print("  PIONEER_DESKTOP_UPDATE_CHANNEL=stable")
    print("  PIONEER_DESKTOP_UPDATE_FORCE_CHECK=1")
    return 0


def create_fixture(root: Path, version: str, mode: str) -> None:
    tag = f"v{version}"
    download_dir = root / "releases" / "download" / tag
    download_dir.mkdir(parents=True, exist_ok=True)
    (root / "releases").mkdir(parents=True, exist_ok=True)

    manifest_assets = []
    for os_name, arch, kind, name in ASSETS:
        payload = f"pioneer desktop update fixture {mode} {version} {name}\n".encode()
        sha256 = hashlib.sha256(payload).hexdigest()
        if mode != "missing":
            (download_dir / name).write_bytes(payload)
        if mode == "mismatch":
            sha256 = "0" * 64
        manifest_assets.append(
            {
                "os": os_name,
                "arch": arch,
                "kind": kind,
                "name": name,
                "sha256": sha256,
                "size_bytes": len(payload),
            }
        )

    manifest = {
        "schema_version": 1,
        "product": "pioneer-desktop",
        "version": version,
        "tag": tag,
        "channel": "stable",
        "published_at": "2026-07-08T00:00:00Z",
        "assets": manifest_assets,
    }
    (download_dir / "desktop-update-manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    (root / "releases" / "latest").write_text(
        json.dumps({"tag_name": tag, "name": tag}, indent=2) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    raise SystemExit(main())
