#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import sys

from PIL import Image


def main() -> int:
    repo_root = Path(__file__).resolve().parents[3]
    assets_dir = repo_root / "crates" / "desktop" / "assets"

    source_path = assets_dir / "app-icon-1024.png"
    if not source_path.is_file():
        print(f"missing icon source: {source_path}", file=sys.stderr)
        return 1

    image = Image.open(source_path).convert("RGBA")

    image.resize((256, 256), Image.Resampling.LANCZOS).save(
        assets_dir / "app-icon-256.png",
        format="PNG",
    )
    image.save(
        assets_dir / "app-icon.ico",
        format="ICO",
        sizes=[(16, 16), (24, 24), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)],
    )
    image.save(assets_dir / "app-icon.icns", format="ICNS")

    print("Generated app-icon-256.png, app-icon.ico, app-icon.icns")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
