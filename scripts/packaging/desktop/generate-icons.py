#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

from PIL import Image, ImageChops, ImageDraw, ImageFilter


def make_app_icon(image: Image.Image) -> Image.Image:
    canvas_size = 1024
    body_size = 824
    inset = (canvas_size - body_size) // 2
    supersampling = 4
    corner_radius = 185

    mask = Image.new("L", (body_size * supersampling, body_size * supersampling))
    ImageDraw.Draw(mask).rounded_rectangle(
        (0, 0, mask.width - 1, mask.height - 1),
        radius=corner_radius * supersampling,
        fill=255,
    )
    mask = mask.resize((body_size, body_size), Image.Resampling.LANCZOS)
    body = image.convert("RGBA").resize(
        (body_size, body_size), Image.Resampling.LANCZOS
    )
    body.putalpha(ImageChops.multiply(body.getchannel("A"), mask))

    shadow_mask = Image.new("L", (canvas_size, canvas_size))
    shadow_mask.paste(body.getchannel("A"), (inset, inset + 8))
    shadow_mask = shadow_mask.filter(ImageFilter.GaussianBlur(14))
    shadow_mask = shadow_mask.point(lambda alpha: round(alpha * 0.18))
    icon = Image.new("RGBA", (canvas_size, canvas_size))
    icon.putalpha(shadow_mask)
    icon.alpha_composite(body, (inset, inset))
    return icon


def main() -> int:
    repo_root = Path(__file__).resolve().parents[3]
    assets_dir = repo_root / "crates" / "desktop" / "assets"

    source_path = assets_dir / "app-icon-1024.png"
    if not source_path.is_file():
        print(f"missing icon source: {source_path}", file=sys.stderr)
        return 1

    with Image.open(source_path) as source:
        image = make_app_icon(source)

    image.resize((256, 256), Image.Resampling.LANCZOS).save(
        assets_dir / "app-icon-256.png",
        format="PNG",
    )
    image.save(
        assets_dir / "app-icon.ico",
        format="ICO",
        sizes=[
            (16, 16),
            (24, 24),
            (32, 32),
            (48, 48),
            (64, 64),
            (128, 128),
            (256, 256),
        ],
    )
    image.save(assets_dir / "app-icon-rounded-1024.png", format="PNG")
    image.save(assets_dir / "app-icon.icns", format="ICNS")

    print(
        "Generated app-icon-256.png, app-icon.ico, app-icon-rounded-1024.png, app-icon.icns"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
