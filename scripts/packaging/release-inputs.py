#!/usr/bin/env python3
"""Restore compact Desktop inputs and validate final Gateway; never consume objects."""
import argparse
import json
import os
from pathlib import Path
import runpy
import shutil

RUNTIME = runpy.run_path(str(Path(__file__).with_name("runtime-inputs.py")))
DESKTOP = runpy.run_path(str(Path(__file__).with_name("desktop-inputs.py")))


def restore(inputs, target):
    suffix = ".exe" if "windows" in target else ""
    names = [name + suffix for name in ("pioneer-app", "pioneer-app-updater")]
    metadata = RUNTIME["read"](inputs, target, "desktop", names)
    output = Path("target") / target / "release"
    output.mkdir(parents=True, exist_ok=True)
    if any((output / name).exists() for name in [*names, "desktop-inputs.json"]):
        raise ValueError("Desktop runtime destination already contains inputs")
    created = []
    try:
        for name in names:
            destination = output / name
            with (inputs / name).open("rb") as source, destination.open("xb") as handle:
                created.append(destination)
                shutil.copyfileobj(source, handle)
            destination.chmod(0o755)
        receipt = output / "desktop-inputs.json"
        receipt.write_text(json.dumps(metadata, indent=2) + "\n")
        created.append(receipt)
        DESKTOP["own_binaries"](target)
        symbols = Path(os.environ["PIONEER_SYMBOL_INPUTS_DIR"])
        symbols.mkdir(parents=True, exist_ok=False)
        shutil.copy2(inputs / "inputs.json", symbols / "inputs.json")
    except BaseException:
        for path in created:
            path.unlink(missing_ok=True)
        raise
    return metadata


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True, choices=sorted(RUNTIME["PLATFORMS"]))
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--gateway", type=Path, required=True)
    args = parser.parse_args()
    DESKTOP["gateway"](args.gateway, args.target)
    restore(args.inputs, args.target)
