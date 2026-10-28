#!/usr/bin/env python3
"""Compact, hash-bound release inputs; no object files, downloads or compilation."""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import runpy
import shutil
import subprocess
import tomllib

ROOT = Path(__file__).resolve().parents[2]
NATIVE = runpy.run_path(str(Path(__file__).with_name("collect-symbols.py")))
CUA = runpy.run_path(str(Path(__file__).with_name("cua-driver.py")))
PLATFORMS = {
    "x86_64-unknown-linux-gnu": ("linux-x86_64", "gz"),
    "aarch64-unknown-linux-gnu": ("linux-aarch64", "gz"),
    "aarch64-apple-darwin": ("macos-aarch64", "gz"),
    "x86_64-apple-darwin": ("macos-x86_64", "zip"),
    "x86_64-pc-windows-msvc": ("windows-x86_64", "zip"),
    "aarch64-pc-windows-msvc": ("windows-aarch64", "zip"),
}


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def asset_name(target, variant):
    platform, extension = PLATFORMS[target]
    if variant == "computer-use":
        return f"pioneer-gateway-{platform}-computer-use-package.zip"
    if variant != "default":
        raise ValueError("Unsupported Gateway variant")
    return f"pioneer-gateway-{platform}.{extension}"


def context(target, variant):
    if target not in PLATFORMS or variant not in ("default", "computer-use", "desktop"):
        raise ValueError("Unsupported release target or variant")
    catalog = Path(os.environ["PIONEER_BUNDLED_MODEL_CATALOG"])
    return dict(schema_version=1, commit=subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        run_id=os.environ["GITHUB_RUN_ID"],
        run_attempt=os.environ.get("GITHUB_RUN_ATTEMPT", "1"),
        version=tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"]["version"],
        target=target, variant=variant, profile="release", catalog_sha256=digest(catalog))


def file_inventory(directory):
    rows = []
    for path in sorted(directory.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"Runtime input must not be a link: {path}")
        if not path.is_file() and not path.is_dir():
            raise ValueError(f"Runtime input must be a regular file or directory: {path}")
        if path.is_file() and path != directory / "provenance.json":
            rows.append(dict(path=path.relative_to(directory).as_posix(),
                             bytes=path.stat().st_size, sha256=digest(path)))
    return rows


def check_link_metadata(directory, metadata):
    link = json.loads((directory / "inputs.json").read_text())
    producer = metadata.get("source", metadata)
    for key in ("commit", "run_id", "run_attempt", "target", "catalog_sha256"):
        if link.get(key) != producer.get(key):
            raise ValueError(f"Runtime and original symbol inputs disagree on {key}")
    if (link.get("variant") != (None if metadata["variant"] == "desktop" else metadata["variant"])
            or link.get("binaries") != producer["binaries"]):
        raise ValueError("Runtime and original symbol executable inventory disagree")
    for key in ("commit", "run_id", "target", "variant", "catalog_sha256", "profile", "version"):
        if producer.get(key) != metadata.get(key):
            raise ValueError(f"Final runtime has different producer {key}")
    originals = {row["filename"]: row for row in producer["binaries"]}
    for binary in metadata["binaries"]:
        name = binary["filename"].replace("pioneer-bootstrap", "pioneer")
        original = originals.get(name)
        if original is None or any(binary[key] != original[key] for key in ("format", "module_id")):
            raise ValueError("Packaged runtime does not match the compiled module")


def check_build(binary, data, metadata):
    gateway = binary.name in ("pioneer", "pioneer.exe", "pioneer-bootstrap", "pioneer-bootstrap.exe")
    if gateway or binary.name in ("pioneer-app", "pioneer-app.exe"):
        build = NATIVE["build_manifest"](data)
        if (build["revision"] != metadata["commit"] or build["target"] != metadata["target"]
                or build.get("profile") != "release"
                or build.get("dependency_revision") != digest(ROOT / "Cargo.lock")):
            raise ValueError("Runtime executable source/target/profile/lock provenance mismatch")
        if gateway and ("COMPUTER_USE" in build["features"]) != (metadata["variant"] == "computer-use"):
            raise ValueError("Runtime Gateway feature/source mismatch")


def write(directory, target, variant, binaries, **extra):
    metadata = context(target, variant)
    rows = []
    for binary in binaries:
        CUA["architecture"](binary, target)
        data = binary.read_bytes()
        fmt, module = NATIVE["identity"](data)
        check_build(binary, data, metadata)
        rows.append(dict(filename=binary.relative_to(directory).as_posix(), format=fmt,
                         module_id=module, sha256=digest(binary)))
    metadata.update(extra, binaries=rows, files=file_inventory(directory))
    check_link_metadata(directory, metadata)
    (directory / "provenance.json").write_text(json.dumps(metadata, indent=2) + "\n")
    return metadata


def read(directory, target, variant, names=None):
    if directory.is_symlink() or not (directory / "provenance.json").is_file():
        raise ValueError(f"Missing ready runtime inputs: {directory}/provenance.json")
    metadata = json.loads((directory / "provenance.json").read_text())
    expected = context(target, variant)
    # A rerun may consume an already successful producer from an earlier attempt
    # of this same run. Byte hashes/module IDs still bind that exact producer.
    for key in ("schema_version", "commit", "run_id", "version", "target", "variant",
                "profile", "catalog_sha256"):
        if metadata.get(key) != expected[key]:
            raise ValueError(f"Runtime inputs have a different {key}: {directory}")
    if not str(metadata.get("run_attempt", "")).isdigit():
        raise ValueError("Runtime inputs missing producer attempt")
    records = metadata.get("files", [])
    if len({row["path"].casefold() for row in records}) != len(records):
        raise ValueError("Duplicate runtime input records")
    for row in records:
        path = PurePosixPath(row["path"])
        if (path.is_absolute() or not path.parts or any(p in ("..", ".") for p in path.parts)
                or "\\" in row["path"] or ":" in row["path"]):
            raise ValueError("Unsafe runtime input path")
    if records != file_inventory(directory):
        raise ValueError(f"Runtime input checksum or inventory mismatch: {directory}")
    entries = metadata.get("binaries", [])
    if names is not None and ({row["filename"] for row in entries} != set(names)
                              or len(entries) != len(names)):
        raise ValueError("Runtime executable inventory mismatch")
    for row in entries:
        binary = directory / row["filename"]
        if row["filename"] not in {r["path"] for r in records}:
            raise ValueError("Runtime executable absent from file inventory")
        if (digest(binary) != row["sha256"]
                or NATIVE["identity"](binary.read_bytes()) != (row["format"], row["module_id"])):
            raise ValueError("Runtime executable hash/module identity mismatch")
        CUA["architecture"](binary, target)
        check_build(binary, binary.read_bytes(), metadata)
    check_link_metadata(directory, metadata)
    return metadata


def export(directory, target, variant, sources, symbol_metadata):
    directory.mkdir(parents=True, exist_ok=False)
    shutil.copy2(symbol_metadata, directory / "inputs.json")
    copied = []
    for source in sources:
        destination = directory / source.name
        if destination.exists():
            raise ValueError("Duplicate runtime input filename")
        shutil.copy2(source, destination)
        copied.append(destination)
    if target == "x86_64-apple-darwin" and variant != "desktop":
        library_dir = Path(os.environ["PIONEER_ONNXRUNTIME_DYLIB_DIR"])
        libraries = sorted(library_dir.glob("libonnxruntime*.dylib"))
        if not libraries:
            raise ValueError("Gateway Intel ONNX Runtime dylibs are missing")
        for library in libraries:
            shutil.copy2(library, directory / library.name)
    return write(directory, target, variant, copied)
