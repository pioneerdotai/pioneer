#!/usr/bin/env python3
"""Pass exact release inputs to the independent symbols runner; never rebuild."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import runpy
import struct
import subprocess
import sys
import tarfile

COLLECT = Path(__file__).with_name("collect-symbols.py")
native = runpy.run_path(str(COLLECT))


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def object_paths(data):
    """Read thin Mach-O N_OSO entries without invoking dsymutil on release."""
    if data[:4] != b"\xcf\xfa\xed\xfe":
        raise ValueError("expected a thin 64-bit release Mach-O")
    cursor = 32
    for _ in range(struct.unpack_from("<I", data, 16)[0]):
        cmd, length = struct.unpack_from("<II", data, cursor)
        if length < 8:
            raise ValueError("invalid Mach-O load command")
        if cmd == 2:
            symbols, count, strings, _ = struct.unpack_from("<4I", data, cursor + 8)
            for index in range(count):
                name, typ, _, _, _ = struct.unpack_from("<IBBHQ", data, symbols + index * 16)
                if typ == 0x66:  # N_OSO; archive(member) references retain the archive.
                    end = data.index(b"\0", strings + name)
                    path = data[strings + name:end].decode()
                    if path.endswith(")") and "(" in path:
                        path = path[:path.rfind("(")]
                    yield Path(path)
            return
        cursor += length
    raise ValueError("release executable has no object debug map")


def export(args):
    args.out.mkdir(parents=True, exist_ok=False)
    files = {}
    binaries = []
    for binary in args.binaries:
        binary = binary.resolve(strict=True)
        data = binary.read_bytes()
        fmt, module = native["identity"](data)
        entry = dict(filename=binary.name, format=fmt, module_id=module,
                     sha256=hashlib.sha256(data).hexdigest())
        binaries.append(entry)
        if "binaries/" + binary.name in files:
            raise ValueError("duplicate release binary name")
        files["binaries/" + binary.name] = binary
        if fmt == "macho_uuid":
            paths = set(object_paths(data))
            if not paths:
                raise ValueError("empty release object debug map")
            for path in paths:
                if not path.is_absolute() or ".." in path.parts:
                    raise ValueError(f"unsupported non-absolute object path: {path}")
                files["objects/" + path.as_posix().lstrip("/")] = path
        elif fmt == "pe_codeview_guid_age":
            pdb = binary.with_name(binary.stem.replace("-", "_") + ".pdb")
            if native["pdb_identity"](pdb.read_bytes()) != module:
                raise ValueError("release PDB does not match executable")
            files["binaries/" + pdb.name] = pdb
    records = []
    # Streaming tar preserves object/archive timestamps. Fast compression keeps
    # private input transfer smaller without doing any DWARF linking here.
    with tarfile.open(args.out / "inputs.tar.gz", "w:gz", compresslevel=1,
                      dereference=True) as archive:
        for name, path in sorted(files.items()):
            if not path.is_file():
                raise ValueError(f"missing exact release input: {path}")
            records.append(dict(path=name, bytes=path.stat().st_size,
                                sha256=digest(path)))
            archive.add(path, arcname=name, recursive=False)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    metadata = dict(schema_version=1, commit=commit,
                    run_id=os.environ["GITHUB_RUN_ID"], target=args.target,
                    catalog_sha256=(digest(Path(os.environ["PIONEER_BUNDLED_MODEL_CATALOG"]))
                                    if os.environ.get("PIONEER_BUNDLED_MODEL_CATALOG") else None),
                    binaries=binaries, files=records)
    (args.out / "inputs.json").write_text(json.dumps(metadata, indent=2) + "\n")


def record(args):
    data = args.binary.read_bytes()
    fmt, module = native["identity"](data)
    rows = json.loads(args.ledger.read_text()) if args.ledger.exists() else []
    row = dict(format=fmt, module_id=module,
               binary_sha256=hashlib.sha256(data).hexdigest(), bytes=len(data))
    if row not in rows:
        rows.append(row)
    args.ledger.write_text(json.dumps(rows, indent=2) + "\n")


def process(args, expected_run_id=None, packaged_override=None, inventory_name="release-inputs.json"):
    metadata = json.loads((args.inputs / "inputs.json").read_text())
    if metadata["commit"] != os.environ["SYMBOL_SOURCE_SHA"]:
        raise ValueError("symbol inputs are from a different source commit")
    if str(metadata["run_id"]) != (expected_run_id or os.environ["SYMBOL_SOURCE_RUN_ID"]):
        raise ValueError("symbol inputs are from a different workflow run")
    if metadata["target"] != os.environ["SYMBOL_TARGET"]:
        raise ValueError("symbol inputs are for a different target")
    records = {row["path"]: row for row in metadata["files"]}
    args.work.mkdir(parents=True, exist_ok=False)
    with tarfile.open(args.inputs / "inputs.tar.gz", "r:gz") as archive:
        seen = set()
        for member in archive:
            path = PurePosixPath(member.name)
            if (not member.isfile() or path.is_absolute() or ".." in path.parts
                    or "\\" in member.name or ":" in member.name
                    or path.as_posix() != member.name
                    or member.name not in records or member.name in seen):
                raise ValueError("unexpected symbol input archive member")
            if member.size != records[member.name]["bytes"]:
                raise ValueError("symbol input archive size mismatch")
            seen.add(member.name)
            source = archive.extractfile(member)
            destination = args.work / member.name
            destination.parent.mkdir(parents=True, exist_ok=True)
            with destination.open("xb") as output:
                while chunk := source.read(8 * 1024 * 1024):
                    output.write(chunk)
            os.utime(destination, (member.mtime, member.mtime))
            row = records[member.name]
            if destination.stat().st_size != row["bytes"] or digest(destination) != row["sha256"]:
                raise ValueError("symbol input checksum mismatch")
        if seen != set(records):
            raise ValueError("missing symbol input archive member")
    # Only this job's downloaded archive; release inputs remain in Actions storage.
    (args.inputs / "inputs.tar.gz").unlink()
    packaged = (packaged_override if packaged_override is not None
                else json.loads((args.inputs / "packaged.json").read_text()))
    env = os.environ.copy()
    for key in ("MallocStackLogging", "MallocStackLoggingNoCompact", "MallocStackLoggingDirectory"):
        env.pop(key, None)
    env["DSYMUTIL_OBJECT_ROOT"] = str((args.work / "objects").resolve())
    for binary in metadata["binaries"]:
        original = args.work / "binaries" / binary["filename"]
        fmt, module = native["identity"](original.read_bytes())
        if (fmt, module) != (binary["format"], binary["module_id"]):
            raise ValueError("restored executable module mismatch")
        matches = [row for row in packaged
                   if (row["format"], row["module_id"]) == (fmt, module)]
        if not matches:
            raise ValueError("symbols have no matching packaged executable")
        subprocess.run([sys.executable, str(COLLECT), "--release-input-manifest",
                        str(args.inputs / "inputs.json"), str(original),
                        str(args.out)], env=env, check=True)
        manifest = args.out / f"{original.stem}-{module}" / "manifest.json"
        value = json.loads(manifest.read_text())
        value.update(source_run_id=metadata["run_id"], target=metadata["target"],
                     packaged_by_run_id=os.environ["SYMBOL_SOURCE_RUN_ID"],
                     packaged_binary_sha256s=[row["binary_sha256"] for row in matches],
                     packaged_module_validated=True)
        manifest.write_text(json.dumps(value, indent=2) + "\n")
    gateway = args.inputs / "gateway"
    if gateway.exists():
        if expected_run_id is not None:
            raise ValueError("nested Gateway reuse is not supported")
        receipt = json.loads((gateway / "reuse.json").read_text())
        original = json.loads((gateway / "inputs.json").read_text())
        if (receipt.get("schema_version") != 1 or receipt.get("variant") != "computer-use"
                or receipt.get("commit") != metadata["commit"] or receipt.get("target") != metadata["target"]
                or str(receipt.get("run_id")) != str(original["run_id"])
                or len(original["binaries"]) != 1
                or original.get("catalog_sha256") != metadata.get("catalog_sha256")
                or receipt.get("module_id") != original["binaries"][0]["module_id"]
                or receipt.get("binary_sha256") != original["binaries"][0]["sha256"]):
            raise ValueError("reused Gateway provenance mismatch")
        child = argparse.Namespace(inputs=gateway, work=args.work / "gateway", out=args.out)
        process(child, str(receipt["run_id"]), packaged, "gateway-release-inputs.json")
        metadata["reused_gateway"] = receipt
    (args.out / inventory_name).write_text(json.dumps(metadata, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    create = sub.add_parser("export")
    create.add_argument("--target", required=True)
    create.add_argument("out", type=Path)
    create.add_argument("binaries", nargs="+", type=Path)
    note = sub.add_parser("record")
    note.add_argument("binary", type=Path)
    note.add_argument("ledger", type=Path)
    consume = sub.add_parser("process")
    consume.add_argument("inputs", type=Path)
    consume.add_argument("work", type=Path)
    consume.add_argument("out", type=Path)
    args = parser.parse_args()
    if args.mode != "process":
        {"export": export, "record": record}[args.mode](args)
        return
    args.out.mkdir(parents=True, exist_ok=False)
    result = args.out / "collection-result.json"
    result.write_text(json.dumps({"status": "in_progress"}) + "\n")
    try:
        process(args)
    except BaseException as error:
        result.write_text(json.dumps({"status": "failed", "error": str(error)}) + "\n")
        raise
    result.write_text(json.dumps({"status": "completed",
                                 "source_run_id": os.environ["SYMBOL_SOURCE_RUN_ID"],
                                 "commit": os.environ["SYMBOL_SOURCE_SHA"]}) + "\n")


if __name__ == "__main__":
    main()
