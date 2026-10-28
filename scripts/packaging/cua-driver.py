#!/usr/bin/env python3
"""Prepare verified prebuilt CUA, package ready inputs, or validate a full CU ZIP.

Preparation is the only network stage. Packaging never invokes a compiler or
fetches sources. The Gateway remains the sole owner of the packaged daemon.
"""
import argparse
from contextlib import nullcontext
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import plistlib
import runpy
import shutil
import stat
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
import tarfile

BUNDLE_ID = "ai.pioneer.gateway"
DECLARATION = Path(__file__).with_name("cua-release.json")
TARGETS = {
    "aarch64-apple-darwin": "darwin-universal",
    "x86_64-apple-darwin": "darwin-universal",
    "aarch64-unknown-linux-gnu": "linux-arm64",
    "x86_64-unknown-linux-gnu": "linux-x86_64",
    "aarch64-pc-windows-msvc": "windows-arm64",
    "x86_64-pc-windows-msvc": "windows-x86_64",
}
MAX_BYTES = 2 * 1024 * 1024 * 1024


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def release():
    value = json.loads(DECLARATION.read_text())
    if (value["schema_version"] != 1 or value["repository"] != "https://github.com/trycua/cua"
            or value["tag"] != "cua-driver-rs-v" + value["version"]
            or len(value["revision"]) != 40 or any(c not in "0123456789abcdef" for c in value["revision"])
            or set(value["assets"]) != set(TARGETS.values())
            or set(value["resources"]) != {"LICENSE.cua-driver.md", "THIRD_PARTY_NOTICES.md"}):
        raise ValueError("Invalid CUA release declaration")
    for row in [*value["assets"].values(), *value["resources"].values()]:
        if len(row["sha256"]) != 64 or any(c not in "0123456789abcdef" for c in row["sha256"]):
            raise ValueError("CUA release requires exact SHA256 inputs")
    for platform, row in value["assets"].items():
        extension = ".zip" if platform.startswith("windows-") else ".tar.gz"
        if row["name"] != f"cua-driver-rs-{value['version']}-{platform}-binary{extension}":
            raise ValueError("CUA asset names must match the declared release and platform")
    return value


def safe_name(name):
    if "\\" in name or "\x00" in name or ":" in name:
        raise ValueError(f"Unsafe archive member: {name!r}")
    parts = PurePosixPath(name).parts
    if not parts or name.startswith("/") or any(part in ("..", "") for part in parts):
        raise ValueError(f"Unsafe archive member: {name!r}")
    normalized = PurePosixPath(*parts).as_posix()
    if any(part.endswith((".", " ")) for part in parts):
        raise ValueError(f"Nonportable archive member: {name!r}")
    devices = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10))}
    if any(part.split(".")[0].upper() in devices for part in parts):
        raise ValueError(f"Windows device archive member: {name!r}")
    return normalized


def extract(archive, destination):
    """Reject every unsafe/duplicate/link entry before writing a single file."""
    destination.mkdir(parents=True, exist_ok=False)
    seen, files, total = set(), [], 0
    if zipfile.is_zipfile(archive):
        container = zipfile.ZipFile(archive)
        entries = container.infolist()
        rows = [(entry, entry.filename, entry.is_dir(), entry.file_size,
                 stat.S_IFMT(entry.external_attr >> 16)) for entry in entries]
        opener = container.open
    else:
        container = tarfile.open(archive, "r:gz")
        entries = container.getmembers()
        rows = [(entry, entry.name, entry.isdir(), entry.size,
                 stat.S_IFDIR if entry.isdir() else stat.S_IFREG if entry.isfile() else -1)
                for entry in entries]
        opener = container.extractfile
    try:
        for entry, name, directory, size, kind in rows:
            name = safe_name(name)
            folded = name.casefold()
            if folded in seen or kind not in (0, stat.S_IFREG, stat.S_IFDIR):
                raise ValueError(f"Duplicate or nonregular archive member: {name}")
            if kind == stat.S_IFDIR and not directory:
                raise ValueError("Archive directory type mismatch")
            seen.add(folded)
            total += size
            if total > MAX_BYTES or size > 512 * 1024 * 1024 or size < 0 or len(seen) > 10000:
                raise ValueError("CUA archive exceeds extraction budget")
            if not directory:
                files.append((entry, name, size))
        for entry, name, size in files:
            output = destination / name
            output.parent.mkdir(parents=True, exist_ok=True)
            with opener(entry) as source, output.open("xb") as target:
                shutil.copyfileobj(source, target)
            if output.stat().st_size != size:
                raise ValueError("Archive member size mismatch")
            mode = (entry.external_attr >> 16) if isinstance(entry, zipfile.ZipInfo) else entry.mode
            output.chmod(0o755 if mode & 0o111 else 0o644)
    finally:
        container.close()


def download(url, path, expected):
    for attempt in range(1, 5):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "Pioneer-release"})
            with urllib.request.urlopen(request, timeout=60) as response, path.open("wb") as output:
                shutil.copyfileobj(response, output)
            break
        except (OSError, urllib.error.URLError) as error:
            path.unlink(missing_ok=True)
            if isinstance(error, urllib.error.HTTPError) and error.code not in (408, 429, 500, 502, 503, 504):
                raise
            if attempt == 4:
                raise
            print(f"CUA download attempt {attempt}/4 failed: {error}; retry in {attempt * 3}s", flush=True)
            time.sleep(attempt * 3)
    if digest(path) != expected:
        path.unlink()
        raise ValueError(f"CUA upstream checksum mismatch: {url}")


def architecture(path, target):
    """Read native headers without launching upstream or project binaries."""
    with path.open("rb") as source:
        data = source.read(4096)
    arm = target.startswith("aarch64-")
    if "linux" in target:
        good = (data[:6] == b"\x7fELF\x02\x01" and len(data) >= 20
                and int.from_bytes(data[18:20], "little") == (183 if arm else 62))
    elif "windows" in target:
        good = False
        if data[:2] == b"MZ" and len(data) >= 64:
            offset = int.from_bytes(data[60:64], "little")
            with path.open("rb") as source:
                source.seek(offset)
                header = source.read(6)
            good = header[:4] == b"PE\0\0" and int.from_bytes(header[4:6], "little") == (0xAA64 if arm else 0x8664)
    else:
        cpu = 0x100000C if arm else 0x1000007
        good = False
        if data[:4] in (b"\xcf\xfa\xed\xfe", b"\xfe\xed\xfa\xcf") and len(data) >= 8:
            order = "little" if data[:4] == b"\xcf\xfa\xed\xfe" else "big"
            good = int.from_bytes(data[4:8], order) == cpu
        elif data[:4] in (b"\xca\xfe\xba\xbe", b"\xca\xfe\xba\xbf"):
            count = int.from_bytes(data[4:8], "big")
            width = 32 if data[:4] == b"\xca\xfe\xba\xbf" else 20
            good = 0 < count <= 16 and any(int.from_bytes(data[8 + index * width:12 + index * width], "big") == cpu
                                             for index in range(count))
    if not good:
        raise ValueError(f"Native architecture mismatch for {target}: {path}")


def file_inventory(root, excluded=()):
    if not root.is_dir() or root.is_symlink():
        raise ValueError(f"A real CUA staging directory is required: {root}")
    rows = {}
    for path in sorted(root.rglob("*")):
        name = path.relative_to(root).as_posix()
        if path.is_symlink():
            raise ValueError(f"Links are forbidden in CUA staging: {path}")
        if not path.is_file() and not path.is_dir():
            raise ValueError(f"Nonregular CUA staging member: {path}")
        if path.is_file() and name not in excluded:
            rows[name] = dict(sha256=digest(path), bytes=path.stat().st_size)
    return rows


def required_runtime(target):
    suffix = ".exe" if "windows" in target else ""
    names = {"cua-driver" + suffix, "LICENSE.cua-driver.md", "THIRD_PARTY_NOTICES.md"}
    if "windows" in target:
        names.add("cua-driver-uia.exe")
    if "linux" in target:
        names.update({"wayland-helper/install.sh", "wayland-helper/README.md",
                      "wayland-helper/winrects@cua/extension.js", "wayland-helper/winrects@cua/metadata.json"})
    return names


def prepare(target, out):
    if target not in TARGETS:
        raise ValueError(f"Unsupported CUA target: {target}")
    selected = release()
    asset = selected["assets"][TARGETS[target]]
    if out.exists():
        raise ValueError(f"CUA preparation output already exists: {out}")
    out.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".cua-prepare-", dir=out.parent) as temporary:
        work = Path(temporary)
        archive = work / asset["name"]
        download(f"{selected['repository']}/releases/download/{selected['tag']}/{asset['name']}", archive, asset["sha256"])
        unpacked = work / "unpacked"
        extract(archive, unpacked)
        runtime = work / "result/runtime"
        runtime.mkdir(parents=True)
        names = required_runtime(target) - {"LICENSE.cua-driver.md", "THIRD_PARTY_NOTICES.md"}
        for name in names:
            source = unpacked / name
            if not source.is_file():
                raise ValueError(f"Official CUA archive is missing required runtime file: {name}")
            destination = runtime / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
        for name, resource in selected["resources"].items():
            download(f"https://raw.githubusercontent.com/trycua/cua/{selected['revision']}/{resource['path']}",
                     runtime / name, resource["sha256"])
        suffix = ".exe" if "windows" in target else ""
        architecture(runtime / ("cua-driver" + suffix), target)
        (runtime / ("cua-driver" + suffix)).chmod(0o755)
        if "windows" in target:
            architecture(runtime / "cua-driver-uia.exe", target)
        metadata = dict(schema_version=1, release=selected, target=target, upstream_asset=asset,
                        files=file_inventory(runtime))
        (runtime.parent / "prepared.json").write_text(json.dumps(metadata, indent=2) + "\n")
        runtime.parent.rename(out)


def check_prepared(target, prepared):
    selected = release()
    metadata = json.loads((prepared / "prepared.json").read_text())
    actual = file_inventory(prepared / "runtime")
    if (metadata.get("schema_version") != 1 or metadata.get("release") != selected
            or metadata.get("target") != target or metadata.get("upstream_asset") != selected["assets"][TARGETS[target]]
            or metadata.get("files") != actual or set(actual) != required_runtime(target)):
        raise ValueError("Prepared CUA inputs do not match declaration, target, inventory or checksums")
    return metadata


def daemon_name(target):
    if "apple-darwin" in target:
        return "Pioneer Gateway.app/Contents/Helpers/cua-driver"
    return "cua-driver.exe" if "windows" in target else "cua-driver"


def verify_macos(root):
    app = root / "cua/Pioneer Gateway.app"
    teams = set()
    for path in sorted(root.rglob("*")):
        if path.is_file():
            with path.open("rb") as source:
                magic = source.read(4)
            if magic in (b"\xcf\xfa\xed\xfe", b"\xfe\xed\xfa\xcf", b"\xca\xfe\xba\xbe", b"\xca\xfe\xba\xbf"):
                subprocess.run(["codesign", "--verify", "--strict", str(path)], check=True)
                details = subprocess.run(["codesign", "--display", "--verbose=4", str(path)],
                                         check=True, capture_output=True, text=True)
                lines = (details.stdout + details.stderr).splitlines()
                team = next((line.removeprefix("TeamIdentifier=") for line in lines
                             if line.startswith("TeamIdentifier=")), "")
                if not team or team == "not set" or not any(line.startswith("Authority=Developer ID Application:") for line in lines):
                    raise ValueError(f"Gateway payload requires Developer ID signatures: {path}")
                teams.add(team)
    if len(teams) != 1:
        raise ValueError("Gateway executable composition has inconsistent signing teams")
    subprocess.run(["codesign", "--verify", "--deep", "--strict", str(app)], check=True)


def validate_tree(target, root, verify_signatures=False):
    if target not in TARGETS:
        raise ValueError(f"Unsupported CUA target: {target}")
    selected = release()
    manifest = json.loads((root / "cua/manifest.json").read_text())
    expected_daemon = daemon_name(target)
    if (manifest.get("schema_version") != 1 or manifest.get("repository") != selected["repository"]
            or manifest.get("revision") != selected["revision"] or manifest.get("version") != selected["version"]
            or manifest.get("release_tag") != selected["tag"] or manifest.get("target") != target
            or manifest.get("permission_owner") != "gateway" or manifest.get("daemon") != expected_daemon
            or manifest.get("upstream_asset") != selected["assets"][TARGETS[target]]):
        raise ValueError("Gateway CUA package identity mismatch")
    actual = file_inventory(root, ("cua/manifest.json",))
    if manifest.get("files") != actual or manifest.get("sha256") != digest(root / "cua" / expected_daemon):
        raise ValueError("Gateway CUA payload inventory or checksum mismatch")
    suffix = ".exe" if "windows" in target else ""
    required = {"pioneer" + suffix, "cua/" + expected_daemon,
                "cua/LICENSE.cua-driver.md", "cua/THIRD_PARTY_NOTICES.md"}
    for name in required_runtime(target) - {"cua-driver" + suffix}:
        required.add("cua/" + name)
    if "apple-darwin" in target:
        required.update({"cua/Pioneer Gateway.app/Contents/MacOS/pioneer", "cua/Pioneer Gateway.app/Contents/Info.plist"})
    if not required.issubset(actual):
        raise ValueError(f"Gateway CUA payload is missing required files: {sorted(required - set(actual))}")
    architecture(root / ("pioneer" + suffix), target)
    architecture(root / "cua" / expected_daemon, target)
    if "windows" in target:
        architecture(root / "cua/cua-driver-uia.exe", target)
    if "apple-darwin" in target:
        gateway = root / "cua/Pioneer Gateway.app/Contents/MacOS/pioneer"
        architecture(gateway, target)
        native = runpy.run_path(str(Path(__file__).with_name("collect-symbols.py")))
        root_bytes, app_bytes = (root / "pioneer").read_bytes(), gateway.read_bytes()
        if (native["identity"](root_bytes) != native["identity"](app_bytes)
                or native["build_manifest"](root_bytes) != native["build_manifest"](app_bytes)):
            raise ValueError("Root and app Gateway module/build identities differ")
        if target == "x86_64-apple-darwin":
            libraries = list(root.glob("libonnxruntime*.dylib"))
            if not libraries:
                raise ValueError("Intel Gateway root ONNX libraries missing")
            nested = {path.name for path in gateway.parent.glob("libonnxruntime*.dylib")}
            if nested != {path.name for path in libraries}:
                raise ValueError("Root/app ONNX inventory mismatch")
            for library in libraries:
                architecture(library, target)
                if digest(library) != digest(gateway.parent / library.name):
                    raise ValueError("Root/app ONNX copies differ")
        if verify_signatures:
            verify_macos(root)
            subprocess.run(["xcrun", "stapler", "validate", str(root / "cua/Pioneer Gateway.app")], check=True)
    elif verify_signatures:
        raise ValueError("Signature verification mode requires a macOS target")
    return manifest


def validate_archive(target, archive, verify_signatures=False):
    with tempfile.TemporaryDirectory(prefix="pioneer-cua-validation-") as temporary:
        root = Path(temporary) / "payload"
        extract(archive, root)
        return validate_tree(target, root, verify_signatures)


def zip_tree(root, archive):
    with zipfile.ZipFile(archive, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as output:
        for path in sorted(root.rglob("*")):
            if path.is_file():
                output.write(path, path.relative_to(root).as_posix())


def package(target, binary, prepared, archive, notary_log_dir=None):
    if target not in TARGETS:
        raise ValueError(f"Unsupported CUA target: {target}")
    selected = release()
    metadata = check_prepared(target, prepared)
    if archive.suffix != ".zip" or archive.exists():
        raise ValueError("A new .zip Gateway asset is required")
    architecture(binary, target)
    if "apple-darwin" in target and notary_log_dir is None:
        raise ValueError("macOS packaging requires persistent --notary-log-dir")
    macos = "apple-darwin" in target
    inputs = dict(target=target, gateway_sha256=digest(binary), prepared=metadata)
    identity = os.environ.get("MACOS_DESKTOP_SIGN_IDENTITY", "") if macos else ""
    if macos:
        if not identity:
            raise ValueError("Gateway CUA requires the existing macOS signing identity")
        inputs["signing_identity"] = identity
    if target == "x86_64-apple-darwin":
        libraries = sorted(Path(os.environ["PIONEER_ONNXRUNTIME_DYLIB_DIR"]).glob("libonnxruntime*.dylib"))
        if not libraries:
            raise ValueError("Gateway ONNX Runtime dylibs missing")
        inputs["onnx"] = {path.name: digest(path) for path in libraries}
    if macos:
        work = Path(notary_log_dir) / "package-work"
        work.mkdir(parents=True, exist_ok=True)
        context = nullcontext(str(work))
    else:
        context = tempfile.TemporaryDirectory(prefix="pioneer-cua-package-")
    with context as temporary:
        root = Path(temporary) / "payload"
        notary_archive = Path(temporary) / "complete-payload.zip"
        state_file = Path(temporary) / "package-inputs.json"
        state = json.loads(state_file.read_text()) if state_file.is_file() else {}
        if root.exists() and "signed_files" not in state:
            if (state.get("inputs") != inputs
                    or (Path(notary_log_dir) / "submission.json").exists()):
                raise ValueError("Incomplete Gateway staging belongs to other inputs or a submission")
            # A marked pre-submit composition can be assembled again. Never
            # discard signed/submitted/uncertain state or an unmarked directory.
            shutil.rmtree(root)
            notary_archive.unlink(missing_ok=True)
        if root.exists():
            exclusions = ("cua/manifest.json", "cua/Pioneer Gateway.app/Contents/CodeResources")
            if (state.get("inputs") != inputs or not notary_archive.is_file()
                    or state.get("signed_files") != file_inventory(root, exclusions)):
                raise ValueError("Retained Gateway staging is incomplete or belongs to different inputs")
            verify_macos(root)
            _finish_macos(root, notary_archive, notary_log_dir)
            _finish_package(target, root, selected, metadata, archive)
            return
        if state and (state.get("inputs") != inputs or "signed_files" in state):
            raise ValueError("Retained Gateway staging is incomplete or belongs to different inputs")
        if macos:
            state_file.write_text(json.dumps(dict(inputs=inputs), indent=2) + "\n")
        root.mkdir()
        payload = root / "cua"
        shutil.copytree(prepared / "runtime", payload)
        suffix = ".exe" if "windows" in target else ""
        shutil.copy2(binary, root / ("pioneer" + suffix))
        (root / ("pioneer" + suffix)).chmod(0o755)
        daemon = payload / daemon_name(target)
        if macos:
            contents = payload / "Pioneer Gateway.app/Contents"
            (contents / "MacOS").mkdir(parents=True)
            (contents / "Helpers").mkdir()
            gateway = contents / "MacOS/pioneer"
            shutil.copy2(root / "pioneer", gateway)
            (payload / "cua-driver").rename(daemon)
            daemon.chmod(0o755)
            with (contents / "Info.plist").open("wb") as output:
                plistlib.dump(dict(CFBundleIdentifier=BUNDLE_ID, CFBundleName="Pioneer Gateway",
                    CFBundleExecutable="pioneer", CFBundlePackageType="APPL", CFBundleVersion=selected["version"],
                    LSUIElement=True, LSMinimumSystemVersion="13.0",
                    NSAppleEventsUsageDescription="Pioneer Gateway controls applications on this computer to carry out your authorized computer-use tasks."), output)
            if target == "x86_64-apple-darwin":
                for library in libraries:
                    shutil.copy2(library, root / library.name)
            entitlements = Path(temporary) / "gateway-entitlements.plist"
            # Public hardened-runtime entitlement used by the daemon's existing
            # Apple Events integration. Screen capture/accessibility remain TCC
            # grants; this is not an App Sandbox or upstream provisioning profile.
            entitlements.write_bytes(plistlib.dumps({"com.apple.security.automation.apple-events": True}))
            for executable in [root / "pioneer", daemon, *root.glob("*.dylib")]:
                options = ["--entitlements", str(entitlements)] if executable.suffix != ".dylib" else []
                subprocess.run(["codesign", "--force", "--timestamp", "--options", "runtime",
                                *options, "--sign", identity, str(executable)], check=True)
            shutil.copy2(root / "pioneer", gateway)
            for library in root.glob("*.dylib"):
                shutil.copy2(library, gateway.parent / library.name)
            subprocess.run(["codesign", "--force", "--timestamp", "--options", "runtime",
                            "--entitlements", str(entitlements), "--sign", identity,
                            str(contents.parent)], check=True)
            # App signing may update its main executable's signature, which seals
            # bundle resources. Keep the separately signed standalone bootstrap;
            # validation compares their invariant module/build identities.
            verify_macos(root)
            zip_tree(root, notary_archive)
            state_file.write_text(json.dumps(dict(inputs=inputs, signed_files=file_inventory(root)), indent=2) + "\n")
            _finish_macos(root, notary_archive, notary_log_dir)
        else:
            # CI artifact transfer flattens Unix permission bits; restore only
            # the known runtime executables rather than making all files executable.
            daemon.chmod(0o755)
            if "linux" in target:
                (payload / "wayland-helper/install.sh").chmod(0o755)
        _finish_package(target, root, selected, metadata, archive)


def _finish_macos(root, notary_archive, log_dir):
    notary = runpy.run_path(str(Path(__file__).with_name("macos-notary.py")))
    notary["notarize"](notary_archive, [root / "cua/Pioneer Gateway.app"], log_dir)


def _finish_package(target, root, selected, metadata, archive):
    daemon = root / "cua" / daemon_name(target)
    manifest = dict(schema_version=1, repository=selected["repository"], revision=selected["revision"],
        version=selected["version"], release_tag=selected["tag"], target=target, permission_owner="gateway",
        upstream_asset=metadata["upstream_asset"], daemon=daemon_name(target),
        sha256=digest(daemon), files=file_inventory(root, ("cua/manifest.json",)))
    (root / "cua/manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    validate_tree(target, root, "apple-darwin" in target)
    archive.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="." + archive.name + ".", dir=archive.parent) as temporary:
        partial = Path(temporary) / archive.name
        zip_tree(root, partial)
        validate_archive(target, partial, "apple-darwin" in target)
        partial.replace(archive)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare")
    prepare_parser.add_argument("--out", type=Path, required=True)
    package_parser = commands.add_parser("package")
    package_parser.add_argument("--binary", type=Path, required=True)
    package_parser.add_argument("--prepared", type=Path, required=True)
    package_parser.add_argument("--notary-log-dir", type=Path)
    package_parser.add_argument("--archive", type=Path, required=True)
    validate_parser = commands.add_parser("validate")
    validate_parser.add_argument("--archive", type=Path, required=True)
    validate_parser.add_argument("--verify-signatures", action="store_true")
    for command_parser in (prepare_parser, package_parser, validate_parser):
        command_parser.add_argument("--target", choices=sorted(TARGETS), required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args.target, args.out)
    elif args.command == "package":
        package(args.target, args.binary, args.prepared, args.archive, args.notary_log_dir)
    else:
        validate_archive(args.target, args.archive, args.verify_signatures)


if __name__ == "__main__":
    main()
