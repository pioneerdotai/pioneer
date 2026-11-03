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
DEV_BUNDLE_ID = "ai.pioneer.gateway.dev"
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
    if "\\" in name or ":" in name or any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in name):
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
    seen, files, directories, total = set(), [], [], 0
    if zipfile.is_zipfile(archive):
        container = zipfile.ZipFile(archive)
        entries = container.infolist()
        rows = [(entry, entry.orig_filename, entry.is_dir(), entry.file_size,
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
            if kind != 0 and kind != (stat.S_IFDIR if directory else stat.S_IFREG):
                raise ValueError("Archive member type mismatch")
            # DOS ZIP producers do not declare Unix permission bits; Windows
            # executables run by extension and destination ACLs govern access.
            if isinstance(entry, zipfile.ZipInfo):
                mode = entry.external_attr >> 16 if entry.create_system == 3 else (0o755 if directory else 0o644)
            else:
                mode = entry.mode
            if mode & 0o7022:
                raise ValueError("Unsafe archive permissions")
            seen.add(folded)
            total += size
            if (total > MAX_BYTES or size > 512 * 1024 * 1024 or size < 0 or len(seen) > 10000
                    or (directory and size != 0)):
                raise ValueError("CUA archive exceeds extraction budget")
            if not directory:
                files.append((entry, name, size, mode))
            else:
                directories.append((name, mode))
        filenames = {name.casefold() for _, name, _, _ in files}
        names = [name for _, name, _, _ in files] + [name for name, _ in directories]
        if any(parent.as_posix().casefold() in filenames
               for name in names for parent in PurePosixPath(name).parents if parent.parts):
            raise ValueError("Archive member conflicts with a parent file")
        for name, _ in directories:
            (destination / name).mkdir(parents=True, exist_ok=True)
        for entry, name, size, mode in files:
            output = destination / name
            output.parent.mkdir(parents=True, exist_ok=True)
            with opener(entry) as source, output.open("xb") as target:
                written = 0
                while chunk := source.read(min(1024 * 1024, size - written + 1)):
                    written += len(chunk)
                    if written > size or written > 512 * 1024 * 1024:
                        raise ValueError("Archive member exceeds declared size")
                    target.write(chunk)
            if written != size:
                raise ValueError("Archive member size mismatch")
            output.chmod(mode & 0o777)
        for name, mode in sorted(directories, key=lambda row: len(PurePosixPath(row[0]).parts), reverse=True):
            (destination / name).chmod(mode & 0o777)
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
    receipt = prepared / "prepared.json"
    if (prepared.is_symlink() or not prepared.is_dir() or receipt.is_symlink()
            or not receipt.is_file() or receipt.stat().st_size > 1024 * 1024):
        raise ValueError("Invalid prepared CUA directory or receipt")
    metadata = json.loads(receipt.read_text())
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


def validate_tree(target, root, verify_signatures=False, development=False):
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
    if development and manifest.get("gateway_bundle_identifier") != DEV_BUNDLE_ID:
        raise ValueError("Dev Gateway CUA identity mismatch")
    actual = file_inventory(root, ("cua/manifest.json",))
    if manifest.get("files") != actual or manifest.get("sha256") != digest(root / "cua" / expected_daemon):
        raise ValueError("Gateway CUA payload inventory or checksum mismatch")
    suffix = ".exe" if "windows" in target else ""
    gateway_name = ("cua/Pioneer Gateway.app/Contents/MacOS/pioneer"
                    if "apple-darwin" in target else "pioneer" + suffix)
    if any(not name.startswith("cua/") and name != gateway_name for name in actual):
        raise ValueError("Gateway CUA package contains files outside the selected runtime layout")
    required = {gateway_name, "cua/" + expected_daemon,
                "cua/LICENSE.cua-driver.md", "cua/THIRD_PARTY_NOTICES.md"}
    for name in required_runtime(target) - {"cua-driver" + suffix}:
        required.add("cua/" + name)
    if "apple-darwin" in target:
        required.add("cua/Pioneer Gateway.app/Contents/Info.plist")
    if not required.issubset(actual):
        raise ValueError(f"Gateway CUA payload is missing required files: {sorted(required - set(actual))}")
    architecture(root / gateway_name, target)
    architecture(root / "cua" / expected_daemon, target)
    if "windows" in target:
        architecture(root / "cua/cua-driver-uia.exe", target)
    if "apple-darwin" in target:
        gateway = root / "cua/Pioneer Gateway.app/Contents/MacOS/pioneer"
        with (gateway.parent.parent / "Info.plist").open("rb") as source:
            info = plistlib.load(source)
        bundle_id = DEV_BUNDLE_ID if development else BUNDLE_ID
        bundle_name = "Pioneer Gateway Dev" if development else "Pioneer Gateway"
        if (info.get("CFBundleIdentifier") != bundle_id or info.get("CFBundleName") != bundle_name
                or info.get("CFBundleExecutable") != "pioneer"):
            raise ValueError("Gateway bundle identity mismatch")
        if target == "x86_64-apple-darwin":
            libraries = list(gateway.parent.glob("libonnxruntime*.dylib"))
            if not libraries:
                raise ValueError("Intel Gateway bundle ONNX libraries missing")
            for library in libraries:
                architecture(library, target)
        if verify_signatures:
            if development:
                subprocess.run(["codesign", "--verify", "--deep", "--strict",
                                str(root / "cua/Pioneer Gateway.app")], check=True)
            else:
                verify_macos(root)
                notary = runpy.run_path(str(Path(__file__).with_name("macos-notary.py")))
                notary["validate_ticket"](root / "cua/Pioneer Gateway.app")
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
        daemon = payload / daemon_name(target)
        if macos:
            contents = payload / "Pioneer Gateway.app/Contents"
            (contents / "MacOS").mkdir(parents=True)
            (contents / "Helpers").mkdir()
            gateway = contents / "MacOS/pioneer"
            shutil.copy2(binary, gateway)
            gateway.chmod(0o755)
            (payload / "cua-driver").rename(daemon)
            daemon.chmod(0o755)
            with (contents / "Info.plist").open("wb") as output:
                plistlib.dump(dict(CFBundleIdentifier=BUNDLE_ID, CFBundleName="Pioneer Gateway",
                    CFBundleExecutable="pioneer", CFBundlePackageType="APPL", CFBundleVersion=selected["version"],
                    LSUIElement=True, LSMinimumSystemVersion="13.0",
                    NSAppleEventsUsageDescription="Pioneer Gateway controls applications on this computer to carry out your authorized computer-use tasks."), output)
            if target == "x86_64-apple-darwin":
                for library in libraries:
                    shutil.copy2(library, gateway.parent / library.name)
            entitlements = Path(temporary) / "gateway-entitlements.plist"
            # Public hardened-runtime entitlement used by the daemon's existing
            # Apple Events integration. Screen capture/accessibility remain TCC
            # grants; this is not an App Sandbox or upstream provisioning profile.
            entitlements.write_bytes(plistlib.dumps({"com.apple.security.automation.apple-events": True}))
            # Sign nested code first; signing the app below also signs its main
            # executable and seals the bundle with the completed nested signatures.
            for executable in [daemon, *gateway.parent.glob("*.dylib")]:
                options = ["--entitlements", str(entitlements)] if executable.suffix != ".dylib" else []
                subprocess.run(["codesign", "--force", "--timestamp", "--options", "runtime",
                                *options, "--sign", identity, str(executable)], check=True)
            subprocess.run(["codesign", "--force", "--timestamp", "--options", "runtime",
                            "--entitlements", str(entitlements), "--sign", identity,
                            str(contents.parent)], check=True)
            verify_macos(root)
            zip_tree(root, notary_archive)
            state_file.write_text(json.dumps(dict(inputs=inputs, signed_files=file_inventory(root)), indent=2) + "\n")
            _finish_macos(root, notary_archive, notary_log_dir)
        else:
            shutil.copy2(binary, root / ("pioneer" + suffix))
            (root / ("pioneer" + suffix)).chmod(0o755)
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


def dev_source(binary, root, target):
    """A saved service executable must refresh from its original Cargo output."""
    binary = binary.resolve(strict=True)
    if binary.is_relative_to(root):
        generation = root / binary.relative_to(root).parts[0]
        receipt_path = generation / "dev-inputs.json"
        if (generation.is_symlink() or receipt_path.is_symlink() or not receipt_path.is_file()
                or receipt_path.stat().st_size > 1024 * 1024):
            raise ValueError("Invalid dev runtime generation or source receipt")
        validate_tree(target, generation / "payload", "apple-darwin" in target, development=True)
        receipt = json.loads(receipt_path.read_text())
        source = Path(receipt["source"])
        if not source.is_absolute() or source.is_relative_to(root):
            raise ValueError("Invalid dev Gateway source receipt")
        binary = source.resolve(strict=True)
    if not binary.is_file():
        raise ValueError("The source Gateway executable is missing; run cargo start from the checkout")
    architecture(binary, target)
    return binary


def dev(target, binary, root):
    """Compose the current debug Gateway, never compile or install a release.

    Generations are immutable and persistent: service registration never points
    into a TemporaryDirectory. A failed preparation cannot publish a generation.
    This is one dev contour; callers must stop it before switching builds.
    """
    if target not in TARGETS:
        raise ValueError(f"Unsupported CUA target: {target}")
    expected = Path.home() / ".pioneer.dev/gateway-runtime"
    if root != expected:
        raise ValueError("CUA dev preparation requires the isolated dev runtime directory")
    for path in (root, *root.parents):
        if path.is_symlink():
            raise ValueError("Dev runtime directories must not traverse links")
    root.mkdir(parents=True, exist_ok=True)
    binary = dev_source(binary, root, target)
    selected = release()
    # A declaration change selects new inputs. Existing prepared inputs are
    # checked against the full declaration, architecture and every file digest.
    declaration_key = hashlib.sha256(json.dumps(selected, sort_keys=True).encode()).hexdigest()
    prepared = root / f"prepared-{target}-{declaration_key}"
    if not prepared.exists():
        prepare(target, prepared)
    metadata = check_prepared(target, prepared)
    inputs = dict(target=target, source=str(binary), gateway_sha256=digest(binary),
                  prepared=metadata, helper_sha256=digest(Path(__file__)))
    libraries = []
    if target == "x86_64-apple-darwin":
        # Use this Cargo output's native companions, never another Gateway's.
        libraries = sorted(binary.parent.glob("libonnxruntime*.dylib"))
        if not libraries:
            raise ValueError("Intel debug Gateway requires libonnxruntime dylibs beside the Cargo executable")
        inputs["onnx"] = {path.name: digest(path) for path in libraries}
    key = hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest()
    generation = root / key
    macos = "apple-darwin" in target
    executable = ("cua/Pioneer Gateway.app/Contents/MacOS/pioneer" if macos
                  else "pioneer.exe" if "windows" in target else "pioneer")
    if generation.exists():
        receipt_path = generation / "dev-inputs.json"
        if (generation.is_symlink() or receipt_path.is_symlink() or not receipt_path.is_file()
                or receipt_path.stat().st_size > 1024 * 1024
                or json.loads(receipt_path.read_text()) != inputs):
            raise ValueError("Dev runtime generation does not match the current source inputs")
        validate_tree(target, generation / "payload", macos, development=True)
        return generation / "payload" / executable
    with tempfile.TemporaryDirectory(prefix=".dev-compose-", dir=root) as temporary:
        staging = Path(temporary) / "generation"
        payload = staging / "payload"
        shutil.copytree(prepared / "runtime", payload / "cua")
        gateway = payload / executable
        gateway.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(binary, gateway)
        if digest(gateway) != inputs["gateway_sha256"]:
            raise ValueError("Cargo executable changed during dev runtime preparation; retry start")
        gateway.chmod(0o755)
        daemon = payload / "cua" / daemon_name(target)
        if macos:
            contents = gateway.parent.parent
            daemon.parent.mkdir()
            (payload / "cua/cua-driver").rename(daemon)
            with (contents / "Info.plist").open("wb") as output:
                plistlib.dump(dict(CFBundleIdentifier=DEV_BUNDLE_ID, CFBundleName="Pioneer Gateway Dev",
                    CFBundleExecutable="pioneer", CFBundlePackageType="APPL", CFBundleVersion=selected["version"],
                    LSUIElement=True, LSMinimumSystemVersion="13.0",
                    NSAppleEventsUsageDescription="Pioneer Gateway Dev controls applications for your authorized development computer-use tasks."), output)
            for library in libraries:
                destination = gateway.parent / library.name
                shutil.copy2(library, destination)
                if digest(destination) != inputs["onnx"][library.name]:
                    raise ValueError("Debug native library changed during preparation")
            for native in [daemon, *gateway.parent.glob("*.dylib")]:
                subprocess.run(["codesign", "--force", "--sign", "-", str(native)], check=True)
            # A stable, separate local designated requirement; no Developer ID,
            # hardened runtime, notarization or production TCC identity involved.
            subprocess.run(["codesign", "--force", "--sign", "-", "--identifier", DEV_BUNDLE_ID,
                "--requirements", f'=designated => identifier "{DEV_BUNDLE_ID}"', str(contents.parent)], check=True)
        else:
            daemon.chmod(0o755)
            if "linux" in target:
                (payload / "cua/wayland-helper/install.sh").chmod(0o755)
        manifest = dict(schema_version=1, repository=selected["repository"], revision=selected["revision"],
            version=selected["version"], release_tag=selected["tag"], target=target, permission_owner="gateway",
            upstream_asset=metadata["upstream_asset"], daemon=daemon_name(target), gateway_bundle_identifier=DEV_BUNDLE_ID,
            sha256=digest(daemon), files=file_inventory(payload, ("cua/manifest.json",)))
        (payload / "cua/manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        (staging / "dev-inputs.json").write_text(json.dumps(inputs, indent=2) + "\n")
        validate_tree(target, payload, macos, development=True)
        staging.rename(generation)
    return generation / "payload" / executable


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
    dev_parser = commands.add_parser("dev")
    dev_parser.add_argument("--binary", type=Path, required=True)
    dev_parser.add_argument("--root", type=Path, required=True)
    for command_parser in (prepare_parser, package_parser, validate_parser, dev_parser):
        command_parser.add_argument("--target", choices=sorted(TARGETS), required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args.target, args.out)
    elif args.command == "package":
        package(args.target, args.binary, args.prepared, args.archive, args.notary_log_dir)
    elif args.command == "dev":
        binary = dev(args.target, args.binary, args.root)
        print(json.dumps(dict(binary=str(binary), root=str(binary.parents[4] if "apple-darwin" in args.target else binary.parent))))
    else:
        validate_archive(args.target, args.archive, args.verify_signatures)


if __name__ == "__main__":
    main()
