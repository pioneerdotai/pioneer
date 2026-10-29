"""CUA archive and ready-input packaging regressions; never launch a daemon."""
import hashlib
import io
import json
import os
from pathlib import Path
import plistlib
import runpy
import stat
import struct
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile

CUA = runpy.run_path(str(Path(__file__).with_name("cua-driver.py")))


def native_header(target):
    # Native ABI headers exercise architecture admission, not executable startup.
    if "windows" in target:
        data = bytearray(128)
        data[:2] = b"MZ"
        struct.pack_into("<I", data, 60, 64)
        data[64:68] = b"PE\0\0"
        struct.pack_into("<H", data, 68, 0xAA64 if target.startswith("aarch64") else 0x8664)
    elif "apple-darwin" in target:
        data = bytearray(64)
        data[:4] = b"\xcf\xfa\xed\xfe"
        struct.pack_into("<I", data, 4, 0x100000C if target.startswith("aarch64") else 0x1000007)
    else:
        data = bytearray(64)
        data[:6] = b"\x7fELF\x02\x01"
        struct.pack_into("<H", data, 18, 183 if target.startswith("aarch64") else 62)
    return bytes(data)


def ready_inputs(root, target):
    prepared = root / "prepared"
    runtime = prepared / "runtime"
    runtime.mkdir(parents=True)
    windows = "windows" in target
    suffix = ".exe" if windows else ""
    members = {"cua-driver" + suffix: native_header(target),
               "LICENSE.cua-driver.md": b"MIT license fixture\n",
               "THIRD_PARTY_NOTICES.md": b"Third party notices fixture\n"}
    if windows:
        members["cua-driver-uia.exe"] = native_header(target) + b"signed-companion-fixture"
    elif "linux" in target:
        members.update({"wayland-helper/install.sh": b"#!/bin/sh\nexit 1\n",
                        "wayland-helper/README.md": b"Helper fixture\n",
                        "wayland-helper/winrects@cua/extension.js": b"// Helper fixture\n",
                        "wayland-helper/winrects@cua/metadata.json": b"{}\n"})
    for name, data in members.items():
        destination = runtime / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
    selected = CUA["release"]()
    inventory = {name: dict(sha256=hashlib.sha256(data).hexdigest(), bytes=len(data))
                 for name, data in members.items()}
    (prepared / "prepared.json").write_text(json.dumps(dict(
        schema_version=1, target=target, release=selected,
        upstream_asset=selected["assets"][CUA["TARGETS"][target]], files=inventory)))
    binary = root / ("pioneer" + suffix)
    binary.write_bytes(native_header(target) + b"Pioneer Gateway fixture")
    return binary, prepared, members


class CuaPayloadTests(unittest.TestCase):
    def test_dev_runtime_tracks_current_source_and_revalidates_cached_inputs(self):
        for target in ("x86_64-unknown-linux-gnu", "aarch64-pc-windows-msvc"):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as directory:
                home = Path(directory).resolve()
                binary, prepared, upstream = ready_inputs(home / "checkout-one", target)
                runtime = home / ".pioneer.dev/gateway-runtime"
                runtime.mkdir(parents=True)
                selected = CUA["release"]()
                declaration = hashlib.sha256(json.dumps(selected, sort_keys=True).encode()).hexdigest()
                cached = runtime / f"prepared-{target}-{declaration}"
                prepared.rename(cached)
                with patch.object(Path, "home", return_value=home), patch.dict(os.environ, PATH=""):
                    first = CUA["dev"](target, binary, runtime)
                    self.assertEqual(first.read_bytes(), binary.read_bytes())
                    self.assertEqual(CUA["dev"](target, binary, runtime), first)
                    for name, content in upstream.items():
                        self.assertEqual((first.parent / "cua" / name).read_bytes(), content)
                    binary.write_bytes(native_header(target) + b"current debug changes")
                    refreshed = CUA["dev"](target, first, runtime)
                    self.assertNotEqual(refreshed, first)
                    self.assertEqual(refreshed.read_bytes(), native_header(target) + b"current debug changes")
                    self.assertNotEqual(first.read_bytes(), refreshed.read_bytes())
                    moved = home / "other-checkout/custom-target/debug" / binary.name
                    moved.parent.mkdir(parents=True)
                    moved.write_bytes(native_header(target) + b"other checkout changes")
                    binary.unlink()
                    switched = CUA["dev"](target, moved, runtime)
                    self.assertEqual(switched.read_bytes(), moved.read_bytes())
                    driver = cached / "runtime" / ("cua-driver.exe" if "windows" in target else "cua-driver")
                    driver.write_bytes(native_header(target) + b"tampered cached driver")
                    generations = set(runtime.iterdir())
                    with self.assertRaisesRegex(ValueError, "Prepared CUA inputs do not match"):
                        CUA["dev"](target, moved, runtime)
                    self.assertEqual(set(runtime.iterdir()), generations)

    def test_dev_runtime_rejects_foreign_roots_and_does_not_publish_partial_payload(self):
        target = "x86_64-unknown-linux-gnu"
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            binary, prepared, _ = ready_inputs(home / "checkout", target)
            runtime = home / ".pioneer.dev/gateway-runtime"
            with patch.object(Path, "home", return_value=home):
                foreign = home / "Pioneer/managed"
                with self.assertRaisesRegex(ValueError, "isolated dev runtime"):
                    CUA["dev"](target, binary, foreign)
                self.assertFalse(foreign.exists())
                runtime.mkdir(parents=True)
                selected = CUA["release"]()
                declaration = hashlib.sha256(json.dumps(selected, sort_keys=True).encode()).hexdigest()
                cached = runtime / f"prepared-{target}-{declaration}"
                prepared.rename(cached)
                # A self-consistent receipt cannot make a foreign-ABI driver
                # into a valid payload for this Gateway's target.
                driver = cached / "runtime/cua-driver"
                driver.write_bytes(native_header("aarch64-unknown-linux-gnu"))
                receipt = json.loads((cached / "prepared.json").read_text())
                receipt["files"]["cua-driver"] = dict(sha256=hashlib.sha256(driver.read_bytes()).hexdigest(), bytes=driver.stat().st_size)
                (cached / "prepared.json").write_text(json.dumps(receipt))
                with self.assertRaisesRegex(ValueError, "architecture mismatch"):
                    CUA["dev"](target, binary, runtime)
                self.assertEqual(list(runtime.iterdir()), [cached])

    def test_macos_bundle_only_layout_and_gateway_permission_identity(self):
        for target in ("aarch64-apple-darwin", "x86_64-apple-darwin"):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                contents = root / "cua/Pioneer Gateway.app/Contents"
                gateway = contents / "MacOS/pioneer"
                daemon = contents / "Helpers/cua-driver"
                for path in (gateway, daemon):
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(native_header(target))
                info = dict(CFBundleIdentifier="ai.pioneer.gateway", CFBundleName="Pioneer Gateway",
                            CFBundleExecutable="pioneer")
                (contents / "Info.plist").write_bytes(plistlib.dumps(info))
                if target == "x86_64-apple-darwin":
                    (gateway.parent / "libonnxruntime.dylib").write_bytes(native_header(target))
                for name in ("LICENSE.cua-driver.md", "THIRD_PARTY_NOTICES.md"):
                    (root / "cua" / name).write_text("Package notices fixture\n")
                selected = CUA["release"]()
                manifest = dict(schema_version=1, repository=selected["repository"],
                    revision=selected["revision"], version=selected["version"], release_tag=selected["tag"],
                    target=target, permission_owner="gateway", daemon="Pioneer Gateway.app/Contents/Helpers/cua-driver",
                    upstream_asset=selected["assets"][CUA["TARGETS"][target]],
                    sha256=hashlib.sha256(daemon.read_bytes()).hexdigest())

                def seal_inventory():
                    manifest["files"] = CUA["file_inventory"](root, ("cua/manifest.json",))
                    (root / "cua/manifest.json").write_text(json.dumps(manifest))

                seal_inventory()
                # Architecture/layout validation deliberately does not claim to
                # validate signatures with this synthetic package.
                CUA["validate_tree"](target, root)

                (root / "pioneer").write_bytes(native_header(target))
                seal_inventory()
                with self.assertRaisesRegex(ValueError, "outside the selected runtime layout"):
                    CUA["validate_tree"](target, root)
                (root / "pioneer").unlink()

                info["CFBundleIdentifier"] = "ai.pioneer.macos"
                (contents / "Info.plist").write_bytes(plistlib.dumps(info))
                seal_inventory()
                with self.assertRaisesRegex(ValueError, "bundle identity mismatch"):
                    CUA["validate_tree"](target, root)

                info["CFBundleIdentifier"] = "ai.pioneer.gateway.dev"
                info["CFBundleName"] = "Pioneer Gateway Dev"
                (contents / "Info.plist").write_bytes(plistlib.dumps(info))
                manifest["gateway_bundle_identifier"] = "ai.pioneer.gateway.dev"
                seal_inventory()
                CUA["validate_tree"](target, root, development=True)
                with self.assertRaisesRegex(ValueError, "bundle identity mismatch"):
                    CUA["validate_tree"](target, root)
                manifest["gateway_bundle_identifier"] = "ai.pioneer.gateway"
                seal_inventory()
                with self.assertRaisesRegex(ValueError, "Dev Gateway CUA identity mismatch"):
                    CUA["validate_tree"](target, root, development=True)

    def test_unsafe_zip_members_are_rejected_before_writing_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cases = ("../escape", "/escape", "C:/escape", "path\\escape", "duplicate", "link")
            for index, defect in enumerate(cases):
                with self.subTest(defect=defect):
                    archive = root / f"unsafe-{index}.zip"
                    with zipfile.ZipFile(archive, "w") as output:
                        output.writestr("safe-file", b"safe")
                        entry = zipfile.ZipInfo("safe-file" if defect == "duplicate" else defect)
                        entry.create_system = 3
                        entry.external_attr = ((stat.S_IFLNK if defect == "link" else stat.S_IFREG) | 0o644) << 16
                        output.writestr(entry, b"../outside")
                    staging = root / f"staging-{index}"
                    with self.assertRaises(ValueError):
                        CUA["extract"](archive, staging)
                    self.assertEqual(list(staging.rglob("*")), [])
            self.assertFalse((root / "escape").exists())

    def test_tar_hardlink_is_rejected_before_regular_member_is_written(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "unsafe.tar.gz"
            with tarfile.open(archive, "w:gz") as output:
                entry = tarfile.TarInfo("ordinary")
                entry.size = 4
                output.addfile(entry, io.BytesIO(b"safe"))
                link = tarfile.TarInfo("hardlink")
                link.type, link.linkname = tarfile.LNKTYPE, "ordinary"
                output.addfile(link)
            with self.assertRaisesRegex(ValueError, "nonregular"):
                CUA["extract"](archive, root / "staging")
            self.assertEqual(list((root / "staging").iterdir()), [])

    def test_checksum_mismatch_removes_untrusted_download(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, downloaded = root / "upstream", root / "downloaded"
            source.write_bytes(b"changed release bytes")
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                CUA["download"](source.as_uri(), downloaded, "0" * 64)
            self.assertFalse(downloaded.exists())

    def test_ready_input_packaging_without_compiler_and_consumer_rejections(self):
        for target in ("x86_64-unknown-linux-gnu", "aarch64-pc-windows-msvc"):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                binary, prepared, upstream = ready_inputs(root, target)
                archive = root / "full.zip"
                with patch.dict(os.environ, PATH=""):
                    CUA["package"](target, binary, prepared, archive)
                    CUA["validate_archive"](target, archive)
                with zipfile.ZipFile(archive) as container:
                    members = {name: container.read(name) for name in container.namelist()}
                self.assertEqual(members[binary.name], binary.read_bytes())
                for name, data in upstream.items():
                    self.assertEqual(members["cua/" + name], data)
                for defect in ("version", "target", "architecture", "checksum", "companion"):
                    with self.subTest(defect=defect):
                        changed = dict(members)
                        manifest = json.loads(changed["cua/manifest.json"])
                        if defect == "version":
                            manifest["version"] = "0.37.0"
                            message = "identity mismatch"
                        elif defect == "target":
                            manifest["target"] = "x86_64-apple-darwin"
                            message = "identity mismatch"
                        elif defect == "architecture":
                            wrong_arch = "x86_64-pc-windows-msvc" if "windows" in target else "aarch64-unknown-linux-gnu"
                            name = "cua/" + manifest["daemon"]
                            changed[name] = native_header(wrong_arch)
                            manifest["files"][name] = dict(sha256=hashlib.sha256(changed[name]).hexdigest(), bytes=len(changed[name]))
                            manifest["sha256"] = hashlib.sha256(changed[name]).hexdigest()
                            message = "architecture mismatch"
                        elif defect == "checksum":
                            changed["cua/" + manifest["daemon"]] += b"corrupt"
                            message = "checksum mismatch"
                        else:
                            companion = ("cua/cua-driver-uia.exe" if "windows" in target else
                                         "cua/wayland-helper/winrects@cua/extension.js")
                            del changed[companion]
                            del manifest["files"][companion]
                            message = "missing required files"
                        changed["cua/manifest.json"] = json.dumps(manifest).encode()
                        invalid = root / (defect + ".zip")
                        with zipfile.ZipFile(invalid, "w") as container:
                            for name, data in changed.items():
                                container.writestr(name, data)
                        with self.assertRaisesRegex(ValueError, message):
                            CUA["validate_archive"](target, invalid)


if __name__ == "__main__":
    unittest.main()
