"""CUA archive and ready-input packaging regressions; never launch a daemon."""
import hashlib
import io
import json
import os
from pathlib import Path
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
    else:
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
