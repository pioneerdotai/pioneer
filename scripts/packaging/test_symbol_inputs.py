"""Fixture-only exact-input handoff regressions. Do not run before review."""
import argparse
import hashlib
import gzip
import io
import json
import os
from pathlib import Path
import runpy
import struct
import tarfile
import tempfile
import unittest
import zipfile
from unittest.mock import patch

inputs = runpy.run_path(str(Path(__file__).with_name("symbol-inputs.py")))
gateway = runpy.run_path(str(Path(__file__).with_name("build-gateway.py")))


class ReleaseInputTests(unittest.TestCase):
    def test_legacy_cu_archives_survive_the_real_binary_only_consumer(self):
        # Selector/extraction rules from 6c1e6b2 / locally tagged 0.64.0.
        # The old extractor does NOT install sidecar ZIP entries.
        targets = [
            ("linux", "x86_64", "x86_64-unknown-linux-gnu"),
            ("linux", "aarch64", "aarch64-unknown-linux-gnu"),
            ("macos", "aarch64", "aarch64-apple-darwin"),
            ("macos", "x86_64", "x86_64-apple-darwin"),
            ("windows", "x86_64", "x86_64-pc-windows-msvc"),
            ("windows", "aarch64", "aarch64-pc-windows-msvc"),
        ]
        for os_name, arch, target in targets:
            with self.subTest(target=target), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                live = root / "bin"
                live.mkdir()
                (live / "libonnxruntime.dylib").write_bytes(b"prior live library")
                (live / "settings.toml").write_bytes(b"prior settings")
                bridge = root / "compiled-bridge"
                bridge.write_bytes(b"opaque compiled bridge including its embedded full package")
                package = root / "complete.zip"
                with zipfile.ZipFile(package, "w") as output:
                    output.writestr("libonnxruntime.dylib", b"genuine companion fixture")
                    output.writestr("cua/Pioneer Gateway.app/Contents/MacOS/pioneer", b"nested Gateway")
                extension = "zip" if os_name == "windows" or (os_name == "macos" and arch == "x86_64") else "gz"
                # Independent old selector, not the new complete-package selector.
                archive = root / f"pioneer-gateway-{os_name}-{arch}-computer-use.{extension}"
                gateway["compatibility_archive"](target, bridge, package, archive)
                candidate = live / "pioneer.new"
                if extension == "gz":
                    with gzip.open(archive, "rb") as source:
                        candidate.write_bytes(source.read())
                else:
                    expected_binary = "pioneer.exe" if os_name == "windows" else "pioneer"
                    found = False
                    companion_count = 0
                    with zipfile.ZipFile(archive) as source:
                        for entry in source.infolist():
                            basename = Path(entry.filename).name.lower()
                            if basename == expected_binary:
                                candidate.write_bytes(source.read(entry))
                                found = True
                            elif os_name == "macos" and basename.startswith("libonnxruntime") and basename.endswith(".dylib"):
                                (live / basename).write_bytes(source.read(entry))
                                companion_count += 1
                        self.assertEqual(sum(Path(row.filename).name.lower() == expected_binary for row in source.infolist()), 1)
                    self.assertTrue(found)
                    if os_name == "macos":
                        self.assertGreater(companion_count, 0)
                        companions = list(live.glob("libonnxruntime.pioneer-cu-*.dylib"))
                        self.assertEqual(len(companions), 1)
                        self.assertEqual(companions[0].read_bytes(), b"genuine companion fixture")
                self.assertEqual(candidate.read_bytes(), bridge.read_bytes())
                self.assertEqual((live / "libonnxruntime.dylib").read_bytes(), b"prior live library")
                self.assertEqual((live / "settings.toml").read_bytes(), b"prior settings")
                self.assertFalse((live / "cua").exists())

    def test_macho_retains_archive_not_member_pseudo_path(self):
        name = b"/tmp/release/libdependency.rlib(unit.o)\0"
        data = bytearray(72 + len(name))
        data[:4] = b"\xcf\xfa\xed\xfe"
        struct.pack_into("<I", data, 16, 1)
        struct.pack_into("<6I", data, 32, 2, 24, 56, 1, 72, len(name))
        struct.pack_into("<IBBHQ", data, 56, 0, 0x66, 0, 0, 123)
        data[72:] = name
        self.assertEqual(list(inputs["object_paths"](data)),
                         [Path("/tmp/release/libdependency.rlib")])

    def test_wrong_source_run_rejected_before_extract_or_native_tool(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "inputs.json").write_text(json.dumps(
                dict(commit="revision", run_id="wrong-run", target="target")))
            args = argparse.Namespace(inputs=root, work=root / "work", out=root / "out")
            with patch.dict(os.environ, SYMBOL_SOURCE_SHA="revision",
                            SYMBOL_SOURCE_RUN_ID="correct-run", SYMBOL_TARGET="target"):
                with self.assertRaisesRegex(ValueError, "different workflow run"):
                    inputs["process"](args)
            self.assertFalse(args.work.exists())

    def test_wrong_gateway_variant_rejected_before_extract_or_native_tool(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "inputs.json").write_text(json.dumps(
                dict(commit="revision", run_id="1", target="target", variant="default")))
            args = argparse.Namespace(inputs=root, work=root / "work", out=root / "out")
            with patch.dict(os.environ, SYMBOL_SOURCE_SHA="revision",
                            SYMBOL_SOURCE_RUN_ID="1", SYMBOL_TARGET="target",
                            SYMBOL_VARIANT="computer-use"):
                with self.assertRaisesRegex(ValueError, "different Gateway variant"):
                    inputs["process"](args)
            self.assertFalse(args.work.exists())

    def test_wrong_compatibility_provenance_rejected_before_child_native_tools(self):
        for field in ["commit", "run_id", "target", "catalog_sha256", "variant"]:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                compatibility = root / "compatibility"
                compatibility.mkdir()
                parent = dict(commit="revision", run_id="1", target="target",
                              variant="computer-use", catalog_sha256="catalog",
                              files=[], binaries=[])
                (root / "inputs.json").write_text(json.dumps(parent))
                (root / "packaged.json").write_text("[]")
                with tarfile.open(root / "inputs.tar.gz", "w:gz"):
                    pass
                bridge = dict(parent, binaries=[dict(filename="pioneer-cu-update")])
                bridge[field] = "wrong-value"
                (compatibility / "inputs.json").write_text(json.dumps(bridge))
                args = argparse.Namespace(inputs=root, work=root / "work", out=root / "out")
                with patch.dict(os.environ, SYMBOL_SOURCE_SHA="revision",
                                SYMBOL_SOURCE_RUN_ID="1", SYMBOL_TARGET="target",
                                SYMBOL_VARIANT="computer-use"):
                    with self.assertRaisesRegex(ValueError, "compatibility bridge provenance mismatch"):
                        inputs["process"](args)
                self.assertFalse((args.work / "compatibility").exists())

    def test_corrupt_transferred_input_rejected_before_native_tool(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            row = dict(path="binaries/pioneer", bytes=3,
                       sha256=hashlib.sha256(b"old").hexdigest())
            (root / "inputs.json").write_text(json.dumps(
                dict(commit="revision", run_id="1", target="target", files=[row])))
            with tarfile.open(root / "inputs.tar.gz", "w:gz") as archive:
                entry = tarfile.TarInfo(row["path"])
                entry.size = 3
                archive.addfile(entry, io.BytesIO(b"new"))
            args = argparse.Namespace(inputs=root, work=root / "work", out=root / "out")
            with patch.dict(os.environ, SYMBOL_SOURCE_SHA="revision",
                            SYMBOL_SOURCE_RUN_ID="1", SYMBOL_TARGET="target"):
                with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                    inputs["process"](args)


if __name__ == "__main__":
    unittest.main()
