"""Fixture-only exact-input handoff regressions. Do not run before review."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import runpy
import struct
import tarfile
import tempfile
import unittest
from unittest.mock import patch

inputs = runpy.run_path(str(Path(__file__).with_name("symbol-inputs.py")))


class ReleaseInputTests(unittest.TestCase):
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
