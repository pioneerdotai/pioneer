"""Exact-input handoff regressions, including native macOS module transfer."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import runpy
import shutil
import struct
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

inputs = runpy.run_path(str(Path(__file__).with_name("symbol-inputs.py")))


class ReleaseInputTests(unittest.TestCase):
    @unittest.skipUnless(sys.platform == "darwin" and shutil.which("xcrun"),
                         "requires the macOS compiler and dsymutil")
    def test_macho_module_debug_info_survives_transfer_without_original_build(self):
        repository = Path(__file__).resolve().parents[2]
        script = Path(__file__).with_name("symbol-inputs.py").resolve()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            output = root / "target/release/build/bridge-123/out"
            output.mkdir(parents=True)
            cache = output / "swift-build/release/ModuleCache"
            (output / "value.h").write_text("typedef struct { int number; } FixtureValue;\n")
            (output / "module.modulemap").write_text(
                'module Fixture { header "value.h" export * }\n')
            source = root / "main.m"
            source.write_text('@import Fixture; int main(void) { '
                              'FixtureValue value = {1}; return value.number; }\n')
            obj = output / "main.o"
            binary = root / "target/release/pioneer"
            subprocess.run(["xcrun", "clang", "-gmodules", "-fmodules",
                            "-fmodules-cache-path=" + str(cache), "-I" + str(output),
                            "-c", str(source), "-o", str(obj)], check=True)
            subprocess.run(["xcrun", "clang", str(obj), "-o", str(binary)], check=True)
            modules = list(cache.rglob("*.pcm"))
            self.assertTrue(modules, "fixture must actually depend on a Clang module")
            unrelated = root / "target/release/build/unlinked-456/out/unused.pcm"
            unrelated.parent.mkdir(parents=True)
            unrelated.write_bytes(b"unrelated dependency")
            transferred = root / "inputs"
            environment = dict(os.environ, GITHUB_RUN_ID="17", GITHUB_RUN_ATTEMPT="1")
            target = ("aarch64" if os.uname().machine == "arm64" else "x86_64") + "-apple-darwin"
            subprocess.run([sys.executable, str(script), "export", "--target",
                            target, str(transferred), str(binary)],
                           env=environment, cwd=repository, check=True)
            metadata = json.loads((transferred / "inputs.json").read_text())
            records = {row["path"]: row for row in metadata["files"]}
            for module in modules:
                name = "objects/" + module.as_posix().lstrip("/")
                self.assertEqual(records[name]["sha256"],
                                 hashlib.sha256(module.read_bytes()).hexdigest())
            self.assertNotIn("objects/" + unrelated.as_posix().lstrip("/"), records)
            subprocess.run([sys.executable, str(script), "record", str(binary),
                            str(transferred / "packaged.json")], check=True)
            # The consumer must use only retained inputs, including module caches.
            shutil.rmtree(root / "target")
            environment.update(SYMBOL_SOURCE_SHA=metadata["commit"], SYMBOL_SOURCE_RUN_ID="17",
                               SYMBOL_TARGET=metadata["target"], SYMBOL_VARIANT="",
                               DSYMUTIL=subprocess.check_output(
                                   ["xcrun", "--find", "dsymutil"], text=True).strip())
            symbols = root / "symbols"
            subprocess.run([sys.executable, str(script), "process", str(transferred),
                            str(root / "work"), str(symbols)], env=environment, check=True)
            manifest, = symbols.glob("*/manifest.json")
            self.assertTrue(json.loads(manifest.read_text())["symbols_validated"])
            self.assertEqual((manifest.parent / "dsymutil.stderr").read_text(), "")

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
