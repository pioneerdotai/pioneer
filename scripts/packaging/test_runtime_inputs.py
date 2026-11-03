"""Compact producer/consumer regressions; author only until external acceptance.

Primary contract: input-only handoff admits the exact producer generation, rejects
corruption/mixed provenance, and restores ready executables without a compiler.
Symbol archive tests cover a different transport and do not cover this boundary.
"""
import hashlib
import json
import os
from pathlib import Path
import runpy
import struct
import tempfile
import unittest
from unittest.mock import patch
import shutil

from test_collect_symbols import pe, embedded
from test_cua_payload import ready_inputs

RUNTIME = runpy.run_path(str(Path(__file__).with_name("runtime-inputs.py")))
RESTORE = runpy.run_path(str(Path(__file__).with_name("release-inputs.py")))
DESKTOP = runpy.run_path(str(Path(__file__).with_name("desktop-inputs.py")))
CUA = runpy.run_path(str(Path(__file__).with_name("cua-driver.py")))
TARGET = "x86_64-pc-windows-msvc"


def fixture(directory):
    directory.mkdir()
    commit = RUNTIME["context"](TARGET, "desktop")["commit"]
    binaries = []
    for index, name in enumerate(("pioneer-app.exe", "pioneer-app-updater.exe")):
        data = bytearray(pe(guid=bytes([index + 1]) * 16))
        struct.pack_into("<H", data, 0x84, 0x8664)
        if name == "pioneer-app.exe":
            data += embedded(dict(schema_version=1, manifest_id="desktop-fixture", revision=commit,
                target=TARGET, profile="release", dependency_revision=RUNTIME["digest"](RUNTIME["ROOT"] / "Cargo.lock")))
        (directory / name).write_bytes(data)
        binaries.append(dict(filename=name, format="pe_codeview_guid_age",
                             module_id=(bytes([index + 1]) * 16).hex() + "-3",
                             sha256=hashlib.sha256(data).hexdigest()))
    metadata = dict(schema_version=1, commit=commit, run_id="17", run_attempt="1",
                    target=TARGET, catalog_sha256=hashlib.sha256(b"catalog fixture").hexdigest(),
                    binaries=binaries, files=[])
    (directory / "inputs.json").write_text(json.dumps(metadata))
    RUNTIME["write"](directory, TARGET, "desktop", [directory / row["filename"] for row in binaries])
    return json.loads((directory / "provenance.json").read_text())


class RuntimeInputTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        catalog = self.root / "catalog.json"
        catalog.write_bytes(b"catalog fixture")
        self.environment = patch.dict(os.environ, GITHUB_RUN_ID="17", GITHUB_RUN_ATTEMPT="1",
                                      PIONEER_BUNDLED_MODEL_CATALOG=str(catalog),
                                      PIONEER_SYMBOL_INPUTS_DIR=str(self.root / "symbols"))
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.ready = self.root / "runtime"
        self.metadata = fixture(self.ready)

    def test_ready_desktop_restores_without_compiler_and_keeps_original_link_receipt(self):
        old = Path.cwd()
        os.chdir(self.root)
        try:
            # An empty PATH removes every compiler; the only executable needed
            # for source identity here is an absolute Git binary discovered by
            # the test runner before clearing PATH.
            git = shutil.which("git")
            real_check_output = RUNTIME["subprocess"].check_output
            def git_only(command, **kwargs):
                if command[:3] != ["git", "rev-parse", "HEAD"]:
                    self.fail(f"unexpected executable requested: {command}")
                return real_check_output([git, *command[1:]], **kwargs)
            with patch.dict(os.environ, PATH=""), patch.object(RUNTIME["subprocess"], "check_output", side_effect=git_only):
                RESTORE["restore"](self.ready, TARGET)
            output = self.root / "target" / TARGET / "release"
            for name in ("pioneer-app.exe", "pioneer-app-updater.exe"):
                self.assertEqual((output / name).read_bytes(), (self.ready / name).read_bytes())
            self.assertEqual((self.root / "symbols/inputs.json").read_bytes(), (self.ready / "inputs.json").read_bytes())
        finally:
            os.chdir(old)

    def test_mixed_version_target_catalog_or_run_is_rejected(self):
        for key in ("version", "target", "variant", "catalog_sha256", "commit", "run_id"):
            with self.subTest(key=key):
                altered = dict(self.metadata, **{key: "different"})
                (self.ready / "provenance.json").write_text(json.dumps(altered))
                with self.assertRaisesRegex(ValueError, "different " + key):
                    RUNTIME["read"](self.ready, TARGET, "desktop")
        (self.ready / "provenance.json").write_text(json.dumps(self.metadata))

    def test_corruption_and_unapproved_extra_input_are_rejected(self):
        binary = self.ready / "pioneer-app.exe"
        original = binary.read_bytes()
        binary.write_bytes(original + b"changed bytes")
        with self.assertRaisesRegex(ValueError, "checksum or inventory mismatch"):
            RUNTIME["read"](self.ready, TARGET, "desktop")
        binary.write_bytes(original)
        (self.ready / "unexpected.exe").write_bytes(b"unapproved")
        with self.assertRaisesRegex(ValueError, "checksum or inventory mismatch"):
            RUNTIME["read"](self.ready, TARGET, "desktop")

    def test_later_attempt_reuses_same_run_but_mixed_link_producer_is_rejected(self):
        with patch.dict(os.environ, GITHUB_RUN_ATTEMPT="2"):
            RUNTIME["read"](self.ready, TARGET, "desktop")
        link = json.loads((self.ready / "inputs.json").read_text())
        link["run_attempt"] = "2"
        (self.ready / "inputs.json").write_text(json.dumps(link))
        # Even a self-consistent transport inventory cannot substitute a receipt
        # for a different producer attempt with the same executable bytes.
        altered = dict(self.metadata, files=RUNTIME["file_inventory"](self.ready))
        (self.ready / "provenance.json").write_text(json.dumps(altered))
        with self.assertRaisesRegex(ValueError, "symbol inputs disagree on run_attempt"):
            RUNTIME["read"](self.ready, TARGET, "desktop")

    def test_desktop_embeds_the_approved_producer_zip_and_rejects_changed_delivery(self):
        gateway_binary, prepared, _ = ready_inputs(self.root, TARGET)
        context = RUNTIME["context"](TARGET, "computer-use")
        body = bytearray(pe())
        struct.pack_into("<H", body, 0x84, 0x8664)
        body += embedded(dict(schema_version=1, manifest_id="gateway-fixture",
            revision=context["commit"], target=TARGET, features=["COMPUTER_USE"], profile="release",
            dependency_revision=RUNTIME["digest"](RUNTIME["ROOT"] / "Cargo.lock")))
        gateway_binary.write_bytes(body)
        original = dict(filename="pioneer.exe", format="pe_codeview_guid_age",
                        module_id=bytes(range(16)).hex() + "-3", sha256=hashlib.sha256(body).hexdigest())
        link = self.root / "gateway-link.json"
        link.write_text(json.dumps(dict(schema_version=1, commit=context["commit"],
            run_id="17", run_attempt="1", target=TARGET, variant="computer-use",
            catalog_sha256=context["catalog_sha256"], binaries=[original], files=[])))
        compiled = self.root / "compiled-gateway"
        source = RUNTIME["export"](compiled, TARGET, "computer-use", [gateway_binary], link)
        approved = self.root / "approved-gateway"
        approved.mkdir()
        asset = "pioneer-gateway-windows-x86_64-computer-use-package.zip"
        CUA["package"](TARGET, gateway_binary, prepared, approved / asset)
        archive_hash = hashlib.sha256((approved / asset).read_bytes()).hexdigest()
        (approved / "SHA256SUMS").write_text(f"sha256:{archive_hash} {asset}\n")
        shutil.copy2(link, approved / "inputs.json")
        RUNTIME["write"](approved, TARGET, "computer-use", [],
            source=source, stage="gateway-final", gateway_asset=asset, gateway_sha256=archive_hash)
        destination = self.root / "desktop/gateway"
        with patch("sys.argv", ["desktop-inputs.py", "embed", "--target", TARGET,
                               "--runtime", str(approved), "--destination", str(destination)]):
            DESKTOP["main"]()
        self.assertEqual((destination / asset).read_bytes(), (approved / asset).read_bytes())
        self.assertEqual({path.name for path in destination.iterdir()},
                         {asset, "SHA256SUMS", "provenance.json"})
        (destination / asset).write_bytes((destination / asset).read_bytes() + b"changed delivery")
        with self.assertRaisesRegex(ValueError, "changed approved Gateway payload"):
            DESKTOP["verify_bundle"](approved, destination, TARGET)

        # A self-consistent transport receipt must still reject a replacement
        # executable that does not map to the retained compiler's module.
        body[0x224:0x234] = bytes([99]) * 16
        gateway_binary.write_bytes(body)
        (approved / asset).unlink()
        CUA["package"](TARGET, gateway_binary, prepared, approved / asset)
        archive_hash = hashlib.sha256((approved / asset).read_bytes()).hexdigest()
        (approved / "SHA256SUMS").write_text(f"sha256:{archive_hash} {asset}\n")
        RUNTIME["write"](approved, TARGET, "computer-use", [],
            source=source, stage="gateway-final", gateway_asset=asset, gateway_sha256=archive_hash)
        with self.assertRaisesRegex(ValueError, "approved compiler module"):
            DESKTOP["gateway"](approved, TARGET)


if __name__ == "__main__":
    unittest.main()
