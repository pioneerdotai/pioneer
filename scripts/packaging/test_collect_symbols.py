"""Fixture-only post-link parser/pipeline regression tests. Run only after approval."""
import importlib.util
import hashlib
import json
import pathlib
import struct
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('collect_symbols', pathlib.Path(__file__).with_name('collect-symbols.py'))
symbols = importlib.util.module_from_spec(spec)
spec.loader.exec_module(symbols)
GUID = bytes(range(16))


def pe(guid=GUID, age=3):
    data = bytearray(0x250)
    data[:2] = b'MZ'
    struct.pack_into('<I', data, 0x3c, 0x80)
    data[0x80:0x84] = b'PE\0\0'
    struct.pack_into('<H', data, 0x86, 1)
    struct.pack_into('<H', data, 0x94, 240)
    optional = 0x98
    struct.pack_into('<H', data, optional, 0x20b)
    struct.pack_into('<II', data, optional + 112 + 48, 0x1000, 28)
    struct.pack_into('<4I', data, optional + 240 + 8, 0x100, 0x1000, 0x100, 0x200)
    struct.pack_into('<4I', data, 0x200 + 12, 2, 24, 0x1020, 0x220)
    data[0x220:0x224] = b'RSDS'
    data[0x224:0x234] = guid
    struct.pack_into('<I', data, 0x234, age)
    return bytes(data)


def pdb(guid=GUID, age=3):
    data = bytearray(512 * 5)
    magic = b'Microsoft C/C++ MSF 7.00\r\n\x1aDS\0\0\0'
    data[:len(magic)] = magic
    struct.pack_into('<6I', data, 32, 512, 1, 5, 16, 0, 2)
    struct.pack_into('<I', data, 1024, 3)
    struct.pack_into('<4I', data, 1536, 2, 0, 28, 4)
    struct.pack_into('<3I', data, 2048, 20000404, 0, age)
    data[2060:2076] = guid
    return bytes(data)


def embedded(build):
    body = json.dumps(build).encode()
    return b'PIONEER_BUILD_V1\0' + struct.pack('<I', len(body)) + body


class ExactSymbolsTests(unittest.TestCase):
    def test_native_ids_are_not_revision_or_rsds_string_search(self):
        macho = bytearray(56)
        macho[:4] = b'\xcf\xfa\xed\xfe'
        struct.pack_into('<I', macho, 16, 1)
        struct.pack_into('<II', macho, 32, 0x1b, 24)
        macho[40:56] = GUID
        self.assertEqual(symbols.identity(macho), ('macho_uuid', GUID.hex()))
        self.assertEqual(symbols.identity(pe() + b'RSDS' + bytes(20)), ('pe_codeview_guid_age', GUID.hex() + '-3'))
        self.assertEqual(symbols.pdb_identity(pdb()), GUID.hex() + '-3')
        self.assertNotEqual(symbols.pdb_identity(pdb(age=4)), symbols.identity(pe())[1])

    def test_elf_build_id_comes_from_gnu_note(self):
        data = bytearray(156)
        data[:6] = b'\x7fELF\x02\x01'
        struct.pack_into('<Q', data, 32, 64)
        struct.pack_into('<HH', data, 54, 56, 1)
        struct.pack_into('<I', data, 64, 4)
        struct.pack_into('<Q', data, 72, 120)
        struct.pack_into('<Q', data, 96, 36)
        struct.pack_into('<III', data, 120, 4, 20, 3)
        data[132:136] = b'GNU\0'
        data[136:156] = bytes(range(20))
        self.assertEqual(symbols.identity(data), ('elf_build_id', bytes(range(20)).hex()))

    def test_release_consumer_retains_exact_pdb_and_checks_packaged_build(self):
        build = {'schema_version': 1, 'manifest_id': 'fixture-build', 'revision': 'same-version'}
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binary = root / 'pioneer.exe'
            binary.write_bytes(pe() + embedded(build))
            binary.with_suffix('.pdb').write_bytes(pdb())
            out = root / 'symbols'
            with patch('sys.argv', ['collect-symbols.py', str(binary), str(out)]):
                symbols.main()
            manifests = list(out.glob('*/manifest.json'))
            self.assertEqual(len(manifests), 1)
            value = json.loads(manifests[0].read_text())
            self.assertTrue(value['symbols_validated'])
            self.assertEqual(value['build'], build)
            self.assertEqual(len(value['files']), 2)
            binary.write_bytes(pe() + embedded(build) + b'signing-envelope')
            with patch('sys.argv', ['collect-symbols.py', '--verify-packaged', str(binary), str(out)]):
                symbols.main()
            value = json.loads(manifests[0].read_text())
            self.assertNotEqual(value['binary_sha256'], value['packaged_binary_sha256'])
            first_packaged = hashlib.sha256(binary.read_bytes()).hexdigest()
            binary.write_bytes(pe() + embedded(build) + b'deep-signing-envelope')
            with patch('sys.argv', ['collect-symbols.py', '--verify-packaged', str(binary), str(out)]):
                symbols.main()
            value = json.loads(manifests[0].read_text())
            self.assertEqual(value['packaged_binary_sha256s'], [first_packaged, hashlib.sha256(binary.read_bytes()).hexdigest()])
            binary.write_bytes(pe() + embedded({**build, 'manifest_id': 'other-build'}))
            with patch('sys.argv', ['collect-symbols.py', '--verify-packaged', str(binary), str(out)]):
                with self.assertRaisesRegex(ValueError, 'build manifest mismatch'):
                    symbols.main()

    def test_hyphenated_desktop_binary_retains_cargo_underscore_pdb_name(self):
        build = {'schema_version': 1, 'manifest_id': 'desktop-fixture'}
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binary = root / 'pioneer-app.exe'
            binary.write_bytes(pe() + embedded(build))
            (root / 'pioneer_app.pdb').write_bytes(pdb())
            out = root / 'symbols'
            with patch('sys.argv', ['collect-symbols.py', str(binary), str(out)]):
                symbols.main()
            manifest = json.loads(next(out.glob('*/manifest.json')).read_text())
            self.assertIn('pioneer_app.pdb', [file['filename'] for file in manifest['files']])
            self.assertNotIn('pioneer-app.pdb', [file['filename'] for file in manifest['files']])

    def test_missing_or_ambiguous_embedded_build_is_rejected(self):
        with self.assertRaises(ValueError):
            symbols.build_manifest(b'no-build')
        with self.assertRaises(ValueError):
            symbols.build_manifest(embedded({'schema_version': 1, 'manifest_id': 'one'}) + embedded({'schema_version': 1, 'manifest_id': 'two'}))


if __name__ == '__main__':
    unittest.main()
