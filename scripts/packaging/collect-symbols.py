#!/usr/bin/env python3
"""Retain and validate symbols for an EXACT linked executable. Never rebuild it.
Called by both release consumers before build outputs can be removed. No tests,
application startup, attach, signing, stripping or publication is performed.
"""
import argparse
import hashlib
import json
import math
import mmap
import os
import pathlib
import shutil
import struct
import subprocess
import sys


def command(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT)


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(8 * 1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def identity(data):
    if data[:4] in (b'\xcf\xfa\xed\xfe', b'\xce\xfa\xed\xfe'):
        cursor = 32 if data[:4] == b'\xcf\xfa\xed\xfe' else 28
        for _ in range(struct.unpack_from('<I', data, 16)[0]):
            cmd, size = struct.unpack_from('<II', data, cursor)
            if cmd == 0x1b:
                return 'macho_uuid', data[cursor+8:cursor+24].hex()
            if size < 8:
                raise ValueError('invalid Mach-O load command')
            cursor += size
    elif data[:4] == b'\x7fELF':
        endian = '<' if data[5] == 1 else '>'
        elf64 = data[4] == 2
        phoff = struct.unpack_from(endian + ('Q' if elf64 else 'I'), data, 32 if elf64 else 28)[0]
        size, count = struct.unpack_from(endian+'HH', data, 54 if elf64 else 42)
        for index in range(count):
            pos = phoff + size*index
            typ = struct.unpack_from(endian+'I', data, pos)[0]
            if typ != 4:
                continue
            off = struct.unpack_from(endian+('Q' if elf64 else 'I'), data, pos+(8 if elf64 else 4))[0]
            length = struct.unpack_from(endian+('Q' if elf64 else 'I'), data, pos+(32 if elf64 else 16))[0]
            cursor, end = off, off+length
            while cursor+12 <= end:
                namesz, descsz, note = struct.unpack_from(endian+'III', data, cursor)
                name = data[cursor+12:cursor+12+namesz]
                desc = cursor+12+((namesz+3)//4)*4
                if name.rstrip(b'\0') == b'GNU' and note == 3:
                    return 'elf_build_id', data[desc:desc+descsz].hex()
                cursor = desc+((descsz+3)//4)*4
    elif data[:2] == b'MZ':
        pe = struct.unpack_from('<I', data, 0x3c)[0]
        if data[pe:pe+4] != b'PE\0\0':
            raise ValueError('invalid PE signature')
        count = struct.unpack_from('<H', data, pe+6)[0]
        optional_size = struct.unpack_from('<H', data, pe+20)[0]
        optional = pe+24
        directory = optional+(112 if struct.unpack_from('<H', data, optional)[0] == 0x20b else 96)
        debug_rva, debug_size = struct.unpack_from('<II', data, directory+6*8)
        def file_offset(rva):
            for index in range(count):
                section = optional+optional_size+index*40
                virtual_size, section_rva, raw_size, raw_pointer = struct.unpack_from('<4I', data, section+8)
                if section_rva <= rva < section_rva+max(virtual_size, raw_size):
                    return raw_pointer+rva-section_rva
            raise ValueError('PE debug RVA not in section')
        debug = file_offset(debug_rva)
        candidates = []
        for cursor in range(debug, debug+debug_size, 28):
            typ, size, _, pointer = struct.unpack_from('<4I', data, cursor+12)
            if typ == 2 and size >= 24 and data[pointer:pointer+4] == b'RSDS':
                candidates.append((data[pointer+4:pointer+20].hex(), struct.unpack_from('<I', data, pointer+20)[0]))
        if len(candidates) == 1:
            guid, age = candidates[0]
            return 'pe_codeview_guid_age', f'{guid}-{age}'
    raise ValueError('missing or ambiguous supported post-link module ID')


def pdb_identity(data):
    if not data.startswith(b'Microsoft C/C++ MSF 7.00'):
        raise ValueError('unsupported PDB container')
    block, _, _, directory_bytes, _, block_map = struct.unpack_from('<6I', data, 32)
    pages = math.ceil(directory_bytes/block)
    directory_blocks = struct.unpack_from('<'+'I'*pages, data, block_map*block)
    directory = b''.join(data[p*block:(p+1)*block] for p in directory_blocks)[:directory_bytes]
    streams = struct.unpack_from('<I', directory)[0]
    sizes = struct.unpack_from('<'+'I'*streams, directory, 4)
    cursor = 4+streams*4
    for index, size in enumerate(sizes):
        if size == 0xffffffff:
            continue
        pages = math.ceil(size/block)
        blocks = struct.unpack_from('<'+'I'*pages, directory, cursor)
        cursor += pages*4
        if index == 1:
            info = b''.join(data[p*block:(p+1)*block] for p in blocks)[:size]
            age = struct.unpack_from('<I', info, 8)[0]
            return f'{info[12:28].hex()}-{age}'
    raise ValueError('PDB info stream missing')


def build_manifest(data):
    marker = b'PIONEER_BUILD_V1\0'
    values = []
    start = 0
    while True:
        start = data.find(marker, start)
        if start < 0:
            break
        try:
            length = struct.unpack_from('<I', data, start+len(marker))[0]
            if length <= 16384:
                obj = json.loads(data[start+len(marker)+4:start+len(marker)+4+length])
                if obj.get('schema_version') == 1 and obj.get('manifest_id'):
                    values.append(obj)
        except (ValueError, struct.error, UnicodeError):
            pass
        start += len(marker)
    if not values or any(value != values[0] for value in values):
        raise ValueError('executable build manifest missing or ambiguous')
    return values[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--verify-packaged', action='store_true')
    parser.add_argument('--release-input-manifest', type=pathlib.Path)
    parser.add_argument('binary', type=pathlib.Path)
    parser.add_argument('out', type=pathlib.Path)
    args = parser.parse_args()
    data = args.binary.read_bytes()
    fmt, module = identity(data)
    if args.release_input_manifest and b'PIONEER_BUILD_V1\0' not in data:
        inputs = json.loads(args.release_input_manifest.read_text())
        build = {'revision': inputs['commit'], 'component': args.binary.stem,
                 'provenance_completeness': 'release-input-manifest; no embedded build manifest'}
    else:
        build = build_manifest(data)
    if args.verify_packaged:
        matches = []
        for path in args.out.glob('*/manifest.json'):
            value = json.loads(path.read_text())
            if value['format'] == fmt and value['module_id'] == module:
                matches.append((path, value))
        if len(matches) != 1:
            raise ValueError('packaged executable has no unique retained symbol manifest')
        path, value = matches[0]
        if value['build'] != build or not value['symbols_validated']:
            raise ValueError('packaged build manifest mismatch')
        digest = hashlib.sha256(data).hexdigest()
        packaged = value.setdefault('packaged_binary_sha256s', [])
        if digest not in packaged:
            packaged.append(digest)
        # Keep the last packaged artifact fields for existing manifest readers.
        value['packaged_binary_sha256'] = digest
        value['packaged_binary_bytes'] = len(data)
        path.write_text(json.dumps(value, indent=2)+'\n')
        return
    destination = args.out / f'{args.binary.stem}-{module}'
    destination.mkdir(parents=True, exist_ok=False)
    if fmt == 'macho_uuid':
        symbols = destination / (args.binary.name+'.dSYM')
        tool = os.environ.get('DSYMUTIL')
        if tool:
            argv = [tool, '--linker', 'classic', '--num-threads', '1',
                    '--verify-dwarf=output']
            if os.environ.get('DSYMUTIL_OBJECT_ROOT'):
                argv += ['--oso-prepend-path', os.environ['DSYMUTIL_OBJECT_ROOT']]
            result = subprocess.run([*argv, str(args.binary), '-o', str(symbols)],
                                    capture_output=True, text=True)
            (destination/'dsymutil.stdout').write_text(result.stdout)
            (destination/'dsymutil.stderr').write_text(result.stderr)
            if result.returncode or result.stderr.strip():
                raise ValueError('dSYM generation/verification failed or warned; inspect retained dsymutil logs')
        else:
            command('xcrun', 'dsymutil', str(args.binary), '-o', str(symbols))
        dwarf = symbols/'Contents/Resources/DWARF'/args.binary.name
        with dwarf.open('rb') as source, mmap.mmap(source.fileno(), 0, access=mmap.ACCESS_READ) as mapped:
            if identity(mapped) != (fmt, module):
                raise ValueError('dSYM UUID mismatch')
        details = command('xcrun', 'dwarfdump', '--uuid', str(symbols))
    elif fmt == 'elf_build_id':
        symbols = destination / (args.binary.name+'.debug')
        command('objcopy', '--only-keep-debug', str(args.binary), str(symbols))
        # .debug preserves ELF notes even though program segments are removed;
        # readelf validates the ID of the separate debug object itself.
        details = command('readelf', '-n', str(symbols))
        if module not in details.replace(' ', '').lower():
            raise ValueError('ELF debug build-id mismatch')
        sections = command('readelf', '-S', '--wide', str(symbols))
        if '.debug_info' not in sections or '.debug_line' not in sections:
            raise ValueError('ELF input has no full source debug information')
    else:
        # Cargo preserves rustc's underscore PDB name even when uplifting a
        # hyphenated executable (e.g. pioneer-app.exe -> pioneer_app.pdb).
        source = args.binary.with_name(args.binary.stem.replace('-', '_')+'.pdb')
        symbols = destination/source.name
        if pdb_identity(source.read_bytes()) != module:
            raise ValueError('PDB GUID/age mismatch')
        shutil.copy2(source, symbols)
        details = 'PDB info stream GUID/age matched PE CodeView'
    # Preserve exact executable too: native tools may need it for inline/code
    # metadata. Archive is private CI storage, not an installer payload.
    shutil.copy2(args.binary, destination/args.binary.name)
    manifest = {'schema_version': 1, 'format': fmt, 'module_id': module,
                'binary_sha256': hashlib.sha256(data).hexdigest(), 'binary_bytes': len(data),
                'build': build, 'symbols_validated': True, 'validation': details,
                'files': []}
    for file in sorted(destination.rglob('*')):
        if file.is_file():
            manifest['files'].append({'filename': str(file.relative_to(destination)),
                                      'bytes': file.stat().st_size, 'sha256': file_sha256(file)})
    (destination/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')


if __name__ == '__main__':
    main()
