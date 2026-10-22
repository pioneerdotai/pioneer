use std::{
    convert::TryInto,
    io::{Read, Seek, SeekFrom, Write},
};

use crate::{
    constants::{HEADER_SIZE, MAGIC, SPEC_MAJOR, SPEC_MINOR, WAL_OFFSET},
    error::{MemvidError, Result},
    types::Header,
};

const VERSION_OFFSET: usize = 4;
const SPEC_BYTES_OFFSET: usize = 6;
const FOOTER_OFFSET_POS: usize = 8;
const WAL_OFFSET_POS: usize = 16;
const WAL_SIZE_POS: usize = 24;
const WAL_CHECKPOINT_POS: usize = 32;
const WAL_SEQUENCE_POS: usize = 40;
const TOC_CHECKSUM_POS: usize = 48;
const TOC_CHECKSUM_END: usize = 80;
// Legacy lock metadata occupied bytes 80..140 within the header padding.
const LEGACY_LOCK_REGION_START: usize = TOC_CHECKSUM_END;
const LEGACY_LOCK_REGION_END: usize = LEGACY_LOCK_REGION_START + 60;
const EXPECTED_VERSION: u16 = ((SPEC_MAJOR as u16) << 8) | SPEC_MINOR as u16;

/// Deterministic encoder/decoder for the fixed-size header region.
pub struct HeaderCodec;

impl HeaderCodec {
    /// Writes the header back to the beginning of the file, zero-filling any unused bytes.
    pub fn write<W: Write + Seek>(mut writer: W, header: &Header) -> Result<()> {
        let bytes = Self::encode(header)?;
        writer.seek(SeekFrom::Start(0))?;
        writer.write_all(&bytes)?;
        Ok(())
    }

    /// Reads and decodes the header from the start of the file. If legacy lock metadata is present
    /// in the reserved padding, it is cleared in-place before decoding to maintain forward
    /// compatibility with older files.
    pub fn read<R: Read + Write + Seek>(mut reader: R) -> Result<Header> {
        let mut buf = [0u8; HEADER_SIZE];
        reader.seek(SeekFrom::Start(0))?;
        reader.read_exact(&mut buf)?;
        if clear_legacy_lock_metadata(&mut buf) {
            reader.seek(SeekFrom::Start(0))?;
            reader.write_all(&buf)?;
            reader.flush()?;
        }
        Self::decode(&buf)
    }

    /// Encodes a header into the canonical 4 KB byte representation.
    pub fn encode(header: &Header) -> Result<[u8; HEADER_SIZE]> {
        if header.magic != MAGIC {
            return Err(MemvidError::InvalidHeader {
                reason: "magic mismatch".into(),
            });
        }
        if header.version != EXPECTED_VERSION {
            return Err(MemvidError::InvalidHeader {
                reason: "unsupported version".into(),
            });
        }
        if header.wal_offset < WAL_OFFSET {
            return Err(MemvidError::InvalidHeader {
                reason: "wal_offset precedes data region".into(),
            });
        }
        if header.wal_size == 0 {
            return Err(MemvidError::InvalidHeader {
                reason: "wal_size must be non-zero".into(),
            });
        }

        let mut buf = [0u8; HEADER_SIZE];
        buf[..MAGIC.len()].copy_from_slice(&header.magic);
        buf[VERSION_OFFSET..VERSION_OFFSET + 2].copy_from_slice(&header.version.to_le_bytes());
        buf[SPEC_BYTES_OFFSET] = SPEC_MAJOR;
        buf[SPEC_BYTES_OFFSET + 1] = SPEC_MINOR;
        buf[FOOTER_OFFSET_POS..FOOTER_OFFSET_POS + 8]
            .copy_from_slice(&header.footer_offset.to_le_bytes());
        buf[WAL_OFFSET_POS..WAL_OFFSET_POS + 8].copy_from_slice(&header.wal_offset.to_le_bytes());
        buf[WAL_SIZE_POS..WAL_SIZE_POS + 8].copy_from_slice(&header.wal_size.to_le_bytes());
        buf[WAL_CHECKPOINT_POS..WAL_CHECKPOINT_POS + 8]
            .copy_from_slice(&header.wal_checkpoint_pos.to_le_bytes());
        buf[WAL_SEQUENCE_POS..WAL_SEQUENCE_POS + 8]
            .copy_from_slice(&header.wal_sequence.to_le_bytes());
        buf[TOC_CHECKSUM_POS..TOC_CHECKSUM_END].copy_from_slice(&header.toc_checksum);
        Ok(buf)
    }

    /// Decodes the canonical header bytes into a strongly typed struct after validation.
    pub fn decode(bytes: &[u8; HEADER_SIZE]) -> Result<Header> {
        // Extract fixed-size arrays from the header buffer
        // All indices are compile-time constants, so these slices are guaranteed to fit
        let magic: [u8; 4] = extract_array(bytes, 0)?;
        if magic != MAGIC {
            return Err(MemvidError::InvalidHeader {
                reason: "magic mismatch".into(),
            });
        }

        let version = u16::from_le_bytes(extract_array(bytes, VERSION_OFFSET)?);
        if version != EXPECTED_VERSION {
            return Err(MemvidError::InvalidHeader {
                reason: "unsupported version".into(),
            });
        }

        if bytes[SPEC_BYTES_OFFSET] != SPEC_MAJOR || bytes[SPEC_BYTES_OFFSET + 1] != SPEC_MINOR {
            return Err(MemvidError::InvalidHeader {
                reason: "spec byte mismatch".into(),
            });
        }

        let footer_offset = u64::from_le_bytes(extract_array(bytes, FOOTER_OFFSET_POS)?);
        let wal_offset = u64::from_le_bytes(extract_array(bytes, WAL_OFFSET_POS)?);
        if wal_offset < WAL_OFFSET {
            return Err(MemvidError::InvalidHeader {
                reason: "wal_offset precedes data region".into(),
            });
        }
        let wal_size = u64::from_le_bytes(extract_array(bytes, WAL_SIZE_POS)?);
        if wal_size == 0 {
            return Err(MemvidError::InvalidHeader {
                reason: "wal_size must be non-zero".into(),
            });
        }
        let wal_checkpoint_pos = u64::from_le_bytes(extract_array(bytes, WAL_CHECKPOINT_POS)?);
        let wal_sequence = u64::from_le_bytes(extract_array(bytes, WAL_SEQUENCE_POS)?);
        let toc_checksum: [u8; 32] = extract_array(bytes, TOC_CHECKSUM_POS)?;

        Ok(Header {
            magic,
            version,
            footer_offset,
            wal_offset,
            wal_size,
            wal_checkpoint_pos,
            wal_sequence,
            toc_checksum,
        })
    }
}

/// Extracts a fixed-size array from a byte slice at the given offset.
/// Returns an error if the slice is too short (should never happen with valid headers).
#[inline]
fn extract_array<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N]> {
    bytes
        .get(offset..offset + N)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| MemvidError::InvalidHeader {
            reason: "header truncated".into(),
        })
}

fn clear_legacy_lock_metadata(buf: &mut [u8; HEADER_SIZE]) -> bool {
    let region = &mut buf[LEGACY_LOCK_REGION_START..LEGACY_LOCK_REGION_END];
    if region.iter().any(|byte| *byte != 0) {
        region.fill(0);
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn sample_header() -> Header {
        Header {
            magic: MAGIC,
            version: EXPECTED_VERSION,
            footer_offset: 1_048_576,
            wal_offset: WAL_OFFSET,
            wal_size: 4 * 1024 * 1024,
            wal_checkpoint_pos: 0,
            wal_sequence: 42,
            toc_checksum: [0xAB; 32],
        }
    }

    #[test]
    fn roundtrip_encode_decode() {
        let header = sample_header();
        let encoded = HeaderCodec::encode(&header).expect("encode header");
        let decoded = HeaderCodec::decode(&encoded).expect("decode header");
        assert_eq!(decoded.magic, MAGIC);
        assert_eq!(decoded.version, EXPECTED_VERSION);
        assert_eq!(decoded.footer_offset, header.footer_offset);
        assert_eq!(decoded.wal_offset, WAL_OFFSET);
        assert_eq!(decoded.toc_checksum, header.toc_checksum);
    }

    #[test]
    fn read_write_from_cursor() {
        let header = sample_header();
        let mut cursor = Cursor::new(vec![0u8; HEADER_SIZE]);
        HeaderCodec::write(&mut cursor, &header).expect("write header");
        cursor.set_position(0);
        let decoded = HeaderCodec::read(&mut cursor).expect("read header");
        assert_eq!(decoded.wal_size, header.wal_size);
        assert_eq!(decoded.wal_sequence, header.wal_sequence);
    }

    #[test]
    fn clears_legacy_lock_metadata() {
        let header = sample_header();
        let mut encoded = HeaderCodec::encode(&header).expect("encode header");
        encoded[LEGACY_LOCK_REGION_START..LEGACY_LOCK_REGION_END].fill(0xAA);
        let mut cursor = Cursor::new(encoded.to_vec());
        HeaderCodec::read(&mut cursor).expect("read header with legacy metadata");
        let sanitized = cursor.into_inner();
        assert!(
            sanitized[LEGACY_LOCK_REGION_START..LEGACY_LOCK_REGION_END]
                .iter()
                .all(|byte| *byte == 0)
        );
    }

    #[test]
    fn reject_invalid_magic() {
        let mut header = sample_header();
        header.magic = *b"BAD!";
        let err = HeaderCodec::encode(&header).expect_err("should fail");
        matches!(err, MemvidError::InvalidHeader { .. });
    }

    #[test]
    fn reject_short_wal_size() {
        let mut header = sample_header();
        header.wal_size = 0;
        let err = HeaderCodec::encode(&header).expect_err("should fail");
        matches!(err, MemvidError::InvalidHeader { .. });
    }

    #[test]
    fn reject_decoding_with_bad_version() {
        let header = sample_header();
        let mut encoded = HeaderCodec::encode(&header).expect("encode header");
        encoded[VERSION_OFFSET] = 0xFF;
        let err = HeaderCodec::decode(&encoded).expect_err("decode should fail");
        matches!(err, MemvidError::InvalidHeader { .. });
    }
}
