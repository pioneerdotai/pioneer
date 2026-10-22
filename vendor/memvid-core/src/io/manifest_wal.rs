//! Durable manifest WAL for append-only segment descriptors (parallel ingestion).
//!
//! This WAL stores [`IndexSegmentRef`] entries using a simple length-prefixed format:
//!
//! ```text
//! file header: [ magic (8 bytes) | version (u32) ]
//! record: [ len (u32 LE) | checksum (32 bytes) | payload (len bytes bincode) ]
//! ```
//!
//! On startup we validate the header, stream all intact records, and truncate any
//! trailing partial record caused by a crash. Writes append records and rely on the
//! caller to `flush()` when durability is required.

use std::{
    fs::{File, OpenOptions},
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use bincode::serde::{decode_from_slice, encode_to_vec};
use blake3::hash;

use crate::{Result, error::MemvidError, types::IndexSegmentRef};

const FILE_MAGIC: [u8; 8] = *b"MVSGWAL1";
const FILE_VERSION: u32 = 1;
const FILE_HEADER_SIZE: usize = FILE_MAGIC.len() + 4;
const RECORD_HEADER_SIZE: usize = 4 + 32; // length (u32) + checksum
const MAX_RECORD_BYTES: usize = 4 * 1024 * 1024; // 4 MiB segments metadata upper bound

fn wal_config() -> impl bincode::config::Config {
    bincode::config::standard()
        .with_fixed_int_encoding()
        .with_little_endian()
}

/// Crash-safe manifest log backing the upcoming parallel segment builder.
pub struct ManifestWal {
    #[allow(dead_code)]
    path: PathBuf,
    file: File,
    entries: Vec<IndexSegmentRef>,
    write_offset: u64,
}

impl ManifestWal {
    /// Opens the WAL at `path`, creating it if necessary and replaying intact entries.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path_buf)?;

        let mut wal = Self {
            path: path_buf,
            file,
            entries: Vec::new(),
            write_offset: FILE_HEADER_SIZE as u64,
        };
        wal.bootstrap()?;
        Ok(wal)
    }

    /// Appends a batch of segment references.
    pub fn append_segments(&mut self, segments: &[IndexSegmentRef]) -> Result<()> {
        for segment in segments {
            self.append_one(segment)?;
        }
        Ok(())
    }

    /// Returns a copy of the replayed entries.
    pub fn replay(&self) -> Result<Vec<IndexSegmentRef>> {
        Ok(self.entries.clone())
    }

    /// Returns true when every journaled descriptor has already been materialised.
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Flushes the WAL to durable storage (fsync).
    pub fn flush(&mut self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Truncates the WAL back to just the header after entries are materialised.
    pub fn truncate(&mut self) -> Result<()> {
        self.truncate_at(FILE_HEADER_SIZE as u64)?;
        self.entries.clear();
        self.write_offset = FILE_HEADER_SIZE as u64;
        self.file.seek(SeekFrom::Start(self.write_offset))?;
        Ok(())
    }

    fn append_one(&mut self, segment: &IndexSegmentRef) -> Result<()> {
        let payload = encode_to_vec(segment, wal_config())?;
        if payload.len() > MAX_RECORD_BYTES {
            return Err(MemvidError::CheckpointFailed {
                reason: "manifest wal payload exceeds limit".into(),
            });
        }

        let checksum = hash(&payload);
        self.file.seek(SeekFrom::Start(self.write_offset))?;
        // Safe: validated payload.len() <= MAX_RECORD_BYTES (4MB) on line 96
        self.file
            .write_all(&(u32::try_from(payload.len()).unwrap_or(u32::MAX)).to_le_bytes())?;
        self.file.write_all(checksum.as_bytes())?;
        self.file.write_all(&payload)?;

        self.write_offset += (RECORD_HEADER_SIZE + payload.len()) as u64;
        self.entries.push(segment.clone());
        Ok(())
    }

    fn bootstrap(&mut self) -> Result<()> {
        self.ensure_header()?;
        let (entries, offset) = self.scan_entries()?;
        self.entries = entries;
        self.write_offset = offset;
        self.file.seek(SeekFrom::Start(self.write_offset))?;
        Ok(())
    }

    fn ensure_header(&mut self) -> Result<()> {
        let len = self.file.metadata()?.len();
        if len < FILE_HEADER_SIZE as u64 {
            self.file.set_len(0)?;
            self.file.seek(SeekFrom::Start(0))?;
            self.file.write_all(&FILE_MAGIC)?;
            self.file.write_all(&FILE_VERSION.to_le_bytes())?;
            self.file.sync_data()?;
            self.write_offset = FILE_HEADER_SIZE as u64;
            return Ok(());
        }

        let mut magic = [0u8; FILE_MAGIC.len()];
        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_exact(&mut magic)?;
        if magic != FILE_MAGIC {
            return Err(MemvidError::InvalidHeader {
                reason: "manifest wal magic mismatch".into(),
            });
        }

        let mut version_bytes = [0u8; 4];
        self.file.read_exact(&mut version_bytes)?;
        let version = u32::from_le_bytes(version_bytes);
        if version != FILE_VERSION {
            return Err(MemvidError::InvalidHeader {
                reason: "manifest wal version mismatch".into(),
            });
        }

        Ok(())
    }

    fn scan_entries(&mut self) -> Result<(Vec<IndexSegmentRef>, u64)> {
        let mut entries = Vec::new();
        let mut offset = FILE_HEADER_SIZE as u64;
        let file_len = self.file.metadata()?.len();

        while offset < file_len {
            if file_len - offset < RECORD_HEADER_SIZE as u64 {
                self.truncate_at(offset)?;
                break;
            }

            self.file.seek(SeekFrom::Start(offset))?;
            let mut header = [0u8; RECORD_HEADER_SIZE];
            if let Err(err) = self.file.read_exact(&mut header) {
                if err.kind() == ErrorKind::UnexpectedEof {
                    self.truncate_at(offset)?;
                    break;
                }
                return Err(err.into());
            }
            offset += RECORD_HEADER_SIZE as u64;

            let payload_len = u32::from_le_bytes(header[..4].try_into().unwrap()) as u64;
            if payload_len == 0 {
                return Err(MemvidError::ManifestWalCorrupted {
                    offset: offset - RECORD_HEADER_SIZE as u64,
                    reason: "record length is zero",
                });
            }
            if payload_len as usize > MAX_RECORD_BYTES {
                return Err(MemvidError::ManifestWalCorrupted {
                    offset: offset - RECORD_HEADER_SIZE as u64,
                    reason: "record length exceeds limit",
                });
            }
            if offset + payload_len > file_len {
                offset -= RECORD_HEADER_SIZE as u64;
                self.truncate_at(offset)?;
                break;
            }

            // Safe: validated payload_len <= MAX_RECORD_BYTES (4MB) on line 184
            let mut payload = vec![0u8; payload_len as usize];
            if let Err(err) = self.file.read_exact(&mut payload) {
                if err.kind() == ErrorKind::UnexpectedEof {
                    offset -= RECORD_HEADER_SIZE as u64;
                    self.truncate_at(offset)?;
                    break;
                }
                return Err(err.into());
            }
            offset += payload_len;

            let digest = hash(&payload);
            if digest.as_bytes() != &header[4..] {
                return Err(MemvidError::ChecksumMismatch {
                    context: "manifest_wal",
                });
            }

            let (segment, consumed) = decode_from_slice(&payload, wal_config())?;
            if consumed != payload.len() {
                return Err(MemvidError::ManifestWalCorrupted {
                    offset: offset - payload_len,
                    reason: "payload contains trailing bytes",
                });
            }
            entries.push(segment);
        }

        Ok((entries, offset))
    }

    fn truncate_at(&mut self, offset: u64) -> Result<()> {
        self.file.set_len(offset)?;
        self.file.sync_data()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{SegmentCommon, SegmentKind, SegmentSpan, SegmentStats};
    use tempfile::tempdir;

    fn sample_segment(id: u64) -> IndexSegmentRef {
        let mut common = SegmentCommon::new(id, id * 10, 128, [id as u8; 32]);
        common.span = Some(SegmentSpan {
            frame_start: id * 100,
            frame_end: id * 100 + 10,
            ..SegmentSpan::default()
        });
        IndexSegmentRef {
            kind: SegmentKind::Vector,
            common,
            stats: SegmentStats {
                doc_count: 1,
                vector_count: 10,
                time_entries: 0,
                bytes_uncompressed: 2048,
                build_micros: 42,
            },
        }
    }

    #[test]
    fn append_and_replay_roundtrip() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("wal.mv2");
        {
            let mut wal = ManifestWal::open(&path)?;
            wal.append_segments(&[sample_segment(1), sample_segment(2)])?;
            wal.flush()?;
        }
        let wal = ManifestWal::open(&path)?;
        let entries = wal.replay()?;
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].common.segment_id, 1);
        assert_eq!(entries[1].common.segment_id, 2);
        Ok(())
    }

    #[test]
    fn truncates_partial_record() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("wal.mv2");
        {
            let mut wal = ManifestWal::open(&path)?;
            wal.append_segments(&[sample_segment(7)])?;
            wal.flush()?;
        }

        // Simulate crash mid-record by chopping a few bytes.
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let len = file.metadata()?.len();
        file.set_len(len.saturating_sub(5))?;

        let wal = ManifestWal::open(&path)?;
        let entries = wal.replay()?;
        assert!(entries.is_empty(), "partial record should be dropped");
        Ok(())
    }
}
