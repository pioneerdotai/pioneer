use crate::apply_patch::history::{
    CommittedTextSnapshot, LineEndingMetadata, TextEncoding, TextSnapshotRef,
};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::{self, Cursor, Read};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub(crate) enum BoundedSnapshotDecodeError {
    LimitExceeded,
    Io(io::Error),
}

/// Decode a stored zstd snapshot without allowing the decompressor to grow the
/// output buffer past the configured logical limit.  `decode_all` is
/// intentionally not used here: database blobs are durable input and may be
/// corrupt even though their metadata claims a smaller size.
pub(crate) fn decode_zstd_bounded(
    compressed: &[u8],
    max_decompressed_bytes: u64,
) -> Result<Vec<u8>, BoundedSnapshotDecodeError> {
    let max_bytes = usize::try_from(max_decompressed_bytes).unwrap_or(usize::MAX);
    let mut decoder = zstd::stream::read::Decoder::new(Cursor::new(compressed))
        .map_err(BoundedSnapshotDecodeError::Io)?;
    let mut output = Vec::with_capacity(max_bytes.min(64 * 1024));
    let mut chunk = [0_u8; 64 * 1024];
    loop {
        let read = decoder
            .read(&mut chunk)
            .map_err(BoundedSnapshotDecodeError::Io)?;
        if read == 0 {
            break;
        }
        if output
            .len()
            .checked_add(read)
            .is_none_or(|length| length > max_bytes)
        {
            return Err(BoundedSnapshotDecodeError::LimitExceeded);
        }
        output.extend_from_slice(&chunk[..read]);
    }
    Ok(output)
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SnapshotDomain {
    pub privacy_scope: String,
    pub encryption_scope: String,
    pub retention_scope: String,
}

impl SnapshotDomain {
    pub fn new(
        privacy_scope: impl Into<String>,
        encryption_scope: impl Into<String>,
        retention_scope: impl Into<String>,
    ) -> Self {
        Self {
            privacy_scope: privacy_scope.into(),
            encryption_scope: encryption_scope.into(),
            retention_scope: retention_scope.into(),
        }
    }

    pub fn id(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(self.privacy_scope.as_bytes());
        digest.update([0]);
        digest.update(self.encryption_scope.as_bytes());
        digest.update([0]);
        digest.update(self.retention_scope.as_bytes());
        format!("domain:{}", hex::encode(digest.finalize()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ContentAddressedSnapshotRef {
    pub domain_id: String,
    pub snapshot: TextSnapshotRef,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotStoreLimits {
    pub max_logical_bytes: u64,
    pub max_physical_bytes: u64,
    pub max_single_bytes: u64,
    pub max_decompressed_bytes: u64,
}

impl Default for SnapshotStoreLimits {
    fn default() -> Self {
        Self {
            max_logical_bytes: 512 * 1024 * 1024,
            max_physical_bytes: 256 * 1024 * 1024,
            max_single_bytes: 16 * 1024 * 1024,
            max_decompressed_bytes: 16 * 1024 * 1024,
        }
    }
}

/// Bytes reserved for one in-flight tracked patch before its first
/// filesystem mutation. Reservations are deliberately conservative: two
/// concurrent patches may reserve the same not-yet-interned content and one
/// will then be rejected rather than allowing the durable store to exceed its
/// configured quota.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SnapshotReservation {
    pub logical_bytes: u64,
    pub physical_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SnapshotStoreMetrics {
    pub blobs: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub references: u64,
    pub referenced_logical_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotStoreError {
    LimitExceeded(&'static str),
    Missing,
    Corrupt,
    HashCollision,
    Poisoned,
}

impl std::fmt::Display for SnapshotStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LimitExceeded(kind) => write!(f, "snapshot {kind} limit exceeded"),
            Self::Missing => f.write_str("snapshot is not present"),
            Self::Corrupt => f.write_str("snapshot failed compressed length/hash verification"),
            Self::HashCollision => f.write_str("content hash collision detected"),
            Self::Poisoned => f.write_str("snapshot store lock is poisoned"),
        }
    }
}

impl std::error::Error for SnapshotStoreError {}

#[derive(Clone, Debug)]
pub struct ContentAddressedSnapshotStore {
    limits: SnapshotStoreLimits,
    state: Arc<Mutex<SnapshotState>>,
}

#[derive(Debug, Default)]
struct SnapshotState {
    blobs: HashMap<(String, [u8; 32], u64), StoredBlob>,
    logical_bytes: u64,
    physical_bytes: u64,
    references: u64,
}

#[derive(Debug)]
struct StoredBlob {
    compressed: Vec<u8>,
    raw_len: u64,
    encoding: TextEncoding,
    line_endings: LineEndingMetadata,
    references: u64,
}

impl ContentAddressedSnapshotStore {
    pub fn new(limits: SnapshotStoreLimits) -> Self {
        Self {
            limits,
            state: Arc::new(Mutex::new(SnapshotState::default())),
        }
    }

    pub fn limits(&self) -> SnapshotStoreLimits {
        self.limits
    }

    pub fn put(
        &self,
        domain: &SnapshotDomain,
        snapshot: &CommittedTextSnapshot,
    ) -> Result<ContentAddressedSnapshotRef, SnapshotStoreError> {
        let bytes = &snapshot.bytes;
        let byte_len = bytes.len() as u64;
        if byte_len > self.limits.max_single_bytes {
            return Err(SnapshotStoreError::LimitExceeded("single blob"));
        }
        if byte_len > self.limits.max_decompressed_bytes {
            return Err(SnapshotStoreError::LimitExceeded("decompression"));
        }
        let content_hash = *snapshot.version.token.digest();
        if snapshot.version.token.byte_len() != byte_len {
            return Err(SnapshotStoreError::Corrupt);
        }
        let actual_hash: [u8; 32] = Sha256::digest(bytes.as_slice()).into();
        if actual_hash != content_hash {
            return Err(SnapshotStoreError::Corrupt);
        }
        let compressed = zstd::stream::encode_all(bytes.as_slice(), 3)
            .map_err(|_| SnapshotStoreError::Corrupt)?;
        let key = (domain.id(), content_hash, byte_len);
        let mut state = self
            .state
            .lock()
            .map_err(|_| SnapshotStoreError::Poisoned)?;
        if state.references == u64::MAX {
            return Err(SnapshotStoreError::Corrupt);
        }
        if let Some(existing) = state.blobs.get_mut(&key) {
            if existing.raw_len != byte_len
                || existing.encoding != snapshot.encoding
                || existing.line_endings != snapshot.line_endings
            {
                return Err(SnapshotStoreError::HashCollision);
            }
            if existing.references == u64::MAX {
                return Err(SnapshotStoreError::Corrupt);
            }
            existing.references += 1;
            state.references += 1;
            return Ok(ContentAddressedSnapshotRef {
                domain_id: key.0,
                snapshot: TextSnapshotRef {
                    schema_version: crate::apply_patch::history::SNAPSHOT_REF_SCHEMA_VERSION,
                    content_hash,
                    byte_len,
                    encoding: snapshot.encoding,
                    line_endings: snapshot.line_endings,
                },
            });
        }
        let next_logical = state
            .logical_bytes
            .checked_add(byte_len)
            .ok_or(SnapshotStoreError::LimitExceeded("logical byte"))?;
        let next_physical = state
            .physical_bytes
            .checked_add(compressed.len() as u64)
            .ok_or(SnapshotStoreError::LimitExceeded("physical byte"))?;
        if next_logical > self.limits.max_logical_bytes {
            return Err(SnapshotStoreError::LimitExceeded("logical byte"));
        }
        if next_physical > self.limits.max_physical_bytes {
            return Err(SnapshotStoreError::LimitExceeded("physical byte"));
        }
        if state.references == u64::MAX {
            return Err(SnapshotStoreError::Corrupt);
        }
        state.logical_bytes = next_logical;
        state.physical_bytes = next_physical;
        state.references += 1;
        state.blobs.insert(
            key.clone(),
            StoredBlob {
                compressed,
                raw_len: byte_len,
                encoding: snapshot.encoding,
                line_endings: snapshot.line_endings,
                references: 1,
            },
        );
        Ok(ContentAddressedSnapshotRef {
            domain_id: key.0,
            snapshot: TextSnapshotRef {
                schema_version: crate::apply_patch::history::SNAPSHOT_REF_SCHEMA_VERSION,
                content_hash,
                byte_len,
                encoding: snapshot.encoding,
                line_endings: snapshot.line_endings,
            },
        })
    }

    pub fn get(
        &self,
        reference: &ContentAddressedSnapshotRef,
    ) -> Result<CommittedTextSnapshot, SnapshotStoreError> {
        if reference.snapshot.schema_version
            != crate::apply_patch::history::SNAPSHOT_REF_SCHEMA_VERSION
        {
            return Err(SnapshotStoreError::Corrupt);
        }
        if reference.snapshot.byte_len > self.limits.max_single_bytes
            || reference.snapshot.byte_len > self.limits.max_decompressed_bytes
        {
            return Err(SnapshotStoreError::LimitExceeded("snapshot reference"));
        }
        let key = (
            reference.domain_id.clone(),
            reference.snapshot.content_hash,
            reference.snapshot.byte_len,
        );
        let state = self
            .state
            .lock()
            .map_err(|_| SnapshotStoreError::Poisoned)?;
        let Some(blob) = state.blobs.get(&key) else {
            return Err(SnapshotStoreError::Missing);
        };
        if blob.encoding != reference.snapshot.encoding
            || blob.line_endings != reference.snapshot.line_endings
        {
            return Err(SnapshotStoreError::Corrupt);
        }
        if blob.raw_len > self.limits.max_decompressed_bytes {
            return Err(SnapshotStoreError::LimitExceeded("decompression"));
        }
        let bytes = match decode_zstd_bounded(
            blob.compressed.as_slice(),
            self.limits.max_decompressed_bytes,
        ) {
            Ok(bytes) => bytes,
            Err(BoundedSnapshotDecodeError::LimitExceeded) => {
                return Err(SnapshotStoreError::LimitExceeded("decompression"));
            }
            Err(BoundedSnapshotDecodeError::Io(_)) => {
                return Err(SnapshotStoreError::Corrupt);
            }
        };
        let actual_hash: [u8; 32] = Sha256::digest(bytes.as_slice()).into();
        if bytes.len() as u64 != blob.raw_len
            || bytes.len() as u64 != reference.snapshot.byte_len
            || actual_hash != reference.snapshot.content_hash
        {
            return Err(SnapshotStoreError::Corrupt);
        }
        Ok(CommittedTextSnapshot::from_bytes(
            bytes,
            blob.encoding,
            blob.line_endings,
        ))
    }

    pub fn release(
        &self,
        reference: &ContentAddressedSnapshotRef,
    ) -> Result<bool, SnapshotStoreError> {
        if reference.snapshot.schema_version
            != crate::apply_patch::history::SNAPSHOT_REF_SCHEMA_VERSION
        {
            return Err(SnapshotStoreError::Corrupt);
        }
        if reference.snapshot.byte_len > self.limits.max_single_bytes
            || reference.snapshot.byte_len > self.limits.max_decompressed_bytes
        {
            return Err(SnapshotStoreError::LimitExceeded("snapshot reference"));
        }
        let key = (
            reference.domain_id.clone(),
            reference.snapshot.content_hash,
            reference.snapshot.byte_len,
        );
        let mut state = self
            .state
            .lock()
            .map_err(|_| SnapshotStoreError::Poisoned)?;
        let (blob_references, raw_len, compressed_len) = {
            let Some(blob) = state.blobs.get(&key) else {
                return Err(SnapshotStoreError::Missing);
            };
            if blob.encoding != reference.snapshot.encoding
                || blob.line_endings != reference.snapshot.line_endings
            {
                return Err(SnapshotStoreError::Corrupt);
            }
            (
                blob.references,
                blob.raw_len,
                u64::try_from(blob.compressed.len()).map_err(|_| SnapshotStoreError::Corrupt)?,
            )
        };
        if blob_references == 0
            || state.references == 0
            || (blob_references == 1
                && (state.logical_bytes < raw_len || state.physical_bytes < compressed_len))
        {
            return Err(SnapshotStoreError::Corrupt);
        }
        state.references -= 1;
        if blob_references == 1 {
            let blob = state
                .blobs
                .remove(&key)
                .ok_or(SnapshotStoreError::Corrupt)?;
            debug_assert_eq!(blob.raw_len, raw_len);
            debug_assert_eq!(blob.compressed.len() as u64, compressed_len);
            state.logical_bytes -= raw_len;
            state.physical_bytes -= compressed_len;
            return Ok(true);
        }
        let blob = state
            .blobs
            .get_mut(&key)
            .ok_or(SnapshotStoreError::Corrupt)?;
        blob.references -= 1;
        Ok(false)
    }

    pub fn metrics(&self) -> Result<SnapshotStoreMetrics, SnapshotStoreError> {
        let state = self
            .state
            .lock()
            .map_err(|_| SnapshotStoreError::Poisoned)?;
        let referenced_logical_bytes = state.blobs.values().try_fold(0u64, |sum, blob| {
            blob.raw_len
                .checked_mul(blob.references)
                .and_then(|value| sum.checked_add(value))
                .ok_or(SnapshotStoreError::Corrupt)
        })?;
        let blobs = u64::try_from(state.blobs.len()).map_err(|_| SnapshotStoreError::Corrupt)?;
        Ok(SnapshotStoreMetrics {
            blobs,
            logical_bytes: state.logical_bytes,
            physical_bytes: state.physical_bytes,
            references: state.references,
            referenced_logical_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::history::{LineEnding, LineEndingMetadata};

    fn snapshot(value: &[u8]) -> CommittedTextSnapshot {
        CommittedTextSnapshot::from_bytes(
            value.to_vec(),
            TextEncoding::Utf8,
            LineEndingMetadata {
                dominant: LineEnding::Lf,
                mixed: false,
                final_newline: true,
            },
        )
    }

    #[test]
    fn identical_content_deduplicates_inside_one_domain() {
        let store = ContentAddressedSnapshotStore::new(SnapshotStoreLimits::default());
        let domain = SnapshotDomain::new("private", "none", "thread");
        let first = store.put(&domain, &snapshot(b"same")).unwrap();
        let second = store.put(&domain, &snapshot(b"same")).unwrap();
        assert_eq!(first, second);
        assert_eq!(store.metrics().unwrap().blobs, 1);
        assert_eq!(store.metrics().unwrap().references, 2);
    }

    #[test]
    fn domains_never_cross_deduplicate() {
        let store = ContentAddressedSnapshotStore::new(SnapshotStoreLimits::default());
        let first = store
            .put(
                &SnapshotDomain::new("a", "none", "thread"),
                &snapshot(b"same"),
            )
            .unwrap();
        let second = store
            .put(
                &SnapshotDomain::new("b", "none", "thread"),
                &snapshot(b"same"),
            )
            .unwrap();
        assert_ne!(first.domain_id, second.domain_id);
        assert_eq!(store.metrics().unwrap().blobs, 2);
    }

    #[test]
    fn release_collects_only_the_last_reference() {
        let store = ContentAddressedSnapshotStore::new(SnapshotStoreLimits::default());
        let domain = SnapshotDomain::new("private", "none", "thread");
        let reference = store.put(&domain, &snapshot(b"same")).unwrap();
        let _ = store.put(&domain, &snapshot(b"same")).unwrap();
        assert!(!store.release(&reference).unwrap());
        assert!(store.release(&reference).unwrap());
        assert_eq!(store.metrics().unwrap().blobs, 0);
    }

    #[test]
    fn compressed_content_round_trips_exactly() {
        let store = ContentAddressedSnapshotStore::new(SnapshotStoreLimits::default());
        let domain = SnapshotDomain::new("private", "none", "thread");
        let reference = store.put(&domain, &snapshot(b"hello\nworld\n")).unwrap();
        let restored = store.get(&reference).unwrap();
        assert_eq!(restored.bytes, b"hello\nworld\n");
    }

    #[test]
    fn forged_reference_metadata_is_rejected() {
        let store = ContentAddressedSnapshotStore::new(SnapshotStoreLimits::default());
        let domain = SnapshotDomain::new("private", "none", "thread");
        let reference = store.put(&domain, &snapshot(b"hello\n")).unwrap();
        let mut forged = reference.clone();
        forged.snapshot.encoding = TextEncoding::Utf8Bom;
        assert_eq!(store.get(&forged), Err(SnapshotStoreError::Corrupt));
    }

    #[test]
    fn put_enforces_the_decompression_limit_before_interning() {
        let store = ContentAddressedSnapshotStore::new(SnapshotStoreLimits {
            max_logical_bytes: 1024,
            max_physical_bytes: 1024,
            max_single_bytes: 1024,
            max_decompressed_bytes: 3,
        });
        let domain = SnapshotDomain::new("private", "none", "thread");
        assert_eq!(
            store.put(&domain, &snapshot(b"1234")),
            Err(SnapshotStoreError::LimitExceeded("decompression"))
        );
        assert_eq!(store.metrics().unwrap().blobs, 0);
    }
}
