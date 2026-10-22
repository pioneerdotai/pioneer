use bincode::serde::{decode_from_slice, encode_to_vec};
use blake3::Hasher;
use serde::{Deserialize, Serialize};

use crate::{
    error::{MemvidError, Result},
    types::{
        Frame, IndexManifests, MemoryBinding, SegmentCatalog, SegmentMeta, TemporalTrackManifest,
        TicketRef, TimeIndexManifest, Toc,
    },
};

#[allow(clippy::cast_possible_truncation)]
fn canonical_config() -> impl bincode::config::Config {
    bincode::config::standard()
        .with_fixed_int_encoding()
        .with_little_endian()
        .with_limit::<{ crate::MAX_INDEX_BYTES as usize }>()
}

/// Legacy TOC format without `memories_track` field (pre-v2.0.105).
/// Used for backwards compatibility with older .mv2 files.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct LegacyTocV1 {
    pub toc_version: u64,
    pub segments: Vec<SegmentMeta>,
    pub frames: Vec<Frame>,
    pub indexes: IndexManifests,
    pub time_index: Option<TimeIndexManifest>,
    pub temporal_track: Option<TemporalTrackManifest>,
    // Note: memories_track, logic_mesh, replay_manifest NOT present
    pub segment_catalog: SegmentCatalog,
    pub ticket_ref: TicketRef,
    pub memory_binding: Option<MemoryBinding>,
    pub merkle_root: [u8; 32],
    pub toc_checksum: [u8; 32],
}

/// Legacy TOC format with `memories_track` but without `replay_manifest` (pre-v2.0.116).
/// Used for backwards compatibility with files created before replay feature.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct LegacyTocV2 {
    pub toc_version: u64,
    pub segments: Vec<SegmentMeta>,
    pub frames: Vec<Frame>,
    pub indexes: IndexManifests,
    pub time_index: Option<TimeIndexManifest>,
    pub temporal_track: Option<TemporalTrackManifest>,
    pub memories_track: Option<crate::types::MemoriesTrackManifest>,
    pub logic_mesh: Option<crate::types::LogicMeshManifest>,
    pub segment_catalog: SegmentCatalog,
    pub ticket_ref: TicketRef,
    pub memory_binding: Option<MemoryBinding>,
    // Note: replay_manifest NOT present in this version
    pub merkle_root: [u8; 32],
    pub toc_checksum: [u8; 32],
}

impl From<LegacyTocV1> for Toc {
    fn from(legacy: LegacyTocV1) -> Self {
        Toc {
            toc_version: legacy.toc_version,
            segments: legacy.segments,
            frames: legacy.frames,
            indexes: legacy.indexes,
            time_index: legacy.time_index,
            temporal_track: legacy.temporal_track,
            memories_track: None, // Default for legacy files
            logic_mesh: None,     // Default for legacy files
            sketch_track: None,   // Default for legacy files
            segment_catalog: legacy.segment_catalog,
            ticket_ref: legacy.ticket_ref,
            memory_binding: legacy.memory_binding,
            replay_manifest: None,                // Default for legacy files
            enrichment_queue: Default::default(), // Default for legacy files
            merkle_root: legacy.merkle_root,
            toc_checksum: legacy.toc_checksum,
        }
    }
}

impl From<LegacyTocV2> for Toc {
    fn from(legacy: LegacyTocV2) -> Self {
        Toc {
            toc_version: legacy.toc_version,
            segments: legacy.segments,
            frames: legacy.frames,
            indexes: legacy.indexes,
            time_index: legacy.time_index,
            temporal_track: legacy.temporal_track,
            memories_track: legacy.memories_track,
            logic_mesh: legacy.logic_mesh,
            sketch_track: None, // Default for pre-sketch files
            segment_catalog: legacy.segment_catalog,
            ticket_ref: legacy.ticket_ref,
            memory_binding: legacy.memory_binding,
            replay_manifest: None, // Default for pre-replay files
            enrichment_queue: Default::default(), // Default for legacy files
            merkle_root: legacy.merkle_root,
            toc_checksum: legacy.toc_checksum,
        }
    }
}

impl Toc {
    /// Serialises the TOC using the canonical bincode configuration.
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(encode_to_vec(self, canonical_config())?)
    }

    /// Deserialises bytes into a TOC, rejecting any trailing data.
    /// Supports current format and legacy formats (pre-replay_manifest, pre-memories_track).
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        // Try current format first (with replay_manifest)
        if let Ok((toc, bytes_read)) = decode_from_slice::<Toc, _>(bytes, canonical_config()) {
            if bytes_read != bytes.len() {
                return Err(MemvidError::InvalidToc {
                    reason: "unexpected trailing bytes".into(),
                });
            }
            return Ok(toc);
        }

        // Try V2 format (with memories_track/logic_mesh, without replay_manifest)
        if let Ok((legacy, bytes_read)) =
            decode_from_slice::<LegacyTocV2, _>(bytes, canonical_config())
        {
            if bytes_read != bytes.len() {
                return Err(MemvidError::InvalidToc {
                    reason: "unexpected trailing bytes in V2 format".into(),
                });
            }
            tracing::debug!("Decoded TOC V2 format (pre-replay_manifest)");
            return Ok(legacy.into());
        }

        // Try V1 format (without memories_track/logic_mesh/replay_manifest)
        match decode_from_slice::<LegacyTocV1, _>(bytes, canonical_config()) {
            Ok((legacy, bytes_read)) => {
                if bytes_read != bytes.len() {
                    return Err(MemvidError::InvalidToc {
                        reason: "unexpected trailing bytes in V1 format".into(),
                    });
                }
                tracing::debug!("Decoded TOC V1 format (pre-memories_track)");
                Ok(legacy.into())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Deserialises bytes into a TOC, allowing trailing data (for recovery).
    /// Supports current format and legacy formats (pre-replay_manifest, pre-memories_track).
    pub fn decode_lenient(bytes: &[u8]) -> Result<Self> {
        // Try current format first (with replay_manifest)
        if let Ok((toc, _)) = decode_from_slice::<Toc, _>(bytes, canonical_config()) {
            return Ok(toc);
        }
        // Try V2 format (with memories_track/logic_mesh, without replay_manifest)
        if let Ok((legacy, _)) = decode_from_slice::<LegacyTocV2, _>(bytes, canonical_config()) {
            tracing::debug!("Decoded TOC V2 format (pre-replay_manifest) in lenient mode");
            return Ok(legacy.into());
        }
        // Try V1 format (without memories_track/logic_mesh/replay_manifest)
        match decode_from_slice::<LegacyTocV1, _>(bytes, canonical_config()) {
            Ok((legacy, _)) => {
                tracing::debug!("Decoded TOC V1 format (pre-memories_track) in lenient mode");
                Ok(legacy.into())
            }
            Err(e) => Err(e.into()),
        }
    }
}

impl LegacyTocV1 {
    /// Encode legacy TOC format for checksum verification.
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(encode_to_vec(self, canonical_config())?)
    }
}

impl LegacyTocV2 {
    /// Encode V2 TOC format for checksum verification.
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(encode_to_vec(self, canonical_config())?)
    }
}

#[cfg(test)]
pub(crate) fn encode_legacy_v1_for_test(toc: &Toc) -> Vec<u8> {
    let mut legacy = LegacyTocV1 {
        toc_version: toc.toc_version,
        segments: toc.segments.clone(),
        frames: toc.frames.clone(),
        indexes: toc.indexes.clone(),
        time_index: toc.time_index.clone(),
        temporal_track: toc.temporal_track.clone(),
        segment_catalog: toc.segment_catalog.clone(),
        ticket_ref: toc.ticket_ref.clone(),
        memory_binding: toc.memory_binding.clone(),
        merkle_root: toc.merkle_root,
        toc_checksum: [0u8; 32],
    };
    let checksum_target = legacy.encode().expect("encode legacy TOC checksum target");
    legacy.toc_checksum = Toc::calculate_checksum(&checksum_target);
    legacy.encode().expect("encode legacy TOC")
}

#[cfg(test)]
pub(crate) fn encode_legacy_v2_for_test(toc: &Toc) -> Vec<u8> {
    let mut legacy = LegacyTocV2 {
        toc_version: toc.toc_version,
        segments: toc.segments.clone(),
        frames: toc.frames.clone(),
        indexes: toc.indexes.clone(),
        time_index: toc.time_index.clone(),
        temporal_track: toc.temporal_track.clone(),
        memories_track: toc.memories_track.clone(),
        logic_mesh: toc.logic_mesh.clone(),
        segment_catalog: toc.segment_catalog.clone(),
        ticket_ref: toc.ticket_ref.clone(),
        memory_binding: toc.memory_binding.clone(),
        merkle_root: toc.merkle_root,
        toc_checksum: [0u8; 32],
    };
    let checksum_target = legacy
        .encode()
        .expect("encode legacy V2 TOC checksum target");
    legacy.toc_checksum = Toc::calculate_checksum(&checksum_target);
    legacy.encode().expect("encode legacy V2 TOC")
}

impl Toc {
    /// Computes the BLAKE3 checksum used for the TOC integrity field.
    #[must_use]
    pub fn calculate_checksum(bytes: &[u8]) -> [u8; 32] {
        let mut hasher = Hasher::new();
        hasher.update(bytes);
        *hasher.finalize().as_bytes()
    }

    /// Verifies that the stored TOC checksum matches the deterministic encoding.
    /// Supports current format and legacy format checksums for backwards compatibility.
    pub fn verify_checksum(&self) -> Result<()> {
        // Try current format first (with replay_manifest)
        let mut clone = self.clone();
        clone.toc_checksum = [0u8; 32];
        let bytes = clone.encode()?;
        let digest = Self::calculate_checksum(&bytes);
        if digest == self.toc_checksum {
            return Ok(());
        }

        // Try V2 format (with memories_track/logic_mesh, without replay_manifest)
        // Only try if replay_manifest is None (indicates pre-replay origin)
        if self.replay_manifest.is_none() {
            let legacy_v2 = LegacyTocV2 {
                toc_version: self.toc_version,
                segments: self.segments.clone(),
                frames: self.frames.clone(),
                indexes: self.indexes.clone(),
                time_index: self.time_index.clone(),
                temporal_track: self.temporal_track.clone(),
                memories_track: self.memories_track.clone(),
                logic_mesh: self.logic_mesh.clone(),
                segment_catalog: self.segment_catalog.clone(),
                ticket_ref: self.ticket_ref.clone(),
                memory_binding: self.memory_binding.clone(),
                merkle_root: self.merkle_root,
                toc_checksum: [0u8; 32],
            };
            let v2_bytes = legacy_v2.encode()?;
            let v2_digest = Self::calculate_checksum(&v2_bytes);
            if v2_digest == self.toc_checksum {
                tracing::debug!("TOC checksum verified using V2 format (pre-replay)");
                return Ok(());
            }
        }

        // Try V1 format (without memories_track/logic_mesh/replay_manifest)
        // Only try if memories_track is None (indicates V1 origin)
        if self.memories_track.is_none() && self.replay_manifest.is_none() {
            let legacy_v1 = LegacyTocV1 {
                toc_version: self.toc_version,
                segments: self.segments.clone(),
                frames: self.frames.clone(),
                indexes: self.indexes.clone(),
                time_index: self.time_index.clone(),
                temporal_track: self.temporal_track.clone(),
                segment_catalog: self.segment_catalog.clone(),
                ticket_ref: self.ticket_ref.clone(),
                memory_binding: self.memory_binding.clone(),
                merkle_root: self.merkle_root,
                toc_checksum: [0u8; 32],
            };
            let v1_bytes = legacy_v1.encode()?;
            let v1_digest = Self::calculate_checksum(&v1_bytes);
            if v1_digest == self.toc_checksum {
                tracing::debug!("TOC checksum verified using V1 format (pre-memories_track)");
                return Ok(());
            }
        }

        Err(MemvidError::ChecksumMismatch { context: "toc" })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        CanonicalEncoding, Frame, FrameId, FrameRole, FrameStatus, IndexManifests, SegmentCatalog,
        SegmentCompression, SegmentMeta, TicketRef, TimeIndexManifest,
    };
    use std::collections::BTreeMap;

    fn sample_toc() -> Toc {
        Toc {
            toc_version: 1,
            segments: vec![SegmentMeta {
                id: 0,
                frame_range: (0, 2),
                primary_checksum: [0x11; 32],
                compression: SegmentCompression::None,
                bytes_offset: 4096,
                bytes_length: 512,
            }],
            frames: vec![
                Frame {
                    id: 0 as FrameId,
                    timestamp: 1_700_000_000,
                    anchor_ts: None,
                    anchor_source: None,
                    kind: Some("text".into()),
                    track: Some("default".into()),
                    payload_offset: 4096,
                    payload_length: 128,
                    checksum: [0x22; 32],
                    uri: Some("mv2://sample/0".into()),
                    title: Some("Sample 0".into()),
                    canonical_encoding: CanonicalEncoding::Plain,
                    canonical_length: Some(128),
                    metadata: None,
                    search_text: None,
                    tags: Vec::new(),
                    labels: Vec::new(),
                    extra_metadata: BTreeMap::new(),
                    content_dates: Vec::new(),
                    role: FrameRole::Document,
                    parent_id: None,
                    chunk_index: None,
                    chunk_count: None,
                    chunk_manifest: None,
                    status: FrameStatus::Active,
                    supersedes: None,
                    superseded_by: None,
                    source_sha256: None,
                    source_path: None,
                    enrichment_state: crate::types::EnrichmentState::default(),
                },
                Frame {
                    id: 1 as FrameId,
                    timestamp: 1_700_000_100,
                    anchor_ts: None,
                    anchor_source: None,
                    kind: None,
                    track: None,
                    payload_offset: 4224,
                    payload_length: 64,
                    checksum: [0x33; 32],
                    uri: Some("mv2://sample/1".into()),
                    title: Some("Sample 1".into()),
                    canonical_encoding: CanonicalEncoding::Plain,
                    canonical_length: Some(64),
                    metadata: None,
                    search_text: None,
                    tags: Vec::new(),
                    labels: Vec::new(),
                    extra_metadata: BTreeMap::new(),
                    content_dates: Vec::new(),
                    role: FrameRole::Document,
                    parent_id: None,
                    chunk_index: None,
                    chunk_count: None,
                    chunk_manifest: None,
                    status: FrameStatus::Active,
                    supersedes: None,
                    superseded_by: None,
                    source_sha256: None,
                    source_path: None,
                    enrichment_state: crate::types::EnrichmentState::default(),
                },
            ],
            indexes: IndexManifests::default(),
            time_index: Some(TimeIndexManifest {
                bytes_offset: 8192,
                bytes_length: 96,
                entry_count: 2,
                checksum: [0x44; 32],
            }),
            temporal_track: None,
            memories_track: None,
            logic_mesh: None,
            sketch_track: None,
            segment_catalog: SegmentCatalog::default(),
            ticket_ref: TicketRef {
                issuer: "memvid".into(),
                seq_no: 0,
                expires_in_secs: 3600,
                capacity_bytes: 0,
                verified: false,
            },
            memory_binding: None,
            replay_manifest: None,
            enrichment_queue: Default::default(),
            merkle_root: [0x55; 32],
            toc_checksum: [0u8; 32],
        }
    }

    fn stamp_checksum(mut toc: Toc) -> Toc {
        let mut checksum_target = toc.clone();
        checksum_target.toc_checksum = [0u8; 32];
        let bytes = checksum_target.encode().expect("encode for checksum");
        toc.toc_checksum = Toc::calculate_checksum(&bytes);
        toc
    }

    #[test]
    fn serialize_deserialize_roundtrip() {
        let toc = stamp_checksum(sample_toc());
        let encoded = toc.encode().expect("encode toc");
        let decoded = Toc::decode(&encoded).expect("decode toc");
        decoded.verify_checksum().expect("checksum matches");
        assert_eq!(decoded.toc_checksum, toc.toc_checksum);
    }

    #[test]
    fn detect_checksum_mismatch() {
        let mut toc = stamp_checksum(sample_toc());
        toc.toc_checksum[0] ^= 0xFF;
        let err = toc.verify_checksum().expect_err("must fail");
        matches!(err, MemvidError::ChecksumMismatch { .. });
    }

    #[test]
    fn reject_trailing_bytes() {
        let toc = stamp_checksum(sample_toc());
        let mut bytes = toc.encode().expect("encode toc");
        bytes.push(0);
        let err = Toc::decode(&bytes).expect_err("should reject");
        matches!(err, MemvidError::InvalidToc { .. });
    }
}
