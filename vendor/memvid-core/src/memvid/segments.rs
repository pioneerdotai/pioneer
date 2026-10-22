use std::io::{Read, Seek, SeekFrom, Write};

#[cfg(feature = "lex")]
use std::collections::{HashMap, HashSet};

use crate::lex::{LexIndexArtifact, LexIndexBuilder};
#[cfg(feature = "lex")]
use crate::types::TantivySegmentDescriptor;
use crate::types::{
    FrameId, FrameRole, FrameStatus, LexSegmentDescriptor, SegmentCommon, TimeSegmentDescriptor,
    VecSegmentDescriptor, VectorCompression,
};
use crate::vec::{VecIndexArtifact, VecIndexBuilder};
use crate::vec_pq::{QuantizedVecIndexArtifact, QuantizedVecIndexBuilder};
use crate::{MemvidError, Result, TimeIndexEntry, time_index_append};
#[cfg(feature = "temporal_track")]
use crate::{
    TEMPORAL_TRACK_FLAG_HAS_ANCHORS, TEMPORAL_TRACK_FLAG_HAS_MENTIONS, TemporalAnchor,
    TemporalMention, temporal_track_append,
};
use std::io::Cursor;

use super::lifecycle::Memvid;

#[cfg(feature = "lex")]
use crate::search::{EmbeddedLexSegment, TantivySnapshot};

/// Minimum number of vectors required to use Product Quantization.
/// Below this threshold, we fall back to uncompressed vectors.
/// PQ requires training k-means on many vectors to learn good codebooks.
const MIN_VECTORS_FOR_PQ: usize = 100;

#[derive(Debug)]
pub(crate) struct LexSegmentArtifact {
    pub bytes: Vec<u8>,
    pub doc_count: u64,
    pub checksum: [u8; 32],
}

#[derive(Debug)]
pub(crate) struct VecSegmentArtifact {
    pub bytes: Vec<u8>,
    pub vector_count: u64,
    pub dimension: u32,
    pub checksum: [u8; 32],
    pub compression: VectorCompression,
    #[cfg(feature = "parallel_segments")]
    pub bytes_uncompressed: u64,
}

#[derive(Debug)]
pub(crate) struct TimeSegmentArtifact {
    pub bytes: Vec<u8>,
    pub entry_count: u64,
    pub checksum: [u8; 32],
}

#[cfg(feature = "temporal_track")]
#[derive(Debug)]
pub(crate) struct TemporalSegmentArtifact {
    pub bytes: Vec<u8>,
    pub entry_count: u64,
    pub anchor_count: u64,
    pub checksum: [u8; 32],
    pub flags: u32,
}

#[cfg(feature = "lex")]
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct TantivySegmentArtifact {
    pub path: String,
    pub bytes: Vec<u8>,
    pub checksum: [u8; 32],
}

#[cfg(feature = "lex")]
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub(crate) struct TantivySegmentDeltaEntry {
    pub path: String,
    pub existing: Option<TantivySegmentDescriptor>,
    pub artifact: Option<TantivySegmentArtifact>,
}

#[cfg(feature = "lex")]
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub(crate) struct TantivySnapshotDelta {
    pub doc_count: u64,
    pub checksum: [u8; 32],
    pub entries: Vec<TantivySegmentDeltaEntry>,
    pub removed_paths: Vec<String>,
}

impl Memvid {
    pub(crate) fn build_lex_segment_from_frames(
        &mut self,
        frame_ids: &[FrameId],
    ) -> Result<Option<LexSegmentArtifact>> {
        if frame_ids.is_empty() || !self.lex_enabled {
            return Ok(None);
        }

        let mut builder = LexIndexBuilder::new();
        let empty_tags = std::collections::HashMap::new();
        for frame_id in frame_ids {
            let frame = self
                .toc
                .frames
                .get(usize::try_from(*frame_id).unwrap_or(0))
                .cloned()
                .ok_or(MemvidError::InvalidFrame {
                    frame_id: *frame_id,
                    reason: "frame id out of range for lex segment",
                })?;

            if frame.status != FrameStatus::Active {
                continue;
            }
            if frame.role != FrameRole::Document && frame.role != FrameRole::DocumentChunk {
                continue;
            }

            // Use search_text if available (covers no_raw mode), otherwise fall back to content
            let content = if let Some(ref text) = frame.search_text {
                if text.trim().is_empty() {
                    self.frame_content(&frame)?
                } else {
                    text.clone()
                }
            } else {
                self.frame_content(&frame)?
            };
            if content.trim().is_empty() {
                continue;
            }
            let uri = frame
                .uri
                .clone()
                .unwrap_or_else(|| crate::default_uri(frame.id));
            builder.add_document(
                frame.id,
                &uri,
                frame.title.as_deref(),
                &content,
                &empty_tags,
            );
        }

        let LexIndexArtifact {
            bytes,
            doc_count,
            checksum,
        } = builder.finish()?;
        if doc_count == 0 {
            return Ok(None);
        }
        Ok(Some(LexSegmentArtifact {
            bytes,
            doc_count,
            checksum,
        }))
    }

    pub(crate) fn build_vec_segment_from_embeddings(
        &self,
        embeddings: &[(FrameId, Vec<f32>)],
    ) -> Result<Option<VecSegmentArtifact>> {
        if embeddings.is_empty() || !self.vec_enabled {
            return Ok(None);
        }

        // Determine vector dimension and validate all provided embeddings are consistent.
        let mut dimension: Option<u32> = None;
        let mut non_empty_count = 0usize;
        for (_frame_id, vector) in embeddings {
            if vector.is_empty() {
                continue;
            }
            non_empty_count = non_empty_count.saturating_add(1);
            let vec_dim = u32::try_from(vector.len()).unwrap_or(0);
            match dimension {
                None => dimension = Some(vec_dim),
                Some(existing) if existing == vec_dim => {}
                Some(existing) => {
                    return Err(MemvidError::VecDimensionMismatch {
                        expected: existing,
                        actual: vector.len(),
                    });
                }
            }
        }
        let Some(dimension) = dimension else {
            // All embeddings were empty.
            return Ok(None);
        };

        // Determine effective compression: use uncompressed if below PQ threshold
        let effective_compression = match &self.vec_compression {
            VectorCompression::Pq96 if non_empty_count < MIN_VECTORS_FOR_PQ => {
                // Fall back to uncompressed for small vector counts
                VectorCompression::None
            }
            other => other.clone(),
        };

        match effective_compression {
            VectorCompression::None => {
                // Uncompressed path - use regular VecIndexBuilder
                let mut builder = VecIndexBuilder::new();
                for (frame_id, vector) in embeddings {
                    if vector.is_empty() {
                        continue;
                    }
                    builder.add_document(*frame_id, vector.clone());
                }

                let VecIndexArtifact {
                    bytes,
                    vector_count,
                    dimension: artifact_dimension,
                    checksum,
                    #[cfg(feature = "parallel_segments")]
                    bytes_uncompressed,
                } = builder.finish()?;

                if vector_count == 0 {
                    return Ok(None);
                }

                Ok(Some(VecSegmentArtifact {
                    bytes,
                    vector_count,
                    dimension: artifact_dimension.max(dimension),
                    checksum,
                    compression: VectorCompression::None,
                    #[cfg(feature = "parallel_segments")]
                    bytes_uncompressed,
                }))
            }
            VectorCompression::Pq96 => {
                // Compressed path - use QuantizedVecIndexBuilder
                let mut builder = QuantizedVecIndexBuilder::new();

                // Collect all vectors for training
                let mut training_vectors = Vec::new();
                for (_frame_id, vector) in embeddings {
                    if vector.is_empty() {
                        continue;
                    }
                    training_vectors.push(vector.clone());
                }

                if training_vectors.is_empty() {
                    return Ok(None);
                }

                // Train the quantizer on all vectors
                builder.train_quantizer(&training_vectors, dimension)?;

                // Add all documents
                for (frame_id, vector) in embeddings {
                    if vector.is_empty() {
                        continue;
                    }
                    builder.add_document(*frame_id, vector.clone())?;
                }

                let QuantizedVecIndexArtifact {
                    bytes,
                    vector_count,
                    dimension: artifact_dimension,
                    checksum,
                    compression_ratio: _,
                } = builder.finish()?;

                if vector_count == 0 {
                    return Ok(None);
                }

                Ok(Some(VecSegmentArtifact {
                    bytes,
                    vector_count,
                    dimension: artifact_dimension.max(dimension),
                    checksum,
                    compression: VectorCompression::Pq96,
                    #[cfg(feature = "parallel_segments")]
                    bytes_uncompressed: 0, // PQ doesn't track uncompressed size
                }))
            }
        }
    }

    pub(crate) fn append_lex_segment(
        &mut self,
        artifact: &LexSegmentArtifact,
        segment_id: u64,
    ) -> Result<LexSegmentDescriptor> {
        if artifact.doc_count == 0 || artifact.bytes.is_empty() {
            return Err(MemvidError::CheckpointFailed {
                reason: "lex segment artifact empty".into(),
            });
        }

        let offset = self.data_end;
        let new_end = offset + artifact.bytes.len() as u64;

        // Write at current data_end
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&artifact.bytes)?;
        self.file.sync_all()?;
        self.data_end = new_end;

        let common = SegmentCommon::new(
            segment_id,
            offset,
            artifact.bytes.len() as u64,
            artifact.checksum,
        );
        Ok(LexSegmentDescriptor::from_common(
            common,
            artifact.doc_count,
        ))
    }

    pub(crate) fn append_vec_segment(
        &mut self,
        artifact: &VecSegmentArtifact,
        segment_id: u64,
    ) -> Result<VecSegmentDescriptor> {
        if artifact.vector_count == 0 || artifact.bytes.is_empty() {
            return Err(MemvidError::CheckpointFailed {
                reason: "vec segment artifact empty".into(),
            });
        }

        let offset = self.data_end;
        let new_end = offset + artifact.bytes.len() as u64;

        // Seek to write position
        self.file.seek(SeekFrom::Start(offset))?;

        // Write the actual data
        self.file.write_all(&artifact.bytes)?;
        self.file.sync_all()?;

        // VERIFY: Read back the first few bytes to confirm write persisted
        self.file.seek(SeekFrom::Start(offset))?;
        let mut verify_buf = vec![0u8; 16.min(artifact.bytes.len())];
        self.file.read_exact(&mut verify_buf)?;
        let expected = &artifact.bytes[..verify_buf.len()];
        if verify_buf != expected {
            return Err(MemvidError::CheckpointFailed {
                reason: format!("vec segment write verification failed at offset {offset}"),
            });
        }

        self.data_end = new_end;

        let common = SegmentCommon::new(
            segment_id,
            offset,
            artifact.bytes.len() as u64,
            artifact.checksum,
        );

        tracing::debug!(
            segment_id = common.segment_id,
            artifact_compression = ?artifact.compression,
            vector_count = artifact.vector_count,
            bytes_len = common.bytes_length,
            "created vec segment descriptor"
        );

        Ok(VecSegmentDescriptor::from_common(
            common,
            artifact.vector_count,
            artifact.dimension,
            artifact.compression.clone(),
        ))
    }

    pub(crate) fn build_time_segment_from_entries(
        &self,
        entries: &[TimeIndexEntry],
    ) -> Result<Option<TimeSegmentArtifact>> {
        if entries.is_empty() {
            return Ok(None);
        }

        let mut sorted_entries = entries.to_owned();
        sorted_entries.sort_by_key(|entry| (entry.timestamp, entry.frame_id));

        let mut cursor = Cursor::new(Vec::new());
        let (_, _length, checksum) = time_index_append(&mut cursor, &mut sorted_entries)?;
        let bytes = cursor.into_inner();
        if bytes.is_empty() {
            return Ok(None);
        }

        Ok(Some(TimeSegmentArtifact {
            bytes,
            entry_count: sorted_entries.len() as u64,
            checksum,
        }))
    }

    #[cfg(feature = "temporal_track")]
    pub(crate) fn build_temporal_segment_from_records(
        &self,
        mentions: &[TemporalMention],
        anchors: &[TemporalAnchor],
    ) -> Result<Option<TemporalSegmentArtifact>> {
        if mentions.is_empty() && anchors.is_empty() {
            return Ok(None);
        }

        #[cfg(test)]
        println!(
            "build_temporal_segment_from_records: mentions={}, anchors={}",
            mentions.len(),
            anchors.len()
        );

        let mut mention_vec = mentions.to_vec();
        let mut anchor_vec = anchors.to_vec();
        mention_vec.sort_by_key(|m| (m.ts_utc, m.frame_id, m.byte_start));
        anchor_vec.sort_by_key(|a| a.frame_id);

        let mut flags = 0;
        if !anchor_vec.is_empty() {
            flags |= TEMPORAL_TRACK_FLAG_HAS_ANCHORS;
        }
        if !mention_vec.is_empty() {
            flags |= TEMPORAL_TRACK_FLAG_HAS_MENTIONS;
        }

        let mut cursor = Cursor::new(Vec::new());
        let (_, _length, checksum) =
            temporal_track_append(&mut cursor, &mut mention_vec, &mut anchor_vec, flags)?;
        let bytes = cursor.into_inner();
        if bytes.is_empty() {
            return Ok(None);
        }

        Ok(Some(TemporalSegmentArtifact {
            bytes,
            entry_count: mention_vec.len() as u64,
            anchor_count: anchor_vec.len() as u64,
            checksum,
            flags,
        }))
    }

    pub(crate) fn append_time_segment(
        &mut self,
        artifact: &TimeSegmentArtifact,
        segment_id: u64,
    ) -> Result<TimeSegmentDescriptor> {
        if artifact.entry_count == 0 || artifact.bytes.is_empty() {
            return Err(MemvidError::CheckpointFailed {
                reason: "time segment artifact empty".into(),
            });
        }

        let offset = self.data_end;
        let new_end = offset + artifact.bytes.len() as u64;

        // Write at current data_end
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&artifact.bytes)?;
        self.file.sync_all()?;
        self.data_end = new_end;

        let common = SegmentCommon::new(
            segment_id,
            offset,
            artifact.bytes.len() as u64,
            artifact.checksum,
        );
        Ok(TimeSegmentDescriptor::from_common(
            common,
            artifact.entry_count,
        ))
    }

    #[cfg(feature = "temporal_track")]
    pub(crate) fn append_temporal_segment(
        &mut self,
        artifact: &TemporalSegmentArtifact,
        segment_id: u64,
    ) -> Result<crate::types::TemporalSegmentDescriptor> {
        if artifact.entry_count == 0 && artifact.anchor_count == 0 {
            return Err(MemvidError::CheckpointFailed {
                reason: "temporal segment artifact empty".into(),
            });
        }

        let offset = self.data_end;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&artifact.bytes)?;
        self.file.flush()?;
        self.data_end = offset + artifact.bytes.len() as u64;

        let common = SegmentCommon::new(
            segment_id,
            offset,
            artifact.bytes.len() as u64,
            artifact.checksum,
        );
        Ok(crate::types::TemporalSegmentDescriptor::from_common(
            common,
            artifact.entry_count,
            artifact.anchor_count,
            artifact.flags,
        ))
    }

    #[cfg(feature = "lex")]
    #[allow(dead_code)]
    pub(crate) fn append_tantivy_segment(
        &mut self,
        artifact: &TantivySegmentArtifact,
        segment_id: u64,
    ) -> Result<TantivySegmentDescriptor> {
        if artifact.bytes.is_empty() {
            return Err(MemvidError::CheckpointFailed {
                reason: format!("tantivy segment artifact '{}' empty", artifact.path),
            });
        }

        let offset = self.data_end;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&artifact.bytes)?;
        self.file.flush()?;
        self.data_end = offset + artifact.bytes.len() as u64;

        let common = SegmentCommon::new(
            segment_id,
            offset,
            artifact.bytes.len() as u64,
            artifact.checksum,
        );
        Ok(TantivySegmentDescriptor::from_common(
            common,
            artifact.path.clone(),
        ))
    }

    #[cfg(feature = "lex")]
    pub(crate) fn derive_tantivy_snapshot_delta(
        &self,
        snapshot: TantivySnapshot,
    ) -> TantivySnapshotDelta {
        let mut latest: HashMap<String, &TantivySegmentDescriptor> = HashMap::new();
        for descriptor in &self.toc.segment_catalog.tantivy_segments {
            latest
                .entry(descriptor.path.clone())
                .and_modify(|existing| {
                    if descriptor.common.segment_id > existing.common.segment_id {
                        *existing = descriptor;
                    }
                })
                .or_insert(descriptor);
        }

        let mut entries = Vec::with_capacity(snapshot.segments.len());
        let mut present_paths: HashSet<String> = HashSet::with_capacity(snapshot.segments.len());

        for blob in snapshot.segments {
            let path = blob.path.clone();
            let existing = latest
                .get(path.as_str())
                .map(|descriptor| (*descriptor).clone());
            let requires_append = existing
                .as_ref()
                .is_none_or(|descriptor| descriptor.common.checksum != blob.checksum);

            let artifact = if requires_append {
                Some(TantivySegmentArtifact {
                    path: path.clone(),
                    bytes: blob.bytes,
                    checksum: blob.checksum,
                })
            } else {
                None
            };

            entries.push(TantivySegmentDeltaEntry {
                path: path.clone(),
                existing,
                artifact,
            });
            present_paths.insert(path);
        }

        let removed_paths = latest
            .keys()
            .filter(|path| !present_paths.contains(path.as_str()))
            .cloned()
            .collect();

        TantivySnapshotDelta {
            doc_count: snapshot.doc_count,
            checksum: snapshot.checksum,
            entries,
            removed_paths,
        }
    }

    #[cfg(feature = "lex")]
    #[allow(dead_code)]
    pub(crate) fn publish_tantivy_delta(&mut self) -> Result<bool> {
        let engine = match self.tantivy.as_mut() {
            Some(engine) => engine,
            None => return Ok(false),
        };

        let snapshot = engine.snapshot_segments()?;
        let delta = self.derive_tantivy_snapshot_delta(snapshot);

        let mut active_descriptors: HashMap<String, TantivySegmentDescriptor> = self
            .toc
            .segment_catalog
            .tantivy_segments
            .iter()
            .map(|descriptor| (descriptor.path.clone(), descriptor.clone()))
            .collect();

        let mut next_segment_id = self.toc.segment_catalog.next_segment_id;
        let initial_offset = self.data_end;
        let mut changed = false;

        for entry in delta.entries {
            if let Some(artifact) = entry.artifact {
                if artifact.bytes.is_empty() {
                    continue;
                }
                let descriptor = match self.append_tantivy_segment(&artifact, next_segment_id) {
                    Ok(descriptor) => descriptor,
                    Err(err) => {
                        self.data_end = initial_offset;
                        self.file.set_len(initial_offset)?;
                        return Err(err);
                    }
                };
                next_segment_id = next_segment_id.saturating_add(1);
                active_descriptors.insert(entry.path.clone(), descriptor);
                changed = true;
            } else if let Some(existing) = entry.existing {
                active_descriptors
                    .entry(entry.path.clone())
                    .or_insert(existing);
            }
        }

        for path in delta.removed_paths {
            if active_descriptors.remove(&path).is_some() {
                changed = true;
            }
        }

        let current_doc_count = self
            .toc
            .indexes
            .lex
            .as_ref()
            .map_or(0, |manifest| manifest.doc_count);
        if current_doc_count != delta.doc_count {
            changed = true;
        }

        let current_checksum = self
            .toc
            .indexes
            .lex
            .as_ref()
            .map_or([0u8; 32], |manifest| manifest.checksum);
        if current_checksum != delta.checksum {
            changed = true;
        }

        if !changed {
            return Ok(false);
        }

        let mut descriptors: Vec<TantivySegmentDescriptor> =
            active_descriptors.into_values().collect();
        descriptors.sort_by_key(|descriptor| descriptor.common.segment_id);

        let embedded_segments: Vec<EmbeddedLexSegment> = descriptors
            .iter()
            .map(|descriptor| EmbeddedLexSegment {
                path: descriptor.path.clone(),
                bytes_offset: descriptor.common.bytes_offset,
                bytes_length: descriptor.common.bytes_length,
                checksum: descriptor.common.checksum,
            })
            .collect();

        let previous_manifest = self.toc.indexes.lex.clone();
        let (index_manifest, manifest_segments) = {
            let mut storage = self.lex_storage.write().map_err(|_| MemvidError::Tantivy {
                reason: "embedded lex storage lock poisoned".into(),
            })?;
            storage.replace(delta.doc_count, delta.checksum, embedded_segments.clone());
            storage.to_manifest()
        };

        if let Some(mut storage_manifest) = index_manifest {
            if storage_manifest.bytes_offset == 0 && storage_manifest.bytes_length == 0 {
                if let Some(prev) = previous_manifest.as_ref() {
                    storage_manifest.bytes_offset = prev.bytes_offset;
                    storage_manifest.bytes_length = prev.bytes_length;
                }
            }
            if let Some(existing) = self.toc.indexes.lex.as_mut() {
                existing.doc_count = storage_manifest.doc_count;
                existing.generation = storage_manifest.generation;
                existing.checksum = storage_manifest.checksum;
                if existing.bytes_length == 0 && storage_manifest.bytes_length != 0 {
                    existing.bytes_offset = storage_manifest.bytes_offset;
                    existing.bytes_length = storage_manifest.bytes_length;
                }
            } else {
                self.toc.indexes.lex = Some(storage_manifest);
            }
        } else {
            self.toc.indexes.lex = None;
        }
        self.toc.indexes.lex_segments = manifest_segments;

        self.toc.segment_catalog.tantivy_segments = descriptors;
        self.toc.segment_catalog.next_segment_id = next_segment_id;
        self.toc.segment_catalog.version = self.toc.segment_catalog.version.max(1);

        Ok(true)
    }
}
