use blake3::hash;
use serde::{Deserialize, Serialize};

use crate::{MemvidError, Result, types::FrameId};

#[cfg(any(feature = "vec", feature = "hnsw_bench"))]
use hnsw::{Hnsw, Params, Searcher};
#[cfg(any(feature = "vec", feature = "hnsw_bench"))]
use rand_pcg::Pcg64;
#[cfg(any(feature = "vec", feature = "hnsw_bench"))]
use space::Metric;

fn vec_config() -> impl bincode::config::Config {
    bincode::config::standard()
        .with_fixed_int_encoding()
        .with_little_endian()
}

#[allow(clippy::cast_possible_truncation)]
const VEC_DECODE_LIMIT: usize = crate::MAX_INDEX_BYTES as usize;

#[cfg(any(feature = "vec", feature = "hnsw_bench"))]
const HNSW_THRESHOLD: usize = 1000;
/// Fixed-point scaling factor for HNSW distances.
/// Necessary because `space::Metric` requires `Unit: Unsigned`, but we use f32 L2 distances.
/// 100,000.0 gives 1e-5 precision and max distance ~42,000 (enough for high-dim embeddings).
#[cfg(any(feature = "vec", feature = "hnsw_bench"))]
const HNSW_DISTANCE_SCALE: f32 = 100_000.0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VecDocument {
    pub frame_id: FrameId,
    pub embedding: Vec<f32>,
}

#[derive(Default)]
pub struct VecIndexBuilder {
    documents: Vec<VecDocument>,
}

impl VecIndexBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_document<I>(&mut self, frame_id: FrameId, embedding: I)
    where
        I: Into<Vec<f32>>,
    {
        self.documents.push(VecDocument {
            frame_id,
            embedding: embedding.into(),
        });
    }

    pub fn finish(self) -> Result<VecIndexArtifact> {
        #[cfg(any(feature = "vec", feature = "hnsw_bench"))]
        if self.documents.len() >= HNSW_THRESHOLD {
            return self.finish_hnsw();
        }

        let bytes = bincode::serde::encode_to_vec(&self.documents, vec_config())?;

        let checksum = *hash(&bytes).as_bytes();
        let dimension = self
            .documents
            .first()
            .map_or(0, |doc| u32::try_from(doc.embedding.len()).unwrap_or(0));
        #[cfg(feature = "parallel_segments")]
        let bytes_uncompressed = self
            .documents
            .iter()
            .map(|doc| doc.embedding.len() * std::mem::size_of::<f32>())
            .sum::<usize>() as u64;
        Ok(VecIndexArtifact {
            bytes,
            vector_count: self.documents.len() as u64,
            dimension,
            checksum,
            #[cfg(feature = "parallel_segments")]
            bytes_uncompressed,
        })
    }

    #[cfg(any(feature = "vec", feature = "hnsw_bench"))]
    #[allow(clippy::cast_possible_truncation)]
    fn finish_hnsw(self) -> Result<VecIndexArtifact> {
        let count = self.documents.len() as u64;
        let dimension = self
            .documents
            .first()
            .map(|d| d.embedding.len() as u32)
            .unwrap_or(0);

        #[cfg(feature = "parallel_segments")]
        let bytes_uncompressed = self
            .documents
            .iter()
            .map(|doc| doc.embedding.len() * std::mem::size_of::<f32>())
            .sum::<usize>() as u64;

        let index = HnswVecIndex::build(&self.documents)?;
        let bytes = bincode::serde::encode_to_vec(&index, vec_config())?;
        let checksum = *hash(&bytes).as_bytes();

        Ok(VecIndexArtifact {
            bytes,
            vector_count: count,
            dimension,
            checksum,
            #[cfg(feature = "parallel_segments")]
            bytes_uncompressed,
        })
    }
}

#[derive(Debug, Clone)]
pub struct VecIndexArtifact {
    pub bytes: Vec<u8>,
    pub vector_count: u64,
    pub dimension: u32,
    pub checksum: [u8; 32],
    #[cfg(feature = "parallel_segments")]
    pub bytes_uncompressed: u64,
}

#[derive(Debug, Clone)]
pub enum VecIndex {
    Uncompressed {
        documents: Vec<VecDocument>,
    },
    Compressed(crate::vec_pq::QuantizedVecIndex),
    #[cfg(any(feature = "vec", feature = "hnsw_bench"))]
    Hnsw(HnswVecIndex),
}

impl VecIndex {
    /// Decode vector index from bytes
    /// For backward compatibility, defaults to uncompressed if no manifest provided
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Self::decode_with_compression(bytes, crate::VectorCompression::None)
    }

    /// Decode vector index with compression mode from manifest
    ///
    /// ALWAYS tries uncompressed format first, regardless of compression flag.
    /// This is necessary because `MIN_VECTORS_FOR_PQ` threshold (100 vectors)
    /// causes most segments to be stored as uncompressed even when Pq96 is requested.
    /// Falls back to PQ format for true compressed segments.
    pub fn decode_with_compression(
        bytes: &[u8],
        _compression: crate::VectorCompression,
    ) -> Result<Self> {
        // Try uncompressed format first, regardless of compression flag.
        // This is necessary because MIN_VECTORS_FOR_PQ threshold (100 vectors)
        // causes most segments to be stored as uncompressed even when Pq96 is requested.
        match bincode::serde::decode_from_slice::<Vec<VecDocument>, _>(
            bytes,
            bincode::config::standard()
                .with_fixed_int_encoding()
                .with_little_endian()
                .with_limit::<VEC_DECODE_LIMIT>(),
        ) {
            Ok((documents, read)) if read == bytes.len() => {
                tracing::debug!(
                    bytes_len = bytes.len(),
                    docs_count = documents.len(),
                    "decoded as uncompressed"
                );
                return Ok(Self::Uncompressed { documents });
            }
            Ok((_, read)) => {
                tracing::debug!(
                    bytes_len = bytes.len(),
                    read = read,
                    "uncompressed decode partial read, trying HNSW/PQ"
                );
            }
            Err(err) => {
                tracing::debug!(
                    error = %err,
                    bytes_len = bytes.len(),
                    "uncompressed decode failed, trying HNSW/PQ"
                );
            }
        }

        #[cfg(any(feature = "vec", feature = "hnsw_bench"))]
        {
            match bincode::serde::decode_from_slice::<HnswVecIndex, _>(
                bytes,
                bincode::config::standard()
                    .with_fixed_int_encoding()
                    .with_little_endian()
                    .with_limit::<VEC_DECODE_LIMIT>(),
            ) {
                Ok((index, _)) => {
                    tracing::debug!(bytes_len = bytes.len(), "decoded as HNSW");
                    return Ok(Self::Hnsw(index));
                }
                Err(err) => {
                    tracing::debug!(
                        error = %err,
                        bytes_len = bytes.len(),
                        "HNSW decode failed, trying PQ"
                    );
                }
            }
        }

        // Try Product Quantization format
        match crate::vec_pq::QuantizedVecIndex::decode(bytes) {
            Ok(quantized_index) => {
                tracing::debug!(bytes_len = bytes.len(), "decoded as PQ");
                Ok(Self::Compressed(quantized_index))
            }
            Err(err) => {
                tracing::debug!(
                    error = %err,
                    bytes_len = bytes.len(),
                    "PQ decode also failed"
                );
                Err(MemvidError::InvalidToc {
                    reason: "unsupported vector index encoding".into(),
                })
            }
        }
    }

    #[must_use]
    pub fn search(&self, query: &[f32], limit: usize) -> Vec<VecSearchHit> {
        if query.is_empty() {
            return Vec::new();
        }
        match self {
            VecIndex::Uncompressed { documents } => {
                let mut hits: Vec<VecSearchHit> = documents
                    .iter()
                    .map(|doc| {
                        let distance = l2_distance(query, &doc.embedding);
                        VecSearchHit {
                            frame_id: doc.frame_id,
                            distance,
                        }
                    })
                    .collect();
                hits.sort_by(|a, b| {
                    a.distance
                        .partial_cmp(&b.distance)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                hits.truncate(limit);
                hits
            }
            VecIndex::Compressed(quantized) => quantized.search(query, limit),
            #[cfg(any(feature = "vec", feature = "hnsw_bench"))]
            VecIndex::Hnsw(index) => index.search(query, limit),
        }
    }

    #[must_use]
    pub fn entries(&self) -> Box<dyn Iterator<Item = (FrameId, &[f32])> + '_> {
        match self {
            VecIndex::Uncompressed { documents } => Box::new(
                documents
                    .iter()
                    .map(|doc| (doc.frame_id, doc.embedding.as_slice())),
            ),
            VecIndex::Compressed(_) => {
                // Compressed vectors don't have direct f32 access
                Box::new(std::iter::empty())
            }
            #[cfg(any(feature = "vec", feature = "hnsw_bench"))]
            VecIndex::Hnsw(_) => {
                // HNSW graph doesn't easily iterate all embeddings
                Box::new(std::iter::empty())
            }
        }
    }

    /// Return an owned, enumerable representation suitable for a destructive
    /// rebuild. Unlike `entries()`, this covers every supported encoding.
    pub(crate) fn documents_for_rebuild(&self) -> Result<Vec<VecDocument>> {
        match self {
            VecIndex::Uncompressed { documents } => Ok(documents.clone()),
            VecIndex::Compressed(quantized) => quantized.decoded_documents(),
            #[cfg(any(feature = "vec", feature = "hnsw_bench"))]
            VecIndex::Hnsw(index) => index.documents(),
        }
    }

    #[cfg(feature = "parallel_segments")]
    pub(crate) fn vector_count(&self) -> usize {
        match self {
            VecIndex::Uncompressed { documents } => documents.len(),
            VecIndex::Compressed(quantized) => quantized.len(),
            #[cfg(any(feature = "vec", feature = "hnsw_bench"))]
            VecIndex::Hnsw(index) => index.ids.len(),
        }
    }

    #[must_use]
    pub fn embedding_for(&self, frame_id: FrameId) -> Option<&[f32]> {
        match self {
            VecIndex::Uncompressed { documents } => documents
                .iter()
                .find(|doc| doc.frame_id == frame_id)
                .map(|doc| doc.embedding.as_slice()),
            VecIndex::Compressed(_) => {
                // Compressed vectors don't have direct f32 access
                None
            }
            #[cfg(any(feature = "vec", feature = "hnsw_bench"))]
            VecIndex::Hnsw(_) => {
                // HNSW storage is internal, would need traversal to find exact embedding
                // For now, return None as we do for Compressed
                None
            }
        }
    }

    pub fn remove(&mut self, frame_id: FrameId) {
        match self {
            VecIndex::Uncompressed { documents } => {
                documents.retain(|doc| doc.frame_id != frame_id);
            }
            VecIndex::Compressed(_quantized) => {
                // Compressed indices are immutable
            }
            #[cfg(any(feature = "vec", feature = "hnsw_bench"))]
            VecIndex::Hnsw(_) => {
                // HNSW indices are immutable in this implementation
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VecSearchHit {
    pub frame_id: FrameId,
    pub distance: f32,
}

fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    crate::simd::l2_distance_simd(a, b)
}

#[cfg(any(feature = "vec", feature = "hnsw_bench"))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Euclidean;

#[cfg(any(feature = "vec", feature = "hnsw_bench"))]
impl Metric<Vec<f32>> for Euclidean {
    type Unit = u32;
    fn distance(&self, a: &Vec<f32>, b: &Vec<f32>) -> u32 {
        let d = l2_distance(a, b);
        // Saturating cast prevents overflow for huge distances (though unlikely for embeddings)
        (d * HNSW_DISTANCE_SCALE).min(u32::MAX as f32) as u32
    }
}

#[cfg(any(feature = "vec", feature = "hnsw_bench"))]
#[derive(Clone, Serialize, Deserialize)]
#[allow(clippy::unsafe_derive_deserialize)]
pub struct HnswVecIndex {
    graph: Hnsw<Euclidean, Vec<f32>, Pcg64, 16, 32>,
    ids: Vec<FrameId>,
    dimension: u32,
}

#[cfg(any(feature = "vec", feature = "hnsw_bench"))]
impl std::fmt::Debug for HnswVecIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HnswVecIndex")
            .field("dimension", &self.dimension)
            .field("vector_count", &self.ids.len())
            .finish_non_exhaustive()
    }
}

#[cfg(any(feature = "vec", feature = "hnsw_bench"))]
impl HnswVecIndex {
    #[allow(clippy::cast_possible_truncation)]
    pub fn build(documents: &[VecDocument]) -> Result<Self> {
        let params = Params::new().ef_construction(100);
        let mut graph = Hnsw::new_params(Euclidean, params);
        let mut ids = Vec::with_capacity(documents.len());
        let mut searcher = Searcher::default();

        for doc in documents {
            graph.insert(doc.embedding.clone(), &mut searcher);
            ids.push(doc.frame_id);
        }

        Ok(Self {
            graph,
            ids,
            dimension: documents
                .first()
                .map(|d| d.embedding.len() as u32)
                .unwrap_or(0),
        })
    }

    #[must_use]
    pub fn search(&self, query: &[f32], limit: usize) -> Vec<VecSearchHit> {
        // Use thread-local searcher and dest buffer to avoid per-query allocations
        thread_local! {
            static SEARCHER: std::cell::RefCell<Searcher<u32>> = std::cell::RefCell::new(Searcher::new());
            static DEST: std::cell::RefCell<Vec<space::Neighbor<u32>>> = const { std::cell::RefCell::new(Vec::new()) };
        }

        // ef_search: query-time search width. Higher = better recall, slower search.
        // Default: 50 as per maintainer specification. Can be exposed as option later.
        // Scoped retrieval widens its candidate request progressively. Keep the
        // graph search width at least that large so an initial global top-k
        // cannot permanently hide a thread outside that first window.
        let ef_search = limit.max(50);

        SEARCHER.with(|searcher_cell| {
            DEST.with(|dest_cell| {
                let mut searcher = searcher_cell.borrow_mut();
                let mut dest = dest_cell.borrow_mut();

                // Ensure dest has enough capacity
                let required_size = limit.max(ef_search);
                if dest.len() < required_size {
                    dest.resize(
                        required_size,
                        space::Neighbor {
                            index: !0,
                            distance: 0,
                        },
                    );
                }

                // Convert query slice to Vec for the graph
                let query_vec: Vec<f32> = query.to_vec();

                let found = self.graph.nearest(
                    &query_vec,
                    ef_search,
                    &mut searcher,
                    &mut dest[..required_size],
                );

                found
                    .iter()
                    .take(limit)
                    .map(|neighbor| VecSearchHit {
                        frame_id: self.ids[neighbor.index],
                        distance: (neighbor.distance as f32) / HNSW_DISTANCE_SCALE,
                    })
                    .collect()
            })
        })
    }

    fn documents(&self) -> Result<Vec<VecDocument>> {
        if self.graph.len() != self.ids.len() || self.graph.layer_len(0) != self.ids.len() {
            return Err(MemvidError::InvalidToc {
                reason: "HNSW vector IDs do not match graph features".into(),
            });
        }
        self.ids
            .iter()
            .enumerate()
            .map(|(index, frame_id)| {
                let embedding = self.graph.feature(index).clone();
                if embedding.len() != self.dimension as usize {
                    return Err(MemvidError::InvalidToc {
                        reason: "HNSW vector dimension does not match its manifest".into(),
                    });
                }
                Ok(VecDocument {
                    frame_id: *frame_id,
                    embedding,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(any(feature = "vec", feature = "hnsw_bench"))]
    fn malformed_hnsw_feature_geometry_returns_error_without_panicking() {
        let documents: Vec<VecDocument> = (0..HNSW_THRESHOLD)
            .map(|frame_id| VecDocument {
                frame_id: frame_id as FrameId,
                embedding: vec![frame_id as f32, 1.0, 2.0, 3.0],
            })
            .collect();
        let valid = HnswVecIndex::build(&documents).unwrap();
        assert_eq!(valid.documents().unwrap().len(), HNSW_THRESHOLD);

        let mut serialized = serde_json::to_string(&valid).unwrap();
        let key = serialized.find("\"features\":").unwrap();
        let start = key + "\"features\":".len();
        let mut depth = 0_usize;
        let mut end = start;
        for (index, byte) in serialized.as_bytes()[start..].iter().enumerate() {
            if *byte == b'[' {
                depth += 1;
            } else if *byte == b']' {
                depth -= 1;
                if depth == 0 {
                    end = start + index + 1;
                    break;
                }
            }
        }
        serialized.replace_range(start..end, "[]");
        let malformed: HnswVecIndex = serde_json::from_str(&serialized).unwrap();
        assert_eq!(malformed.ids.len(), HNSW_THRESHOLD);
        assert_eq!(malformed.graph.len(), HNSW_THRESHOLD);
        assert_eq!(malformed.graph.layer_len(0), 0);

        let bytes = bincode::serde::encode_to_vec(&malformed, vec_config()).unwrap();
        let decoded = VecIndex::decode(&bytes).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            decoded.documents_for_rebuild()
        }));
        assert!(result.is_ok(), "malformed HNSW enumeration panicked");
        assert!(result.unwrap().is_err());
    }

    #[test]
    fn builder_roundtrip() {
        let mut builder = VecIndexBuilder::new();
        builder.add_document(1, vec![0.0, 1.0, 2.0]);
        builder.add_document(2, vec![1.0, 2.0, 3.0]);
        let artifact = builder.finish().expect("finish");
        assert_eq!(artifact.vector_count, 2);
        assert_eq!(artifact.dimension, 3);

        let index = VecIndex::decode(&artifact.bytes).expect("decode");
        let hits = index.search(&[0.0, 1.0, 2.0], 10);
        assert_eq!(hits[0].frame_id, 1);
    }

    #[test]
    fn l2_distance_behaves() {
        let d = l2_distance(&[0.0, 0.0], &[3.0, 4.0]);
        assert!((d - 5.0).abs() < 1e-6);
    }

    /// Test that HNSW is used for indices with >1000 vectors (HNSW_THRESHOLD)
    #[test]
    #[cfg(any(feature = "vec", feature = "hnsw_bench"))]
    fn hnsw_threshold_triggers_hnsw_index() {
        use super::HNSW_THRESHOLD;

        // Create index with exactly HNSW_THRESHOLD vectors
        let mut builder = VecIndexBuilder::new();
        let dim = 32;
        for i in 0..HNSW_THRESHOLD {
            let embedding: Vec<f32> = (0..dim).map(|j| (i * dim + j) as f32 / 1000.0).collect();
            builder.add_document(i as FrameId, embedding);
        }

        let artifact = builder.finish().expect("finish hnsw");
        assert_eq!(artifact.vector_count, HNSW_THRESHOLD as u64);

        // Decode and verify it's an HNSW index
        let index = VecIndex::decode(&artifact.bytes).expect("decode");
        assert!(
            matches!(index, VecIndex::Hnsw(_)),
            "Expected HNSW index for {} vectors",
            HNSW_THRESHOLD
        );
    }

    /// Test that brute force is used for indices below threshold
    #[test]
    #[cfg(any(feature = "vec", feature = "hnsw_bench"))]
    fn below_threshold_uses_brute_force() {
        use super::HNSW_THRESHOLD;

        // Create index with fewer than HNSW_THRESHOLD vectors
        let mut builder = VecIndexBuilder::new();
        let count = HNSW_THRESHOLD - 1;
        let dim = 32;
        for i in 0..count {
            let embedding: Vec<f32> = (0..dim).map(|j| (i * dim + j) as f32 / 1000.0).collect();
            builder.add_document(i as FrameId, embedding);
        }

        let artifact = builder.finish().expect("finish brute force");
        assert_eq!(artifact.vector_count, count as u64);

        // Decode and verify it's NOT an HNSW index
        let index = VecIndex::decode(&artifact.bytes).expect("decode");
        assert!(
            matches!(index, VecIndex::Uncompressed { .. }),
            "Expected Uncompressed index for {} vectors",
            count
        );
    }

    /// Test HNSW search returns correct nearest neighbors
    #[test]
    #[cfg(any(feature = "vec", feature = "hnsw_bench"))]
    fn hnsw_search_finds_nearest_neighbors() {
        use super::HNSW_THRESHOLD;

        let mut builder = VecIndexBuilder::new();
        let dim = 32;

        // Insert HNSW_THRESHOLD vectors with predictable embeddings
        for i in 0..HNSW_THRESHOLD {
            let embedding: Vec<f32> = (0..dim).map(|_| i as f32).collect();
            builder.add_document(i as FrameId, embedding);
        }

        let artifact = builder.finish().expect("finish");
        let index = VecIndex::decode(&artifact.bytes).expect("decode");

        // Query with a vector identical to frame_id=500
        let query: Vec<f32> = (0..dim).map(|_| 500.0_f32).collect();
        let hits = index.search(&query, 5);

        assert!(!hits.is_empty(), "Should find at least one hit");
        assert_eq!(
            hits[0].frame_id, 500,
            "Nearest neighbor should be exact match"
        );
        assert!(
            hits[0].distance < 0.001,
            "Distance to exact match should be near zero"
        );
    }

    /// Test HNSW serialization/deserialization roundtrip
    #[test]
    #[cfg(any(feature = "vec", feature = "hnsw_bench"))]
    fn hnsw_serialization_roundtrip() {
        use super::HNSW_THRESHOLD;

        let mut builder = VecIndexBuilder::new();
        let dim = 64;

        for i in 0..HNSW_THRESHOLD {
            let embedding: Vec<f32> = (0..dim).map(|j| ((i + j) % 100) as f32 / 100.0).collect();
            builder.add_document(i as FrameId, embedding);
        }

        let artifact = builder.finish().expect("finish");
        let original_bytes = artifact.bytes.clone();

        // Decode
        let index = VecIndex::decode(&original_bytes).expect("decode");
        assert!(matches!(index, VecIndex::Hnsw(_)));

        // Search before any re-serialization
        let query: Vec<f32> = (0..dim).map(|j| (j % 100) as f32 / 100.0).collect();
        let hits_1 = index.search(&query, 10);

        // Decode again from same bytes (simulates loading from disk)
        let index_2 = VecIndex::decode(&original_bytes).expect("decode again");
        let hits_2 = index_2.search(&query, 10);

        // Results should be identical
        assert_eq!(hits_1.len(), hits_2.len());
        for (h1, h2) in hits_1.iter().zip(hits_2.iter()) {
            assert_eq!(h1.frame_id, h2.frame_id);
            assert!((h1.distance - h2.distance).abs() < 1e-6);
        }
    }

    /// Test HNSW with larger dataset to verify approximate search quality
    #[test]
    #[cfg(any(feature = "vec", feature = "hnsw_bench"))]
    fn hnsw_recall_quality() {
        use super::HNSW_THRESHOLD;

        let count = HNSW_THRESHOLD + 500; // 1500 vectors
        let dim = 32;

        // Build HNSW index
        let mut builder = VecIndexBuilder::new();
        let embeddings: Vec<Vec<f32>> = (0..count)
            .map(|i| {
                (0..dim)
                    .map(|j| ((i * 7 + j * 13) % 1000) as f32 / 1000.0)
                    .collect()
            })
            .collect();

        for (i, emb) in embeddings.iter().enumerate() {
            builder.add_document(i as FrameId, emb.clone());
        }

        let artifact = builder.finish().expect("finish");
        let hnsw_index = VecIndex::decode(&artifact.bytes).expect("decode");

        // Also build brute force index for ground truth
        let brute_index = VecIndex::Uncompressed {
            documents: embeddings
                .iter()
                .enumerate()
                .map(|(i, emb)| VecDocument {
                    frame_id: i as FrameId,
                    embedding: emb.clone(),
                })
                .collect(),
        };

        // Query with vector similar to index 750
        let query = embeddings[750].clone();
        let k = 10;

        let hnsw_hits = hnsw_index.search(&query, k);
        let brute_hits = brute_index.search(&query, k);

        // HNSW should find the exact match first
        assert_eq!(hnsw_hits[0].frame_id, 750, "HNSW should find exact match");
        assert_eq!(
            brute_hits[0].frame_id, 750,
            "Brute force should find exact match"
        );

        // Calculate recall: how many of top-k from HNSW are in top-k from brute force
        let brute_set: std::collections::HashSet<_> =
            brute_hits.iter().map(|h| h.frame_id).collect();
        let recall = hnsw_hits
            .iter()
            .filter(|h| brute_set.contains(&h.frame_id))
            .count();
        let recall_ratio = recall as f32 / k as f32;

        // HNSW should achieve at least 80% recall on this simple dataset
        assert!(
            recall_ratio >= 0.8,
            "HNSW recall {} should be >= 0.8",
            recall_ratio
        );
    }
}
