//! Product Quantization (PQ) for vector compression
//!
//! Compresses 384-dim f32 vectors from 1,536 bytes to 96 bytes (16x compression)
//! while maintaining ~95% search accuracy using codebook-based quantization.
//!
//! **Algorithm**:
//! 1. Split 384-dim vector into 96 subspaces of 4 dimensions each
//! 2. For each subspace, train 256 centroids using k-means
//! 3. Each vector is encoded as 96 bytes (one u8 index per subspace)
//! 4. Search uses ADC (Asymmetric Distance Computation) with lookup tables

use blake3::hash;
use serde::{Deserialize, Serialize};

use crate::vec::VecSearchHit;
use crate::{MemvidError, Result, types::FrameId};

fn vec_config() -> impl bincode::config::Config {
    bincode::config::standard()
        .with_fixed_int_encoding()
        .with_little_endian()
}

#[allow(clippy::cast_possible_truncation)]
const VEC_DECODE_LIMIT: usize = crate::MAX_INDEX_BYTES as usize;

/// Product Quantization parameters
const NUM_SUBSPACES: usize = 96; // 384 dims / 4 dims per subspace
const SUBSPACE_DIM: usize = 4; // Dimensions per subspace
const NUM_CENTROIDS: usize = 256; // 2^8 centroids (encoded as u8)
const TOTAL_DIM: usize = NUM_SUBSPACES * SUBSPACE_DIM; // 384

/// Codebook for one subspace: 256 centroids, each with `SUBSPACE_DIM` dimensions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubspaceCodebook {
    /// Flat array: centroids[i*`SUBSPACE_DIM..(i+1)`*`SUBSPACE_DIM`] is centroid i
    centroids: Vec<f32>,
}

impl SubspaceCodebook {
    fn new() -> Self {
        Self {
            centroids: vec![0.0; NUM_CENTROIDS * SUBSPACE_DIM],
        }
    }

    fn get_centroid(&self, index: u8) -> &[f32] {
        let start = (index as usize) * SUBSPACE_DIM;
        &self.centroids[start..start + SUBSPACE_DIM]
    }

    fn set_centroid(&mut self, index: u8, values: &[f32]) {
        assert_eq!(values.len(), SUBSPACE_DIM);
        let start = (index as usize) * SUBSPACE_DIM;
        self.centroids[start..start + SUBSPACE_DIM].copy_from_slice(values);
    }

    /// Find nearest centroid to a subspace vector
    fn quantize(&self, subspace: &[f32]) -> u8 {
        assert_eq!(subspace.len(), SUBSPACE_DIM);

        let mut best_idx = 0u8;
        let mut best_dist = f32::INFINITY;

        for i in 0..NUM_CENTROIDS {
            #[allow(clippy::cast_possible_truncation)]
            let centroid = self.get_centroid(i as u8);
            let dist = l2_distance_squared(subspace, centroid);
            if dist < best_dist {
                best_dist = dist;
                #[allow(clippy::cast_possible_truncation)]
                {
                    best_idx = i as u8;
                }
            }
        }

        best_idx
    }
}

/// Product Quantizer with codebooks for all subspaces
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductQuantizer {
    /// One codebook per subspace
    codebooks: Vec<SubspaceCodebook>,
    dimension: u32,
}

impl ProductQuantizer {
    fn validate_decode_geometry(&self) -> Result<()> {
        if self.dimension as usize != TOTAL_DIM {
            return Err(MemvidError::InvalidToc {
                reason: format!(
                    "PQ dimension mismatch: expected {TOTAL_DIM}, got {}",
                    self.dimension
                )
                .into(),
            });
        }
        if self.codebooks.len() != NUM_SUBSPACES {
            return Err(MemvidError::InvalidToc {
                reason: format!(
                    "PQ codebook count mismatch: expected {NUM_SUBSPACES}, got {}",
                    self.codebooks.len()
                )
                .into(),
            });
        }
        let expected_centroids = NUM_CENTROIDS * SUBSPACE_DIM;
        for (index, codebook) in self.codebooks.iter().enumerate() {
            if codebook.centroids.len() != expected_centroids {
                return Err(MemvidError::InvalidToc {
                    reason: format!(
                        "PQ codebook {index} centroid length mismatch: expected {expected_centroids}, got {}",
                        codebook.centroids.len()
                    )
                    .into(),
                });
            }
        }
        Ok(())
    }

    /// Create uninitialized quantizer
    pub fn new(dimension: u32) -> Result<Self> {
        if dimension as usize != TOTAL_DIM {
            return Err(MemvidError::InvalidQuery {
                reason: format!("PQ only supports {TOTAL_DIM}-dim vectors, got {dimension}"),
            });
        }

        Ok(Self {
            codebooks: vec![SubspaceCodebook::new(); NUM_SUBSPACES],
            dimension,
        })
    }

    /// Train codebooks using k-means on sample vectors
    pub fn train(&mut self, training_vectors: &[Vec<f32>], max_iterations: usize) -> Result<()> {
        if training_vectors.is_empty() {
            return Err(MemvidError::InvalidQuery {
                reason: "Cannot train PQ with empty training set".to_string(),
            });
        }

        // Verify all vectors have correct dimension
        for vec in training_vectors {
            if vec.len() != TOTAL_DIM {
                return Err(MemvidError::InvalidQuery {
                    reason: format!(
                        "Training vector has wrong dimension: expected {}, got {}",
                        TOTAL_DIM,
                        vec.len()
                    ),
                });
            }
        }

        // Train each subspace independently
        for subspace_idx in 0..NUM_SUBSPACES {
            let start_dim = subspace_idx * SUBSPACE_DIM;
            let end_dim = start_dim + SUBSPACE_DIM;

            // Extract subspace vectors
            let subspace_vecs: Vec<Vec<f32>> = training_vectors
                .iter()
                .map(|v| v[start_dim..end_dim].to_vec())
                .collect();

            // Run k-means
            let centroids = kmeans(&subspace_vecs, NUM_CENTROIDS, max_iterations)?;

            // Store in codebook
            for (i, centroid) in centroids.iter().enumerate() {
                #[allow(clippy::cast_possible_truncation)]
                self.codebooks[subspace_idx].set_centroid(i as u8, centroid);
            }
        }

        Ok(())
    }

    /// Encode a vector into PQ codes (96 bytes)
    pub fn encode(&self, vector: &[f32]) -> Result<Vec<u8>> {
        if vector.len() != TOTAL_DIM {
            return Err(MemvidError::InvalidQuery {
                reason: format!(
                    "Vector dimension mismatch: expected {}, got {}",
                    TOTAL_DIM,
                    vector.len()
                ),
            });
        }

        let mut codes = Vec::with_capacity(NUM_SUBSPACES);

        for subspace_idx in 0..NUM_SUBSPACES {
            let start_dim = subspace_idx * SUBSPACE_DIM;
            let end_dim = start_dim + SUBSPACE_DIM;
            let subspace = &vector[start_dim..end_dim];

            let code = self.codebooks[subspace_idx].quantize(subspace);
            codes.push(code);
        }

        Ok(codes)
    }

    /// Decode PQ codes back to approximate vector (for debugging/verification)
    pub fn decode(&self, codes: &[u8]) -> Result<Vec<f32>> {
        self.validate_decode_geometry()?;
        if codes.len() != NUM_SUBSPACES {
            return Err(MemvidError::InvalidQuery {
                reason: format!(
                    "Invalid PQ codes length: expected {}, got {}",
                    NUM_SUBSPACES,
                    codes.len()
                ),
            });
        }

        let mut vector = Vec::with_capacity(TOTAL_DIM);

        for (subspace_idx, &code) in codes.iter().enumerate() {
            let centroid = self.codebooks[subspace_idx].get_centroid(code);
            vector.extend_from_slice(centroid);
        }

        Ok(vector)
    }

    /// Compute asymmetric distance between query vector and PQ-encoded vector
    /// Uses precomputed lookup tables for efficiency
    #[must_use]
    pub fn asymmetric_distance(&self, query: &[f32], codes: &[u8]) -> f32 {
        if query.len() != TOTAL_DIM || codes.len() != NUM_SUBSPACES {
            return f32::INFINITY;
        }

        let mut total_dist_sq = 0.0f32;

        for subspace_idx in 0..NUM_SUBSPACES {
            let start_dim = subspace_idx * SUBSPACE_DIM;
            let end_dim = start_dim + SUBSPACE_DIM;
            let query_subspace = &query[start_dim..end_dim];

            let code = codes[subspace_idx];
            let centroid = self.codebooks[subspace_idx].get_centroid(code);

            total_dist_sq += l2_distance_squared(query_subspace, centroid);
        }

        total_dist_sq.sqrt()
    }
}

/// Compressed vector document
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantizedVecDocument {
    pub frame_id: FrameId,
    /// PQ codes: 96 bytes (one u8 per subspace)
    pub codes: Vec<u8>,
}

/// Builder for compressed vector index
#[derive(Default)]
pub struct QuantizedVecIndexBuilder {
    documents: Vec<QuantizedVecDocument>,
    quantizer: Option<ProductQuantizer>,
}

impl QuantizedVecIndexBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Train quantizer on sample vectors before encoding
    pub fn train_quantizer(&mut self, training_vectors: &[Vec<f32>], dimension: u32) -> Result<()> {
        let mut pq = ProductQuantizer::new(dimension)?;
        pq.train(training_vectors, 25)?; // 25 k-means iterations
        self.quantizer = Some(pq);
        Ok(())
    }

    /// Add document with pre-trained quantizer
    pub fn add_document(&mut self, frame_id: FrameId, embedding: Vec<f32>) -> Result<()> {
        let quantizer = self
            .quantizer
            .as_ref()
            .ok_or_else(|| MemvidError::InvalidQuery {
                reason: "Quantizer not trained. Call train_quantizer first".to_string(),
            })?;

        let codes = quantizer.encode(&embedding)?;

        self.documents
            .push(QuantizedVecDocument { frame_id, codes });

        Ok(())
    }

    pub fn finish(self) -> Result<QuantizedVecIndexArtifact> {
        let quantizer = self.quantizer.ok_or_else(|| MemvidError::InvalidQuery {
            reason: "Quantizer not trained".to_string(),
        })?;

        let vector_count = self.documents.len() as u64;
        let bytes =
            bincode::serde::encode_to_vec(&(quantizer.clone(), self.documents), vec_config())?;
        let checksum = *hash(&bytes).as_bytes();

        Ok(QuantizedVecIndexArtifact {
            bytes,
            vector_count,
            dimension: quantizer.dimension,
            checksum,
            compression_ratio: 16.0, // 1536 bytes -> 96 bytes
        })
    }
}

#[derive(Debug, Clone)]
pub struct QuantizedVecIndexArtifact {
    pub bytes: Vec<u8>,
    pub vector_count: u64,
    pub dimension: u32,
    pub checksum: [u8; 32],
    pub compression_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct QuantizedVecIndex {
    quantizer: ProductQuantizer,
    documents: Vec<QuantizedVecDocument>,
}

impl QuantizedVecIndex {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        // Try decoding with current format (with dimension field)
        let config = bincode::config::standard()
            .with_fixed_int_encoding()
            .with_little_endian()
            .with_limit::<VEC_DECODE_LIMIT>();

        if let Ok(((quantizer, documents), read)) = bincode::serde::decode_from_slice::<
            (ProductQuantizer, Vec<QuantizedVecDocument>),
            _,
        >(bytes, config)
        {
            if read == bytes.len() {
                return Ok(Self {
                    quantizer,
                    documents,
                });
            }
        }

        // Fall back to old format (without dimension field)
        // Old ProductQuantizer struct without dimension field
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct OldProductQuantizer {
            codebooks: Vec<SubspaceCodebook>,
        }

        let ((old_quantizer, documents), read): (
            (OldProductQuantizer, Vec<QuantizedVecDocument>),
            usize,
        ) = bincode::serde::decode_from_slice(bytes, config)?;

        if read != bytes.len() {
            return Err(MemvidError::InvalidToc {
                reason: "unsupported quantized vector index encoding".into(),
            });
        }

        // Convert old format to new format
        let quantizer = ProductQuantizer {
            codebooks: old_quantizer.codebooks,
            dimension: u32::try_from(NUM_SUBSPACES * SUBSPACE_DIM).unwrap_or(u32::MAX),
        };

        Ok(Self {
            quantizer,
            documents,
        })
    }

    /// Search using asymmetric distance computation
    #[must_use]
    pub fn search(&self, query: &[f32], limit: usize) -> Vec<VecSearchHit> {
        if query.is_empty() {
            return Vec::new();
        }

        let mut hits: Vec<VecSearchHit> = self
            .documents
            .iter()
            .map(|doc| {
                let distance = self.quantizer.asymmetric_distance(query, &doc.codes);
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

    pub fn remove(&mut self, frame_id: FrameId) {
        self.documents.retain(|doc| doc.frame_id != frame_id);
    }

    pub(crate) fn decoded_documents(&self) -> Result<Vec<crate::vec::VecDocument>> {
        self.quantizer.validate_decode_geometry()?;
        self.documents
            .iter()
            .map(|document| {
                Ok(crate::vec::VecDocument {
                    frame_id: document.frame_id,
                    // Decode each code exactly once into its selected centroids.
                    // Rebuilds persist these values without another PQ pass.
                    embedding: self.quantizer.decode(&document.codes)?,
                })
            })
            .collect()
    }

    #[cfg(feature = "parallel_segments")]
    pub(crate) fn len(&self) -> usize {
        self.documents.len()
    }

    /// Get compression statistics
    #[must_use]
    pub fn compression_stats(&self) -> CompressionStats {
        let original_bytes = self.documents.len() * TOTAL_DIM * std::mem::size_of::<f32>();
        let compressed_bytes = self.documents.len() * NUM_SUBSPACES; // 96 bytes per vector
        let codebook_bytes =
            NUM_SUBSPACES * NUM_CENTROIDS * SUBSPACE_DIM * std::mem::size_of::<f32>();

        CompressionStats {
            vector_count: self.documents.len() as u64,
            original_bytes: original_bytes as u64,
            compressed_bytes: compressed_bytes as u64,
            codebook_bytes: codebook_bytes as u64,
            total_bytes: (compressed_bytes + codebook_bytes) as u64,
            compression_ratio: original_bytes as f64 / (compressed_bytes + codebook_bytes) as f64,
        }
    }
}

#[cfg(test)]
pub(crate) fn malformed_codebook_artifact() -> QuantizedVecIndexArtifact {
    let vector = vec![0.0; TOTAL_DIM];
    let mut builder = QuantizedVecIndexBuilder::new();
    builder
        .train_quantizer(std::slice::from_ref(&vector), TOTAL_DIM as u32)
        .unwrap();
    builder.add_document(0, vector).unwrap();
    let valid = builder.finish().unwrap();
    let mut index = QuantizedVecIndex::decode(&valid.bytes).unwrap();
    index.quantizer.codebooks[0].centroids.clear();
    let bytes =
        bincode::serde::encode_to_vec((&index.quantizer, &index.documents), vec_config()).unwrap();
    QuantizedVecIndexArtifact {
        checksum: *hash(&bytes).as_bytes(),
        bytes,
        vector_count: 1,
        dimension: TOTAL_DIM as u32,
        compression_ratio: valid.compression_ratio,
    }
}

#[derive(Debug, Clone)]
pub struct CompressionStats {
    pub vector_count: u64,
    pub original_bytes: u64,
    pub compressed_bytes: u64,
    pub codebook_bytes: u64,
    pub total_bytes: u64,
    pub compression_ratio: f64,
}

/// K-means clustering for a single subspace
fn kmeans(vectors: &[Vec<f32>], k: usize, max_iterations: usize) -> Result<Vec<Vec<f32>>> {
    if vectors.is_empty() {
        return Err(MemvidError::InvalidQuery {
            reason: "Cannot run k-means on empty vector set".to_string(),
        });
    }

    let dim = vectors[0].len();

    // Initialize centroids using k-means++ for better convergence
    let mut centroids = kmeans_plus_plus_init(vectors, k)?;

    for _iteration in 0..max_iterations {
        // Assignment step: assign each vector to nearest centroid
        let mut assignments = vec![Vec::new(); k];

        for vec in vectors {
            let mut best_cluster = 0;
            let mut best_dist = f32::INFINITY;

            for (cluster_idx, centroid) in centroids.iter().enumerate() {
                let dist = l2_distance_squared(vec, centroid);
                if dist < best_dist {
                    best_dist = dist;
                    best_cluster = cluster_idx;
                }
            }

            assignments[best_cluster].push(vec.clone());
        }

        // Update step: recompute centroids
        let mut changed = false;
        for (cluster_idx, cluster_vecs) in assignments.iter().enumerate() {
            if cluster_vecs.is_empty() {
                // Empty cluster: reinitialize with random vector
                centroids[cluster_idx] = vectors[cluster_idx % vectors.len()].clone();
                changed = true;
                continue;
            }

            let mut new_centroid = vec![0.0f32; dim];
            for vec in cluster_vecs {
                for (i, &val) in vec.iter().enumerate() {
                    new_centroid[i] += val;
                }
            }
            for val in &mut new_centroid {
                *val /= cluster_vecs.len() as f32;
            }

            // Check if centroid changed
            if l2_distance_squared(&centroids[cluster_idx], &new_centroid) > 1e-6 {
                changed = true;
            }

            centroids[cluster_idx] = new_centroid;
        }

        if !changed {
            break; // Converged
        }
    }

    Ok(centroids)
}

/// K-means++ initialization for better initial centroids
fn kmeans_plus_plus_init(vectors: &[Vec<f32>], k: usize) -> Result<Vec<Vec<f32>>> {
    if vectors.is_empty() || k == 0 {
        return Err(MemvidError::InvalidQuery {
            reason: "Invalid k-means++ initialization".to_string(),
        });
    }

    let mut centroids = Vec::new();

    // Choose first centroid randomly (use first vector for determinism)
    centroids.push(vectors[0].clone());

    // Choose remaining k-1 centroids
    for _ in 1..k {
        let mut distances = Vec::new();

        // Compute distance to nearest existing centroid for each vector
        for vec in vectors {
            let mut min_dist = f32::INFINITY;
            for centroid in &centroids {
                let dist = l2_distance_squared(vec, centroid);
                min_dist = min_dist.min(dist);
            }
            distances.push(min_dist);
        }

        // Choose vector with maximum distance as next centroid
        let max_idx = distances
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map_or(0, |(idx, _)| idx);

        centroids.push(vectors[max_idx].clone());
    }

    Ok(centroids)
}

/// Squared L2 distance between two vectors
fn l2_distance_squared(a: &[f32], b: &[f32]) -> f32 {
    crate::simd::l2_distance_squared_simd(a, b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_pq_codebook_returns_error_without_panicking() {
        let valid_vector = vec![0.0; TOTAL_DIM];
        let mut valid_builder = QuantizedVecIndexBuilder::new();
        valid_builder
            .train_quantizer(std::slice::from_ref(&valid_vector), TOTAL_DIM as u32)
            .unwrap();
        valid_builder.add_document(0, valid_vector).unwrap();
        let valid_artifact = valid_builder.finish().unwrap();
        let valid = QuantizedVecIndex::decode(&valid_artifact.bytes).unwrap();
        assert_eq!(valid.decoded_documents().unwrap().len(), 1);

        let artifact = malformed_codebook_artifact();
        let malformed = QuantizedVecIndex::decode(&artifact.bytes).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            malformed.decoded_documents()
        }));
        assert!(result.is_ok(), "malformed PQ enumeration panicked");
        assert!(result.unwrap().is_err());
    }

    #[test]
    fn test_subspace_codebook() {
        let mut codebook = SubspaceCodebook::new();

        // Set a centroid
        codebook.set_centroid(0, &[1.0, 2.0, 3.0, 4.0]);

        // Retrieve it
        let centroid = codebook.get_centroid(0);
        assert_eq!(centroid, &[1.0, 2.0, 3.0, 4.0]);

        // Quantize a similar vector
        let code = codebook.quantize(&[1.1, 2.1, 3.1, 4.1]);
        assert_eq!(code, 0);
    }

    #[test]
    fn test_product_quantizer_roundtrip() {
        // Create sample 384-dim vectors
        let mut training_vecs = Vec::new();
        for i in 0..100 {
            let mut vec = vec![0.0f32; TOTAL_DIM];
            for j in 0..TOTAL_DIM {
                vec[j] = ((i * TOTAL_DIM + j) % 100) as f32 / 100.0;
            }
            training_vecs.push(vec);
        }

        // Train quantizer
        let mut pq = ProductQuantizer::new(u32::try_from(TOTAL_DIM).unwrap()).unwrap();
        pq.train(&training_vecs, 10).unwrap();

        // Encode a vector
        let test_vec = &training_vecs[0];
        let codes = pq.encode(test_vec).unwrap();
        assert_eq!(codes.len(), NUM_SUBSPACES);

        // Decode and verify approximate reconstruction
        let decoded = pq.decode(&codes).unwrap();
        assert_eq!(decoded.len(), TOTAL_DIM);

        // Distance between original and decoded should be small
        let dist = l2_distance_squared(test_vec, &decoded).sqrt();
        assert!(dist < 10.0, "Reconstruction error too large: {}", dist);
    }

    #[test]
    fn test_quantized_index_builder() {
        // Create sample vectors
        let mut training_vecs = Vec::new();
        for i in 0..50 {
            let mut vec = vec![0.0f32; TOTAL_DIM];
            for j in 0..TOTAL_DIM {
                vec[j] = ((i + j) % 10) as f32;
            }
            training_vecs.push(vec);
        }

        // Build index
        let mut builder = QuantizedVecIndexBuilder::new();
        builder
            .train_quantizer(&training_vecs, u32::try_from(TOTAL_DIM).unwrap())
            .unwrap();

        for (i, vec) in training_vecs.iter().take(10).enumerate() {
            builder
                .add_document((i + 1) as FrameId, vec.clone())
                .unwrap();
        }

        let artifact = builder.finish().unwrap();
        assert_eq!(artifact.vector_count, 10);
        assert_eq!(artifact.dimension, u32::try_from(TOTAL_DIM).unwrap());
        assert!(artifact.compression_ratio > 10.0);

        // Decode and search
        let index = QuantizedVecIndex::decode(&artifact.bytes).unwrap();
        let query = &training_vecs[0];
        let hits = index.search(query, 5);

        assert!(!hits.is_empty());
        assert_eq!(hits[0].frame_id, 1); // Should find exact match first
    }

    #[test]
    fn test_kmeans_simple() {
        let vectors = vec![
            vec![0.0, 0.0],
            vec![0.1, 0.1],
            vec![10.0, 10.0],
            vec![10.1, 10.1],
        ];

        let centroids = kmeans(&vectors, 2, 100).unwrap();
        assert_eq!(centroids.len(), 2);

        // One centroid should be near [0, 0], the other near [10, 10]
        let near_zero = centroids.iter().any(|c| c[0] < 5.0 && c[1] < 5.0);
        let near_ten = centroids.iter().any(|c| c[0] > 5.0 && c[1] > 5.0);
        assert!(near_zero && near_ten);
    }
}
