//! SIMD-accelerated distance calculations for vector search.
//!
//! This module provides optimized L2 (Euclidean) distance functions using
//! the `wide` crate for portable SIMD across `x86_64` and aarch64.

#[cfg(feature = "simd")]
use wide::f32x8;

/// Compute squared L2 distance between two f32 slices using SIMD.
///
/// Uses 8-wide SIMD lanes (AVX2 on `x86_64`, NEON on aarch64).
/// Falls back to scalar for remainder elements.
#[cfg(feature = "simd")]
#[must_use]
pub fn l2_distance_squared_simd(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "vectors must have same length");

    let len = a.len();
    let chunks = len / 8;
    let remainder = len % 8;

    let mut sum = f32x8::ZERO;

    // Process 8 elements at a time
    for i in 0..chunks {
        let offset = i * 8;
        let a_chunk = f32x8::new([
            a[offset],
            a[offset + 1],
            a[offset + 2],
            a[offset + 3],
            a[offset + 4],
            a[offset + 5],
            a[offset + 6],
            a[offset + 7],
        ]);
        let b_chunk = f32x8::new([
            b[offset],
            b[offset + 1],
            b[offset + 2],
            b[offset + 3],
            b[offset + 4],
            b[offset + 5],
            b[offset + 6],
            b[offset + 7],
        ]);
        let diff = a_chunk - b_chunk;
        sum += diff * diff;
    }

    // Horizontal sum of the SIMD vector
    let sum_array: [f32; 8] = sum.into();
    let mut total: f32 = sum_array.iter().sum();

    // Handle remainder elements with scalar code
    let offset = chunks * 8;
    for i in 0..remainder {
        let diff = a[offset + i] - b[offset + i];
        total += diff * diff;
    }

    total
}

/// Compute L2 distance (with sqrt) using SIMD.
#[cfg(feature = "simd")]
#[must_use]
pub fn l2_distance_simd(a: &[f32], b: &[f32]) -> f32 {
    l2_distance_squared_simd(a, b).sqrt()
}

// Scalar fallbacks when SIMD feature is disabled

/// Compute squared L2 distance using scalar math.
#[cfg(not(feature = "simd"))]
pub fn l2_distance_squared_simd(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let diff = x - y;
            diff * diff
        })
        .sum()
}

/// Compute L2 distance using scalar math.
#[cfg(not(feature = "simd"))]
pub fn l2_distance_simd(a: &[f32], b: &[f32]) -> f32 {
    l2_distance_squared_simd(a, b).sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l2_distance_squared_basic() {
        let a = [0.0, 0.0, 0.0];
        let b = [3.0, 4.0, 0.0];
        let dist_sq = l2_distance_squared_simd(&a, &b);
        assert!(
            (dist_sq - 25.0).abs() < 1e-6,
            "expected 25.0, got {}",
            dist_sq
        );
    }

    #[test]
    fn test_l2_distance_basic() {
        let a = [0.0, 0.0];
        let b = [3.0, 4.0];
        let dist = l2_distance_simd(&a, &b);
        assert!((dist - 5.0).abs() < 1e-6, "expected 5.0, got {}", dist);
    }

    #[test]
    fn test_l2_distance_384_dims() {
        // Test with realistic 384-dim vectors
        let a: Vec<f32> = (0..384).map(|i| i as f32 * 0.01).collect();
        let b: Vec<f32> = (0..384).map(|i| (i + 1) as f32 * 0.01).collect();

        let dist_simd = l2_distance_simd(&a, &b);

        // Compare with scalar implementation
        let dist_scalar: f32 = a
            .iter()
            .zip(b.iter())
            .map(|(x, y)| (x - y).powi(2))
            .sum::<f32>()
            .sqrt();

        assert!(
            (dist_simd - dist_scalar).abs() < 1e-4,
            "SIMD {} vs Scalar {}",
            dist_simd,
            dist_scalar
        );
    }
}
