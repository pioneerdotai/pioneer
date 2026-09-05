//! Accounting and retention contracts for explicitly approved Client caches.
//!
//! Implementations remain capability-owned. This module neither creates a
//! cache nor grants retention beyond the limits supplied by that capability.

use std::time::{Duration, SystemTime};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientCacheAccounting {
    entries: usize,
    bytes: u64,
}

impl ClientCacheAccounting {
    pub const fn new(entries: usize, bytes: u64) -> Self {
        Self { entries, bytes }
    }

    pub const fn entries(self) -> usize {
        self.entries
    }

    pub const fn bytes(self) -> u64 {
        self.bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientCacheRetentionLimits {
    maximum_entries: usize,
    maximum_bytes: u64,
    maximum_age: Duration,
}

impl ClientCacheRetentionLimits {
    pub const fn new(
        maximum_entries: usize,
        maximum_bytes: u64,
        maximum_age: Duration,
    ) -> Option<Self> {
        if maximum_entries == 0 || maximum_bytes == 0 || maximum_age.is_zero() {
            return None;
        }
        Some(Self {
            maximum_entries,
            maximum_bytes,
            maximum_age,
        })
    }

    pub const fn maximum_entries(self) -> usize {
        self.maximum_entries
    }

    pub const fn maximum_bytes(self) -> u64 {
        self.maximum_bytes
    }

    pub const fn maximum_age(self) -> Duration {
        self.maximum_age
    }
}

pub trait ClientCacheRetention {
    fn limits(&self) -> ClientCacheRetentionLimits;
    fn accounting(&self) -> ClientCacheAccounting;
    fn enforce_retention(&mut self, now: SystemTime) -> ClientCacheAccounting;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_limits_refuse_indefinite_dimensions() {
        assert!(ClientCacheRetentionLimits::new(0, 1, Duration::from_secs(1)).is_none());
        assert!(ClientCacheRetentionLimits::new(1, 0, Duration::from_secs(1)).is_none());
        assert!(ClientCacheRetentionLimits::new(1, 1, Duration::ZERO).is_none());
        assert!(ClientCacheRetentionLimits::new(1, 1, Duration::from_secs(1)).is_some());
    }
}
