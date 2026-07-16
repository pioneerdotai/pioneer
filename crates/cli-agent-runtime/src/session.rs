//! CLI agent runtime session primitives.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CLIAgentProcessGeneration(u64);

impl CLIAgentProcessGeneration {
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug)]
pub struct CLIAgentProcessGenerationAllocator {
    next: AtomicU64,
}

impl Default for CLIAgentProcessGenerationAllocator {
    fn default() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }
}

impl CLIAgentProcessGenerationAllocator {
    pub fn allocate(&self) -> Option<CLIAgentProcessGeneration> {
        self.next
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                (current != u64::MAX).then_some(current + 1)
            })
            .ok()
            .map(CLIAgentProcessGeneration)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_generation_is_monotonic_and_never_reused() {
        let allocator = CLIAgentProcessGenerationAllocator::default();
        let first = allocator.allocate().unwrap();
        let second = allocator.allocate().unwrap();
        let third = allocator.allocate().unwrap();
        assert_eq!((first.get(), second.get(), third.get()), (1, 2, 3));
    }
}
