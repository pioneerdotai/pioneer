use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Duration, timeout};

pub(crate) const TURN_FILESYSTEM_MAX_BYTES: u64 = 256 * 1024 * 1024;
pub(crate) const TURN_FILESYSTEM_MAX_OPERATIONS: u32 = 4_096;
const TURN_FILESYSTEM_MAX_CONCURRENCY: usize = 8;
const TURN_FILESYSTEM_QUEUE_TIMEOUT: Duration = Duration::from_secs(30);

/// A cumulative per-Turn admission budget for native filesystem operations.
/// The byte and operation counters are intentionally not released when a
/// permit is dropped: a Turn must not obtain the same I/O budget again merely
/// by completing an earlier operation.  Only the concurrency slot is released.
#[derive(Debug)]
pub(crate) struct TurnResourceBudget {
    bytes_admitted: AtomicU64,
    operations_admitted: AtomicU32,
    concurrency: Arc<Semaphore>,
}

impl Default for TurnResourceBudget {
    fn default() -> Self {
        Self {
            bytes_admitted: AtomicU64::new(0),
            operations_admitted: AtomicU32::new(0),
            concurrency: Arc::new(Semaphore::new(TURN_FILESYSTEM_MAX_CONCURRENCY)),
        }
    }
}

impl TurnResourceBudget {
    pub(crate) fn exhausted() -> Self {
        Self {
            bytes_admitted: AtomicU64::new(TURN_FILESYSTEM_MAX_BYTES),
            operations_admitted: AtomicU32::new(TURN_FILESYSTEM_MAX_OPERATIONS),
            concurrency: Arc::new(Semaphore::new(TURN_FILESYSTEM_MAX_CONCURRENCY)),
        }
    }

    pub(crate) async fn acquire(
        self: &Arc<Self>,
        operation: &str,
        requested_bytes: u64,
    ) -> Result<TurnResourceBudgetPermit, String> {
        let semaphore = self.concurrency.clone();
        let permit = timeout(
            TURN_FILESYSTEM_QUEUE_TIMEOUT,
            semaphore.acquire_owned(),
        )
        .await
        .map_err(|_| {
            format!(
                "FILESYSTEM_IO_CONCURRENCY_TIMEOUT: operation `{operation}` waited more than {}s for a Turn filesystem slot",
                TURN_FILESYSTEM_QUEUE_TIMEOUT.as_secs()
            )
        })?
        .map_err(|_| "FILESYSTEM_IO_BUDGET_CLOSED: Turn filesystem budget is closed".to_owned())?;

        let operations =
            self.operations_admitted
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                    (used < TURN_FILESYSTEM_MAX_OPERATIONS).then_some(used.saturating_add(1))
                });
        if operations.is_err() {
            drop(permit);
            return Err(format!(
                "FILESYSTEM_IO_OPERATION_BUDGET_EXCEEDED: operation `{operation}` exceeds the per-Turn operation budget of {TURN_FILESYSTEM_MAX_OPERATIONS}"
            ));
        }

        let bytes = self
            .bytes_admitted
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(requested_bytes)
                    .filter(|next| *next <= TURN_FILESYSTEM_MAX_BYTES)
            });
        if bytes.is_err() {
            self.operations_admitted.fetch_sub(1, Ordering::AcqRel);
            drop(permit);
            let used = self.bytes_admitted.load(Ordering::Acquire);
            return Err(format!(
                "FILESYSTEM_IO_BYTE_BUDGET_EXCEEDED: operation `{operation}` requests {requested_bytes} bytes with {used} already admitted; per-Turn limit is {TURN_FILESYSTEM_MAX_BYTES} bytes"
            ));
        }

        Ok(TurnResourceBudgetPermit { _permit: permit })
    }
}

#[derive(Debug)]
pub(crate) struct TurnResourceBudgetPermit {
    _permit: OwnedSemaphorePermit,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cumulative_byte_admission_is_not_released_when_operation_finishes() {
        let budget = Arc::new(TurnResourceBudget::default());
        let permit = budget
            .acquire("first", TURN_FILESYSTEM_MAX_BYTES)
            .await
            .expect("the first operation should fit");
        drop(permit);

        let error = budget
            .acquire("second", 1)
            .await
            .expect_err("completed I/O must not replenish the Turn byte budget");
        assert!(error.contains("FILESYSTEM_IO_BYTE_BUDGET_EXCEEDED"));
    }

    #[tokio::test]
    async fn failed_byte_admission_rolls_back_operation_counter_and_slot() {
        let budget = Arc::new(TurnResourceBudget::default());
        let error = budget
            .acquire("oversized", TURN_FILESYSTEM_MAX_BYTES.saturating_add(1))
            .await
            .expect_err("an operation larger than the Turn budget must be rejected");
        assert!(error.contains("FILESYSTEM_IO_BYTE_BUDGET_EXCEEDED"));

        let permit = budget
            .acquire("retry-within-budget", 1)
            .await
            .expect("a rejected admission must not consume the operation slot");
        drop(permit);
    }
}
