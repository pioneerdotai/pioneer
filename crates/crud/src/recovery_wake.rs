use std::sync::Arc;
use tokio::sync::Notify;

/// A retained hint for the existing recovery_job consumer. The guard is
/// created before DB admission and dropped after the operation releases its
/// transaction/capacity, also on cancellation or an unknown commit outcome.
/// It does not acknowledge work or replace the durable fallback poll.
pub(crate) struct RecoveryChange(pub(crate) Arc<Notify>);
impl Drop for RecoveryChange {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}
