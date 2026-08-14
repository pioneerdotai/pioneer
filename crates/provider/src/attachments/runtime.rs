use crate::attachments::errors::AttachmentPipelineError;
use crate::attachments::observability;
use crate::attachments::types::AttachmentRuntimePolicy;
use anyhow::{Result, anyhow};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const MAX_CIRCUIT_BREAKERS: usize = 1_024;
const CIRCUIT_BREAKER_IDLE_TTL: Duration = Duration::from_secs(5 * 60);

tokio::task_local! {
    static ASYNC_AUTHORITY_FINGERPRINT: String;
}

thread_local! {
    static BLOCKING_AUTHORITY_FINGERPRINT: std::cell::RefCell<Option<String>> = const {
        std::cell::RefCell::new(None)
    };
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AttachmentOperationAuthority {
    authority_fingerprint: String,
    operation_kind: String,
    endpoint_fingerprint: String,
}

impl AttachmentOperationAuthority {
    pub fn new(
        authority_fingerprint: impl Into<String>,
        operation_kind: impl Into<String>,
        canonical_endpoint_identity: impl AsRef<str>,
    ) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"pioneer-attachment-endpoint-v1");
        digest.update([0]);
        digest.update(canonical_endpoint_identity.as_ref().as_bytes());
        Self {
            authority_fingerprint: authority_fingerprint.into(),
            operation_kind: operation_kind.into(),
            endpoint_fingerprint: hex::encode(digest.finalize()),
        }
    }

    fn jitter_identity(&self) -> String {
        format!(
            "{}:{}:{}",
            self.authority_fingerprint, self.operation_kind, self.endpoint_fingerprint
        )
    }
}

pub(crate) async fn with_async_authority_scope<T>(
    authority_fingerprint: String,
    future: impl Future<Output = T>,
) -> T {
    ASYNC_AUTHORITY_FINGERPRINT
        .scope(authority_fingerprint, future)
        .await
}

pub(crate) fn current_authority_fingerprint() -> Result<String> {
    if let Ok(fingerprint) = ASYNC_AUTHORITY_FINGERPRINT.try_with(Clone::clone) {
        return Ok(fingerprint);
    }
    if let Some(fingerprint) = BLOCKING_AUTHORITY_FINGERPRINT.with(|value| value.borrow().clone()) {
        return Ok(fingerprint);
    }
    Err(AttachmentPipelineError::contract_violation(
        "attachment operation has no provider authority fingerprint",
    )
    .into())
}

pub(crate) fn with_blocking_authority_scope<T>(
    authority_fingerprint: String,
    operation: impl FnOnce() -> T,
) -> T {
    struct RestoreAuthority(Option<String>);
    impl Drop for RestoreAuthority {
        fn drop(&mut self) {
            BLOCKING_AUTHORITY_FINGERPRINT.with(|slot| {
                slot.replace(self.0.take());
            });
        }
    }

    let previous =
        BLOCKING_AUTHORITY_FINGERPRINT.with(|slot| slot.replace(Some(authority_fingerprint)));
    let _restore = RestoreAuthority(previous);
    operation()
}

#[derive(Debug)]
pub enum AttachmentOperationError {
    Retryable(anyhow::Error),
    NonRetryable(anyhow::Error),
}

impl AttachmentOperationError {
    pub fn retryable(error: impl Into<anyhow::Error>) -> Self {
        Self::Retryable(error.into())
    }

    pub fn non_retryable(error: impl Into<anyhow::Error>) -> Self {
        Self::NonRetryable(error.into())
    }

    fn into_error(self) -> anyhow::Error {
        match self {
            Self::Retryable(error) | Self::NonRetryable(error) => error,
        }
    }

    fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_))
    }

    fn message(&self) -> String {
        match self {
            Self::Retryable(error) | Self::NonRetryable(error) => error.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
struct CircuitState {
    consecutive_failures: u32,
    open_until: Option<Instant>,
    last_touched: Instant,
}

impl Default for CircuitState {
    fn default() -> Self {
        Self {
            consecutive_failures: 0,
            open_until: None,
            last_touched: Instant::now(),
        }
    }
}

static CIRCUIT_BREAKERS: OnceLock<Mutex<HashMap<AttachmentOperationAuthority, CircuitState>>> =
    OnceLock::new();

fn breakers() -> &'static Mutex<HashMap<AttachmentOperationAuthority, CircuitState>> {
    CIRCUIT_BREAKERS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn stable_jitter_seed(operation_key: &str, attempt: usize) -> u64 {
    let mut acc: u64 = 1469598103934665603;
    for byte in operation_key.as_bytes() {
        acc ^= u64::from(*byte);
        acc = acc.wrapping_mul(1099511628211);
    }
    acc ^= attempt as u64;
    acc
}

fn next_delay(policy: &AttachmentRuntimePolicy, operation_key: &str, attempt: usize) -> Duration {
    let base = policy.retry.initial_backoff_ms;
    let capped_pow = 2u64.saturating_pow(attempt.saturating_sub(1) as u32);
    let mut backoff = base.saturating_mul(capped_pow);
    backoff = backoff.min(policy.retry.max_backoff_ms.max(base));

    let jitter = policy.retry.jitter_ms;
    if jitter == 0 {
        return Duration::from_millis(backoff);
    }

    let range = jitter.saturating_mul(2).saturating_add(1);
    let offset = (stable_jitter_seed(operation_key, attempt) % range) as i64 - jitter as i64;
    let adjusted = (backoff as i64 + offset).max(0) as u64;
    Duration::from_millis(adjusted)
}

fn prune_breakers(map: &mut HashMap<AttachmentOperationAuthority, CircuitState>, now: Instant) {
    map.retain(|_, state| {
        state.open_until.is_some_and(|until| until > now)
            || now.saturating_duration_since(state.last_touched) <= CIRCUIT_BREAKER_IDLE_TTL
    });
}

fn evict_oldest_if_full(map: &mut HashMap<AttachmentOperationAuthority, CircuitState>) {
    if map.len() < MAX_CIRCUIT_BREAKERS {
        return;
    }
    if let Some(oldest) = map
        .iter()
        .min_by_key(|(_, state)| state.last_touched)
        .map(|(key, _)| key.clone())
    {
        map.remove(&oldest);
    }
}

fn circuit_open_for(operation_key: &AttachmentOperationAuthority) -> Option<Duration> {
    let mut map = breakers().lock().expect("circuit breaker lock poisoned");
    let now = Instant::now();
    prune_breakers(&mut map, now);
    let state = map.get_mut(operation_key)?;
    state.last_touched = now;
    let Some(open_until) = state.open_until else {
        return None;
    };
    if open_until <= now {
        map.remove(operation_key);
        return None;
    }
    Some(open_until.saturating_duration_since(now))
}

fn record_success(operation_key: &AttachmentOperationAuthority) {
    let mut map = breakers().lock().expect("circuit breaker lock poisoned");
    map.remove(operation_key);
}

fn record_failure(operation_key: &AttachmentOperationAuthority, policy: &AttachmentRuntimePolicy) {
    let mut map = breakers().lock().expect("circuit breaker lock poisoned");
    let now = Instant::now();
    prune_breakers(&mut map, now);
    if !map.contains_key(operation_key) {
        evict_oldest_if_full(&mut map);
    }
    let state = map.entry(operation_key.clone()).or_default();
    state.last_touched = now;
    state.consecutive_failures = state.consecutive_failures.saturating_add(1);
    if state.consecutive_failures >= policy.circuit_breaker.failure_threshold {
        state.open_until = Some(now + Duration::from_millis(policy.circuit_breaker.open_ms.max(1)));
        state.consecutive_failures = 0;
    }
}

pub fn execute_with_retry_blocking<T, F>(
    provider: &str,
    operation: &str,
    operation_key: &AttachmentOperationAuthority,
    policy: &AttachmentRuntimePolicy,
    mut attempt_fn: F,
) -> Result<T>
where
    F: FnMut(usize) -> std::result::Result<T, AttachmentOperationError>,
{
    if let Some(remaining) = circuit_open_for(operation_key) {
        return Err(AttachmentPipelineError::circuit_breaker_open(
            operation,
            remaining.as_millis() as u64,
        )
        .into());
    }

    let max_attempts = policy.retry.max_attempts.max(1);
    let jitter_identity = operation_key.jitter_identity();
    let mut last_error: Option<anyhow::Error> = None;

    for attempt in 1..=max_attempts {
        match attempt_fn(attempt) {
            Ok(result) => {
                record_success(operation_key);
                return Ok(result);
            }
            Err(error) if error.is_retryable() && attempt < max_attempts => {
                let delay = next_delay(policy, jitter_identity.as_str(), attempt);
                observability::emit_upload_retry(provider, operation, attempt, delay);
                std::thread::sleep(delay);
                last_error = Some(error.into_error());
            }
            Err(error) => {
                let reason = error.message();
                record_failure(operation_key, policy);
                observability::emit_upload_fail(
                    provider,
                    operation,
                    attempt,
                    "ATTACHMENT_OPERATION_FAILED",
                    reason.as_str(),
                );
                return Err(error.into_error());
            }
        }
    }

    let last = last_error.unwrap_or_else(|| anyhow!("attachment operation failed"));
    record_failure(operation_key, policy);
    Err(AttachmentPipelineError::retry_exhausted(
        operation,
        max_attempts,
        last.to_string().as_str(),
    )
    .into())
}

pub async fn execute_with_retry_async<T, F, Fut>(
    provider: &str,
    operation: &str,
    operation_key: &AttachmentOperationAuthority,
    policy: &AttachmentRuntimePolicy,
    mut attempt_fn: F,
) -> Result<T>
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, AttachmentOperationError>>,
{
    if let Some(remaining) = circuit_open_for(operation_key) {
        return Err(AttachmentPipelineError::circuit_breaker_open(
            operation,
            remaining.as_millis() as u64,
        )
        .into());
    }

    let max_attempts = policy.retry.max_attempts.max(1);
    let jitter_identity = operation_key.jitter_identity();
    let mut last_error: Option<anyhow::Error> = None;

    for attempt in 1..=max_attempts {
        match attempt_fn(attempt).await {
            Ok(result) => {
                record_success(operation_key);
                return Ok(result);
            }
            Err(error) if error.is_retryable() && attempt < max_attempts => {
                let delay = next_delay(policy, jitter_identity.as_str(), attempt);
                observability::emit_upload_retry(provider, operation, attempt, delay);
                tokio::time::sleep(delay).await;
                last_error = Some(error.into_error());
            }
            Err(error) => {
                let reason = error.message();
                record_failure(operation_key, policy);
                observability::emit_upload_fail(
                    provider,
                    operation,
                    attempt,
                    "ATTACHMENT_OPERATION_FAILED",
                    reason.as_str(),
                );
                return Err(error.into_error());
            }
        }
    }

    let last = last_error.unwrap_or_else(|| anyhow!("attachment operation failed"));
    record_failure(operation_key, policy);
    Err(AttachmentPipelineError::retry_exhausted(
        operation,
        max_attempts,
        last.to_string().as_str(),
    )
    .into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn test_policy() -> AttachmentRuntimePolicy {
        AttachmentRuntimePolicy {
            retry: crate::attachments::types::AttachmentRetryPolicy {
                max_attempts: 3,
                initial_backoff_ms: 0,
                max_backoff_ms: 0,
                jitter_ms: 0,
            },
            circuit_breaker: crate::attachments::types::AttachmentCircuitBreakerPolicy {
                failure_threshold: 2,
                open_ms: 500,
            },
        }
    }

    #[test]
    fn blocking_retry_retries_then_succeeds() {
        let policy = test_policy();
        let attempts = AtomicUsize::new(0);
        let operation_authority = AttachmentOperationAuthority::new(
            "runtime-tests",
            "blocking_retry",
            "retry-retries-then-succeeds",
        );
        let value =
            execute_with_retry_blocking("test", "op", &operation_authority, &policy, |_| {
                let current = attempts.fetch_add(1, Ordering::SeqCst);
                if current < 2 {
                    Err(AttachmentOperationError::retryable(anyhow!("transient")))
                } else {
                    Ok("ok")
                }
            })
            .expect("retry flow should succeed");
        assert_eq!(value, "ok");
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn blocking_retry_non_retryable_fails_fast() {
        let policy = test_policy();
        let attempts = AtomicUsize::new(0);
        let operation_authority = AttachmentOperationAuthority::new(
            "runtime-tests",
            "blocking_retry",
            "non-retryable-fails-fast",
        );
        let err = execute_with_retry_blocking::<(), _>(
            "test",
            "op",
            &operation_authority,
            &policy,
            |_| {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(AttachmentOperationError::non_retryable(anyhow!("fatal")))
            },
        )
        .expect_err("non-retryable error must fail");
        assert!(err.to_string().contains("fatal"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn circuit_breaker_opens_after_threshold() {
        let policy = AttachmentRuntimePolicy {
            retry: crate::attachments::types::AttachmentRetryPolicy {
                max_attempts: 1,
                initial_backoff_ms: 0,
                max_backoff_ms: 0,
                jitter_ms: 0,
            },
            circuit_breaker: crate::attachments::types::AttachmentCircuitBreakerPolicy {
                failure_threshold: 1,
                open_ms: 500,
            },
        };

        let key = AttachmentOperationAuthority::new(
            "runtime-tests",
            "circuit_breaker",
            "opens-after-threshold",
        );
        let _ = execute_with_retry_blocking::<(), _>("test", "op", &key, &policy, |_| {
            Err(AttachmentOperationError::non_retryable(anyhow!("boom")))
        });
        let err = execute_with_retry_blocking::<(), _>("test", "op", &key, &policy, |_| Ok(()))
            .expect_err("circuit breaker should block operation");
        assert!(err.to_string().contains("ATTACHMENT_CIRCUIT_BREAKER_OPEN"));
    }
}
