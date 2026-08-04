use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use pioneer_config::GatewayArtifactsConfig;
use pioneer_protocol::{AccessChangeKind, AuthSessionId, AuthSessionTerminationReason};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Sleep;
use tokio_util::sync::CancellationToken;

use crate::auth::AuthSessionDisconnectHook;
use crate::authorization::AccessChangeSignal;

const MAX_TINY_RANGE_TRACKED_SESSIONS: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamAdmissionError {
    GlobalCapacity,
    PrincipalCapacity,
    SessionCapacity,
    RangeTooLarge,
    TinyRangeRate,
    ShuttingDown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct HttpStreamConfigError;

#[derive(Debug, Clone, Copy)]
pub(super) struct HttpStreamLimits {
    global_streams: usize,
    per_principal_streams: usize,
    per_session_streams: usize,
    open_handles: usize,
    max_single_range_bytes: u64,
    tiny_range_bytes: u64,
    tiny_range_window: Duration,
    tiny_range_max_requests: usize,
    open_timeout: Duration,
    body_idle_timeout: Duration,
}

impl HttpStreamLimits {
    fn from_config(config: &GatewayArtifactsConfig) -> Result<Self, HttpStreamConfigError> {
        if config.http_streams_global == 0
            || config.http_streams_global > Semaphore::MAX_PERMITS
            || config.http_streams_per_principal == 0
            || config.http_streams_per_principal > config.http_streams_global
            || config.http_streams_per_session == 0
            || config.http_streams_per_session > config.http_streams_per_principal
            || config.http_open_handles == 0
            || config.http_open_handles > Semaphore::MAX_PERMITS
            || config.http_max_single_range_bytes == 0
            || config.http_tiny_range_bytes == 0
            || config.http_tiny_range_bytes > config.http_max_single_range_bytes
            || config.http_tiny_range_window_secs == 0
            || config.http_tiny_range_max_requests == 0
            || config.http_open_timeout_secs == 0
            || config.http_body_idle_timeout_secs == 0
        {
            return Err(HttpStreamConfigError);
        }
        Ok(Self {
            global_streams: config.http_streams_global,
            per_principal_streams: config.http_streams_per_principal,
            per_session_streams: config.http_streams_per_session,
            open_handles: config.http_open_handles,
            max_single_range_bytes: config.http_max_single_range_bytes,
            tiny_range_bytes: config.http_tiny_range_bytes,
            tiny_range_window: Duration::from_secs(config.http_tiny_range_window_secs),
            tiny_range_max_requests: config.http_tiny_range_max_requests,
            open_timeout: Duration::from_secs(config.http_open_timeout_secs),
            body_idle_timeout: Duration::from_secs(config.http_body_idle_timeout_secs),
        })
    }

    pub(super) const fn open_timeout(self) -> Duration {
        self.open_timeout
    }

    pub(super) const fn body_idle_timeout(self) -> Duration {
        self.body_idle_timeout
    }
}

#[derive(Debug, Clone)]
struct ActiveStream {
    session_id: String,
    principal_id: String,
    workspace_id: String,
    artifact_id: String,
    cancellation: CancellationToken,
}

#[derive(Debug, Clone, Copy)]
struct TinyRangeWindow {
    started_at: Instant,
    requests: usize,
}

struct RegistryState {
    accepting: bool,
    next_id: u64,
    active: HashMap<u64, ActiveStream>,
    per_principal: HashMap<String, usize>,
    per_session: HashMap<String, usize>,
    tiny_ranges: HashMap<String, TinyRangeWindow>,
}

impl Default for RegistryState {
    fn default() -> Self {
        Self {
            accepting: true,
            next_id: 0,
            active: HashMap::new(),
            per_principal: HashMap::new(),
            per_session: HashMap::new(),
            tiny_ranges: HashMap::new(),
        }
    }
}

#[derive(Debug, Default)]
struct StreamMetrics {
    completed: AtomicU64,
    cancelled: AtomicU64,
    idle_timeout: AtomicU64,
    failed: AtomicU64,
    abandoned: AtomicU64,
}

pub(crate) struct HttpStreamRegistry {
    limits: HttpStreamLimits,
    global: Arc<Semaphore>,
    open_handles: Arc<Semaphore>,
    state: Mutex<RegistryState>,
    metrics: StreamMetrics,
}

impl std::fmt::Debug for HttpStreamRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpStreamRegistry")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl HttpStreamRegistry {
    pub(super) fn new(config: &GatewayArtifactsConfig) -> Result<Arc<Self>, HttpStreamConfigError> {
        let limits = HttpStreamLimits::from_config(config)?;
        Ok(Arc::new(Self {
            limits,
            global: Arc::new(Semaphore::new(limits.global_streams)),
            open_handles: Arc::new(Semaphore::new(limits.open_handles)),
            state: Mutex::new(RegistryState::default()),
            metrics: StreamMetrics::default(),
        }))
    }

    pub(super) const fn limits(&self) -> HttpStreamLimits {
        self.limits
    }

    pub(super) fn admit_range(
        &self,
        session_id: &AuthSessionId,
        length: u64,
    ) -> Result<(), StreamAdmissionError> {
        if length > self.limits.max_single_range_bytes {
            return Err(StreamAdmissionError::RangeTooLarge);
        }
        let mut state = self.lock_state();
        if !state.accepting {
            return Err(StreamAdmissionError::ShuttingDown);
        }
        if length >= self.limits.tiny_range_bytes {
            return Ok(());
        }

        let now = Instant::now();
        state.tiny_ranges.retain(|_, window| {
            now.duration_since(window.started_at) < self.limits.tiny_range_window
        });
        let session_key = session_id.as_str();
        if !state.tiny_ranges.contains_key(session_key)
            && state.tiny_ranges.len() >= MAX_TINY_RANGE_TRACKED_SESSIONS
        {
            return Err(StreamAdmissionError::TinyRangeRate);
        }
        let window = state
            .tiny_ranges
            .entry(session_key.to_owned())
            .or_insert(TinyRangeWindow {
                started_at: now,
                requests: 0,
            });
        if now.duration_since(window.started_at) >= self.limits.tiny_range_window {
            *window = TinyRangeWindow {
                started_at: now,
                requests: 0,
            };
        }
        if window.requests >= self.limits.tiny_range_max_requests {
            return Err(StreamAdmissionError::TinyRangeRate);
        }
        window.requests += 1;
        Ok(())
    }

    pub(super) fn acquire(
        self: &Arc<Self>,
        session_id: &AuthSessionId,
        principal_id: &str,
        workspace_id: &str,
        artifact_id: &str,
    ) -> Result<HttpStreamLease, StreamAdmissionError> {
        let global_permit = self
            .global
            .clone()
            .try_acquire_owned()
            .map_err(|_| StreamAdmissionError::GlobalCapacity)?;
        let open_handle_permit = self
            .open_handles
            .clone()
            .try_acquire_owned()
            .map_err(|_| StreamAdmissionError::GlobalCapacity)?;

        let mut state = self.lock_state();
        if !state.accepting {
            return Err(StreamAdmissionError::ShuttingDown);
        }
        let principal_key = principal_id.to_owned();
        if state
            .per_principal
            .get(&principal_key)
            .copied()
            .unwrap_or(0)
            >= self.limits.per_principal_streams
        {
            return Err(StreamAdmissionError::PrincipalCapacity);
        }
        let session_key = session_id.as_str().to_owned();
        if state.per_session.get(&session_key).copied().unwrap_or(0)
            >= self.limits.per_session_streams
        {
            return Err(StreamAdmissionError::SessionCapacity);
        }
        state.next_id = state
            .next_id
            .checked_add(1)
            .ok_or(StreamAdmissionError::GlobalCapacity)?;
        let stream_id = state.next_id;
        let cancellation = CancellationToken::new();
        state.active.insert(
            stream_id,
            ActiveStream {
                session_id: session_key.clone(),
                principal_id: principal_key.clone(),
                workspace_id: workspace_id.to_owned(),
                artifact_id: artifact_id.to_owned(),
                cancellation: cancellation.clone(),
            },
        );
        *state.per_principal.entry(principal_key).or_insert(0) += 1;
        *state.per_session.entry(session_key).or_insert(0) += 1;
        drop(state);

        Ok(HttpStreamLease {
            stream_id,
            registry: Arc::downgrade(self),
            cancellation: cancellation.clone(),
            cancellation_wait: Box::pin(cancellation.cancelled_owned()),
            global_permit: Some(global_permit),
            open_handle_permit: Some(open_handle_permit),
            started_at: Instant::now(),
            bytes: 0,
            outcome: StreamOutcome::Abandoned,
        })
    }

    pub(crate) fn cancel_session(&self, session_id: &AuthSessionId) -> usize {
        self.cancel_matching(|stream| stream.session_id == session_id.as_str())
    }

    pub(crate) fn cancel_authorization_signal(&self, signal: &AccessChangeSignal) -> usize {
        if !matches!(
            signal.kind,
            AccessChangeKind::WorkspaceMembership
                | AccessChangeKind::ThreadVisibility
                | AccessChangeKind::ThreadParticipantRemoved
        ) {
            return 0;
        }
        self.cancel_matching(|stream| {
            stream.workspace_id == signal.workspace_id
                && signal
                    .affected_principal_id
                    .as_ref()
                    .is_none_or(|principal_id| stream.principal_id == principal_id.as_str())
        })
    }

    pub(crate) fn cancel_artifact(&self, workspace_id: &str, artifact_id: &str) -> usize {
        self.cancel_matching(|stream| {
            stream.workspace_id == workspace_id && stream.artifact_id == artifact_id
        })
    }

    pub(crate) fn begin_shutdown(&self) -> usize {
        let cancellations = {
            let mut state = self.lock_state();
            state.accepting = false;
            state
                .active
                .values()
                .map(|stream| stream.cancellation.clone())
                .collect::<Vec<_>>()
        };
        for cancellation in &cancellations {
            cancellation.cancel();
        }
        cancellations.len()
    }

    fn cancel_matching(&self, predicate: impl Fn(&ActiveStream) -> bool) -> usize {
        let cancellations = self
            .lock_state()
            .active
            .values()
            .filter(|stream| predicate(stream))
            .map(|stream| stream.cancellation.clone())
            .collect::<Vec<_>>();
        for cancellation in &cancellations {
            cancellation.cancel();
        }
        cancellations.len()
    }

    fn release(&self, stream_id: u64, bytes: u64, duration: Duration, outcome: StreamOutcome) {
        let mut state = self.lock_state();
        let Some(stream) = state.active.remove(&stream_id) else {
            return;
        };
        if let Some(active) = state.per_principal.get_mut(&stream.principal_id) {
            *active = active.saturating_sub(1);
            if *active == 0 {
                state.per_principal.remove(&stream.principal_id);
            }
        }
        if let Some(active) = state.per_session.get_mut(&stream.session_id) {
            *active = active.saturating_sub(1);
            if *active == 0 {
                state.per_session.remove(&stream.session_id);
            }
        }
        drop(state);

        (match outcome {
            StreamOutcome::Completed => &self.metrics.completed,
            StreamOutcome::Cancelled => &self.metrics.cancelled,
            StreamOutcome::IdleTimeout => &self.metrics.idle_timeout,
            StreamOutcome::Failed => &self.metrics.failed,
            StreamOutcome::Abandoned => &self.metrics.abandoned,
        })
        .fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            event = "http_storage_stream_finished",
            outcome = outcome.safe_name(),
            bytes_sent = bytes,
            reason_code = duration_bucket(duration),
        );
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, RegistryState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(test)]
    pub(super) fn active_count(&self) -> usize {
        self.lock_state().active.len()
    }

    #[cfg(test)]
    pub(super) fn metrics_observation(&self) -> (u64, u64, u64, u64, u64) {
        (
            self.metrics.completed.load(Ordering::Relaxed),
            self.metrics.cancelled.load(Ordering::Relaxed),
            self.metrics.idle_timeout.load(Ordering::Relaxed),
            self.metrics.failed.load(Ordering::Relaxed),
            self.metrics.abandoned.load(Ordering::Relaxed),
        )
    }
}

#[async_trait]
impl AuthSessionDisconnectHook for HttpStreamRegistry {
    async fn disconnect_session(
        &self,
        session_id: &AuthSessionId,
        _reason: AuthSessionTerminationReason,
    ) {
        self.cancel_session(session_id);
    }
}

impl crate::message::ArtifactStreamInvalidation for HttpStreamRegistry {
    fn cancel_artifact(&self, workspace_id: &str, artifact_id: &str) -> usize {
        HttpStreamRegistry::cancel_artifact(self, workspace_id, artifact_id)
    }

    fn cancel_authorization_signal(&self, signal: &AccessChangeSignal) -> usize {
        HttpStreamRegistry::cancel_authorization_signal(self, signal)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamOutcome {
    Completed,
    Cancelled,
    IdleTimeout,
    Failed,
    Abandoned,
}

impl StreamOutcome {
    const fn safe_name(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::IdleTimeout => "idle_timeout",
            Self::Failed => "failed",
            Self::Abandoned => "abandoned",
        }
    }
}

pub(super) struct HttpStreamLease {
    stream_id: u64,
    registry: Weak<HttpStreamRegistry>,
    cancellation: CancellationToken,
    cancellation_wait: Pin<Box<dyn Future<Output = ()> + Send>>,
    global_permit: Option<OwnedSemaphorePermit>,
    open_handle_permit: Option<OwnedSemaphorePermit>,
    started_at: Instant,
    bytes: u64,
    outcome: StreamOutcome,
}

impl std::fmt::Debug for HttpStreamLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpStreamLease")
            .field("bytes", &self.bytes)
            .field("outcome", &self.outcome)
            .finish()
    }
}

impl HttpStreamLease {
    fn cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    fn record_bytes(&mut self, bytes: usize) {
        self.bytes = self.bytes.saturating_add(bytes as u64);
    }

    fn set_outcome(&mut self, outcome: StreamOutcome) {
        self.outcome = outcome;
    }
}

impl Drop for HttpStreamLease {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            let outcome =
                if self.outcome == StreamOutcome::Abandoned && self.cancellation.is_cancelled() {
                    StreamOutcome::Cancelled
                } else {
                    self.outcome
                };
            registry.release(
                self.stream_id,
                self.bytes,
                self.started_at.elapsed(),
                outcome,
            );
        }
        self.open_handle_permit.take();
        self.global_permit.take();
    }
}

#[derive(Debug)]
pub(super) struct ManagedArtifactReader<R> {
    reader: R,
    remaining: u64,
    lease: HttpStreamLease,
    idle_timeout: Duration,
    idle_sleep: Pin<Box<Sleep>>,
}

impl<R> ManagedArtifactReader<R> {
    pub(super) fn new(
        reader: R,
        length: u64,
        lease: HttpStreamLease,
        idle_timeout: Duration,
    ) -> Self {
        Self {
            reader,
            remaining: length,
            lease,
            idle_timeout,
            idle_sleep: Box::pin(tokio::time::sleep(idle_timeout)),
        }
    }

    pub(super) fn cancellation_token(&self) -> CancellationToken {
        self.lease.cancellation.clone()
    }

    pub(super) fn mark_cancelled(&mut self) {
        self.lease.set_outcome(StreamOutcome::Cancelled);
    }

    pub(super) fn mark_idle_timeout(&mut self) {
        self.lease.set_outcome(StreamOutcome::IdleTimeout);
    }
}

impl<R> AsyncRead for ManagedArtifactReader<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();
        if this.remaining == 0 {
            this.lease.set_outcome(StreamOutcome::Completed);
            return Poll::Ready(Ok(()));
        }
        if this.lease.cancelled() || this.lease.cancellation_wait.as_mut().poll(cx).is_ready() {
            this.lease.set_outcome(StreamOutcome::Cancelled);
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "artifact stream authority was revoked",
            )));
        }
        if this.idle_sleep.as_mut().poll(cx).is_ready() {
            this.lease.set_outcome(StreamOutcome::IdleTimeout);
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "artifact stream made no progress before idle timeout",
            )));
        }
        let maximum_read = usize::try_from(this.remaining)
            .unwrap_or(usize::MAX)
            .min(buffer.remaining());
        if maximum_read == 0 {
            return Poll::Ready(Ok(()));
        }
        let (result, bytes) = {
            let initialized = buffer.initialize_unfilled_to(maximum_read);
            let mut limited = ReadBuf::new(&mut initialized[..maximum_read]);
            let result = Pin::new(&mut this.reader).poll_read(cx, &mut limited);
            (result, limited.filled().len())
        };
        match result {
            Poll::Ready(Ok(())) => {
                if bytes == 0 {
                    this.lease.set_outcome(StreamOutcome::Failed);
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "artifact stream ended before the declared content length",
                    )));
                }
                buffer.advance(bytes);
                this.lease.record_bytes(bytes);
                this.remaining -= bytes as u64;
                this.idle_sleep
                    .as_mut()
                    .reset(tokio::time::Instant::now() + this.idle_timeout);
                if this.remaining == 0 {
                    this.lease.set_outcome(StreamOutcome::Completed);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => {
                this.lease.set_outcome(StreamOutcome::Failed);
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

fn duration_bucket(duration: Duration) -> &'static str {
    match duration.as_secs() {
        0 => "lt_1s",
        1..=9 => "lt_10s",
        10..=59 => "lt_1m",
        60..=599 => "lt_10m",
        _ => "gte_10m",
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;

    use super::*;

    #[derive(Debug)]
    struct RepeatedReader {
        remaining: u64,
        byte: u8,
        max_read: usize,
    }

    impl AsyncRead for RepeatedReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let read = self
                .remaining
                .min(buffer.remaining() as u64)
                .min(self.max_read as u64) as usize;
            if read > 0 {
                buffer.initialize_unfilled_to(read).fill(self.byte);
                buffer.advance(read);
                self.remaining -= read as u64;
            }
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Debug)]
    struct PendingReader;

    impl AsyncRead for PendingReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    #[derive(Debug)]
    struct FailingReader {
        remaining_success: usize,
    }

    impl AsyncRead for FailingReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.remaining_success == 0 {
                return Poll::Ready(Err(io::Error::other("injected storage fault")));
            }
            let read = self.remaining_success.min(buffer.remaining());
            buffer.initialize_unfilled_to(read).fill(0x61);
            buffer.advance(read);
            self.remaining_success -= read;
            Poll::Ready(Ok(()))
        }
    }

    fn config() -> GatewayArtifactsConfig {
        GatewayArtifactsConfig {
            http_streams_global: 2,
            http_streams_per_principal: 2,
            http_streams_per_session: 1,
            http_open_handles: 2,
            http_max_single_range_bytes: 64,
            http_tiny_range_bytes: 8,
            http_tiny_range_window_secs: 10,
            http_tiny_range_max_requests: 2,
            http_open_timeout_secs: 2,
            http_body_idle_timeout_secs: 2,
            ..GatewayArtifactsConfig::default()
        }
    }

    fn session(value: &str) -> AuthSessionId {
        AuthSessionId::new(value).unwrap()
    }

    #[test]
    fn invalid_stream_limits_are_rejected_instead_of_silently_clamped() {
        let mut invalid = config();
        invalid.http_streams_global = 0;
        assert_eq!(
            HttpStreamRegistry::new(&invalid).unwrap_err(),
            HttpStreamConfigError
        );

        let mut invalid = config();
        invalid.http_streams_per_principal = invalid.http_streams_global + 1;
        assert_eq!(
            HttpStreamRegistry::new(&invalid).unwrap_err(),
            HttpStreamConfigError
        );

        let mut invalid = config();
        invalid.http_streams_per_session = invalid.http_streams_global + 1;
        assert_eq!(
            HttpStreamRegistry::new(&invalid).unwrap_err(),
            HttpStreamConfigError
        );

        let mut invalid = config();
        invalid.http_tiny_range_bytes = invalid.http_max_single_range_bytes + 1;
        assert_eq!(
            HttpStreamRegistry::new(&invalid).unwrap_err(),
            HttpStreamConfigError
        );
    }

    #[test]
    fn shutdown_cancels_existing_streams_and_rejects_late_admission() {
        let registry = HttpStreamRegistry::new(&config()).unwrap();
        let session = session("S00000000000000000001");
        let existing = registry
            .acquire(&session, "principal-a", "workspace-a", "artifact-a")
            .unwrap();

        assert_eq!(registry.begin_shutdown(), 1);
        assert!(existing.cancelled());
        assert!(matches!(
            registry.acquire(&session, "principal-a", "workspace-a", "artifact-b"),
            Err(StreamAdmissionError::ShuttingDown)
        ));
        assert_eq!(
            registry.admit_range(&session, 8),
            Err(StreamAdmissionError::ShuttingDown)
        );
        drop(existing);
        assert_eq!(registry.active_count(), 0);
    }

    #[test]
    fn permits_are_bounded_targeted_and_released_exactly_once() {
        let registry = HttpStreamRegistry::new(&config()).unwrap();
        let first_session = session("S00000000000000000001");
        let second_session = session("S00000000000000000002");
        let first = registry
            .acquire(&first_session, "principal-a", "workspace-a", "artifact-a")
            .unwrap();
        assert!(matches!(
            registry.acquire(&first_session, "principal-a", "workspace-a", "artifact-b"),
            Err(StreamAdmissionError::SessionCapacity)
        ));
        let second = registry
            .acquire(&second_session, "principal-b", "workspace-b", "artifact-b")
            .unwrap();
        assert!(matches!(
            registry.acquire(
                &session("S00000000000000000003"),
                "principal-c",
                "workspace-c",
                "artifact-c"
            ),
            Err(StreamAdmissionError::GlobalCapacity)
        ));
        assert_eq!(registry.active_count(), 2);
        registry.cancel_artifact("workspace-a", "artifact-a");
        assert!(first.cancelled());
        assert!(!second.cancelled());
        drop(first);
        assert_eq!(registry.active_count(), 1);
        drop(second);
        assert_eq!(registry.active_count(), 0);
    }

    #[test]
    fn principal_capacity_spans_independent_auth_sessions() {
        let mut config = config();
        config.http_streams_global = 3;
        config.http_streams_per_principal = 2;
        config.http_streams_per_session = 1;
        config.http_open_handles = 3;
        let registry = HttpStreamRegistry::new(&config).unwrap();
        let first = registry
            .acquire(
                &session("S00000000000000000001"),
                "principal-a",
                "workspace-a",
                "artifact-a",
            )
            .unwrap();
        let second = registry
            .acquire(
                &session("S00000000000000000002"),
                "principal-a",
                "workspace-a",
                "artifact-b",
            )
            .unwrap();

        assert!(matches!(
            registry.acquire(
                &session("S00000000000000000003"),
                "principal-a",
                "workspace-a",
                "artifact-c",
            ),
            Err(StreamAdmissionError::PrincipalCapacity)
        ));
        drop(first);
        assert!(
            registry
                .acquire(
                    &session("S00000000000000000003"),
                    "principal-a",
                    "workspace-a",
                    "artifact-c",
                )
                .is_ok()
        );
        drop(second);
    }

    #[test]
    fn committed_access_change_cancels_only_matching_principal_and_workspace() {
        let registry = HttpStreamRegistry::new(&config()).unwrap();
        let first_session = session("S00000000000000000001");
        let second_session = session("S00000000000000000002");
        let first = registry
            .acquire(
                &first_session,
                "P00000000000000000001",
                "workspace-a",
                "artifact-a",
            )
            .unwrap();
        let second = registry
            .acquire(
                &second_session,
                "P00000000000000000002",
                "workspace-a",
                "artifact-b",
            )
            .unwrap();
        let signal = AccessChangeSignal {
            authorization_revision: 7,
            kind: AccessChangeKind::WorkspaceMembership,
            affected_principal_id: Some(
                pioneer_protocol::PrincipalId::new("P00000000000000000001").unwrap(),
            ),
            workspace_id: "workspace-a".to_owned(),
            thread_id: None,
        };
        assert_eq!(registry.cancel_authorization_signal(&signal), 1);
        assert!(first.cancelled());
        assert!(!second.cancelled());
        drop(first);
        drop(second);
        assert_eq!(registry.active_count(), 0);
    }

    #[test]
    fn range_size_and_tiny_range_policy_are_bounded() {
        let registry = HttpStreamRegistry::new(&config()).unwrap();
        let session = session("S00000000000000000001");
        assert_eq!(
            registry.admit_range(&session, 65),
            Err(StreamAdmissionError::RangeTooLarge)
        );
        assert!(registry.admit_range(&session, 1).is_ok());
        assert!(registry.admit_range(&session, 1).is_ok());
        assert_eq!(
            registry.admit_range(&session, 1),
            Err(StreamAdmissionError::TinyRangeRate)
        );
        assert!(registry.admit_range(&session, 8).is_ok());
    }

    #[tokio::test]
    async fn cancellation_and_repeated_drop_release_only_the_target_stream() {
        let registry = HttpStreamRegistry::new(&config()).unwrap();
        let first_session = session("S00000000000000000001");
        let second_session = session("S00000000000000000002");
        let first_lease = registry
            .acquire(&first_session, "principal-a", "workspace-a", "artifact-a")
            .unwrap();
        let second_lease = registry
            .acquire(&second_session, "principal-b", "workspace-b", "artifact-b")
            .unwrap();
        let mut first =
            ManagedArtifactReader::new(PendingReader, 1, first_lease, Duration::from_secs(60));
        let second =
            ManagedArtifactReader::new(PendingReader, 1, second_lease, Duration::from_secs(60));

        assert_eq!(registry.cancel_session(&first_session), 1);
        assert_eq!(registry.cancel_session(&first_session), 1);
        let error = first.read_u8().await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        drop(first);
        assert_eq!(registry.active_count(), 1);
        drop(second);
        assert_eq!(registry.active_count(), 0);
    }

    #[tokio::test]
    async fn idle_timeout_has_no_download_wall_clock_and_large_reader_is_constant_memory() {
        let registry = HttpStreamRegistry::new(&config()).unwrap();
        let session = session("S00000000000000000001");
        let idle_lease = registry
            .acquire(&session, "principal-a", "workspace-a", "artifact-a")
            .unwrap();
        let mut idle = ManagedArtifactReader::new(PendingReader, 1, idle_lease, Duration::ZERO);
        let error = idle.read_u8().await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        drop(idle);

        let logical_size = 8 * 1024_u64 * 1024 * 1024 * 1024;
        let lease = registry
            .acquire(&session, "principal-a", "workspace-a", "artifact-large")
            .unwrap();
        let mut reader = ManagedArtifactReader::new(
            RepeatedReader {
                remaining: logical_size,
                byte: 0x61,
                max_read: 7,
            },
            logical_size,
            lease,
            Duration::from_secs(60),
        );
        let mut sample = [0_u8; 32];
        reader.read_exact(&mut sample).await.unwrap();
        assert_eq!(sample, [0x61; 32]);
        drop(reader);
        assert_eq!(registry.active_count(), 0);
    }

    #[tokio::test]
    async fn storage_fault_and_truncation_release_handles_with_stable_failure_outcomes() {
        let registry = HttpStreamRegistry::new(&config()).unwrap();
        let session = session("S00000000000000000001");

        let failed_lease = registry
            .acquire(&session, "principal-a", "workspace-a", "artifact-failed")
            .unwrap();
        let mut failed = ManagedArtifactReader::new(
            FailingReader {
                remaining_success: 2,
            },
            4,
            failed_lease,
            Duration::from_secs(60),
        );
        let mut prefix = [0_u8; 2];
        failed.read_exact(&mut prefix).await.unwrap();
        let error = failed.read_u8().await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        drop(failed);
        assert_eq!(registry.active_count(), 0);
        assert_eq!(registry.metrics_observation().3, 1);

        let truncated_lease = registry
            .acquire(&session, "principal-a", "workspace-a", "artifact-truncated")
            .unwrap();
        let mut truncated = ManagedArtifactReader::new(
            RepeatedReader {
                remaining: 2,
                byte: 0x62,
                max_read: 2,
            },
            4,
            truncated_lease,
            Duration::from_secs(60),
        );
        let mut bytes = Vec::new();
        let error = truncated.read_to_end(&mut bytes).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(bytes, b"bb");
        drop(truncated);
        assert_eq!(registry.active_count(), 0);
        assert_eq!(registry.metrics_observation().3, 2);

        let bounded_lease = registry
            .acquire(&session, "principal-a", "workspace-a", "artifact-bounded")
            .unwrap();
        let mut bounded = ManagedArtifactReader::new(
            RepeatedReader {
                remaining: 8,
                byte: 0x63,
                max_read: 8,
            },
            4,
            bounded_lease,
            Duration::from_secs(60),
        );
        let mut exact = Vec::new();
        bounded.read_to_end(&mut exact).await.unwrap();
        assert_eq!(exact, b"cccc");
        drop(bounded);
        assert_eq!(registry.metrics_observation().0, 1);

        let replacement = registry
            .acquire(
                &session,
                "principal-a",
                "workspace-a",
                "artifact-replacement",
            )
            .unwrap();
        drop(replacement);
        assert_eq!(registry.active_count(), 0);
    }
}
