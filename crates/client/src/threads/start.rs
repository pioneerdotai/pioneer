//! Thread start coordination.

use crate::workspaces::selectors as workspace_selectors;
use pioneer_protocol::{Thread, ThreadStartParams, ThreadStartResponse, generate_id};
use std::time::{Duration, Instant};

pub const THREAD_START_ID_LEN: usize = 21;
pub const WORKSPACE_START_SCOPE_BOOTSTRAP: &str = "__bootstrap__";
pub const THREAD_START_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(500);
pub const THREAD_START_RETRY_MAX_DELAY: Duration = Duration::from_millis(5_000);

#[derive(Default)]
pub struct ThreadStartCoordinator {
    pub pending_thread_id: Option<String>,
    pub in_progress: bool,
    pub retry_attempt: u32,
    pub next_attempt_at: Option<Instant>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ThreadStartRequestPlan {
    pub clear_draft_thread_id: Option<String>,
    pub ensure_pending_thread_id: bool,
    pub enqueue_start_request: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThreadStartDrivePlan {
    NotReady,
    ClearQueue,
    Start { connection_id: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadStartAttemptPlan {
    pub requested_thread_id: String,
    pub requested_workspace_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadStartRetryPlan {
    pub attempt: u32,
    pub delay: Duration,
    pub next_attempt_at: Instant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThreadStartWorkspaceResolution {
    Requested(String),
    LoadDefaultWorkspace,
}

#[derive(Clone, Debug)]
pub struct ThreadStartBootstrapReduction {
    pub thread: Thread,
    pub thread_id: String,
    pub workspace_id: String,
    pub persist_active_gateway_workspace_id: String,
    pub set_draft_thread_id: String,
    pub set_active_thread_id: Option<String>,
    pub set_preferred_workspace_id: String,
    pub reset_thread_start: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadStartSubscriptionReduction {
    pub thread: Thread,
    pub thread_id: String,
    pub workspace_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThreadStartBootstrapFailurePlan {
    Retry { thread_id: String },
    Reset,
}

pub fn reset_thread_start_coordinator(start: &mut ThreadStartCoordinator) {
    *start = ThreadStartCoordinator::default();
}

pub fn enqueue_thread_start_request(start_requested: &mut bool) {
    *start_requested = true;
}

pub fn dequeue_thread_start_request(start_requested: &mut bool) -> bool {
    if !*start_requested {
        return false;
    }
    *start_requested = false;
    true
}

pub fn clear_thread_start_request(start_requested: &mut bool) {
    *start_requested = false;
}

pub fn generate_thread_start_id() -> String {
    generate_id(THREAD_START_ID_LEN)
}

pub fn ensure_pending_thread_start_id(
    start: &mut ThreadStartCoordinator,
    generated_thread_id: String,
) -> bool {
    if start.pending_thread_id.is_some() {
        return false;
    }

    start.pending_thread_id = Some(generated_thread_id);
    true
}

pub fn plan_thread_start_request(
    draft_thread_id: Option<&str>,
    draft_thread_exists: bool,
    start: &ThreadStartCoordinator,
) -> ThreadStartRequestPlan {
    if draft_thread_id.is_some() && draft_thread_exists {
        return ThreadStartRequestPlan::default();
    }

    let clear_draft_thread_id = draft_thread_id.map(str::to_owned);
    if start.in_progress {
        return ThreadStartRequestPlan {
            clear_draft_thread_id,
            ..ThreadStartRequestPlan::default()
        };
    }

    ThreadStartRequestPlan {
        clear_draft_thread_id,
        ensure_pending_thread_id: start.pending_thread_id.is_none(),
        enqueue_start_request: true,
    }
}

pub fn plan_thread_start_drive(
    draft_thread_id: Option<&str>,
    draft_thread_exists: bool,
    connection_id: Option<u64>,
    gateway_connected: bool,
    start: &ThreadStartCoordinator,
    start_requested: bool,
) -> ThreadStartDrivePlan {
    if draft_thread_id.is_some() && draft_thread_exists {
        return ThreadStartDrivePlan::ClearQueue;
    }

    if !gateway_connected || start.in_progress || !start_requested {
        return ThreadStartDrivePlan::NotReady;
    }

    match connection_id {
        Some(connection_id) => ThreadStartDrivePlan::Start { connection_id },
        None => ThreadStartDrivePlan::NotReady,
    }
}

pub fn begin_thread_start_attempt(
    start: &mut ThreadStartCoordinator,
    fallback_thread_id: String,
    start_scope: String,
) -> Option<ThreadStartAttemptPlan> {
    if start.in_progress {
        return None;
    }

    start.in_progress = true;
    start.next_attempt_at = None;

    let requested_thread_id = start
        .pending_thread_id
        .clone()
        .unwrap_or(fallback_thread_id);
    start.pending_thread_id = Some(requested_thread_id.clone());

    Some(ThreadStartAttemptPlan {
        requested_thread_id,
        requested_workspace_id: requested_workspace_id_for_thread_start(start_scope),
    })
}

pub fn finish_thread_start_attempt(start: &mut ThreadStartCoordinator) {
    start.in_progress = false;
}

pub fn apply_thread_start_retry(
    start: &mut ThreadStartCoordinator,
    thread_id: &str,
    now: Instant,
) -> ThreadStartRetryPlan {
    let delay = thread_start_retry_delay(start.retry_attempt);
    let attempt = start.retry_attempt.saturating_add(1);

    start.pending_thread_id = Some(thread_id.to_owned());
    start.retry_attempt = attempt;
    start.next_attempt_at = Some(now + delay);

    ThreadStartRetryPlan {
        attempt,
        delay,
        next_attempt_at: now + delay,
    }
}

pub fn should_fire_scheduled_thread_start_retry(
    start: &ThreadStartCoordinator,
    expected_attempt: u32,
    expected_pending_thread_id: &str,
) -> bool {
    start.retry_attempt == expected_attempt
        && start.pending_thread_id.as_deref() == Some(expected_pending_thread_id)
        && !start.in_progress
}

pub fn thread_start_retry_delay(attempt: u32) -> Duration {
    let multiplier = 1u64 << attempt.min(8);
    let delay_ms = (THREAD_START_RETRY_INITIAL_DELAY.as_millis() as u64).saturating_mul(multiplier);
    Duration::from_millis(delay_ms.min(THREAD_START_RETRY_MAX_DELAY.as_millis() as u64))
}

pub fn thread_start_scope(workspace_id: Option<&str>) -> Option<String> {
    workspace_selectors::normalize_workspace_id(workspace_id.map(str::to_owned))
}

pub fn default_thread_start_scope(
    preferred_workspace_id: Option<&str>,
    runtime_workspace_id: Option<&str>,
) -> String {
    thread_start_scope(preferred_workspace_id.or(runtime_workspace_id))
        .unwrap_or_else(|| WORKSPACE_START_SCOPE_BOOTSTRAP.to_owned())
}

pub fn requested_workspace_id_for_thread_start(scope: String) -> Option<String> {
    (scope.as_str() != WORKSPACE_START_SCOPE_BOOTSTRAP).then_some(scope)
}

pub fn plan_workspace_id_for_thread_start(
    requested_workspace_id: Option<String>,
) -> ThreadStartWorkspaceResolution {
    match workspace_selectors::normalize_workspace_id(requested_workspace_id) {
        Some(workspace_id) => ThreadStartWorkspaceResolution::Requested(workspace_id),
        None => ThreadStartWorkspaceResolution::LoadDefaultWorkspace,
    }
}

pub fn normalize_default_workspace_id_for_thread_start(workspace_id: String) -> Option<String> {
    workspace_selectors::normalize_workspace_id(Some(workspace_id))
}

pub fn thread_start_params(thread_id: String, workspace_id: String) -> ThreadStartParams {
    ThreadStartParams {
        thread_id,
        workspace_id,
        name: None,
        model: None,
        model_provider: None,
        sandbox: None,
        mode: None,
        origin_kind: Some(pioneer_protocol::ThreadOriginKind::Collaborative),
        sidebar_visibility: None,
        visibility: None,
        agent_nickname: None,
        agent_role: None,
    }
}

/// Create a new collaborative user thread with an explicit server-owned
/// visibility choice. Subscription/reconnect callers must continue using
/// [`thread_start_params`] so they never mutate an existing thread's scope.
pub fn thread_create_params(
    thread_id: String,
    workspace_id: String,
    visibility: pioneer_protocol::ThreadVisibility,
) -> ThreadStartParams {
    ThreadStartParams {
        visibility: Some(visibility),
        ..thread_start_params(thread_id, workspace_id)
    }
}

pub fn reduce_thread_start_bootstrap_success(
    resolved_workspace_id: String,
    response: ThreadStartResponse,
    active_thread_id: Option<&str>,
) -> ThreadStartBootstrapReduction {
    let thread = response.thread;
    let thread_id = thread.id.clone();
    let workspace_id = thread.workspace_id.clone();

    ThreadStartBootstrapReduction {
        thread,
        thread_id: thread_id.clone(),
        workspace_id: workspace_id.clone(),
        persist_active_gateway_workspace_id: resolved_workspace_id,
        set_draft_thread_id: thread_id.clone(),
        set_active_thread_id: active_thread_id.is_none().then_some(thread_id),
        set_preferred_workspace_id: workspace_id,
        reset_thread_start: true,
    }
}

pub fn reduce_thread_start_subscription_success(
    response: ThreadStartResponse,
) -> ThreadStartSubscriptionReduction {
    let thread = response.thread;
    let thread_id = thread.id.clone();
    let workspace_id = thread.workspace_id.clone();

    ThreadStartSubscriptionReduction {
        thread,
        thread_id,
        workspace_id,
    }
}

pub fn plan_thread_start_bootstrap_failure(
    requested_thread_id: &str,
    error_message: &str,
) -> ThreadStartBootstrapFailurePlan {
    if is_transient_thread_start_error_message(error_message) {
        ThreadStartBootstrapFailurePlan::Retry {
            thread_id: requested_thread_id.to_owned(),
        }
    } else {
        ThreadStartBootstrapFailurePlan::Reset
    }
}

pub fn is_transient_thread_start_error_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    if message.contains("invalid json-rpc response payload")
        || message.contains("failed to decode `thread/start` response payload")
        || message.contains("invalid request")
    {
        return false;
    }

    message.contains("timeout")
        || message.contains("timed out")
        || message.contains("temporar")
        || message.contains("websocket")
        || message.contains("connection")
        || message.contains("internal error")
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        SandboxMode, SandboxPolicy, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility,
        ThreadStatus,
    };

    fn thread(id: &str, workspace_id: &str) -> Thread {
        Thread {
            workspace_id: workspace_id.to_owned(),
            id: id.to_owned(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Chat,
            model: "gpt-5".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: 1,
            updated_at: 2,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        }
    }

    fn thread_start_response(thread: Thread) -> ThreadStartResponse {
        ThreadStartResponse {
            thread,
            sandbox: SandboxPolicy::from_mode(SandboxMode::FullAccess),
        }
    }

    #[test]
    fn request_plan_preserves_existing_draft_and_queues_missing_draft() {
        let start = ThreadStartCoordinator::default();

        assert_eq!(
            plan_thread_start_request(Some("draft_known"), true, &start),
            ThreadStartRequestPlan::default()
        );
        assert_eq!(
            plan_thread_start_request(Some("draft_missing"), false, &start),
            ThreadStartRequestPlan {
                clear_draft_thread_id: Some("draft_missing".to_owned()),
                ensure_pending_thread_id: true,
                enqueue_start_request: true,
            }
        );
    }

    #[test]
    fn pending_thread_id_helper_sets_id_once() {
        let mut start = ThreadStartCoordinator::default();

        assert!(ensure_pending_thread_start_id(
            &mut start,
            "thread_a".to_owned()
        ));
        assert!(!ensure_pending_thread_start_id(
            &mut start,
            "thread_b".to_owned()
        ));
        assert_eq!(start.pending_thread_id.as_deref(), Some("thread_a"));
        assert_eq!(
            generate_thread_start_id().chars().count(),
            THREAD_START_ID_LEN
        );
    }

    #[test]
    fn thread_start_queue_flag_batches_until_dequeued_or_cleared() {
        let mut start_requested = false;

        assert!(!dequeue_thread_start_request(&mut start_requested));
        enqueue_thread_start_request(&mut start_requested);
        enqueue_thread_start_request(&mut start_requested);
        assert!(dequeue_thread_start_request(&mut start_requested));
        assert!(!dequeue_thread_start_request(&mut start_requested));

        enqueue_thread_start_request(&mut start_requested);
        clear_thread_start_request(&mut start_requested);
        assert!(!start_requested);
    }

    #[test]
    fn drive_plan_clears_queue_for_known_draft_before_connection_checks() {
        let start = ThreadStartCoordinator::default();

        assert_eq!(
            plan_thread_start_drive(Some("draft_known"), true, None, false, &start, true),
            ThreadStartDrivePlan::ClearQueue
        );
        assert_eq!(
            plan_thread_start_drive(None, false, Some(7), true, &start, true),
            ThreadStartDrivePlan::Start { connection_id: 7 }
        );
    }

    #[test]
    fn begin_attempt_sets_in_progress_and_resolves_bootstrap_scope() {
        let mut start = ThreadStartCoordinator::default();
        let plan = begin_thread_start_attempt(
            &mut start,
            "generated_thread".to_owned(),
            WORKSPACE_START_SCOPE_BOOTSTRAP.to_owned(),
        )
        .expect("start plan");

        assert!(start.in_progress);
        assert_eq!(start.pending_thread_id.as_deref(), Some("generated_thread"));
        assert_eq!(plan.requested_thread_id, "generated_thread");
        assert_eq!(plan.requested_workspace_id, None);
    }

    #[test]
    fn retry_state_increments_attempt_and_uses_exponential_cap() {
        let now = Instant::now();
        let mut start = ThreadStartCoordinator {
            retry_attempt: 3,
            ..ThreadStartCoordinator::default()
        };

        let plan = apply_thread_start_retry(&mut start, "thread_a", now);

        assert_eq!(plan.attempt, 4);
        assert_eq!(plan.delay, Duration::from_millis(4_000));
        assert_eq!(start.retry_attempt, 4);
        assert_eq!(start.pending_thread_id.as_deref(), Some("thread_a"));
        assert_eq!(
            start.next_attempt_at,
            Some(now + Duration::from_millis(4_000))
        );
        assert_eq!(thread_start_retry_delay(20), THREAD_START_RETRY_MAX_DELAY);
    }

    #[test]
    fn scheduled_thread_start_retry_fires_only_for_matching_pending_attempt() {
        let start = ThreadStartCoordinator {
            pending_thread_id: Some("thread_a".to_owned()),
            retry_attempt: 2,
            in_progress: false,
            next_attempt_at: None,
        };

        assert!(should_fire_scheduled_thread_start_retry(
            &start, 2, "thread_a"
        ));
        assert!(!should_fire_scheduled_thread_start_retry(
            &start, 1, "thread_a"
        ));
        assert!(!should_fire_scheduled_thread_start_retry(
            &start, 2, "thread_b"
        ));

        let in_progress = ThreadStartCoordinator {
            in_progress: true,
            ..start
        };
        assert!(!should_fire_scheduled_thread_start_retry(
            &in_progress,
            2,
            "thread_a"
        ));
    }

    #[test]
    fn default_scope_prefers_preferred_then_runtime_then_bootstrap() {
        assert_eq!(
            default_thread_start_scope(Some(" ws_a "), Some("ws_b")),
            "ws_a"
        );
        assert_eq!(default_thread_start_scope(None, Some("ws_b")), "ws_b");
        assert_eq!(
            default_thread_start_scope(Some(" "), None),
            WORKSPACE_START_SCOPE_BOOTSTRAP
        );
    }

    #[test]
    fn workspace_resolution_requests_default_for_empty_id() {
        assert_eq!(
            plan_workspace_id_for_thread_start(Some(" ws_a ".to_owned())),
            ThreadStartWorkspaceResolution::Requested("ws_a".to_owned())
        );
        assert_eq!(
            plan_workspace_id_for_thread_start(None),
            ThreadStartWorkspaceResolution::LoadDefaultWorkspace
        );
        assert_eq!(
            normalize_default_workspace_id_for_thread_start(" ".to_owned()),
            None
        );

        let params = thread_start_params("thread_a".to_owned(), "ws_a".to_owned());
        assert_eq!(params.thread_id, "thread_a");
        assert_eq!(params.workspace_id, "ws_a");
        assert_eq!(params.name, None);
        assert_eq!(params.visibility, None);

        let create = thread_create_params(
            "thread_b".to_owned(),
            "ws_a".to_owned(),
            pioneer_protocol::ThreadVisibility::Workspace,
        );
        assert_eq!(
            create.visibility,
            Some(pioneer_protocol::ThreadVisibility::Workspace)
        );
    }

    #[test]
    fn bootstrap_success_reduction_preserves_desktop_state_changes() {
        let reduction = reduce_thread_start_bootstrap_success(
            "resolved_ws".to_owned(),
            thread_start_response(thread("thread_a", "thread_ws")),
            None,
        );

        assert_eq!(reduction.thread_id, "thread_a");
        assert_eq!(reduction.workspace_id, "thread_ws");
        assert_eq!(reduction.thread.id, "thread_a");
        assert_eq!(reduction.thread.workspace_id, "thread_ws");
        assert_eq!(reduction.persist_active_gateway_workspace_id, "resolved_ws");
        assert_eq!(reduction.set_draft_thread_id, "thread_a");
        assert_eq!(reduction.set_active_thread_id.as_deref(), Some("thread_a"));
        assert_eq!(reduction.set_preferred_workspace_id, "thread_ws");
        assert!(reduction.reset_thread_start);
    }

    #[test]
    fn bootstrap_success_reduction_does_not_replace_existing_active_thread() {
        let reduction = reduce_thread_start_bootstrap_success(
            "resolved_ws".to_owned(),
            thread_start_response(thread("thread_a", "thread_ws")),
            Some("active_thread"),
        );

        assert_eq!(reduction.set_active_thread_id, None);
        assert_eq!(reduction.set_draft_thread_id, "thread_a");
    }

    #[test]
    fn subscription_success_reduction_extracts_thread_snapshot_and_workspace_mapping() {
        let reduction = reduce_thread_start_subscription_success(thread_start_response(thread(
            "thread_a", "ws_a",
        )));

        assert_eq!(reduction.thread.id, "thread_a");
        assert_eq!(reduction.thread.workspace_id, "ws_a");
        assert_eq!(reduction.thread_id, "thread_a");
        assert_eq!(reduction.workspace_id, "ws_a");
    }

    #[test]
    fn bootstrap_failure_plan_retries_only_transient_errors() {
        assert_eq!(
            plan_thread_start_bootstrap_failure("thread_a", "websocket connection timeout"),
            ThreadStartBootstrapFailurePlan::Retry {
                thread_id: "thread_a".to_owned()
            }
        );
        assert_eq!(
            plan_thread_start_bootstrap_failure("thread_a", "invalid request: workspace_id"),
            ThreadStartBootstrapFailurePlan::Reset
        );
    }

    #[test]
    fn transient_classifier_rejects_decode_and_invalid_request_errors() {
        assert!(is_transient_thread_start_error_message(
            "websocket connection timeout"
        ));
        assert!(is_transient_thread_start_error_message(
            "internal error while starting"
        ));
        assert!(!is_transient_thread_start_error_message(
            "failed to decode `thread/start` response payload"
        ));
        assert!(!is_transient_thread_start_error_message(
            "invalid request: workspace_id"
        ));
    }
}
