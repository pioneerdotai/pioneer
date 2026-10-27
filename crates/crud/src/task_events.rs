pub use pioneer_protocol::TaskEventPayload;

use sea_orm::entity::prelude::DateTimeWithTimeZone;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskEventAppendStatus {
    Inserted,
    AlreadyExists,
}

impl TaskEventAppendStatus {
    pub const fn is_inserted(self) -> bool {
        matches!(self, Self::Inserted)
    }
}

#[derive(Debug, Clone)]
pub struct AppendedTaskEvent {
    pub id: String,
    pub task_id: String,
    pub run_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub sequence: i64,
    pub event_type: String,
    pub idempotency_key: Option<String>,
    pub payload: TaskEventPayload,
    pub workspace_id: Option<String>,
    pub root_task_id: Option<String>,
    pub parent_task_id: Option<String>,
    pub created_at: DateTimeWithTimeZone,
    pub append_status: TaskEventAppendStatus,
    pub(crate) candidate_gate_resolution:
        Option<crate::repositories::native_terminal_effect_outbox::PreparedCandidateGateResolution>,
    pub(crate) candidate_projection:
        Option<crate::repositories::task_result_candidate::PreparedTaskResultCandidate>,
    pub(crate) review_projection:
        Option<crate::repositories::task_result_review_event::PreparedTaskResultReviewEvent>,
    pub(crate) delivery_authority:
        Option<crate::repositories::task_actor_contract::PreparedTaskDeliveryAuthority>,
    pub(crate) projection: crate::task_projector::PreparedTaskProjection,
}

// Internal writer result: retain the selected, fenced winner without copying its
// body. Materialize caller-owned payloads only after commit releases the writer.
#[derive(Debug)]
pub(crate) struct DeferredAppendedTaskEvent {
    pub id: String,
    pub task_id: String,
    pub run_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub sequence: i64,
    pub event_type: String,
    pub idempotency_key: Option<String>,
    pub payload: std::sync::Arc<TaskEventPayload>,
    pub workspace_id: Option<String>,
    pub root_task_id: Option<String>,
    pub parent_task_id: Option<String>,
    pub created_at: DateTimeWithTimeZone,
    pub append_status: TaskEventAppendStatus,
    pub(crate) candidate_gate_resolution:
        Option<crate::repositories::native_terminal_effect_outbox::PreparedCandidateGateResolution>,
    pub(crate) candidate_projection:
        Option<crate::repositories::task_result_candidate::PreparedTaskResultCandidate>,
    pub(crate) review_projection:
        Option<crate::repositories::task_result_review_event::PreparedTaskResultReviewEvent>,
    pub(crate) delivery_authority:
        Option<crate::repositories::task_actor_contract::PreparedTaskDeliveryAuthority>,
    pub(crate) projection: crate::task_projector::PreparedTaskProjection,
}

impl DeferredAppendedTaskEvent {
    pub(crate) fn into_owned(self) -> AppendedTaskEvent {
        AppendedTaskEvent {
            id: self.id,
            task_id: self.task_id,
            run_id: self.run_id,
            thread_id: self.thread_id,
            turn_id: self.turn_id,
            sequence: self.sequence,
            event_type: self.event_type,
            idempotency_key: self.idempotency_key,
            payload: std::sync::Arc::unwrap_or_clone(self.payload),
            workspace_id: self.workspace_id,
            root_task_id: self.root_task_id,
            parent_task_id: self.parent_task_id,
            created_at: self.created_at,
            append_status: self.append_status,
            candidate_gate_resolution: self.candidate_gate_resolution,
            candidate_projection: self.candidate_projection,
            review_projection: self.review_projection,
            delivery_authority: self.delivery_authority,
            projection: self.projection,
        }
    }
}

pub(crate) fn materialize_task_events(
    events: Vec<DeferredAppendedTaskEvent>,
) -> Vec<AppendedTaskEvent> {
    events
        .into_iter()
        .map(DeferredAppendedTaskEvent::into_owned)
        .collect()
}
