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
