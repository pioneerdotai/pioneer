pub use pioneer_protocol::TaskEventPayload;

use sea_orm::entity::prelude::DateTimeWithTimeZone;

#[derive(Debug, Clone)]
pub struct AppendedTaskEvent {
    pub id: String,
    pub task_id: String,
    pub run_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub sequence: i64,
    pub event_type: String,
    pub payload: TaskEventPayload,
    pub workspace_id: Option<String>,
    pub root_task_id: Option<String>,
    pub parent_task_id: Option<String>,
    pub created_at: DateTimeWithTimeZone,
}
