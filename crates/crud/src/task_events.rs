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

/// Covers only Task mutations that can create runnable/dependent work. The
/// guard starts outside DB admission so cancellation retains uncertainty.
pub(crate) struct TaskWorkChange {
    changes: Vec<pioneer_sqlite::WorkChange>,
    frozen: pioneer_sqlite::WorkChange,
    frozen_keys: Option<Vec<String>>,
}
impl TaskWorkChange {
    pub(crate) fn new(database: &pioneer_sqlite::SqliteDatabase) -> Self {
        Self {
            frozen: database.work_signal("maintenance.frozen").changing(),
            frozen_keys: None,
            changes: [
                "tasks.scheduler",
                "tasks.lifecycle",
                "tasks.fanout",
                "tasks.review",
                "compaction",
                "maintenance.portable",
                "maintenance.html",
            ]
            .into_iter()
            .map(|domain| database.work_signal(domain).changing())
            .collect(),
        }
    }
    pub(crate) fn for_tasks(
        database: &pioneer_sqlite::SqliteDatabase,
        tasks: impl IntoIterator<Item = String>,
    ) -> Self {
        let mut change = Self::new(database);
        let keys = tasks
            .into_iter()
            .take(257)
            .map(|id| format!("t:{id}"))
            .collect::<Vec<_>>();
        change.frozen_keys = (!keys.is_empty()).then_some(keys);
        change
    }
    pub(crate) fn for_source(
        database: &pioneer_sqlite::SqliteDatabase,
        kind: &str,
        id: &str,
    ) -> Self {
        let mut change = Self::new(database);
        change.frozen_keys = Some(vec![format!("{kind}:{id}")]);
        change
    }
    pub(crate) fn completed(self, changed: bool) {
        crate::frozen_lifetime::complete_keys(self.frozen, &self.frozen_keys, changed);
        for change in self.changes {
            change.completed(changed);
        }
    }
}

pub(crate) fn inserted_task_events(events: &[AppendedTaskEvent]) -> bool {
    events.iter().any(|event| event.append_status.is_inserted())
}

impl crate::CrudStore {
    pub async fn next_task_event_fanout_deadline(&self) -> anyhow::Result<Option<i64>> {
        crate::repositories::task_event_fanout::next_deadline(&self.connection).await
    }
    pub async fn next_task_occurrence_reconcile_deadline(
        &self,
        now: i64,
    ) -> anyhow::Result<Option<i64>> {
        if self.occurrence_compatibility_cursors[1]
            .lock()
            .await
            .has_more()
        {
            return Ok(Some(now));
        }
        crate::repositories::task_occurrence_reconcile::next_deadline(&self.connection, now).await
    }
}

impl crate::CrudStore {
    pub async fn next_queued_task_ready_deadline(&self, now: i64) -> anyhow::Result<Option<i64>> {
        crate::repositories::task_run::next_ready_deadline(&self.connection, now).await
    }
}

/// Only Turn identity/status facts feed the existing Task dirty-ticket
/// producers. Keep payloads out of this comparison and never notify for a
/// repeated finalization or an item-only event.
pub(crate) struct TaskTurnChange {
    change: pioneer_sqlite::WorkChange,
    portable: pioneer_sqlite::WorkChange,
    before: Vec<(String, TurnDependencyFacts)>,
}
impl TaskTurnChange {
    pub(crate) async fn begin(
        database: &pioneer_sqlite::SqliteDatabase,
        ids: Vec<String>,
    ) -> anyhow::Result<Self> {
        let change = database.work_signal("tasks.lifecycle").changing();
        let portable = database.work_signal("maintenance.portable").changing();
        let mut ids = ids;
        ids.sort();
        ids.dedup();
        let mut before = Vec::with_capacity(ids.len());
        for id in ids {
            before.push((id.clone(), turn_dependency_facts(database, &id).await?));
        }
        Ok(Self {
            change,
            portable,
            before,
        })
    }
    pub(crate) async fn completed(
        self,
        database: &pioneer_sqlite::SqliteDatabase,
    ) -> anyhow::Result<()> {
        let mut changed = false;
        let mut terminal_turns = Vec::new();
        for (id, before) in self.before {
            let after = turn_dependency_facts(database, &id).await?;
            if before.turn != after.turn
                && after.turn.as_ref().is_some_and(|(_, status, _)| {
                    matches!(
                        status.as_str(),
                        "completed" | "failed" | "interrupted" | "blocked"
                    )
                })
            {
                terminal_turns.push(id);
            }
            changed |= before != after;
        }
        self.change.completed(changed);
        self.portable.completed_keys(&terminal_turns);
        Ok(())
    }
}
#[derive(PartialEq, Eq)]
struct TurnDependencyFacts {
    turn: Option<(String, String, String)>,
    receipt: Option<(i64, Option<i64>, String)>,
}
async fn turn_dependency_facts(
    database: &pioneer_sqlite::SqliteDatabase,
    id: &str,
) -> anyhow::Result<TurnDependencyFacts> {
    use pioneer_entity::{turn, turn_event_projection_stream_state as stream};
    use sea_orm::{EntityTrait, QuerySelect};
    let turn = turn::Entity::find_by_id(id.to_owned())
        .select_only()
        .columns([
            turn::Column::ThreadId,
            turn::Column::Status,
            turn::Column::TurnKind,
        ])
        .into_tuple::<(String, String, String)>()
        .one(database)
        .await?;
    let receipt = stream::Entity::find_by_id(id.to_owned())
        .select_only()
        .columns([
            stream::Column::ProjectedThroughSequence,
            stream::Column::AcceptedTerminalSequence,
            stream::Column::Status,
        ])
        .into_tuple::<(i64, Option<i64>, String)>()
        .one(database)
        .await?;
    Ok(TurnDependencyFacts { turn, receipt })
}

impl crate::CrudStore {
    /// Call after the existing Gateway child/parent/delivery bounded pass.
    /// Errors remain errors; a saturated quantum can additionally continue
    /// immediately before consulting these durable deadlines.
    pub async fn next_task_lifecycle_deadline(&self, now: i64) -> anyhow::Result<Option<i64>> {
        if self.occurrence_compatibility_cursors[0]
            .lock()
            .await
            .has_more()
        {
            return Ok(Some(now));
        }
        let child = self.next_task_child_work_deadline(now).await?;
        let parent =
            crate::repositories::task_run_occurrence_reconcile::next_deadline(&self.connection)
                .await?;
        let delivery =
            crate::repositories::task_delivery::next_work_deadline(&self.connection, now, 300)
                .await?;
        Ok(child.into_iter().chain(parent).chain(delivery).min())
    }
}
