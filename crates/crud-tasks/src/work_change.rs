use crate::event_records::AppendedTaskEvent;

/// Covers only Task mutations that can create runnable/dependent work. The
/// guard starts outside DB admission so cancellation retains uncertainty.
pub struct TaskWorkChange {
    changes: Vec<pioneer_postgres_pools::WorkChange>,
    frozen: pioneer_postgres_pools::WorkChange,
    frozen_keys: Option<Vec<String>>,
}
impl TaskWorkChange {
    pub fn new(database: &pioneer_postgres_pools::PostgresDatabase) -> Self {
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
    pub fn for_tasks(
        database: &pioneer_postgres_pools::PostgresDatabase,
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
    pub fn for_source(
        database: &pioneer_postgres_pools::PostgresDatabase,
        kind: &str,
        id: &str,
    ) -> Self {
        let mut change = Self::new(database);
        change.frozen_keys = Some(vec![format!("{kind}:{id}")]);
        change
    }
    pub fn completed(self, changed: bool) {
        match (changed, self.frozen_keys) {
            (false, _) => self.frozen.completed(false),
            (true, Some(keys)) => self.frozen.completed_keys(&keys),
            (true, None) => self.frozen.completed(true),
        }
        for change in self.changes {
            change.completed(changed);
        }
    }
}

pub fn inserted_task_events(events: &[AppendedTaskEvent]) -> bool {
    events.iter().any(|event| event.append_status.is_inserted())
}

/// Only Turn identity/status facts feed the existing Task dirty-ticket
/// producers. Keep payloads out of this comparison and never notify for a
/// repeated finalization or an item-only event.
pub struct TaskTurnChange {
    change: pioneer_postgres_pools::WorkChange,
    portable: pioneer_postgres_pools::WorkChange,
    before: Vec<(String, TurnDependencyFacts)>,
}
impl TaskTurnChange {
    pub async fn begin(
        database: &pioneer_postgres_pools::PostgresDatabase,
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
    pub async fn completed(
        self,
        database: &pioneer_postgres_pools::PostgresDatabase,
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
    database: &pioneer_postgres_pools::PostgresDatabase,
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
