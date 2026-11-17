//! Transaction-local child obligations. Callers supply their business transaction;
//! no helper commits, sends a wake, or acquires database capacity.
use anyhow::Result;
use pioneer_entity::{task, task_run, task_run_turn, turn, turn_event_projection_stream_state};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect};

// Migration history is catalog metadata, deliberately excluded from generated
// Entity. This bounded PK read keeps historical-schema/old-writer adapters on
// their compatibility path instead of referencing work tables not installed yet.
async fn installed<C: ConnectionTrait>(db: &C) -> Result<bool> {
    use sea_orm::sea_query::{Alias, Expr, ExprTrait, Query};
    let query = Query::select()
        .column(Alias::new("version"))
        .from(Alias::new("seaql_migrations"))
        .and_where(Expr::col(Alias::new("version")).eq("m20261101_000101_idle_task_child_work"))
        .limit(1)
        .to_owned();
    Ok(db
        .query_one_raw(db.get_database_backend().build(&query))
        .await?
        .is_some())
}

// These adapters deliberately keep domain change detection separate from queue
// protocol. The queue module owns revision/incarnation and claim preservation.
async fn scope<C: ConnectionTrait>(db: &C, kind: &str, key: &str) -> Result<()> {
    if !installed(db).await? {
        return Ok(());
    }
    use super::task_child_work::ChildScope;
    let input = match kind {
        "task" => ChildScope::Task(key.into()),
        "run" => ChildScope::Run(key.into()),
        "child-thread" => ChildScope::ChildThread(key.into()),
        "creator-thread" => ChildScope::CreatorThread(key.into()),
        "accepted-source" => ChildScope::AcceptedSource(key.into()),
        "parent-thread" => ChildScope::ParentThread(key.into()),
        "creator-turn" => {
            let (thread_id, turn_id) = serde_json::from_str(key)?;
            ChildScope::CreatorTurn { thread_id, turn_id }
        }
        "task+run-group" => {
            let (task_id, run_group_id) = serde_json::from_str(key)?;
            ChildScope::Group {
                task_id,
                run_group_id,
            }
        }
        _ => anyhow::bail!("unknown child producer scope"),
    };
    super::task_child_work::enqueue_scope(db, input, chrono::Utc::now().timestamp()).await
}
pub(crate) async fn point<C: ConnectionTrait>(db: &C, thread: &str, turn: &str) -> Result<()> {
    if !installed(db).await? {
        return Ok(());
    }
    // Never gate a durable publication on a cross-table RR snapshot. Resolve
    // the locator with a current locking read: a stale caller header cannot
    // overwrite a newer canonical point. Missing Turn still publishes; its
    // eventual creation/link transition supplies a fresh canonical revision.
    let canonical = lock_capsule_turn(db, turn).await?;
    super::task_child_work::enqueue_point(
        db,
        canonical.as_deref().unwrap_or(thread),
        turn,
        chrono::Utc::now().timestamp(),
    )
    .await?;
    Ok(())
}
pub(crate) async fn run<C: ConnectionTrait>(db: &C, id: &str) -> Result<()> {
    scope(db, "run", id).await
}
pub(crate) async fn task_change<C: ConnectionTrait>(
    db: &C,
    old: Option<&task::Model>,
    new: Option<&task::Model>,
) -> Result<()> {
    let facts = |r: &task::Model| {
        (
            r.id.clone(),
            r.workspace_id.clone(),
            r.created_by_thread_id.clone(),
            r.created_by_turn_id.clone(),
            r.status.clone(),
        )
    };
    if old.map(facts) == new.map(facts) {
        return Ok(());
    }
    for id in old
        .into_iter()
        .chain(new)
        .map(|r| &r.id)
        .collect::<std::collections::BTreeSet<_>>()
    {
        scope(db, "task", id).await?;
    }
    Ok(())
}
pub(crate) async fn run_change<C: ConnectionTrait>(
    db: &C,
    old: Option<&task_run::Model>,
    new: Option<&task_run::Model>,
) -> Result<()> {
    let facts = |r: &task_run::Model| {
        (
            r.id.clone(),
            r.task_id.clone(),
            r.run_group_id.clone(),
            r.trigger_id.clone(),
            r.attempt_number,
            r.status.clone(),
        )
    };
    if old.map(facts) == new.map(facts) {
        return Ok(());
    }
    for id in old
        .into_iter()
        .chain(new)
        .map(|r| &r.id)
        .collect::<std::collections::BTreeSet<_>>()
    {
        run(db, id).await?;
    }
    for pair in old
        .into_iter()
        .chain(new)
        .map(|r| (&r.task_id, &r.run_group_id))
        .collect::<std::collections::BTreeSet<_>>()
    {
        scope(db, "task+run-group", &serde_json::to_string(&pair)?).await?;
    }
    Ok(())
}
pub(crate) async fn link_change<C: ConnectionTrait>(
    db: &C,
    old: Option<&task_run_turn::Model>,
    new: Option<&task_run_turn::Model>,
) -> Result<()> {
    let facts = |r: &task_run_turn::Model| {
        (
            r.task_id.clone(),
            r.run_id.clone(),
            r.thread_id.clone(),
            r.turn_id.clone(),
            r.status.clone(),
        )
    };
    if old.map(facts) == new.map(facts) {
        return Ok(());
    }
    if !installed(db).await? {
        return Ok(());
    }
    // A point is keyed by Turn, so the new canonical locator must win even
    // when a move sorts before the old Thread. Do not sort these two writes.
    if let Some(row) = old {
        if new.is_none_or(|n| n.thread_id != row.thread_id || n.turn_id != row.turn_id) {
            super::task_child_work::enqueue_point(
                db,
                &row.thread_id,
                &row.turn_id,
                chrono::Utc::now().timestamp(),
            )
            .await?;
        }
    }
    if let Some(row) = new {
        super::task_child_work::enqueue_point(
            db,
            &row.thread_id,
            &row.turn_id,
            chrono::Utc::now().timestamp(),
        )
        .await?;
    }
    Ok(())
}
pub(crate) async fn turn_change<C: ConnectionTrait>(
    db: &C,
    old: Option<&turn::Model>,
    new: Option<&turn::Model>,
) -> Result<()> {
    let identity = |r: &turn::Model| (r.id.clone(), r.thread_id.clone(), r.turn_kind.clone());
    let identity_changed = old.map(identity) != new.map(identity);
    // Accepted-source proof watches presence/thread, independently of the
    // creator's kind requirement. A kind-only repair needs creator invalidation
    // but must not fan out unrelated accepted snapshots.
    let source_identity = |r: &turn::Model| (r.id.clone(), r.thread_id.clone());
    let source_changed = old.map(source_identity) != new.map(source_identity);
    let status_changed = old.map(|r| &r.status) != new.map(|r| &r.status);
    if !identity_changed && !status_changed {
        return Ok(());
    }
    let mut points = std::collections::BTreeSet::new();
    let mut runs = std::collections::BTreeSet::new();
    let mut accepted_sources = std::collections::BTreeSet::new();
    let mut creators = std::collections::BTreeSet::new();
    for row in old.into_iter().chain(new) {
        if source_changed {
            accepted_sources.insert(&row.id);
        }
        if identity_changed {
            creators.insert((&row.thread_id, &row.id));
        }
        if row.turn_kind == "task_run" {
            runs.insert(&row.id);
        }
        if row.turn_kind == "conversation" && row.status != "in_progress" {
            points.insert((&row.thread_id, &row.id));
        }
    }
    for id in accepted_sources {
        scope(db, "accepted-source", id).await?;
    }
    for pair in creators {
        scope(db, "creator-turn", &serde_json::to_string(&pair)?).await?;
    }
    for id in runs {
        run(db, id).await?;
    }
    for (thread, id) in points {
        point(db, thread, id).await?;
    }
    Ok(())
}
pub(crate) async fn thread<C: ConnectionTrait>(db: &C, id: &str) -> Result<()> {
    for kind in ["child-thread", "creator-thread", "parent-thread"] {
        scope(db, kind, id).await?;
    }
    Ok(())
}
pub(crate) async fn lineage<C: ConnectionTrait>(db: &C, id: &str) -> Result<()> {
    scope(db, "child-thread", id).await
}

fn receipt_eligible(row: Option<&turn_event_projection_stream_state::Model>) -> bool {
    row.is_some_and(|r| {
        r.status == "healthy"
            && r.accepted_terminal_sequence
                .is_some_and(|s| r.projected_through_sequence >= s)
    })
}
pub(crate) async fn receipt_change<C: ConnectionTrait>(
    db: &C,
    old: Option<&turn_event_projection_stream_state::Model>,
    new: Option<&turn_event_projection_stream_state::Model>,
) -> Result<()> {
    if receipt_eligible(old) == receipt_eligible(new)
        && old.map(|r| {
            (
                &r.turn_id,
                &r.thread_id,
                &r.status,
                r.accepted_terminal_sequence,
            )
        }) == new.map(|r| {
            (
                &r.turn_id,
                &r.thread_id,
                &r.status,
                r.accepted_terminal_sequence,
            )
        })
    {
        return Ok(());
    }
    for id in old
        .into_iter()
        .chain(new)
        .map(|r| &r.turn_id)
        .collect::<std::collections::BTreeSet<_>>()
    {
        run(db, id).await?;
    }
    Ok(())
}
/// Capsule publication only; successful cleanup never calls this function.
pub(crate) async fn capsule_birth<C: ConnectionTrait>(db: &C, id: &str) -> Result<()> {
    // Birth is source-local, independent of current Link presence and Turn
    // status. The consumer may ACK before another dependency commits; that
    // dependency publishes its own revision, while a late birth always does too.
    point(db, "", id).await
}

/// Current source lock shared with destructive cleanup. Only call inside the
/// owning write transaction. SQLite's writer already provides this exclusion.
pub(crate) async fn lock_capsule_turn<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<String>> {
    Ok(turn::Entity::find_by_id(id)
        .select_only()
        .column(turn::Column::ThreadId)
        .lock_exclusive()
        .into_tuple::<String>()
        .one(db)
        .await?)
}

/// An eligible context source becoming present is a dependency transition;
/// subsequent payload appends and assistant/tool-only context are not.
pub(crate) async fn context_will_create<C: ConnectionTrait>(
    db: &C,
    id: &str,
    source: &str,
) -> Result<bool> {
    use pioneer_entity::turn_llm_context as context;
    if super::compaction::model_history_context_source(source) {
        return Ok(false);
    }
    lock_capsule_turn(db, id).await?;
    Ok(context::Entity::find()
        .select_only()
        .column(context::Column::Id)
        .filter(context::Column::TurnId.eq(id))
        .filter(context::Column::Source.is_not_in(super::compaction::MODEL_HISTORY_CONTEXT_SOURCES))
        .lock_exclusive()
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_none())
}

/// Accepted launch topology depends on the occurrence's origin group, including
/// retries. Status-only occurrence bookkeeping does not affect this proof.
pub(crate) async fn occurrence_identity<C: ConnectionTrait>(db: &C, run_id: &str) -> Result<()> {
    run(db, run_id).await?;
    if let Some(row) = task_run::Entity::find_by_id(run_id.to_owned())
        .lock_exclusive()
        .one(db)
        .await?
    {
        scope(
            db,
            "task+run-group",
            &serde_json::to_string(&(&row.task_id, &row.run_group_id))?,
        )
        .await?;
    }
    Ok(())
}
