//! Transaction-local child obligations. Callers supply their business transaction;
//! no helper commits, sends a wake, or acquires database capacity.
use anyhow::Result;
use pioneer_entity::{task, task_run, task_run_turn, turn, turn_event_projection_stream_state};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect};

// These adapters deliberately keep domain change detection separate from queue
// protocol. The queue module owns revision/incarnation and claim preservation.
async fn scope<C: ConnectionTrait>(db: &C, kind: &str, key: &str) -> Result<()> {
    crate::child_work::enqueue_scope(db, kind, key, chrono::Utc::now().timestamp()).await
}
pub async fn point<C: ConnectionTrait>(db: &C, thread: &str, turn: &str) -> Result<()> {
    // Never gate a durable publication on a cross-table RR snapshot. Resolve
    // the locator with a current locking read: a stale caller header cannot
    // overwrite a newer canonical point. Missing Turn still publishes; its
    // eventual creation/link transition supplies a fresh canonical revision.
    let canonical = lock_capsule_turn(db, turn).await?;
    crate::child_work::enqueue_point(
        db,
        canonical.as_deref().unwrap_or(thread),
        turn,
        chrono::Utc::now().timestamp(),
    )
    .await?;
    Ok(())
}
pub async fn run<C: ConnectionTrait>(db: &C, id: &str) -> Result<()> {
    scope(db, "run", id).await
}
pub async fn task_change<C: ConnectionTrait>(
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
pub async fn run_change<C: ConnectionTrait>(
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
pub async fn link_change<C: ConnectionTrait>(
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
    // A point is keyed by Turn, so the new canonical locator must win even
    // when a move sorts before the old Thread. Do not sort these two writes.
    if let Some(row) = old {
        if new.is_none_or(|n| n.thread_id != row.thread_id || n.turn_id != row.turn_id) {
            crate::child_work::enqueue_point(
                db,
                &row.thread_id,
                &row.turn_id,
                chrono::Utc::now().timestamp(),
            )
            .await?;
        }
    }
    if let Some(row) = new {
        crate::child_work::enqueue_point(
            db,
            &row.thread_id,
            &row.turn_id,
            chrono::Utc::now().timestamp(),
        )
        .await?;
    }
    Ok(())
}
pub async fn turn_change<C: ConnectionTrait>(
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
pub async fn thread<C: ConnectionTrait>(db: &C, id: &str) -> Result<()> {
    for kind in ["child-thread", "creator-thread", "parent-thread"] {
        scope(db, kind, id).await?;
    }
    Ok(())
}
pub async fn lineage<C: ConnectionTrait>(db: &C, id: &str) -> Result<()> {
    scope(db, "child-thread", id).await
}

fn receipt_eligible(row: Option<&turn_event_projection_stream_state::Model>) -> bool {
    row.is_some_and(|r| {
        r.status == "healthy"
            && r.accepted_terminal_sequence
                .is_some_and(|s| r.projected_through_sequence >= s)
    })
}
pub async fn receipt_change<C: ConnectionTrait>(
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
pub async fn capsule_birth<C: ConnectionTrait>(db: &C, id: &str) -> Result<()> {
    // Birth is source-local, independent of current Link presence and Turn
    // status. The consumer may ACK before another dependency commits; that
    // dependency publishes its own revision, while a late birth always does too.
    point(db, "", id).await
}

/// Current source lock shared with destructive cleanup. Only call inside the
/// owning write transaction.
pub async fn lock_capsule_turn<C: ConnectionTrait>(db: &C, id: &str) -> Result<Option<String>> {
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
pub async fn context_will_create<C: ConnectionTrait>(
    db: &C,
    id: &str,
    source: &str,
    history_sources: &[&str],
) -> Result<bool> {
    use pioneer_entity::turn_llm_context as context;
    if history_sources.contains(&source) {
        return Ok(false);
    }
    lock_capsule_turn(db, id).await?;
    Ok(context::Entity::find()
        .select_only()
        .column(context::Column::Id)
        .filter(context::Column::TurnId.eq(id))
        .filter(context::Column::Source.is_not_in(history_sources.iter().copied()))
        .lock_exclusive()
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_none())
}

/// Accepted launch topology depends on the occurrence's origin group, including
/// retries. Status-only occurrence bookkeeping does not affect this proof.
pub async fn occurrence_identity<C: ConnectionTrait>(db: &C, run_id: &str) -> Result<()> {
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
