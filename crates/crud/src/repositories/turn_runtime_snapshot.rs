use anyhow::Result;
use sea_orm::{ConnectionTrait, DbBackend, Statement};

use pioneer_crud_execution::turn_runtime_snapshot;
pub use pioneer_crud_execution::turn_runtime_snapshot::{
    NewTurnRuntimeSnapshot, TurnRuntimeSnapshotRecord, clear_agent_skill_versions_if_matches,
    delete_turn_runtime_snapshot, delete_turn_runtime_snapshots_for_closed_turns,
    find_turn_runtime_snapshot,
};

/// Recheck the request's bounded metadata in the same short writer transaction
/// that attaches the live runtime root. The JSON is prepared before admission.
pub(crate) async fn verify_source_epochs<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    epochs_json: &str,
) -> Result<()> {
    // json_each_text supplies the prepared source epoch map as relational
    // keys. SeaQuery has no builder for a table-valued JSON expansion.
    let changed = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT wanted.key FROM json_each_text($1::text::json) wanted \
         LEFT JOIN thread t ON t.id=wanted.key AND t.workspace_id=$2 \
         LEFT JOIN compaction_projection_epoch e ON e.thread_id=t.id \
         WHERE t.id IS NULL OR COALESCE(e.version,0)<>wanted.value::bigint LIMIT 1",
            [epochs_json.into(), workspace.into()],
        ))
        .await?;
    anyhow::ensure!(
        changed.is_none(),
        "history changed before runtime publication"
    );
    Ok(())
}

pub async fn upsert_turn_runtime_snapshot<C: ConnectionTrait>(
    db: &C,
    snapshot: NewTurnRuntimeSnapshot,
    root: &crate::frozen_lifetime::PreparedFrozenRoot,
) -> Result<TurnRuntimeSnapshotRecord> {
    root.verify(db, &snapshot.workspace_id).await?;
    super::task_child_producers::lock_capsule_turn(db, &snapshot.turn_id).await?;
    let old = turn_runtime_snapshot::lock_identity(db, &snapshot.turn_id).await?;
    let capsule_identity_changed = old.as_ref().is_none_or(|(thread, workspace)| {
        thread != &snapshot.thread_id || workspace != &snapshot.workspace_id
    });
    let frozen_manifest_id = root.locator();
    let row = turn_runtime_snapshot::upsert_turn_runtime_snapshot(
        db,
        snapshot,
        frozen_manifest_id.as_deref(),
    )
    .await?;
    if capsule_identity_changed {
        super::task_child_producers::capsule_birth(db, &row.turn_id).await?;
    }
    Ok(row)
}
