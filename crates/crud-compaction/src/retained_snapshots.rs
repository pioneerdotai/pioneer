//! Attach and release retained history roots for runtime and Task snapshots.
use crate::{frozen_lifetime, store::CompactionStore};
use anyhow::Result;
use pioneer_crud_execution::turn_runtime_snapshot::{
    self, NewTurnRuntimeSnapshot, TurnRuntimeSnapshotRecord,
};
use pioneer_crud_tasks::{
    conversation_snapshot::{
        self as task_run_conversation_snapshot, NewTaskRunConversationSnapshot,
        TaskRunConversationSnapshotRecord,
    },
    task_child_producers,
};
use pioneer_postgres_pools::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, retry_with_backoff,
};
use sea_orm::{ConnectionTrait, DbBackend, EntityTrait, QuerySelect, Statement, TransactionTrait};
use std::{future::Future, time::Duration};

async fn retry_snapshot_write<T, F, Fut>(operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    retry_with_backoff(
        operation,
        pioneer_crud_tasks::retry::is_retryable,
        DEFAULT_LOCK_RETRY_ATTEMPTS,
        Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
    )
    .await
}

/// Recheck the request's bounded metadata in the same short writer transaction
/// that attaches the live runtime root. The JSON is prepared before admission.
async fn verify_source_epochs<C: ConnectionTrait>(
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

async fn attach_runtime_snapshot<C: ConnectionTrait>(
    db: &C,
    snapshot: NewTurnRuntimeSnapshot,
    root: &frozen_lifetime::PreparedFrozenRoot,
) -> Result<TurnRuntimeSnapshotRecord> {
    root.verify(db, &snapshot.workspace_id).await?;
    task_child_producers::lock_capsule_turn(db, &snapshot.turn_id).await?;
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
        task_child_producers::capsule_birth(db, &row.turn_id).await?;
    }
    Ok(row)
}

impl CompactionStore {
    pub async fn upsert_turn_runtime_snapshot(
        &self,
        snapshot: NewTurnRuntimeSnapshot,
    ) -> Result<TurnRuntimeSnapshotRecord> {
        self.upsert_turn_runtime_snapshot_with_source_epochs(
            snapshot,
            &std::collections::BTreeMap::new(),
        )
        .await
    }

    pub async fn upsert_turn_runtime_snapshot_with_source_epochs(
        &self,
        snapshot: NewTurnRuntimeSnapshot,
        source_epochs: &std::collections::BTreeMap<String, u64>,
    ) -> Result<TurnRuntimeSnapshotRecord> {
        let task_change = self.connection.work_signal("tasks.lifecycle").changing();
        let task_before =
            pioneer_entity::turn_runtime_snapshot::Entity::find_by_id(snapshot.turn_id.clone())
                .select_only()
                .columns([
                    pioneer_entity::turn_runtime_snapshot::Column::ThreadId,
                    pioneer_entity::turn_runtime_snapshot::Column::WorkspaceId,
                ])
                .into_tuple::<(String, String)>()
                .one(&self.connection)
                .await?;
        let task_requested = (snapshot.thread_id.clone(), snapshot.workspace_id.clone());
        let task_result = async {
            let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
            let html_change = self.connection.work_signal("maintenance.html").changing();
            let maintenance_result = async {
                let root = frozen_lifetime::PreparedFrozenRoot::history(&snapshot.history_json)?;
                // Preparation reads these epochs without writer capacity. Edits can
                // race the handoff, so revalidate them in the runtime-root transaction.
                let source_epochs_json = serde_json::to_string(source_epochs)?;
                anyhow::ensure!(
                    source_epochs_json.len() <= crate::SOURCE_PAGE_BYTES,
                    "runtime source epochs exceed metadata byte bound"
                );
                retry_snapshot_write(|| async {
                    let transaction = self.connection.begin().await?;
                    let before = pioneer_entity::turn_runtime_snapshot::Entity::find_by_id(
                        snapshot.turn_id.clone(),
                    )
                    .select_only()
                    .columns([
                        pioneer_entity::turn_runtime_snapshot::Column::FrozenManifestId,
                        pioneer_entity::turn_runtime_snapshot::Column::WorkspaceId,
                    ])
                    .into_tuple::<(Option<String>, String)>()
                    .one(&transaction)
                    .await?;
                    let html_changed = before
                        .as_ref()
                        .is_none_or(|(_, workspace)| workspace != &snapshot.workspace_id);
                    let old_root = before.and_then(|(root, _)| root);

                    let result = async {
                        verify_source_epochs(
                            &transaction,
                            &snapshot.workspace_id,
                            &source_epochs_json,
                        )
                        .await?;
                        attach_runtime_snapshot(&transaction, snapshot.clone(), &root).await
                    }
                    .await?;
                    transaction.commit().await?;
                    Ok((
                        result,
                        frozen_lifetime::changed_root_keys(old_root, root.locator()),
                        html_changed,
                    ))
                })
                .await
            }
            .await;
            if let Ok((_, keys, html_changed)) = &maintenance_result {
                html_change.completed(*html_changed);
                frozen_lifetime::complete_keys(frozen_change, keys, true);
            }
            maintenance_result.map(|(record, _, _)| record)
        }
        .await;
        if task_result.is_ok() {
            task_change.completed(task_before.as_ref() != Some(&task_requested));
        }
        task_result
    }

    pub async fn get_turn_runtime_snapshot(
        &self,
        turn_id: &str,
    ) -> Result<Option<TurnRuntimeSnapshotRecord>> {
        turn_runtime_snapshot::find_turn_runtime_snapshot(&self.connection, turn_id).await
    }

    pub async fn clear_turn_runtime_snapshot_agent_skill_versions_if_matches(
        &self,
        thread_id: &str,
        turn_id: &str,
        expected_agent_skill_versions_json: &str,
    ) -> Result<bool> {
        let thread_id = thread_id.to_owned();
        let turn_id = turn_id.to_owned();
        let expected_agent_skill_versions_json = expected_agent_skill_versions_json.to_owned();
        retry_snapshot_write(|| async {
            turn_runtime_snapshot::clear_agent_skill_versions_if_matches(
                &self.connection,
                thread_id.as_str(),
                turn_id.as_str(),
                expected_agent_skill_versions_json.as_str(),
                chrono::Utc::now().fixed_offset(),
            )
            .await
        })
        .await
    }

    pub async fn delete_turn_runtime_snapshot(&self, turn_id: &str) -> Result<u64> {
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let maintenance_changes = [self.connection.work_signal("maintenance.html").changing()];
        let maintenance_result = async {
            let turn_id = turn_id.to_owned();
            retry_snapshot_write(|| async {
                let transaction = self.connection.begin().await?;
                let old_root =
                    pioneer_entity::turn_runtime_snapshot::Entity::find_by_id(turn_id.to_owned())
                        .select_only()
                        .column(pioneer_entity::turn_runtime_snapshot::Column::FrozenManifestId)
                        .into_tuple::<Option<String>>()
                        .one(&transaction)
                        .await?
                        .flatten();
                let deleted = turn_runtime_snapshot::delete_turn_runtime_snapshot(
                    &transaction,
                    turn_id.as_str(),
                )
                .await?;
                transaction.commit().await?;
                Ok((deleted, frozen_lifetime::changed_root_keys(old_root, None)))
            })
            .await
        }
        .await;
        let maintenance_changed = maintenance_result
            .as_ref()
            .is_ok_and(|(changed, _)| *changed > 0);
        if let Ok((_, keys)) = &maintenance_result {
            frozen_lifetime::complete_keys(frozen_change, keys, maintenance_changed);
        }
        if maintenance_result.is_ok() {
            for change in maintenance_changes {
                change.completed(maintenance_changed);
            }
        }
        maintenance_result.map(|(deleted, _)| deleted)
    }

    pub async fn delete_turn_runtime_snapshots_for_closed_turns(&self) -> Result<u64> {
        let maintenance_changes = [
            self.connection.work_signal("maintenance.frozen").changing(),
            self.connection.work_signal("maintenance.html").changing(),
        ];
        let maintenance_result = async {
            retry_snapshot_write(|| async {
                turn_runtime_snapshot::delete_turn_runtime_snapshots_for_closed_turns(
                    &self.connection,
                )
                .await
            })
            .await
        }
        .await;
        let maintenance_changed = maintenance_result
            .as_ref()
            .is_ok_and(|changed| *changed > 0);
        if maintenance_result.is_ok() {
            for change in maintenance_changes {
                change.completed(maintenance_changed);
            }
        }
        maintenance_result
    }

    pub async fn insert_task_run_conversation_snapshot_if_absent(
        &self,
        snapshot: NewTaskRunConversationSnapshot,
    ) -> Result<TaskRunConversationSnapshotRecord> {
        let change = self.connection.work_signal("tasks.lifecycle").changing();
        let existed = pioneer_entity::task_run_conversation_snapshot::Entity::find_by_id(
            snapshot.run_id.clone(),
        )
        .select_only()
        .column(pioneer_entity::task_run_conversation_snapshot::Column::RunId)
        .into_tuple::<String>()
        .one(&self.connection)
        .await?
        .is_some();
        let task_result = async {
            let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
            let frozen_task_keys = vec![format!("t:{}", snapshot.task_id)];
            let maintenance_change = self.connection.work_signal("maintenance.html").changing();
            let maintenance_result = async {
                let root = frozen_lifetime::PreparedFrozenRoot::history(&snapshot.history_json)?;
                let prepared = root
                    .locator()
                    .is_none()
                    .then(|| {
                        pioneer_crud_payloads::storage::PreparedPayload::new(&snapshot.history_json)
                    })
                    .transpose()?;
                retry_snapshot_write(|| async {
                    // An already accepted winner is a complete logical read. Do not
                    // acquire/replace its root or replay a committed write to hydrate.
                    if let Some(winner) = task_run_conversation_snapshot::get_scoped(
                        &self.connection,
                        &snapshot.run_id,
                    )
                    .await?
                    {
                        return Ok((winner, false));
                    }
                    let transaction = self.connection.begin().await?;
                    root.verify(&transaction, &snapshot.workspace_id).await?;
                    let (stored, inserted) = task_run_conversation_snapshot::insert_if_absent(
                        &transaction,
                        &snapshot,
                        root.locator(),
                        prepared.as_ref(),
                    )
                    .await?;
                    if !inserted {
                        transaction.rollback().await?;
                        return Err(pioneer_crud_conversations::turn_payload::PreflightRace.into());
                    }
                    task_child_producers::run(&transaction, &snapshot.run_id).await?;
                    transaction.commit().await?;
                    // The bytes prepared by the winning transaction remain
                    // available even if the row is deleted after commit.
                    Ok((
                        task_run_conversation_snapshot::record_from_bytes(
                            stored,
                            snapshot.history_json.clone(),
                        ),
                        true,
                    ))
                })
                .await
            }
            .await;
            let maintenance_changed = maintenance_result
                .as_ref()
                .is_ok_and(|(_, inserted)| *inserted);
            if maintenance_result.is_ok() {
                frozen_change.completed_keys(if maintenance_changed {
                    &frozen_task_keys
                } else {
                    &[]
                });
                maintenance_change.completed(maintenance_changed);
            }
            maintenance_result.map(|(record, _)| record)
        }
        .await;
        if task_result.is_ok() {
            change.completed(!existed);
        }
        task_result
    }

    pub async fn delete_task_run_conversation_snapshot(&self, run_id: &str) -> Result<u64> {
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let maintenance_changes = [
            self.connection
                .work_signal("maintenance.portable")
                .changing(),
            self.connection.work_signal("maintenance.html").changing(),
        ];
        let maintenance_result = async {
            retry_snapshot_write(|| async {
                let tx = self.connection.begin().await?;
                let old_root = pioneer_entity::task_run_conversation_snapshot::Entity::find_by_id(
                    run_id.to_owned(),
                )
                .select_only()
                .column(pioneer_entity::task_run_conversation_snapshot::Column::FrozenManifestId)
                .into_tuple::<Option<String>>()
                .one(&tx)
                .await?
                .flatten();

                let deleted = task_run_conversation_snapshot::delete_by_run(&tx, run_id).await?;
                if deleted > 0 {
                    task_child_producers::run(&tx, run_id).await?;
                }
                tx.commit().await?;
                Ok((deleted, frozen_lifetime::changed_root_keys(old_root, None)))
            })
            .await
        }
        .await;
        let maintenance_changed = maintenance_result
            .as_ref()
            .is_ok_and(|(changed, _)| *changed > 0);
        if let Ok((_, keys)) = &maintenance_result {
            frozen_lifetime::complete_keys(frozen_change, keys, maintenance_changed);
        }
        if maintenance_result.is_ok() {
            for change in maintenance_changes {
                change.completed(maintenance_changed);
            }
        }
        maintenance_result.map(|(deleted, _)| deleted)
    }
}

#[cfg(test)]
#[path = "retained_snapshots_tests.rs"]
mod tests;
