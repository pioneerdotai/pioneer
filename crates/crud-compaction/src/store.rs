//! Scoped access and shared reader holds for compaction and frozen history.
use crate::background::CompletedHistoryCheck;
use crate::canonical_history::{CanonicalFragment, PagedSource, SourcePage};
use crate::checkpoint_apply::SourceAssertion;
use crate::runner_repository::{ManifestEntry, RunnerPlanRecord};
use pioneer_compaction::ModelBudget;

use crate::accepted_import::{DeliveryCheckpointImportSource, PreparedFrozenImport};
use crate::checkpoint_apply::CommitOutcome;
use crate::checkpoint_projection::{CheckpointBody, CheckpointMetadata};
use crate::history::{
    AcceptedTaskBasis, HistoryCausalBoundary, HistoryReadFence, HistoryTurnBoundary,
    TaskInputCopyAlias,
};
use crate::source_revision::HistoricalEventProjection;
use crate::task_output::{
    DeliveredTaskOutputCursor, DeliveredTaskOutputPage, TaskDeliveryOutputSnapshot,
    TaskOutputSnapshot,
};

use crate::checkpoint_proofs::CheckpointEdges;
use crate::frozen::LayoutScan;
use crate::frozen_import::FrozenImportRecord;
use crate::frozen_lifetime::{self, FrozenReadHold, ReaderCounts};
use crate::operations::OperationRecord;
use crate::sweep_work::retry_conflict;
use anyhow::Result;
use pioneer_compaction::SourceRef;
use pioneer_compaction::{
    Checkpoint, OperationSnapshot,
    frozen::{FrozenHistoryRef, FrozenMessageRef},
    runner::RunnerState,
};
use pioneer_crud_execution::cli_runtime_binding::{self, *};
use pioneer_crud_execution::gateway_control::{GatewayControlWake, control_work_change};
use pioneer_entity::compaction_frozen_history as history;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{EntityTrait, QuerySelect, TransactionTrait};
use std::future::Future;

#[derive(Clone)]
pub struct CompactionStore {
    pub(crate) connection: SqliteDatabase,
    pub(crate) frozen_readers: ReaderCounts,
}

impl CompactionStore {
    pub fn new(connection: SqliteDatabase, frozen_readers: ReaderCounts) -> Self {
        Self {
            connection,
            frozen_readers,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(connection: SqliteDatabase) -> Self {
        Self::new(connection, Default::default())
    }

    pub async fn compact_frozen_storage_quantum(
        &self,
        progress: &mut crate::frozen_maintenance::FrozenStorageLifetimeProgress,
    ) -> Result<bool> {
        progress
            .convert_storage(&self.with_maintenance_access().connection)
            .await
    }

    pub fn database_connection(&self) -> SqliteDatabase {
        self.connection.clone()
    }

    pub fn with_maintenance_access(&self) -> Self {
        Self::new(self.connection.maintenance(), self.frozen_readers.clone())
    }

    pub(crate) async fn run_serialized_write<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        retry_conflict(operation).await
    }
    pub async fn compaction_admit(
        &self,
        workspace: &str,
        thread: &str,
        snapshot: &OperationSnapshot,
    ) -> Result<OperationRecord> {
        let change = self.connection.work_signal("compaction").changing();
        let result = async {
            crate::operations::compaction_admit(&self.connection, workspace, thread, snapshot).await
        }
        .await;
        if result.is_ok() {
            change.completed(true);
        }
        result
    }

    pub async fn compaction_finish(&self, id: &str, status: &str, outcome: &str) -> Result<()> {
        let change = self.connection.work_signal("compaction").changing();
        let result = async {
            let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
            let maintenance_changes = [self.connection.work_signal("maintenance.html").changing()];
            let maintenance_result = async {
                crate::operations::compaction_finish(&self.connection, id, status, outcome).await
            }
            .await;
            let maintenance_changed = matches!(&maintenance_result, Ok(true));
            if maintenance_result.is_ok() {
                let frozen_keys = vec![format!("o:{}", id)];
                frozen_change.completed_keys(if maintenance_changed {
                    &frozen_keys
                } else {
                    &[]
                });
                for change in maintenance_changes {
                    change.completed(maintenance_changed);
                }
            }
            maintenance_result
        }
        .await;
        if let Ok(changed) = &result {
            change.completed(*changed);
        }
        result.map(|_| ())
    }

    pub async fn compaction_save_candidate(
        &self,
        checkpoint: &Checkpoint,
        portion: i64,
    ) -> Result<()> {
        let change = self.connection.work_signal("compaction").changing();
        let result = async {
            crate::checkpoint_candidate::compaction_save_candidate(
                &self.connection,
                checkpoint,
                portion,
            )
            .await
        }
        .await;
        if result.is_ok() {
            change.completed(true);
        }
        result
    }

    pub async fn compaction_checkpoint_edges(&self, id: &str) -> Result<Option<CheckpointEdges>> {
        crate::checkpoint_proofs::compaction_checkpoint_edges(self, id).await
    }

    pub async fn compaction_checkpoint(&self, id: &str) -> Result<Option<Checkpoint>> {
        crate::checkpoint_apply::compaction_checkpoint(&self.connection, id).await
    }

    pub async fn compaction_begin_frozen_history(
        &self,
        workspace: &str,
        owner_thread: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<()> {
        self.compaction_begin_frozen_history_with_imports(
            workspace,
            owner_thread,
            descriptor,
            0,
            crate::frozen_import::EMPTY_FROZEN_IMPORT_SHA256,
        )
        .await
    }

    pub async fn compaction_begin_frozen_history_with_imports(
        &self,
        workspace: &str,
        owner_thread: &str,
        descriptor: &FrozenHistoryRef,
        imports: u64,
        imports_sha256: &str,
    ) -> Result<()> {
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let maintenance_changes = [self.connection.work_signal("maintenance.html").changing()];
        let maintenance_result = async {
            crate::frozen_capture::compaction_begin_frozen_history_with_imports(
                &self.connection,
                workspace,
                owner_thread,
                descriptor,
                imports,
                imports_sha256,
            )
            .await
        }
        .await;
        let maintenance_changed = maintenance_result.is_ok();
        if maintenance_result.is_ok() {
            let frozen_keys = vec![format!("m:{}", descriptor.manifest_id)];
            frozen_change.completed_keys(if maintenance_changed {
                &frozen_keys
            } else {
                &[]
            });
            for change in maintenance_changes {
                change.completed(maintenance_changed);
            }
        }
        maintenance_result
    }

    pub async fn compaction_append_frozen_history(
        &self,
        workspace: &str,
        owner_thread: &str,
        manifest: &str,
        start: u64,
        messages: &[FrozenMessageRef],
    ) -> Result<()> {
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let maintenance_changes = [self.connection.work_signal("maintenance.html").changing()];
        let maintenance_result = async {
            crate::frozen_capture::compaction_append_frozen_history(
                &self.connection,
                workspace,
                owner_thread,
                manifest,
                start,
                messages,
            )
            .await
        }
        .await;
        let maintenance_changed = maintenance_result.is_ok();
        if maintenance_result.is_ok() {
            let frozen_keys = vec![format!("m:{}", manifest)];
            frozen_change.completed_keys(if maintenance_changed {
                &frozen_keys
            } else {
                &[]
            });
            for change in maintenance_changes {
                change.completed(maintenance_changed);
            }
        }
        maintenance_result
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn compaction_finish_frozen_history(
        &self,
        workspace: &str,
        owner_thread: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<bool> {
        use crate::frozen_capture as frozen;
        use sea_orm::TransactionTrait;
        let layout_check =
            frozen::ReadyLayoutCheck::prepare(&self.connection, &descriptor.manifest_id).await?;
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let finished = frozen::compaction_finish_frozen_history(
                &tx,
                workspace,
                owner_thread,
                descriptor,
                layout_check.as_ref(),
            )
            .await?;
            if finished {
                tx.commit().await?;
            } else {
                tx.rollback().await?;
            }
            Ok(finished)
        })
        .await
    }

    pub async fn compaction_finish_frozen_history_held(
        &self,
        workspace: &str,
        owner: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<FrozenReadHold> {
        let layout_check = crate::frozen_capture::ReadyLayoutCheck::prepare(
            &self.connection,
            &descriptor.manifest_id,
        )
        .await?;
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let maintenance_changes = [self.connection.work_signal("maintenance.html").changing()];
        let maintenance_result = async {
            self.run_serialized_write(|| async {
                use sea_orm::TransactionTrait;
                let tx = self.connection.begin().await?;
                anyhow::ensure!(
                    crate::frozen_capture::compaction_finish_frozen_history(
                        &tx,
                        workspace,
                        owner,
                        descriptor,
                        layout_check.as_ref()
                    )
                    .await?,
                    "frozen history publication failed"
                );
                crate::frozen_lifetime::exact_header(&tx, workspace, descriptor).await?;
                let hold = FrozenReadHold::register(&self.frozen_readers, &descriptor.manifest_id);
                tx.commit().await?;
                Ok(hold)
            })
            .await
        }
        .await;
        let maintenance_changed = maintenance_result.is_ok();
        if maintenance_result.is_ok() {
            let frozen_keys = vec![format!("m:{}", descriptor.manifest_id)];
            frozen_change.completed_keys(if maintenance_changed {
                &frozen_keys
            } else {
                &[]
            });
            for change in maintenance_changes {
                change.completed(maintenance_changed);
            }
        }
        maintenance_result
    }

    pub async fn compaction_runner_state(&self, operation: &str) -> Result<Option<RunnerState>> {
        crate::runner_repository::compaction_runner_state(&self.connection, operation).await
    }

    pub async fn compaction_resume_deadline(
        &self,
        operation: &str,
        execution_turn: &str,
        deadline_ms: u64,
    ) -> Result<bool> {
        crate::runner_resume::compaction_resume_deadline(
            &self.connection,
            operation,
            execution_turn,
            deadline_ms,
        )
        .await
    }

    pub async fn upsert_cli_runtime_thread_binding(
        &self,
        binding: NewCliRuntimeThreadBinding,
    ) -> Result<CliRuntimeThreadBindingRecord> {
        let control_wakes = vec![GatewayControlWake::Thread(
            binding.runtime_id.clone(),
            binding.thread_id.clone(),
        )];
        let html_change = self.connection.work_signal("maintenance.html").changing();
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let changes = ["gateway.control", "gateway.cli"]
            .map(|domain| control_work_change(&self.connection, domain, &control_wakes));
        let result = async {
            let root = frozen_lifetime::PreparedFrozenRoot::cli(&binding.resume_cursor_json, true)?;
            self.run_serialized_write(|| async {
                let transaction = self.connection.begin().await?;
                let old_root = pioneer_entity::thread_cli_runtime_binding::Entity::find_by_id(
                    binding.thread_id.clone(),
                )
                .select_only()
                .column(pioneer_entity::thread_cli_runtime_binding::Column::FrozenManifestId)
                .into_tuple::<Option<String>>()
                .one(&transaction)
                .await?
                .flatten();

                let before =
                    cli_runtime_binding::find_thread_binding(&transaction, &binding.thread_id)
                        .await?;
                let result = cli_runtime_binding::upsert_thread_binding(
                    &transaction,
                    binding.clone(),
                    &root,
                )
                .await?;
                let changed = before.as_ref() != Some(&result);
                let html_before =
                    before.map(|value| (value.workspace_id, value.resume_cursor_json));
                transaction.commit().await?;
                Ok((
                    result,
                    changed,
                    frozen_lifetime::changed_root_keys(old_root, root.locator()),
                    html_before,
                ))
            })
            .await
        }
        .await;
        if let Ok((binding, changed, keys, html_before)) = &result {
            html_change.completed(
                *changed
                    && frozen_lifetime::html_cli_root_changed(
                        html_before.as_ref(),
                        &binding.workspace_id,
                        &binding.resume_cursor_json,
                        true,
                    ),
            );
            frozen_lifetime::complete_keys(frozen_change, keys, *changed);
            for change in changes {
                change.completed(*changed);
            }
        }
        result.map(|(binding, _, _, _)| binding)
    }

    pub async fn insert_cli_fork_intent_if_absent(
        &self,
        pending: NewCliRuntimeThreadBinding,
    ) -> Result<bool> {
        let html_change = self.connection.work_signal("maintenance.html").changing();
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let result = async {
            let root = frozen_lifetime::PreparedFrozenRoot::cli(&pending.resume_cursor_json, true)?;
            self.run_serialized_write(|| async {
                let transaction = self.connection.begin().await?;
                let old_root = pioneer_entity::thread_cli_runtime_binding::Entity::find_by_id(
                    pending.thread_id.clone(),
                )
                .select_only()
                .column(pioneer_entity::thread_cli_runtime_binding::Column::FrozenManifestId)
                .into_tuple::<Option<String>>()
                .one(&transaction)
                .await?
                .flatten();

                let result = async {
                    cli_runtime_binding::insert_fork_intent_if_absent(
                        &transaction,
                        pending.clone(),
                        &root,
                    )
                    .await
                }
                .await?;
                transaction.commit().await?;
                Ok((
                    result,
                    frozen_lifetime::changed_root_keys(old_root, root.locator()),
                ))
            })
            .await
        }
        .await;
        if let Ok((value, keys)) = &result {
            html_change.completed(
                *value
                    && frozen_lifetime::html_cli_root_changed(
                        None,
                        &pending.workspace_id,
                        &pending.resume_cursor_json,
                        true,
                    ),
            );
            frozen_lifetime::complete_keys(frozen_change, keys, *value);
        }
        result.map(|(record, _)| record)
    }

    pub async fn discard_rejected_cli_fork_intent(
        &self,
        thread_id: &str,
        runtime_id: &str,
        source_native_thread_id: &str,
        expected_cursor_json: &str,
    ) -> Result<bool> {
        let html_change = self.connection.work_signal("maintenance.html").changing();
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let frozen_keys = frozen_lifetime::PreparedFrozenRoot::cli(expected_cursor_json, true)
            .ok()
            .map(|root| {
                root.locator()
                    .into_iter()
                    .map(|id| format!("m:{id}"))
                    .collect::<Vec<_>>()
            });
        let maintenance_result = async {
            self.run_serialized_write(|| async {
                cli_runtime_binding::discard_rejected_fork_intent(
                    &self.connection,
                    thread_id,
                    runtime_id,
                    source_native_thread_id,
                    expected_cursor_json,
                )
                .await
            })
            .await
        }
        .await;
        let maintenance_changed = maintenance_result.as_ref().is_ok_and(|changed| *changed);
        if maintenance_result.is_ok() {
            html_change.completed(
                maintenance_changed
                    && frozen_lifetime::html_cli_root_changed(None, "", expected_cursor_json, true),
            );
            frozen_lifetime::complete_keys(frozen_change, &frozen_keys, maintenance_changed);
        }
        maintenance_result
    }

    pub async fn confirm_marked_cli_fork_intent(
        &self,
        pending: &CliRuntimeThreadBindingRecord,
        fork_native_thread_id: &str,
        native_cwd: Option<String>,
        native_model: Option<String>,
        resume_cursor_json: String,
        updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<CliRuntimeThreadBindingRecord> {
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let html_change = self.connection.work_signal("maintenance.html").changing();
        let result = async {
            let root = frozen_lifetime::PreparedFrozenRoot::cli(&resume_cursor_json, true)?;
            self.run_serialized_write(|| async {
                let transaction = self.connection.begin().await?;
                let old_root = pioneer_entity::thread_cli_runtime_binding::Entity::find_by_id(
                    pending.thread_id.clone(),
                )
                .select_only()
                .column(pioneer_entity::thread_cli_runtime_binding::Column::FrozenManifestId)
                .into_tuple::<Option<String>>()
                .one(&transaction)
                .await?
                .flatten();

                let html_before =
                    cli_runtime_binding::find_thread_binding(&transaction, &pending.thread_id)
                        .await?
                        .map(|value| (value.workspace_id, value.resume_cursor_json));
                let result = async {
                    cli_runtime_binding::confirm_marked_fork_intent(
                        &transaction,
                        pending,
                        fork_native_thread_id,
                        native_cwd.clone(),
                        native_model.clone(),
                        resume_cursor_json.clone(),
                        updated_at,
                        &root,
                    )
                    .await
                }
                .await?;
                transaction.commit().await?;
                Ok((
                    result,
                    frozen_lifetime::changed_root_keys(old_root, root.locator()),
                    html_before,
                ))
            })
            .await
        }
        .await;
        if let Ok((binding, keys, html_before)) = &result {
            html_change.completed(frozen_lifetime::html_cli_root_changed(
                html_before.as_ref(),
                &binding.workspace_id,
                &binding.resume_cursor_json,
                true,
            ));
            frozen_lifetime::complete_keys(frozen_change, keys, true);
        }
        result.map(|(record, _, _)| record)
    }

    pub async fn update_cli_runtime_thread_resume_cursor(
        &self,
        thread_id: &str,
        expected_native_thread_id: &str,
        expected_cursor_json: &str,
        resume_cursor_json: String,
        updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<CliRuntimeThreadBindingRecord> {
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let html_change = self.connection.work_signal("maintenance.html").changing();
        let maintenance_result = async {
            let thread_id = thread_id.to_owned();
            let expected_native_thread_id = expected_native_thread_id.to_owned();
            let expected_cursor_json = expected_cursor_json.to_owned();
            let root = frozen_lifetime::PreparedFrozenRoot::cli(&resume_cursor_json, true)?;
            self.run_serialized_write(|| async {
                let transaction = self.connection.begin().await?;
                let old_root = pioneer_entity::thread_cli_runtime_binding::Entity::find_by_id(
                    thread_id.clone(),
                )
                .select_only()
                .column(pioneer_entity::thread_cli_runtime_binding::Column::FrozenManifestId)
                .into_tuple::<Option<String>>()
                .one(&transaction)
                .await?
                .flatten();

                let html_before =
                    cli_runtime_binding::find_thread_binding(&transaction, &thread_id)
                        .await?
                        .map(|value| (value.workspace_id, value.resume_cursor_json));
                let result = async {
                    cli_runtime_binding::update_thread_resume_cursor(
                        &transaction,
                        thread_id.as_str(),
                        expected_native_thread_id.as_str(),
                        expected_cursor_json.as_str(),
                        resume_cursor_json.clone(),
                        updated_at,
                        &root,
                    )
                    .await
                }
                .await?;
                transaction.commit().await?;
                Ok((
                    result,
                    frozen_lifetime::changed_root_keys(old_root, root.locator()),
                    html_before,
                ))
            })
            .await
        }
        .await;
        if let Ok((binding, keys, html_before)) = &maintenance_result {
            html_change.completed(frozen_lifetime::html_cli_root_changed(
                html_before.as_ref(),
                &binding.workspace_id,
                &binding.resume_cursor_json,
                true,
            ));
            frozen_lifetime::complete_keys(frozen_change, keys, true);
        }
        maintenance_result.map(|(record, _, _)| record)
    }

    pub async fn prepare_claude_provider_session_binding(
        &self,
        request: PrepareClaudeProviderSessionBinding,
    ) -> Result<PreparedClaudeProviderSessionBinding> {
        let html_change = self.connection.work_signal("maintenance.html").changing();
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let result = async {
            let root = frozen_lifetime::PreparedFrozenRoot::cli(
                &request.thread_binding.resume_cursor_json,
                true,
            )?;
            self.run_serialized_write(|| async {
                let transaction = self.connection.begin().await?;
                let old_root = pioneer_entity::thread_cli_runtime_binding::Entity::find_by_id(
                    request.thread_binding.thread_id.clone(),
                )
                .select_only()
                .column(pioneer_entity::thread_cli_runtime_binding::Column::FrozenManifestId)
                .into_tuple::<Option<String>>()
                .one(&transaction)
                .await?
                .flatten();

                let before = cli_runtime_binding::find_thread_binding(
                    &transaction,
                    &request.thread_binding.thread_id,
                )
                .await?;
                let result = async {
                    cli_runtime_binding::prepare_claude_provider_session_binding(
                        &transaction,
                        request.clone(),
                        &root,
                    )
                    .await
                }
                .await?;
                let changed = before.as_ref() != Some(&result.binding);
                let html_before =
                    before.map(|value| (value.workspace_id, value.resume_cursor_json));
                transaction.commit().await?;
                Ok((
                    result,
                    changed,
                    frozen_lifetime::changed_root_keys(old_root, root.locator()),
                    html_before,
                ))
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            html_change.completed(
                value.1
                    && frozen_lifetime::html_cli_root_changed(
                        value.3.as_ref(),
                        &value.0.binding.workspace_id,
                        &value.0.binding.resume_cursor_json,
                        true,
                    ),
            );
            frozen_lifetime::complete_keys(frozen_change, &value.2, value.1);
        }
        result.map(|(binding, _, _, _)| binding)
    }

    pub async fn upsert_cli_runtime_turn_binding(
        &self,
        binding: NewCliRuntimeTurnBinding,
    ) -> Result<CliRuntimeTurnBindingRecord> {
        let control_wakes = vec![
            GatewayControlWake::Turn(binding.turn_id.clone()),
            GatewayControlWake::CommandTurn(binding.turn_id.clone()),
        ];
        let html_change = self.connection.work_signal("maintenance.html").changing();
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let changes = [
            "gateway.control",
            "gateway.cli",
            "maintenance.portable",
            "maintenance.native",
        ]
        .map(|domain| control_work_change(&self.connection, domain, &control_wakes));
        let task_change = self.connection.work_signal("tasks.lifecycle").changing();
        let result = async {
            let root =
                frozen_lifetime::PreparedFrozenRoot::cli(&binding.input_mapping_json, false)?;
            let mapping = pioneer_crud_execution::input_mapping::InputMapping::prepare(
                &binding.input_mapping_json,
                root.locator().is_some(),
            )?;
            self.run_serialized_write(|| async {
                let transaction = self.connection.begin().await?;
                let old_root = pioneer_entity::turn_cli_runtime_binding::Entity::find_by_id(
                    binding.turn_id.clone(),
                )
                .select_only()
                .column(pioneer_entity::turn_cli_runtime_binding::Column::FrozenManifestId)
                .into_tuple::<Option<String>>()
                .one(&transaction)
                .await?
                .flatten();

                let before =
                    cli_runtime_binding::find_turn_binding_control(&transaction, &binding.turn_id)
                        .await?;
                let stored = cli_runtime_binding::upsert_turn_binding(
                    &transaction,
                    binding.clone(),
                    mapping.clone(),
                    &root,
                )
                .await?;
                let changed = before.as_ref().is_none_or(|before| {
                    !cli_runtime_binding::same_binding_version(before, &stored)
                });
                let task_changed = before.as_ref().is_none_or(|before| {
                    before.thread_id != stored.thread_id
                        || before.continuation_thread_id != stored.continuation_thread_id
                        || before.status != stored.status
                        || before.native_turn_id != stored.native_turn_id
                });
                let new_root = root.locator();
                let html_changed = before.map_or_else(
                    || new_root.is_some() || mapping.has_inline_history(),
                    |before| {
                        old_root != new_root
                            || before.input_mapping_has_inline_history
                                != mapping.has_inline_history()
                            || (before.workspace_id != stored.workspace_id
                                && (new_root.is_some() || mapping.has_inline_history()))
                    },
                );
                transaction.commit().await?;
                Ok((
                    stored,
                    changed,
                    task_changed,
                    frozen_lifetime::changed_root_keys(old_root, root.locator()),
                    html_changed,
                ))
            })
            .await
        }
        .await;
        if let Ok((_, changed, task_changed, keys, html_changed)) = &result {
            html_change.completed(*changed && *html_changed);
            frozen_lifetime::complete_keys(frozen_change, keys, *changed);
            for change in changes {
                change.completed(*changed);
            }
            task_change.completed(*task_changed);
        }
        let (binding, _, _, _, _) = result?;
        cli_runtime_binding::hydrate_record(&self.connection, binding).await
    }

    pub async fn acquire_frozen_header(
        &self,
        workspace: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<(history::Model, FrozenReadHold)> {
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let (header, hold) = crate::frozen_lifetime::acquire_header(
                &tx,
                &self.frozen_readers,
                workspace,
                descriptor,
            )
            .await?;
            tx.commit().await?;
            Ok((header, hold))
        })
        .await
    }

    pub async fn compaction_acquire_frozen_history(
        &self,
        workspace: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<FrozenReadHold> {
        let (header, hold) = self.acquire_frozen_header(workspace, descriptor).await?;
        let mut scan = LayoutScan::new(header, 0, 2)?;
        while !scan.done() {
            scan.step(&self.connection).await?;
        }
        Ok(hold)
    }
}

impl CompactionStore {
    pub async fn compaction_frozen_history_owner(
        &self,
        workspace: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<Option<String>> {
        crate::frozen_capture::compaction_frozen_history_owner(
            &self.connection,
            workspace,
            descriptor,
        )
        .await
    }

    pub async fn compaction_frozen_history_page(
        &self,
        workspace: &str,
        owner_thread: &str,
        manifest: &str,
        start: u64,
    ) -> Result<Vec<FrozenMessageRef>> {
        crate::frozen_capture::compaction_frozen_history_page(
            &self.connection,
            workspace,
            owner_thread,
            manifest,
            start,
        )
        .await
    }

    pub async fn compaction_frozen_import_page(
        &self,
        workspace: &str,
        owner: &str,
        manifest: &str,
        start: u64,
    ) -> Result<Vec<FrozenImportRecord>> {
        crate::frozen_import::compaction_frozen_import_page(
            &self.connection,
            workspace,
            owner,
            manifest,
            start,
        )
        .await
    }

    pub async fn get_cli_runtime_thread_binding(
        &self,
        thread_id: &str,
    ) -> Result<Option<CliRuntimeThreadBindingRecord>> {
        let thread_id = thread_id.to_owned();
        cli_runtime_binding::find_thread_binding(&self.connection, thread_id.as_str()).await
    }
}

impl CompactionStore {
    pub async fn compaction_operation(&self, id: &str) -> Result<Option<OperationRecord>> {
        crate::operations::compaction_operation(&self.connection, id).await
    }

    pub async fn compaction_sources_current(
        &self,
        workspace: &str,
        thread: &str,
        sources: &[SourceRef],
    ) -> Result<bool> {
        crate::source_revision::compaction_sources_current(
            &self.connection,
            workspace,
            thread,
            sources,
        )
        .await
    }

    pub async fn compaction_head(&self, owner: &str) -> Result<Option<String>> {
        crate::operations::compaction_head(&self.connection, owner).await
    }

    pub async fn compaction_apply_runner(
        &self,
        operation: &str,
        state: &RunnerState,
        expected_head: Option<&str>,
    ) -> Result<CommitOutcome> {
        let change = self.connection.work_signal("compaction").changing();
        let result = async {
            let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
            let maintenance_changes = [self.connection.work_signal("maintenance.html").changing()];
            let maintenance_result = async {
                crate::runner_publication::compaction_apply_runner(
                    self,
                    operation,
                    state,
                    expected_head,
                )
                .await
            }
            .await;
            let maintenance_changed = matches!(&maintenance_result, Ok(CommitOutcome::Applied));
            if maintenance_result.is_ok() {
                let frozen_keys = std::iter::once(format!("o:{operation}"))
                    .chain(expected_head.into_iter().map(|id| format!("c:{id}")))
                    .collect::<Vec<_>>();
                frozen_change.completed_keys(if maintenance_changed {
                    &frozen_keys
                } else {
                    &[]
                });
                for change in maintenance_changes {
                    change.completed(maintenance_changed);
                }
            }
            maintenance_result
        }
        .await;
        if result.is_ok() {
            change.completed(matches!(&result, Ok(CommitOutcome::Applied)));
        }
        result
    }
}

impl CompactionStore {
    pub async fn compaction_reference_thread(
        &self,
        workspace: &str,
        source: &SourceRef,
    ) -> Result<Option<String>> {
        crate::source_revision::compaction_reference_thread(&self.connection, workspace, source)
            .await
    }

    pub async fn compaction_prepare_frozen_import(
        &self,
        workspace: &str,
        destination: &str,
        delivery: &str,
        acknowledgement: &SourceRef,
        output_ordinal: u64,
        source_thread: &str,
        source: &SourceRef,
    ) -> Result<PreparedFrozenImport> {
        crate::accepted_import::compaction_prepare_frozen_import(
            self,
            workspace,
            destination,
            delivery,
            acknowledgement,
            output_ordinal,
            source_thread,
            source,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn compaction_prepare_delivery_checkpoint_imports(
        &self,
        workspace: &str,
        destination: &str,
        delivery: &str,
        acknowledgement: &SourceRef,
        sources: &[DeliveryCheckpointImportSource],
        checkpoint_thread: &str,
        checkpoint: &SourceRef,
    ) -> Result<Vec<PreparedFrozenImport>> {
        crate::accepted_import::compaction_prepare_delivery_checkpoint_imports(
            self,
            workspace,
            destination,
            delivery,
            acknowledgement,
            sources,
            checkpoint_thread,
            checkpoint,
        )
        .await
    }

    pub async fn compaction_prepare_accepted_import(
        &self,
        workspace: &str,
        destination: &str,
        turn: &str,
        ordinal: u64,
    ) -> Result<PreparedFrozenImport> {
        crate::accepted_import::compaction_prepare_accepted_import(
            self,
            workspace,
            destination,
            turn,
            ordinal,
        )
        .await
    }

    pub async fn compaction_prepare_accepted_checkpoint_import(
        &self,
        workspace: &str,
        destination: &str,
        turn: &str,
        ordinal: u64,
        checkpoint_thread: &str,
        checkpoint: &SourceRef,
    ) -> Result<PreparedFrozenImport> {
        crate::accepted_import::compaction_prepare_accepted_checkpoint_import(
            self,
            workspace,
            destination,
            turn,
            ordinal,
            checkpoint_thread,
            checkpoint,
        )
        .await
    }

    pub async fn compaction_prepare_accepted_checkpoint_imports(
        &self,
        workspace: &str,
        destination: &str,
        turn: &str,
        ordinals: &[u64],
        checkpoint_thread: &str,
        checkpoint: &SourceRef,
    ) -> Result<Vec<PreparedFrozenImport>> {
        crate::accepted_import::compaction_prepare_accepted_checkpoint_imports(
            self,
            workspace,
            destination,
            turn,
            ordinals,
            checkpoint_thread,
            checkpoint,
        )
        .await
    }

    pub async fn compaction_append_frozen_imports(
        &self,
        workspace: &str,
        owner: &str,
        manifest: &str,
        start: u64,
        imports: &[(u64, PreparedFrozenImport)],
    ) -> Result<()> {
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let maintenance_changes = [self.connection.work_signal("maintenance.html").changing()];
        let maintenance_result = async {
            crate::accepted_import::compaction_append_frozen_imports(
                self, workspace, owner, manifest, start, imports,
            )
            .await
        }
        .await;
        let maintenance_changed = maintenance_result.is_ok();
        if maintenance_result.is_ok() {
            let frozen_keys = vec![format!("m:{}", manifest)];
            frozen_change.completed_keys(if maintenance_changed {
                &frozen_keys
            } else {
                &[]
            });
            for change in maintenance_changes {
                change.completed(maintenance_changed);
            }
        }
        maintenance_result
    }

    pub async fn compaction_frozen_import_state(
        &self,
        workspace: &str,
        owner: &str,
        manifest: &str,
    ) -> Result<Option<(u64, String)>> {
        crate::accepted_import::compaction_frozen_import_state(
            &self.connection,
            workspace,
            owner,
            manifest,
        )
        .await
    }

    pub async fn compaction_task_basis_snapshot(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) -> Result<Option<AcceptedTaskBasis>> {
        self.run_serialized_write(|| async {
            crate::history::compaction_task_basis_snapshot(
                &self.connection,
                workspace,
                thread,
                turn,
            )
            .await
        })
        .await
    }

    pub async fn compaction_record_task_output(
        &self,
        workspace: &str,
        task_run_turn: &str,
        history: &FrozenHistoryRef,
    ) -> Result<TaskOutputSnapshot> {
        let change = self.connection.work_signal("compaction").changing();
        let result = async {
            let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
            let maintenance_changes = [self.connection.work_signal("maintenance.html").changing()];
            let maintenance_result = async {
                crate::task_output::compaction_record_task_output(
                    &self.connection,
                    workspace,
                    task_run_turn,
                    history,
                )
                .await
            }
            .await;
            let maintenance_changed = maintenance_result.is_ok();
            if maintenance_result.is_ok() {
                let frozen_keys = vec![format!("m:{}", history.manifest_id)];
                frozen_change.completed_keys(if maintenance_changed {
                    &frozen_keys
                } else {
                    &[]
                });
                for change in maintenance_changes {
                    change.completed(maintenance_changed);
                }
            }
            maintenance_result
        }
        .await;
        if result.is_ok() {
            change.completed(true);
        }
        result
    }

    pub async fn compaction_delivery_output(
        &self,
        workspace: &str,
        delivery: &str,
    ) -> Result<Option<TaskDeliveryOutputSnapshot>> {
        crate::task_output::compaction_delivery_output(&self.connection, workspace, delivery).await
    }
    pub async fn html_preview_archived_frozen_result(
        &self,
        workspace: &str,
        reference: &FrozenMessageRef,
    ) -> Result<Option<(String, String, SourceRef)>> {
        pioneer_crud_artifacts::html_preview_receipts::html_preview_archived_frozen_result(
            &self.connection,
            workspace,
            reference,
        )
        .await
    }
}

impl CompactionStore {
    pub async fn compaction_admit_for_turn(
        &self,
        workspace: &str,
        thread: &str,
        snapshot: &OperationSnapshot,
        execution_turn: Option<&str>,
    ) -> Result<OperationRecord> {
        let change = self.connection.work_signal("compaction").changing();
        let result = async {
            crate::operations::compaction_admit_for_turn(
                &self.connection,
                workspace,
                thread,
                snapshot,
                execution_turn,
            )
            .await
        }
        .await;
        if result.is_ok() {
            change.completed(true);
        }
        result
    }

    pub async fn compaction_claim_attempt(
        &self,
        id: &str,
        expected_attempts: i64,
        now_ms: i64,
        transient_retry: bool,
        correction: bool,
    ) -> Result<bool> {
        let change = self.connection.work_signal("compaction").changing();
        let result = async {
            crate::operations::compaction_claim_attempt(
                &self.connection,
                id,
                expected_attempts,
                now_ms,
                transient_retry,
                correction,
            )
            .await
        }
        .await;
        if let Ok(changed) = &result {
            change.completed(*changed);
        }
        result
    }

    pub async fn compaction_apply(
        &self,
        checkpoint: &Checkpoint,
        expected_head: Option<&str>,
        assertions: &[SourceAssertion],
    ) -> Result<CommitOutcome> {
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let frozen_keys = std::iter::once(format!("o:{}", checkpoint.operation_id))
            .chain(expected_head.into_iter().map(|id| format!("c:{id}")))
            .chain(checkpoint.previous.iter().map(|id| format!("c:{id}")))
            .collect::<Vec<_>>();
        let maintenance_changes = [self.connection.work_signal("maintenance.html").changing()];
        let maintenance_result = async {
            crate::checkpoint_apply::compaction_apply(self, checkpoint, expected_head, assertions)
                .await
        }
        .await;
        let maintenance_changed = matches!(&maintenance_result, Ok(CommitOutcome::Applied));
        if maintenance_result.is_ok() {
            frozen_change.completed_keys(if maintenance_changed {
                &frozen_keys
            } else {
                &[]
            });
            for change in maintenance_changes {
                change.completed(maintenance_changed);
            }
        }
        maintenance_result
    }

    pub async fn compaction_reference_fragment(
        &self,
        workspace: &str,
        thread: &str,
        reference: &SourceRef,
        character_offset: u64,
    ) -> Result<Option<CanonicalFragment>> {
        self.run_serialized_write(|| async {
            crate::canonical_history::compaction_reference_fragment(
                &self.connection,
                workspace,
                thread,
                reference,
                character_offset,
            )
            .await
        })
        .await
    }

    pub async fn compaction_enqueue_native_history_check(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        descriptor: &str,
    ) -> Result<()> {
        let change = self.connection.work_signal("compaction").changing();
        let result = async {
            crate::background::compaction_enqueue_native_history_check(
                &self.connection,
                workspace,
                thread,
                turn,
                descriptor,
            )
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(*value);
        }
        result.map(|_| ())
    }

    pub async fn compaction_record_event_projection(
        &self,
        workspace: &str,
        thread: &str,
        source: &SourceRef,
        event: &pioneer_crud_conversations::events::CanonicalTurnEventPayload,
    ) -> Result<bool> {
        let maintenance_changes = [self.connection.work_signal("maintenance.html").changing()];
        let maintenance_result = async {
            crate::history::compaction_record_event_projection(
                &self.connection,
                workspace,
                thread,
                source,
                event,
            )
            .await
        }
        .await;
        let maintenance_changed = maintenance_result.as_ref().is_ok_and(|changed| *changed);
        if maintenance_result.is_ok() {
            for change in maintenance_changes {
                change.completed(maintenance_changed);
            }
        }
        maintenance_result
    }

    pub async fn compaction_stop_execution(
        &self,
        workspace: &str,
        thread: &str,
        owner: &str,
        turn: &str,
    ) -> Result<()> {
        let change = self.connection.work_signal("compaction").changing();
        let result = async {
            crate::stop_execution::compaction_stop_execution(self, workspace, thread, owner, turn)
                .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(*value);
        }
        result.map(|_| ())
    }

    pub async fn compaction_bind_execution_turn(&self, operation: &str, turn: &str) -> Result<()> {
        let change = self.connection.work_signal("compaction").changing();
        let result = async {
            crate::runner_repository::compaction_bind_execution_turn(
                &self.connection,
                operation,
                turn,
            )
            .await
        }
        .await;
        if let Ok(changed) = &result {
            change.completed(*changed);
        }
        result.map(|_| ())
    }

    pub async fn compaction_prepare_runner(
        &self,
        operation: &str,
        budget: &ModelBudget,
        source_count: u64,
        reference_count: u64,
    ) -> Result<()> {
        let change = self.connection.work_signal("compaction").changing();
        let result = async {
            crate::runner_repository::compaction_prepare_runner(
                &self.connection,
                operation,
                budget,
                source_count,
                reference_count,
            )
            .await
        }
        .await;
        if let Ok(changed) = &result {
            change.completed(*changed);
        }
        result.map(|_| ())
    }

    pub async fn compaction_activate_runner(
        &self,
        operation: &str,
        initial: &RunnerState,
    ) -> Result<()> {
        let change = self.connection.work_signal("compaction").changing();
        let result = async {
            crate::runner_writes::compaction_activate_runner(&self.connection, operation, initial)
                .await
        }
        .await;
        if let Ok(changed) = &result {
            change.completed(*changed);
        }
        result.map(|_| ())
    }

    pub async fn compaction_finish_execution(
        &self,
        operation: &str,
        execution_turn: Option<&str>,
        deadline_ms: u64,
        status: &str,
        outcome: &str,
    ) -> Result<()> {
        let change = self.connection.work_signal("compaction").changing();
        let result = async {
            let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
            let maintenance_changes = [self.connection.work_signal("maintenance.html").changing()];
            let maintenance_result = async {
                crate::runner_repository::compaction_finish_execution(
                    &self.connection,
                    operation,
                    execution_turn,
                    deadline_ms,
                    status,
                    outcome,
                )
                .await
            }
            .await;
            let maintenance_changed = matches!(&maintenance_result, Ok(true));
            if maintenance_result.is_ok() {
                let frozen_keys = vec![format!("o:{}", operation)];
                frozen_change.completed_keys(if maintenance_changed {
                    &frozen_keys
                } else {
                    &[]
                });
                for change in maintenance_changes {
                    change.completed(maintenance_changed);
                }
            }
            maintenance_result
        }
        .await;
        if let Ok(value) = &result {
            change.completed(*value);
        }
        result.map(|_| ())
    }

    pub async fn compaction_reconcile_runner_state(
        &self,
        operation: &str,
    ) -> Result<Option<RunnerState>> {
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let changes = ["compaction", "maintenance.html"]
            .map(|domain| self.connection.work_signal(domain).changing());
        let result = crate::runner_repository::compaction_reconcile_runner_state(
            &self.connection,
            operation,
        )
        .await;
        if let Ok((_, changed)) = &result {
            let frozen_keys = vec![format!("o:{}", operation)];
            frozen_change.completed_keys(if *changed { &frozen_keys } else { &[] });
            for change in changes {
                change.completed(*changed);
            }
        }
        result.map(|(state, _)| state)
    }

    pub async fn compaction_runner_transition(
        &self,
        operation: &str,
        expected: u64,
        next: &RunnerState,
        candidate: Option<&Checkpoint>,
    ) -> Result<bool> {
        let change = self.connection.work_signal("compaction").changing();
        let result = async {
            crate::runner_writes::compaction_runner_transition(
                &self.connection,
                operation,
                expected,
                next,
                candidate,
            )
            .await
        }
        .await;
        if let Ok(changed) = &result {
            change.completed(*changed);
        }
        result
    }

    pub async fn compaction_bind_source_projection(
        &self,
        operation: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<()> {
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let maintenance_changes = [self.connection.work_signal("maintenance.html").changing()];
        let prepared = crate::source_projection::PreparedSourceProjection::new(descriptor)?;
        let maintenance_result = self
            .run_serialized_write(|| async {
                let tx = self.connection.begin().await?;
                prepared.bind(&tx, operation).await?;
                tx.commit().await?;
                Ok(())
            })
            .await;
        let maintenance_changed = maintenance_result.is_ok();
        if maintenance_result.is_ok() {
            let frozen_keys = vec![format!("o:{}", operation)];
            frozen_change.completed_keys(if maintenance_changed {
                &frozen_keys
            } else {
                &[]
            });
            for change in maintenance_changes {
                change.completed(maintenance_changed);
            }
        }
        maintenance_result
    }

    pub async fn compaction_share_frozen_prefix(
        &self,
        workspace: &str,
        owner: &str,
        manifest: &str,
        messages: &[pioneer_compaction::frozen::FrozenMessageRef],
        imports: &[(u64, PreparedFrozenImport)],
    ) -> Result<(u64, u64)> {
        let frozen_change = self.connection.work_signal("maintenance.frozen").changing();
        let frozen_keys = vec![format!("m:{manifest}")];
        let maintenance_changes = [self.connection.work_signal("maintenance.html").changing()];
        let maintenance_result = async {
            let refs = messages
                .iter()
                .map(serde_json::to_string)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let proofs = imports
                .iter()
                .map(|(ordinal, p)| serde_json::to_string(&p.record_at(*ordinal)))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let messages = crate::frozen_storage::prepare(
                &self.connection,
                workspace,
                owner,
                manifest,
                0,
                &refs,
            )
            .await?;
            let imports = crate::frozen_storage::prepare(
                &self.connection,
                workspace,
                owner,
                manifest,
                1,
                &proofs,
            )
            .await?;
            crate::frozen_capture::mark_storage_registered(&self.connection, manifest).await?;
            Ok((messages, imports))
        }
        .await;
        let maintenance_changed = maintenance_result.is_ok();
        if maintenance_result.is_ok() {
            frozen_change.completed_keys(if maintenance_changed {
                &frozen_keys
            } else {
                &[]
            });
            for change in maintenance_changes {
                change.completed(maintenance_changed);
            }
        }
        maintenance_result
    }

    pub async fn next_compaction_background_deadline(
        &self,
        owned: &[(String, String)],
    ) -> Result<Option<i64>> {
        let history = if owned.len() >= 16 {
            None
        } else {
            crate::background::next_history_deadline(&self.connection, owned).await?
        };
        let lifecycle = crate::lifecycle_pending::next_deadline(&self.connection).await?;
        Ok(history.into_iter().chain(lifecycle).min().map(|ms| {
            ms.div_euclid(1000)
                .saturating_add(i64::from(ms.rem_euclid(1000) != 0))
        }))
    }

    pub async fn compaction_record_history_result(
        &self,
        id: &str,
        revision: i64,
        failures: i64,
        outcome: crate::history_check_result::HistoryCheckOutcome,
        diagnostic: &crate::history_check_result::HistoryCheckDiagnostic,
        now: i64,
    ) -> Result<bool> {
        let change = self.connection.work_signal("compaction").changing();
        let result = async {
            // Prepare bounded diagnostics before database admission.
            let json = serde_json::to_string(diagnostic)?;
            anyhow::ensure!(json.len() <= 4096, "history diagnostic exceeds bound");
            let diagnostic: serde_json::Value = serde_json::from_str(&json)?;
            self.run_serialized_write(|| {
                let diagnostic = diagnostic.clone();
                async move {
                    let tx = self.connection.begin().await?;
                    let recorded = crate::background::record_history_result(
                        &tx, id, revision, failures, outcome, diagnostic, now,
                    )
                    .await?;
                    tx.commit().await?;
                    Ok(recorded)
                }
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            let _ = value;
            change.completed(*value);
        }
        result
    }
}

impl CompactionStore {
    pub async fn compaction_reference_payload(
        &self,
        workspace: &str,
        thread: &str,
        reference: &SourceRef,
    ) -> Result<Option<String>> {
        self.run_serialized_write(|| async {
            crate::canonical_history::compaction_reference_payload(
                &self.connection,
                workspace,
                thread,
                reference,
            )
            .await
        })
        .await
    }
}

impl CompactionStore {
    pub async fn compaction_assert_history_scope(
        &self,
        workspace: &str,
        thread: &str,
    ) -> Result<()> {
        crate::source_payloads::require_history_scope(&self.connection, workspace, thread).await
    }

    pub async fn compaction_source_high_water(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        kind: PagedSource,
    ) -> Result<i64> {
        crate::canonical_history::compaction_source_high_water(
            &self.connection,
            workspace,
            thread,
            turn,
            kind,
        )
        .await
    }

    pub async fn compaction_source_page(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        kind: PagedSource,
        after: i64,
    ) -> Result<SourcePage> {
        crate::canonical_history::compaction_source_page(
            &self.connection,
            workspace,
            thread,
            turn,
            kind,
            after,
        )
        .await
    }

    pub async fn compaction_source_metadata_page(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        kind: PagedSource,
        after: i64,
    ) -> Result<SourcePage> {
        crate::canonical_history::compaction_source_metadata_page(
            &self.connection,
            workspace,
            thread,
            turn,
            kind,
            after,
        )
        .await
    }

    pub async fn compaction_source_metadata_page_at_fence(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        kind: PagedSource,
        after: i64,
        capture_order: i64,
    ) -> Result<SourcePage> {
        crate::canonical_history::compaction_source_metadata_page_at_fence(
            &self.connection,
            workspace,
            thread,
            turn,
            kind,
            after,
            capture_order,
        )
        .await
    }

    pub async fn compaction_source_metadata_page_before(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        kind: PagedSource,
        after: i64,
        capture_order: i64,
        before: Option<sea_orm::prelude::DateTimeWithTimeZone>,
    ) -> Result<SourcePage> {
        crate::canonical_history::compaction_source_page_inner_bounded(
            &self.connection,
            workspace,
            thread,
            turn,
            kind,
            after,
            false,
            capture_order,
            before,
        )
        .await
    }

    pub async fn compaction_item_created_at(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        id: &str,
    ) -> Result<Option<sea_orm::prelude::DateTimeWithTimeZone>> {
        crate::canonical_history::compaction_item_created_at(
            &self.connection,
            workspace,
            thread,
            turn,
            id,
        )
        .await
    }

    pub async fn compaction_operation_for_plan(
        &self,
        workspace: &str,
        thread: &str,
        owner: &str,
        fingerprint: &str,
    ) -> Result<Option<OperationRecord>> {
        crate::operations::compaction_operation_for_plan(
            &self.connection,
            workspace,
            thread,
            owner,
            fingerprint,
        )
        .await
    }

    pub async fn compaction_invalid_completion_progress(
        &self,
        workspace: &str,
        thread: &str,
        owner: &str,
    ) -> Result<Option<OperationRecord>> {
        crate::operations::compaction_invalid_completion_progress(
            &self.connection,
            workspace,
            thread,
            owner,
        )
        .await
    }

    pub async fn compaction_model_context_sources_current(
        &self,
        workspace: &str,
        thread: &str,
        sources: &[SourceRef],
    ) -> Result<bool> {
        crate::source_revision::compaction_model_context_sources_current(
            &self.connection,
            workspace,
            thread,
            sources,
        )
        .await
    }

    pub async fn compaction_historical_event_projections(
        &self,
        workspace: &str,
        thread: &str,
        sources: &[SourceRef],
    ) -> Result<Vec<HistoricalEventProjection>> {
        crate::source_revision::compaction_historical_event_projections(
            &self.connection,
            workspace,
            thread,
            sources,
        )
        .await
    }

    pub async fn compaction_checkpoint_body(&self, id: &str) -> Result<Option<CheckpointBody>> {
        crate::checkpoint_projection::compaction_checkpoint_body(&self.connection, id).await
    }

    pub async fn compaction_checkpoint_metadata(
        &self,
        id: &str,
    ) -> Result<Option<CheckpointMetadata>> {
        crate::checkpoint_projection::compaction_checkpoint_metadata(&self.connection, id).await
    }

    pub async fn compaction_source_fragment(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        id: &str,
        expected_revision: Option<&str>,
        character_offset: u64,
    ) -> Result<Option<CanonicalFragment>> {
        crate::canonical_history::compaction_source_fragment(
            &self.connection,
            workspace,
            thread,
            turn,
            id,
            expected_revision,
            character_offset,
        )
        .await
    }

    pub async fn compaction_tool_result_id(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
    ) -> Result<Option<String>> {
        crate::canonical_history::compaction_tool_result_id(
            &self.connection,
            workspace,
            thread,
            turn,
            item,
        )
        .await
    }

    pub async fn compaction_tool_result_fragment(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
        expected_revision: Option<&str>,
        character_offset: u64,
    ) -> Result<Option<CanonicalFragment>> {
        crate::canonical_history::compaction_tool_result_fragment(
            &self.connection,
            workspace,
            thread,
            turn,
            item,
            expected_revision,
            character_offset,
        )
        .await
    }

    pub async fn compaction_reference_payload_batch(
        &self,
        workspace: &str,
        thread: &str,
        references: &[SourceRef],
    ) -> Result<(usize, Vec<String>)> {
        self.run_serialized_write(|| async {
            crate::canonical_history::compaction_reference_payload_batch(
                &self.connection,
                workspace,
                thread,
                references,
            )
            .await
        })
        .await
    }

    pub async fn compaction_references_current(
        &self,
        workspace: &str,
        thread: &str,
        references: &[SourceRef],
    ) -> Result<bool> {
        crate::source_revision::compaction_sources_current(
            &self.connection,
            workspace,
            thread,
            references,
        )
        .await
    }

    pub async fn compaction_projection_version(
        &self,
        workspace: &str,
        thread: &str,
    ) -> Result<u64> {
        crate::canonical_history::compaction_projection_version(&self.connection, workspace, thread)
            .await
    }

    pub async fn compaction_checkpoint_source(
        &self,
        workspace: &str,
        thread: &str,
        checkpoint: &str,
    ) -> Result<Option<SourceRef>> {
        crate::source_revision::compaction_checkpoint_source(
            &self.connection,
            workspace,
            thread,
            checkpoint,
        )
        .await
    }

    pub async fn compaction_context_reference_for_item(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
        source: &str,
    ) -> Result<Option<SourceRef>> {
        crate::canonical_history::compaction_context_reference_for_item(
            &self.connection,
            workspace,
            thread,
            turn,
            item,
            source,
        )
        .await
    }

    pub async fn compaction_tool_item_reference(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
    ) -> Result<Option<SourceRef>> {
        crate::tool_sources::compaction_tool_item_reference(
            &self.connection,
            workspace,
            thread,
            turn,
            item,
        )
        .await
    }

    pub async fn compaction_replay_item_id(
        &self,
        workspace: &str,
        thread: &str,
        source: &SourceRef,
    ) -> Result<Option<String>> {
        crate::canonical_history::compaction_replay_item_id(
            &self.connection,
            workspace,
            thread,
            source,
        )
        .await
    }

    pub async fn compaction_item_reference(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
    ) -> Result<Option<SourceRef>> {
        crate::canonical_history::compaction_item_reference(
            &self.connection,
            workspace,
            thread,
            turn,
            item,
        )
        .await
    }

    pub async fn compaction_expand_lifecycle_scope(
        &self,
        seed: bool,
        clock: &(dyn Fn() -> i64 + Send + Sync),
    ) -> Result<u64> {
        let maintenance = self.with_maintenance_access();
        crate::lifecycle_pending::expand(&maintenance.connection, seed, clock).await
    }

    pub async fn compaction_due_lifecycle(
        &self,
        now_ms: i64,
    ) -> Result<Vec<crate::lifecycle_pending::CompactionLifecycleCandidate>> {
        let maintenance = self.with_maintenance_access();
        crate::lifecycle_pending::due(&maintenance.connection, now_ms).await
    }

    pub async fn compaction_claim_lifecycle(
        &self,
        candidate: &crate::lifecycle_pending::CompactionLifecycleCandidate,
        clock: &(dyn Fn() -> i64 + Send + Sync),
    ) -> Result<Option<crate::lifecycle_pending::CompactionLifecycleClaim>> {
        let maintenance = self.with_maintenance_access();
        crate::lifecycle_pending::claim(&maintenance.connection, candidate, clock).await
    }

    pub async fn compaction_history_check_is_current(&self, turn: &str) -> Result<bool> {
        crate::background::compaction_history_check_is_current(&self.connection, turn, None).await
    }

    pub async fn compaction_history_check_revision_is_current(
        &self,
        turn: &str,
        revision: i64,
    ) -> Result<bool> {
        crate::background::compaction_history_check_is_current(
            &self.connection,
            turn,
            Some(revision),
        )
        .await
    }

    pub async fn compaction_root_history_activity(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) -> Result<Option<i64>> {
        crate::background::root_history_activity(&self.connection, workspace, thread, turn).await
    }

    pub async fn compaction_root_history_busy(
        &self,
        workspace: &str,
        thread: &str,
    ) -> Result<bool> {
        crate::background::root_history_busy(&self.connection, workspace, thread).await
    }

    pub async fn compaction_root_for_execution(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) -> Result<Option<String>> {
        crate::background::root_for_history_execution(&self.connection, workspace, thread, turn)
            .await
    }

    pub async fn compaction_pending_history_checks(&self) -> Result<Vec<CompletedHistoryCheck>> {
        crate::background::compaction_pending_history_checks(&self.connection).await
    }

    pub async fn compaction_capture_history_check(
        &self,
        turn: &str,
        descriptor: &str,
    ) -> Result<Option<String>> {
        crate::background::compaction_capture_history_check(&self.connection, turn, descriptor)
            .await
    }

    pub async fn compaction_finish_history_check(&self, turn: &str, outcome: &str) -> Result<()> {
        crate::background::compaction_finish_history_check(&self.connection, turn, outcome).await
    }

    pub async fn compaction_turn_is_completed(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) -> Result<bool> {
        crate::history::compaction_turn_is_completed(&self.connection, workspace, thread, turn)
            .await
    }

    pub async fn compaction_legacy_task_basis_source(
        &self,
        workspace: &str,
        parent: &str,
        run: &str,
    ) -> Result<Option<SourceRef>> {
        crate::history::compaction_legacy_task_basis_source(
            &self.connection,
            workspace,
            parent,
            run,
        )
        .await
    }

    pub async fn compaction_task_basis_thread(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) -> Result<Option<String>> {
        crate::history::compaction_task_basis_thread(&self.connection, workspace, thread, turn)
            .await
    }

    pub async fn compaction_latest_task_basis_turn(
        &self,
        workspace: &str,
        thread: &str,
        fence: &HistoryReadFence,
    ) -> Result<Option<String>> {
        crate::history::compaction_latest_task_basis_turn(
            &self.connection,
            workspace,
            thread,
            fence,
        )
        .await
    }

    pub async fn compaction_task_delivery_command(
        &self,
        workspace: &str,
        thread: &str,
        source: &SourceRef,
    ) -> Result<Option<String>> {
        crate::history::compaction_task_delivery_command(
            &self.connection,
            workspace,
            thread,
            source,
        )
        .await
    }

    pub async fn compaction_task_input_copy_aliases(
        &self,
        workspace: &str,
        input_ids: &[String],
    ) -> Result<Vec<TaskInputCopyAlias>> {
        crate::history::compaction_task_input_copy_aliases(&self.connection, workspace, input_ids)
            .await
    }

    pub(crate) async fn compaction_failed_delivery_command(
        &self,
        workspace: &str,
        thread: &str,
        command: Option<&str>,
        source: Option<&SourceRef>,
        event_fence: i64,
        before: Option<sea_orm::prelude::DateTimeWithTimeZone>,
    ) -> Result<Option<String>> {
        crate::history::compaction_failed_delivery_command(
            &self.connection,
            workspace,
            thread,
            command,
            source,
            event_fence,
            before,
        )
        .await
    }

    pub async fn compaction_history_causal_boundary(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        fence: &HistoryReadFence,
    ) -> Result<HistoryCausalBoundary> {
        crate::history::compaction_history_causal_boundary(
            &self.connection,
            workspace,
            thread,
            turn,
            fence,
        )
        .await
    }

    pub async fn compaction_child_history_parent(
        &self,
        workspace: &str,
        child: &str,
    ) -> Result<Option<crate::history::ChildHistoryParent>> {
        crate::history::compaction_child_history_parent(&self.connection, workspace, child).await
    }

    pub async fn compaction_history_source_created_at(
        &self,
        workspace: &str,
        thread: &str,
        source: &SourceRef,
    ) -> Result<Option<sea_orm::prelude::DateTimeWithTimeZone>> {
        crate::history::compaction_history_source_created_at(
            &self.connection,
            workspace,
            thread,
            source,
        )
        .await
    }

    pub async fn compaction_history_read_fence(&self) -> Result<HistoryReadFence> {
        crate::history::compaction_history_read_fence(&self.connection).await
    }

    pub async fn compaction_history_turn_page(
        &self,
        workspace: &str,
        thread: &str,
        after: &str,
        fence: &HistoryReadFence,
    ) -> Result<Vec<HistoryTurnBoundary>> {
        crate::history::compaction_history_turn_page(
            &self.connection,
            workspace,
            thread,
            after,
            fence,
        )
        .await
    }

    pub async fn compaction_history_selected_turn_page(
        &self,
        workspace: &str,
        thread: &str,
        selected: &[String],
        fence: &HistoryReadFence,
    ) -> Result<Vec<HistoryTurnBoundary>> {
        crate::history::compaction_history_selected_turn_page(
            &self.connection,
            workspace,
            thread,
            selected,
            fence,
        )
        .await
    }

    pub async fn compaction_execution_cancelled(&self, operation: &str) -> Result<bool> {
        crate::runner_repository::compaction_execution_cancelled(&self.connection, operation).await
    }

    pub async fn compaction_runner_plan(
        &self,
        operation: &str,
    ) -> Result<Option<RunnerPlanRecord>> {
        crate::runner_repository::compaction_runner_plan(&self.connection, operation).await
    }

    pub async fn compaction_append_manifest(
        &self,
        operation: &str,
        entries: &[ManifestEntry],
    ) -> Result<()> {
        crate::runner_writes::compaction_append_manifest(&self.connection, operation, entries).await
    }

    pub async fn compaction_manifest_sources_current(&self, operation: &str) -> Result<bool> {
        crate::runner_publication::compaction_manifest_sources_current(&self.connection, operation)
            .await
    }

    pub async fn compaction_resume_invalid_completion(
        &self,
        operation: &str,
        execution_turn: &str,
        deadline_ms: u64,
    ) -> Result<bool> {
        crate::runner_resume::compaction_resume_invalid_completion(
            &self.connection,
            operation,
            execution_turn,
            deadline_ms,
        )
        .await
    }

    pub async fn compaction_manifest_page(
        &self,
        operation: &str,
        reference_only: bool,
        unit: u64,
        source_offset: u32,
    ) -> Result<Vec<ManifestEntry>> {
        crate::runner_repository::compaction_manifest_page(
            &self.connection,
            operation,
            reference_only,
            unit,
            source_offset,
        )
        .await
    }

    pub async fn compaction_bound_source_projection(
        &self,
        operation: &str,
    ) -> Result<Option<FrozenHistoryRef>> {
        crate::source_projection::compaction_bound_source_projection(&self.connection, operation)
            .await
    }

    pub async fn compaction_task_output(
        &self,
        workspace: &str,
        task_run_turn: &str,
    ) -> Result<Option<TaskOutputSnapshot>> {
        crate::task_output::compaction_task_output(&self.connection, workspace, task_run_turn).await
    }

    pub async fn compaction_delivered_output_page(
        &self,
        workspace: &str,
        thread: &str,
        cursor: &DeliveredTaskOutputCursor,
        fence: &HistoryReadFence,
    ) -> Result<DeliveredTaskOutputPage> {
        crate::task_output::compaction_delivered_output_page(
            &self.connection,
            workspace,
            thread,
            cursor,
            fence,
        )
        .await
    }

    pub async fn compaction_recheck_delivered_output_page(
        &self,
        workspace: &str,
        thread: &str,
        page: &DeliveredTaskOutputPage,
        fence: &HistoryReadFence,
    ) -> Result<DeliveredTaskOutputPage> {
        crate::task_output::compaction_recheck_delivered_output_page(
            &self.connection,
            workspace,
            thread,
            page,
            fence,
        )
        .await
    }

    pub async fn compaction_equivalent_frozen_history(
        &self,
        workspace: &str,
        owner: &str,
        descriptor: &FrozenHistoryRef,
        imports: u64,
        digest: &str,
    ) -> Result<Option<FrozenHistoryRef>> {
        crate::frozen_storage::equivalent(
            &self.connection,
            workspace,
            owner,
            descriptor,
            imports,
            digest,
        )
        .await
    }

    pub async fn compaction_due_history_checks_excluding(
        &self,
        now: i64,
        owned: &[(String, String)],
    ) -> Result<Vec<CompletedHistoryCheck>> {
        if owned.len() >= 16 {
            return Ok(Vec::new());
        }
        crate::background::compaction_due_history_checks_excluding(&self.connection, now, owned)
            .await
    }

    pub async fn compaction_due_history_checks(
        &self,
        now: i64,
    ) -> Result<Vec<CompletedHistoryCheck>> {
        crate::background::compaction_due_history_checks(&self.connection, now).await
    }

    pub async fn compaction_claim_history_check(
        &self,
        id: &str,
        revision: i64,
        now: i64,
    ) -> Result<Option<pioneer_entity::compaction_history_check::Model>> {
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let claimed = crate::background::claim_history_check(&tx, id, revision, now).await?;
            tx.commit().await?;
            Ok(claimed)
        })
        .await
    }

    pub async fn compaction_begin_history_attempt(
        &self,
        id: &str,
        revision: i64,
        descriptor: &str,
        hash: &str,
        now: i64,
    ) -> Result<Option<i64>> {
        crate::background::begin_history_attempt(
            &self.connection,
            id,
            revision,
            descriptor,
            hash,
            now,
        )
        .await
    }
}
