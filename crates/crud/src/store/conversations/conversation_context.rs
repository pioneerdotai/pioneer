use crate::{CrudStore, NewTurnLlmContextEntry, TurnLlmContextAppendOutcome, TurnLlmContextEntry};
use anyhow::{Context, Result};
use sea_orm::TransactionTrait;
use std::collections::BTreeMap;

impl CrudStore {
    pub async fn insert_turn_llm_context(
        &self,
        entry: NewTurnLlmContextEntry,
    ) -> Result<pioneer_entity::turn_llm_context::Model> {
        let task_change = self.connection.work_signal("tasks.lifecycle").changing();
        // Ordinary context appends change source revisions, not frozen roots or
        // retirement eligibility. A new preview result is relevant to HTML.
        let html_change = (entry.source == "tool_result_v2"
            && entry.tool_name.as_deref() == Some("html_preview"))
        .then(|| self.connection.work_signal("maintenance.html").changing());
        let result = self
            .run_serialized_write(|| async {
                let tx = self.connection.begin().await?;
                let row = crate::store::conversations::context_writes::insert_turn_llm_context(
                    &tx,
                    entry.clone(),
                )
                .await?;
                tx.commit().await?;
                Ok(row)
            })
            .await;
        if result.is_ok() {
            task_change.completed(true);
            if let Some(change) = html_change {
                change.completed(true);
            }
        }
        result
    }

    pub async fn append_turn_llm_context(
        &self,
        entry: NewTurnLlmContextEntry,
        delivery_key: &str,
    ) -> Result<TurnLlmContextAppendOutcome> {
        let task_change = self.connection.work_signal("tasks.lifecycle").changing();
        let is_preview =
            entry.source == "tool_result_v2" && entry.tool_name.as_deref() == Some("html_preview");
        // Keep uncertainty/settlement for a relevant dispatch, but a successful
        // delivery-key replay must not restart a completed maintenance pass.
        let html_change =
            is_preview.then(|| self.connection.work_signal("maintenance.html").changing());
        let preview_archive = is_preview
            .then(|| pioneer_crud_artifacts::html_preview::archive_result(&entry.payload).ok());
        // Prepare outside writer admission. An invalid/unknown preview remains
        // unchanged unless the commit finds an authoritative release marker.
        let delivery_key = delivery_key.to_owned();
        let source_observation = self
            .prepare_retained_tool_source(&entry, &delivery_key)
            .await?;
        let archived_observation = if let Some(Some(payload)) = &preview_archive {
            let mut archived = entry.clone();
            archived.payload = payload.clone();
            self.prepare_retained_tool_source(&archived, &delivery_key)
                .await?
        } else {
            None
        };
        let result = self
            .run_serialized_write(|| {
                let mut entry = entry.clone();
                let preview_archive = preview_archive.clone();
                let mut source_observation = source_observation.clone();
                let archived_observation = archived_observation.clone();
                let delivery_key = delivery_key.clone();
                async move {
                    let transaction = self
                        .connection
                        .begin()
                        .await
                        .context("failed to begin turn_llm_context append transaction")?;
                    if let Some(prepared) = preview_archive {
                        if pioneer_crud_artifacts::html_preview::preview_result_was_released(
                            &transaction,
                            &entry.turn_id,
                            entry.item_id.as_deref().unwrap_or_default(),
                        )
                        .await?
                        {
                            source_observation = archived_observation;
                            entry.payload = prepared.ok_or_else(|| {
                                anyhow::anyhow!(
                                    "released HTML preview result cannot be reintroduced"
                                )
                            })?;
                        }
                    }
                    let outcome =
                        crate::store::conversations::context_writes::append_turn_llm_context(
                            &transaction,
                            entry,
                            &delivery_key,
                        )
                        .await;
                    match outcome {
                        Ok(outcome) => {
                            if !outcome.already_committed {
                                if let Some(prepared) = &source_observation {
                                    pioneer_crud_execution::execution_observability::commit(
                                        &transaction,
                                        prepared,
                                    )
                                    .await?;
                                }
                            }
                            transaction
                                .commit()
                                .await
                                .context("failed to commit turn_llm_context append transaction")?;
                            Ok(outcome)
                        }
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            Err(error)
                        }
                    }
                }
            })
            .await;
        if let Ok(outcome) = &result {
            task_change.completed(!outcome.already_committed);
            if let Some(change) = html_change {
                change.completed(!outcome.already_committed);
            }
        }
        result
    }

    pub async fn list_turn_llm_context(&self, turn_id: &str) -> Result<Vec<TurnLlmContextEntry>> {
        pioneer_crud_conversations::turn_context::list_turn_llm_context(&self.connection, turn_id)
            .await
    }

    /// Called by the existing terminal/recovery owners after their durable
    /// settlement. The deleting statement revalidates all remaining obligations.
    pub async fn release_settled_shell_controls(&self, turn_id: &str) -> Result<u64> {
        let changes = [
            self.connection.work_signal("maintenance.frozen").changing(),
            self.connection.work_signal("maintenance.html").changing(),
        ];
        let result = self
            .run_scoped_database_quantum(|| {
                pioneer_crud_conversations::context_cleanup::delete_settled_shell_controls(
                    &self.connection,
                    turn_id,
                )
            })
            .await;
        if let Ok(deleted) = &result {
            for change in changes {
                change.completed(*deleted > 0);
            }
        }
        result
    }

    pub async fn delete_turn_llm_context_for_turn(&self, turn_id: &str) -> Result<u64> {
        // Removing transient context can release HTML retirement protection.
        let change = self.connection.work_signal("maintenance.html").changing();
        let result = pioneer_crud_conversations::context_cleanup::delete_turn_llm_context_for_turn(
            &self.connection,
            turn_id,
        )
        .await;
        if let Ok(deleted) = &result {
            change.completed(*deleted > 0);
        }
        result
    }

    pub async fn delete_expired_turn_llm_context(&self) -> Result<u64> {
        pioneer_crud_conversations::context_cleanup::delete_expired_turn_llm_context(
            &self.connection,
        )
        .await
    }

    pub async fn delete_turn_llm_context_for_terminal_turns(&self) -> Result<u64> {
        let maintenance_changes = [self.connection.work_signal("maintenance.html").changing()];
        let maintenance_result = async {
            pioneer_crud_conversations::context_cleanup::delete_turn_llm_context_for_terminal_turns(
                &self.connection,
            )
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
}
