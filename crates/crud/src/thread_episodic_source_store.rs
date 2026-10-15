use crate::repositories::{thread, thread_episodic, turn};
use crate::thread_episodic_source::*;
use crate::{CrudStore, NewThreadEpisodicItemRecord, ThreadEpisodicSourceReconcileOutcome};
use anyhow::{Context, Result, ensure};
use sea_orm::{DatabaseTransaction, TransactionTrait};

#[derive(Clone, Debug)]
pub(crate) struct PreparedSourceSave {
    source_payload: String,
    workspace_id: String,
    thread_id: String,
    turn_id: String,
    item_id: String,
    context: Option<thread_episodic::SourceProjectionContext>,
    record: Option<NewThreadEpisodicItemRecord>,
}

impl CrudStore {
    /// Gateway must own this workspace across cleanup/reset/execution, outside
    /// DB capacity, so a capsule writer cannot outlive destructive replacement.
    /// Caller must finish artifact cleanup before entering this reset. The first
    /// source batch records that cleanup together with job replacement progress.
    pub async fn reset_thread_episodic_projection(
        &self,
        workspace_id: &str,
        now_unix: i64,
    ) -> Result<u64> {
        use sea_orm::{ActiveModelTrait, IntoActiveModel, Set};
        let checkpoint_key = crate::thread_episodic_projection_reset_key(workspace_id)?;
        let mut total_replaced = 0_u64;
        loop {
            let checkpoint = crate::find_projection_meta(&self.connection, &checkpoint_key)
                .await?
                .context("episodic reset checkpoint missing")?;
            ensure!(
                checkpoint.projection_config_hash.is_some(),
                "episodic reset target identity missing"
            );
            ensure!(
                checkpoint.status == crate::PROJECTION_META_STATUS_PENDING,
                "episodic reset checkpoint is not pending"
            );
            let mut progress: crate::ThreadEpisodicProjectionResetProgress = serde_json::from_str(
                checkpoint
                    .projection_config_json
                    .as_deref()
                    .context("episodic reset progress missing")?,
            )?;
            let sources = thread_episodic::list_projection_reset_sources(
                &self.connection,
                workspace_id,
                progress.after_source.as_ref(),
            )
            .await?;
            if sources.is_empty() && progress.files_cleaned {
                return Ok(total_replaced);
            }
            progress.files_cleaned = true;
            if let Some(last) = sources.last() {
                progress.after_source = Some([
                    last.thread_id.clone(),
                    last.turn_id.clone(),
                    last.item_id.clone(),
                    last.text_hash.clone(),
                ]);
            }
            let progress_json = serde_json::to_string(&progress)?;
            let replacements = sources
                .iter()
                .map(|item| {
                    (
                        item.id.clone(),
                        [
                            item.thread_id.clone(),
                            item.turn_id.clone(),
                            item.item_id.clone(),
                            item.text_hash.clone(),
                        ],
                        pioneer_protocol::generate_id(crate::convention::DB_ID_LEN),
                    )
                })
                .collect::<Vec<_>>();
            // JSON/IDs are prepared outside writer capacity. The progress row
            // and every source/job replacement are one guarded write set.
            let replaced = self
                .run_serialized_write(|| async {
                    let transaction = self.connection.begin().await?;
                    let current = crate::find_projection_meta(&transaction, &checkpoint_key)
                        .await?
                        .context("episodic reset checkpoint disappeared")?;
                    ensure!(
                        current.status == checkpoint.status
                            && current.projection_version == checkpoint.projection_version
                            && current.projection_config_hash == checkpoint.projection_config_hash
                            && current.projection_config_json == checkpoint.projection_config_json,
                        "episodic reset progress changed before commit"
                    );
                    let replaced = thread_episodic::reset_projection_batch(
                        &transaction,
                        workspace_id,
                        &replacements,
                        crate::util::unix_to_datetime(now_unix),
                    )
                    .await?;
                    let mut current = current.into_active_model();
                    current.projection_config_json = Set(Some(progress_json.clone()));
                    current.updated_at = Set(crate::util::unix_to_datetime(now_unix));
                    current.update(&transaction).await?;
                    transaction.commit().await?;
                    Ok(replaced)
                })
                .await?;
            total_replaced = total_replaced.saturating_add(replaced);
        }
    }

    // Source selection, hash and grouping are prepared outside DB capacity.
    pub(crate) async fn prepare_episodic_source_save(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item: &pioneer_protocol::TurnItem,
    ) -> Result<PreparedSourceSave> {
        let committed = committed_item_ingestion_input_from_parts(
            workspace_id,
            thread_id,
            turn_id,
            item.clone(),
        )
        .context("invalid episodic canonical source identity")?;
        let (record, context) = match select_committed_item_source(&committed) {
            ThreadEpisodicSourceSelection::Indexable(source) => {
                let context = thread_episodic::load_source_projection_context(
                    &self.connection,
                    thread_id,
                    turn_id,
                    item.item_id(),
                    item.item_type(),
                )
                .await?;
                let hash = source_text_hash(source.text.trim());
                let group = context.projection_group_id(&committed, &hash);
                (
                    Some(prepare_source_record(&committed, source, group)),
                    Some(context),
                )
            }
            ThreadEpisodicSourceSelection::Rejected { .. } => (None, None),
        };
        Ok(PreparedSourceSave {
            source_payload: serde_json::to_string(item)?,
            workspace_id: workspace_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            item_id: item.item_id().to_owned(),
            record,
            context,
        })
    }

    pub(crate) async fn commit_episodic_source_save(
        &self,
        db: &DatabaseTransaction,
        prepared: &PreparedSourceSave,
        now: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<()> {
        let thread = thread::find_thread_by_id(db, &prepared.thread_id)
            .await?
            .context("episodic source thread missing")?;
        ensure!(
            thread.workspace_id == prepared.workspace_id,
            "episodic source workspace changed"
        );
        ensure!(
            turn::find_turn_by_thread_and_id(db, &prepared.thread_id, &prepared.turn_id)
                .await?
                .is_some(),
            "episodic source turn ownership changed"
        );
        let source = turn::find_turn_item(db, &prepared.turn_id, &prepared.item_id)
            .await?
            .context("episodic canonical source missing")?;
        ensure!(
            source.payload == prepared.source_payload,
            "episodic source payload changed before commit"
        );
        // Started items remain drafts even when their payload contains final-answer text.
        if !source_status_is_committed(source.status.as_deref()) {
            return Ok(());
        }
        if let Some(record) = &prepared.record {
            let current_context = thread_episodic::load_source_projection_context(
                db,
                &prepared.thread_id,
                &prepared.turn_id,
                &prepared.item_id,
                match record.source_runtime_kind {
                    crate::ThreadEpisodicSourceRuntimeKind::UserTurn => {
                        pioneer_protocol::TurnItemType::UserMessage
                    }
                    crate::ThreadEpisodicSourceRuntimeKind::AssistantTurn => {
                        pioneer_protocol::TurnItemType::AgentMessage
                    }
                    _ => pioneer_protocol::TurnItemType::Task,
                },
            )
            .await?;
            ensure!(
                prepared.context.as_ref() == Some(&current_context),
                "episodic source context changed before commit"
            );
        }
        // MoreWork is a bounded version-retirement quantum. An atomic direct save
        // cannot commit a partially reconciled occurrence: roll back and surface it.
        let outcome = match &prepared.record {
            Some(record) => {
                thread_episodic::reconcile_item_source_version(db, record.clone(), now).await?
            }
            None => {
                thread_episodic::retire_source_occurrence(
                    db,
                    &prepared.workspace_id,
                    &prepared.thread_id,
                    &prepared.turn_id,
                    &prepared.item_id,
                    now,
                )
                .await?
            }
        };
        ensure!(
            outcome != ThreadEpisodicSourceReconcileOutcome::MoreWork,
            "episodic source save exceeds version reconciliation quantum"
        );
        Ok(())
    }
}
