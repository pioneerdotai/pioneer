use anyhow::{Context, Result};
use pioneer_crud::{
    CrudStore, PROJECTION_META_STATUS_BACKFILLING, PROJECTION_META_STATUS_COMPLETE,
    PROJECTION_META_STATUS_FAILED, ProjectionMetaRecord, TurnItemAttemptPayloadCompactionSummary,
    find_projection_meta, upsert_projection_meta,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use tracing::{info, warn};

const TURN_ITEM_ATTEMPT_PAYLOAD_COMPACTION_KEY: &str = "turn_item_attempt_payload_compaction";
const TURN_ITEM_ATTEMPT_PAYLOAD_COMPACTION_VERSION: i64 = 1;
const TURN_ITEM_ATTEMPT_PAYLOAD_COMPACTION_BATCH_SIZE: u64 = 4096;

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct TurnItemAttemptPayloadCompactionStartupSummary {
    pub(crate) skipped: bool,
    pub(crate) batches: u64,
    pub(crate) candidate_rows: u64,
    pub(crate) updated_rows: u64,
    pub(crate) payload_bytes: u64,
    pub(crate) turns_touched: u64,
}

pub(super) async fn run(crud_store: &CrudStore) {
    match compact_once(crud_store).await {
        Ok(summary) if summary.skipped => {}
        Ok(summary) => {
            info!(
                batches = summary.batches,
                candidate_rows = summary.candidate_rows,
                updated_rows = summary.updated_rows,
                payload_bytes = summary.payload_bytes,
                turns_touched = summary.turns_touched,
                "turn_item_attempt terminal payload compaction completed"
            );
        }
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "turn_item_attempt terminal payload compaction failed at startup"
            );
        }
    }
}

pub(crate) async fn compact_once(
    crud_store: &CrudStore,
) -> Result<TurnItemAttemptPayloadCompactionStartupSummary> {
    let db = crud_store.database_connection();
    if compaction_is_current(crud_store).await? {
        return Ok(TurnItemAttemptPayloadCompactionStartupSummary {
            skipped: true,
            ..Default::default()
        });
    }

    let started_at = now_datetime();
    upsert_projection_meta(
        &db,
        ProjectionMetaRecord {
            projection_key: TURN_ITEM_ATTEMPT_PAYLOAD_COMPACTION_KEY.to_owned(),
            projection_version: TURN_ITEM_ATTEMPT_PAYLOAD_COMPACTION_VERSION,
            status: PROJECTION_META_STATUS_BACKFILLING.to_owned(),
            source_thread_count: 0,
            source_turn_count: 0,
            source_turn_item_count: 0,
            source_turn_event_count: 0,
            last_error: None,
            backfill_started_at: Some(started_at),
            backfilled_at: None,
            created_at: started_at,
            updated_at: started_at,
        },
    )
    .await?;

    let result = compact_all_batches(crud_store).await;
    match result {
        Ok(summary) => {
            mark_compaction_complete(&db, &summary).await?;
            Ok(summary)
        }
        Err(error) => {
            mark_compaction_failed(&db, &error).await?;
            Err(error)
        }
    }
}

async fn compact_all_batches(
    crud_store: &CrudStore,
) -> Result<TurnItemAttemptPayloadCompactionStartupSummary> {
    let mut summary = TurnItemAttemptPayloadCompactionStartupSummary::default();

    loop {
        let batch = crud_store
            .compact_terminal_turn_item_attempt_payloads(
                TURN_ITEM_ATTEMPT_PAYLOAD_COMPACTION_BATCH_SIZE,
                false,
            )
            .await
            .context("failed to compact terminal turn_item_attempt payloads")?;
        merge_batch_summary(&mut summary, &batch);

        if batch.candidate_rows == 0 {
            break;
        }
    }

    Ok(summary)
}

async fn compaction_is_current(crud_store: &CrudStore) -> Result<bool> {
    let db = crud_store.database_connection();
    let Some(meta) = find_projection_meta(&db, TURN_ITEM_ATTEMPT_PAYLOAD_COMPACTION_KEY).await?
    else {
        return Ok(false);
    };
    Ok(
        meta.projection_version == TURN_ITEM_ATTEMPT_PAYLOAD_COMPACTION_VERSION
            && meta.status == PROJECTION_META_STATUS_COMPLETE,
    )
}

fn merge_batch_summary(
    aggregate: &mut TurnItemAttemptPayloadCompactionStartupSummary,
    batch: &TurnItemAttemptPayloadCompactionSummary,
) {
    aggregate.batches = aggregate.batches.saturating_add(1);
    aggregate.candidate_rows = aggregate
        .candidate_rows
        .saturating_add(batch.candidate_rows);
    aggregate.updated_rows = aggregate.updated_rows.saturating_add(batch.updated_rows);
    aggregate.payload_bytes = aggregate.payload_bytes.saturating_add(batch.payload_bytes);
    aggregate.turns_touched = aggregate.turns_touched.saturating_add(batch.turns_touched);
}

async fn mark_compaction_complete(
    db: &sea_orm::DatabaseConnection,
    summary: &TurnItemAttemptPayloadCompactionStartupSummary,
) -> Result<()> {
    let now = now_datetime();
    upsert_projection_meta(
        db,
        ProjectionMetaRecord {
            projection_key: TURN_ITEM_ATTEMPT_PAYLOAD_COMPACTION_KEY.to_owned(),
            projection_version: TURN_ITEM_ATTEMPT_PAYLOAD_COMPACTION_VERSION,
            status: PROJECTION_META_STATUS_COMPLETE.to_owned(),
            source_thread_count: 0,
            source_turn_count: summary.turns_touched as i64,
            source_turn_item_count: summary.updated_rows as i64,
            source_turn_event_count: 0,
            last_error: None,
            backfill_started_at: Some(now),
            backfilled_at: Some(now),
            created_at: now,
            updated_at: now,
        },
    )
    .await
}

async fn mark_compaction_failed(
    db: &sea_orm::DatabaseConnection,
    error: &anyhow::Error,
) -> Result<()> {
    let now = now_datetime();
    upsert_projection_meta(
        db,
        ProjectionMetaRecord {
            projection_key: TURN_ITEM_ATTEMPT_PAYLOAD_COMPACTION_KEY.to_owned(),
            projection_version: TURN_ITEM_ATTEMPT_PAYLOAD_COMPACTION_VERSION,
            status: PROJECTION_META_STATUS_FAILED.to_owned(),
            source_thread_count: 0,
            source_turn_count: 0,
            source_turn_item_count: 0,
            source_turn_event_count: 0,
            last_error: Some(format!("{error:#}")),
            backfill_started_at: None,
            backfilled_at: None,
            created_at: now,
            updated_at: now,
        },
    )
    .await
}

fn now_datetime() -> DateTimeWithTimeZone {
    chrono::Utc::now().fixed_offset()
}

#[cfg(test)]
mod tests {
    use super::compact_once;
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{CrudStore, find_projection_meta};
    use pioneer_protocol::{
        ItemCompletedNotification, ItemStartedNotification, SandboxMode, Thread, ThreadMode,
        ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus, ToolCallStatus,
        ToolDisplayPayload, ToolMetadata, ToolOutputPolicySnapshot, ToolStoragePayload, Turn,
        TurnItem, TurnStatus, default_turn_permission_profile_snapshot,
    };
    use sea_orm::{ColumnTrait, Database, EntityTrait, QueryFilter, Set};

    #[tokio::test]
    async fn startup_compaction_clears_legacy_terminal_attempt_payloads_once() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let workspace_id = "ws_attempt_payload_compaction";
        let thread_id = "thr_attempt_payload_compaction";
        let turn_id = "turn_attempt_payload_compaction";
        let completed_item_id = "completed_attempt_payload";
        let running_item_id = "running_attempt_payload";
        let timestamp = 1_700_000_000;
        let now = super::now_datetime();

        pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
            id: Set(workspace_id.to_owned()),
            name: Set("Attempt Payload Compaction".to_owned()),
            is_active: Set(true),
            is_current: Set(true),
            created_at: Set(now),
            updated_at: Set(now),
        })
        .exec(&connection)
        .await
        .expect("workspace should insert");

        let store = CrudStore::new(connection.clone());
        let thread = Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: default_turn_permission_profile_snapshot(),
        };
        store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .expect("turn start should persist");

        let started_item = command_item(completed_item_id, ToolCallStatus::InProgress);
        let completed_item = command_item(completed_item_id, ToolCallStatus::Completed);
        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: started_item.clone(),
                },
                timestamp + 1,
            )
            .await
            .expect("completed item start should persist");
        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: completed_item,
                },
                timestamp + 2,
            )
            .await
            .expect("completed item terminal should persist");

        let legacy_payload =
            serde_json::to_string(&started_item).expect("legacy payload should serialize");
        pioneer_entity::turn_item_attempt::Entity::update_many()
            .col_expr(
                pioneer_entity::turn_item_attempt::Column::Payload,
                sea_orm::sea_query::Expr::value(legacy_payload),
            )
            .filter(pioneer_entity::turn_item_attempt::Column::TurnId.eq(turn_id.to_owned()))
            .filter(
                pioneer_entity::turn_item_attempt::Column::ItemId.eq(completed_item_id.to_owned()),
            )
            .exec(&connection)
            .await
            .expect("legacy terminal payload should restore");

        store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: command_item(running_item_id, ToolCallStatus::InProgress),
                },
                timestamp + 3,
            )
            .await
            .expect("running item start should persist");

        let dry_run = store
            .compact_terminal_turn_item_attempt_payloads(100, true)
            .await
            .expect("dry run should succeed");
        assert_eq!(dry_run.candidate_rows, 1);
        assert_eq!(dry_run.updated_rows, 0);
        assert_eq!(dry_run.turns_touched, 1);
        assert!(dry_run.payload_bytes > 2);

        let summary = compact_once(&store)
            .await
            .expect("startup compaction should succeed");
        assert_eq!(summary.updated_rows, 1);
        assert_eq!(summary.candidate_rows, 1);
        assert_eq!(summary.turns_touched, 1);
        assert!(summary.payload_bytes > 2);

        let completed_attempt = attempt_payload(&connection, turn_id, completed_item_id).await;
        assert_eq!(completed_attempt.as_str(), "{}");

        let running_attempt = attempt_payload(&connection, turn_id, running_item_id).await;
        assert_ne!(running_attempt.as_str(), "{}");

        let second = compact_once(&store)
            .await
            .expect("second startup compaction should succeed");
        assert!(second.skipped, "migration should be idempotent");

        let meta =
            find_projection_meta(&connection, super::TURN_ITEM_ATTEMPT_PAYLOAD_COMPACTION_KEY)
                .await
                .expect("meta should query")
                .expect("meta should exist");
        assert_eq!(meta.status, pioneer_crud::PROJECTION_META_STATUS_COMPLETE);
        assert_eq!(
            meta.projection_version,
            super::TURN_ITEM_ATTEMPT_PAYLOAD_COMPACTION_VERSION
        );
    }

    fn command_item(id: &str, status: ToolCallStatus) -> TurnItem {
        TurnItem::CommandExecution {
            id: id.to_owned(),
            tool_name: "shell".to_owned(),
            arguments: serde_json::json!({ "cmd": "printf heavy" }),
            status,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("shell"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::empty(),
            },
            recovery: None,
            command: vec!["printf".to_owned(), "heavy".to_owned()],
            cwd: Some("/tmp".to_owned()),
            success: (status == ToolCallStatus::Completed).then_some(true),
            outcome: None,
            observation: None,
        }
    }

    async fn attempt_payload(
        connection: &sea_orm::DatabaseConnection,
        turn_id: &str,
        item_id: &str,
    ) -> String {
        pioneer_entity::turn_item_attempt::Entity::find()
            .filter(pioneer_entity::turn_item_attempt::Column::TurnId.eq(turn_id.to_owned()))
            .filter(pioneer_entity::turn_item_attempt::Column::ItemId.eq(item_id.to_owned()))
            .one(connection)
            .await
            .expect("attempt should query")
            .expect("attempt should exist")
            .payload
    }
}
