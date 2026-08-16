use anyhow::Result;
use pioneer_config::GatewayContextCompactionTimeoutConfig;
use pioneer_crud::{
    CrudStore, PROJECTION_META_STATUS_BACKFILLING, PROJECTION_META_STATUS_COMPLETE,
    PROJECTION_META_STATUS_FAILED, ProjectionMetaRecord, TurnItemAttemptTimeoutDurations,
    find_projection_meta, upsert_projection_meta,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use tracing::{info, warn};

const TURN_ITEM_EXECUTION_CLASS_BACKFILL_KEY: &str = "turn_item_execution_class_backfill";
const TURN_ITEM_EXECUTION_CLASS_BACKFILL_VERSION: i64 = 1;
const TURN_ITEM_EXECUTION_CLASS_BACKFILL_BATCH_SIZE: u64 = 256;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TurnItemExecutionClassBackfillSummary {
    pub(crate) skipped: bool,
    pub(crate) batches: u64,
    pub(crate) attempts_classified: u64,
    pub(crate) context_compactions_classified: u64,
}

pub(super) async fn run(
    crud_store: &CrudStore,
    context_compaction_config: GatewayContextCompactionTimeoutConfig,
) {
    match backfill_once(crud_store, context_compaction_config).await {
        Ok(summary) if summary.skipped => {}
        Ok(summary) => {
            info!(
                batches = summary.batches,
                attempts_classified = summary.attempts_classified,
                context_compactions_classified = summary.context_compactions_classified,
                "turn item execution-class background backfill completed"
            );
        }
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "turn item execution-class background backfill failed"
            );
        }
    }
}

pub(crate) async fn backfill_once(
    crud_store: &CrudStore,
    context_compaction_config: GatewayContextCompactionTimeoutConfig,
) -> Result<TurnItemExecutionClassBackfillSummary> {
    let db = crud_store.database_connection();
    if backfill_is_current(&db).await? {
        return Ok(TurnItemExecutionClassBackfillSummary {
            skipped: true,
            ..Default::default()
        });
    }

    let started_at = now_datetime();
    upsert_projection_meta(
        &db,
        ProjectionMetaRecord {
            projection_key: TURN_ITEM_EXECUTION_CLASS_BACKFILL_KEY.to_owned(),
            projection_version: TURN_ITEM_EXECUTION_CLASS_BACKFILL_VERSION,
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

    let result = backfill_all_batches(crud_store, context_compaction_config).await;
    match result {
        Ok(summary) => {
            mark_backfill_complete(&db, started_at, &summary).await?;
            Ok(summary)
        }
        Err(error) => {
            mark_backfill_failed(&db, started_at, &error).await?;
            Err(error)
        }
    }
}

async fn backfill_all_batches(
    crud_store: &CrudStore,
    context_compaction_config: GatewayContextCompactionTimeoutConfig,
) -> Result<TurnItemExecutionClassBackfillSummary> {
    let timeout_durations = TurnItemAttemptTimeoutDurations {
        lease_secs: context_compaction_config.lease_secs,
        idle_secs: context_compaction_config.idle_secs,
        hard_secs: context_compaction_config.hard_secs,
    };
    let mut summary = TurnItemExecutionClassBackfillSummary::default();
    loop {
        let batch = crud_store
            .backfill_turn_item_execution_classes_batch(
                TURN_ITEM_EXECUTION_CLASS_BACKFILL_BATCH_SIZE,
                timeout_durations,
            )
            .await?;
        if batch.attempts_classified == 0 {
            break;
        }
        summary.batches = summary.batches.saturating_add(1);
        summary.attempts_classified = summary
            .attempts_classified
            .saturating_add(batch.attempts_classified as u64);
        summary.context_compactions_classified = summary
            .context_compactions_classified
            .saturating_add(batch.context_compactions_classified as u64);
        tokio::task::yield_now().await;
    }
    Ok(summary)
}

async fn backfill_is_current(db: &sea_orm::DatabaseConnection) -> Result<bool> {
    let Some(meta) = find_projection_meta(db, TURN_ITEM_EXECUTION_CLASS_BACKFILL_KEY).await? else {
        return Ok(false);
    };
    Ok(
        meta.projection_version == TURN_ITEM_EXECUTION_CLASS_BACKFILL_VERSION
            && meta.status == PROJECTION_META_STATUS_COMPLETE,
    )
}

async fn mark_backfill_complete(
    db: &sea_orm::DatabaseConnection,
    started_at: DateTimeWithTimeZone,
    summary: &TurnItemExecutionClassBackfillSummary,
) -> Result<()> {
    let completed_at = now_datetime();
    upsert_projection_meta(
        db,
        ProjectionMetaRecord {
            projection_key: TURN_ITEM_EXECUTION_CLASS_BACKFILL_KEY.to_owned(),
            projection_version: TURN_ITEM_EXECUTION_CLASS_BACKFILL_VERSION,
            status: PROJECTION_META_STATUS_COMPLETE.to_owned(),
            source_thread_count: 0,
            source_turn_count: 0,
            source_turn_item_count: summary.attempts_classified as i64,
            source_turn_event_count: 0,
            last_error: None,
            backfill_started_at: Some(started_at),
            backfilled_at: Some(completed_at),
            created_at: started_at,
            updated_at: completed_at,
        },
    )
    .await
}

async fn mark_backfill_failed(
    db: &sea_orm::DatabaseConnection,
    started_at: DateTimeWithTimeZone,
    error: &anyhow::Error,
) -> Result<()> {
    let failed_at = now_datetime();
    upsert_projection_meta(
        db,
        ProjectionMetaRecord {
            projection_key: TURN_ITEM_EXECUTION_CLASS_BACKFILL_KEY.to_owned(),
            projection_version: TURN_ITEM_EXECUTION_CLASS_BACKFILL_VERSION,
            status: PROJECTION_META_STATUS_FAILED.to_owned(),
            source_thread_count: 0,
            source_turn_count: 0,
            source_turn_item_count: 0,
            source_turn_event_count: 0,
            last_error: Some(format!("{error:#}")),
            backfill_started_at: Some(started_at),
            backfilled_at: None,
            created_at: started_at,
            updated_at: failed_at,
        },
    )
    .await
}

fn now_datetime() -> DateTimeWithTimeZone {
    chrono::Utc::now().fixed_offset()
}

#[cfg(test)]
mod tests {
    use super::{TURN_ITEM_EXECUTION_CLASS_BACKFILL_KEY, backfill_once};
    use migration::{Migrator, MigratorTrait};
    use pioneer_config::GatewayContextCompactionTimeoutConfig;
    use pioneer_crud::{CrudStore, PROJECTION_META_STATUS_COMPLETE, find_projection_meta};
    use pioneer_entity::{turn_item, turn_item_attempt};
    use pioneer_protocol::{SystemEventLevel, TurnItem};
    use sea_orm::{Database, EntityTrait, Set};

    #[tokio::test]
    async fn background_backfill_classifies_legacy_attempts_and_rebases_compaction_deadlines() {
        let db = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&db, None)
            .await
            .expect("schema migrations must succeed");
        let started_at = chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .expect("fixed test timestamp")
            .fixed_offset();
        let legacy_lease = started_at + chrono::Duration::minutes(5);
        let legacy_hard = started_at + chrono::Duration::minutes(10);

        let context_compaction = TurnItem::SystemEvent {
            id: "legacy-compaction".to_owned(),
            level: SystemEventLevel::Info,
            message: "Context compaction started".to_owned(),
            code: Some("agent_context_compaction".to_owned()),
            details: Some(serde_json::json!({
                "nativeItemKind": "contextCompaction",
                "status": "started"
            })),
        };
        let ordinary_event = TurnItem::SystemEvent {
            id: "legacy-standard".to_owned(),
            level: SystemEventLevel::Info,
            message: "Ordinary event".to_owned(),
            code: Some("agent_runtime_item".to_owned()),
            details: None,
        };

        for (row_id, turn_id, item, attempt_id) in [
            (
                "legacy_item_compact",
                "legacy_turn_compact",
                &context_compaction,
                "legacy_attempt_cmpct",
            ),
            (
                "legacy_item_standard",
                "legacy_turn_standard",
                &ordinary_event,
                "legacy_attempt_std",
            ),
        ] {
            turn_item::Entity::insert(turn_item::ActiveModel {
                id: Set(row_id.to_owned()),
                turn_id: Set(turn_id.to_owned()),
                item_id: Set(item.item_id().to_owned()),
                item_type: Set("system_event".to_owned()),
                status: Set(Some("in_progress".to_owned())),
                payload: Set(serde_json::to_string(item).expect("item should serialize")),
                active_attempt_number: Set(1),
                active_attempt_status: Set(Some("running".to_owned())),
                active_attempt_id: Set(Some(attempt_id.to_owned())),
                last_heartbeat_at: Set(Some(started_at)),
                lease_expires_at: Set(Some(legacy_lease)),
                created_at: Set(started_at),
                updated_at: Set(started_at),
            })
            .exec(&db)
            .await
            .expect("legacy turn item should insert");
            turn_item_attempt::Entity::insert(turn_item_attempt::ActiveModel {
                id: Set(attempt_id.to_owned()),
                turn_id: Set(turn_id.to_owned()),
                item_id: Set(item.item_id().to_owned()),
                item_type: Set("system_event".to_owned()),
                execution_class: Set(None),
                attempt_number: Set(1),
                status: Set("running".to_owned()),
                timeout_reason: Set(None),
                failure_reason: Set(None),
                recovery_action: Set(None),
                idempotency_key: Set(None),
                trace_id: Set(None),
                payload: Set(serde_json::to_string(item).expect("item should serialize")),
                started_at: Set(started_at),
                last_heartbeat_at: Set(Some(started_at)),
                lease_expires_at: Set(Some(legacy_lease)),
                idle_deadline_at: Set(Some(legacy_lease)),
                hard_deadline_at: Set(Some(legacy_hard)),
                updated_at: Set(started_at),
                started_event_sequence: Set(Some(1)),
                recovery_suppressed_reason: Set(None),
                recovery_suppressed_at: Set(None),
                recovery_suppression_context_json: Set(None),
            })
            .exec(&db)
            .await
            .expect("legacy attempt should insert");
        }
        let store = CrudStore::new(db.clone());

        assert!(
            store
                .list_timeout_candidates(started_at.timestamp() + 3_600, 16)
                .await
                .expect("legacy timeout query should succeed")
                .is_empty(),
            "unclassified legacy rows must stay outside destructive supervision"
        );

        let config = GatewayContextCompactionTimeoutConfig {
            lease_secs: 120,
            idle_secs: 300,
            hard_secs: 1_800,
            recovery_grace_secs: 600,
        };
        let summary = backfill_once(&store, config)
            .await
            .expect("background backfill should complete");
        assert_eq!(summary.attempts_classified, 2);
        assert_eq!(summary.context_compactions_classified, 1);

        let compact = turn_item_attempt::Entity::find_by_id("legacy_attempt_cmpct")
            .one(&db)
            .await
            .expect("compaction attempt should query")
            .expect("compaction attempt should exist");
        assert_eq!(
            compact.execution_class.as_deref(),
            Some("context_compaction")
        );
        assert_eq!(
            compact.lease_expires_at,
            Some(started_at + chrono::Duration::seconds(120))
        );
        assert_eq!(
            compact.idle_deadline_at,
            Some(started_at + chrono::Duration::seconds(300))
        );
        assert_eq!(
            compact.hard_deadline_at,
            Some(started_at + chrono::Duration::seconds(1_800))
        );

        let standard = turn_item_attempt::Entity::find_by_id("legacy_attempt_std")
            .one(&db)
            .await
            .expect("standard attempt should query")
            .expect("standard attempt should exist");
        assert_eq!(standard.execution_class.as_deref(), Some("standard"));
        assert_eq!(standard.lease_expires_at, Some(legacy_lease));
        assert_eq!(standard.idle_deadline_at, Some(legacy_lease));
        assert_eq!(standard.hard_deadline_at, Some(legacy_hard));

        let marker = find_projection_meta(&db, TURN_ITEM_EXECUTION_CLASS_BACKFILL_KEY)
            .await
            .expect("backfill marker should query")
            .expect("backfill marker should exist");
        assert_eq!(marker.status, PROJECTION_META_STATUS_COMPLETE);
        assert_eq!(marker.source_turn_item_count, 2);

        let repeated = backfill_once(&store, config)
            .await
            .expect("completed backfill should be idempotent");
        assert!(repeated.skipped);
        assert_eq!(repeated.attempts_classified, 0);
    }
}
