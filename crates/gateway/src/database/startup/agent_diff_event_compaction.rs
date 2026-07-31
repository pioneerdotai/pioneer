use anyhow::{Context, Result};
use pioneer_crud::{
    CrudStore, PROJECTION_META_STATUS_BACKFILLING, PROJECTION_META_STATUS_COMPLETE,
    PROJECTION_META_STATUS_FAILED, ProjectionMetaRecord, TurnEventCompactionSummary,
    find_projection_meta, upsert_projection_meta,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use tracing::{info, warn};

const AGENT_DIFF_EVENT_COMPACTION_KEY: &str = "agent_diff_turn_event_compaction";
const AGENT_DIFF_EVENT_COMPACTION_VERSION: i64 = 1;
const AGENT_DIFF_EVENT_COMPACTION_BATCH_SIZE: u64 = 1024;

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct AgentDiffEventCompactionStartupSummary {
    pub(crate) skipped: bool,
    pub(crate) batches: u64,
    pub(crate) candidate_rows: u64,
    pub(crate) deleted_rows: u64,
    pub(crate) payload_bytes: u64,
    pub(crate) latest_snapshots_kept: u64,
    pub(crate) skipped_unprojected: u64,
    pub(crate) skipped_failed: u64,
}

pub(super) async fn run(crud_store: &CrudStore) {
    match compact_once(crud_store).await {
        Ok(summary) if summary.skipped => {}
        Ok(summary) => {
            info!(
                batches = summary.batches,
                candidate_rows = summary.candidate_rows,
                deleted_rows = summary.deleted_rows,
                payload_bytes = summary.payload_bytes,
                latest_snapshots_kept = summary.latest_snapshots_kept,
                skipped_unprojected = summary.skipped_unprojected,
                skipped_failed = summary.skipped_failed,
                "agent diff turn_event compaction completed"
            );
        }
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "agent diff turn_event compaction failed at startup"
            );
        }
    }
}

pub(crate) async fn compact_once(
    crud_store: &CrudStore,
) -> Result<AgentDiffEventCompactionStartupSummary> {
    let db = crud_store.database_connection();
    if compaction_is_current(crud_store).await? {
        return Ok(AgentDiffEventCompactionStartupSummary {
            skipped: true,
            ..Default::default()
        });
    }

    let started_at = now_datetime();
    upsert_projection_meta(
        &db,
        ProjectionMetaRecord {
            projection_key: AGENT_DIFF_EVENT_COMPACTION_KEY.to_owned(),
            projection_version: AGENT_DIFF_EVENT_COMPACTION_VERSION,
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
) -> Result<AgentDiffEventCompactionStartupSummary> {
    let mut summary = AgentDiffEventCompactionStartupSummary::default();

    loop {
        let batch = crud_store
            .compact_superseded_agent_diff_turn_events(
                AGENT_DIFF_EVENT_COMPACTION_BATCH_SIZE,
                false,
            )
            .await
            .context("failed to compact superseded agent diff turn events")?;
        merge_batch_summary(&mut summary, &batch);

        if batch.candidate_rows == 0 {
            break;
        }
    }

    Ok(summary)
}

async fn compaction_is_current(crud_store: &CrudStore) -> Result<bool> {
    let db = crud_store.database_connection();
    let Some(meta) = find_projection_meta(&db, AGENT_DIFF_EVENT_COMPACTION_KEY).await? else {
        return Ok(false);
    };
    Ok(
        meta.projection_version == AGENT_DIFF_EVENT_COMPACTION_VERSION
            && meta.status == PROJECTION_META_STATUS_COMPLETE,
    )
}

fn merge_batch_summary(
    aggregate: &mut AgentDiffEventCompactionStartupSummary,
    batch: &TurnEventCompactionSummary,
) {
    aggregate.batches = aggregate.batches.saturating_add(1);
    aggregate.candidate_rows = aggregate
        .candidate_rows
        .saturating_add(batch.candidate_rows);
    aggregate.deleted_rows = aggregate.deleted_rows.saturating_add(batch.deleted_rows);
    aggregate.payload_bytes = aggregate.payload_bytes.saturating_add(batch.payload_bytes);
    aggregate.latest_snapshots_kept = batch.latest_snapshots_kept;
    aggregate.skipped_unprojected = batch.skipped_unprojected;
    aggregate.skipped_failed = batch.skipped_failed;
}

async fn mark_compaction_complete(
    db: &sea_orm::DatabaseConnection,
    summary: &AgentDiffEventCompactionStartupSummary,
) -> Result<()> {
    let now = now_datetime();
    upsert_projection_meta(
        db,
        ProjectionMetaRecord {
            projection_key: AGENT_DIFF_EVENT_COMPACTION_KEY.to_owned(),
            projection_version: AGENT_DIFF_EVENT_COMPACTION_VERSION,
            status: PROJECTION_META_STATUS_COMPLETE.to_owned(),
            source_thread_count: 0,
            source_turn_count: 0,
            source_turn_item_count: 0,
            source_turn_event_count: summary.deleted_rows as i64,
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
            projection_key: AGENT_DIFF_EVENT_COMPACTION_KEY.to_owned(),
            projection_version: AGENT_DIFF_EVENT_COMPACTION_VERSION,
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
        ItemCompletedNotification, SandboxMode, SystemEventLevel, Thread, ThreadMode,
        ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus, Turn, TurnItem, TurnStatus,
        default_turn_permission_profile_snapshot,
    };
    use sea_orm::{ColumnTrait, Database, EntityTrait, QueryFilter, QueryOrder, Set};

    #[tokio::test]
    async fn startup_compaction_migration_deletes_projected_duplicate_agent_diff_events_once() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let workspace_id = "ws_startup_agent_diff_compaction";
        let thread_id = "thr_startup_agent_diff_compaction";
        let turn_id = "turn_startup_agent_diff_compaction";
        let timestamp = 1_700_000_000;

        pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
            id: Set(workspace_id.to_owned()),
            name: Set("Startup Agent Diff Compaction".to_owned()),
            is_active: Set(true),
            is_current: Set(true),
            created_at: Set(super::now_datetime()),
            updated_at: Set(super::now_datetime()),
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
            visibility: None,
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
        let persisted_turn = pioneer_entity::turn::Entity::find_by_id(turn_id)
            .one(&connection)
            .await
            .expect("startup fixture turn provenance query should succeed")
            .expect("startup fixture turn should exist");
        assert_eq!(
            persisted_turn.initiated_by_actor_kind.as_deref(),
            Some("system")
        );
        assert_eq!(persisted_turn.initiated_by_actor_id, None);

        for (index, payload) in ["first diff", "second diff", "third diff"]
            .into_iter()
            .enumerate()
        {
            store
                .materialize_item_completed(
                    ItemCompletedNotification {
                        workspace_id: workspace_id.to_owned(),
                        thread_id: thread_id.to_owned(),
                        turn_id: turn_id.to_owned(),
                        item: TurnItem::SystemEvent {
                            id: "agent_diff_native_turn".to_owned(),
                            level: SystemEventLevel::Info,
                            message: "Diff updated".to_owned(),
                            code: Some("agent_diff_updated".to_owned()),
                            details: Some(serde_json::json!({"payload": payload})),
                        },
                    },
                    timestamp + 1 + index as i64,
                )
                .await
                .expect("historical diff event should persist");
        }

        let summary = compact_once(&store)
            .await
            .expect("startup compaction should succeed");
        assert_eq!(summary.deleted_rows, 2);
        assert_eq!(summary.latest_snapshots_kept, 1);

        let raw_events = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn_id.to_owned()))
            .order_by_asc(pioneer_entity::turn_event::Column::Sequence)
            .all(&connection)
            .await
            .expect("raw events should query");
        assert_eq!(
            raw_events.len(),
            2,
            "turn/start plus latest diff snapshot should remain"
        );
        assert_eq!(
            raw_events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 4],
            "compaction must not renumber turn_event.sequence"
        );

        let second = compact_once(&store)
            .await
            .expect("second startup compaction should succeed");
        assert!(second.skipped, "migration should be idempotent");

        let meta = find_projection_meta(&connection, super::AGENT_DIFF_EVENT_COMPACTION_KEY)
            .await
            .expect("meta should query")
            .expect("meta should exist");
        assert_eq!(meta.status, pioneer_crud::PROJECTION_META_STATUS_COMPLETE);
    }
}
