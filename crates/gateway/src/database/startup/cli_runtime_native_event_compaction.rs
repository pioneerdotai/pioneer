use anyhow::{Context, Result};
use pioneer_crud::{
    CliRuntimeNativeEventCompactionSummary, CrudStore, PROJECTION_META_STATUS_BACKFILLING,
    PROJECTION_META_STATUS_COMPLETE, PROJECTION_META_STATUS_FAILED, ProjectionMetaRecord,
    find_projection_meta, upsert_projection_meta,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use tracing::{info, warn};

const CLI_RUNTIME_NATIVE_EVENT_COMPACTION_KEY: &str =
    "cli_runtime_native_event_terminal_delta_compaction";
const CLI_RUNTIME_NATIVE_EVENT_COMPACTION_VERSION: i64 = 2;
const CLI_RUNTIME_NATIVE_EVENT_COMPACTION_BATCH_SIZE: u64 = 16_384;

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct CliRuntimeNativeEventCompactionStartupSummary {
    pub(crate) skipped: bool,
    pub(crate) batches: u64,
    pub(crate) candidate_rows: u64,
    pub(crate) deleted_rows: u64,
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
                deleted_rows = summary.deleted_rows,
                payload_bytes = summary.payload_bytes,
                turns_touched = summary.turns_touched,
                "CLI runtime native terminal delta event compaction completed"
            );
        }
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "CLI runtime native terminal delta event compaction failed at startup"
            );
        }
    }
}

pub(crate) async fn compact_once(
    crud_store: &CrudStore,
) -> Result<CliRuntimeNativeEventCompactionStartupSummary> {
    let db = crud_store.database_connection();
    if compaction_is_current(crud_store).await? {
        return Ok(CliRuntimeNativeEventCompactionStartupSummary {
            skipped: true,
            ..Default::default()
        });
    }

    let started_at = now_datetime();
    upsert_projection_meta(
        &db,
        ProjectionMetaRecord {
            projection_key: CLI_RUNTIME_NATIVE_EVENT_COMPACTION_KEY.to_owned(),
            projection_version: CLI_RUNTIME_NATIVE_EVENT_COMPACTION_VERSION,
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
) -> Result<CliRuntimeNativeEventCompactionStartupSummary> {
    let mut summary = CliRuntimeNativeEventCompactionStartupSummary::default();

    loop {
        let batch = crud_store
            .compact_terminal_cli_runtime_native_events(
                CLI_RUNTIME_NATIVE_EVENT_COMPACTION_BATCH_SIZE,
                false,
            )
            .await
            .context("failed to compact terminal CLI runtime native delta events")?;
        merge_batch_summary(&mut summary, &batch);

        if batch.candidate_rows == 0 {
            break;
        }
    }

    Ok(summary)
}

async fn compaction_is_current(crud_store: &CrudStore) -> Result<bool> {
    let db = crud_store.database_connection();
    let Some(meta) = find_projection_meta(&db, CLI_RUNTIME_NATIVE_EVENT_COMPACTION_KEY).await?
    else {
        return Ok(false);
    };
    Ok(
        meta.projection_version == CLI_RUNTIME_NATIVE_EVENT_COMPACTION_VERSION
            && meta.status == PROJECTION_META_STATUS_COMPLETE,
    )
}

fn merge_batch_summary(
    aggregate: &mut CliRuntimeNativeEventCompactionStartupSummary,
    batch: &CliRuntimeNativeEventCompactionSummary,
) {
    aggregate.batches = aggregate.batches.saturating_add(1);
    aggregate.candidate_rows = aggregate
        .candidate_rows
        .saturating_add(batch.candidate_rows);
    aggregate.deleted_rows = aggregate.deleted_rows.saturating_add(batch.deleted_rows);
    aggregate.payload_bytes = aggregate.payload_bytes.saturating_add(batch.payload_bytes);
    aggregate.turns_touched = aggregate.turns_touched.saturating_add(batch.turns_touched);
}

async fn mark_compaction_complete(
    db: &sea_orm::DatabaseConnection,
    summary: &CliRuntimeNativeEventCompactionStartupSummary,
) -> Result<()> {
    let now = now_datetime();
    upsert_projection_meta(
        db,
        ProjectionMetaRecord {
            projection_key: CLI_RUNTIME_NATIVE_EVENT_COMPACTION_KEY.to_owned(),
            projection_version: CLI_RUNTIME_NATIVE_EVENT_COMPACTION_VERSION,
            status: PROJECTION_META_STATUS_COMPLETE.to_owned(),
            source_thread_count: 0,
            source_turn_count: summary.turns_touched as i64,
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
            projection_key: CLI_RUNTIME_NATIVE_EVENT_COMPACTION_KEY.to_owned(),
            projection_version: CLI_RUNTIME_NATIVE_EVENT_COMPACTION_VERSION,
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
    use pioneer_crud::{
        CliRuntimeNativeEventListFilter, CrudStore, NewCliRuntimeNativeEvent,
        NewCliRuntimeTurnBinding, PROJECTION_META_STATUS_COMPLETE, ProjectionMetaRecord,
        find_projection_meta, upsert_projection_meta,
    };
    use pioneer_protocol::{
        SandboxMode, Thread, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus,
        Turn, TurnStatus, default_turn_permission_profile_snapshot,
    };
    use sea_orm::{Database, EntityTrait, Set};

    #[tokio::test]
    async fn startup_native_event_compaction_deletes_terminal_delta_events_once() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let workspace_id = "ws_startup_native_event_compaction";
        let thread_id = "thr_startup_native_event_compaction";
        let turn_id = "turn_startup_native_event_compaction";
        let timestamp = 1_700_000_000;
        let now = super::now_datetime();

        pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
            id: Set(workspace_id.to_owned()),
            name: Set("Startup Native Event Compaction".to_owned()),
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

        pioneer_entity::turn::Entity::update(pioneer_entity::turn::ActiveModel {
            id: Set(turn_id.to_owned()),
            status: Set("completed".to_owned()),
            updated_at: Set(now),
            ..Default::default()
        })
        .exec(&connection)
        .await
        .expect("turn should become terminal");

        store
            .upsert_cli_runtime_turn_binding(NewCliRuntimeTurnBinding {
                turn_id: turn_id.to_owned(),
                thread_id: thread_id.to_owned(),
                continuation_thread_id: thread_id.to_owned(),
                workspace_id: workspace_id.to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                native_thread_id: "native-thread-startup-compaction".to_owned(),
                native_turn_id: Some("native-turn-startup-compaction".to_owned()),
                request_id: None,
                status: "completed".to_owned(),
                model: None,
                cwd: None,
                sandbox_json: None,
                approval_policy: None,
                input_mapping_json: "{}".to_owned(),
                created_at: now,
                updated_at: now,
            })
            .await
            .expect("turn binding should persist");

        for (id, sequence, method) in [
            (
                "startup-native-agent-delta",
                1_i64,
                "item/agentMessage/delta",
            ),
            (
                "startup-native-output-delta",
                2_i64,
                "item/commandExecution/outputDelta",
            ),
            ("startup-native-completed", 3_i64, "item/completed"),
        ] {
            store
                .append_cli_runtime_native_event(NewCliRuntimeNativeEvent {
                    id: id.to_owned(),
                    runtime_id: "codex".to_owned(),
                    runtime_kind: "codex".to_owned(),
                    workspace_id: Some(workspace_id.to_owned()),
                    thread_id: Some(thread_id.to_owned()),
                    turn_id: Some(turn_id.to_owned()),
                    native_thread_id: Some("native-thread-startup-compaction".to_owned()),
                    native_turn_id: Some("native-turn-startup-compaction".to_owned()),
                    native_method: method.to_owned(),
                    payload_redacted_json: serde_json::json!({"nativeItemId":"item"}).to_string(),
                    sequence,
                    created_at: now,
                })
                .await
                .expect("native event should persist");
        }

        upsert_projection_meta(
            &connection,
            ProjectionMetaRecord {
                projection_key: super::CLI_RUNTIME_NATIVE_EVENT_COMPACTION_KEY.to_owned(),
                projection_version: 1,
                status: PROJECTION_META_STATUS_COMPLETE.to_owned(),
                source_thread_count: 0,
                source_turn_count: 0,
                source_turn_item_count: 0,
                source_turn_event_count: 0,
                last_error: None,
                backfill_started_at: Some(now),
                backfilled_at: Some(now),
                created_at: now,
                updated_at: now,
            },
        )
        .await
        .expect("outdated compaction meta should persist");

        let summary = compact_once(&store)
            .await
            .expect("startup compaction should rerun outdated complete meta");
        assert_eq!(summary.deleted_rows, 2);
        assert_eq!(summary.turns_touched, 1);

        let meta =
            find_projection_meta(&connection, super::CLI_RUNTIME_NATIVE_EVENT_COMPACTION_KEY)
                .await
                .expect("meta should query")
                .expect("meta should exist");
        assert_eq!(meta.status, pioneer_crud::PROJECTION_META_STATUS_COMPLETE);
        assert_eq!(
            meta.projection_version,
            super::CLI_RUNTIME_NATIVE_EVENT_COMPACTION_VERSION
        );
        assert_eq!(meta.source_turn_event_count, 2);

        let second = compact_once(&store)
            .await
            .expect("second startup compaction should succeed");
        assert!(second.skipped);

        let remaining = store
            .list_cli_runtime_native_events(CliRuntimeNativeEventListFilter {
                runtime_id: Some("codex".to_owned()),
                thread_id: Some(thread_id.to_owned()),
                turn_id: Some(turn_id.to_owned()),
                ..Default::default()
            })
            .await
            .expect("remaining native events should list");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].id, "startup-native-completed");
    }
}
