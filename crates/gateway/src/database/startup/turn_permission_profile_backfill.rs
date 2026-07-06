use anyhow::{Context, Result};
use pioneer_crud::{
    CrudStore, PROJECTION_META_STATUS_BACKFILLING, PROJECTION_META_STATUS_COMPLETE,
    PROJECTION_META_STATUS_FAILED, ProjectionMetaRecord, find_projection_meta,
    upsert_projection_meta,
};
use pioneer_entity::turn_event;
use sea_orm::{
    ActiveModelTrait, ConnectionTrait, DatabaseConnection, FromQueryResult, Set, Statement,
    entity::prelude::DateTimeWithTimeZone,
};
use serde_json::Value as JsonValue;
use std::time::Duration;
use tracing::{info, warn};

const TURN_PERMISSION_PROFILE_BACKFILL_KEY: &str = "turn_permission_profile_payload_backfill";
const TURN_PERMISSION_PROFILE_BACKFILL_VERSION: i64 = 1;
const TURN_PERMISSION_PROFILE_BACKFILL_BATCH_SIZE: u64 = 512;
const TURN_PERMISSION_PROFILE_BACKFILL_YIELD_MS: u64 = 10;
const DEFAULT_TURN_PERMISSION_PROFILE_MODE: &str = "full_access";
const DEFAULT_TURN_PERMISSION_PROFILE_SOURCE: &str = "defaulted";
const DEFAULT_TURN_PERMISSION_PROFILE_SNAPSHOT_JSON: &str = r#"{"mode":"full_access","source":"defaulted","effective_policy":{"default_behavior":"allow","file_read":"allow","file_write":"allow","shell_command":"allow","network":"allow","mcp_read":"allow","mcp_write_or_unknown":"allow","dynamic_skill_tool":"allow","computer_use":"allow","task_subagent":"allow"}}"#;

#[derive(Debug, FromQueryResult)]
struct LegacyTurnEventPayload {
    id: String,
    payload: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct TurnPermissionProfileBackfillSummary {
    pub(crate) skipped: bool,
    pub(crate) batches: u64,
    pub(crate) turn_events_updated: u64,
    pub(crate) turns_updated: u64,
}

pub(super) async fn run(crud_store: &CrudStore) {
    match backfill_once(crud_store).await {
        Ok(summary) if summary.skipped => {}
        Ok(summary) => {
            info!(
                batches = summary.batches,
                turn_events_updated = summary.turn_events_updated,
                turns_updated = summary.turns_updated,
                "turn permission profile payload backfill completed"
            );
        }
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "turn permission profile payload backfill failed at startup"
            );
        }
    }
}

pub(crate) async fn backfill_once(
    crud_store: &CrudStore,
) -> Result<TurnPermissionProfileBackfillSummary> {
    let db = crud_store.database_connection();
    if backfill_is_current(&db).await? {
        return Ok(TurnPermissionProfileBackfillSummary {
            skipped: true,
            ..Default::default()
        });
    }

    let started_at = now_datetime();
    upsert_projection_meta(
        &db,
        ProjectionMetaRecord {
            projection_key: TURN_PERMISSION_PROFILE_BACKFILL_KEY.to_owned(),
            projection_version: TURN_PERMISSION_PROFILE_BACKFILL_VERSION,
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

    let result = backfill_all_batches(&db).await;
    match result {
        Ok(summary) => {
            mark_backfill_complete(&db, &summary).await?;
            Ok(summary)
        }
        Err(error) => {
            mark_backfill_failed(&db, &error).await?;
            Err(error)
        }
    }
}

async fn backfill_all_batches(
    db: &DatabaseConnection,
) -> Result<TurnPermissionProfileBackfillSummary> {
    let mut summary = TurnPermissionProfileBackfillSummary::default();

    loop {
        let turn_events_updated = backfill_turn_event_batch(db)
            .await
            .context("failed to backfill turn_event permission profiles")?;
        let turns_updated = backfill_turn_batch(db)
            .await
            .context("failed to backfill turn permission profile columns")?;

        if turn_events_updated == 0 && turns_updated == 0 {
            break;
        }

        summary.batches = summary.batches.saturating_add(1);
        summary.turn_events_updated = summary
            .turn_events_updated
            .saturating_add(turn_events_updated);
        summary.turns_updated = summary.turns_updated.saturating_add(turns_updated);

        tokio::time::sleep(Duration::from_millis(
            TURN_PERMISSION_PROFILE_BACKFILL_YIELD_MS,
        ))
        .await;
    }

    Ok(summary)
}

async fn backfill_turn_event_batch(db: &DatabaseConnection) -> Result<u64> {
    let candidates = LegacyTurnEventPayload::find_by_statement(Statement::from_string(
        db.get_database_backend(),
        format!(
            "SELECT id, payload \
             FROM turn_event \
             WHERE (\
                    json_type(payload, '$.payload.turn') = 'object' \
                    AND (\
                        json_type(payload, '$.payload.turn.permission_profile') IS NULL \
                        OR json_type(payload, '$.payload.turn.permission_profile') = 'null'\
                    )\
                 ) \
                OR EXISTS (\
                    SELECT 1 \
                    FROM json_each(payload, '$.payload.thread.turns') \
                    WHERE json_type(value) = 'object' \
                      AND (\
                          json_type(value, '$.permission_profile') IS NULL \
                          OR json_type(value, '$.permission_profile') = 'null'\
                      )\
               ) \
                OR (\
                    json_extract(payload, '$.kind') = 'turn_tool_loop_budget_exceeded' \
                    AND json_extract(payload, '$.payload.action') = 'fail_turn'\
               ) \
             ORDER BY created_at, id \
             LIMIT {TURN_PERMISSION_PROFILE_BACKFILL_BATCH_SIZE}",
        ),
    ))
    .all(db)
    .await
    .context("failed to list legacy turn_event permission profile payloads")?;

    let default_profile = default_permission_profile_json()?;
    let mut updated = 0_u64;
    for candidate in candidates {
        let mut payload: JsonValue = serde_json::from_str(candidate.payload.as_str())
            .with_context(|| format!("failed to decode turn_event `{}` payload", candidate.id))?;
        if !patch_turn_event_payload(&mut payload, &default_profile) {
            continue;
        }

        turn_event::ActiveModel {
            id: Set(candidate.id),
            payload: Set(serde_json::to_string(&payload)
                .context("failed to serialize patched turn_event payload")?),
            ..Default::default()
        }
        .update(db)
        .await
        .context("failed to update legacy turn_event permission profile payloads")?;
        updated = updated.saturating_add(1);
    }

    Ok(updated)
}

async fn backfill_turn_batch(db: &DatabaseConnection) -> Result<u64> {
    let sql = format!(
        "UPDATE turn \
         SET permission_profile_mode = COALESCE(\
                permission_profile_mode, \
                '{DEFAULT_TURN_PERMISSION_PROFILE_MODE}'\
             ), \
             permission_profile_source = COALESCE(\
                permission_profile_source, \
                '{DEFAULT_TURN_PERMISSION_PROFILE_SOURCE}'\
             ), \
             permission_profile_snapshot_json = COALESCE(\
                permission_profile_snapshot_json, \
                '{DEFAULT_TURN_PERMISSION_PROFILE_SNAPSHOT_JSON}'\
             ) \
         WHERE id IN (\
            SELECT id FROM (\
                SELECT id \
                FROM turn \
                WHERE permission_profile_mode IS NULL \
                   OR permission_profile_source IS NULL \
                   OR permission_profile_snapshot_json IS NULL \
                ORDER BY created_at, id \
                LIMIT {TURN_PERMISSION_PROFILE_BACKFILL_BATCH_SIZE}\
            )\
         )",
    );
    let result = db
        .execute_unprepared(sql.as_str())
        .await
        .context("failed to update legacy turn permission profile columns")?;
    Ok(result.rows_affected())
}

async fn backfill_is_current(db: &DatabaseConnection) -> Result<bool> {
    let Some(meta) = find_projection_meta(db, TURN_PERMISSION_PROFILE_BACKFILL_KEY).await? else {
        return Ok(false);
    };
    Ok(
        meta.projection_version == TURN_PERMISSION_PROFILE_BACKFILL_VERSION
            && meta.status == PROJECTION_META_STATUS_COMPLETE,
    )
}

async fn mark_backfill_complete(
    db: &DatabaseConnection,
    summary: &TurnPermissionProfileBackfillSummary,
) -> Result<()> {
    let now = now_datetime();
    upsert_projection_meta(
        db,
        ProjectionMetaRecord {
            projection_key: TURN_PERMISSION_PROFILE_BACKFILL_KEY.to_owned(),
            projection_version: TURN_PERMISSION_PROFILE_BACKFILL_VERSION,
            status: PROJECTION_META_STATUS_COMPLETE.to_owned(),
            source_thread_count: 0,
            source_turn_count: summary.turns_updated as i64,
            source_turn_item_count: 0,
            source_turn_event_count: summary.turn_events_updated as i64,
            last_error: None,
            backfill_started_at: Some(now),
            backfilled_at: Some(now),
            created_at: now,
            updated_at: now,
        },
    )
    .await
}

async fn mark_backfill_failed(db: &DatabaseConnection, error: &anyhow::Error) -> Result<()> {
    let now = now_datetime();
    upsert_projection_meta(
        db,
        ProjectionMetaRecord {
            projection_key: TURN_PERMISSION_PROFILE_BACKFILL_KEY.to_owned(),
            projection_version: TURN_PERMISSION_PROFILE_BACKFILL_VERSION,
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

fn default_permission_profile_json() -> Result<JsonValue> {
    serde_json::from_str(DEFAULT_TURN_PERMISSION_PROFILE_SNAPSHOT_JSON)
        .context("failed to parse default permission profile snapshot")
}

fn patch_turn_event_payload(payload: &mut JsonValue, default_profile: &JsonValue) -> bool {
    let mut changed = false;

    if let Some(turn) = payload
        .get_mut("payload")
        .and_then(|value| value.get_mut("turn"))
        .and_then(JsonValue::as_object_mut)
    {
        if !has_non_null_field(&*turn, "permission_profile") {
            turn.insert("permission_profile".to_owned(), default_profile.clone());
            changed = true;
        }
    }

    if let Some(turns) = payload
        .get_mut("payload")
        .and_then(|value| value.get_mut("thread"))
        .and_then(|value| value.get_mut("turns"))
        .and_then(JsonValue::as_array_mut)
    {
        for turn in turns {
            let Some(turn) = turn.as_object_mut() else {
                continue;
            };
            if has_non_null_field(&*turn, "permission_profile") {
                continue;
            }
            turn.insert("permission_profile".to_owned(), default_profile.clone());
            changed = true;
        }
    }

    if payload.get("kind").and_then(JsonValue::as_str) == Some("turn_tool_loop_budget_exceeded") {
        if let Some(event_payload) = payload
            .get_mut("payload")
            .and_then(JsonValue::as_object_mut)
        {
            if event_payload.get("action").and_then(JsonValue::as_str) == Some("fail_turn") {
                event_payload.insert(
                    "action".to_owned(),
                    JsonValue::String("continue_in_next_window".to_owned()),
                );
                changed = true;
            }
        }
    }

    changed
}

fn has_non_null_field(object: &serde_json::Map<String, JsonValue>, field: &str) -> bool {
    object.get(field).is_some_and(|value| !value.is_null())
}

#[cfg(test)]
mod tests {
    use super::{TURN_PERMISSION_PROFILE_BACKFILL_KEY, backfill_once, now_datetime};
    use anyhow::Context;
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{
        CrudStore, PROJECTION_META_STATUS_COMPLETE, ProjectionMetaRecord, find_projection_meta,
        upsert_projection_meta,
    };
    use pioneer_entity::{turn, turn_event};
    use sea_orm::{Database, EntityTrait, Set};
    use sea_orm::{FromQueryResult, Statement};
    use serde_json::Value as JsonValue;

    #[derive(Debug, FromQueryResult)]
    struct CandidateCount {
        count: i64,
    }

    async fn count_missing_turn_event_permission_profiles(
        db: &sea_orm::DatabaseConnection,
    ) -> anyhow::Result<i64> {
        let row = CandidateCount::find_by_statement(Statement::from_string(
            db.get_database_backend(),
            "SELECT COUNT(*) AS count \
             FROM turn_event \
             WHERE (\
                    json_type(payload, '$.payload.turn') = 'object' \
                    AND (\
                        json_type(payload, '$.payload.turn.permission_profile') IS NULL \
                        OR json_type(payload, '$.payload.turn.permission_profile') = 'null'\
                    )\
                 ) \
                OR EXISTS (\
                    SELECT 1 \
                    FROM json_each(payload, '$.payload.thread.turns') \
                    WHERE json_type(value) = 'object' \
                      AND (\
                          json_type(value, '$.permission_profile') IS NULL \
                          OR json_type(value, '$.permission_profile') = 'null'\
                      )\
               ) \
                OR (\
                    json_extract(payload, '$.kind') = 'turn_tool_loop_budget_exceeded' \
                    AND json_extract(payload, '$.payload.action') = 'fail_turn'\
               )",
        ))
        .one(db)
        .await
        .context("failed to count missing turn_event permission profiles")?;
        Ok(row.map_or(0, |row| row.count))
    }

    async fn count_missing_turn_permission_profile_columns(
        db: &sea_orm::DatabaseConnection,
    ) -> anyhow::Result<i64> {
        let row = CandidateCount::find_by_statement(Statement::from_string(
            db.get_database_backend(),
            "SELECT COUNT(*) AS count \
             FROM turn \
             WHERE permission_profile_mode IS NULL \
                OR permission_profile_source IS NULL \
                OR permission_profile_snapshot_json IS NULL",
        ))
        .one(db)
        .await
        .context("failed to count missing turn permission profile columns")?;
        Ok(row.map_or(0, |row| row.count))
    }

    #[tokio::test]
    async fn startup_backfill_patches_legacy_permission_profiles_without_resetting_refill_marker() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection.clone());
        let now = now_datetime();
        let turn_id = "turn_legacy_permission_profile";
        let refill_key = "thread_episodic_workspace_capsule_refill";

        turn::Entity::insert(turn::ActiveModel {
            id: Set(turn_id.to_owned()),
            thread_id: Set("thread_legacy_permission_profile".to_owned()),
            status: Set("completed".to_owned()),
            error: Set(None),
            prompt_manifest_json: Set("{}".to_owned()),
            prompt_compiler_version: Set(None),
            prompt_profile: Set(None),
            prompt_fingerprint_stable: Set(None),
            prompt_fingerprint_dynamic: Set(None),
            prompt_fingerprint_full: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
            turn_kind: Set("conversation".to_owned()),
            origin: Set("user".to_owned()),
            reasoning_effort: Set(None),
            permission_profile_mode: Set(None),
            permission_profile_source: Set(None),
            permission_profile_snapshot_json: Set(None),
        })
        .exec(&connection)
        .await
        .expect("legacy turn should insert");

        turn_event::Entity::insert(turn_event::ActiveModel {
            id: Set("evt_legacy_permission_profile".to_owned()),
            thread_id: Set("thread_legacy_permission_profile".to_owned()),
            turn_id: Set(turn_id.to_owned()),
            sequence: Set(1),
            event_type: Set("turn/completed".to_owned()),
            payload: Set(serde_json::json!({
                "kind": "turn_started",
                "payload": {
                    "thread": {
                        "id": "thread_legacy_permission_profile",
                        "turns": [
                            {
                                "id": turn_id,
                                "status": "Completed"
                            },
                            {
                                "id": "turn_already_profiled",
                                "status": "Completed",
                                "permission_profile": {
                                    "mode": "supervised",
                                    "source": "composer",
                                    "effective_policy": {
                                        "default_behavior": "ask",
                                        "file_read": "allow",
                                        "file_write": "ask",
                                        "shell_command": "ask",
                                        "network": "ask",
                                        "mcp_read": "allow",
                                        "mcp_write_or_unknown": "ask",
                                        "dynamic_skill_tool": "ask",
                                        "computer_use": "ask",
                                        "task_subagent": "ask"
                                    }
                                }
                            }
                        ]
                    },
                    "sandbox_mode": "FullAccess",
                    "turn": {
                        "id": turn_id,
                        "status": "Completed",
                        "permission_profile": null
                    },
                    "input": []
                }
            })
            .to_string()),
            created_at: Set(now),
        })
        .exec(&connection)
        .await
        .expect("legacy turn_event should insert");

        turn_event::Entity::insert(turn_event::ActiveModel {
            id: Set("evt_legacy_tool_loop_action".to_owned()),
            thread_id: Set("thread_legacy_permission_profile".to_owned()),
            turn_id: Set(turn_id.to_owned()),
            sequence: Set(2),
            event_type: Set("turn/tool_loop/budget_exceeded".to_owned()),
            payload: Set(serde_json::json!({
                "kind": "turn_tool_loop_budget_exceeded",
                "payload": {
                    "workspace_id": "workspace_legacy_permission_profile",
                    "thread_id": "thread_legacy_permission_profile",
                    "turn_id": turn_id,
                    "limit_kind": "provider_returned_tools_after_tools_disabled",
                    "limit": 0,
                    "observed": 2,
                    "action": "fail_turn",
                    "reason": "provider_returned_tools_after_tools_disabled"
                }
            })
            .to_string()),
            created_at: Set(now),
        })
        .exec(&connection)
        .await
        .expect("legacy tool loop event should insert");

        upsert_projection_meta(
            &connection,
            ProjectionMetaRecord {
                projection_key: refill_key.to_owned(),
                projection_version: 1,
                status: PROJECTION_META_STATUS_COMPLETE.to_owned(),
                source_thread_count: 1,
                source_turn_count: 1,
                source_turn_item_count: 1,
                source_turn_event_count: 1,
                last_error: None,
                backfill_started_at: Some(now),
                backfilled_at: Some(now),
                created_at: now,
                updated_at: now,
            },
        )
        .await
        .expect("refill marker should insert");
        let summary = backfill_once(&store).await.expect("backfill should run");
        assert!(!summary.skipped);
        assert_eq!(summary.turn_events_updated, 2);
        assert_eq!(summary.turns_updated, 1);

        assert_eq!(
            count_missing_turn_event_permission_profiles(&connection)
                .await
                .expect("must count event candidates"),
            0
        );
        assert_eq!(
            count_missing_turn_permission_profile_columns(&connection)
                .await
                .expect("must count turn candidates"),
            0
        );

        let event = turn_event::Entity::find_by_id("evt_legacy_permission_profile".to_owned())
            .one(&connection)
            .await
            .expect("event should load")
            .expect("event should exist");
        let payload: JsonValue =
            serde_json::from_str(event.payload.as_str()).expect("payload should decode");
        assert_eq!(
            payload["payload"]["turn"]["permission_profile"]["mode"],
            "full_access"
        );
        assert_eq!(
            payload["payload"]["turn"]["permission_profile"]["source"],
            "defaulted"
        );
        assert_eq!(
            payload["payload"]["thread"]["turns"][0]["permission_profile"]["mode"],
            "full_access"
        );
        assert_eq!(
            payload["payload"]["thread"]["turns"][0]["permission_profile"]["source"],
            "defaulted"
        );
        assert_eq!(
            payload["payload"]["thread"]["turns"][1]["permission_profile"]["mode"],
            "supervised"
        );
        assert_eq!(
            payload["payload"]["thread"]["turns"][1]["permission_profile"]["source"],
            "composer"
        );

        let event = turn_event::Entity::find_by_id("evt_legacy_tool_loop_action".to_owned())
            .one(&connection)
            .await
            .expect("tool loop event should load")
            .expect("tool loop event should exist");
        let payload: JsonValue =
            serde_json::from_str(event.payload.as_str()).expect("payload should decode");
        assert_eq!(payload["payload"]["action"], "continue_in_next_window");

        let turn = turn::Entity::find_by_id(turn_id.to_owned())
            .one(&connection)
            .await
            .expect("turn should load")
            .expect("turn should exist");
        assert_eq!(turn.permission_profile_mode.as_deref(), Some("full_access"));
        assert_eq!(turn.permission_profile_source.as_deref(), Some("defaulted"));
        assert!(turn.permission_profile_snapshot_json.is_some());

        let refill_meta = find_projection_meta(&connection, refill_key)
            .await
            .expect("refill meta should query")
            .expect("refill marker should remain");
        assert_eq!(refill_meta.status, PROJECTION_META_STATUS_COMPLETE);

        let own_meta = find_projection_meta(&connection, TURN_PERMISSION_PROFILE_BACKFILL_KEY)
            .await
            .expect("own meta should query")
            .expect("own marker should exist");
        assert_eq!(own_meta.status, PROJECTION_META_STATUS_COMPLETE);
        assert_eq!(own_meta.projection_version, 1);

        let skipped = backfill_once(&store)
            .await
            .expect("second backfill should skip");
        assert!(skipped.skipped);
    }
}
