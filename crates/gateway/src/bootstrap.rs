use anyhow::{Context, Result};
use pioneer_entity::workspace;
use pioneer_sqlite::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, is_anyhow_sqlite_lock,
    retry_with_backoff,
};
use sea_orm::{ActiveModelTrait, DatabaseConnection, EntityTrait, Set};
use std::time::Duration;
use tracing::info;

use crate::workspace::{DEFAULT_WORKSPACE_ID, DEFAULT_WORKSPACE_NAME};

pub async fn bootstrap(connection: &DatabaseConnection) -> Result<()> {
    let created_default_workspace = ensure_default_workspace_exists(connection).await?;
    let repaired_completed_turn_rows =
        repair_turns_completed_after_final_agent_message(connection).await?;
    let repaired_thread_foreground_status_rows =
        repair_thread_foreground_statuses(connection).await?;
    let repaired_terminal_execution_window_rows =
        repair_terminal_turn_execution_windows(connection).await?;
    let (deleted_terminal_llm_context_rows, deleted_expired_llm_context_rows) =
        cleanup_turn_llm_context(connection).await?;
    let deleted_closed_runtime_snapshot_rows = cleanup_turn_runtime_snapshots(connection).await?;

    info!(
        created_default_workspace,
        repaired_completed_turn_rows,
        repaired_thread_foreground_status_rows,
        repaired_terminal_execution_window_rows,
        deleted_terminal_llm_context_rows,
        deleted_expired_llm_context_rows,
        deleted_closed_runtime_snapshot_rows,
        "gateway bootstrap completed"
    );
    Ok(())
}

async fn repair_thread_foreground_statuses(connection: &DatabaseConnection) -> Result<u64> {
    let store = pioneer_crud::CrudStore::new(connection.clone());
    store
        .reconcile_thread_foreground_statuses(chrono::Utc::now().fixed_offset().timestamp())
        .await
        .context("failed to reconcile thread foreground statuses during bootstrap")
}

async fn cleanup_turn_llm_context(connection: &DatabaseConnection) -> Result<(u64, u64)> {
    let store = pioneer_crud::CrudStore::new(connection.clone());
    let terminal = store
        .delete_turn_llm_context_for_terminal_turns()
        .await
        .context("failed to cleanup terminal turn_llm_context rows during bootstrap")?;
    let expired = store
        .delete_expired_turn_llm_context()
        .await
        .context("failed to cleanup expired turn_llm_context rows during bootstrap")?;
    Ok((terminal, expired))
}

async fn repair_turns_completed_after_final_agent_message(
    connection: &DatabaseConnection,
) -> Result<u64> {
    let store = pioneer_crud::CrudStore::new(connection.clone());
    store
        .complete_in_progress_turns_after_final_agent_message(
            chrono::Utc::now().fixed_offset().timestamp(),
        )
        .await
        .context("failed to repair completed turns missing terminal event during bootstrap")
}

async fn repair_terminal_turn_execution_windows(connection: &DatabaseConnection) -> Result<u64> {
    let store = pioneer_crud::CrudStore::new(connection.clone());
    store
        .close_active_execution_windows_for_terminal_turns(chrono::Utc::now().fixed_offset())
        .await
        .context("failed to repair active execution windows for terminal turns during bootstrap")
}

async fn cleanup_turn_runtime_snapshots(connection: &DatabaseConnection) -> Result<u64> {
    let store = pioneer_crud::CrudStore::new(connection.clone());
    store
        .delete_turn_runtime_snapshots_for_closed_turns()
        .await
        .context("failed to cleanup closed turn_runtime_snapshot rows during bootstrap")
}

async fn ensure_default_workspace_exists(connection: &DatabaseConnection) -> Result<bool> {
    retry_with_backoff(
        || async {
            let inserted = workspace::ActiveModel {
                id: Set(DEFAULT_WORKSPACE_ID.to_owned()),
                name: Set(DEFAULT_WORKSPACE_NAME.to_owned()),
                is_active: Set(true),
                is_current: Set(true),
                ..Default::default()
            }
            .insert(connection)
            .await;

            match inserted {
                Ok(_) => Ok(true),
                Err(insert_error) => {
                    let has_workspace = workspace::Entity::find()
                        .one(connection)
                        .await
                        .context("failed to query existing workspaces after insert failure")?
                        .is_some();

                    if has_workspace {
                        Ok(false)
                    } else {
                        Err(insert_error).context("failed to create default workspace")
                    }
                }
            }
        },
        is_anyhow_sqlite_lock,
        DEFAULT_LOCK_RETRY_ATTEMPTS,
        Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_WORKSPACE_ID, DEFAULT_WORKSPACE_NAME, cleanup_turn_llm_context,
        cleanup_turn_runtime_snapshots, ensure_default_workspace_exists,
        repair_terminal_turn_execution_windows, repair_turns_completed_after_final_agent_message,
    };
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{
        CrudStore, NewTurnExecutionWindowRecord, NewTurnLlmContextEntry, NewTurnRuntimeSnapshot,
    };
    use pioneer_entity::{thread, turn, workspace};
    use pioneer_protocol::{
        ExecutionWindowStatus, ItemCompletedNotification, TurnItem, TurnStatus,
    };
    use sea_orm::{ActiveModelTrait, Database, EntityTrait, Set};

    const DEFAULT_WORKSPACE_ID_LEN: usize = 21;

    #[tokio::test]
    async fn creates_default_workspace_when_database_is_empty() {
        let connection = setup_workspace_database().await;

        let created = ensure_default_workspace_exists(&connection)
            .await
            .expect("default workspace creation should succeed");
        assert!(created);

        let workspaces = workspace::Entity::find()
            .all(&connection)
            .await
            .expect("must read workspaces");
        assert_eq!(workspaces.len(), 1);

        let workspace = &workspaces[0];
        assert_eq!(workspace.name, DEFAULT_WORKSPACE_NAME);
        assert_eq!(workspace.id.len(), DEFAULT_WORKSPACE_ID_LEN);
        assert_eq!(workspace.id, DEFAULT_WORKSPACE_ID);
        assert!(workspace.is_active);
        assert!(workspace.is_current);
    }

    #[tokio::test]
    async fn does_not_create_second_default_workspace() {
        let connection = setup_workspace_database().await;

        let first_created = ensure_default_workspace_exists(&connection)
            .await
            .expect("first initialization should succeed");
        assert!(first_created);

        let second_created = ensure_default_workspace_exists(&connection)
            .await
            .expect("second initialization should succeed");
        assert!(!second_created);

        let workspaces = workspace::Entity::find()
            .all(&connection)
            .await
            .expect("must read workspaces");
        assert_eq!(workspaces.len(), 1);
    }

    #[tokio::test]
    async fn cleanup_removes_llm_context_for_terminal_turns() {
        let connection = setup_workspace_database().await;
        let now = chrono::Utc::now().fixed_offset();
        turn::ActiveModel {
            id: Set("terminal_turn".to_owned()),
            thread_id: Set("thread_1".to_owned()),
            status: Set("completed".to_owned()),
            turn_kind: Set("conversation".to_owned()),
            origin: Set("user".to_owned()),
            error: Set(None),
            prompt_manifest_json: Set("{}".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        }
        .insert(&connection)
        .await
        .expect("terminal turn insert should succeed");
        turn::ActiveModel {
            id: Set("active_turn".to_owned()),
            thread_id: Set("thread_1".to_owned()),
            status: Set("in_progress".to_owned()),
            turn_kind: Set("conversation".to_owned()),
            origin: Set("user".to_owned()),
            error: Set(None),
            prompt_manifest_json: Set("{}".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        }
        .insert(&connection)
        .await
        .expect("active turn insert should succeed");

        let store = CrudStore::new(connection.clone());
        store
            .insert_turn_llm_context(NewTurnLlmContextEntry {
                turn_id: "terminal_turn".to_owned(),
                item_id: Some("item_1".to_owned()),
                attempt_id: None,
                sequence: 1,
                source: "tool_result".to_owned(),
                tool_name: Some("web_fetch".to_owned()),
                payload: "{}".to_owned(),
                output_policy_snapshot: "{}".to_owned(),
                created_at: now,
                expires_at: None,
            })
            .await
            .expect("llm context insert should succeed");
        store
            .insert_turn_llm_context(NewTurnLlmContextEntry {
                turn_id: "active_turn".to_owned(),
                item_id: Some("item_2".to_owned()),
                attempt_id: None,
                sequence: 2,
                source: "tool_result".to_owned(),
                tool_name: Some("read_file".to_owned()),
                payload: "{}".to_owned(),
                output_policy_snapshot: "{}".to_owned(),
                created_at: now,
                expires_at: Some(now - chrono::Duration::days(1)),
            })
            .await
            .expect("expired llm context insert should succeed");

        let (deleted_terminal, deleted_expired) = cleanup_turn_llm_context(&connection)
            .await
            .expect("cleanup should succeed");
        assert_eq!(deleted_terminal, 1);
        assert_eq!(deleted_expired, 1);
        assert!(
            store
                .list_turn_llm_context("terminal_turn")
                .await
                .expect("context list should succeed")
                .is_empty()
        );
        assert!(
            store
                .list_turn_llm_context("active_turn")
                .await
                .expect("active context list should succeed")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn cleanup_removes_runtime_snapshots_for_closed_turns_only() {
        let connection = setup_workspace_database().await;
        let now = chrono::Utc::now().fixed_offset();
        for (turn_id, status) in [
            ("completed_turn", "completed"),
            ("failed_turn", "failed"),
            ("interrupted_turn", "interrupted"),
            ("blocked_turn", "blocked"),
            ("active_turn", "in_progress"),
        ] {
            turn::ActiveModel {
                id: Set(turn_id.to_owned()),
                thread_id: Set("thread_1".to_owned()),
                status: Set(status.to_owned()),
                turn_kind: Set("conversation".to_owned()),
                origin: Set("user".to_owned()),
                error: Set(None),
                prompt_manifest_json: Set("{}".to_owned()),
                created_at: Set(now),
                updated_at: Set(now),
                ..Default::default()
            }
            .insert(&connection)
            .await
            .expect("turn insert should succeed");
        }

        let store = CrudStore::new(connection.clone());
        for turn_id in [
            "completed_turn",
            "failed_turn",
            "interrupted_turn",
            "blocked_turn",
            "active_turn",
        ] {
            store
                .upsert_turn_runtime_snapshot(NewTurnRuntimeSnapshot {
                    turn_id: turn_id.to_owned(),
                    thread_id: "thread_1".to_owned(),
                    workspace_id: DEFAULT_WORKSPACE_ID.to_owned(),
                    mode_json: r#""Agent""#.to_owned(),
                    model: "model-a".to_owned(),
                    provider_name: "provider-a".to_owned(),
                    reasoning_effort: None,
                    agent_skill_versions_json: None,
                    hook_runtime_context_json: r#"{"mode":"agent","actor_kind":"agent"}"#
                        .to_owned(),
                    workspace_skill_policies_json: "[]".to_owned(),
                    input_json: "[]".to_owned(),
                    capabilities_json: "[]".to_owned(),
                    resolved_artifacts_json: "[]".to_owned(),
                    runtime_environment_json: "{}".to_owned(),
                    history_json: "[]".to_owned(),
                    created_at: now,
                    updated_at: now,
                })
                .await
                .expect("runtime snapshot insert should succeed");
        }

        let deleted = cleanup_turn_runtime_snapshots(&connection)
            .await
            .expect("runtime snapshot cleanup should succeed");
        assert_eq!(deleted, 3);

        for turn_id in ["completed_turn", "failed_turn", "interrupted_turn"] {
            assert!(
                store
                    .get_turn_runtime_snapshot(turn_id)
                    .await
                    .expect("runtime snapshot read should succeed")
                    .is_none(),
                "{turn_id} snapshot should be removed"
            );
        }
        for turn_id in ["blocked_turn", "active_turn"] {
            assert!(
                store
                    .get_turn_runtime_snapshot(turn_id)
                    .await
                    .expect("runtime snapshot read should succeed")
                    .is_some(),
                "{turn_id} snapshot should be retained"
            );
        }
    }

    #[tokio::test]
    async fn bootstrap_repair_completes_turn_after_final_agent_message() {
        let connection = setup_workspace_database().await;
        ensure_default_workspace_exists(&connection)
            .await
            .expect("default workspace creation should succeed");
        let now = chrono::Utc::now().fixed_offset();
        let thread_id = "thread_final_agent_message";
        let turn_id = "turn_final_agent_message";

        thread::ActiveModel {
            id: Set(thread_id.to_owned()),
            workspace_id: Set(DEFAULT_WORKSPACE_ID.to_owned()),
            name: Set(Some("final message repair".to_owned())),
            preview: Set("final message repair".to_owned()),
            mode: Set("agent".to_owned()),
            model: Set("model-a".to_owned()),
            model_provider: Set("provider-a".to_owned()),
            status: Set("active".to_owned()),
            origin_kind: Set("user".to_owned()),
            sidebar_visibility: Set("visible".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        }
        .insert(&connection)
        .await
        .expect("thread insert should succeed");
        turn::ActiveModel {
            id: Set(turn_id.to_owned()),
            thread_id: Set(thread_id.to_owned()),
            status: Set("in_progress".to_owned()),
            turn_kind: Set("conversation".to_owned()),
            origin: Set("user".to_owned()),
            error: Set(None),
            prompt_manifest_json: Set("{}".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        }
        .insert(&connection)
        .await
        .expect("turn insert should succeed");

        let store = CrudStore::new(connection.clone());
        let window = store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: DEFAULT_WORKSPACE_ID.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 1,
                    tool_call_count: 1,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({"runtimeWindowId": "running_window"}),
                    started_at: now,
                },
                now,
                now,
            )
            .await
            .expect("running window should insert");
        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: DEFAULT_WORKSPACE_ID.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::AgentMessage {
                        id: "agent_final_message".to_owned(),
                        text: "done".to_owned(),
                        phase: Default::default(),
                        markdown: None,
                        markdown_version: None,
                    },
                },
                now.timestamp(),
            )
            .await
            .expect("final agent message should materialize");

        let repaired_completed = repair_turns_completed_after_final_agent_message(&connection)
            .await
            .expect("final-agent-message repair should succeed");
        assert_eq!(repaired_completed, 1);
        let repaired_windows = repair_terminal_turn_execution_windows(&connection)
            .await
            .expect("execution window repair should succeed");
        assert_eq!(repaired_windows, 1);

        let (_workspace_id, repaired_turn) = store
            .get_turn(thread_id, turn_id)
            .await
            .expect("turn should load")
            .expect("turn should exist");
        assert_eq!(repaired_turn.status, TurnStatus::Completed);
        let repaired_window = store
            .get_turn_execution_window(window.id.as_str())
            .await
            .expect("window should load")
            .expect("window should exist");
        assert_eq!(repaired_window.status, ExecutionWindowStatus::Completed);
    }

    #[tokio::test]
    async fn bootstrap_repair_closes_active_execution_windows_for_terminal_turns() {
        let connection = setup_workspace_database().await;
        let now = chrono::Utc::now().fixed_offset();
        for (turn_id, status, error) in [
            (
                "blocked_turn_with_running_window",
                "blocked",
                Some("operator action required"),
            ),
            ("active_turn_with_running_window", "in_progress", None),
        ] {
            turn::ActiveModel {
                id: Set(turn_id.to_owned()),
                thread_id: Set("thread_1".to_owned()),
                status: Set(status.to_owned()),
                turn_kind: Set("conversation".to_owned()),
                origin: Set("user".to_owned()),
                error: Set(error.map(str::to_owned)),
                prompt_manifest_json: Set("{}".to_owned()),
                created_at: Set(now),
                updated_at: Set(now),
                ..Default::default()
            }
            .insert(&connection)
            .await
            .expect("turn insert should succeed");
        }

        let store = CrudStore::new(connection.clone());
        let blocked_window = store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: DEFAULT_WORKSPACE_ID.to_owned(),
                    thread_id: "thread_1".to_owned(),
                    turn_id: "blocked_turn_with_running_window".to_owned(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 1,
                    tool_call_count: 1,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({"runtimeWindowId": "blocked_window"}),
                    started_at: now,
                },
                now,
                now,
            )
            .await
            .expect("blocked turn window should insert");
        let active_window = store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: DEFAULT_WORKSPACE_ID.to_owned(),
                    thread_id: "thread_1".to_owned(),
                    turn_id: "active_turn_with_running_window".to_owned(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 1,
                    tool_call_count: 1,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({"runtimeWindowId": "active_window"}),
                    started_at: now,
                },
                now,
                now,
            )
            .await
            .expect("active turn window should insert");

        let repaired = repair_terminal_turn_execution_windows(&connection)
            .await
            .expect("execution window repair should succeed");
        assert_eq!(repaired, 1);

        let repaired_window = store
            .get_turn_execution_window(blocked_window.id.as_str())
            .await
            .expect("blocked window should reload")
            .expect("blocked window should exist");
        assert_eq!(repaired_window.status, ExecutionWindowStatus::Blocked);
        assert_eq!(
            repaired_window
                .metadata_json
                .get("repairedBy")
                .and_then(serde_json::Value::as_str),
            Some("bootstrap_terminal_turn_window_repair")
        );
        assert_eq!(
            repaired_window
                .metadata_json
                .get("terminalReason")
                .and_then(serde_json::Value::as_str),
            Some("operator action required")
        );

        let untouched_window = store
            .get_turn_execution_window(active_window.id.as_str())
            .await
            .expect("active window should reload")
            .expect("active window should exist");
        assert_eq!(untouched_window.status, ExecutionWindowStatus::Running);
    }

    async fn setup_workspace_database() -> sea_orm::DatabaseConnection {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to in-memory database");
        Migrator::up(&connection, None)
            .await
            .expect("must apply migrations");
        connection
    }
}
