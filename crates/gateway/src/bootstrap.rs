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
    let (deleted_terminal_llm_context_rows, deleted_expired_llm_context_rows) =
        cleanup_turn_llm_context(connection).await?;

    info!(
        created_default_workspace,
        deleted_terminal_llm_context_rows,
        deleted_expired_llm_context_rows,
        "gateway bootstrap completed"
    );
    Ok(())
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
        ensure_default_workspace_exists,
    };
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{CrudStore, NewTurnLlmContextEntry};
    use pioneer_entity::{turn, workspace};
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
