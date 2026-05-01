use pioneer_entity::workspace;
use pioneer_protocol::Workspace;
use pioneer_sqlite::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, SqliteWriteCoordinator,
    is_sqlite_lock_message, retry_with_backoff,
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter,
    QueryOrder, Set,
};
use std::future::Future;
use std::time::Duration;

pub const DEFAULT_WORKSPACE_ID: &str = "000000000000000000000";
pub const DEFAULT_WORKSPACE_NAME: &str = "Default Workspace";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkspaceError {
    InvalidWorkspaceId,
    InvalidWorkspaceName,
    WorkspaceNotFound(String),
    WorkspaceInactive(String),
    Internal(String),
}

impl std::fmt::Display for WorkspaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidWorkspaceId => f.write_str("workspace id must not be empty"),
            Self::InvalidWorkspaceName => f.write_str("workspace name must not be empty"),
            Self::WorkspaceNotFound(id) => write!(f, "workspace `{id}` does not exist"),
            Self::WorkspaceInactive(id) => write!(f, "workspace `{id}` is not active"),
            Self::Internal(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for WorkspaceError {}

#[derive(Clone)]
pub struct WorkspaceManager {
    connection: DatabaseConnection,
    write_coordinator: SqliteWriteCoordinator,
}

impl WorkspaceManager {
    pub fn new(connection: DatabaseConnection) -> Self {
        Self {
            connection,
            write_coordinator: SqliteWriteCoordinator::default(),
        }
    }

    pub async fn validate_workspace_id(
        &self,
        requested_workspace_id: &str,
    ) -> Result<String, WorkspaceError> {
        self.run_with_retry(|| async {
            let workspace_id = normalize_workspace_id(requested_workspace_id)?;

            let model = workspace::Entity::find_by_id(workspace_id.clone())
                .one(&self.connection)
                .await
                .map_err(|error| {
                    WorkspaceError::Internal(format!(
                        "failed to query workspace `{workspace_id}`: {error}"
                    ))
                })?;

            let Some(model) = model else {
                return Err(WorkspaceError::WorkspaceNotFound(workspace_id));
            };

            if !model.is_active {
                return Err(WorkspaceError::WorkspaceInactive(model.id));
            }

            Ok(model.id)
        })
        .await
    }

    pub async fn list_workspaces(&self) -> Result<Vec<Workspace>, WorkspaceError> {
        self.run_with_retry(|| async {
            let models = workspace::Entity::find()
                .order_by_asc(workspace::Column::CreatedAt)
                .all(&self.connection)
                .await
                .map_err(|error| {
                    WorkspaceError::Internal(format!("failed to query workspaces: {error}"))
                })?;

            Ok(models.into_iter().map(model_to_workspace).collect())
        })
        .await
    }

    pub async fn create_workspace(
        &self,
        requested_workspace_id: &str,
        requested_name: Option<&str>,
    ) -> Result<Workspace, WorkspaceError> {
        self.run_serialized_write(|| async {
            let requested_workspace_id = normalize_workspace_id(requested_workspace_id)?;
            let explicit_name = normalize_workspace_name(requested_name)?;

            let existing_workspace = workspace::Entity::find_by_id(requested_workspace_id.clone())
                .one(&self.connection)
                .await
                .map_err(|error| {
                    WorkspaceError::Internal(format!(
                        "failed to query workspace `{requested_workspace_id}`: {error}"
                    ))
                })?;

            if let Some(existing_workspace) = existing_workspace {
                return Ok(model_to_workspace(existing_workspace));
            }

            let name = match explicit_name {
                Some(name) => name,
                None => {
                    let workspace_count = workspace::Entity::find()
                        .count(&self.connection)
                        .await
                        .map_err(|error| {
                        WorkspaceError::Internal(format!("failed to count workspaces: {error}"))
                    })?;
                    format!("Workspace {}", workspace_count + 1)
                }
            };

            let has_current_active_workspace = workspace::Entity::find()
                .filter(workspace::Column::IsCurrent.eq(true))
                .filter(workspace::Column::IsActive.eq(true))
                .count(&self.connection)
                .await
                .map_err(|error| {
                    WorkspaceError::Internal(format!(
                        "failed to check current active workspace: {error}"
                    ))
                })?
                > 0;

            let model = workspace::ActiveModel {
                id: Set(requested_workspace_id),
                name: Set(name),
                is_active: Set(true),
                is_current: Set(!has_current_active_workspace),
                ..Default::default()
            }
            .insert(&self.connection)
            .await
            .map_err(|error| {
                WorkspaceError::Internal(format!("failed to create workspace: {error}"))
            })?;

            Ok(model_to_workspace(model))
        })
        .await
    }

    pub async fn ensure_default_workspace(&self) -> Result<Workspace, WorkspaceError> {
        self.run_serialized_write(|| async {
            let existing_workspaces = self.list_workspaces().await?;
            if let Some(workspace) = select_workspace(existing_workspaces.as_slice()) {
                return Ok(workspace);
            }

            let inserted = workspace::ActiveModel {
                id: Set(DEFAULT_WORKSPACE_ID.to_owned()),
                name: Set(DEFAULT_WORKSPACE_NAME.to_owned()),
                is_active: Set(true),
                is_current: Set(true),
                ..Default::default()
            }
            .insert(&self.connection)
            .await;

            match inserted {
                Ok(model) => Ok(model_to_workspace(model)),
                Err(insert_error) => {
                    let existing = workspace::Entity::find_by_id(DEFAULT_WORKSPACE_ID.to_owned())
                        .one(&self.connection)
                        .await
                        .map_err(|error| {
                            WorkspaceError::Internal(format!(
                                "failed to query default workspace after insert failure: {error}"
                            ))
                        })?;

                    let Some(existing) = existing else {
                        return Err(WorkspaceError::Internal(format!(
                            "failed to ensure default workspace: {insert_error}"
                        )));
                    };

                    if existing.is_active && existing.is_current {
                        return Ok(model_to_workspace(existing));
                    }

                    let mut active: workspace::ActiveModel = existing.into();
                    active.name = Set(DEFAULT_WORKSPACE_NAME.to_owned());
                    active.is_active = Set(true);
                    active.is_current = Set(true);

                    let updated = active.update(&self.connection).await.map_err(|error| {
                        WorkspaceError::Internal(format!(
                            "failed to activate default workspace after insert failure: {error}"
                        ))
                    })?;

                    Ok(model_to_workspace(updated))
                }
            }
        })
        .await
    }

    async fn run_with_retry<T, F, Fut>(&self, operation: F) -> Result<T, WorkspaceError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, WorkspaceError>>,
    {
        retry_with_backoff(
            operation,
            is_workspace_sqlite_lock_error,
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }

    async fn run_serialized_write<T, F, Fut>(&self, operation: F) -> Result<T, WorkspaceError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, WorkspaceError>>,
    {
        self.write_coordinator
            .run_serialized_with_retry(operation, is_workspace_sqlite_lock_error)
            .await
    }
}

fn select_workspace(workspaces: &[Workspace]) -> Option<Workspace> {
    workspaces
        .iter()
        .find(|workspace| workspace.is_active && workspace.is_current)
        .or_else(|| workspaces.iter().find(|workspace| workspace.is_active))
        .cloned()
}

fn normalize_workspace_id(value: &str) -> Result<String, WorkspaceError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(WorkspaceError::InvalidWorkspaceId);
    }
    Ok(trimmed.to_owned())
}

fn normalize_workspace_name(value: Option<&str>) -> Result<Option<String>, WorkspaceError> {
    let Some(value) = value else {
        return Ok(None);
    };

    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(WorkspaceError::InvalidWorkspaceName);
    }

    Ok(Some(trimmed.to_owned()))
}

fn model_to_workspace(model: workspace::Model) -> Workspace {
    Workspace {
        id: model.id,
        name: model.name,
        is_active: model.is_active,
        is_current: model.is_current,
        created_at: model.created_at.timestamp(),
        updated_at: model.updated_at.timestamp(),
    }
}

fn is_workspace_sqlite_lock_error(error: &WorkspaceError) -> bool {
    let WorkspaceError::Internal(message) = error else {
        return false;
    };

    is_sqlite_lock_message(message)
}

#[cfg(test)]
mod tests {
    use super::{WorkspaceError, WorkspaceManager};
    use migration::{Migrator, MigratorTrait};
    use pioneer_entity::workspace;
    use sea_orm::{ActiveModelTrait, Database, EntityTrait, Set};

    fn workspace_id(index: u32) -> String {
        format!("ws_{index:018}")
    }

    #[tokio::test]
    async fn validates_explicit_active_workspace() {
        let manager = setup_workspace_manager().await;
        let requested_id = workspace_id(1);
        insert_workspace(&manager, &requested_id, true, false).await;

        let validated = manager
            .validate_workspace_id(requested_id.as_str())
            .await
            .expect("explicit workspace should validate");

        assert_eq!(validated, requested_id);
    }

    #[tokio::test]
    async fn rejects_unknown_workspace() {
        let manager = setup_workspace_manager().await;
        let error = manager
            .validate_workspace_id(workspace_id(2).as_str())
            .await
            .expect_err("unknown workspace should fail");

        assert!(matches!(error, WorkspaceError::WorkspaceNotFound(_)));
    }

    #[tokio::test]
    async fn rejects_inactive_workspace() {
        let manager = setup_workspace_manager().await;
        let requested_id = workspace_id(3);
        insert_workspace(&manager, &requested_id, false, false).await;

        let error = manager
            .validate_workspace_id(requested_id.as_str())
            .await
            .expect_err("inactive workspace should fail");

        assert_eq!(error, WorkspaceError::WorkspaceInactive(requested_id));
    }

    #[tokio::test]
    async fn rejects_empty_workspace_id() {
        let manager = setup_workspace_manager().await;
        let error = manager
            .validate_workspace_id("   ")
            .await
            .expect_err("empty id should fail");
        assert_eq!(error, WorkspaceError::InvalidWorkspaceId);
    }

    #[tokio::test]
    async fn lists_workspaces() {
        let manager = setup_workspace_manager().await;
        let first = workspace_id(4);
        let second = workspace_id(5);
        insert_workspace(&manager, &first, true, true).await;
        insert_workspace(&manager, &second, true, false).await;

        let workspaces = manager
            .list_workspaces()
            .await
            .expect("workspace list should succeed");

        let ids: Vec<&str> = workspaces
            .iter()
            .map(|workspace| workspace.id.as_str())
            .collect();
        assert_eq!(ids, vec![first.as_str(), second.as_str()]);
    }

    #[tokio::test]
    async fn create_workspace_uses_requested_id_and_current_when_first() {
        let manager = setup_workspace_manager().await;
        let requested_id = workspace_id(8);

        let workspace = manager
            .create_workspace(requested_id.as_str(), Some("Sandbox"))
            .await
            .expect("workspace create should succeed");

        assert_eq!(workspace.id, requested_id);
        assert_eq!(workspace.name, "Sandbox");
        assert!(workspace.is_active);
        assert!(workspace.is_current);
    }

    #[tokio::test]
    async fn create_workspace_uses_default_name_and_not_current_when_current_exists() {
        let manager = setup_workspace_manager().await;
        insert_workspace(&manager, &workspace_id(6), true, true).await;
        let requested_id = workspace_id(9);

        let workspace = manager
            .create_workspace(requested_id.as_str(), None)
            .await
            .expect("workspace create should succeed");

        assert_eq!(workspace.id, requested_id);
        assert_eq!(workspace.name, "Workspace 2");
        assert!(workspace.is_active);
        assert!(!workspace.is_current);
    }

    #[tokio::test]
    async fn create_workspace_rejects_empty_name() {
        let manager = setup_workspace_manager().await;
        let error = manager
            .create_workspace(workspace_id(10).as_str(), Some("  "))
            .await
            .expect_err("empty name should fail");

        assert_eq!(error, WorkspaceError::InvalidWorkspaceName);
    }

    #[tokio::test]
    async fn create_workspace_rejects_empty_workspace_id() {
        let manager = setup_workspace_manager().await;
        let error = manager
            .create_workspace("   ", Some("Sandbox"))
            .await
            .expect_err("empty id should fail");

        assert_eq!(error, WorkspaceError::InvalidWorkspaceId);
    }

    #[tokio::test]
    async fn create_workspace_is_idempotent_for_existing_id() {
        let manager = setup_workspace_manager().await;
        let requested_id = workspace_id(11);

        let first = manager
            .create_workspace(requested_id.as_str(), Some("Workspace A"))
            .await
            .expect("first create should succeed");
        let second = manager
            .create_workspace(requested_id.as_str(), Some("Workspace B"))
            .await
            .expect("second create should return existing workspace");

        assert_eq!(first.id, requested_id);
        assert_eq!(second.id, requested_id);
        assert_eq!(second.name, "Workspace A");
    }

    #[tokio::test]
    async fn ensure_default_workspace_creates_single_workspace_when_empty() {
        let manager = setup_workspace_manager().await;

        let first = manager
            .ensure_default_workspace()
            .await
            .expect("first ensure should succeed");
        let second = manager
            .ensure_default_workspace()
            .await
            .expect("second ensure should succeed");

        assert_eq!(first.id, super::DEFAULT_WORKSPACE_ID);
        assert_eq!(second.id, super::DEFAULT_WORKSPACE_ID);
        assert_eq!(first.name, super::DEFAULT_WORKSPACE_NAME);
        assert!(first.is_active);
        assert!(first.is_current);
        assert!(second.is_active);
        assert!(second.is_current);

        let rows = workspace::Entity::find()
            .all(&manager.connection)
            .await
            .expect("workspace list should succeed");
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn ensure_default_workspace_prefers_existing_active_workspace() {
        let manager = setup_workspace_manager().await;
        let active_id = workspace_id(12);
        insert_workspace(&manager, &active_id, true, true).await;

        let workspace = manager
            .ensure_default_workspace()
            .await
            .expect("ensure should return existing workspace");

        assert_eq!(workspace.id, active_id);
    }

    #[tokio::test]
    async fn ensure_default_workspace_creates_default_when_only_inactive_exist() {
        let manager = setup_workspace_manager().await;
        let inactive_id = workspace_id(13);
        insert_workspace(&manager, &inactive_id, false, false).await;

        let workspace = manager
            .ensure_default_workspace()
            .await
            .expect("ensure should create active default workspace");

        assert_eq!(workspace.id, super::DEFAULT_WORKSPACE_ID);
        assert!(workspace.is_active);
        assert!(workspace.is_current);

        let rows = workspace::Entity::find()
            .all(&manager.connection)
            .await
            .expect("workspace list should succeed");
        assert_eq!(rows.len(), 2);
    }

    async fn setup_workspace_manager() -> WorkspaceManager {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect to sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations should run");
        WorkspaceManager::new(connection)
    }

    async fn insert_workspace(
        manager: &WorkspaceManager,
        id: &str,
        is_active: bool,
        is_current: bool,
    ) {
        workspace::ActiveModel {
            id: Set(id.to_owned()),
            name: Set(format!("Workspace {id}")),
            is_active: Set(is_active),
            is_current: Set(is_current),
            ..Default::default()
        }
        .insert(&manager.connection)
        .await
        .expect("workspace insert should succeed");
    }
}
