use pioneer_crud::{list_active_workspaces_for_principal, workspace_repository};
use pioneer_entity::workspace;
use pioneer_postgres_pools::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, PostgresDatabase,
    is_retryable_database_conflict, retry_with_backoff,
};
use pioneer_protocol::Workspace;
use sea_orm::ConnectionTrait;
use sea_orm::{ActiveModelTrait, EntityTrait, Set, TransactionTrait};
use std::future::Future;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;
use tokio::sync::Notify;

use crate::authorization::{AuthorizedWorkspace, AuthorizedWorkspaceCollection, ResourceAction};

pub use workspace_repository::{DEFAULT_WORKSPACE_ID, DEFAULT_WORKSPACE_NAME};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkspaceError {
    InvalidWorkspaceId,
    InvalidWorkspaceName,
    NoWorkspaceUpdateFields,
    WorkspaceNotFound(String),
    WorkspaceInactive(String),
    Internal(String),
    DatabaseConflict(String),
}

impl std::fmt::Display for WorkspaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidWorkspaceId => f.write_str("workspace id must not be empty"),
            Self::InvalidWorkspaceName => f.write_str("workspace name must not be empty"),
            Self::NoWorkspaceUpdateFields => {
                f.write_str("at least one workspace field is required")
            }
            Self::WorkspaceNotFound(id) => write!(f, "workspace `{id}` does not exist"),
            Self::WorkspaceInactive(id) => write!(f, "workspace `{id}` is not active"),
            Self::Internal(message) | Self::DatabaseConflict(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for WorkspaceError {}

#[derive(Clone)]
pub struct WorkspaceManager {
    connection: PostgresDatabase,
    changes: Arc<WorkspaceChanges>,
}

#[derive(Default)]
pub(crate) struct WorkspaceChanges {
    revision: AtomicU64,
    pub(crate) wake: Notify,
}

impl WorkspaceChanges {
    pub(crate) fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }
    fn committed(&self) {
        self.revision
            .try_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .expect("Workspace revision exhausted");
        self.wake.notify_one();
    }
}

impl WorkspaceManager {
    pub fn new(connection: impl Into<PostgresDatabase>) -> Self {
        Self {
            connection: connection.into(),
            changes: Arc::default(),
        }
    }

    pub(crate) fn with_database(&self, connection: PostgresDatabase) -> Self {
        Self {
            connection,
            changes: self.changes.clone(),
        }
    }

    pub(crate) fn changes(&self) -> Arc<WorkspaceChanges> {
        self.changes.clone()
    }

    pub(crate) async fn active_page(
        &self,
        after: Option<&str>,
    ) -> Result<Vec<workspace::Model>, WorkspaceError> {
        pioneer_crud::CrudStore::new(self.connection.clone())
            .list_active_workspace_page(after)
            .await
            .map_err(|error| {
                workspace_storage_error(error, "failed to page active workspaces".to_owned())
            })
    }

    pub async fn validate_workspace_id(
        &self,
        requested_workspace_id: &str,
    ) -> Result<String, WorkspaceError> {
        self.run_with_retry(|| async {
            let workspace_id = normalize_workspace_id(requested_workspace_id)?;
            let model = find_active_workspace(&self.connection, workspace_id.as_str()).await?;
            Ok(model.id)
        })
        .await
    }

    pub async fn list_workspaces(&self) -> Result<Vec<Workspace>, WorkspaceError> {
        self.run_with_retry(|| async {
            let models = workspace_repository::list(&self.connection)
                .await
                .map_err(|error| {
                    workspace_storage_error(error, "failed to query workspaces".to_owned())
                })?;

            Ok(models.into_iter().map(model_to_workspace).collect())
        })
        .await
    }

    pub(crate) async fn list_authorized_workspaces(
        &self,
        proof: &AuthorizedWorkspaceCollection,
    ) -> Result<Vec<Workspace>, WorkspaceError> {
        if proof.action() != ResourceAction::WorkspaceList {
            return Err(WorkspaceError::Internal(
                "workspace list authorization proof action mismatch".to_owned(),
            ));
        }
        if proof.decision().is_absolute() {
            return self.list_workspaces().await;
        }
        self.run_with_retry(|| async {
            list_active_workspaces_for_principal(&self.connection, proof.principal_id())
                .await
                .map(|models| models.into_iter().map(model_to_workspace).collect())
                .map_err(|error| {
                    workspace_storage_error(
                        error,
                        "failed to query authorized workspaces".to_owned(),
                    )
                })
        })
        .await
    }

    #[cfg(test)]
    pub async fn create_workspace(
        &self,
        requested_workspace_id: &str,
        requested_name: Option<&str>,
    ) -> Result<Workspace, WorkspaceError> {
        self.create_workspace_with_cli_identities(requested_workspace_id, requested_name, &[])
            .await
    }

    pub(crate) async fn create_workspace_with_cli_identities(
        &self,
        requested_workspace_id: &str,
        requested_name: Option<&str>,
        cli_identities: &[pioneer_crud::CliRuntimeIdentitySeed],
    ) -> Result<Workspace, WorkspaceError> {
        let workspace_id = normalize_workspace_id(requested_workspace_id)?;
        let name = normalize_workspace_name(requested_name)?;
        let result = async {
            let transaction = self.connection.begin().await?;
            let (model, inserted) =
                workspace_repository::create(&transaction, &workspace_id, name).await?;
            let now = chrono::Utc::now().fixed_offset();
            pioneer_crud::ensure_pioneer_for_workspace(&transaction, &model.id, now).await?;
            pioneer_crud::ensure_cli_identities_for_workspace(
                &transaction, &model.id, cli_identities, now,
            ).await?;
            transaction.commit().await?;
            Ok::<_, anyhow::Error>((model, inserted))
        }
        .await;
        let (model, inserted) = result.map_err(|error| {
            workspace_storage_error(
                error,
                format!("failed to create workspace `{workspace_id}`"),
            )
        })?;
        if inserted {
            self.changes.committed();
        }
        Ok(model_to_workspace(model))
    }

    pub async fn ensure_default_workspace(&self) -> Result<Workspace, WorkspaceError> {
        let workspace = self
            .run_with_retry(|| async {
                let existing_workspaces = self.list_workspaces().await?;
                if let Some(workspace) = select_default_workspace(existing_workspaces.as_slice()) {
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
                    Ok(model) => {
                        self.changes.committed();
                        Ok(model_to_workspace(model))
                    }
                    Err(insert_error) => {
                        let existing =
                            workspace::Entity::find_by_id(DEFAULT_WORKSPACE_ID.to_owned())
                                .one(&self.connection)
                                .await
                                .map_err(|error| {
                                    workspace_storage_error(
                                        error,
                                        "failed to query default workspace after insert failure"
                                            .to_owned(),
                                    )
                                })?;

                        let Some(existing) = existing else {
                            return Err(workspace_storage_error(
                                insert_error,
                                "failed to ensure default workspace".to_owned(),
                            ));
                        };

                        if existing.is_active && existing.is_current {
                            return Ok(model_to_workspace(existing));
                        }

                        let mut active: workspace::ActiveModel = existing.into();
                        active.name = Set(DEFAULT_WORKSPACE_NAME.to_owned());
                        active.is_active = Set(true);
                        active.is_current = Set(true);

                        let updated = active.update(&self.connection).await.map_err(|error| {
                            workspace_storage_error(
                                error,
                                "failed to activate default workspace after insert failure"
                                    .to_owned(),
                            )
                        })?;

                        self.changes.committed();
                        Ok(model_to_workspace(updated))
                    }
                }
            })
            .await?;
        pioneer_crud::ensure_pioneer_for_workspace(
            &self.connection,
            workspace.id.as_str(),
            chrono::Utc::now().fixed_offset(),
        )
        .await
        .map_err(|error| {
            workspace_storage_error(
                error,
                format!(
                    "failed to seed reserved Pioneer identity for workspace `{}`",
                    workspace.id
                ),
            )
        })?;
        Ok(workspace)
    }

    pub(crate) async fn authorized_default_workspace(
        &self,
        proof: &AuthorizedWorkspaceCollection,
    ) -> Result<Option<Workspace>, WorkspaceError> {
        if proof.action() != ResourceAction::WorkspaceRead {
            return Err(WorkspaceError::Internal(
                "workspace default authorization proof action mismatch".to_owned(),
            ));
        }
        if proof.decision().is_absolute() {
            return self.ensure_default_workspace().await.map(Some);
        }
        self.run_with_retry(|| async {
            let workspaces =
                list_active_workspaces_for_principal(&self.connection, proof.principal_id())
                    .await
                    .map_err(|error| {
                        workspace_storage_error(
                            error,
                            "failed to query authorized default workspace".to_owned(),
                        )
                    })?
                    .into_iter()
                    .map(model_to_workspace)
                    .collect::<Vec<_>>();
            Ok(select_default_workspace(workspaces.as_slice()))
        })
        .await
    }

    pub async fn select_workspace(
        &self,
        requested_workspace_id: &str,
        make_current: bool,
    ) -> Result<Workspace, WorkspaceError> {
        if !make_current {
            return self
                .run_with_retry(|| async {
                    let workspace_id = normalize_workspace_id(requested_workspace_id)?;
                    let model =
                        find_active_workspace(&self.connection, workspace_id.as_str()).await?;
                    Ok(model_to_workspace(model))
                })
                .await;
        }

        self.run_with_retry(|| async {
            let workspace_id = normalize_workspace_id(requested_workspace_id)?;
            let transaction = self.connection.begin().await.map_err(|error| {
                workspace_storage_error(
                    error,
                    "failed to start workspace select transaction".to_owned(),
                )
            })?;

            workspace_repository::lock_current_selection(&transaction)
                .await
                .map_err(|error| {
                    workspace_storage_error(error, "lock workspace selection".to_owned())
                })?;
            let model = find_active_workspace(&transaction, workspace_id.as_str()).await?;

            if model.is_current {
                transaction.commit().await.map_err(|error| {
                    workspace_storage_error(
                        error,
                        "failed to commit workspace select transaction".to_owned(),
                    )
                })?;
                return Ok(model_to_workspace(model));
            }

            let updated = workspace_repository::set_current(
                &transaction,
                model,
                chrono::Utc::now().fixed_offset(),
            )
            .await
            .map_err(|error| workspace_storage_error(error, "select workspace".to_owned()))?;

            transaction.commit().await.map_err(|error| {
                workspace_storage_error(
                    error,
                    "failed to commit workspace select transaction".to_owned(),
                )
            })?;

            self.changes.committed();
            Ok(model_to_workspace(updated))
        })
        .await
    }

    pub(crate) async fn select_authorized_workspace(
        &self,
        proof: &AuthorizedWorkspace,
        requested_make_current: bool,
    ) -> Result<Workspace, WorkspaceError> {
        if proof.action() != ResourceAction::WorkspaceRead {
            return Err(WorkspaceError::Internal(
                "workspace selection authorization proof action mismatch".to_owned(),
            ));
        }
        let make_current = proof.decision().is_absolute() && requested_make_current;
        self.select_workspace(proof.workspace_id(), make_current)
            .await
    }

    pub async fn update_workspace(
        &self,
        requested_workspace_id: &str,
        requested_name: Option<&str>,
    ) -> Result<Workspace, WorkspaceError> {
        self.run_with_retry(|| async {
            let workspace_id = normalize_workspace_id(requested_workspace_id)?;
            let name = normalize_workspace_name(requested_name)?
                .ok_or(WorkspaceError::NoWorkspaceUpdateFields)?;
            let model = find_active_workspace(&self.connection, workspace_id.as_str()).await?;
            let updated = workspace_repository::set_name(
                &self.connection,
                model,
                name,
                chrono::Utc::now().fixed_offset(),
            )
            .await
            .map_err(|error| workspace_storage_error(error, "rename workspace".to_owned()))?;

            self.changes.committed();
            Ok(model_to_workspace(updated))
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
            |error| matches!(error, WorkspaceError::DatabaseConflict(_)),
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }
}

fn select_default_workspace(workspaces: &[Workspace]) -> Option<Workspace> {
    workspaces
        .iter()
        .find(|workspace| workspace.is_active && workspace.is_current)
        .or_else(|| workspaces.iter().find(|workspace| workspace.is_active))
        .cloned()
}

async fn find_active_workspace<C>(
    connection: &C,
    workspace_id: &str,
) -> Result<workspace::Model, WorkspaceError>
where
    C: ConnectionTrait,
{
    let model = workspace_repository::find(connection, workspace_id)
        .await
        .map_err(|error| {
            workspace_storage_error(error, format!("failed to query workspace `{workspace_id}`"))
        })?;

    let Some(model) = model else {
        return Err(WorkspaceError::WorkspaceNotFound(workspace_id.to_owned()));
    };

    if !model.is_active {
        return Err(WorkspaceError::WorkspaceInactive(model.id));
    }

    Ok(model)
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

fn workspace_storage_error(error: impl Into<anyhow::Error>, message: String) -> WorkspaceError {
    if is_retryable_database_conflict(&error.into()) {
        WorkspaceError::DatabaseConflict(message)
    } else {
        WorkspaceError::Internal(message)
    }
}

#[cfg(test)]
mod tests {
    use super::{WorkspaceError, WorkspaceManager};
    use pioneer_entity::workspace;
    use sea_orm::{ActiveModelTrait, EntityTrait, Set};

    fn workspace_id(index: u32) -> String {
        format!("ws_{index:018}")
    }

    #[tokio::test]
    async fn validates_explicit_active_workspace() {
        pioneer_postgres_test::with_database(async |test_database| {
            let manager = setup_workspace_manager(test_database).await;
            let requested_id = workspace_id(1);
            insert_workspace(&manager, &requested_id, true, false).await;

            let validated = manager
                .validate_workspace_id(requested_id.as_str())
                .await
                .expect("explicit workspace should validate");

            assert_eq!(validated, requested_id);
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn rejects_unknown_workspace() {
        pioneer_postgres_test::with_database(async |test_database| {
            let manager = setup_workspace_manager(test_database).await;
            let error = manager
                .validate_workspace_id(workspace_id(2).as_str())
                .await
                .expect_err("unknown workspace should fail");

            assert!(matches!(error, WorkspaceError::WorkspaceNotFound(_)));
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn rejects_inactive_workspace() {
        pioneer_postgres_test::with_database(async |test_database| {
            let manager = setup_workspace_manager(test_database).await;
            let requested_id = workspace_id(3);
            insert_workspace(&manager, &requested_id, false, false).await;

            let error = manager
                .validate_workspace_id(requested_id.as_str())
                .await
                .expect_err("inactive workspace should fail");

            assert_eq!(error, WorkspaceError::WorkspaceInactive(requested_id));
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn rejects_empty_workspace_id() {
        pioneer_postgres_test::with_database(async |test_database| {
            let manager = setup_workspace_manager(test_database).await;
            let error = manager
                .validate_workspace_id("   ")
                .await
                .expect_err("empty id should fail");
            assert_eq!(error, WorkspaceError::InvalidWorkspaceId);
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn lists_workspaces() {
        pioneer_postgres_test::with_database(async |test_database| {
            let manager = setup_workspace_manager(test_database).await;
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
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn create_workspace_uses_requested_id_and_current_when_first() {
        pioneer_postgres_test::with_database(async |test_database| {
            let manager = setup_workspace_manager(test_database).await;
            let requested_id = workspace_id(8);

            let workspace = manager
                .create_workspace(requested_id.as_str(), Some("Sandbox"))
                .await
                .expect("workspace create should succeed");

            assert_eq!(workspace.id, requested_id);
            assert_eq!(workspace.name, "Sandbox");
            assert!(workspace.is_active);
            assert!(workspace.is_current);
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn create_workspace_uses_default_name_and_not_current_when_current_exists() {
        pioneer_postgres_test::with_database(async |test_database| {
            let manager = setup_workspace_manager(test_database).await;
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
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn create_workspace_rejects_invalid_inputs() {
        for (requested_id, name, expected) in [
            (
                workspace_id(10),
                Some("  "),
                WorkspaceError::InvalidWorkspaceName,
            ),
            (
                "   ".to_owned(),
                Some("Sandbox"),
                WorkspaceError::InvalidWorkspaceId,
            ),
        ] {
            pioneer_postgres_test::with_database(async |test_database| {
                let manager = setup_workspace_manager(test_database).await;
                let error = manager
                    .create_workspace(requested_id.as_str(), name)
                    .await
                    .expect_err("invalid create input should fail");
                assert_eq!(
                    error, expected,
                    "requested_id={requested_id:?}, name={name:?}"
                );
                Ok(())
            })
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn create_workspace_is_idempotent_for_existing_id() {
        pioneer_postgres_test::with_database(async |test_database| {
            let manager = setup_workspace_manager(test_database).await;
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
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn select_workspace_returns_active_without_changing_current() {
        pioneer_postgres_test::with_database(async |test_database| {
            let manager = setup_workspace_manager(test_database).await;
            let current_id = workspace_id(14);
            let selected_id = workspace_id(15);
            insert_workspace(&manager, &current_id, true, true).await;
            insert_workspace(&manager, &selected_id, true, false).await;

            let selected = manager
                .select_workspace(selected_id.as_str(), false)
                .await
                .expect("active workspace should select");

            assert_eq!(selected.id, selected_id);
            assert!(!selected.is_current);

            let current = workspace::Entity::find_by_id(current_id.clone())
                .one(&manager.connection)
                .await
                .expect("current workspace query succeeds")
                .expect("current workspace exists");
            assert!(current.is_current);
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn select_workspace_rejects_unknown_workspace() {
        pioneer_postgres_test::with_database(async |test_database| {
            let manager = setup_workspace_manager(test_database).await;

            let error = manager
                .select_workspace(workspace_id(16).as_str(), false)
                .await
                .expect_err("unknown workspace should fail");

            assert!(matches!(error, WorkspaceError::WorkspaceNotFound(_)));
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn select_workspace_rejects_inactive_workspace() {
        pioneer_postgres_test::with_database(async |test_database| {
            let manager = setup_workspace_manager(test_database).await;
            let inactive_id = workspace_id(17);
            insert_workspace(&manager, &inactive_id, false, false).await;

            let error = manager
                .select_workspace(inactive_id.as_str(), false)
                .await
                .expect_err("inactive workspace should fail");

            assert_eq!(error, WorkspaceError::WorkspaceInactive(inactive_id));
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn select_workspace_make_current_unsets_previous_current() {
        pioneer_postgres_test::with_database(async |test_database| {
            let manager = setup_workspace_manager(test_database).await;
            let previous_id = workspace_id(18);
            let selected_id = workspace_id(19);
            insert_workspace(&manager, &previous_id, true, true).await;
            insert_workspace(&manager, &selected_id, true, false).await;

            let selected = manager
                .select_workspace(selected_id.as_str(), true)
                .await
                .expect("active workspace should become current");

            assert_eq!(selected.id, selected_id);
            assert!(selected.is_current);

            let previous = workspace::Entity::find_by_id(previous_id)
                .one(&manager.connection)
                .await
                .expect("previous workspace query succeeds")
                .expect("previous workspace exists");
            assert!(!previous.is_current);
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn update_workspace_rejects_invalid_fields() {
        for (requested_id, name, expected) in [
            (
                workspace_id(21),
                Some("   "),
                WorkspaceError::InvalidWorkspaceName,
            ),
            (
                workspace_id(22),
                None,
                WorkspaceError::NoWorkspaceUpdateFields,
            ),
        ] {
            pioneer_postgres_test::with_database(async |test_database| {
                let manager = setup_workspace_manager(test_database).await;
                insert_workspace(&manager, &requested_id, true, true).await;
                let error = manager
                    .update_workspace(requested_id.as_str(), name)
                    .await
                    .expect_err("invalid update fields should fail");
                assert_eq!(
                    error, expected,
                    "requested_id={requested_id:?}, name={name:?}"
                );
                Ok(())
            })
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn update_workspace_rejects_unknown_workspace() {
        pioneer_postgres_test::with_database(async |test_database| {
            let manager = setup_workspace_manager(test_database).await;

            let error = manager
                .update_workspace(workspace_id(23).as_str(), Some("Renamed"))
                .await
                .expect_err("unknown workspace should fail");

            assert!(matches!(error, WorkspaceError::WorkspaceNotFound(_)));
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn ensure_default_workspace_creates_single_workspace_when_empty() {
        pioneer_postgres_test::with_database(async |test_database| {
            let manager = setup_workspace_manager(test_database).await;

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
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn ensure_default_workspace_prefers_existing_active_workspace() {
        pioneer_postgres_test::with_database(async |test_database| {
            let manager = setup_workspace_manager(test_database).await;
            let active_id = workspace_id(12);
            insert_workspace(&manager, &active_id, true, true).await;

            let workspace = manager
                .ensure_default_workspace()
                .await
                .expect("ensure should return existing workspace");

            assert_eq!(workspace.id, active_id);
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn ensure_default_workspace_creates_default_when_only_inactive_exist() {
        pioneer_postgres_test::with_database(async |test_database| {
            let manager = setup_workspace_manager(test_database).await;
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
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn manager_preserves_the_typed_database_scope() {
        pioneer_postgres_test::with_database(async |test_database| {
            let connection = test_database.connection().clone();
            let database =
                pioneer_postgres_pools::PostgresDatabase::from_single_connection(connection)
                    .maintenance()
                    .with_critical_writes();

            let manager = WorkspaceManager::new(database);

            assert_eq!(
                manager.connection.read_class(),
                pioneer_postgres_pools::PostgresReadClass::Maintenance
            );
            assert_eq!(
                manager.connection.write_class(),
                pioneer_postgres_pools::PostgresWriteClass::Critical
            );
            Ok(())
        })
        .await
        .unwrap();
    }

    async fn setup_workspace_manager(
        test_database: &pioneer_postgres_test::TestDatabase,
    ) -> WorkspaceManager {
        let connection = test_database.connection();
        workspace::Entity::delete_by_id(super::DEFAULT_WORKSPACE_ID)
            .exec(connection)
            .await
            .unwrap();
        WorkspaceManager::new(connection.clone())
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
