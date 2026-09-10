//! Workspace catalog and request state. Selection belongs to Client navigation.
use crate::core::{ClientCore, ClientMutationAuthority, ClientScope};
use pioneer_protocol::{Workspace, WorkspaceChangedNotification};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceCatalogOperation {
    Bootstrap,
    Select,
    Create,
    Rename,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceCatalogPublication {
    revision: u64,
    pub(super) workspaces: Vec<Workspace>,
    loading: bool,
    pub bootstrapped_connection_id: Option<u64>,
    pub(super) action_pending: bool,
    pub(super) operation: Option<WorkspaceCatalogOperation>,
    pub(super) error: Option<String>,
}
impl WorkspaceCatalogPublication {
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn workspaces(&self) -> &[Workspace] {
        &self.workspaces
    }
    pub fn is_loading(&self) -> bool {
        self.loading
    }
    pub fn is_action_pending(&self) -> bool {
        self.action_pending
    }
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }
}
#[derive(Default)]
pub struct WorkspaceCatalogStore {
    pub(super) publication: WorkspaceCatalogPublication,
    pub(super) generation: u64,
    pub(super) request: Option<(u64, Option<u64>)>,
    pub(super) request_catalog: Vec<Workspace>,
}
impl WorkspaceCatalogStore {
    pub(crate) fn invalidate(&mut self) {
        self.generation += 1;
        self.request = None;
        self.request_catalog.clear();
        let revision = self.publication.revision;
        self.publication = WorkspaceCatalogPublication {
            revision,
            ..Default::default()
        };
    }
    fn changed(&mut self, notification: &WorkspaceChangedNotification) -> bool {
        if self.publication.workspaces.iter().any(|workspace| {
            workspace.id == notification.workspace.id
                && workspace.updated_at > notification.workspace.updated_at
        }) {
            return false;
        }
        let before = self.publication.workspaces.clone();
        crate::notifications::router::apply_workspace_changed_to_catalog(
            &mut self.publication.workspaces,
            notification,
        );
        before != self.publication.workspaces
    }
}
impl ClientCore {
    pub fn workspace_catalog(&self) -> Arc<WorkspaceCatalogPublication> {
        Arc::new(
            self.workspace_catalog
                .lock()
                .expect("workspace catalog poisoned")
                .publication
                .clone(),
        )
    }
    pub fn bootstrap_workspace_catalog(
        &self,
        persisted_workspace_id: Option<String>,
    ) -> anyhow::Result<super::actions::WorkspaceBootstrapSuccessReduction> {
        let connection = self.gateway_http_generation();
        let presentation_connection = self
            .gateway_session()
            .presentation_connection_for(connection);
        let navigation_revision = self
            .snapshot(&ClientScope::Navigation)
            .map(|p| p.revisions().scoped().get());
        let generation = {
            let mut owner = self
                .workspace_catalog
                .lock()
                .expect("workspace catalog poisoned");
            anyhow::ensure!(
                !self.is_stopped() && owner.request.is_none(),
                "Workspace catalog is unavailable or busy"
            );
            owner.generation += 1;
            let generation = owner.generation;
            owner.request_catalog = owner.publication.workspaces.clone();
            owner.request = Some((generation, connection));
            owner.publication.loading = true;
            owner.publication.operation = Some(WorkspaceCatalogOperation::Bootstrap);
            owner.publication.error = None;
            self.publish_workspace_catalog(&mut owner);
            generation
        };
        let mut result = super::bootstrap::bootstrap_workspace_catalog(
            &self.compatibility_runtime().ws_command_sender(),
            super::bootstrap::WorkspaceBootstrapRequest {
                persisted_workspace_id,
            },
        );
        let mut owner = self
            .workspace_catalog
            .lock()
            .expect("workspace catalog poisoned");
        anyhow::ensure!(
            !self.is_stopped()
                && owner.request == Some((generation, connection))
                && connection == self.gateway_http_generation(),
            "Workspace catalog request is stale"
        );
        owner.request = None;
        owner.publication.loading = false;
        if let Ok(reduction) = &mut result {
            merge_catalog_changes(
                &owner.request_catalog,
                &owner.publication.workspaces,
                &mut reduction.workspaces,
            );
            if !reduction.workspaces.iter().any(|workspace| {
                workspace.id == reduction.selected.workspace_id && workspace.is_active
            }) {
                owner.publication.workspaces = reduction.workspaces.clone();
                result = Err(anyhow::anyhow!("Workspace selection changed during loading").into());
            }
        }
        owner.request_catalog.clear();
        match &result {
            Ok(reduction) => {
                owner.publication.workspaces = reduction.workspaces.clone();
                owner.publication.bootstrapped_connection_id = presentation_connection;
            }
            Err(error) => owner.publication.error = Some(error.to_string()),
        }
        self.publish_workspace_catalog(&mut owner);
        drop(owner);
        let reduction = result?;
        self.navigate(
            crate::navigation::NavigationIntent::SelectWorkspace {
                workspace_id: Some(reduction.selected.workspace_id.clone()),
            },
            navigation_revision,
        );
        Ok(reduction)
    }
    pub(crate) fn observe_workspace_catalog(
        &self,
        notification: &pioneer_protocol::GatewayNotification,
    ) {
        if let pioneer_protocol::GatewayNotification::WorkspaceChanged(change) = notification {
            let mut owner = self
                .workspace_catalog
                .lock()
                .expect("workspace catalog poisoned");
            if !self.is_stopped() && owner.changed(change) {
                self.publish_workspace_catalog(&mut owner);
            }
        }
    }
    pub(super) fn publish_workspace_catalog(&self, owner: &mut WorkspaceCatalogStore) {
        let scope = ClientScope::WorkspaceTree { workspace_id: None };
        let previous = self.snapshot(&scope);
        let current = previous
            .as_ref()
            .and_then(|p| p.snapshot().payload::<WorkspaceCatalogPublication>());
        owner.publication.revision = current.as_ref().map_or(0, |p| p.revision);
        if current
            .as_ref()
            .is_some_and(|p| p.as_ref() == &owner.publication)
        {
            return;
        }
        owner.publication.revision = previous.map_or(1, |p| p.revisions().scoped().get() + 1);
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(owner.publication.revision),
            Arc::new(owner.publication.clone()),
            vec![],
        );
    }
}

pub(super) fn merge_catalog_changes(
    before: &[Workspace],
    current: &[Workspace],
    response: &mut Vec<Workspace>,
) {
    for workspace in current {
        if before.iter().find(|old| old.id == workspace.id) == Some(workspace) {
            continue;
        }
        if let Some(existing) = response
            .iter_mut()
            .find(|existing| existing.id == workspace.id)
        {
            *existing = workspace.clone();
        } else {
            response.push(workspace.clone());
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bootstrap_does_not_overwrite_catalog_events_received_during_loading() {
        let old = Workspace {
            id: "a".into(),
            name: "old".into(),
            is_active: true,
            is_current: true,
            created_at: 1,
            updated_at: 1,
        };
        let mut current = old.clone();
        current.name = "new".into();
        current.updated_at = 2;
        let mut response = vec![old.clone()];
        merge_catalog_changes(&[old], &[current.clone()], &mut response);
        assert_eq!(response, vec![current]);
    }
}
