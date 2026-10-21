//! CLI artwork uses the same revision-addressed Client publications as Avatar Rail.
//! This owner retains demand only; HTTP, image validation and cache stay in Client.
use crate::{ports::ProviderAvatarPort, providers::ProviderCatalogView};
use gpui_kit::{component::theme::ActiveTheme, *};
use pioneer_client::{
    avatars::{
        AgentAvatarCacheResult, AvatarCacheError, AvatarPublication, avatar_identity_key,
        cli_runtime_avatar_revision,
    },
    core::{ClientCore, ClientScope},
    gateway::identity_authorization::IdentityAuthorizationPublication,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};
use tokio_util::sync::CancellationToken;

#[derive(PartialEq, Eq)]
struct AvatarScope {
    workspace: String,
    endpoint: Option<String>,
    connection: Option<u64>,
    connected: bool,
    authorization: u64,
    policy: u64,
}
struct AvatarDemand {
    cancellation: CancellationToken,
    _task: Task<Result<AgentAvatarCacheResult, AvatarCacheError>>,
}
impl Drop for AvatarDemand {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}
pub(crate) struct CliAvatars {
    port: Arc<dyn ProviderAvatarPort>,
    scope: Option<AvatarScope>,
    revisions: BTreeSet<String>,
    requests: BTreeMap<String, AvatarDemand>,
    publication_sequences: Vec<(String, Option<u64>)>,
}
fn avatar_scope(revision: &str) -> ClientScope {
    ClientScope::Avatar {
        principal_id: avatar_identity_key(&format!("agent:{revision}"), revision),
    }
}
impl CliAvatars {
    pub(crate) fn new(port: Arc<dyn ProviderAvatarPort>) -> Self {
        Self {
            port,
            scope: None,
            revisions: BTreeSet::new(),
            requests: BTreeMap::new(),
            publication_sequences: Vec::new(),
        }
    }
    fn reconcile(&mut self, scope: Option<AvatarScope>, revisions: BTreeSet<String>) {
        if self.scope != scope {
            self.requests.clear();
            self.scope = scope;
        }
        self.revisions = if self.scope.is_some() {
            revisions
        } else {
            BTreeSet::new()
        };
        // Dropping demand cancels before dropping its task, including on theme
        // changes, navigation away, access changes and destruction of the view.
        self.requests
            .retain(|revision, _| self.revisions.contains(revision));
    }
    pub(crate) fn scopes(&self) -> Vec<ClientScope> {
        self.revisions
            .iter()
            .map(|revision| avatar_scope(revision))
            .collect()
    }
    pub(crate) fn start_requests(&mut self, cx: &mut App) {
        for revision in &self.revisions {
            if self.requests.contains_key(revision) {
                continue;
            }
            let cancellation = CancellationToken::new();
            let task = self
                .port
                .resolve_agent(revision.clone(), cancellation.clone(), cx);
            self.requests.insert(
                revision.clone(),
                AvatarDemand {
                    cancellation,
                    _task: task,
                },
            );
        }
    }
    pub(crate) fn sync_publications(&mut self, client: &ClientCore) -> bool {
        let sequences = self
            .revisions
            .iter()
            .map(|revision| {
                (
                    revision.clone(),
                    client
                        .snapshot(&avatar_scope(revision))
                        .map(|p| p.snapshot().sequence().get()),
                )
            })
            .collect();
        if self.publication_sequences == sequences {
            return false;
        }
        self.publication_sequences = sequences;
        true
    }
    pub(crate) fn image_path(&self, client: &ClientCore, revision: &str) -> Option<PathBuf> {
        if !self.revisions.contains(revision) {
            return None;
        }
        // Read the current authenticated publication, never a task's returned
        // path. Client fences publication by session, authorization, generation,
        // epoch and cancellation; an old completion cannot restore an image.
        let publication = client
            .snapshot(&avatar_scope(revision))?
            .snapshot()
            .payload::<AvatarPublication>()?;
        if publication.avatar_revision() != revision
            || publication.principal_id() != format!("agent:{revision}")
        {
            return None;
        }
        publication
            .local_path()
            .map(|path| path.as_path().to_owned())
    }
}
impl ProviderCatalogView {
    pub(crate) fn reconcile_cli_avatars(&mut self, cx: &mut Context<Self>) {
        let visible = self.visible()
            && pioneer_client::providers::selectors::provider_filter_shows_cli_providers(
                self.navigation_input.providers_route(),
            )
            && self.gateway.capabilities.can_manage_capabilities;
        let identity = self
            .client
            .snapshot(&ClientScope::Administration { workspace_id: None })
            .and_then(|p| p.snapshot().payload::<IdentityAuthorizationPublication>());
        let scope = self
            .active_workspace_id()
            .filter(|_| visible)
            .map(|workspace| AvatarScope {
                workspace: workspace.to_owned(),
                endpoint: self.gateway.endpoint_id.clone(),
                connection: self.gateway.ws_connection_id,
                connected: self.gateway.connection_state
                    == crate::providers::GatewayConnectionState::Connected,
                authorization: self.client.authorization_connection_generation(),
                policy: identity
                    .as_ref()
                    .map_or(0, |identity| identity.authorization_change_sequence),
            });
        let dark = cx.theme().mode.is_dark();
        let revisions = crate::view::cli_runtime_displayed_runtimes(
            self.gateway
                .settings
                .as_ref()
                .map(|s| s.cli_runtimes.instances.as_slice()),
            &self.providers.cli_runtimes(),
        )
        .iter()
        .map(|runtime| cli_runtime_avatar_revision(runtime.kind, dark).to_owned())
        .collect();
        self.cli_avatars.reconcile(scope, revisions);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::prelude::v1::test;
    use pioneer_client::providers::types::CLIAgentRuntimeKind;
    use std::cell::RefCell;

    #[derive(Default)]
    struct Requests(RefCell<Vec<(String, CancellationToken)>>);
    impl ProviderAvatarPort for Requests {
        fn resolve_agent(
            &self,
            revision: String,
            cancellation: CancellationToken,
            _: &mut App,
        ) -> Task<Result<AgentAvatarCacheResult, AvatarCacheError>> {
            self.0.borrow_mut().push((revision, cancellation));
            Task::ready(Err(AvatarCacheError::Offline))
        }
    }
    fn scope(workspace: &str) -> Option<AvatarScope> {
        Some(AvatarScope {
            workspace: workspace.into(),
            endpoint: Some("gateway".into()),
            connection: Some(1),
            connected: true,
            authorization: 1,
            policy: 1,
        })
    }
    fn revisions(dark: bool) -> BTreeSet<String> {
        [
            CLIAgentRuntimeKind::Codex,
            CLIAgentRuntimeKind::Claude,
            CLIAgentRuntimeKind::OpenCode,
        ]
        .map(|kind| cli_runtime_avatar_revision(kind, dark).to_owned())
        .into_iter()
        .collect()
    }
    #[gpui_kit::test]
    fn theme_and_scope_changes_retire_only_obsolete_avatar_demand(cx: &mut TestAppContext) {
        let port = Arc::new(Requests::default());
        let mut avatars = CliAvatars::new(port.clone());
        let light_opencode = cli_runtime_avatar_revision(CLIAgentRuntimeKind::OpenCode, false);
        let dark_opencode = cli_runtime_avatar_revision(CLIAgentRuntimeKind::OpenCode, true);
        let client = ClientCore::new();

        avatars.reconcile(scope("one"), revisions(false));
        cx.update(|cx| avatars.start_requests(cx));
        avatars.reconcile(scope("one"), revisions(false));
        cx.update(|cx| avatars.start_requests(cx));
        assert_eq!(
            port.0.borrow().len(),
            3,
            "publications must not restart fetches"
        );
        let light_token = port
            .0
            .borrow()
            .iter()
            .find(|(revision, _)| revision == light_opencode)
            .unwrap()
            .1
            .clone();

        avatars.reconcile(scope("one"), revisions(true));
        cx.update(|cx| avatars.start_requests(cx));
        assert!(light_token.is_cancelled());
        assert!(!avatars.scopes().contains(&avatar_scope(light_opencode)));
        assert!(avatars.scopes().contains(&avatar_scope(dark_opencode)));
        assert!(avatars.image_path(&client, light_opencode).is_none());
        assert_eq!(
            port.0.borrow().len(),
            4,
            "theme-independent artwork keeps its demand"
        );
        assert!(
            port.0
                .borrow()
                .iter()
                .filter(|(revision, _)| revision != light_opencode)
                .all(|(_, token)| !token.is_cancelled())
        );

        avatars.reconcile(scope("two"), revisions(true));
        assert!(
            port.0
                .borrow()
                .iter()
                .all(|(_, token)| token.is_cancelled())
        );
        cx.update(|cx| avatars.start_requests(cx));
        avatars.reconcile(None, revisions(true));
        assert!(avatars.scopes().is_empty());
        assert!(
            port.0
                .borrow()
                .iter()
                .all(|(_, token)| token.is_cancelled())
        );

        avatars.reconcile(scope("two"), revisions(false));
        cx.update(|cx| avatars.start_requests(cx));
        drop(avatars);
        assert!(
            port.0
                .borrow()
                .iter()
                .all(|(_, token)| token.is_cancelled())
        );
    }
}
