//! Process-local semantic navigation. Shells retain publications and dispatch intents.
use crate::core::{
    ClientCore, ClientMutationAuthority, ClientScope, ClientTransition, ClientTransitionOutcome,
};
use crate::providers::selectors::ProviderFilter;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum AdministrationRoute {
    #[default]
    Members,
    Invitations,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum SettingsRoute {
    General,
    #[default]
    Account,
    Memory,
    SelfImprovement,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SemanticDestination {
    Threads,
    AgentsDocument,
    Providers {
        filter: ProviderFilter,
    },
    Administration {
        route: AdministrationRoute,
    },
    Mcp {
        server_id: Option<String>,
    },
    Skills {
        skill_id: Option<pioneer_protocol::SkillId>,
    },
    Settings {
        route: SettingsRoute,
    },
}
impl Default for SemanticDestination {
    fn default() -> Self {
        Self::Threads
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskThreadLineage {
    parent_thread_id: String,
    child_thread_id: String,
    workspace_id: String,
    title: String,
}
impl TaskThreadLineage {
    pub fn new(
        parent_thread_id: String,
        child_thread_id: String,
        workspace_id: String,
        title: String,
    ) -> Self {
        Self {
            parent_thread_id,
            child_thread_id,
            workspace_id,
            title,
        }
    }
    pub fn parent_thread_id(&self) -> &str {
        &self.parent_thread_id
    }
    pub fn child_thread_id(&self) -> &str {
        &self.child_thread_id
    }
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }
    pub fn title(&self) -> &str {
        &self.title
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClientNavigationState {
    pub(crate) active_thread_id: Option<String>,
    pub(crate) workspace_id: Option<String>,
    pub(crate) drafts: BTreeMap<String, String>,
    pub(crate) last_active: BTreeMap<String, String>,
    destination: SemanticDestination,
    #[serde(default)]
    agents_document_scope: Option<crate::agents_doc::scope::AgentsDocEditorScope>,
    lineage: Vec<TaskThreadLineage>,
    providers: Option<ProviderFilter>,
    administration: AdministrationRoute,
    settings: SettingsRoute,
    mcp_server_id: Option<String>,
    skill_id: Option<pioneer_protocol::SkillId>,
}
impl ClientNavigationState {
    pub fn active_thread_id(&self) -> Option<&str> {
        self.active_thread_id.as_deref()
    }
    pub fn workspace_id(&self) -> Option<&str> {
        self.workspace_id.as_deref()
    }
    pub fn destination(&self) -> &SemanticDestination {
        &self.destination
    }
    pub fn agents_document_scope(&self) -> Option<&crate::agents_doc::scope::AgentsDocEditorScope> {
        self.agents_document_scope.as_ref()
    }
    pub fn lineage(&self) -> &[TaskThreadLineage] {
        &self.lineage
    }
    pub fn draft(&self, workspace: &str) -> Option<&str> {
        self.drafts.get(workspace).map(String::as_str)
    }
    pub fn last_active(&self, workspace: &str) -> Option<&str> {
        self.last_active.get(workspace).map(String::as_str)
    }
    pub fn providers_route(&self) -> ProviderFilter {
        self.providers.unwrap_or(ProviderFilter::Api)
    }
    pub fn administration_route(&self) -> AdministrationRoute {
        self.administration
    }
    pub fn settings_route(&self) -> SettingsRoute {
        self.settings
    }
    pub fn mcp_server_id(&self) -> Option<&str> {
        self.mcp_server_id.as_deref()
    }
    pub fn skill_id(&self) -> Option<&pioneer_protocol::SkillId> {
        self.skill_id.as_ref()
    }

    pub(crate) fn remove_thread(&mut self, id: &str) {
        self.drafts.retain(|_, value| value != id);
        self.last_active.retain(|_, value| value != id);
        self.lineage
            .retain(|entry| entry.parent_thread_id != id && entry.child_thread_id != id);
        if self.active_thread_id.as_deref() == Some(id) {
            self.active_thread_id = None;
        }
    }
    pub(crate) fn remove_workspace(&mut self, id: &str) {
        self.drafts.remove(id);
        self.last_active.remove(id);
        self.lineage.retain(|entry| entry.workspace_id != id);
        if self.workspace_id.as_deref() == Some(id) {
            self.workspace_id = None;
            self.active_thread_id = None;
            self.destination = SemanticDestination::Threads;
        }
    }
    pub(crate) fn apply(&mut self, intent: NavigationIntent) -> ClientTransitionOutcome {
        let valid = |value: &str| !value.trim().is_empty() && value.trim() == value;
        match &intent {
            NavigationIntent::SelectWorkspace { workspace_id }
                if workspace_id.as_deref().is_some_and(|id| !valid(id)) =>
            {
                return ClientTransitionOutcome::Rejected;
            }
            NavigationIntent::SelectThread {
                workspace_id,
                thread_id,
            } if workspace_id.as_deref().is_some_and(|id| !valid(id))
                || thread_id.as_deref().is_some_and(|id| !valid(id)) =>
            {
                return ClientTransitionOutcome::Rejected;
            }
            NavigationIntent::RememberDraft {
                workspace_id,
                thread_id,
            }
            | NavigationIntent::RememberLast {
                workspace_id,
                thread_id,
            } if !valid(workspace_id) || thread_id.as_deref().is_some_and(|id| !valid(id)) => {
                return ClientTransitionOutcome::Rejected;
            }
            NavigationIntent::PushTaskThread { entry }
                if !valid(&entry.workspace_id)
                    || !valid(&entry.parent_thread_id)
                    || !valid(&entry.child_thread_id)
                    || entry.parent_thread_id == entry.child_thread_id =>
            {
                return ClientTransitionOutcome::Rejected;
            }
            NavigationIntent::Navigate {
                destination:
                    SemanticDestination::Mcp {
                        server_id: Some(id),
                    },
            }
            | NavigationIntent::SetMcpRoute {
                server_id: Some(id),
            } if !valid(id) => return ClientTransitionOutcome::Rejected,
            _ => {}
        }
        let before = self.clone();
        match intent {
            NavigationIntent::SetMcpRoute { server_id } => {
                self.mcp_server_id = server_id.clone();
                if matches!(self.destination, SemanticDestination::Mcp { .. }) {
                    self.destination = SemanticDestination::Mcp { server_id };
                }
            }
            NavigationIntent::SetSkillsRoute { skill_id } => {
                self.skill_id = skill_id.clone();
                if matches!(self.destination, SemanticDestination::Skills { .. }) {
                    self.destination = SemanticDestination::Skills { skill_id };
                }
            }
            NavigationIntent::SetAdministrationRoute { route } => {
                self.administration = route;
                if matches!(self.destination, SemanticDestination::Administration { .. }) {
                    self.destination = SemanticDestination::Administration { route };
                }
            }
            NavigationIntent::SetSettingsRoute { route } => {
                self.settings = route;
                if matches!(self.destination, SemanticDestination::Settings { .. }) {
                    self.destination = SemanticDestination::Settings { route };
                }
            }
            NavigationIntent::SetProvidersRoute { filter } => {
                self.providers = Some(filter);
                if matches!(self.destination, SemanticDestination::Providers { .. }) {
                    self.destination = SemanticDestination::Providers { filter };
                }
            }
            NavigationIntent::SelectWorkspace { workspace_id } => {
                if self.workspace_id != workspace_id {
                    self.workspace_id = workspace_id;
                    self.active_thread_id = None;
                    self.lineage.clear();
                }
            }
            NavigationIntent::SelectThread {
                workspace_id,
                thread_id,
            } => {
                if let Some(workspace_id) = workspace_id {
                    if self.workspace_id.as_deref() != Some(&workspace_id) {
                        self.lineage.clear();
                    }
                    if let Some(id) = &thread_id {
                        self.last_active.insert(workspace_id.clone(), id.clone());
                    }
                    self.workspace_id = Some(workspace_id);
                }
                if self.active_thread_id != thread_id {
                    if self.lineage.last().is_some_and(|entry| {
                        Some(entry.parent_thread_id.as_str()) == thread_id.as_deref()
                    }) {
                        self.lineage.pop();
                    } else {
                        self.lineage.clear();
                    }
                }
                self.active_thread_id = thread_id;
            }
            NavigationIntent::OpenAgentsDocument { scope } => {
                if self.workspace_id.as_deref() != Some(scope.workspace_id()) {
                    return ClientTransitionOutcome::Rejected;
                }
                self.destination = SemanticDestination::AgentsDocument;
                self.agents_document_scope = Some(scope);
            }
            NavigationIntent::Navigate { destination } => {
                match &destination {
                    SemanticDestination::Providers { filter } => self.providers = Some(*filter),
                    SemanticDestination::Administration { route } => self.administration = *route,
                    SemanticDestination::Settings { route } => self.settings = *route,
                    SemanticDestination::Mcp { server_id } => {
                        self.mcp_server_id = server_id.clone()
                    }
                    SemanticDestination::Skills { skill_id } => self.skill_id = skill_id.clone(),
                    _ => {}
                }
                self.destination = destination;
            }
            NavigationIntent::RememberDraft {
                workspace_id,
                thread_id,
            } => set_mapping(&mut self.drafts, workspace_id, thread_id),
            NavigationIntent::RememberLast {
                workspace_id,
                thread_id,
            } => set_mapping(&mut self.last_active, workspace_id, thread_id),
            NavigationIntent::PromoteThread { thread_id } => {
                self.drafts.retain(|_, id| id != &thread_id)
            }
            NavigationIntent::PushTaskThread { entry } => {
                if self.active_thread_id.as_deref() != Some(&entry.parent_thread_id)
                    || self.workspace_id.as_deref() != Some(&entry.workspace_id)
                {
                    return ClientTransitionOutcome::Stale;
                }
                self.lineage
                    .retain(|old| old.child_thread_id != entry.child_thread_id);
                self.active_thread_id = Some(entry.child_thread_id.clone());
                self.workspace_id = Some(entry.workspace_id.clone());
                self.last_active
                    .insert(entry.workspace_id.clone(), entry.child_thread_id.clone());
                self.lineage.push(entry);
                self.destination = SemanticDestination::Threads;
            }
            NavigationIntent::PopTaskThread => {
                if let Some(entry) = self.lineage.last() {
                    if self.active_thread_id.as_deref() != Some(&entry.child_thread_id) {
                        return ClientTransitionOutcome::Stale;
                    }
                    let entry = self.lineage.pop().unwrap();
                    self.active_thread_id = Some(entry.parent_thread_id.clone());
                    self.workspace_id = Some(entry.workspace_id.clone());
                    self.last_active
                        .insert(entry.workspace_id, entry.parent_thread_id);
                    self.destination = SemanticDestination::Threads;
                }
            }
            NavigationIntent::ClearLineage => self.lineage.clear(),
            NavigationIntent::Reset => *self = Self::default(),
        }
        if *self == before {
            ClientTransitionOutcome::Noop
        } else {
            ClientTransitionOutcome::Changed
        }
    }
}
fn set_mapping(map: &mut BTreeMap<String, String>, workspace: String, id: Option<String>) {
    if let Some(id) = id {
        map.insert(workspace, id);
    } else {
        map.remove(&workspace);
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NavigationIntent {
    OpenAgentsDocument {
        scope: crate::agents_doc::scope::AgentsDocEditorScope,
    },
    SetAdministrationRoute {
        route: AdministrationRoute,
    },
    SetSettingsRoute {
        route: SettingsRoute,
    },
    SetProvidersRoute {
        filter: ProviderFilter,
    },
    SetMcpRoute {
        server_id: Option<String>,
    },
    SetSkillsRoute {
        skill_id: Option<pioneer_protocol::SkillId>,
    },
    SelectWorkspace {
        workspace_id: Option<String>,
    },
    SelectThread {
        workspace_id: Option<String>,
        thread_id: Option<String>,
    },
    Navigate {
        destination: SemanticDestination,
    },
    RememberDraft {
        workspace_id: String,
        thread_id: Option<String>,
    },
    RememberLast {
        workspace_id: String,
        thread_id: Option<String>,
    },
    PromoteThread {
        thread_id: String,
    },
    PushTaskThread {
        entry: TaskThreadLineage,
    },
    PopTaskThread,
    ClearLineage,
    Reset,
}

impl ClientCore {
    pub fn navigation_snapshot(&self) -> Arc<ClientNavigationState> {
        let registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        registry
            .navigation_publication
            .clone()
            .unwrap_or_else(|| Arc::new(registry.navigation.clone()))
    }
    pub fn initialize_navigation(&self) {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if !self.is_stopped() {
            self.publish_navigation(&mut registry);
        }
    }
    /// Select and present a workspace thread as one navigation transition.
    pub fn open_workspace_thread(
        &self,
        workspace: String,
        thread: Option<String>,
        expected_revision: Option<u64>,
    ) -> ClientTransition {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if self.is_stopped() {
            return self.navigation_outcome(ClientTransitionOutcome::Rejected);
        }
        if expected_revision.is_some_and(|revision| revision != registry.navigation_revision) {
            return self.navigation_outcome(ClientTransitionOutcome::Stale);
        }
        registry.navigation.apply(NavigationIntent::ClearLineage);
        registry.navigation.apply(NavigationIntent::Navigate {
            destination: SemanticDestination::Threads,
        });
        registry.navigation.apply(NavigationIntent::SelectThread {
            workspace_id: Some(workspace),
            thread_id: thread,
        });
        self.publish_navigation(&mut registry)
    }
    pub fn navigate(
        &self,
        intent: NavigationIntent,
        expected_revision: Option<u64>,
    ) -> ClientTransition {
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let outcome = if self.is_stopped() {
            ClientTransitionOutcome::Rejected
        } else if expected_revision.is_some_and(|revision| revision != registry.navigation_revision)
        {
            ClientTransitionOutcome::Stale
        } else {
            registry.navigation.apply(intent)
        };
        if outcome == ClientTransitionOutcome::Changed {
            self.publish_navigation(&mut registry)
        } else {
            self.navigation_outcome(outcome)
        }
    }
    pub(crate) fn publish_navigation(
        &self,
        registry: &mut crate::threads::registry::ThreadRegistry,
    ) -> ClientTransition {
        match registry.navigation_change() {
            Some(publication) => self.transition_directory(
                registry,
                &ClientMutationAuthority { _private: () },
                vec![publication],
                vec![],
            ),
            None => self.navigation_outcome(ClientTransitionOutcome::Noop),
        }
    }
}
impl crate::threads::registry::ThreadRegistry {
    pub(crate) fn reset_navigation(&mut self) {
        self.navigation = ClientNavigationState::default();
    }
    pub(crate) fn navigation_change(&mut self) -> Option<crate::core::ClientPublicationDraft> {
        if self.navigation_publication.as_deref() == Some(&self.navigation) {
            return None;
        }
        // Draft membership changes the directory even when thread metadata is equal
        // (a thread notification may precede the creation response).
        let previous = self.navigation_publication.as_deref();
        for workspace in self.navigation.drafts.keys().chain(
            previous
                .into_iter()
                .flat_map(|navigation| navigation.drafts.keys()),
        ) {
            if previous.and_then(|navigation| navigation.draft(workspace))
                != self.navigation.draft(workspace)
            {
                self.directory.dirty_workspaces.insert(workspace.clone());
            }
        }
        self.navigation_revision += 1;
        self.session_revision += 1;
        let snapshot = Arc::new(self.navigation.clone());
        self.navigation_publication = Some(snapshot.clone());
        Some(ClientMutationAuthority { _private: () }.publication(
            ClientScope::Navigation,
            crate::threads::registry::revisions(self.navigation_revision),
            snapshot,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ClientIntent, ClientScope};
    fn select(core: &ClientCore, workspace: &str, thread: &str) -> ClientTransition {
        core.dispatch(ClientIntent::Navigation {
            intent: NavigationIntent::SelectThread {
                workspace_id: Some(workspace.into()),
                thread_id: Some(thread.into()),
            },
            expected_revision: None,
        })
    }
    #[test]
    fn opening_a_workspace_thread_is_one_transition_and_rejects_late_navigation() {
        let core = ClientCore::new();
        core.navigate(
            NavigationIntent::Navigate {
                destination: SemanticDestination::Settings {
                    route: SettingsRoute::Account,
                },
            },
            None,
        );
        let before = core
            .snapshot(&crate::core::ClientScope::Navigation)
            .unwrap()
            .revisions()
            .scoped()
            .get();
        core.open_workspace_thread("workspace".into(), Some("thread".into()), Some(before));
        let opened = core
            .snapshot(&crate::core::ClientScope::Navigation)
            .unwrap();
        assert_eq!(opened.revisions().scoped().get(), before + 1);
        assert_eq!(
            core.navigation_snapshot().destination(),
            &SemanticDestination::Threads
        );
        assert_eq!(
            core.open_workspace_thread("workspace".into(), Some("late".into()), Some(before))
                .outcome(),
            ClientTransitionOutcome::Stale
        );
        assert_eq!(
            core.navigation_snapshot().active_thread_id(),
            Some("thread")
        );
        assert_eq!(
            core.open_workspace_thread("workspace".into(), Some("thread".into()), None)
                .outcome(),
            ClientTransitionOutcome::Noop
        );
    }
    #[test]
    fn revoked_workspace_clears_selection_even_without_an_open_thread() {
        let core = ClientCore::new();
        core.navigate(
            NavigationIntent::SelectWorkspace {
                workspace_id: Some("workspace".into()),
            },
            None,
        );
        let plan = core.apply_thread_access_change(&pioneer_protocol::AccessChangedNotification {
            authorization_revision: 1,
            workspace_id: "workspace".into(),
            thread_id: None,
            outcome: pioneer_protocol::AccessChangeOutcome::Revoked,
            change: pioneer_protocol::AccessChangeKind::WorkspaceMembership,
        });
        assert!(plan.clear_active_workspace);
        assert_eq!(core.navigation_snapshot().workspace_id(), None);
        assert_eq!(core.navigation_snapshot().active_thread_id(), None);
    }
    #[test]
    fn authorization_fence_clears_typed_navigation_without_reusing_revision() {
        let core = ClientCore::shared();
        select(&core, "workspace", "protected");
        let revision = core
            .snapshot(&ClientScope::Navigation)
            .unwrap()
            .revisions()
            .scoped()
            .get();
        core.begin_authorization_epoch(None);
        assert_eq!(core.navigation_snapshot().active_thread_id(), None);
        assert_eq!(core.navigation_snapshot().workspace_id(), None);
        let publication = core.snapshot(&ClientScope::Navigation).unwrap();
        assert!(publication.typed::<ClientNavigationState>().is_some());
        assert!(publication.revisions().scoped().get() > revision);
        assert_eq!(
            core.navigate(NavigationIntent::Reset, Some(revision))
                .outcome(),
            ClientTransitionOutcome::Stale
        );
    }
    #[test]
    fn returning_through_platform_stack_pops_the_same_client_lineage() {
        let core = ClientCore::new();
        select(&core, "workspace", "parent");
        core.navigate(
            NavigationIntent::PushTaskThread {
                entry: TaskThreadLineage::new(
                    "parent".into(),
                    "child".into(),
                    "workspace".into(),
                    "Task".into(),
                ),
            },
            None,
        );
        select(&core, "workspace", "parent");
        assert!(core.navigation_snapshot().lineage().is_empty());
        assert_eq!(
            core.navigation_snapshot().active_thread_id(),
            Some("parent")
        );
    }

    #[test]
    fn selection_is_one_publication_and_noop_keeps_identity() {
        let core = ClientCore::new();
        assert_eq!(
            select(&core, "workspace", "thread").outcome(),
            ClientTransitionOutcome::Changed
        );
        let first = core.navigation_snapshot();
        assert_eq!(first.last_active("workspace"), Some("thread"));
        let revision = core.snapshot(&ClientScope::Navigation).unwrap().revisions();
        assert_eq!(
            select(&core, "workspace", "thread").outcome(),
            ClientTransitionOutcome::Noop
        );
        assert!(Arc::ptr_eq(&first, &core.navigation_snapshot()));
        assert_eq!(
            revision,
            core.snapshot(&ClientScope::Navigation).unwrap().revisions()
        );
        assert_eq!(
            core.navigate(NavigationIntent::Reset, Some(0)).outcome(),
            ClientTransitionOutcome::Stale
        );
        assert!(Arc::ptr_eq(&first, &core.navigation_snapshot()));
    }
    #[test]
    fn compatibility_selection_and_typed_intents_have_identical_outcomes() {
        let direct = ClientCore::new();
        let compatibility = ClientCore::new();
        select(&direct, "workspace", "thread");
        compatibility.activate_thread(Some("thread"), Some("workspace"));
        direct.navigate(
            NavigationIntent::RememberDraft {
                workspace_id: "workspace".into(),
                thread_id: Some("draft".into()),
            },
            None,
        );
        compatibility.remember_thread_draft("workspace", Some("draft".into()));
        assert_eq!(
            direct.navigation_snapshot(),
            compatibility.navigation_snapshot()
        );
        assert!(compatibility.promote_thread("draft"));
        direct.navigate(
            NavigationIntent::PromoteThread {
                thread_id: "draft".into(),
            },
            None,
        );
        assert_eq!(
            direct.navigation_snapshot(),
            compatibility.navigation_snapshot()
        );
        assert!(!compatibility.promote_thread("draft"));
    }
    #[test]
    fn lineage_back_and_stale_child_preserve_one_selection_owner() {
        let core = ClientCore::new();
        select(&core, "workspace", "parent");
        let previous = core.navigation_snapshot();
        assert_eq!(
            core.navigate(
                NavigationIntent::PushTaskThread {
                    entry: TaskThreadLineage::new(
                        "parent".into(),
                        "child".into(),
                        "other-workspace".into(),
                        "Task".into()
                    )
                },
                None
            )
            .outcome(),
            ClientTransitionOutcome::Stale
        );
        assert!(Arc::ptr_eq(&previous, &core.navigation_snapshot()));
        let enter = NavigationIntent::PushTaskThread {
            entry: TaskThreadLineage::new(
                "parent".into(),
                "child".into(),
                "workspace".into(),
                "Task".into(),
            ),
        };
        assert_eq!(
            core.navigate(enter.clone(), None).outcome(),
            ClientTransitionOutcome::Changed
        );
        assert_eq!(
            core.navigate(enter, None).outcome(),
            ClientTransitionOutcome::Stale
        );
        assert_eq!(core.navigation_snapshot().active_thread_id(), Some("child"));
        assert_eq!(
            core.navigate(NavigationIntent::PopTaskThread, None)
                .outcome(),
            ClientTransitionOutcome::Changed
        );
        assert_eq!(
            core.navigation_snapshot().active_thread_id(),
            Some("parent")
        );
        assert_eq!(
            core.navigate(NavigationIntent::PopTaskThread, None)
                .outcome(),
            ClientTransitionOutcome::Noop
        );
    }
    #[test]
    fn semantic_destinations_keep_selection_and_publish_once() {
        let core = ClientCore::new();
        select(&core, "workspace", "thread");
        for destination in [
            SemanticDestination::Providers {
                filter: ProviderFilter::Connected,
            },
            SemanticDestination::Administration {
                route: AdministrationRoute::Invitations,
            },
            SemanticDestination::Mcp {
                server_id: Some("server".into()),
            },
            SemanticDestination::Skills { skill_id: None },
            SemanticDestination::Settings {
                route: SettingsRoute::Memory,
            },
        ] {
            let intent = NavigationIntent::Navigate {
                destination: destination.clone(),
            };
            assert_eq!(
                core.navigate(intent.clone(), None).outcome(),
                ClientTransitionOutcome::Changed
            );
            let first = core.navigation_snapshot();
            assert_eq!(first.destination(), &destination);
            assert_eq!(first.active_thread_id(), Some("thread"));
            assert_eq!(
                core.navigate(intent, None).outcome(),
                ClientTransitionOutcome::Noop
            );
            assert!(Arc::ptr_eq(&first, &core.navigation_snapshot()));
        }
    }
    #[test]
    fn deletion_and_logout_drop_lineage_and_keep_revision_namespace() {
        let core = ClientCore::new();
        select(&core, "workspace", "parent");
        core.navigate(
            NavigationIntent::PushTaskThread {
                entry: TaskThreadLineage::new(
                    "parent".into(),
                    "child".into(),
                    "workspace".into(),
                    "Task".into(),
                ),
            },
            None,
        );
        core.remove_thread_store("child");
        assert!(core.navigation_snapshot().active_thread_id().is_none());
        assert!(core.navigation_snapshot().lineage().is_empty());
        let revision = core
            .snapshot(&ClientScope::Navigation)
            .unwrap()
            .revisions()
            .scoped();
        core.clear_thread_stores();
        assert_eq!(
            *core.navigation_snapshot(),
            ClientNavigationState::default()
        );
        assert!(
            core.snapshot(&ClientScope::Navigation)
                .unwrap()
                .revisions()
                .scoped()
                > revision
        );
        core.shutdown();
        assert_eq!(
            select(&core, "workspace", "thread").outcome(),
            ClientTransitionOutcome::Rejected
        );
    }
    #[test]
    fn malformed_selection_is_rejected_without_publication() {
        let core = ClientCore::new();
        assert_eq!(
            select(&core, " ", "thread").outcome(),
            ClientTransitionOutcome::Rejected
        );
        assert!(core.snapshot(&ClientScope::Navigation).is_none());
        assert_eq!(
            core.navigate(
                NavigationIntent::SetMcpRoute {
                    server_id: Some(" ".into())
                },
                None
            )
            .outcome(),
            ClientTransitionOutcome::Rejected
        );
        assert!(core.snapshot(&ClientScope::Navigation).is_none());
    }
}
