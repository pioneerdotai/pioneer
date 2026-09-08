//! Draft-scoped capability catalogs and explicit picker selection sessions.
use super::{
    capabilities::{self, SelectableMcpCapability},
    skill_selection::{self, ComposerSkillSelection},
    state_machine::ComposerDomainAction,
    store::{ComposerIntent, ComposerOperationIdentity, ComposerStore, DraftId},
};
use crate::{
    core::{ClientCore, ClientDemand, ClientMutationAuthority, ClientScope, ClientTransition},
    skills::catalog::SkillManagementProjection,
};
use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, mpsc},
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ComposerCatalogKind {
    Skills,
    McpServers,
    McpTools { server_id: String },
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ComposerCatalogRequestState {
    #[default]
    Idle,
    Loading,
    Ready,
    Failed {
        message: String,
    },
    Cancelled,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ComposerCatalogRequest {
    pub generation: u64,
    pub state: ComposerCatalogRequestState,
}
impl ComposerCatalogRequest {
    pub fn transport_unavailable(&self) -> bool {
        matches!(&self.state, ComposerCatalogRequestState::Failed { message } if message.contains(crate::rpc::WEBSOCKET_WORKER_UNAVAILABLE_MESSAGE))
    }
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComposerPickerKind {
    Skills,
    Mcp,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ComposerPickerSelection {
    Immediate,
    Skills {
        selections: Vec<ComposerSkillSelection>,
    },
    Mcp {
        selected: std::collections::BTreeSet<String>,
    },
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ComposerPickerSession {
    pub identity: ComposerOperationIdentity,
    pub kind: ComposerPickerKind,
    pub selection: ComposerPickerSelection,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ComposerCatalogPublication {
    pub thread_id: String,
    pub draft_id: DraftId,
    pub revision: u64,
    pub skills: SkillManagementProjection,
    pub skill_request: ComposerCatalogRequest,
    pub mcp_servers: Vec<SelectableMcpCapability>,
    pub mcp_tools: Vec<SelectableMcpCapability>,
    pub mcp_request: ComposerCatalogRequest,
    pub tool_requests: BTreeMap<String, ComposerCatalogRequest>,
    pub session: Option<ComposerPickerSession>,
}
impl ComposerCatalogPublication {
    fn new(thread: &str, draft_id: DraftId) -> Self {
        Self {
            thread_id: thread.into(),
            draft_id,
            revision: 0,
            skills: Default::default(),
            skill_request: Default::default(),
            mcp_servers: vec![],
            mcp_tools: vec![],
            mcp_request: Default::default(),
            tool_requests: Default::default(),
            session: None,
        }
    }
    fn request_mut(&mut self, kind: &ComposerCatalogKind) -> &mut ComposerCatalogRequest {
        match kind {
            ComposerCatalogKind::Skills => &mut self.skill_request,
            ComposerCatalogKind::McpServers => &mut self.mcp_request,
            ComposerCatalogKind::McpTools { server_id } => {
                self.tool_requests.entry(server_id.clone()).or_default()
            }
        }
    }
    fn request(&self, kind: &ComposerCatalogKind) -> Option<&ComposerCatalogRequest> {
        match kind {
            ComposerCatalogKind::Skills => Some(&self.skill_request),
            ComposerCatalogKind::McpServers => Some(&self.mcp_request),
            ComposerCatalogKind::McpTools { server_id } => self.tool_requests.get(server_id),
        }
    }
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ComposerCatalogIntent {
    RemoveSkillChip {
        thread_id: String,
        draft_id: DraftId,
        key: String,
    },
    Observe {
        thread_id: String,
        draft_id: DraftId,
        catalog: ComposerCatalogKind,
    },
    Retry {
        thread_id: String,
        draft_id: DraftId,
        catalog: ComposerCatalogKind,
    },
    OpenPicker {
        thread_id: String,
        draft_id: DraftId,
        picker: ComposerPickerKind,
        deferred: bool,
    },
    ToggleSkill {
        identity: ComposerOperationIdentity,
        selection: ComposerSkillSelection,
    },
    ToggleMcp {
        identity: ComposerOperationIdentity,
        key: String,
    },
    CommitPicker {
        identity: ComposerOperationIdentity,
    },
    ClosePicker {
        identity: ComposerOperationIdentity,
    },
}
#[derive(Clone)]
struct CatalogWork {
    identity: ComposerOperationIdentity,
    workspace: String,
    kind: ComposerCatalogKind,
    auth: (u64, Option<u64>),
}
enum CatalogResult {
    Skills(SkillManagementProjection),
    Servers(Vec<SelectableMcpCapability>, Vec<String>),
    Tools(Vec<SelectableMcpCapability>),
}
#[derive(Default)]
pub(crate) struct ComposerCatalogController {
    sender: Option<mpsc::SyncSender<CatalogWork>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl ComposerCatalogController {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
    }
}
impl Drop for ComposerCatalogController {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}
impl ClientCore {
    pub fn composer_catalog_skill_picker(
        &self,
        thread: &str,
        draft: DraftId,
        query: &str,
    ) -> skill_selection::ComposerSkillPickerProjection {
        let store = self.composer_store.lock().expect("composer store poisoned");
        if store
            .drafts
            .get(thread)
            .is_none_or(|input| input.draft_id() != draft)
        {
            return Default::default();
        }
        store
            .catalogs
            .get(thread)
            .filter(|p| p.draft_id == draft)
            .map(|p| skill_selection::project_composer_skill_picker(&p.skills, query))
            .unwrap_or_default()
    }
    pub fn composer_catalog_snapshot(
        &self,
        thread: &str,
    ) -> Option<Arc<ComposerCatalogPublication>> {
        self.composer_store
            .lock()
            .expect("composer store poisoned")
            .catalogs
            .get(thread)
            .cloned()
    }
    fn catalog_transition(&self) -> ClientTransition {
        self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![])
    }
    fn publish_composer_catalog(
        &self,
        store: &mut ComposerStore,
        mut next: ComposerCatalogPublication,
    ) -> ClientTransition {
        let scope = ClientScope::ComposerCatalog {
            thread_id: next.thread_id.clone(),
        };
        next.revision = store
            .catalogs
            .get(&next.thread_id)
            .map_or_else(
                || {
                    self.snapshot(&scope)
                        .map_or(0, |p| p.revisions().scoped().get())
                },
                |p| p.revision,
            )
            .checked_add(1)
            .expect("composer catalog revision exhausted");
        let revision = next.revision;
        let next = Arc::new(next);
        store.catalogs.insert(next.thread_id.clone(), next.clone());
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(revision),
            next,
            vec![],
        )
    }
    fn catalog_allowed(&self, thread: &str, workspace: &str, kind: &ComposerCatalogKind) -> bool {
        self.thread_capability_snapshot(thread)
            .filter(|p| {
                p.workspace_id == workspace
                    && p.request
                        == crate::threads::capabilities::ThreadCapabilityRequestState::Ready
            })
            .and_then(|p| p.snapshot.clone())
            .or_else(|| self.authorization_snapshot(Some(workspace), None))
            .is_some_and(|p| {
                let capabilities = crate::authorization::principal_presentation_capabilities(&p);
                match kind {
                    ComposerCatalogKind::Skills => capabilities.can_use_skills,
                    _ => capabilities.can_use_mcp,
                }
            })
    }
    fn request_composer_catalog(
        &self,
        thread: &str,
        draft: DraftId,
        kind: ComposerCatalogKind,
        retry: bool,
    ) -> ClientTransition {
        let Some(workspace) = self
            .thread_coordinator_snapshot(thread)
            .map(|p| p.workspace_id.clone())
        else {
            return self.reject_intent();
        };
        if self.is_stopped() || !self.catalog_allowed(thread, &workspace, &kind) {
            return self.reject_intent();
        }
        let auth = self.current_auth_ticket();
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        if store.catalog_suspended.contains(thread)
            || store.suspended.contains(thread)
            || store
                .drafts
                .get(thread)
                .is_none_or(|p| p.draft_id() != draft)
        {
            return self.catalog_transition();
        }
        let mut next = store
            .catalogs
            .get(thread)
            .filter(|p| p.draft_id == draft)
            .map(|p| (**p).clone())
            .unwrap_or_else(|| ComposerCatalogPublication::new(thread, draft));
        if let ComposerCatalogKind::McpTools { server_id } = &kind {
            if !next
                .mcp_servers
                .iter()
                .any(|row| &row.server_id == server_id)
            {
                return self.reject_intent();
            }
        }
        let state = &next.request_mut(&kind).state;
        if *state == ComposerCatalogRequestState::Loading
            || (!retry
                && !matches!(
                    state,
                    ComposerCatalogRequestState::Idle | ComposerCatalogRequestState::Cancelled
                ))
        {
            return self.catalog_transition();
        }
        store.next_operation = store
            .next_operation
            .checked_add(1)
            .expect("composer catalog generation exhausted");
        let generation = store.next_operation;
        *next.request_mut(&kind) = ComposerCatalogRequest {
            generation,
            state: ComposerCatalogRequestState::Loading,
        };
        let work = CatalogWork {
            identity: ComposerOperationIdentity {
                thread_id: thread.into(),
                draft_id: draft,
                generation,
            },
            workspace,
            kind,
            auth,
        };
        let transition = self.publish_composer_catalog(&mut store, next);
        drop(store);
        let queued = self
            .composer_catalog_requests
            .lock()
            .expect("composer catalogs poisoned")
            .sender
            .as_ref()
            .is_some_and(|sender| sender.try_send(work.clone()).is_ok());
        if !queued {
            self.complete_composer_catalog(work, Err("Catalog request queue unavailable".into()));
        }
        transition
    }
    pub fn composer_catalog_intent(&self, intent: ComposerCatalogIntent) -> ClientTransition {
        match intent {
            ComposerCatalogIntent::RemoveSkillChip {
                thread_id,
                draft_id,
                key,
            } => {
                let Some(draft) = self
                    .composer_snapshot(&thread_id)
                    .filter(|p| p.draft_id() == draft_id)
                else {
                    return self.catalog_transition();
                };
                let picker = self.composer_catalog_skill_picker(&thread_id, draft_id, "");
                let Some(chip) = skill_selection::project_composer_skill_chips(
                    &draft.domain().skill_selections,
                    &picker,
                )
                .into_iter()
                .find(|chip| chip.key == key) else {
                    return self.catalog_transition();
                };
                let selection = match chip.kind {
                    skill_selection::ComposerSkillChipKind::SkillPack => chip
                        .pack_id
                        .map(|pack_id| ComposerSkillSelection::SkillPack { pack_id }),
                    _ => chip.skill_id.map(|skill_id| ComposerSkillSelection::Skill {
                        skill_id,
                        pack_id: chip.pack_id,
                    }),
                };
                let Some(selection) = selection else {
                    return self.catalog_transition();
                };
                self.composer_intent(ComposerIntent::Domain {
                    thread_id,
                    draft_id,
                    action: ComposerDomainAction::RemoveSkillSelection { selection },
                })
            }
            ComposerCatalogIntent::Observe {
                thread_id,
                draft_id,
                catalog,
            } => self.request_composer_catalog(&thread_id, draft_id, catalog, false),
            ComposerCatalogIntent::Retry {
                thread_id,
                draft_id,
                catalog,
            } => self.request_composer_catalog(&thread_id, draft_id, catalog, true),
            ComposerCatalogIntent::OpenPicker {
                thread_id,
                draft_id,
                picker,
                deferred,
            } => {
                let catalog = match picker {
                    ComposerPickerKind::Skills => ComposerCatalogKind::Skills,
                    ComposerPickerKind::Mcp => ComposerCatalogKind::McpServers,
                };
                let Some(workspace) = self
                    .thread_coordinator_snapshot(&thread_id)
                    .map(|p| p.workspace_id.clone())
                else {
                    return self.reject_intent();
                };
                if self.is_stopped() || !self.catalog_allowed(&thread_id, &workspace, &catalog) {
                    return self.reject_intent();
                }
                let mut store = self.composer_store.lock().expect("composer store poisoned");
                let Some(draft) = store
                    .drafts
                    .get(&thread_id)
                    .filter(|p| p.draft_id() == draft_id)
                    .cloned()
                else {
                    return self.catalog_transition();
                };
                if store.catalog_suspended.contains(&thread_id)
                    || store.suspended.contains(&thread_id)
                {
                    return self.catalog_transition();
                }
                let mut next = store
                    .catalogs
                    .get(&thread_id)
                    .filter(|p| p.draft_id == draft_id)
                    .map(|p| (**p).clone())
                    .unwrap_or_else(|| ComposerCatalogPublication::new(&thread_id, draft_id));
                if next.session.is_some() {
                    return self.catalog_transition();
                }
                store.next_operation = store
                    .next_operation
                    .checked_add(1)
                    .expect("composer picker generation exhausted");
                next.session = Some(ComposerPickerSession {
                    identity: ComposerOperationIdentity {
                        thread_id: thread_id.clone(),
                        draft_id,
                        generation: store.next_operation,
                    },
                    kind: picker,
                    selection: if !deferred {
                        ComposerPickerSelection::Immediate
                    } else {
                        match picker {
                            ComposerPickerKind::Skills => ComposerPickerSelection::Skills {
                                selections: draft.domain().skill_selections.clone(),
                            },
                            ComposerPickerKind::Mcp => ComposerPickerSelection::Mcp {
                                selected: draft
                                    .domain()
                                    .capabilities
                                    .iter()
                                    .map(|c| c.id.clone())
                                    .collect(),
                            },
                        }
                    },
                });
                let result = self.publish_composer_catalog(&mut store, next);
                drop(store);
                self.request_composer_catalog(&thread_id, draft_id, catalog, true);
                result
            }
            intent => self.composer_picker_action(intent),
        }
    }
    fn composer_picker_action(&self, intent: ComposerCatalogIntent) -> ClientTransition {
        let identity = match &intent {
            ComposerCatalogIntent::ToggleSkill { identity, .. }
            | ComposerCatalogIntent::ToggleMcp { identity, .. }
            | ComposerCatalogIntent::CommitPicker { identity }
            | ComposerCatalogIntent::ClosePicker { identity } => identity.clone(),
            _ => unreachable!(),
        };
        if self.is_stopped() {
            return self.reject_intent();
        }
        if !matches!(intent, ComposerCatalogIntent::ClosePicker { .. }) {
            let catalog = self
                .composer_catalog_snapshot(&identity.thread_id)
                .and_then(|p| {
                    p.session.as_ref().map(|s| match s.kind {
                        ComposerPickerKind::Skills => ComposerCatalogKind::Skills,
                        ComposerPickerKind::Mcp => ComposerCatalogKind::McpServers,
                    })
                });
            let workspace = self
                .thread_coordinator_snapshot(&identity.thread_id)
                .map(|p| p.workspace_id.clone());
            if let (Some(catalog), Some(workspace)) = (catalog, workspace) {
                if !self.catalog_allowed(&identity.thread_id, &workspace, &catalog) {
                    return self.reject_intent();
                }
            } else {
                return self.catalog_transition();
            }
        }
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        if store.catalog_suspended.contains(&identity.thread_id)
            || store.suspended.contains(&identity.thread_id)
        {
            return self.catalog_transition();
        }
        let Some(draft) = store
            .drafts
            .get(&identity.thread_id)
            .filter(|p| p.draft_id() == identity.draft_id)
            .cloned()
        else {
            return self.catalog_transition();
        };
        let Some(current) = store
            .catalogs
            .get(&identity.thread_id)
            .filter(|p| p.session.as_ref().is_some_and(|s| s.identity == identity))
            .cloned()
        else {
            return self.catalog_transition();
        };
        let mut next = (*current).clone();
        let session = next.session.as_mut().expect("matched session");
        let mut action = None;
        match intent {
            ComposerCatalogIntent::ToggleSkill { selection, .. } => {
                if session.kind != ComposerPickerKind::Skills {
                    return self.reject_intent();
                }
                let picker = skill_selection::project_composer_skill_picker(&next.skills, "");
                let selected = match &session.selection {
                    ComposerPickerSelection::Skills { selections } => selections,
                    ComposerPickerSelection::Immediate => &draft.domain().skill_selections,
                    _ => return self.reject_intent(),
                };
                let reduction = skill_selection::reduce_composer_skill_selection_toggle(
                    selected,
                    &picker,
                    selection.clone(),
                );
                if !reduction.changed {
                    return self.catalog_transition();
                }
                if let ComposerPickerSelection::Skills { selections } = &mut session.selection {
                    *selections = reduction.selections;
                } else {
                    action = Some(ComposerDomainAction::ToggleSkillSelection { picker, selection });
                }
            }
            ComposerCatalogIntent::ToggleMcp { key, .. } => {
                if session.kind != ComposerPickerKind::Mcp {
                    return self.reject_intent();
                }
                let Some(row) = next
                    .mcp_servers
                    .iter()
                    .chain(&next.mcp_tools)
                    .find(|row| row.key == key && row.selectable)
                else {
                    return self.reject_intent();
                };
                let mut selected: HashSet<String> = match &session.selection {
                    ComposerPickerSelection::Mcp { selected } => selected.iter().cloned().collect(),
                    ComposerPickerSelection::Immediate => draft
                        .domain()
                        .capabilities
                        .iter()
                        .map(|c| c.id.clone())
                        .collect(),
                    _ => return self.reject_intent(),
                };
                capabilities::toggle_mcp_capability_selection(
                    &mut selected,
                    &next.mcp_servers,
                    &next.mcp_tools,
                    row,
                );
                if let ComposerPickerSelection::Mcp { selected: output } = &mut session.selection {
                    *output = selected.into_iter().collect();
                } else {
                    action = Some(ComposerDomainAction::ToggleMcpSelection {
                        server_rows: next.mcp_servers.clone(),
                        tool_rows: next.mcp_tools.clone(),
                        key,
                    });
                }
            }
            ComposerCatalogIntent::CommitPicker { .. } => {
                action = match &session.selection {
                    ComposerPickerSelection::Skills { selections } => {
                        Some(ComposerDomainAction::SetSkillSelections {
                            selections: selections.clone(),
                        })
                    }
                    ComposerPickerSelection::Mcp { selected } => {
                        // The deferred MCP picker confirms additions; immediate toggles replace selection.
                        Some(ComposerDomainAction::AddCapabilities {
                            capabilities:
                                capabilities::selected_mcp_composer_capabilities_from_rows(
                                    &next.mcp_servers,
                                    &next.mcp_tools,
                                    &selected.iter().cloned().collect(),
                                ),
                        })
                    }
                    ComposerPickerSelection::Immediate => None,
                };
                next.session = None;
            }
            ComposerCatalogIntent::ClosePicker { .. } => {
                next.session = None;
            }
            _ => unreachable!(),
        }
        let changed = next != *current;
        let result = if changed {
            self.publish_composer_catalog(&mut store, next)
        } else {
            self.catalog_transition()
        };
        drop(store);
        if let Some(action) = action {
            return self.composer_intent(ComposerIntent::Domain {
                thread_id: identity.thread_id,
                draft_id: identity.draft_id,
                action,
            });
        }
        result
    }
    fn catalog_work_current(&self, work: &CatalogWork) -> bool {
        !self.is_stopped()
            && self.current_auth_ticket() == work.auth
            && self
                .composer_snapshot(&work.identity.thread_id)
                .is_some_and(|p| p.draft_id() == work.identity.draft_id)
            && self
                .composer_catalog_snapshot(&work.identity.thread_id)
                .is_some_and(|p| {
                    p.draft_id == work.identity.draft_id
                        && p.request(&work.kind).is_some_and(|r| {
                            r.generation == work.identity.generation
                                && r.state == ComposerCatalogRequestState::Loading
                        })
                })
    }
    fn complete_composer_catalog(&self, work: CatalogWork, result: Result<CatalogResult, String>) {
        if !self.catalog_work_current(&work) {
            return;
        }
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        if store
            .drafts
            .get(&work.identity.thread_id)
            .is_none_or(|p| p.draft_id() != work.identity.draft_id)
        {
            return;
        }
        let Some(current) = store
            .catalogs
            .get(&work.identity.thread_id)
            .filter(|p| {
                p.draft_id == work.identity.draft_id
                    && p.request(&work.kind).is_some_and(|r| {
                        r.generation == work.identity.generation
                            && r.state == ComposerCatalogRequestState::Loading
                    })
            })
            .cloned()
        else {
            return;
        };
        let mut next = (*current).clone();
        let mut prefetch = vec![];
        match result {
            Ok(result) => {
                next.request_mut(&work.kind).state = ComposerCatalogRequestState::Ready;
                match result {
                    CatalogResult::Skills(management) => next.skills = management,
                    CatalogResult::Servers(rows, ids) => {
                        next.mcp_servers = rows;
                        next.mcp_tools.clear();
                        next.tool_requests.clear();
                        prefetch = ids;
                    }
                    CatalogResult::Tools(rows) => {
                        if let ComposerCatalogKind::McpTools { server_id } = &work.kind {
                            next.mcp_tools.retain(|row| &row.server_id != server_id);
                            next.mcp_tools.extend(rows);
                        }
                    }
                }
            }
            Err(message) => {
                next.request_mut(&work.kind).state = ComposerCatalogRequestState::Failed { message }
            }
        }
        self.publish_composer_catalog(&mut store, next);
        drop(store);
        for server_id in prefetch {
            self.request_composer_catalog(
                &work.identity.thread_id,
                work.identity.draft_id,
                ComposerCatalogKind::McpTools { server_id },
                false,
            );
        }
    }
    pub(crate) fn cancel_composer_catalog(&self, thread: &str) {
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        if let Some(current) = store.catalogs.get(thread).cloned() {
            let mut next = ComposerCatalogPublication::new(thread, current.draft_id);
            next.skill_request.state = ComposerCatalogRequestState::Cancelled;
            next.mcp_request.state = ComposerCatalogRequestState::Cancelled;
            self.publish_composer_catalog(&mut store, next);
            store.catalogs.remove(thread);
        }
    }
    pub(crate) fn composer_catalog_subscription_changed(&self, scope: &ClientScope, added: bool) {
        let ClientScope::ComposerCatalog { thread_id } = scope else {
            return;
        };
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        let count = store
            .catalog_subscriptions
            .entry(thread_id.clone())
            .or_default();
        if added {
            *count += 1;
            store.catalog_suspended.remove(thread_id);
        } else {
            *count = count.saturating_sub(1);
            if *count == 0 {
                store.catalog_subscriptions.remove(thread_id);
                drop(store);
                self.cancel_composer_catalog(thread_id);
            }
        }
    }
    pub(crate) fn composer_catalog_demand_changed(
        &self,
        scope: &ClientScope,
        demand: ClientDemand,
    ) {
        let ClientScope::ComposerCatalog { thread_id } = scope else {
            return;
        };
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        if demand != ClientDemand::Suspended {
            store.catalog_suspended.remove(thread_id);
            return;
        }
        store.catalog_suspended.insert(thread_id.clone());
        drop(store);
        self.cancel_composer_catalog(thread_id);
    }
    pub(crate) fn start_composer_catalog_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<CatalogWork>(64);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-composer-catalog".into())
            .spawn(move || {
                while let Ok(work) = receiver.recv() {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if !core.catalog_work_current(&work) {
                        continue;
                    }
                    let sender = core.compatibility_runtime().ws_command_sender();
                    drop(core);
                    let result = match &work.kind {
                        ComposerCatalogKind::Skills => sender
                            .skills_list(crate::skills::catalog::skill_list_params(
                                work.workspace.clone(),
                            ))
                            .map(|response| {
                                let split =
                                    crate::skills::catalog::derive_skills_catalog_and_installed(
                                        response.skills,
                                    );
                                CatalogResult::Skills(
                                    crate::skills::catalog::project_skill_management(
                                        &split.installed,
                                        response.packs,
                                    ),
                                )
                            }),
                        ComposerCatalogKind::McpServers => sender
                            .mcp_list(crate::mcp::list::mcp_list_params(work.workspace.clone()))
                            .map(|response| {
                                let rows =
                                    capabilities::reduce_composer_mcp_server_picker_rows_response(
                                        response, "",
                                    );
                                CatalogResult::Servers(rows.rows, rows.prefetch_server_ids)
                            }),
                        ComposerCatalogKind::McpTools { server_id } => sender
                            .mcp_server_details(crate::mcp::details::mcp_server_details_params(
                                work.workspace.clone(),
                                server_id.clone(),
                            ))
                            .map(|response| {
                                CatalogResult::Tools(
                                    capabilities::reduce_composer_mcp_tool_picker_rows_response(
                                        response, "",
                                    )
                                    .rows,
                                )
                            }),
                    }
                    .map_err(|error| format!("{error:#}"));
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    core.complete_composer_catalog(work, result);
                }
            })
            .expect("composer catalog worker could not start");
        let mut owner = self
            .composer_catalog_requests
            .lock()
            .expect("composer catalogs poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ClientTransitionOutcome;
    use pioneer_protocol::*;

    fn fixture() -> (Arc<ClientCore>, mpsc::Receiver<CatalogWork>) {
        let core = Arc::new(ClientCore::new());
        core.upsert_thread(serde_json::from_value(serde_json::json!({
            "workspace_id":"ws", "id":"a", "preview":"", "mode":"Agent", "model":"model", "model_provider":"provider", "created_at":1,"updated_at":1,"status":"Idle","origin_kind":"user","sidebar_visibility":"visible","turns":[]
        })).unwrap());
        ClientMutationAuthority { _private: () }.accept_thread_capabilities_for_test(
            &core,
            AuthorizationCapabilitySnapshot {
                schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
                authorization_revision: 1,
                principal_id: PrincipalId::new("P00000000000000000001").unwrap(),
                role_key: "member".into(),
                role: AuthorizationRolePresentation {
                    key: "member".into(),
                    display_name: "Synthetic".into(),
                    description: String::new(),
                    built_in: false,
                },
                global: Default::default(),
                workspace: Some(AuthorizationWorkspaceCapabilitySnapshot {
                    workspace_id: "ws".into(),
                    capabilities: AuthorizationWorkspaceCapabilities {
                        can_use_skills: true,
                        can_use_mcp: true,
                        ..Default::default()
                    },
                    operational_resources: AuthorizationOperationalResourceProjection {
                        skills: AuthorizationResourceSelector {
                            all: true,
                            ids: vec![],
                        },
                        mcp_servers: AuthorizationResourceSelector {
                            all: true,
                            ids: vec![],
                        },
                        ..Default::default()
                    },
                    execution_draft_policy: AuthorizationExecutionDraftPolicyProjection {
                        fingerprint: "policy".into(),
                        resources: AuthorizationOperationalResourceProjection {
                            skills: AuthorizationResourceSelector {
                                all: true,
                                ids: vec![],
                            },
                            mcp_servers: AuthorizationResourceSelector {
                                all: true,
                                ids: vec![],
                            },
                            ..Default::default()
                        },
                        permission_options: vec![],
                        can_attach_artifacts: false,
                        mcp_invocation_limits: Default::default(),
                    },
                }),
                thread: Some(AuthorizationThreadCapabilitySnapshot {
                    thread_id: "a".into(),
                    workspace_id: "ws".into(),
                    capabilities: AuthorizationThreadCapabilities {
                        can_start_turn: true,
                        ..Default::default()
                    },
                }),
            },
        );
        core.composer_intent(ComposerIntent::Open {
            thread_id: "a".into(),
            defaults: super::super::state_machine::ComposerDomainState {
                selected_mode: ThreadMode::Agent,
                ..Default::default()
            },
        });
        let (sender, receiver) = mpsc::sync_channel(64);
        core.composer_catalog_requests.lock().unwrap().sender = Some(sender);
        (core, receiver)
    }
    fn observe(
        core: &ClientCore,
        kind: ComposerCatalogKind,
        retry: bool,
    ) -> ClientTransitionOutcome {
        let draft_id = core.composer_snapshot("a").unwrap().draft_id();
        core.composer_catalog_intent(if retry {
            ComposerCatalogIntent::Retry {
                thread_id: "a".into(),
                draft_id,
                catalog: kind,
            }
        } else {
            ComposerCatalogIntent::Observe {
                thread_id: "a".into(),
                draft_id,
                catalog: kind,
            }
        })
        .outcome()
    }
    fn open(
        core: &ClientCore,
        kind: ComposerPickerKind,
        deferred: bool,
    ) -> ComposerOperationIdentity {
        let draft_id = core.composer_snapshot("a").unwrap().draft_id();
        assert_eq!(
            core.composer_catalog_intent(ComposerCatalogIntent::OpenPicker {
                thread_id: "a".into(),
                draft_id,
                picker: kind,
                deferred
            })
            .outcome(),
            ClientTransitionOutcome::Changed
        );
        core.composer_catalog_snapshot("a")
            .unwrap()
            .session
            .as_ref()
            .unwrap()
            .identity
            .clone()
    }
    fn skill(id: char) -> SkillListItem {
        serde_json::from_value(serde_json::json!({
            "skill_id":id.to_string().repeat(21), "owner":null, "slug":id.to_string(), "source_kind":"user", "display_name":id.to_string(), "description":"Synthetic skill", "version":null,"fingerprint":"skill", "trust_level":"community", "install":{"managed":true,"installed":true,"lifecycle_editable":true,"install_path":null,"updated_at":null}, "policy":{"enabled":true,"allow_implicit_invocation":true,"allow_implicit_invocation_editable":true}, "health":{"status":"ok","dependency_failures":[],"security_blocks":[],"validation_issues":[]},"status":"active","status_reason":null
        })).unwrap()
    }
    fn selection(id: char) -> ComposerSkillSelection {
        ComposerSkillSelection::Skill {
            skill_id: skill(id).skill_id,
            pack_id: None,
        }
    }
    fn management() -> SkillManagementProjection {
        SkillManagementProjection {
            standalone: vec![skill('A'), skill('B')],
            packs: vec![],
        }
    }
    fn server(tool: bool) -> SelectableMcpCapability {
        SelectableMcpCapability {
            key: if tool { "tool" } else { "server" }.into(),
            label: "Synthetic".into(),
            description: String::new(),
            server_id: "server".into(),
            server_name: "Synthetic".into(),
            raw_tool_name: tool.then(|| "tool".into()),
            scope_kind: McpScopeKind::Workspace,
            tools_count: Some(1),
            selectable: true,
            unavailable_reason: None,
        }
    }
    #[test]
    fn persistent_failure_requires_retry_and_equal_observation_does_not_schedule_work() {
        let (core, receiver) = fixture();
        observe(&core, ComposerCatalogKind::Skills, false);
        let first = receiver.try_recv().unwrap();
        core.complete_composer_catalog(first.clone(), Err("persistent".into()));
        let failed = core.composer_catalog_snapshot("a").unwrap();
        for _ in 0..100 {
            assert_eq!(
                observe(&core, ComposerCatalogKind::Skills, false),
                ClientTransitionOutcome::Noop
            );
        }
        assert!(receiver.try_recv().is_err());
        assert!(Arc::ptr_eq(
            &failed,
            &core.composer_catalog_snapshot("a").unwrap()
        ));
        observe(&core, ComposerCatalogKind::Skills, true);
        let retry = receiver.try_recv().unwrap();
        assert_ne!(first.identity.generation, retry.identity.generation);
        core.complete_composer_catalog(first, Ok(CatalogResult::Skills(management())));
        assert_eq!(
            core.composer_catalog_snapshot("a")
                .unwrap()
                .skill_request
                .state,
            ComposerCatalogRequestState::Loading
        );
        core.complete_composer_catalog(retry.clone(), Ok(CatalogResult::Skills(management())));
        let ready = core.composer_catalog_snapshot("a").unwrap();
        core.complete_composer_catalog(retry, Err("duplicate".into()));
        assert!(Arc::ptr_eq(
            &ready,
            &core.composer_catalog_snapshot("a").unwrap()
        ));
    }
    #[test]
    fn deferred_selection_is_owned_by_client_and_only_commit_changes_the_draft() {
        let (core, receiver) = fixture();
        let identity = open(&core, ComposerPickerKind::Skills, true);
        core.complete_composer_catalog(
            receiver.try_recv().unwrap(),
            Ok(CatalogResult::Skills(management())),
        );
        let draft = core.composer_snapshot("a").unwrap();
        core.composer_catalog_intent(ComposerCatalogIntent::ToggleSkill {
            identity: identity.clone(),
            selection: selection('B'),
        });
        assert!(Arc::ptr_eq(&draft, &core.composer_snapshot("a").unwrap()));
        core.composer_catalog_intent(ComposerCatalogIntent::ClosePicker {
            identity: identity.clone(),
        });
        let next = open(&core, ComposerPickerKind::Skills, true);
        assert_ne!(identity, next);
        assert_eq!(
            core.composer_catalog_intent(ComposerCatalogIntent::ToggleSkill {
                identity,
                selection: selection('A')
            })
            .outcome(),
            ClientTransitionOutcome::Noop
        );
        core.composer_catalog_intent(ComposerCatalogIntent::ToggleSkill {
            identity: next.clone(),
            selection: selection('B'),
        });
        core.composer_catalog_intent(ComposerCatalogIntent::CommitPicker {
            identity: next.clone(),
        });
        assert_eq!(
            core.composer_snapshot("a")
                .unwrap()
                .domain()
                .skill_selections,
            vec![selection('B')]
        );
        assert_eq!(
            core.composer_catalog_intent(ComposerCatalogIntent::CommitPicker { identity: next })
                .outcome(),
            ClientTransitionOutcome::Noop
        );
    }
    #[test]
    fn immediate_selection_uses_domain_identity_after_catalog_reordering() {
        let (core, receiver) = fixture();
        let identity = open(&core, ComposerPickerKind::Skills, false);
        core.complete_composer_catalog(
            receiver.try_recv().unwrap(),
            Ok(CatalogResult::Skills(management())),
        );
        core.composer_catalog_intent(ComposerCatalogIntent::ToggleSkill {
            identity: identity.clone(),
            selection: selection('B'),
        });
        assert_eq!(
            core.composer_snapshot("a")
                .unwrap()
                .domain()
                .skill_selections,
            vec![selection('B')]
        );
        observe(&core, ComposerCatalogKind::Skills, true);
        let mut reordered = management();
        reordered.standalone.reverse();
        core.complete_composer_catalog(
            receiver.try_recv().unwrap(),
            Ok(CatalogResult::Skills(reordered)),
        );
        core.composer_catalog_intent(ComposerCatalogIntent::ToggleSkill {
            identity,
            selection: selection('B'),
        });
        assert!(
            core.composer_snapshot("a")
                .unwrap()
                .domain()
                .skill_selections
                .is_empty()
        );
    }
    #[test]
    fn mcp_catalog_prefetch_and_selection_share_canonical_rows() {
        let (core, receiver) = fixture();
        let identity = open(&core, ComposerPickerKind::Mcp, false);
        core.complete_composer_catalog(
            receiver.try_recv().unwrap(),
            Ok(CatalogResult::Servers(
                vec![server(false)],
                vec!["server".into()],
            )),
        );
        let tools = receiver.try_recv().unwrap();
        assert_eq!(
            tools.kind,
            ComposerCatalogKind::McpTools {
                server_id: "server".into()
            }
        );
        core.complete_composer_catalog(tools, Ok(CatalogResult::Tools(vec![server(true)])));
        core.composer_catalog_intent(ComposerCatalogIntent::ToggleMcp {
            identity: identity.clone(),
            key: "tool".into(),
        });
        assert_eq!(
            core.composer_snapshot("a")
                .unwrap()
                .domain()
                .capabilities
                .len(),
            1
        );
        core.composer_catalog_intent(ComposerCatalogIntent::ToggleMcp {
            identity: identity.clone(),
            key: "server".into(),
        });
        assert_eq!(
            core.composer_snapshot("a")
                .unwrap()
                .domain()
                .capabilities
                .len(),
            1
        );
        assert!(matches!(
            core.composer_snapshot("a").unwrap().domain().capabilities[0].kind,
            capabilities::ComposerCapabilityKind::McpServer { .. }
        ));
        assert_eq!(
            core.composer_catalog_intent(ComposerCatalogIntent::ToggleMcp {
                identity,
                key: "missing".into()
            })
            .outcome(),
            ClientTransitionOutcome::Rejected
        );
    }
    #[test]
    fn cancelled_wrong_draft_and_unmounted_requests_cannot_restore_a_catalog() {
        for scenario in 0..7 {
            let (core, receiver) = fixture();
            let scope = ClientScope::ComposerCatalog {
                thread_id: "a".into(),
            };
            let lease = core.subscribe(scope.clone(), std::num::NonZeroUsize::new(8).unwrap());
            observe(&core, ComposerCatalogKind::Skills, false);
            let work = receiver.try_recv().unwrap();
            match scenario {
                0 => {
                    core.composer_intent(ComposerIntent::Clear {
                        thread_id: "a".into(),
                        draft_id: work.identity.draft_id,
                    });
                }
                1 => drop(lease),
                2 => core.composer_catalog_demand_changed(&scope, ClientDemand::Suspended),
                3 => {
                    core.composer_intent(ComposerIntent::ClearAll);
                }
                4 => core.clear_authorization_projections(),
                5 => core.remove_thread_store("a"),
                _ => core.shutdown(),
            }
            let before = core.composer_catalog_snapshot("a");
            core.complete_composer_catalog(work, Ok(CatalogResult::Skills(management())));
            assert_eq!(
                before,
                core.composer_catalog_snapshot("a"),
                "scenario {scenario}"
            );
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl ClientMutationAuthority {
    pub fn accept_composer_catalog_for_test(
        &self,
        core: &ClientCore,
        thread: &str,
        skills: SkillManagementProjection,
        servers: Vec<SelectableMcpCapability>,
        tools: Vec<SelectableMcpCapability>,
    ) {
        let mut store = core.composer_store.lock().unwrap();
        let draft = store
            .drafts
            .get(thread)
            .expect("composer fixture")
            .draft_id();
        let mut next = store
            .catalogs
            .get(thread)
            .filter(|p| p.draft_id == draft)
            .map(|p| (**p).clone())
            .unwrap_or_else(|| ComposerCatalogPublication::new(thread, draft));
        next.skills = skills;
        next.mcp_servers = servers;
        next.mcp_tools = tools;
        next.skill_request.state = ComposerCatalogRequestState::Ready;
        next.mcp_request.state = ComposerCatalogRequestState::Ready;
        core.publish_composer_catalog(&mut store, next);
    }
}
