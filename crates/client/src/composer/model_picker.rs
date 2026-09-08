//! Draft-owned model selection sessions. Shells retain only controls and overlay state.
use super::{
    capabilities::composer_capability_target_for_provider,
    catalog::{ComposerCatalogRequest, ComposerCatalogRequestState},
    model_selection::ModelSelectorSelection,
    store::{ComposerOperationIdentity, ComposerStore, DraftId},
};
use crate::{
    core::{ClientCore, ClientDemand, ClientMutationAuthority, ClientScope, ClientTransition},
    providers::{
        list::{self, ProviderModelSelectorState},
        presentation,
    },
};
pub use pioneer_protocol::ProviderModelInfo;
use pioneer_protocol::{ProviderListModelsResponse, ProviderListResponse, RuntimeSummary};
use std::sync::{Arc, mpsc};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ComposerModelPickerProvider {
    pub id: String,
    pub label: String,
    pub cli_runtime: bool,
    pub capability_target: super::capabilities::ComposerCapabilityTarget,
    pub mcp_readiness_reason: Option<crate::providers::diagnostics::CliRuntimeMcpReadinessReason>,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ComposerModelPickerPublication {
    pub provider_rows: Vec<ComposerModelPickerProvider>,
    pub reasoning_rows: Vec<presentation::ReasoningEffortRow>,
    pub selected_provider_ready: bool,
    pub identity: ComposerOperationIdentity,
    pub revision: u64,
    pub selector: ProviderModelSelectorState,
    pub selected_reasoning_effort: Option<String>,
    pub deferred: bool,
    pub closed: bool,
    pub providers_request: ComposerCatalogRequest,
    pub models_request: ComposerCatalogRequest,
}
impl ComposerModelPickerPublication {
    pub fn selection(&self) -> ModelSelectorSelection {
        let (provider, model) = self.selector.selection_parts();
        ModelSelectorSelection {
            provider,
            model,
            selected_reasoning_effort: self.selected_reasoning_effort.clone(),
        }
    }
    pub fn reasoning_rows(&self) -> Vec<presentation::ReasoningEffortRow> {
        self.selector
            .models()
            .iter()
            .find(|m| Some(m.id.as_str()) == self.selector.selected_model())
            .map(|model| {
                presentation::reasoning_effort_rows_for_model(
                    model,
                    self.selected_reasoning_effort.as_deref(),
                )
            })
            .unwrap_or_default()
    }
    fn validate_effort(&mut self) {
        if self.selected_reasoning_effort.is_some()
            && !self.reasoning_rows().iter().any(|r| r.selected)
        {
            self.selected_reasoning_effort = None;
        }
    }
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ComposerModelPickerIntent {
    Open {
        thread_id: String,
        draft_id: DraftId,
        deferred: bool,
    },
    SelectProvider {
        identity: ComposerOperationIdentity,
        provider: String,
    },
    SelectModel {
        identity: ComposerOperationIdentity,
        model: String,
    },
    SelectReasoningEffort {
        identity: ComposerOperationIdentity,
        effort: Option<String>,
    },
    RetryProviders {
        identity: ComposerOperationIdentity,
    },
    RetryModels {
        identity: ComposerOperationIdentity,
    },
    Commit {
        identity: ComposerOperationIdentity,
    },
    Close {
        identity: ComposerOperationIdentity,
    },
}
#[derive(Clone)]
enum RequestKind {
    Providers,
    Models(String),
}
#[derive(Clone)]
struct ModelPickerWork {
    identity: ComposerOperationIdentity,
    generation: u64,
    workspace: String,
    auth: (u64, Option<u64>),
    kind: RequestKind,
}
enum ModelPickerResult {
    Providers(Result<ProviderListResponse, String>),
    Models(Result<ProviderListModelsResponse, String>),
}
#[derive(Default)]
pub(crate) struct ComposerModelPickerController {
    sender: Option<mpsc::SyncSender<ModelPickerWork>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl ComposerModelPickerController {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
    }
}
impl Drop for ComposerModelPickerController {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take()
            && task.thread().id() != std::thread::current().id()
        {
            let _ = task.join();
        }
    }
}
impl ClientCore {
    pub fn composer_model_picker_snapshot(
        &self,
        thread: &str,
    ) -> Option<Arc<ComposerModelPickerPublication>> {
        self.composer_store
            .lock()
            .expect("composer store poisoned")
            .model_pickers
            .get(thread)
            .cloned()
    }
    fn model_picker_noop(&self) -> ClientTransition {
        self.transition(&ClientMutationAuthority { _private: () }, vec![], vec![])
    }
    fn model_picker_allowed(&self, thread: &str, workspace: &str) -> bool {
        self.thread_capability_snapshot(thread)
            .and_then(|p| p.snapshot.clone())
            .or_else(|| self.authorization_snapshot(Some(workspace), None))
            .is_some_and(|snapshot| {
                let capabilities =
                    crate::authorization::principal_presentation_capabilities(&snapshot);
                capabilities.can_use_providers || capabilities.can_use_cli_runtimes
            })
    }
    pub(super) fn resume_composer_model_picker_models(&self, thread: &str) {
        let Some(current) = self.composer_model_picker_snapshot(thread).filter(|p| {
            !p.closed
                && p.selected_provider_ready
                && p.models_request.state == ComposerCatalogRequestState::Idle
        }) else {
            return;
        };
        if let Some(provider) = current.selector.selected_provider() {
            self.request_composer_model_picker(
                current.identity.clone(),
                RequestKind::Models(provider.to_owned()),
                false,
            );
        }
    }
    pub(super) fn sync_composer_model_picker_runtimes(
        &self,
        store: &mut ComposerStore,
        thread: &str,
        runtimes: Vec<RuntimeSummary>,
    ) {
        let Some(current) = store
            .model_pickers
            .get(thread)
            .filter(|p| !p.closed && !store.model_picker_suspended.contains(thread))
            .cloned()
        else {
            return;
        };
        if current.selector.cli_runtimes() == runtimes.as_slice() {
            return;
        }
        let mut next = (*current).clone();
        next.selector.sync_cli_runtime_snapshot(runtimes);
        self.publish_composer_model_picker(store, next);
    }
    fn publish_composer_model_picker(
        &self,
        store: &mut ComposerStore,
        mut next: ComposerModelPickerPublication,
    ) -> ClientTransition {
        next.provider_rows = next
            .selector
            .provider_rows()
            .into_iter()
            .map(|row| {
                let runtime =
                    list::runtime_id_from_cli_runtime_provider_key(&row.id).and_then(|id| {
                        next.selector
                            .cli_runtimes()
                            .iter()
                            .find(|r| r.runtime_id == id)
                    });
                ComposerModelPickerProvider {
                    capability_target: composer_capability_target_for_provider(
                        Some(&row.id),
                        next.selector.cli_runtimes(),
                    ),
                    mcp_readiness_reason: runtime
                        .and_then(crate::providers::diagnostics::cli_runtime_mcp_readiness_reason),
                    cli_runtime: runtime.is_some(),
                    id: row.id,
                    label: row.label,
                }
            })
            .collect();
        next.reasoning_rows = next.reasoning_rows();
        next.selected_provider_ready = list::provider_ready_for_model_selector(
            next.selector.selected_provider(),
            next.selector.cli_runtimes(),
        );
        let scope = ClientScope::ComposerModelPicker {
            thread_id: next.identity.thread_id.clone(),
        };
        next.revision = store
            .model_pickers
            .get(&next.identity.thread_id)
            .map_or_else(
                || {
                    self.snapshot(&scope)
                        .map_or(0, |p| p.revisions().scoped().get())
                },
                |p| p.revision,
            )
            .checked_add(1)
            .expect("composer model picker revision exhausted");
        let revision = next.revision;
        let next = Arc::new(next);
        store
            .model_pickers
            .insert(next.identity.thread_id.clone(), next.clone());
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(revision),
            next,
            vec![],
        )
    }
    pub fn composer_model_picker_intent(
        &self,
        intent: ComposerModelPickerIntent,
    ) -> ClientTransition {
        if self.is_stopped() {
            return self.reject_intent();
        }
        if let ComposerModelPickerIntent::Open {
            thread_id,
            draft_id,
            deferred,
        } = intent
        {
            let Some(workspace) = self
                .thread_coordinator_snapshot(&thread_id)
                .map(|p| p.workspace_id.clone())
            else {
                return self.reject_intent();
            };
            if !self.model_picker_allowed(&thread_id, &workspace) {
                return self.reject_intent();
            }
            let mut store = self.composer_store.lock().expect("composer store poisoned");
            let Some(draft) = store
                .drafts
                .get(&thread_id)
                .filter(|p| p.draft_id() == draft_id)
                .cloned()
            else {
                return self.model_picker_noop();
            };
            if store.suspended.contains(&thread_id)
                || store.model_picker_suspended.contains(&thread_id)
                || store
                    .model_pickers
                    .get(&thread_id)
                    .is_some_and(|p| !p.closed)
            {
                return self.model_picker_noop();
            }
            store.next_operation = store
                .next_operation
                .checked_add(1)
                .expect("composer operation generation exhausted");
            let identity = ComposerOperationIdentity {
                thread_id: thread_id.clone(),
                draft_id,
                generation: store.next_operation,
            };
            let next = ComposerModelPickerPublication {
                provider_rows: vec![],
                reasoning_rows: vec![],
                selected_provider_ready: false,
                identity: identity.clone(),
                revision: 0,
                selector: ProviderModelSelectorState::new(
                    draft.domain().selected_provider.clone(),
                    draft.domain().selected_model.clone(),
                ),
                selected_reasoning_effort: draft.domain().selected_reasoning_effort.clone(),
                deferred,
                closed: false,
                providers_request: Default::default(),
                models_request: Default::default(),
            };
            let transition = self.publish_composer_model_picker(&mut store, next);
            drop(store);
            self.observe_composer_runtime(&identity.thread_id, Some(identity.draft_id), true);
            self.request_composer_model_picker(identity, RequestKind::Providers, false);
            return transition;
        }
        let identity = match &intent {
            ComposerModelPickerIntent::SelectProvider { identity, .. }
            | ComposerModelPickerIntent::SelectModel { identity, .. }
            | ComposerModelPickerIntent::SelectReasoningEffort { identity, .. }
            | ComposerModelPickerIntent::RetryProviders { identity }
            | ComposerModelPickerIntent::RetryModels { identity }
            | ComposerModelPickerIntent::Commit { identity }
            | ComposerModelPickerIntent::Close { identity } => identity.clone(),
            _ => unreachable!(),
        };
        if matches!(intent, ComposerModelPickerIntent::RetryProviders { .. }) {
            self.observe_composer_runtime(&identity.thread_id, Some(identity.draft_id), true);
            return self.request_composer_model_picker(identity, RequestKind::Providers, true);
        }
        if matches!(intent, ComposerModelPickerIntent::RetryModels { .. }) {
            let Some(provider) = self
                .composer_model_picker_snapshot(&identity.thread_id)
                .and_then(|p| p.selector.selected_provider().map(str::to_owned))
            else {
                return self.model_picker_noop();
            };
            return self.request_composer_model_picker(
                identity,
                RequestKind::Models(provider),
                true,
            );
        }
        let Some(workspace) = self
            .thread_coordinator_snapshot(&identity.thread_id)
            .map(|p| p.workspace_id.clone())
        else {
            return self.model_picker_noop();
        };
        if !matches!(intent, ComposerModelPickerIntent::Close { .. })
            && !self.model_picker_allowed(&identity.thread_id, &workspace)
        {
            return self.reject_intent();
        }
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        if !model_picker_matches(&store, &identity) {
            return self.model_picker_noop();
        }
        let current = store
            .model_pickers
            .get(&identity.thread_id)
            .expect("matched picker")
            .clone();
        let mut next = (*current).clone();
        let mut request = None;
        let mut apply = !next.deferred;
        match intent {
            ComposerModelPickerIntent::SelectProvider { provider, .. } => {
                if next.selector.selected_provider() == Some(provider.as_str()) {
                    return self.model_picker_noop();
                }
                if !next
                    .selector
                    .provider_rows()
                    .iter()
                    .any(|p| p.id == provider)
                {
                    return self.reject_intent();
                }
                next.selector.select_provider(provider.clone());
                next.selected_reasoning_effort = None;
                next.models_request = Default::default();
                request = Some(RequestKind::Models(provider));
            }
            ComposerModelPickerIntent::SelectModel { model, .. } => {
                if next.selector.selected_model() == Some(model.as_str()) {
                    return self.model_picker_noop();
                }
                if !next.selector.models().iter().any(|m| m.id == model) {
                    return self.reject_intent();
                }
                next.selector.set_selected_model(model);
                next.selected_reasoning_effort = None;
            }
            ComposerModelPickerIntent::SelectReasoningEffort { effort, .. } => {
                if next.selected_reasoning_effort == effort {
                    return self.model_picker_noop();
                }
                if effort.as_ref().is_some_and(|effort| {
                    !next.reasoning_rows().iter().any(|r| &r.effort == effort)
                }) {
                    return self.reject_intent();
                }
                next.selected_reasoning_effort = effort;
            }
            ComposerModelPickerIntent::Commit { .. } => {
                apply = true;
                next.closed = true;
            }
            ComposerModelPickerIntent::Close { .. } => {
                apply = false;
                next.closed = true;
            }
            _ => unreachable!(),
        }
        let selection = next.selection();
        let target = composer_capability_target_for_provider(
            selection.provider.as_deref(),
            next.selector.cli_runtimes(),
        );
        let transition = self.publish_composer_model_picker(&mut store, next);
        if apply {
            self.apply_composer_model_picker_selection(&mut store, &identity, selection, target);
        }
        drop(store);
        if let Some(request) = request {
            self.request_composer_model_picker(identity.clone(), request, false);
        }
        if apply {
            self.observe_composer_model_display(&identity.thread_id, None);
        }
        transition
    }
    fn request_composer_model_picker(
        &self,
        identity: ComposerOperationIdentity,
        kind: RequestKind,
        retry: bool,
    ) -> ClientTransition {
        let Some(workspace) = self
            .thread_coordinator_snapshot(&identity.thread_id)
            .map(|p| p.workspace_id.clone())
        else {
            return self.model_picker_noop();
        };
        if self.is_stopped() || !self.model_picker_allowed(&identity.thread_id, &workspace) {
            return self.reject_intent();
        }
        let auth = self.current_auth_ticket();
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        if !model_picker_matches(&store, &identity) {
            return self.model_picker_noop();
        }
        let current = store
            .model_pickers
            .get(&identity.thread_id)
            .expect("matched picker");
        let mut next = (**current).clone();
        let request = match &kind {
            RequestKind::Providers => &mut next.providers_request,
            RequestKind::Models(provider) => {
                if next.selector.selected_provider() != Some(provider.as_str()) {
                    return self.model_picker_noop();
                }
                &mut next.models_request
            }
        };
        if request.state == ComposerCatalogRequestState::Loading
            || (!retry && request.state != ComposerCatalogRequestState::Idle)
        {
            return self.model_picker_noop();
        }
        store.next_operation = store
            .next_operation
            .checked_add(1)
            .expect("composer model request generation exhausted");
        let generation = store.next_operation;
        *request = ComposerCatalogRequest {
            generation,
            state: ComposerCatalogRequestState::Loading,
        };
        match &kind {
            RequestKind::Providers => next.selector.mark_providers_loading(),
            RequestKind::Models(_) => {
                next.selector.preload_selected_provider_models();
            }
        }
        let transition = self.publish_composer_model_picker(&mut store, next);
        drop(store);
        let work = ModelPickerWork {
            identity,
            generation,
            workspace,
            auth,
            kind,
        };
        if !self
            .composer_model_picker_requests
            .lock()
            .expect("model picker poisoned")
            .sender
            .as_ref()
            .is_some_and(|tx| tx.try_send(work.clone()).is_ok())
        {
            let message = "Model picker request queue unavailable".to_owned();
            let result = match work.kind {
                RequestKind::Providers => ModelPickerResult::Providers(Err(message)),
                RequestKind::Models(_) => ModelPickerResult::Models(Err(message)),
            };
            self.complete_composer_model_picker(work, result);
        }
        transition
    }
    fn model_picker_work_current(&self, work: &ModelPickerWork) -> bool {
        if self.is_stopped() || self.current_auth_ticket() != work.auth {
            return false;
        }
        let store = self.composer_store.lock().expect("composer store poisoned");
        model_picker_work_matches(&store, work)
    }
    fn complete_composer_model_picker(&self, work: ModelPickerWork, result: ModelPickerResult) {
        if !self.model_picker_work_current(&work) {
            return;
        }
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        if !model_picker_work_matches(&store, &work) {
            return;
        }
        let mut next = (**store
            .model_pickers
            .get(&work.identity.thread_id)
            .expect("matched picker"))
        .clone();
        let mut model_request = None;
        match result {
            ModelPickerResult::Providers(api) => {
                let mut error = None;
                match api {
                    Ok(response) => next.selector.apply_provider_list_success(response),
                    Err(message) => {
                        next.selector.apply_provider_list_error(message.clone());
                        error = Some(message);
                    }
                }
                next.selector
                    .sync_cli_runtime_snapshot(Self::composer_runtime_rows(
                        &store,
                        &work.identity.thread_id,
                    ));
                next.providers_request.state = if let Some(message) = error {
                    ComposerCatalogRequestState::Failed { message }
                } else {
                    ComposerCatalogRequestState::Ready
                };
                if let Some(provider) = next.selector.preload_selected_provider_models() {
                    next.models_request = Default::default();
                    model_request = Some(RequestKind::Models(provider));
                }
            }
            ModelPickerResult::Models(result) => match result {
                Ok(response) => {
                    next.selector.apply_provider_models_success(response);
                    next.models_request.state = ComposerCatalogRequestState::Ready;
                    next.validate_effort();
                }
                Err(message) => {
                    if let RequestKind::Models(provider) = &work.kind {
                        next.selector
                            .apply_provider_models_error(provider, message.clone());
                    }
                    next.models_request.state = ComposerCatalogRequestState::Failed { message };
                }
            },
        }
        let apply_effort = !next.deferred
            && store
                .drafts
                .get(&work.identity.thread_id)
                .is_some_and(|draft| {
                    draft.domain().selected_provider.as_deref() == next.selector.selected_provider()
                        && draft.domain().selected_model.as_deref()
                            == next.selector.selected_model()
                        && draft.domain().selected_reasoning_effort
                            != next.selected_reasoning_effort
                });
        if apply_effort {
            let target = composer_capability_target_for_provider(
                next.selector.selected_provider(),
                next.selector.cli_runtimes(),
            );
            self.apply_composer_model_picker_selection(
                &mut store,
                &work.identity,
                next.selection(),
                target,
            );
        }
        self.publish_composer_model_picker(&mut store, next);
        drop(store);
        if apply_effort {
            self.observe_composer_model_display(&work.identity.thread_id, None);
        }
        if let Some(kind) = model_request {
            self.request_composer_model_picker(work.identity, kind, false);
        }
    }
    pub(crate) fn cancel_composer_model_picker(&self, thread: &str) {
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        if let Some(current) = store.model_pickers.get(thread).cloned() {
            let mut next = (*current).clone();
            next.closed = true;
            next.selector = ProviderModelSelectorState::new(None, None);
            next.selected_reasoning_effort = None;
            next.providers_request.state = ComposerCatalogRequestState::Cancelled;
            next.models_request.state = ComposerCatalogRequestState::Cancelled;
            self.publish_composer_model_picker(&mut store, next);
            store.model_pickers.remove(thread);
        }
    }
    pub(crate) fn composer_model_picker_subscription_changed(
        &self,
        scope: &ClientScope,
        added: bool,
    ) {
        let ClientScope::ComposerModelPicker { thread_id } = scope else {
            return;
        };
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        let count = store
            .model_picker_subscriptions
            .entry(thread_id.clone())
            .or_default();
        if added {
            *count += 1;
            store.model_picker_suspended.remove(thread_id);
        } else {
            *count = count.saturating_sub(1);
            if *count == 0 {
                store.model_picker_subscriptions.remove(thread_id);
                drop(store);
                self.cancel_composer_model_picker(thread_id);
            }
        }
    }
    pub(crate) fn composer_model_picker_demand_changed(
        &self,
        scope: &ClientScope,
        demand: ClientDemand,
    ) {
        let ClientScope::ComposerModelPicker { thread_id } = scope else {
            return;
        };
        let mut store = self.composer_store.lock().expect("composer store poisoned");
        if demand != ClientDemand::Suspended {
            store.model_picker_suspended.remove(thread_id);
            return;
        }
        store.model_picker_suspended.insert(thread_id.clone());
        drop(store);
        self.cancel_composer_model_picker(thread_id);
    }
    pub(crate) fn start_composer_model_picker_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<ModelPickerWork>(64);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new().name("client-composer-model-picker".into()).spawn(move || {
            while let Ok(work) = receiver.recv() {
                let Some(core) = weak.upgrade() else { return; };
                if !core.model_picker_work_current(&work) { continue; }
                let sender = core.compatibility_runtime().ws_command_sender(); drop(core);
                let result = match &work.kind {
                    RequestKind::Providers => {
                        let api = sender.provider_list(list::provider_list_params(work.workspace.clone())).map_err(|e| format!("{e:#}"));
                        ModelPickerResult::Providers(api)
                    }
                    RequestKind::Models(provider) => ModelPickerResult::Models(
                        if let Some(runtime) = list::runtime_id_from_cli_runtime_provider_key(provider) {
                            sender.cli_runtime_list_models(list::cli_runtime_list_models_params(work.workspace.clone(), runtime.to_owned()))
                                .map(|r| list::provider_models_response_from_cli_runtime_models_response(provider.clone(), r))
                        } else { sender.provider_list_models(list::provider_list_models_params(work.workspace.clone(), provider.clone())) }
                        .map_err(|e| format!("{e:#}"))
                    ),
                };
                let Some(core) = weak.upgrade() else { return; }; core.complete_composer_model_picker(work, result);
            }
        }).expect("composer model picker worker could not start");
        let mut owner = self
            .composer_model_picker_requests
            .lock()
            .expect("composer model picker poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
}
fn model_picker_matches(store: &ComposerStore, identity: &ComposerOperationIdentity) -> bool {
    !store.suspended.contains(&identity.thread_id)
        && !store.model_picker_suspended.contains(&identity.thread_id)
        && store
            .drafts
            .get(&identity.thread_id)
            .is_some_and(|p| p.draft_id() == identity.draft_id)
        && store
            .model_pickers
            .get(&identity.thread_id)
            .is_some_and(|p| p.identity == *identity && !p.closed)
}
fn model_picker_work_matches(store: &ComposerStore, work: &ModelPickerWork) -> bool {
    if !model_picker_matches(store, &work.identity) {
        return false;
    }
    let input = store
        .model_pickers
        .get(&work.identity.thread_id)
        .expect("matched picker");
    let request = match &work.kind {
        RequestKind::Providers => &input.providers_request,
        RequestKind::Models(provider) => {
            if input.selector.selected_provider() != Some(provider.as_str()) {
                return false;
            }
            &input.models_request
        }
    };
    request.generation == work.generation && request.state == ComposerCatalogRequestState::Loading
}

#[cfg(test)]
mod tests {
    use super::super::store::ComposerIntent;
    use super::*;
    use crate::core::ClientTransitionOutcome;
    use pioneer_protocol::*;
    fn fixture() -> (Arc<ClientCore>, mpsc::Receiver<ModelPickerWork>) {
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
                        can_use_providers: true,
                        can_use_cli_runtimes: true,
                        can_use_skills: true,
                        can_use_mcp: true,
                        ..Default::default()
                    },
                    operational_resources: AuthorizationOperationalResourceProjection {
                        provider_models_all: true,
                        cli_models_all: true,
                        cli_runtimes: AuthorizationResourceSelector {
                            all: true,
                            ids: vec![],
                        },
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
                            provider_models_all: true,
                            cli_models_all: true,
                            cli_runtimes: AuthorizationResourceSelector {
                                all: true,
                                ids: vec![],
                            },
                            providers: AuthorizationResourceSelector {
                                all: true,
                                ids: vec![],
                            },
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
        core.composer_model_picker_requests.lock().unwrap().sender = Some(sender);
        (core, receiver)
    }
    fn open(core: &ClientCore, deferred: bool) -> ComposerOperationIdentity {
        let draft_id = core.composer_snapshot("a").unwrap().draft_id();
        assert_eq!(
            core.composer_model_picker_intent(ComposerModelPickerIntent::Open {
                thread_id: "a".into(),
                draft_id,
                deferred
            })
            .outcome(),
            ClientTransitionOutcome::Changed
        );
        core.composer_model_picker_snapshot("a")
            .unwrap()
            .identity
            .clone()
    }
    fn providers() -> ModelPickerResult {
        ModelPickerResult::Providers(Ok(ProviderListResponse {
            providers: vec!["provider", "other"]
                .into_iter()
                .map(|name| serde_json::from_value(serde_json::json!({"name":name})).unwrap())
                .collect(),
        }))
    }
    fn models(provider: &str) -> ModelPickerResult {
        ModelPickerResult::Models(Ok(serde_json::from_value(serde_json::json!({"provider":provider,"models":[{"id":"one", "provider":provider,"name":"One", "limits":{}, "capabilities":{}}]})).unwrap()))
    }
    fn ready(core: &ClientCore, rx: &mpsc::Receiver<ModelPickerWork>) {
        core.complete_composer_model_picker(rx.try_recv().unwrap(), providers());
        if let Ok(work) = rx.try_recv() {
            let RequestKind::Models(provider) = &work.kind else {
                panic!("models expected");
            };
            let result = models(provider);
            core.complete_composer_model_picker(work, result);
        }
    }
    #[test]
    fn runtime_readiness_arriving_after_providers_resumes_the_selected_model_request() {
        let (core, rx) = fixture();
        let draft = core.composer_snapshot("a").unwrap();
        core.composer_intent(ComposerIntent::Domain {
            thread_id: "a".into(),
            draft_id: draft.draft_id(),
            action: super::super::state_machine::ComposerDomainAction::SetModelSelectionFromUser {
                provider: Some("cli_runtime:codex".into()),
                model: Some("model".into()),
                capability_target: None,
            },
        });
        open(&core, true);
        core.complete_composer_model_picker(rx.try_recv().unwrap(), providers());
        assert!(rx.try_recv().is_err());
        let runtime = serde_json::from_value(serde_json::json!({"runtime_id":"codex", "kind":"codex", "display_name":"Codex", "enabled":true, "status":{"state":"ready"}, "capabilities":RuntimeCapabilities { supports_threads:true, supports_model_list:true, ..Default::default() }})).unwrap();
        ClientMutationAuthority { _private: () }.accept_composer_runtime_for_test(
            &core,
            "a",
            CLIRuntimeListResponse {
                revision: 1,
                runtimes: vec![runtime],
            },
        );
        let work = rx.try_recv().unwrap();
        assert!(
            matches!(&work.kind, RequestKind::Models(provider) if provider == "cli_runtime:codex")
        );
        core.complete_composer_model_picker(work, models("cli_runtime:codex"));
        let picker = core.composer_model_picker_snapshot("a").unwrap();
        assert!(picker.selected_provider_ready);
        assert_eq!(picker.selector.models().len(), 1);
        assert!(rx.try_recv().is_err());
    }
    #[test]
    fn deferred_model_session_commits_once_without_editor_echo() {
        let (core, rx) = fixture();
        let identity = open(&core, true);
        ready(&core, &rx);
        let before = core.composer_snapshot("a").unwrap();
        core.composer_model_picker_intent(ComposerModelPickerIntent::SelectProvider {
            identity: identity.clone(),
            provider: "other".into(),
        });
        let work = rx.try_recv().unwrap();
        core.complete_composer_model_picker(work, models("other"));
        core.composer_model_picker_intent(ComposerModelPickerIntent::SelectModel {
            identity: identity.clone(),
            model: "one".into(),
        });
        assert!(Arc::ptr_eq(&before, &core.composer_snapshot("a").unwrap()));
        let selected = core.composer_model_picker_snapshot("a").unwrap();
        assert_eq!(
            core.composer_model_picker_intent(ComposerModelPickerIntent::SelectModel {
                identity: identity.clone(),
                model: "one".into()
            })
            .outcome(),
            ClientTransitionOutcome::Noop
        );
        assert!(Arc::ptr_eq(
            &selected,
            &core.composer_model_picker_snapshot("a").unwrap()
        ));
        assert_eq!(
            core.composer_model_picker_intent(ComposerModelPickerIntent::Commit {
                identity: identity.clone()
            })
            .outcome(),
            ClientTransitionOutcome::Changed
        );
        let after = core.composer_snapshot("a").unwrap();
        assert_eq!(after.domain().selected_provider.as_deref(), Some("other"));
        assert_eq!(after.domain().selected_model.as_deref(), Some("one"));
        assert_eq!(
            core.composer_model_picker_intent(ComposerModelPickerIntent::Commit { identity })
                .outcome(),
            ClientTransitionOutcome::Noop
        );
        assert!(Arc::ptr_eq(&after, &core.composer_snapshot("a").unwrap()));
    }
    #[test]
    fn repeated_provider_identity_does_not_accept_an_older_request() {
        let (core, rx) = fixture();
        let identity = open(&core, false);
        ready(&core, &rx);
        let select = |provider: &str| {
            core.composer_model_picker_intent(ComposerModelPickerIntent::SelectProvider {
                identity: identity.clone(),
                provider: provider.into(),
            })
        };
        select("other");
        let first = rx.try_recv().unwrap();
        select("provider");
        let second = rx.try_recv().unwrap();
        select("other");
        let third = rx.try_recv().unwrap();
        let pending = core.composer_model_picker_snapshot("a").unwrap();
        core.complete_composer_model_picker(first, models("other"));
        core.complete_composer_model_picker(second, models("provider"));
        assert!(Arc::ptr_eq(
            &pending,
            &core.composer_model_picker_snapshot("a").unwrap()
        ));
        core.complete_composer_model_picker(third.clone(), models("other"));
        let ready = core.composer_model_picker_snapshot("a").unwrap();
        core.complete_composer_model_picker(
            third,
            ModelPickerResult::Models(Err("duplicate".into())),
        );
        assert!(Arc::ptr_eq(
            &ready,
            &core.composer_model_picker_snapshot("a").unwrap()
        ));
        core.composer_model_picker_intent(ComposerModelPickerIntent::SelectModel {
            identity,
            model: "one".into(),
        });
        assert_eq!(
            core.composer_snapshot("a")
                .unwrap()
                .domain()
                .selected_model
                .as_deref(),
            Some("one")
        );
    }
    #[test]
    fn persistent_failure_has_explicit_retry_and_matching_generation() {
        let (core, rx) = fixture();
        let identity = open(&core, true);
        let first = rx.try_recv().unwrap();
        core.complete_composer_model_picker(
            first.clone(),
            ModelPickerResult::Providers(Err("persistent".into())),
        );
        let failed = core.composer_model_picker_snapshot("a").unwrap();
        for _ in 0..100 {
            assert!(Arc::ptr_eq(
                &failed,
                &core.composer_model_picker_snapshot("a").unwrap()
            ));
        }
        assert!(rx.try_recv().is_err());
        core.composer_model_picker_intent(ComposerModelPickerIntent::RetryProviders {
            identity: identity.clone(),
        });
        let retry = rx.try_recv().unwrap();
        assert_ne!(retry.generation, first.generation);
        assert_eq!(
            core.composer_model_picker_intent(ComposerModelPickerIntent::RetryProviders {
                identity
            })
            .outcome(),
            ClientTransitionOutcome::Noop
        );
        core.complete_composer_model_picker(first, providers());
        assert_eq!(
            core.composer_model_picker_snapshot("a")
                .unwrap()
                .providers_request
                .state,
            ComposerCatalogRequestState::Loading
        );
        core.complete_composer_model_picker(retry, providers());
        assert_eq!(
            core.composer_model_picker_snapshot("a")
                .unwrap()
                .providers_request
                .state,
            ComposerCatalogRequestState::Ready
        );
    }
    #[test]
    fn close_draft_replacement_access_loss_and_drop_reject_late_results() {
        for scenario in 0..8 {
            let (core, rx) = fixture();
            let scope = ClientScope::ComposerModelPicker {
                thread_id: "a".into(),
            };
            let lease = core.subscribe(scope.clone(), std::num::NonZeroUsize::new(8).unwrap());
            let identity = open(&core, true);
            let work = rx.try_recv().unwrap();
            match scenario {
                0 => {
                    core.composer_model_picker_intent(ComposerModelPickerIntent::Close {
                        identity: identity.clone(),
                    });
                }
                1 => {
                    core.composer_intent(ComposerIntent::Clear {
                        thread_id: "a".into(),
                        draft_id: identity.draft_id,
                    });
                }
                2 => drop(lease),
                3 => core.composer_model_picker_demand_changed(&scope, ClientDemand::Suspended),
                4 => core.clear_authorization_projections(),
                5 => core.remove_thread_store("a"),
                6 => {
                    core.composer_intent(ComposerIntent::ClearAll);
                }
                _ => core.shutdown(),
            }
            let before = core
                .composer_model_picker_snapshot("a")
                .map(|p| serde_json::to_value(p.as_ref()).unwrap());
            core.complete_composer_model_picker(work, providers());
            assert_eq!(
                before,
                core.composer_model_picker_snapshot("a")
                    .map(|p| serde_json::to_value(p.as_ref()).unwrap()),
                "scenario {scenario}"
            );
            assert_eq!(
                core.composer_model_picker_intent(ComposerModelPickerIntent::SelectModel {
                    identity,
                    model: "one".into()
                })
                .outcome(),
                if scenario == 7 {
                    ClientTransitionOutcome::Rejected
                } else {
                    ClientTransitionOutcome::Noop
                }
            );
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl ClientMutationAuthority {
    pub fn accept_composer_model_picker_for_test(
        &self,
        core: &ClientCore,
        thread: &str,
        providers: ProviderListResponse,
        models: ProviderListModelsResponse,
    ) {
        let mut store = core.composer_store.lock().expect("composer store poisoned");
        let current = store
            .model_pickers
            .get(thread)
            .expect("model picker fixture");
        let mut next = (**current).clone();
        next.selector.apply_provider_list_success(providers);
        next.selector.sync_cli_runtime_snapshot(vec![]);
        next.selector.select_provider(models.provider.clone());
        next.selector.apply_provider_models_success(models);
        next.providers_request.state = ComposerCatalogRequestState::Ready;
        next.models_request.state = ComposerCatalogRequestState::Ready;
        core.publish_composer_model_picker(&mut store, next);
    }
}
