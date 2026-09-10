//! Settings model selection sessions over the shared provider collection owner.
use crate::{
    composer::model_selection::ModelSelectorSelection,
    core::*,
    providers::{
        list::{
            ProviderModelSelectorMode, ProviderModelSelectorState,
            provider_ready_for_model_selector,
        },
        presentation,
        runtime::ProviderRuntimeIntent,
        store::{ProviderCollectionKey, ProviderModelKind},
    },
};
use std::{collections::BTreeMap, sync::Arc};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize)]
pub struct SettingsModelPickerPublication {
    pub picker_id: String,
    pub owner_generation: u64,
    pub workspace_id: String,
    pub selector: ProviderModelSelectorState,
    pub selected_reasoning_effort: Option<String>,
    pub closed: bool,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SettingsModelPickerIntent {
    Open {
        picker_id: String,
        workspace_id: String,
        mode: ProviderModelSelectorMode,
        selection: ModelSelectorSelection,
    },
    SelectProvider {
        picker_id: String,
        expected_owner: u64,
        provider: String,
    },
    SelectModel {
        picker_id: String,
        expected_owner: u64,
        model: String,
    },
    SelectReasoningEffort {
        picker_id: String,
        expected_owner: u64,
        effort: Option<String>,
    },
    RetryProviders {
        picker_id: String,
        expected_owner: u64,
    },
    RetryModels {
        picker_id: String,
        expected_owner: u64,
    },
    Close {
        picker_id: String,
        expected_owner: u64,
    },
}
impl SettingsModelPickerIntent {
    fn id(&self) -> &str {
        match self {
            Self::Open { picker_id, .. }
            | Self::SelectProvider { picker_id, .. }
            | Self::SelectModel { picker_id, .. }
            | Self::SelectReasoningEffort { picker_id, .. }
            | Self::RetryProviders { picker_id, .. }
            | Self::RetryModels { picker_id, .. }
            | Self::Close { picker_id, .. } => picker_id,
        }
    }
    fn generation(&self) -> Option<u64> {
        match self {
            Self::Open { .. } => None,
            Self::SelectProvider { expected_owner, .. }
            | Self::SelectModel { expected_owner, .. }
            | Self::SelectReasoningEffort { expected_owner, .. }
            | Self::RetryProviders { expected_owner, .. }
            | Self::RetryModels { expected_owner, .. }
            | Self::Close { expected_owner, .. } => Some(*expected_owner),
        }
    }
}
struct Session {
    value: Arc<SettingsModelPickerPublication>,
    authorization: (u64, u64),
    navigation_revision: u64,
    mode: ProviderModelSelectorMode,
    provider_generation: u64,
    model_generation: u64,
    provider_pending: bool,
    model_pending: bool,
    runtime_demand: bool,
}
#[derive(Clone)]
struct Work {
    id: String,
    owner: u64,
    generation: u64,
    key: ProviderCollectionKey,
    models: bool,
}
#[derive(Default)]
pub(crate) struct SettingsModelPickerController {
    sessions: BTreeMap<String, Session>,
    generation: u64,
    subscriptions: BTreeMap<String, usize>,
    sender: Option<tokio::sync::mpsc::Sender<Work>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl SettingsModelPickerController {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
        self.sessions.clear();
        self.subscriptions.clear();
    }
}
impl Drop for SettingsModelPickerController {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}
fn scope(id: &str) -> ClientScope {
    ClientScope::SettingsModelPicker {
        picker_id: id.into(),
    }
}
impl ClientCore {
    pub fn settings_model_picker(&self, id: &str) -> Option<Arc<SettingsModelPickerPublication>> {
        self.snapshot(&scope(id))
            .and_then(|p| p.snapshot().payload())
    }
    pub fn settings_model_picker_selection(
        &self,
        id: &str,
        generation: u64,
    ) -> Option<ModelSelectorSelection> {
        let identity_guard = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        let navigation_guard = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let authorization = identity_guard.authorization_epoch();
        let workspace = navigation_guard.navigation.workspace_id();
        let store = self
            .settings_model_pickers
            .lock()
            .expect("settings model picker poisoned");
        let session = store.sessions.get(id)?;
        if session.navigation_revision != navigation_guard.navigation_revision
            || session.authorization != authorization
            || workspace.as_deref() != Some(&session.value.workspace_id)
        {
            return None;
        }
        let value = &session.value;
        if value.closed || value.owner_generation != generation {
            return None;
        }
        let (provider, model) = value.selector.selection_parts();
        Some(ModelSelectorSelection {
            provider,
            model,
            selected_reasoning_effort: value.selected_reasoning_effort.clone(),
        })
    }
    pub fn settings_model_picker_intent(
        &self,
        intent: SettingsModelPickerIntent,
    ) -> ClientTransition {
        if self.is_stopped() {
            return self.reject_intent();
        }
        let authorization = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned")
            .authorization_epoch();
        let workspace = self.settings_workspace();
        let allowed = self
            .authorization_snapshot(None, None)
            .is_some_and(|value| value.global.can_manage_gateway_settings);
        let runtimes = workspace
            .as_deref()
            .and_then(|id| self.provider_runtime_snapshot(id))
            .map(|p| {
                p.runtimes()
                    .iter()
                    .map(|r| r.runtime().clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let id = intent.id().to_owned();
        let identity_guard = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if identity_guard.authorization_epoch() != authorization {
            return self.reject_intent();
        }
        let navigation_guard = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if navigation_guard.navigation.workspace_id() != workspace.as_deref() {
            return self.reject_intent();
        }
        let mut store = self
            .settings_model_pickers
            .lock()
            .expect("settings model picker poisoned");
        let mut released = None;
        let mut observe = None;
        let mut work = vec![];
        if let SettingsModelPickerIntent::Open {
            workspace_id,
            mode,
            selection,
            ..
        } = &intent
        {
            if workspace.as_deref() != Some(workspace_id)
                || !allowed
                || (store.sessions.len() >= 16 && !store.sessions.contains_key(&id))
            {
                return self.reject_intent();
            }
            let previous = store.sessions.remove(&id);
            if let Some(previous) = previous.filter(|s| s.runtime_demand) {
                released = Some(previous.value.workspace_id.clone());
            }
            store.generation += 1;
            let mut selector = ProviderModelSelectorState::new_with_mode(
                selection.provider.clone(),
                selection.model.clone(),
                *mode,
            );
            selector.mark_providers_loading();
            let runtime = *mode == ProviderModelSelectorMode::Chat;
            if runtime {
                observe = Some(workspace_id.clone());
                selector.sync_cli_runtime_snapshot(runtimes);
            }
            let selected = selector.preload_selected_provider_models();
            let value = SettingsModelPickerPublication {
                picker_id: id.clone(),
                owner_generation: store.generation,
                workspace_id: workspace_id.clone(),
                selector,
                selected_reasoning_effort: selection.selected_reasoning_effort.clone(),
                closed: false,
            };
            let session = Session {
                value: Arc::new(value),
                authorization,
                navigation_revision: navigation_guard.navigation_revision,
                mode: *mode,
                provider_generation: 1,
                model_generation: 1,
                provider_pending: true,
                model_pending: selected.is_some(),
                runtime_demand: runtime,
            };
            work.push(Work {
                id: id.clone(),
                owner: session.value.owner_generation,
                generation: 1,
                key: ProviderCollectionKey::catalog(workspace_id.clone()),
                models: false,
            });
            if let Some(provider) = selected {
                work.push(model_work(&session, provider));
            }
            store.sessions.insert(id.clone(), session);
        } else {
            let Some(session) = store.sessions.get_mut(&id) else {
                return self.reject_intent();
            };
            if intent.generation() != Some(session.value.owner_generation) {
                return self.reject_intent();
            }
            if !matches!(intent, SettingsModelPickerIntent::Close { .. })
                && (session.navigation_revision != navigation_guard.navigation_revision
                    || session.authorization != authorization
                    || workspace.as_deref() != Some(&session.value.workspace_id))
            {
                return self.reject_intent();
            }
            let mut next = (*session.value).clone();
            match intent {
                SettingsModelPickerIntent::SelectProvider { provider, .. } => {
                    if next.selector.selected_provider() == Some(&provider) {
                        return self.navigation_outcome(ClientTransitionOutcome::Noop);
                    }
                    if !next
                        .selector
                        .provider_rows()
                        .iter()
                        .any(|row| row.id == provider)
                    {
                        return self.reject_intent();
                    }
                    let provider = next.selector.select_provider(provider);
                    next.selected_reasoning_effort = None;
                    session.model_generation += 1;
                    session.model_pending = true;
                    work.push(model_work(session, provider));
                }
                SettingsModelPickerIntent::SelectModel { model, .. } => {
                    if !next.selector.models().iter().any(|row| row.id == model) {
                        return self.reject_intent();
                    }
                    if next.selector.selected_model() != Some(model.as_str()) {
                        next.selector.set_selected_model(model);
                        next.selected_reasoning_effort = None;
                    }
                }
                SettingsModelPickerIntent::SelectReasoningEffort { effort, .. } => {
                    let rows = next
                        .selector
                        .models()
                        .iter()
                        .find(|row| Some(row.id.as_str()) == next.selector.selected_model())
                        .map(|row| {
                            presentation::reasoning_effort_rows_for_model(row, effort.as_deref())
                        })
                        .unwrap_or_default();
                    if effort.is_some() && !rows.iter().any(|row| row.selected) {
                        return self.reject_intent();
                    }
                    next.selected_reasoning_effort = effort;
                }
                SettingsModelPickerIntent::RetryProviders { .. } => {
                    if session.provider_pending {
                        return self.navigation_outcome(ClientTransitionOutcome::Noop);
                    }
                    next.selector.mark_providers_loading();
                    if session.runtime_demand {
                        next.selector.sync_cli_runtime_snapshot(runtimes);
                    }
                    session.provider_generation += 1;
                    session.provider_pending = true;
                    work.push(Work {
                        id: id.clone(),
                        owner: next.owner_generation,
                        generation: session.provider_generation,
                        key: ProviderCollectionKey::catalog(next.workspace_id.clone()),
                        models: false,
                    });
                }
                SettingsModelPickerIntent::RetryModels { .. } => {
                    if next.selector.loading_models() {
                        return self.navigation_outcome(ClientTransitionOutcome::Noop);
                    }
                    if let Some(provider) = next.selector.preload_selected_provider_models() {
                        session.model_generation += 1;
                        session.model_pending = true;
                        work.push(model_work(session, provider));
                    }
                }
                SettingsModelPickerIntent::Close { .. } => {
                    next.closed = true;
                    next.selector = ProviderModelSelectorState::new(None, None);
                    next.selected_reasoning_effort = None;
                    if session.runtime_demand {
                        released = Some(next.workspace_id.clone());
                        session.runtime_demand = false;
                    }
                }
                SettingsModelPickerIntent::Open { .. } => unreachable!(),
            }
            session.value = Arc::new(next);
        }
        let value = store.sessions[&id].value.clone();
        let sender = store.sender.clone();
        let transition = self.publish_settings_value(scope(&id), (*value).clone());
        if value.closed {
            store.sessions.remove(&id);
        }
        drop(store);
        drop(navigation_guard);
        drop(identity_guard);
        if let Some(workspace_id) = released {
            self.provider_runtime_intent(ProviderRuntimeIntent::Release { workspace_id });
        }
        if let Some(workspace_id) = observe {
            self.provider_runtime_intent(ProviderRuntimeIntent::Observe { workspace_id });
        }
        for work in work {
            if let Some(sender) = &sender {
                if sender.try_send(work.clone()).is_err() {
                    self.complete_settings_model_picker(work, Err("model_picker_busy".into()));
                }
            }
        }
        transition
    }
    pub(crate) fn settings_model_picker_subscription_changed(
        &self,
        scope: &ClientScope,
        added: bool,
    ) {
        let ClientScope::SettingsModelPicker { picker_id } = scope else {
            return;
        };
        let mut store = self
            .settings_model_pickers
            .lock()
            .expect("settings model picker poisoned");
        if added {
            *store.subscriptions.entry(picker_id.clone()).or_default() += 1;
            return;
        }
        let Some(count) = store.subscriptions.get_mut(picker_id) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count > 0 {
            return;
        }
        store.subscriptions.remove(picker_id);
        let close = store
            .sessions
            .get(picker_id)
            .map(|session| session.value.owner_generation);
        drop(store);
        if let Some(expected_owner) = close {
            self.settings_model_picker_intent(SettingsModelPickerIntent::Close {
                picker_id: picker_id.clone(),
                expected_owner,
            });
        }
    }
    pub(crate) fn settings_model_picker_demand_changed(
        &self,
        scope: &ClientScope,
        demand: ClientDemand,
    ) {
        if demand != ClientDemand::Suspended {
            return;
        }
        if let ClientScope::SettingsModelPicker { picker_id } = scope {
            if let Some(value) = self.settings_model_picker(picker_id) {
                self.settings_model_picker_intent(SettingsModelPickerIntent::Close {
                    picker_id: picker_id.clone(),
                    expected_owner: value.owner_generation,
                });
            }
        }
    }
    fn settings_model_picker_work_current(&self, work: &Work) -> bool {
        if self.is_stopped() {
            return false;
        }
        let authorization = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned")
            .authorization_epoch();
        let navigation = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let workspace = navigation.navigation.workspace_id();
        self.settings_model_pickers
            .lock()
            .expect("settings model picker poisoned")
            .sessions
            .get(&work.id)
            .is_some_and(|s| {
                s.authorization == authorization
                    && s.navigation_revision == navigation.navigation_revision
                    && workspace.as_deref() == Some(&s.value.workspace_id)
                    && s.value.owner_generation == work.owner
                    && !s.value.closed
                    && if work.models {
                        s.model_pending && s.model_generation == work.generation
                    } else {
                        s.provider_pending && s.provider_generation == work.generation
                    }
            })
    }
    fn complete_settings_model_picker(
        &self,
        work: Work,
        result: Result<Arc<crate::providers::store::ProviderCollectionPublication>, String>,
    ) {
        if !self.settings_model_picker_work_current(&work)
            || result.as_ref().is_ok_and(|value| value.key() != &work.key)
        {
            return;
        }
        let identity_guard = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        let navigation_guard = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if navigation_guard.navigation.workspace_id() != Some(work.key.workspace_id()) {
            return;
        }
        let mut store = self
            .settings_model_pickers
            .lock()
            .expect("settings model picker poisoned");
        let Some(session) = store.sessions.get_mut(&work.id) else {
            return;
        };
        if session.navigation_revision != navigation_guard.navigation_revision
            || session.authorization != identity_guard.authorization_epoch()
            || session.value.owner_generation != work.owner
            || (if work.models {
                session.model_generation
            } else {
                session.provider_generation
            }) != work.generation
        {
            return;
        }
        if work.models {
            if !session.model_pending {
                return;
            }
            session.model_pending = false;
        } else {
            if !session.provider_pending {
                return;
            }
            session.provider_pending = false;
        }
        let mut next = (*session.value).clone();
        if work.models {
            match result.and_then(|p| {
                p.models_response()
                    .map_err(|_| "provider_models_unavailable".into())
            }) {
                Ok(response) => {
                    next.selector.apply_provider_models_success(response);
                }
                Err(error) => {
                    if let Some(provider) = next.selector.selected_provider().map(str::to_owned) {
                        next.selector.apply_provider_models_error(&provider, error);
                    }
                }
            }
            let valid = next
                .selector
                .models()
                .iter()
                .find(|row| Some(row.id.as_str()) == next.selector.selected_model())
                .is_some_and(|row| {
                    presentation::reasoning_effort_rows_for_model(
                        row,
                        next.selected_reasoning_effort.as_deref(),
                    )
                    .iter()
                    .any(|row| row.selected)
                });
            if !valid {
                next.selected_reasoning_effort = None;
            }
        } else {
            match result.and_then(|p| {
                p.catalog_response()
                    .map_err(|_| "provider_catalog_unavailable".into())
            }) {
                Ok(response) => next.selector.apply_provider_list_success(response),
                Err(error) => next.selector.apply_provider_list_error(error),
            }
        }
        session.value = Arc::new(next);
        self.publish_settings_value(scope(&work.id), (*session.value).clone());
    }
    fn reconcile_settings_model_pickers(&self) {
        let authorization = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned")
            .authorization_epoch();
        let workspace = self.settings_workspace();
        let navigation_revision = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned")
            .navigation_revision;
        let sessions = self
            .settings_model_pickers
            .lock()
            .expect("settings model picker poisoned")
            .sessions
            .values()
            .map(|s| {
                (
                    s.value.clone(),
                    s.authorization,
                    s.runtime_demand,
                    s.navigation_revision,
                )
            })
            .collect::<Vec<_>>();
        for (value, epoch, runtime, opened_revision) in sessions {
            if navigation_revision != opened_revision
                || epoch != authorization
                || workspace.as_deref() != Some(&value.workspace_id)
            {
                self.settings_model_picker_intent(SettingsModelPickerIntent::Close {
                    picker_id: value.picker_id.clone(),
                    expected_owner: value.owner_generation,
                });
            } else if runtime {
                let runtimes = self
                    .provider_runtime_snapshot(&value.workspace_id)
                    .map(|p| {
                        p.runtimes()
                            .iter()
                            .map(|r| r.runtime().clone())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let identity_guard = self
                    .identity_authorization
                    .lock()
                    .expect("identity owner poisoned");
                let navigation_guard = self
                    .thread_registry
                    .lock()
                    .expect("thread registry poisoned");
                if navigation_guard.navigation_revision != opened_revision
                    || identity_guard.authorization_epoch() != epoch
                    || navigation_guard.navigation.workspace_id() != Some(&value.workspace_id)
                {
                    continue;
                }
                let mut store = self
                    .settings_model_pickers
                    .lock()
                    .expect("settings model picker poisoned");
                if let Some(session) = store.sessions.get_mut(&value.picker_id).filter(|s| {
                    s.value.owner_generation == value.owner_generation
                        && s.value.selector.cli_runtimes() != runtimes.as_slice()
                }) {
                    let mut next = (*session.value).clone();
                    let was_ready = provider_ready_for_model_selector(
                        next.selector.selected_provider(),
                        next.selector.cli_runtimes(),
                    );
                    next.selector.sync_cli_runtime_snapshot(runtimes);
                    let ready = provider_ready_for_model_selector(
                        next.selector.selected_provider(),
                        next.selector.cli_runtimes(),
                    );
                    let mut work = None;
                    if was_ready != ready {
                        session.model_generation += 1;
                        let provider = next.selector.preload_selected_provider_models();
                        session.model_pending = provider.is_some();
                        work = provider.map(|provider| model_work(session, provider));
                    }
                    session.value = Arc::new(next);
                    self.publish_settings_value(scope(&value.picker_id), (*session.value).clone());
                    let sender = store.sender.clone();
                    drop(store);
                    drop(navigation_guard);
                    drop(identity_guard);
                    if let (Some(work), Some(sender)) = (work, sender) {
                        if sender.try_send(work.clone()).is_err() {
                            self.complete_settings_model_picker(
                                work,
                                Err("model_picker_busy".into()),
                            );
                        }
                    }
                }
            }
        }
    }
    pub(crate) fn start_settings_model_picker_controller(self: &Arc<Self>) {
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<Work>(64);
        let weak = Arc::downgrade(self);
        let mut changed = self.watch_publications();
        let task = std::thread::Builder::new().name("client-settings-model-picker".into()).spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("settings model picker runtime");
            runtime.block_on(async {
                loop {
                    let work = tokio::select! {
                        result = changed.changed() => {
                            if result.is_err() { return; }
                            let Some(core) = weak.upgrade().filter(|core| !core.is_stopped()) else { return; };
                            core.reconcile_settings_model_pickers(); continue;
                        }
                        work = receiver.recv() => { let Some(work) = work else { return; }; work }
                    };
                    let Some(core) = weak.upgrade() else { return; };
                    if !core.settings_model_picker_work_current(&work) { continue; }
                    let read = core.read_provider_collection(work.key.clone(), true); drop(core);
                    let result = read.and_then(|read| read.wait_while(|| weak.upgrade().is_some_and(|core| core.settings_model_picker_work_current(&work)))).map_err(|_| "provider_catalog_unavailable".into());
                    if let Some(core) = weak.upgrade() { core.complete_settings_model_picker(work, result); }
                }
            });
        }).expect("settings model picker worker");
        let mut store = self
            .settings_model_pickers
            .lock()
            .expect("settings model picker poisoned");
        store.sender = Some(sender);
        store.task = Some(task);
    }
}
fn model_work(session: &Session, provider: String) -> Work {
    let kind = match session.mode {
        ProviderModelSelectorMode::Embeddings => ProviderModelKind::Embeddings,
        ProviderModelSelectorMode::Transcription => ProviderModelKind::Transcription,
        _ => ProviderModelKind::Chat,
    };
    Work {
        id: session.value.picker_id.clone(),
        owner: session.value.owner_generation,
        generation: session.model_generation,
        key: ProviderCollectionKey::models(session.value.workspace_id.clone(), provider, kind),
        models: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{navigation::NavigationIntent, providers::store::ProviderCollectionPublication};
    fn fixture() -> (Arc<ClientCore>, tokio::sync::mpsc::Receiver<Work>) {
        let core = crate::catalog_test_support::settings_model_picker_client();
        let (sender, receiver) = tokio::sync::mpsc::channel(64);
        core.settings_model_pickers.lock().unwrap().sender = Some(sender);
        (core, receiver)
    }
    fn open(core: &ClientCore, mode: ProviderModelSelectorMode) -> u64 {
        core.settings_model_picker_intent(SettingsModelPickerIntent::Open {
            picker_id: "picker".into(),
            workspace_id: "workspace".into(),
            mode,
            selection: ModelSelectorSelection {
                provider: Some("openai".into()),
                model: Some("baseline".into()),
                selected_reasoning_effort: None,
            },
        });
        core.settings_model_picker("picker")
            .unwrap()
            .owner_generation
    }
    fn complete(core: &ClientCore, work: Work) {
        let providers = ["openai", "anthropic"].into_iter().map(|name| serde_json::from_value(serde_json::json!({"name":name,"capabilities":{"embeddings":true},"api_key_configured":true})).unwrap()).collect();
        let models = ["baseline", "next"]
            .into_iter()
            .map(|id| {
                serde_json::from_value(
                    serde_json::json!({"id":id,"provider":"openai","limits":{},"capabilities":{}}),
                )
                .unwrap()
            })
            .collect();
        core.complete_settings_model_picker(
            work.clone(),
            Ok(ProviderCollectionPublication::for_test(
                work.key, providers, models,
            )),
        );
    }
    #[test]
    fn requests_are_coalesced_retried_and_late_provider_completions_do_not_replace_selection() {
        let (core, mut requests) = fixture();
        let owner = open(&core, ProviderModelSelectorMode::Embeddings);
        let catalog = requests.try_recv().unwrap();
        let models = requests.try_recv().unwrap();
        core.settings_model_picker_intent(SettingsModelPickerIntent::RetryProviders {
            picker_id: "picker".into(),
            expected_owner: owner,
        });
        assert!(requests.try_recv().is_err());
        complete(&core, catalog.clone());
        let before = core.settings_model_picker("picker").unwrap();
        complete(&core, catalog);
        assert!(Arc::ptr_eq(
            &before,
            &core.settings_model_picker("picker").unwrap()
        ));
        core.settings_model_picker_intent(SettingsModelPickerIntent::SelectProvider {
            picker_id: "picker".into(),
            expected_owner: owner,
            provider: "anthropic".into(),
        });
        let current = requests.try_recv().unwrap();
        let before = core.settings_model_picker("picker").unwrap();
        complete(&core, models);
        assert!(Arc::ptr_eq(
            &before,
            &core.settings_model_picker("picker").unwrap()
        ));
        core.complete_settings_model_picker(current.clone(), Err("offline".into()));
        assert_eq!(
            core.settings_model_picker("picker")
                .unwrap()
                .selector
                .error(),
            Some("offline")
        );
        core.settings_model_picker_intent(SettingsModelPickerIntent::RetryModels {
            picker_id: "picker".into(),
            expected_owner: owner,
        });
        let retry = requests.try_recv().unwrap();
        assert!(retry.generation > current.generation);
        core.complete_settings_model_picker(current, Err("old failure".into()));
        assert!(
            core.settings_model_picker("picker")
                .unwrap()
                .selector
                .loading_models()
        );
        complete(&core, retry);
        assert!(
            !core
                .settings_model_picker("picker")
                .unwrap()
                .selector
                .loading_models()
        );
    }
    #[test]
    fn wrong_collection_owner_and_duplicate_edits_do_not_publish() {
        let (core, mut requests) = fixture();
        let owner = open(&core, ProviderModelSelectorMode::Embeddings);
        complete(&core, requests.try_recv().unwrap());
        let work = requests.try_recv().unwrap();
        let before = core.settings_model_picker("picker").unwrap();
        core.complete_settings_model_picker(
            work.clone(),
            Ok(ProviderCollectionPublication::for_test(
                ProviderCollectionKey::models("other", "openai", ProviderModelKind::Embeddings),
                vec![],
                vec![],
            )),
        );
        assert!(Arc::ptr_eq(
            &before,
            &core.settings_model_picker("picker").unwrap()
        ));
        complete(&core, work);
        let before = core.settings_model_picker("picker").unwrap();
        for expected_owner in [owner, owner + 1] {
            core.settings_model_picker_intent(SettingsModelPickerIntent::SelectModel {
                picker_id: "picker".into(),
                expected_owner,
                model: "baseline".into(),
            });
            assert!(Arc::ptr_eq(
                &before,
                &core.settings_model_picker("picker").unwrap()
            ));
        }
        core.settings_model_picker_intent(SettingsModelPickerIntent::SelectModel {
            picker_id: "picker".into(),
            expected_owner: owner,
            model: "next".into(),
        });
        assert_eq!(
            core.settings_model_picker_selection("picker", owner)
                .unwrap()
                .model
                .as_deref(),
            Some("next")
        );
    }
    #[test]
    fn last_subscription_and_scope_replacement_cancel_work_without_touching_other_pages() {
        let (core, mut requests) = fixture();
        let owner = open(&core, ProviderModelSelectorMode::Embeddings);
        let work = requests.try_recv().unwrap();
        let subscription = core.subscribe(scope("picker"), std::num::NonZeroUsize::new(4).unwrap());
        drop(subscription);
        assert!(core.settings_model_picker("picker").unwrap().closed);
        assert!(!core.settings_model_picker_work_current(&work));
        assert!(
            core.settings_model_picker_selection("picker", owner)
                .is_none()
        );
        let next = open(&core, ProviderModelSelectorMode::Embeddings);
        assert!(next > owner);
        let before = core.settings_model_picker("picker").unwrap();
        core.settings_model_picker_intent(SettingsModelPickerIntent::Close {
            picker_id: "picker".into(),
            expected_owner: owner,
        });
        complete(&core, work);
        assert!(Arc::ptr_eq(
            &before,
            &core.settings_model_picker("picker").unwrap()
        ));
        core.navigate(
            NavigationIntent::SelectWorkspace {
                workspace_id: Some("other".into()),
            },
            None,
        );
        assert!(
            core.settings_model_picker_selection("picker", next)
                .is_none()
        );
        core.reconcile_settings_model_pickers();
        assert!(core.settings_model_picker("picker").unwrap().closed);
    }
    #[test]
    fn subscriptions_created_before_open_and_reentry_have_one_shared_lifecycle() {
        let (core, _) = fixture();
        let first = core.subscribe(scope("picker"), std::num::NonZeroUsize::new(4).unwrap());
        let second = core.subscribe(scope("picker"), std::num::NonZeroUsize::new(4).unwrap());
        open(&core, ProviderModelSelectorMode::Embeddings);
        open(&core, ProviderModelSelectorMode::Embeddings);
        drop(first);
        assert!(!core.settings_model_picker("picker").unwrap().closed);
        drop(second);
        assert!(core.settings_model_picker("picker").unwrap().closed);
    }
    #[test]
    fn rapid_workspace_round_trip_still_invalidates_the_old_dialog() {
        let (core, mut requests) = fixture();
        let owner = open(&core, ProviderModelSelectorMode::Embeddings);
        let work = requests.try_recv().unwrap();
        for workspace in ["other", "workspace"] {
            core.navigate(
                NavigationIntent::SelectWorkspace {
                    workspace_id: Some(workspace.into()),
                },
                None,
            );
        }
        assert!(
            core.settings_model_picker_selection("picker", owner)
                .is_none()
        );
        assert!(!core.settings_model_picker_work_current(&work));
        let before = core.settings_model_picker("picker").unwrap();
        complete(&core, work);
        assert!(Arc::ptr_eq(
            &before,
            &core.settings_model_picker("picker").unwrap()
        ));
        core.reconcile_settings_model_pickers();
        assert!(core.settings_model_picker("picker").unwrap().closed);
    }
    #[test]
    fn policy_loss_cancels_without_requiring_a_gateway_disconnect() {
        let (core, mut requests) = fixture();
        let owner = open(&core, ProviderModelSelectorMode::Embeddings);
        let work = requests.try_recv().unwrap();
        core.invalidate_authorization_revision(3);
        assert!(
            core.settings_model_picker_selection("picker", owner)
                .is_none()
        );
        assert!(!core.settings_model_picker_work_current(&work));
        core.reconcile_settings_model_pickers();
        assert!(core.settings_model_picker("picker").unwrap().closed);
    }
    #[test]
    fn runtime_becoming_ready_loads_selected_cli_models_once_and_unready_cancels_them() {
        let (core, mut requests) = fixture();
        core.settings_model_picker_intent(SettingsModelPickerIntent::Open {
            picker_id: "picker".into(),
            workspace_id: "workspace".into(),
            mode: ProviderModelSelectorMode::Chat,
            selection: ModelSelectorSelection {
                provider: Some(crate::providers::list::cli_runtime_provider_key("codex")),
                model: None,
                selected_reasoning_effort: None,
            },
        });
        requests.try_recv().unwrap();
        assert!(requests.try_recv().is_err());
        let runtime = pioneer_protocol::RuntimeSummary {
            runtime_id: "codex".into(),
            kind: pioneer_protocol::CLIAgentRuntimeKind::Codex,
            display_name: "Codex".into(),
            enabled: true,
            status: pioneer_protocol::RuntimeStatus::Ready,
            capabilities: pioneer_protocol::RuntimeCapabilities {
                supports_threads: true,
                supports_model_list: true,
                ..Default::default()
            },
            account: None,
            version: None,
            binary_path: None,
            home_path: None,
            shadow_home_path: None,
            proxy_url: None,
            debug_native_events_enabled: false,
            models_refreshed_at_unix_ms: None,
            diagnostics: vec![],
            recent_stderr: vec![],
        };
        let request = core.provider_runtime_request_for_test("workspace").unwrap();
        core.complete_provider_runtime_for_test(
            request,
            Ok(pioneer_protocol::CLIRuntimeListResponse {
                revision: 1,
                runtimes: vec![runtime],
            }),
        );
        core.reconcile_settings_model_pickers();
        let models = requests.try_recv().unwrap();
        assert!(models.models);
        core.reconcile_settings_model_pickers();
        assert!(requests.try_recv().is_err());
        core.provider_runtime_intent(ProviderRuntimeIntent::Refresh {
            workspace_id: "workspace".into(),
        });
        let request = core.provider_runtime_request_for_test("workspace").unwrap();
        core.complete_provider_runtime_for_test(
            request,
            Ok(pioneer_protocol::CLIRuntimeListResponse {
                revision: 2,
                runtimes: vec![],
            }),
        );
        core.reconcile_settings_model_pickers();
        assert!(!core.settings_model_picker_work_current(&models));
        assert!(
            !core
                .settings_model_picker("picker")
                .unwrap()
                .selector
                .loading_models()
        );
        let before = core.settings_model_picker("picker").unwrap();
        complete(&core, models);
        assert!(Arc::ptr_eq(
            &before,
            &core.settings_model_picker("picker").unwrap()
        ));
    }
    #[test]
    fn purpose_maps_to_existing_transport_and_access_loss_fails_closed() {
        for (mode, kind) in [
            (ProviderModelSelectorMode::Chat, ProviderModelKind::Chat),
            (
                ProviderModelSelectorMode::SelfImprovement,
                ProviderModelKind::Chat,
            ),
            (
                ProviderModelSelectorMode::Embeddings,
                ProviderModelKind::Embeddings,
            ),
            (
                ProviderModelSelectorMode::Transcription,
                ProviderModelKind::Transcription,
            ),
        ] {
            let (core, mut requests) = fixture();
            let owner = open(&core, mode);
            requests.try_recv().unwrap();
            let work = requests.try_recv().unwrap();
            assert_eq!(
                work.key,
                ProviderCollectionKey::models("workspace", "openai", kind)
            );
            core.clear_authorization_projections();
            assert!(
                core.settings_model_picker_selection("picker", owner)
                    .is_none()
            );
            assert!(!core.settings_model_picker_work_current(&work));
            core.reconcile_settings_model_pickers();
            let before = core.settings_model_picker("picker").unwrap();
            complete(&core, work);
            assert!(Arc::ptr_eq(
                &before,
                &core.settings_model_picker("picker").unwrap()
            ));
            assert!(core.settings_model_picker("picker").unwrap().closed);
        }
    }
}
