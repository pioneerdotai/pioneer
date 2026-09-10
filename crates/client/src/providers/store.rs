//! Scoped catalog and model descriptor ownership with coalesced, cancellable reads.
use super::list;
use crate::core::*;
use pioneer_protocol::{
    ProviderListModelsResponse, ProviderListResponse, ProviderModelInfo, ProviderSummary,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Weak, mpsc},
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ProviderModelKind {
    Chat,
    Embeddings,
    Transcription,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderCollection {
    Catalog,
    Models {
        provider: String,
        purpose: ProviderModelKind,
    },
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct ProviderCollectionKey {
    workspace_id: String,
    collection: ProviderCollection,
}
impl ProviderCollectionKey {
    pub fn catalog(workspace_id: impl Into<String>) -> Self {
        Self {
            workspace_id: workspace_id.into(),
            collection: ProviderCollection::Catalog,
        }
    }
    pub fn models(
        workspace_id: impl Into<String>,
        provider: impl Into<String>,
        purpose: ProviderModelKind,
    ) -> Self {
        Self {
            workspace_id: workspace_id.into(),
            collection: ProviderCollection::Models {
                provider: provider.into(),
                purpose,
            },
        }
    }
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }
    pub fn collection(&self) -> &ProviderCollection {
        &self.collection
    }
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderCollectionIntent {
    Observe { key: ProviderCollectionKey },
    Release { key: ProviderCollectionKey },
    Refresh { key: ProviderCollectionKey },
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderLoadState {
    #[default]
    Idle,
    Loading,
    Ready,
    Failed,
    Forbidden,
    Cancelled,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize)]
pub struct ProviderCatalogRow {
    id: String,
    revision: u64,
    provider: ProviderSummary,
}
impl ProviderCatalogRow {
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn provider(&self) -> &ProviderSummary {
        &self.provider
    }
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize)]
pub struct ProviderModelRow {
    id: String,
    revision: u64,
    model: ProviderModelInfo,
}
impl ProviderModelRow {
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn model(&self) -> &ProviderModelInfo {
        &self.model
    }
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize)]
pub struct ProviderCollectionPublication {
    key: ProviderCollectionKey,
    revision: u64,
    request: ProviderLoadState,
    providers: Vec<Arc<ProviderCatalogRow>>,
    models: Vec<Arc<ProviderModelRow>>,
    #[serde(skip)]
    runtime_models: Option<Arc<pioneer_protocol::CLIRuntimeListModelsResponse>>,
}
impl ProviderCollectionPublication {
    pub fn key(&self) -> &ProviderCollectionKey {
        &self.key
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn request(&self) -> ProviderLoadState {
        self.request
    }
    pub fn providers(&self) -> &[Arc<ProviderCatalogRow>] {
        &self.providers
    }
    pub fn models(&self) -> &[Arc<ProviderModelRow>] {
        &self.models
    }
    pub fn catalog_response(&self) -> anyhow::Result<ProviderListResponse> {
        anyhow::ensure!(
            self.request == ProviderLoadState::Ready
                && matches!(self.key.collection, ProviderCollection::Catalog),
            "provider_catalog_unavailable"
        );
        Ok(ProviderListResponse {
            providers: self
                .providers
                .iter()
                .map(|row| row.provider.clone())
                .collect(),
        })
    }
    pub fn runtime_models_response(
        &self,
    ) -> anyhow::Result<pioneer_protocol::CLIRuntimeListModelsResponse> {
        anyhow::ensure!(
            self.request == ProviderLoadState::Ready,
            "provider_models_unavailable"
        );
        self.runtime_models
            .as_deref()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("provider_models_unavailable"))
    }
    pub fn models_response(&self) -> anyhow::Result<ProviderListModelsResponse> {
        anyhow::ensure!(
            self.request == ProviderLoadState::Ready,
            "provider_models_unavailable"
        );
        let ProviderCollection::Models { provider, .. } = &self.key.collection else {
            anyhow::bail!("provider_collection_mismatch")
        };
        Ok(ProviderListModelsResponse {
            provider: provider.clone(),
            models: self.models.iter().map(|row| row.model.clone()).collect(),
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Request {
    key: ProviderCollectionKey,
    generation: u64,
    epoch: (u64, u64, Option<u64>),
}
struct Collection {
    publication: Arc<ProviderCollectionPublication>,
    request: Option<Request>,
    demand: usize,
    waiters: BTreeMap<u64, mpsc::SyncSender<Arc<ProviderCollectionPublication>>>,
}
#[derive(Default)]
pub(crate) struct ProviderStore {
    collections: BTreeMap<ProviderCollectionKey, Collection>,
    generation: u64,
    waiter_generation: u64,
    sender: Option<mpsc::SyncSender<Request>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl ProviderStore {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
        self.collections.clear();
    }
}
impl Drop for ProviderStore {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}
/// A background consumer's read lease. Dropping it withdraws only that demand.
/// Waiting never retains Client and must not run on a UI thread.
pub struct ProviderRead {
    core: Weak<ClientCore>,
    key: ProviderCollectionKey,
    identity: u64,
    receiver: mpsc::Receiver<Arc<ProviderCollectionPublication>>,
}
impl ProviderRead {
    pub fn wait(self) -> anyhow::Result<Arc<ProviderCollectionPublication>> {
        self.wait_while(|| true)
    }
    pub fn wait_while(
        self,
        current: impl Fn() -> bool,
    ) -> anyhow::Result<Arc<ProviderCollectionPublication>> {
        loop {
            anyhow::ensure!(current(), "provider_read_cancelled");
            match self
                .receiver
                .recv_timeout(std::time::Duration::from_millis(100))
            {
                Ok(publication) => return Ok(publication),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(_) => anyhow::bail!("provider_read_cancelled"),
            }
        }
    }
}
impl Drop for ProviderRead {
    fn drop(&mut self) {
        if let Some(core) = self.core.upgrade() {
            core.release_provider_read(&self.key, self.identity);
        }
    }
}
enum Response {
    Catalog(ProviderListResponse),
    Models(ProviderListModelsResponse),
    RuntimeModels(pioneer_protocol::CLIRuntimeListModelsResponse),
}
fn response_matches(key: &ProviderCollectionKey, response: &Response) -> bool {
    match (&key.collection, response) {
        (ProviderCollection::Catalog, Response::Catalog(_)) => true,
        (ProviderCollection::Models { provider, .. }, Response::Models(response)) => {
            &response.provider == provider
        }
        (ProviderCollection::Models { provider, .. }, Response::RuntimeModels(response)) => {
            list::runtime_id_from_cli_runtime_provider_key(provider)
                == Some(response.runtime_id.as_str())
        }
        _ => false,
    }
}
fn equal<T: serde::Serialize>(a: &T, b: &T) -> bool {
    serde_json::to_value(a).expect("provider serialization")
        == serde_json::to_value(b).expect("provider serialization")
}
impl ClientCore {
    pub fn provider_collection_snapshot(
        &self,
        key: &ProviderCollectionKey,
    ) -> Option<Arc<ProviderCollectionPublication>> {
        self.snapshot(&ClientScope::ProviderCollection { key: key.clone() })
            .and_then(|p| p.snapshot().payload())
    }
    fn provider_collection_allowed(&self, key: &ProviderCollectionKey) -> bool {
        let Some(snapshot) = self
            .authorization_snapshot(Some(&key.workspace_id), None)
            .or_else(|| self.authorization_snapshot(None, None))
        else {
            return false;
        };
        let capability = crate::authorization::principal_presentation_capabilities(&snapshot);
        capability.can_manage_capabilities
            || snapshot
                .workspace
                .as_ref()
                .is_some_and(|workspace| match &key.collection {
                    ProviderCollection::Models { provider, .. }
                        if list::runtime_id_from_cli_runtime_provider_key(provider).is_some() =>
                    {
                        workspace.capabilities.can_use_cli_runtimes
                    }
                    _ => workspace.capabilities.can_use_providers,
                })
    }
    fn publish_provider_collection(
        &self,
        state: &mut Collection,
        mut next: ProviderCollectionPublication,
    ) -> ClientTransition {
        next.revision = state.publication.revision;
        if equal(&next, state.publication.as_ref())
            && next.runtime_models == state.publication.runtime_models
        {
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        }
        let scope = ClientScope::ProviderCollection {
            key: next.key.clone(),
        };
        next.revision = next
            .revision
            .max(
                self.snapshot(&scope)
                    .map_or(0, |p| p.revisions().scoped().get()),
            )
            .checked_add(1)
            .expect("provider revision exhausted");
        state.publication = Arc::new(next);
        let transition = self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(state.publication.revision),
            state.publication.clone(),
            vec![],
        );
        if state.publication.request != ProviderLoadState::Loading {
            for (_, waiter) in std::mem::take(&mut state.waiters) {
                let _ = waiter.try_send(state.publication.clone());
            }
        }
        transition
    }
    fn sync_provider_startup(&self, key: &ProviderCollectionKey) {
        use crate::gateway::session_controller::{StartupStage, StartupStageState};
        if !matches!(key.collection, ProviderCollection::Catalog)
            || self.navigation_snapshot().workspace_id() != Some(key.workspace_id.as_str())
        {
            return;
        }
        let Some(publication) = self.provider_collection_snapshot(key) else {
            return;
        };
        let startup = self
            .gateway_session()
            .startup
            .stages
            .get(&StartupStage::Provider)
            .copied();
        if startup.is_none() || startup == Some(StartupStageState::Pending) {
            let stage = match publication.request {
                ProviderLoadState::Loading => StartupStageState::Pending,
                ProviderLoadState::Ready => StartupStageState::Succeeded,
                ProviderLoadState::Failed | ProviderLoadState::Forbidden => {
                    StartupStageState::Failed
                }
                _ => StartupStageState::Cancelled,
            };
            self.update_startup_stage(StartupStage::Provider, stage);
        }
    }
    pub fn provider_collection_intent(&self, intent: ProviderCollectionIntent) -> ClientTransition {
        let key = match &intent {
            ProviderCollectionIntent::Observe { key }
            | ProviderCollectionIntent::Release { key }
            | ProviderCollectionIntent::Refresh { key } => key.clone(),
        };
        let result = self.reduce_provider_collection_intent(intent);
        self.sync_provider_startup(&key);
        result
    }
    fn reduce_provider_collection_intent(
        &self,
        intent: ProviderCollectionIntent,
    ) -> ClientTransition {
        let (key, observe, release) = match intent {
            ProviderCollectionIntent::Observe { key } => (key, true, false),
            ProviderCollectionIntent::Release { key } => (key, false, true),
            ProviderCollectionIntent::Refresh { key } => (key, false, false),
        };
        if self.is_stopped() || key.workspace_id.trim().is_empty() {
            return self.reject_intent();
        }
        let allowed = self.provider_collection_allowed(&key);
        let epoch = self.provider_runtime_epoch();
        let mut owner = self.provider_store.lock().expect("provider store poisoned");
        if release && !owner.collections.contains_key(&key) {
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        }
        let state = owner
            .collections
            .entry(key.clone())
            .or_insert_with(|| Collection {
                publication: Arc::new(ProviderCollectionPublication {
                    key: key.clone(),
                    revision: 0,
                    request: ProviderLoadState::Idle,
                    providers: vec![],
                    models: vec![],
                    runtime_models: None,
                }),
                request: None,
                demand: 0,
                waiters: BTreeMap::new(),
            });
        if release {
            state.demand = state.demand.saturating_sub(1);
            return self.cancel_unused_provider_collection(state);
        }
        if observe {
            state.demand = state
                .demand
                .checked_add(1)
                .expect("provider demand exhausted");
        }
        if !allowed {
            state.request = None;
            let mut next = (*state.publication).clone();
            next.request = ProviderLoadState::Forbidden;
            next.providers.clear();
            next.models.clear();
            next.runtime_models = None;
            return self.publish_provider_collection(state, next);
        }
        if state.request.is_some()
            || observe
                && matches!(
                    state.publication.request,
                    ProviderLoadState::Ready | ProviderLoadState::Failed
                )
        {
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        }
        owner.generation = owner
            .generation
            .checked_add(1)
            .expect("provider request generation exhausted");
        let request = Request {
            key: key.clone(),
            generation: owner.generation,
            epoch,
        };
        let queued = owner
            .sender
            .as_ref()
            .is_some_and(|sender| sender.try_send(request.clone()).is_ok());
        let state = owner
            .collections
            .get_mut(&key)
            .expect("registered collection");
        state.request = queued.then_some(request);
        let mut next = (*state.publication).clone();
        next.request = if queued {
            ProviderLoadState::Loading
        } else {
            ProviderLoadState::Failed
        };
        self.publish_provider_collection(state, next)
    }
    fn cancel_unused_provider_collection(&self, state: &mut Collection) -> ClientTransition {
        if state.demand > 0 || !state.waiters.is_empty() || state.request.is_none() {
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        }
        state.request = None;
        let mut next = (*state.publication).clone();
        next.request = ProviderLoadState::Cancelled;
        self.publish_provider_collection(state, next)
    }
    pub fn read_provider_collection(
        self: &Arc<Self>,
        key: ProviderCollectionKey,
        retry: bool,
    ) -> anyhow::Result<ProviderRead> {
        self.provider_collection_intent(ProviderCollectionIntent::Observe { key: key.clone() });
        if retry {
            self.provider_collection_intent(ProviderCollectionIntent::Refresh { key: key.clone() });
        }
        let (sender, receiver) = mpsc::sync_channel(1);
        let mut owner = self.provider_store.lock().expect("provider store poisoned");
        owner.waiter_generation = owner
            .waiter_generation
            .checked_add(1)
            .expect("provider read identity exhausted");
        let identity = owner.waiter_generation;
        let Some(state) = owner.collections.get_mut(&key) else {
            anyhow::bail!("provider_read_unavailable")
        };
        state.demand = state.demand.saturating_sub(1);
        if state.waiters.len() >= 64 {
            self.cancel_unused_provider_collection(state);
            anyhow::bail!("provider_read_overloaded");
        }
        if state.publication.request == ProviderLoadState::Loading {
            state.waiters.insert(identity, sender);
        } else {
            let _ = sender.try_send(state.publication.clone());
        }
        Ok(ProviderRead {
            core: Arc::downgrade(self),
            key,
            identity,
            receiver,
        })
    }
    fn release_provider_read(&self, key: &ProviderCollectionKey, identity: u64) {
        let mut owner = self.provider_store.lock().expect("provider store poisoned");
        if let Some(state) = owner.collections.get_mut(key) {
            state.waiters.remove(&identity);
            self.cancel_unused_provider_collection(state);
        }
    }
    fn provider_collection_request_current(&self, request: &Request) -> bool {
        !self.is_stopped()
            && self.provider_runtime_epoch() == request.epoch
            && self
                .provider_store
                .lock()
                .expect("provider store poisoned")
                .collections
                .get(&request.key)
                .is_some_and(|state| state.request.as_ref() == Some(request))
    }
    fn complete_provider_collection(&self, request: Request, result: anyhow::Result<Response>) {
        if self.is_stopped()
            || self.provider_runtime_epoch() != request.epoch
            || result
                .as_ref()
                .is_ok_and(|response| !response_matches(&request.key, response))
        {
            return;
        }
        let allowed = self.provider_collection_allowed(&request.key);
        let mut owner = self.provider_store.lock().expect("provider store poisoned");
        let Some(state) = owner
            .collections
            .get_mut(&request.key)
            .filter(|state| state.request.as_ref() == Some(&request))
        else {
            return;
        };
        state.request = None;
        let mut next = (*state.publication).clone();
        let result = match result {
            Ok(Response::RuntimeModels(mut response)) => {
                let expected = match &request.key.collection {
                    ProviderCollection::Models { provider, .. } => {
                        list::runtime_id_from_cli_runtime_provider_key(provider)
                    }
                    _ => None,
                };
                if expected != Some(response.runtime_id.as_str()) {
                    return;
                }
                for diagnostic in &mut response.diagnostics {
                    diagnostic.message =
                        pioneer_protocol::sanitize_runtime_diagnostic_line(&diagnostic.message);
                    diagnostic.code =
                        pioneer_protocol::sanitize_runtime_diagnostic_line(&diagnostic.code);
                }
                let provider = list::cli_runtime_provider_key(&response.runtime_id);
                let models = list::provider_models_response_from_cli_runtime_models_response(
                    provider,
                    response.clone(),
                );
                next.runtime_models = Some(Arc::new(response));
                Ok(Response::Models(models))
            }
            other => other,
        };
        if !allowed {
            next.request = ProviderLoadState::Forbidden;
            next.providers.clear();
            next.models.clear();
            next.runtime_models = None;
        } else {
            next.request = ProviderLoadState::Failed;
            match (result, &request.key.collection) {
                (Ok(Response::Catalog(response)), ProviderCollection::Catalog) => {
                    let mut ids = std::collections::BTreeSet::new();
                    if response
                        .providers
                        .iter()
                        .all(|p| !p.name.is_empty() && ids.insert(p.name.clone()))
                    {
                        next.providers = response
                            .providers
                            .into_iter()
                            .map(|mut provider| {
                                provider.proxy_url =
                                    super::credentials::public_proxy(provider.proxy_url.take());
                                let previous =
                                    next.providers.iter().find(|row| row.id == provider.name);
                                if let Some(row) =
                                    previous.filter(|row| equal(&row.provider, &provider))
                                {
                                    return row.clone();
                                }
                                Arc::new(ProviderCatalogRow {
                                    id: provider.name.clone(),
                                    revision: previous.map_or(1, |row| {
                                        row.revision
                                            .checked_add(1)
                                            .expect("provider row revision exhausted")
                                    }),
                                    provider,
                                })
                            })
                            .collect();
                        next.request = ProviderLoadState::Ready;
                    }
                }
                (
                    Ok(Response::Models(mut response)),
                    ProviderCollection::Models { provider, purpose },
                ) if &response.provider == provider => {
                    if *purpose == ProviderModelKind::Transcription {
                        list::order_transcription_selector_models(&mut response.models);
                    }
                    let mut ids = std::collections::BTreeSet::new();
                    if response.models.iter().all(|m| {
                        !m.id.is_empty() && &m.provider == provider && ids.insert(m.id.clone())
                    }) {
                        next.models = response
                            .models
                            .into_iter()
                            .map(|model| {
                                let previous = next.models.iter().find(|row| row.id == model.id);
                                if let Some(row) = previous.filter(|row| equal(&row.model, &model))
                                {
                                    return row.clone();
                                }
                                Arc::new(ProviderModelRow {
                                    id: model.id.clone(),
                                    revision: previous.map_or(1, |row| {
                                        row.revision
                                            .checked_add(1)
                                            .expect("model row revision exhausted")
                                    }),
                                    model,
                                })
                            })
                            .collect();
                        next.request = ProviderLoadState::Ready;
                    }
                }
                _ => {}
            }
        }
        self.publish_provider_collection(state, next);
        drop(owner);
        self.sync_provider_startup(&request.key);
    }
    pub(crate) fn invalidate_provider_collections(&self) {
        let mut owner = self.provider_store.lock().expect("provider store poisoned");
        for state in owner.collections.values_mut() {
            state.request = None;
            let mut next = (*state.publication).clone();
            next.providers.clear();
            next.models.clear();
            next.runtime_models = None;
            next.request = ProviderLoadState::Cancelled;
            self.publish_provider_collection(state, next);
        }
    }
    pub(crate) fn resume_provider_collection_demand(&self) {
        let keys: Vec<_> = self
            .provider_store
            .lock()
            .expect("provider store poisoned")
            .collections
            .iter()
            .filter(|(_, state)| {
                state.demand > 0
                    && matches!(
                        state.publication.request,
                        ProviderLoadState::Cancelled | ProviderLoadState::Forbidden
                    )
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            self.provider_collection_intent(ProviderCollectionIntent::Refresh { key });
        }
    }
    pub(super) fn refresh_provider_models(&self, workspace: &str, provider: &str) {
        let keys: Vec<_> = self.provider_store.lock().expect("provider store poisoned").collections.keys().filter(|key| key.workspace_id == workspace && matches!(&key.collection, ProviderCollection::Models { provider: id, .. } if id == provider)).cloned().collect();
        for key in keys {
            self.provider_collection_intent(ProviderCollectionIntent::Refresh { key });
        }
    }
    pub(crate) fn start_provider_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<Request>(64);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-provider-catalog".into())
            .spawn(move || {
                while let Ok(request) = receiver.recv() {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if !core.provider_collection_request_current(&request) {
                        continue;
                    }
                    let sender = core.compatibility_runtime().ws_command_sender();
                    drop(core);
                    let key = &request.key;
                    let result = match &key.collection {
                        ProviderCollection::Catalog => sender
                            .provider_list(list::provider_list_params(&key.workspace_id))
                            .map(Response::Catalog),
                        ProviderCollection::Models { provider, purpose } => {
                            if let Some(runtime) =
                                list::runtime_id_from_cli_runtime_provider_key(provider)
                            {
                                sender
                                    .cli_runtime_list_models(list::cli_runtime_list_models_params(
                                        &key.workspace_id,
                                        runtime,
                                    ))
                                    .map(Response::RuntimeModels)
                            } else {
                                let params =
                                    list::provider_list_models_params(&key.workspace_id, provider);
                                match purpose {
                                    ProviderModelKind::Chat => sender.provider_list_models(params),
                                    ProviderModelKind::Embeddings => {
                                        sender.provider_list_embedding_models(params)
                                    }
                                    ProviderModelKind::Transcription => {
                                        sender.provider_list_transcription_models(params)
                                    }
                                }
                                .map(Response::Models)
                            }
                        }
                    };
                    // A malformed transport reply is an explicit request failure. The reducer
                    // itself rejects misrouted completions without consuming the active request.
                    let result = result.and_then(|response| {
                        anyhow::ensure!(
                            response_matches(key, &response),
                            "provider_response_scope_mismatch"
                        );
                        Ok(response)
                    });
                    if let Some(core) = weak.upgrade() {
                        core.complete_provider_collection(request, result);
                    } else {
                        return;
                    }
                }
            })
            .expect("provider catalog worker");
        let mut owner = self.provider_store.lock().expect("provider store poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::*;
    fn fixture() -> (Arc<ClientCore>, mpsc::Receiver<Request>) {
        let core = Arc::new(ClientCore::new());
        let role = AuthorizationRolePresentation {
            key: "admin".into(),
            display_name: "Administrator".into(),
            description: String::new(),
            built_in: false,
        };
        core.accept_authorization_projection(
            0,
            None,
            AuthorizationCapabilitySnapshot {
                schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
                authorization_revision: 1,
                principal_id: PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").unwrap(),
                role_key: "admin".into(),
                role,
                global: AuthorizationGlobalCapabilities {
                    can_manage_capabilities: true,
                    ..Default::default()
                },
                workspace: None,
                thread: None,
            },
        );
        let (sender, receiver) = mpsc::sync_channel(64);
        core.provider_store.lock().unwrap().sender = Some(sender);
        (core, receiver)
    }
    fn catalog(names: &[&str]) -> Response {
        Response::Catalog(ProviderListResponse { providers: names.iter().map(|name| serde_json::from_value(serde_json::json!({"name": name, "capabilities": {}, "api_key_configured": true})).unwrap()).collect() })
    }
    fn models(provider: &str, names: &[&str]) -> Response {
        Response::Models(ProviderListModelsResponse { provider: provider.into(), models: names.iter().map(|id| serde_json::from_value(serde_json::json!({"id":id,"name":id,"provider":provider,"limits":{},"capabilities":{}})).unwrap()).collect() })
    }
    fn transcription_model(id: &str, recommended: bool) -> ProviderModelInfo {
        ProviderModelInfo {
            id: id.to_owned(),
            name: Some(id.to_owned()),
            description: None,
            created: None,
            provider: "local".to_owned(),
            owned_by: None,
            limits: ProviderModelLimits::default(),
            capabilities: ProviderModelCapabilities {
                transcription: Some(true),
                ..Default::default()
            },
            transcription: Some(ProviderTranscriptionModelMetadata {
                engine: "test".to_owned(),
                download_size_mb: 1,
                accuracy_score: 1,
                speed_score: 1,
                supports_translation: false,
                supported_languages: vec!["en".to_owned()],
                supports_language_selection: false,
                recommended,
            }),
            pricing: None,
            active: Some(true),
            family: Some("test".to_owned()),
            lifecycle_status: None,
        }
    }

    #[test]
    fn transcription_collection_preserves_recommended_order_and_safe_public_metadata() {
        let expected_ids = [
            "small",
            "medium",
            "turbo",
            "large",
            "breeze-asr",
            "parakeet-tdt-0.6b-v2",
            "parakeet-tdt-0.6b-v3",
            "moonshine-base",
            "moonshine-tiny-streaming-en",
            "moonshine-small-streaming-en",
            "moonshine-medium-streaming-en",
            "sense-voice-int8",
            "gigaam-v3-e2e-ctc",
            "canary-180m-flash",
            "canary-1b-v2",
            "cohere-int8",
        ];
        let models = expected_ids
            .iter()
            .map(|id| transcription_model(id, *id == "parakeet-tdt-0.6b-v3"))
            .collect();
        let (core, requests) = fixture();
        let key =
            ProviderCollectionKey::models("workspace", "local", ProviderModelKind::Transcription);
        let read = core.read_provider_collection(key, false).unwrap();
        core.complete_provider_collection(
            requests.try_recv().unwrap(),
            Ok(Response::Models(ProviderListModelsResponse {
                provider: "local".into(),
                models,
            })),
        );
        let response = read.wait().unwrap().models_response().unwrap();
        assert_eq!(response.models.len(), expected_ids.len());
        assert_eq!(response.models[0].id, "parakeet-tdt-0.6b-v3");
        let mut actual_ids = response
            .models
            .iter()
            .map(|model| model.id.as_str())
            .collect::<Vec<_>>();
        actual_ids.sort_unstable();
        let mut expected_ids = expected_ids.to_vec();
        expected_ids.sort_unstable();
        assert_eq!(actual_ids, expected_ids);

        let public_json = serde_json::to_string(&response).expect("public catalog JSON");
        for private_field in ["url", "sha256", "checksum", "artifact", "install_dir"] {
            assert!(!public_json.contains(private_field));
        }
    }
    #[test]
    fn wrong_scope_and_duplicate_completion_do_not_consume_the_current_read() {
        let (core, requests) = fixture();
        let key = ProviderCollectionKey::models("workspace", "openai", ProviderModelKind::Chat);
        let read = core.read_provider_collection(key.clone(), false).unwrap();
        let request = requests.try_recv().unwrap();
        let loading = core.provider_collection_snapshot(&key).unwrap();
        core.complete_provider_collection(request.clone(), Ok(models("anthropic", &["other"])));
        assert!(Arc::ptr_eq(
            &loading,
            &core.provider_collection_snapshot(&key).unwrap()
        ));
        assert!(core.provider_collection_request_current(&request));
        core.complete_provider_collection(request.clone(), Ok(models("openai", &["model"])));
        let ready = read.wait().unwrap();
        core.complete_provider_collection(request, Ok(models("openai", &["other"])));
        assert!(Arc::ptr_eq(
            &ready,
            &core.provider_collection_snapshot(&key).unwrap()
        ));
    }
    #[test]
    fn coalesced_reads_keep_rows_across_equal_replace_and_reordering() {
        let (core, requests) = fixture();
        let key = ProviderCollectionKey::catalog("workspace");
        let first = core.read_provider_collection(key.clone(), false).unwrap();
        let second = core.read_provider_collection(key.clone(), false).unwrap();
        let request = requests.try_recv().unwrap();
        assert!(requests.try_recv().is_err());
        core.complete_provider_collection(request.clone(), Ok(catalog(&["openai", "anthropic"])));
        let ready = first.wait().unwrap();
        assert!(Arc::ptr_eq(&ready, &second.wait().unwrap()));
        core.complete_provider_collection(request, Ok(catalog(&["anthropic"])));
        assert!(Arc::ptr_eq(
            &ready,
            &core.provider_collection_snapshot(&key).unwrap()
        ));
        let retry = core.read_provider_collection(key.clone(), true).unwrap();
        let request = requests.try_recv().unwrap();
        core.complete_provider_collection(request, Ok(catalog(&["anthropic", "openai"])));
        let reordered = retry.wait().unwrap();
        assert!(Arc::ptr_eq(
            &ready.providers()[0],
            &reordered.providers()[1]
        ));
        assert!(Arc::ptr_eq(
            &ready.providers()[1],
            &reordered.providers()[0]
        ));
        let cached = core
            .read_provider_collection(key, false)
            .unwrap()
            .wait()
            .unwrap();
        assert!(Arc::ptr_eq(&cached, &reordered));
        assert!(requests.try_recv().is_err());
    }
    #[test]
    fn last_read_drop_cancels_and_late_completion_cannot_revive_it() {
        let (core, requests) = fixture();
        let key = ProviderCollectionKey::catalog("workspace");
        let first = core.read_provider_collection(key.clone(), false).unwrap();
        let second = core.read_provider_collection(key.clone(), false).unwrap();
        let stale = requests.try_recv().unwrap();
        drop(first);
        assert_eq!(
            core.provider_collection_snapshot(&key).unwrap().request(),
            ProviderLoadState::Loading
        );
        drop(second);
        let cancelled = core.provider_collection_snapshot(&key).unwrap();
        assert_eq!(cancelled.request(), ProviderLoadState::Cancelled);
        core.complete_provider_collection(stale.clone(), Ok(catalog(&["openai"])));
        assert!(Arc::ptr_eq(
            &cancelled,
            &core.provider_collection_snapshot(&key).unwrap()
        ));
        let read = core.read_provider_collection(key.clone(), false).unwrap();
        let current = requests.try_recv().unwrap();
        assert!(current.generation > stale.generation);
        core.complete_provider_collection(current, Ok(catalog(&["anthropic"])));
        assert_eq!(read.wait().unwrap().providers()[0].id(), "anthropic");
    }
    #[test]
    fn failed_read_is_explicit_and_only_retry_starts_another_request() {
        let (core, requests) = fixture();
        let key = ProviderCollectionKey::catalog("workspace");
        let read = core.read_provider_collection(key.clone(), false).unwrap();
        core.complete_provider_collection(
            requests.try_recv().unwrap(),
            Err(anyhow::anyhow!("synthetic transport failure")),
        );
        let failed = read.wait().unwrap();
        assert_eq!(failed.request(), ProviderLoadState::Failed);
        assert!(failed.catalog_response().is_err());
        assert!(Arc::ptr_eq(
            &failed,
            &core
                .read_provider_collection(key.clone(), false)
                .unwrap()
                .wait()
                .unwrap()
        ));
        assert!(requests.try_recv().is_err());
        let retry = core.read_provider_collection(key, true).unwrap();
        core.complete_provider_collection(requests.try_recv().unwrap(), Ok(catalog(&[])));
        assert!(
            retry
                .wait()
                .unwrap()
                .catalog_response()
                .unwrap()
                .providers
                .is_empty()
        );
    }
    #[test]
    fn model_purpose_and_workspace_partitions_preserve_sibling_identity() {
        let (core, requests) = fixture();
        let a = ProviderCollectionKey::models("one", "openai", ProviderModelKind::Chat);
        let b = ProviderCollectionKey::models("one", "openai", ProviderModelKind::Embeddings);
        let c = ProviderCollectionKey::models("two", "openai", ProviderModelKind::Chat);
        let reads = [a.clone(), b.clone(), c.clone()]
            .map(|key| core.read_provider_collection(key, false).unwrap());
        for _ in 0..3 {
            core.complete_provider_collection(
                requests.try_recv().unwrap(),
                Ok(models("openai", &["model"])),
            );
        }
        let [ar, br, cr] = reads.map(|r| r.wait().unwrap());
        let refresh = core.read_provider_collection(a, true).unwrap();
        core.complete_provider_collection(
            requests.try_recv().unwrap(),
            Ok(models("openai", &["model", "new"])),
        );
        assert!(Arc::ptr_eq(
            &ar.models()[0],
            &refresh.wait().unwrap().models()[0]
        ));
        assert!(Arc::ptr_eq(
            &br,
            &core.provider_collection_snapshot(&b).unwrap()
        ));
        assert!(Arc::ptr_eq(
            &cr,
            &core.provider_collection_snapshot(&c).unwrap()
        ));
    }
    #[test]
    fn access_fence_wakes_waiters_and_rejects_late_success() {
        let (core, requests) = fixture();
        let key = ProviderCollectionKey::catalog("workspace");
        let read = core.read_provider_collection(key.clone(), false).unwrap();
        let request = requests.try_recv().unwrap();
        core.invalidate_authorization_revision(2);
        assert_eq!(read.wait().unwrap().request(), ProviderLoadState::Cancelled);
        core.complete_provider_collection(request, Ok(catalog(&["openai"])));
        assert!(core.provider_collection_snapshot(&key).is_none());
        assert_eq!(
            core.read_provider_collection(key, false)
                .unwrap()
                .wait()
                .unwrap()
                .request(),
            ProviderLoadState::Forbidden
        );
    }
    #[test]
    fn read_queue_and_waiter_capacity_are_bounded() {
        let (core, requests) = fixture();
        let key = ProviderCollectionKey::catalog("workspace");
        let reads: Vec<_> = (0..64)
            .map(|_| core.read_provider_collection(key.clone(), false).unwrap())
            .collect();
        assert!(core.read_provider_collection(key.clone(), false).is_err());
        assert!(requests.try_recv().is_ok());
        assert!(requests.try_recv().is_err());
        drop(reads);
        assert_eq!(
            core.provider_collection_snapshot(&key).unwrap().request(),
            ProviderLoadState::Cancelled
        );
    }
    #[test]
    fn catalog_projection_does_not_publish_proxy_credentials_or_notify_other_feature_scopes() {
        let (core, requests) = fixture();
        let scopes = [
            ClientScope::AdministrationOperation,
            ClientScope::Navigation,
            ClientScope::Composer {
                thread_id: "thread".into(),
            },
            ClientScope::Settings,
        ];
        let subscriptions: Vec<_> = scopes
            .into_iter()
            .map(|scope| core.subscribe(scope, std::num::NonZeroUsize::new(8).unwrap()))
            .collect();
        let key = ProviderCollectionKey::catalog("workspace");
        let read = core.read_provider_collection(key, false).unwrap();
        let mut provider: ProviderSummary = serde_json::from_value(
            serde_json::json!({"name":"openai", "capabilities":{}, "api_key_configured":true}),
        )
        .unwrap();
        provider.proxy_url = Some("http://synthetic-user:synthetic-password@localhost:8080".into());
        core.complete_provider_collection(
            requests.try_recv().unwrap(),
            Ok(Response::Catalog(ProviderListResponse {
                providers: vec![provider],
            })),
        );
        let json = serde_json::to_string(&read.wait().unwrap()).unwrap();
        assert!(!json.contains("synthetic-user"));
        assert!(!json.contains("synthetic-password"));
        for subscription in subscriptions {
            assert!(subscription.try_next().is_none());
        }
    }
}

#[cfg(test)]
impl ProviderCollectionPublication {
    pub(crate) fn for_test(
        key: ProviderCollectionKey,
        providers: Vec<ProviderSummary>,
        models: Vec<ProviderModelInfo>,
    ) -> Arc<Self> {
        Arc::new(Self {
            key,
            revision: 1,
            request: ProviderLoadState::Ready,
            providers: providers
                .into_iter()
                .map(|provider| {
                    Arc::new(ProviderCatalogRow {
                        id: provider.name.clone(),
                        revision: 1,
                        provider,
                    })
                })
                .collect(),
            models: models
                .into_iter()
                .map(|model| {
                    Arc::new(ProviderModelRow {
                        id: model.id.clone(),
                        revision: 1,
                        model,
                    })
                })
                .collect(),
            runtime_models: None,
        })
    }
}
