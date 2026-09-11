//! Scoped settings publications and process-owned, demand-driven request execution.
use crate::{core::*, gateway::settings_store::GatewaySettingsStore};
use pioneer_protocol::*;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum SettingsPage {
    General,
    RemoteAccess,
    Voice,
    Memory,
    SelfImprovement,
}
impl SettingsPage {
    const ALL: [Self; 5] = [
        Self::General,
        Self::RemoteAccess,
        Self::Voice,
        Self::Memory,
        Self::SelfImprovement,
    ];
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SettingsPageValue {
    General {
        settings: GatewayGeneralSettings,
    },
    RemoteAccess {
        settings: GatewayRemoteAccessSettings,
    },
    Voice {
        settings: GatewayVoiceInputSettings,
    },
    Memory {
        memory: GatewayMemorySettings,
        thread_episodic: GatewayThreadEpisodicSettings,
    },
    SelfImprovement {
        settings: GatewaySelfImprovementSettings,
        status: Option<GatewaySelfImprovementStatus>,
    },
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize)]
pub struct SettingsPagePublication {
    pub value: Option<SettingsPageValue>,
    pub workspace_id: Option<String>,
    pub loading: bool,
    pub pending: bool,
    pub error: Option<String>,
    pub needs_selection: bool,
    pub input_reset_generation: u64,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SettingsIntent {
    CreateDeviceActivation,
    CloseDeviceActivation {
        generation: u64,
    },
    Refresh,
    RefreshSessions,
    RevokeSession {
        expected_owner: u64,
        session_id: AuthSessionId,
        expected_status: Option<AuthSessionStatus>,
    },
    Keepawake {
        enabled: bool,
    },
    Telemetry {
        enabled: bool,
    },
    PreflightModel {
        selection: GatewayMemoryModelSelection,
    },
    RemoteAccess {
        enabled: bool,
        key: Option<String>,
        clear_key: bool,
    },
    Memory {
        settings: GatewayMemorySettings,
    },
    MemoryToggle {
        toggle: super::memory::MemorySettingToggle,
        enabled: bool,
    },
    MemoryModel {
        setting: super::memory::MemoryModelSetting,
        selection: GatewayMemoryModelSelection,
    },
    VectorEnabled {
        enabled: bool,
    },
    VectorInstructions {
        enabled: bool,
    },
    VectorModel {
        selection: crate::composer::model_selection::ModelSelectorSelection,
    },
    ImprovementEnabled {
        enabled: bool,
    },
    ImprovementModel {
        setting: super::self_improvement::SelfImprovementModelSetting,
        selection: Option<GatewaySelfImprovementModelSelection>,
    },
    ThreadEpisodic {
        enabled: bool,
    },
    VectorSearch {
        settings: GatewayThreadEpisodicVectorSearchSettings,
    },
    SelfImprovement {
        settings: GatewaySelfImprovementSettings,
    },
    VoiceEnabled {
        enabled: bool,
    },
    VoiceModel {
        provider: Option<String>,
        model: Option<String>,
    },
    VoiceRetry,
}
impl SettingsIntent {
    fn page(&self) -> Option<SettingsPage> {
        Some(match self {
            Self::CreateDeviceActivation
            | Self::CloseDeviceActivation { .. }
            | Self::Refresh
            | Self::RefreshSessions
            | Self::RevokeSession { .. } => return None,
            Self::Keepawake { .. } | Self::Telemetry { .. } | Self::PreflightModel { .. } => {
                SettingsPage::General
            }
            Self::RemoteAccess { .. } => SettingsPage::RemoteAccess,
            Self::Memory { .. }
            | Self::MemoryToggle { .. }
            | Self::MemoryModel { .. }
            | Self::VectorEnabled { .. }
            | Self::VectorInstructions { .. }
            | Self::VectorModel { .. }
            | Self::ThreadEpisodic { .. }
            | Self::VectorSearch { .. } => SettingsPage::Memory,
            Self::SelfImprovement { .. }
            | Self::ImprovementEnabled { .. }
            | Self::ImprovementModel { .. } => SettingsPage::SelfImprovement,
            Self::VoiceEnabled { .. } | Self::VoiceModel { .. } | Self::VoiceRetry => {
                SettingsPage::Voice
            }
        })
    }
    fn update(&self, snapshot: &GatewaySettingsSnapshot) -> Option<GatewaySettingsUpdate> {
        use super::{gateway as g, voice as v};
        let voice = |plan| match plan {
            v::VoiceInputSettingsPlan::Update { update } => Some(update),
            _ => None,
        };
        match self {
            Self::Keepawake { enabled } => {
                g::keepawake_update_plan(Some(snapshot), *enabled).map(|p| p.update)
            }
            Self::Telemetry { enabled } => {
                g::telemetry_enabled_update_plan(Some(snapshot), *enabled).map(|p| p.update)
            }
            Self::PreflightModel { selection } => {
                g::preflight_model_update_plan(Some(snapshot), selection.clone()).map(|p| p.update)
            }
            Self::RemoteAccess {
                enabled,
                key,
                clear_key,
            } => g::remote_access_update_plan(Some(snapshot), *enabled, key.clone(), *clear_key)
                .map(|p| p.update),
            Self::MemoryToggle { toggle, enabled } => {
                Some(super::memory::gateway_settings_update_for_memory(
                    super::memory::memory_settings_with_toggle(
                        snapshot.memory.clone(),
                        *toggle,
                        *enabled,
                    ),
                ))
            }
            Self::MemoryModel { setting, selection } => {
                Some(super::memory::gateway_settings_update_for_memory(
                    super::memory::memory_settings_with_model_selection(
                        snapshot.memory.clone(),
                        *setting,
                        selection.clone(),
                    ),
                ))
            }
            Self::VectorEnabled { enabled } => {
                let mut settings = snapshot.thread_episodic.vector_search.clone();
                settings.enabled = *enabled;
                g::thread_episodic_vector_search_update_plan(Some(snapshot), settings)
                    .map(|p| p.update)
            }
            Self::VectorInstructions { enabled } => {
                let mut settings = snapshot.thread_episodic.vector_search.clone();
                settings.use_search_instructions = *enabled;
                g::thread_episodic_vector_search_update_plan(Some(snapshot), settings)
                    .map(|p| p.update)
            }
            Self::VectorModel { selection } => {
                let provider = match selection
                    .provider
                    .as_deref()?
                    .trim()
                    .to_ascii_lowercase()
                    .as_str()
                {
                    "openai" => GatewayThreadEpisodicVectorProvider::OpenAi,
                    "openrouter" => GatewayThreadEpisodicVectorProvider::OpenRouter,
                    "local" => GatewayThreadEpisodicVectorProvider::Local,
                    _ => return None,
                };
                let model = selection.model.as_deref()?.trim();
                if model.is_empty() {
                    return None;
                }
                let mut settings = snapshot.thread_episodic.vector_search.clone();
                settings.enabled = true;
                settings.provider = Some(provider);
                settings.model = Some(model.into());
                if provider == GatewayThreadEpisodicVectorProvider::Local {
                    settings.local_model = Some(model.into());
                }
                settings.embedding_dimension = None;
                g::thread_episodic_vector_search_update_plan(Some(snapshot), settings)
                    .map(|p| p.update)
            }
            Self::ImprovementEnabled { enabled } => {
                super::self_improvement::enabled_update_plan(Some(snapshot), *enabled)
                    .map(|p| p.update)
            }
            Self::ImprovementModel { setting, selection } => {
                super::self_improvement::model_update_plan(
                    Some(snapshot),
                    *setting,
                    selection.clone(),
                )
                .map(|p| p.update)
            }
            Self::Memory { settings } => Some(GatewaySettingsUpdate {
                memory: Some(settings.clone()),
                ..Default::default()
            }),
            Self::ThreadEpisodic { enabled } => {
                g::thread_episodic_enabled_update_plan(Some(snapshot), *enabled).map(|p| p.update)
            }
            Self::VectorSearch { settings } => {
                g::thread_episodic_vector_search_update_plan(Some(snapshot), settings.clone())
                    .map(|p| p.update)
            }
            Self::SelfImprovement { settings } => Some(GatewaySettingsUpdate {
                self_improvement: Some(settings.clone()),
                ..Default::default()
            }),
            Self::VoiceEnabled { enabled } => voice(if *enabled {
                v::voice_input_enable_plan(&snapshot.voice_input)
            } else {
                v::voice_input_disable_plan(&snapshot.voice_input)
            }),
            Self::VoiceModel { provider, model } => voice(v::voice_input_model_selection_plan(
                &snapshot.voice_input,
                provider.as_deref(),
                model.clone(),
            )),
            Self::VoiceRetry => voice(v::voice_input_retry_plan(&snapshot.voice_input)),
            _ => None,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SettingsEpoch {
    authorization: u64,
    permissions_generation: Option<u64>,
    workspace: Option<String>,
}
impl SettingsEpoch {
    pub(crate) fn matches_owner(
        &self,
        owner: &crate::gateway::identity_authorization::IdentityAuthorizationStore,
        workspace: Option<String>,
    ) -> bool {
        self.authorization == owner.authorization_epoch().0
            && self.permissions_generation == owner.permissions_generation()
            && self.workspace == workspace
    }
}
#[derive(Default)]
struct PageAction {
    pending: bool,
    error: Option<String>,
    needs_selection: bool,
    epoch: Option<SettingsEpoch>,
}
pub(crate) enum SettingsWork {
    LogoutCleanup,
    Activation(super::device_activation::DeviceActivationWork),
    Intent {
        intent: SettingsIntent,
        epoch: SettingsEpoch,
    },
    Profile {
        save: super::profile::ProfileSave,
        epoch: u64,
    },
}
#[derive(Default)]
pub(crate) struct SettingsRuntime {
    queue: VecDeque<SettingsWork>,
    actions: BTreeMap<SettingsPage, PageAction>,
    demands: BTreeMap<SettingsPage, bool>,
    desktop_demands: BTreeMap<SettingsPage, usize>,
    sessions_leases: usize,
    refreshing: Option<SettingsEpoch>,
    demand_authorization: Option<u64>,
    sessions_pending: Option<SettingsEpoch>,
    sessions_demand_authorization: Option<u64>,
    logout_cleanup: Option<crate::gateway::types::GatewayEndpoint>,
    logout_cleanup_active: bool,
    remote_key_reset_generation: u64,
    remote_poll: Option<(Instant, usize, SettingsEpoch)>,
    self_poll: Option<(Instant, SettingsEpoch)>,
    wake: Option<mpsc::SyncSender<()>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl SettingsRuntime {
    pub(crate) fn stop(&mut self) {
        self.wake.take();
        self.invalidate();
        self.queue.clear();
    }
    pub(crate) fn invalidate(&mut self) {
        self.queue
            .retain(|work| matches!(work, SettingsWork::LogoutCleanup));
        self.actions.clear();
        self.remote_key_reset_generation = self
            .remote_key_reset_generation
            .checked_add(1)
            .expect("remote key reset generation exhausted");
        self.refreshing = None;
        self.demand_authorization = None;
        self.sessions_pending = None;
        self.sessions_demand_authorization = None;
        self.remote_poll = None;
        self.self_poll = None;
    }
    pub(crate) fn remote_key_reset_generation(&self) -> u64 {
        self.remote_key_reset_generation
    }
    fn active(&self, page: SettingsPage) -> bool {
        self.demands.get(&page) == Some(&true)
            || self
                .desktop_demands
                .get(&page)
                .is_some_and(|count| *count > 0)
    }
    fn wake(&self) {
        if let Some(wake) = &self.wake {
            let _ = wake.try_send(());
        }
    }
}
impl Drop for SettingsRuntime {
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
    pub(crate) fn settings_epoch(&self) -> SettingsEpoch {
        let identity = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        SettingsEpoch {
            authorization: identity.authorization_epoch().0,
            permissions_generation: identity.permissions_generation(),
            workspace: self.settings_workspace(),
        }
    }
    pub(crate) fn settings_workspace(&self) -> Option<String> {
        self.snapshot(&ClientScope::Navigation)
            .and_then(|p| p.typed::<crate::navigation::ClientNavigationState>())
            .and_then(|p| p.payload().workspace_id().map(str::to_owned))
    }
    pub(crate) fn publish_settings_value<T: 'static + Send + Sync + serde::Serialize>(
        &self,
        scope: ClientScope,
        value: T,
    ) -> ClientTransition {
        let revision = self
            .snapshot(&scope)
            .map_or(1, |p| p.revisions().scoped().get() + 1);
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(revision),
            Arc::new(value),
            vec![],
        )
    }
    // Called while retaining IdentityAuthorizationStore, so a policy fence cannot
    // be followed by a publication made from a previously cloned settings value.
    pub(crate) fn publish_settings_pages_locked(&self, store: &GatewaySettingsStore) {
        let navigation = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        let workspace = navigation.navigation.workspace_id().map(str::to_owned);
        let runtime = self
            .settings_runtime
            .lock()
            .expect("settings owner poisoned");
        for page in SettingsPage::ALL {
            let value = store
                .settings
                .as_ref()
                .filter(|_| {
                    page != SettingsPage::SelfImprovement || store.workspace_id == workspace
                })
                .map(|s| match page {
                    SettingsPage::General => SettingsPageValue::General {
                        settings: s.general.clone(),
                    },
                    SettingsPage::RemoteAccess => SettingsPageValue::RemoteAccess {
                        settings: s.remote_access.clone(),
                    },
                    SettingsPage::Voice => SettingsPageValue::Voice {
                        settings: s.voice_input.clone(),
                    },
                    SettingsPage::Memory => SettingsPageValue::Memory {
                        memory: s.memory.clone(),
                        thread_episodic: s.thread_episodic.clone(),
                    },
                    SettingsPage::SelfImprovement => SettingsPageValue::SelfImprovement {
                        settings: s.self_improvement.clone(),
                        status: s.self_improvement_status.clone(),
                    },
                });
            let action = runtime.actions.get(&page);
            self.publish_settings_value(
                ClientScope::SettingsPage { page },
                SettingsPagePublication {
                    workspace_id: if page == SettingsPage::SelfImprovement {
                        store.workspace_id.clone()
                    } else {
                        None
                    },
                    loading: value.is_none() && store.loading,
                    error: action.and_then(|a| a.error.clone()).or_else(|| {
                        if value.is_none() {
                            store.error.clone()
                        } else {
                            None
                        }
                    }),
                    value,
                    pending: action.is_some_and(|a| a.pending),
                    needs_selection: action.is_some_and(|a| a.needs_selection),
                    input_reset_generation: if page == SettingsPage::RemoteAccess {
                        runtime.remote_key_reset_generation
                    } else {
                        0
                    },
                },
            );
        }
    }
    fn publish_current_settings_pages(&self) {
        let identity = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        self.publish_settings_pages_locked(&identity.settings);
    }
    pub fn settings_intent(&self, intent: SettingsIntent) -> ClientTransition {
        if self.is_stopped() {
            return self.reject_intent();
        }
        match intent {
            SettingsIntent::CreateDeviceActivation => {
                return self.create_current_device_activation();
            }
            SettingsIntent::CloseDeviceActivation { generation } => {
                return self.close_device_activation(generation);
            }
            _ => {}
        }
        let epoch = self.settings_epoch();
        if matches!(intent, SettingsIntent::RefreshSessions) {
            let mut runtime = self
                .settings_runtime
                .lock()
                .expect("settings owner poisoned");
            if runtime.logout_cleanup.is_some() {
                if !runtime.logout_cleanup_active {
                    runtime.logout_cleanup_active = true;
                    runtime.queue.push_back(SettingsWork::LogoutCleanup);
                    runtime.wake();
                }
                return self.navigation_outcome(ClientTransitionOutcome::Changed);
            }
        }
        if matches!(
            intent,
            SettingsIntent::RefreshSessions | SettingsIntent::RevokeSession { .. }
        ) {
            if self.current_auth().is_none() || self.authorization_snapshot(None, None).is_none() {
                return self.reject_intent();
            }
            if let SettingsIntent::RevokeSession {
                expected_owner,
                session_id,
                expected_status,
            } = &intent
            {
                if *expected_owner != self.auth_sessions().owner_generation
                    || !self.auth_sessions().sessions.iter().any(|item| {
                        item.session.id == *session_id
                            && expected_status.is_none_or(|status| item.session.status == status)
                    })
                {
                    return self.reject_intent();
                }
            }
        }
        let page = intent.page();
        if page.is_some() || matches!(intent, SettingsIntent::Refresh) {
            let identity = self.snapshot(&ClientScope::Administration { workspace_id: None });
            let allowed=identity.and_then(|s|s.typed::<crate::gateway::identity_authorization::IdentityAuthorizationPublication>()).is_some_and(|p|p.payload().capabilities.snapshot(None,None).is_some_and(|s|s.global.can_manage_gateway_settings));
            if !allowed {
                return self.reject_intent();
            }
        }
        let identity = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if identity.authorization_epoch().0 != epoch.authorization
            || identity.permissions_generation() != epoch.permissions_generation
            || self.settings_workspace() != epoch.workspace
        {
            return self.reject_intent();
        }
        if let SettingsIntent::RevokeSession { expected_owner, .. } = &intent {
            if *expected_owner != identity.authorization_epoch().1 {
                return self.reject_intent();
            }
        }
        let mut runtime = self
            .settings_runtime
            .lock()
            .expect("settings owner poisoned");
        if let Some(page) = page {
            let action = runtime.actions.entry(page).or_default();
            if action.pending {
                return self.navigation_outcome(ClientTransitionOutcome::Noop);
            }
            action.pending = true;
            action.error = None;
            action.needs_selection = false;
            action.epoch = Some(epoch.clone());
        } else if matches!(intent, SettingsIntent::Refresh) {
            if runtime.refreshing.is_some() {
                return self.navigation_outcome(ClientTransitionOutcome::Noop);
            }
            runtime.refreshing = Some(epoch.clone());
        } else {
            if runtime.sessions_pending.is_some() {
                return self.navigation_outcome(ClientTransitionOutcome::Noop);
            }
            runtime.sessions_pending = Some(epoch.clone());
            if matches!(intent, SettingsIntent::RefreshSessions) {
                runtime.sessions_demand_authorization = Some(epoch.authorization);
            }
        }
        runtime
            .queue
            .push_back(SettingsWork::Intent { intent, epoch });
        runtime.wake();
        drop(runtime);
        self.publish_settings_pages_locked(&identity.settings);
        self.navigation_outcome(ClientTransitionOutcome::Changed)
    }
    pub(crate) fn queue_profile_save(&self, save: super::profile::ProfileSave, epoch: u64) {
        let mut runtime = self
            .settings_runtime
            .lock()
            .expect("settings owner poisoned");
        runtime
            .queue
            .push_back(SettingsWork::Profile { save, epoch });
        runtime.wake();
    }
    pub(crate) fn settings_demand_changed(&self, scope: &ClientScope, demand: ClientDemand) {
        if *scope == ClientScope::DeviceActivation
            && self.current_scope_demand(scope).unwrap_or(demand) == ClientDemand::Suspended
        {
            let generation = self.device_activation_publication().generation;
            self.close_device_activation(generation);
            return;
        }
        if *scope == ClientScope::AuthSessions && demand != ClientDemand::Suspended {
            self.settings_intent(SettingsIntent::RefreshSessions);
            return;
        }
        let ClientScope::SettingsPage { page } = scope else {
            return;
        };
        let active = self.current_scope_demand(scope).unwrap_or(demand) != ClientDemand::Suspended;
        let allowed = self
            .authorization_snapshot(None, None)
            .is_some_and(|s| s.global.can_manage_gateway_settings);
        let epoch = self.settings_epoch();
        let mut runtime = self
            .settings_runtime
            .lock()
            .expect("settings owner poisoned");
        let changed = runtime.demands.insert(*page, active) != Some(active);
        if changed && *page == SettingsPage::SelfImprovement {
            let combined = active
                || runtime
                    .desktop_demands
                    .get(page)
                    .is_some_and(|count| *count > 0);
            runtime.self_poll =
                (combined && allowed).then(|| (Instant::now() + Duration::from_secs(5), epoch));
        }
        if !runtime.active(SettingsPage::RemoteAccess) {
            runtime.remote_poll = None;
        }
        runtime.wake();
        drop(runtime);
        if active && changed {
            self.settings_intent(SettingsIntent::Refresh);
        }
    }
    pub(crate) fn start_settings_controller(self: &Arc<Self>) {
        let (wake, receiver) = mpsc::sync_channel(1);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-settings".into())
            .spawn(move || {
                loop {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    let (work, wait) = {
                        let mut runtime = core
                            .settings_runtime
                            .lock()
                            .expect("settings owner poisoned");
                        let now = Instant::now();
                        if runtime.queue.is_empty() {
                            let remote = runtime
                                .remote_poll
                                .as_ref()
                                .filter(|(at, _, _)| *at <= now)
                                .cloned();
                            let recurring = runtime
                                .self_poll
                                .as_ref()
                                .filter(|(at, _)| *at <= now)
                                .cloned();
                            if let Some((_, left, epoch)) = remote {
                                runtime.refreshing = Some(epoch.clone());
                                runtime.remote_poll = (left > 1).then(|| {
                                    (now + Duration::from_secs(1), left - 1, epoch.clone())
                                });
                                runtime.queue.push_back(SettingsWork::Intent {
                                    intent: SettingsIntent::Refresh,
                                    epoch,
                                });
                            } else if let Some((_, epoch)) = recurring {
                                runtime.refreshing = Some(epoch.clone());
                                runtime.self_poll =
                                    Some((now + Duration::from_secs(5), epoch.clone()));
                                runtime.queue.push_back(SettingsWork::Intent {
                                    intent: SettingsIntent::Refresh,
                                    epoch,
                                });
                            }
                        }
                        let wait = runtime
                            .remote_poll
                            .as_ref()
                            .map(|(at, _, _)| *at)
                            .into_iter()
                            .chain(runtime.self_poll.as_ref().map(|(at, _)| *at))
                            .min()
                            .map(|at| at.saturating_duration_since(now));
                        (runtime.queue.pop_front(), wait)
                    };
                    if let Some(work) = work {
                        core.execute_settings_work(work);
                        continue;
                    }
                    drop(core);
                    match wait {
                        Some(wait) => {
                            if matches!(
                                receiver.recv_timeout(wait),
                                Err(mpsc::RecvTimeoutError::Disconnected)
                            ) {
                                return;
                            }
                        }
                        None => {
                            if receiver.recv().is_err() {
                                return;
                            }
                        }
                    }
                }
            })
            .expect("settings worker could not start");
        let mut runtime = self
            .settings_runtime
            .lock()
            .expect("settings owner poisoned");
        runtime.wake = Some(wake);
        runtime.task = Some(task);
    }
    fn discard_settings_work(&self, intent: &SettingsIntent, epoch: &SettingsEpoch) {
        let current = self.settings_epoch();
        let mut runtime = self
            .settings_runtime
            .lock()
            .expect("settings owner poisoned");
        if let Some(page) = intent.page() {
            if let Some(action) = runtime.actions.get_mut(&page) {
                if action.epoch.as_ref() == Some(epoch) {
                    action.pending = false;
                }
            }
        } else if matches!(intent, SettingsIntent::Refresh) {
            if runtime.refreshing.as_ref() == Some(epoch) {
                runtime.refreshing = None;
            }
        }
        if matches!(
            intent,
            SettingsIntent::RefreshSessions | SettingsIntent::RevokeSession { .. }
        ) && runtime.sessions_pending.as_ref() == Some(epoch)
        {
            runtime.sessions_pending = None;
        }
        let active = runtime.demands.values().any(|active| *active)
            || runtime.desktop_demands.values().any(|count| *count > 0);
        if runtime.self_poll.is_some() {
            runtime.self_poll = Some((Instant::now() + Duration::from_secs(5), current));
        }
        drop(runtime);
        self.publish_current_settings_pages();
        if active && !self.is_stopped() {
            self.settings_intent(SettingsIntent::Refresh);
        }
    }
    pub(crate) fn queue_device_activation(
        &self,
        work: super::device_activation::DeviceActivationWork,
    ) {
        let mut runtime = self
            .settings_runtime
            .lock()
            .expect("settings owner poisoned");
        runtime.queue.push_back(SettingsWork::Activation(work));
        runtime.wake();
    }
    fn execute_settings_work(&self, work: SettingsWork) {
        self.execute_settings_work_with_sessions(work, |epoch| {
            self.refresh_auth_sessions_for_epoch(epoch).map(|_| ())
        });
    }
    fn execute_settings_work_with_sessions(
        &self,
        work: SettingsWork,
        refresh_sessions: impl FnOnce(&SettingsEpoch) -> anyhow::Result<()>,
    ) {
        let work = match work {
            SettingsWork::LogoutCleanup => {
                self.execute_logout_cleanup();
                return;
            }
            SettingsWork::Activation(work) => {
                self.execute_device_activation(work);
                return;
            }
            other => other,
        };
        let SettingsWork::Intent { intent, epoch } = work else {
            if let SettingsWork::Profile { save, epoch } = work {
                self.execute_profile_save(save, epoch);
            }
            return;
        };
        if self.is_stopped() || self.settings_epoch() != epoch {
            self.discard_settings_work(&intent, &epoch);
            return;
        }
        let page = intent.page();
        if (page.is_some() || matches!(intent, SettingsIntent::Refresh))
            && !self
                .authorization_snapshot(None, None)
                .is_some_and(|s| s.global.can_manage_gateway_settings)
        {
            {
                let mut runtime = self
                    .settings_runtime
                    .lock()
                    .expect("settings owner poisoned");
                runtime.self_poll = None;
                runtime.remote_poll = None;
            }
            self.discard_settings_work(&intent, &epoch);
            return;
        }
        let mut needs_selection = false;
        let result = match &intent {
            SettingsIntent::Refresh => self
                .request_gateway_settings_for_epoch(&epoch)
                .and_then(|generation| self.load_gateway_settings(generation))
                .map(|_| ()),
            SettingsIntent::RefreshSessions => refresh_sessions(&epoch),
            SettingsIntent::RevokeSession {
                session_id,
                expected_status,
                expected_owner,
            } => {
                if self
                    .auth_sessions()
                    .sessions
                    .iter()
                    .any(|item| item.current && item.session.id == *session_id)
                {
                    self.logout_current_settings_session(&epoch, *expected_owner)
                } else {
                    self.revoke_auth_session_for_epoch(
                        AuthSessionRevokeParams {
                            session_id: session_id.clone(),
                            expected_status: *expected_status,
                        },
                        &epoch,
                        *expected_owner,
                    )
                    .map(|_| ())
                }
            }
            _ => match self.gateway_settings().settings {
                Some(snapshot) => match intent.update(&snapshot) {
                    Some(update) => self
                        .prepare_gateway_settings_update_for_epoch(&epoch)
                        .and_then(|generation| {
                            self.execute_gateway_settings_update(generation, update)
                        })
                        .map(|_| ()),
                    None => {
                        needs_selection =
                            matches!(intent, SettingsIntent::VoiceEnabled { enabled: true })
                                && matches!(
                                    super::voice::voice_input_enable_plan(&snapshot.voice_input),
                                    super::voice::VoiceInputSettingsPlan::NeedsSelection
                                );
                        Ok(())
                    }
                },
                None => Err(anyhow::anyhow!("settings_unavailable")),
            },
        };
        self.complete_settings_action(&intent, &epoch, result.is_ok(), needs_selection);
    }
    fn complete_settings_action(
        &self,
        intent: &SettingsIntent,
        epoch: &SettingsEpoch,
        success: bool,
        needs_selection: bool,
    ) {
        let identity = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped()
            || identity.authorization_epoch().0 != epoch.authorization
            || identity.permissions_generation() != epoch.permissions_generation
            || self.settings_workspace() != epoch.workspace
        {
            drop(identity);
            self.discard_settings_work(intent, epoch);
            return;
        }
        let store = &identity.settings;
        let page = intent.page();
        let mut runtime = self
            .settings_runtime
            .lock()
            .expect("settings owner poisoned");
        if let Some(page) = page {
            let Some(action) = runtime
                .actions
                .get_mut(&page)
                .filter(|action| action.pending && action.epoch.as_ref() == Some(epoch))
            else {
                return;
            };
            action.pending = false;
            action.needs_selection = needs_selection;
            action.error = (!success).then(|| "settings_save_failed".into());
            if success && matches!(intent, SettingsIntent::RemoteAccess { key: Some(_), .. }) {
                runtime.remote_key_reset_generation += 1;
            }
            if page == SettingsPage::RemoteAccess
                && runtime.active(page)
                && success
                && super::gateway::remote_access_status_needs_poll(store.settings.as_ref())
            {
                runtime.remote_poll =
                    Some((Instant::now() + Duration::from_secs(1), 12, epoch.clone()));
            }
        } else if matches!(intent, SettingsIntent::Refresh) {
            if runtime.refreshing.as_ref() != Some(epoch) {
                return;
            }
            runtime.refreshing = None;
        } else {
            if runtime.sessions_pending.as_ref() != Some(epoch) {
                return;
            }
            runtime.sessions_pending = None;
        }
        if !super::gateway::remote_access_status_needs_poll(store.settings.as_ref()) {
            runtime.remote_poll = None;
        }
        drop(runtime);
        self.publish_settings_pages_locked(store);
    }
}
/// Keeps the account device list demanded while its owning surface is active.
/// Independent leases let multiple windows release their demand separately.
pub struct AuthSessionsDemand {
    client: std::sync::Weak<ClientCore>,
}
impl Drop for AuthSessionsDemand {
    fn drop(&mut self) {
        if let Some(client) = self.client.upgrade() {
            let mut runtime = client
                .settings_runtime
                .lock()
                .expect("settings owner poisoned");
            runtime.sessions_leases = runtime
                .sessions_leases
                .checked_sub(1)
                .expect("auth sessions demand released without acquisition");
        }
    }
}
impl ClientCore {
    pub fn acquire_auth_sessions(self: &Arc<Self>) -> AuthSessionsDemand {
        let first = {
            let mut runtime = self
                .settings_runtime
                .lock()
                .expect("settings owner poisoned");
            runtime.sessions_leases = runtime
                .sessions_leases
                .checked_add(1)
                .expect("auth sessions demand exhausted");
            runtime.sessions_leases == 1
        };
        if first {
            self.settings_intent(SettingsIntent::RefreshSessions);
        }
        AuthSessionsDemand {
            client: Arc::downgrade(self),
        }
    }
}
pub struct SettingsPageDemand {
    client: std::sync::Weak<ClientCore>,
    page: SettingsPage,
}
impl Drop for SettingsPageDemand {
    fn drop(&mut self) {
        if let Some(client) = self.client.upgrade() {
            client.desktop_settings_demand(self.page, false);
        }
    }
}
impl ClientCore {
    pub fn acquire_settings_page(self: &Arc<Self>, page: SettingsPage) -> SettingsPageDemand {
        self.desktop_settings_demand(page, true);
        SettingsPageDemand {
            client: Arc::downgrade(self),
            page,
        }
    }
    fn desktop_settings_demand(&self, page: SettingsPage, acquire: bool) {
        let epoch = self.settings_epoch();
        let allowed = self
            .authorization_snapshot(None, None)
            .is_some_and(|s| s.global.can_manage_gateway_settings);
        let mut runtime = self
            .settings_runtime
            .lock()
            .expect("settings owner poisoned");
        let count = runtime.desktop_demands.entry(page).or_default();
        let changed = if acquire {
            *count += 1;
            *count == 1
        } else {
            *count = count.saturating_sub(1);
            *count == 0
        };
        let active = *count > 0 || runtime.demands.get(&page) == Some(&true);
        if page == SettingsPage::SelfImprovement {
            runtime.self_poll =
                (active && allowed).then(|| (Instant::now() + Duration::from_secs(5), epoch));
        }
        if !runtime.active(SettingsPage::RemoteAccess) {
            runtime.remote_poll = None;
        }
        runtime.wake();
        drop(runtime);
        if acquire && changed {
            self.settings_intent(SettingsIntent::Refresh);
        }
    }
}

impl ClientCore {
    fn logout_current_settings_session(
        &self,
        epoch: &SettingsEpoch,
        owner: u64,
    ) -> anyhow::Result<()> {
        let endpoint_id = self
            .snapshot(&ClientScope::Administration { workspace_id: None })
            .and_then(|p| {
                p.typed::<crate::gateway::identity_authorization::IdentityAuthorizationPublication>(
                )
            })
            .and_then(|p| p.payload().endpoint_id.clone())
            .ok_or_else(|| anyhow::anyhow!("session_endpoint_unavailable"))?;
        let endpoint = self
            .gateway_session
            .lock()
            .expect("session owner poisoned")
            .endpoints
            .get(&endpoint_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("session_endpoint_unavailable"))?;
        self.logout_auth_session_for_epoch(epoch, owner)?;
        self.reduce_gateway_session_lifecycle(
            &endpoint.id,
            crate::gateway::session_lifecycle::SessionLifecycleEvent::AuthFailed {
                reason: crate::gateway::session_lifecycle::SessionTerminalReason::SessionRevoked,
            },
        );
        let disconnect = self.transport_runtime().ws_command_sender().disconnect();
        {
            let mut runtime = self
                .settings_runtime
                .lock()
                .expect("settings owner poisoned");
            runtime.logout_cleanup = Some(endpoint);
            runtime.logout_cleanup_active = true;
        }
        self.execute_logout_cleanup();
        disconnect
    }
    fn execute_logout_cleanup(&self) {
        let endpoint = self
            .settings_runtime
            .lock()
            .expect("settings owner poisoned")
            .logout_cleanup
            .clone();
        let Some(endpoint) = endpoint else {
            return;
        };
        let epoch = self.authorization_connection_generation();
        let result = self.request_platform_effect(
            crate::gateway::session_refresh::GatewaySessionStorageEffect::DeleteGatewaySession {
                endpoint: endpoint.clone(),
            },
        );
        let success = matches!(result, Ok(ClientEffectResult::Completed));
        let mut runtime = self
            .settings_runtime
            .lock()
            .expect("settings owner poisoned");
        runtime.logout_cleanup_active = false;
        if success && runtime.logout_cleanup.as_ref() == Some(&endpoint) {
            runtime.logout_cleanup = None;
        }
        drop(runtime);
        if self.authorization_connection_generation() == epoch && !self.is_stopped() {
            self.record_session_cleanup_result(epoch, success);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn store() -> GatewaySettingsStore {
        GatewaySettingsStore {
            settings: Some(GatewaySettingsSnapshot {
                general: Default::default(),
                memory: Default::default(),
                self_improvement: Default::default(),
                self_improvement_status: None,
                thread_episodic: Default::default(),
                cli_runtimes: Default::default(),
                remote_access: Default::default(),
                voice_input: Default::default(),
            }),
            ..Default::default()
        }
    }
    #[test]
    fn saved_profile_policy_hint_preserves_devices_without_reloading() {
        for demand in [ClientDemand::Visible, ClientDemand::Suspended] {
            let core = crate::catalog_test_support::settings_client();
            let mut capabilities = core.authorization_snapshot(None, None).unwrap();
            assert!(!capabilities.global.can_manage_gateway_settings);
            let mut auth = core.current_auth().unwrap();
            let response = AuthSessionListResponse {
                sessions: vec![AuthSessionListItem {
                    current: true,
                    last_seen_at_unix: 100,
                    device: auth.device.clone(),
                    session: auth.session.clone(),
                }],
            };
            let refresh = |epoch: &SettingsEpoch| {
                assert_eq!(*epoch, core.settings_epoch());
                let request = core.request_auth_sessions()?;
                core.load_auth_sessions_with_reader(request, |_| Ok(response.clone()))
                    .map(|_| ())
            };
            core.dispatch(ClientIntent::SetScopeDemand {
                scope: ClientScope::AuthSessions,
                demand: ClientDemand::Visible,
                generation: ClientGeneration::new(1),
            });
            let work = core
                .settings_runtime
                .lock()
                .unwrap()
                .queue
                .pop_front()
                .unwrap();
            core.execute_settings_work_with_sessions(work, refresh);
            assert_eq!(core.auth_sessions().sessions.len(), 1);
            if demand == ClientDemand::Suspended {
                core.dispatch(ClientIntent::SetScopeDemand {
                    scope: ClientScope::AuthSessions,
                    demand,
                    generation: ClientGeneration::new(2),
                });
            }
            // Gateway replies to profile.update, then publishes RoleAssignment
            // for the same principal and MemberChanged. No route remount occurs.
            auth.principal.display_name = "Changed Name".into();
            let (request, connection) = core.current_auth_ticket();
            core.finish_auth_profile_update(
                request,
                connection,
                AuthProfileUpdateResponse {
                    principal: auth.principal.clone(),
                    changed: true,
                },
            )
            .unwrap();
            core.observe_policy_change(&AuthorizationProjectionChangedNotification {
                policy_generation: PolicyGeneration::new(2).unwrap(),
                change: AuthorizationChangeKind::RoleAssignment,
                affected: AuthorizationChangeScope::Principal {
                    principal_id: auth.principal.id.clone(),
                },
            });
            core.observe_administration_notification(&GatewayNotification::MemberChanged(
                MemberChangedNotification {
                    revision: 2,
                    principal_id: auth.principal.id.clone(),
                },
            ));
            assert_eq!(core.auth_sessions().sessions, response.sessions);
            capabilities.authorization_revision = 2;
            let (request, connection) = core.current_auth_ticket();
            core.accept_authorization_projection(request, connection, capabilities);
            core.resume_current_settings_demand();
            let work = core
                .settings_runtime
                .lock()
                .unwrap()
                .queue
                .drain(..)
                .collect::<Vec<_>>();
            assert!(work.is_empty(), "unchanged permissions reloaded devices");
            assert_eq!(core.auth_sessions().sessions, response.sessions);
            assert_eq!(
                core.current_auth().unwrap().principal.display_name,
                "Changed Name"
            );
            core.shutdown();
        }
    }
    #[test]
    fn account_leases_survive_revalidation_and_release_independently() {
        use crate::catalog_test_support::{
            replay_account_requests, revalidate_saved_profile, settings_client,
        };
        let core = settings_client();
        let first = core.acquire_auth_sessions();
        let second = core.acquire_auth_sessions();
        assert_eq!(replay_account_requests(&core), (1, 0));
        drop(first);
        revalidate_saved_profile(&core);
        assert_eq!(replay_account_requests(&core), (0, 0));
        assert_eq!(core.auth_sessions().sessions.len(), 1);
        drop(second);
        revalidate_saved_profile(&core);
        assert_eq!(replay_account_requests(&core), (0, 0));
        // A mobile demand and a retained surface have independent lifetimes.
        let last = core.acquire_auth_sessions();
        core.dispatch(ClientIntent::SetScopeDemand {
            scope: ClientScope::AuthSessions,
            demand: ClientDemand::Visible,
            generation: ClientGeneration::new(1),
        });
        assert_eq!(replay_account_requests(&core), (1, 0));
        drop(last);
        revalidate_saved_profile(&core);
        assert_eq!(replay_account_requests(&core), (0, 0));
        core.dispatch(ClientIntent::SetScopeDemand {
            scope: ClientScope::AuthSessions,
            demand: ClientDemand::Suspended,
            generation: ClientGeneration::new(2),
        });
        revalidate_saved_profile(&core);
        assert_eq!(replay_account_requests(&core), (0, 0));
        let late = core.acquire_auth_sessions();
        core.shutdown();
        drop(late);
        assert_eq!(core.settings_runtime.lock().unwrap().sessions_leases, 0);
        assert_eq!(replay_account_requests(&core), (0, 0));
    }
    #[test]
    fn account_lease_before_auth_resumes_once_when_capabilities_arrive() {
        let fixture = crate::catalog_test_support::settings_client();
        let core = Arc::new(ClientCore::new());
        let _demand = core.acquire_auth_sessions();
        assert_eq!(core.replay_account_requests(), (0, 0));
        core.finish_current_auth(0, None, fixture.current_auth().unwrap())
            .unwrap();
        assert_eq!(core.replay_account_requests(), (0, 0));
        core.accept_authorization_projection(
            0,
            None,
            fixture.authorization_snapshot(None, None).unwrap(),
        );
        core.resume_current_settings_demand();
        assert_eq!(core.replay_account_requests(), (1, 0));
        core.resume_current_settings_demand();
        assert_eq!(core.replay_account_requests(), (0, 0));
    }
    #[test]
    fn devices_demand_waits_for_identity_and_capabilities_then_resumes_once() {
        let fixture = crate::catalog_test_support::settings_client();
        let auth = fixture.current_auth().unwrap();
        let capabilities = fixture.authorization_snapshot(None, None).unwrap();
        let core = ClientCore::new();
        core.dispatch(ClientIntent::SetScopeDemand {
            scope: ClientScope::AuthSessions,
            demand: ClientDemand::Visible,
            generation: ClientGeneration::new(1),
        });
        assert!(core.settings_runtime.lock().unwrap().queue.is_empty());
        core.finish_current_auth(0, None, auth).unwrap();
        assert!(core.settings_runtime.lock().unwrap().queue.is_empty());
        core.accept_authorization_projection(0, None, capabilities);
        core.resume_current_settings_demand();
        let runtime = core.settings_runtime.lock().unwrap();
        assert_eq!(runtime.queue.len(), 1);
        assert!(matches!(
            runtime.queue.front(),
            Some(SettingsWork::Intent {
                intent: SettingsIntent::RefreshSessions,
                ..
            })
        ));
    }
    #[test]
    fn pages_publish_equal_values_once_and_only_the_changed_page_advances() {
        let core = ClientCore::new();
        let mut store = store();
        core.publish_settings_pages_locked(&store);
        let before: Vec<_> = SettingsPage::ALL
            .into_iter()
            .map(|page| {
                core.snapshot(&ClientScope::SettingsPage { page })
                    .unwrap()
                    .revisions()
            })
            .collect();
        core.publish_settings_pages_locked(&store);
        store.settings.as_mut().unwrap().general.keepawake = true;
        core.publish_settings_pages_locked(&store);
        for (page, revision) in SettingsPage::ALL.into_iter().zip(before) {
            let next = core.snapshot(&ClientScope::SettingsPage { page }).unwrap();
            assert_eq!(
                next.revisions().scoped().get(),
                revision.scoped().get() + u64::from(page == SettingsPage::General)
            );
        }
        assert!(core.snapshot(&ClientScope::Navigation).is_none());
        assert!(core.snapshot(&ClientScope::Profile).is_none());
    }
    #[test]
    fn late_action_completion_after_policy_replacement_preserves_the_new_page_request() {
        let core = crate::catalog_test_support::settings_model_picker_client();
        let intent = SettingsIntent::Keepawake { enabled: true };
        core.settings_intent(intent.clone());
        let old = core.settings_epoch();
        let reset_before = core
            .settings_runtime
            .lock()
            .unwrap()
            .remote_key_reset_generation;
        let mut capabilities = core.authorization_snapshot(None, None).unwrap();
        capabilities.authorization_revision += 1;
        core.invalidate_authorization_revision(capabilities.authorization_revision);
        let (generation, connection) = core.current_auth_ticket();
        core.accept_authorization_projection(generation, connection, capabilities);
        assert_ne!(core.settings_epoch(), old);
        let remote = core
            .snapshot(&ClientScope::SettingsPage {
                page: SettingsPage::RemoteAccess,
            })
            .unwrap()
            .typed::<SettingsPagePublication>()
            .unwrap();
        assert!(remote.payload().input_reset_generation > reset_before);
        assert!(
            core.prepare_gateway_settings_update_for_epoch(&old)
                .is_err()
        );
        assert!(core.request_gateway_settings_for_epoch(&old).is_err());
        assert!(core.refresh_auth_sessions_for_epoch(&old).is_err());
        core.settings_intent(intent.clone());
        let before = core
            .snapshot(&ClientScope::SettingsPage {
                page: SettingsPage::General,
            })
            .unwrap()
            .snapshot();
        core.complete_settings_action(&intent, &old, false, false);
        assert!(std::sync::Arc::ptr_eq(
            &before,
            &core
                .snapshot(&ClientScope::SettingsPage {
                    page: SettingsPage::General
                })
                .unwrap()
                .snapshot()
        ));
        let epoch = core.settings_epoch();
        core.complete_settings_action(&intent, &epoch, false, false);
        let value = core
            .snapshot(&ClientScope::SettingsPage {
                page: SettingsPage::General,
            })
            .unwrap()
            .typed::<SettingsPagePublication>()
            .unwrap();
        assert!(!value.payload().pending);
        assert_eq!(
            value.payload().error.as_deref(),
            Some("settings_save_failed")
        );
    }
    #[test]
    fn old_refresh_cannot_release_a_new_policy_refresh_demand() {
        let core = crate::catalog_test_support::settings_model_picker_client();
        core.settings_intent(SettingsIntent::Refresh);
        let old = core.settings_epoch();
        let mut capabilities = core.authorization_snapshot(None, None).unwrap();
        capabilities.authorization_revision += 1;
        core.invalidate_authorization_revision(capabilities.authorization_revision);
        let (generation, connection) = core.current_auth_ticket();
        core.accept_authorization_projection(generation, connection, capabilities);
        core.settings_intent(SettingsIntent::Refresh);
        let current = core.settings_epoch();
        core.complete_settings_action(&SettingsIntent::Refresh, &old, false, false);
        assert_eq!(
            core.settings_runtime.lock().unwrap().refreshing,
            Some(current)
        );
    }
    #[test]
    fn settings_and_sessions_fail_closed_without_authorization() {
        let core = ClientCore::new();
        for intent in [
            SettingsIntent::Refresh,
            SettingsIntent::RefreshSessions,
            SettingsIntent::Keepawake { enabled: true },
            SettingsIntent::CreateDeviceActivation,
        ] {
            assert_eq!(
                core.settings_intent(intent).outcome(),
                ClientTransitionOutcome::Rejected
            );
        }
        assert!(core.settings_runtime.lock().unwrap().queue.is_empty());
    }
    #[test]
    fn desktop_demand_is_reference_counted_and_last_release_cancels_poll() {
        let core = crate::catalog_test_support::client();
        let mut capabilities = core.authorization_snapshot(None, None).unwrap();
        capabilities.authorization_revision += 1;
        capabilities.global.can_manage_gateway_settings = true;
        let (generation, connection) = core.current_auth_ticket();
        core.accept_authorization_projection(generation, connection, capabilities);
        let first = core.acquire_settings_page(SettingsPage::SelfImprovement);
        let second = core.acquire_settings_page(SettingsPage::SelfImprovement);
        assert!(core.settings_runtime.lock().unwrap().self_poll.is_some());
        drop(first);
        assert!(core.settings_runtime.lock().unwrap().self_poll.is_some());
        drop(second);
        assert!(core.settings_runtime.lock().unwrap().self_poll.is_none());
        assert_eq!(core.settings_runtime.lock().unwrap().queue.len(), 1);
    }
    #[test]
    fn native_logout_deletion_failure_keeps_retry_and_validates_completion_shape() {
        use crate::gateway::{
            endpoint::GatewayBaseUrl,
            types::{GatewayEndpoint, GatewayEndpointKind},
        };
        let core = Arc::new(ClientCore::new());
        core.settings_runtime.lock().unwrap().logout_cleanup = Some(GatewayEndpoint {
            id: "synthetic".into(),
            name: "Synthetic".into(),
            gateway_base_url: GatewayBaseUrl::parse_presentation("https://gateway.invalid")
                .unwrap(),
            kind: GatewayEndpointKind::Remote,
            session_ref: Some("synthetic-storage-reference".into()),
            server_gateway_id: None,
            workspace_id: None,
            service_name: None,
        });
        let mut sequence = ClientChangeSequence::ZERO;
        for success in [false, true] {
            let worker_core = core.clone();
            let worker = std::thread::spawn(move || worker_core.execute_logout_cleanup());
            let effect = loop {
                let batch = core.wait_for_publications(sequence);
                sequence = batch.sequence;
                if let Some(effect) = batch.effects.into_iter().next() {
                    break effect;
                }
            };
            assert!(matches!(effect.effect(),ClientPlannedEffect::GatewaySessionStorage(crate::gateway::session_refresh::GatewaySessionStorageEffect::DeleteGatewaySession{..})));
            assert_eq!(
                core.complete_effect(ClientEffectCompletion::new(
                    effect.operation_id().clone(),
                    effect.generation(),
                    ClientEffectResult::GatewaySessionEnvelopeLoaded { envelope: None }
                ))
                .outcome(),
                ClientTransitionOutcome::Rejected
            );
            assert!(!worker.is_finished());
            let result = if success {
                ClientEffectResult::Completed
            } else {
                ClientEffectResult::Failed {
                    code: "synthetic-write-failure".into(),
                }
            };
            core.complete_effect(ClientEffectCompletion::new(
                effect.operation_id().clone(),
                effect.generation(),
                result,
            ));
            worker.join().unwrap();
            assert_eq!(
                core.settings_runtime
                    .lock()
                    .unwrap()
                    .logout_cleanup
                    .is_none(),
                success
            );
            assert_eq!(core.auth_sessions().error.is_none(), success);
        }
    }
}

impl ClientCore {
    pub(crate) fn resume_settings_demand(
        &self,
        identity: &crate::gateway::identity_authorization::IdentityAuthorizationPublication,
    ) {
        if self.is_stopped() {
            return;
        }
        let Some(capabilities) = identity.capabilities.snapshot(None, None) else {
            return;
        };
        let sessions_visible = self
            .current_scope_demand(&ClientScope::AuthSessions)
            .is_some_and(|demand| demand != ClientDemand::Suspended);
        let epoch = SettingsEpoch {
            authorization: identity.connection_generation,
            permissions_generation: identity
                .capabilities
                .accepted_revision()
                .map(|_| identity.authorization_change_sequence),
            workspace: self.settings_workspace(),
        };
        let mut runtime = self
            .settings_runtime
            .lock()
            .expect("settings owner poisoned");
        // The account's own device list is independent of Gateway settings
        // management. Policy eviction retires its request but not its demand.
        if (sessions_visible || runtime.sessions_leases > 0)
            && identity.current_auth.is_some()
            && runtime.sessions_demand_authorization != Some(identity.connection_generation)
        {
            runtime.sessions_demand_authorization = Some(identity.connection_generation);
            if runtime.sessions_pending.is_none() {
                runtime.sessions_pending = Some(epoch.clone());
                runtime.queue.push_back(SettingsWork::Intent {
                    intent: SettingsIntent::RefreshSessions,
                    epoch: epoch.clone(),
                });
                runtime.wake();
            }
        }
        if !capabilities.global.can_manage_gateway_settings
            || runtime.demand_authorization == Some(identity.connection_generation)
        {
            return;
        }
        runtime.demand_authorization = Some(identity.connection_generation);
        if runtime.active(SettingsPage::SelfImprovement) {
            runtime.self_poll = Some((Instant::now() + Duration::from_secs(5), epoch.clone()));
        }
        if SettingsPage::ALL
            .into_iter()
            .any(|page| runtime.active(page))
            && runtime.refreshing.is_none()
        {
            runtime.refreshing = Some(epoch.clone());
            runtime.queue.push_back(SettingsWork::Intent {
                intent: SettingsIntent::Refresh,
                epoch,
            });
            runtime.wake();
        }
    }
}

impl ClientCore {
    pub(crate) fn resume_current_settings_demand(&self) {
        if let Some(publication) = self
            .snapshot(&ClientScope::Administration { workspace_id: None })
            .and_then(|p| {
                p.typed::<crate::gateway::identity_authorization::IdentityAuthorizationPublication>(
                )
            })
        {
            self.resume_settings_demand(&publication.payload());
        }
    }
}

#[cfg(test)]
impl ClientCore {
    pub(crate) fn take_activation_cleanup_for_test(&self) -> Option<AuthSessionId> {
        match self.settings_runtime.lock().unwrap().queue.pop_front() {
            Some(SettingsWork::Activation(
                super::device_activation::DeviceActivationWork::Cleanup { session, .. },
            )) => Some(session),
            None => None,
            _ => panic!("unexpected Settings work"),
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl ClientCore {
    /// Replay queued account work with synthetic responses, without a transport or worker.
    pub(crate) fn replay_account_requests(&self) -> (usize, usize) {
        let mut sessions = 0;
        let mut profiles = 0;
        loop {
            let work = self.settings_runtime.lock().unwrap().queue.pop_front();
            let Some(work) = work else { break };
            if let SettingsWork::Profile { save, epoch } = work {
                profiles += 1;
                self.execute_profile_save_with_writer(save, epoch, |params, _| {
                    let mut principal = self.current_auth().unwrap().principal;
                    principal.display_name = params.display_name;
                    principal.nickname = params.nickname;
                    let (request, connection) = self.current_auth_ticket();
                    self.finish_auth_profile_update(
                        request,
                        connection,
                        AuthProfileUpdateResponse {
                            principal,
                            changed: true,
                        },
                    )
                });
            } else {
                assert!(
                    matches!(
                        &work,
                        SettingsWork::Intent {
                            intent: SettingsIntent::RefreshSessions,
                            ..
                        }
                    ),
                    "unexpected account request"
                );
                self.execute_settings_work_with_sessions(work, |epoch| {
                    assert_eq!(*epoch, self.settings_epoch());
                    sessions += 1;
                    let auth = self.current_auth().unwrap();
                    let request = self.request_auth_sessions()?;
                    self.load_auth_sessions_with_reader(request, |_| {
                        Ok(AuthSessionListResponse {
                            sessions: vec![AuthSessionListItem {
                                current: true,
                                last_seen_at_unix: 100,
                                device: auth.device,
                                session: auth.session,
                            }],
                        })
                    })
                    .map(|_| ())
                });
            }
        }
        (sessions, profiles)
    }
}
