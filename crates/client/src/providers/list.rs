//! Provider list state.

use super::{
    catalog,
    cli_runtime_settings::{CLIRuntimeProviderDraft, CLIRuntimeProviderDraftField},
    selectors::ProviderFilter,
};
use pioneer_protocol::{
    AgentExecutionBackend, CLIAgentRuntimeKind, CLIRuntimeListModelsParams,
    CLIRuntimeListModelsResponse, CLIRuntimeListParams, CLIRuntimeListResponse,
    CLIRuntimeRefreshParams, CLIRuntimeRefreshResponse, GatewayCliRuntimeSettings,
    ProviderListModelsParams, ProviderListModelsResponse, ProviderListParams, ProviderListResponse,
    ProviderModelCapabilities, ProviderModelInfo, ProviderModelLimits,
    ProviderModelReasoningCapabilities, ProviderSummary, ReasoningCapabilitySource,
    RuntimeModelInfo, RuntimeStatus, RuntimeSummary,
};
use std::collections::{HashMap, HashSet};

pub const CLI_RUNTIME_REFRESH_AUTO_INTERVAL_MS: i64 = 60_000;
pub const CLI_RUNTIME_REFRESH_STALE_AFTER_MS: i64 = 5 * 60_000;
const CLI_RUNTIME_REFRESH_BACKOFF_BASE_MS: i64 = 30_000;
const CLI_RUNTIME_REFRESH_BACKOFF_MAX_MS: i64 = 5 * 60_000;
pub const CLI_RUNTIME_PROVIDER_PREFIX: &str = "cli_runtime:";

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum CLIRuntimeRefreshTrigger {
    Auto,
    Manual,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum CLIRuntimeRefreshSkipReason {
    AlreadyRefreshing,
    Throttled { next_refresh_at_unix_ms: i64 },
    BackingOff { next_refresh_at_unix_ms: i64 },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct CLIRuntimeRefreshStatus {
    pub in_flight: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_started_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_finished_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_success_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_auto_refresh_at_unix_ms: Option<i64>,
    #[serde(default)]
    pub failure_count: u32,
}

impl CLIRuntimeRefreshStatus {
    pub fn mark_started(&mut self, now_unix_ms: i64) {
        self.in_flight = true;
        self.last_started_at_unix_ms = Some(now_unix_ms);
    }

    pub fn mark_success(&mut self, now_unix_ms: i64) {
        self.in_flight = false;
        self.last_finished_at_unix_ms = Some(now_unix_ms);
        self.last_success_at_unix_ms = Some(now_unix_ms);
        self.next_auto_refresh_at_unix_ms =
            Some(now_unix_ms + CLI_RUNTIME_REFRESH_AUTO_INTERVAL_MS);
        self.failure_count = 0;
    }

    pub fn mark_failure(&mut self, now_unix_ms: i64) {
        self.in_flight = false;
        self.last_finished_at_unix_ms = Some(now_unix_ms);
        self.last_failure_at_unix_ms = Some(now_unix_ms);
        self.failure_count = self.failure_count.saturating_add(1);
        self.next_auto_refresh_at_unix_ms =
            Some(now_unix_ms + cli_runtime_refresh_backoff_ms(self.failure_count));
    }

    pub fn is_stale(&self, now_unix_ms: i64) -> bool {
        self.last_success_at_unix_ms.is_some_and(|last_success| {
            now_unix_ms.saturating_sub(last_success) >= CLI_RUNTIME_REFRESH_STALE_AFTER_MS
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProviderListState {
    configured_names: HashSet<String>,
    provider_proxy_urls: HashMap<String, String>,
    cli_runtimes: Vec<RuntimeSummary>,
    cli_refresh_status: CLIRuntimeRefreshStatus,
    filter: ProviderFilter,
    loading: bool,
    cli_loading: bool,
    error: Option<String>,
    cli_error: Option<String>,
    cli_login_message: Option<String>,
    cli_runtime_draft: Option<CLIRuntimeProviderDraft>,
    expanded_cli_runtime_ids: HashSet<String>,
}

impl Default for ProviderListState {
    fn default() -> Self {
        Self {
            configured_names: HashSet::new(),
            provider_proxy_urls: HashMap::new(),
            cli_runtimes: Vec::new(),
            cli_refresh_status: CLIRuntimeRefreshStatus::default(),
            filter: ProviderFilter::Api,
            loading: false,
            cli_loading: false,
            error: None,
            cli_error: None,
            cli_login_message: None,
            cli_runtime_draft: None,
            expanded_cli_runtime_ids: HashSet::new(),
        }
    }
}

impl ProviderListState {
    pub fn configured_names(&self) -> &HashSet<String> {
        &self.configured_names
    }

    pub fn cli_runtimes(&self) -> &[RuntimeSummary] {
        self.cli_runtimes.as_slice()
    }

    pub fn cli_runtime_proxy_url(&self, runtime_id: &str) -> Option<&str> {
        self.cli_runtimes
            .iter()
            .find(|runtime| runtime.runtime_id == runtime_id)
            .and_then(|runtime| runtime.proxy_url.as_deref())
    }

    pub fn cli_refresh_status(&self) -> &CLIRuntimeRefreshStatus {
        &self.cli_refresh_status
    }

    pub fn is_configured(&self, provider_id: &str) -> bool {
        self.configured_names.contains(provider_id)
    }

    pub fn provider_proxy_url(&self, provider_id: &str) -> Option<&str> {
        self.provider_proxy_urls
            .get(provider_id)
            .map(String::as_str)
    }

    pub fn filter(&self) -> ProviderFilter {
        self.filter
    }

    pub fn loading(&self) -> bool {
        self.loading
    }

    pub fn cli_loading(&self) -> bool {
        self.cli_loading
    }

    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    pub fn cli_error(&self) -> Option<&str> {
        self.cli_error.as_deref()
    }

    pub fn cli_login_message(&self) -> Option<&str> {
        self.cli_login_message.as_deref()
    }

    pub fn cli_runtime_draft(&self) -> Option<&CLIRuntimeProviderDraft> {
        self.cli_runtime_draft.as_ref()
    }

    pub fn is_cli_runtime_expanded(&self, runtime_id: &str) -> bool {
        self.expanded_cli_runtime_ids.contains(runtime_id)
    }

    pub fn expanded_cli_runtime_ids(&self) -> &HashSet<String> {
        &self.expanded_cli_runtime_ids
    }

    pub fn toggle_cli_runtime_expanded(&mut self, runtime_id: String) {
        if !self.expanded_cli_runtime_ids.insert(runtime_id.clone()) {
            self.expanded_cli_runtime_ids.remove(runtime_id.as_str());
        }
    }

    pub fn set_filter(&mut self, filter: ProviderFilter) {
        self.filter = filter;
    }

    pub fn mark_refresh_started(&mut self) {
        self.loading = true;
        self.error = None;
    }

    pub fn mark_cli_runtime_refresh_started(&mut self, now_unix_ms: i64) {
        self.cli_loading = true;
        self.cli_error = None;
        self.cli_login_message = None;
        self.cli_refresh_status.mark_started(now_unix_ms);
    }

    pub fn apply_refresh_success(&mut self, configured_names: HashSet<String>) {
        self.configured_names = configured_names;
        self.provider_proxy_urls.clear();
        self.loading = false;
        self.error = None;
    }

    pub fn apply_refresh_response(&mut self, response: ProviderListResponse) {
        self.configured_names = configured_provider_names_from_list(response.providers.as_slice());
        self.provider_proxy_urls = provider_proxy_urls_from_list(response.providers.as_slice());
        self.loading = false;
        self.error = None;
    }

    pub fn apply_cli_runtime_refresh_response(
        &mut self,
        response: CLIRuntimeRefreshResponse,
        now_unix_ms: i64,
    ) {
        self.cli_runtimes = response.runtimes;
        self.cli_loading = false;
        self.cli_error = None;
        self.cli_login_message = None;
        self.cli_refresh_status.mark_success(now_unix_ms);
    }

    pub fn apply_cli_runtime_instance_refresh_response(
        &mut self,
        response: CLIRuntimeRefreshResponse,
        now_unix_ms: i64,
    ) {
        for runtime in response.runtimes {
            match self
                .cli_runtimes
                .iter()
                .position(|existing| existing.runtime_id == runtime.runtime_id)
            {
                Some(index) => self.cli_runtimes[index] = runtime,
                None => self.cli_runtimes.push(runtime),
            }
        }
        self.cli_loading = false;
        self.cli_error = None;
        self.cli_login_message = None;
        self.cli_refresh_status.mark_success(now_unix_ms);
    }

    pub fn apply_refresh_failed(&mut self, error: String) {
        self.loading = false;
        self.error = Some(error);
    }

    pub fn apply_cli_runtime_refresh_failed(&mut self, error: String, now_unix_ms: i64) {
        self.cli_loading = false;
        self.cli_error = Some(error);
        self.cli_refresh_status.mark_failure(now_unix_ms);
    }

    pub fn apply_cli_runtime_action_failed(&mut self, error: String) {
        self.cli_loading = false;
        self.cli_error = Some(error);
    }

    pub fn apply_cli_runtime_proxy_url(&mut self, runtime_id: &str, proxy_url: Option<String>) {
        if let Some(runtime) = self
            .cli_runtimes
            .iter_mut()
            .find(|runtime| runtime.runtime_id == runtime_id)
        {
            runtime.proxy_url = proxy_url;
        }
        self.cli_error = None;
    }

    pub fn apply_cli_runtime_login_message(&mut self, message: String) {
        self.cli_error = None;
        self.cli_login_message = Some(message);
    }

    pub fn set_cli_runtime_draft(&mut self, draft: CLIRuntimeProviderDraft) {
        self.cli_error = None;
        self.cli_runtime_draft = Some(draft);
    }

    pub fn update_cli_runtime_draft_field(
        &mut self,
        field: CLIRuntimeProviderDraftField,
        value: String,
    ) {
        if let Some(draft) = &mut self.cli_runtime_draft {
            draft.set_text_field(field, value);
        }
    }

    pub fn set_cli_runtime_draft_enabled(&mut self, enabled: bool) {
        if let Some(draft) = &mut self.cli_runtime_draft {
            draft.enabled = enabled;
        }
    }

    pub fn clear_cli_runtime_draft(&mut self) {
        self.cli_runtime_draft = None;
    }

    pub fn apply_unavailable(&mut self, error: String) {
        self.loading = false;
        self.error = Some(error);
    }

    pub fn clear_for_workspace_switch(&mut self) {
        self.clear_gateway_scoped_state();
    }

    pub fn clear_for_gateway_switch(&mut self) {
        self.clear_gateway_scoped_state();
    }

    fn clear_gateway_scoped_state(&mut self) {
        self.configured_names.clear();
        self.provider_proxy_urls.clear();
        self.cli_runtimes.clear();
        self.cli_refresh_status = CLIRuntimeRefreshStatus::default();
        self.loading = false;
        self.cli_loading = false;
        self.error = None;
        self.cli_error = None;
        self.cli_login_message = None;
        self.cli_runtime_draft = None;
        self.expanded_cli_runtime_ids.clear();
    }

    pub fn insert_configured(&mut self, provider_id: String) {
        self.configured_names.insert(provider_id);
        self.error = None;
    }

    pub fn remove_configured(&mut self, provider_id: &str) {
        self.configured_names.remove(provider_id);
        self.error = None;
    }

    pub fn set_provider_proxy_url(&mut self, provider_id: String, proxy_url: String) {
        self.provider_proxy_urls.insert(provider_id, proxy_url);
        self.error = None;
    }

    pub fn remove_provider_proxy_url(&mut self, provider_id: &str) {
        self.provider_proxy_urls.remove(provider_id);
        self.error = None;
    }

    pub fn set_error(&mut self, error: Option<String>) {
        self.error = error;
    }
}

pub fn configured_provider_names_from_list(providers: &[ProviderSummary]) -> HashSet<String> {
    providers
        .iter()
        .filter(|provider| provider.api_key_configured)
        .map(|provider| catalog::canonical_provider_id(provider.name.as_str()))
        .collect()
}

pub fn provider_proxy_urls_from_list(providers: &[ProviderSummary]) -> HashMap<String, String> {
    providers
        .iter()
        .filter_map(|provider| {
            provider.proxy_url.as_ref().map(|proxy_url| {
                (
                    catalog::canonical_provider_id(provider.name.as_str()),
                    proxy_url.clone(),
                )
            })
        })
        .collect()
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ProviderListRefreshUnavailable {
    GatewayNotConnected,
    WorkspaceNotSelected,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ProviderListRefreshRequest {
    pub connection_id: u64,
    pub params: ProviderListParams,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CLIRuntimeRefreshRequest {
    pub connection_id: u64,
    pub params: CLIRuntimeRefreshParams,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum ProviderListRefreshPlan {
    Send(ProviderListRefreshRequest),
    Unavailable(ProviderListRefreshUnavailable),
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum CLIRuntimeRefreshPlan {
    Send(CLIRuntimeRefreshRequest),
    Skip(CLIRuntimeRefreshSkipReason),
    Unavailable(ProviderListRefreshUnavailable),
}

pub fn plan_provider_list_refresh(
    gateway_connected: bool,
    connection_id: Option<u64>,
    workspace_id: Option<String>,
) -> ProviderListRefreshPlan {
    let Some(connection_id) = gateway_connected.then_some(connection_id).flatten() else {
        return ProviderListRefreshPlan::Unavailable(
            ProviderListRefreshUnavailable::GatewayNotConnected,
        );
    };
    let Some(workspace_id) = workspace_id else {
        return ProviderListRefreshPlan::Unavailable(
            ProviderListRefreshUnavailable::WorkspaceNotSelected,
        );
    };

    ProviderListRefreshPlan::Send(ProviderListRefreshRequest {
        connection_id,
        params: provider_list_params(workspace_id),
    })
}

pub fn plan_cli_runtime_refresh(
    gateway_connected: bool,
    connection_id: Option<u64>,
    workspace_id: Option<String>,
) -> CLIRuntimeRefreshPlan {
    plan_cli_runtime_refresh_with_policy(
        gateway_connected,
        connection_id,
        workspace_id,
        &CLIRuntimeRefreshStatus::default(),
        CLIRuntimeRefreshTrigger::Manual,
        0,
    )
}

pub fn plan_cli_runtime_refresh_with_policy(
    gateway_connected: bool,
    connection_id: Option<u64>,
    workspace_id: Option<String>,
    refresh_status: &CLIRuntimeRefreshStatus,
    trigger: CLIRuntimeRefreshTrigger,
    now_unix_ms: i64,
) -> CLIRuntimeRefreshPlan {
    let Some(connection_id) = gateway_connected.then_some(connection_id).flatten() else {
        return CLIRuntimeRefreshPlan::Unavailable(
            ProviderListRefreshUnavailable::GatewayNotConnected,
        );
    };
    let Some(workspace_id) = workspace_id else {
        return CLIRuntimeRefreshPlan::Unavailable(
            ProviderListRefreshUnavailable::WorkspaceNotSelected,
        );
    };
    if let Some(skip_reason) = cli_runtime_refresh_skip_reason(refresh_status, trigger, now_unix_ms)
    {
        return CLIRuntimeRefreshPlan::Skip(skip_reason);
    }

    CLIRuntimeRefreshPlan::Send(CLIRuntimeRefreshRequest {
        connection_id,
        params: cli_runtime_refresh_params(workspace_id),
    })
}

pub fn plan_cli_runtime_instance_refresh(
    gateway_connected: bool,
    connection_id: Option<u64>,
    workspace_id: Option<String>,
    runtime_id: String,
) -> CLIRuntimeRefreshPlan {
    plan_cli_runtime_instance_refresh_with_policy(
        gateway_connected,
        connection_id,
        workspace_id,
        runtime_id,
        &CLIRuntimeRefreshStatus::default(),
        CLIRuntimeRefreshTrigger::Manual,
        0,
    )
}

pub fn plan_cli_runtime_instance_refresh_with_policy(
    gateway_connected: bool,
    connection_id: Option<u64>,
    workspace_id: Option<String>,
    runtime_id: String,
    refresh_status: &CLIRuntimeRefreshStatus,
    trigger: CLIRuntimeRefreshTrigger,
    now_unix_ms: i64,
) -> CLIRuntimeRefreshPlan {
    let Some(connection_id) = gateway_connected.then_some(connection_id).flatten() else {
        return CLIRuntimeRefreshPlan::Unavailable(
            ProviderListRefreshUnavailable::GatewayNotConnected,
        );
    };
    let Some(workspace_id) = workspace_id else {
        return CLIRuntimeRefreshPlan::Unavailable(
            ProviderListRefreshUnavailable::WorkspaceNotSelected,
        );
    };
    if let Some(skip_reason) = cli_runtime_refresh_skip_reason(refresh_status, trigger, now_unix_ms)
    {
        return CLIRuntimeRefreshPlan::Skip(skip_reason);
    }

    CLIRuntimeRefreshPlan::Send(CLIRuntimeRefreshRequest {
        connection_id,
        params: cli_runtime_instance_refresh_params(workspace_id, runtime_id),
    })
}

pub fn provider_list_refresh_matches_connection(
    refresh_connection_id: u64,
    current_connection_id: Option<u64>,
) -> bool {
    current_connection_id == Some(refresh_connection_id)
}

fn cli_runtime_refresh_skip_reason(
    refresh_status: &CLIRuntimeRefreshStatus,
    trigger: CLIRuntimeRefreshTrigger,
    now_unix_ms: i64,
) -> Option<CLIRuntimeRefreshSkipReason> {
    if refresh_status.in_flight {
        return Some(CLIRuntimeRefreshSkipReason::AlreadyRefreshing);
    }
    if trigger == CLIRuntimeRefreshTrigger::Manual {
        return None;
    }
    let next_refresh_at = refresh_status.next_auto_refresh_at_unix_ms?;
    if next_refresh_at <= now_unix_ms {
        return None;
    }
    if refresh_status.failure_count > 0 {
        Some(CLIRuntimeRefreshSkipReason::BackingOff {
            next_refresh_at_unix_ms: next_refresh_at,
        })
    } else {
        Some(CLIRuntimeRefreshSkipReason::Throttled {
            next_refresh_at_unix_ms: next_refresh_at,
        })
    }
}

fn cli_runtime_refresh_backoff_ms(failure_count: u32) -> i64 {
    let exponent = failure_count.saturating_sub(1).min(4);
    let multiplier = 1_i64 << exponent;
    (CLI_RUNTIME_REFRESH_BACKOFF_BASE_MS * multiplier).min(CLI_RUNTIME_REFRESH_BACKOFF_MAX_MS)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderModelSelectorProviderKind {
    ApiProvider {
        provider: String,
    },
    CLIRuntime {
        runtime_id: String,
        runtime_kind: CLIAgentRuntimeKind,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderModelSelectorProvider {
    pub id: String,
    pub label: String,
    pub kind: ProviderModelSelectorProviderKind,
}

#[derive(Clone, Debug)]
pub struct ProviderModelSelectorState {
    providers: Vec<ProviderSummary>,
    cli_runtimes: Vec<RuntimeSummary>,
    models: Vec<ProviderModelInfo>,
    selected_provider: Option<String>,
    selected_model: Option<String>,
    mode: ProviderModelSelectorMode,
    loading_providers: bool,
    loading_cli_runtimes: bool,
    loading_models: bool,
    error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderModelSelectorMode {
    Chat,
    Embeddings,
}

impl ProviderModelSelectorState {
    pub fn new(selected_provider: Option<String>, selected_model: Option<String>) -> Self {
        Self::new_with_mode(
            selected_provider,
            selected_model,
            ProviderModelSelectorMode::Chat,
        )
    }

    pub fn new_with_mode(
        selected_provider: Option<String>,
        selected_model: Option<String>,
        mode: ProviderModelSelectorMode,
    ) -> Self {
        Self {
            providers: Vec::new(),
            cli_runtimes: Vec::new(),
            models: Vec::new(),
            selected_provider,
            selected_model,
            mode,
            loading_providers: false,
            loading_cli_runtimes: false,
            loading_models: false,
            error: None,
        }
    }

    pub fn providers(&self) -> &[ProviderSummary] {
        &self.providers
    }

    pub fn provider_rows(&self) -> Vec<ProviderModelSelectorProvider> {
        let mut rows = self
            .providers
            .iter()
            .filter(|provider| {
                if self.mode == ProviderModelSelectorMode::Chat {
                    provider.name != "local"
                } else {
                    provider.capabilities.embeddings
                }
            })
            .map(|provider| ProviderModelSelectorProvider {
                id: provider.name.clone(),
                label: provider.name.clone(),
                kind: ProviderModelSelectorProviderKind::ApiProvider {
                    provider: provider.name.clone(),
                },
            })
            .collect::<Vec<_>>();

        if self.mode == ProviderModelSelectorMode::Chat {
            rows.extend(
                self.cli_runtimes
                    .iter()
                    .filter_map(cli_runtime_provider_row),
            );
        }
        rows
    }

    pub fn models(&self) -> &[ProviderModelInfo] {
        &self.models
    }

    pub fn selected_provider(&self) -> Option<&str> {
        self.selected_provider.as_deref()
    }

    pub fn selected_provider_label(&self) -> Option<String> {
        let selected = self.selected_provider.as_deref()?;
        self.provider_rows()
            .into_iter()
            .find(|row| row.id == selected)
            .map(|row| row.label)
            .or_else(|| Some(selected.to_owned()))
    }

    pub fn selected_model(&self) -> Option<&str> {
        self.selected_model.as_deref()
    }

    pub fn loading_providers(&self) -> bool {
        self.loading_providers || self.loading_cli_runtimes
    }

    pub fn loading_models(&self) -> bool {
        self.loading_models
    }

    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    pub fn selection_parts(&self) -> (Option<String>, Option<String>) {
        (self.selected_provider.clone(), self.selected_model.clone())
    }

    pub fn mark_providers_loading(&mut self) {
        self.loading_providers = true;
        self.loading_cli_runtimes = self.mode == ProviderModelSelectorMode::Chat;
        self.error = None;
    }

    pub fn apply_provider_list_success(&mut self, response: ProviderListResponse) {
        self.providers = response.providers;
        self.loading_providers = false;
        self.error = None;
    }

    pub fn apply_provider_list_error(&mut self, error: String) {
        self.loading_providers = false;
        self.error = Some(error);
    }

    pub fn apply_cli_runtime_list_success(&mut self, response: CLIRuntimeListResponse) {
        self.cli_runtimes = response.runtimes;
        self.loading_cli_runtimes = false;
        self.error = None;
    }

    pub fn apply_cli_runtime_list_error(&mut self, error: String) {
        self.loading_cli_runtimes = false;
        if self.providers.is_empty() {
            self.error = Some(error);
        }
    }

    pub fn preload_selected_provider_models(&mut self) -> Option<String> {
        let provider = self.selected_provider.clone()?;
        self.loading_models = true;
        self.error = None;
        Some(provider)
    }

    pub fn select_provider(&mut self, provider_name: String) -> String {
        self.selected_provider = Some(provider_name.clone());
        self.selected_model = None;
        self.models.clear();
        self.loading_models = true;
        self.error = None;
        provider_name
    }

    pub fn set_selected_model(&mut self, model_id: String) {
        self.selected_model = Some(model_id);
    }

    pub fn apply_provider_models_success(&mut self, response: ProviderListModelsResponse) -> bool {
        if self.selected_provider.as_deref() != Some(response.provider.as_str()) {
            return false;
        }

        self.models = response.models;
        self.loading_models = false;
        self.error = None;
        true
    }

    pub fn apply_provider_models_error(&mut self, provider_name: &str, error: String) -> bool {
        if self.selected_provider.as_deref() != Some(provider_name) {
            return false;
        }

        self.loading_models = false;
        self.error = Some(error);
        true
    }
}

pub fn cli_runtime_provider_key(runtime_id: &str) -> String {
    format!("{CLI_RUNTIME_PROVIDER_PREFIX}{runtime_id}")
}

pub fn runtime_id_from_cli_runtime_provider_key(provider_key: &str) -> Option<&str> {
    provider_key.strip_prefix(CLI_RUNTIME_PROVIDER_PREFIX)
}

pub fn resolve_cli_runtime_execution_backend(
    selected_provider: Option<&str>,
    cli_runtimes: &[RuntimeSummary],
    settings: Option<&GatewayCliRuntimeSettings>,
) -> Result<Option<AgentExecutionBackend>, String> {
    let Some(provider_key) = selected_provider
        .map(str::trim)
        .filter(|key| !key.is_empty())
    else {
        return Ok(None);
    };
    let Some(runtime_id) = runtime_id_from_cli_runtime_provider_key(provider_key)
        .map(str::trim)
        .filter(|runtime_id| !runtime_id.is_empty())
    else {
        return Ok(None);
    };

    if let Some(runtime) = cli_runtimes
        .iter()
        .find(|runtime| runtime.runtime_id == runtime_id && runtime.enabled)
    {
        return Ok(Some(AgentExecutionBackend::CLIAgentRuntime {
            runtime_id: runtime.runtime_id.clone(),
            runtime_kind: runtime.kind,
        }));
    }

    if let Some(instance) = settings.and_then(|settings| {
        settings
            .instances
            .iter()
            .find(|instance| instance.id == runtime_id && instance.enabled)
    }) {
        return Ok(Some(AgentExecutionBackend::CLIAgentRuntime {
            runtime_id: instance.id.clone(),
            runtime_kind: instance.kind,
        }));
    }

    Err(format!(
        "CLI runtime `{runtime_id}` is not available for message submission"
    ))
}

pub fn cli_runtime_list_params(workspace_id: impl Into<String>) -> CLIRuntimeListParams {
    CLIRuntimeListParams {
        workspace_id: workspace_id.into(),
    }
}

pub fn cli_runtime_list_models_params(
    workspace_id: impl Into<String>,
    runtime_id: impl Into<String>,
) -> CLIRuntimeListModelsParams {
    CLIRuntimeListModelsParams {
        workspace_id: workspace_id.into(),
        runtime_id: runtime_id.into(),
    }
}

pub fn provider_models_from_cli_runtime_models(
    provider_key: &str,
    models: Vec<RuntimeModelInfo>,
) -> Vec<ProviderModelInfo> {
    models
        .into_iter()
        .map(|model| provider_model_from_runtime_model(provider_key, model))
        .collect()
}

pub fn provider_models_response_from_cli_runtime_models_response(
    provider_key: String,
    response: CLIRuntimeListModelsResponse,
) -> ProviderListModelsResponse {
    ProviderListModelsResponse {
        provider: provider_key.clone(),
        models: provider_models_from_cli_runtime_models(provider_key.as_str(), response.models),
    }
}

fn cli_runtime_provider_row(runtime: &RuntimeSummary) -> Option<ProviderModelSelectorProvider> {
    if !runtime.enabled
        || !runtime.capabilities.supports_threads
        || !runtime.capabilities.supports_model_list
        || !matches!(
            runtime.status,
            RuntimeStatus::Ready | RuntimeStatus::Degraded { .. }
        )
    {
        return None;
    }

    Some(ProviderModelSelectorProvider {
        id: cli_runtime_provider_key(runtime.runtime_id.as_str()),
        label: runtime.display_name.clone(),
        kind: ProviderModelSelectorProviderKind::CLIRuntime {
            runtime_id: runtime.runtime_id.clone(),
            runtime_kind: runtime.kind,
        },
    })
}

fn provider_model_from_runtime_model(
    provider_key: &str,
    model: RuntimeModelInfo,
) -> ProviderModelInfo {
    let supports_reasoning = model
        .supports_reasoning
        .or_else(|| (!model.effort_options.is_empty()).then_some(true));
    let reasoning = supports_reasoning.map(|supported| ProviderModelReasoningCapabilities {
        supported: Some(supported),
        effort_options: model.effort_options.clone(),
        default_effort: None,
        mandatory: None,
        supports_token_budget: None,
        source: Some(ReasoningCapabilitySource::CliMetadata),
    });

    ProviderModelInfo {
        id: model.id,
        name: model.name,
        description: model.description,
        created: None,
        provider: provider_key.to_owned(),
        owned_by: None,
        limits: ProviderModelLimits {
            max_input_tokens: model.max_input_tokens,
            max_output_tokens: model.max_output_tokens,
            context_window: model.max_input_tokens,
        },
        capabilities: ProviderModelCapabilities {
            vision: model.supports_vision,
            tool_calling: None,
            json_output: None,
            streaming: Some(true),
            embeddings: None,
            thinking: supports_reasoning,
            reasoning,
            fine_tuning: None,
            input_modalities: (!model.input_modalities.is_empty())
                .then_some(model.input_modalities),
            output_modalities: (!model.output_modalities.is_empty())
                .then_some(model.output_modalities),
        },
        pricing: None,
        active: model.active,
        family: model.family,
        lifecycle_status: None,
    }
}

impl Default for ProviderModelSelectorState {
    fn default() -> Self {
        Self::new(None, None)
    }
}

pub fn provider_list_params(workspace_id: impl Into<String>) -> ProviderListParams {
    ProviderListParams {
        workspace_id: workspace_id.into(),
    }
}

pub fn cli_runtime_refresh_params(workspace_id: impl Into<String>) -> CLIRuntimeRefreshParams {
    CLIRuntimeRefreshParams {
        workspace_id: workspace_id.into(),
        runtime_id: None,
    }
}

pub fn cli_runtime_instance_refresh_params(
    workspace_id: impl Into<String>,
    runtime_id: impl Into<String>,
) -> CLIRuntimeRefreshParams {
    CLIRuntimeRefreshParams {
        workspace_id: workspace_id.into(),
        runtime_id: Some(runtime_id.into()),
    }
}

pub fn provider_list_models_params(
    workspace_id: impl Into<String>,
    provider: impl Into<String>,
) -> ProviderListModelsParams {
    ProviderListModelsParams {
        workspace_id: workspace_id.into(),
        provider: provider.into(),
    }
}

pub fn provider_list_embedding_models_params(
    workspace_id: impl Into<String>,
    provider: impl Into<String>,
) -> ProviderListModelsParams {
    ProviderListModelsParams {
        workspace_id: workspace_id.into(),
        provider: provider.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime_summary(id: &str, display_name: &str, status: RuntimeStatus) -> RuntimeSummary {
        RuntimeSummary {
            runtime_id: id.to_owned(),
            kind: CLIAgentRuntimeKind::Codex,
            display_name: display_name.to_owned(),
            enabled: true,
            status,
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
            diagnostics: Vec::new(),
            recent_stderr: Vec::new(),
        }
    }

    #[test]
    fn provider_list_state_tracks_refresh_and_filter() {
        let mut state = ProviderListState::default();

        assert_eq!(state.filter(), ProviderFilter::Api);
        assert!(!state.loading());

        state.set_filter(ProviderFilter::Connected);
        state.mark_refresh_started();
        assert_eq!(state.filter(), ProviderFilter::Connected);
        assert!(state.loading());
        assert!(state.error().is_none());

        state.apply_refresh_response(ProviderListResponse {
            providers: vec![ProviderSummary {
                name: "OpenAI".to_owned(),
                capabilities: Default::default(),
                api_key_configured: true,
                proxy_url: None,
            }],
        });
        assert!(!state.loading());
        assert!(state.is_configured("openai"));

        state.apply_refresh_failed("failed".to_owned());
        assert_eq!(state.error(), Some("failed"));
        assert!(!state.loading());
    }

    #[test]
    fn provider_list_state_clears_gateway_scoped_cli_runtime_data() {
        let mut state = ProviderListState::default();
        state.configured_names.insert("openai".to_owned());
        state
            .cli_runtimes
            .push(runtime_summary("codex", "Codex CLI", RuntimeStatus::Ready));
        state.cli_refresh_status.mark_success(1_000);
        state.cli_loading = true;
        state.cli_error = Some("old cli error".to_owned());
        state.cli_login_message = Some("old login message".to_owned());
        state.cli_runtime_draft = Some(CLIRuntimeProviderDraft::create_for_kind(
            None,
            CLIAgentRuntimeKind::Codex,
        ));
        state.expanded_cli_runtime_ids.insert("codex".to_owned());

        state.clear_for_gateway_switch();

        assert!(state.configured_names().is_empty());
        assert!(state.cli_runtimes().is_empty());
        assert_eq!(
            state.cli_refresh_status(),
            &CLIRuntimeRefreshStatus::default()
        );
        assert!(!state.cli_loading());
        assert!(state.cli_error().is_none());
        assert!(state.cli_login_message().is_none());
        assert!(state.cli_runtime_draft().is_none());
        assert!(state.expanded_cli_runtime_ids().is_empty());
    }

    #[test]
    fn configured_provider_names_are_canonicalized() {
        let providers = vec![
            ProviderSummary {
                name: "AWS_Bedrock".to_owned(),
                capabilities: Default::default(),
                api_key_configured: true,
                proxy_url: None,
            },
            ProviderSummary {
                name: "lm_studio".to_owned(),
                capabilities: Default::default(),
                api_key_configured: true,
                proxy_url: None,
            },
        ];

        let configured = configured_provider_names_from_list(&providers);

        assert!(configured.contains("bedrock"));
        assert!(configured.contains("lmstudio"));
    }

    #[test]
    fn provider_list_refresh_plan_requires_gateway_and_workspace() {
        assert!(matches!(
            plan_provider_list_refresh(false, Some(7), Some("ws".to_owned())),
            ProviderListRefreshPlan::Unavailable(
                ProviderListRefreshUnavailable::GatewayNotConnected
            )
        ));
        assert!(matches!(
            plan_provider_list_refresh(true, None, Some("ws".to_owned())),
            ProviderListRefreshPlan::Unavailable(
                ProviderListRefreshUnavailable::GatewayNotConnected
            )
        ));
        assert!(matches!(
            plan_provider_list_refresh(true, Some(7), None),
            ProviderListRefreshPlan::Unavailable(
                ProviderListRefreshUnavailable::WorkspaceNotSelected
            )
        ));

        let plan = plan_provider_list_refresh(true, Some(7), Some("ws".to_owned()));
        let ProviderListRefreshPlan::Send(request) = plan else {
            panic!("expected send plan");
        };
        assert_eq!(request.connection_id, 7);
        assert_eq!(request.params.workspace_id, "ws");
    }

    #[test]
    fn cli_runtime_refresh_plan_requires_gateway_and_workspace() {
        assert!(matches!(
            plan_cli_runtime_refresh(false, None, Some("ws".to_owned())),
            CLIRuntimeRefreshPlan::Unavailable(ProviderListRefreshUnavailable::GatewayNotConnected)
        ));
        assert!(matches!(
            plan_cli_runtime_refresh(true, Some(7), None),
            CLIRuntimeRefreshPlan::Unavailable(
                ProviderListRefreshUnavailable::WorkspaceNotSelected
            )
        ));

        let plan = plan_cli_runtime_refresh(true, Some(7), Some("ws".to_owned()));
        let CLIRuntimeRefreshPlan::Send(request) = plan else {
            panic!("expected send plan");
        };
        assert_eq!(request.connection_id, 7);
        assert_eq!(request.params.workspace_id, "ws");
        assert!(request.params.runtime_id.is_none());
    }

    #[test]
    fn provider_list_state_tracks_cli_runtime_refresh_separately() {
        let mut state = ProviderListState::default();

        state.mark_cli_runtime_refresh_started(1_000);
        assert!(state.cli_loading());
        assert!(state.loading() == false);
        assert!(state.cli_refresh_status().in_flight);
        state.apply_cli_runtime_refresh_response(
            CLIRuntimeRefreshResponse {
                runtimes: Vec::new(),
            },
            2_000,
        );
        assert!(!state.cli_loading());
        assert!(state.cli_error().is_none());
        assert_eq!(
            state.cli_refresh_status().last_success_at_unix_ms,
            Some(2_000)
        );
        assert_eq!(
            state.cli_refresh_status().next_auto_refresh_at_unix_ms,
            Some(2_000 + CLI_RUNTIME_REFRESH_AUTO_INTERVAL_MS)
        );

        state.apply_cli_runtime_refresh_failed("failed".to_owned(), 3_000);
        assert_eq!(state.cli_error(), Some("failed"));
        assert!(state.error().is_none());
        assert_eq!(state.cli_refresh_status().failure_count, 1);
        assert_eq!(
            state.cli_refresh_status().next_auto_refresh_at_unix_ms,
            Some(3_000 + CLI_RUNTIME_REFRESH_BACKOFF_BASE_MS)
        );
    }

    #[test]
    fn cli_runtime_auto_refresh_policy_throttles_and_backs_off() {
        let mut status = CLIRuntimeRefreshStatus::default();
        status.mark_success(1_000);

        assert!(matches!(
            plan_cli_runtime_refresh_with_policy(
                true,
                Some(7),
                Some("ws".to_owned()),
                &status,
                CLIRuntimeRefreshTrigger::Auto,
                10_000,
            ),
            CLIRuntimeRefreshPlan::Skip(CLIRuntimeRefreshSkipReason::Throttled {
                next_refresh_at_unix_ms
            }) if next_refresh_at_unix_ms == 1_000 + CLI_RUNTIME_REFRESH_AUTO_INTERVAL_MS
        ));
        assert!(matches!(
            plan_cli_runtime_refresh_with_policy(
                true,
                Some(7),
                Some("ws".to_owned()),
                &status,
                CLIRuntimeRefreshTrigger::Manual,
                10_000,
            ),
            CLIRuntimeRefreshPlan::Send(_)
        ));

        status.mark_failure(20_000);
        assert!(matches!(
            plan_cli_runtime_refresh_with_policy(
                true,
                Some(7),
                Some("ws".to_owned()),
                &status,
                CLIRuntimeRefreshTrigger::Auto,
                30_000,
            ),
            CLIRuntimeRefreshPlan::Skip(CLIRuntimeRefreshSkipReason::BackingOff {
                next_refresh_at_unix_ms
            }) if next_refresh_at_unix_ms == 20_000 + CLI_RUNTIME_REFRESH_BACKOFF_BASE_MS
        ));
    }

    #[test]
    fn cli_runtime_refresh_policy_prevents_parallel_refreshes() {
        let mut status = CLIRuntimeRefreshStatus::default();
        status.mark_started(1_000);

        assert!(matches!(
            plan_cli_runtime_instance_refresh_with_policy(
                true,
                Some(7),
                Some("ws".to_owned()),
                "codex".to_owned(),
                &status,
                CLIRuntimeRefreshTrigger::Manual,
                2_000,
            ),
            CLIRuntimeRefreshPlan::Skip(CLIRuntimeRefreshSkipReason::AlreadyRefreshing)
        ));
    }

    #[test]
    fn provider_list_refresh_connection_guard_detects_stale_results() {
        assert!(provider_list_refresh_matches_connection(7, Some(7)));
        assert!(!provider_list_refresh_matches_connection(7, Some(8)));
        assert!(!provider_list_refresh_matches_connection(7, None));
    }

    #[test]
    fn provider_model_selector_tracks_provider_and_model_loading() {
        let mut state = ProviderModelSelectorState::new(None, None);

        state.mark_providers_loading();
        assert!(state.loading_providers());
        assert!(state.error().is_none());

        state.apply_provider_list_success(ProviderListResponse {
            providers: vec![ProviderSummary {
                name: "openai".to_owned(),
                capabilities: Default::default(),
                api_key_configured: true,
                proxy_url: None,
            }],
        });
        assert!(state.loading_providers());
        state.apply_cli_runtime_list_success(CLIRuntimeListResponse {
            runtimes: Vec::new(),
        });
        assert!(!state.loading_providers());
        assert_eq!(state.providers().len(), 1);
        assert_eq!(state.provider_rows().len(), 1);

        let provider = state.select_provider("openai".to_owned());
        assert_eq!(provider, "openai");
        assert_eq!(state.selected_provider(), Some("openai"));
        assert_eq!(state.selected_model(), None);
        assert!(state.models().is_empty());
        assert!(state.loading_models());

        let applied = state.apply_provider_models_success(ProviderListModelsResponse {
            provider: "openai".to_owned(),
            models: vec![ProviderModelInfo {
                id: "gpt-5.4".to_owned(),
                name: Some("GPT 5.4".to_owned()),
                description: None,
                created: None,
                provider: "openai".to_owned(),
                owned_by: None,
                limits: Default::default(),
                capabilities: Default::default(),
                pricing: None,
                active: Some(true),
                family: None,
                lifecycle_status: None,
            }],
        });
        assert!(applied);
        assert!(!state.loading_models());
        assert_eq!(state.models().len(), 1);

        state.set_selected_model("gpt-5.4".to_owned());
        assert_eq!(
            state.selection_parts(),
            (Some("openai".to_owned()), Some("gpt-5.4".to_owned()))
        );
    }

    #[test]
    fn provider_model_selector_ignores_stale_model_responses() {
        let mut state = ProviderModelSelectorState::new(Some("openai".to_owned()), None);
        assert_eq!(
            state.preload_selected_provider_models().as_deref(),
            Some("openai")
        );

        state.select_provider("anthropic".to_owned());
        let applied = state.apply_provider_models_success(ProviderListModelsResponse {
            provider: "openai".to_owned(),
            models: Vec::new(),
        });

        assert!(!applied);
        assert_eq!(state.selected_provider(), Some("anthropic"));
        assert!(state.loading_models());

        let applied = state.apply_provider_models_error("openai", "stale load failed".to_owned());
        assert!(!applied);
        assert!(state.error().is_none());
    }

    #[test]
    fn provider_model_selector_exposes_ready_cli_runtime_rows() {
        let mut state = ProviderModelSelectorState::new(None, None);
        state.apply_provider_list_success(ProviderListResponse {
            providers: vec![ProviderSummary {
                name: "openai".to_owned(),
                capabilities: Default::default(),
                api_key_configured: true,
                proxy_url: None,
            }],
        });
        state.apply_cli_runtime_list_success(CLIRuntimeListResponse {
            runtimes: vec![runtime_summary(
                "codex_work",
                "Codex Work",
                RuntimeStatus::Ready,
            )],
        });

        let rows = state.provider_rows();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|row| row.id == "openai"));
        let codex = rows
            .iter()
            .find(|row| row.id == cli_runtime_provider_key("codex_work"))
            .expect("codex runtime row");
        assert_eq!(codex.label, "Codex Work");
        assert_eq!(
            codex.kind,
            ProviderModelSelectorProviderKind::CLIRuntime {
                runtime_id: "codex_work".to_owned(),
                runtime_kind: CLIAgentRuntimeKind::Codex,
            }
        );
    }

    #[test]
    fn provider_model_selector_embedding_mode_filters_providers_and_runtimes() {
        let mut state = ProviderModelSelectorState::new_with_mode(
            None,
            None,
            ProviderModelSelectorMode::Embeddings,
        );
        state.mark_providers_loading();
        assert!(state.loading_providers());

        state.apply_provider_list_success(ProviderListResponse {
            providers: vec![
                ProviderSummary {
                    name: "openai".to_owned(),
                    capabilities: pioneer_protocol::ProviderSummaryCapabilities {
                        embeddings: true,
                    },
                    api_key_configured: true,
                    proxy_url: None,
                },
                ProviderSummary {
                    name: "anthropic".to_owned(),
                    capabilities: Default::default(),
                    api_key_configured: true,
                    proxy_url: None,
                },
            ],
        });
        assert!(!state.loading_providers());
        state.apply_cli_runtime_list_success(CLIRuntimeListResponse {
            runtimes: vec![runtime_summary(
                "codex_work",
                "Codex Work",
                RuntimeStatus::Ready,
            )],
        });

        let rows = state.provider_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "openai");

        state.select_provider("openai".to_owned());
        let applied = state.apply_provider_models_success(ProviderListModelsResponse {
            provider: "openai".to_owned(),
            models: vec![ProviderModelInfo {
                id: "text-embedding-3-small".to_owned(),
                name: None,
                description: None,
                created: None,
                provider: "openai".to_owned(),
                owned_by: None,
                limits: Default::default(),
                capabilities: ProviderModelCapabilities {
                    embeddings: Some(true),
                    ..Default::default()
                },
                pricing: None,
                active: Some(true),
                family: None,
                lifecycle_status: None,
            }],
        });

        assert!(applied);
        assert_eq!(state.models().len(), 1);
        assert_eq!(state.models()[0].id, "text-embedding-3-small");
    }

    #[test]
    fn provider_model_selector_hides_local_from_chat_and_shows_it_for_embeddings() {
        let providers = vec![
            ProviderSummary {
                name: "openai".to_owned(),
                capabilities: pioneer_protocol::ProviderSummaryCapabilities { embeddings: true },
                api_key_configured: true,
                proxy_url: None,
            },
            ProviderSummary {
                name: "local".to_owned(),
                capabilities: pioneer_protocol::ProviderSummaryCapabilities { embeddings: true },
                api_key_configured: true,
                proxy_url: None,
            },
        ];

        let mut chat = ProviderModelSelectorState::new(None, None);
        chat.apply_provider_list_success(ProviderListResponse {
            providers: providers.clone(),
        });
        let chat_rows = chat.provider_rows();
        assert!(chat_rows.iter().any(|row| row.id == "openai"));
        assert!(!chat_rows.iter().any(|row| row.id == "local"));

        let mut embeddings = ProviderModelSelectorState::new_with_mode(
            None,
            None,
            ProviderModelSelectorMode::Embeddings,
        );
        embeddings.apply_provider_list_success(ProviderListResponse { providers });
        let embedding_rows = embeddings.provider_rows();
        assert!(embedding_rows.iter().any(|row| row.id == "openai"));
        assert!(embedding_rows.iter().any(|row| row.id == "local"));
    }

    #[test]
    fn cli_runtime_models_map_to_provider_model_rows() {
        let provider_key = cli_runtime_provider_key("codex_work");
        let response = provider_models_response_from_cli_runtime_models_response(
            provider_key.clone(),
            CLIRuntimeListModelsResponse {
                runtime_id: "codex_work".to_owned(),
                models: vec![RuntimeModelInfo {
                    id: "gpt-5.4".to_owned(),
                    name: Some("GPT 5.4".to_owned()),
                    description: Some("Reasoning model".to_owned()),
                    family: Some("gpt-5".to_owned()),
                    is_custom: false,
                    active: Some(true),
                    effort_options: vec!["low".to_owned(), "high".to_owned()],
                    input_modalities: vec!["text".to_owned()],
                    output_modalities: vec!["text".to_owned()],
                    supports_reasoning: None,
                    supports_vision: Some(false),
                    max_input_tokens: Some(128_000),
                    max_output_tokens: Some(16_000),
                }],
                diagnostics: Vec::new(),
                refreshed_at_unix_ms: Some(1),
            },
        );

        assert_eq!(response.provider, provider_key);
        assert_eq!(response.models.len(), 1);
        assert_eq!(response.models[0].id, "gpt-5.4");
        assert_eq!(
            response.models[0].provider,
            cli_runtime_provider_key("codex_work")
        );
        assert_eq!(response.models[0].name.as_deref(), Some("GPT 5.4"));
        assert_eq!(response.models[0].capabilities.thinking, Some(true));
        let reasoning = response.models[0]
            .capabilities
            .reasoning
            .as_ref()
            .expect("CLI reasoning metadata should be preserved");
        assert_eq!(reasoning.supported, Some(true));
        assert_eq!(
            reasoning.effort_options,
            vec!["low".to_owned(), "high".to_owned()]
        );
        assert_eq!(
            reasoning.source,
            Some(ReasoningCapabilitySource::CliMetadata)
        );
    }

    #[test]
    fn cli_runtime_backend_resolves_from_runtime_cache() {
        let backend = resolve_cli_runtime_execution_backend(
            Some(cli_runtime_provider_key("codex_work").as_str()),
            &[runtime_summary(
                "codex_work",
                "Codex Work",
                RuntimeStatus::Ready,
            )],
            None,
        )
        .expect("CLI runtime provider should resolve")
        .expect("CLI runtime backend should be selected");

        assert_eq!(
            backend,
            AgentExecutionBackend::CLIAgentRuntime {
                runtime_id: "codex_work".to_owned(),
                runtime_kind: CLIAgentRuntimeKind::Codex,
            }
        );
    }

    #[test]
    fn cli_runtime_backend_falls_back_to_gateway_settings() {
        let settings = GatewayCliRuntimeSettings {
            instances: vec![pioneer_protocol::GatewayCliRuntimeInstanceSettings {
                id: "codex".to_owned(),
                kind: CLIAgentRuntimeKind::Codex,
                display_name: "Codex CLI".to_owned(),
                enabled: true,
                binary_path: "codex".to_owned(),
                home_path: "~/.codex".to_owned(),
                shadow_home_path: None,
            }],
        };

        let backend =
            resolve_cli_runtime_execution_backend(Some("cli_runtime:codex"), &[], Some(&settings))
                .expect("settings fallback should resolve")
                .expect("CLI runtime backend should be selected");

        assert_eq!(
            backend,
            AgentExecutionBackend::CLIAgentRuntime {
                runtime_id: "codex".to_owned(),
                runtime_kind: CLIAgentRuntimeKind::Codex,
            }
        );
    }

    #[test]
    fn cli_runtime_backend_rejects_missing_cli_runtime_provider() {
        let error = resolve_cli_runtime_execution_backend(Some("cli_runtime:missing"), &[], None)
            .expect_err("missing CLI runtime must not fall through to API provider");

        assert!(error.contains("missing"));
    }

    #[test]
    fn provider_model_list_params_preserve_workspace_and_provider() {
        assert_eq!(provider_list_params("ws").workspace_id, "ws");

        let params = provider_list_models_params("ws", "openai");
        assert_eq!(params.workspace_id, "ws");
        assert_eq!(params.provider, "openai");

        let params = provider_list_embedding_models_params("ws", "openrouter");
        assert_eq!(params.workspace_id, "ws");
        assert_eq!(params.provider, "openrouter");

        let params = cli_runtime_list_models_params("ws", "codex_work");
        assert_eq!(params.workspace_id, "ws");
        assert_eq!(params.runtime_id, "codex_work");
    }
}
