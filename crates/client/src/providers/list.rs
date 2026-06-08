//! Provider list state.

use super::{catalog, selectors::ProviderFilter};
use pioneer_protocol::{
    ProviderListModelsParams, ProviderListModelsResponse, ProviderListParams, ProviderListResponse,
    ProviderModelInfo, ProviderSummary,
};
use std::collections::HashSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderListState {
    configured_names: HashSet<String>,
    filter: ProviderFilter,
    loading: bool,
    error: Option<String>,
}

impl Default for ProviderListState {
    fn default() -> Self {
        Self {
            configured_names: HashSet::new(),
            filter: ProviderFilter::All,
            loading: false,
            error: None,
        }
    }
}

impl ProviderListState {
    pub fn configured_names(&self) -> &HashSet<String> {
        &self.configured_names
    }

    pub fn is_configured(&self, provider_id: &str) -> bool {
        self.configured_names.contains(provider_id)
    }

    pub fn filter(&self) -> ProviderFilter {
        self.filter
    }

    pub fn loading(&self) -> bool {
        self.loading
    }

    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    pub fn set_filter(&mut self, filter: ProviderFilter) {
        self.filter = filter;
    }

    pub fn mark_refresh_started(&mut self) {
        self.loading = true;
        self.error = None;
    }

    pub fn apply_refresh_success(&mut self, configured_names: HashSet<String>) {
        self.configured_names = configured_names;
        self.loading = false;
        self.error = None;
    }

    pub fn apply_refresh_response(&mut self, response: ProviderListResponse) {
        self.apply_refresh_success(configured_provider_names_from_list(
            response.providers.as_slice(),
        ));
    }

    pub fn apply_refresh_failed(&mut self, error: String) {
        self.loading = false;
        self.error = Some(error);
    }

    pub fn apply_unavailable(&mut self, error: String) {
        self.loading = false;
        self.error = Some(error);
    }

    pub fn clear_for_workspace_switch(&mut self) {
        self.configured_names.clear();
        self.loading = false;
        self.error = None;
    }

    pub fn insert_configured(&mut self, provider_id: String) {
        self.configured_names.insert(provider_id);
        self.error = None;
    }

    pub fn remove_configured(&mut self, provider_id: &str) {
        self.configured_names.remove(provider_id);
        self.error = None;
    }

    pub fn set_error(&mut self, error: Option<String>) {
        self.error = error;
    }
}

pub fn configured_provider_names_from_list(providers: &[ProviderSummary]) -> HashSet<String> {
    providers
        .iter()
        .map(|provider| catalog::canonical_provider_id(provider.name.as_str()))
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
pub enum ProviderListRefreshPlan {
    Send(ProviderListRefreshRequest),
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

pub fn provider_list_refresh_matches_connection(
    refresh_connection_id: u64,
    current_connection_id: Option<u64>,
) -> bool {
    current_connection_id == Some(refresh_connection_id)
}

#[derive(Clone, Debug)]
pub struct ProviderModelSelectorState {
    providers: Vec<ProviderSummary>,
    models: Vec<ProviderModelInfo>,
    selected_provider: Option<String>,
    selected_model: Option<String>,
    loading_providers: bool,
    loading_models: bool,
    error: Option<String>,
}

impl ProviderModelSelectorState {
    pub fn new(selected_provider: Option<String>, selected_model: Option<String>) -> Self {
        Self {
            providers: Vec::new(),
            models: Vec::new(),
            selected_provider,
            selected_model,
            loading_providers: false,
            loading_models: false,
            error: None,
        }
    }

    pub fn providers(&self) -> &[ProviderSummary] {
        &self.providers
    }

    pub fn models(&self) -> &[ProviderModelInfo] {
        &self.models
    }

    pub fn selected_provider(&self) -> Option<&str> {
        self.selected_provider.as_deref()
    }

    pub fn selected_model(&self) -> Option<&str> {
        self.selected_model.as_deref()
    }

    pub fn loading_providers(&self) -> bool {
        self.loading_providers
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

pub fn provider_list_models_params(
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

    #[test]
    fn provider_list_state_tracks_refresh_and_filter() {
        let mut state = ProviderListState::default();

        assert_eq!(state.filter(), ProviderFilter::All);
        assert!(!state.loading());

        state.set_filter(ProviderFilter::Connected);
        state.mark_refresh_started();
        assert_eq!(state.filter(), ProviderFilter::Connected);
        assert!(state.loading());
        assert!(state.error().is_none());

        state.apply_refresh_response(ProviderListResponse {
            providers: vec![ProviderSummary {
                name: "OpenAI".to_owned(),
            }],
        });
        assert!(!state.loading());
        assert!(state.is_configured("openai"));

        state.apply_refresh_failed("failed".to_owned());
        assert_eq!(state.error(), Some("failed"));
        assert!(!state.loading());
    }

    #[test]
    fn configured_provider_names_are_canonicalized() {
        let providers = vec![
            ProviderSummary {
                name: "AWS_Bedrock".to_owned(),
            },
            ProviderSummary {
                name: "lm_studio".to_owned(),
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
            }],
        });
        assert!(!state.loading_providers());
        assert_eq!(state.providers().len(), 1);

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
    fn provider_model_list_params_preserve_workspace_and_provider() {
        assert_eq!(provider_list_params("ws").workspace_id, "ws");

        let params = provider_list_models_params("ws", "openai");
        assert_eq!(params.workspace_id, "ws");
        assert_eq!(params.provider, "openai");
    }
}
