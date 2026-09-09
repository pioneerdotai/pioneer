use pioneer_client::providers::{
    cli_runtime_settings::CLIRuntimeProviderDraft, operations::*, runtime::*, store::*,
    types::RuntimeSummary,
};
use std::{collections::HashSet, sync::Arc};
#[derive(Default)]
pub(crate) struct ProviderInput {
    pub(crate) catalog: Option<Arc<ProviderCollectionPublication>>,
    pub(crate) runtimes: Option<Arc<ProviderRuntimePublication>>,
    pub(crate) operation: Option<Arc<ProviderOperationPublication>>,
    pub(crate) error: Option<String>,
    pub(crate) cli_error: Option<String>,
    pub(crate) login_message: Option<String>,
    draft: Option<CLIRuntimeProviderDraft>,
    expanded: HashSet<String>,
}
impl ProviderInput {
    pub(crate) fn clear_scope(&mut self) {
        self.expanded.clear();
        self.draft = None;
        self.login_message = None;
    }
    pub(crate) fn configured_names(&self) -> HashSet<String> {
        self.catalog
            .iter()
            .flat_map(|p| p.providers().iter())
            .filter(|row| row.provider().api_key_configured)
            .map(|row| pioneer_client::providers::catalog::canonical_provider_id(row.id()))
            .collect()
    }
    pub(crate) fn is_configured(&self, id: &str) -> bool {
        self.catalog.iter().flat_map(|p| p.providers()).any(|row| {
            pioneer_client::providers::catalog::canonical_provider_id(row.id()) == id
                && row.provider().api_key_configured
        })
    }
    pub(crate) fn provider_proxy_url(&self, id: &str) -> Option<&str> {
        self.catalog
            .as_ref()?
            .providers()
            .iter()
            .find(|row| pioneer_client::providers::catalog::canonical_provider_id(row.id()) == id)?
            .provider()
            .proxy_url
            .as_deref()
    }
    pub(crate) fn cli_runtimes(&self) -> Vec<RuntimeSummary> {
        self.runtimes
            .iter()
            .flat_map(|p| p.runtimes())
            .map(|row| row.runtime().clone())
            .collect()
    }
    pub(crate) fn loading(&self) -> bool {
        self.catalog
            .as_ref()
            .is_some_and(|p| p.request() == ProviderLoadState::Loading)
            || self
                .operation
                .as_ref()
                .is_some_and(|p| p.request() == ProviderLoadState::Loading)
    }
    pub(crate) fn cli_loading(&self) -> bool {
        self.runtimes
            .as_ref()
            .is_some_and(|p| *p.request() == ProviderRuntimeRequestState::Loading)
    }
    pub(crate) fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }
    pub(crate) fn cli_error(&self) -> Option<&str> {
        self.cli_error.as_deref()
    }
    pub(crate) fn cli_login_message(&self) -> Option<&str> {
        self.login_message.as_deref()
    }
    pub(crate) fn expanded_cli_runtime_ids(&self) -> &HashSet<String> {
        &self.expanded
    }
    pub(crate) fn toggle_cli_runtime_expanded(&mut self, id: String) {
        if !self.expanded.insert(id.clone()) {
            self.expanded.remove(&id);
        }
    }
    pub(crate) fn set_cli_runtime_draft(&mut self, draft: CLIRuntimeProviderDraft) {
        self.draft = Some(draft);
    }
    pub(crate) fn set_cli_runtime_draft_enabled(&mut self, enabled: bool) {
        if let Some(draft) = &mut self.draft {
            draft.enabled = enabled;
        }
    }
    pub(crate) fn clear_cli_runtime_draft(&mut self) {
        self.draft = None;
    }
}
