mod platform;
pub(crate) use platform::provider_config;
use crate::app::root::{PioneerDesktop, MainContentView};
use gpui_kit::Context;
use pioneer_client::providers::{store::*, runtime::ProviderRuntimeIntent};
impl PioneerDesktop {
    pub(in crate::app) fn open_providers_screen_from_bottom_bar(&mut self, cx: &mut Context<Self>) { self.set_main_content_view(MainContentView::Providers, cx); }
    pub(in crate::app) fn refresh_configured_providers(&mut self, _: &mut Context<Self>) { if let Some(workspace) = self.active_workspace_id() { self.gateway.client_runtime.client_core().provider_collection_intent(ProviderCollectionIntent::Refresh { key: ProviderCollectionKey::catalog(workspace) }); } }
    pub(in crate::app) fn load_cli_provider_snapshot(&mut self, _: &mut Context<Self>) { if let Some(workspace) = self.active_workspace_id() { self.gateway.client_runtime.client_core().provider_runtime_intent(ProviderRuntimeIntent::Refresh { workspace_id: workspace.into() }); } }
    pub(in crate::app) fn provider_catalog_loading(&self) -> bool { self.active_workspace_id().and_then(|workspace| self.gateway.client_runtime.client_core().provider_collection_snapshot(&ProviderCollectionKey::catalog(workspace))).is_some_and(|p| p.request() == ProviderLoadState::Loading) }

}
