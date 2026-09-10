mod platform;
use crate::app::root::{MainContentView, PioneerDesktop, SettingsContentView};
use gpui_kit::*;
pub(crate) use platform::settings_config;
impl PioneerDesktop {
    pub(in crate::app) fn open_settings_content_from_sidebar(
        &mut self,
        route: SettingsContentView,
        cx: &mut Context<Self>,
    ) {
        self.navigation_intent(
            pioneer_client::navigation::NavigationIntent::SetSettingsRoute { route },
        );
        self.set_main_content_view(MainContentView::Settings, cx);
    }
    pub(in crate::app) fn refresh_gateway_settings(&mut self, _: &mut Context<Self>) {
        if self.startup.has_presented_operational_frame() {
            self.gateway
                .client_runtime
                .client_core()
                .settings_intent(pioneer_client::settings::runtime::SettingsIntent::Refresh);
        }
    }
}
