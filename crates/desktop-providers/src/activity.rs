use crate::{assets::PioneerIconName, providers::ProviderCatalogView};
use gpui_kit::component::{Disableable, Sizable, button::*};
use gpui_kit::{prelude::*, *};

/// Stock spinner invalidations stay inside the retained refresh control.
pub(crate) struct RefreshButton {
    owner: WeakEntity<ProviderCatalogView>,
    id: SharedString,
    connected: bool,
    loading: bool,
}
impl RefreshButton {
    pub(crate) fn new(
        owner: WeakEntity<ProviderCatalogView>,
        mount: u64,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|_| Self {
            owner,
            id: format!("providers:{mount}:toolbar:refresh").into(),
            connected: false,
            loading: false,
        })
    }
    pub(crate) fn sync(&mut self, connected: bool, loading: bool, cx: &mut Context<Self>) {
        if (self.connected, self.loading) != (connected, loading) {
            self.connected = connected;
            self.loading = loading;
            cx.notify();
        }
    }
}
impl Render for RefreshButton {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        let owner = self.owner.clone();
        Button::new(self.id.clone())
            .small()
            .ghost()
            .mt_1p5()
            .icon(PioneerIconName::RefreshCw)
            .tooltip(t!("providers.button.refresh").to_string())
            .disabled(!self.connected)
            .loading(self.loading)
            .on_click(move |_, _, cx| {
                let _ = owner.update(cx, |view, cx| {
                    view.refresh_configured_providers(cx);
                    view.load_cli_provider_snapshot(cx);
                });
            })
    }
}
