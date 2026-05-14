use super::catalog::{PROVIDER_CATALOG, ProviderCatalogEntry};
use crate::{
    app::root::{GatewayConnectionState, PioneerDesktop, ProviderFilter},
    assets::PioneerIconName,
};
use gpui::{prelude::*, *};
use gpui_component::{button::*, theme::ActiveTheme, *};

impl PioneerDesktop {
    pub(crate) fn render_providers(&self, window: &Window, cx: &mut Context<Self>) -> AnyElement {
        let desktop_entity = cx.entity().clone();
        let providers_error = self.providers_error.clone();
        let is_loading = self.providers_loading;
        let is_connected = self.gateway.connection_state == GatewayConnectionState::Connected;
        let configured_provider_names = self.provider_configured_names.clone();
        let grid_columns = self.provider_grid_columns(window);
        let visible_providers = PROVIDER_CATALOG
            .iter()
            .enumerate()
            .filter(|(_, provider)| match self.provider_filter {
                ProviderFilter::All => true,
                ProviderFilter::Connected => configured_provider_names.contains(provider.id),
            })
            .collect::<Vec<_>>();
        let show_empty_connected_state =
            self.provider_filter == ProviderFilter::Connected && visible_providers.is_empty();

        v_flex()
            .size_full()
            .bg(cx.theme().background)
            .child(
                h_flex()
                    .justify_between()
                    .items_center()
                    .pt_3()
                    .px_6()
                    .pb_3()
                    .gap_2()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(
                        v_flex()
                            .child(
                                div()
                                    .text_xl()
                                    .font_semibold()
                                    .child(t!("providers.screen.title").to_string()),
                            )
                            .child(
                                div()
                                    .text_sm()
                                    .opacity(0.6)
                                    .child(t!("providers.screen.description").to_string()),
                            ),
                    )
                    .child(
                        Button::new("refresh-provider-config")
                            .small()
                            .ghost()
                            .mt_1p5()
                            .icon(PioneerIconName::RefreshCw)
                            .disabled(!is_connected)
                            .loading(is_loading)
                            .on_click(cx.listener(|view, _, _, cx| {
                                view.refresh_configured_providers(cx);
                                cx.notify();
                            })),
                    ),
            )
            .child(
                v_flex()
                    .id("providers-scroll")
                    .flex_1()
                    .overflow_y_scroll()
                    .p_6()
                    .child(
                        v_flex()
                            .w_full()
                            .gap_3()
                            .when_some(providers_error, |this, error| {
                                this.child(
                                    h_flex()
                                        .w_full()
                                        .gap_2()
                                        .items_start()
                                        .p_3()
                                        .rounded_md()
                                        .bg(cx.theme().danger.opacity(0.08))
                                        .border_1()
                                        .border_color(cx.theme().danger.opacity(0.25))
                                        .child(
                                            Icon::new(IconName::TriangleAlert)
                                                .size_4()
                                                .text_color(cx.theme().danger),
                                        )
                                        .child(
                                            div()
                                                .text_sm()
                                                .line_height(relative(1.3))
                                                .text_color(cx.theme().danger)
                                                .child(error),
                                        ),
                                )
                            })
                            .when(show_empty_connected_state, |this| {
                                this.child(
                                    div()
                                        .w_full()
                                        .p_6()
                                        .rounded_lg()
                                        .border_1()
                                        .border_color(cx.theme().border)
                                        .text_sm()
                                        .text_color(cx.theme().muted_foreground)
                                        .child(t!("providers.screen.empty_connected").to_string()),
                                )
                            })
                            .when(!show_empty_connected_state, |this| {
                                this.child(
                                    div()
                                        .w_full()
                                        .grid()
                                        .grid_cols(grid_columns)
                                        .gap_3()
                                        .children(visible_providers.iter().map(
                                            |(index, provider)| {
                                                Self::render_provider_card(
                                                    *index,
                                                    provider,
                                                    configured_provider_names.contains(provider.id),
                                                    is_connected,
                                                    desktop_entity.clone(),
                                                    cx,
                                                )
                                            },
                                        )),
                                )
                            }),
                    ),
            )
            .into_any_element()
    }

    fn provider_grid_columns(&self, window: &Window) -> u16 {
        let viewport_width = window.viewport_size().width;
        let sidebar_width = if self.show_sidebar {
            self.sidebar_panel_width
        } else {
            px(0.)
        };

        let content_padding_x = px(48.); // .p_6 on providers scroll area
        let available_width = (viewport_width - sidebar_width - content_padding_x).max(px(0.));

        for columns in (1..=5).rev() {
            let columns_f = columns as f32;
            let required_width = px(columns_f * 246. + (columns_f - 1.) * 12.); // card + gaps
            if available_width >= required_width {
                return columns;
            }
        }

        1
    }

    fn render_provider_card(
        index: usize,
        provider: &ProviderCatalogEntry,
        is_configured: bool,
        is_connected: bool,
        desktop_entity: Entity<Self>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let provider_id = provider.id.to_owned();
        let provider_title = provider.title();
        let provider_description = provider.description();

        v_flex()
            .id(("provider-card", index))
            .w_full()
            .h_auto()
            .p_4()
            .rounded_lg()
            .bg(cx.theme().background)
            .border_1()
            .border_color(cx.theme().border)
            .gap_3()
            .justify_between()
            .child(
                v_flex()
                    .gap_1p5()
                    .child(
                        h_flex().w_full().justify_between().items_start().child(
                            h_flex()
                                .items_center()
                                .gap_2()
                                .child(Self::render_provider_logo(
                                    provider.id,
                                    provider.logo_path,
                                    px(20.),
                                    cx.theme().mode.is_dark(),
                                ))
                                .child(div().text_sm().font_semibold().child(provider_title)),
                        ),
                    )
                    .child(
                        div()
                            .text_xs()
                            .line_height(relative(1.35))
                            .opacity(0.6)
                            .child(provider_description),
                    ),
            )
            .child(
                h_flex()
                    .justify_between()
                    .child(
                        div()
                            .flex()
                            .h_5()
                            .w_5()
                            .rounded_full()
                            .justify_center()
                            .items_center()
                            .bg(cx.theme().accent)
                            .when(is_configured, |this| this.bg(cx.theme().success))
                            .when(is_configured, |this| {
                                this.child(
                                    Icon::new(IconName::Check)
                                        .size_3()
                                        .mt_px()
                                        .text_color(cx.theme().background),
                                )
                            })
                            .when(!is_configured, |this| {
                                this.child(Icon::new(IconName::Close).size_3().opacity(0.4))
                            }),
                    )
                    .child(
                        div().mt_auto().child(
                            Button::new(("provider-configure", index))
                                .small()
                                .ghost()
                                .icon(PioneerIconName::Bolt)
                                .disabled(!is_connected)
                                .opacity(0.6)
                                .on_click({
                                    let provider_id = provider_id.clone();
                                    move |_, window, cx| {
                                        let _ = desktop_entity.update(cx, |view, cx| {
                                            view.open_provider_configuration_dialog(
                                                provider_id.clone(),
                                                window,
                                                cx,
                                            );
                                            cx.notify();
                                        });
                                    }
                                }),
                        ),
                    ),
            )
            .into_any_element()
    }
    fn themed_provider_logo_path(
        provider_id: &str,
        default_path: &'static str,
        is_dark_theme: bool,
    ) -> &'static str {
        match (provider_id, is_dark_theme) {
            ("anthropic", true) => "logos/providers/anthropic-dark.svg",
            ("anthropic", false) => "logos/providers/anthropic-light.svg",
            ("cerebras", true) => "logos/providers/cerebras-dark.svg",
            ("cerebras", false) => "logos/providers/cerebras-light.svg",
            ("friendli", true) => "logos/providers/friendli-dark.svg",
            ("friendli", false) => "logos/providers/friendli-light.svg",
            ("nebius", true) => "logos/providers/nebius-dark.svg",
            ("nebius", false) => "logos/providers/nebius-light.svg",
            ("ollama", true) => "logos/providers/ollama-dark.svg",
            ("ollama", false) => "logos/providers/ollama-light.svg",
            ("ovhcloud", true) => "logos/providers/ovhcloud-dark.svg",
            ("ovhcloud", false) => "logos/providers/ovhcloud-light.svg",
            ("openai", true) => "logos/providers/openai-dark.svg",
            ("openai", false) => "logos/providers/openai-light.svg",
            ("xai", true) => "logos/providers/xai-dark.svg",
            ("xai", false) => "logos/providers/xai-light.svg",
            ("yi", true) => "logos/providers/yi-dark.svg",
            ("yi", false) => "logos/providers/yi-light.svg",
            _ => default_path,
        }
    }

    pub(super) fn render_provider_logo(
        provider_id: &'static str,
        path: &'static str,
        size: Pixels,
        is_dark_theme: bool,
    ) -> AnyElement {
        let themed_path = Self::themed_provider_logo_path(provider_id, path, is_dark_theme);
        div()
            .size(size)
            .flex()
            .items_center()
            .justify_center()
            .child(div().size(size).child(img(themed_path).w_full().h_full()))
            .into_any_element()
    }
}
