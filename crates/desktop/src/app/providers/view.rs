use super::catalog::{ProviderCatalogEntry, provider_catalog_entries};
use crate::{
    app::root::{GatewayConnectionState, PioneerDesktop, ProviderFilter},
    assets::PioneerIconName,
};
use gpui::{prelude::*, *};
use gpui_component::{
    button::*,
    form::{field, v_form},
    input::{Input, InputEvent, InputState},
    menu::{ContextMenuExt, DropdownMenu, PopupMenuItem},
    switch::Switch,
    theme::ActiveTheme,
    *,
};
use pioneer_client::providers::cli_runtime_settings::CLIRuntimeProviderDraftField;
use pioneer_client::providers::{
    cli_runtime_settings as cli_provider_settings, list as provider_list, selectors,
};
use pioneer_protocol::{
    CLIAgentRuntimeKind, GatewayCliRuntimeInstanceSettings, RuntimeCapabilities,
    RuntimeDiagnosticLevel, RuntimeStatus, RuntimeSummary,
};
use std::{
    cmp::Ordering,
    collections::HashSet,
    time::{SystemTime, UNIX_EPOCH},
};

impl PioneerDesktop {
    pub(crate) fn render_providers(
        &self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let desktop_entity = cx.entity().clone();
        let providers_error = self.providers.error().map(str::to_owned);
        let cli_error = self.providers.cli_error().map(str::to_owned);
        let cli_login_message = self.providers.cli_login_message().map(str::to_owned);
        let cli_runtime_settings = self
            .gateway
            .settings
            .as_ref()
            .map(|settings| settings.cli_runtimes.instances.clone());
        let cli_runtime_input_scope_key = format!(
            "{}:{}",
            self.gateway
                .ws_connection_id
                .map(|connection_id| connection_id.to_string())
                .unwrap_or_else(|| "disconnected".to_owned()),
            self.active_workspace_id().unwrap_or("no-workspace")
        );
        let is_loading = self.providers.loading() || self.providers.cli_loading();
        let is_connected = self.gateway.connection_state == GatewayConnectionState::Connected;
        let configured_provider_names = self.providers.configured_names().clone();
        let cli_runtimes = self.providers.cli_runtimes().to_vec();
        let cli_refresh_status = self.providers.cli_refresh_status().clone();
        let expanded_cli_runtime_ids = self.providers.expanded_cli_runtime_ids().clone();
        let provider_filter = self.providers.filter();
        let show_api_providers = selectors::provider_filter_shows_api_providers(provider_filter);
        let show_cli_providers = selectors::provider_filter_shows_cli_providers(provider_filter);
        let screen_title = match provider_filter {
            ProviderFilter::Api => t!("providers.sidebar.api").to_string(),
            ProviderFilter::Connected => t!("providers.sidebar.connected").to_string(),
            ProviderFilter::Cli => t!("providers.cli.title").to_string(),
        };
        let screen_description = if show_cli_providers {
            cli_runtime_refresh_status_label(&cli_refresh_status)
        } else {
            t!("providers.screen.description").to_string()
        };
        let grid_columns = self.provider_grid_columns(window);
        let visible_providers = provider_catalog_entries()
            .enumerate()
            .filter(|(_, provider)| {
                selectors::provider_filter_includes_provider(
                    provider_filter,
                    provider.id,
                    &configured_provider_names,
                )
            })
            .collect::<Vec<_>>();
        let show_empty_connected_state = selectors::provider_filter_empty_connected_state(
            provider_filter,
            visible_providers.len(),
        );

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
                                    .child(screen_title),
                            )
                            .child(div().text_sm().opacity(0.6).child(screen_description)),
                    )
                    .child(if show_cli_providers {
                        h_flex()
                            .gap_2()
                            .items_center()
                            .when(self.providers.cli_loading(), |this| {
                                this.child(
                                    div()
                                        .text_xs()
                                        .opacity(0.6)
                                        .child(t!("providers.cli.refreshing").to_string()),
                                )
                            })
                            .child(
                                Button::new("cli-runtime-add-provider")
                                    .small()
                                    .ghost()
                                    .icon(IconName::Plus)
                                    .tooltip(t!("providers.cli.action.add").to_string())
                                    .disabled(!is_connected)
                                    .dropdown_menu_with_anchor(Corner::TopRight, {
                                        let desktop_entity = desktop_entity.clone();
                                        move |menu, _, _| {
                                            cli_provider_settings::CLI_RUNTIME_PROVIDER_SUPPORTED_KINDS
                                                .iter()
                                                .fold(menu.min_w(px(180.)), |menu, kind| {
                                                    let kind = *kind;
                                                    let desktop_entity = desktop_entity.clone();
                                                    menu.item(
                                                        PopupMenuItem::new(
                                                            cli_provider_settings::cli_runtime_provider_kind_label(
                                                                kind,
                                                            )
                                                            .to_owned(),
                                                        )
                                                        .on_click(move |_, window, cx| {
                                                            let _ = desktop_entity.update(cx, |view, cx| {
                                                                let draft =
                                                                    cli_provider_settings::CLIRuntimeProviderDraft::create_for_kind(
                                                                        view.gateway.settings.as_ref(),
                                                                        kind,
                                                                    );
                                                                view.open_cli_runtime_provider_dialog(
                                                                    draft, window, cx,
                                                                );
                                                                cx.notify();
                                                            });
                                                        }),
                                                    )
                                                })
                                        }
                                    }),
                            )
                            .into_any_element()
                    } else {
                        Button::new("refresh-provider-config")
                            .small()
                            .ghost()
                            .mt_1p5()
                            .icon(PioneerIconName::RefreshCw)
                            .tooltip(t!("providers.button.refresh").to_string())
                            .disabled(!is_connected)
                            .loading(is_loading)
                            .on_click(cx.listener(|view, _, _, cx| {
                                view.refresh_configured_providers(cx);
                                view.refresh_cli_providers(cx);
                                cx.notify();
                            }))
                            .into_any_element()
                    }),
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
                            .when(show_api_providers, |this| {
                                this.when_some(providers_error, |this, error| {
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
                                            .child(
                                                t!("providers.screen.empty_connected").to_string(),
                                            ),
                                    )
                                })
                                .when(
                                    !show_empty_connected_state,
                                    |this| {
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
                                                            *provider,
                                                            configured_provider_names
                                                                .contains(provider.id),
                                                            is_connected,
                                                            desktop_entity.clone(),
                                                            cx,
                                                        )
                                                    },
                                                )),
                                        )
                                    },
                                )
                            })
                            .when(show_cli_providers, |this| {
                                this.child(Self::render_cli_providers_section(
                                    cli_runtime_settings.as_deref(),
                                    cli_runtimes.as_slice(),
                                    cli_error,
                                    cli_login_message,
                                    is_connected,
                                    self.providers.cli_loading(),
                                    &expanded_cli_runtime_ids,
                                    cli_runtime_input_scope_key.as_str(),
                                    desktop_entity.clone(),
                                    window,
                                    cx,
                                ))
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
        provider: ProviderCatalogEntry,
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

    fn render_cli_providers_section(
        settings_instances: Option<&[GatewayCliRuntimeInstanceSettings]>,
        runtimes: &[RuntimeSummary],
        error: Option<String>,
        login_message: Option<String>,
        is_connected: bool,
        is_loading: bool,
        expanded_runtime_ids: &HashSet<String>,
        input_scope_key: &str,
        desktop_entity: Entity<Self>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let displayed_runtimes = cli_runtime_displayed_runtimes(settings_instances, runtimes);
        v_flex()
            .w_full()
            .gap_3()
            .when_some(error, |this, error| {
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
                        .child(div().text_sm().text_color(cx.theme().danger).child(error)),
                )
            })
            .when_some(login_message, |this, message| {
                this.child(
                    div()
                        .w_full()
                        .p_3()
                        .rounded_md()
                        .bg(cx.theme().accent.opacity(0.08))
                        .border_1()
                        .border_color(cx.theme().border)
                        .text_sm()
                        .child(message),
                )
            })
            .child(if displayed_runtimes.is_empty() {
                div()
                    .w_full()
                    .p_4()
                    .rounded_lg()
                    .border_1()
                    .border_color(cx.theme().border)
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child(t!("providers.cli.empty").to_string())
                    .into_any_element()
            } else {
                v_flex()
                    .w_full()
                    .rounded_lg()
                    .bg(cx.theme().background)
                    .border_1()
                    .border_color(cx.theme().border)
                    .overflow_hidden()
                    .children(
                        displayed_runtimes
                            .iter()
                            .enumerate()
                            .map(|(index, runtime)| {
                                Self::render_cli_runtime_card(
                                    index,
                                    runtime,
                                    expanded_runtime_ids.contains(runtime.runtime_id.as_str()),
                                    is_connected,
                                    is_loading,
                                    input_scope_key,
                                    desktop_entity.clone(),
                                    window,
                                    cx,
                                )
                            }),
                    )
                    .into_any_element()
            })
            .into_any_element()
    }

    fn render_cli_runtime_card(
        index: usize,
        runtime: &RuntimeSummary,
        expanded: bool,
        is_connected: bool,
        is_loading: bool,
        input_scope_key: &str,
        desktop_entity: Entity<Self>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let runtime_id = runtime.runtime_id.clone();
        let can_login = runtime.capabilities.supports_auth_management
            && matches!(runtime.status, RuntimeStatus::NeedsAuth);
        let menu_runtime = runtime.clone();
        let menu_runtime_id = runtime_id.clone();
        let menu_home_path = runtime.home_path.clone();
        let menu_shadow_home_path = runtime.shadow_home_path.clone();
        let menu_desktop_entity = desktop_entity.clone();
        let summary = cli_runtime_summary_line(runtime);

        v_flex()
            .id(("cli-runtime-card", index))
            .w_full()
            .border_t_1()
            .border_color(cx.theme().border)
            .when(index == 0, |this| this.border_t_0())
            .context_menu(move |menu, _, _| {
                let runtime_id = menu_runtime_id.clone();
                let runtime = menu_runtime.clone();
                let desktop_entity = menu_desktop_entity.clone();
                let home_path = menu_home_path.clone();
                let shadow_home_path = menu_shadow_home_path.clone();
                let has_path_actions = home_path.is_some() || shadow_home_path.is_some();

                let mut menu = menu.min_w(px(220.));

                if let Some(path) = home_path {
                    let desktop_entity = desktop_entity.clone();
                    menu = menu.item(
                        PopupMenuItem::new(t!("providers.cli.action.open_home").to_string())
                            .disabled(!is_connected)
                            .on_click(move |_, _, cx| {
                                let _ = desktop_entity.update(cx, |view, cx| {
                                    view.open_cli_runtime_provider_path(path.clone());
                                    cx.notify();
                                });
                            }),
                    );
                }

                if let Some(path) = shadow_home_path {
                    let desktop_entity = desktop_entity.clone();
                    menu = menu.item(
                        PopupMenuItem::new(t!("providers.cli.action.open_shadow_home").to_string())
                            .disabled(!is_connected)
                            .on_click(move |_, _, cx| {
                                let _ = desktop_entity.update(cx, |view, cx| {
                                    view.open_cli_runtime_provider_path(path.clone());
                                    cx.notify();
                                });
                            }),
                    );
                }

                if has_path_actions {
                    menu = menu.separator();
                }

                let mut menu = menu
                    .item(
                        PopupMenuItem::new(t!("providers.cli.action.refresh").to_string())
                            .disabled(!is_connected || is_loading)
                            .on_click({
                                let runtime_id = runtime_id.clone();
                                let desktop_entity = desktop_entity.clone();
                                move |_, _, cx| {
                                    let _ = desktop_entity.update(cx, |view, cx| {
                                        view.refresh_cli_provider(runtime_id.clone(), cx);
                                        cx.notify();
                                    });
                                }
                            }),
                    )
                    .item(
                        PopupMenuItem::new(t!("providers.cli.action.copy_diagnostics").to_string())
                            .on_click({
                                let runtime = runtime.clone();
                                let desktop_entity = desktop_entity.clone();
                                move |_, _, cx| {
                                    let runtime = runtime.clone();
                                    let _ = desktop_entity.update(cx, |view, cx| {
                                        view.copy_cli_runtime_provider_diagnostics(
                                            runtime.clone(),
                                            cx,
                                        );
                                        cx.notify();
                                    });
                                }
                            }),
                    )
                    .separator()
                    .item(
                        PopupMenuItem::new(t!("providers.cli.action.duplicate").to_string())
                            .disabled(!is_connected)
                            .on_click({
                                let runtime_id = runtime_id.clone();
                                let desktop_entity = desktop_entity.clone();
                                move |_, window, cx| {
                                    let _ = desktop_entity.update(cx, |view, cx| {
                                        let settings = view.gateway.settings.as_ref();
                                        let instance =
                                            cli_provider_settings::find_cli_runtime_provider_instance(
                                                settings,
                                                runtime_id.as_str(),
                                            )
                                            .cloned();

                                        match instance {
                                            Some(instance) => {
                                                let draft =
                                                    cli_provider_settings::CLIRuntimeProviderDraft::duplicate(
                                                        settings,
                                                        &instance,
                                                    );
                                                view.open_cli_runtime_provider_dialog(
                                                    draft, window, cx,
                                                );
                                            }
                                            None => view.refresh_gateway_settings(cx),
                                        }
                                        cx.notify();
                                    });
                                }
                            }),
                    );

                if runtime.capabilities.supports_auth_management {
                    menu = menu.item(
                        PopupMenuItem::new(t!("providers.cli.action.login").to_string())
                            .disabled(!is_connected || !can_login)
                            .on_click({
                                let runtime_id = runtime_id.clone();
                                let desktop_entity = desktop_entity.clone();
                                move |_, _, cx| {
                                    let _ = desktop_entity.update(cx, |view, cx| {
                                        view.start_cli_runtime_login(runtime_id.clone(), cx);
                                        cx.notify();
                                    });
                                }
                            }),
                    );
                }

                menu
            })
            .child(
                h_flex()
                    .w_full()
                    .gap_4()
                    .px_4()
                    .py_3()
                    .justify_between()
                    .items_center()
                    .child(
                        v_flex()
                            .min_w_0()
                            .flex_1()
                            .gap_0p5()
                            .items_start()
                            .child(
                                h_flex()
                                    .min_w_0()
                                    .gap_1()
                                    .child(Self::render_cli_runtime_logo(runtime, cx))
                                    .child(
                                        h_flex()
                                            .min_w_0()
                                            .gap_2()
                                            .items_baseline()
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .font_semibold()
                                                    .child(runtime.display_name.clone()),
                                            )
                                            .when_some(runtime.version.clone(), |this, version| {
                                                this.child(
                                                    div()
                                                        .text_sm()
                                                        .opacity(0.6)
                                                        .child(format!("v{version}")),
                                                )
                                            }),
                                    ),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .line_height(relative(1.35))
                                    .opacity(0.6)
                                    .child(summary),
                            ),
                    )
                    .child(
                        h_flex()
                            .gap_3()
                            .items_center()
                            .child(
                                Button::new(("cli-runtime-expand", index))
                                    .small()
                                    .ghost()
                                    .icon(if expanded {
                                        IconName::ChevronUp
                                    } else {
                                        IconName::ChevronDown
                                    })
                                    .tooltip(if expanded {
                                        t!("providers.cli.action.collapse").to_string()
                                    } else {
                                        t!("providers.cli.action.expand").to_string()
                                    })
                                    .on_click({
                                        let runtime_id = runtime_id.clone();
                                        let desktop_entity = desktop_entity.clone();
                                        move |_, _, cx| {
                                            let _ = desktop_entity.update(cx, |view, cx| {
                                                view.providers.toggle_cli_runtime_expanded(
                                                    runtime_id.clone(),
                                                );
                                                cx.notify();
                                            });
                                        }
                                    }),
                            )
                            .child(
                                Switch::new(("cli-runtime-toggle-enabled", index))
                                    .checked(runtime.enabled)
                                    .disabled(!is_connected)
                                    .on_click({
                                        let runtime_id = runtime_id.clone();
                                        let desktop_entity = desktop_entity.clone();
                                        move |enabled, _, cx| {
                                            let _ = desktop_entity.update(cx, |view, cx| {
                                                view.toggle_cli_runtime_provider_enabled(
                                                    runtime_id.clone(),
                                                    *enabled,
                                                    cx,
                                                );
                                                cx.notify();
                                            });
                                        }
                                    }),
                            ),
                    ),
            )
            .when(expanded, |this| {
                this.child(
                    v_flex()
                        .w_full()
                        .border_t_1()
                        .border_color(cx.theme().border)
                        .child(
                            v_form().w_full().child(
                                field().label_indent(false).child(
                                    v_flex()
                                        .w_full()
                                        .child(cli_runtime_inline_settings_row(
                                            runtime,
                                            CLIRuntimeProviderDraftField::DisplayName,
                                            t!("providers.cli.inline.display_name_label")
                                                .to_string(),
                                            t!("providers.cli.inline.display_name_hint")
                                                .to_string(),
                                            cli_runtime_inline_field_placeholder(
                                                runtime,
                                                CLIRuntimeProviderDraftField::DisplayName,
                                            ),
                                            is_connected,
                                            true,
                                            input_scope_key,
                                            desktop_entity.clone(),
                                            window,
                                            cx,
                                        ))
                                        .child(cli_runtime_inline_settings_row(
                                            runtime,
                                            CLIRuntimeProviderDraftField::BinaryPath,
                                            t!("providers.cli.inline.binary_label").to_string(),
                                            t!("providers.cli.inline.binary_hint").to_string(),
                                            cli_runtime_inline_field_placeholder(
                                                runtime,
                                                CLIRuntimeProviderDraftField::BinaryPath,
                                            ),
                                            is_connected,
                                            true,
                                            input_scope_key,
                                            desktop_entity.clone(),
                                            window,
                                            cx,
                                        ))
                                        .child(cli_runtime_inline_settings_row(
                                            runtime,
                                            CLIRuntimeProviderDraftField::HomePath,
                                            t!("providers.cli.inline.home_label").to_string(),
                                            t!("providers.cli.inline.home_hint").to_string(),
                                            cli_runtime_inline_field_placeholder(
                                                runtime,
                                                CLIRuntimeProviderDraftField::HomePath,
                                            ),
                                            is_connected,
                                            true,
                                            input_scope_key,
                                            desktop_entity.clone(),
                                            window,
                                            cx,
                                        ))
                                        .child(cli_runtime_inline_settings_row(
                                            runtime,
                                            CLIRuntimeProviderDraftField::ShadowHomePath,
                                            t!("providers.cli.inline.shadow_home_label")
                                                .to_string(),
                                            t!("providers.cli.inline.shadow_home_hint").to_string(),
                                            t!("providers.cli.value.disabled").to_string(),
                                            is_connected,
                                            false,
                                            input_scope_key,
                                            desktop_entity.clone(),
                                            window,
                                            cx,
                                        )),
                                ),
                            ),
                        )
                        .when(cli_runtime_has_diagnostics(runtime), |this| {
                            this.child(
                                div()
                                    .px_4()
                                    .py_3()
                                    .border_t_1()
                                    .border_color(cx.theme().border)
                                    .child(cli_runtime_diagnostics_panel(runtime, cx)),
                            )
                        }),
                )
            })
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

    fn render_cli_runtime_logo(runtime: &RuntimeSummary, cx: &mut Context<Self>) -> AnyElement {
        let (provider_id, logo_path) = cli_runtime_provider_logo(runtime.kind);

        div()
            .relative()
            .size_8()
            .flex_none()
            .flex()
            .items_center()
            .justify_center()
            .child(Self::render_provider_logo(
                provider_id,
                logo_path,
                px(22.),
                cx.theme().mode.is_dark(),
            ))
            .child(
                div()
                    .absolute()
                    .top(px(1.))
                    .left(px(1.))
                    .size_2p5()
                    .rounded_full()
                    .border_2()
                    .border_color(cx.theme().background)
                    .bg(cli_runtime_status_dot_color(&runtime.status, cx)),
            )
            .into_any_element()
    }
}

fn cli_runtime_provider_logo(kind: CLIAgentRuntimeKind) -> (&'static str, &'static str) {
    match kind {
        CLIAgentRuntimeKind::Codex => ("openai", "logos/providers/openai.svg"),
        CLIAgentRuntimeKind::Claude => ("claude", "logos/providers/claude.svg"),
    }
}

fn cli_runtime_displayed_runtimes(
    settings_instances: Option<&[GatewayCliRuntimeInstanceSettings]>,
    live_runtimes: &[RuntimeSummary],
) -> Vec<RuntimeSummary> {
    let Some(settings_instances) = settings_instances else {
        let mut runtimes = live_runtimes.to_vec();
        sort_cli_runtime_display_order(runtimes.as_mut_slice());
        return runtimes;
    };

    let mut runtimes = settings_instances
        .iter()
        .map(|instance| {
            let Some(mut runtime) = live_runtimes
                .iter()
                .find(|runtime| runtime.runtime_id == instance.id)
                .cloned()
            else {
                return cli_runtime_summary_from_settings(instance);
            };
            runtime.kind = instance.kind;
            runtime.display_name = instance.display_name.clone();
            runtime.enabled = instance.enabled;
            runtime.binary_path = Some(instance.binary_path.clone());
            runtime.home_path = Some(instance.home_path.clone());
            runtime.shadow_home_path = instance.shadow_home_path.clone();
            runtime
        })
        .collect::<Vec<_>>();
    sort_cli_runtime_display_order(runtimes.as_mut_slice());
    runtimes
}

fn sort_cli_runtime_display_order(runtimes: &mut [RuntimeSummary]) {
    runtimes.sort_by(|left, right| {
        let left_order = cli_runtime_default_display_order(left.runtime_id.as_str());
        let right_order = cli_runtime_default_display_order(right.runtime_id.as_str());
        left_order.cmp(&right_order).then_with(|| {
            if left_order == usize::MAX {
                Ordering::Equal
            } else {
                left.runtime_id.cmp(&right.runtime_id)
            }
        })
    });
}

fn cli_runtime_default_display_order(runtime_id: &str) -> usize {
    match runtime_id {
        "codex" => 0,
        "claude" => 1,
        _ => usize::MAX,
    }
}

fn cli_runtime_summary_from_settings(
    instance: &GatewayCliRuntimeInstanceSettings,
) -> RuntimeSummary {
    RuntimeSummary {
        runtime_id: instance.id.clone(),
        kind: instance.kind,
        display_name: instance.display_name.clone(),
        enabled: instance.enabled,
        status: if instance.enabled {
            RuntimeStatus::Initializing
        } else {
            RuntimeStatus::Disabled
        },
        capabilities: RuntimeCapabilities::default(),
        account: None,
        version: None,
        binary_path: Some(instance.binary_path.clone()),
        home_path: Some(instance.home_path.clone()),
        shadow_home_path: instance.shadow_home_path.clone(),
        debug_native_events_enabled: false,
        models_refreshed_at_unix_ms: None,
        diagnostics: Vec::new(),
        recent_stderr: Vec::new(),
    }
}

fn cli_runtime_refresh_status_label(status: &provider_list::CLIRuntimeRefreshStatus) -> String {
    if status.in_flight {
        return t!("providers.cli.refreshing").to_string();
    }
    if status.is_stale(provider_view_now_unix_ms()) {
        return t!("providers.cli.stale").to_string();
    }
    if status.last_success_at_unix_ms.is_some() {
        return t!("providers.cli.updated").to_string();
    }
    if status.last_failure_at_unix_ms.is_some() {
        return t!("providers.cli.refresh_failed").to_string();
    }
    t!("providers.cli.not_refreshed").to_string()
}

fn provider_view_now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}

fn cli_runtime_status_label(status: &RuntimeStatus) -> String {
    match status {
        RuntimeStatus::Disabled => t!("providers.cli.status.disabled").to_string(),
        RuntimeStatus::MissingBinary { .. } => {
            t!("providers.cli.status.missing_binary").to_string()
        }
        RuntimeStatus::SpawnFailed { .. } => t!("providers.cli.status.spawn_failed").to_string(),
        RuntimeStatus::Initializing => t!("providers.cli.status.initializing").to_string(),
        RuntimeStatus::NeedsAuth => t!("providers.cli.status.needs_auth").to_string(),
        RuntimeStatus::Ready => t!("providers.cli.status.ready").to_string(),
        RuntimeStatus::Degraded { .. } => t!("providers.cli.status.degraded").to_string(),
        RuntimeStatus::UnsupportedVersion { .. } => {
            t!("providers.cli.status.unsupported").to_string()
        }
        RuntimeStatus::Error { .. } => t!("providers.cli.status.error").to_string(),
    }
}

fn cli_runtime_status_dot_color(status: &RuntimeStatus, cx: &mut Context<PioneerDesktop>) -> Hsla {
    match status {
        RuntimeStatus::Ready => cx.theme().success,
        RuntimeStatus::NeedsAuth | RuntimeStatus::Degraded { .. } => cx.theme().warning,
        RuntimeStatus::Disabled => cx.theme().muted_foreground.opacity(0.45),
        RuntimeStatus::Initializing => cx.theme().accent,
        RuntimeStatus::MissingBinary { .. }
        | RuntimeStatus::SpawnFailed { .. }
        | RuntimeStatus::UnsupportedVersion { .. }
        | RuntimeStatus::Error { .. } => cx.theme().danger,
    }
}

fn cli_runtime_summary_line(runtime: &RuntimeSummary) -> String {
    match &runtime.status {
        RuntimeStatus::Ready => runtime
            .account
            .as_ref()
            .and_then(cli_runtime_authenticated_account_line)
            .unwrap_or_else(|| cli_runtime_status_label(&runtime.status)),
        RuntimeStatus::Disabled => t!("providers.cli.status.disabled").to_string(),
        RuntimeStatus::MissingBinary { binary_path } => {
            let binary = binary_path
                .clone()
                .or_else(|| runtime.binary_path.clone())
                .unwrap_or_else(|| runtime.runtime_id.clone());
            format!(
                "{} - {}",
                t!("providers.cli.status.missing_binary"),
                t!(
                    "providers.cli.status_detail.not_on_path",
                    binary = binary.as_str()
                )
            )
        }
        RuntimeStatus::SpawnFailed { message } | RuntimeStatus::Error { message } => {
            format!("{} - {message}", cli_runtime_status_label(&runtime.status))
        }
        RuntimeStatus::Degraded { message } => runtime
            .diagnostics
            .first()
            .map(|diagnostic| diagnostic.message.clone())
            .unwrap_or_else(|| {
                format!("{} - {message}", cli_runtime_status_label(&runtime.status))
            }),
        RuntimeStatus::UnsupportedVersion {
            version,
            minimum_version,
        } => {
            let detail = match (version.as_deref(), minimum_version.as_deref()) {
                (Some(version), Some(minimum)) => t!(
                    "providers.cli.status_detail.unsupported_version",
                    version = version,
                    minimum = minimum
                )
                .to_string(),
                (Some(version), None) => version.to_owned(),
                _ => cli_runtime_status_label(&runtime.status),
            };
            format!("{} - {detail}", cli_runtime_status_label(&runtime.status))
        }
        RuntimeStatus::Initializing | RuntimeStatus::NeedsAuth => {
            cli_runtime_status_label(&runtime.status)
        }
    }
}

fn cli_runtime_authenticated_account_line(
    account: &pioneer_protocol::RuntimeAccountSnapshot,
) -> Option<String> {
    if !account.authenticated {
        return None;
    }
    let identity = account
        .email
        .clone()
        .or_else(|| account.display_name.clone())
        .or_else(|| account.account_id.clone())?;
    Some(match account.plan.as_deref() {
        // TODO: Make plans enum
        // Some(plan) if !plan.trim().is_empty() => t!(
        //     "providers.cli.status_detail.authenticated_with_plan",
        //     account = identity.as_str(),
        //     plan = plan
        // )
        // .to_string(),
        _ => t!(
            "providers.cli.status_detail.authenticated",
            account = identity.as_str()
        )
        .to_string(),
    })
}

struct CliRuntimeInlineInputState {
    input: Entity<InputState>,
    _subscription: Subscription,
}

fn cli_runtime_inline_settings_row(
    runtime: &RuntimeSummary,
    field_id: CLIRuntimeProviderDraftField,
    label: String,
    hint: String,
    placeholder: String,
    is_connected: bool,
    show_divider: bool,
    input_scope_key: &str,
    desktop_entity: Entity<PioneerDesktop>,
    window: &mut Window,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    let input_state = cli_runtime_inline_input_state(
        runtime,
        field_id,
        placeholder,
        input_scope_key,
        desktop_entity,
        window,
        cx,
    );
    let input = input_state.read(cx).input.clone();

    v_flex()
        .w_full()
        .px_4()
        .py_3()
        .gap_1p5()
        .when(show_divider, |this| {
            this.border_b_1().border_color(cx.theme().border)
        })
        .child(div().text_sm().font_medium().child(label))
        .child(
            Input::new(&input)
                .w_full()
                .min_w_0()
                .disabled(!is_connected),
        )
        .child(
            div()
                .text_xs()
                .line_height(relative(1.35))
                .opacity(0.6)
                .child(hint),
        )
        .into_any_element()
}

fn cli_runtime_inline_input_state(
    runtime: &RuntimeSummary,
    field_id: CLIRuntimeProviderDraftField,
    placeholder: String,
    input_scope_key: &str,
    desktop_entity: Entity<PioneerDesktop>,
    window: &mut Window,
    cx: &mut Context<PioneerDesktop>,
) -> Entity<CliRuntimeInlineInputState> {
    let runtime_id = runtime.runtime_id.clone();
    let initial_value = cli_runtime_inline_field_value(runtime, field_id);
    let state_key = SharedString::from(format!(
        "cli-runtime-inline-input:{}:{}:{}",
        input_scope_key,
        runtime_id,
        cli_runtime_inline_field_key(field_id)
    ));

    window.use_keyed_state(state_key, cx, |window, cx| {
        let input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(placeholder)
                .default_value(initial_value)
        });
        let subscription = cx.subscribe(&input, {
            let runtime_id = runtime_id.clone();
            move |_, input, event: &InputEvent, cx| {
                if !matches!(event, InputEvent::Change) {
                    return;
                }
                let value = input.read(cx).value().to_string();
                let _ = desktop_entity.update(cx, |view, cx| {
                    view.save_cli_runtime_provider_inline_field(
                        runtime_id.clone(),
                        field_id,
                        value,
                        cx,
                    );
                    cx.notify();
                });
            }
        });

        CliRuntimeInlineInputState {
            input,
            _subscription: subscription,
        }
    })
}

fn cli_runtime_inline_field_value(
    runtime: &RuntimeSummary,
    field_id: CLIRuntimeProviderDraftField,
) -> String {
    match field_id {
        CLIRuntimeProviderDraftField::DisplayName => runtime.display_name.clone(),
        CLIRuntimeProviderDraftField::BinaryPath => {
            runtime.binary_path.clone().unwrap_or_else(|| {
                cli_provider_settings::cli_runtime_provider_default_binary_path(runtime.kind)
                    .to_owned()
            })
        }
        CLIRuntimeProviderDraftField::HomePath => runtime.home_path.clone().unwrap_or_else(|| {
            cli_provider_settings::cli_runtime_provider_default_home_path(runtime.kind).to_owned()
        }),
        CLIRuntimeProviderDraftField::ShadowHomePath => {
            runtime.shadow_home_path.clone().unwrap_or_default()
        }
        CLIRuntimeProviderDraftField::Id => String::new(),
    }
}

fn cli_runtime_inline_field_placeholder(
    runtime: &RuntimeSummary,
    field_id: CLIRuntimeProviderDraftField,
) -> String {
    match field_id {
        CLIRuntimeProviderDraftField::DisplayName => {
            cli_provider_settings::cli_runtime_provider_default_display_name(runtime.kind)
                .to_owned()
        }
        CLIRuntimeProviderDraftField::BinaryPath => {
            cli_provider_settings::cli_runtime_provider_default_binary_path(runtime.kind).to_owned()
        }
        CLIRuntimeProviderDraftField::HomePath => {
            cli_provider_settings::cli_runtime_provider_default_home_path(runtime.kind).to_owned()
        }
        CLIRuntimeProviderDraftField::ShadowHomePath => {
            cli_provider_settings::cli_runtime_provider_default_shadow_home_placeholder(
                runtime.kind,
            )
            .to_owned()
        }
        CLIRuntimeProviderDraftField::Id => String::new(),
    }
}

fn cli_runtime_inline_field_key(field_id: CLIRuntimeProviderDraftField) -> &'static str {
    match field_id {
        CLIRuntimeProviderDraftField::BinaryPath => "binary-path",
        CLIRuntimeProviderDraftField::HomePath => "home-path",
        CLIRuntimeProviderDraftField::ShadowHomePath => "shadow-home-path",
        CLIRuntimeProviderDraftField::Id => "id",
        CLIRuntimeProviderDraftField::DisplayName => "display-name",
    }
}

fn cli_runtime_has_diagnostics(runtime: &RuntimeSummary) -> bool {
    runtime.debug_native_events_enabled
        || !runtime.diagnostics.is_empty()
        || !runtime.recent_stderr.is_empty()
}

fn cli_runtime_diagnostics_panel(
    runtime: &RuntimeSummary,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    let stderr_start = runtime.recent_stderr.len().saturating_sub(8);
    let stderr_lines = runtime.recent_stderr[stderr_start..].to_vec();

    v_flex()
        .w_full()
        .pt_2()
        .border_t_1()
        .border_color(cx.theme().border)
        .gap_2()
        .child(
            h_flex()
                .justify_between()
                .items_center()
                .child(
                    div()
                        .text_xs()
                        .font_semibold()
                        .child(t!("providers.cli.diagnostics").to_string()),
                )
                .when(runtime.debug_native_events_enabled, |this| {
                    this.child(
                        div()
                            .text_xs()
                            .px_2()
                            .py_0p5()
                            .rounded_sm()
                            .bg(cx.theme().accent.opacity(0.10))
                            .text_color(cx.theme().muted_foreground)
                            .child(t!("providers.cli.native_events").to_string()),
                    )
                }),
        )
        .when(!runtime.diagnostics.is_empty(), |this| {
            this.child(
                v_flex()
                    .gap_1()
                    .children(runtime.diagnostics.iter().take(6).map(|diagnostic| {
                        let color = match diagnostic.level {
                            RuntimeDiagnosticLevel::Error => cx.theme().danger,
                            RuntimeDiagnosticLevel::Warning => cx.theme().warning,
                            RuntimeDiagnosticLevel::Info => cx.theme().muted_foreground,
                        };
                        div()
                            .text_xs()
                            .line_height(relative(1.25))
                            .whitespace_normal()
                            .text_color(color)
                            .child(format!("{}: {}", diagnostic.code, diagnostic.message))
                    })),
            )
        })
        .when(!stderr_lines.is_empty(), |this| {
            this.child(
                v_flex()
                    .gap_1()
                    .child(
                        div()
                            .text_xs()
                            .font_semibold()
                            .text_color(cx.theme().muted_foreground)
                            .child(t!("providers.cli.recent_stderr").to_string()),
                    )
                    .children(stderr_lines.into_iter().map(|line| {
                        div()
                            .w_full()
                            .px_2()
                            .py_1()
                            .rounded_sm()
                            .bg(cx.theme().muted_foreground.opacity(0.06))
                            .text_xs()
                            .line_height(relative(1.25))
                            .whitespace_normal()
                            .child(line)
                    })),
            )
        })
        .into_any_element()
}
