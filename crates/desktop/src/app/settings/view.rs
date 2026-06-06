use crate::{
    app::{
        root::{PioneerDesktop, SettingsContentView},
        settings::{MemoryModelSetting, MemorySettingToggle},
    },
    assets::PioneerIconName,
    components::{
        buttonts::small_outline_button,
        model_selector::{ModelSelectorDialogOptions, ModelSelectorSelection},
    },
    settings::{self, AppLanguagePreference, WindowThemePreference},
};
use gpui::{prelude::*, *};
use gpui_component::{
    button::*,
    popover::{Popover, PopoverState},
    switch::Switch,
    theme::ActiveTheme,
    *,
};
use pioneer_protocol::{GatewayMemoryModelSelection, GatewayMemorySettings};
use std::rc::Rc;

const SETTINGS_CONTENT_MAX_WIDTH_PX: f32 = 860.0;

const LANGUAGE_OPTIONS: [AppLanguagePreference; 9] = [
    AppLanguagePreference::System,
    AppLanguagePreference::English,
    AppLanguagePreference::Russian,
    AppLanguagePreference::German,
    AppLanguagePreference::Spanish,
    AppLanguagePreference::French,
    AppLanguagePreference::Hindi,
    AppLanguagePreference::Japanese,
    AppLanguagePreference::Chinese,
];

impl PioneerDesktop {
    pub(crate) fn render_settings(&self, _window: &Window, cx: &mut Context<Self>) -> AnyElement {
        match self.settings_content_view {
            SettingsContentView::General => self.render_settings_general(cx),
            SettingsContentView::Memory => self.render_settings_memory(cx),
        }
    }

    fn render_settings_general(&self, cx: &mut Context<Self>) -> AnyElement {
        let selected_language = settings::app_language(cx);
        let selected_theme = settings::window_theme(cx);
        let desktop_entity = cx.entity().clone();

        let mut general_settings = v_flex()
            .w_full()
            .gap_0()
            .px_4()
            .py_0()
            .rounded_lg()
            .border_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().background)
            .child(Self::render_locale_setting(
                selected_language,
                desktop_entity.clone(),
                cx,
            ))
            .child(Self::render_settings_divider(cx))
            .child(Self::render_theme_setting(
                selected_theme,
                desktop_entity.clone(),
                cx,
            ));

        general_settings = match &self.gateway.settings {
            Some(settings) => general_settings
                .child(Self::render_settings_divider(cx))
                .child(Self::render_keepawake_setting(
                    settings.general.keepawake,
                    desktop_entity.clone(),
                    cx,
                ))
                .child(Self::render_settings_divider(cx))
                .child(Self::render_preflight_model_setting(
                    settings.general.preflight_model.clone(),
                    desktop_entity,
                    cx,
                )),
            None => general_settings
                .child(Self::render_settings_divider(cx))
                .child(Self::render_gateway_settings_status_row(
                    self.gateway_settings_status_message(),
                )),
        };

        v_flex()
            .id("settings-scroll")
            .flex_1()
            .overflow_y_scroll()
            .p_6()
            .bg(cx.theme().background)
            .child(
                h_flex().w_full().justify_center().child(
                    v_flex()
                        .w_full()
                        .max_w(px(SETTINGS_CONTENT_MAX_WIDTH_PX))
                        .gap_6()
                        .child(
                            v_flex()
                                .child(
                                    div()
                                        .text_xl()
                                        .font_semibold()
                                        .child(t!("settings.screen.title").to_string()),
                                )
                                .child(
                                    div()
                                        .text_sm()
                                        .opacity(0.6)
                                        .child(t!("settings.screen.description").to_string()),
                                ),
                        )
                        .child(general_settings),
                ),
            )
            .into_any_element()
    }

    fn render_settings_memory(&self, cx: &mut Context<Self>) -> AnyElement {
        let desktop_entity = cx.entity().clone();
        let memory_settings_panel = match &self.gateway.settings {
            Some(settings) => {
                Self::render_memory_settings(settings.memory.clone(), desktop_entity, cx)
            }
            None => {
                Self::render_gateway_settings_status(self.gateway_settings_status_message(), cx)
            }
        };

        v_flex()
            .id("settings-memory-scroll")
            .flex_1()
            .overflow_y_scroll()
            .p_6()
            .bg(cx.theme().background)
            .child(
                h_flex().w_full().justify_center().child(
                    v_flex()
                        .w_full()
                        .max_w(px(SETTINGS_CONTENT_MAX_WIDTH_PX))
                        .gap_6()
                        .child(
                            v_flex()
                                .child(
                                    div()
                                        .text_xl()
                                        .font_semibold()
                                        .child(t!("settings.memory.title").to_string()),
                                )
                                .child(
                                    div()
                                        .text_sm()
                                        .opacity(0.6)
                                        .child(t!("settings.memory.description").to_string()),
                                ),
                        )
                        .child(memory_settings_panel),
                ),
            )
            .into_any_element()
    }

    fn render_locale_setting(
        selected_language: AppLanguagePreference,
        desktop_entity: Entity<Self>,
        _cx: &mut Context<Self>,
    ) -> AnyElement {
        let selected_label = Self::language_option_label(selected_language);

        h_flex()
            .w_full()
            .gap_6()
            .py_3()
            .justify_between()
            .items_center()
            .child(
                v_flex()
                    .min_w_0()
                    .flex_1()
                    .child(
                        div()
                            .text_sm()
                            .font_semibold()
                            .child(t!("settings.option.language.label").to_string()),
                    )
                    .child(
                        div()
                            .text_xs()
                            .opacity(0.6)
                            .child(t!("settings.option.language.description").to_string()),
                    ),
            )
            .child(
                Popover::new("settings-language-popover")
                    .anchor(Corner::TopRight)
                    .p_0()
                    .trigger(
                        small_outline_button("settings-language-trigger").child(
                            h_flex()
                                .w_full()
                                .items_center()
                                .justify_between()
                                .gap_1()
                                .child(div().text_sm().child(selected_label))
                                .child(Icon::new(IconName::ChevronsUpDown).size_3p5()),
                        ),
                    )
                    .content(move |_, _, popover_cx| {
                        let popover_entity: Entity<PopoverState> = popover_cx.entity();

                        v_flex().children(LANGUAGE_OPTIONS.iter().enumerate().map(
                            |(index, option)| {
                                let option = *option;

                                let option_label = Self::language_option_label(option);
                                let is_selected = option == selected_language;

                                let desktop_entity = desktop_entity.clone();
                                let popover_entity = popover_entity.clone();

                                Button::new(("settings-language-option", index))
                                    .ghost()
                                    .small()
                                    .rounded_none()
                                    .h_7()
                                    .justify_start()
                                    .selected(is_selected)
                                    .label(option_label)
                                    .on_click(move |_, window, cx| {
                                        let _ = desktop_entity.update(cx, |view, cx| {
                                            view.apply_language_setting(option, cx);
                                            cx.notify();
                                        });
                                        let _ = popover_entity.update(cx, |popover, cx| {
                                            popover.dismiss(window, cx);
                                        });
                                    })
                            },
                        ))
                    }),
            )
            .into_any_element()
    }

    fn render_memory_settings(
        memory: GatewayMemorySettings,
        desktop_entity: Entity<Self>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let mut settings = v_flex()
            .w_full()
            .gap_0()
            .px_4()
            .py_0()
            .rounded_lg()
            .border_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().background)
            .child(Self::render_memory_toggle_row(
                "settings-memory-enabled",
                MemorySettingToggle::Enabled,
                memory.enabled,
                t!("settings.memory.enabled.label").to_string(),
                t!("settings.memory.enabled.description").to_string(),
                desktop_entity.clone(),
                cx,
            ));

        if memory.enabled {
            settings = settings.child(Self::render_settings_divider(cx)).child(
                Self::render_memory_toggle_row(
                    "settings-memory-active-recall",
                    MemorySettingToggle::ActiveRecall,
                    memory.active_recall_enabled,
                    t!("settings.memory.active_recall.label").to_string(),
                    t!("settings.memory.active_recall.description").to_string(),
                    desktop_entity.clone(),
                    cx,
                ),
            );

            settings = settings.child(Self::render_settings_divider(cx)).child(
                Self::render_memory_toggle_row(
                    "settings-memory-proactive-writes",
                    MemorySettingToggle::ProactiveWrites,
                    memory.proactive_writes_enabled,
                    t!("settings.memory.proactive_writes.label").to_string(),
                    t!("settings.memory.proactive_writes.description").to_string(),
                    desktop_entity.clone(),
                    cx,
                ),
            );

            if memory.proactive_writes_enabled {
                settings = settings.child(Self::render_settings_divider(cx)).child(
                    Self::render_memory_model_row(
                        "settings-memory-post-turn-extractor-model",
                        MemoryModelSetting::PostTurnExtractor,
                        memory.proactive_writes_model.clone(),
                        t!("settings.memory.proactive_writes_model.label").to_string(),
                        t!("settings.memory.proactive_writes_model.description").to_string(),
                        desktop_entity.clone(),
                        cx,
                    ),
                );
            }

            settings = settings
                .child(Self::render_settings_divider(cx))
                .child(Self::render_memory_toggle_row(
                    "settings-memory-background-extraction",
                    MemorySettingToggle::BackgroundExtraction,
                    memory.background_extraction_enabled,
                    t!("settings.memory.background_extraction.label").to_string(),
                    t!("settings.memory.background_extraction.description").to_string(),
                    desktop_entity.clone(),
                    cx,
                ))
                .child(Self::render_settings_divider(cx))
                .child(Self::render_memory_toggle_row(
                    "settings-memory-debug-trace",
                    MemorySettingToggle::DebugTrace,
                    memory.debug_trace_enabled,
                    t!("settings.memory.debug_trace.label").to_string(),
                    t!("settings.memory.debug_trace.description").to_string(),
                    desktop_entity,
                    cx,
                ));
        }

        settings.into_any_element()
    }

    fn render_gateway_settings_status(message: String, cx: &mut Context<Self>) -> AnyElement {
        v_flex()
            .w_full()
            .px_4()
            .py_4()
            .rounded_lg()
            .border_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().background)
            .child(div().text_xs().opacity(0.65).child(message))
            .into_any_element()
    }

    fn render_gateway_settings_status_row(message: String) -> AnyElement {
        h_flex()
            .w_full()
            .py_3()
            .child(div().text_xs().opacity(0.65).child(message))
            .into_any_element()
    }

    fn render_preflight_model_setting(
        selection: GatewayMemoryModelSelection,
        desktop_entity: Entity<Self>,
        _cx: &mut Context<Self>,
    ) -> AnyElement {
        let selected_provider = selection.model_provider_override();
        let selected_model = selection.model_override();
        let is_custom = selected_provider.is_some() && selected_model.is_some();
        let selection_label = if let (Some(provider), Some(model)) =
            (selected_provider.clone(), selected_model.clone())
        {
            format!("{provider}/{model}")
        } else {
            t!("settings.general.preflight_model.default").to_string()
        };

        v_flex()
            .w_full()
            .gap_3()
            .py_3()
            .justify_between()
            .items_start()
            .child(
                v_flex()
                    .min_w_0()
                    .flex_1()
                    .child(
                        div()
                            .text_sm()
                            .font_semibold()
                            .child(t!("settings.general.preflight_model.label").to_string()),
                    )
                    .child(
                        div()
                            .text_xs()
                            .opacity(0.6)
                            .child(t!("settings.general.preflight_model.description").to_string()),
                    ),
            )
            .child(
                h_flex()
                    .w_full()
                    .gap_6()
                    .items_center()
                    .justify_between()
                    .mt_0p5()
                    .child(
                        div()
                            .min_w_0()
                            .flex_1()
                            .text_sm()
                            .font_medium()
                            .overflow_hidden()
                            .text_ellipsis()
                            .child(selection_label),
                    )
                    .child(
                        h_flex()
                            .flex_none()
                            .gap_1()
                            .child(
                                small_outline_button(("settings-preflight-model", 0usize))
                                    .label(
                                        t!("settings.general.preflight_model.select_model")
                                            .to_string(),
                                    )
                                    .on_click({
                                        let desktop_entity = desktop_entity.clone();
                                        let selected_provider = selected_provider.clone();
                                        let selected_model = selected_model.clone();
                                        move |_, window, cx| {
                                            let _ = desktop_entity.update(cx, |view, cx| {
                                                let workspace_id = view.model_selector_workspace_id();
                                                view.open_model_selector_dialog(
                                                    ModelSelectorDialogOptions {
                                                        title: t!(
                                                            "settings.general.preflight_model.dialog_title"
                                                        )
                                                        .to_string(),
                                                        selected_provider: selected_provider
                                                            .clone(),
                                                        selected_model: selected_model.clone(),
                                                        workspace_id,
                                                        ws_sender: view
                                                            .gateway
                                                            .ws_command_sender
                                                            .clone(),
                                                        on_save: Rc::new(
                                                            move |view: &mut PioneerDesktop,
                                                                  selection: ModelSelectorSelection,
                                                                  cx| {
                                                                let model_selection = pioneer_client::settings::memory::gateway_memory_model_selection_from_model_selector(selection);
                                                                view.apply_preflight_model_setting(
                                                                    model_selection,
                                                                    cx,
                                                                );
                                                                true
                                                            },
                                                        ),
                                                    },
                                                    window,
                                                    cx,
                                                );
                                            });
                                        }
                                    }),
                            )
                            .when(is_custom, |row| {
                                row.child(div().flex_none().child(
                                    small_outline_button(("settings-preflight-model", 1usize))
                                        .label(
                                            t!("settings.general.preflight_model.default")
                                                .to_string(),
                                        )
                                        .on_click({
                                            let desktop_entity = desktop_entity.clone();
                                            move |_, _, cx| {
                                                let _ = desktop_entity.update(cx, |view, cx| {
                                                    view.apply_preflight_model_setting(
                                                        GatewayMemoryModelSelection::thread(),
                                                        cx,
                                                    );
                                                    cx.notify();
                                                });
                                            }
                                        }),
                                ))
                            }),
                    ),
            )
            .into_any_element()
    }

    fn gateway_settings_status_message(&self) -> String {
        self.gateway.settings_error.clone().unwrap_or_else(|| {
            if self.gateway.settings_loading {
                t!("settings.loading").to_string()
            } else {
                t!("settings.unavailable").to_string()
            }
        })
    }

    fn render_keepawake_setting(
        selected: bool,
        desktop_entity: Entity<Self>,
        _cx: &mut Context<Self>,
    ) -> AnyElement {
        h_flex()
            .w_full()
            .gap_6()
            .py_3()
            .justify_between()
            .items_center()
            .child(
                v_flex()
                    .min_w_0()
                    .flex_1()
                    .child(
                        div()
                            .text_sm()
                            .font_semibold()
                            .child(t!("settings.option.keepawake.label").to_string()),
                    )
                    .child(
                        div()
                            .text_xs()
                            .opacity(0.6)
                            .child(t!("settings.option.keepawake.description").to_string()),
                    ),
            )
            .child(
                Switch::new("settings-keepawake")
                    .checked(selected)
                    .on_click(move |enabled, _, cx| {
                        let _ = desktop_entity.update(cx, |view, cx| {
                            view.apply_keepawake_setting(*enabled, cx);
                            cx.notify();
                        });
                    }),
            )
            .into_any_element()
    }

    fn render_settings_divider(cx: &mut Context<Self>) -> AnyElement {
        div()
            .w_full()
            .border_t_1()
            .border_color(cx.theme().border)
            .into_any_element()
    }

    fn render_memory_toggle_row(
        id: &'static str,
        toggle: MemorySettingToggle,
        selected: bool,
        label: String,
        description: String,
        desktop_entity: Entity<Self>,
        _cx: &mut Context<Self>,
    ) -> AnyElement {
        h_flex()
            .w_full()
            .gap_6()
            .py_3()
            .justify_between()
            .items_center()
            .child(
                v_flex()
                    .min_w_0()
                    .flex_1()
                    .child(div().text_sm().font_semibold().child(label))
                    .child(div().text_xs().opacity(0.6).child(description)),
            )
            .child(
                Switch::new(id)
                    .checked(selected)
                    .on_click(move |enabled, _, cx| {
                        let _ = desktop_entity.update(cx, |view, cx| {
                            view.apply_memory_setting(toggle, *enabled, cx);
                            cx.notify();
                        });
                    }),
            )
            .into_any_element()
    }

    fn render_memory_model_row(
        id: &'static str,
        setting: MemoryModelSetting,
        selection: GatewayMemoryModelSelection,
        label: String,
        description: String,
        desktop_entity: Entity<Self>,
        _cx: &mut Context<Self>,
    ) -> AnyElement {
        let selected_provider = selection.model_provider_override();
        let selected_model = selection.model_override();
        let is_custom = selected_provider.is_some() && selected_model.is_some();
        let selection_label = if let (Some(provider), Some(model)) =
            (selected_provider.clone(), selected_model.clone())
        {
            format!("{provider}/{model}")
        } else {
            t!("settings.memory.model.default").to_string()
        };

        v_flex()
            .w_full()
            .gap_3()
            .pt_3()
            .pb_4()
            .justify_between()
            .items_start()
            .child(
                v_flex()
                    .min_w_0()
                    .flex_1()
                    .child(div().text_sm().font_semibold().child(label))
                    .child(div().text_xs().opacity(0.6).child(description)),
            )
            .child(
                h_flex()
                    .w_full()
                    .gap_6()
                    .items_center()
                    .justify_between()
                    .mt_0p5()
                    .child(
                        div()
                            .min_w_0()
                            .flex_1()
                            .text_sm()
                            .font_medium()
                            .overflow_hidden()
                            .text_ellipsis()
                            .child(selection_label),
                    )
                    .child(h_flex().flex_none().gap_1().child(
                        small_outline_button((id, 0usize))
                            .label(t!("settings.memory.model.select_model").to_string())
                            .on_click({
                                let desktop_entity = desktop_entity.clone();
                                let selected_provider = selected_provider.clone();
                                let selected_model = selected_model.clone();
                                move |_, window, cx| {
                                    let _ = desktop_entity.update(cx, |view, cx| {
                                        let workspace_id = view.model_selector_workspace_id();
                                        view.open_model_selector_dialog(
                                            ModelSelectorDialogOptions {
                                                title: t!(
                                                    "settings.memory.model.dialog_title"
                                                )
                                                .to_string(),
                                                selected_provider: selected_provider.clone(),
                                                selected_model: selected_model.clone(),
                                                workspace_id,
                                                ws_sender: view.gateway.ws_command_sender.clone(),
                                                on_save: Rc::new(
                                                    move |view: &mut PioneerDesktop,
                                                          selection: ModelSelectorSelection,
                                                          cx| {
                                                        let model_selection = pioneer_client::settings::memory::gateway_memory_model_selection_from_model_selector(selection);
                                                        view.apply_memory_model_setting(
                                                            setting,
                                                            model_selection,
                                                            cx,
                                                        );
                                                        true
                                                    },
                                                ),
                                            },
                                            window,
                                            cx,
                                        );
                                    });
                                }
                            }),
                    ).when(is_custom, |row| {
                        row.child(
                            div().flex_none().child(
                                small_outline_button((id, 1usize))
                                    .label(t!("settings.memory.model.default").to_string())
                                    .on_click({
                                        let desktop_entity = desktop_entity.clone();
                                        move |_, _, cx| {
                                            let _ = desktop_entity.update(cx, |view, cx| {
                                                view.apply_memory_model_setting(
                                                    setting,
                                                    GatewayMemoryModelSelection::thread(),
                                                    cx,
                                                );
                                                cx.notify();
                                            });
                                        }
                                    }),
                            ),
                        )
                    })),
            )
            .into_any_element()
    }

    fn render_theme_setting(
        selected_theme: WindowThemePreference,
        desktop_entity: Entity<Self>,
        _cx: &mut Context<Self>,
    ) -> AnyElement {
        let options = [
            (
                WindowThemePreference::System,
                t!("settings.theme.system").to_string(),
            ),
            (
                WindowThemePreference::Light,
                t!("settings.theme.light").to_string(),
            ),
            (
                WindowThemePreference::Dark,
                t!("settings.theme.dark").to_string(),
            ),
        ];
        let preferences = [
            WindowThemePreference::System,
            WindowThemePreference::Light,
            WindowThemePreference::Dark,
        ];

        h_flex()
            .w_full()
            .gap_6()
            .py_3()
            .justify_between()
            .items_center()
            .child(
                v_flex()
                    .min_w_0()
                    .flex_1()
                    .child(
                        div()
                            .text_sm()
                            .font_semibold()
                            .child(t!("settings.option.theme.label").to_string()),
                    )
                    .child(
                        div()
                            .text_xs()
                            .opacity(0.6)
                            .child(t!("settings.option.theme.description").to_string()),
                    ),
            )
            .child(
                ButtonGroup::new("settings-theme-group")
                    .children(options.into_iter().enumerate().map(
                        |(index, (preference, label))| {
                            let is_selected = selected_theme == preference;

                            small_outline_button(("settings-theme-option", index))
                                .selected(is_selected)
                                .child(
                                    h_flex()
                                        .items_center()
                                        .gap_2()
                                        .child(Self::theme_icon(preference))
                                        .child(div().text_sm().child(label)),
                                )
                        },
                    ))
                    .on_click(move |selected_indices, window, cx| {
                        let Some(index) = selected_indices.first().copied() else {
                            return;
                        };
                        let Some(preference) = preferences.get(index).copied() else {
                            return;
                        };

                        let _ = desktop_entity.update(cx, |view, cx| {
                            view.apply_theme_setting(preference, window, cx);
                            cx.notify();
                        });
                    }),
            )
            .into_any_element()
    }

    fn theme_icon(preference: WindowThemePreference) -> AnyElement {
        match preference {
            WindowThemePreference::System => Icon::new(PioneerIconName::SunMoon)
                .size_3p5()
                .into_any_element(),
            WindowThemePreference::Light => Icon::new(IconName::Sun).size_3p5().into_any_element(),
            WindowThemePreference::Dark => Icon::new(IconName::Moon).size_3p5().into_any_element(),
        }
    }

    fn language_option_label(language: AppLanguagePreference) -> String {
        match language {
            AppLanguagePreference::System => t!("settings.language.system").to_string(),
            AppLanguagePreference::English => t!("settings.language.english").to_string(),
            AppLanguagePreference::Russian => t!("settings.language.russian").to_string(),
            AppLanguagePreference::German => t!("settings.language.german").to_string(),
            AppLanguagePreference::Spanish => t!("settings.language.spanish").to_string(),
            AppLanguagePreference::French => t!("settings.language.french").to_string(),
            AppLanguagePreference::Hindi => t!("settings.language.hindi").to_string(),
            AppLanguagePreference::Japanese => t!("settings.language.japanese").to_string(),
            AppLanguagePreference::Chinese => t!("settings.language.chinese").to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    fn production_view_source() -> &'static str {
        include_str!("view.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("production source segment exists")
    }

    #[test]
    fn settings_general_view_owns_preflight_model_selector() {
        let source = production_view_source();
        let general_view = source
            .split("fn render_settings_general")
            .nth(1)
            .expect("general renderer exists")
            .split("fn render_settings_memory")
            .next()
            .expect("general renderer body exists");

        assert!(general_view.contains("render_preflight_model_setting"));
        assert!(general_view.contains("settings.general.preflight_model"));
        assert!(source.contains("\"settings-preflight-model\""));
    }

    #[test]
    fn settings_memory_view_keeps_memory_toggles_without_legacy_planner_model_selector() {
        let source = production_view_source();
        let memory_view = source
            .split("fn render_memory_settings")
            .nth(1)
            .expect("memory renderer exists")
            .split("fn render_gateway_settings_status")
            .next()
            .expect("memory renderer body exists");

        assert!(memory_view.contains("\"settings-memory-active-recall\""));
        assert!(memory_view.contains("settings.memory.active_recall"));
        assert!(memory_view.contains("\"settings-memory-post-turn-extractor-model\""));
        assert!(memory_view.contains("MemoryModelSetting::PostTurnExtractor"));
        assert!(!memory_view.contains("render_preflight_model_setting"));
        assert!(!memory_view.contains("settings.general.preflight_model"));
        assert!(!memory_view.contains("settings.memory.active_recall_model"));
        assert!(!memory_view.contains("settings-memory-active-recall-model"));
    }

    #[test]
    fn settings_locale_keys_cover_preflight_and_memory_model_rows() {
        let en = include_str!("../../../locales/en.toml");
        let ru = include_str!("../../../locales/ru.toml");

        for source in [en, ru] {
            for key in [
                "[settings.general.preflight_model]",
                "select_model",
                "dialog_title",
                "[settings.memory.active_recall]",
                "[settings.memory.proactive_writes_model]",
            ] {
                assert!(source.contains(key), "missing locale key `{key}`");
            }
            assert!(!source.contains("[settings.memory.active_recall_model]"));
        }
    }
}
