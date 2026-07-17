use crate::{
    app::{
        root::{PioneerDesktop, SettingsContentView},
        settings::{MemoryModelSetting, MemorySettingToggle, VoiceInputEnableAction},
    },
    assets::PioneerIconName,
    components::{
        buttonts::small_outline_button,
        model_selector::{ModelSelectorDialogOptions, ModelSelectorSelection},
        progress_circle::ProgressCircle,
    },
    settings::{self, AppLanguagePreference, WindowThemePreference},
};
use gpui::{prelude::*, *};
use gpui_component::{
    button::*,
    input::{Input, InputEvent, InputState},
    popover::{Popover, PopoverState},
    switch::Switch,
    theme::ActiveTheme,
    *,
};
use pioneer_client::providers::list::ProviderModelSelectorMode;
use pioneer_client::settings::gateway::{self as gateway_settings, ThreadEpisodicSettingToggle};
use pioneer_client::settings::memory as settings_memory;
use pioneer_protocol::{
    GatewayMemoryModelSelection, GatewayMemorySettings, GatewayRemoteAccessSettings,
    GatewayThreadEpisodicVectorLocalModelStatus, GatewayThreadEpisodicVectorProvider,
    GatewayThreadEpisodicVectorRefillStatus, GatewayThreadEpisodicVectorSearchSettings,
    GatewayVoiceInputProvider, GatewayVoiceInputRuntimePhase, GatewayVoiceInputSettings,
};
use std::rc::Rc;

const SETTINGS_CONTENT_MAX_WIDTH_PX: f32 = 860.0;

#[derive(Clone, Debug, PartialEq)]
struct VoiceInputDownloadPresentation {
    label: String,
    fraction: Option<f32>,
}

struct RemoteAccessSettingsInputState {
    key: Entity<InputState>,
    _key_subscription: Subscription,
}

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
    pub(crate) fn render_settings(
        &self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        match self.settings_content_view {
            SettingsContentView::General => self.render_settings_general(window, cx),
            SettingsContentView::Memory => self.render_settings_memory(window, cx),
        }
    }

    fn render_settings_general(&self, window: &mut Window, cx: &mut Context<Self>) -> AnyElement {
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
                    desktop_entity.clone(),
                    cx,
                ))
                .child(Self::render_settings_divider(cx))
                .child(Self::render_voice_input_setting(
                    settings.voice_input.clone(),
                    self.voice_input_action_error.clone(),
                    desktop_entity.clone(),
                    window,
                    cx,
                ))
                .child(Self::render_settings_divider(cx))
                .child(Self::render_remote_access_setting(
                    settings.remote_access.clone(),
                    self.remote_access_settings_expanded,
                    self.remote_access_key_input_revision,
                    desktop_entity,
                    window,
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
            .min_h_0()
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

    fn render_settings_memory(&self, window: &mut Window, cx: &mut Context<Self>) -> AnyElement {
        let desktop_entity = cx.entity().clone();
        let memory_settings_panel = match &self.gateway.settings {
            Some(settings) => Self::render_memory_settings(
                settings.memory.clone(),
                settings.thread_episodic.enabled,
                settings.thread_episodic.vector_search.clone(),
                desktop_entity,
                window,
                cx,
            ),
            None => {
                Self::render_gateway_settings_status(self.gateway_settings_status_message(), cx)
            }
        };

        v_flex()
            .id("settings-memory-scroll")
            .flex_1()
            .min_h_0()
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
        thread_context_enabled: bool,
        vector_search: GatewayThreadEpisodicVectorSearchSettings,
        desktop_entity: Entity<Self>,
        window: &mut Window,
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
                Self::render_thread_episodic_toggle_row(
                    "settings-memory-thread-context",
                    ThreadEpisodicSettingToggle::Enabled,
                    thread_context_enabled,
                    t!("settings.memory.thread_context.label").to_string(),
                    t!("settings.memory.thread_context.description").to_string(),
                    desktop_entity.clone(),
                    cx,
                ),
            );

            settings = settings.child(Self::render_settings_divider(cx)).child(
                Self::render_vector_search_setting(
                    vector_search,
                    desktop_entity.clone(),
                    window,
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
        let is_custom =
            settings_memory::gateway_memory_model_selection_has_custom_model(&selection);
        let selection_label = settings_memory::gateway_memory_model_selection_display_label(
            &selection,
            t!("settings.general.preflight_model.default").to_string(),
        );

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
                                                        selected_reasoning_effort: None,
                                                        mode: ProviderModelSelectorMode::Chat,
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

    fn render_voice_input_setting(
        settings: GatewayVoiceInputSettings,
        action_error: Option<String>,
        desktop_entity: Entity<Self>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let selected_provider = (settings.provider == Some(GatewayVoiceInputProvider::Local))
            .then(|| "local".to_owned());
        let selected_model = settings.model.clone();
        let model_selection = settings.enabled.then(|| {
            Self::render_voice_input_model_selection(
                &settings,
                action_error,
                selected_provider.clone(),
                selected_model.clone(),
                desktop_entity.clone(),
                cx,
            )
        });
        let label = t!("settings.voice_input.label").to_string();
        let description = t!("settings.voice_input.description").to_string();
        let status_badge = Self::render_vector_status_badge(
            Self::voice_input_runtime_phase_label(settings.runtime.phase),
            Self::voice_input_runtime_phase_color(settings.runtime.phase, cx),
        );

        v_flex()
            .w_full()
            .child(
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
                                h_flex()
                                    .gap_2()
                                    .items_center()
                                    .child(div().text_sm().font_semibold().child(label))
                                    .child(status_badge),
                            )
                            .child(div().text_xs().opacity(0.6).child(description)),
                    )
                    .child(
                        Switch::new("settings-voice-input-enabled")
                            .checked(settings.enabled)
                            .on_click({
                                let desktop_entity = desktop_entity.clone();
                                let selected_provider = selected_provider.clone();
                                let selected_model = selected_model.clone();
                                move |enabled, window, cx| {
                                    let _ = desktop_entity.update(cx, |view, cx| {
                                        let action = view.apply_voice_input_enabled(*enabled, cx);
                                        if action == VoiceInputEnableAction::NeedsSelection {
                                            view.open_voice_input_model_selector(
                                                selected_provider.clone(),
                                                selected_model.clone(),
                                                window,
                                                cx,
                                            );
                                        }
                                        cx.notify();
                                    });
                                }
                            }),
                    ),
            )
            .when_some(model_selection, |this, selection| this.child(selection))
            .into_any_element()
    }

    fn render_voice_input_model_selection(
        settings: &GatewayVoiceInputSettings,
        action_error: Option<String>,
        selected_provider: Option<String>,
        selected_model: Option<String>,
        desktop_entity: Entity<Self>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let selection_label = selected_model
            .as_ref()
            .map(|model| format!("local/{model}"))
            .unwrap_or_else(|| t!("settings.voice_input.no_model_selected").to_string());
        let progress =
            (settings.runtime.phase == GatewayVoiceInputRuntimePhase::Downloading).then(|| {
                Self::render_voice_input_download_progress(
                    settings.runtime.downloaded_bytes,
                    settings.runtime.total_bytes,
                    cx,
                )
            });
        let actions = Self::render_voice_input_model_actions(
            settings.runtime.phase == GatewayVoiceInputRuntimePhase::Failed,
            selected_provider,
            selected_model,
            desktop_entity,
        );

        v_flex()
            .w_full()
            .pb_4()
            .gap_3()
            .child(
                h_flex()
                    .w_full()
                    .gap_6()
                    .justify_between()
                    .items_center()
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
                    .child(actions),
            )
            .when_some(progress, |this, progress| this.child(progress))
            .when_some(settings.runtime.error.clone(), |this, error| {
                this.child(Self::render_voice_input_error(error, cx))
            })
            .when_some(action_error, |this, error| {
                this.child(Self::render_voice_input_error(error, cx))
            })
            .into_any_element()
    }

    fn render_voice_input_download_progress(
        downloaded_bytes: Option<u64>,
        total_bytes: Option<u64>,
        _cx: &mut Context<Self>,
    ) -> AnyElement {
        let progress = Self::voice_input_download_presentation(downloaded_bytes, total_bytes);
        let progress_percent = progress.fraction.unwrap_or_default() * 100.0;

        h_flex()
            .gap_2()
            .mb_0p5()
            .items_center()
            .child(
                ProgressCircle::new("settings-voice-input-download-progress")
                    .value(progress_percent)
                    .size_4(),
            )
            .child(div().text_xs().opacity(0.6).child(progress.label))
            .into_any_element()
    }

    fn render_voice_input_error(error: String, cx: &mut Context<Self>) -> AnyElement {
        div()
            .text_xs()
            .text_color(cx.theme().danger)
            .child(error)
            .into_any_element()
    }

    fn render_voice_input_model_actions(
        retry_available: bool,
        selected_provider: Option<String>,
        selected_model: Option<String>,
        desktop_entity: Entity<Self>,
    ) -> AnyElement {
        let change_model = small_outline_button("settings-voice-input-change-model")
            .label(t!("settings.voice_input.change_model").to_string())
            .tooltip(t!("settings.voice_input.change_model_tooltip").to_string())
            .on_click({
                let desktop_entity = desktop_entity.clone();
                move |_, window, cx| {
                    let _ = desktop_entity.update(cx, |view, cx| {
                        view.open_voice_input_model_selector(
                            selected_provider.clone(),
                            selected_model.clone(),
                            window,
                            cx,
                        );
                    });
                }
            });
        let retry = retry_available.then(|| {
            small_outline_button("settings-voice-input-retry")
                .label(t!("settings.voice_input.retry").to_string())
                .tooltip(t!("settings.voice_input.retry_tooltip").to_string())
                .on_click(move |_, _, cx| {
                    let _ = desktop_entity.update(cx, |view, cx| {
                        view.retry_voice_input_install(cx);
                        cx.notify();
                    });
                })
        });

        h_flex()
            .flex_none()
            .gap_1()
            .items_center()
            .child(change_model)
            .when_some(retry, |this, retry| this.child(retry))
            .into_any_element()
    }

    fn voice_input_runtime_phase_label(phase: GatewayVoiceInputRuntimePhase) -> String {
        match phase {
            GatewayVoiceInputRuntimePhase::Disabled => {
                t!("settings.voice_input.status_disabled")
            }
            GatewayVoiceInputRuntimePhase::ModelNotSelected => {
                t!("settings.voice_input.status_model_not_selected")
            }
            GatewayVoiceInputRuntimePhase::Missing => t!("settings.voice_input.status_missing"),
            GatewayVoiceInputRuntimePhase::Downloading => {
                t!("settings.voice_input.status_downloading")
            }
            GatewayVoiceInputRuntimePhase::Installing => {
                t!("settings.voice_input.status_installing")
            }
            GatewayVoiceInputRuntimePhase::Loading => t!("settings.voice_input.status_loading"),
            GatewayVoiceInputRuntimePhase::Ready => t!("settings.voice_input.status_ready"),
            GatewayVoiceInputRuntimePhase::Failed => t!("settings.voice_input.status_failed"),
        }
        .to_string()
    }

    fn voice_input_runtime_phase_color(
        phase: GatewayVoiceInputRuntimePhase,
        cx: &mut Context<Self>,
    ) -> Hsla {
        match phase {
            GatewayVoiceInputRuntimePhase::Disabled
            | GatewayVoiceInputRuntimePhase::ModelNotSelected
            | GatewayVoiceInputRuntimePhase::Missing => cx.theme().muted_foreground,
            GatewayVoiceInputRuntimePhase::Downloading
            | GatewayVoiceInputRuntimePhase::Installing
            | GatewayVoiceInputRuntimePhase::Loading => cx.theme().warning,
            GatewayVoiceInputRuntimePhase::Ready => cx.theme().success,
            GatewayVoiceInputRuntimePhase::Failed => cx.theme().danger,
        }
    }

    fn voice_input_download_presentation(
        downloaded_bytes: Option<u64>,
        total_bytes: Option<u64>,
    ) -> VoiceInputDownloadPresentation {
        let downloaded_bytes = downloaded_bytes.unwrap_or(0);
        match total_bytes.filter(|total| *total > 0) {
            Some(total_bytes) => {
                let fraction = (downloaded_bytes as f64 / total_bytes as f64).clamp(0.0, 1.0);
                VoiceInputDownloadPresentation {
                    label: t!(
                        "settings.voice_input.progress_known",
                        percent = (fraction * 100.0).round() as u64,
                        downloaded = Self::format_voice_input_megabytes(downloaded_bytes),
                        total = Self::format_voice_input_megabytes(total_bytes)
                    )
                    .to_string(),
                    fraction: Some(fraction as f32),
                }
            }
            None => VoiceInputDownloadPresentation {
                label: if downloaded_bytes == 0 {
                    t!("settings.voice_input.progress_unknown").to_string()
                } else {
                    t!(
                        "settings.voice_input.progress_unknown_downloaded",
                        downloaded = Self::format_voice_input_megabytes(downloaded_bytes)
                    )
                    .to_string()
                },
                fraction: None,
            },
        }
    }

    fn format_voice_input_megabytes(bytes: u64) -> String {
        const MIB: f64 = 1024.0 * 1024.0;
        format!("{:.1} MB", bytes as f64 / MIB)
    }

    fn open_voice_input_model_selector(
        &mut self,
        selected_provider: Option<String>,
        selected_model: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let workspace_id = self.model_selector_workspace_id();
        self.open_model_selector_dialog(
            ModelSelectorDialogOptions {
                title: t!("settings.voice_input.select_dialog_title").to_string(),
                selected_provider,
                selected_model,
                selected_reasoning_effort: None,
                mode: ProviderModelSelectorMode::Transcription,
                workspace_id,
                ws_sender: self.gateway.ws_command_sender.clone(),
                on_save: Rc::new(
                    move |view: &mut PioneerDesktop, selection: ModelSelectorSelection, cx| {
                        view.apply_voice_input_model_selection(selection, cx)
                    },
                ),
            },
            window,
            cx,
        );
    }

    fn render_remote_access_setting(
        settings: GatewayRemoteAccessSettings,
        expanded: bool,
        key_input_revision: u64,
        desktop_entity: Entity<Self>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let input_state = Self::remote_access_settings_input_state(
            &settings,
            key_input_revision,
            desktop_entity.clone(),
            window,
            cx,
        );
        let key_input = input_state.read(cx).key.clone();
        let status_label = Self::remote_access_status_label(&settings);

        v_flex()
            .w_full()
            .gap_0()
            .child(
                h_flex()
                    .w_full()
                    .gap_4()
                    .px_0()
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
                                    .child(t!("settings.remote_access.label").to_string()),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .opacity(0.6)
                                    .child(t!("settings.remote_access.description").to_string()),
                            )
                            .child(div().text_xs().mt_1().opacity(0.72).child(status_label)),
                    )
                    .child(
                        h_flex()
                            .flex_none()
                            .gap_2()
                            .items_center()
                            .child(
                                Button::new("settings-remote-access-expand")
                                    .small()
                                    .ghost()
                                    .icon(if expanded {
                                        IconName::ChevronUp
                                    } else {
                                        IconName::ChevronDown
                                    })
                                    .tooltip(if expanded {
                                        t!("settings.remote_access.collapse").to_string()
                                    } else {
                                        t!("settings.remote_access.expand").to_string()
                                    })
                                    .on_click({
                                        let desktop_entity = desktop_entity.clone();
                                        move |_, _, cx| {
                                            let _ = desktop_entity.update(cx, |view, cx| {
                                                view.toggle_remote_access_settings_expanded();
                                                cx.notify();
                                            });
                                        }
                                    }),
                            )
                            .child(
                                Switch::new("settings-remote-access-enabled")
                                    .checked(settings.enabled)
                                    .on_click({
                                        let desktop_entity = desktop_entity.clone();
                                        move |enabled, _, cx| {
                                            let _ = desktop_entity.update(cx, |view, cx| {
                                                view.apply_remote_access_setting(
                                                    *enabled, None, false, cx,
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
                            v_flex().w_full().child(
                                v_flex()
                                    .w_full()
                                    .py_3()
                                    .gap_1p5()
                                    .child(
                                        div().text_sm().font_medium().child(
                                            t!("settings.remote_access.key_label").to_string(),
                                        ),
                                    )
                                    .child(Input::new(&key_input).w_full().min_w_0().mask_toggle())
                                    .child(
                                        div()
                                            .text_xs()
                                            .line_height(relative(1.35))
                                            .opacity(0.6)
                                            .child(
                                                t!("settings.remote_access.key_hint").to_string(),
                                            ),
                                    ),
                            ),
                        ),
                )
            })
            .into_any_element()
    }

    fn remote_access_settings_input_state(
        settings: &GatewayRemoteAccessSettings,
        key_input_revision: u64,
        desktop_entity: Entity<Self>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<RemoteAccessSettingsInputState> {
        let key_placeholder = if settings.has_key {
            t!("settings.remote_access.key_placeholder_configured").to_string()
        } else {
            t!("settings.remote_access.key_placeholder").to_string()
        };
        let state_key = SharedString::from(format!(
            "settings-remote-access-input:{}",
            key_input_revision
        ));

        window.use_keyed_state(state_key, cx, |window, cx| {
            let key_input = cx.new(|cx| {
                InputState::new(window, cx)
                    .placeholder(key_placeholder)
                    .masked(true)
            });
            let key_subscription = cx.subscribe(&key_input, {
                let desktop_entity = desktop_entity.clone();
                move |_, input, event: &InputEvent, cx| {
                    if !matches!(event, InputEvent::Blur | InputEvent::PressEnter { .. }) {
                        return;
                    }
                    let key = input.read(cx).value().to_string();
                    if key.trim().is_empty() {
                        return;
                    }
                    let _ = desktop_entity.update(cx, |view, cx| {
                        view.save_remote_access_key_inline(key, cx);
                        cx.notify();
                    });
                }
            });

            RemoteAccessSettingsInputState {
                key: key_input,
                _key_subscription: key_subscription,
            }
        })
    }

    fn render_vector_search_setting(
        settings: GatewayThreadEpisodicVectorSearchSettings,
        desktop_entity: Entity<Self>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let (selected_provider, selected_model) = Self::vector_search_model_selection(&settings);

        v_flex()
            .w_full()
            .child(
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
                                h_flex()
                                    .gap_2()
                                    .items_center()
                                    .child(div().text_sm().font_semibold().child(
                                        t!("settings.memory.vector_search.label").to_string(),
                                    ))
                                    .child(Self::render_vector_status_badges(&settings, cx)),
                            )
                            .child(div().text_xs().opacity(0.6).child(
                                t!("settings.memory.vector_search.description").to_string(),
                            )),
                    )
                    .child(
                        Switch::new("settings-vector-search-enabled")
                            .checked(settings.enabled)
                            .on_click({
                                let desktop_entity = desktop_entity.clone();
                                let selected_provider = selected_provider.clone();
                                let selected_model = selected_model.clone();
                                move |enabled, window, cx| {
                                    let _ = desktop_entity.update(cx, |view, cx| {
                                        if *enabled {
                                            view.open_vector_search_model_selector(
                                                selected_provider.clone(),
                                                selected_model.clone(),
                                                window,
                                                cx,
                                            );
                                        } else {
                                            view.apply_vector_search_enabled(false, cx);
                                        }
                                        cx.notify();
                                    });
                                }
                            }),
                    ),
            )
            .when(settings.enabled, |this| {
                this.child(Self::render_vector_embedding_model_selection(
                    &settings,
                    desktop_entity.clone(),
                    window,
                    cx,
                ))
                .child(Self::render_settings_divider(cx))
                .child(Self::render_vector_search_instructions_setting(
                    &settings,
                    desktop_entity,
                    cx,
                ))
            })
            .into_any_element()
    }

    fn render_vector_embedding_model_selection(
        settings: &GatewayThreadEpisodicVectorSearchSettings,
        desktop_entity: Entity<Self>,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> AnyElement {
        let (selected_provider, selected_model) = Self::vector_search_model_selection(settings);
        let selection_label = match (&selected_provider, &selected_model) {
            (Some(provider), Some(model)) => format!("{provider}/{model}"),
            _ => t!("settings.memory.vector_search.embedding_model_not_selected").to_string(),
        };

        v_flex()
            .w_full()
            .pb_4()
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
                        h_flex().flex_none().gap_1().child(
                            small_outline_button("settings-vector-search-change-model")
                                .label(t!("settings.memory.vector_search.change_model").to_string())
                                .on_click({
                                    let desktop_entity = desktop_entity.clone();
                                    let selected_provider = selected_provider.clone();
                                    let selected_model = selected_model.clone();
                                    move |_, window, cx| {
                                        let _ = desktop_entity.update(cx, |view, cx| {
                                            view.open_vector_search_model_selector(
                                                selected_provider.clone(),
                                                selected_model.clone(),
                                                window,
                                                cx,
                                            );
                                        });
                                    }
                                }),
                        ),
                    ),
            )
            .into_any_element()
    }

    fn vector_search_model_selection(
        settings: &GatewayThreadEpisodicVectorSearchSettings,
    ) -> (Option<String>, Option<String>) {
        let provider = match settings.provider {
            Some(GatewayThreadEpisodicVectorProvider::OpenAi) => Some("openai".to_owned()),
            Some(GatewayThreadEpisodicVectorProvider::OpenRouter) => Some("openrouter".to_owned()),
            Some(GatewayThreadEpisodicVectorProvider::Local) => Some("local".to_owned()),
            None => None,
        };
        let model = match settings.provider {
            Some(GatewayThreadEpisodicVectorProvider::Local) => settings
                .model
                .clone()
                .or_else(|| settings.local_model.clone()),
            _ => provider.as_ref().and(settings.model.as_ref()).cloned(),
        };
        (provider, model)
    }

    fn open_vector_search_model_selector(
        &mut self,
        selected_provider: Option<String>,
        selected_model: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let workspace_id = self.model_selector_workspace_id();
        self.open_model_selector_dialog(
            ModelSelectorDialogOptions {
                title: t!("settings.memory.vector_search.embedding_model_dialog_title").to_string(),
                selected_provider,
                selected_model,
                selected_reasoning_effort: None,
                mode: ProviderModelSelectorMode::Embeddings,
                workspace_id,
                ws_sender: self.gateway.ws_command_sender.clone(),
                on_save: Rc::new(
                    move |view: &mut PioneerDesktop, selection: ModelSelectorSelection, cx| {
                        view.apply_vector_search_embedding_model_selection(selection, cx)
                    },
                ),
            },
            window,
            cx,
        );
    }

    fn render_vector_search_instructions_setting(
        settings: &GatewayThreadEpisodicVectorSearchSettings,
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
                    .child(div().text_sm().font_semibold().child(
                        t!("settings.memory.vector_search.search_instructions_label").to_string(),
                    ))
                    .child(
                        div().text_xs().opacity(0.6).child(
                            t!("settings.memory.vector_search.search_instructions_description")
                                .to_string(),
                        ),
                    ),
            )
            .child(
                Switch::new("settings-vector-search-use-search-instructions")
                    .checked(settings.use_search_instructions)
                    .on_click(move |enabled, _, cx| {
                        let _ = desktop_entity.update(cx, |view, cx| {
                            view.apply_vector_search_use_search_instructions(*enabled, cx);
                            cx.notify();
                        });
                    }),
            )
            .into_any_element()
    }

    fn vector_refill_status_label(status: GatewayThreadEpisodicVectorRefillStatus) -> String {
        match status {
            GatewayThreadEpisodicVectorRefillStatus::Disabled => {
                t!("settings.memory.vector_search.refill_disabled")
            }
            GatewayThreadEpisodicVectorRefillStatus::Unknown => {
                t!("settings.memory.vector_search.refill_unknown")
            }
            GatewayThreadEpisodicVectorRefillStatus::Required => {
                t!("settings.memory.vector_search.refill_required")
            }
            GatewayThreadEpisodicVectorRefillStatus::Running => {
                t!("settings.memory.vector_search.refill_running")
            }
            GatewayThreadEpisodicVectorRefillStatus::Complete => {
                t!("settings.memory.vector_search.refill_complete")
            }
            GatewayThreadEpisodicVectorRefillStatus::Failed => {
                t!("settings.memory.vector_search.refill_failed")
            }
        }
        .to_string()
    }

    fn render_vector_status_badges(
        settings: &GatewayThreadEpisodicVectorSearchSettings,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let mut badges = h_flex().mt_0().gap_2().items_center().flex_wrap();
        let refill_label = if settings.enabled {
            Self::vector_refill_status_label(settings.refill_status)
        } else {
            t!("settings.memory.vector_search.status_disabled").to_string()
        };
        badges = badges.child(Self::render_vector_status_badge(
            refill_label,
            Self::vector_refill_status_color(settings.enabled, settings.refill_status, cx),
        ));

        if settings.enabled && settings.provider == Some(GatewayThreadEpisodicVectorProvider::Local)
        {
            badges = badges.child(Self::render_vector_status_badge(
                Self::vector_local_model_status_label(settings.local_model_status),
                Self::vector_local_model_status_color(settings.local_model_status, cx),
            ));
        }

        badges.into_any_element()
    }

    fn render_vector_status_badge(label: String, color: Hsla) -> AnyElement {
        div()
            .px_1()
            .py_0()
            .rounded_md()
            .border_1()
            .border_color(color)
            .text_size(px(10.))
            .text_color(color)
            .font_medium()
            .child(label)
            .into_any_element()
    }

    fn vector_refill_status_color(
        enabled: bool,
        status: GatewayThreadEpisodicVectorRefillStatus,
        cx: &mut Context<Self>,
    ) -> Hsla {
        if !enabled {
            return cx.theme().muted_foreground;
        }

        match status {
            GatewayThreadEpisodicVectorRefillStatus::Disabled
            | GatewayThreadEpisodicVectorRefillStatus::Unknown => cx.theme().muted_foreground,
            GatewayThreadEpisodicVectorRefillStatus::Required
            | GatewayThreadEpisodicVectorRefillStatus::Running => cx.theme().warning,
            GatewayThreadEpisodicVectorRefillStatus::Complete => cx.theme().success,
            GatewayThreadEpisodicVectorRefillStatus::Failed => cx.theme().danger,
        }
    }

    fn vector_local_model_status_label(
        status: GatewayThreadEpisodicVectorLocalModelStatus,
    ) -> String {
        match status {
            GatewayThreadEpisodicVectorLocalModelStatus::NotSelected => {
                t!("settings.memory.vector_search.local_status_not_selected")
            }
            GatewayThreadEpisodicVectorLocalModelStatus::Unknown => {
                t!("settings.memory.vector_search.local_status_unknown")
            }
            GatewayThreadEpisodicVectorLocalModelStatus::Missing => {
                t!("settings.memory.vector_search.local_status_missing")
            }
            GatewayThreadEpisodicVectorLocalModelStatus::Downloading => {
                t!("settings.memory.vector_search.local_status_downloading")
            }
            GatewayThreadEpisodicVectorLocalModelStatus::Installed => {
                t!("settings.memory.vector_search.local_status_installed")
            }
            GatewayThreadEpisodicVectorLocalModelStatus::Failed => {
                t!("settings.memory.vector_search.local_status_failed")
            }
        }
        .to_string()
    }

    fn vector_local_model_status_color(
        status: GatewayThreadEpisodicVectorLocalModelStatus,
        cx: &mut Context<Self>,
    ) -> Hsla {
        match status {
            GatewayThreadEpisodicVectorLocalModelStatus::NotSelected
            | GatewayThreadEpisodicVectorLocalModelStatus::Unknown => cx.theme().muted_foreground,
            GatewayThreadEpisodicVectorLocalModelStatus::Missing
            | GatewayThreadEpisodicVectorLocalModelStatus::Downloading => cx.theme().warning,
            GatewayThreadEpisodicVectorLocalModelStatus::Installed => cx.theme().success,
            GatewayThreadEpisodicVectorLocalModelStatus::Failed => cx.theme().danger,
        }
    }

    fn remote_access_status_label(settings: &GatewayRemoteAccessSettings) -> String {
        match gateway_settings::remote_access_status_label(settings) {
            gateway_settings::GatewayRemoteAccessStatusLabel::Disabled => {
                t!("settings.remote_access.status_disabled")
            }
            gateway_settings::GatewayRemoteAccessStatusLabel::NotRunning => {
                t!("settings.remote_access.status_not_running")
            }
            gateway_settings::GatewayRemoteAccessStatusLabel::InvalidSettings => {
                t!("settings.remote_access.status_invalid_settings")
            }
            gateway_settings::GatewayRemoteAccessStatusLabel::MissingKey => {
                t!("settings.remote_access.status_missing_key")
            }
            gateway_settings::GatewayRemoteAccessStatusLabel::ConnectFailed => {
                t!("settings.remote_access.status_connect_failed")
            }
            gateway_settings::GatewayRemoteAccessStatusLabel::AuthFailed => {
                t!("settings.remote_access.status_auth_failed")
            }
            gateway_settings::GatewayRemoteAccessStatusLabel::Starting => {
                t!("settings.remote_access.status_starting")
            }
            gateway_settings::GatewayRemoteAccessStatusLabel::Connected => {
                t!("settings.remote_access.status_connected")
            }
            gateway_settings::GatewayRemoteAccessStatusLabel::Reconnecting => {
                t!("settings.remote_access.status_reconnecting")
            }
            gateway_settings::GatewayRemoteAccessStatusLabel::Failed => {
                t!("settings.remote_access.status_failed")
            }
            gateway_settings::GatewayRemoteAccessStatusLabel::Stopped => {
                t!("settings.remote_access.status_stopped")
            }
        }
        .to_string()
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

    fn render_thread_episodic_toggle_row(
        id: &'static str,
        toggle: ThreadEpisodicSettingToggle,
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
                            view.apply_thread_episodic_setting(toggle, *enabled, cx);
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
        let is_custom =
            settings_memory::gateway_memory_model_selection_has_custom_model(&selection);
        let selection_label = settings_memory::gateway_memory_model_selection_display_label(
            &selection,
            t!("settings.memory.model.default").to_string(),
        );

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
                                                selected_reasoning_effort: None,
                                                mode: ProviderModelSelectorMode::Chat,
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
    use super::{GatewayVoiceInputRuntimePhase, PioneerDesktop};
    use pioneer_protocol::{
        GatewayRemoteAccessErrorKind, GatewayRemoteAccessSettings, GatewayRemoteAccessState,
    };

    fn production_view_source() -> &'static str {
        include_str!("view.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("production source segment exists")
    }

    #[::core::prelude::v1::test]
    fn remote_access_status_label_follows_switch_state() {
        rust_i18n::set_locale("en");

        let mut settings = GatewayRemoteAccessSettings {
            enabled: true,
            ..GatewayRemoteAccessSettings::default()
        };
        settings.status.state = GatewayRemoteAccessState::Disabled;
        settings.status.message = Some("remote access tunnel is disabled".to_owned());

        assert_eq!(
            super::PioneerDesktop::remote_access_status_label(&settings),
            "Service is not running."
        );

        settings.enabled = false;
        settings.status.state = GatewayRemoteAccessState::Connected;
        settings.status.message = Some("remote access tunnel is running".to_owned());

        assert_eq!(
            super::PioneerDesktop::remote_access_status_label(&settings),
            "Remote access is disabled."
        );
    }

    #[::core::prelude::v1::test]
    fn remote_access_status_label_localizes_validation_failures() {
        rust_i18n::set_locale("en");

        let mut settings = GatewayRemoteAccessSettings {
            enabled: true,
            ..GatewayRemoteAccessSettings::default()
        };
        settings.status.state = GatewayRemoteAccessState::Failed;
        settings.status.error_kind = Some(GatewayRemoteAccessErrorKind::InvalidSettings);
        settings.status.message =
            Some("remote access relay address must include a port".to_owned());

        assert_eq!(
            super::PioneerDesktop::remote_access_status_label(&settings),
            "Remote access config is invalid."
        );

        settings.status.error_kind = Some(GatewayRemoteAccessErrorKind::MissingKey);

        assert_eq!(
            super::PioneerDesktop::remote_access_status_label(&settings),
            "Paste a new service token and press Enter."
        );

        settings.status.error_kind = Some(GatewayRemoteAccessErrorKind::RelayConnectFailed);

        assert_eq!(
            super::PioneerDesktop::remote_access_status_label(&settings),
            "Could not connect to the relay."
        );

        settings.status.error_kind = Some(GatewayRemoteAccessErrorKind::TunnelAuthFailed);

        assert_eq!(
            super::PioneerDesktop::remote_access_status_label(&settings),
            "Key rejected."
        );
    }

    #[::core::prelude::v1::test]
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
        assert!(general_view.contains("render_remote_access_setting"));
        assert!(general_view.contains("settings.general.preflight_model"));
        assert!(general_view.contains("settings.remote_access"));
        assert!(!general_view.contains("\"settings-general-thread-context\""));
        assert!(!general_view.contains("settings.general.thread_context"));
        assert!(source.contains("\"settings-preflight-model\""));

        let remote_access_view = source
            .split("fn render_remote_access_setting")
            .nth(1)
            .expect("remote access renderer exists")
            .split("fn remote_access_settings_input_state")
            .next()
            .expect("remote access renderer body exists");
        assert!(!remote_access_view.contains("v_form()"));
        assert!(!remote_access_view.contains("field().label_indent(false)"));
        assert!(!remote_access_view.contains("settings-remote-access-save"));
        assert!(!remote_access_view.contains("settings.remote_access.save"));
        assert!(!remote_access_view.contains("settings-remote-access-clear-key"));
        assert!(!remote_access_view.contains("clear_remote_access_key_inline"));
        assert!(!source.contains("save_remote_access_server_inline"));
    }

    #[::core::prelude::v1::test]
    fn voice_settings_enable_flow_is_gateway_authoritative_and_selector_first() {
        let source = production_view_source();
        let general_view = source
            .split("fn render_settings_general")
            .nth(1)
            .expect("general renderer exists")
            .split("fn render_settings_memory")
            .next()
            .expect("general renderer body exists");
        assert!(general_view.contains("render_voice_input_setting"));

        let voice_view = source
            .split("fn render_voice_input_setting")
            .nth(1)
            .expect("Voice Input renderer exists")
            .split("fn open_voice_input_model_selector")
            .next()
            .expect("Voice Input renderer body exists");
        assert!(voice_view.contains(".checked(settings.enabled)"));
        assert!(voice_view.contains("apply_voice_input_enabled"));
        assert!(voice_view.contains("VoiceInputEnableAction::NeedsSelection"));
        assert!(voice_view.contains("open_voice_input_model_selector"));
        assert!(!voice_view.contains("settings.voice_input.enabled ="));

        let selector = source
            .split("fn open_voice_input_model_selector")
            .nth(1)
            .expect("Voice Input selector exists")
            .split("fn render_remote_access_setting")
            .next()
            .expect("Voice Input selector body exists");
        assert!(selector.contains("ProviderModelSelectorMode::Transcription"));
        assert!(selector.contains("apply_voice_input_model_selection"));
        assert!(!selector.contains("apply_voice_input_enabled"));
    }

    #[::core::prelude::v1::test]
    fn voice_settings_runtime_states_have_deterministic_labels_and_progress() {
        let phases = [
            (GatewayVoiceInputRuntimePhase::Disabled, "Disabled"),
            (
                GatewayVoiceInputRuntimePhase::ModelNotSelected,
                "Model not selected",
            ),
            (GatewayVoiceInputRuntimePhase::Missing, "Missing"),
            (GatewayVoiceInputRuntimePhase::Downloading, "Downloading"),
            (GatewayVoiceInputRuntimePhase::Installing, "Installing"),
            (GatewayVoiceInputRuntimePhase::Loading, "Loading"),
            (GatewayVoiceInputRuntimePhase::Ready, "Ready"),
            (GatewayVoiceInputRuntimePhase::Failed, "Failed"),
        ];
        for (phase, expected) in phases {
            assert_eq!(
                PioneerDesktop::voice_input_runtime_phase_label(phase),
                expected
            );
        }

        let known = PioneerDesktop::voice_input_download_presentation(
            Some(50 * 1024 * 1024),
            Some(100 * 1024 * 1024),
        );
        assert_eq!(known.label, "50% - 50.0 MB / 100.0 MB");
        assert_eq!(known.fraction, Some(0.5));

        let clamped = PioneerDesktop::voice_input_download_presentation(Some(120), Some(100));
        assert_eq!(clamped.fraction, Some(1.0));
        assert!(clamped.label.starts_with("100%"));

        let unknown = PioneerDesktop::voice_input_download_presentation(Some(2048), None);
        assert_eq!(unknown.label, "0.0 MB downloaded - size unknown");
        assert_eq!(unknown.fraction, None);

        assert_eq!(
            PioneerDesktop::format_voice_input_megabytes(1024 * 1024 * 1024),
            "1024.0 MB"
        );
    }

    #[::core::prelude::v1::test]
    fn voice_settings_retry_change_controls_follow_authoritative_runtime() {
        let source = production_view_source();
        let voice_view = source
            .split("fn render_voice_input_setting")
            .nth(1)
            .expect("Voice Input renderer exists")
            .split("fn voice_input_runtime_phase_label")
            .next()
            .expect("Voice Input renderer body exists");

        assert!(
            voice_view.contains("settings.runtime.phase == GatewayVoiceInputRuntimePhase::Failed")
        );
        assert!(voice_view.contains("settings-voice-input-retry"));
        assert!(voice_view.contains("retry_voice_input_install"));
        assert!(voice_view.contains("settings-voice-input-change-model"));
        assert!(voice_view.contains("open_voice_input_model_selector"));
        assert!(voice_view.contains("settings.runtime.downloaded_bytes"));
        assert!(voice_view.contains("settings.runtime.total_bytes"));
        assert!(!voice_view.contains("remove_file"));
        assert!(!voice_view.contains("remove_dir"));
        assert!(!voice_view.contains("auto_retry"));
    }

    #[::core::prelude::v1::test]
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
        assert!(memory_view.contains("\"settings-memory-thread-context\""));
        assert!(memory_view.contains("settings.memory.thread_context"));
        assert!(memory_view.contains("ThreadEpisodicSettingToggle::Enabled"));
        assert!(memory_view.contains("render_vector_search_setting"));
        assert!(source.contains("settings.memory.vector_search"));
        assert!(source.contains("\"settings-vector-search-enabled\""));
        assert!(source.contains("\"settings-vector-search-use-search-instructions\""));
        assert!(source.contains("render_vector_search_instructions_setting"));
        assert!(source.contains("settings.memory.vector_search.search_instructions_label"));
        let vector_search_view = source
            .split("fn render_vector_search_setting")
            .nth(1)
            .expect("vector search renderer exists")
            .split("fn render_vector_embedding_model_selection")
            .next()
            .expect("vector search renderer body exists");
        assert!(vector_search_view.contains("if *enabled"));
        assert!(vector_search_view.contains("open_vector_search_model_selector"));
        assert!(vector_search_view.contains("apply_vector_search_enabled(false"));
        assert!(!vector_search_view.contains("apply_vector_search_enabled(*enabled"));
        assert!(vector_search_view.contains("render_vector_embedding_model_selection"));
        assert!(vector_search_view.contains(".child(Self::render_settings_divider(cx))"));
        assert!(vector_search_view.contains("render_vector_search_instructions_setting"));
        let vector_model_view = source
            .split("fn render_vector_embedding_model_selection")
            .nth(1)
            .expect("vector model renderer exists")
            .split("fn vector_search_model_selection")
            .next()
            .expect("vector model renderer body exists");
        assert!(vector_model_view.contains("settings-vector-search-change-model"));
        assert!(vector_model_view.contains("settings.memory.vector_search.change_model"));
        assert!(vector_model_view.contains("open_vector_search_model_selector"));
        assert!(!memory_view.contains("ThreadEpisodicSettingToggle::Indexing"));
        assert!(!memory_view.contains("ThreadEpisodicSettingToggle::Recall"));
        assert!(!memory_view.contains("render_preflight_model_setting"));
        assert!(!memory_view.contains("settings.general.preflight_model"));
        assert!(!memory_view.contains("settings.memory.active_recall_model"));
        assert!(!memory_view.contains("settings-memory-active-recall-model"));
    }

    #[::core::prelude::v1::test]
    fn settings_locale_keys_cover_preflight_and_memory_model_rows() {
        let en = include_str!("../../../locales/en.toml");
        let ru = include_str!("../../../locales/ru.toml");

        for source in [en, ru] {
            for key in [
                "[settings.general.preflight_model]",
                "select_model",
                "dialog_title",
                "[settings.remote_access]",
                "key_placeholder_configured",
                "status_not_running",
                "status_invalid_settings",
                "status_missing_key",
                "status_connect_failed",
                "status_auth_failed",
                "[settings.memory.vector_search]",
                "refill_required",
                "local_status_installed",
                "[settings.memory.thread_context]",
                "[settings.memory.active_recall]",
                "[settings.memory.proactive_writes_model]",
            ] {
                assert!(source.contains(key), "missing locale key `{key}`");
            }
            assert!(!source.contains("[settings.memory.active_recall_model]"));
        }
    }

    #[::core::prelude::v1::test]
    fn settings_voice_input_locales_have_complete_key_parity() {
        let locales = [
            ("de", include_str!("../../../locales/de.toml")),
            ("en", include_str!("../../../locales/en.toml")),
            ("es", include_str!("../../../locales/es.toml")),
            ("fr", include_str!("../../../locales/fr.toml")),
            ("hi", include_str!("../../../locales/hi.toml")),
            ("jp", include_str!("../../../locales/jp.toml")),
            ("ru", include_str!("../../../locales/ru.toml")),
            ("zh", include_str!("../../../locales/zh.toml")),
        ];
        let required_keys = [
            "label",
            "description",
            "no_model_selected",
            "recommended",
            "change_model",
            "change_model_tooltip",
            "retry",
            "retry_tooltip",
            "select_dialog_title",
            "status_disabled",
            "status_model_not_selected",
            "status_missing",
            "status_downloading",
            "status_installing",
            "status_loading",
            "status_ready",
            "status_failed",
            "progress_known",
            "progress_unknown",
            "progress_unknown_downloaded",
        ];

        for (locale, source) in locales {
            let section = source
                .split("[settings.voice_input]")
                .nth(1)
                .unwrap_or_else(|| panic!("{locale} is missing settings.voice_input"))
                .split("\n[")
                .next()
                .expect("Voice Input locale section is bounded");
            for key in required_keys {
                assert!(
                    section
                        .lines()
                        .any(|line| line.starts_with(&format!("{key} ="))),
                    "{locale} is missing settings.voice_input.{key}"
                );
            }
            assert!(
                source.contains("failed_open_settings ="),
                "{locale} is missing chat.composer.voice.failed_open_settings"
            );
        }
    }

    #[::core::prelude::v1::test]
    fn settings_vector_search_locales_use_change_model_label() {
        let locales = [
            ("de", include_str!("../../../locales/de.toml")),
            ("en", include_str!("../../../locales/en.toml")),
            ("es", include_str!("../../../locales/es.toml")),
            ("fr", include_str!("../../../locales/fr.toml")),
            ("hi", include_str!("../../../locales/hi.toml")),
            ("jp", include_str!("../../../locales/jp.toml")),
            ("ru", include_str!("../../../locales/ru.toml")),
            ("zh", include_str!("../../../locales/zh.toml")),
        ];

        for (locale, source) in locales {
            let section = source
                .split("[settings.memory.vector_search]")
                .nth(1)
                .unwrap_or_else(|| panic!("{locale} is missing settings.memory.vector_search"))
                .split("\n[")
                .next()
                .expect("Vector Search locale section is bounded");
            assert!(
                section
                    .lines()
                    .any(|line| line.starts_with("change_model =")),
                "{locale} is missing settings.memory.vector_search.change_model"
            );
            assert!(!section.contains("select_embedding_model ="));
        }
    }

    #[::core::prelude::v1::test]
    fn voice_settings_match_vector_search_control_structure() {
        let source = production_view_source();
        let voice_view = source
            .split("fn render_voice_input_setting")
            .nth(1)
            .expect("Voice Input renderer exists")
            .split("fn voice_input_runtime_phase_label")
            .next()
            .expect("Voice Input renderer body exists");
        assert!(voice_view.contains(".min_w_0()"));
        assert!(voice_view.contains(".overflow_hidden()"));
        assert!(voice_view.contains(".text_ellipsis()"));
        assert!(voice_view.contains("render_vector_status_badge"));
        assert!(voice_view.contains("render_voice_input_model_selection"));
        assert!(voice_view.contains(".when_some(model_selection"));
        assert!(voice_view.contains(".pb_4()"));
        assert!(voice_view.contains("ProgressCircle::new"));
        assert!(voice_view.contains("settings-voice-input-download-progress"));
        assert!(!voice_view.contains("VOICE_INPUT_PROGRESS_WIDTH_PX"));
        assert!(!voice_view.contains("settings-voice-input-expand"));
        assert!(!voice_view.contains("settings.voice_input.expand"));
        assert!(!voice_view.contains("settings.voice_input.collapse"));
        assert!(voice_view.contains("settings.voice_input.change_model_tooltip"));
        assert!(voice_view.contains("settings.voice_input.retry_tooltip"));
    }
}
