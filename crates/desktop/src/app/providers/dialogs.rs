use crate::app::root::PioneerDesktop;
use crate::components::buttonts::{default_outline_button, default_primary_button};
use gpui::{prelude::*, *};
use gpui_component::{
    WindowExt,
    button::*,
    dialog::DialogFooter,
    form::{field, v_form},
    input::{Input, InputState},
    switch::Switch,
    theme::ActiveTheme,
    *,
};
use pioneer_client::providers::cli_runtime_settings::{
    self as cli_provider_settings, CLIRuntimeProviderDraft, CLIRuntimeProviderDraftField,
    CLIRuntimeProviderDraftMode, CLIRuntimeProviderSettingsRejection,
    cli_runtime_provider_settings_rejection_message,
};
use std::cell::{Cell, RefCell};
use std::rc::Rc;

impl PioneerDesktop {
    pub(super) fn open_provider_configuration_dialog(
        &mut self,
        provider_id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let provider_id = Self::canonical_provider_id(provider_id.as_str());
        let Some(provider) = Self::provider_catalog_entry(provider_id.as_str()) else {
            return;
        };

        let provider_title = provider.title();
        let provider_description = provider.description();
        let is_configured = self.providers.is_configured(provider.id);
        let current_proxy_url = self
            .providers
            .provider_proxy_url(provider.id)
            .map(str::to_owned);
        let api_key_input_state = cx.new(|cx| {
            InputState::new(window, cx)
                .multi_line(true)
                .auto_grow(2, 7)
                .placeholder(t!("providers.dialog.api_key_placeholder").to_string())
        });
        let proxy_input_state = cx.new(|cx| {
            let mut state = InputState::new(window, cx)
                .placeholder(t!("providers.dialog.proxy_placeholder").to_string());
            if let Some(proxy_url) = current_proxy_url.as_deref() {
                state.set_value(proxy_url.to_owned(), window, cx);
            }
            state
        });
        let desktop_entity = cx.entity().clone();
        let did_focus_initial_field = Rc::new(Cell::new(false));

        let save_provider_configuration: Rc<dyn Fn(&mut App) -> bool> = Rc::new({
            let desktop_entity = desktop_entity.clone();
            let provider_id = provider.id.to_owned();
            let api_key_input_state = api_key_input_state.clone();
            let proxy_input_state = proxy_input_state.clone();
            let current_proxy_url = current_proxy_url.clone();
            move |cx| {
                let api_key = api_key_input_state.read(cx).value().trim().to_owned();
                let proxy_url = proxy_input_state.read(cx).value().trim().to_owned();
                let api_key = (!api_key.is_empty()).then_some(api_key);
                let proxy_changed = current_proxy_url.as_deref() != Some(proxy_url.as_str());
                let proxy_url = (proxy_changed && !proxy_url.is_empty()).then_some(proxy_url);
                let clear_proxy =
                    proxy_changed && proxy_url.is_none() && current_proxy_url.is_some();
                if api_key.is_none() && proxy_url.is_none() && !clear_proxy {
                    return false;
                }

                let _ = desktop_entity.update(cx, |view, cx| {
                    view.configure_provider(
                        provider_id.clone(),
                        api_key.clone(),
                        proxy_url.clone(),
                        clear_proxy,
                        cx,
                    );
                    cx.notify();
                });

                true
            }
        });

        let delete_provider_id = is_configured.then(|| provider.id.to_owned());

        window.open_dialog(cx, move |dialog, window, cx| {
            if !did_focus_initial_field.get() {
                did_focus_initial_field.set(true);
                api_key_input_state.update(cx, |state, cx| state.focus(window, cx));
            }

            dialog
                .gap_1()
                .rounded_2xl()
                .title(
                    h_flex()
                        .items_center()
                        .gap_2()
                        .child(Self::render_provider_logo(
                            provider.id,
                            provider.logo_path,
                            px(20.),
                            cx.theme().mode.is_dark(),
                        ))
                        .child(
                            div().text_base().font_semibold().child(
                                t!("providers.dialog.title", provider = provider_title.as_str())
                                    .to_string(),
                            ),
                        ),
                )
                .on_ok({
                    let save_provider_configuration = save_provider_configuration.clone();
                    move |_, _, cx| save_provider_configuration(cx)
                })
                .footer(DialogFooter::new().children({
                    let save_provider_configuration = save_provider_configuration.clone();
                    let desktop_entity = desktop_entity.clone();
                    let delete_provider_id = delete_provider_id.clone();

                    let mut actions = vec![
                        default_outline_button("provider-dialog-cancel")
                            .label(t!("buttons.cancel").to_string())
                            .outline()
                            .on_click(|_, window, cx| {
                                window.close_dialog(cx);
                            })
                            .into_any_element(),
                        default_primary_button("provider-dialog-save")
                            .label(t!("providers.button.submit").to_string())
                            .on_click({
                                let save_provider_configuration =
                                    save_provider_configuration.clone();
                                move |_, window, cx| {
                                    if save_provider_configuration(cx) {
                                        window.close_dialog(cx);
                                    }
                                }
                            })
                            .into_any_element(),
                    ];

                    if let Some(provider_id) = delete_provider_id.clone() {
                        actions.insert(
                            1,
                            default_outline_button("provider-dialog-delete")
                                .label(t!("providers.button.remove_key").to_string())
                                .danger()
                                .on_click({
                                    let desktop_entity = desktop_entity.clone();
                                    let provider_id = provider_id.clone();
                                    move |_, window, cx| {
                                        let _ = desktop_entity.update(cx, |view, cx| {
                                            view.delete_provider_api_key(provider_id.clone(), cx);
                                            cx.notify();
                                        });
                                        window.close_dialog(cx);
                                    }
                                })
                                .into_any_element(),
                        );
                    }

                    actions
                }))
                .child(
                    v_flex()
                        .w_full()
                        .pb_5()
                        .gap_4()
                        .child(
                            div()
                                .text_sm()
                                .opacity(0.6)
                                .line_height(relative(1.35))
                                .child(provider_description.clone()),
                        )
                        .child(
                            v_form()
                                .child(
                                    field()
                                        .label(t!("providers.dialog.api_key_label").to_string())
                                        .child(Input::new(&api_key_input_state).min_w_0()),
                                )
                                .child(
                                    field()
                                        .label(t!("providers.dialog.proxy_label").to_string())
                                        .child(Input::new(&proxy_input_state).min_w_0()),
                                ),
                        ),
                )
        });
    }

    pub(super) fn open_cli_runtime_provider_dialog(
        &mut self,
        draft: CLIRuntimeProviderDraft,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.providers.set_cli_runtime_draft(draft.clone());

        let id_input_state = cx.new(|cx| {
            let mut state = InputState::new(window, cx)
                .placeholder(t!("providers.cli.dialog.id_placeholder").to_string());
            state.set_value(draft.id.clone(), window, cx);
            state
        });
        let display_name_input_state = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder(
                cli_provider_settings::cli_runtime_provider_default_display_name(draft.kind)
                    .to_owned(),
            );
            state.set_value(draft.display_name.clone(), window, cx);
            state
        });
        let binary_input_state = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder(
                cli_provider_settings::cli_runtime_provider_default_binary_path(draft.kind)
                    .to_owned(),
            );
            state.set_value(draft.binary_path.clone(), window, cx);
            state
        });
        let home_input_state = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder(
                cli_provider_settings::cli_runtime_provider_default_home_path(draft.kind)
                    .to_owned(),
            );
            state.set_value(draft.home_path.clone(), window, cx);
            state
        });
        let shadow_home_input_state = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder(
                cli_provider_settings::cli_runtime_provider_default_shadow_home_placeholder(
                    draft.kind,
                )
                .to_owned(),
            );
            state.set_value(draft.shadow_home_path.clone(), window, cx);
            state
        });
        let enabled_state = Rc::new(Cell::new(draft.enabled));
        let did_focus_initial_field = Rc::new(Cell::new(false));
        let field_error: Rc<RefCell<Option<CLIRuntimeProviderDialogFieldError>>> =
            Rc::new(RefCell::new(None));
        let desktop_entity = cx.entity().clone();
        let dialog_title = cli_runtime_provider_dialog_title(&draft.mode);

        let save_cli_provider: Rc<dyn Fn(&mut App) -> bool> = Rc::new({
            let desktop_entity = desktop_entity.clone();
            let field_error = field_error.clone();
            let id_input_state = id_input_state.clone();
            let display_name_input_state = display_name_input_state.clone();
            let binary_input_state = binary_input_state.clone();
            let home_input_state = home_input_state.clone();
            let shadow_home_input_state = shadow_home_input_state.clone();
            let enabled_state = enabled_state.clone();
            let draft_seed = draft.clone();
            move |cx| {
                let mut draft = draft_seed.clone();
                draft.set_text_field(
                    CLIRuntimeProviderDraftField::Id,
                    id_input_state.read(cx).value().to_string(),
                );
                draft.set_text_field(
                    CLIRuntimeProviderDraftField::DisplayName,
                    display_name_input_state.read(cx).value().to_string(),
                );
                draft.set_text_field(
                    CLIRuntimeProviderDraftField::BinaryPath,
                    binary_input_state.read(cx).value().to_string(),
                );
                draft.set_text_field(
                    CLIRuntimeProviderDraftField::HomePath,
                    home_input_state.read(cx).value().to_string(),
                );
                draft.set_text_field(
                    CLIRuntimeProviderDraftField::ShadowHomePath,
                    shadow_home_input_state.read(cx).value().to_string(),
                );
                draft.enabled = enabled_state.get();

                let mut result = Ok(());
                let _ = desktop_entity.update(cx, |view, cx| {
                    view.providers.set_cli_runtime_draft(draft.clone());
                    result = view.save_cli_runtime_provider_draft(draft.clone(), cx);
                    cx.notify();
                });
                match result {
                    Ok(()) => {
                        *field_error.borrow_mut() = None;
                        true
                    }
                    Err(rejection) => {
                        *field_error.borrow_mut() =
                            Some(cli_runtime_provider_dialog_field_error(&rejection));
                        let _ = desktop_entity.update(cx, |_, cx| cx.notify());
                        false
                    }
                }
            }
        });

        window.open_dialog(cx, move |dialog, window, cx| {
            if !did_focus_initial_field.get() {
                did_focus_initial_field.set(true);
                id_input_state.update(cx, |state, cx| state.focus(window, cx));
            }
            let field_error_message = field_error.borrow().clone();

            dialog
                .w(px(420.))
                .gap_1()
                .rounded_2xl()
                .close_button(true)
                .overlay_closable(true)
                .keyboard(true)
                .title(
                    h_flex()
                        .items_center()
                        .gap_2()
                        .child(Icon::new(crate::assets::PioneerIconName::Terminal).size_4())
                        .child(
                            div()
                                .text_base()
                                .font_semibold()
                                .child(dialog_title.clone()),
                        ),
                )
                .on_ok({
                    let save_cli_provider = save_cli_provider.clone();
                    move |_, _, cx| save_cli_provider(cx)
                })
                .footer(DialogFooter::new().children({
                    let save_cli_provider = save_cli_provider.clone();
                    let desktop_entity = desktop_entity.clone();

                    vec![
                        default_outline_button("cli-runtime-provider-dialog-cancel")
                            .label(t!("buttons.cancel").to_string())
                            .outline()
                            .on_click({
                                let desktop_entity = desktop_entity.clone();
                                move |_, window, cx| {
                                    let _ = desktop_entity.update(cx, |view, cx| {
                                        view.providers.clear_cli_runtime_draft();
                                        cx.notify();
                                    });
                                    window.close_dialog(cx);
                                }
                            })
                            .into_any_element(),
                        default_primary_button("cli-runtime-provider-dialog-save")
                            .label(t!("providers.button.submit").to_string())
                            .on_click({
                                let save_cli_provider = save_cli_provider.clone();
                                move |_, window, cx| {
                                    if save_cli_provider(cx) {
                                        window.close_dialog(cx);
                                    }
                                }
                            })
                            .into_any_element(),
                    ]
                }))
                .child(
                    v_flex()
                        .w_full()
                        .pt_4()
                        .pb_5()
                        .gap_4()
                        .child(
                            v_form()
                                .child(
                                    field()
                                        .label(t!("providers.cli.dialog.id_label").to_string())
                                        .child(Input::new(&id_input_state).min_w_0()),
                                )
                                .when_some(
                                    cli_runtime_provider_dialog_error_for_field(
                                        field_error_message.as_ref(),
                                        Some(CLIRuntimeProviderDraftField::Id),
                                    ),
                                    |this, error| {
                                        this.child(cli_runtime_provider_dialog_error_field(
                                            error, cx,
                                        ))
                                    },
                                )
                                .child(
                                    field()
                                        .label(
                                            t!("providers.cli.dialog.display_name_label")
                                                .to_string(),
                                        )
                                        .child(Input::new(&display_name_input_state).min_w_0()),
                                )
                                .when_some(
                                    cli_runtime_provider_dialog_error_for_field(
                                        field_error_message.as_ref(),
                                        Some(CLIRuntimeProviderDraftField::DisplayName),
                                    ),
                                    |this, error| {
                                        this.child(cli_runtime_provider_dialog_error_field(
                                            error, cx,
                                        ))
                                    },
                                )
                                .child(
                                    field()
                                        .label(t!("providers.cli.dialog.binary_label").to_string())
                                        .child(Input::new(&binary_input_state).min_w_0()),
                                )
                                .when_some(
                                    cli_runtime_provider_dialog_error_for_field(
                                        field_error_message.as_ref(),
                                        Some(CLIRuntimeProviderDraftField::BinaryPath),
                                    ),
                                    |this, error| {
                                        this.child(cli_runtime_provider_dialog_error_field(
                                            error, cx,
                                        ))
                                    },
                                )
                                .child(
                                    field()
                                        .label(t!("providers.cli.dialog.home_label").to_string())
                                        .child(Input::new(&home_input_state).min_w_0()),
                                )
                                .when_some(
                                    cli_runtime_provider_dialog_error_for_field(
                                        field_error_message.as_ref(),
                                        Some(CLIRuntimeProviderDraftField::HomePath),
                                    ),
                                    |this, error| {
                                        this.child(cli_runtime_provider_dialog_error_field(
                                            error, cx,
                                        ))
                                    },
                                )
                                .child(
                                    field()
                                        .label(
                                            t!("providers.cli.dialog.shadow_home_label")
                                                .to_string(),
                                        )
                                        .child(Input::new(&shadow_home_input_state).min_w_0()),
                                )
                                .when_some(
                                    cli_runtime_provider_dialog_error_for_field(
                                        field_error_message.as_ref(),
                                        Some(CLIRuntimeProviderDraftField::ShadowHomePath),
                                    ),
                                    |this, error| {
                                        this.child(cli_runtime_provider_dialog_error_field(
                                            error, cx,
                                        ))
                                    },
                                )
                                .when_some(
                                    cli_runtime_provider_dialog_error_for_field(
                                        field_error_message.as_ref(),
                                        None,
                                    ),
                                    |this, error| {
                                        this.child(cli_runtime_provider_dialog_error_field(
                                            error, cx,
                                        ))
                                    },
                                ),
                        )
                        .child(
                            h_flex()
                                .w_full()
                                .items_center()
                                .justify_between()
                                .gap_6()
                                .py_1()
                                .child(
                                    div().text_sm().font_medium().child(
                                        t!("providers.cli.dialog.enabled_label").to_string(),
                                    ),
                                )
                                .child(
                                    Switch::new("cli-runtime-provider-dialog-enabled")
                                        .checked(enabled_state.get())
                                        .on_click({
                                            let enabled_state = enabled_state.clone();
                                            let desktop_entity = desktop_entity.clone();
                                            move |enabled, _, cx| {
                                                enabled_state.set(*enabled);
                                                let _ = desktop_entity.update(cx, |view, cx| {
                                                    view.providers
                                                        .set_cli_runtime_draft_enabled(*enabled);
                                                    cx.notify();
                                                });
                                            }
                                        }),
                                ),
                        ),
                )
        });
    }
}

fn cli_runtime_provider_dialog_title(mode: &CLIRuntimeProviderDraftMode) -> String {
    match mode {
        CLIRuntimeProviderDraftMode::Create => t!("providers.cli.dialog.title_add").to_string(),
        CLIRuntimeProviderDraftMode::Edit { .. } => {
            t!("providers.cli.dialog.title_edit").to_string()
        }
        CLIRuntimeProviderDraftMode::Duplicate { .. } => {
            t!("providers.cli.dialog.title_duplicate").to_string()
        }
    }
}

#[derive(Clone)]
struct CLIRuntimeProviderDialogFieldError {
    field: Option<CLIRuntimeProviderDraftField>,
    message: String,
}

fn cli_runtime_provider_dialog_field_error(
    rejection: &CLIRuntimeProviderSettingsRejection,
) -> CLIRuntimeProviderDialogFieldError {
    let field = match rejection {
        CLIRuntimeProviderSettingsRejection::EmptyId
        | CLIRuntimeProviderSettingsRejection::InvalidId { .. }
        | CLIRuntimeProviderSettingsRejection::DuplicateId { .. } => {
            Some(CLIRuntimeProviderDraftField::Id)
        }
        CLIRuntimeProviderSettingsRejection::DuplicateDisplayName { .. } => {
            Some(CLIRuntimeProviderDraftField::DisplayName)
        }
        CLIRuntimeProviderSettingsRejection::EmptyPath { field }
        | CLIRuntimeProviderSettingsRejection::InvalidPath { field, .. } => {
            cli_runtime_provider_draft_field_from_settings_field(field.as_str())
        }
        CLIRuntimeProviderSettingsRejection::ShadowHomeMatchesHome => {
            Some(CLIRuntimeProviderDraftField::ShadowHomePath)
        }
        CLIRuntimeProviderSettingsRejection::MissingSettings
        | CLIRuntimeProviderSettingsRejection::MissingRuntime { .. }
        | CLIRuntimeProviderSettingsRejection::UnsupportedKind { .. } => None,
    };

    CLIRuntimeProviderDialogFieldError {
        field,
        message: cli_runtime_provider_settings_rejection_message(rejection),
    }
}

fn cli_runtime_provider_dialog_error_for_field(
    error: Option<&CLIRuntimeProviderDialogFieldError>,
    field: Option<CLIRuntimeProviderDraftField>,
) -> Option<String> {
    let error = error?;
    (error.field == field).then(|| error.message.clone())
}

fn cli_runtime_provider_draft_field_from_settings_field(
    field: &str,
) -> Option<CLIRuntimeProviderDraftField> {
    match field {
        "display_name" => Some(CLIRuntimeProviderDraftField::DisplayName),
        "binary_path" => Some(CLIRuntimeProviderDraftField::BinaryPath),
        "home_path" => Some(CLIRuntimeProviderDraftField::HomePath),
        "shadow_home_path" => Some(CLIRuntimeProviderDraftField::ShadowHomePath),
        _ => None,
    }
}

fn cli_runtime_provider_dialog_error_field(
    error: String,
    cx: &mut App,
) -> gpui_component::form::Field {
    field().label_indent(false).child(
        div()
            .text_sm()
            .text_color(cx.theme().danger)
            .line_height(relative(1.3))
            .whitespace_normal()
            .child(error),
    )
}
