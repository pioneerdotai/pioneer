use crate::app::root::{GatewayConnectionState, PioneerDesktop};
use crate::components::buttonts::{default_outline_button, default_primary_button};
use gpui::{prelude::*, *};
use gpui_component::{
    StyledExt, WindowExt,
    form::{field, v_form},
    input::{Input, InputState},
    *,
};
use pioneer_client::mcp::{
    actions as mcp_actions,
    list::{self as mcp_list, MCP_INSTALL_PENDING_KEY},
};
use pioneer_protocol::McpDiagnosticLevel;
use std::{cell::RefCell, rc::Rc};
use tracing::warn;

const MCP_CONFIG_PLACEHOLDER: &str = r#"{ "mcpServers": { ... } }"#;

impl PioneerDesktop {
    pub(super) fn confirm_uninstall_mcp_server(
        &mut self,
        name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(name) = mcp_list::normalize_mcp_server_name(name.as_str()) else {
            self.mcp_error = Some(t!("mcp.error.server_name_required").to_string());
            return;
        };

        let title = t!("mcp.dialog.uninstall_title", name = name.as_str()).to_string();
        let description = t!("mcp.dialog.uninstall_description").to_string();
        let answer = window.prompt(
            PromptLevel::Info,
            title.as_str(),
            Some(description.as_str()),
            &[
                PromptButton::new(t!("mcp.dialog.uninstall").to_string()),
                PromptButton::cancel(t!("buttons.cancel").to_string()),
            ],
            cx,
        );

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                if answer.await != Ok(0) {
                    return;
                }

                let _ = this.update(&mut cx, |view, cx| {
                    view.uninstall_mcp_server(name, cx);
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn open_mcp_config_dialog(
        &mut self,
        initial_config: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let config_input_state = cx.new(|cx| {
            let state = InputState::new(window, cx)
                .multi_line(true)
                .auto_grow(5, 14)
                .placeholder(MCP_CONFIG_PLACEHOLDER.to_owned());
            state
        });
        if let Some(initial_config) = initial_config {
            config_input_state.update(cx, |state, cx| {
                state.set_value(initial_config, window, cx);
            });
        }

        let desktop_entity = cx.entity().clone();
        let field_error: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));
        let install_pending = Rc::new(RefCell::new(false));
        let install_config: Rc<dyn Fn(&mut Window, &mut App) -> bool> = Rc::new({
            let desktop_entity = desktop_entity.clone();
            let config_input_state = config_input_state.clone();
            let field_error = field_error.clone();
            let install_pending = install_pending.clone();
            move |window, cx| {
                if *install_pending.borrow() {
                    return false;
                }

                let raw_config = config_input_state.read(cx).value().to_string();
                let Some(config_json) = mcp_actions::normalize_mcp_config_json(raw_config.as_str())
                else {
                    set_mcp_config_field_error(
                        &field_error,
                        t!("mcp.dialog.error.config_required").to_string(),
                        &desktop_entity,
                        cx,
                    );
                    return false;
                };
                if let Err(error) =
                    mcp_actions::validate_mcp_config_for_submit(config_json.as_str())
                {
                    set_mcp_config_field_error(
                        &field_error,
                        mcp_config_validation_error_message(error),
                        &desktop_entity,
                        cx,
                    );
                    return false;
                }

                let request = desktop_entity.update(cx, |view, cx| {
                    let scope = match mcp_actions::plan_mcp_action_scope(
                        matches!(
                            view.gateway.connection_state,
                            GatewayConnectionState::Connected
                        ),
                        view.gateway.ws_connection_id,
                        view.mcp_workspace_scope(),
                    ) {
                        mcp_actions::McpActionScopePlan::Send(scope) => scope,
                        mcp_actions::McpActionScopePlan::Unavailable(reason) => {
                            return Err(mcp_dialog_action_unavailable_message(reason));
                        }
                    };

                    view.mcp_error = None;
                    view.mark_mcp_pending(MCP_INSTALL_PENDING_KEY, true);
                    cx.notify();

                    Ok((
                        scope.connection_id,
                        scope.workspace_id,
                        view.gateway.ws_command_sender.clone(),
                    ))
                });
                let (connection_id, workspace_id, ws_sender) = match request {
                    Ok(request) => request,
                    Err(error) => {
                        set_mcp_config_field_error(&field_error, error, &desktop_entity, cx);
                        return false;
                    }
                };

                *field_error.borrow_mut() = None;
                *install_pending.borrow_mut() = true;
                let _ = desktop_entity.update(cx, |_, cx| cx.notify());

                window
                    .spawn(cx, {
                        let desktop_entity = desktop_entity.clone();
                        let field_error = field_error.clone();
                        let install_pending = install_pending.clone();
                        async move |cx| {
                            let result = cx
                                .background_spawn(async move {
                                    ws_sender.mcp_install(mcp_actions::mcp_install_params(
                                        workspace_id,
                                        config_json,
                                    ))
                                })
                                .await;

                            let reduction = match result {
                                Ok(response) => mcp_actions::reduce_mcp_install_finish(
                                    mcp_actions::McpInstallFinishOutcome::Response(response),
                                ),
                                Err(error) => {
                                    let details = format!("{error:#}");
                                    let message = t!(
                                        "mcp.dialog.error.install_failed",
                                        error = details.as_str()
                                    )
                                    .to_string();
                                    warn!(
                                        error = %format!("{error:#}"),
                                        "failed to install MCP server"
                                    );
                                    mcp_actions::reduce_mcp_install_finish(
                                        mcp_actions::McpInstallFinishOutcome::Failure {
                                            field_error: message,
                                        },
                                    )
                                }
                            };

                            *install_pending.borrow_mut() = false;
                            *field_error.borrow_mut() =
                                reduction.field_error.as_ref().map(mcp_install_field_error);

                            let _ = desktop_entity.update(cx, |view, cx| {
                                if mcp_actions::mcp_action_matches_connection(
                                    connection_id,
                                    view.gateway.ws_connection_id,
                                ) {
                                    view.mark_mcp_pending(
                                        reduction.pending.target.name.as_str(),
                                        reduction.pending.pending,
                                    );
                                    if reduction.clear_mcp_error {
                                        view.mcp_error = None;
                                    }
                                    if reduction.queue_refresh {
                                        view.queue_mcp_refresh();
                                    }
                                }
                                cx.notify();
                            });

                            if reduction.close_dialog {
                                let _ = cx.update(|window, cx| window.close_dialog(cx));
                            }
                        }
                    })
                    .detach();

                false
            }
        });

        window.open_dialog(cx, move |dialog, window, cx| {
            config_input_state.update(cx, |state, cx| state.focus(window, cx));
            let field_error_message = field_error.borrow().clone();
            let is_install_pending = *install_pending.borrow();

            dialog
                .gap_1()
                .rounded_2xl()
                .close_button(!is_install_pending)
                .overlay_closable(!is_install_pending)
                .keyboard(!is_install_pending)
                .title(
                    div()
                        .text_base()
                        .font_semibold()
                        .child(t!("mcp.dialog.install_title").to_string()),
                )
                .on_ok({
                    let install_config = install_config.clone();
                    move |_, window, cx| install_config(window, cx)
                })
                .footer({
                    let install_config = install_config.clone();
                    move |_, _, _, _| {
                        vec![
                            default_outline_button("mcp-config-dialog-cancel")
                                .label(t!("buttons.cancel").to_string())
                                .outline()
                                .disabled(is_install_pending)
                                .on_click(|_, window, cx| {
                                    window.close_dialog(cx);
                                })
                                .into_any_element(),
                            default_primary_button("mcp-config-dialog-save")
                                .label(t!("mcp.dialog.install").to_string())
                                .disabled(is_install_pending)
                                .loading(is_install_pending)
                                .on_click({
                                    let install_config = install_config.clone();
                                    move |_, window, cx| {
                                        install_config(window, cx);
                                    }
                                })
                                .into_any_element(),
                        ]
                    }
                })
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
                                        .label(t!("mcp.dialog.config_json").to_string())
                                        .child(Input::new(&config_input_state).min_w_0()),
                                )
                                .when_some(field_error_message, |this, error| {
                                    this.child(
                                        field()
                                            .label_indent(false)
                                            .child(mcp_config_field_error(error, cx)),
                                    )
                                }),
                        )
                        .child(
                            div()
                                .text_xs()
                                .opacity(0.6)
                                .line_height(relative(1.3))
                                .child(t!("mcp.dialog.secret_store_hint").to_string()),
                        ),
                )
        });
    }
}

fn set_mcp_config_field_error(
    field_error: &Rc<RefCell<Option<String>>>,
    error: String,
    desktop_entity: &Entity<PioneerDesktop>,
    cx: &mut App,
) {
    *field_error.borrow_mut() = Some(error);
    let _ = desktop_entity.update(cx, |_, cx| cx.notify());
}

fn mcp_config_field_error(error: String, cx: &mut App) -> AnyElement {
    v_flex()
        .gap_0p5()
        .text_xs()
        .line_height(relative(1.3))
        .text_color(cx.theme().danger)
        .children(
            error
                .lines()
                .map(|line| div().child(line.to_owned()).into_any_element()),
        )
        .into_any_element()
}

fn mcp_install_field_error(error: &mcp_actions::McpInstallFieldError) -> String {
    let issues = match error {
        mcp_actions::McpInstallFieldError::Failure { message } => return message.clone(),
        mcp_actions::McpInstallFieldError::ValidationIssues(issues) => issues,
    };

    let mut lines = issues
        .iter()
        .map(|issue| match issue {
            mcp_actions::McpInstallFieldIssue::ServerValidationError { name } => t!(
                "mcp.dialog.error.server_validation_error",
                name = name.as_str()
            )
            .to_string(),
            mcp_actions::McpInstallFieldIssue::Diagnostic {
                name,
                level,
                message,
                field_path,
            } => {
                let level = match *level {
                    McpDiagnosticLevel::Error => t!("mcp.dialog.error.level_error").to_string(),
                    McpDiagnosticLevel::Warning => t!("mcp.dialog.error.level_warning").to_string(),
                };
                let field = field_path
                    .as_deref()
                    .map(|field| format!(" ({field})"))
                    .unwrap_or_default();
                t!(
                    "mcp.dialog.error.diagnostic",
                    name = name.as_str(),
                    level = level.as_str(),
                    message = message.as_str(),
                    field = field.as_str()
                )
                .to_string()
            }
        })
        .collect::<Vec<_>>();

    if lines.is_empty() {
        lines.push(t!("mcp.dialog.error.validation_failed").to_string());
    }

    lines.join("\n")
}

fn mcp_dialog_action_unavailable_message(reason: mcp_actions::McpActionUnavailable) -> String {
    match reason {
        mcp_actions::McpActionUnavailable::GatewayNotConnected => {
            t!("mcp.dialog.error.gateway_not_connected").to_string()
        }
        mcp_actions::McpActionUnavailable::WorkspaceNotSelected => {
            t!("mcp.dialog.error.workspace_not_selected").to_string()
        }
    }
}

fn mcp_config_validation_error_message(error: mcp_actions::McpConfigValidationError) -> String {
    match error {
        mcp_actions::McpConfigValidationError::InvalidJson { error } => {
            t!("mcp.dialog.error.config_invalid", error = error.as_str()).to_string()
        }
        mcp_actions::McpConfigValidationError::ServersRequired => {
            t!("mcp.dialog.error.servers_required").to_string()
        }
        mcp_actions::McpConfigValidationError::ServersEmpty => {
            t!("mcp.dialog.error.servers_empty").to_string()
        }
        mcp_actions::McpConfigValidationError::ServerNameEmpty => {
            t!("mcp.dialog.error.server_name_empty").to_string()
        }
        mcp_actions::McpConfigValidationError::ServerConfigObject { name } => t!(
            "mcp.dialog.error.server_config_object",
            name = name.as_str()
        )
        .to_string(),
        mcp_actions::McpConfigValidationError::CommandOrUrlRequired { name } => t!(
            "mcp.dialog.error.command_or_url_required",
            name = name.as_str()
        )
        .to_string(),
        mcp_actions::McpConfigValidationError::CommandUrlExclusive { name } => t!(
            "mcp.dialog.error.command_url_exclusive",
            name = name.as_str()
        )
        .to_string(),
    }
}
