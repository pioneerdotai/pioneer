use super::actions::MCP_INSTALL_PENDING_KEY;
use crate::app::root::{GatewayConnectionState, PioneerDesktop};
use crate::components::buttonts::{default_outline_button, default_primary_button};
use gpui::{prelude::*, *};
use gpui_component::{
    StyledExt, WindowExt,
    input::{Input, InputState},
    label::Label,
    *,
};
use pioneer_protocol::{
    McpDiagnosticLevel, McpInstallParams, McpInstallResponse, McpInstallResultStatus,
    McpInstallStatus, McpScopeKind,
};
use serde_json::Value as JsonValue;
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
        let name = name.trim().to_owned();
        if name.is_empty() {
            self.mcp_error = Some(t!("mcp.error.server_name_required").to_string());
            return;
        }

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

                let config_json = config_input_state.read(cx).value().trim().to_owned();
                if config_json.is_empty() {
                    set_mcp_config_field_error(
                        &field_error,
                        t!("mcp.dialog.error.config_required").to_string(),
                        &desktop_entity,
                        cx,
                    );
                    return false;
                }
                if let Err(error) = validate_mcp_config_for_submit(config_json.as_str()) {
                    set_mcp_config_field_error(&field_error, error, &desktop_entity, cx);
                    return false;
                }

                let request = desktop_entity.update(cx, |view, cx| {
                    if view.gateway.connection_state != GatewayConnectionState::Connected {
                        return Err(t!("mcp.dialog.error.gateway_not_connected").to_string());
                    }
                    let Some(connection_id) = view.gateway.ws_connection_id else {
                        return Err(t!("mcp.dialog.error.gateway_not_connected").to_string());
                    };
                    let Some(workspace_id) = view.mcp_workspace_scope() else {
                        return Err(t!("mcp.dialog.error.workspace_not_selected").to_string());
                    };

                    view.mcp_error = None;
                    view.mark_mcp_pending(MCP_INSTALL_PENDING_KEY, true);
                    cx.notify();

                    Ok((
                        connection_id,
                        workspace_id,
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
                                    ws_sender.mcp_install(McpInstallParams {
                                        workspace_id,
                                        config_json,
                                        scope_kind: McpScopeKind::Workspace,
                                        enabled: true,
                                        allow_implicit_invocation: false,
                                    })
                                })
                                .await;

                            let mut close_dialog = false;
                            let mut should_refresh = false;
                            let mut error_message = None;

                            match result {
                                Ok(response) => {
                                    should_refresh = mcp_install_has_success(&response);
                                    match response.status {
                                        McpInstallStatus::Ok => {
                                            close_dialog = true;
                                        }
                                        McpInstallStatus::Partial
                                        | McpInstallStatus::ValidationError => {
                                            error_message =
                                                Some(mcp_install_response_field_error(&response));
                                        }
                                    }
                                }
                                Err(error) => {
                                    let details = format!("{error:#}");
                                    let message = t!(
                                        "mcp.dialog.error.install_failed",
                                        error = details.as_str()
                                    )
                                    .to_string();
                                    error_message = Some(message);
                                    warn!(
                                        error = %format!("{error:#}"),
                                        "failed to install MCP server"
                                    );
                                }
                            }

                            *install_pending.borrow_mut() = false;
                            *field_error.borrow_mut() = error_message;

                            let _ = desktop_entity.update(cx, |view, cx| {
                                if view.gateway.ws_connection_id == Some(connection_id) {
                                    view.mark_mcp_pending(MCP_INSTALL_PENDING_KEY, false);
                                    if close_dialog {
                                        view.mcp_error = None;
                                    }
                                    if should_refresh || close_dialog {
                                        view.queue_mcp_refresh();
                                    }
                                }
                                cx.notify();
                            });

                            if close_dialog {
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
                            v_flex()
                                .gap_1()
                                .child(Label::new(t!("mcp.dialog.config_json")).text_xs())
                                .child(Input::new(&config_input_state))
                                .when_some(field_error_message, |this, error| {
                                    this.child(mcp_config_field_error(error, cx))
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

fn mcp_install_has_success(response: &McpInstallResponse) -> bool {
    response.servers.iter().any(|item| {
        matches!(
            item.status,
            McpInstallResultStatus::Installed | McpInstallResultStatus::Updated
        )
    })
}

fn mcp_install_response_field_error(response: &McpInstallResponse) -> String {
    let mut lines = Vec::new();

    for item in response
        .servers
        .iter()
        .filter(|item| item.status == McpInstallResultStatus::ValidationError)
    {
        if item.diagnostics.is_empty() {
            lines.push(
                t!(
                    "mcp.dialog.error.server_validation_error",
                    name = item.name.as_str()
                )
                .to_string(),
            );
            continue;
        }

        for diagnostic in &item.diagnostics {
            let level = match diagnostic.level {
                McpDiagnosticLevel::Error => t!("mcp.dialog.error.level_error").to_string(),
                McpDiagnosticLevel::Warning => t!("mcp.dialog.error.level_warning").to_string(),
            };
            let field = diagnostic
                .field_path
                .as_ref()
                .map(|field| format!(" ({field})"))
                .unwrap_or_default();
            lines.push(
                t!(
                    "mcp.dialog.error.diagnostic",
                    name = item.name.as_str(),
                    level = level.as_str(),
                    message = diagnostic.message.as_str(),
                    field = field.as_str()
                )
                .to_string(),
            );
        }
    }

    if lines.is_empty() {
        lines.push(t!("mcp.dialog.error.validation_failed").to_string());
    }

    lines.join("\n")
}

fn validate_mcp_config_for_submit(raw: &str) -> Result<(), String> {
    let value = serde_json::from_str::<JsonValue>(raw).map_err(|error| {
        let error = error.to_string();
        t!("mcp.dialog.error.config_invalid", error = error.as_str()).to_string()
    })?;
    let servers = value
        .get("mcpServers")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| t!("mcp.dialog.error.servers_required").to_string())?;
    if servers.is_empty() {
        return Err(t!("mcp.dialog.error.servers_empty").to_string());
    }

    for (name, server) in servers {
        if name.trim().is_empty() {
            return Err(t!("mcp.dialog.error.server_name_empty").to_string());
        }
        let Some(server) = server.as_object() else {
            return Err(t!(
                "mcp.dialog.error.server_config_object",
                name = name.as_str()
            )
            .to_string());
        };
        let has_command = server.contains_key("command");
        let has_url = server.contains_key("url");
        match (has_command, has_url) {
            (true, false) | (false, true) => {}
            (false, false) => {
                return Err(t!(
                    "mcp.dialog.error.command_or_url_required",
                    name = name.as_str()
                )
                .to_string());
            }
            (true, true) => {
                return Err(t!(
                    "mcp.dialog.error.command_url_exclusive",
                    name = name.as_str()
                )
                .to_string());
            }
        }
    }

    Ok(())
}
