use crate::{
    buttons::{default_outline_button, default_primary_button},
    catalog::McpCatalogView,
};
use gpui_kit::component::{
    StyledExt, WindowExt,
    dialog::DialogFooter,
    form::{field, v_form},
    input::{Textarea, TextareaState},
    *,
};
use gpui_kit::{prelude::*, *};
use pioneer_client::mcp::{actions as mcp_actions, operations::*};
use std::sync::Arc;
pub(crate) struct McpConfigForm {
    input: Entity<TextareaState>,
    workspace: String,
    operation: Option<u64>,
    publication: Option<Arc<McpActionPublication>>,
    error: Option<String>,
    open: bool,
    parent_generation: u64,
    lifetime: Entity<crate::dialog_lifetime::DialogLifetime>,
}
impl McpConfigForm {
    fn pending(&self) -> bool {
        self.publication
            .as_ref()
            .is_some_and(|p| p.state == McpActionState::Pending)
    }
}
impl McpCatalogView {
    pub(crate) fn confirm_uninstall_mcp_server(
        &mut self,
        server_id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return;
        }
        let Some(name) = self
            .input
            .mcp_servers
            .iter()
            .find(|s| s.id == server_id)
            .map(|s| s.name.clone())
        else {
            return;
        };
        let answer = window.prompt(
            PromptLevel::Info,
            &t!("mcp.dialog.uninstall_title", name = name.as_str()).to_string(),
            Some(&t!("mcp.dialog.uninstall_description").to_string()),
            &[
                PromptButton::new(t!("mcp.dialog.uninstall").to_string()),
                PromptButton::cancel(t!("buttons.cancel").to_string()),
            ],
            cx,
        );
        let generation = self.parent_generation;
        self.native_task = Some(cx.spawn(async move |view: WeakEntity<Self>, cx| {
            if answer.await == Ok(0) {
                let _ = view.update(cx, |view, cx| {
                    if view.parent_generation == generation {
                        view.uninstall_mcp_server(server_id, cx);
                    }
                });
            }
        }));
    }
    pub(crate) fn open_mcp_config_dialog(
        &mut self,
        initial_config: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return;
        }
        let Some(workspace) = self
            .input
            .navigation_input
            .workspace_id()
            .map(str::to_owned)
        else {
            return;
        };
        let input = cx.new(|cx| {
            TextareaState::new(window, cx)
                .auto_grow(5, 14)
                .placeholder(r#"{ "mcpServers": { ... } }"#)
        });
        if let Some(value) = initial_config {
            input.update(cx, |input, cx| input.set_value(value, window, cx));
        }
        let clear_input = input.clone();
        let initial_focus = input.clone();
        let lifetime = crate::dialog_lifetime::DialogLifetime::new(
            move |window, cx| {
                clear_input.update(cx, |input, cx| input.set_value("", window, cx));
            },
            cx,
        );
        self.dialogs.push(lifetime.clone());
        let attach = lifetime.clone();
        let form = cx.new(|_| McpConfigForm {
            input,
            workspace,
            operation: None,
            publication: None,
            error: None,
            open: true,
            parent_generation: self.parent_generation,
            lifetime,
        });
        attach.update(cx, |lifetime, cx| lifetime.track_form(&form, cx));
        self.config_form = Some(form.clone());
        let owner = cx.weak_entity();
        window.open_dialog(cx, move |dialog, _, cx| {
            let state = form.read(cx);
            let pending = state.pending();
            let input = state.input.clone();
            let error = state.error.clone();
            let submit = {
                let owner = owner.clone();
                let form = form.clone();
                move |_: &ClickEvent, window: &mut Window, cx: &mut App| {
                    let _ = owner.update(cx, |view, cx| view.submit_config(&form, window, cx));
                    false
                }
            };
            dialog
                .gap_1()
                .rounded_2xl()
                .close_button(!pending)
                .overlay_closable(!pending)
                .keyboard(!pending)
                .title(
                    div()
                        .text_base()
                        .font_semibold()
                        .child(t!("mcp.dialog.install_title").to_string()),
                )
                .on_close({
                    let form = form.clone();
                    move |_, window, cx| {
                        form.update(cx, |form, cx| {
                            form.open = false;
                            form.lifetime
                                .update(cx, |lifetime, cx| lifetime.dismissed(window, cx));
                        });
                    }
                })
                .on_ok(submit)
                .footer(DialogFooter::new().children(vec![
                        default_outline_button("mcp-config-dialog-cancel")
                            .label(t!("buttons.cancel").to_string())
                            .outline()
                            .disabled(pending)
                            .on_click(|_, window, cx| window.close_dialog(cx))
                            .into_any_element(),
                        default_primary_button("mcp-config-dialog-save")
                            .label(t!("mcp.dialog.install").to_string())
                            .disabled(pending)
                            .loading(pending)
                            .on_click({
                                let owner = owner.clone();
                                let form = form.clone();
                                move |_, window, cx| {
                                    let _ = owner.update(cx, |view, cx| {
                                        view.submit_config(&form, window, cx)
                                    });
                                }
                            })
                            .into_any_element(),
                    ]))
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
                                        .child(Textarea::new(&input).min_w_0()),
                                )
                                .when_some(error, |form, error| {
                                    form.child(
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
        attach.update(cx, |lifetime, cx| lifetime.attach(window, cx));
        initial_focus.update(cx, |input, cx| input.focus(window, cx));
    }
    fn submit_config(
        &mut self,
        form: &Entity<McpConfigForm>,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        form.update(cx, |form, cx| {
            if !form.open
                || !form.lifetime.read(cx).valid()
                || form.pending()
                || form.parent_generation != self.parent_generation
            {
                return;
            }
            let config = form.input.read(cx).value().to_string();
            if let Err(error) = mcp_actions::validate_mcp_config_for_submit(&config) {
                form.error = Some(mcp_config_validation_error_message(error));
                cx.notify();
                return;
            }
            match self.client.mcp_intent(
                &form.workspace,
                McpIntent::Configure {
                    config_json: config,
                },
            ) {
                Ok(id) => {
                    form.operation = Some(id);
                    form.publication = self
                        .client
                        .mcp_action_snapshot(&form.workspace, "configuration");
                    form.error = None;
                }
                Err(_) => form.error = Some(t!("mcp.error.gateway_not_connected").to_string()),
            }
            cx.notify();
        });
        cx.notify();
    }
    pub(crate) fn sync_config_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(form) = self.config_form.clone() else {
            return;
        };
        let close = form.update(cx, |form, cx| {
            if !form.open {
                return false;
            }
            if form.parent_generation != self.parent_generation {
                form.open = false;
                form.input
                    .update(cx, |input, cx| input.set_value("", window, cx));
                return true;
            }
            let publication = self
                .client
                .mcp_action_snapshot(&form.workspace, "configuration");
            if form.operation.is_none()
                || publication.as_ref().map(|p| p.operation_id) != form.operation
                || form.publication == publication
            {
                return false;
            }
            form.publication = publication;
            cx.notify();
            match form.publication.as_ref().map(|p| p.state) {
                Some(McpActionState::Succeeded) => {
                    form.open = false;
                    form.input
                        .update(cx, |input, cx| input.set_value("", window, cx));
                    true
                }
                Some(McpActionState::Failed) => {
                    form.error = Some(
                        form.publication
                            .as_ref()
                            .and_then(|p| p.field_error.as_ref())
                            .map(mcp_install_field_error)
                            .unwrap_or_else(|| {
                                t!("mcp.dialog.error.install_failed", error = "").to_string()
                            }),
                    );
                    false
                }
                _ => false,
            }
        });
        if close {
            form.read(cx)
                .lifetime
                .clone()
                .update(cx, |lifetime, cx| lifetime.invalidate(window, cx));
            self.config_form = None;
        } else if !form.read(cx).open {
            self.config_form = None;
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
                    pioneer_client::mcp::types::McpDiagnosticLevel::Error => {
                        t!("mcp.dialog.error.level_error").to_string()
                    }
                    pioneer_client::mcp::types::McpDiagnosticLevel::Warning => {
                        t!("mcp.dialog.error.level_warning").to_string()
                    }
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
