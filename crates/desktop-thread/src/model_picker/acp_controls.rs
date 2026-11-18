use super::*;
use gpui_kit::component::button::Button;
use gpui_kit::component::{
    menu::{DropdownMenu, PopupMenuItem},
    switch::Switch,
};
use pioneer_client::cli_runtime::acp::{
    AcpAuthMethodKind, AcpSessionOperation as Operation, AcpSessionParams, acp,
};
use pioneer_client::providers::operations::{ProviderCommand, ProviderOperationCompletion};

impl ComposerModelPickerView {
    pub(super) fn cleanup_acp_auth(&mut self, _cx: &mut App) {
        if let Some(owner) = self.acp_auth_owner.as_ref() {
            if self.client.release_acp_auth(owner.clone()).is_err() {
                return;
            }
        }
        self.acp_auth_owner = None;
        self.acp_auth_result = None;
    }
    fn run_acp_control(&mut self, operation: Operation, cx: &mut Context<Self>) {
        if self.acp_busy
            || (matches!(&operation, Operation::TerminalAuthStart { .. })
                && self.acp_auth_owner.is_some())
        {
            return;
        }
        let Some(snapshot) = self.state.input.acp_session.clone() else {
            return;
        };
        let Ok(prepared) = self
            .client
            .prepare_provider_command(ProviderCommand::AcpSession(AcpSessionParams {
                workspace_id: snapshot.workspace_id.clone(),
                thread_id: snapshot.thread_id.clone(),
                runtime_id: snapshot.runtime_id.clone(),
                operation: operation.clone(),
            }))
        else {
            return;
        };
        if let Operation::TerminalAuthStart {
            terminal_id,
            expected_generation,
            ..
        } = &operation
        {
            // Retain the exact ID before the worker can create the process.
            self.acp_auth_result = Some(serde_json::json!({"terminal_id":terminal_id}));
            self.acp_auth_owner = Some(AcpSessionParams {
                workspace_id: snapshot.workspace_id.clone(),
                thread_id: snapshot.thread_id.clone(),
                runtime_id: snapshot.runtime_id.clone(),
                operation: Operation::TerminalAuthClose {
                    terminal_id: terminal_id.clone(),
                    expected_generation: *expected_generation,
                },
            });
        }
        let control = operation.clone();
        self.acp_busy = true;
        self.acp_error = None;
        let client = Arc::downgrade(&self.client);
        self.acp_operation = Some(cx.spawn(async move |view, cx| {
            let result = cx
                .background_spawn(async move { prepared.execute(client) })
                .await;
            let _ = view.update(cx, |view, cx| {
                view.acp_busy = false;
                if view.state.input.acp_session.as_ref().is_none_or(|current| {
                    current.runtime_id != snapshot.runtime_id
                        || current.generation != snapshot.generation
                }) {
                    return;
                }
                match result {
                    Ok(ProviderOperationCompletion::AcpSession(response)) => {
                        // Auth terminal output is transient, owned by this dialog.
                        if matches!(control, Operation::TerminalAuthInput { .. }) {
                            if response
                                .result
                                .as_ref()
                                .and_then(|result| result.get("accepted_bytes"))
                                .and_then(serde_json::Value::as_u64)
                                .zip(match &control {
                                    Operation::TerminalAuthInput { data, .. } => {
                                        Some(data.len() as u64)
                                    }
                                    _ => None,
                                })
                                .is_some_and(|(accepted, sent)| accepted != sent)
                            {
                                view.acp_error = Some(t!("chat.acp.partial_input").to_string());
                            }
                        }
                        if matches!(control, Operation::TerminalAuthClose { .. }) {
                            view.acp_auth_result = None;
                            view.acp_auth_owner = None;
                        } else if matches!(
                            control,
                            Operation::TerminalAuthStart { .. }
                                | Operation::TerminalAuthInput { .. }
                                | Operation::TerminalAuthOutput { .. }
                        ) {
                            view.acp_auth_result = response.result;
                        } else {
                            view.acp_result = response.result;
                        }
                        if !view.dialog_open {
                            view.cleanup_acp_auth(cx);
                        }
                    }
                    Ok(_) => {}
                    Err(error) => {
                        if matches!(control, Operation::TerminalAuthStart { .. }) {
                            view.cleanup_acp_auth(cx);
                        }
                        view.acp_error = Some(error.to_string());
                    }
                }
                cx.notify();
            });
        }));
        cx.notify();
    }

    fn acp_button(
        state: &ModelSelectorDialogState,
        id: String,
        label: String,
        operation: Operation,
        disabled: bool,
    ) -> Button {
        let owner = state.owner.clone();
        Button::new(SharedString::from(id))
            .label(label)
            .disabled(disabled)
            .on_click(move |_, _, cx| {
                let _ = owner.update(cx, |view, cx| view.run_acp_control(operation.clone(), cx));
            })
    }

    pub(super) fn render_acp_controls(state: ModelSelectorDialogState, cx: &mut App) -> AnyElement {
        let Some(snapshot) = state.input.acp_session.as_ref() else {
            return div().into_any_element();
        };
        let Some(owner) = state.owner.upgrade() else {
            return div().into_any_element();
        };
        let view = owner.read(cx);
        let busy = view.acp_busy;
        let error = view.acp_error.clone();
        let result = view.acp_result.clone();
        let auth_result = view.acp_auth_result.clone();
        let auth_input = view.acp_auth_input.clone();
        let session_input = view.acp_session_input.clone();
        let roots_input = view.acp_roots_input.clone();
        let mut controls = v_flex().w_full().gap_3();
        if let (Some(session), Some(expected)) = (&snapshot.session_id, &snapshot.version) {
            for option in &snapshot.config_options {
                let id = option.id.to_string();
                let control = match &option.kind {
                    acp::SessionConfigKind::Boolean(boolean) => {
                        let state = state.clone();
                        let session = session.clone();
                        let expected = expected.clone();
                        Switch::new(SharedString::from(format!("acp-option:{id}")))
                            .label(option.name.clone())
                            .checked(boolean.current_value)
                            .disabled(busy)
                            .on_click(move |value, _, cx| {
                                let _ = state.owner.update(cx, |view, cx| {
                                    view.run_acp_control(
                                        Operation::SetOption {
                                            session_id: session.clone(),
                                            expected: expected.clone(),
                                            config_id: id.clone(),
                                            value: acp::SessionConfigOptionValue::boolean(*value),
                                        },
                                        cx,
                                    )
                                });
                            })
                            .into_any_element()
                    }
                    acp::SessionConfigKind::Select(select) => {
                        let groups = match &select.options {
                            acp::SessionConfigSelectOptions::Ungrouped(options) => {
                                vec![(None, options.clone())]
                            }
                            acp::SessionConfigSelectOptions::Grouped(groups) => groups
                                .iter()
                                .map(|group| (Some(group.name.clone()), group.options.clone()))
                                .collect(),
                            _ => vec![],
                        };
                        let label = groups
                            .iter()
                            .flat_map(|(_, options)| options)
                            .find(|value| value.value == select.current_value)
                            .map(|value| value.name.clone())
                            .unwrap_or_else(|| select.current_value.to_string());
                        let state = state.clone();
                        let session = session.clone();
                        let expected = expected.clone();
                        let current = select.current_value.clone();
                        let button = Button::new(SharedString::from(format!("acp-option:{id}")))
                            .label(label)
                            .disabled(busy)
                            .dropdown_menu(move |mut menu, _, _| {
                                for (group, options) in &groups {
                                    if let Some(group) = group {
                                        menu = menu.label(group.clone());
                                    }
                                    for value in options {
                                        let operation = Operation::SetOption {
                                            session_id: session.clone(),
                                            expected: expected.clone(),
                                            config_id: id.clone(),
                                            value: acp::SessionConfigOptionValue::value_id(
                                                value.value.clone(),
                                            ),
                                        };
                                        let owner = state.owner.clone();
                                        menu = menu.item(
                                            PopupMenuItem::new(value.name.clone())
                                                .checked(value.value == current)
                                                .on_click(move |_, _, cx| {
                                                    let _ = owner.update(cx, |view, cx| {
                                                        view.run_acp_control(operation.clone(), cx)
                                                    });
                                                }),
                                        );
                                    }
                                }
                                menu
                            });
                        field()
                            .label(option.name.clone())
                            .child(button)
                            .into_any_element()
                    }
                    _ => continue,
                };
                controls = controls.child(control).when_some(
                    option.description.clone(),
                    |column, description| {
                        column.child(
                            div()
                                .text_sm()
                                .text_color(cx.theme().muted_foreground)
                                .child(description),
                        )
                    },
                );
            }
            if !snapshot.has_config_options {
                if let Some(modes) = &snapshot.modes {
                    let mut buttons = h_flex().flex_wrap().gap_2();
                    for mode in &modes.available_modes {
                        buttons = buttons.child(
                            Self::acp_button(
                                &state,
                                format!("acp-mode:{}", mode.id),
                                mode.name.clone(),
                                Operation::SetMode {
                                    session_id: session.clone(),
                                    expected: expected.clone(),
                                    mode_id: mode.id.to_string(),
                                },
                                busy,
                            )
                            .selected(mode.id == modes.current_mode_id),
                        );
                    }
                    controls = controls.child(buttons);
                }
            }
            for command in &snapshot.commands {
                controls = controls.child(
                    div()
                        .text_sm()
                        .child(format!("/{} — {}", command.name, command.description)),
                );
            }
        }
        for method in &snapshot.auth_methods {
            let operation = match method.kind {
                AcpAuthMethodKind::Agent => Operation::Authenticate {
                    method_id: method.id.clone(),
                },
                AcpAuthMethodKind::Terminal => Operation::TerminalAuthStart {
                    expected_generation: snapshot.generation,
                    terminal_id: pioneer_client::cli_runtime::acp::new_auth_terminal_id(),
                    method_id: method.id.clone(),
                    columns: 90,
                    rows: 24,
                },
            };
            controls = controls.child(Self::acp_button(
                &state,
                format!("acp-auth:{}", method.id),
                method.name.clone(),
                operation,
                busy || (method.kind == AcpAuthMethodKind::Terminal
                    && view.acp_auth_owner.is_some()),
            ));
        }
        if snapshot.session_id.is_none() {
            controls = controls.child(Self::acp_button(
                &state,
                "acp-prepare".into(),
                t!("chat.acp.prepare").to_string(),
                Operation::Prepare {
                    additional_directories: {
                        let value = roots_input.read(cx).value().to_string();
                        if value.is_empty() {
                            snapshot.additional_directories.clone()
                        } else {
                            value
                                .lines()
                                .filter(|line| !line.is_empty())
                                .map(str::to_owned)
                                .collect()
                        }
                    },
                },
                busy,
            ));
        }
        if snapshot.session_id.is_none() {
            controls = controls.child(
                field()
                    .label(t!("chat.acp.additional_directories").to_string())
                    .child(Input::new(&roots_input)),
            );
        }
        if snapshot.capabilities.auth.logout.is_some() {
            controls = controls.child(Self::acp_button(
                &state,
                "acp-logout".into(),
                t!("chat.acp.logout").to_string(),
                Operation::Logout,
                busy,
            ));
        }
        if snapshot.capabilities.session_capabilities.list.is_some() {
            controls = controls.child(Self::acp_button(
                &state,
                "acp-list".into(),
                t!("chat.acp.list").to_string(),
                Operation::List {
                    cursor: None,
                    cwd: None,
                },
                busy,
            ));
        }
        controls = controls.child(Input::new(&session_input));
        let target = session_input.read(cx).value().to_string();
        let mut session_actions = h_flex().flex_wrap().gap_2();
        if !target.is_empty() {
            if snapshot.capabilities.load_session {
                session_actions = session_actions.child(Self::acp_button(
                    &state,
                    "acp-load".into(),
                    t!("chat.acp.load").to_string(),
                    Operation::Load {
                        session_id: target.clone(),
                    },
                    busy,
                ));
            }
            if snapshot.capabilities.session_capabilities.resume.is_some() {
                session_actions = session_actions.child(Self::acp_button(
                    &state,
                    "acp-resume".into(),
                    t!("chat.acp.resume").to_string(),
                    Operation::Resume {
                        session_id: target.clone(),
                    },
                    busy,
                ));
            }
            if snapshot.capabilities.session_capabilities.delete.is_some() {
                session_actions = session_actions.child(Self::acp_button(
                    &state,
                    "acp-delete".into(),
                    t!("chat.acp.delete").to_string(),
                    Operation::Delete {
                        session_id: target.clone(),
                    },
                    busy,
                ));
            }
        }
        if snapshot.capabilities.session_capabilities.close.is_some() {
            if let Some(session) = &snapshot.session_id {
                session_actions = session_actions.child(Self::acp_button(
                    &state,
                    "acp-close-session".into(),
                    t!("chat.acp.close_session").to_string(),
                    Operation::Close {
                        session_id: session.clone(),
                    },
                    busy,
                ));
            }
        }
        controls = controls.child(session_actions);
        if let Some(result) = &result {
            if let Some(sessions) = result.get("sessions").and_then(serde_json::Value::as_array) {
                for session in sessions {
                    if let Some(id) = session.get("sessionId").and_then(serde_json::Value::as_str) {
                        let id = id.to_owned();
                        let label = session
                            .get("title")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or(&id)
                            .to_owned();
                        let input = session_input.clone();
                        controls = controls.child(
                            Button::new(SharedString::from(format!("acp-session:{id}")))
                                .label(label)
                                .on_click(move |_, window, cx| {
                                    input.update(cx, |input, cx| {
                                        input.set_value(id.clone(), window, cx)
                                    });
                                }),
                        );
                    }
                }
                if let Some(cursor) = result.get("nextCursor").and_then(serde_json::Value::as_str) {
                    controls = controls.child(Self::acp_button(
                        &state,
                        "acp-next-sessions".into(),
                        t!("chat.acp.more").to_string(),
                        Operation::List {
                            cursor: Some(cursor.into()),
                            cwd: None,
                        },
                        busy,
                    ));
                }
            }
        }
        if let Some(result) = &auth_result {
            let completed = result
                .pointer("/snapshot/completed")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            if result
                .pointer("/snapshot/truncated")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
            {
                controls = controls.child(
                    div()
                        .text_sm()
                        .child(t!("chat.acp.output_truncated").to_string()),
                );
            }
            if completed {
                let code = result
                    .pointer("/snapshot/exit_code")
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "—".into());
                controls = controls.child(
                    div()
                        .text_sm()
                        .child(t!("chat.acp.auth_finished", code = code).to_string()),
                );
            }
            if let Some(error) = result
                .pointer("/snapshot/error")
                .and_then(serde_json::Value::as_str)
            {
                controls = controls.child(
                    div()
                        .text_sm()
                        .text_color(cx.theme().danger)
                        .child(error.to_owned()),
                );
            }
            if let Some(terminal) = result
                .get("terminal_id")
                .and_then(serde_json::Value::as_str)
            {
                let terminal = terminal.to_owned();
                let state_send = state.clone();
                let input_send = auth_input.clone();
                let terminal_send = terminal.clone();
                controls = controls.child(Input::new(&auth_input)).child(
                    h_flex()
                        .gap_2()
                        .child(
                            Button::new("acp-auth-send")
                                .label(t!("chat.acp.send_input").to_string())
                                .disabled(busy || completed)
                                .on_click(move |_, window, cx| {
                                    let data = format!("{}\r", input_send.read(cx).value());
                                    input_send
                                        .update(cx, |input, cx| input.set_value("", window, cx));
                                    let _ = state_send.owner.update(cx, |view, cx| {
                                        view.run_acp_control(
                                            Operation::TerminalAuthInput {
                                                terminal_id: terminal_send.clone(),
                                                data: data.clone(),
                                            },
                                            cx,
                                        )
                                    });
                                }),
                        )
                        .child(Self::acp_button(
                            &state,
                            "acp-auth-output".into(),
                            t!("chat.acp.refresh_output").to_string(),
                            Operation::TerminalAuthOutput {
                                terminal_id: terminal.clone(),
                            },
                            busy,
                        ))
                        .child(Self::acp_button(
                            &state,
                            "acp-auth-close".into(),
                            t!("chat.acp.close_reconnect").to_string(),
                            Operation::TerminalAuthClose {
                                expected_generation: snapshot.generation,
                                terminal_id: terminal,
                            },
                            busy,
                        )),
                );
            }
            if let Some(output) = result
                .pointer("/snapshot/output")
                .and_then(serde_json::Value::as_str)
            {
                controls = controls.child(
                    div()
                        .text_sm()
                        .font_family("monospace")
                        .whitespace_normal()
                        .child(output.to_owned()),
                );
            }
        }
        if let Some(notice) = &snapshot.notice {
            controls = controls.child(div().text_sm().child(notice.title.clone()));
        }
        if let Some(error) = error {
            controls = controls.child(div().text_sm().text_color(cx.theme().danger).child(error));
        }
        controls.into_any_element()
    }
}
