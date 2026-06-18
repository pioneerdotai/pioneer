use crate::{
    app::root::{CLIRuntimePendingRequestEntry, PioneerDesktop},
    components::buttonts::{default_outline_button, default_primary_button},
};
use gpui::{prelude::*, *};
use gpui_component::{
    Icon, IconName, WindowExt,
    button::*,
    form::{Field, field, v_form},
    input::{Input, InputState},
    scroll::ScrollableElement,
    theme::ActiveTheme,
    *,
};
use pioneer_protocol::{CLIRuntimeRequestKind, CLIRuntimeRequestResolution};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use std::rc::Rc;

impl PioneerDesktop {
    pub(super) fn render_cli_runtime_pending_requests_panel(
        &self,
        requests: Vec<CLIRuntimePendingRequestEntry>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if requests.is_empty() {
            return div().into_any_element();
        }

        h_flex()
            .w_full()
            .justify_center()
            .px_6()
            .pb_2()
            .child(
                v_flex().w_full().max_w(px(800.)).gap_2().children(
                    requests
                        .into_iter()
                        .map(|request| self.render_cli_runtime_pending_request_card(request, cx)),
                ),
            )
            .into_any_element()
    }

    fn render_cli_runtime_pending_request_card(
        &self,
        entry: CLIRuntimePendingRequestEntry,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let icon = match entry.request.kind {
            CLIRuntimeRequestKind::CommandApproval => IconName::SquareTerminal,
            CLIRuntimeRequestKind::FileChangeApproval => IconName::File,
            CLIRuntimeRequestKind::UserInput => IconName::User,
            CLIRuntimeRequestKind::Other => IconName::Info,
        };
        let title = entry
            .request
            .title
            .clone()
            .unwrap_or_else(|| cli_runtime_request_kind_label(entry.request.kind).to_owned());

        v_flex()
            .w_full()
            .gap_3()
            .rounded_lg()
            .border_1()
            .border_color(cx.theme().warning.opacity(0.35))
            .bg(cx.theme().warning.opacity(0.08))
            .px_3()
            .py_3()
            .child(
                h_flex()
                    .items_start()
                    .gap_2()
                    .child(
                        div()
                            .flex_none()
                            .size(px(24.))
                            .rounded_full()
                            .flex()
                            .items_center()
                            .justify_center()
                            .bg(cx.theme().warning.opacity(0.14))
                            .child(Icon::new(icon).size_4().text_color(cx.theme().warning)),
                    )
                    .child(
                        v_flex()
                            .min_w_0()
                            .flex_1()
                            .gap_1()
                            .child(div().text_sm().font_semibold().child(title))
                            .when_some(entry.request.message.clone(), |this, message| {
                                this.child(
                                    div()
                                        .text_xs()
                                        .line_height(relative(1.35))
                                        .text_color(cx.theme().muted_foreground)
                                        .child(message),
                                )
                            }),
                    ),
            )
            .children(self.render_cli_runtime_request_details(&entry, cx))
            .child(self.render_cli_runtime_request_actions(entry, cx))
            .into_any_element()
    }

    fn render_cli_runtime_request_details(
        &self,
        entry: &CLIRuntimePendingRequestEntry,
        cx: &mut Context<Self>,
    ) -> Vec<AnyElement> {
        match entry.request.kind {
            CLIRuntimeRequestKind::CommandApproval => {
                self.render_cli_runtime_command_details(entry, cx)
            }
            CLIRuntimeRequestKind::FileChangeApproval => {
                self.render_cli_runtime_file_change_details(entry, cx)
            }
            CLIRuntimeRequestKind::UserInput => {
                self.render_cli_runtime_user_input_details(entry, cx)
            }
            CLIRuntimeRequestKind::Other => Vec::new(),
        }
    }

    fn render_cli_runtime_command_details(
        &self,
        entry: &CLIRuntimePendingRequestEntry,
        cx: &mut Context<Self>,
    ) -> Vec<AnyElement> {
        let payload = entry.request.payload.as_ref();
        let command = payload
            .and_then(|payload| string_field(payload, &["command"]))
            .or_else(|| payload.and_then(command_from_argv));
        let cwd = payload.and_then(|payload| string_field(payload, &["cwd"]));
        let reason = payload.and_then(|payload| string_field(payload, &["reason"]));

        let mut rows = Vec::new();
        if let Some(command) = command {
            rows.push(cli_runtime_detail_row("Command", command, true, cx));
        }
        if let Some(cwd) = cwd {
            rows.push(cli_runtime_detail_row("Directory", cwd, true, cx));
        }
        if let Some(reason) = reason {
            rows.push(cli_runtime_detail_row("Reason", reason, false, cx));
        }
        rows
    }

    fn render_cli_runtime_file_change_details(
        &self,
        entry: &CLIRuntimePendingRequestEntry,
        cx: &mut Context<Self>,
    ) -> Vec<AnyElement> {
        let payload = entry.request.payload.as_ref();
        let mut rows = Vec::new();

        if let Some(grant_root) = payload.and_then(|payload| string_field(payload, &["grantRoot"]))
        {
            rows.push(cli_runtime_detail_row("Root", grant_root, true, cx));
        }

        if let Some(files) = payload.and_then(|payload| string_array_field(payload, "changedFiles"))
            && !files.is_empty()
        {
            rows.push(cli_runtime_detail_row("Files", files.join("\n"), true, cx));
        }

        if let Some(reason) = payload.and_then(|payload| string_field(payload, &["reason"])) {
            rows.push(cli_runtime_detail_row("Reason", reason, false, cx));
        }

        if let Some(diff_preview) = payload.and_then(diff_preview_text) {
            rows.push(
                v_flex()
                    .gap_1()
                    .child(
                        div()
                            .text_xs()
                            .font_semibold()
                            .text_color(cx.theme().muted_foreground)
                            .child("Diff"),
                    )
                    .child(
                        div()
                            .max_h(px(180.))
                            .rounded_md()
                            .border_1()
                            .border_color(cx.theme().border)
                            .bg(cx.theme().muted.opacity(0.35))
                            .px_2()
                            .py_2()
                            .font_family("monospace")
                            .text_xs()
                            .line_height(relative(1.35))
                            .whitespace_normal()
                            .overflow_y_scrollbar()
                            .child(diff_preview),
                    )
                    .into_any_element(),
            );
        }

        rows
    }

    fn render_cli_runtime_user_input_details(
        &self,
        entry: &CLIRuntimePendingRequestEntry,
        cx: &mut Context<Self>,
    ) -> Vec<AnyElement> {
        let questions = cli_runtime_user_input_questions(entry);
        if questions.is_empty() {
            return Vec::new();
        }

        vec![
            v_flex()
                .gap_1()
                .children(questions.into_iter().take(3).map(|question| {
                    let header = question
                        .header
                        .clone()
                        .unwrap_or_else(|| question.id.clone());
                    v_flex()
                        .gap_0p5()
                        .child(
                            div()
                                .text_xs()
                                .font_semibold()
                                .text_color(cx.theme().muted_foreground)
                                .child(header),
                        )
                        .child(
                            div()
                                .text_xs()
                                .line_height(relative(1.35))
                                .child(question.question),
                        )
                        .into_any_element()
                }))
                .into_any_element(),
        ]
    }

    fn render_cli_runtime_request_actions(
        &self,
        entry: CLIRuntimePendingRequestEntry,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let request_id = entry.request_id.clone();

        match entry.request.kind {
            CLIRuntimeRequestKind::UserInput => h_flex()
                .w_full()
                .justify_end()
                .gap_2()
                .child(
                    default_outline_button(runtime_element_id(
                        "cli-runtime-input-cancel",
                        request_id.as_str(),
                    ))
                    .label("Cancel turn")
                    .danger()
                    .on_click({
                        let request_id = request_id.clone();
                        cx.listener(move |view, _, _, cx| {
                            view.respond_cli_runtime_pending_request(
                                request_id.clone(),
                                CLIRuntimeRequestResolution::Cancelled,
                                cx,
                            );
                        })
                    }),
                )
                .child(
                    default_primary_button(runtime_element_id(
                        "cli-runtime-input-answer",
                        request_id.as_str(),
                    ))
                    .label("Answer")
                    .on_click({
                        cx.listener(move |view, _, window, cx| {
                            view.open_cli_runtime_user_input_dialog(entry.clone(), window, cx);
                        })
                    }),
                )
                .into_any_element(),
            CLIRuntimeRequestKind::CommandApproval | CLIRuntimeRequestKind::FileChangeApproval => {
                approval_actions_row(request_id, cx)
            }
            CLIRuntimeRequestKind::Other => h_flex()
                .w_full()
                .justify_end()
                .gap_2()
                .child(
                    default_outline_button(runtime_element_id(
                        "cli-runtime-other-cancel",
                        request_id.as_str(),
                    ))
                    .label("Cancel turn")
                    .danger()
                    .on_click({
                        let request_id = request_id.clone();
                        cx.listener(move |view, _, _, cx| {
                            view.respond_cli_runtime_pending_request(
                                request_id.clone(),
                                CLIRuntimeRequestResolution::Cancelled,
                                cx,
                            );
                        })
                    }),
                )
                .child(
                    default_primary_button(runtime_element_id(
                        "cli-runtime-other-allow",
                        request_id.as_str(),
                    ))
                    .label("Allow")
                    .on_click({
                        cx.listener(move |view, _, _, cx| {
                            view.respond_cli_runtime_pending_request(
                                entry.request_id.clone(),
                                CLIRuntimeRequestResolution::Approved,
                                cx,
                            );
                        })
                    }),
                )
                .into_any_element(),
        }
    }

    fn open_cli_runtime_user_input_dialog(
        &mut self,
        entry: CLIRuntimePendingRequestEntry,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let questions = cli_runtime_user_input_questions(&entry);
        if questions.is_empty() {
            return;
        }

        let inputs = questions
            .iter()
            .map(|question| {
                let input = cx.new(|cx| {
                    let mut state = InputState::new(window, cx).placeholder("Answer");
                    if question.options.is_empty() {
                        state = state.multi_line(true).auto_grow(1, 4);
                    }
                    state
                });
                (question.id.clone(), input)
            })
            .collect::<Vec<_>>();
        let inputs = Rc::new(inputs);
        let desktop_entity = cx.entity().clone();
        let request_id = entry.request_id.clone();

        let submit: Rc<dyn Fn(&mut App) -> bool> = Rc::new({
            let desktop_entity = desktop_entity.clone();
            let inputs = inputs.clone();
            let request_id = request_id.clone();
            move |cx| {
                let mut answers = JsonMap::new();
                for (id, input) in inputs.iter() {
                    answers.insert(
                        id.clone(),
                        JsonValue::String(input.read(cx).value().to_string()),
                    );
                }

                let _ = desktop_entity.update(cx, |view, cx| {
                    view.respond_cli_runtime_pending_request(
                        request_id.clone(),
                        CLIRuntimeRequestResolution::Answered {
                            response: Some(json!({ "answers": answers })),
                        },
                        cx,
                    );
                    cx.notify();
                });
                true
            }
        });

        window.open_dialog(cx, move |dialog, window, cx| {
            if let Some((_, first_input)) = inputs.first() {
                first_input.update(cx, |state, cx| state.focus(window, cx));
            }

            dialog
                .w(px(520.))
                .gap_1()
                .rounded_2xl()
                .close_button(true)
                .overlay_closable(false)
                .keyboard(true)
                .title(div().text_base().font_semibold().child("Input requested"))
                .on_ok({
                    let submit = submit.clone();
                    move |_, _, cx| submit(cx)
                })
                .footer({
                    let submit = submit.clone();
                    let request_id = request_id.clone();
                    let desktop_entity = desktop_entity.clone();
                    move |_, _, _, _| {
                        vec![
                            default_outline_button("cli-runtime-user-input-cancel")
                                .label("Cancel turn")
                                .danger()
                                .on_click({
                                    let request_id = request_id.clone();
                                    let desktop_entity = desktop_entity.clone();
                                    move |_, window, cx| {
                                        let _ = desktop_entity.update(cx, |view, cx| {
                                            view.respond_cli_runtime_pending_request(
                                                request_id.clone(),
                                                CLIRuntimeRequestResolution::Cancelled,
                                                cx,
                                            );
                                            cx.notify();
                                        });
                                        window.close_dialog(cx);
                                    }
                                })
                                .into_any_element(),
                            default_primary_button("cli-runtime-user-input-submit")
                                .label("Submit")
                                .on_click({
                                    let submit = submit.clone();
                                    move |_, window, cx| {
                                        if submit(cx) {
                                            window.close_dialog(cx);
                                        }
                                    }
                                })
                                .into_any_element(),
                        ]
                    }
                })
                .child(
                    v_form()
                        .gap_4()
                        .children(questions.iter().filter_map(|question| {
                            let input = inputs
                                .iter()
                                .find(|(id, _)| id == &question.id)
                                .map(|(_, input)| input.clone())?;
                            Some(render_user_input_question(question.clone(), input, cx))
                        })),
                )
        });
    }
}

fn approval_actions_row(request_id: String, cx: &mut Context<PioneerDesktop>) -> AnyElement {
    h_flex()
        .w_full()
        .justify_end()
        .gap_2()
        .child(
            default_outline_button(runtime_element_id(
                "cli-runtime-approval-cancel",
                request_id.as_str(),
            ))
            .label("Cancel turn")
            .danger()
            .on_click({
                let request_id = request_id.clone();
                cx.listener(move |view, _, _, cx| {
                    view.respond_cli_runtime_pending_request(
                        request_id.clone(),
                        CLIRuntimeRequestResolution::Cancelled,
                        cx,
                    );
                })
            }),
        )
        .child(
            default_outline_button(runtime_element_id(
                "cli-runtime-approval-deny",
                request_id.as_str(),
            ))
            .label("Deny")
            .on_click({
                let request_id = request_id.clone();
                cx.listener(move |view, _, _, cx| {
                    view.respond_cli_runtime_pending_request(
                        request_id.clone(),
                        CLIRuntimeRequestResolution::Denied { reason: None },
                        cx,
                    );
                })
            }),
        )
        .child(
            default_outline_button(runtime_element_id(
                "cli-runtime-approval-session",
                request_id.as_str(),
            ))
            .label("Allow for session")
            .on_click({
                let request_id = request_id.clone();
                cx.listener(move |view, _, _, cx| {
                    view.respond_cli_runtime_pending_request(
                        request_id.clone(),
                        CLIRuntimeRequestResolution::Answered {
                            response: Some(json!({ "decision": "allow_for_session" })),
                        },
                        cx,
                    );
                })
            }),
        )
        .child(
            default_primary_button(runtime_element_id(
                "cli-runtime-approval-allow",
                request_id.as_str(),
            ))
            .label("Allow")
            .on_click({
                cx.listener(move |view, _, _, cx| {
                    view.respond_cli_runtime_pending_request(
                        request_id.clone(),
                        CLIRuntimeRequestResolution::Approved,
                        cx,
                    );
                })
            }),
        )
        .into_any_element()
}

fn render_user_input_question(
    question: CliRuntimeUserInputQuestion,
    input: Entity<InputState>,
    cx: &mut App,
) -> Field {
    let label = question
        .header
        .clone()
        .unwrap_or_else(|| question.question.clone());

    field().label(label).child(
        v_flex()
            .gap_2()
            .child(
                div()
                    .text_sm()
                    .line_height(relative(1.35))
                    .child(question.question),
            )
            .when(!question.options.is_empty(), |this| {
                this.child(
                    h_flex()
                        .gap_2()
                        .flex_wrap()
                        .children(question.options.iter().map(|option| {
                            default_outline_button(runtime_element_id(
                                "cli-runtime-user-input-option",
                                format!("{}-{}", question.id, option.label).as_str(),
                            ))
                            .label(option.label.clone())
                            .on_click({
                                let input = input.clone();
                                let label = option.label.clone();
                                move |_, window, cx| {
                                    input.update(cx, |state, cx| {
                                        state.set_value(label.clone(), window, cx)
                                    });
                                }
                            })
                            .into_any_element()
                        })),
                )
            })
            .child(Input::new(&input).min_w_0())
            .when(question.is_secret, |this| {
                this.child(
                    div()
                        .text_xs()
                        .line_height(relative(1.3))
                        .text_color(cx.theme().muted_foreground)
                        .child("This answer will be sent only to the active CLI runtime."),
                )
            }),
    )
}

fn runtime_element_id(prefix: &str, value: &str) -> SharedString {
    format!("{prefix}-{value}").into()
}

fn cli_runtime_detail_row(
    label: &'static str,
    value: String,
    monospace: bool,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    v_flex()
        .gap_1()
        .child(
            div()
                .text_xs()
                .font_semibold()
                .text_color(cx.theme().muted_foreground)
                .child(label),
        )
        .child(
            div()
                .rounded_md()
                .border_1()
                .border_color(cx.theme().border)
                .bg(cx.theme().muted.opacity(0.35))
                .px_2()
                .py_1p5()
                .text_xs()
                .line_height(relative(1.35))
                .whitespace_normal()
                .when(monospace, |this| this.font_family("monospace"))
                .child(value),
        )
        .into_any_element()
}

fn cli_runtime_request_kind_label(kind: CLIRuntimeRequestKind) -> &'static str {
    match kind {
        CLIRuntimeRequestKind::CommandApproval => "Command approval",
        CLIRuntimeRequestKind::FileChangeApproval => "File change approval",
        CLIRuntimeRequestKind::UserInput => "Input requested",
        CLIRuntimeRequestKind::Other => "CLI runtime request",
    }
}

fn string_field(payload: &JsonValue, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| payload.get(*key).and_then(JsonValue::as_str))
        .map(str::to_owned)
        .filter(|value| !value.trim().is_empty())
}

fn command_from_argv(payload: &JsonValue) -> Option<String> {
    payload
        .get("argv")
        .and_then(JsonValue::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(JsonValue::as_str)
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.trim().is_empty())
}

fn string_array_field(payload: &JsonValue, key: &str) -> Option<Vec<String>> {
    payload.get(key).and_then(JsonValue::as_array).map(|items| {
        items
            .iter()
            .filter_map(JsonValue::as_str)
            .map(str::to_owned)
            .filter(|value| !value.trim().is_empty())
            .collect::<Vec<_>>()
    })
}

fn diff_preview_text(payload: &JsonValue) -> Option<String> {
    payload
        .get("diffPreview")
        .and_then(|preview| preview.get("text"))
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
        .filter(|value| !value.trim().is_empty())
}

#[derive(Clone, Debug)]
struct CliRuntimeUserInputQuestion {
    id: String,
    header: Option<String>,
    question: String,
    options: Vec<CliRuntimeUserInputOption>,
    is_secret: bool,
}

#[derive(Clone, Debug)]
struct CliRuntimeUserInputOption {
    label: String,
}

fn cli_runtime_user_input_questions(
    entry: &CLIRuntimePendingRequestEntry,
) -> Vec<CliRuntimeUserInputQuestion> {
    let Some(payload) = entry.request.payload.as_ref() else {
        return Vec::new();
    };
    let Some(questions) = payload.get("questions").and_then(JsonValue::as_array) else {
        return Vec::new();
    };

    questions
        .iter()
        .enumerate()
        .map(|(ix, value)| {
            let id = value
                .get("id")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
                .filter(|id| !id.trim().is_empty())
                .unwrap_or_else(|| format!("question_{}", ix + 1));
            let header = value
                .get("header")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
                .filter(|header| !header.trim().is_empty());
            let question = value
                .get("question")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
                .filter(|question| !question.trim().is_empty())
                .or_else(|| header.clone())
                .unwrap_or_else(|| id.clone());
            let options = value
                .get("options")
                .and_then(JsonValue::as_array)
                .into_iter()
                .flatten()
                .filter_map(|option| {
                    option
                        .get("label")
                        .and_then(JsonValue::as_str)
                        .map(str::to_owned)
                        .filter(|label| !label.trim().is_empty())
                        .map(|label| CliRuntimeUserInputOption { label })
                })
                .collect();
            let is_secret = value
                .get("isSecret")
                .and_then(JsonValue::as_bool)
                .unwrap_or(false);

            CliRuntimeUserInputQuestion {
                id,
                header,
                question,
                options,
                is_secret,
            }
        })
        .collect()
}
