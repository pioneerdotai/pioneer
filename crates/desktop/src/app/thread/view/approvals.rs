use crate::{
    app::root::{PendingRequest, PioneerDesktop},
    components::buttonts::{default_outline_button, default_primary_button},
};
use gpui_kit::component::{
    Icon, IconName, WindowExt,
    button::*,
    dialog::DialogFooter,
    form::{Field, field, v_form},
    input::{Input, InputState, Textarea, TextareaState},
    scroll::ScrollableElement,
    theme::ActiveTheme,
    *,
};
use gpui_kit::{prelude::*, *};
use pioneer_client::cli_runtime::approvals::{
    PendingRequestActionKind, PendingRequestAvailableAction, PendingRequestDetailRow,
    PendingRequestDetailStyle, PendingRequestKind, PendingRequestPresentation,
    PendingRequestResolution, PendingRequestUserInputQuestion, pending_request_answered_resolution,
    present_pending_request,
};
use std::rc::Rc;

#[derive(Clone)]
enum PendingRequestAnswerInput {
    SingleLine(Entity<InputState>),
    MultiLine(Entity<TextareaState>),
}

impl PendingRequestAnswerInput {
    fn value(&self, cx: &App) -> String {
        match self {
            Self::SingleLine(input) => input.read(cx).value().to_string(),
            Self::MultiLine(input) => input.read(cx).value().to_string(),
        }
    }

    fn focus(&self, window: &mut Window, cx: &mut App) {
        match self {
            Self::SingleLine(input) => {
                input.update(cx, |state, cx| state.focus(window, cx));
            }
            Self::MultiLine(input) => {
                input.update(cx, |state, cx| state.focus(window, cx));
            }
        }
    }

    fn set_value(&self, value: String, window: &mut Window, cx: &mut App) {
        match self {
            Self::SingleLine(input) => {
                input.update(cx, |state, cx| state.set_value(value, window, cx));
            }
            Self::MultiLine(input) => {
                input.update(cx, |state, cx| state.set_value(value, window, cx));
            }
        }
    }

    fn render(&self) -> AnyElement {
        match self {
            Self::SingleLine(input) => Input::new(input).min_w_0().into_any_element(),
            Self::MultiLine(input) => Textarea::new(input).min_w_0().into_any_element(),
        }
    }
}

impl PioneerDesktop {
    pub(super) fn render_pending_request_card(
        &self,
        request: PendingRequest,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let icon = pending_request_icon(request.kind);
        let presentation = present_pending_request(&request);
        let title = presentation.title.clone();
        let origin_label = presentation.origin_label.clone();

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
                            .child(
                                div()
                                    .text_xs()
                                    .font_semibold()
                                    .text_color(cx.theme().warning)
                                    .child(origin_label),
                            )
                            .child(div().text_sm().font_semibold().child(title))
                            .when_some(presentation.message.clone(), |this, message| {
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
            .children(self.render_pending_request_details(&presentation, cx))
            .child(self.render_pending_request_actions(request, presentation.actions, cx))
            .into_any_element()
    }

    fn render_pending_request_details(
        &self,
        presentation: &PendingRequestPresentation,
        cx: &mut Context<Self>,
    ) -> Vec<AnyElement> {
        let mut rows = presentation
            .details
            .iter()
            .cloned()
            .map(|row| render_pending_request_detail_row(row, cx))
            .collect::<Vec<_>>();
        if !presentation.user_input_questions.is_empty() {
            rows.push(render_user_input_question_summary(
                &presentation.user_input_questions,
                cx,
            ));
        }
        rows
    }

    fn render_pending_request_actions(
        &self,
        request: PendingRequest,
        actions: Vec<PendingRequestAvailableAction>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        // A native permission opened by a child/subagent is intentionally
        // projected into every visible ancestor timeline. The capability to
        // answer therefore belongs to the currently displayed root scope;
        // the Gateway still validates the exact request/session/turn.
        if !self.can_respond_to_agent_requests_presentation(self.current_active_thread_id()) {
            return div().into_any_element();
        }
        let action_elements = actions
            .into_iter()
            .map(|action| render_pending_request_action(request.clone(), action, cx))
            .collect::<Vec<_>>();

        h_flex()
            .w_full()
            .justify_end()
            .gap_2()
            .children(action_elements)
            .into_any_element()
    }

    fn open_pending_request_user_input_dialog(
        &mut self,
        request: PendingRequest,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let questions = present_pending_request(&request).user_input_questions;
        if questions.is_empty() {
            return;
        }

        let inputs = questions
            .iter()
            .map(|question| {
                let input = if question.options.is_empty() && !question.is_secret {
                    PendingRequestAnswerInput::MultiLine(cx.new(|cx| {
                        TextareaState::new(window, cx)
                            .auto_grow(1, 4)
                            .placeholder("Answer")
                    }))
                } else {
                    let is_secret = question.is_secret;
                    PendingRequestAnswerInput::SingleLine(cx.new(|cx| {
                        InputState::new(window, cx)
                            .placeholder("Answer")
                            .masked(is_secret)
                    }))
                };
                (question.id.clone(), input)
            })
            .collect::<Vec<_>>();
        let inputs = Rc::new(inputs);
        let desktop_entity = cx.entity().clone();
        let submit_request = request.clone();

        let submit: Rc<dyn Fn(&mut App) -> bool> = Rc::new({
            let desktop_entity = desktop_entity.clone();
            let inputs = inputs.clone();
            let request = submit_request;
            move |cx| {
                let answers = inputs
                    .iter()
                    .map(|(id, input)| (id.clone(), input.value(cx)));
                let resolution = pending_request_answered_resolution(answers);

                let _ = desktop_entity.update(cx, |view, cx| {
                    view.respond_pending_request(request.clone(), resolution.clone(), cx);
                    cx.notify();
                });
                true
            }
        });

        window.open_dialog(cx, move |dialog, window, cx| {
            if let Some((_, first_input)) = inputs.first() {
                first_input.focus(window, cx);
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
                .footer(DialogFooter::new().children({
                    let submit = submit.clone();
                    let request = request.clone();
                    let desktop_entity = desktop_entity.clone();
                    vec![
                        default_outline_button("cli-runtime-user-input-cancel")
                            .label("Cancel turn")
                            .danger()
                            .on_click({
                                let request = request.clone();
                                let desktop_entity = desktop_entity.clone();
                                move |_, window, cx| {
                                    let _ = desktop_entity.update(cx, |view, cx| {
                                        view.respond_pending_request(
                                            request.clone(),
                                            PendingRequestResolution::Cancel,
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
                }))
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

fn pending_request_icon(kind: PendingRequestKind) -> IconName {
    match kind {
        PendingRequestKind::CommandApproval => IconName::SquareTerminal,
        PendingRequestKind::FileChangeApproval => IconName::File,
        PendingRequestKind::PermissionApproval => IconName::TriangleAlert,
        PendingRequestKind::UserInput => IconName::User,
        PendingRequestKind::Other => IconName::Info,
    }
}

fn render_pending_request_action(
    request: PendingRequest,
    action: PendingRequestAvailableAction,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    let request_id = request.request_id.clone();
    let element_id = pending_request_element_id(
        pending_request_action_element_prefix(action.kind),
        request_id.as_str(),
    );

    match action.kind {
        PendingRequestActionKind::CancelTurn => respond_pending_request_button(
            default_outline_button(element_id)
                .label("Cancel turn")
                .danger(),
            request,
            action.resolution,
            cx,
        ),
        PendingRequestActionKind::Deny => respond_pending_request_button(
            default_outline_button(element_id).label("Deny"),
            request,
            action.resolution,
            cx,
        ),
        PendingRequestActionKind::AllowForTurn => respond_pending_request_button(
            default_outline_button(element_id).label("Allow for turn"),
            request,
            action.resolution,
            cx,
        ),
        PendingRequestActionKind::AllowForSession => respond_pending_request_button(
            default_outline_button(element_id).label("Allow for session"),
            request,
            action.resolution,
            cx,
        ),
        PendingRequestActionKind::Allow => respond_pending_request_button(
            default_primary_button(element_id).label("Allow"),
            request,
            action.resolution,
            cx,
        ),
        PendingRequestActionKind::Answer => default_primary_button(element_id)
            .label("Answer")
            .on_click({
                let request = request.clone();
                cx.listener(move |view, _, window, cx| {
                    view.open_pending_request_user_input_dialog(request.clone(), window, cx);
                })
            })
            .into_any_element(),
    }
}

fn respond_pending_request_button(
    button: Button,
    request: PendingRequest,
    resolution: Option<PendingRequestResolution>,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    let Some(resolution) = resolution else {
        return button.into_any_element();
    };
    button
        .on_click({
            let request = request.clone();
            cx.listener(move |view, _, _, cx| {
                view.respond_pending_request(request.clone(), resolution.clone(), cx);
            })
        })
        .into_any_element()
}

fn pending_request_action_element_prefix(kind: PendingRequestActionKind) -> &'static str {
    match kind {
        PendingRequestActionKind::CancelTurn => "pending-request-action-cancel",
        PendingRequestActionKind::Deny => "pending-request-action-deny",
        PendingRequestActionKind::Allow => "pending-request-action-allow",
        PendingRequestActionKind::AllowForTurn => "pending-request-action-allow-turn",
        PendingRequestActionKind::AllowForSession => "pending-request-action-allow-session",
        PendingRequestActionKind::Answer => "pending-request-action-answer",
    }
}

fn render_user_input_question(
    question: PendingRequestUserInputQuestion,
    input: PendingRequestAnswerInput,
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
                                    input.set_value(label.clone(), window, cx);
                                }
                            })
                            .into_any_element()
                        })),
                )
            })
            .child(input.render())
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

fn pending_request_element_id(prefix: &str, value: &str) -> SharedString {
    format!("{prefix}-{value}").into()
}

fn render_pending_request_detail_row(
    row: PendingRequestDetailRow,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    match row.style {
        PendingRequestDetailStyle::Field => render_pending_request_field_detail_row(row, cx),
        PendingRequestDetailStyle::Diff => render_pending_request_diff_detail_row(row, cx),
    }
}

fn render_pending_request_field_detail_row(
    row: PendingRequestDetailRow,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    v_flex()
        .gap_1()
        .child(
            div()
                .text_xs()
                .font_semibold()
                .text_color(cx.theme().muted_foreground)
                .child(row.label),
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
                .when(row.monospace, |this| this.font_family("monospace"))
                .child(row.value),
        )
        .into_any_element()
}

fn render_pending_request_diff_detail_row(
    row: PendingRequestDetailRow,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    v_flex()
        .gap_1()
        .child(
            div()
                .text_xs()
                .font_semibold()
                .text_color(cx.theme().muted_foreground)
                .child(row.label),
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
                .child(row.value),
        )
        .into_any_element()
}

fn render_user_input_question_summary(
    questions: &[PendingRequestUserInputQuestion],
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    v_flex()
        .gap_1()
        .children(questions.iter().take(3).map(|question| {
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
                        .child(question.question.clone()),
                )
                .into_any_element()
        }))
        .into_any_element()
}

#[cfg(test)]
mod tests {
    use super::{pending_request_action_element_prefix, pending_request_icon};
    use gpui_kit::component::{IconName, IconNamed};
    use pioneer_client::cli_runtime::approvals::PendingRequestActionKind;
    use pioneer_client::cli_runtime::approvals::PendingRequestKind;

    #[test]
    fn provider_session_approval_has_a_distinct_desktop_action_identity() {
        assert_eq!(
            pending_request_action_element_prefix(PendingRequestActionKind::AllowForSession),
            "pending-request-action-allow-session"
        );
        assert_ne!(
            pending_request_action_element_prefix(PendingRequestActionKind::AllowForSession),
            pending_request_action_element_prefix(PendingRequestActionKind::AllowForTurn)
        );
    }

    #[test]
    fn permission_approval_has_a_distinct_desktop_icon() {
        assert_eq!(
            pending_request_icon(PendingRequestKind::PermissionApproval).path(),
            IconName::TriangleAlert.path()
        );
    }
}
