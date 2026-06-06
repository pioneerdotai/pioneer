use super::{format_elapsed, format_elapsed_ms, now_unix_ms};
use crate::{
    app::{
        conversation::{ItemView, TimelineEntry, TimelineEntryStatus, tool_display_text},
        root::PioneerDesktop,
    },
    assets::PioneerIconName,
};
use gpui::{prelude::*, *};
use gpui_component::{
    Disableable, WindowExt,
    button::{Button, ButtonVariants},
    collapsible::Collapsible,
    form::{field, v_form},
    h_flex,
    input::{Input, InputState},
    spinner::Spinner,
    v_flex, *,
};
use pioneer_client::{
    tasks::review as task_review,
    timeline::labels::{
        McpTimelineMetadata, TaskWaitReviewDisplay, TaskWaitReviewDisplayItem,
        final_dynamic_tool_status, mcp_timeline_metadata, pretty_json, task_review_button_id,
        task_wait_review_display,
    },
};
use pioneer_protocol::TurnItem;
use std::cell::RefCell;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

impl PioneerDesktop {
    pub(super) fn render_item_dynamic_tool_call(
        &self,
        entry: &TimelineEntry,
        item_view: &ItemView,
        item: &TurnItem,
        is_first_row: bool,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let (tool_name, arguments, display_text, success, mcp_metadata, task_wait_review) =
            match item {
                TurnItem::DynamicToolCall {
                    tool_name,
                    arguments,
                    display,
                    success,
                    ..
                } => (
                    tool_name.clone(),
                    pretty_json(arguments),
                    tool_display_text(display),
                    *success,
                    mcp_timeline_metadata(display),
                    task_wait_review_display(tool_name, display),
                ),
                _ => (
                    "tool".to_owned(),
                    None,
                    Some(Self::timeline_entry_text(item_view).to_owned()),
                    None,
                    None,
                    None,
                ),
            };

        let mcp_tool_label = mcp_metadata.as_ref().map(McpTimelineMetadata::label);
        let tool_label_source = mcp_tool_label.as_deref().unwrap_or(tool_name.as_str());
        let tool_label = Self::truncate_for_card(tool_label_source, 180);
        let is_running = item_view.status == TimelineEntryStatus::Running;
        let tool_row = || {
            h_flex()
                .flex_1()
                .min_w_0()
                .items_center()
                .gap_2()
                .when(is_running, |this| {
                    this.child(Spinner::new().icon(IconName::Loader))
                })
                .when(!is_running, |this| {
                    this.child(Icon::new(PioneerIconName::Terminal).size_4().opacity(0.8))
                })
                .child(
                    div()
                        .flex_1()
                        .min_w_0()
                        .overflow_hidden()
                        .text_ellipsis()
                        .text_sm()
                        .line_height(relative(1.45))
                        .child(tool_label.clone()),
                )
                .into_any_element()
        };

        let elapsed_label = format_elapsed(item_view);
        let running_elapsed_label = item_view
            .started_at_unix_ms
            .map(|started| format_elapsed_ms(now_unix_ms().saturating_sub(started) as u64));

        let open = self
            .thread_timeline_item_expanded
            .borrow()
            .contains(entry.id.as_str());

        let entry_id = entry.id.clone();
        let mut toggle_id_hasher = std::collections::hash_map::DefaultHasher::new();
        entry.id.hash(&mut toggle_id_hasher);
        let toggle_id = toggle_id_hasher.finish();

        let (final_status, is_successful) = final_dynamic_tool_status(item_view.status, success);

        let content = if is_running {
            v_flex()
                .w_full()
                .gap_3()
                .child(tool_row())
                .child(
                    h_flex()
                        .w_full()
                        .items_center()
                        .justify_between()
                        .text_sm()
                        .font_semibold()
                        .child(
                            h_flex()
                                .items_center()
                                .gap_2()
                                .child(Spinner::new().icon(IconName::Loader))
                                .child(t!("timeline.tool.running").to_string()),
                        )
                        .child(
                            h_flex()
                                .items_center()
                                .gap_2()
                                .when_some(running_elapsed_label, |this, elapsed| {
                                    this.child(elapsed)
                                }),
                        ),
                )
                .into_any_element()
        } else {
            let details = self.dynamic_tool_details(
                arguments.as_deref(),
                display_text.as_deref(),
                mcp_metadata.as_ref(),
                task_wait_review.as_ref(),
                cx,
            );

            Collapsible::new()
                .gap_2()
                .open(open)
                .child(
                    div()
                        .id(("dynamic-tool-toggle", toggle_id))
                        .w_full()
                        .flex()
                        .items_center()
                        .opacity(0.7)
                        .hover(|this| this.opacity(0.9))
                        .child(
                            h_flex()
                                .w_full()
                                .items_center()
                                .justify_between()
                                .gap_3()
                                .child(tool_row())
                                .child(
                                    h_flex()
                                        .flex_none()
                                        .max_w(px(280.0))
                                        .items_center()
                                        .gap_2()
                                        .text_sm()
                                        .child(
                                            Icon::new(if is_successful {
                                                IconName::Check
                                            } else {
                                                IconName::TriangleAlert
                                            })
                                            .size_3p5(),
                                        )
                                        .child(
                                            div()
                                                .min_w_0()
                                                .overflow_hidden()
                                                .text_ellipsis()
                                                .child(final_status),
                                        )
                                        .when_some(elapsed_label, |this, elapsed| {
                                            this.child(elapsed)
                                        })
                                        .child(
                                            Icon::new(if open {
                                                IconName::ChevronUp
                                            } else {
                                                IconName::ChevronDown
                                            })
                                            .size_4(),
                                        ),
                                ),
                        )
                        .on_click({
                            let entry_id = entry_id.clone();
                            cx.listener(move |this, _, _, cx| {
                                this.toggle_timeline_item_expanded(entry_id.as_str(), cx);
                            })
                        }),
                )
                .content(details)
                .into_any_element()
        };

        self.render_item_row(is_first_row, is_last_row, content_width, content)
    }

    fn dynamic_tool_details(
        &self,
        arguments: Option<&str>,
        display_text: Option<&str>,
        mcp_metadata: Option<&McpTimelineMetadata>,
        task_wait_review: Option<&TaskWaitReviewDisplay>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let mut details = v_flex().w_full().gap_2().pt_1();
        let mut has_details = false;
        let mut open_mcp_server_id = None;

        if let Some(mcp_metadata) = mcp_metadata {
            has_details = true;
            details = details.child(self.timeline_detail_block(
                "MCP".to_owned(),
                mcp_metadata.details(),
                false,
                cx,
            ));
            open_mcp_server_id = mcp_metadata.server_id.clone().or_else(|| {
                self.mcp_servers
                    .iter()
                    .find(|server| server.name == mcp_metadata.server_name)
                    .map(|server| server.id.clone())
            });
        }

        if let Some(task_wait_review) = task_wait_review {
            has_details = true;
            details = details.child(self.timeline_detail_block(
                "Review required".to_owned(),
                Self::truncate_for_card(task_wait_review.details().as_str(), 4_000),
                false,
                cx,
            ));
            if let Some(controls) = self.render_task_wait_review_controls(task_wait_review, cx) {
                details = details.child(controls);
            }
        }

        if let Some(arguments) = arguments.filter(|value| !value.trim().is_empty()) {
            has_details = true;
            details = details.child(self.timeline_detail_block(
                t!("timeline.tool.arguments").to_string(),
                Self::truncate_for_card(arguments, 2_000),
                true,
                cx,
            ));
        }

        if let Some(display_text) = display_text.filter(|value| !value.trim().is_empty()) {
            has_details = true;
            details = details.child(self.timeline_detail_block(
                t!("timeline.tool.result").to_string(),
                Self::truncate_for_card(display_text, 4_000),
                false,
                cx,
            ));
        }

        if let Some(server_id) = open_mcp_server_id {
            details = details.child(
                h_flex().w_full().child(
                    Button::new("dynamic-tool-open-mcp-server")
                        .small()
                        .ghost()
                        .icon(PioneerIconName::Mcp)
                        .tooltip(t!("timeline.tool.open_mcp_server").to_string())
                        .on_click(cx.listener(move |view, _, _, cx| {
                            view.open_mcp_server_details_from_timeline(server_id.clone(), cx);
                            cx.notify();
                        })),
                ),
            );
        }

        if !has_details {
            details = details.child(
                div()
                    .text_sm()
                    .opacity(0.75)
                    .child(t!("timeline.common.no_details").to_string()),
            );
        }

        details.into_any_element()
    }

    fn render_task_wait_review_controls(
        &self,
        review: &TaskWaitReviewDisplay,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        let actionable_items = review
            .items
            .iter()
            .filter(|item| item.user_controls_allowed())
            .cloned()
            .collect::<Vec<_>>();
        if actionable_items.is_empty() {
            return None;
        }

        let mut controls = v_flex().w_full().gap_2();
        for item in actionable_items {
            let candidate_id = item.candidate_id.clone();
            let accept_enabled = task_review::task_review_action_enabled(
                &item,
                task_review::TaskReviewAction::Accept,
                &self.task_review_actions,
            );
            let revise_enabled = task_review::task_review_action_enabled(
                &item,
                task_review::TaskReviewAction::Revise,
                &self.task_review_actions,
            );
            let cancel_enabled = task_review::task_review_action_enabled(
                &item,
                task_review::TaskReviewAction::Cancel,
                &self.task_review_actions,
            );
            let error = self
                .task_review_actions
                .error(candidate_id.as_str())
                .map(str::to_owned);

            let accept_item = item.clone();
            let revise_item = item.clone();
            let cancel_item = item.clone();
            let candidate_label =
                format!("Candidate {}", Self::truncate_for_card(&candidate_id, 96));

            controls =
                controls.child(
                    div()
                        .w_full()
                        .overflow_hidden()
                        .rounded_lg()
                        .bg(cx.theme().muted)
                        .p_3()
                        .child(
                            v_flex()
                                .w_full()
                                .gap_2()
                                .child(div().text_xs().opacity(0.6).child(candidate_label))
                                .child(
                                    h_flex()
                                        .w_full()
                                        .flex_wrap()
                                        .gap_2()
                                        .child(
                                            Button::new(task_review_button_id(
                                                &candidate_id,
                                                "task-review-accept",
                                            ))
                                            .small()
                                            .primary()
                                            .label("Accept result")
                                            .disabled(!accept_enabled)
                                            .on_click(cx.listener(move |view, _, _, cx| {
                                                view.perform_task_review_accept(
                                                    accept_item.clone(),
                                                    cx,
                                                );
                                            })),
                                        )
                                        .child(
                                            Button::new(task_review_button_id(
                                                &candidate_id,
                                                "task-review-revise",
                                            ))
                                            .small()
                                            .outline()
                                            .label("Request revision")
                                            .disabled(!revise_enabled)
                                            .on_click(cx.listener(move |view, _, window, cx| {
                                                view.open_task_review_revise_dialog(
                                                    revise_item.clone(),
                                                    window,
                                                    cx,
                                                );
                                            })),
                                        )
                                        .child(
                                            Button::new(task_review_button_id(
                                                &candidate_id,
                                                "task-review-cancel",
                                            ))
                                            .small()
                                            .danger()
                                            .label("Cancel task")
                                            .disabled(!cancel_enabled)
                                            .on_click(cx.listener(move |view, _, _, cx| {
                                                view.perform_task_review_cancel(
                                                    cancel_item.clone(),
                                                    cx,
                                                );
                                            })),
                                        ),
                                )
                                .when_some(error, |this, error| {
                                    this.child(
                                        div()
                                            .text_xs()
                                            .line_height(relative(1.35))
                                            .text_color(cx.theme().danger)
                                            .whitespace_normal()
                                            .child(error),
                                    )
                                }),
                        ),
                );
        }

        Some(controls.into_any_element())
    }

    fn perform_task_review_accept(
        &mut self,
        item: TaskWaitReviewDisplayItem,
        cx: &mut Context<Self>,
    ) {
        let request = match task_review::plan_task_review_accept(
            &item,
            Some("Accepted in desktop".to_owned()),
            &mut self.task_review_actions,
        ) {
            Ok(Some(request)) => request,
            Ok(None) => return,
            Err(error) => {
                self.apply_task_review_plan_error(item.candidate_id.as_str(), error);
                cx.notify();
                return;
            }
        };
        let task_review::TaskReviewActionRequest {
            action_key,
            candidate_id,
            params,
        } = request;

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.task_accept(params) })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    let error = result.err().map(|error| format!("{error:#}"));
                    view.task_review_actions.finish_action(
                        action_key.as_str(),
                        candidate_id.as_str(),
                        error,
                    );
                    cx.notify();
                });
            }
        })
        .detach();

        cx.notify();
    }

    fn apply_task_review_plan_error(
        &mut self,
        candidate_id: &str,
        error: task_review::TaskReviewPlanError,
    ) {
        self.task_review_actions
            .set_error(candidate_id, Self::task_review_plan_error_message(error));
    }

    fn task_review_plan_error_message(error: task_review::TaskReviewPlanError) -> String {
        match error {
            task_review::TaskReviewPlanError::BlankFeedback => "Feedback is required".to_owned(),
            task_review::TaskReviewPlanError::MissingRunId
            | task_review::TaskReviewPlanError::MissingTaskId
            | task_review::TaskReviewPlanError::MissingCandidateId => {
                "Task review target is incomplete".to_owned()
            }
            task_review::TaskReviewPlanError::UserControlsNotAllowed
            | task_review::TaskReviewPlanError::ActionNotAllowed { .. } => {
                "Task review action is unavailable".to_owned()
            }
        }
    }

    fn open_task_review_revise_dialog(
        &mut self,
        item: TaskWaitReviewDisplayItem,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if item.run_id.is_none() {
            return;
        }

        let feedback_state = cx.new(|cx| {
            InputState::new(window, cx)
                .multi_line(true)
                .auto_grow(3, 8)
                .placeholder("Revision feedback")
        });
        let field_error: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));
        let desktop_entity = cx.entity().clone();

        let submit_revision: Rc<dyn Fn(&mut App) -> bool> = Rc::new({
            let feedback_state = feedback_state.clone();
            let field_error = field_error.clone();
            let item = item.clone();
            move |cx| {
                let feedback = match task_review::validate_revision_feedback(
                    feedback_state.read(cx).value().as_str(),
                ) {
                    Ok(feedback) => feedback,
                    Err(error) => {
                        *field_error.borrow_mut() =
                            Some(Self::task_review_plan_error_message(error));
                        return false;
                    }
                };
                *field_error.borrow_mut() = None;
                desktop_entity.update(cx, |view, cx| {
                    view.perform_task_review_revise(item.clone(), feedback, cx);
                });
                true
            }
        });

        window.open_dialog(cx, move |dialog, window, cx| {
            feedback_state.update(cx, |state, cx| state.focus(window, cx));
            let error = field_error.borrow().clone();
            let can_submit =
                task_review::validate_revision_feedback(feedback_state.read(cx).value().as_str())
                    .is_ok();
            dialog
                .w(px(420.))
                .gap_1()
                .rounded_2xl()
                .close_button(true)
                .overlay_closable(true)
                .keyboard(true)
                .title(div().text_base().font_semibold().child("Request revision"))
                .on_ok({
                    let submit_revision = submit_revision.clone();
                    move |_, _, cx| submit_revision(cx)
                })
                .footer({
                    let submit_revision = submit_revision.clone();
                    move |_, _, _, _| {
                        vec![
                            Button::new("task-review-revise-cancel")
                                .small()
                                .outline()
                                .label("Cancel")
                                .on_click(|_, window, cx| {
                                    window.close_dialog(cx);
                                })
                                .into_any_element(),
                            Button::new("task-review-revise-submit")
                                .small()
                                .primary()
                                .label("Request revision")
                                .disabled(!can_submit)
                                .on_click({
                                    let submit_revision = submit_revision.clone();
                                    move |_, window, cx| {
                                        if submit_revision(cx) {
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
                        .child(
                            field()
                                .label("Feedback")
                                .child(Input::new(&feedback_state).min_w_0()),
                        )
                        .when_some(error, |this, error| {
                            this.child(
                                field().label_indent(false).child(
                                    div()
                                        .text_sm()
                                        .line_height(relative(1.35))
                                        .text_color(cx.theme().danger)
                                        .whitespace_normal()
                                        .child(error),
                                ),
                            )
                        }),
                )
        });
    }

    fn perform_task_review_revise(
        &mut self,
        item: TaskWaitReviewDisplayItem,
        feedback: String,
        cx: &mut Context<Self>,
    ) {
        let request = match task_review::plan_task_review_revise(
            &item,
            feedback,
            &mut self.task_review_actions,
        ) {
            Ok(Some(request)) => request,
            Ok(None) => return,
            Err(error) => {
                self.apply_task_review_plan_error(item.candidate_id.as_str(), error);
                cx.notify();
                return;
            }
        };
        let task_review::TaskReviewActionRequest {
            action_key,
            candidate_id,
            params,
        } = request;

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.task_revise(params) })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    let error = result.err().map(|error| format!("{error:#}"));
                    view.task_review_actions.finish_action(
                        action_key.as_str(),
                        candidate_id.as_str(),
                        error,
                    );
                    cx.notify();
                });
            }
        })
        .detach();

        cx.notify();
    }

    fn perform_task_review_cancel(
        &mut self,
        item: TaskWaitReviewDisplayItem,
        cx: &mut Context<Self>,
    ) {
        let request = match task_review::plan_task_review_cancel(
            &item,
            Some("Cancelled during result review".to_owned()),
            &mut self.task_review_actions,
        ) {
            Ok(Some(request)) => request,
            Ok(None) => return,
            Err(error) => {
                self.apply_task_review_plan_error(item.candidate_id.as_str(), error);
                cx.notify();
                return;
            }
        };
        let task_review::TaskReviewActionRequest {
            action_key,
            candidate_id,
            params,
        } = request;

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.task_cancel(params) })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    let error = result.err().map(|error| format!("{error:#}"));
                    view.task_review_actions.finish_action(
                        action_key.as_str(),
                        candidate_id.as_str(),
                        error,
                    );
                    cx.notify();
                });
            }
        })
        .detach();

        cx.notify();
    }

    fn timeline_detail_block(
        &self,
        label: String,
        text: String,
        monospace: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        div()
            .w_full()
            .overflow_hidden()
            .rounded_lg()
            .bg(cx.theme().muted)
            .p_3()
            .child(
                v_flex()
                    .w_full()
                    .gap_2()
                    .child(div().text_xs().opacity(0.6).child(label))
                    .child(
                        div()
                            .w_full()
                            .whitespace_normal()
                            .text_xs()
                            .when(monospace, |this| this.font_family("monospace"))
                            .child(text),
                    ),
            )
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::{mcp_timeline_metadata, task_wait_review_display};
    use pioneer_protocol::{ToolDisplayPayload, ToolMetadata, ToolOutputSummary};
    use serde_json::json;

    #[test]
    fn extracts_mcp_timeline_metadata_from_summary_display() {
        let display = ToolDisplayPayload::Summary(ToolOutputSummary {
            title: "MCP resend/send completed".to_owned(),
            lines: vec!["12 ms".to_owned()],
            metadata: ToolMetadata::from_json(json!({
                "source": "mcp",
                "duration_ms": 12,
                "mcp": {
                    "server_name": "resend",
                    "raw_tool_name": "send",
                    "catalog_version": "cat_1",
                    "snapshot_version": 7,
                    "runtime_state": "ready",
                    "result_truncated": false
                }
            })),
            truncated: false,
        });

        let metadata = mcp_timeline_metadata(&display).expect("MCP metadata should be visible");

        assert_eq!(metadata.label(), "resend/send");
        assert_eq!(metadata.catalog_version.as_deref(), Some("cat_1"));
        assert_eq!(metadata.snapshot_version, Some(7));
        assert_eq!(metadata.runtime_state.as_deref(), Some("ready"));
        assert_eq!(metadata.duration_ms, Some(12));
        assert_eq!(metadata.result_truncated, Some(false));
    }

    #[test]
    fn phase_12_extracts_task_wait_review_required_display_from_summary_metadata() {
        let display = ToolDisplayPayload::Summary(ToolOutputSummary {
            title: "task_wait completed".to_owned(),
            lines: Vec::new(),
            metadata: ToolMetadata::from_json(json!({
                "sanitizedResult": {
                    "mode": "all_terminal_or_review_required",
                    "reviewRequiredCount": 1,
                    "reviewRequired": [{
                        "taskId": "task_review00000001",
                        "runId": "run_review000000001",
                        "title": "Review child work",
                        "status": "waiting_review",
                        "candidateId": "candidate_review0001",
                        "candidateStatus": "pending_review",
                        "reviewMode": "user_approval",
                        "userApprovalRequired": true,
                        "round": 0,
                        "summary": "child result",
                        "resultPreview": "child result",
                        "diagnostics": ["schema matched"],
                        "maxRevisionRounds": 2,
                        "remainingRevisionRounds": 1,
                        "allowedActions": ["task_accept", "task_revise", "task_cancel"]
                    }]
                }
            })),
            truncated: false,
        });

        let review = task_wait_review_display("task_wait", &display)
            .expect("review-required task_wait should produce display model");

        assert_eq!(review.review_required_count, 1);
        assert_eq!(
            review.mode.as_deref(),
            Some("all_terminal_or_review_required")
        );
        assert_eq!(review.items[0].candidate_id, "candidate_review0001");
        assert!(review.items[0].user_approval_required);
        assert!(review.items[0].user_controls_allowed());
        assert!(
            review
                .details()
                .contains("Action required: task_accept or task_revise or task_cancel")
        );
    }

    #[test]
    fn phase_12_parent_agent_review_required_display_is_read_only() {
        let display = ToolDisplayPayload::Summary(ToolOutputSummary {
            title: "task_wait completed".to_owned(),
            lines: Vec::new(),
            metadata: ToolMetadata::from_json(json!({
                "sanitizedResult": {
                    "mode": "all_terminal_or_review_required",
                    "reviewRequiredCount": 1,
                    "reviewRequired": [{
                        "taskId": "task_review00000001",
                        "runId": "run_review000000001",
                        "title": "Review child work",
                        "status": "waiting_review",
                        "candidateId": "candidate_review0001",
                        "candidateStatus": "pending_review",
                        "reviewMode": "parent_agent",
                        "userApprovalRequired": false,
                        "round": 0,
                        "summary": "child result",
                        "allowedActions": ["task_accept", "task_revise", "task_cancel"]
                    }]
                }
            })),
            truncated: false,
        });

        let review = task_wait_review_display("task_wait", &display)
            .expect("parent-agent review-required task_wait should still render");

        assert_eq!(review.items[0].review_mode.as_deref(), Some("parent_agent"));
        assert!(!review.items[0].user_controls_allowed());
        assert!(review.details().contains("Review mode: parent_agent"));
        assert!(!review.details().contains("User approval required"));
    }
}
