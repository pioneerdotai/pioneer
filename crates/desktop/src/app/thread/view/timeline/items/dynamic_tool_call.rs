use super::super::TimelineRowTopSpacing;
use super::format_running_elapsed;
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
    dialog::DialogFooter,
    form::{field, v_form},
    h_flex,
    input::{Input, InputState},
    spinner::Spinner,
    v_flex, *,
};
use pioneer_client::{
    tasks::review as task_review,
    timeline::labels::{
        McpTimelineMetadata, McpTimelineMetadataDetail, McpTimelineMetadataDetailKind,
        McpTimelineMetadataDetailValue, TaskWaitReviewDetailKind, TaskWaitReviewDetailRow,
        TaskWaitReviewDisplay, TaskWaitReviewDisplayItem, TimelineFinalStatusKind,
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
        top_spacing: TimelineRowTopSpacing,
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

        let running_elapsed_label = format_running_elapsed(item_view);

        let open = self
            .thread_timeline_item_expanded
            .borrow()
            .contains(entry.id.as_str());

        let entry_id = entry.id.clone();
        let mut toggle_id_hasher = std::collections::hash_map::DefaultHasher::new();
        entry.id.hash(&mut toggle_id_hasher);
        let toggle_id = toggle_id_hasher.finish();

        let status = final_dynamic_tool_status(item_view.status, success);
        let final_status = dynamic_tool_status_label(status.kind);
        let is_successful = status.successful;
        let details = self.dynamic_tool_details(
            arguments.as_deref(),
            display_text.as_deref(),
            mcp_metadata.as_ref(),
            task_wait_review.as_ref(),
            cx,
        );

        let content = if is_running {
            Collapsible::new()
                .gap_2()
                .open(open)
                .child(
                    div()
                        .id(("dynamic-tool-toggle", toggle_id))
                        .w_full()
                        .flex()
                        .items_center()
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
                                        .items_center()
                                        .gap_2()
                                        .text_sm()
                                        .font_semibold()
                                        .child(t!("timeline.tool.running").to_string())
                                        .when_some(running_elapsed_label, |this, elapsed| {
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
        } else {
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

        self.render_item_row(top_spacing, is_last_row, content_width, content)
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
                mcp_timeline_details_text(mcp_metadata.detail_rows().as_slice()),
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
            details = details.child(
                self.timeline_detail_block(
                    t!("timeline.task_review.details.title").to_string(),
                    Self::truncate_for_card(
                        task_wait_review_details_text(task_wait_review.detail_rows().as_slice())
                            .as_str(),
                        4_000,
                    ),
                    false,
                    cx,
                ),
            );
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

        if let Some(server_id) =
            open_mcp_server_id.filter(|_| self.principal_presentation_capabilities().can_use_mcp)
        {
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

    fn task_review_presentation_capabilities(
        &self,
    ) -> task_review::TaskReviewPresentationCapabilities {
        self.current_active_thread_id()
            .and_then(|thread_id| self.thread_presentation_capabilities(thread_id))
            .map_or_else(Default::default, |capabilities| {
                task_review::TaskReviewPresentationCapabilities {
                    can_review: capabilities.can_review_tasks,
                    can_cancel: capabilities.can_cancel_tasks,
                }
            })
    }

    fn render_task_wait_review_controls(
        &self,
        review: &TaskWaitReviewDisplay,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        let review_capabilities = self.task_review_presentation_capabilities();
        let actionable_items = review
            .items
            .iter()
            .filter(|item| {
                item.user_controls_allowed()
                    && task_review::task_review_item_is_manageable_by(item, review_capabilities)
            })
            .cloned()
            .collect::<Vec<_>>();
        if actionable_items.is_empty() {
            return None;
        }

        let mut controls = v_flex().w_full().gap_2();
        for item in actionable_items {
            let candidate_id = item.candidate_id.clone();
            let accept_enabled = task_review::task_review_action_authorized_and_enabled(
                &item,
                task_review::TaskReviewAction::Accept,
                review_capabilities,
                &self.task_review_actions,
            );
            let revise_enabled = task_review::task_review_action_authorized_and_enabled(
                &item,
                task_review::TaskReviewAction::Revise,
                review_capabilities,
                &self.task_review_actions,
            );
            let cancel_enabled = task_review::task_review_action_authorized_and_enabled(
                &item,
                task_review::TaskReviewAction::Cancel,
                review_capabilities,
                &self.task_review_actions,
            );
            let error = self
                .task_review_actions
                .error(candidate_id.as_str())
                .map(str::to_owned);

            let accept_item = item.clone();
            let revise_item = item.clone();
            let cancel_item = item.clone();
            let candidate_label = t!(
                "timeline.task_review.candidate",
                candidate_id = Self::truncate_for_card(&candidate_id, 96).as_str()
            )
            .to_string();

            controls = controls.child(
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
                                        .label(t!("timeline.task_review.accept_result").to_string())
                                        .disabled(!accept_enabled)
                                        .on_click(
                                            cx.listener(move |view, _, _, cx| {
                                                view.perform_task_review_accept(
                                                    accept_item.clone(),
                                                    cx,
                                                );
                                            }),
                                        ),
                                    )
                                    .child(
                                        Button::new(task_review_button_id(
                                            &candidate_id,
                                            "task-review-revise",
                                        ))
                                        .small()
                                        .outline()
                                        .label(
                                            t!("timeline.task_review.request_revision").to_string(),
                                        )
                                        .disabled(!revise_enabled)
                                        .on_click(
                                            cx.listener(move |view, _, window, cx| {
                                                view.open_task_review_revise_dialog(
                                                    revise_item.clone(),
                                                    window,
                                                    cx,
                                                );
                                            }),
                                        ),
                                    )
                                    .child(
                                        Button::new(task_review_button_id(
                                            &candidate_id,
                                            "task-review-cancel",
                                        ))
                                        .small()
                                        .danger()
                                        .label(t!("timeline.task_review.cancel_task").to_string())
                                        .disabled(!cancel_enabled)
                                        .on_click(
                                            cx.listener(move |view, _, _, cx| {
                                                view.perform_task_review_cancel(
                                                    cancel_item.clone(),
                                                    cx,
                                                );
                                            }),
                                        ),
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
            task_review::TaskReviewPlanError::BlankFeedback => {
                t!("timeline.task_review.error.feedback_required").to_string()
            }
            task_review::TaskReviewPlanError::MissingRunId
            | task_review::TaskReviewPlanError::MissingTaskId
            | task_review::TaskReviewPlanError::MissingCandidateId => {
                t!("timeline.task_review.error.target_incomplete").to_string()
            }
            task_review::TaskReviewPlanError::UserControlsNotAllowed
            | task_review::TaskReviewPlanError::ActionNotAllowed { .. } => {
                t!("timeline.task_review.error.action_unavailable").to_string()
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
                .placeholder(t!("timeline.task_review.revision_feedback_placeholder").to_string())
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
                .title(
                    div()
                        .text_base()
                        .font_semibold()
                        .child(t!("timeline.task_review.request_revision").to_string()),
                )
                .on_ok({
                    let submit_revision = submit_revision.clone();
                    move |_, _, cx| submit_revision(cx)
                })
                .footer(DialogFooter::new().children({
                    let submit_revision = submit_revision.clone();
                    vec![
                        Button::new("task-review-revise-cancel")
                            .small()
                            .outline()
                            .label(t!("buttons.cancel").to_string())
                            .on_click(|_, window, cx| {
                                window.close_dialog(cx);
                            })
                            .into_any_element(),
                        Button::new("task-review-revise-submit")
                            .small()
                            .primary()
                            .label(t!("timeline.task_review.request_revision").to_string())
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
                }))
                .child(
                    v_form()
                        .child(
                            field()
                                .label(t!("timeline.task_review.feedback_label").to_string())
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

fn dynamic_tool_status_label(kind: TimelineFinalStatusKind) -> String {
    match kind {
        TimelineFinalStatusKind::Cancelled => t!("timeline.tool.cancelled").to_string(),
        TimelineFinalStatusKind::Blocked => t!("timeline.tool.blocked").to_string(),
        TimelineFinalStatusKind::Failed => t!("timeline.tool.failed").to_string(),
        TimelineFinalStatusKind::Running => t!("timeline.tool.running").to_string(),
        TimelineFinalStatusKind::Completed => t!("timeline.tool.completed").to_string(),
    }
}

fn mcp_timeline_details_text(rows: &[McpTimelineMetadataDetail]) -> String {
    rows.iter()
        .map(|row| {
            format!(
                "{}: {}",
                mcp_timeline_detail_kind_label(row.kind),
                mcp_timeline_detail_value_label(&row.value)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn mcp_timeline_detail_kind_label(kind: McpTimelineMetadataDetailKind) -> String {
    match kind {
        McpTimelineMetadataDetailKind::Server => t!("timeline.tool.mcp_detail_server").to_string(),
        McpTimelineMetadataDetailKind::Tool => t!("timeline.tool.mcp_detail_tool").to_string(),
        McpTimelineMetadataDetailKind::Catalog => {
            t!("timeline.tool.mcp_detail_catalog").to_string()
        }
        McpTimelineMetadataDetailKind::Snapshot => {
            t!("timeline.tool.mcp_detail_snapshot").to_string()
        }
        McpTimelineMetadataDetailKind::Runtime => {
            t!("timeline.tool.mcp_detail_runtime").to_string()
        }
        McpTimelineMetadataDetailKind::Duration => {
            t!("timeline.tool.mcp_detail_duration").to_string()
        }
        McpTimelineMetadataDetailKind::Result => t!("timeline.tool.mcp_detail_result").to_string(),
    }
}

fn mcp_timeline_detail_value_label(value: &McpTimelineMetadataDetailValue) -> String {
    match value {
        McpTimelineMetadataDetailValue::Text(value) => value.clone(),
        McpTimelineMetadataDetailValue::U64(value) => value.to_string(),
        McpTimelineMetadataDetailValue::DurationMs(duration_ms) => t!(
            "timeline.tool.duration_value_ms",
            duration_ms = *duration_ms
        )
        .to_string(),
        McpTimelineMetadataDetailValue::Truncated => {
            t!("timeline.tool.mcp_detail_truncated").to_string()
        }
    }
}

fn task_wait_review_details_text(rows: &[TaskWaitReviewDetailRow]) -> String {
    let mut lines = Vec::new();
    for row in rows {
        match row {
            TaskWaitReviewDetailRow::ReviewRequiredCount { count } => lines.push(
                t!(
                    "timeline.task_review.details.review_required_count",
                    count = *count
                )
                .to_string(),
            ),
            TaskWaitReviewDetailRow::WaitMode { mode } => lines.push(format!(
                "{}: {mode}",
                t!("timeline.task_review.details.wait_mode")
            )),
            TaskWaitReviewDetailRow::Candidate { index } => {
                if !lines.is_empty() {
                    lines.push(String::new());
                }
                lines
                    .push(t!("timeline.task_review.details.candidate", index = *index).to_string());
            }
            TaskWaitReviewDetailRow::Field { kind, value } => lines.push(format!(
                "{}: {value}",
                task_wait_review_detail_kind_label(*kind)
            )),
            TaskWaitReviewDetailRow::UserApprovalRequired => {
                lines.push(t!("timeline.task_review.details.user_approval_required").to_string())
            }
            TaskWaitReviewDetailRow::ActionRequired { actions } => {
                let separator = format!(
                    " {} ",
                    t!("timeline.task_review.details.action_separator_or")
                );
                lines.push(format!(
                    "{}: {}",
                    t!("timeline.task_review.details.action_required"),
                    actions.join(separator.as_str())
                ));
            }
            TaskWaitReviewDetailRow::RevisionRoundsRemaining { remaining, max } => {
                let max = max
                    .map(|max| max.to_string())
                    .unwrap_or_else(|| t!("timeline.task_review.details.unknown").to_string());
                lines.push(format!(
                    "{}: {remaining}/{max}",
                    t!("timeline.task_review.details.revision_rounds_remaining")
                ));
            }
            TaskWaitReviewDetailRow::Diagnostics { diagnostics } => lines.push(format!(
                "{}: {}",
                t!("timeline.task_review.details.diagnostics"),
                diagnostics.join("; ")
            )),
        }
    }
    lines.join("\n")
}

fn task_wait_review_detail_kind_label(kind: TaskWaitReviewDetailKind) -> String {
    match kind {
        TaskWaitReviewDetailKind::Task => t!("timeline.task_review.details.task").to_string(),
        TaskWaitReviewDetailKind::TaskId => t!("timeline.task_review.details.task_id").to_string(),
        TaskWaitReviewDetailKind::RunId => t!("timeline.task_review.details.run_id").to_string(),
        TaskWaitReviewDetailKind::CandidateId => {
            t!("timeline.task_review.details.candidate_id").to_string()
        }
        TaskWaitReviewDetailKind::TaskStatus => {
            t!("timeline.task_review.details.task_status").to_string()
        }
        TaskWaitReviewDetailKind::CandidateStatus => {
            t!("timeline.task_review.details.candidate_status").to_string()
        }
        TaskWaitReviewDetailKind::Round => t!("timeline.task_review.details.round").to_string(),
        TaskWaitReviewDetailKind::ReviewMode => {
            t!("timeline.task_review.details.review_mode").to_string()
        }
        TaskWaitReviewDetailKind::PermissionMode => {
            t!("timeline.task_review.details.permission_mode").to_string()
        }
        TaskWaitReviewDetailKind::PermissionSource => {
            t!("timeline.task_review.details.permission_source").to_string()
        }
        TaskWaitReviewDetailKind::RevisionBlocked => {
            t!("timeline.task_review.details.revision_blocked").to_string()
        }
        TaskWaitReviewDetailKind::Summary => t!("timeline.task_review.details.summary").to_string(),
        TaskWaitReviewDetailKind::ResultPreview => {
            t!("timeline.task_review.details.result_preview").to_string()
        }
        TaskWaitReviewDetailKind::ExtractionError => {
            t!("timeline.task_review.details.extraction_error").to_string()
        }
    }
}
