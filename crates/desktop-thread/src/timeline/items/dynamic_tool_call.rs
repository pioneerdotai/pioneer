use super::super::TimelineRowTopSpacing;
use crate::assets::PioneerIconName;
use crate::timeline::row_view::RowPresentation;
use gpui_kit::component::button::Button;
use gpui_kit::component::button::ButtonVariants;
use gpui_kit::component::collapsible::Collapsible;
use gpui_kit::component::h_flex;
use gpui_kit::component::v_flex;
use gpui_kit::component::*;
use gpui_kit::prelude::*;
use gpui_kit::*;
use pioneer_client::conversation::display::tool_display_text;
use pioneer_client::conversation::reducer::ItemView;
use pioneer_client::conversation::reducer::TimelineEntry;
use pioneer_client::conversation::reducer::TimelineEntryStatus;
use pioneer_client::timeline::labels::McpTimelineMetadata;
use pioneer_client::timeline::labels::McpTimelineMetadataDetail;
use pioneer_client::timeline::labels::McpTimelineMetadataDetailKind;
use pioneer_client::timeline::labels::McpTimelineMetadataDetailValue;
use pioneer_client::timeline::labels::TaskWaitReviewDetailKind;
use pioneer_client::timeline::labels::TaskWaitReviewDetailRow;
use pioneer_client::timeline::labels::TaskWaitReviewDisplay;
use pioneer_client::timeline::labels::TimelineFinalStatusKind;
use pioneer_client::timeline::labels::final_dynamic_tool_status;
use pioneer_client::timeline::labels::pretty_json;
use pioneer_client::timeline::labels::task_wait_review_display;
use pioneer_client::timeline::types::TurnItem;
use std::hash::Hash;
use std::hash::Hasher;

impl RowPresentation {
    pub(super) fn render_item_dynamic_tool_call(
        &self,
        entry: &TimelineEntry,
        item_view: &ItemView,
        item: &TurnItem,
        top_spacing: TimelineRowTopSpacing,
        is_last_row: bool,
        content_width: Pixels,
        expanded: bool,
        cx: &mut App,
    ) -> AnyElement {
        let (tool_name, success) = match item {
            TurnItem::DynamicToolCall {
                tool_name, success, ..
            } => (tool_name.as_str(), *success),
            _ => ("tool", None),
        };
        let mcp_metadata = self.tool_content().and_then(|tool| tool.mcp.as_ref());
        let mcp_tool_label = mcp_metadata.map(McpTimelineMetadata::label);
        let tool_label_source = mcp_tool_label.as_deref().unwrap_or(tool_name);
        let tool_label = Self::truncate_for_card(tool_label_source, 180);
        let is_running = item_view.status == TimelineEntryStatus::Running;
        let tool_row = || {
            h_flex()
                .flex_1()
                .min_w_0()
                .items_center()
                .gap_2()
                .when(is_running, |this| {
                    this.child(self.spinner_element(
                        crate::timeline::running_indicator::ActivitySpinnerKind::DynamicTool,
                    ))
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

        let running_elapsed_label = self.inline_elapsed();

        let open = expanded;

        let entry_id = entry.id.clone();
        let mut toggle_id_hasher = std::collections::hash_map::DefaultHasher::new();
        entry.id.hash(&mut toggle_id_hasher);
        let toggle_id = toggle_id_hasher.finish();

        let status = final_dynamic_tool_status(item_view.status, success);
        let final_status = dynamic_tool_status_label(status.kind);
        let is_successful = status.successful;
        let details = self.body_element().unwrap_or_else(|| {
            let (arguments, display_text, task_wait_review) = match item {
                TurnItem::DynamicToolCall {
                    arguments,
                    display,
                    tool_name,
                    ..
                } => (
                    pretty_json(arguments),
                    tool_display_text(display),
                    task_wait_review_display(tool_name, display),
                ),
                _ => (
                    None,
                    Some(Self::timeline_entry_text(item_view).to_owned()),
                    None,
                ),
            };
            self.dynamic_tool_details(
                arguments.as_deref(),
                display_text.as_deref(),
                mcp_metadata,
                task_wait_review.as_ref(),
                cx,
            )
        });
        if self.body_only {
            return details;
        }

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
                            self.actions.listener(move |this, _, window, cx| {
                                this.toggle_timeline_item_expanded(entry_id.as_str(), window, cx);
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
                            self.actions.listener(move |this, _, window, cx| {
                                this.toggle_timeline_item_expanded(entry_id.as_str(), window, cx);
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
        cx: &mut App,
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
                self.catalog_input.as_ref().and_then(|input| {
                    input
                        .mcp_servers
                        .iter()
                        .find(|server| server.server_name == mcp_metadata.server_name)
                        .map(|server| server.server_id.clone())
                })
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
                        .on_click(self.actions.listener(move |view, _, _, cx| {
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
        _cx: &mut App,
    ) -> Option<AnyElement> {
        let thread_id = self.current_active_thread_id()?;
        let capabilities = self
            .thread_presentation_capabilities(thread_id)
            .map_or_else(Default::default, |capabilities| {
                pioneer_client::tasks::review::TaskReviewPresentationCapabilities {
                    can_review: capabilities.can_review_tasks,
                    can_cancel: capabilities.can_cancel_tasks,
                }
            });
        let children = review
            .items
            .iter()
            .filter(|item| {
                item.user_controls_allowed()
                    && pioneer_client::tasks::review::task_review_item_is_manageable_by(
                        item,
                        capabilities,
                    )
            })
            .filter_map(|item| {
                self.task_review_views
                    .get(&(thread_id.to_owned(), item.candidate_id.clone()))
                    .cloned()
            })
            .collect::<Vec<_>>();
        if children.is_empty() {
            return None;
        }
        Some(
            v_flex()
                .w_full()
                .gap_2()
                .children(children)
                .into_any_element(),
        )
    }

    fn timeline_detail_block(
        &self,
        label: String,
        text: String,
        monospace: bool,
        cx: &mut App,
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

impl crate::screen::TimelineView {
    pub(crate) fn reconcile_task_review_views(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(thread_id) = self.current_active_thread_id().map(str::to_owned) else {
            self.task_review_views.clear();
            return;
        };
        let client = self.client.clone();
        let items = client
            .snapshot(&pioneer_client::core::ClientScope::Timeline {
                thread_id: thread_id.clone(),
            })
            .and_then(|p| p.typed::<pioneer_client::timeline::presentation::TimelineSnapshot>())
            .map(|p| {
                p.payload()
                    .rows()
                    .iter()
                    .filter_map(|row| row.content()?.tool.as_ref()?.task_review.as_ref())
                    .flat_map(|r| r.items.iter().map(|item| item.candidate_id.clone()))
                    .collect::<std::collections::HashSet<_>>()
            })
            .unwrap_or_default();
        self.task_review_views
            .retain(|(thread, candidate), _| thread == &thread_id && items.contains(candidate));
        for candidate in items {
            let key = (thread_id.clone(), candidate.clone());
            if let Some(view) = self.task_review_views.get(&key) {
                view.read(cx).observe();
                continue;
            }
            let label = t!(
                "timeline.task_review.candidate",
                candidate_id = Self::truncate_for_card(&candidate, 96).as_str()
            )
            .to_string();
            let view = crate::task_review::TaskReviewActionView::new(
                client.clone(),
                self.thread_bindings.registrar(),
                thread_id.clone(),
                candidate,
                label,
                window,
                cx,
            );
            self.task_review_views.insert(key, view);
        }
    }
}
