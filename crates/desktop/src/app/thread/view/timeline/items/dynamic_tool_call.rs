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
use pioneer_protocol::{
    TaskAcceptParams, TaskCancelParams, TaskCancelScope, TaskReviseParams, ToolDisplayPayload,
    ToolMetadataValue, TurnItem,
};
use serde_json::Value as JsonValue;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

fn final_dynamic_tool_status(status: TimelineEntryStatus, success: Option<bool>) -> (String, bool) {
    match status {
        TimelineEntryStatus::Cancelled => (t!("timeline.tool.cancelled").to_string(), false),
        TimelineEntryStatus::Failed => (t!("timeline.tool.failed").to_string(), false),
        TimelineEntryStatus::Running => (t!("timeline.tool.running").to_string(), true),
        TimelineEntryStatus::Completed => {
            if matches!(success, Some(false)) {
                (t!("timeline.tool.failed").to_string(), false)
            } else {
                (t!("timeline.tool.completed").to_string(), true)
            }
        }
    }
}

fn pretty_json(value: &JsonValue) -> Option<String> {
    if value.is_null() {
        return None;
    }
    let text = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());
    (!text.trim().is_empty() && text.trim() != "{}").then_some(text)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct McpTimelineMetadata {
    server_id: Option<String>,
    server_name: String,
    raw_tool_name: String,
    catalog_version: Option<String>,
    snapshot_version: Option<u64>,
    runtime_state: Option<String>,
    duration_ms: Option<u64>,
    result_truncated: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TaskWaitReviewDisplay {
    review_required_count: u32,
    mode: Option<String>,
    items: Vec<TaskWaitReviewDisplayItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TaskWaitReviewDisplayItem {
    task_id: String,
    run_id: Option<String>,
    title: Option<String>,
    status: Option<String>,
    candidate_id: String,
    candidate_status: Option<String>,
    review_mode: Option<String>,
    user_approval_required: bool,
    round: Option<u32>,
    summary: Option<String>,
    result_preview: Option<String>,
    extraction_error_preview: Option<String>,
    diagnostics: Vec<String>,
    max_revision_rounds: Option<u32>,
    remaining_revision_rounds: Option<u32>,
    allowed_actions: Vec<String>,
    revision_blocked_reason: Option<String>,
}

impl McpTimelineMetadata {
    fn label(&self) -> String {
        format!("{}/{}", self.server_name, self.raw_tool_name)
    }

    fn details(&self) -> String {
        let mut lines = vec![
            format!("Server: {}", self.server_name),
            format!("Tool: {}", self.raw_tool_name),
        ];
        if let Some(catalog_version) = self.catalog_version.as_deref() {
            lines.push(format!("Catalog: {catalog_version}"));
        }
        if let Some(snapshot_version) = self.snapshot_version {
            lines.push(format!("Snapshot: {snapshot_version}"));
        }
        if let Some(runtime_state) = self.runtime_state.as_deref() {
            lines.push(format!("Runtime: {runtime_state}"));
        }
        if let Some(duration_ms) = self.duration_ms {
            lines.push(format!("Duration: {duration_ms} ms"));
        }
        if self.result_truncated == Some(true) {
            lines.push("Result: truncated".to_owned());
        }
        lines.join("\n")
    }
}

impl TaskWaitReviewDisplay {
    fn details(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!(
            "{} review-required candidate(s)",
            self.review_required_count.max(self.items.len() as u32)
        ));
        if let Some(mode) = self.mode.as_deref() {
            lines.push(format!("Wait mode: {mode}"));
        }

        for (index, item) in self.items.iter().enumerate() {
            if !lines.is_empty() {
                lines.push(String::new());
            }
            lines.push(format!("Candidate {}", index + 1));
            lines.extend(item.details());
        }

        lines.join("\n")
    }
}

impl TaskWaitReviewDisplayItem {
    fn user_controls_allowed(&self) -> bool {
        self.user_approval_required && self.review_mode.as_deref() == Some("user_approval")
    }

    fn allows_action(&self, action: &str) -> bool {
        self.allowed_actions.iter().any(|value| value == action)
    }

    fn details(&self) -> Vec<String> {
        let mut lines = Vec::new();
        if let Some(title) = self.title.as_deref() {
            lines.push(format!("Task: {title}"));
        }
        lines.push(format!("Task id: {}", self.task_id));
        if let Some(run_id) = self.run_id.as_deref() {
            lines.push(format!("Run id: {run_id}"));
        }
        lines.push(format!("Candidate id: {}", self.candidate_id));
        if let Some(status) = self.status.as_deref() {
            lines.push(format!("Task status: {status}"));
        }
        if let Some(candidate_status) = self.candidate_status.as_deref() {
            lines.push(format!("Candidate status: {candidate_status}"));
        }
        if let Some(round) = self.round {
            lines.push(format!("Round: {round}"));
        }
        if let Some(review_mode) = self.review_mode.as_deref() {
            lines.push(format!("Review mode: {review_mode}"));
        }
        if self.user_approval_required {
            lines.push("User approval required".to_owned());
        }
        if !self.allowed_actions.is_empty() {
            lines.push(format!(
                "Action required: {}",
                self.allowed_actions.join(" or ")
            ));
        }
        if let Some(remaining) = self.remaining_revision_rounds {
            let max = self
                .max_revision_rounds
                .map(|max| max.to_string())
                .unwrap_or_else(|| "?".to_owned());
            lines.push(format!("Revision rounds remaining: {remaining}/{max}"));
        }
        if let Some(reason) = self.revision_blocked_reason.as_deref() {
            lines.push(format!("Revision blocked: {reason}"));
        }
        if let Some(summary) = self.summary.as_deref() {
            lines.push(format!("Summary: {summary}"));
        }
        if let Some(result_preview) = self.result_preview.as_deref() {
            lines.push(format!("Result preview: {result_preview}"));
        }
        if let Some(error_preview) = self.extraction_error_preview.as_deref() {
            lines.push(format!("Extraction error: {error_preview}"));
        }
        if !self.diagnostics.is_empty() {
            lines.push(format!("Diagnostics: {}", self.diagnostics.join("; ")));
        }
        lines
    }
}

fn mcp_timeline_metadata(display: &ToolDisplayPayload) -> Option<McpTimelineMetadata> {
    let metadata = match display {
        ToolDisplayPayload::Summary(summary) => &summary.metadata,
        ToolDisplayPayload::Progress { metadata, .. } => metadata,
        _ => return None,
    };
    let source_is_mcp = metadata
        .get("source")
        .and_then(ToolMetadataValue::as_str)
        .is_some_and(|source| source == "mcp");
    let mcp = metadata.get("mcp").and_then(ToolMetadataValue::as_object)?;

    if !source_is_mcp && mcp.is_empty() {
        return None;
    }

    Some(McpTimelineMetadata {
        server_id: metadata_string(mcp, &["server_id", "serverId"]),
        server_name: metadata_string(mcp, &["server_name", "serverName"])?,
        raw_tool_name: metadata_string(mcp, &["raw_tool_name", "rawToolName"])?,
        catalog_version: metadata_string(mcp, &["catalog_version", "catalogVersion"]),
        snapshot_version: metadata_u64(mcp, &["snapshot_version", "snapshotVersion"]),
        runtime_state: metadata_string(mcp, &["runtime_state", "runtimeState"]),
        duration_ms: metadata_u64(mcp, &["duration_ms", "durationMs"])
            .or_else(|| {
                metadata
                    .get("duration_ms")
                    .and_then(ToolMetadataValue::as_u64)
            })
            .or_else(|| {
                metadata
                    .get("durationMs")
                    .and_then(ToolMetadataValue::as_u64)
            }),
        result_truncated: metadata_bool(mcp, &["result_truncated", "resultTruncated"]).or_else(
            || {
                metadata
                    .get("truncated")
                    .and_then(ToolMetadataValue::as_bool)
            },
        ),
    })
}

fn task_wait_review_display(
    tool_name: &str,
    display: &ToolDisplayPayload,
) -> Option<TaskWaitReviewDisplay> {
    if tool_name != "task_wait" {
        return None;
    }
    let value = display_summary_sanitized_result(display)?;
    let review_required = value.get("reviewRequired")?.as_array()?;
    if review_required.is_empty() {
        return None;
    }

    let items = review_required
        .iter()
        .filter_map(task_wait_review_display_item)
        .collect::<Vec<_>>();
    if items.is_empty() {
        return None;
    }

    Some(TaskWaitReviewDisplay {
        review_required_count: json_u32(&value, "reviewRequiredCount")
            .unwrap_or(items.len() as u32),
        mode: json_string(&value, "mode"),
        items,
    })
}

fn display_summary_sanitized_result(display: &ToolDisplayPayload) -> Option<JsonValue> {
    let ToolDisplayPayload::Summary(summary) = display else {
        return None;
    };
    summary
        .metadata
        .get("sanitizedResult")
        .map(ToolMetadataValue::to_json)
}

fn task_wait_review_display_item(value: &JsonValue) -> Option<TaskWaitReviewDisplayItem> {
    let task_id = json_string(value, "taskId")?;
    let candidate_id = json_string(value, "candidateId")?;
    Some(TaskWaitReviewDisplayItem {
        task_id,
        run_id: json_string(value, "runId"),
        title: json_string(value, "title"),
        status: json_string(value, "status"),
        candidate_id,
        candidate_status: json_string(value, "candidateStatus"),
        review_mode: json_string(value, "reviewMode"),
        user_approval_required: json_bool(value, "userApprovalRequired").unwrap_or(false),
        round: json_u32(value, "round"),
        summary: json_string(value, "summary"),
        result_preview: json_string(value, "resultPreview"),
        extraction_error_preview: json_string(value, "extractionErrorPreview"),
        diagnostics: json_string_array(value, "diagnostics"),
        max_revision_rounds: json_u32(value, "maxRevisionRounds"),
        remaining_revision_rounds: json_u32(value, "remainingRevisionRounds"),
        allowed_actions: json_string_array(value, "allowedActions"),
        revision_blocked_reason: json_string(value, "revisionBlockedReason"),
    })
}

fn json_string(value: &JsonValue, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn json_bool(value: &JsonValue, key: &str) -> Option<bool> {
    value.get(key).and_then(JsonValue::as_bool)
}

fn json_u32(value: &JsonValue, key: &str) -> Option<u32> {
    value
        .get(key)
        .and_then(JsonValue::as_u64)
        .and_then(|value| u32::try_from(value).ok())
}

fn json_string_array(value: &JsonValue, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(JsonValue::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(JsonValue::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn task_review_action_key(candidate_id: &str, action: &str) -> String {
    format!("task-review:{candidate_id}:{action}")
}

fn task_review_button_id(candidate_id: &str, action: &'static str) -> (&'static str, u64) {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    candidate_id.hash(&mut hasher);
    (action, hasher.finish())
}

fn metadata_string(fields: &BTreeMap<String, ToolMetadataValue>, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| fields.get(*key).and_then(ToolMetadataValue::as_str))
        .map(str::to_owned)
}

fn metadata_u64(fields: &BTreeMap<String, ToolMetadataValue>, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| fields.get(*key).and_then(ToolMetadataValue::as_u64))
}

fn metadata_bool(fields: &BTreeMap<String, ToolMetadataValue>, keys: &[&str]) -> Option<bool> {
    keys.iter()
        .find_map(|key| fields.get(*key).and_then(ToolMetadataValue::as_bool))
}

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
            let accept_in_flight = self.task_review_action_in_flight(&candidate_id, "accept");
            let revise_in_flight = self.task_review_action_in_flight(&candidate_id, "revise");
            let cancel_in_flight = self.task_review_action_in_flight(&candidate_id, "cancel");
            let any_in_flight = accept_in_flight || revise_in_flight || cancel_in_flight;
            let can_target = item.run_id.is_some();
            let error = self
                .task_review_action_errors
                .get(candidate_id.as_str())
                .cloned();

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
                                            .disabled(
                                                !can_target
                                                    || any_in_flight
                                                    || !item.allows_action("task_accept"),
                                            )
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
                                            .disabled(
                                                !can_target
                                                    || any_in_flight
                                                    || !item.allows_action("task_revise"),
                                            )
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
                                            .disabled(
                                                !can_target
                                                    || any_in_flight
                                                    || !item.allows_action("task_cancel"),
                                            )
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

    fn task_review_action_in_flight(&self, candidate_id: &str, action: &str) -> bool {
        self.task_review_actions_in_flight
            .contains(task_review_action_key(candidate_id, action).as_str())
    }

    fn perform_task_review_accept(
        &mut self,
        item: TaskWaitReviewDisplayItem,
        cx: &mut Context<Self>,
    ) {
        let Some(run_id) = item.run_id.clone() else {
            return;
        };
        let action_key = task_review_action_key(item.candidate_id.as_str(), "accept");
        if !self
            .task_review_actions_in_flight
            .insert(action_key.clone())
        {
            return;
        }
        self.task_review_action_errors
            .remove(item.candidate_id.as_str());

        let ws_sender = self.gateway.ws_command_sender.clone();
        let candidate_id = item.candidate_id.clone();
        let task_id = item.task_id.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let candidate_id_for_request = candidate_id.clone();
                let result = cx
                    .background_spawn(async move {
                        ws_sender.task_accept(TaskAcceptParams {
                            task_id,
                            run_id,
                            candidate_id: candidate_id_for_request,
                            reason: Some("Accepted in desktop".to_owned()),
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    view.task_review_actions_in_flight
                        .remove(action_key.as_str());
                    if let Err(error) = result {
                        view.task_review_action_errors
                            .insert(candidate_id, format!("{error:#}"));
                    }
                    cx.notify();
                });
            }
        })
        .detach();

        cx.notify();
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
                let feedback = feedback_state.read(cx).value().trim().to_owned();
                if feedback.is_empty() {
                    *field_error.borrow_mut() = Some("Feedback is required".to_owned());
                    return false;
                }
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
            let can_submit = !feedback_state.read(cx).value().trim().is_empty();
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
        let Some(run_id) = item.run_id.clone() else {
            return;
        };
        let action_key = task_review_action_key(item.candidate_id.as_str(), "revise");
        if !self
            .task_review_actions_in_flight
            .insert(action_key.clone())
        {
            return;
        }
        self.task_review_action_errors
            .remove(item.candidate_id.as_str());

        let ws_sender = self.gateway.ws_command_sender.clone();
        let candidate_id = item.candidate_id.clone();
        let task_id = item.task_id.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let candidate_id_for_request = candidate_id.clone();
                let result = cx
                    .background_spawn(async move {
                        ws_sender.task_revise(TaskReviseParams {
                            task_id,
                            run_id,
                            candidate_id: candidate_id_for_request,
                            feedback,
                            additional_instructions: Vec::new(),
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    view.task_review_actions_in_flight
                        .remove(action_key.as_str());
                    if let Err(error) = result {
                        view.task_review_action_errors
                            .insert(candidate_id, format!("{error:#}"));
                    }
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
        let action_key = task_review_action_key(item.candidate_id.as_str(), "cancel");
        if !self
            .task_review_actions_in_flight
            .insert(action_key.clone())
        {
            return;
        }
        self.task_review_action_errors
            .remove(item.candidate_id.as_str());

        let ws_sender = self.gateway.ws_command_sender.clone();
        let candidate_id = item.candidate_id.clone();
        let task_id = item.task_id.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.task_cancel(TaskCancelParams {
                            task_id,
                            reason: Some("Cancelled during result review".to_owned()),
                            scope: TaskCancelScope::AttachedSubtree,
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    view.task_review_actions_in_flight
                        .remove(action_key.as_str());
                    if let Err(error) = result {
                        view.task_review_action_errors
                            .insert(candidate_id, format!("{error:#}"));
                    }
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
