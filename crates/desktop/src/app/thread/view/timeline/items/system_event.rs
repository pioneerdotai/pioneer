use super::super::TimelineRowTopSpacing;
use crate::app::{
    conversation::{ItemView, TimelineEntry},
    root::PioneerDesktop,
};
use gpui::{prelude::*, *};
use gpui_component::{collapsible::Collapsible, h_flex, v_flex, *};
use pioneer_client::timeline::labels::{
    CapabilityRejectionKind, CapabilityRejectionLabel, SystemEventDetailLabel,
    SystemEventDetailValue, SystemEventLabel, SystemEventMessage,
    capability_rejection_rows_for_event, execution_window_detail_rows, pretty_details,
    system_event_presentation,
};
use pioneer_protocol::{SystemEventLevel, TurnItem};
use serde_json::Value as JsonValue;
use std::hash::{Hash, Hasher};

fn system_event_icon(level: &SystemEventLevel) -> IconName {
    match level {
        SystemEventLevel::Info => IconName::Info,
        SystemEventLevel::Warning | SystemEventLevel::Error => IconName::TriangleAlert,
    }
}

fn system_event_message_text(message: &SystemEventMessage) -> String {
    match message {
        SystemEventMessage::Raw(message) => message.clone(),
        SystemEventMessage::TurnCancelled => t!("timeline.system.turn_cancelled").to_string(),
        SystemEventMessage::TurnFailed => t!("timeline.system.turn_failed").to_string(),
        SystemEventMessage::TurnBlocked => t!("timeline.system.turn_blocked").to_string(),
        SystemEventMessage::Timeout { recovery_started } => {
            if *recovery_started {
                t!("timeline.system.timeout_with_recovery").to_string()
            } else {
                t!("timeline.system.timeout_without_recovery").to_string()
            }
        }
        SystemEventMessage::RecoveryOpened => t!("timeline.system.recovery_opened").to_string(),
        SystemEventMessage::RecoveryAttached => t!("timeline.system.recovery_attached").to_string(),
        SystemEventMessage::RetryScheduled => t!("timeline.system.retry_scheduled").to_string(),
        SystemEventMessage::RetryStarted => t!("timeline.system.retry_started").to_string(),
        SystemEventMessage::RecoverySucceeded => {
            t!("timeline.system.recovery_succeeded").to_string()
        }
        SystemEventMessage::RecoveryFailed => t!("timeline.system.recovery_failed").to_string(),
        SystemEventMessage::ToolRetryScheduled { tool_name } => t!(
            "timeline.system.tool_retry_scheduled",
            tool_name = tool_name.as_str()
        )
        .to_string(),
        SystemEventMessage::ToolRetryResolved { tool_name } => t!(
            "timeline.system.tool_retry_resolved",
            tool_name = tool_name.as_str()
        )
        .to_string(),
        SystemEventMessage::ToolRetryExhausted { tool_name } => t!(
            "timeline.system.tool_retry_exhausted",
            tool_name = tool_name.as_str()
        )
        .to_string(),
        SystemEventMessage::ToolLoopBudgetExceeded => {
            t!("timeline.system.tool_loop_budget_exceeded").to_string()
        }
    }
}

fn system_event_label_text(label: &SystemEventLabel) -> String {
    match label {
        SystemEventLabel::Level(SystemEventLevel::Info) => t!("timeline.system.info").to_string(),
        SystemEventLabel::Level(SystemEventLevel::Warning) => {
            t!("timeline.system.warning").to_string()
        }
        SystemEventLabel::Level(SystemEventLevel::Error) => t!("timeline.system.error").to_string(),
        SystemEventLabel::Attempt { attempt } => {
            t!("timeline.system.attempt", attempt = *attempt).to_string()
        }
        SystemEventLabel::Timeout => t!("timeline.system.timeout_label").to_string(),
        SystemEventLabel::Recovery => t!("timeline.system.recovery_label").to_string(),
        SystemEventLabel::Retry => t!("timeline.system.retry_label").to_string(),
        SystemEventLabel::Recovered => t!("timeline.system.recovered_label").to_string(),
        SystemEventLabel::Error => t!("timeline.system.error").to_string(),
        SystemEventLabel::RetryResolved => t!("timeline.system.retry_resolved_label").to_string(),
        SystemEventLabel::RetriesExhausted => {
            t!("timeline.system.retries_exhausted_label").to_string()
        }
        SystemEventLabel::ExecutionWindow { window_index } => window_index
            .map(|window_index| {
                t!(
                    "timeline.system.execution_window_with_index",
                    window_index = window_index
                )
                .to_string()
            })
            .unwrap_or_else(|| t!("timeline.system.execution_window_label").to_string()),
        SystemEventLabel::Checkpoint => t!("timeline.system.checkpoint_label").to_string(),
        SystemEventLabel::Continued => t!("timeline.system.continued_label").to_string(),
        SystemEventLabel::Paused => t!("timeline.system.paused_label").to_string(),
        SystemEventLabel::Permissions => t!("timeline.system.permissions_label").to_string(),
    }
}

fn system_event_detail_label_text(label: SystemEventDetailLabel) -> String {
    match label {
        SystemEventDetailLabel::Window => t!("timeline.system.detail_window").to_string(),
        SystemEventDetailLabel::Status => t!("timeline.system.detail_status").to_string(),
        SystemEventDetailLabel::Reason => t!("timeline.system.detail_reason").to_string(),
        SystemEventDetailLabel::WindowExhaustion => {
            t!("timeline.system.detail_window_exhaustion").to_string()
        }
        SystemEventDetailLabel::Checkpoint => t!("timeline.system.detail_checkpoint").to_string(),
        SystemEventDetailLabel::PreviousWindow => {
            t!("timeline.system.detail_previous_window").to_string()
        }
        SystemEventDetailLabel::Limit => t!("timeline.system.detail_limit").to_string(),
        SystemEventDetailLabel::AgentRounds => {
            t!("timeline.system.detail_agent_rounds").to_string()
        }
        SystemEventDetailLabel::ToolCalls => t!("timeline.system.detail_tool_calls").to_string(),
        SystemEventDetailLabel::ProviderTokens => {
            t!("timeline.system.detail_provider_tokens").to_string()
        }
        SystemEventDetailLabel::TotalWindows => {
            t!("timeline.system.detail_total_windows").to_string()
        }
        SystemEventDetailLabel::CheckpointKind => {
            t!("timeline.system.detail_checkpoint_kind").to_string()
        }
        SystemEventDetailLabel::CheckpointSize => {
            t!("timeline.system.detail_checkpoint_size").to_string()
        }
    }
}

fn system_event_detail_value_text(value: &SystemEventDetailValue) -> String {
    match value {
        SystemEventDetailValue::Text(value) => value.clone(),
        SystemEventDetailValue::WindowIndex(window_index) => t!(
            "timeline.system.execution_window_with_index",
            window_index = *window_index
        )
        .to_string(),
        SystemEventDetailValue::Bytes(bytes) => {
            t!("timeline.system.bytes", bytes = *bytes).to_string()
        }
    }
}

fn capability_rejection_kind_text(kind: CapabilityRejectionKind) -> String {
    match kind {
        CapabilityRejectionKind::Skill => t!("timeline.system.capability_kind_skill").to_string(),
        CapabilityRejectionKind::McpServer => {
            t!("timeline.system.capability_kind_mcp_server").to_string()
        }
        CapabilityRejectionKind::McpTool => {
            t!("timeline.system.capability_kind_mcp_tool").to_string()
        }
        CapabilityRejectionKind::Capability => {
            t!("timeline.system.capability_kind_capability").to_string()
        }
    }
}

fn capability_rejection_label_text(label: &CapabilityRejectionLabel) -> String {
    match label {
        CapabilityRejectionLabel::Text(value) => value.clone(),
        CapabilityRejectionLabel::Skill => t!("timeline.system.capability_kind_skill").to_string(),
        CapabilityRejectionLabel::McpServer => {
            t!("timeline.system.capability_kind_mcp_server").to_string()
        }
        CapabilityRejectionLabel::McpTool => {
            t!("timeline.system.capability_kind_mcp_tool").to_string()
        }
        CapabilityRejectionLabel::Capability => {
            t!("timeline.system.capability_kind_capability").to_string()
        }
    }
}

impl PioneerDesktop {
    pub(super) fn render_item_system_event(
        &self,
        entry: &TimelineEntry,
        item_view: &ItemView,
        item: &TurnItem,
        top_spacing: TimelineRowTopSpacing,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let (level, message, code, details_value) = match item {
            TurnItem::SystemEvent {
                level,
                message,
                code,
                details,
                ..
            } => (
                level.clone(),
                message.clone(),
                code.clone(),
                details.clone(),
            ),
            _ => (
                SystemEventLevel::Info,
                Self::timeline_entry_text(item_view).to_owned(),
                None,
                None,
            ),
        };
        let presentation = system_event_presentation(
            &level,
            message.as_str(),
            code.as_deref(),
            details_value.as_ref(),
        );
        let presentation_message = system_event_message_text(&presentation.message);
        let presentation_label = system_event_label_text(&presentation.label);
        let has_details = code
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
            || details_value
                .as_ref()
                .and_then(pretty_details)
                .is_some_and(|value| !value.trim().is_empty());

        let tone = match level {
            SystemEventLevel::Info => cx.theme().muted_foreground,
            SystemEventLevel::Warning => cx.theme().warning,
            SystemEventLevel::Error => cx.theme().danger,
        };

        let event_row = || {
            h_flex()
                .flex_1()
                .min_w_0()
                .items_center()
                .gap_2()
                .child(
                    Icon::new(system_event_icon(&level))
                        .size_4()
                        .text_color(tone),
                )
                .child(
                    div()
                        .flex_1()
                        .min_w_0()
                        .overflow_hidden()
                        .text_ellipsis()
                        .text_sm()
                        .line_height(relative(1.45))
                        .child(presentation_message.clone()),
                )
                .into_any_element()
        };

        let content = if has_details {
            let open = self
                .thread_timeline_item_expanded
                .borrow()
                .contains(entry.id.as_str());

            let entry_id = entry.id.clone();
            let mut toggle_id_hasher = std::collections::hash_map::DefaultHasher::new();
            entry.id.hash(&mut toggle_id_hasher);
            let toggle_id = toggle_id_hasher.finish();

            Collapsible::new()
                .gap_2()
                .open(open)
                .child(
                    div()
                        .id(("system-event-toggle", toggle_id))
                        .w_full()
                        .flex()
                        .items_center()
                        .opacity(0.75)
                        .hover(|this| this.opacity(0.95))
                        .child(
                            h_flex()
                                .w_full()
                                .items_center()
                                .justify_between()
                                .gap_3()
                                .child(event_row())
                                .child(
                                    h_flex()
                                        .flex_none()
                                        .max_w(px(260.0))
                                        .items_center()
                                        .gap_2()
                                        .text_sm()
                                        .opacity(0.8)
                                        .child(
                                            div()
                                                .min_w_0()
                                                .overflow_hidden()
                                                .text_ellipsis()
                                                .child(presentation_label.clone()),
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
                .content(self.system_event_details(code.as_deref(), details_value.as_ref(), cx))
                .into_any_element()
        } else {
            div()
                .w_full()
                .flex()
                .items_center()
                .opacity(0.75)
                .child(event_row())
                .into_any_element()
        };

        self.render_item_row(top_spacing, is_last_row, content_width, content)
    }

    fn system_event_details(
        &self,
        code: Option<&str>,
        details: Option<&JsonValue>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let mut body = v_flex().w_full().gap_2().pt_1();

        if let Some(code) = code.filter(|value| !value.trim().is_empty()) {
            body = body.child(
                h_flex()
                    .items_center()
                    .gap_2()
                    .text_sm()
                    .opacity(0.8)
                    .child(t!("timeline.system.code").to_string())
                    .child(code.to_owned()),
            );
        }

        let capability_rows = capability_rejection_rows_for_event(code, details);
        if !capability_rows.is_empty() {
            let mut rows = v_flex().w_full().gap_2();
            for row in capability_rows {
                let label = capability_rejection_label_text(&row.label);
                rows = rows.child(
                    v_flex()
                        .w_full()
                        .gap_1()
                        .rounded_lg()
                        .border_1()
                        .border_color(cx.theme().border)
                        .p_3()
                        .child(
                            h_flex()
                                .items_center()
                                .gap_2()
                                .text_sm()
                                .font_weight(FontWeight::MEDIUM)
                                .child(label)
                                .child(
                                    div()
                                        .rounded_md()
                                        .bg(cx.theme().warning.opacity(0.10))
                                        .px_1p5()
                                        .py_0p5()
                                        .text_xs()
                                        .text_color(cx.theme().warning)
                                        .child(capability_rejection_kind_text(row.kind)),
                                ),
                        )
                        .child(
                            div()
                                .text_sm()
                                .line_height(relative(1.45))
                                .text_color(cx.theme().muted_foreground)
                                .child(row.message),
                        ),
                );
            }
            return body.child(rows).into_any_element();
        }

        let execution_window_rows = execution_window_detail_rows(code, details);
        if !execution_window_rows.is_empty() {
            let mut rows = v_flex().w_full().gap_1p5();
            for row in execution_window_rows {
                let label = system_event_detail_label_text(row.label);
                let value = system_event_detail_value_text(&row.value);
                rows = rows.child(
                    h_flex()
                        .w_full()
                        .items_start()
                        .gap_3()
                        .text_sm()
                        .child(
                            div()
                                .w(px(132.0))
                                .flex_none()
                                .text_color(cx.theme().muted_foreground)
                                .child(label),
                        )
                        .child(
                            div()
                                .min_w_0()
                                .flex_1()
                                .overflow_hidden()
                                .text_ellipsis()
                                .child(value),
                        ),
                );
            }
            return body.child(rows).into_any_element();
        }

        let details = details.and_then(pretty_details);
        if let Some(details) = details.filter(|value| !value.trim().is_empty()) {
            body = body.child(
                div()
                    .w_full()
                    .overflow_hidden()
                    .rounded_lg()
                    .border_1()
                    .border_color(cx.theme().border)
                    .p_3()
                    .child(
                        div()
                            .w_full()
                            .whitespace_normal()
                            .text_sm()
                            .line_height(relative(1.45))
                            .font_family("monospace")
                            .child(Self::truncate_for_card(&details, 3_000)),
                    ),
            );
        }

        body.into_any_element()
    }
}
