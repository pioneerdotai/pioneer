use crate::app::{
    conversation::{ItemView, TimelineEntry},
    root::PioneerDesktop,
};
use gpui::{prelude::*, *};
use gpui_component::{collapsible::Collapsible, h_flex, v_flex, *};
#[cfg(test)]
use pioneer_client::timeline::labels::capability_rejection_rows;
use pioneer_client::timeline::labels::{
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

impl PioneerDesktop {
    pub(super) fn render_item_system_event(
        &self,
        entry: &TimelineEntry,
        item_view: &ItemView,
        item: &TurnItem,
        is_first_row: bool,
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
                        .child(presentation.message.clone()),
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
                                                .child(presentation.label.clone()),
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

        self.render_item_row(is_first_row, is_last_row, content_width, content)
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
                                .child(row.label)
                                .child(
                                    div()
                                        .rounded_md()
                                        .bg(cx.theme().warning.opacity(0.10))
                                        .px_1p5()
                                        .py_0p5()
                                        .text_xs()
                                        .text_color(cx.theme().warning)
                                        .child(row.kind),
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
                                .child(row.label),
                        )
                        .child(
                            div()
                                .min_w_0()
                                .flex_1()
                                .overflow_hidden()
                                .text_ellipsis()
                                .child(row.value),
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

#[cfg(test)]
mod tests {
    use super::{
        capability_rejection_rows, capability_rejection_rows_for_event,
        execution_window_detail_rows, system_event_presentation,
    };
    use pioneer_protocol::SystemEventLevel;
    use serde_json::json;

    #[test]
    fn capability_rejection_rows_render_skill_and_mcp_diagnostics() {
        rust_i18n::set_locale("en");
        let details = json!({
            "rejected": [
                {
                    "id": "skill:user:docs",
                    "label": "docs",
                    "kind": {
                        "type": "skill",
                        "slug": "docs",
                        "sourceKind": "user"
                    },
                    "message": "Skill `docs` is not installed or not available in this workspace."
                },
                {
                    "id": "mcp-tool:workspace:resend:send",
                    "kind": {
                        "type": "mcpTool",
                        "scopeKind": "workspace",
                        "serverName": "resend",
                        "rawToolName": "send"
                    },
                    "message": "MCP server `resend` does not expose tool `send`."
                }
            ]
        });

        let rows = capability_rejection_rows_for_event(Some("capability.rejected"), Some(&details));

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].label, "docs");
        assert_eq!(rows[0].kind, "Skill");
        assert_eq!(
            rows[0].message,
            "Skill `docs` is not installed or not available in this workspace."
        );
        assert_eq!(rows[1].label, "resend/send");
        assert_eq!(rows[1].kind, "MCP tool");
        assert_eq!(
            rows[1].message,
            "MCP server `resend` does not expose tool `send`."
        );
    }

    #[test]
    fn capability_rejection_rows_ignore_other_system_events_and_malformed_entries() {
        rust_i18n::set_locale("en");
        let details = json!({
            "rejected": [
                {
                    "kind": {
                        "type": "mcpServer",
                        "name": "github"
                    },
                    "message": "MCP server `github` is disabled."
                },
                {
                    "kind": {
                        "type": "skill",
                        "slug": "missing-message"
                    }
                }
            ]
        });

        assert!(
            capability_rejection_rows_for_event(Some("other.event"), Some(&details)).is_empty()
        );

        let rows = capability_rejection_rows(Some(&details));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].label, "github");
        assert_eq!(rows[0].kind, "MCP server");
        assert_eq!(rows[0].message, "MCP server `github` is disabled.");
    }

    #[test]
    fn execution_window_rows_render_compact_continuation_details() {
        let details = json!({
            "window_index": 2,
            "status": "continued",
            "previous_window_index": 1,
            "checkpoint_id": "chk_000000000000000001",
            "payload": {
                "large": "payload must not be rendered inline"
            }
        });

        let presentation = system_event_presentation(
            &SystemEventLevel::Info,
            "Continued in execution window #2 after window #1 limit",
            Some("turn_execution_window_continued"),
            Some(&details),
        );
        assert_eq!(presentation.label, "Continued");
        assert!(
            presentation
                .message
                .contains("Continued in execution window #2")
        );

        let rows =
            execution_window_detail_rows(Some("turn_execution_window_continued"), Some(&details));
        assert!(
            rows.iter()
                .any(|row| row.label == "Previous window" && row.value == "Window #1")
        );
        assert!(
            rows.iter()
                .any(|row| row.label == "Checkpoint" && row.value == "chk_000000000000000001")
        );
        assert!(
            !rows
                .iter()
                .any(|row| row.label == "payload" || row.value.contains("large"))
        );
    }

    #[test]
    fn execution_window_rows_render_blocked_reason_and_checkpoint_reference() {
        let details = json!({
            "window_index": 3,
            "status": "blocked",
            "exhaustion_reason": "max_agent_rounds_per_window",
            "checkpoint_id": "chk_000000000000000003",
            "total_windows": 3,
            "total_tool_calls": 384,
            "reason": "max_total_windows_exceeded"
        });

        let presentation = system_event_presentation(
            &SystemEventLevel::Warning,
            "Execution paused: max_total_windows_exceeded",
            Some("turn_execution_window_blocked"),
            Some(&details),
        );
        assert_eq!(presentation.label, "Paused");
        assert_eq!(
            presentation.message,
            "Execution paused: max_total_windows_exceeded"
        );

        let rows =
            execution_window_detail_rows(Some("turn_execution_window_blocked"), Some(&details));
        assert!(
            rows.iter()
                .any(|row| row.label == "Reason" && row.value == "max_total_windows_exceeded")
        );
        assert!(
            rows.iter().any(|row| row.label == "Window exhaustion"
                && row.value == "max_agent_rounds_per_window")
        );
        assert!(
            rows.iter()
                .any(|row| row.label == "Checkpoint" && row.value == "chk_000000000000000003")
        );
        assert!(
            rows.iter()
                .any(|row| row.label == "Total windows" && row.value == "3")
        );
        assert!(
            rows.iter()
                .any(|row| row.label == "Tool calls" && row.value == "384")
        );
    }
}
