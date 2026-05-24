use crate::app::{
    conversation::{ItemView, TimelineEntry},
    root::PioneerDesktop,
};
use gpui::{prelude::*, *};
use gpui_component::{collapsible::Collapsible, h_flex, v_flex, *};
use pioneer_protocol::{SystemEventLevel, TurnItem};
use serde_json::Value as JsonValue;
use std::hash::{Hash, Hasher};

struct SystemEventPresentation {
    message: String,
    label: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CapabilityRejectionRow {
    label: String,
    kind: String,
    message: String,
}

fn system_event_icon(level: &SystemEventLevel) -> IconName {
    match level {
        SystemEventLevel::Info => IconName::Info,
        SystemEventLevel::Warning | SystemEventLevel::Error => IconName::TriangleAlert,
    }
}

fn system_event_label(level: &SystemEventLevel) -> String {
    match level {
        SystemEventLevel::Info => t!("timeline.system.info").to_string(),
        SystemEventLevel::Warning => t!("timeline.system.warning").to_string(),
        SystemEventLevel::Error => t!("timeline.system.error").to_string(),
    }
}

fn pretty_details(details: &JsonValue) -> Option<String> {
    let text = serde_json::to_string_pretty(details).unwrap_or_else(|_| details.to_string());
    (!text.trim().is_empty() && text.trim() != "null").then_some(text)
}

fn capability_rejection_kind(kind: &JsonValue) -> String {
    match kind.get("type").and_then(JsonValue::as_str) {
        Some("skill") => t!("timeline.system.capability_kind_skill").to_string(),
        Some("mcpServer") => t!("timeline.system.capability_kind_mcp_server").to_string(),
        Some("mcpTool") => t!("timeline.system.capability_kind_mcp_tool").to_string(),
        _ => t!("timeline.system.capability_kind_capability").to_string(),
    }
}

fn fallback_capability_label(kind: &JsonValue) -> String {
    match kind.get("type").and_then(JsonValue::as_str) {
        Some("skill") => kind
            .get("slug")
            .and_then(JsonValue::as_str)
            .unwrap_or("skill")
            .to_owned(),
        Some("mcpServer") => kind
            .get("name")
            .and_then(JsonValue::as_str)
            .unwrap_or("MCP server")
            .to_owned(),
        Some("mcpTool") => {
            let server = kind
                .get("serverName")
                .and_then(JsonValue::as_str)
                .unwrap_or("MCP");
            let tool = kind
                .get("rawToolName")
                .and_then(JsonValue::as_str)
                .unwrap_or("tool");
            format!("{server}/{tool}")
        }
        _ => t!("timeline.system.capability_kind_capability").to_string(),
    }
}

fn capability_rejection_rows(details: Option<&JsonValue>) -> Vec<CapabilityRejectionRow> {
    details
        .and_then(|details| details.get("rejected"))
        .and_then(JsonValue::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let kind = item.get("kind")?;
                    let label = item
                        .get("label")
                        .and_then(JsonValue::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(str::to_owned)
                        .unwrap_or_else(|| fallback_capability_label(kind));
                    let message = item
                        .get("message")
                        .and_then(JsonValue::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())?
                        .to_owned();
                    Some(CapabilityRejectionRow {
                        label,
                        kind: capability_rejection_kind(kind),
                        message,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn capability_rejection_rows_for_event(
    code: Option<&str>,
    details: Option<&JsonValue>,
) -> Vec<CapabilityRejectionRow> {
    if code == Some("capability.rejected") {
        capability_rejection_rows(details)
    } else {
        Vec::new()
    }
}

fn detail_string(details: Option<&JsonValue>, key: &str) -> Option<String> {
    details?
        .get(key)
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn detail_u64(details: Option<&JsonValue>, key: &str) -> Option<u64> {
    details?.get(key).and_then(JsonValue::as_u64)
}

fn tool_name_from_details(details: Option<&JsonValue>) -> String {
    detail_string(details, "tool_name").unwrap_or_else(|| t!("timeline.system.tool").to_string())
}

fn attempt_label(details: Option<&JsonValue>) -> Option<String> {
    detail_u64(details, "attempt_no")
        .map(|attempt| t!("timeline.system.attempt", attempt = attempt).to_string())
}

fn next_attempt_label(details: Option<&JsonValue>) -> Option<String> {
    detail_u64(details, "next_attempt_no")
        .map(|attempt| t!("timeline.system.attempt", attempt = attempt).to_string())
}

fn detail_has_string(details: Option<&JsonValue>, key: &str) -> bool {
    detail_string(details, key).is_some()
}

fn is_recovery_failure_message(message: &str) -> bool {
    message.starts_with("recovery failed for item `")
}

fn system_event_presentation(
    level: &SystemEventLevel,
    message: &str,
    code: Option<&str>,
    details: Option<&JsonValue>,
) -> SystemEventPresentation {
    match code {
        Some("item_timeout_detected") => {
            let recovery_started = detail_has_string(details, "recovery_job_id");
            SystemEventPresentation {
                message: if recovery_started {
                    t!("timeline.system.timeout_with_recovery").to_string()
                } else {
                    t!("timeline.system.timeout_without_recovery").to_string()
                },
                label: attempt_label(details)
                    .unwrap_or_else(|| t!("timeline.system.timeout_label").to_string()),
            }
        }
        Some("item_recovery_opened") => SystemEventPresentation {
            message: t!("timeline.system.recovery_opened").to_string(),
            label: attempt_label(details)
                .unwrap_or_else(|| t!("timeline.system.recovery_label").to_string()),
        },
        Some("item_recovery_attached") => SystemEventPresentation {
            message: t!("timeline.system.recovery_attached").to_string(),
            label: next_attempt_label(details)
                .unwrap_or_else(|| t!("timeline.system.recovery_label").to_string()),
        },
        Some("item_retry_scheduled") => SystemEventPresentation {
            message: t!("timeline.system.retry_scheduled").to_string(),
            label: attempt_label(details)
                .unwrap_or_else(|| t!("timeline.system.retry_label").to_string()),
        },
        Some("item_retry_attempt_started") => SystemEventPresentation {
            message: t!("timeline.system.retry_started").to_string(),
            label: attempt_label(details)
                .unwrap_or_else(|| t!("timeline.system.retry_label").to_string()),
        },
        Some("item_recovery_succeeded") => SystemEventPresentation {
            message: t!("timeline.system.recovery_succeeded").to_string(),
            label: t!("timeline.system.recovered_label").to_string(),
        },
        Some("item_recovery_exhausted") => SystemEventPresentation {
            message: t!("timeline.system.recovery_failed").to_string(),
            label: t!("timeline.system.error").to_string(),
        },
        Some("item_tool_retry_scheduled") => {
            let tool_name = tool_name_from_details(details);
            SystemEventPresentation {
                message: t!(
                    "timeline.system.tool_retry_scheduled",
                    tool_name = tool_name
                )
                .to_string(),
                label: attempt_label(details)
                    .unwrap_or_else(|| t!("timeline.system.retry_label").to_string()),
            }
        }
        Some("item_tool_retry_resolved") => {
            let tool_name = tool_name_from_details(details);
            SystemEventPresentation {
                message: t!("timeline.system.tool_retry_resolved", tool_name = tool_name)
                    .to_string(),
                label: t!("timeline.system.retry_resolved_label").to_string(),
            }
        }
        Some("item_tool_retry_exhausted") => {
            let tool_name = tool_name_from_details(details);
            SystemEventPresentation {
                message: t!(
                    "timeline.system.tool_retry_exhausted",
                    tool_name = tool_name
                )
                .to_string(),
                label: t!("timeline.system.retries_exhausted_label").to_string(),
            }
        }
        Some("turn_tool_loop_budget_exceeded") => SystemEventPresentation {
            message: t!("timeline.system.tool_loop_budget_exceeded").to_string(),
            label: system_event_label(level),
        },
        Some("turn_failed") if is_recovery_failure_message(message) => SystemEventPresentation {
            message: t!("timeline.system.recovery_failed").to_string(),
            label: system_event_label(level),
        },
        _ => SystemEventPresentation {
            message: message.to_owned(),
            label: system_event_label(level),
        },
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
    use super::{capability_rejection_rows, capability_rejection_rows_for_event};
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
}
