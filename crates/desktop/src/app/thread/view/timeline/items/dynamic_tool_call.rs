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
    button::{Button, ButtonVariants},
    collapsible::Collapsible,
    h_flex,
    spinner::Spinner,
    v_flex, *,
};
use pioneer_protocol::{ToolDisplayPayload, ToolMetadataValue, TurnItem};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

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
        let (tool_name, arguments, display_text, success, mcp_metadata) = match item {
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
            ),
            _ => (
                "tool".to_owned(),
                None,
                Some(Self::timeline_entry_text(item_view).to_owned()),
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
    use super::mcp_timeline_metadata;
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
}
