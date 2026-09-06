//! Row-owned semantic content prepared when a timeline revision is materialized.
use super::labels;
use crate::conversation::{ItemView, TimelineEntryStatus};
use pioneer_protocol::{MarkdownDocument, TurnItem};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TimelineItemKind {
    UserMessage,
    AgentMessage,
    Reasoning,
    SystemEvent,
    Task,
    CommandExecution,
    FileChange,
    WebSearch,
    WebFetch,
    Download,
    DynamicToolCall,
    Unknown,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TimelineCommandContent {
    pub command: String,
    pub output: TimelineTextPreview,
    pub terminal_output: TimelineTextPreview,
    pub duration_ms: Option<f64>,
    pub exit_code: Option<f64>,
    pub timed_out: Option<bool>,
    pub truncated: Option<bool>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TimelineToolContent {
    pub detail: String,
    pub url: Option<String>,
    pub host: Option<String>,
    pub result_count: Option<u64>,
    pub bytes: Option<u64>,
    pub arguments_text: Option<TimelineTextPreview>,
    pub result_text: Option<TimelineTextPreview>,
    pub mcp: Option<labels::McpTimelineMetadata>,
    pub task_review: Option<labels::TaskWaitReviewDisplay>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TimelineItemPresentation {
    pub kind: TimelineItemKind,
    pub text: String,
    pub markdown: Option<MarkdownDocument>,
    pub streaming: bool,
    pub collapsed: bool,
    pub task_timeline: bool,
    pub timestamp: Option<i64>,
    pub edited_timestamp: Option<i64>,
    pub command: Option<TimelineCommandContent>,
    pub file_output: Option<TimelineTextPreview>,
    pub final_status: Option<labels::TimelineFinalStatus>,
    pub tool: Option<TimelineToolContent>,
    pub system_label: Option<labels::SystemEventLabel>,
    pub attachments: Vec<TimelineAttachment>,
    pub capability_rejections: Vec<TimelineCapabilityRejection>,
}

pub(crate) fn project_item(item: &ItemView) -> TimelineItemPresentation {
    use TimelineItemKind as K;
    let partial = item.partial_text.as_str();
    let final_text = item.final_text.as_deref().unwrap_or_default();
    let fallback = if final_text.is_empty() {
        partial
    } else {
        final_text
    };
    let streaming = item.status == TimelineEntryStatus::Running;
    let kind = match &item.item {
        TurnItem::UserMessage { .. } => K::UserMessage,
        TurnItem::AgentMessage { .. } => K::AgentMessage,
        TurnItem::Reasoning { .. } => K::Reasoning,
        TurnItem::SystemEvent { .. } => K::SystemEvent,
        TurnItem::Task { .. } => K::Task,
        TurnItem::CommandExecution { .. } => K::CommandExecution,
        TurnItem::FileChange { .. } => K::FileChange,
        TurnItem::WebSearch { .. } => K::WebSearch,
        TurnItem::WebFetch { .. } => K::WebFetch,
        TurnItem::Download { .. } => K::Download,
        TurnItem::DynamicToolCall { .. } => K::DynamicToolCall,
    };
    let raw = serde_json::to_value(&item.item).expect("turn item serializes");
    let raw_text = raw["text"].as_str().unwrap_or_default();
    let text = match kind {
        K::AgentMessage if streaming => first(&[partial, raw_text, fallback]),
        K::AgentMessage => first(&[raw_text, final_text, partial]),
        K::UserMessage => first(&[raw_text, fallback]),
        K::SystemEvent => first(&[raw["message"].as_str().unwrap_or_default(), fallback]),
        _ => fallback,
    }
    .to_owned();
    let mut result = TimelineItemPresentation {
        kind,
        text,
        markdown: item
            .final_markdown
            .clone()
            .or(item.partial_markdown.clone())
            .or_else(|| match &item.item {
                TurnItem::AgentMessage { markdown, .. } => markdown.clone(),
                _ => None,
            }),
        streaming,
        collapsed: item.status == TimelineEntryStatus::Completed,
        task_timeline: labels::is_task_timeline_agent_message(item),
        timestamp: item
            .started_at_unix_ms
            .or(item.updated_at_unix_ms)
            .or(item.completed_at_unix_ms),
        edited_timestamp: item
            .updated_at_unix_ms
            .or(item.started_at_unix_ms)
            .or(item.completed_at_unix_ms),
        command: None,
        file_output: None,
        final_status: None,
        tool: None,
        system_label: None,
        attachments: match &item.item {
            TurnItem::UserMessage { attachments, .. } => {
                attachments.iter().map(project_attachment).collect()
            }
            _ => Vec::new(),
        },
        capability_rejections: Vec::new(),
    };
    match &item.item {
        TurnItem::CommandExecution {
            command, arguments, ..
        } => {
            let command =
                labels::command_execution_display_command(command, arguments).unwrap_or_default();
            let shell = if raw["display"]["kind"] == "shell" {
                &raw["display"]
            } else if raw["storage"]["kind"] == "shell" {
                &raw["storage"]
            } else {
                &Value::Null
            };
            let output = shell["aggregated_output"]
                .as_str()
                .or(shell["stdout"].as_str())
                .or(shell["stderr"].as_str())
                .unwrap_or_default();
            let normalized = normalize_output(output);
            let output = if normalized.is_empty() {
                TimelineTextPreview {
                    text: partial.to_owned(),
                    truncated: false,
                }
            } else {
                truncate(&normalized, 24_000)
            };
            let terminal_output = truncate(
                &normalize_output(if normalized.is_empty() {
                    partial
                } else {
                    &normalized
                }),
                24_000,
            );
            result.command = Some(TimelineCommandContent {
                command,
                output,
                terminal_output,
                duration_ms: shell["duration_ms"].as_f64(),
                exit_code: shell["exit_code"].as_f64(),
                timed_out: shell["timed_out"].as_bool(),
                truncated: shell["truncated"].as_bool(),
            });
        }
        TurnItem::FileChange {
            success, exit_code, ..
        } => {
            result.final_status = Some(labels::final_file_change_status(
                item.status,
                *success,
                *exit_code,
            ));
            result.file_output = Some(truncate(
                &normalize_output(first(&[
                    raw["stdout"].as_str().unwrap_or_default(),
                    raw["stderr"].as_str().unwrap_or_default(),
                    partial,
                ])),
                4000,
            ));
        }
        TurnItem::SystemEvent {
            level,
            message,
            code,
            details,
            ..
        } => {
            result.capability_rejections =
                project_capability_rejections(code.as_deref(), details.as_ref());
            result.system_label = Some(
                labels::system_event_presentation(
                    level,
                    message,
                    code.as_deref(),
                    details.as_ref(),
                )
                .label,
            );
        }
        TurnItem::WebSearch { .. }
        | TurnItem::WebFetch { .. }
        | TurnItem::Download { .. }
        | TurnItem::DynamicToolCall { .. } => {
            let fallback = first(&[partial, fallback]);
            let raw_kind = raw["type"].as_str().unwrap_or_default();
            let is_http = matches!(raw_kind, "webFetch" | "download");
            let url = is_http
                .then(|| {
                    first(&[
                        raw["finalUrl"].as_str().unwrap_or_default(),
                        raw["url"].as_str().unwrap_or_default(),
                        raw["arguments"]["url"].as_str().unwrap_or_default(),
                        fallback,
                    ])
                })
                .filter(|s| !s.is_empty())
                .map(str::to_owned);
            let detail = match raw_kind {
                "webSearch" => first(&[
                    raw["query"].as_str().unwrap_or_default(),
                    raw["arguments"]["query"].as_str().unwrap_or_default(),
                    raw["arguments"]["q"].as_str().unwrap_or_default(),
                    fallback,
                ]),
                "webFetch" => first(&[
                    raw["title"].as_str().unwrap_or_default(),
                    raw["resolvedMode"].as_str().unwrap_or_default(),
                    fallback,
                    url.as_deref().unwrap_or_default(),
                ]),
                "download" => first(&[
                    raw["path"].as_str().unwrap_or_default(),
                    fallback,
                    url.as_deref().unwrap_or_default(),
                ]),
                _ => fallback,
            }
            .to_owned();
            let host = url.as_deref().map(|url| {
                url::Url::parse(url)
                    .or_else(|_| url::Url::parse(&format!("https://{url}")))
                    .ok()
                    .and_then(|u| {
                        u.host_str().map(|host| {
                            if let Some(port) = u.port() {
                                format!("{host}:{port}")
                            } else {
                                host.to_owned()
                            }
                        })
                    })
                    .unwrap_or_else(|| url.to_owned())
            });
            let status_kind = if item.status == TimelineEntryStatus::Cancelled {
                labels::TimelineFinalStatusKind::Cancelled
            } else if matches!(
                item.status,
                TimelineEntryStatus::Blocked | TimelineEntryStatus::Failed
            ) || raw["status"] == "failed"
            {
                labels::TimelineFinalStatusKind::Failed
            } else if streaming || raw["status"] == "in_progress" {
                labels::TimelineFinalStatusKind::Running
            } else if raw["success"] == false
                || (is_http && raw["statusCode"].as_u64().is_some_and(|code| code >= 400))
            {
                labels::TimelineFinalStatusKind::Failed
            } else {
                labels::TimelineFinalStatusKind::Completed
            };
            result.final_status = Some(labels::TimelineFinalStatus::new(
                status_kind,
                matches!(
                    status_kind,
                    labels::TimelineFinalStatusKind::Running
                        | labels::TimelineFinalStatusKind::Completed
                ),
            ));
            let (arguments_text, result_text, mcp, task_review) = match &item.item {
                TurnItem::DynamicToolCall {
                    arguments,
                    display,
                    tool_name,
                    ..
                } => {
                    let arguments_text =
                        (!arguments.is_null() && arguments != &serde_json::json!({})).then(|| {
                            truncate(
                                &serde_json::to_string_pretty(arguments)
                                    .expect("arguments serialize"),
                                2000,
                            )
                        });
                    let result_text = crate::conversation::display::tool_display_text(display)
                        .filter(|text| !text.is_empty())
                        .map(|text| truncate(&text, 4000));
                    (
                        arguments_text,
                        result_text,
                        labels::mcp_timeline_metadata(display),
                        labels::task_wait_review_display(tool_name, display),
                    )
                }
                _ => (None, None, None, None),
            };
            result.tool = Some(TimelineToolContent {
                detail,
                url,
                host,
                result_count: match raw_kind {
                    "webSearch" => Some(raw["resultCount"].as_u64().unwrap_or_else(|| {
                        raw["results"]
                            .as_array()
                            .map_or(0, |rows| rows.len() as u64)
                    })),
                    "webFetch" => raw["wordCount"].as_u64(),
                    _ => None,
                },
                bytes: match raw_kind {
                    "webFetch" => raw["bytesReceived"].as_u64(),
                    "download" => raw["bytesWritten"].as_u64(),
                    _ => None,
                },
                arguments_text,
                result_text,
                mcp,
                task_review,
            });
        }
        _ => {}
    }
    result
}
fn first<'a>(values: &[&'a str]) -> &'a str {
    values
        .iter()
        .find(|s| !s.is_empty())
        .copied()
        .unwrap_or_default()
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TimelineTextPreview {
    pub text: String,
    pub truncated: bool,
}
fn truncate(text: &str, limit: usize) -> TimelineTextPreview {
    let truncated = text.encode_utf16().count() > limit;
    TimelineTextPreview {
        text: if truncated {
            String::from_utf16_lossy(&text.encode_utf16().take(limit).collect::<Vec<_>>())
        } else {
            text.to_owned()
        },
        truncated,
    }
}

fn normalize_output(text: &str) -> String {
    text.replace("\r\n", "\n")
        .replace('\r', "\n")
        .replace('\t', "    ")
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimelineAttachmentKind {
    Artifact,
    File,
    Image,
    Audio,
    Video,
    Skill,
    Mcp,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TimelineAttachment {
    pub id: String,
    pub kind: TimelineAttachmentKind,
    pub title: String,
    pub parent_title: Option<String>,
    pub artifact: Option<pioneer_protocol::ArtifactRef>,
}

pub(crate) fn project_attachment(
    attachment: &pioneer_protocol::UserMessageAttachment,
) -> TimelineAttachment {
    use TimelineAttachmentKind as K;
    use pioneer_protocol::UserMessageAttachment as A;
    let (id, kind, title, parent_title, artifact) = match attachment {
        A::Artifact { artifact } => (
            format!(
                "artifact:{}:{}",
                artifact.artifact_id,
                artifact.version_id.as_deref().unwrap_or("latest")
            ),
            K::Artifact,
            artifact.display_name.clone(),
            None,
            Some(artifact.clone()),
        ),
        A::Skill { capability } => (
            format!("skill:{}", capability.skill_id),
            K::Skill,
            capability.label.clone(),
            capability.pack.as_ref().map(|pack| pack.label.clone()),
            None,
        ),
        A::SkillPack { capability } => (
            format!("skill-pack:{}", capability.pack_id),
            K::Skill,
            capability.label.clone(),
            None,
            None,
        ),
        A::McpServer { capability } => (
            capability.id.clone(),
            K::Mcp,
            capability.label.clone(),
            None,
            None,
        ),
        A::McpTool { capability } => (
            capability.id.clone(),
            K::Mcp,
            capability.label.clone(),
            None,
            None,
        ),
        A::Image { url } | A::LocalImage { path: url } => {
            return source_attachment(url, K::Image, "image");
        }
        A::Audio { url } | A::LocalAudio { path: url } => {
            return source_attachment(url, K::Audio, "audio");
        }
        A::Video { url } | A::LocalVideo { path: url } => {
            return source_attachment(url, K::Video, "video");
        }
        A::File { url } | A::LocalFile { path: url } => {
            return source_attachment(url, K::File, "file");
        }
    };
    TimelineAttachment {
        id,
        kind,
        title,
        parent_title,
        artifact,
    }
}
fn source_attachment(
    source: &str,
    kind: TimelineAttachmentKind,
    prefix: &str,
) -> TimelineAttachment {
    let path = if source.contains("://") || source.starts_with("data:") {
        source
            .split('?')
            .next()
            .unwrap_or(source)
            .split('#')
            .next()
            .unwrap_or(source)
    } else {
        source
    };
    TimelineAttachment {
        id: format!("{prefix}:{source}"),
        kind,
        title: path
            .split('/')
            .rfind(|part| !part.is_empty())
            .unwrap_or(source)
            .to_owned(),
        parent_title: None,
        artifact: None,
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TimelineCapabilityKind {
    Skill,
    McpServer,
    McpTool,
    Unknown,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TimelineCapabilityRejection {
    pub id: Option<String>,
    pub label: Option<String>,
    pub kind: TimelineCapabilityKind,
    pub name: Option<String>,
    pub message: String,
}
fn project_capability_rejections(
    code: Option<&str>,
    details: Option<&Value>,
) -> Vec<TimelineCapabilityRejection> {
    if code != Some("capability.rejected") {
        return Vec::new();
    }
    details
        .and_then(|value| value["rejected"].as_array())
        .into_iter()
        .flatten()
        .filter_map(|record| {
            let message = record["message"].as_str()?.trim();
            if message.is_empty() {
                return None;
            }
            let value = &record["kind"];
            let (kind, name) = match value["type"].as_str() {
                Some("skill") => (
                    TimelineCapabilityKind::Skill,
                    Some(value["slug"].as_str().unwrap_or("skill").to_owned()),
                ),
                Some("mcpServer") => (
                    TimelineCapabilityKind::McpServer,
                    value["name"].as_str().map(str::to_owned),
                ),
                Some("mcpTool") => (
                    TimelineCapabilityKind::McpTool,
                    Some(format!(
                        "{}/{}",
                        value["serverName"].as_str().unwrap_or("MCP"),
                        value["rawToolName"].as_str().unwrap_or("tool")
                    )),
                ),
                _ => (TimelineCapabilityKind::Unknown, None),
            };
            Some(TimelineCapabilityRejection {
                id: record["id"].as_str().map(str::to_owned),
                label: record["label"]
                    .as_str()
                    .map(str::trim)
                    .filter(|label| !label.is_empty())
                    .map(str::to_owned),
                kind,
                name,
                message: message.to_owned(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn attachments_and_rejections_are_semantic_and_keep_existing_fallbacks() {
        use pioneer_protocol::UserMessageAttachment as A;
        let attachments = [
            A::Image {
                url: "https://example.test/a.png?token=synthetic#preview".into(),
            },
            A::LocalImage {
                path: "/synthetic/a.png".into(),
            },
            A::Audio {
                url: "https://example.test/a.wav".into(),
            },
            A::LocalAudio {
                path: "/synthetic/a.wav".into(),
            },
            A::Video {
                url: "https://example.test/a.mp4".into(),
            },
            A::LocalVideo {
                path: "/synthetic/a.mp4".into(),
            },
            A::File {
                url: "https://example.test/a.txt".into(),
            },
            A::LocalFile {
                path: "/synthetic/a.txt".into(),
            },
        ];
        let output = attachments
            .iter()
            .map(project_attachment)
            .collect::<Vec<_>>();
        assert_eq!(
            output.iter().map(|a| a.title.as_str()).collect::<Vec<_>>(),
            [
                "a.png", "a.png", "a.wav", "a.wav", "a.mp4", "a.mp4", "a.txt", "a.txt"
            ]
        );
        assert_eq!(
            output[0].id,
            "image:https://example.test/a.png?token=synthetic#preview"
        );
        let details = json!({"rejected":[
            {"id":"stable", "label":" Label ", "kind":{"type":"skill","slug":"search"}, "message":" rejected "},
            {"kind":{"type":"mcpServer"}, "message":"offline"},
            {"kind":{"type":"mcpTool","serverName":"server","rawToolName":"query"}, "message":"denied"},
            {"message":"unknown"}, {"message":" "}, null
        ]});
        let rows = project_capability_rejections(Some("capability.rejected"), Some(&details));
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].id.as_deref(), Some("stable"));
        assert_eq!(rows[0].label.as_deref(), Some("Label"));
        assert_eq!(rows[0].message, "rejected");
        assert_eq!(rows[1].name, None);
        assert_eq!(rows[2].name.as_deref(), Some("server/query"));
        assert!(matches!(rows[3].kind, TimelineCapabilityKind::Unknown));
        assert!(project_capability_rejections(Some("other"), Some(&details)).is_empty());
    }

    #[test]
    fn previews_keep_terminal_normalization_and_a_separate_truncation_token() {
        assert_eq!(normalize_output("a\r\nb\rc\td"), "a\nb\nc    d");
        assert_eq!(
            truncate("a😀b", 3),
            TimelineTextPreview {
                text: "a😀".into(),
                truncated: true
            }
        );
        assert_eq!(
            truncate("a😀b", 4),
            TimelineTextPreview {
                text: "a😀b".into(),
                truncated: false
            }
        );
    }
}
