use super::*;

const APPLY_PATCH_ADD_FILE: &str = "*** Add File: ";
const REGISTRATION_MAX_FILE_BYTES: u64 = 50 * 1024 * 1024;

impl MessageProcessor {
    pub(super) async fn register_artifacts_for_completed_item(
        &self,
        notification: &pioneer_protocol::ItemCompletedNotification,
    ) {
        let candidates = artifact_registration_candidates_from_completed_item(&notification.item);
        if candidates.is_empty() {
            return;
        }

        let allowed_root = match std::env::current_dir() {
            Ok(path) => path,
            Err(error) => {
                warn!(
                    thread_id = notification.thread_id,
                    turn_id = notification.turn_id,
                    item_id = notification.item.item_id(),
                    error = %error,
                    "failed to resolve artifact registration workspace root"
                );
                return;
            }
        };

        let mut artifact_ids = Vec::new();
        for candidate in candidates {
            let context = ArtifactRegistrationContext {
                workspace_id: notification.workspace_id.clone(),
                thread_id: notification.thread_id.clone(),
                turn_id: notification.turn_id.clone(),
                message_id: None,
                turn_item_id: Some(notification.item.item_id().to_owned()),
                tool_call_id: Some(notification.item.item_id().to_owned()),
                created_by_kind: ArtifactCreatedByKind::Tool,
                created_by_actor_id: item_tool_name(&notification.item).map(str::to_owned),
                item_index: None,
                binding_kind: ArtifactBindingKind::ToolOutput,
                binding_direction: ArtifactBindingDirection::Output,
                binding_role: Some(ArtifactRole::Tool),
                allowed_roots: vec![allowed_root.clone()],
                max_file_bytes: Some(REGISTRATION_MAX_FILE_BYTES),
                cleanup_source_after_success: false,
            };

            match self
                .artifact_service
                .register_candidate(context, candidate)
                .await
            {
                Ok(summary) => {
                    artifact_ids.push(summary.artifact.artifact_id.clone());
                    self.send_notification_to_thread_subscribers(
                        notification.thread_id.as_str(),
                        events::ARTIFACT_CREATED,
                        &ArtifactCreatedNotification {
                            workspace_id: notification.workspace_id.clone(),
                            artifact: summary,
                        },
                    )
                    .await;
                }
                Err(error) => {
                    warn!(
                        thread_id = notification.thread_id,
                        turn_id = notification.turn_id,
                        item_id = notification.item.item_id(),
                        error = %error,
                        "failed to register tool output artifact"
                    );
                }
            }
        }

        if !artifact_ids.is_empty() {
            self.send_thread_artifacts_changed_to_thread_and_ancestors(
                notification.workspace_id.as_str(),
                notification.thread_id.as_str(),
                artifact_ids,
                "tool_output_registration",
                now_timestamp_secs(),
            )
            .await;
        }
    }
}

fn artifact_registration_candidates_from_completed_item(
    item: &TurnItem,
) -> Vec<ArtifactRegistrationCandidate> {
    match item {
        TurnItem::Download {
            status,
            success,
            path,
            bytes_written,
            sha256,
            content_type,
            truncated,
            ..
        } if *status == ToolCallStatus::Completed
            && *success == Some(true)
            && *truncated != Some(true) =>
        {
            let Some(path) = path.as_deref().filter(|value| !value.trim().is_empty()) else {
                return Vec::new();
            };
            vec![ArtifactRegistrationCandidate {
                path: PathBuf::from(path),
                display_name: display_name_from_path(path),
                mime_type: content_type.clone(),
                kind_hint: None,
                description: None,
                sha256: sha256.clone(),
                size_bytes: *bytes_written,
                source: ArtifactRegistrationSource::DownloadTool,
            }]
        }
        TurnItem::FileChange {
            tool_name,
            arguments,
            status,
            success,
            ..
        } if tool_name == "apply_patch"
            && *status == ToolCallStatus::Completed
            && *success == Some(true) =>
        {
            extract_apply_patch_add_file_paths(arguments)
                .into_iter()
                .map(|path| ArtifactRegistrationCandidate {
                    display_name: display_name_from_path(path.as_str()),
                    path: PathBuf::from(path),
                    mime_type: None,
                    kind_hint: Some(ArtifactKind::WorkspaceFile),
                    description: None,
                    sha256: None,
                    size_bytes: None,
                    source: ArtifactRegistrationSource::ApplyPatchAddFile,
                })
                .collect()
        }
        TurnItem::DynamicToolCall {
            tool_name,
            status,
            success,
            storage,
            ..
        } if *status == ToolCallStatus::Completed && *success == Some(true) => {
            dynamic_tool_registration_candidate(tool_name.as_str(), storage)
                .into_iter()
                .collect()
        }
        _ => Vec::new(),
    }
}

fn dynamic_tool_registration_candidate(
    tool_name: &str,
    storage: &ToolStoragePayload,
) -> Option<ArtifactRegistrationCandidate> {
    let metadata = storage_metadata_json(storage)?;
    if tool_name == "computer_use" {
        let path = metadata
            .get("snapshot")
            .and_then(|value| value.get("path"))
            .and_then(JsonValue::as_str)
            .or_else(|| {
                metadata
                    .get("llm_context")
                    .and_then(|value| value.get("attachment"))
                    .and_then(|value| value.get("path"))
                    .and_then(JsonValue::as_str)
            })
            .or_else(|| metadata.get("path").and_then(JsonValue::as_str))?;
        return Some(ArtifactRegistrationCandidate {
            path: PathBuf::from(path),
            display_name: display_name_from_path(path),
            mime_type: Some("image/png".to_owned()),
            kind_hint: Some(ArtifactKind::Screenshot),
            description: None,
            sha256: None,
            size_bytes: None,
            source: ArtifactRegistrationSource::ComputerUseSnapshot,
        });
    }

    let path = metadata.get("path").and_then(JsonValue::as_str)?;
    let content_type = metadata
        .get("contentType")
        .and_then(JsonValue::as_str)
        .map(str::to_owned);
    if tool_name.contains("image")
        || content_type
            .as_deref()
            .is_some_and(|value| value.starts_with("image/"))
    {
        return Some(ArtifactRegistrationCandidate {
            path: PathBuf::from(path),
            display_name: display_name_from_path(path),
            mime_type: content_type,
            kind_hint: Some(ArtifactKind::GeneratedImage),
            description: None,
            sha256: metadata
                .get("sha256")
                .and_then(JsonValue::as_str)
                .map(str::to_owned),
            size_bytes: metadata.get("bytesWritten").and_then(JsonValue::as_u64),
            source: ArtifactRegistrationSource::GeneratedImage,
        });
    }

    None
}

fn storage_metadata_json(storage: &ToolStoragePayload) -> Option<JsonValue> {
    match storage {
        ToolStoragePayload::Metadata { metadata } => Some(metadata.to_json()),
        ToolStoragePayload::Summary(summary) => Some(summary.metadata.to_json()),
        ToolStoragePayload::Shell { .. } | ToolStoragePayload::None => None,
    }
}

fn extract_apply_patch_add_file_paths(arguments: &JsonValue) -> Vec<String> {
    let Some(patch) = arguments
        .get("input")
        .or_else(|| arguments.get("patch"))
        .and_then(JsonValue::as_str)
        .or_else(|| arguments.as_str())
    else {
        return Vec::new();
    };

    patch
        .lines()
        .filter_map(|line| {
            line.strip_prefix(APPLY_PATCH_ADD_FILE)
                .map(str::trim)
                .filter(|path| !path.is_empty())
                .map(str::to_owned)
        })
        .collect()
}

fn item_tool_name(item: &TurnItem) -> Option<&str> {
    match item {
        TurnItem::CommandExecution { tool_name, .. }
        | TurnItem::FileChange { tool_name, .. }
        | TurnItem::WebSearch { tool_name, .. }
        | TurnItem::WebFetch { tool_name, .. }
        | TurnItem::Download { tool_name, .. }
        | TurnItem::DynamicToolCall { tool_name, .. } => Some(tool_name.as_str()),
        TurnItem::UserMessage { .. }
        | TurnItem::AgentMessage { .. }
        | TurnItem::Reasoning { .. }
        | TurnItem::SystemEvent { .. }
        | TurnItem::Task { .. } => None,
    }
}

fn display_name_from_path(path: &str) -> Option<String> {
    PathBuf::from(path)
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        DeltaOutputPolicy, LlmOutputPolicy, LlmRetentionPolicy, RecoveryAction,
        RecoveryOutputPolicy, StorageOutputPolicy, TimelineOutputPolicy, ToolMetadata,
        ToolOutputPolicySnapshot, ToolRecoveryIdempotencyMode, ToolRecoveryPolicySnapshot,
        ToolRecoveryRetryClass,
    };
    use serde_json::json;

    #[test]
    fn artifact_registration_download_url_success_produces_candidate() {
        let item = TurnItem::Download {
            id: "call_download".to_owned(),
            tool_name: "download_url".to_owned(),
            arguments: json!({"url": "https://example.com/report.txt"}),
            status: ToolCallStatus::Completed,
            recovery_policy: None,
            output_policy: output_policy(),
            display: Default::default(),
            storage: Default::default(),
            recovery: None,
            url: Some("https://example.com/report.txt".to_owned()),
            final_url: Some("https://example.com/report.txt".to_owned()),
            status_code: Some(200),
            path: Some("/workspace/report.txt".to_owned()),
            bytes_written: Some(12),
            sha256: Some("abc".to_owned()),
            content_type: Some("text/plain".to_owned()),
            elapsed_ms: Some(1),
            truncated: Some(false),
            success: Some(true),
            outcome: None,
            observation: None,
        };

        let candidates = artifact_registration_candidates_from_completed_item(&item);

        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].source,
            ArtifactRegistrationSource::DownloadTool
        );
        assert_eq!(candidates[0].path, PathBuf::from("/workspace/report.txt"));
        assert_eq!(candidates[0].sha256.as_deref(), Some("abc"));
        assert_eq!(candidates[0].size_bytes, Some(12));
    }

    #[test]
    fn artifact_registration_failed_or_truncated_download_does_not_capture() {
        let mut item = download_item(false, false);
        assert!(artifact_registration_candidates_from_completed_item(&item).is_empty());

        item = download_item(true, true);
        assert!(artifact_registration_candidates_from_completed_item(&item).is_empty());
    }

    #[test]
    fn artifact_registration_apply_patch_add_file_only() {
        let item = TurnItem::FileChange {
            id: "call_patch".to_owned(),
            tool_name: "apply_patch".to_owned(),
            arguments: json!({
                "patch": "*** Begin Patch\n*** Add File: created.txt\n+hi\n*** Update File: updated.txt\n@@\n-old\n+new\n*** End Patch"
            }),
            status: ToolCallStatus::Completed,
            recovery_policy: None,
            output_policy: output_policy(),
            display: Default::default(),
            storage: Default::default(),
            recovery: None,
            changed_files: vec!["created.txt".to_owned(), "updated.txt".to_owned()],
            exit_code: Some(0),
            stdout: None,
            stderr: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };

        let candidates = artifact_registration_candidates_from_completed_item(&item);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].path, PathBuf::from("created.txt"));
        assert_eq!(candidates[0].kind_hint, Some(ArtifactKind::WorkspaceFile));
        assert_eq!(
            candidates[0].source,
            ArtifactRegistrationSource::ApplyPatchAddFile
        );
    }

    #[test]
    fn artifact_registration_write_file_does_not_create_candidate() {
        let created = write_file_item(
            json!({
                "changedFiles": ["/workspace/docs/created.md"],
                "operation": "created",
                "bytesWritten": 5,
                "sha256": "abc123"
            }),
            vec!["/workspace/docs/created.md".to_owned()],
        );
        assert!(artifact_registration_candidates_from_completed_item(&created).is_empty());

        let overwritten = write_file_item(
            json!({
                "changedFiles": ["/workspace/docs/existing.md"],
                "operation": "overwritten",
                "bytesWritten": 12,
                "sha256": "def456"
            }),
            vec!["/workspace/docs/existing.md".to_owned()],
        );
        assert!(artifact_registration_candidates_from_completed_item(&overwritten).is_empty());
    }

    #[test]
    fn artifact_registration_edit_file_does_not_create_candidate() {
        let item = TurnItem::FileChange {
            id: "call_edit".to_owned(),
            tool_name: "edit_file".to_owned(),
            arguments: json!({
                "path": "/workspace/docs/existing.md",
                "old_string": "old",
                "new_string": "new"
            }),
            status: ToolCallStatus::Completed,
            recovery_policy: None,
            output_policy: output_policy(),
            display: Default::default(),
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(json!({
                    "changedFiles": ["/workspace/docs/existing.md"],
                    "operation": "edited",
                    "matchesReplaced": 1,
                    "replaceAll": false,
                    "bytesBefore": 3,
                    "bytesAfter": 3,
                    "bytesWritten": 3,
                    "sha256Before": "before123",
                    "sha256": "after456",
                    "lineEndingMode": "none"
                })),
            },
            recovery: None,
            changed_files: vec!["/workspace/docs/existing.md".to_owned()],
            exit_code: None,
            stdout: None,
            stderr: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };

        assert!(artifact_registration_candidates_from_completed_item(&item).is_empty());
    }

    #[test]
    fn artifact_registration_computer_use_snapshot_candidate() {
        let item = TurnItem::DynamicToolCall {
            id: "call_cu".to_owned(),
            tool_name: "computer_use".to_owned(),
            arguments: json!({"action": "snapshot"}),
            status: ToolCallStatus::Completed,
            recovery_policy: Some(recovery_policy()),
            output_policy: output_policy(),
            display: Default::default(),
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(json!({
                    "snapshot": {"path": "/workspace/snap.png"}
                })),
            },
            recovery: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };

        let candidates = artifact_registration_candidates_from_completed_item(&item);

        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].source,
            ArtifactRegistrationSource::ComputerUseSnapshot
        );
        assert_eq!(candidates[0].kind_hint, Some(ArtifactKind::Screenshot));
    }

    #[test]
    fn artifact_registration_computer_use_artifact_reads_llm_context_attachment_path() {
        let item = TurnItem::DynamicToolCall {
            id: "call_cu".to_owned(),
            tool_name: "computer_use".to_owned(),
            arguments: json!({"action": "snapshot"}),
            status: ToolCallStatus::Completed,
            recovery_policy: Some(recovery_policy()),
            output_policy: output_policy(),
            display: Default::default(),
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(json!({
                    "action": "snapshot",
                    "session_id": 7,
                    "llm_context": {
                        "attachment": {
                            "path": "/workspace/snap-from-llm-context.png",
                            "mime_type": "image/png"
                        }
                    }
                })),
            },
            recovery: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };

        let candidates = artifact_registration_candidates_from_completed_item(&item);

        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].path,
            PathBuf::from("/workspace/snap-from-llm-context.png")
        );
        assert_eq!(candidates[0].kind_hint, Some(ArtifactKind::Screenshot));
    }

    fn download_item(success: bool, truncated: bool) -> TurnItem {
        TurnItem::Download {
            id: "call_download".to_owned(),
            tool_name: "download_url".to_owned(),
            arguments: json!({}),
            status: ToolCallStatus::Completed,
            recovery_policy: None,
            output_policy: output_policy(),
            display: Default::default(),
            storage: Default::default(),
            recovery: None,
            url: None,
            final_url: None,
            status_code: Some(if success { 200 } else { 500 }),
            path: Some("/workspace/report.txt".to_owned()),
            bytes_written: Some(12),
            sha256: Some("abc".to_owned()),
            content_type: Some("text/plain".to_owned()),
            elapsed_ms: None,
            truncated: Some(truncated),
            success: Some(success),
            outcome: None,
            observation: None,
        }
    }

    fn write_file_item(metadata: JsonValue, changed_files: Vec<String>) -> TurnItem {
        TurnItem::FileChange {
            id: "call_write".to_owned(),
            tool_name: "write_file".to_owned(),
            arguments: json!({"path": "/workspace/docs/created.md"}),
            status: ToolCallStatus::Completed,
            recovery_policy: None,
            output_policy: output_policy(),
            display: Default::default(),
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(metadata),
            },
            recovery: None,
            changed_files,
            exit_code: None,
            stdout: None,
            stderr: None,
            success: Some(true),
            outcome: None,
            observation: None,
        }
    }

    fn output_policy() -> ToolOutputPolicySnapshot {
        ToolOutputPolicySnapshot {
            llm: LlmOutputPolicy::SummaryOnly,
            llm_retention: LlmRetentionPolicy::DoNotRetain,
            timeline: TimelineOutputPolicy::Summary { max_chars: 128 },
            storage: StorageOutputPolicy::MetadataOnly,
            recovery: RecoveryOutputPolicy::MetadataOnly,
            deltas: DeltaOutputPolicy::Disabled,
        }
    }

    fn recovery_policy() -> ToolRecoveryPolicySnapshot {
        ToolRecoveryPolicySnapshot {
            retry_class: ToolRecoveryRetryClass::Never,
            idempotency_mode: ToolRecoveryIdempotencyMode::None,
            max_attempts: 0,
            can_resume: false,
            resolved_action: RecoveryAction::MarkFailed,
            base_backoff_secs: 0,
            max_wall_clock_secs: 0,
            no_progress_limit: 0,
        }
    }
}
