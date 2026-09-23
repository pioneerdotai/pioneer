// Prepares the compact Pioneer context payload and manifest shared by CLI runtimes.

use anyhow::Result;
use pioneer_cli_agent_runtime::input::{
    CLIRuntimeInputMappingDiagnostic, CLIRuntimeInputMappingDiagnosticLevel,
    CLIRuntimeTurnInputItem, CLIRuntimeTurnInputMapping,
};
use pioneer_promt::{
    CliRuntimeContextInput, CliRuntimeContextText, CliRuntimeSelectedCapabilitiesInput,
    CliRuntimeSelectedServerInput, CliRuntimeSelectedSkillsInput, CompiledInstructionDeliveryPlan,
    PromptDiagnosticCode, PromptProfile,
    compile_cli_runtime_delivery_plan as compile_prompt_cli_runtime_delivery_plan,
};
use pioneer_protocol::{
    PromptManifest, PromptManifestDiagnostic, PromptManifestDiagnosticCode, PromptManifestProfile,
    TurnPermissionProfileSnapshot,
};
use pioneer_provider::{AttachmentDataSource, ChatMessage, MessageContentPart};
use std::collections::BTreeMap;
use std::path::Path;

pub(crate) const MAX_CLI_TURN_INPUT_FRAME_BYTES: usize = 8 * 1024 * 1024;

pub(crate) struct CLIRuntimeContextBuildInput<'a> {
    pub workspace_id: &'a str,
    pub thread_id: &'a str,
    pub initiating_thread_id: &'a str,
    pub turn_id: &'a str,
    pub runtime_id: &'a str,
    pub runtime_label: &'a str,
    pub runtime_kind: pioneer_protocol::CLIAgentRuntimeKind,
    pub model: Option<&'a str>,
    pub cwd: Option<&'a str>,
    pub permission_profile: TurnPermissionProfileSnapshot,
    /// Accepted Pioneer projection used only to bootstrap a new or stale
    /// provider conversation. `None` means the provider session has a durable
    /// continuity receipt and already owns this history.
    pub history: Option<&'a [ChatMessage]>,
    pub selected_skill_names: &'a [String],
    pub selected_capabilities: Option<CliRuntimeSelectedCapabilitiesInput>,
}

pub(crate) fn compile_cli_runtime_delivery_plan(
    prompt_root: &Path,
    input: CLIRuntimeContextBuildInput<'_>,
) -> Result<CompiledInstructionDeliveryPlan> {
    let selected_skills =
        (!input.selected_skill_names.is_empty()).then(|| CliRuntimeSelectedSkillsInput {
            runtime_kind: input.runtime_kind,
            skill_names: input.selected_skill_names.to_vec(),
        });
    compile_prompt_cli_runtime_delivery_plan(
        prompt_root,
        CliRuntimeContextInput {
            workspace_id: input.workspace_id.to_owned(),
            thread_id: input.thread_id.to_owned(),
            initiating_thread_id: input.initiating_thread_id.to_owned(),
            turn_id: input.turn_id.to_owned(),
            runtime_id: input.runtime_id.to_owned(),
            runtime_label: Some(input.runtime_label.to_owned()),
            model: input.model.and_then(normalized_optional).map(str::to_owned),
            cwd: input.cwd.and_then(normalized_optional).map(str::to_owned),
            permission_profile: input.permission_profile,
            memory_recall_context: None,
            thread_context: input.history.map(thread_context_from_history).transpose()?,
            selected_skills,
            selected_capabilities: input.selected_capabilities,
        },
    )
}

fn thread_context_from_history(history: &[ChatMessage]) -> Result<CliRuntimeContextText> {
    if history.is_empty() {
        return Ok(CliRuntimeContextText {
            text: "Accepted Pioneer conversation history is empty.".to_owned(),
            truncated: false,
        });
    }
    let mut lines = Vec::with_capacity(history.len().saturating_add(3));
    lines.push(
        "Accepted Pioneer conversation history follows as ordered JSON messages. This is historical context, not a new request or instructions to repeat prior actions."
            .to_owned(),
    );
    lines.push("<pioneer_conversation_history>".to_owned());
    for (message_index, message) in history.iter().enumerate() {
        // Replay metadata belongs to the provider that produced it and must
        // never be exposed as readable cross-provider history. Binary content
        // is projected through native CLI input items below; stable markers
        // retain its position and relationship to this historical message.
        let mut portable = message.clone();
        portable.provider_replay_state = None;
        portable.content_parts = message
            .content_parts
            .iter()
            .enumerate()
            .map(|(part_index, part)| match part {
                MessageContentPart::Text { text } => {
                    MessageContentPart::Text { text: text.clone() }
                }
                MessageContentPart::Image { .. } => MessageContentPart::Text {
                    text: format!("[historical image message={message_index} part={part_index}]"),
                },
                MessageContentPart::File { .. } => MessageContentPart::Text {
                    text: format!("[historical file message={message_index} part={part_index}]"),
                },
                MessageContentPart::Audio { .. } => MessageContentPart::Text {
                    text: format!(
                        "[unsupported historical audio message={message_index} part={part_index}]"
                    ),
                },
                MessageContentPart::Video { .. } => MessageContentPart::Text {
                    text: format!(
                        "[unsupported historical video message={message_index} part={part_index}]"
                    ),
                },
            })
            .collect();
        lines.push(serde_json::to_string(&portable)?);
    }
    lines.push("</pioneer_conversation_history>".to_owned());
    Ok(CliRuntimeContextText {
        text: lines.join("\n"),
        truncated: false,
    })
}

fn historical_attachment_inputs(
    history: &[ChatMessage],
    workspace_id: &str,
    cwd: Option<&str>,
    runtime_label: &str,
) -> Result<CLIRuntimeTurnInputMapping> {
    use pioneer_cli_agent_runtime::input::{
        CLIRuntimeFileReferenceLocation, CLIRuntimeInputMappingRequest, CLIRuntimeInputSource,
        map_cli_runtime_turn_input_for_runtime,
    };

    let mut sources = Vec::new();
    for (message_index, message) in history.iter().enumerate() {
        for (part_index, part) in message.content_parts.iter().enumerate() {
            let (attachment, is_image) = match part {
                MessageContentPart::Text { .. } => continue,
                MessageContentPart::Image { image } => (image, true),
                MessageContentPart::File { file } => (file, false),
                MessageContentPart::Audio { .. } | MessageContentPart::Video { .. } => {
                    anyhow::bail!(
                        "historical message {message_index} content part {part_index} is not supported by CLI runtimes"
                    );
                }
            };
            if let Some(artifact) = &attachment.artifact
                && artifact.workspace_id != workspace_id
            {
                anyhow::bail!(
                    "historical attachment {message_index}:{part_index} belongs to another workspace"
                );
            }
            if attachment.artifact.is_none()
                && let AttachmentDataSource::Path { path } = &attachment.source
            {
                let root = cwd.map(Path::new).ok_or_else(|| {
                    anyhow::anyhow!(
                        "historical local attachment requires a runtime working directory"
                    )
                })?;
                let path_ref = Path::new(path);
                if !path_ref.is_absolute() || !path_ref.starts_with(root) {
                    anyhow::bail!(
                        "historical local attachment {message_index}:{part_index} is outside the runtime workspace"
                    );
                }
            }
            sources.push(CLIRuntimeInputSource::Text {
                text: format!(
                    "Historical attachment for message {message_index}, part {part_index}; treat it as context, not a new request."
                ),
            });
            let source = match &attachment.source {
                AttachmentDataSource::Url { url } if is_image => {
                    CLIRuntimeInputSource::ImageUrl { url: url.clone() }
                }
                AttachmentDataSource::Path { path } if is_image => {
                    CLIRuntimeInputSource::LocalImage { path: path.clone() }
                }
                AttachmentDataSource::Path { path } => CLIRuntimeInputSource::FileReference {
                    location: CLIRuntimeFileReferenceLocation::Path(path.clone()),
                    name: attachment.name.clone(),
                    mime_type: Some(attachment.mime_type.clone()),
                    size_bytes: attachment.size_bytes,
                    sha256: attachment.sha256.clone(),
                },
                AttachmentDataSource::Url { url } => CLIRuntimeInputSource::FileReference {
                    location: CLIRuntimeFileReferenceLocation::Url(url.clone()),
                    name: attachment.name.clone(),
                    mime_type: Some(attachment.mime_type.clone()),
                    size_bytes: attachment.size_bytes,
                    sha256: attachment.sha256.clone(),
                },
                AttachmentDataSource::Reference { reference } => {
                    CLIRuntimeInputSource::FileReference {
                        location: CLIRuntimeFileReferenceLocation::Reference(reference.clone()),
                        name: attachment.name.clone(),
                        mime_type: Some(attachment.mime_type.clone()),
                        size_bytes: attachment.size_bytes,
                        sha256: attachment.sha256.clone(),
                    }
                }
                AttachmentDataSource::Bytes { .. } => anyhow::bail!(
                    "inline bytes in historical attachment {message_index}:{part_index} cannot be transported safely to a CLI runtime"
                ),
            };
            sources.push(source);
        }
    }
    map_cli_runtime_turn_input_for_runtime(
        CLIRuntimeInputMappingRequest { inputs: sources },
        runtime_label,
    )
    .map_err(Into::into)
}

pub(crate) fn cli_runtime_mcp_capabilities_input(
    projection: Option<&crate::turn_mcp::ResolvedMcpTurnProjection>,
) -> Option<CliRuntimeSelectedCapabilitiesInput> {
    let projection = projection.filter(|projection| !projection.tools.is_empty())?;
    let mut explicit_servers = BTreeMap::<String, usize>::new();
    let mut explicit_tools = Vec::new();
    let mut policy_tools = 0_usize;
    for tool in &projection.tools {
        match tool.selection_reason {
            crate::turn_mcp::McpSelectionReason::ExplicitServer => {
                *explicit_servers
                    .entry(tool.server_name.clone())
                    .or_default() += 1;
            }
            crate::turn_mcp::McpSelectionReason::ExplicitTool => {
                explicit_tools.push(tool.canonical_callable_name.clone());
            }
            crate::turn_mcp::McpSelectionReason::ImplicitPolicy => {
                policy_tools = policy_tools.saturating_add(1);
            }
        }
    }
    explicit_tools.sort();
    explicit_tools.dedup();
    Some(CliRuntimeSelectedCapabilitiesInput {
        total_tool_count: projection.tools.len(),
        explicit_servers: explicit_servers
            .into_iter()
            .map(|(server_name, tool_count)| CliRuntimeSelectedServerInput {
                server_name,
                tool_count,
            })
            .collect(),
        explicit_tool_names: explicit_tools,
        implicit_policy_tool_count: policy_tools,
    })
}

pub(crate) fn prepend_cli_turn_context_input(
    mapping: &mut CLIRuntimeTurnInputMapping,
    plan: &CompiledInstructionDeliveryPlan,
    runtime_label: &str,
) -> bool {
    prepend_cli_turn_context_input_with_diagnostic(
        mapping,
        plan,
        runtime_label,
        "cli_runtime_input.turn_context_mapped",
    )
}

pub(crate) fn prepend_cli_turn_context_and_history_input(
    mapping: &mut CLIRuntimeTurnInputMapping,
    plan: &CompiledInstructionDeliveryPlan,
    history: Option<&[ChatMessage]>,
    workspace_id: &str,
    cwd: Option<&str>,
    runtime_label: &str,
) -> Result<bool> {
    let mut history_mapping = match history {
        Some(history) => historical_attachment_inputs(history, workspace_id, cwd, runtime_label)?,
        None => CLIRuntimeTurnInputMapping {
            input: Vec::new(),
            diagnostics: Vec::new(),
        },
    };
    let current_input = std::mem::take(&mut mapping.input);
    let inserted = prepend_cli_turn_context_input(mapping, plan, runtime_label);
    mapping.input.append(&mut history_mapping.input);
    mapping.input.extend(current_input);
    mapping.diagnostics.append(&mut history_mapping.diagnostics);
    Ok(inserted)
}

pub(crate) fn validate_cli_runtime_turn_input_frame(
    mapping: &CLIRuntimeTurnInputMapping,
    plan: &CompiledInstructionDeliveryPlan,
    max_input_tokens: Option<u64>,
) -> Result<()> {
    // Include the turn envelope fields that accompany input at the adapter
    // boundary. Claude additionally performs an exact post-materialization
    // check after LocalImage paths have become base64 blocks.
    let frame_bytes = serde_json::to_vec(&serde_json::json!({
        "input": &mapping.input,
        "elevatedInstructions": &plan.provider_instructions.text,
        "instructionFingerprint": &plan.provider_instructions.fingerprint,
    }))?
    .len();
    anyhow::ensure!(
        frame_bytes <= MAX_CLI_TURN_INPUT_FRAME_BYTES,
        "accepted CLI bootstrap requires {frame_bytes} bytes, exceeding the {}-byte request frame; compact the canonical conversation before retrying",
        MAX_CLI_TURN_INPUT_FRAME_BYTES
    );
    if let Some(max_input_tokens) = max_input_tokens {
        let mut admitted_text = plan.provider_instructions.text.clone();
        admitted_text.push('\n');
        admitted_text.push_str(std::str::from_utf8(&serde_json::to_vec(&mapping.input)?)?);
        let estimated_tokens = pioneer_compaction::text_tokens(admitted_text.as_str());
        anyhow::ensure!(
            estimated_tokens <= max_input_tokens,
            "accepted CLI bootstrap requires approximately {estimated_tokens} input tokens, exceeding the selected model limit of {max_input_tokens}; compact the canonical conversation before retrying"
        );
    }
    Ok(())
}

fn prepend_cli_turn_context_input_with_diagnostic(
    mapping: &mut CLIRuntimeTurnInputMapping,
    plan: &CompiledInstructionDeliveryPlan,
    runtime_label: &str,
    diagnostic_code: &str,
) -> bool {
    let Some(context_text) = cli_runtime_context_text(plan) else {
        return false;
    };
    let runtime_label = normalized_optional(runtime_label).unwrap_or("CLI runtime");
    mapping
        .input
        .insert(0, CLIRuntimeTurnInputItem::Text { text: context_text });
    mapping.diagnostics.push(CLIRuntimeInputMappingDiagnostic {
        level: CLIRuntimeInputMappingDiagnosticLevel::Info,
        code: diagnostic_code.to_owned(),
        message: format!("Prepended non-governing Pioneer turn context for {runtime_label}."),
        input_index: None,
    });
    true
}

pub(crate) fn cli_runtime_prompt_manifest_from_plan(
    plan: &CompiledInstructionDeliveryPlan,
) -> PromptManifest {
    let bundle = &plan.bundle;
    PromptManifest {
        compiler_version: bundle.compiler_version.to_owned(),
        profile: prompt_manifest_profile(bundle.profile),
        section_ids: bundle
            .sections
            .iter()
            .map(|section| section.id.manifest_id())
            .collect(),
        fingerprint_stable: bundle.fingerprint_stable.clone(),
        fingerprint_dynamic: bundle.fingerprint_dynamic.clone(),
        fingerprint_full: bundle.fingerprint_full.clone(),
        diagnostics: bundle
            .diagnostics
            .iter()
            .map(|diagnostic| PromptManifestDiagnostic {
                code: prompt_diagnostic_code(diagnostic.code),
                message: diagnostic.message.clone(),
                file: diagnostic.file.clone(),
                section_id: diagnostic.section_id.clone(),
                hook_source: None,
            })
            .collect(),
        hook_sources: Vec::new(),
    }
}

fn cli_runtime_context_text(plan: &CompiledInstructionDeliveryPlan) -> Option<String> {
    let text = plan.turn_context.text.trim();
    (!text.is_empty()).then(|| text.to_owned())
}

fn prompt_manifest_profile(profile: PromptProfile) -> PromptManifestProfile {
    match profile {
        PromptProfile::AssistantFull => PromptManifestProfile::AssistantFull,
        PromptProfile::AssistantMinimal => PromptManifestProfile::AssistantMinimal,
        PromptProfile::AssistantNone => PromptManifestProfile::AssistantNone,
        PromptProfile::CliRuntime => PromptManifestProfile::CliRuntime,
    }
}

fn prompt_diagnostic_code(code: PromptDiagnosticCode) -> PromptManifestDiagnosticCode {
    match code {
        PromptDiagnosticCode::MissingFile => PromptManifestDiagnosticCode::MissingFile,
        PromptDiagnosticCode::FileReadError => PromptManifestDiagnosticCode::FileReadError,
        PromptDiagnosticCode::FileTruncated => PromptManifestDiagnosticCode::FileTruncated,
        PromptDiagnosticCode::TotalBudgetTruncated => {
            PromptManifestDiagnosticCode::TotalBudgetTruncated
        }
        PromptDiagnosticCode::FileFilteredByProfile => {
            PromptManifestDiagnosticCode::FileFilteredByProfile
        }
        PromptDiagnosticCode::DynamicSectionTruncated => {
            PromptManifestDiagnosticCode::DynamicSectionTruncated
        }
        PromptDiagnosticCode::DynamicSectionOmitted => {
            PromptManifestDiagnosticCode::DynamicSectionOmitted
        }
    }
}

fn normalized_optional(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::{
        CLIRuntimeContextBuildInput, cli_runtime_mcp_capabilities_input,
        cli_runtime_prompt_manifest_from_plan, compile_cli_runtime_delivery_plan,
        prepend_cli_turn_context_and_history_input, prepend_cli_turn_context_input,
        validate_cli_runtime_turn_input_frame,
    };
    use pioneer_cli_agent_runtime::input::{CLIRuntimeTurnInputItem, CLIRuntimeTurnInputMapping};
    use pioneer_protocol::{CLIAgentRuntimeKind, PromptManifestProfile};
    use pioneer_provider::{
        AttachmentDataSource, ChatMessage, MessageAttachment, MessageContentPart,
        ProviderReplayState, ProviderToolCall,
    };

    fn mcp_projection() -> crate::turn_mcp::ResolvedMcpTurnProjection {
        let mut projection =
            crate::turn_mcp::ResolvedMcpTurnProjection::empty("workspace_1", "turn_1");
        for (raw_tool_name, selection_reason) in [
            (
                "send-email",
                crate::turn_mcp::McpSelectionReason::ExplicitTool,
            ),
            (
                "list-domains",
                crate::turn_mcp::McpSelectionReason::ExplicitServer,
            ),
        ] {
            projection.tools.push(crate::turn_mcp::ResolvedMcpTurnTool {
                canonical_callable_name: String::new(),
                workspace_id: "workspace_1".to_owned(),
                server_installation_id: "resend-installation".to_owned(),
                server_name: "resend".to_owned(),
                raw_tool_name: raw_tool_name.to_owned(),
                description: Some(
                    "UNTRUSTED DESCRIPTION MUST NOT ENTER CONTROL CONTEXT".to_owned(),
                ),
                input_schema: serde_json::json!({"type": "object"}),
                annotations: None,
                timeout_ms: 20_000,
                catalog_version: "catalog-1".to_owned(),
                installation_fingerprint: "installation-fingerprint".to_owned(),
                schema_fingerprint: String::new(),
                runtime_generation: 1,
                selection_reason,
                capability_id: Some("capability".to_owned()),
            });
        }
        projection
            .finalize_identity(crate::turn_mcp::McpProjectionLimits::default())
            .expect("finalize projection");
        projection
    }

    fn temp_workspace(name: &str) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!(
            "pioneer_gateway_cli_runtime_context_{name}_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create temp workspace");
        root
    }

    #[test]
    fn cli_runtime_prompt_manifest_uses_runtime_profile_without_api_sections() {
        let root = temp_workspace("manifest");
        std::fs::write(root.join("SOUL.md"), "api prompt file").expect("write SOUL");
        let long_accepted_answer = format!("{}ACCEPTED_HISTORY_TAIL", "x".repeat(40_000));
        let accepted_history = [
            ChatMessage::user("accepted parent question"),
            ChatMessage::assistant(long_accepted_answer),
            ChatMessage::assistant_tool_calls(
                None::<String>,
                vec![ProviderToolCall {
                    id: "historical-call".to_owned(),
                    name: "read_file".to_owned(),
                    arguments: r#"{"path":"notes.txt"}"#.to_owned(),
                }],
            ),
            ChatMessage::tool_result("historical-call", "read_file", "HISTORICAL_TOOL_ROUND_TAIL"),
        ];
        let selected_skills = ["review".to_owned()];
        let plan = compile_cli_runtime_delivery_plan(
            root.as_path(),
            CLIRuntimeContextBuildInput {
                workspace_id: "workspace_1",
                thread_id: "thread_1",
                initiating_thread_id: "thread_1",
                turn_id: "turn_1",
                runtime_id: "codex-default",
                runtime_label: "Codex CLI",
                runtime_kind: CLIAgentRuntimeKind::Codex,
                model: Some("gpt-5-codex"),
                cwd: Some("/workspace"),
                permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
                history: Some(&accepted_history),
                selected_skill_names: &selected_skills,
                selected_capabilities: None,
            },
        )
        .expect("compile plan");

        let manifest = cli_runtime_prompt_manifest_from_plan(&plan);
        assert_eq!(manifest.profile, PromptManifestProfile::CliRuntime);
        assert!(
            manifest
                .section_ids
                .contains(&"pioneer_cli_runtime_context".to_owned())
        );
        assert!(manifest.section_ids.contains(&"thread_context".to_owned()));
        let user = plan
            .turn_context
            .text
            .find("\"role\":\"user\"")
            .expect("accepted user role should be preserved");
        let assistant = plan
            .turn_context
            .text
            .find("\"role\":\"assistant\"")
            .expect("accepted assistant role should be preserved");
        assert!(user < assistant);
        assert!(plan.turn_context.text.contains("ACCEPTED_HISTORY_TAIL"));
        let tool_call = plan.turn_context.text.find("historical-call").unwrap();
        let tool_result = plan
            .turn_context
            .text
            .find("HISTORICAL_TOOL_ROUND_TAIL")
            .unwrap();
        assert!(
            tool_call < tool_result,
            "the complete tool round must survive atomically"
        );
        assert!(
            !manifest
                .section_ids
                .contains(&"current_permissions".to_owned()),
            "the default full-access profile has no extra permission guidance"
        );
        assert!(plan.provider_instructions.text.contains("Selected Skills"));
        assert!(plan.provider_instructions.text.contains("review"));
        assert!(
            !plan
                .bundle
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.section_id.as_deref() == Some("thread_context")),
            "accepted history must not be silently truncated by a second prompt budget"
        );
        assert!(!plan.bundle.full_system_text.contains("Tool Usage"));
        assert!(!plan.bundle.full_system_text.contains("api prompt file"));
    }

    #[test]
    fn historical_multimodal_content_uses_native_cli_items_without_replay_state() {
        let root = temp_workspace("multimodal");
        let materialized = tempfile::tempdir().expect("materialized artifact root");
        let image_path = materialized.path().join("history.png");
        let history = [ChatMessage {
            provider_replay_state: Some(ProviderReplayState::new(
                "native-provider",
                serde_json::json!({"opaqueSecret":"must-not-cross"}),
            )),
            content_parts: vec![
                MessageContentPart::Text {
                    text: "look at the historical image".to_owned(),
                },
                MessageContentPart::Image {
                    image: MessageAttachment {
                        mime_type: "image/png".to_owned(),
                        name: Some("history.png".to_owned()),
                        size_bytes: Some(42),
                        sha256: None,
                        source: AttachmentDataSource::Path {
                            path: image_path.display().to_string(),
                        },
                        artifact: Some(pioneer_provider::AttachmentArtifactContext {
                            workspace_id: "workspace_1".to_owned(),
                            artifact_id: "artifact_history_image".to_owned(),
                            artifact_version_id: Some("version_history_image".to_owned()),
                        }),
                    },
                },
                MessageContentPart::File {
                    file: MessageAttachment::from_url(
                        "https://example.test/history.txt",
                        "text/plain",
                    ),
                },
            ],
            ..ChatMessage::user("")
        }];
        let plan = compile_cli_runtime_delivery_plan(
            root.as_path(),
            CLIRuntimeContextBuildInput {
                workspace_id: "workspace_1",
                thread_id: "thread_1",
                initiating_thread_id: "thread_1",
                turn_id: "turn_1",
                runtime_id: "codex-default",
                runtime_label: "Codex CLI",
                runtime_kind: CLIAgentRuntimeKind::Codex,
                model: Some("gpt-5-codex"),
                cwd: Some(root.to_str().unwrap()),
                permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
                history: Some(&history),
                selected_skill_names: &[],
                selected_capabilities: None,
            },
        )
        .unwrap();
        let mut mapping = CLIRuntimeTurnInputMapping {
            input: vec![CLIRuntimeTurnInputItem::Text {
                text: "current question".to_owned(),
            }],
            diagnostics: Vec::new(),
        };
        prepend_cli_turn_context_and_history_input(
            &mut mapping,
            &plan,
            Some(&history),
            "workspace_1",
            Some(root.to_str().unwrap()),
            "Codex CLI",
        )
        .unwrap();
        let serialized = serde_json::to_string(&mapping.input).unwrap();
        assert!(serialized.contains("historical image message=0 part=1"));
        assert!(serialized.contains("history.png"));
        assert!(serialized.contains("https://example.test/history.txt"));
        assert!(serialized.contains("current question"));
        assert!(!serialized.contains("opaqueSecret"));
        assert!(matches!(
            &mapping.input[2],
            CLIRuntimeTurnInputItem::LocalImage { path }
                if path == image_path.to_string_lossy().as_ref()
        ));
    }

    #[test]
    fn oversized_atomic_bootstrap_is_rejected_before_adapter_send() {
        let mapping = CLIRuntimeTurnInputMapping {
            input: vec![CLIRuntimeTurnInputItem::Text {
                text: "x".repeat(32_000),
            }],
            diagnostics: Vec::new(),
        };
        let root = temp_workspace("oversized");
        let plan = compile_cli_runtime_delivery_plan(
            root.as_path(),
            CLIRuntimeContextBuildInput {
                workspace_id: "workspace_1",
                thread_id: "thread_1",
                initiating_thread_id: "thread_1",
                turn_id: "turn_1",
                runtime_id: "codex-default",
                runtime_label: "Codex CLI",
                runtime_kind: CLIAgentRuntimeKind::Codex,
                model: Some("tiny-model"),
                cwd: Some(root.to_str().unwrap()),
                permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
                history: None,
                selected_skill_names: &[],
                selected_capabilities: None,
            },
        )
        .unwrap();
        let error = validate_cli_runtime_turn_input_frame(&mapping, &plan, Some(128)).unwrap_err();
        let error = format!("{error:#}");
        assert!(error.contains("selected model limit of 128"));
        assert!(error.contains("compact the canonical conversation"));
    }

    #[test]
    fn unsupported_historical_media_is_rejected_instead_of_becoming_text() {
        let root = temp_workspace("historical-audio");
        let history = [ChatMessage::user_parts(vec![MessageContentPart::audio(
            MessageAttachment::from_url("https://example.test/history.wav", "audio/wav"),
        )])];
        let plan = compile_cli_runtime_delivery_plan(
            root.as_path(),
            CLIRuntimeContextBuildInput {
                workspace_id: "workspace_1",
                thread_id: "thread_1",
                initiating_thread_id: "thread_1",
                turn_id: "turn_1",
                runtime_id: "codex-default",
                runtime_label: "Codex CLI",
                runtime_kind: CLIAgentRuntimeKind::Codex,
                model: Some("gpt-5-codex"),
                cwd: Some(root.to_str().unwrap()),
                permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
                history: Some(&history),
                selected_skill_names: &[],
                selected_capabilities: None,
            },
        )
        .unwrap();
        let mut mapping = CLIRuntimeTurnInputMapping {
            input: vec![CLIRuntimeTurnInputItem::Text {
                text: "current question".to_owned(),
            }],
            diagnostics: Vec::new(),
        };
        let error = prepend_cli_turn_context_and_history_input(
            &mut mapping,
            &plan,
            Some(&history),
            "workspace_1",
            Some(root.to_str().unwrap()),
            "Codex CLI",
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("not supported by CLI runtimes"));
    }

    #[test]
    fn cli_runtime_context_is_prepended_to_runtime_input_mapping() {
        let root = temp_workspace("input");
        let plan = compile_cli_runtime_delivery_plan(
            root.as_path(),
            CLIRuntimeContextBuildInput {
                workspace_id: "workspace_1",
                thread_id: "thread_1",
                initiating_thread_id: "thread_1",
                turn_id: "turn_1",
                runtime_id: "claude-default",
                runtime_label: "Claude CLI",
                runtime_kind: CLIAgentRuntimeKind::Claude,
                model: None,
                cwd: None,
                permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
                history: None,
                selected_skill_names: &[],
                selected_capabilities: None,
            },
        )
        .expect("compile bundle");
        let mut mapping = CLIRuntimeTurnInputMapping {
            input: vec![CLIRuntimeTurnInputItem::Text {
                text: "user request".to_owned(),
            }],
            diagnostics: Vec::new(),
        };

        assert!(prepend_cli_turn_context_input(
            &mut mapping,
            &plan,
            "Claude CLI"
        ));
        let CLIRuntimeTurnInputItem::Text { text } = &mapping.input[0] else {
            panic!("prepended Pioneer turn context should be text input");
        };
        assert!(text.contains("Pioneer Context"));
        assert!(text.contains("Claude CLI"));
        assert!(!text.contains("Codex CLI"));
        assert_eq!(mapping.input.len(), 2);
        assert!(
            mapping
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "cli_runtime_input.turn_context_mapped")
        );
    }

    #[test]
    fn nested_child_bootstrap_preserves_each_accepted_level_before_current_input() {
        for depth in 1..=3 {
            let root = temp_workspace(format!("nested-{depth}").as_str());
            let mut history = vec![ChatMessage::user("accepted immediate-parent basis")];
            for level in 1..=depth {
                history.push(ChatMessage::assistant(format!(
                    "accepted child output level {level}"
                )));
            }
            let plan = compile_cli_runtime_delivery_plan(
                root.as_path(),
                CLIRuntimeContextBuildInput {
                    workspace_id: "workspace_nested",
                    thread_id: "nested_child",
                    initiating_thread_id: "root_thread",
                    turn_id: "current_nested_turn",
                    runtime_id: "codex-default",
                    runtime_label: "Codex CLI",
                    runtime_kind: CLIAgentRuntimeKind::Codex,
                    model: Some("gpt-5-codex"),
                    cwd: Some("/workspace"),
                    permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(
                    ),
                    history: Some(history.as_slice()),
                    selected_skill_names: &[],
                    selected_capabilities: None,
                },
            )
            .expect("nested accepted history should compile");
            let mut mapping = CLIRuntimeTurnInputMapping {
                input: vec![CLIRuntimeTurnInputItem::Text {
                    text: "CURRENT NESTED QUESTION".to_owned(),
                }],
                diagnostics: Vec::new(),
            };
            assert!(prepend_cli_turn_context_input(
                &mut mapping,
                &plan,
                "Codex CLI"
            ));
            let provider_input = serde_json::to_string(&mapping.input).unwrap();
            let parent = provider_input
                .find("accepted immediate-parent basis")
                .unwrap();
            let mut preceding = parent;
            for level in 1..=depth {
                let current = provider_input
                    .find(format!("accepted child output level {level}").as_str())
                    .unwrap();
                assert!(preceding < current);
                preceding = current;
            }
            let current = provider_input.find("CURRENT NESTED QUESTION").unwrap();
            assert!(preceding < current);
            assert!(!provider_input.contains("LATE PARENT APPEND"));
            assert!(!provider_input.contains("UNAUTHORIZED SIBLING"));
        }
    }

    #[test]
    fn cli_runtime_context_build_input_carries_restricted_permissions() {
        let root = temp_workspace("permissions");
        let plan = compile_cli_runtime_delivery_plan(
            root.as_path(),
            CLIRuntimeContextBuildInput {
                workspace_id: "workspace_1",
                thread_id: "thread_1",
                initiating_thread_id: "thread_1",
                turn_id: "turn_1",
                runtime_id: "codex-default",
                runtime_label: "Codex CLI",
                runtime_kind: CLIAgentRuntimeKind::Codex,
                model: Some("gpt-5-codex"),
                cwd: Some("/workspace"),
                permission_profile: pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                    pioneer_protocol::TurnPermissionMode::Supervised,
                    pioneer_protocol::TurnPermissionProfileSource::Composer,
                ),
                history: None,
                selected_skill_names: &[],
                selected_capabilities: None,
            },
        )
        .expect("compile bundle");

        let manifest = cli_runtime_prompt_manifest_from_plan(&plan);
        assert!(
            manifest
                .section_ids
                .contains(&"current_permissions".to_owned())
        );
        assert!(
            plan.provider_instructions
                .text
                .contains("## Current Permissions")
        );
        assert!(
            plan.provider_instructions
                .text
                .contains("- mode: supervised")
        );
        assert!(!plan.turn_context.text.contains("Current Permissions"));
    }

    #[test]
    fn selected_mcp_context_requires_provider_neutral_tool_discovery() {
        let projection = mcp_projection();
        let selected =
            cli_runtime_mcp_capabilities_input(Some(&projection)).expect("selected MCP input");

        let root = temp_workspace("selected_mcp");
        let plan = compile_cli_runtime_delivery_plan(
            root.as_path(),
            CLIRuntimeContextBuildInput {
                workspace_id: "workspace_1",
                thread_id: "thread_1",
                initiating_thread_id: "thread_1",
                turn_id: "turn_1",
                runtime_id: "codex-default",
                runtime_label: "Codex CLI",
                runtime_kind: CLIAgentRuntimeKind::Codex,
                model: Some("gpt-5-codex"),
                cwd: Some("/workspace"),
                permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
                history: None,
                selected_skill_names: &[],
                selected_capabilities: Some(selected),
            },
        )
        .expect("compile selected MCP context");
        let manifest = cli_runtime_prompt_manifest_from_plan(&plan);
        assert!(
            manifest
                .section_ids
                .contains(&"selected_capabilities".to_owned())
        );
        assert!(
            plan.provider_instructions
                .text
                .contains("Before answering, determine whether the user's request")
        );
        assert!(
            plan.provider_instructions
                .text
                .contains("2 executable MCP tool(s)")
        );
        assert!(
            plan.provider_instructions
                .text
                .contains("through the runtime's available tool-discovery mechanism")
        );
        assert!(
            plan.provider_instructions
                .text
                .contains("the user does not need to mention an MCP server, tool name")
        );
        assert!(!plan.provider_instructions.text.contains("tool_search"));
        assert!(!plan.provider_instructions.text.contains("ALL_TOOLS"));
        assert!(
            plan.provider_instructions
                .text
                .contains("resend: 1 tool(s)")
        );
        assert!(
            plan.provider_instructions
                .text
                .contains("mcp_resend_send_email")
        );
        assert!(
            !plan
                .provider_instructions
                .text
                .contains("UNTRUSTED DESCRIPTION"),
            "MCP-controlled descriptions must not enter governing context"
        );
        assert!(!plan.turn_context.text.contains("tool_search"));
    }
}
