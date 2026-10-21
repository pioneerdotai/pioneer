//! Standard content preparation after existing artifact/skill authorization.
//! Source IO and exact-byte checks remain in Pioneer's attachment pipeline.

use anyhow::{Context, Result, bail, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use pioneer_agent::ResolvedArtifactInput;
use pioneer_cli_agent_runtime::{
    acp::schema,
    input::{CLIRuntimeTurnInputItem as Input, CLIRuntimeTurnInputMapping},
};
use pioneer_protocol::{TurnExecutionSecuritySnapshot, UserInput};
use pioneer_provider::{
    AttachmentDataSource, InputContentType, InputTypeSupport, MessageAttachment,
    ProviderCapabilities, ProviderInputCapabilities,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

pub(crate) async fn append_skills(
    mapping: &mut CLIRuntimeTurnInputMapping,
    skills: &[pioneer_skills::ResolvedSkill],
    capabilities: &schema::AgentCapabilities,
) -> Result<()> {
    use sha2::{Digest, Sha256};
    let skills = skills.to_vec();
    let packages = tokio::task::spawn_blocking(move || -> Result<Vec<_>> {
        let mut packages = Vec::with_capacity(skills.len());
        let mut remaining = 256 * 1024usize;
        for skill in skills {
            let files = pioneer_skills::snapshot_skill_package(
                std::path::Path::new(&skill.definition.identity.skill_dir),
                256,
                remaining,
            )?;
            let source = files
                .iter()
                .find(|file| file.relative_path == "SKILL.md")
                .context("resolved package has no SKILL.md")?;
            let source = std::str::from_utf8(&source.bytes)?.replace("\r\n", "\n");
            ensure!(
                hex::encode(Sha256::digest(source.as_bytes()))
                    == skill.definition.identity.fingerprint,
                "selected skill changed after resolution"
            );
            remaining = remaining
                .checked_sub(files.iter().map(|file| file.bytes.len()).sum())
                .context("selected skills exceed transfer budget")?;
            packages.push((skill, files));
        }
        Ok(packages)
    })
    .await??;
    for (skill, files) in packages {
        let mut digest = Sha256::new();
        for file in &files {
            digest.update((file.relative_path.len() as u64).to_be_bytes());
            digest.update(file.relative_path.as_bytes());
            digest.update((file.bytes.len() as u64).to_be_bytes());
            digest.update(&file.bytes);
        }
        let package_hash = hex::encode(digest.finalize());
        let root = format!("pioneer-skill:{}/{package_hash}/", skill.skill_id);
        mapping.input.push(Input::Text { text: format!("Selected skill: {}\nReference: skill:{}\nVersion: {}\nDefinition SHA-256: {}\nPackage SHA-256: {}\nSupporting resource root: {}\nThese are ordinary user context and authorized package bytes. Resolve supporting resources under this resource URI; local execution remains subject to Pioneer callbacks and permissions.\n\n{}", skill.definition.identity.display_name, skill.skill_id, skill.definition.identity.version_hint.as_deref().unwrap_or("unspecified"), skill.definition.identity.fingerprint, package_hash, root, skill.definition.instructions.body) });
        for file in files
            .into_iter()
            .filter(|file| file.relative_path != "SKILL.md")
        {
            let hash = hex::encode(Sha256::digest(&file.bytes));
            let resource = match String::from_utf8(file.bytes.clone()) {
                Ok(text) if capabilities.prompt_capabilities.embedded_context => {
                    json!({"uri":format!("{root}{}",file.relative_path),"mimeType":"text/plain","text":text})
                }
                Ok(text) => {
                    mapping.input.push(Input::Text {
                        text: format!(
                            "Supporting file: {}{}\nSHA-256: {}\n\n{}",
                            root, file.relative_path, hash, text
                        ),
                    });
                    continue;
                }
                Err(_) => {
                    ensure!(
                        capabilities.prompt_capabilities.embedded_context,
                        "agent cannot receive binary skill resources"
                    );
                    json!({"uri":format!("{root}{}",file.relative_path),"mimeType":"application/octet-stream","blob":STANDARD.encode(&file.bytes)})
                }
            };
            let content = json!({"type":"resource","resource":resource});
            let _: schema::ContentBlock = serde_json::from_value(content.clone())?;
            mapping.input.push(Input::AcpContent { content });
        }
    }
    Ok(())
}

pub(crate) fn map_input(
    input: &[UserInput],
    artifacts: &[ResolvedArtifactInput],
) -> Result<CLIRuntimeTurnInputMapping> {
    let mut mapped = Vec::with_capacity(input.len());
    for item in input {
        let (kind, attachment, uri) = match item {
            UserInput::Text { text, .. } => {
                mapped.push(Input::Text { text: text.clone() });
                continue;
            }
            UserInput::Artifact {
                artifact_id,
                version_id,
            } => {
                let resolved = artifacts
                    .iter()
                    .find(|resolved| {
                        resolved.artifact_id == *artifact_id
                            && version_id
                                .as_ref()
                                .is_none_or(|version| resolved.version_id.as_ref() == Some(version))
                    })
                    .context("ACP input has no exact authorized artifact version")?;
                let version = resolved
                    .version_id
                    .as_ref()
                    .context("ACP artifact version was not frozen by preparation")?;
                (
                    resolved.content_type,
                    resolved.attachment.clone(),
                    format!("pioneer-artifact:{artifact_id}:{version}"),
                )
            }
            UserInput::Image { url } => (
                InputContentType::Image,
                url_attachment(url, InputContentType::Image)?,
                String::new(),
            ),
            UserInput::Audio { url } => (
                InputContentType::Audio,
                url_attachment(url, InputContentType::Audio)?,
                String::new(),
            ),
            UserInput::Video { url } => (
                InputContentType::Video,
                url_attachment(url, InputContentType::Video)?,
                String::new(),
            ),
            UserInput::File { url } => (
                InputContentType::File,
                url_attachment(url, InputContentType::File)?,
                String::new(),
            ),
            UserInput::LocalImage { path } => (
                InputContentType::Image,
                path_attachment(path, InputContentType::Image),
                String::new(),
            ),
            UserInput::LocalAudio { path } => (
                InputContentType::Audio,
                path_attachment(path, InputContentType::Audio),
                String::new(),
            ),
            UserInput::LocalVideo { path } => (
                InputContentType::Video,
                path_attachment(path, InputContentType::Video),
                String::new(),
            ),
            UserInput::LocalFile { path } => (
                InputContentType::File,
                path_attachment(path, InputContentType::File),
                String::new(),
            ),
            UserInput::Mention { .. } => bail!("ACP input contains an unresolved mention"),
        };
        mapped.push(Input::AcpAttachment {
            content_type: serde_json::to_value(kind)?
                .as_str()
                .context("media kind is not a string")?
                .to_owned(),
            attachment: serde_json::to_value(attachment)?,
            uri,
        });
    }
    Ok(CLIRuntimeTurnInputMapping {
        input: mapped,
        diagnostics: Vec::new(),
    })
}

fn path_attachment(path: &str, kind: InputContentType) -> MessageAttachment {
    MessageAttachment::from_path(
        path,
        pioneer_provider::attachments::infer_mime_from_reference(path, kind),
    )
}

fn url_attachment(url: &str, kind: InputContentType) -> Result<MessageAttachment> {
    if let Some(data) = url.strip_prefix("data:") {
        let (header, data) = data
            .split_once(',')
            .context("malformed inline attachment")?;
        let mime = header
            .strip_suffix(";base64")
            .context("inline attachment must use base64 encoding")?;
        return Ok(MessageAttachment {
            mime_type: mime.to_owned(),
            name: None,
            size_bytes: None,
            sha256: None,
            source: AttachmentDataSource::Bytes {
                base64_data: data.to_owned(),
            },
            artifact: None,
        });
    }
    Ok(MessageAttachment::from_url(
        url,
        pioneer_provider::attachments::infer_mime_from_reference(url, kind),
    ))
}

/// Replace all preparatory sources with owned standard blocks before writing
/// the immutable sent-input mapping. No private artifact/skill path is exposed
/// as an agent-readable URI merely because it exists on the Gateway host.
pub(crate) async fn materialize(
    mapping: &mut CLIRuntimeTurnInputMapping,
    capabilities: &schema::AgentCapabilities,
    security: &TurnExecutionSecuritySnapshot,
    authority_fingerprint: &str,
    byte_limit: usize,
    cancellation: CancellationToken,
) -> Result<()> {
    let mut config = pioneer_provider::attachments::default_attachment_pipeline_config();
    config.max_bytes_per_attachment = config
        .max_bytes_per_attachment
        .min(byte_limit.saturating_mul(3) / 4);
    config.max_total_bytes_per_request = config.max_total_bytes_per_request.min(byte_limit);
    let support = InputTypeSupport {
        native: true,
        ..Default::default()
    };
    let attachment_capabilities = ProviderCapabilities {
        streaming: true,
        vision: capabilities.prompt_capabilities.image,
        tool_calling: true,
        embeddings: false,
        transcription: false,
        input_types: ProviderInputCapabilities {
            text: true,
            file: support,
            image: support,
            audio: support,
            video: support,
        },
    };
    let count = mapping
        .input
        .iter()
        .filter(|input| matches!(input, Input::AcpAttachment { .. }))
        .count();
    ensure!(
        count <= config.max_attachments_per_request,
        "ACP input exceeds the attachment count budget"
    );
    let mut total_bytes = 0usize;
    for input in &mut mapping.input {
        let Input::AcpAttachment {
            content_type,
            attachment,
            uri,
        } = input
        else {
            continue;
        };
        let kind: InputContentType = serde_json::from_value(Value::String(content_type.clone()))?;
        ensure!(
            kind != InputContentType::Image || capabilities.prompt_capabilities.image,
            "agent did not negotiate image input"
        );
        ensure!(
            kind != InputContentType::Audio || capabilities.prompt_capabilities.audio,
            "agent did not negotiate audio input"
        );
        let mut attachment: MessageAttachment = serde_json::from_value(attachment.clone())?;
        match &attachment.source {
            AttachmentDataSource::Path { path } if attachment.artifact.is_none() => {
                let bytes = pioneer_tools::handlers::ReadFileHandler
                    .read_bytes(
                        security,
                        std::path::Path::new(path),
                        config.max_bytes_per_attachment.min(
                            config
                                .max_total_bytes_per_request
                                .saturating_sub(total_bytes),
                        ),
                        cancellation.clone(),
                    )
                    .await?;
                attachment.source = AttachmentDataSource::Bytes {
                    base64_data: STANDARD.encode(bytes),
                };
            }
            AttachmentDataSource::Url { url } => pioneer_tools::enforce_network_url(
                Some(security),
                url,
                "ACP attachment preparation",
            )?,
            _ => {}
        }
        let prepared = pioneer_provider::attachments::materialize_acp_attachment(
            authority_fingerprint.to_owned(),
            attachment_capabilities.clone(),
            kind,
            attachment,
            config.clone(),
            total_bytes,
            cancellation.clone(),
        )
        .await?;
        total_bytes = total_bytes
            .checked_add(prepared.size_bytes)
            .context("attachment byte count overflow")?;
        let bytes = pioneer_provider::attachments::attachment_bytes(&prepared)?;
        let uri = if uri.is_empty() {
            format!("pioneer-input:sha256:{}", prepared.sha256)
        } else {
            uri.clone()
        };
        let content = match kind {
            InputContentType::Image => {
                json!({"type":"image", "mimeType":prepared.mime_type, "data":STANDARD.encode(bytes)})
            }
            InputContentType::Audio => {
                json!({"type":"audio", "mimeType":prepared.mime_type, "data":STANDARD.encode(bytes)})
            }
            _ if capabilities.prompt_capabilities.embedded_context => {
                let resource = match std::str::from_utf8(bytes) {
                    Ok(text) => json!({"uri":uri,"mimeType":prepared.mime_type,"text":text}),
                    Err(_) => {
                        json!({"uri":uri,"mimeType":prepared.mime_type,"blob":STANDARD.encode(bytes)})
                    }
                };
                json!({"type":"resource","resource":resource})
            }
            InputContentType::File | InputContentType::Text => {
                let text = std::str::from_utf8(bytes)
                    .context("agent did not negotiate embedded resources for this binary file")?;
                json!({"type":"text", "text":format!("Selected file: {}\nResource: {uri}\nSHA-256: {}\n\n{text}", prepared.name, prepared.sha256)})
            }
            _ => bail!("agent did not negotiate embedded resources for this media input"),
        };
        let _: schema::ContentBlock = serde_json::from_value(content.clone())
            .context("prepared content violates the standard ACP schema")?;
        *input = Input::AcpContent { content };
    }
    ensure!(
        serde_json::to_vec(&mapping.input)?.len() <= byte_limit,
        "prepared ACP input exceeds the request frame budget"
    );
    Ok(())
}

pub(crate) fn prompt_content(input: Value) -> Result<Vec<schema::ContentBlock>> {
    let input: Vec<Input> = serde_json::from_value(input)?;
    input
        .into_iter()
        .map(|item| match item {
            Input::Text { text } => Ok(schema::ContentBlock::Text(schema::TextContent::new(text))),
            Input::AcpContent { content } => {
                serde_json::from_value(content).context("invalid prepared ACP content")
            }
            _ => bail!("ACP prompt contains an unprepared or native input item"),
        })
        .collect()
}

/// A control-selected destination may only continue the exact accepted binding.
/// Load/resume cannot import external history or override a required bootstrap.
pub(crate) fn validate_selected_continuation(
    selected_session: &str,
    binding: Option<&pioneer_crud::CliRuntimeThreadBindingRecord>,
    runtime_id: &str,
    bootstrap_context: bool,
    can_continue: bool,
) -> Result<()> {
    ensure!(
        !bootstrap_context
            && can_continue
            && binding.is_some_and(|binding| binding.runtime_id == runtime_id
                && binding.runtime_kind == "acp"
                && binding.native_thread_id == selected_session),
        "Selected ACP Load/Resume session is incompatible with the accepted Pioneer context or process generation; no prompt was dispatched"
    );
    Ok(())
}

/// One end-to-end budget for the whole descriptor replay, never per choice.
/// Dropping this future drops the pending mutation wait; its session owner must
/// retire that exact transport before it can be reused.
pub(crate) async fn await_configuration_transfer<T>(
    timeout: std::time::Duration,
    cancellation: &CancellationToken,
    transfer: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    let deadline = tokio::time::Instant::now() + timeout;
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => bail!("ACP configuration transfer cancelled; mutation outcome is unknown"),
        _ = tokio::time::sleep_until(deadline) => bail!("ACP configuration transfer timed out; mutation outcome is unknown"),
        result = transfer => result,
    }
}

pub(crate) async fn apply_configuration(
    connection: &pioneer_cli_agent_runtime::acp::Connection,
    session_id: &str,
    selected: &pioneer_protocol::AcpTurnConfiguration,
) -> Result<()> {
    let session = schema::SessionId::new(session_id.to_owned());
    let config = connection
        .configuration(&session)
        .context("ACP configuration unavailable")?;
    let same_configuration = session_id == selected.session_id
        && config.connection_id.to_string() == selected.expected.connection_id
        && config.revision == selected.expected.revision;
    let already_selected = validate_applied_configuration(&config, selected).is_ok();
    if !same_configuration && !already_selected {
        ensure!(
            pioneer_protocol::acp_descriptor_fingerprint(&config.options, config.modes.as_ref())?
                == selected.initial_descriptor_fingerprint,
            "Agent initial configuration changed since preparation"
        );
        // Reproduce actual choices, never sort opaque ids to infer dependency
        // order. Each choice is validated against its original descriptors.
        for choice in &selected.configuration_replay {
            let config = connection
                .configuration(&session)
                .context("ACP configuration unavailable")?;
            ensure!(
                pioneer_protocol::acp_descriptor_fingerprint(
                    &config.options,
                    config.modes.as_ref()
                )? == choice.descriptor_fingerprint,
                "ACP descriptors drifted before a prepared choice"
            );
            connection
                .set_config_option(
                    schema::SetSessionConfigOptionRequest::new(
                        session.clone(),
                        choice.config_id.clone(),
                        choice.value.clone(),
                    ),
                    config.version(),
                )
                .await
                .map_err(|error| anyhow::anyhow!(error.message))?;
        }
        // Transfer any untouched defaults only after the dependency transcript.
        // Descriptor order is supplied by the agent; final exact validation
        // below rejects any changed offers or selections before prompt.
        let current = connection
            .configuration(&session)
            .context("ACP configuration unavailable")?;
        ensure!(
            pioneer_protocol::acp_descriptor_fingerprint(&current.options, current.modes.as_ref())?
                == selected.descriptor_fingerprint,
            "ACP descriptors changed after prepared choices"
        );
        for option in current.options {
            let id = option.id.to_string();
            let value = selected.values.get(&id).context("ACP selection missing")?;
            let current = match option.kind {
                schema::SessionConfigKind::Select(select) => {
                    schema::SessionConfigOptionValue::value_id(select.current_value)
                }
                schema::SessionConfigKind::Boolean(boolean) => {
                    schema::SessionConfigOptionValue::boolean(boolean.current_value)
                }
                _ => bail!("Unsupported ACP option kind"),
            };
            if &current != value {
                let config = connection
                    .configuration(&session)
                    .context("ACP configuration unavailable")?;
                connection
                    .set_config_option(
                        schema::SetSessionConfigOptionRequest::new(
                            session.clone(),
                            id,
                            value.clone(),
                        ),
                        config.version(),
                    )
                    .await
                    .map_err(|error| anyhow::anyhow!(error.message))?;
            }
        }
    }
    if let Some(mode) = &selected.mode {
        let config = connection
            .configuration(&session)
            .context("ACP configuration unavailable")?;
        if config
            .modes
            .as_ref()
            .is_none_or(|modes| modes.current_mode_id.to_string() != *mode)
        {
            connection
                .set_mode(
                    schema::SetSessionModeRequest::new(session.clone(), mode.clone()),
                    config.version(),
                )
                .await
                .map_err(|error| anyhow::anyhow!(error.message))?;
        }
    }
    let applied = connection
        .configuration(&session)
        .context("ACP configuration unavailable after selection")?;
    validate_applied_configuration(&applied, selected)
}

fn validate_applied_configuration(
    applied: &pioneer_cli_agent_runtime::acp::ConfigurationSnapshot,
    selected: &pioneer_protocol::AcpTurnConfiguration,
) -> Result<()> {
    ensure!(
        pioneer_protocol::acp_descriptor_fingerprint(&applied.options, applied.modes.as_ref())?
            == selected.descriptor_fingerprint,
        "ACP descriptors changed during selection"
    );
    ensure!(
        applied.options.len() == selected.values.len(),
        "ACP option set changed"
    );
    for (id, value) in &selected.values {
        let option = applied
            .options
            .iter()
            .find(|option| option.id.to_string() == *id)
            .context("ACP option disappeared")?;
        let current = match &option.kind {
            schema::SessionConfigKind::Select(select) => {
                schema::SessionConfigOptionValue::value_id(select.current_value.clone())
            }
            schema::SessionConfigKind::Boolean(boolean) => {
                schema::SessionConfigOptionValue::boolean(boolean.current_value)
            }
            _ => bail!("Unsupported ACP option kind"),
        };
        ensure!(
            &current == value,
            "ACP agent did not apply the selected value"
        );
    }
    if let Some(mode) = &selected.mode {
        ensure!(
            applied
                .modes
                .as_ref()
                .is_some_and(|modes| modes.current_mode_id.to_string() == *mode),
            "ACP agent did not apply the selected mode"
        );
    }
    Ok(())
}

#[cfg(test)]
mod configuration_tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn transfer_deadline_covers_first_later_option_and_mode_without_prompt_or_retry() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        };
        struct Guard(Arc<AtomicBool>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        // Third position is the legacy mode mutation; all waits share ONE budget.
        for pending_step in 0..3 {
            let calls = Arc::new(AtomicUsize::new(0));
            let released = Arc::new(AtomicBool::new(false));
            let prompt = Arc::new(AtomicBool::new(false));
            let token = CancellationToken::new();
            let result = await_configuration_transfer(
                std::time::Duration::from_millis(100),
                &token,
                async {
                    let _guard = Guard(released.clone());
                    for step in 0..3 {
                        calls.fetch_add(1, Ordering::SeqCst);
                        if step == pending_step {
                            std::future::pending::<()>().await;
                        }
                    }
                    Ok(())
                },
            )
            .await;
            if result.is_ok() {
                prompt.store(true, Ordering::SeqCst);
            }
            assert!(result.is_err());
            assert_eq!(calls.load(Ordering::SeqCst), pending_step + 1);
            assert!(released.load(Ordering::SeqCst));
            assert!(!prompt.load(Ordering::SeqCst));
        }
        let start = tokio::time::Instant::now();
        let mut acknowledged = 0;
        let token = CancellationToken::new();
        let result =
            await_configuration_transfer(std::time::Duration::from_millis(100), &token, async {
                for _ in 0..3 {
                    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
                    acknowledged += 1;
                }
                Ok(())
            })
            .await;
        assert!(result.is_err());
        assert_eq!(acknowledged, 1);
        assert_eq!(
            tokio::time::Instant::now() - start,
            std::time::Duration::from_millis(100)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_drops_mutation_wait_and_success_keeps_ordered_choices() {
        let token = CancellationToken::new();
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let cancelled = token.clone();
        let cancel = async {
            waiting.await.unwrap();
            cancelled.cancel();
        };
        let transfer =
            await_configuration_transfer(std::time::Duration::from_secs(60), &token, async {
                entered.send(()).unwrap();
                std::future::pending::<Result<()>>().await
            });
        let (result, ()) = tokio::join!(transfer, cancel);
        assert!(result.unwrap_err().to_string().contains("cancelled"));
        let token = CancellationToken::new();
        let mut choices = Vec::new();
        await_configuration_transfer(std::time::Duration::from_secs(1), &token, async {
            for opaque in ["z", "a", "boolean"] {
                choices.push(opaque);
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(choices, ["z", "a", "boolean"]);
    }

    #[test]
    fn final_configuration_is_exact_despite_legitimate_dependency_changes() {
        let mut applied = pioneer_cli_agent_runtime::acp::ConfigurationSnapshot::default();
        applied.options = serde_json::from_value(serde_json::json!([
            {"id":"z","name":"Choice","type":"select","currentValue":"B","options":[{"value":"A","name":"A"},{"value":"B","name":"B"}]},
            {"id":"a","name":"Choice","type":"select","currentValue":"dependent","options":[{"group":"opaque","name":"Group","options":[{"value":"dependent","name":"Dependent"}]}]},
            {"id":"flag","name":"Flag","type":"boolean","currentValue":true}
        ])).unwrap();
        let selected = pioneer_protocol::AcpTurnConfiguration {
            session_id: "preparation".into(),
            expected: pioneer_protocol::AcpConfigurationVersion {
                connection_id: "preparation-connection".into(),
                revision: 3,
            },
            descriptor_fingerprint: pioneer_protocol::acp_descriptor_fingerprint(
                &applied.options,
                None,
            )
            .unwrap(),
            initial_descriptor_fingerprint: "initial-A-options".into(),
            configuration_replay: vec![],
            continuation_requested: false,
            values: std::collections::BTreeMap::from([
                ("z".into(), schema::SessionConfigOptionValue::value_id("B")),
                (
                    "a".into(),
                    schema::SessionConfigOptionValue::value_id("dependent"),
                ),
                (
                    "flag".into(),
                    schema::SessionConfigOptionValue::boolean(true),
                ),
            ]),
            mode: None,
            additional_directories: vec![],
        };
        // Fresh execution already at B is safe even when its initial defaults
        // differ: the complete final offers and selected values are exact.
        validate_applied_configuration(&applied, &selected).unwrap();
        if let schema::SessionConfigKind::Boolean(flag) = &mut applied.options[2].kind {
            flag.current_value = false;
        }
        assert!(validate_applied_configuration(&applied, &selected).is_err());
        if let schema::SessionConfigKind::Boolean(flag) = &mut applied.options[2].kind {
            flag.current_value = true;
        }
        applied.options.remove(1);
        assert!(validate_applied_configuration(&applied, &selected).is_err());
    }
}
