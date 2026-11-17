//! Bounded application-layer footprint. No request text or replay payloads are
//! retained. This deliberately makes no final-wire or billed-token claim.
use crate::{ChatRequest, Role};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

struct Fingerprint(Sha256);
impl std::io::Write for Fingerprint {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn fingerprint(value: &impl Serialize) -> anyhow::Result<String> {
    let mut writer = Fingerprint(Sha256::new());
    serde_json::to_writer(&mut writer, value)?;
    Ok(hex::encode(writer.0.finalize()))
}

pub fn request_manifest(request: &ChatRequest) -> anyhow::Result<Value> {
    const MAX_ENTRIES: usize = 64;
    let mut entries = Vec::new();
    for (ordinal, message) in request.messages.iter().enumerate().take(MAX_ENTRIES) {
        let category = if message
            .provenance
            .as_ref()
            .is_some_and(|p| p.sources.iter().any(|s| s.scope.starts_with("checkpoint:")))
        {
            "compaction_summary"
        } else {
            match message.role {
                Role::System => "system_unclassified",
                Role::User => "user_message",
                Role::Assistant => "assistant_message",
                Role::Tool => "tool_result",
            }
        };
        // Do not turn media/base64/opaque payload bytes into a token estimate.
        // Only the separately identified text part is measured here.
        entries.push(
            json!({"ordinal":ordinal,"category":category,"role":message.role,
            "text_bytes":message.content.len(),"request_inclusion_text_bytes":message.content.len(),
            "tool_size_stage":if message.role==Role::Tool{Some("request_occurrence")}else{None},"text_characters":message.content.chars().count(),
            "text_fingerprint":fingerprint(&message.content)?,
            "reasoning_text_bytes":message.reasoning_content.as_ref().map(|v|v.len()),
            "opaque_replay_present":message.provider_replay_state.is_some(),
            "content_parts_count":message.content_parts.len(),
            "generated_tool_calls_count":message.tool_calls.as_ref().map_or(0,Vec::len),
            "estimated_text_tokens":pioneer_compaction::text_tokens(&message.content),"estimator_version":"pioneer_utf16_heuristic_v1",
            "metric_kind":"estimated_input_footprint","media_estimated_tokens":null,
            "reasoning_text_estimated_tokens":message.reasoning_content.as_deref().map(pioneer_compaction::text_tokens),
            "tool_arguments":message.tool_calls.as_ref().map(|calls|calls.iter().take(16).map(|call|
                Ok(json!({"call_id":crate::usage::native_id(Some(&call.id)),"tool_name_fingerprint":fingerprint(&call.name)?,
                    "bytes":call.arguments.len(),"estimated_text_tokens":pioneer_compaction::text_tokens(&call.arguments),
                    "fingerprint":fingerprint(&call.arguments)?,"basis":"assistant_history_input_occurrence"})))
                .collect::<anyhow::Result<Vec<_>>>()).transpose()?,
            "source_reference_count":message.provenance.as_ref().map_or(0,|p|p.sources.len()),
            "source_reconstruction":"not_retained_by_manifest",
            "inclusion":if message.role==Role::System && request.compiled_prompt.is_some(){"replaced_by_compiled_prompt"}else{"application_input_before_adapter_transforms"},
            "tool_call_id":message.tool_call_id.as_deref().and_then(|v|crate::usage::native_id(Some(v))),
            "source_occurrences":message.provenance.as_ref().map(|p|p.sources.iter().take(16).map(|s|
                Ok(json!({"source_id_fingerprint":fingerprint(&(s.scope.as_str(),s.id.as_str()))?,"source_version_fingerprint":fingerprint(&s.version)?,"retained_reference":safe_source_reference(&s.scope,&s.id,&s.version)})))
                .collect::<anyhow::Result<Vec<_>>>()).transpose()?}),
        );
    }
    let mut tools = Vec::new();
    for (ordinal, tool) in request.tools.iter().flatten().enumerate().take(MAX_ENTRIES) {
        let definition = serde_json::to_string(tool)?;
        let origin = request
            .compiled_prompt
            .as_ref()
            .and_then(|p| p.observation_manifest.as_ref())
            .and_then(|v| v["tool_origins"].get(&tool.name));
        tools.push(
            json!({"serialized_canonical_bytes":definition.len(),"estimated_text_tokens":pioneer_compaction::text_tokens(&definition),
            "estimator_version":"pioneer_utf16_heuristic_v1","ordinal":ordinal,"definition_fingerprint":hex::encode(Sha256::digest(definition.as_bytes())),
            "kind":origin.and_then(|v|v.get("kind")).unwrap_or(&Value::Null),"origin":origin,"tool_name":crate::usage::cli_statistics_identifier(&json!(tool.name)),"estimated_tokens":null}),
        );
    }
    let mut manifest = json!({"version":1,"layer":"application_canonical","render_version":null,
        "coverage":"partial","entries":entries,"tool_entries":tools,
        "message_count":request.messages.len(),
        "omitted_message_entries":request.messages.len().saturating_sub(MAX_ENTRIES),
        "omitted_tool_entries":request.tools.as_ref().map_or(0,Vec::len).saturating_sub(MAX_ENTRIES),
        "ordered_canonical_definitions_hash":fingerprint(&request.tools)?,
        "rendered_definitions_hash":null,"definition_change_reason":request.compiled_prompt.as_ref().and_then(|p|p.observation_manifest.as_ref()).and_then(|v|v.get("definition_change_reason")),
        "compiled_prompt_present":request.compiled_prompt.is_some(),
        "compiled_sections":request.compiled_prompt.as_ref().and_then(|p|p.observation_manifest.as_ref()),
        "effective_budget":request.compiled_prompt.as_ref().and_then(|p|p.observation_manifest.as_ref()).and_then(|v|v.get("effective_budget")),
        "output_reserve_tokens":request.max_tokens,"reserve_basis":"request_max_tokens_not_billed_input",
        "unmeasured":["adapter_transforms","framing","media","opaque_replay","source_reconstruction"],
        "provider_cache_diagnostics":null});
    // Tool origins are already represented per definition; avoid retaining a
    // second map inside the compiled instruction section metadata.
    if let Some(compiled) = manifest["compiled_sections"].as_object_mut() {
        compiled.remove("tool_origins");
        compiled.remove("effective_budget");
        if let Some(sections) = compiled.get_mut("sections").and_then(Value::as_array_mut) {
            for section in sections {
                // The bundle is a reference, not proof of delivery this round.
                // TurnContext (including inventory baseline reuse) is already
                // measured in actual message occurrences. Never add it again.
                section["footprint_inclusion"] =
                    if section["included_in_provider_instructions"] == true {
                        "provider_instruction_occurrence"
                    } else {
                        "nonadditive_bundle_reference"
                    }
                    .into();
            }
        }
    }
    // The independent 12 KiB budget leaves room for referenced tariff evidence and
    // bounded usage inside the journal's existing 32 KiB envelope. Tail loss is
    // explicit; financial evidence is never trimmed to admit diagnostics.
    while serde_json::to_vec(&manifest)?.len() > 12 * 1024 - 128 {
        if let Some(sections) = manifest["compiled_sections"]["sections"].as_array_mut() {
            if sections.pop().is_some() {
                manifest["compiled_sections"]["detail_truncated"] = true.into();
                let omitted = manifest["compiled_sections"]["omitted_sections"]
                    .as_u64()
                    .unwrap_or(0);
                manifest["compiled_sections"]["omitted_sections"] = (omitted + 1).into();
                continue;
            }
        }
        let key = if manifest["tool_entries"]
            .as_array()
            .is_some_and(|v| !v.is_empty())
        {
            "tool_entries"
        } else {
            "entries"
        };
        let entries = manifest[key].as_array_mut().expect("manifest entries");
        if entries.pop().is_none() {
            break;
        }
    }
    manifest["omitted_message_entries"] =
        json!(request.messages.len() - manifest["entries"].as_array().expect("entries").len());
    manifest["omitted_tool_entries"] = json!(
        request.tools.as_ref().map_or(0, Vec::len)
            - manifest["tool_entries"]
                .as_array()
                .expect("tool entries")
                .len()
    );
    Ok(manifest)
}

// Existing runtime locators only. Arbitrary provider URLs/path-like source IDs
// remain fingerprints; this endpoint does not grant access to referenced text.
fn safe_source_reference(scope: &str, id: &str, version: &str) -> Option<Value> {
    let safe = |s: &str| {
        !s.is_empty()
            && s.len() <= 256
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b':' | b'.'))
    };
    let known = scope.split_once(':').is_some_and(|(kind, _)| {
        matches!(
            kind,
            "input" | "item" | "event" | "context" | "checkpoint" | "task-basis"
        )
    });
    (known && safe(scope) && safe(id) && safe(version)).then(||json!({"scope":scope,"id":id,"version":version,"availability":"not_checked_subject_to_existing_source_retention_and_acl"}))
}

/// Non-chat requests use the same content-free contract, including a separate
/// occurrence for every input even when the same source text is embedded again.
pub fn embedding_request_manifest(request: &crate::EmbeddingRequest) -> anyhow::Result<Value> {
    let mut entries=request.input.iter().enumerate().take(64).map(|(ordinal,text)|
        Ok(json!({"ordinal":ordinal,"category":"embedding_input","role":null,
            "text_bytes":text.len(),"text_characters":text.chars().count(),
            "estimated_text_tokens":pioneer_compaction::text_tokens(text),"estimator_version":"pioneer_utf16_heuristic_v1",
            "text_fingerprint":fingerprint(text)?,"inclusion":"application_input_before_adapter_transforms",
            "source_reconstruction":"not_archived_by_manifest"})))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut manifest = json!({"version":1,"layer":"application_canonical","kind":"embedding_input",
        "input_count":request.input.len(),"text_bytes":request.input.iter().map(String::len).sum::<usize>(),
        "ordered_inputs_fingerprint":fingerprint(&request.input)?,"entries":[],"coverage":"bounded_estimated_text_not_billed_tokens"});
    // Bound the metadata, never the request or its financial quantities.
    while serde_json::to_vec(&entries)?.len() > 11 * 1024 {
        entries.pop();
    }
    manifest["omitted_component_entries"] = (request.input.len() - entries.len()).into();
    manifest["entries"] = entries.into();
    Ok(manifest)
}
