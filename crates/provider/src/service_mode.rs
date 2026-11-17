//! Apply admitted typed fields only. Every request starts with fresh generation
//! fields, so switching presets cannot leak fields from the previous request.
use crate::ChatRequest;
use anyhow::{Result, ensure};
use pioneer_protocol::{OpenAiTier, ServiceModeParameters as P};
use serde_json::{Map, Value, json};

pub(crate) fn apply(
    provider: &str,
    api: &str,
    request: &ChatRequest,
    fields: &mut Map<String, Value>,
) -> Result<()> {
    let Some(plan) = &request.service_mode else {
        return Ok(());
    }; // CLI-owned API flows keep omission.
    match &plan.parameters {
        P::Neutral => {}
        P::OpenAiStandard => {
            ensure!(provider == "openai", "invalid Standard adapter");
            fields.insert("service_tier".into(), json!("default"));
        }
        P::OpenAiTier { tier } => {
            ensure!(
                matches!(provider, "openai" | "fireworks"),
                "invalid service tier adapter"
            );
            ensure!(
                *tier != OpenAiTier::Ultrafast || api == "responses",
                "Ultrafast requires Responses"
            );
            let tier = match tier {
                OpenAiTier::Priority => "priority",
                OpenAiTier::Flex => "flex",
                OpenAiTier::Ultrafast => "ultrafast",
            };
            fields.insert("service_tier".into(), json!(tier));
        }
        P::OpenAiPro => {
            ensure!(
                provider == "openai" && api == "responses",
                "Pro requires the confirmed Responses contract"
            );
            ensure!(
                !crate::generation::selected_off(request.reasoning),
                "Pro is incompatible with disabled reasoning"
            );
            fields.insert("service_tier".into(), json!("default"));
            let reasoning = fields.entry("reasoning").or_insert_with(|| json!({}));
            reasoning
                .as_object_mut()
                .ok_or_else(|| anyhow::anyhow!("invalid reasoning preparation"))?
                .insert("mode".into(), json!("pro"));
        }
        P::AnthropicStandard | P::AnthropicFast { .. } => {
            ensure!(
                provider == "anthropic" && api == "messages",
                "invalid speed adapter"
            );
            // Standard is the neutral request without speed. Sending even
            // speed=standard requires the fast-mode beta on some models.
            // https://platform.claude.com/docs/en/build-with-claude/fast-mode#falling-back-to-standard-speed
            if matches!(plan.parameters, P::AnthropicFast { .. }) {
                fields.insert("speed".into(), json!("fast"));
            }
        }
        P::BedrockStandard | P::BedrockNestedUltrafast => {
            ensure!(
                provider == "bedrock" && api == "bedrock_converse",
                "invalid nested service tier adapter"
            );
            fields.insert(
                "service_tier".into(),
                json!(if matches!(plan.parameters, P::BedrockNestedUltrafast) {
                    "ultrafast"
                } else {
                    "default"
                }),
            );
        }
    }
    Ok(())
}

pub(crate) fn anthropic_beta(request: &ChatRequest, inline_tools: bool) -> Option<String> {
    let mut tokens = Vec::new();
    if inline_tools {
        tokens.push("inline-tools-2026-09-15");
    }
    if let Some(pioneer_protocol::NativeServiceModePlan {
        parameters: P::AnthropicFast { beta: Some(beta) },
        ..
    }) = &request.service_mode
    {
        // The normalized descriptor admits only this exact service beta token.
        if beta == "fast-mode-2026-02-01" {
            tokens.push("fast-mode-2026-02-01");
        }
    }
    (!tokens.is_empty()).then(|| tokens.join(","))
}

/// Exact bounded service-specific wire subset, using the same mapping as the
/// adapter. It contains no messages, credentials, URL, or arbitrary headers.
pub(crate) fn request_fields(provider: &str, api: &str, request: &ChatRequest) -> Result<Value> {
    let mut fields = Map::new();
    apply(provider, api, request, &mut fields)?;
    let body = if api == "bedrock_converse" && !fields.is_empty() {
        json!({"additionalModelRequestFields": fields})
    } else {
        Value::Object(fields)
    };
    let beta_tokens = anthropic_beta(request, false)
        .map(|beta| vec![beta])
        .unwrap_or_default();
    Ok(json!({"body":body,"beta_tokens":beta_tokens}))
}
