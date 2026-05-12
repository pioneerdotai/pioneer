use super::*;

pub(super) fn memory_policy_contribution(policy: &MemoryTurnPolicy) -> PolicyContribution {
    PolicyContribution {
        domain: memory_policy_domain(),
        key: memory_turn_policy_key(),
        value: memory_turn_policy_to_hook_value(policy),
        priority: 500,
        diagnostics: hook_diagnostics_from_strings(policy.diagnostics.as_slice()),
    }
}

pub(crate) fn memory_turn_policy_to_hook_value(policy: &MemoryTurnPolicy) -> HookValue {
    let mut object = BTreeMap::new();
    insert_policy_text(&mut object, "recall", policy.recall.as_str());
    insert_policy_text(&mut object, "prompt", policy.prompt.as_str());
    insert_policy_text(&mut object, "read_tools", policy.read_tools.as_str());
    insert_policy_text(&mut object, "remember_tool", policy.remember_tool.as_str());
    insert_policy_text(&mut object, "forget_tool", policy.forget_tool.as_str());
    insert_policy_text(
        &mut object,
        "post_turn_extraction",
        policy.post_turn_extraction.as_str(),
    );
    insert_policy_text(&mut object, "active_memory", policy.active_memory.as_str());
    insert_policy_bool(&mut object, "explicit_remember", policy.explicit_remember);
    insert_policy_bool(&mut object, "explicit_forget", policy.explicit_forget);
    insert_policy_text_optional(
        &mut object,
        "forget_target_hint",
        policy.forget_target_hint.as_deref(),
    );
    insert_policy_text(&mut object, "reason_code", policy.reason_code.as_str());
    object.insert(
        hook_metadata_key("confidence"),
        HookValue::F64(policy.confidence.clamp(0.0, 1.0) as f64),
    );
    insert_policy_text(&mut object, "source", policy.source.as_str());
    insert_policy_text_optional(
        &mut object,
        "detected_language",
        policy.detected_language.as_deref(),
    );
    if !policy.diagnostics.is_empty() {
        object.insert(
            hook_metadata_key("diagnostics_summary"),
            HookValue::List(
                policy
                    .diagnostics
                    .iter()
                    .map(|diagnostic| HookValue::Text(safe_memory_policy_diagnostic(diagnostic)))
                    .collect(),
            ),
        );
    }
    HookValue::Object(object)
}

pub(crate) fn memory_turn_policy_from_hook_value(
    value: &HookValue,
) -> Result<MemoryTurnPolicy, MemoryPolicyDecodeError> {
    let HookValue::Object(object) = value else {
        return Err(MemoryPolicyDecodeError::invalid_type("turn_policy"));
    };

    let mut policy = MemoryTurnPolicy {
        recall: MemoryRecallPolicy::from_str(required_text(object, "recall")?)?,
        prompt: MemoryPromptPolicy::from_str(required_text(object, "prompt")?)?,
        read_tools: MemoryReadToolPolicy::from_str(required_text(object, "read_tools")?)?,
        remember_tool: MemoryMutationToolPolicy::from_str(required_text(object, "remember_tool")?)?,
        forget_tool: MemoryMutationToolPolicy::from_str(required_text(object, "forget_tool")?)?,
        post_turn_extraction: MemoryExtractionPolicy::from_str(required_text(
            object,
            "post_turn_extraction",
        )?)?,
        active_memory: MemoryActiveContextPolicy::from_str(required_text(
            object,
            "active_memory",
        )?)?,
        explicit_remember: required_bool(object, "explicit_remember")?,
        explicit_forget: required_bool(object, "explicit_forget")?,
        forget_target_hint: optional_text(object, "forget_target_hint")?,
        reason_code: MemoryPolicyReasonCode::from_str(required_text(object, "reason_code")?)?,
        confidence: required_f32(object, "confidence")?.clamp(0.0, 1.0),
        source: MemoryPolicySource::from_str(required_text(object, "source")?)?,
        detected_language: optional_text(object, "detected_language")?,
        diagnostics: optional_text_list(object, "diagnostics_summary")?,
    };
    policy.detected_language = policy.detected_language.and_then(|language| {
        let trimmed = language.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    });
    Ok(policy)
}

pub fn memory_turn_policy_from_hook_policy_set(
    policy_set: &HookPolicySet,
) -> Option<Result<MemoryTurnPolicy, MemoryPolicyDecodeError>> {
    policy_set
        .get(&memory_policy_domain(), &memory_turn_policy_key())
        .map(|entry| memory_turn_policy_from_hook_value(&entry.value))
}

pub(super) fn memory_policy_domain() -> HookDomain {
    HookDomain::new(MEMORY_POLICY_DOMAIN).expect("static domain is valid")
}

pub(super) fn memory_turn_policy_key() -> HookPolicyKey {
    HookPolicyKey::new(MEMORY_TURN_POLICY_KEY).expect("static policy key is valid")
}

pub(super) fn hook_metadata_key(key: &'static str) -> HookMetadataKey {
    HookMetadataKey::new(key).expect("static metadata key is valid")
}

pub(super) fn insert_usize_metadata(object: &mut HookMetadata, key: &'static str, value: usize) {
    object.insert(
        hook_metadata_key(key),
        HookValue::I64(i64::try_from(value).unwrap_or(i64::MAX)),
    );
}

pub(super) fn insert_policy_text(
    object: &mut BTreeMap<HookMetadataKey, HookValue>,
    key: &'static str,
    value: &'static str,
) {
    object.insert(hook_metadata_key(key), HookValue::Text(value.to_owned()));
}

pub(super) fn insert_policy_text_optional(
    object: &mut BTreeMap<HookMetadataKey, HookValue>,
    key: &'static str,
    value: Option<&str>,
) {
    match value.and_then(|text| {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    }) {
        Some(value) => {
            object.insert(hook_metadata_key(key), HookValue::Text(value.to_owned()));
        }
        None => {
            object.insert(hook_metadata_key(key), HookValue::Null);
        }
    }
}

pub(super) fn insert_policy_bool(
    object: &mut BTreeMap<HookMetadataKey, HookValue>,
    key: &'static str,
    value: bool,
) {
    object.insert(hook_metadata_key(key), HookValue::Bool(value));
}

pub(super) fn required_value<'a>(
    object: &'a BTreeMap<HookMetadataKey, HookValue>,
    key: &'static str,
) -> Result<&'a HookValue, MemoryPolicyDecodeError> {
    object
        .get(&hook_metadata_key(key))
        .ok_or_else(|| MemoryPolicyDecodeError::missing(key))
}

pub(super) fn required_text<'a>(
    object: &'a BTreeMap<HookMetadataKey, HookValue>,
    key: &'static str,
) -> Result<&'a str, MemoryPolicyDecodeError> {
    match required_value(object, key)? {
        HookValue::Text(value) if !value.trim().is_empty() => Ok(value.trim()),
        _ => Err(MemoryPolicyDecodeError::invalid_type(key)),
    }
}

pub(super) fn optional_text(
    object: &BTreeMap<HookMetadataKey, HookValue>,
    key: &'static str,
) -> Result<Option<String>, MemoryPolicyDecodeError> {
    match object.get(&hook_metadata_key(key)) {
        None | Some(HookValue::Null) => Ok(None),
        Some(HookValue::Text(value)) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_owned()))
            }
        }
        Some(_) => Err(MemoryPolicyDecodeError::invalid_type(key)),
    }
}

pub(super) fn required_bool(
    object: &BTreeMap<HookMetadataKey, HookValue>,
    key: &'static str,
) -> Result<bool, MemoryPolicyDecodeError> {
    match required_value(object, key)? {
        HookValue::Bool(value) => Ok(*value),
        _ => Err(MemoryPolicyDecodeError::invalid_type(key)),
    }
}

pub(super) fn required_f32(
    object: &BTreeMap<HookMetadataKey, HookValue>,
    key: &'static str,
) -> Result<f32, MemoryPolicyDecodeError> {
    match required_value(object, key)? {
        HookValue::F64(value) => Ok(*value as f32),
        HookValue::I64(value) => Ok(*value as f32),
        _ => Err(MemoryPolicyDecodeError::invalid_type(key)),
    }
}

pub(super) fn optional_text_list(
    object: &BTreeMap<HookMetadataKey, HookValue>,
    key: &'static str,
) -> Result<Vec<String>, MemoryPolicyDecodeError> {
    match object.get(&hook_metadata_key(key)) {
        None | Some(HookValue::Null) => Ok(Vec::new()),
        Some(HookValue::List(values)) => values
            .iter()
            .map(|value| match value {
                HookValue::Text(text) => Ok(safe_memory_policy_diagnostic(text)),
                _ => Err(MemoryPolicyDecodeError::invalid_type(key)),
            })
            .collect(),
        Some(_) => Err(MemoryPolicyDecodeError::invalid_type(key)),
    }
}
pub(super) fn memory_turn_policy_request_from_metadata(
    metadata: &HookMetadata,
) -> (MemoryTurnPolicyRequest, Vec<String>) {
    let mut request = MemoryTurnPolicyRequest::default();
    let mut diagnostics = Vec::new();

    if let Some(value) = metadata.get(&hook_metadata_key(MEMORY_TURN_POLICY_DEFAULT_METADATA_KEY)) {
        match memory_turn_policy_from_hook_value(value) {
            Ok(default_policy) => request.default_policy = default_policy,
            Err(error) => diagnostics.push(format!(
                "memory.policy.metadata_invalid: default_policy {error}"
            )),
        }
    }

    if let Some(value) = metadata.get(&hook_metadata_key(MEMORY_TURN_POLICY_OVERRIDE_METADATA_KEY))
    {
        match memory_turn_policy_from_hook_value(value) {
            Ok(policy) => {
                request.structured_override = Some(MemoryTurnPolicyOverride::new(policy));
            }
            Err(error) => {
                diagnostics.push(format!("memory.policy.metadata_invalid: override {error}"))
            }
        }
    }

    if let Some(value) = metadata.get(&hook_metadata_key(
        MEMORY_TURN_POLICY_CLASSIFIER_ENABLED_METADATA_KEY,
    )) {
        match value {
            HookValue::Bool(enabled) => request.classifier_enabled = *enabled,
            _ => diagnostics
                .push("memory.policy.metadata_invalid: classifier_enabled invalid_type".to_owned()),
        }
    }

    if let Some(value) = metadata.get(&hook_metadata_key(MEMORY_TURN_POLICY_FALLBACK_METADATA_KEY))
    {
        match value {
            HookValue::Text(fallback) => match MemoryClassifierFallbackPolicy::from_str(fallback) {
                Ok(fallback) => request.fallback = fallback,
                Err(error) => {
                    diagnostics.push(format!("memory.policy.metadata_invalid: fallback {error}"))
                }
            },
            _ => {
                diagnostics.push("memory.policy.metadata_invalid: fallback invalid_type".to_owned())
            }
        }
    }

    (request, diagnostics)
}

pub(super) fn safe_memory_policy_diagnostic(message: &str) -> String {
    const MAX_DIAGNOSTIC_CHARS: usize = 300;
    let mut safe = message.replace(['\r', '\n'], " ");
    safe.truncate(MAX_DIAGNOSTIC_CHARS);
    if safe.trim().is_empty() {
        "memory diagnostic unavailable".to_owned()
    } else {
        safe
    }
}

pub(crate) async fn resolve_memory_turn_policy(
    provider: Option<&Arc<dyn AgentMemoryTurnPolicyProvider>>,
    context: MemoryTurnPolicyContext,
    request: MemoryTurnPolicyRequest,
) -> MemoryTurnPolicy {
    if context.mode == ThreadMode::Chat {
        return MemoryTurnPolicy::no_use().with_source(
            MemoryPolicySource::StructuredOverride,
            MemoryPolicyReasonCode::ChatModeDisabled,
            1.0,
        );
    }

    if let Some(override_policy) = request.structured_override {
        return override_policy.policy.with_source(
            MemoryPolicySource::StructuredOverride,
            MemoryPolicyReasonCode::StructuredOverride,
            1.0,
        );
    }

    if !request.classifier_enabled {
        return fallback_policy(
            request.default_policy,
            request.fallback,
            MemoryPolicyReasonCode::ClassifierUnavailable,
            Some("memory.policy.fallback_used: classifier disabled"),
        );
    }

    let Some(provider) = provider else {
        return fallback_policy(
            request.default_policy,
            request.fallback,
            MemoryPolicyReasonCode::ClassifierUnavailable,
            Some("memory.policy.fallback_used: classifier provider unavailable"),
        );
    };

    match provider
        .resolve_memory_turn_policy(context, request.clone())
        .await
    {
        Ok(policy) => policy,
        Err(error) => {
            let reason_code = fallback_reason_for_classifier_error(error.as_str());
            fallback_policy(
                request.default_policy,
                request.fallback,
                reason_code,
                Some(format!(
                    "memory.policy.classifier_failed: reason={}",
                    reason_code.as_str()
                )),
            )
        }
    }
}

pub(super) fn fallback_reason_for_classifier_error(error: &str) -> MemoryPolicyReasonCode {
    if error.contains("classifier_unavailable")
        || error.contains("missing model")
        || error.contains("failed to create")
        || error.contains("request failed")
    {
        MemoryPolicyReasonCode::ClassifierUnavailable
    } else {
        MemoryPolicyReasonCode::ClassifierInvalidJson
    }
}

pub(super) fn fallback_policy(
    default_policy: MemoryTurnPolicy,
    fallback: MemoryClassifierFallbackPolicy,
    reason_code: MemoryPolicyReasonCode,
    diagnostic: Option<impl Into<String>>,
) -> MemoryTurnPolicy {
    let mut policy = match fallback {
        MemoryClassifierFallbackPolicy::DefaultAllow => {
            default_policy.with_source(MemoryPolicySource::DefaultFallback, reason_code, 0.0)
        }
        MemoryClassifierFallbackPolicy::StrictDeny => {
            MemoryTurnPolicy::strict_deny_fallback(reason_code)
        }
        MemoryClassifierFallbackPolicy::AllowReadOnly => {
            MemoryTurnPolicy::allow_read_only_fallback(reason_code)
        }
    };
    if let Some(diagnostic) = diagnostic {
        let diagnostic: String = diagnostic.into();
        policy
            .diagnostics
            .push(safe_memory_policy_diagnostic(diagnostic.as_str()));
    }
    policy
}
