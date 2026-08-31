use super::*;
use sha2::{Digest, Sha256};

const MEMORY_POST_TURN_RUNTIME_FINGERPRINT_KEY: &str = "runtime_config_sha256";

pub(super) fn memory_policy_classifier_capabilities() -> HookCapabilities {
    HookCapabilities::new([
        HookCapability::new("memory").expect("static capability is valid"),
        HookCapability::new("call_provider").expect("static capability is valid"),
        HookCapability::new("contribute_policy").expect("static capability is valid"),
    ])
}

pub(super) fn memory_tool_bundle_capabilities() -> HookCapabilities {
    HookCapabilities::new([
        HookCapability::new("memory").expect("static capability is valid"),
        HookCapability::new("read_domain_context").expect("static capability is valid"),
        HookCapability::new("write_domain_context").expect("static capability is valid"),
        HookCapability::new("contribute_tool_bundle").expect("static capability is valid"),
    ])
}

pub(super) fn memory_deterministic_recall_capabilities() -> HookCapabilities {
    HookCapabilities::new([
        HookCapability::new("memory").expect("static capability is valid"),
        HookCapability::new("read_domain_context").expect("static capability is valid"),
        HookCapability::new("contribute_prompt_context").expect("static capability is valid"),
        HookCapability::new("emit_audit").expect("static capability is valid"),
    ])
}

pub(super) fn memory_active_recall_capabilities() -> HookCapabilities {
    HookCapabilities::new([
        HookCapability::new("memory").expect("static capability is valid"),
        HookCapability::new("read_domain_context").expect("static capability is valid"),
        HookCapability::new("contribute_prompt_context").expect("static capability is valid"),
        HookCapability::new("emit_audit").expect("static capability is valid"),
    ])
}

pub(super) fn memory_prompt_contract_capabilities() -> HookCapabilities {
    HookCapabilities::new([
        HookCapability::new("memory").expect("static capability is valid"),
        HookCapability::new("read_domain_context").expect("static capability is valid"),
        HookCapability::new("contribute_prompt_section").expect("static capability is valid"),
    ])
}

pub(super) fn memory_post_turn_extractor_capabilities(
    config: &MemoryPostTurnExtractorConfig,
    write_provider_available: bool,
    extractor_provider_available: bool,
) -> HookCapabilities {
    let mut capabilities = vec![
        HookCapability::new("memory").expect("static capability is valid"),
        HookCapability::new("read_domain_context").expect("static capability is valid"),
        HookCapability::new("idempotent_side_effect").expect("static capability is valid"),
    ];
    if write_provider_available {
        capabilities
            .push(HookCapability::new("write_domain_context").expect("static capability is valid"));
    }
    if extractor_provider_available {
        capabilities
            .push(HookCapability::new("call_provider").expect("static capability is valid"));
    }
    let mut capabilities = HookCapabilities::new(capabilities);
    capabilities.metadata.insert(
        HookMetadataKey::new(MEMORY_POST_TURN_RUNTIME_FINGERPRINT_KEY)
            .expect("static hook metadata key is valid"),
        HookValue::Text(memory_post_turn_runtime_fingerprint(
            config,
            write_provider_available,
            extractor_provider_available,
        )),
    );
    capabilities
}

/// Fingerprint the complete, normalized execution contract without retaining
/// provider credentials or raw configuration in a durable hook snapshot.
/// A pending terminal effect must fail closed if a restart would execute it
/// with materially different handler semantics.
fn memory_post_turn_runtime_fingerprint(
    config: &MemoryPostTurnExtractorConfig,
    write_provider_available: bool,
    extractor_provider_available: bool,
) -> String {
    let config = config.normalized();
    let canonical = serde_json::json!({
        "schema_version": 1,
        "write_provider_available": write_provider_available,
        "extractor_provider_available": extractor_provider_available,
        "config": {
            "enabled": config.enabled,
            "provider_enabled": config.provider_enabled,
            "proactive_writes_enabled": config.proactive_writes_enabled,
            "await_policy": config.await_policy,
            "provider_name": config.provider_name,
            "model": config.model,
            "timeout_ms": config.timeout_ms,
            "max_facts_per_turn": config.max_facts_per_turn,
            "max_input_chars": config.max_input_chars,
            "max_manifest_items": config.max_manifest_items,
            "max_fact_content_chars": config.max_fact_content_chars,
            "max_evidence_chars": config.max_evidence_chars,
            "strict_debug": config.strict_debug,
        }
    })
    .to_string();
    hex::encode(Sha256::digest(canonical.as_bytes()))
}
