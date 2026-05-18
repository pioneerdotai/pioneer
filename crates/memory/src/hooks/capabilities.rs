use super::*;

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

pub(super) fn memory_active_recall_capabilities(provider_enabled: bool) -> HookCapabilities {
    let mut capabilities = vec![
        HookCapability::new("memory").expect("static capability is valid"),
        HookCapability::new("read_domain_context").expect("static capability is valid"),
        HookCapability::new("contribute_prompt_context").expect("static capability is valid"),
        HookCapability::new("emit_audit").expect("static capability is valid"),
    ];
    if provider_enabled {
        capabilities
            .push(HookCapability::new("call_provider").expect("static capability is valid"));
    }
    HookCapabilities::new(capabilities)
}

pub(super) fn memory_prompt_contract_capabilities() -> HookCapabilities {
    HookCapabilities::new([
        HookCapability::new("memory").expect("static capability is valid"),
        HookCapability::new("read_domain_context").expect("static capability is valid"),
        HookCapability::new("contribute_prompt_section").expect("static capability is valid"),
    ])
}

pub(super) fn memory_post_turn_extractor_capabilities(provider_enabled: bool) -> HookCapabilities {
    let mut capabilities = vec![
        HookCapability::new("memory").expect("static capability is valid"),
        HookCapability::new("read_domain_context").expect("static capability is valid"),
        HookCapability::new("write_domain_context").expect("static capability is valid"),
        HookCapability::new("idempotent_side_effect").expect("static capability is valid"),
    ];
    if provider_enabled {
        capabilities
            .push(HookCapability::new("call_provider").expect("static capability is valid"));
    }
    HookCapabilities::new(capabilities)
}
