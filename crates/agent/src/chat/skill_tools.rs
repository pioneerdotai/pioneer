use crate::SkillsLoopConfig;
use pioneer_skills::{
    ReadSkillEntry, RuntimeExecutionClassHint, SkillRuntimeDescriptor, SkillRuntimePlan,
    SkillRuntimeToolKind,
};
use pioneer_tools::{
    DynamicToolOutputPolicyCaps, ExecutionClass, SkillDynamicToolDescriptor, SkillDynamicToolKind,
    SkillReadToolConfig, SkillReadToolEntry, SkillRuntimeToolMaterialization,
    materialize_skill_runtime_tools,
};
use std::collections::HashMap;

pub(super) type SkillToolMaterialization = SkillRuntimeToolMaterialization;

pub(super) fn materialize_skill_tooling(
    runtime_plan: &SkillRuntimePlan,
    skills_config: &SkillsLoopConfig,
) -> SkillToolMaterialization {
    let descriptors = runtime_plan
        .tools
        .iter()
        .map(runtime_descriptor_to_dynamic_descriptor)
        .collect::<Vec<_>>();

    let contains_inline_agent = runtime_plan
        .read_skill_index
        .values()
        .any(|entry| entry.source.is_inline_agent());
    let read_skill =
        (skills_config.runtime.enable_read_skill || contains_inline_agent).then(|| {
            SkillReadToolConfig {
                index: map_read_skill_index(&runtime_plan.read_skill_index),
                default_max_chars: skills_config.runtime.read_skill_max_chars.max(1),
            }
        });

    materialize_skill_runtime_tools(
        descriptors.as_slice(),
        read_skill,
        DynamicToolOutputPolicyCaps {
            shell_persist_min_trust: skills_config.security.min_trust_for_shell_tools.clone(),
            allow_dynamic_shell_full_output: skills_config.runtime.allow_shell_tools,
            allow_dynamic_function_proxy_policy_inheritance: skills_config
                .runtime
                .allow_function_proxy_tools,
            ..DynamicToolOutputPolicyCaps::default()
        },
    )
}

fn runtime_descriptor_to_dynamic_descriptor(
    descriptor: &SkillRuntimeDescriptor,
) -> SkillDynamicToolDescriptor {
    SkillDynamicToolDescriptor {
        canonical_tool_name: descriptor.canonical_tool_name.clone(),
        skill_id: descriptor.skill_id.clone(),
        skill_owner: descriptor.skill_owner.clone(),
        skill_slug: descriptor.skill_slug.clone(),
        skill_asset_root: descriptor.skill_asset_root.clone(),
        skill_fingerprint: descriptor.skill_fingerprint.clone(),
        source_kind: descriptor.source_kind.clone(),
        trust_level: descriptor.trust_level.clone(),
        description: descriptor.definition.description.clone(),
        parameters: descriptor.definition.parameters.clone(),
        execution_class: map_execution_class(descriptor.definition.execution_class),
        kind: map_kind(descriptor.definition.kind.clone()),
        config: descriptor.definition.config.clone(),
        requested_output_policy: descriptor.definition.output_policy.clone(),
    }
}

fn map_execution_class(hint: RuntimeExecutionClassHint) -> ExecutionClass {
    match hint {
        RuntimeExecutionClassHint::Shared => ExecutionClass::Shared,
        RuntimeExecutionClassHint::Exclusive => ExecutionClass::Exclusive,
        RuntimeExecutionClassHint::SessionScoped => ExecutionClass::SessionScoped,
    }
}

fn map_kind(kind: SkillRuntimeToolKind) -> SkillDynamicToolKind {
    match kind {
        SkillRuntimeToolKind::Http => SkillDynamicToolKind::Http,
        SkillRuntimeToolKind::Shell => SkillDynamicToolKind::Shell,
        SkillRuntimeToolKind::FunctionProxy => SkillDynamicToolKind::FunctionProxy,
    }
}

fn map_read_skill_index(
    source: &HashMap<String, ReadSkillEntry>,
) -> HashMap<String, SkillReadToolEntry> {
    source
        .iter()
        .map(|(skill_ref, entry)| {
            (
                skill_ref.clone(),
                SkillReadToolEntry {
                    skill_id: entry.skill_id.clone(),
                    owner: entry.owner.clone(),
                    slug: entry.slug.clone(),
                    name: entry.name.clone(),
                    description: entry.description.clone(),
                    body: entry.body.clone(),
                    source: entry.source.clone(),
                    fingerprint: entry.fingerprint.clone(),
                    source_kind: entry.source_kind.clone(),
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::materialize_skill_tooling;
    use crate::{
        SkillsDependenciesLoopConfig, SkillsLoopConfig, SkillsRuntimeLoopConfig,
        SkillsSecurityLoopConfig, SkillsValidationLoopConfig,
    };
    use pioneer_protocol::SkillId;
    use pioneer_skills::{ReadSkillEntry, ReadSkillSource, SkillRuntimePlan, SkillTrustLevel};
    use std::collections::HashMap;

    fn config_with_ordinary_read_disabled() -> SkillsLoopConfig {
        SkillsLoopConfig {
            enabled: false,
            max_skills_per_source: 1,
            max_skill_file_bytes: 1,
            prompt_max_chars: 1,
            allow_implicit_invocation: false,
            system_roots: Vec::new(),
            user_roots: Vec::new(),
            registry_roots: Vec::new(),
            system_import_roots: Vec::new(),
            user_import_roots: Vec::new(),
            registry_import_roots: Vec::new(),
            validation: SkillsValidationLoopConfig {
                strict_agentskills: true,
                accept_openclaw_profile: false,
            },
            security: SkillsSecurityLoopConfig {
                allow_untrusted_install: false,
                min_trust_for_shell_tools: SkillTrustLevel::Verified,
                min_trust_for_http_tools: SkillTrustLevel::Verified,
                min_trust_for_function_proxy_tools: SkillTrustLevel::Verified,
                max_install_archive_bytes: 1,
                max_install_archive_compressed_bytes: 1,
                max_install_archive_uncompressed_bytes: 1,
                max_install_archive_entries: 1,
                max_install_file_bytes: 1,
                upload_ttl_secs: 60,
                upload_recommended_chunk_size_bytes: 1,
                upload_max_chunk_size_bytes: 1,
            },
            dependencies: SkillsDependenciesLoopConfig {
                preflight_on_resolve: false,
                runtime_recheck_on_tool_call: false,
            },
            runtime: SkillsRuntimeLoopConfig {
                enable_dynamic_tools: false,
                enable_read_skill: false,
                max_dynamic_tools_per_skill: 1,
                read_skill_max_chars: 1024,
                compact_mode_threshold: 0,
                allow_shell_tools: false,
                allow_http_tools: false,
                allow_function_proxy_tools: false,
            },
        }
    }

    #[test]
    fn inline_agent_index_materializes_existing_read_skill_override() {
        let skill_id = SkillId::new("AAAAAAAAAAAAAAAAAAAAA").expect("valid skill ID");
        let plan = SkillRuntimePlan {
            tools: Vec::new(),
            read_skill_index: HashMap::from([(
                format!("skill:{skill_id}"),
                ReadSkillEntry {
                    skill_id,
                    owner: None,
                    slug: "stable-procedure".to_owned(),
                    name: "Stable procedure".to_owned(),
                    description: "Use for stable procedures.".to_owned(),
                    body: "Exact inline body.".to_owned(),
                    source: ReadSkillSource::InlineAgent {
                        version_id: "111111111111111111111".to_owned(),
                    },
                    fingerprint: "a".repeat(64),
                    source_kind: "agent".to_owned(),
                },
            )]),
            excluded_tools: Vec::new(),
        };

        let materialized = materialize_skill_tooling(&plan, &config_with_ordinary_read_disabled());
        assert_eq!(materialized.bundles.len(), 1);
        assert!(
            materialized.bundles[0]
                .specs
                .iter()
                .any(|spec| spec.spec.name == "read_skill")
        );
    }
}
