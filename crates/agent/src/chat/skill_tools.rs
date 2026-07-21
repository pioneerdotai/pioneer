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

    let read_skill = skills_config
        .runtime
        .enable_read_skill
        .then(|| SkillReadToolConfig {
            index: map_read_skill_index(&runtime_plan.read_skill_index),
            default_max_chars: skills_config.runtime.read_skill_max_chars.max(1),
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
                    skill_asset_root: entry.skill_asset_root.clone(),
                    fingerprint: entry.fingerprint.clone(),
                    source_kind: entry.source_kind.clone(),
                },
            )
        })
        .collect()
}
