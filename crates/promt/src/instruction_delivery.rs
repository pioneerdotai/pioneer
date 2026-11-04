use crate::fingerprint::sha256_hex;
use crate::render::text::render_sections;
use crate::{CompiledPromptBundle, PromptProfile, PromptSection, PromptSectionId};
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstructionDeliveryChannel {
    ProviderInstructions,
    TurnContext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstructionScope {
    Thread,
    Turn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstructionTrust {
    Pioneer,
    Workspace,
    Conversation,
    External,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstructionSectionPlan {
    pub section_id: String,
    pub channel: InstructionDeliveryChannel,
    pub scope: InstructionScope,
    pub trust: InstructionTrust,
    pub required: bool,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledInstructionPayload {
    pub text: String,
    pub fingerprint: String,
    pub section_ids: Vec<String>,
}

impl CompiledInstructionPayload {
    pub fn is_empty(&self) -> bool {
        self.text.trim().is_empty()
    }
}

impl fmt::Debug for CompiledInstructionPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompiledInstructionPayload")
            .field("text", &"[REDACTED]")
            .field("fingerprint", &self.fingerprint)
            .field("section_ids", &self.section_ids)
            .finish()
    }
}

/// Provider-neutral delivery projection for a compiled prompt profile.
///
/// All profiles separate governing instructions from turn facts before an
/// adapter maps the former to its elevated transport and the latter to new input.
#[derive(Clone, PartialEq, Eq)]
pub struct CompiledInstructionDeliveryPlan {
    pub bundle: CompiledPromptBundle,
    pub provider_instructions: CompiledInstructionPayload,
    pub turn_context: CompiledInstructionPayload,
    pub sections: Vec<InstructionSectionPlan>,
}

impl CompiledInstructionDeliveryPlan {
    pub fn is_native_api_profile(&self) -> bool {
        self.bundle.profile != PromptProfile::CliRuntime
    }

    /// Reuse a bounded inventory slice only if its exact prior projection is
    /// retained in model-facing history. Pass None after compaction/fork or an
    /// unknown dispatch outcome unless the existing recovery path replays it.
    /// A rendered bundle or a local durable ACK is not proof of this baseline.
    /// Changed slices are bounded current replacements, never global deltas.
    pub fn turn_context_with_inventory_baseline(
        &self,
        delivered: Option<&CompiledPromptBundle>,
    ) -> CompiledInstructionPayload {
        let Some(delivered) = delivered.filter(|prior| prior.profile == self.bundle.profile) else {
            return self.turn_context.clone();
        };
        let mut selected = Vec::new();
        for (section, plan) in self.bundle.sections.iter().zip(&self.sections) {
            if plan.channel != InstructionDeliveryChannel::TurnContext {
                continue;
            }
            if is_inventory_section(&section.id)
                && delivered.sections.iter().any(|prior| {
                    prior.id == section.id
                        && !inventory_section_was_clipped(delivered, &prior.id)
                        && !inventory_section_was_clipped(&self.bundle, &section.id)
                        && prior.content == section.content
                        && prior.title == section.title
                })
            {
                continue;
            }
            let mut current = section.clone();
            if is_inventory_section(&section.id) {
                current.content = format!(
                    "Current bounded slice replaces the previously shown slice for this section.\n{}",
                    current.content
                );
            }
            selected.push(current);
        }
        for prior in &delivered.sections {
            if is_inventory_section(&prior.id)
                && !self
                    .bundle
                    .sections
                    .iter()
                    .any(|section| section.id == prior.id)
            {
                selected.push(PromptSection {
                    id: prior.id.clone(),
                    stability: crate::PromptStability::Dynamic,
                    title: prior.title.clone(),
                    content:
                        "Previously shown slice is no longer available in the current projection."
                            .to_owned(),
                    sources: Vec::new(),
                });
            }
        }
        let text = render_turn_sections(&selected, self.bundle.profile);
        CompiledInstructionPayload {
            fingerprint: sha256_hex(&text),
            text,
            section_ids: selected
                .iter()
                .map(|section| section.id.manifest_id())
                .collect(),
        }
    }
}

impl fmt::Debug for CompiledInstructionDeliveryPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompiledInstructionDeliveryPlan")
            .field("compiler_version", &self.bundle.compiler_version)
            .field("profile", &self.bundle.profile)
            .field("provider_instructions", &self.provider_instructions)
            .field("turn_context", &self.turn_context)
            .field("sections", &self.sections)
            .finish()
    }
}

pub fn compile_instruction_delivery_plan(
    bundle: CompiledPromptBundle,
) -> Result<CompiledInstructionDeliveryPlan> {
    let cli_profile = bundle.profile == PromptProfile::CliRuntime;
    let sections = bundle
        .sections
        .iter()
        .map(|section| {
            let policy = if cli_profile {
                cli_instruction_policy(&section.id)
            } else {
                native_instruction_policy(section)
            };
            if cli_profile
                && policy.channel == InstructionDeliveryChannel::ProviderInstructions
                && matches!(
                    policy.trust,
                    InstructionTrust::Conversation | InstructionTrust::External
                )
            {
                bail!(
                    "untrusted CLI prompt section `{}` cannot enter the provider instruction channel",
                    section.id.manifest_id()
                );
            }
            Ok(InstructionSectionPlan {
                section_id: section.id.manifest_id(),
                channel: policy.channel,
                scope: policy.scope,
                trust: policy.trust,
                required: policy.required,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let provider_instructions = compile_rendered_channel(
        bundle.sections.as_slice(),
        sections.as_slice(),
        InstructionDeliveryChannel::ProviderInstructions,
        bundle.profile,
    );
    let turn_context = compile_rendered_channel(
        bundle.sections.as_slice(),
        sections.as_slice(),
        InstructionDeliveryChannel::TurnContext,
        bundle.profile,
    );

    if provider_instructions.is_empty()
        && sections.iter().any(|section| {
            section.channel == InstructionDeliveryChannel::ProviderInstructions && section.required
        })
    {
        bail!("required provider instructions compiled to an empty payload");
    }

    Ok(CompiledInstructionDeliveryPlan {
        bundle,
        provider_instructions,
        turn_context,
        sections,
    })
}

#[derive(Debug, Clone, Copy)]
struct InstructionPolicy {
    channel: InstructionDeliveryChannel,
    scope: InstructionScope,
    trust: InstructionTrust,
    required: bool,
}

const fn provider_instructions(
    scope: InstructionScope,
    trust: InstructionTrust,
) -> InstructionPolicy {
    InstructionPolicy {
        channel: InstructionDeliveryChannel::ProviderInstructions,
        scope,
        trust,
        required: true,
    }
}

const fn turn_context(trust: InstructionTrust) -> InstructionPolicy {
    InstructionPolicy {
        channel: InstructionDeliveryChannel::TurnContext,
        scope: InstructionScope::Turn,
        trust,
        required: false,
    }
}

fn inventory_section_was_clipped(bundle: &CompiledPromptBundle, id: &PromptSectionId) -> bool {
    let manifest_id = id.manifest_id();
    bundle.diagnostics.iter().any(|diagnostic| {
        diagnostic.section_id.as_deref() == Some(manifest_id.as_str())
            && matches!(
                diagnostic.code,
                crate::PromptDiagnosticCode::DynamicSectionTruncated
                    | crate::PromptDiagnosticCode::DynamicSectionOmitted
            )
    })
}

fn is_inventory_section(id: &PromptSectionId) -> bool {
    matches!(
        id,
        PromptSectionId::SkillsRuntimePrompt | PromptSectionId::SelectedCapabilities
    ) || matches!(id, PromptSectionId::Dynamic(id)
            if id.as_str() == crate::REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID)
}

fn native_instruction_policy(section: &PromptSection) -> InstructionPolicy {
    match &section.id {
        PromptSectionId::SkillsRuntimePrompt
        | PromptSectionId::SelectedSkills
        | PromptSectionId::SelectedCapabilities
        | PromptSectionId::CurrentPermissions
        | PromptSectionId::PioneerCliRuntimeContext
        | PromptSectionId::ThreadContext
        | PromptSectionId::DynamicContext
        | PromptSectionId::RecoveryContinuation
        | PromptSectionId::ExecutionContinuation
        | PromptSectionId::RetryRuntimeInstruction => turn_context(instruction_trust(&section.id)),
        PromptSectionId::Dynamic(id)
            if id.as_str() == crate::REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID =>
        {
            turn_context(InstructionTrust::Pioneer)
        }
        PromptSectionId::Dynamic(id)
            if id.as_str() == crate::FILESYSTEM_CAPABILITY_RUNTIME_SECTION_ID
                && section.stability == crate::PromptStability::Stable =>
        {
            // The compiler-owned static fallback previously lived inside
            // ToolUsagePolicy. Preserve that provenance after separating it.
            provider_instructions(InstructionScope::Thread, InstructionTrust::Pioneer)
        }
        // MemoryRecall is the explicit memory-tools operating contract, not
        // automatic recall. Unknown hooks and ExtraSystem can contain rules.
        _ => provider_instructions(InstructionScope::Thread, instruction_trust(&section.id)),
    }
}

fn cli_instruction_policy(id: &PromptSectionId) -> InstructionPolicy {
    match id {
        PromptSectionId::IdentityBase
        | PromptSectionId::AssistantSafety
        | PromptSectionId::ArtifactOutputContract
        | PromptSectionId::ToolUsagePolicy
        | PromptSectionId::ToolRecoveryPolicy
        | PromptSectionId::TaskOrchestrationPolicy
        | PromptSectionId::SubagentsPolicy
        | PromptSectionId::HtmlPolicy
        | PromptSectionId::TasksPolicy
        | PromptSectionId::ThreadsPolicy
        | PromptSectionId::PioneerCliRuntimeInstructions => {
            provider_instructions(InstructionScope::Thread, InstructionTrust::Pioneer)
        }
        PromptSectionId::SoulCore
        | PromptSectionId::IdentityCore
        | PromptSectionId::UserPersona
        | PromptSectionId::AgentsMd => {
            provider_instructions(InstructionScope::Thread, InstructionTrust::Workspace)
        }
        PromptSectionId::SkillsRuntimePrompt
        | PromptSectionId::SelectedSkills
        | PromptSectionId::SelectedCapabilities
        | PromptSectionId::CurrentPermissions
        | PromptSectionId::RecoveryContinuation
        | PromptSectionId::ExecutionContinuation
        | PromptSectionId::RetryRuntimeInstruction => turn_context(InstructionTrust::Pioneer),
        PromptSectionId::PioneerCliRuntimeContext => turn_context(InstructionTrust::Pioneer),
        PromptSectionId::MemoryRecall
        | PromptSectionId::ThreadContext
        | PromptSectionId::DynamicContext => turn_context(InstructionTrust::Conversation),
        PromptSectionId::ExtraSystem | PromptSectionId::Dynamic(_) => {
            turn_context(InstructionTrust::External)
        }
    }
}

fn instruction_trust(id: &PromptSectionId) -> InstructionTrust {
    match id {
        PromptSectionId::SoulCore
        | PromptSectionId::IdentityCore
        | PromptSectionId::UserPersona
        | PromptSectionId::AgentsMd
        | PromptSectionId::SkillsRuntimePrompt => InstructionTrust::Workspace,
        PromptSectionId::MemoryRecall
        | PromptSectionId::ThreadContext
        | PromptSectionId::DynamicContext => InstructionTrust::Conversation,
        PromptSectionId::ExtraSystem | PromptSectionId::Dynamic(_) => InstructionTrust::External,
        _ => InstructionTrust::Pioneer,
    }
}

// Only native turn facts use this layout. CLI and governing documents keep
// their existing renderer; section content and inventory identity stay intact.
fn render_turn_sections(sections: &[PromptSection], profile: PromptProfile) -> String {
    if profile == PromptProfile::CliRuntime {
        return render_sections(sections);
    }
    let mut skills = Vec::new();
    let mut tools = Vec::new();
    let mut other = Vec::new();
    for section in sections
        .iter()
        .filter(|section| !section.content.trim().is_empty())
    {
        match &section.id {
            PromptSectionId::SkillsRuntimePrompt | PromptSectionId::SelectedSkills => {
                skills.push(section.content.trim().to_owned());
            }
            PromptSectionId::SelectedCapabilities => tools.push(section.content.trim().to_owned()),
            PromptSectionId::Dynamic(id)
                if id.as_str() == crate::REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID =>
            {
                tools.push(format!(
                    "### {}\n\n{}",
                    section.title.trim(),
                    section.content.trim()
                ));
            }
            _ => other.push(render_sections(std::slice::from_ref(section))),
        }
    }
    let mut groups = Vec::new();
    if !skills.is_empty() {
        groups.push(format!("## Skills\n\n{}", skills.join("\n\n")));
    }
    if !tools.is_empty() {
        groups.push(format!("## Tools\n\n{}", tools.join("\n\n")));
    }
    groups.extend(other);
    groups.join("\n\n")
}

fn compile_rendered_channel(
    prompt_sections: &[PromptSection],
    plan_sections: &[InstructionSectionPlan],
    channel: InstructionDeliveryChannel,
    profile: PromptProfile,
) -> CompiledInstructionPayload {
    let mut selected = prompt_sections
        .iter()
        .zip(plan_sections)
        .filter(|(_, plan)| plan.channel == channel)
        .map(|(section, _)| section.clone())
        .collect::<Vec<_>>();
    // Frozen governing sections may be joined with freshly materialized
    // operating contracts in another order. Sort section objects, not text;
    // equal keys preserve source/override order. Persisted payload replay
    // bypasses this renderer and remains byte-for-byte unchanged.
    selected.sort_by_key(|section| section.id.instruction_order());
    let text = if channel == InstructionDeliveryChannel::TurnContext {
        render_turn_sections(&selected, profile)
    } else {
        render_sections(&selected)
    };
    CompiledInstructionPayload {
        fingerprint: sha256_hex(text.as_str()),
        text,
        section_ids: selected
            .iter()
            .map(|section| section.id.manifest_id())
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::{InstructionDeliveryChannel, InstructionTrust, compile_instruction_delivery_plan};
    use crate::fingerprint::sha256_hex;
    use crate::{
        CompiledPromptBundle, PromptProfile, PromptSection, PromptSectionId, PromptStability,
    };

    fn bundle(profile: PromptProfile, sections: Vec<PromptSection>) -> CompiledPromptBundle {
        let full_system_text = if profile == PromptProfile::CliRuntime {
            String::new()
        } else {
            "native prompt bytes\n\n<!-- PROMT_CACHE_BOUNDARY -->\n\nnative dynamic bytes"
                .to_owned()
        };
        CompiledPromptBundle {
            compiler_version: "test",
            profile,
            stable_system_text: String::new(),
            dynamic_system_text: String::new(),
            fingerprint_stable: sha256_hex(""),
            fingerprint_dynamic: sha256_hex(""),
            fingerprint_full: sha256_hex(full_system_text.as_str()),
            full_system_text,
            boundary_marker: "",
            sections,
            source_manifest: Vec::new(),
            diagnostics: Vec::new(),
        }
    }

    fn section(id: PromptSectionId, content: &str) -> PromptSection {
        PromptSection {
            id,
            stability: PromptStability::Dynamic,
            title: "Section".to_owned(),
            content: content.to_owned(),
            sources: Vec::new(),
        }
    }

    #[test]
    fn cli_profile_separates_provider_instructions_from_turn_context() {
        let plan = compile_instruction_delivery_plan(bundle(
            PromptProfile::CliRuntime,
            vec![
                section(
                    PromptSectionId::PioneerCliRuntimeInstructions,
                    "trusted policy",
                ),
                section(PromptSectionId::SelectedCapabilities, "discover tools"),
                section(PromptSectionId::ThreadContext, "user: untrusted text"),
            ],
        ))
        .expect("compile plan");

        assert!(plan.provider_instructions.text.contains("trusted policy"));
        assert!(plan.turn_context.text.contains("discover tools"));
        assert!(!plan.provider_instructions.text.contains("discover tools"));
        assert!(!plan.provider_instructions.text.contains("untrusted text"));
        assert!(plan.turn_context.text.contains("untrusted text"));
        assert!(!plan.turn_context.text.contains("trusted policy"));
        assert_ne!(
            plan.provider_instructions.fingerprint,
            plan.turn_context.fingerprint
        );
    }

    #[test]
    fn native_profiles_preserve_governing_rules_and_separate_turn_facts() {
        for profile in [
            PromptProfile::AssistantFull,
            PromptProfile::AssistantMinimal,
            PromptProfile::AssistantNone,
        ] {
            let bundle = bundle(
                profile,
                vec![
                    section(PromptSectionId::ExtraSystem, "external system text"),
                    section(PromptSectionId::MemoryRecall, "memory operating contract"),
                    section(PromptSectionId::DynamicContext, "current date and cwd"),
                    section(
                        PromptSectionId::SkillsRuntimePrompt,
                        "[Internal Skill References]\n- pioneer/tasks: `skill:exact`\n  version: 2; package fingerprint: sha256:fixture; binding: system",
                    ),
                    section(
                        PromptSectionId::SelectedCapabilities,
                        "read_file unavailable",
                    ),
                    section(
                        PromptSectionId::Dynamic(
                            crate::PromptDynamicSectionId::new(
                                crate::REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID,
                            )
                            .unwrap(),
                        ),
                        "call request_tools with domains: [\"task\"]",
                    ),
                ],
            );

            let plan = compile_instruction_delivery_plan(bundle).expect("compile native plan");

            assert!(plan.is_native_api_profile());
            assert!(
                plan.provider_instructions
                    .text
                    .contains("external system text")
            );
            assert!(
                plan.provider_instructions
                    .text
                    .contains("memory operating contract")
            );
            assert!(
                !plan
                    .provider_instructions
                    .text
                    .contains("current date and cwd")
            );
            assert!(plan.turn_context.text.contains("current date and cwd"));
            assert!(plan.turn_context.text.starts_with("## Skills\n\n[Internal Skill References]\n- pioneer/tasks: `skill:exact`\n  version: 2; package fingerprint: sha256:fixture; binding: system\n\n## Tools\n\nread_file unavailable\n\n### Section\n\ncall request_tools with domains: [\"task\"]"));
            assert!(!plan.turn_context.text.contains("Selected Capabilities"));
            assert!(!plan.provider_instructions.text.contains("skill:exact"));

            assert_eq!(
                plan.sections[0].channel,
                InstructionDeliveryChannel::ProviderInstructions
            );
            assert_eq!(plan.sections[0].trust, InstructionTrust::External);
        }
    }

    #[test]
    fn unknown_dynamic_sections_are_never_provider_instructions_for_cli() {
        let id = crate::PromptDynamicSectionId::new("external_extension").unwrap();
        let plan = compile_instruction_delivery_plan(bundle(
            PromptProfile::CliRuntime,
            vec![section(PromptSectionId::Dynamic(id), "external text")],
        ))
        .expect("compile plan");

        assert!(plan.provider_instructions.is_empty());
        assert_eq!(
            plan.sections[0].channel,
            InstructionDeliveryChannel::TurnContext
        );
        assert_eq!(plan.sections[0].trust, InstructionTrust::External);
    }

    #[test]
    fn freeform_extra_system_text_is_never_provider_instructions_for_cli() {
        let plan = compile_instruction_delivery_plan(bundle(
            PromptProfile::CliRuntime,
            vec![section(PromptSectionId::ExtraSystem, "untyped extra text")],
        ))
        .expect("compile plan");

        assert!(plan.provider_instructions.is_empty());
        assert!(plan.turn_context.text.contains("untyped extra text"));
        assert_eq!(
            plan.sections[0].channel,
            InstructionDeliveryChannel::TurnContext
        );
        assert_eq!(plan.sections[0].trust, InstructionTrust::External);
    }

    #[test]
    fn inventory_uses_only_retained_bounded_slice_and_reports_replacements_and_removal() {
        let initial = compile_instruction_delivery_plan(bundle(
            PromptProfile::AssistantFull,
            vec![
                section(
                    PromptSectionId::SkillsRuntimePrompt,
                    "skill:exact-v1 description one",
                ),
                section(PromptSectionId::DynamicContext, "turn one"),
            ],
        ))
        .unwrap();
        let same_inventory = compile_instruction_delivery_plan(bundle(
            PromptProfile::AssistantFull,
            vec![
                section(
                    PromptSectionId::SkillsRuntimePrompt,
                    "skill:exact-v1 description one",
                ),
                section(PromptSectionId::DynamicContext, "turn two"),
            ],
        ))
        .unwrap();
        let delta = same_inventory.turn_context_with_inventory_baseline(Some(&initial.bundle));
        assert!(!delta.text.contains("skill:exact-v1"));
        assert!(delta.text.contains("turn two"));
        // A lost baseline (e.g. compaction/fork) must resend the bounded slice.
        assert!(
            same_inventory
                .turn_context_with_inventory_baseline(None)
                .text
                .contains("skill:exact-v1")
        );
        // Same readable name: each reference, immutable-version and
        // description-only change must be announced independently.
        for changed_card in [
            "skill:exact-v2 description one",
            "skill:exact-v1 description two",
            "skill:exact-v1 description one; immutable version 2",
        ] {
            let changed = compile_instruction_delivery_plan(bundle(
                PromptProfile::AssistantFull,
                vec![section(PromptSectionId::SkillsRuntimePrompt, changed_card)],
            ))
            .unwrap();
            let delta = changed.turn_context_with_inventory_baseline(Some(&initial.bundle));
            assert!(delta.text.contains(changed_card));
            assert!(delta.text.contains("replaces the previously shown slice"));
        }
        let revoked =
            compile_instruction_delivery_plan(bundle(PromptProfile::AssistantFull, vec![]))
                .unwrap();
        let delta = revoked.turn_context_with_inventory_baseline(Some(&initial.bundle));
        assert!(delta.text.contains("no longer available"));
        assert!(!delta.text.contains("skill:exact-v1"));
        assert_eq!(initial.turn_context.text.matches("turn one").count(), 1);
    }

    #[test]
    fn compiler_clipped_inventory_never_proves_the_unshown_tail_known() {
        let content = format!("{}\nunshown-card", "visible ".repeat(20));
        let compile = |max_chars| {
            compile_instruction_delivery_plan(
                crate::compile_prompt(crate::PromptCompileInput {
                    workspace_root: std::env::temp_dir().join("pioneer_clipped_inventory_fixture"),
                    profile: PromptProfile::AssistantFull,
                    skills_prompt: None,
                    retry_instruction: None,
                    include_tool_recovery_policy: false,
                    include_task_orchestration_policy: false,
                    include_threads_policy: false,
                    include_html_policy: false,
                    continue_generation_hint: false,
                    runtime_sections: vec![crate::PromptRuntimeSectionInput {
                        id: crate::PromptRuntimeSectionId::BuiltIn(
                            crate::PromptRuntimeBuiltInSectionId::SelectedCapabilities,
                        ),
                        title: None,
                        content: content.clone(),
                        max_chars: Some(max_chars),
                        truncated: false,
                    }],
                    dynamic_sections: vec![],
                    dynamic_context: None,
                    extra_system: None,
                    limits: crate::PromptLimits::default(),
                })
                .unwrap(),
            )
            .unwrap()
        };
        let clipped = compile(32);
        assert!(!clipped.turn_context.text.contains("unshown-card"));
        let repeated = compile(32).turn_context_with_inventory_baseline(Some(&clipped.bundle));
        assert!(repeated.text.contains("visible"));
        assert!(!repeated.text.contains("unshown-card"));
        let expanded = compile(1_000).turn_context_with_inventory_baseline(Some(&clipped.bundle));
        assert!(expanded.text.contains("unshown-card"));
    }

    #[test]
    fn debug_redacts_provider_and_conversation_text() {
        let provider_canary = "provider-prompt-canary";
        let context_canary = "conversation-context-canary";
        let plan = compile_instruction_delivery_plan(bundle(
            PromptProfile::CliRuntime,
            vec![
                section(
                    PromptSectionId::PioneerCliRuntimeInstructions,
                    provider_canary,
                ),
                section(PromptSectionId::ThreadContext, context_canary),
            ],
        ))
        .expect("compile plan");

        let debug = format!("{plan:?}");
        assert!(!debug.contains(provider_canary));
        assert!(!debug.contains(context_canary));
        assert!(debug.contains("[REDACTED]"));
    }
}
