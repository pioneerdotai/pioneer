use crate::fingerprint::sha256_hex;
use crate::render::text::render_sections;
use crate::{CompiledPromptBundle, PromptProfile, PromptSection, PromptSectionId, PromptStability};
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
/// Native API profiles preserve the compiler's exact full system prompt as the
/// provider instruction payload. CLI profiles split governing instructions
/// from non-governing turn context before a provider adapter maps the former
/// to its elevated transport.
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

    let provider_instructions = if cli_profile {
        compile_rendered_channel(
            bundle.sections.as_slice(),
            sections.as_slice(),
            InstructionDeliveryChannel::ProviderInstructions,
        )
    } else {
        compile_native_provider_instructions(&bundle, sections.as_slice())?
    };
    let turn_context = compile_rendered_channel(
        bundle.sections.as_slice(),
        sections.as_slice(),
        InstructionDeliveryChannel::TurnContext,
    );

    if provider_instructions.is_empty()
        && sections.iter().any(|section| {
            section.channel == InstructionDeliveryChannel::ProviderInstructions && section.required
        })
    {
        bail!("required provider instructions compiled to an empty payload");
    }
    if !cli_profile && !turn_context.is_empty() {
        bail!("native API prompt profiles cannot emit user-level turn context");
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

fn native_instruction_policy(section: &PromptSection) -> InstructionPolicy {
    let scope = match section.stability {
        PromptStability::Stable => InstructionScope::Thread,
        PromptStability::Dynamic => InstructionScope::Turn,
    };
    provider_instructions(scope, instruction_trust(&section.id))
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
        | PromptSectionId::TasksPolicy
        | PromptSectionId::PioneerCliRuntimeInstructions => {
            provider_instructions(InstructionScope::Thread, InstructionTrust::Pioneer)
        }
        PromptSectionId::SoulCore
        | PromptSectionId::IdentityCore
        | PromptSectionId::UserPersona
        | PromptSectionId::AgentsMd
        | PromptSectionId::SkillsRuntimePrompt => {
            provider_instructions(InstructionScope::Thread, InstructionTrust::Workspace)
        }
        PromptSectionId::SelectedSkills
        | PromptSectionId::SelectedCapabilities
        | PromptSectionId::CurrentPermissions
        | PromptSectionId::RecoveryContinuation
        | PromptSectionId::ExecutionContinuation
        | PromptSectionId::RetryRuntimeInstruction => {
            provider_instructions(InstructionScope::Turn, InstructionTrust::Pioneer)
        }
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

fn compile_native_provider_instructions(
    bundle: &CompiledPromptBundle,
    sections: &[InstructionSectionPlan],
) -> Result<CompiledInstructionPayload> {
    let fingerprint = sha256_hex(bundle.full_system_text.as_str());
    if fingerprint != bundle.fingerprint_full {
        bail!("native API prompt bundle has an invalid full-system fingerprint");
    }
    Ok(CompiledInstructionPayload {
        text: bundle.full_system_text.clone(),
        fingerprint,
        section_ids: sections
            .iter()
            .filter(|section| section.channel == InstructionDeliveryChannel::ProviderInstructions)
            .map(|section| section.section_id.clone())
            .collect(),
    })
}

fn compile_rendered_channel(
    prompt_sections: &[PromptSection],
    plan_sections: &[InstructionSectionPlan],
    channel: InstructionDeliveryChannel,
) -> CompiledInstructionPayload {
    let selected = prompt_sections
        .iter()
        .zip(plan_sections)
        .filter(|(_, plan)| plan.channel == channel)
        .map(|(section, _)| section.clone())
        .collect::<Vec<_>>();
    let text = render_sections(selected.as_slice());
    CompiledInstructionPayload {
        fingerprint: sha256_hex(text.as_str()),
        text,
        section_ids: plan_sections
            .iter()
            .filter(|section| section.channel == channel)
            .map(|section| section.section_id.clone())
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
        assert!(plan.provider_instructions.text.contains("discover tools"));
        assert!(!plan.provider_instructions.text.contains("untrusted text"));
        assert!(plan.turn_context.text.contains("untrusted text"));
        assert!(!plan.turn_context.text.contains("trusted policy"));
        assert_ne!(
            plan.provider_instructions.fingerprint,
            plan.turn_context.fingerprint
        );
    }

    #[test]
    fn native_profiles_preserve_exact_compiled_system_prompt() {
        for profile in [
            PromptProfile::AssistantFull,
            PromptProfile::AssistantMinimal,
            PromptProfile::AssistantNone,
        ] {
            let bundle = bundle(
                profile,
                vec![section(
                    PromptSectionId::ExtraSystem,
                    "external system text",
                )],
            );
            let expected = bundle.full_system_text.clone();
            let expected_fingerprint = bundle.fingerprint_full.clone();
            let plan = compile_instruction_delivery_plan(bundle).expect("compile native plan");

            assert!(plan.is_native_api_profile());
            assert_eq!(plan.provider_instructions.text, expected);
            assert_eq!(plan.provider_instructions.fingerprint, expected_fingerprint);
            assert!(plan.turn_context.is_empty());
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
