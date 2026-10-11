use crate::fingerprint::sha256_hex;
use crate::render::text::render_sections;
use crate::{
    CompiledInstructionDeliveryPlan, PromptRuntimeBuiltInSectionId, PromptRuntimeSectionId,
    PromptRuntimeSectionInput, PromptSection, PromptSectionId, PromptStability,
    compile_instruction_delivery_plan,
};
use anyhow::{Result, ensure};

/// These are the existing expanded execution-request limits, applied before
/// rendering or cloning. The durable initial owner additionally accounts the
/// encoded plan against the *remaining* snapshot allowance.
const MAX_BYTES: usize = pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES;
const MAX_SECTIONS: usize = 256;

fn check_render_sizes<'a>(sizes: impl Iterator<Item = (&'a str, &'a str)>) -> Result<()> {
    let mut remaining = MAX_BYTES.checked_sub(256).expect("boundary allowance");
    let mut count = 0;
    for (title, content) in sizes {
        count += 1;
        ensure!(
            count <= MAX_SECTIONS,
            "native prompt section limit exceeded"
        );
        // Reserve exact rendering punctuation and untrimmed UTF-8 bytes.
        for bytes in [title.len(), content.len(), 7] {
            remaining = remaining
                .checked_sub(bytes)
                .ok_or_else(|| anyhow::anyhow!("native prompt byte limit exceeded"))?;
        }
    }
    Ok(())
}

fn check_sections(sections: &[PromptSection]) -> Result<()> {
    check_render_sizes(
        sections
            .iter()
            .map(|s| (s.title.as_str(), s.content.as_str())),
    )
}

/// Validate both exact instruction bytes and their section/channel projection.
/// A hash-only manifest is not an authoritative compiled prompt.
pub fn validate_native_instruction_plan(plan: &CompiledInstructionDeliveryPlan) -> Result<()> {
    ensure!(
        plan.is_native_api_profile(),
        "native prompt has a foreign delivery profile"
    );
    check_sections(&plan.bundle.sections)?;
    let bundle = &plan.bundle;
    ensure!(
        bundle.compiler_version.len() <= 64
            && !bundle.compiler_version.is_empty()
            && bundle.boundary_marker.len() <= 256,
        "native prompt codec identity is invalid"
    );
    let stable = render_sections(
        &bundle
            .sections
            .iter()
            .filter(|s| s.stability == PromptStability::Stable)
            .cloned()
            .collect::<Vec<_>>(),
    );
    let dynamic = render_sections(
        &bundle
            .sections
            .iter()
            .filter(|s| s.stability == PromptStability::Dynamic)
            .cloned()
            .collect::<Vec<_>>(),
    );
    ensure!(
        stable == bundle.stable_system_text
            && dynamic == bundle.dynamic_system_text
            && sha256_hex(&stable) == bundle.fingerprint_stable
            && sha256_hex(&dynamic) == bundle.fingerprint_dynamic,
        "native prompt section projection is invalid"
    );
    let full = join_channels(&stable, &dynamic, &bundle.boundary_marker);
    ensure!(
        full == bundle.full_system_text && sha256_hex(&full) == bundle.fingerprint_full,
        "native prompt instruction bytes are invalid"
    );
    ensure!(
        &compile_instruction_delivery_plan(bundle.clone())? == plan,
        "native prompt delivery projection is invalid"
    );
    Ok(())
}

fn join_channels(stable: &str, dynamic: &str, boundary: &str) -> String {
    match (stable.trim().is_empty(), dynamic.trim().is_empty()) {
        (true, true) => String::new(),
        (false, true) => stable.to_owned(),
        (true, false) => format!("{boundary}{dynamic}"),
        (false, false) => format!("{stable}{boundary}{dynamic}"),
    }
}

/// Reproject only named mutable control sections. There are no filesystem,
/// clock, hook, configuration, or provider reads here. Base sources, clock,
/// skill/context outputs and their trust remain the originally compiled ones.
pub fn reproject_native_instruction_plan(
    base: &CompiledInstructionDeliveryPlan,
    retry_instruction: Option<&str>,
    tools_enabled: bool,
    continuation: Option<&PromptRuntimeSectionInput>,
) -> Result<CompiledInstructionDeliveryPlan> {
    reproject(base, retry_instruction, tools_enabled, continuation, None)
}

/// The caller supplies only fixed typed host directives; observation bytes stay
/// in the untrusted native data channel. Initial source sections remain exact.
pub fn reproject_native_control_plan(
    base: &CompiledInstructionDeliveryPlan,
    instructions: &str,
) -> Result<CompiledInstructionDeliveryPlan> {
    let retry = base
        .bundle
        .sections
        .iter()
        .find(|section| section.id == PromptSectionId::RetryRuntimeInstruction)
        .map(|section| section.content.as_str());
    reproject(base, retry, true, None, Some(instructions))
}

fn reproject(
    base: &CompiledInstructionDeliveryPlan,
    retry_instruction: Option<&str>,
    tools_enabled: bool,
    continuation: Option<&PromptRuntimeSectionInput>,
    controls: Option<&str>,
) -> Result<CompiledInstructionDeliveryPlan> {
    ensure!(
        controls.is_none_or(|s| s.len() <= 32 * 1024),
        "native controls byte limit exceeded"
    );
    validate_native_instruction_plan(base)?;
    let retry = retry_instruction.map(str::trim).filter(|s| !s.is_empty());
    ensure!(
        retry.is_none_or(|s| s.len() <= MAX_BYTES),
        "native retry byte limit exceeded"
    );
    if let Some(section) = continuation {
        ensure!(
            section.id
                == PromptRuntimeSectionId::BuiltIn(
                    PromptRuntimeBuiltInSectionId::ExecutionContinuation
                )
                && section.content.len() <= MAX_BYTES
                && section.title.as_ref().is_none_or(|s| s.len() <= 256)
                && !section.truncated,
            "native continuation section is invalid"
        );
    }
    let retained = |section: &&PromptSection| {
        section.id != PromptSectionId::RetryRuntimeInstruction
            && (controls.is_none() || section.id != PromptSectionId::NativeRuntimeControls)
            && (tools_enabled
                || section.id.manifest_id() != crate::REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID)
            && (continuation.is_none() || section.id != PromptSectionId::ExecutionContinuation)
    };
    // Account the entire projected request before allocating any new section.
    let continuation_title =
        continuation.map(|s| s.title.clone().unwrap_or_else(|| s.id.default_title()));
    check_render_sizes(
        base.bundle
            .sections
            .iter()
            .filter(retained)
            .map(|s| (s.title.as_str(), s.content.as_str()))
            .chain(
                continuation
                    .filter(|s| !s.content.trim().is_empty())
                    .map(|s| {
                        (
                            continuation_title.as_deref().expect("validated title"),
                            s.content.as_str(),
                        )
                    }),
            )
            .chain(retry.map(|s| (crate::content::SECTION_TITLE_RETRY_INSTRUCTION, s)))
            .chain(
                controls
                    .filter(|s| !s.trim().is_empty())
                    .map(|s| ("Native runtime controls", s)),
            ),
    )?;
    let mut bundle = base.bundle.clone();
    bundle.sections.retain(|section| retained(&section));
    if let Some(section) = continuation.filter(|section| !section.content.trim().is_empty()) {
        bundle.sections.push(PromptSection {
            id: PromptSectionId::ExecutionContinuation,
            stability: PromptStability::Dynamic,
            title: section
                .title
                .clone()
                .unwrap_or_else(|| section.id.default_title()),
            content: section.content.clone(),
            sources: Vec::new(),
        });
    }
    if let Some(content) = retry {
        bundle.sections.push(PromptSection {
            id: PromptSectionId::RetryRuntimeInstruction,
            stability: PromptStability::Dynamic,
            title: crate::content::SECTION_TITLE_RETRY_INSTRUCTION.to_owned(),
            content: content.to_owned(),
            sources: Vec::new(),
        });
    }
    if let Some(content) = controls.filter(|s| !s.trim().is_empty()) {
        bundle.sections.push(PromptSection {
            id: PromptSectionId::NativeRuntimeControls,
            stability: PromptStability::Dynamic,
            title: "Native runtime controls".to_owned(),
            content: content.to_owned(),
            sources: Vec::new(),
        });
    }
    check_sections(&bundle.sections)?;
    bundle.dynamic_system_text = render_sections(
        &bundle
            .sections
            .iter()
            .filter(|section| section.stability == PromptStability::Dynamic)
            .cloned()
            .collect::<Vec<_>>(),
    );
    bundle.full_system_text = join_channels(
        &bundle.stable_system_text,
        &bundle.dynamic_system_text,
        &bundle.boundary_marker,
    );
    bundle.fingerprint_dynamic = sha256_hex(&bundle.dynamic_system_text);
    bundle.fingerprint_full = sha256_hex(&bundle.full_system_text);
    compile_instruction_delivery_plan(bundle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CompiledPromptBundle, PromptProfile, PromptSourceManifestEntry, PromptSourceStatus,
    };

    fn accepted() -> CompiledInstructionDeliveryPlan {
        let section = |id, stability, title: &str, content: &str| PromptSection {
            id,
            stability,
            title: title.to_owned(),
            content: content.to_owned(),
            sources: Vec::new(),
        };
        let sections = vec![
            section(
                PromptSectionId::IdentityBase,
                PromptStability::Stable,
                "Identity",
                "original governing bytes",
            ),
            section(
                PromptSectionId::ExtraSystem,
                PromptStability::Dynamic,
                "Runtime",
                "Current date/time: original-clock",
            ),
            section(
                PromptSectionId::SkillsRuntimePrompt,
                PromptStability::Dynamic,
                "Skills",
                "accepted skill bytes",
            ),
            section(
                PromptSectionId::Dynamic(
                    crate::PromptDynamicSectionId::new(
                        crate::REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID,
                    )
                    .unwrap(),
                ),
                PromptStability::Dynamic,
                "Hidden domains",
                "original catalog",
            ),
        ];
        let stable = render_sections(&sections[..1]);
        let dynamic = render_sections(&sections[1..]);
        let full = join_channels(&stable, &dynamic, "\n<original-boundary>\n");
        compile_instruction_delivery_plan(CompiledPromptBundle {
            compiler_version: "original-compiler".to_owned(),
            profile: PromptProfile::AssistantFull,
            full_system_text: full.clone(),
            stable_system_text: stable.clone(),
            dynamic_system_text: dynamic.clone(),
            boundary_marker: "\n<original-boundary>\n".to_owned(),
            fingerprint_stable: sha256_hex(&stable),
            fingerprint_dynamic: sha256_hex(&dynamic),
            fingerprint_full: sha256_hex(&full),
            sections,
            source_manifest: vec![PromptSourceManifestEntry {
                file: "accepted.md".to_owned(),
                path: "/unavailable/after/restart".to_owned(),
                status: PromptSourceStatus::Loaded,
                chars: 25,
            }],
            diagnostics: Vec::new(),
        })
        .unwrap()
    }

    #[test]
    fn native_runtime_controls_replace_only_the_named_pioneer_instruction_section() {
        let base = accepted();
        let retry =
            reproject_native_instruction_plan(&base, Some("original retry policy"), false, None)
                .unwrap();
        let control = reproject_native_control_plan(&retry, "fixed host directive one").unwrap();
        assert_eq!(
            control.bundle.stable_system_text,
            base.bundle.stable_system_text
        );
        assert_eq!(control.bundle.source_manifest, base.bundle.source_manifest);
        assert!(
            control
                .provider_instructions
                .text
                .contains("original retry policy")
        );
        let section = control
            .sections
            .iter()
            .find(|section| section.section_id == "native_runtime_controls");
        // The typed delivery plan, rather than a chat role or display label, owns authority.
        let section = section.unwrap();
        assert_eq!(
            section.channel,
            crate::InstructionDeliveryChannel::ProviderInstructions
        );
        assert_eq!(section.trust, crate::InstructionTrust::Pioneer);
        assert!(section.required);
        let replaced = reproject_native_control_plan(&control, "fixed host directive two").unwrap();
        assert!(
            !replaced
                .provider_instructions
                .text
                .contains("fixed host directive one")
        );
        assert!(
            replaced
                .provider_instructions
                .text
                .contains("fixed host directive two")
        );
        assert_eq!(reproject_native_control_plan(&control, "").unwrap(), retry);
    }

    #[test]
    fn cold_owned_plan_preserves_base_clock_sources_and_delivery() {
        let base = accepted();
        let json = serde_json::to_string(&base).unwrap();
        let cold: CompiledInstructionDeliveryPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(cold, base);
        validate_native_instruction_plan(&cold).unwrap();
        let retry =
            reproject_native_instruction_plan(&cold, Some("explicit retry control"), false, None)
                .unwrap();
        assert_eq!(retry.bundle.compiler_version, "original-compiler");
        assert_eq!(
            retry.bundle.stable_system_text,
            base.bundle.stable_system_text
        );
        assert_eq!(retry.bundle.source_manifest, base.bundle.source_manifest);
        assert!(retry.provider_instructions.text.contains("original-clock"));
        assert!(
            retry
                .provider_instructions
                .text
                .contains("accepted skill bytes")
        );
        assert!(
            retry
                .provider_instructions
                .text
                .contains("explicit retry control")
        );
        assert!(
            !retry
                .provider_instructions
                .text
                .contains("original catalog")
        );
        assert!(retry.turn_context.is_empty());
        assert_ne!(
            retry.provider_instructions.fingerprint,
            base.provider_instructions.fingerprint
        );
        // Each projection starts from the accepted base, not the previous delta.
        assert_eq!(
            reproject_native_instruction_plan(&cold, None, true, None).unwrap(),
            base
        );
    }

    #[test]
    fn continuation_can_replace_only_named_control_section() {
        let base = accepted();
        let mut control = PromptRuntimeSectionInput {
            id: PromptRuntimeSectionId::BuiltIn(
                PromptRuntimeBuiltInSectionId::ExecutionContinuation,
            ),
            title: Some("Continuation".to_owned()),
            content: "observed sequence 2".to_owned(),
            max_chars: None,
            truncated: false,
        };
        let continued =
            reproject_native_instruction_plan(&base, None, true, Some(&control)).unwrap();
        assert!(
            continued
                .provider_instructions
                .text
                .contains("observed sequence 2")
        );
        assert_eq!(
            continued.bundle.sections[..base.bundle.sections.len()],
            base.bundle.sections
        );
        control.id =
            PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::CurrentPermissions);
        assert!(reproject_native_instruction_plan(&base, None, true, Some(&control)).is_err());
        control.id =
            PromptRuntimeSectionId::BuiltIn(PromptRuntimeBuiltInSectionId::ExecutionContinuation);
        control.truncated = true;
        assert!(reproject_native_instruction_plan(&base, None, true, Some(&control)).is_err());
    }

    #[test]
    fn tampered_section_bytes_or_delivery_never_become_an_accepted_base() {
        let mut base = accepted();
        base.bundle.sections[0].content.push_str(" altered");
        assert!(validate_native_instruction_plan(&base).is_err());
        let mut base = accepted();
        base.provider_instructions.section_ids.clear();
        assert!(reproject_native_instruction_plan(&base, None, true, None).is_err());
        let mut base = accepted();
        base.sections[0].trust = crate::InstructionTrust::External;
        assert!(validate_native_instruction_plan(&base).is_err());
    }

    #[test]
    fn expanded_utf8_delta_and_cardinality_are_rejected_before_projection() {
        let base = accepted();
        let utf8 = "я".repeat(MAX_BYTES / 2);
        assert!(reproject_native_instruction_plan(&base, Some(&utf8), true, None).is_err());
        let mut base = accepted();
        base.bundle.sections.extend(std::iter::repeat_n(
            base.bundle.sections[0].clone(),
            MAX_SECTIONS,
        ));
        assert!(validate_native_instruction_plan(&base).is_err());
    }
}
