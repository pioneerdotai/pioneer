use anyhow::{Context, Result, bail};
use pioneer_cli_agent_runtime::instructions::{
    CLIRuntimeElevatedInstructionTransport, CLIRuntimeElevatedInstructions,
};
use pioneer_crud::{
    CliRuntimeInstructionProjectionRecord, CrudStore, NewCliRuntimeInstructionProjection,
};
use pioneer_promt::{CompiledInstructionDeliveryPlan, InstructionDeliveryChannel};
use pioneer_protocol::CLIAgentRuntimeKind;

pub(crate) async fn persist_cli_runtime_instruction_projection(
    store: &CrudStore,
    turn_id: &str,
    runtime_kind: CLIAgentRuntimeKind,
    plan: &CompiledInstructionDeliveryPlan,
    instructions: &CLIRuntimeElevatedInstructions,
) -> Result<()> {
    if plan.provider_instructions.fingerprint != instructions.fingerprint()
        || plan.provider_instructions.text != instructions.text()
    {
        bail!("compiled instruction delivery plan does not match its CLI elevated projection");
    }
    let section_ids = plan
        .sections
        .iter()
        .filter(|section| section.channel == InstructionDeliveryChannel::ProviderInstructions)
        .map(|section| section.section_id.clone())
        .collect::<Vec<_>>();
    if section_ids.is_empty() {
        bail!("CLI elevated instruction projection has no governing sections");
    }
    let section_ids_json = serde_json::to_string(&section_ids)
        .context("failed to serialize CLI elevated instruction section ids")?;
    let compiler_version = plan.bundle.compiler_version.to_owned();
    let now = chrono::Utc::now().fixed_offset();
    let transport = CLIRuntimeElevatedInstructionTransport::for_runtime(runtime_kind);
    let runtime_kind_name = runtime_kind_label(runtime_kind);
    let persisted = store
        .insert_cli_runtime_instruction_projection_if_absent(NewCliRuntimeInstructionProjection {
            turn_id: turn_id.to_owned(),
            runtime_kind: runtime_kind_name.to_owned(),
            transport_kind: transport.as_str().to_owned(),
            instruction_text: instructions.text().to_owned(),
            instruction_fingerprint: instructions.fingerprint().to_owned(),
            section_ids_json: section_ids_json.clone(),
            compiler_version: compiler_version.clone(),
            created_at: now,
            updated_at: now,
        })
        .await
        .context("failed to persist CLI runtime instruction projection")?;
    if persisted.runtime_kind != runtime_kind_name
        || persisted.transport_kind != transport.as_str()
        || persisted.instruction_text != instructions.text()
        || persisted.instruction_fingerprint != instructions.fingerprint()
        || persisted.section_ids_json != section_ids_json
        || persisted.compiler_version != compiler_version
    {
        bail!("CLI runtime instruction projection conflicts with immutable turn state");
    }
    restore_cli_runtime_instruction_projection_record(&persisted, runtime_kind)?;
    Ok(())
}

pub(crate) async fn load_cli_runtime_instruction_projection(
    store: &CrudStore,
    turn_id: &str,
    runtime_kind: CLIAgentRuntimeKind,
) -> Result<CLIRuntimeElevatedInstructions> {
    let projection = store
        .get_cli_runtime_instruction_projection(turn_id)
        .await
        .context("failed to load CLI runtime instruction projection")?
        .with_context(|| {
            format!("CLI runtime instruction projection for turn `{turn_id}` is missing")
        })?;
    restore_cli_runtime_instruction_projection_record(&projection, runtime_kind)
}

fn restore_cli_runtime_instruction_projection_record(
    projection: &CliRuntimeInstructionProjectionRecord,
    runtime_kind: CLIAgentRuntimeKind,
) -> Result<CLIRuntimeElevatedInstructions> {
    let expected_runtime_kind = runtime_kind_label(runtime_kind);
    if projection.runtime_kind != expected_runtime_kind {
        bail!(
            "CLI runtime instruction projection kind `{}` does not match `{expected_runtime_kind}`",
            projection.runtime_kind
        );
    }
    let expected_transport =
        CLIRuntimeElevatedInstructionTransport::for_runtime(runtime_kind).as_str();
    if projection.transport_kind != expected_transport {
        bail!(
            "CLI runtime instruction transport `{}` does not match `{expected_transport}`",
            projection.transport_kind
        );
    }
    let section_ids: Vec<String> = serde_json::from_str(projection.section_ids_json.as_str())
        .context("CLI runtime instruction section manifest is invalid")?;
    if section_ids.is_empty() || section_ids.iter().any(|section| section.trim().is_empty()) {
        bail!("CLI runtime instruction section manifest is empty or invalid");
    }
    if projection.compiler_version.trim().is_empty() {
        bail!("CLI runtime instruction compiler identity is empty");
    }
    CLIRuntimeElevatedInstructions::try_new(
        projection.instruction_text.clone(),
        projection.instruction_fingerprint.clone(),
    )
}

const fn runtime_kind_label(runtime_kind: CLIAgentRuntimeKind) -> &'static str {
    match runtime_kind {
        CLIAgentRuntimeKind::Codex => "codex",
        CLIAgentRuntimeKind::Claude => "claude",
    }
}

#[cfg(test)]
mod tests {
    use super::restore_cli_runtime_instruction_projection_record;
    use pioneer_cli_agent_runtime::instructions::CLIRuntimeElevatedInstructionTransport;
    use pioneer_crud::CliRuntimeInstructionProjectionRecord;
    use pioneer_protocol::CLIAgentRuntimeKind;
    use sha2::{Digest, Sha256};

    fn record(text: &str) -> CliRuntimeInstructionProjectionRecord {
        let now = chrono::Utc::now().fixed_offset();
        let fingerprint = Sha256::digest(text.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        CliRuntimeInstructionProjectionRecord {
            turn_id: "turn_1".to_owned(),
            runtime_kind: "codex".to_owned(),
            transport_kind: CLIRuntimeElevatedInstructionTransport::CodexTurnCollaborationMode
                .as_str()
                .to_owned(),
            instruction_text: text.to_owned(),
            instruction_fingerprint: fingerprint,
            section_ids_json: r#"["pioneer_cli_runtime_instructions"]"#.to_owned(),
            compiler_version: "test".to_owned(),
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn durable_projection_restores_only_an_exact_transport_and_digest() {
        let projection = record("governing text");
        let restored = restore_cli_runtime_instruction_projection_record(
            &projection,
            CLIAgentRuntimeKind::Codex,
        )
        .expect("restore exact projection");
        assert_eq!(restored.text(), "governing text");

        let mut corrupted = projection.clone();
        corrupted.instruction_text.push_str(" changed");
        assert!(
            restore_cli_runtime_instruction_projection_record(
                &corrupted,
                CLIAgentRuntimeKind::Codex
            )
            .is_err()
        );
        assert!(
            restore_cli_runtime_instruction_projection_record(
                &projection,
                CLIAgentRuntimeKind::Claude
            )
            .is_err()
        );
    }
}
