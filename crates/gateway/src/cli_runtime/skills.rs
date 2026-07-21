use std::{
    collections::HashMap,
    env, fs,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use pioneer_agent::{AgentMcpServerRef, AgentMcpToolRef};
use pioneer_protocol::{SkillId, TurnCapability, TurnCapabilityKind, TurnSkillBinding};
use pioneer_skills::{
    ExternalRuntimeReceiptConversionCandidate, ExternalRuntimeSkillReceiptEntry,
    compute_skill_folder_hash, ensure_external_runtime_receipt_v2,
    external_runtime_skill_is_current, find_external_runtime_receipt_destination_entry,
    remove_external_runtime_receipt_destination_entry, replace_external_runtime_skill,
    upsert_external_runtime_receipt_entry, write_external_runtime_receipt_atomic,
};
use tokio::sync::{Mutex, OwnedMutexGuard};

pub(crate) type CliRuntimeSkillDestinationLocks = Arc<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>>;

pub(crate) const CLI_RUNTIME_SYSTEM_SKILL_NOT_EXPORTABLE: &str =
    "cli_runtime.system_skill_not_exportable";
pub(crate) const CLI_RUNTIME_CLAUDE_SKILL_NOT_MODEL_INVOCABLE: &str =
    "cli_runtime.claude_skill_not_model_invocable";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CliRuntimeSystemSkillNotExportable {
    pub skill_slug: String,
    pub display_name: String,
}

impl std::fmt::Display for CliRuntimeSystemSkillNotExportable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{CLI_RUNTIME_SYSTEM_SKILL_NOT_EXPORTABLE}: required system skill `{}` (`{}`) is Pioneer-only at export_boundary stage",
            self.skill_slug, self.display_name
        )
    }
}

impl std::error::Error for CliRuntimeSystemSkillNotExportable {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CliRuntimeClaudeSkillNotModelInvocable {
    pub runtime_display_name: String,
    pub skill_display_name: String,
}

impl std::fmt::Display for CliRuntimeClaudeSkillNotModelInvocable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{CLI_RUNTIME_CLAUDE_SKILL_NOT_MODEL_INVOCABLE}: runtime `{}` cannot model-invoke selected skill `{}`",
            self.runtime_display_name, self.skill_display_name
        )
    }
}

impl std::error::Error for CliRuntimeClaudeSkillNotModelInvocable {}

pub(crate) fn ensure_cli_runtime_skills_exportable(
    resolved: &[pioneer_skills::ResolvedSkill],
) -> std::result::Result<&[pioneer_skills::ResolvedSkill], CliRuntimeSystemSkillNotExportable> {
    for skill in resolved {
        if !pioneer_skills::skill_implicit_invocation_editable(&skill.definition) {
            return Err(CliRuntimeSystemSkillNotExportable {
                skill_slug: skill.slug.clone(),
                display_name: skill.definition.identity.display_name.clone(),
            });
        }
    }
    Ok(resolved)
}

pub(crate) fn ensure_cli_runtime_skill_invocation_eligible(
    runtime_kind: pioneer_protocol::CLIAgentRuntimeKind,
    runtime_display_name: &str,
    resolved: &[pioneer_skills::ResolvedSkill],
) -> std::result::Result<(), CliRuntimeClaudeSkillNotModelInvocable> {
    if !matches!(runtime_kind, pioneer_protocol::CLIAgentRuntimeKind::Claude) {
        return Ok(());
    }
    for skill in resolved {
        if skill.definition.runtime.disable_model_invocation {
            return Err(CliRuntimeClaudeSkillNotModelInvocable {
                runtime_display_name: runtime_display_name.to_owned(),
                skill_display_name: skill.definition.identity.display_name.clone(),
            });
        }
    }
    Ok(())
}

pub(crate) fn cli_runtime_turn_skill_bindings(
    resolved: &[pioneer_skills::ResolvedSkill],
) -> Vec<TurnSkillBinding> {
    resolved
        .iter()
        .map(|skill| TurnSkillBinding {
            skill_id: skill.skill_id.clone(),
            skill_owner: skill.definition.identity.owner.clone(),
            skill_slug: skill.slug.clone(),
            skill_version: skill.definition.identity.version_hint.clone(),
            fingerprint: skill.definition.identity.fingerprint.clone(),
            source_kind: skill
                .definition
                .identity
                .source_kind
                .as_db_value()
                .to_owned(),
            resolved_reason: skill.reason.as_db_value().to_owned(),
        })
        .collect()
}

pub(crate) fn cli_runtime_native_skills_root(
    runtime: &pioneer_config::EffectiveGatewayCliAgentRuntimeInstanceConfig,
    runtime_kind: pioneer_protocol::CLIAgentRuntimeKind,
) -> Result<PathBuf> {
    let configured_root = match runtime_kind {
        pioneer_protocol::CLIAgentRuntimeKind::Codex => runtime.home_path.as_str(),
        pioneer_protocol::CLIAgentRuntimeKind::Claude => runtime
            .shadow_home_path
            .as_deref()
            .unwrap_or(runtime.home_path.as_str()),
    };
    let expanded = pioneer_cli_agent_runtime::process::expand_home_path(configured_root, None)?;
    normalized_destination(&expanded.join("skills"))
}

pub(crate) fn build_cli_runtime_skill_install_plans(
    runtime: &pioneer_config::EffectiveGatewayCliAgentRuntimeInstanceConfig,
    runtime_kind: pioneer_protocol::CLIAgentRuntimeKind,
    resolved: &[pioneer_skills::ResolvedSkill],
    receipt_path: &Path,
) -> Result<Vec<CliRuntimeSkillInstallPlan>> {
    let native_skills_root = cli_runtime_native_skills_root(runtime, runtime_kind)?;
    let mut destinations = std::collections::BTreeMap::<PathBuf, String>::new();
    let mut plans = Vec::with_capacity(resolved.len());
    for skill in resolved {
        let install_name = pioneer_skills::sanitize_name(&skill.definition.identity.name);
        let destination = normalized_destination(&native_skills_root.join(&install_name))?;
        if let Some(first_slug) = destinations.insert(destination.clone(), skill.slug.clone()) {
            anyhow::bail!(
                "cli_runtime.skill_destination_collision: skills `{first_slug}` and `{}` both target `{}`",
                skill.slug,
                destination.display()
            );
        }
        plans.push(CliRuntimeSkillInstallPlan {
            skill_id: skill.skill_id.clone(),
            owner: skill.definition.identity.owner.clone(),
            slug: skill.definition.identity.slug.clone(),
            source: PathBuf::from(&skill.definition.identity.skill_dir),
            destination,
            native_skills_root: native_skills_root.clone(),
            receipt_path: receipt_path.to_path_buf(),
            runtime_id: runtime.id.clone(),
            runtime_kind: match runtime_kind {
                pioneer_protocol::CLIAgentRuntimeKind::Codex => "codex",
                pioneer_protocol::CLIAgentRuntimeKind::Claude => "claude",
            }
            .to_owned(),
            install_name,
            skill_slug: skill.slug.clone(),
            source_kind: skill
                .definition
                .identity
                .source_kind
                .as_db_value()
                .to_owned(),
        });
    }
    Ok(plans)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CliRuntimeSkillAttachment {
    pub capability_id: String,
    pub label: Option<String>,
    pub skill_id: SkillId,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CliRuntimeCapabilityPartition {
    pub(crate) skills: Vec<CliRuntimeSkillAttachment>,
    pub(crate) mcp_servers: Vec<AgentMcpServerRef>,
    pub(crate) mcp_tools: Vec<AgentMcpToolRef>,
}

impl CliRuntimeCapabilityPartition {
    pub(crate) fn has_mcp(&self) -> bool {
        !self.mcp_servers.is_empty() || !self.mcp_tools.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CliRuntimeCombinedPreflightInput {
    pub(crate) capabilities: CliRuntimeCapabilityPartition,
    pub(crate) mcp_projection: Option<crate::turn_mcp::ResolvedMcpTurnProjection>,
}

impl CliRuntimeCombinedPreflightInput {
    pub(crate) fn exact_mcp_availability(&self) -> pioneer_agent::AgentMcpAvailability {
        self.mcp_projection
            .as_ref()
            .map(|projection| pioneer_agent::AgentMcpAvailability {
                available_mcp: projection.available_mcp.clone(),
                blocked_mcp: projection.blocked_mcp.clone(),
            })
            .unwrap_or_default()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CliRuntimeCombinedPreflightPlan {
    pub(crate) mcp_projection: Option<crate::turn_mcp::ResolvedMcpTurnProjection>,
    pub(crate) skill_install_plans: Vec<CliRuntimeSkillInstallPlan>,
    pub(crate) skill_bindings: Vec<TurnSkillBinding>,
}

pub(crate) fn partition_cli_runtime_capabilities(
    capabilities: &[TurnCapability],
) -> CliRuntimeCapabilityPartition {
    let mut partition = CliRuntimeCapabilityPartition::default();
    for capability in capabilities {
        match &capability.kind {
            TurnCapabilityKind::Skill { skill_id } => {
                partition.skills.push(CliRuntimeSkillAttachment {
                    capability_id: capability.id.clone(),
                    label: capability.label.clone(),
                    skill_id: skill_id.clone(),
                });
            }
            TurnCapabilityKind::McpServer { name, scope_kind } => {
                partition.mcp_servers.push(AgentMcpServerRef {
                    capability_id: capability.id.clone(),
                    label: capability.label.clone(),
                    name: name.clone(),
                    scope_kind: *scope_kind,
                });
            }
            TurnCapabilityKind::McpTool {
                server_name,
                raw_tool_name,
                scope_kind,
            } => {
                partition.mcp_tools.push(AgentMcpToolRef {
                    capability_id: capability.id.clone(),
                    label: capability.label.clone(),
                    server_name: server_name.clone(),
                    raw_tool_name: raw_tool_name.clone(),
                    scope_kind: *scope_kind,
                });
            }
        }
    }
    partition
}

pub(crate) fn new_cli_runtime_skill_destination_locks() -> CliRuntimeSkillDestinationLocks {
    Arc::new(Mutex::new(HashMap::new()))
}

fn normalized_destination(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .context("failed to resolve current directory for CLI runtime skill destination")?
            .join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::Normal(part) => normalized.push(part),
        }
    }
    Ok(normalized)
}

pub(crate) async fn acquire_cli_runtime_skill_destination_lock(
    locks: &CliRuntimeSkillDestinationLocks,
    destination: &Path,
) -> Result<OwnedMutexGuard<()>> {
    let key = normalized_destination(destination)?;
    let lock = {
        let mut locks = locks.lock().await;
        locks
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    };
    Ok(lock.lock_owned().await)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CliRuntimeSkillInstallPlan {
    pub skill_id: SkillId,
    pub owner: Option<String>,
    pub slug: String,
    pub source: PathBuf,
    pub destination: PathBuf,
    pub native_skills_root: PathBuf,
    pub receipt_path: PathBuf,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub install_name: String,
    pub skill_slug: String,
    pub source_kind: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CliRuntimeSkillInstallStatus {
    Current,
    Installed,
    Updated,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CliRuntimeSkillInstallResult {
    pub status: CliRuntimeSkillInstallStatus,
    pub install_name: String,
    pub source_folder_hash: String,
    pub installed_path: PathBuf,
    pub receipt_updated_at_unix_ms: u64,
}

fn unix_timestamp_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or_default()
}

fn planned_receipt_entry(
    plan: &CliRuntimeSkillInstallPlan,
    source_folder_hash: String,
    installed_at_unix_ms: u64,
    updated_at_unix_ms: u64,
) -> ExternalRuntimeSkillReceiptEntry {
    ExternalRuntimeSkillReceiptEntry {
        skill_id: plan.skill_id.clone(),
        owner: plan.owner.clone(),
        slug: plan.slug.clone(),
        runtime_id: plan.runtime_id.clone(),
        runtime_kind: plan.runtime_kind.clone(),
        native_skills_root: plan.native_skills_root.to_string_lossy().into_owned(),
        install_name: plan.install_name.clone(),
        skill_slug: plan.skill_slug.clone(),
        source_kind: plan.source_kind.clone(),
        source_folder_hash,
        install_path: plan.destination.to_string_lossy().into_owned(),
        installed_at_unix_ms,
        updated_at_unix_ms,
    }
}

pub(crate) async fn install_one_cli_runtime_skill(
    destination_locks: &CliRuntimeSkillDestinationLocks,
    skills_write_lock: &Arc<Mutex<()>>,
    receipt_conversion_candidates: &[ExternalRuntimeReceiptConversionCandidate],
    plan: &CliRuntimeSkillInstallPlan,
) -> Result<CliRuntimeSkillInstallResult> {
    let _destination_guard =
        acquire_cli_runtime_skill_destination_lock(destination_locks, &plan.destination).await?;
    let source_folder_hash = compute_skill_folder_hash(&plan.source)?;
    let expected = planned_receipt_entry(plan, source_folder_hash.clone(), 0, 0);

    let (receipt, previous) = {
        let _receipt_guard = skills_write_lock.clone().lock_owned().await;
        let receipt =
            ensure_external_runtime_receipt_v2(&plan.receipt_path, receipt_conversion_candidates)?;
        let previous = find_external_runtime_receipt_destination_entry(
            &receipt,
            &plan.native_skills_root,
            &plan.install_name,
        )?
        .cloned();
        (receipt, previous)
    };
    if let Some(entry) = previous.as_ref()
        && external_runtime_skill_is_current(&receipt, &expected, &plan.destination)?
    {
        return Ok(CliRuntimeSkillInstallResult {
            status: CliRuntimeSkillInstallStatus::Current,
            install_name: plan.install_name.clone(),
            source_folder_hash,
            installed_path: plan.destination.clone(),
            receipt_updated_at_unix_ms: entry.updated_at_unix_ms,
        });
    }

    {
        let _receipt_guard = skills_write_lock.clone().lock_owned().await;
        let mut receipt =
            ensure_external_runtime_receipt_v2(&plan.receipt_path, receipt_conversion_candidates)?;
        if remove_external_runtime_receipt_destination_entry(
            &mut receipt,
            &plan.native_skills_root,
            &plan.install_name,
        )?
        .is_some()
        {
            write_external_runtime_receipt_atomic(&plan.receipt_path, &receipt)?;
        }
    }

    replace_external_runtime_skill(&plan.source, &plan.destination)?;
    fs::read(plan.destination.join("SKILL.md")).with_context(|| {
        format!(
            "installed external runtime skill has no readable SKILL.md at `{}`",
            plan.destination.display()
        )
    })?;

    let updated_at_unix_ms = unix_timestamp_millis();
    let installed_at_unix_ms = previous
        .as_ref()
        .filter(|entry| entry.skill_id == plan.skill_id)
        .map(|entry| entry.installed_at_unix_ms)
        .unwrap_or(updated_at_unix_ms);
    let entry = planned_receipt_entry(
        plan,
        source_folder_hash.clone(),
        installed_at_unix_ms,
        updated_at_unix_ms,
    );
    {
        let _receipt_guard = skills_write_lock.clone().lock_owned().await;
        let mut receipt =
            ensure_external_runtime_receipt_v2(&plan.receipt_path, receipt_conversion_candidates)?;
        upsert_external_runtime_receipt_entry(&mut receipt, entry)?;
        write_external_runtime_receipt_atomic(&plan.receipt_path, &receipt)?;
    }

    Ok(CliRuntimeSkillInstallResult {
        status: if previous.is_some() {
            CliRuntimeSkillInstallStatus::Updated
        } else {
            CliRuntimeSkillInstallStatus::Installed
        },
        install_name: plan.install_name.clone(),
        source_folder_hash,
        installed_path: plan.destination.clone(),
        receipt_updated_at_unix_ms: updated_at_unix_ms,
    })
}

pub(crate) fn prepend_codex_installed_skill_items(
    installed: &[CliRuntimeSkillInstallResult],
    mapping: &mut pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputMapping,
) {
    if installed.is_empty() {
        return;
    }
    let mut prefix = Vec::with_capacity(installed.len());
    prefix.extend(installed.iter().map(|skill| {
        pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputItem::Skill {
            name: skill.install_name.clone(),
            path: skill
                .installed_path
                .join("SKILL.md")
                .to_string_lossy()
                .into_owned(),
        }
    }));
    prefix.append(&mut mapping.input);
    mapping.input = prefix;
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_skills::{
        SkillDependencies, SkillImplicitInvocationPolicy, SkillResolvedReason, SkillSourceKind,
        SkillTrustLevel,
        compile::{CompileSkillInput, compile_skill_definition},
        contract::default_skill_conformance,
        read_external_runtime_receipt,
    };
    use tokio::time::{Duration, timeout};

    fn test_skill_id(slug: &str, source_kind: SkillSourceKind) -> SkillId {
        let suffix = match source_kind {
            SkillSourceKind::System => 'S',
            SkillSourceKind::User => 'U',
            SkillSourceKind::Registry => 'R',
        };
        let mut value = slug
            .chars()
            .filter(char::is_ascii_alphanumeric)
            .collect::<String>();
        value.truncate(20);
        while value.len() < 20 {
            value.push(suffix);
        }
        value.push(suffix);
        SkillId::new(value).expect("valid CLI runtime test SkillId")
    }

    fn install_plan(root: &Path, name: &str) -> CliRuntimeSkillInstallPlan {
        let source = root.join(format!("source-{name}"));
        let native_skills_root = root.join("runtime/skills");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("SKILL.md"), format!("# {name}\n")).unwrap();
        CliRuntimeSkillInstallPlan {
            skill_id: test_skill_id(name, SkillSourceKind::Registry),
            owner: Some("registry".to_owned()),
            slug: name.to_owned(),
            source,
            destination: native_skills_root.join(name),
            native_skills_root,
            receipt_path: root.join("state/cli-runtime-skills-lock.json"),
            runtime_id: "runtime-1".to_owned(),
            runtime_kind: "codex".to_owned(),
            install_name: name.to_owned(),
            skill_slug: format!("registry/{name}"),
            source_kind: "registry".to_owned(),
        }
    }

    fn receipt_candidate(
        plan: &CliRuntimeSkillInstallPlan,
    ) -> ExternalRuntimeReceiptConversionCandidate {
        ExternalRuntimeReceiptConversionCandidate {
            skill_id: plan.skill_id.clone(),
            owner: plan.owner.clone(),
            slug: plan.slug.clone(),
            source_kind: plan.source_kind.clone(),
        }
    }

    fn skill_capability(slug: &str, source_kind: SkillSourceKind) -> TurnCapability {
        let skill_id = test_skill_id(slug, source_kind);
        TurnCapability {
            id: format!("skill:{skill_id}"),
            kind: TurnCapabilityKind::Skill {
                skill_id: skill_id.clone(),
            },
            label: Some(format!("label-{slug}")),
        }
    }

    fn resolved_skill(slug: &str, source_kind: SkillSourceKind) -> pioneer_skills::ResolvedSkill {
        let skill_id = test_skill_id(slug, source_kind);
        let definition = compile_skill_definition(CompileSkillInput {
            skill_id: skill_id.clone(),
            owner: Some("workspace".to_owned()),
            slug: slug.to_owned(),
            name: slug.to_owned(),
            display_name: format!("Display {slug}"),
            description: "description".to_owned(),
            body: "body".to_owned(),
            source_kind,
            source_root: "/tmp".to_owned(),
            skill_dir: format!("/tmp/{slug}"),
            skill_file: format!("/tmp/{slug}/SKILL.md"),
            version_hint: None,
            fingerprint: "fingerprint".to_owned(),
            user_invocable: true,
            disable_model_invocation: false,
            paths: Vec::new(),
            allowed_tools: Vec::new(),
            runtime_tools: Vec::new(),
            trust_level: SkillTrustLevel::Community,
            dependencies: SkillDependencies::default(),
            license: None,
            compatibility: None,
            metadata_raw: serde_json::json!({}),
            conformance: default_skill_conformance(),
        });
        pioneer_skills::ResolvedSkill {
            skill_id,
            slug: slug.to_owned(),
            reason: SkillResolvedReason::ExplicitCapability,
            definition,
        }
    }

    fn runtime_instance(
        kind: pioneer_config::GatewayCliAgentRuntimeKindConfig,
        home_path: String,
        shadow_home_path: Option<String>,
    ) -> pioneer_config::EffectiveGatewayCliAgentRuntimeInstanceConfig {
        pioneer_config::EffectiveGatewayCliAgentRuntimeInstanceConfig {
            id: "runtime-1".to_owned(),
            kind,
            display_name: "Runtime".to_owned(),
            enabled: true,
            binary_path: "runtime".to_owned(),
            home_path,
            shadow_home_path,
            custom_models: Vec::new(),
            app_server_args: Vec::new(),
            startup_probe_timeout_ms: 1_000,
            request_timeout_ms: 1_000,
            idle_session_ttl_secs: 60,
            event_channel_capacity: 32,
            stderr_ring_lines: 32,
            debug_native_events: false,
        }
    }

    fn installed_skill(name: &str, path: &str) -> CliRuntimeSkillInstallResult {
        CliRuntimeSkillInstallResult {
            status: CliRuntimeSkillInstallStatus::Current,
            install_name: name.to_owned(),
            source_folder_hash: "source-hash-not-prompt-content".to_owned(),
            installed_path: PathBuf::from(path),
            receipt_updated_at_unix_ms: 1,
        }
    }

    #[test]
    fn combined_cli_preflight_partition_preserves_skills_and_empty_fast_path() {
        assert_eq!(
            partition_cli_runtime_capabilities(&[]),
            CliRuntimeCapabilityPartition::default()
        );
        let input = [
            skill_capability("one", SkillSourceKind::Registry),
            skill_capability("two", SkillSourceKind::User),
        ];
        assert_eq!(
            partition_cli_runtime_capabilities(&input),
            CliRuntimeCapabilityPartition {
                skills: vec![
                    CliRuntimeSkillAttachment {
                        capability_id: format!(
                            "skill:{}",
                            test_skill_id("one", SkillSourceKind::Registry)
                        ),
                        label: Some("label-one".to_owned()),
                        skill_id: test_skill_id("one", SkillSourceKind::Registry),
                    },
                    CliRuntimeSkillAttachment {
                        capability_id: format!(
                            "skill:{}",
                            test_skill_id("two", SkillSourceKind::User)
                        ),
                        label: Some("label-two".to_owned()),
                        skill_id: test_skill_id("two", SkillSourceKind::User),
                    },
                ],
                ..CliRuntimeCapabilityPartition::default()
            }
        );
    }

    #[test]
    fn combined_cli_preflight_partition_preserves_mcp_only_and_mixed() {
        use pioneer_protocol::McpScopeKind;

        let server = TurnCapability {
            id: "server".to_owned(),
            kind: TurnCapabilityKind::McpServer {
                name: "server".to_owned(),
                scope_kind: McpScopeKind::Workspace,
            },
            label: None,
        };
        let tool = TurnCapability {
            id: "tool".to_owned(),
            kind: TurnCapabilityKind::McpTool {
                server_name: "server".to_owned(),
                raw_tool_name: "tool".to_owned(),
                scope_kind: McpScopeKind::Workspace,
            },
            label: None,
        };
        let server_only = partition_cli_runtime_capabilities(std::slice::from_ref(&server));
        assert!(server_only.skills.is_empty());
        assert_eq!(server_only.mcp_servers.len(), 1);
        assert!(server_only.mcp_tools.is_empty());
        assert!(server_only.has_mcp());

        let tool_only = partition_cli_runtime_capabilities(std::slice::from_ref(&tool));
        assert!(tool_only.skills.is_empty());
        assert!(tool_only.mcp_servers.is_empty());
        assert_eq!(tool_only.mcp_tools.len(), 1);
        assert!(tool_only.has_mcp());

        let mixed = partition_cli_runtime_capabilities(&[
            skill_capability("one", SkillSourceKind::User),
            server,
            tool,
        ]);
        assert_eq!(mixed.skills.len(), 1);
        assert_eq!(mixed.mcp_servers.len(), 1);
        assert_eq!(mixed.mcp_tools.len(), 1);
        assert!(mixed.has_mcp());
    }

    #[test]
    fn cli_runtime_export_allows_user_controlled_system_skills_and_rejects_required_ones() {
        let user = resolved_skill("user-skill", SkillSourceKind::User);
        let registry = resolved_skill("registry-skill", SkillSourceKind::Registry);
        let browser = resolved_skill("browser", SkillSourceKind::System);
        let exportable = vec![user.clone(), browser, registry.clone()];
        assert_eq!(
            ensure_cli_runtime_skills_exportable(&exportable).unwrap(),
            exportable.as_slice()
        );

        let mut subagents = resolved_skill("subagents", SkillSourceKind::System);
        subagents.definition.policy_hints.implicit_invocation =
            SkillImplicitInvocationPolicy::Required;
        let error = ensure_cli_runtime_skills_exportable(&[user, subagents, registry]).unwrap_err();
        assert_eq!(error.skill_slug, "subagents");
        let message = error.to_string();
        assert!(message.contains(CLI_RUNTIME_SYSTEM_SKILL_NOT_EXPORTABLE));
        assert!(message.contains("required system skill"));
        assert!(message.contains("export_boundary"));
        assert!(!message.contains("destination"));
        assert!(!message.contains("hash"));
    }

    #[test]
    fn user_controlled_system_browser_reaches_codex_and_claude_install_plans() {
        use pioneer_config::GatewayCliAgentRuntimeKindConfig;
        use pioneer_protocol::CLIAgentRuntimeKind;

        let temp = tempfile::tempdir().unwrap();
        let receipt = temp.path().join("state/receipt.json");
        let browser = resolved_skill("browser", SkillSourceKind::System);
        assert!(ensure_cli_runtime_skills_exportable(std::slice::from_ref(&browser)).is_ok());

        let runtimes = [
            (
                CLIAgentRuntimeKind::Codex,
                runtime_instance(
                    GatewayCliAgentRuntimeKindConfig::Codex,
                    temp.path().join("codex").to_string_lossy().into_owned(),
                    None,
                ),
            ),
            (
                CLIAgentRuntimeKind::Claude,
                runtime_instance(
                    GatewayCliAgentRuntimeKindConfig::Claude,
                    temp.path().join("claude").to_string_lossy().into_owned(),
                    Some(
                        temp.path()
                            .join("claude-shadow")
                            .to_string_lossy()
                            .into_owned(),
                    ),
                ),
            ),
        ];

        for (runtime_kind, runtime) in runtimes {
            ensure_cli_runtime_skill_invocation_eligible(
                runtime_kind,
                runtime.display_name.as_str(),
                std::slice::from_ref(&browser),
            )
            .unwrap();
            let plans = build_cli_runtime_skill_install_plans(
                &runtime,
                runtime_kind,
                std::slice::from_ref(&browser),
                &receipt,
            )
            .unwrap();
            assert_eq!(plans.len(), 1);
            assert_eq!(plans[0].skill_id, browser.skill_id);
            assert_eq!(plans[0].owner.as_deref(), Some("workspace"));
            assert_eq!(plans[0].slug, "browser");
            assert_eq!(plans[0].skill_slug, "browser");
            assert_eq!(plans[0].source_kind, "system");
            assert_eq!(plans[0].install_name, "browser");
        }
    }

    #[test]
    fn claude_skill_not_model_invocable_rejects_complete_set_before_planning() {
        use pioneer_protocol::CLIAgentRuntimeKind;

        let eligible = resolved_skill("eligible", SkillSourceKind::User);
        let mut blocked = resolved_skill("blocked", SkillSourceKind::Registry);
        blocked.definition.runtime.disable_model_invocation = true;
        let selected = vec![eligible, blocked.clone()];
        let error = ensure_cli_runtime_skill_invocation_eligible(
            CLIAgentRuntimeKind::Claude,
            "Claude Work",
            &selected,
        )
        .unwrap_err();
        assert_eq!(error.runtime_display_name, "Claude Work");
        assert_eq!(error.skill_display_name, "Display blocked");
        let message = error.to_string();
        assert!(message.contains(CLI_RUNTIME_CLAUDE_SKILL_NOT_MODEL_INVOCABLE));
        assert!(!message.contains("SKILL.md"));
        assert!(!message.contains("credential"));

        assert!(
            ensure_cli_runtime_skill_invocation_eligible(
                CLIAgentRuntimeKind::Codex,
                "Codex",
                &[blocked]
            )
            .is_ok()
        );
    }

    #[test]
    fn cli_runtime_skill_destination_plan_uses_codex_shared_and_claude_effective_homes() {
        use pioneer_config::GatewayCliAgentRuntimeKindConfig;
        use pioneer_protocol::CLIAgentRuntimeKind;

        let temp = tempfile::tempdir().unwrap();
        let codex_home = temp.path().join("codex-shared");
        let codex_shadow = temp.path().join("codex-shadow");
        let codex = runtime_instance(
            GatewayCliAgentRuntimeKindConfig::Codex,
            codex_home.to_string_lossy().into_owned(),
            Some(codex_shadow.to_string_lossy().into_owned()),
        );
        let mut selected = resolved_skill("my-skill", SkillSourceKind::User);
        selected.definition.identity.name = "My Skill".to_owned();
        let resolved = vec![selected];
        let receipt = temp.path().join("state/receipt.json");
        let plans = build_cli_runtime_skill_install_plans(
            &codex,
            CLIAgentRuntimeKind::Codex,
            &resolved,
            &receipt,
        )
        .unwrap();
        assert_eq!(plans[0].native_skills_root, codex_home.join("skills"));
        assert_eq!(plans[0].destination, codex_home.join("skills/my-skill"));
        assert!(!plans[0].destination.starts_with(&codex_shadow));

        let claude_home = temp.path().join("claude-home");
        let claude_shadow = temp.path().join("claude-effective");
        let claude = runtime_instance(
            GatewayCliAgentRuntimeKindConfig::Claude,
            claude_home.to_string_lossy().into_owned(),
            Some(claude_shadow.to_string_lossy().into_owned()),
        );
        let plans = build_cli_runtime_skill_install_plans(
            &claude,
            CLIAgentRuntimeKind::Claude,
            &resolved,
            &receipt,
        )
        .unwrap();
        assert_eq!(plans[0].native_skills_root, claude_shadow.join("skills"));
        assert_eq!(plans[0].destination, claude_shadow.join("skills/my-skill"));
    }

    #[test]
    fn cli_runtime_skill_destination_plan_uses_exact_default_native_roots() {
        use pioneer_config::GatewayCliAgentRuntimeKindConfig;
        use pioneer_protocol::CLIAgentRuntimeKind;

        let home = PathBuf::from(std::env::var_os("HOME").expect("HOME"));
        let temp = tempfile::tempdir().unwrap();
        let receipt = temp.path().join("state/receipt.json");
        let selected = vec![resolved_skill("default-root", SkillSourceKind::User)];

        let codex = runtime_instance(
            GatewayCliAgentRuntimeKindConfig::Codex,
            "~/.codex".to_owned(),
            None,
        );
        let codex_plans = build_cli_runtime_skill_install_plans(
            &codex,
            CLIAgentRuntimeKind::Codex,
            &selected,
            &receipt,
        )
        .unwrap();
        assert_eq!(
            codex_plans[0].native_skills_root,
            home.join(".codex/skills")
        );

        let claude = runtime_instance(
            GatewayCliAgentRuntimeKindConfig::Claude,
            "~/.claude".to_owned(),
            None,
        );
        let claude_plans = build_cli_runtime_skill_install_plans(
            &claude,
            CLIAgentRuntimeKind::Claude,
            &selected,
            &receipt,
        )
        .unwrap();
        assert_eq!(
            claude_plans[0].native_skills_root,
            home.join(".claude/skills")
        );
    }

    #[test]
    fn cli_runtime_skill_without_frontmatter_name_keeps_folder_name_fallback() {
        use pioneer_config::GatewayCliAgentRuntimeKindConfig;
        use pioneer_protocol::CLIAgentRuntimeKind;

        let temp = tempfile::tempdir().unwrap();
        let skill_dir = temp.path().join("source/folder-fallback");
        fs::create_dir_all(&skill_dir).unwrap();
        fs::write(
            skill_dir.join("SKILL.md"),
            "---\ndescription: no explicit name\n---\nFallback body",
        )
        .unwrap();
        let skill_id = test_skill_id("folder-fallback", SkillSourceKind::User);
        let definition = pioneer_skills::parse_skill_from_file(
            skill_id.clone(),
            &skill_dir.join("SKILL.md"),
            SkillSourceKind::User,
            temp.path(),
            1024 * 1024,
        )
        .unwrap();
        assert_eq!(definition.identity.name, "folder-fallback");
        let resolved = pioneer_skills::ResolvedSkill {
            skill_id,
            slug: "folder-fallback".to_owned(),
            reason: SkillResolvedReason::ExplicitCapability,
            definition,
        };
        let runtime = runtime_instance(
            GatewayCliAgentRuntimeKindConfig::Codex,
            temp.path().join("codex").to_string_lossy().into_owned(),
            None,
        );

        let plans = build_cli_runtime_skill_install_plans(
            &runtime,
            CLIAgentRuntimeKind::Codex,
            &[resolved],
            &temp.path().join("receipt.json"),
        )
        .unwrap();

        assert_eq!(plans[0].install_name, "folder-fallback");
        assert_eq!(
            plans[0].destination,
            temp.path().join("codex/skills/folder-fallback")
        );
    }

    #[test]
    fn combined_cli_preflight_skill_collision_with_mcp_is_side_effect_free() {
        use pioneer_config::GatewayCliAgentRuntimeKindConfig;
        use pioneer_protocol::{CLIAgentRuntimeKind, McpScopeKind};

        let home = PathBuf::from(std::env::var_os("HOME").expect("HOME"));
        let runtime = runtime_instance(
            GatewayCliAgentRuntimeKindConfig::Codex,
            "~/.codex-custom".to_owned(),
            None,
        );
        let receipt = home.join("state/receipt.json");
        let one = resolved_skill("one", SkillSourceKind::User);
        let plans = build_cli_runtime_skill_install_plans(
            &runtime,
            CLIAgentRuntimeKind::Codex,
            std::slice::from_ref(&one),
            &receipt,
        )
        .unwrap();
        assert_eq!(
            plans[0].native_skills_root,
            home.join(".codex-custom/skills")
        );

        let mut first = resolved_skill("first", SkillSourceKind::User);
        first.definition.identity.name = "Same Skill".to_owned();
        let mut second = resolved_skill("second", SkillSourceKind::Registry);
        second.definition.identity.name = "same@skill".to_owned();
        let partition = partition_cli_runtime_capabilities(&[
            skill_capability("first", SkillSourceKind::User),
            skill_capability("second", SkillSourceKind::Registry),
            TurnCapability {
                id: "mcp-tool:workspace:resend:send".to_owned(),
                kind: TurnCapabilityKind::McpTool {
                    server_name: "resend".to_owned(),
                    raw_tool_name: "send".to_owned(),
                    scope_kind: McpScopeKind::Workspace,
                },
                label: Some("resend/send".to_owned()),
            },
        ]);
        assert_eq!(partition.skills.len(), 2);
        assert_eq!(partition.mcp_tools.len(), 1);
        let temp = tempfile::tempdir().unwrap();
        let collision_home = temp.path().join("collision-home");
        let collision_runtime = runtime_instance(
            GatewayCliAgentRuntimeKindConfig::Codex,
            collision_home.to_string_lossy().into_owned(),
            None,
        );
        let collision_receipt = temp.path().join("state/receipt.json");
        let error = build_cli_runtime_skill_install_plans(
            &collision_runtime,
            CLIAgentRuntimeKind::Codex,
            &[first, second],
            &collision_receipt,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("cli_runtime.skill_destination_collision"));
        assert!(error.contains("first"));
        assert!(error.contains("second"));
        assert!(!collision_home.join("skills").exists());
        assert!(!collision_receipt.exists());
    }

    #[test]
    fn cli_runtime_codex_skill_input_builder_is_noop_for_zero_skills() {
        let mut mapping = pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputMapping {
            input: vec![
                pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputItem::Text {
                    text: "user request".to_owned(),
                },
            ],
            diagnostics: Vec::new(),
        };
        let before = mapping.clone();
        prepend_codex_installed_skill_items(&[], &mut mapping);
        assert_eq!(mapping, before);
    }

    #[test]
    fn cli_runtime_codex_skill_input_builder_prepends_only_structured_items() {
        let mut mapping = pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputMapping {
            input: vec![
                pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputItem::Text {
                    text: "original user request".to_owned(),
                },
            ],
            diagnostics: Vec::new(),
        };
        prepend_codex_installed_skill_items(
            &[
                installed_skill("pdf", "/native/skills/pdf"),
                installed_skill("slides", "/native/skills/slides"),
            ],
            &mut mapping,
        );
        assert_eq!(
            mapping.input,
            vec![
                pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputItem::Skill {
                    name: "pdf".to_owned(),
                    path: "/native/skills/pdf/SKILL.md".to_owned(),
                },
                pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputItem::Skill {
                    name: "slides".to_owned(),
                    path: "/native/skills/slides/SKILL.md".to_owned(),
                },
                pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputItem::Text {
                    text: "original user request".to_owned(),
                },
            ]
        );
        let serialized = serde_json::to_string(&mapping.input).unwrap();
        assert!(!serialized.contains("SKILL BODY SENTINEL"));
        assert!(!serialized.contains("SUPPORTING FILE SENTINEL"));
        assert!(!serialized.contains("source-hash-not-prompt-content"));
    }

    #[test]
    fn codex_skill_builder_emits_persistent_tree_path_with_readable_supporting_file() {
        let temp = tempfile::tempdir().unwrap();
        let installed_path = temp.path().join("skills/proposal-51-sentinel");
        std::fs::create_dir_all(installed_path.join("references")).unwrap();
        std::fs::write(installed_path.join("SKILL.md"), b"# sentinel\n").unwrap();
        std::fs::write(
            installed_path.join("references/guide.txt"),
            b"CODEX SUPPORTING FILE SENTINEL\n",
        )
        .unwrap();
        let installed = CliRuntimeSkillInstallResult {
            status: CliRuntimeSkillInstallStatus::Installed,
            install_name: "proposal-51-sentinel".to_owned(),
            source_folder_hash: "fixture-hash".to_owned(),
            installed_path: installed_path.clone(),
            receipt_updated_at_unix_ms: 1,
        };
        let mut mapping = pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputMapping {
            input: vec![
                pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputItem::Text {
                    text: "original user request".to_owned(),
                },
            ],
            diagnostics: Vec::new(),
        };

        prepend_codex_installed_skill_items(&[installed], &mut mapping);

        assert_eq!(
            mapping.input,
            vec![
                pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputItem::Skill {
                    name: "proposal-51-sentinel".to_owned(),
                    path: installed_path
                        .join("SKILL.md")
                        .to_string_lossy()
                        .into_owned(),
                },
                pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputItem::Text {
                    text: "original user request".to_owned(),
                },
            ]
        );
        assert_eq!(
            std::fs::read(installed_path.join("references/guide.txt")).unwrap(),
            b"CODEX SUPPORTING FILE SENTINEL\n"
        );
    }

    #[tokio::test]
    async fn cli_runtime_skill_destination_lock_serializes_same_normalized_path() {
        let locks = new_cli_runtime_skill_destination_locks();
        let first = acquire_cli_runtime_skill_destination_lock(
            &locks,
            Path::new("/tmp/runtime/skills/../skills/one"),
        )
        .await
        .unwrap();
        let waiting_locks = locks.clone();
        let mut waiting = tokio::spawn(async move {
            acquire_cli_runtime_skill_destination_lock(
                &waiting_locks,
                Path::new("/tmp/runtime/skills/one"),
            )
            .await
            .unwrap()
        });
        assert!(
            timeout(Duration::from_millis(20), &mut waiting)
                .await
                .is_err()
        );
        drop(first);
        let _second = timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn cli_runtime_skill_destination_lock_keeps_different_paths_independent() {
        let locks = new_cli_runtime_skill_destination_locks();
        let _first = acquire_cli_runtime_skill_destination_lock(
            &locks,
            Path::new("/tmp/runtime/skills/one"),
        )
        .await
        .unwrap();
        let _second = timeout(
            Duration::from_millis(100),
            acquire_cli_runtime_skill_destination_lock(
                &locks,
                Path::new("/tmp/runtime/skills/two"),
            ),
        )
        .await
        .expect("a different destination must not wait")
        .unwrap();
    }

    #[tokio::test]
    async fn cli_runtime_skill_install_one_same_destination_changes_once() {
        let temp = tempfile::tempdir().unwrap();
        let plan = install_plan(temp.path(), "one");
        let destination_locks = new_cli_runtime_skill_destination_locks();
        let receipt_lock = Arc::new(Mutex::new(()));
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let plan = plan.clone();
            let destination_locks = destination_locks.clone();
            let receipt_lock = receipt_lock.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                let candidates = [receipt_candidate(&plan)];
                install_one_cli_runtime_skill(&destination_locks, &receipt_lock, &candidates, &plan)
                    .await
                    .unwrap()
            }));
        }
        barrier.wait().await;
        let first = tasks.remove(0).await.unwrap();
        let second = tasks.remove(0).await.unwrap();
        let statuses = [first.status, second.status];
        assert!(statuses.contains(&CliRuntimeSkillInstallStatus::Installed));
        assert!(statuses.contains(&CliRuntimeSkillInstallStatus::Current));
        assert_eq!(
            first.receipt_updated_at_unix_ms,
            second.receipt_updated_at_unix_ms
        );
    }

    #[tokio::test]
    async fn cli_runtime_first_projection_race_converts_legacy_receipt_once() {
        let temp = tempfile::tempdir().unwrap();
        let plan = install_plan(temp.path(), "one");
        fs::create_dir_all(plan.receipt_path.parent().unwrap()).unwrap();
        fs::write(
            &plan.receipt_path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "version": 1,
                "entries": [{
                    "runtime_id": plan.runtime_id,
                    "runtime_kind": plan.runtime_kind,
                    "native_skills_root": plan.native_skills_root,
                    "install_name": plan.install_name,
                    "skill_slug": "registry/one",
                    "source_kind": "registry",
                    "source_folder_hash": "old-hash",
                    "install_path": plan.destination,
                    "installed_at_unix_ms": 7,
                    "updated_at_unix_ms": 8
                }]
            }))
            .unwrap(),
        )
        .unwrap();
        let destination_locks = new_cli_runtime_skill_destination_locks();
        let receipt_lock = Arc::new(Mutex::new(()));
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let plan = plan.clone();
            let destination_locks = destination_locks.clone();
            let receipt_lock = receipt_lock.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                let candidates = [receipt_candidate(&plan)];
                barrier.wait().await;
                install_one_cli_runtime_skill(&destination_locks, &receipt_lock, &candidates, &plan)
                    .await
                    .unwrap()
            }));
        }
        barrier.wait().await;
        let first = tasks.remove(0).await.unwrap();
        let second = tasks.remove(0).await.unwrap();
        let statuses = [first.status, second.status];
        assert!(statuses.contains(&CliRuntimeSkillInstallStatus::Updated));
        assert!(statuses.contains(&CliRuntimeSkillInstallStatus::Current));
        let receipt = read_external_runtime_receipt(&plan.receipt_path).unwrap();
        assert_eq!(
            receipt.version,
            pioneer_skills::EXTERNAL_RUNTIME_RECEIPT_VERSION
        );
        assert_eq!(receipt.entries.len(), 1);
        assert_eq!(receipt.entries[0].skill_id, plan.skill_id);
    }

    #[tokio::test]
    async fn cli_runtime_skill_install_one_preserves_concurrent_destination_receipts() {
        let temp = tempfile::tempdir().unwrap();
        let plans = [
            install_plan(temp.path(), "one"),
            install_plan(temp.path(), "two"),
        ];
        let destination_locks = new_cli_runtime_skill_destination_locks();
        let receipt_lock = Arc::new(Mutex::new(()));
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let mut tasks = Vec::new();
        for plan in plans.clone() {
            let destination_locks = destination_locks.clone();
            let receipt_lock = receipt_lock.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                let candidates = [receipt_candidate(&plan)];
                install_one_cli_runtime_skill(&destination_locks, &receipt_lock, &candidates, &plan)
                    .await
                    .unwrap()
            }));
        }
        barrier.wait().await;
        for task in tasks {
            assert_eq!(
                task.await.unwrap().status,
                CliRuntimeSkillInstallStatus::Installed
            );
        }
        let receipt = read_external_runtime_receipt(&plans[0].receipt_path).unwrap();
        assert_eq!(receipt.entries.len(), 2);
        assert_eq!(receipt.entries[0].install_name, "one");
        assert_eq!(receipt.entries[1].install_name, "two");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cli_runtime_skill_install_one_copy_failure_clears_receipt_and_retries() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let plan = install_plan(temp.path(), "one");
        let destination_locks = new_cli_runtime_skill_destination_locks();
        let receipt_lock = Arc::new(Mutex::new(()));

        let candidates = [receipt_candidate(&plan)];
        install_one_cli_runtime_skill(&destination_locks, &receipt_lock, &candidates, &plan)
            .await
            .unwrap();
        fs::write(plan.source.join("changed"), b"changed").unwrap();
        symlink("loop", plan.source.join("loop")).unwrap();
        let failed =
            install_one_cli_runtime_skill(&destination_locks, &receipt_lock, &candidates, &plan)
                .await;
        assert!(failed.is_err());
        assert!(
            read_external_runtime_receipt(&plan.receipt_path)
                .unwrap()
                .entries
                .is_empty()
        );

        fs::remove_file(plan.source.join("loop")).unwrap();
        let retried =
            install_one_cli_runtime_skill(&destination_locks, &receipt_lock, &candidates, &plan)
                .await
                .unwrap();
        assert_eq!(retried.status, CliRuntimeSkillInstallStatus::Installed);
    }
}
