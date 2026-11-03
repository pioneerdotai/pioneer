#![allow(dead_code)]
// Runtime readiness classification and continuation input for CLI-backed turn recovery.

use anyhow::Result;
use pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputItem;
use pioneer_crud::{CliRuntimeTurnBindingListFilter, CliRuntimeTurnBindingRecord, CrudStore};
use pioneer_protocol::{
    CLIAgentRuntimeKind, RuntimeStatus, RuntimeSummary, TurnBlockedResumeMetadata, TurnStatus,
};
use std::collections::HashMap;

const CLI_RUNTIME_RECOVERY_CONTINUATION_PROMPT: &str = "Continue the interrupted task from the existing thread context.\n\nRecovery contract:\n- The original user request and prior conversation are authoritative.\n- Treat the current workspace and external systems as the source of truth.\n- Inspect and reconcile current state before making further changes.\n- Do not repeat actions or tool calls that already completed.\n- Continue from the first unfinished step.\n- If safe continuation cannot be established, stop and explain what must be resolved.";

pub(crate) fn cli_runtime_recovery_turn_input(
    initiating_thread_id: &str,
    execution_thread_id: &str,
) -> serde_json::Value {
    serde_json::to_value(vec![CLIRuntimeTurnInputItem::Text {
        text: format!(
            "{}\n\n{CLI_RUNTIME_RECOVERY_CONTINUATION_PROMPT}",
            pioneer_promt::render_thread_ids(Some(initiating_thread_id), execution_thread_id)
        ),
    }])
    .expect("CLI runtime recovery text input must serialize")
}

use crate::cli_runtime::turn_binding::{
    CLI_RUNTIME_TURN_STATUS_RUNNING, CLI_RUNTIME_TURN_STATUS_STARTING,
};

#[derive(Debug, Clone)]
pub(crate) struct CLIAgentRuntimeRecoveryCatalog {
    runtimes: HashMap<String, CLIAgentRuntimeRecoveryRuntime>,
}

impl CLIAgentRuntimeRecoveryCatalog {
    pub(crate) fn empty() -> Self {
        Self {
            runtimes: HashMap::new(),
        }
    }

    pub(crate) fn from_runtime_summaries(summaries: Vec<RuntimeSummary>) -> Self {
        Self {
            runtimes: summaries
                .into_iter()
                .map(|summary| {
                    (
                        summary.runtime_id.clone(),
                        CLIAgentRuntimeRecoveryRuntime {
                            runtime_id: summary.runtime_id,
                            runtime_kind: cli_agent_runtime_kind_label(summary.kind).to_owned(),
                            display_name: summary.display_name,
                            enabled: summary.enabled,
                            status: summary.status,
                            binary_path: summary.binary_path,
                        },
                    )
                })
                .collect(),
        }
    }

    fn get(&self, runtime_id: &str) -> Option<&CLIAgentRuntimeRecoveryRuntime> {
        self.runtimes.get(runtime_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CLIAgentRuntimeRecoveryRuntime {
    pub runtime_id: String,
    pub runtime_kind: String,
    pub display_name: String,
    pub enabled: bool,
    pub status: RuntimeStatus,
    pub binary_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CLIAgentRuntimeTurnRecoveryPlan {
    pub binding: CliRuntimeTurnBindingRecord,
    pub outcome: CLIAgentRuntimeTurnRecoveryOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CLIAgentRuntimeTurnRecoveryOutcome {
    Recoverable {
        reason: String,
    },
    Blocked {
        reason: String,
        resume: TurnBlockedResumeMetadata,
    },
    Ignored {
        reason: String,
    },
}

pub(crate) async fn scan_cli_runtime_turn_recovery(
    store: &CrudStore,
    catalog: &CLIAgentRuntimeRecoveryCatalog,
    limit: u64,
) -> Result<Vec<CLIAgentRuntimeTurnRecoveryPlan>> {
    let bindings = store
        .list_cli_runtime_turn_bindings(CliRuntimeTurnBindingListFilter {
            statuses: vec![
                CLI_RUNTIME_TURN_STATUS_STARTING.to_owned(),
                CLI_RUNTIME_TURN_STATUS_RUNNING.to_owned(),
            ],
            limit: Some(limit),
            ..Default::default()
        })
        .await?;

    let mut plans = Vec::with_capacity(bindings.len());
    for binding in bindings {
        plans.push(classify_cli_runtime_turn_binding_for_recovery(store, catalog, binding).await?);
    }
    Ok(plans)
}

async fn classify_cli_runtime_turn_binding_for_recovery(
    store: &CrudStore,
    catalog: &CLIAgentRuntimeRecoveryCatalog,
    binding: CliRuntimeTurnBindingRecord,
) -> Result<CLIAgentRuntimeTurnRecoveryPlan> {
    let outcome = match store
        .get_turn(binding.thread_id.as_str(), binding.turn_id.as_str())
        .await?
    {
        Some((_workspace_id, turn)) if turn.status == TurnStatus::InProgress => {
            classify_cli_runtime_turn_recovery_outcome(
                &binding,
                catalog.get(binding.runtime_id.as_str()),
            )
        }
        Some((_workspace_id, turn)) => CLIAgentRuntimeTurnRecoveryOutcome::Ignored {
            reason: format!(
                "Pioneer turn `{}` is `{}` and does not need CLI runtime restart recovery",
                binding.turn_id,
                turn_status_label(turn.status)
            ),
        },
        None => CLIAgentRuntimeTurnRecoveryOutcome::Ignored {
            reason: format!(
                "Pioneer turn `{}` is missing for CLI runtime binding",
                binding.turn_id
            ),
        },
    };

    Ok(CLIAgentRuntimeTurnRecoveryPlan { binding, outcome })
}

fn classify_cli_runtime_turn_recovery_outcome(
    binding: &CliRuntimeTurnBindingRecord,
    runtime: Option<&CLIAgentRuntimeRecoveryRuntime>,
) -> CLIAgentRuntimeTurnRecoveryOutcome {
    let Some(runtime) = runtime else {
        return blocked_recovery(
            binding,
            "cli_runtime_missing",
            format!(
                "CLI runtime `{}` is no longer configured.",
                binding.runtime_id
            ),
            vec![
                format!(
                    "Re-enable or recreate CLI runtime `{}`.",
                    binding.runtime_id
                ),
                "Resume the same turn after the runtime is available.".to_owned(),
            ],
        );
    };

    if !runtime.enabled {
        return blocked_recovery(
            binding,
            "cli_runtime_disabled",
            format!("CLI runtime `{}` is disabled.", runtime.runtime_id),
            vec![
                format!("Enable CLI runtime `{}`.", runtime.display_name),
                "Resume the same turn after the runtime is enabled.".to_owned(),
            ],
        );
    }

    match &runtime.status {
        RuntimeStatus::Ready => CLIAgentRuntimeTurnRecoveryOutcome::Recoverable {
            reason: format!(
                "CLI runtime `{}` is ready for native turn recovery.",
                runtime.runtime_id
            ),
        },
        RuntimeStatus::Initializing | RuntimeStatus::Degraded { .. } => {
            CLIAgentRuntimeTurnRecoveryOutcome::Recoverable {
                reason: format!(
                    "CLI runtime `{}` is available for native turn recovery.",
                    runtime.runtime_id
                ),
            }
        }
        RuntimeStatus::MissingBinary { binary_path } => blocked_recovery(
            binding,
            "cli_runtime_missing_binary",
            cli_runtime_missing_binary_message(binding, runtime, binary_path.as_deref()),
            vec![
                cli_runtime_missing_binary_requirement(binding, binary_path.as_deref()),
                "Resume the same turn after the binary is available.".to_owned(),
            ],
        ),
        RuntimeStatus::NeedsAuth => blocked_recovery(
            binding,
            "cli_runtime_needs_auth",
            cli_runtime_needs_auth_message(binding, runtime),
            vec![
                cli_runtime_needs_auth_requirement(binding),
                "Resume the same turn after authentication succeeds.".to_owned(),
            ],
        ),
        RuntimeStatus::SpawnFailed { message } => blocked_recovery(
            binding,
            "cli_runtime_spawn_failed",
            format!(
                "CLI runtime `{}` could not start during restart recovery: {message}",
                runtime.runtime_id
            ),
            vec![
                "Fix the CLI runtime process startup error.".to_owned(),
                "Resume the same turn after the runtime starts successfully.".to_owned(),
            ],
        ),
        RuntimeStatus::UnsupportedVersion {
            version,
            minimum_version,
        } => blocked_recovery(
            binding,
            "cli_runtime_unsupported_version",
            format!(
                "CLI runtime `{}` version `{}` is unsupported.",
                runtime.runtime_id,
                version.as_deref().unwrap_or("unknown")
            ),
            vec![
                format!(
                    "Update the CLI runtime{}.",
                    minimum_version
                        .as_deref()
                        .map(|version| format!(" to at least `{version}`"))
                        .unwrap_or_default()
                ),
                "Resume the same turn after the runtime version is supported.".to_owned(),
            ],
        ),
        RuntimeStatus::Error { message } => blocked_recovery(
            binding,
            "cli_runtime_error",
            format!(
                "CLI runtime `{}` reported an error during restart recovery: {message}",
                runtime.runtime_id
            ),
            vec![
                "Fix the CLI runtime error shown in provider settings.".to_owned(),
                "Resume the same turn after the runtime reports ready.".to_owned(),
            ],
        ),
        RuntimeStatus::Disabled => blocked_recovery(
            binding,
            "cli_runtime_disabled",
            format!("CLI runtime `{}` is disabled.", runtime.runtime_id),
            vec![
                format!("Enable CLI runtime `{}`.", runtime.display_name),
                "Resume the same turn after the runtime is enabled.".to_owned(),
            ],
        ),
    }
}

fn blocked_recovery(
    binding: &CliRuntimeTurnBindingRecord,
    reason_class: &str,
    human_message: String,
    resume_requirements: Vec<String>,
) -> CLIAgentRuntimeTurnRecoveryOutcome {
    CLIAgentRuntimeTurnRecoveryOutcome::Blocked {
        reason: human_message.clone(),
        resume: TurnBlockedResumeMetadata {
            reason_class: reason_class.to_owned(),
            human_message,
            resume_requirements,
            resume_command: format!("turn.resume:{}", binding.turn_id),
            blocked_recovery_job_id: None,
            latest_checkpoint_id: None,
            can_resume_same_turn: true,
        },
    }
}

fn cli_runtime_missing_binary_message(
    binding: &CliRuntimeTurnBindingRecord,
    runtime: &CLIAgentRuntimeRecoveryRuntime,
    binary_path: Option<&str>,
) -> String {
    if binding.runtime_kind == "codex" {
        return format!(
            "Codex CLI binary is not available for runtime `{}`{}.",
            runtime.runtime_id,
            binary_path
                .map(|path| format!(" at `{path}`"))
                .unwrap_or_default()
        );
    }
    format!(
        "CLI runtime binary is not available for runtime `{}`{}.",
        runtime.runtime_id,
        binary_path
            .map(|path| format!(" at `{path}`"))
            .unwrap_or_default()
    )
}

fn cli_runtime_missing_binary_requirement(
    binding: &CliRuntimeTurnBindingRecord,
    binary_path: Option<&str>,
) -> String {
    if binding.runtime_kind == "codex" {
        return binary_path
            .map(|path| format!("Install Codex CLI or configure a valid binary path for `{path}`."))
            .unwrap_or_else(|| "Install Codex CLI or configure a valid binary path.".to_owned());
    }
    binary_path
        .map(|path| {
            format!("Install the CLI runtime binary or configure a valid path for `{path}`.")
        })
        .unwrap_or_else(|| "Install the CLI runtime binary or configure a valid path.".to_owned())
}

fn cli_runtime_needs_auth_message(
    binding: &CliRuntimeTurnBindingRecord,
    runtime: &CLIAgentRuntimeRecoveryRuntime,
) -> String {
    if binding.runtime_kind == "codex" {
        return format!(
            "Codex authentication is required before runtime `{}` can recover turn `{}`.",
            runtime.runtime_id, binding.turn_id
        );
    }
    format!(
        "CLI runtime `{}` requires authentication before it can recover turn `{}`.",
        runtime.runtime_id, binding.turn_id
    )
}

fn cli_runtime_needs_auth_requirement(binding: &CliRuntimeTurnBindingRecord) -> String {
    if binding.runtime_kind == "codex" {
        "Log in to Codex in provider settings.".to_owned()
    } else {
        "Authenticate the CLI runtime in provider settings.".to_owned()
    }
}

fn turn_status_label(status: TurnStatus) -> &'static str {
    match status {
        TurnStatus::InProgress => "in_progress",
        TurnStatus::Completed => "completed",
        TurnStatus::Failed => "failed",
        TurnStatus::Interrupted => "interrupted",
        TurnStatus::Blocked => "blocked",
    }
}

fn cli_agent_runtime_kind_label(kind: CLIAgentRuntimeKind) -> &'static str {
    match kind {
        CLIAgentRuntimeKind::Codex => "codex",
        CLIAgentRuntimeKind::Claude => "claude",
        CLIAgentRuntimeKind::OpenCode => "opencode",
    }
}
