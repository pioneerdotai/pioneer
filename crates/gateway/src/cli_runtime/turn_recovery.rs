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

pub(crate) fn cli_runtime_recovery_turn_input() -> serde_json::Value {
    serde_json::to_value(vec![CLIRuntimeTurnInputItem::Text {
        text: CLI_RUNTIME_RECOVERY_CONTINUATION_PROMPT.to_owned(),
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

    #[cfg(test)]
    fn with_runtime(mut self, runtime: CLIAgentRuntimeRecoveryRuntime) -> Self {
        self.runtimes.insert(runtime.runtime_id.clone(), runtime);
        self
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
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CLIAgentRuntimeRecoveryCatalog, CLIAgentRuntimeRecoveryRuntime,
        CLIAgentRuntimeTurnRecoveryOutcome, scan_cli_runtime_turn_recovery,
    };
    use crate::cli_runtime::turn_binding::{
        CLI_RUNTIME_TURN_STATUS_COMPLETED, CLI_RUNTIME_TURN_STATUS_RUNNING,
        CLI_RUNTIME_TURN_STATUS_STARTING,
    };
    use chrono::{FixedOffset, TimeZone};
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{CrudStore, NewCliRuntimeTurnBinding};
    use pioneer_protocol::{
        RuntimeStatus, SandboxMode, Thread, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility,
        ThreadStatus, Turn, TurnStatus, UserInput,
    };
    use sea_orm::Database;
    use sea_orm::entity::prelude::DateTimeWithTimeZone;

    async fn test_store() -> CrudStore {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");
        CrudStore::new(connection)
    }

    fn timestamp(secs: i64) -> DateTimeWithTimeZone {
        FixedOffset::east_opt(0)
            .expect("UTC offset should exist")
            .timestamp_opt(secs, 0)
            .single()
            .expect("valid timestamp")
    }

    fn runtime(runtime_id: &str, status: RuntimeStatus) -> CLIAgentRuntimeRecoveryRuntime {
        CLIAgentRuntimeRecoveryRuntime {
            runtime_id: runtime_id.to_owned(),
            runtime_kind: "codex".to_owned(),
            display_name: format!("Runtime {runtime_id}"),
            enabled: true,
            status,
            binary_path: Some("codex".to_owned()),
        }
    }

    async fn materialize_in_progress_turn(store: &CrudStore, thread_id: &str, turn_id: &str) {
        let timestamp_secs = 1_700_030_000;
        let thread = Thread {
            workspace_id: "ws_cli_recovery".to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "test-model".to_owned(),
            model_provider: "echo".to_owned(),
            reasoning_effort: None,
            created_at: timestamp_secs,
            updated_at: timestamp_secs,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[UserInput::Text {
                    text: "recover cli runtime turn".to_owned(),
                    text_elements: Vec::new(),
                }],
            )
            .await
            .expect("turn should materialize");
    }

    async fn persist_binding(
        store: &CrudStore,
        runtime_id: &str,
        thread_id: &str,
        turn_id: &str,
        status: &str,
    ) {
        store
            .upsert_cli_runtime_turn_binding(NewCliRuntimeTurnBinding {
                turn_id: turn_id.to_owned(),
                thread_id: thread_id.to_owned(),
                continuation_thread_id: thread_id.to_owned(),
                workspace_id: "ws_cli_recovery".to_owned(),
                runtime_id: runtime_id.to_owned(),
                runtime_kind: "codex".to_owned(),
                native_thread_id: format!("native-thread-{thread_id}"),
                native_turn_id: (status == CLI_RUNTIME_TURN_STATUS_RUNNING)
                    .then(|| format!("native-turn-{turn_id}")),
                request_id: Some(format!("request-{turn_id}")),
                status: status.to_owned(),
                model: Some("gpt-5".to_owned()),
                cwd: Some("/tmp/project".to_owned()),
                sandbox_json: Some(r#"{"mode":"workspace-write"}"#.to_owned()),
                approval_policy: Some("on-request".to_owned()),
                input_mapping_json: r#"{"input":1}"#.to_owned(),
                created_at: timestamp(1_700_030_000),
                updated_at: timestamp(1_700_030_001),
            })
            .await
            .expect("binding should persist");
    }

    #[tokio::test]
    async fn cli_runtime_recovery_scanner_marks_ready_runtime_recoverable() {
        let store = test_store().await;
        materialize_in_progress_turn(&store, "thread_ready", "turn_ready").await;
        persist_binding(
            &store,
            "codex-ready",
            "thread_ready",
            "turn_ready",
            CLI_RUNTIME_TURN_STATUS_RUNNING,
        )
        .await;

        let catalog = CLIAgentRuntimeRecoveryCatalog::empty()
            .with_runtime(runtime("codex-ready", RuntimeStatus::Ready));
        let plans = scan_cli_runtime_turn_recovery(&store, &catalog, 64)
            .await
            .expect("scan should succeed");

        assert_eq!(plans.len(), 1);
        assert!(matches!(
            &plans[0].outcome,
            CLIAgentRuntimeTurnRecoveryOutcome::Recoverable { reason }
                if reason.contains("ready for native turn recovery")
        ));
    }

    #[tokio::test]
    async fn cli_runtime_recovery_scanner_blocks_missing_runtime_binary_and_auth() {
        let store = test_store().await;
        for (thread_id, turn_id, runtime_id, status) in [
            (
                "thread_missing",
                "turn_missing",
                "codex-missing",
                CLI_RUNTIME_TURN_STATUS_STARTING,
            ),
            (
                "thread_binary",
                "turn_binary",
                "codex-binary",
                CLI_RUNTIME_TURN_STATUS_RUNNING,
            ),
            (
                "thread_auth",
                "turn_auth",
                "codex-auth",
                CLI_RUNTIME_TURN_STATUS_RUNNING,
            ),
            (
                "thread_done",
                "turn_done",
                "codex-ready",
                CLI_RUNTIME_TURN_STATUS_COMPLETED,
            ),
        ] {
            materialize_in_progress_turn(&store, thread_id, turn_id).await;
            persist_binding(&store, runtime_id, thread_id, turn_id, status).await;
        }

        let catalog = CLIAgentRuntimeRecoveryCatalog::empty()
            .with_runtime(runtime(
                "codex-binary",
                RuntimeStatus::MissingBinary {
                    binary_path: Some("/bad/codex".to_owned()),
                },
            ))
            .with_runtime(runtime("codex-auth", RuntimeStatus::NeedsAuth))
            .with_runtime(runtime("codex-ready", RuntimeStatus::Ready));
        let plans = scan_cli_runtime_turn_recovery(&store, &catalog, 64)
            .await
            .expect("scan should succeed");

        assert_eq!(plans.len(), 3);
        let mut reason_classes = plans
            .iter()
            .map(|plan| match &plan.outcome {
                CLIAgentRuntimeTurnRecoveryOutcome::Blocked { resume, .. } => {
                    resume.reason_class.as_str()
                }
                other => panic!("expected blocked recovery plan, got {other:?}"),
            })
            .collect::<Vec<_>>();
        reason_classes.sort_unstable();
        assert_eq!(
            reason_classes,
            vec![
                "cli_runtime_missing",
                "cli_runtime_missing_binary",
                "cli_runtime_needs_auth",
            ]
        );
        assert!(plans.iter().all(|plan| match &plan.outcome {
            CLIAgentRuntimeTurnRecoveryOutcome::Blocked { resume, .. } => {
                resume.can_resume_same_turn
                    && resume.resume_command == format!("turn.resume:{}", plan.binding.turn_id)
                    && !resume.resume_requirements.is_empty()
            }
            _ => false,
        }));
    }
}
