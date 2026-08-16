//! Typed, pre-spawn CLI provider continuation and MCP launch identity.

use crate::cli_runtime::claude_mcp::ClaudeMcpSessionLaunchProjection;
use crate::cli_runtime::codex_mcp::CodexMcpSessionLaunchProjection;
use crate::cli_runtime::manager::CLIAgentRuntimeSessionStartOptions;
use std::fmt;
use uuid::Uuid;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum CliMcpSessionLaunch {
    /// Transitional or explicitly management-only launch with no managed MCP
    /// injection. Provider adapters must not reinterpret this as a persisted
    /// turn projection.
    #[default]
    Disabled,
    /// An isolated process used only for account/model/fork style operations.
    /// It is deliberately a different reuse identity from a turn process.
    ManagementOnly,
    Codex(CodexMcpSessionLaunchProjection),
    Claude(ClaudeMcpSessionLaunchProjection),
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) enum CliProviderContinuation {
    CodexRpcThread { native_thread_id: Option<String> },
    ClaudeNew { provider_session_id: Uuid },
    ClaudeResume { provider_session_id: Uuid },
}

impl fmt::Debug for CliProviderContinuation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CodexRpcThread { native_thread_id } => formatter
                .debug_struct("CodexRpcThread")
                .field("native_thread_id", native_thread_id)
                .finish(),
            Self::ClaudeNew { .. } => formatter
                .debug_struct("ClaudeNew")
                .field("provider_session_id", &"<redacted>")
                .finish(),
            Self::ClaudeResume { .. } => formatter
                .debug_struct("ClaudeResume")
                .field("provider_session_id", &"<redacted>")
                .finish(),
        }
    }
}

impl CliProviderContinuation {
    pub(crate) fn claude_provider_session_id(&self) -> Option<Uuid> {
        match self {
            Self::ClaudeNew {
                provider_session_id,
            }
            | Self::ClaudeResume {
                provider_session_id,
            } => Some(*provider_session_id),
            Self::CodexRpcThread { .. } => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CliSessionLaunchSpec {
    pub(crate) options: CLIAgentRuntimeSessionStartOptions,
    pub(crate) mcp: CliMcpSessionLaunch,
    pub(crate) continuation: CliProviderContinuation,
    pub(crate) native_event_budget: pioneer_cli_agent_runtime::NativeEventBudget,
}

impl CliSessionLaunchSpec {
    pub(crate) fn codex(
        options: CLIAgentRuntimeSessionStartOptions,
        mcp: CliMcpSessionLaunch,
        native_thread_id: Option<String>,
    ) -> Self {
        Self {
            options,
            mcp,
            continuation: CliProviderContinuation::CodexRpcThread { native_thread_id },
            native_event_budget: pioneer_cli_agent_runtime::NativeEventBudget::default(),
        }
    }

    pub(crate) fn unmanaged_codex(options: CLIAgentRuntimeSessionStartOptions) -> Self {
        Self::codex(options, CliMcpSessionLaunch::Disabled, None)
    }

    pub(crate) fn codex_management(options: CLIAgentRuntimeSessionStartOptions) -> Self {
        Self::codex(options, CliMcpSessionLaunch::ManagementOnly, None)
    }

    pub(crate) fn claude_new(
        options: CLIAgentRuntimeSessionStartOptions,
        mcp: CliMcpSessionLaunch,
        provider_session_id: Uuid,
    ) -> Self {
        Self {
            options,
            mcp,
            continuation: CliProviderContinuation::ClaudeNew {
                provider_session_id,
            },
            native_event_budget: pioneer_cli_agent_runtime::NativeEventBudget::default(),
        }
    }

    pub(crate) fn claude_resume(
        options: CLIAgentRuntimeSessionStartOptions,
        mcp: CliMcpSessionLaunch,
        provider_session_id: Uuid,
    ) -> Self {
        Self {
            options,
            mcp,
            continuation: CliProviderContinuation::ClaudeResume {
                provider_session_id,
            },
            native_event_budget: pioneer_cli_agent_runtime::NativeEventBudget::default(),
        }
    }

    pub(crate) fn with_native_event_budget(
        mut self,
        budget: pioneer_cli_agent_runtime::NativeEventBudget,
    ) -> Self {
        self.native_event_budget = budget;
        self
    }
}

/// Explicit process-reuse contract. Turn-local continuation details are not a
/// restart trigger; they are consumed only when a new process is actually
/// spawned. The stable provider identity must nevertheless remain the same.
pub(crate) fn requires_restart(old: &CliSessionLaunchSpec, new: &CliSessionLaunchSpec) -> bool {
    restart_relevant_options_changed(&old.options, &new.options)
        || old.mcp != new.mcp
        || old.native_event_budget != new.native_event_budget
        || continuation_identity_changed(&old.continuation, &new.continuation)
}

fn restart_relevant_options_changed(
    old: &CLIAgentRuntimeSessionStartOptions,
    new: &CLIAgentRuntimeSessionStartOptions,
) -> bool {
    old.cwd != new.cwd
        || old.approval_policy != new.approval_policy
        || old.app_server_args != new.app_server_args
        || old.env != new.env
        || old.enable_user_skills != new.enable_user_skills
        || instruction_fingerprint(&old.elevated_instructions)
            != instruction_fingerprint(&new.elevated_instructions)
}

fn instruction_fingerprint(
    instructions: &Option<pioneer_cli_agent_runtime::instructions::CLIRuntimeElevatedInstructions>,
) -> Option<&str> {
    instructions
        .as_ref()
        .map(pioneer_cli_agent_runtime::instructions::CLIRuntimeElevatedInstructions::fingerprint)
}

fn continuation_identity_changed(
    old: &CliProviderContinuation,
    new: &CliProviderContinuation,
) -> bool {
    match (old, new) {
        (
            CliProviderContinuation::CodexRpcThread { .. },
            CliProviderContinuation::CodexRpcThread { .. },
        ) => false,
        (
            CliProviderContinuation::ClaudeNew {
                provider_session_id: old,
            }
            | CliProviderContinuation::ClaudeResume {
                provider_session_id: old,
            },
            CliProviderContinuation::ClaudeNew {
                provider_session_id: new,
            }
            | CliProviderContinuation::ClaudeResume {
                provider_session_id: new,
            },
        ) => old != new,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_cli_agent_runtime::instructions::CLIRuntimeElevatedInstructions;
    use sha2::{Digest, Sha256};

    fn elevated(text: &str) -> CLIRuntimeElevatedInstructions {
        let fingerprint = Sha256::digest(text.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        CLIRuntimeElevatedInstructions::try_new(text, fingerprint)
            .expect("valid elevated instructions")
    }

    #[test]
    fn codex_continuation_id_is_not_a_process_restart_trigger() {
        let first = CliSessionLaunchSpec::codex(
            CLIAgentRuntimeSessionStartOptions::default(),
            CliMcpSessionLaunch::Disabled,
            None,
        );
        let resumed = CliSessionLaunchSpec::codex(
            CLIAgentRuntimeSessionStartOptions::default(),
            CliMcpSessionLaunch::Disabled,
            Some("native-thread".to_owned()),
        );
        assert!(!requires_restart(&first, &resumed));
    }

    #[test]
    fn native_event_budget_change_is_a_process_restart_trigger() {
        let first =
            CliSessionLaunchSpec::unmanaged_codex(CLIAgentRuntimeSessionStartOptions::default());
        let narrowed =
            first
                .clone()
                .with_native_event_budget(pioneer_cli_agent_runtime::NativeEventBudget {
                    max_frame_bytes: 4_096,
                    ..pioneer_cli_agent_runtime::NativeEventBudget::default()
                });
        assert!(requires_restart(&first, &narrowed));
    }

    #[test]
    fn claude_mode_change_preserves_stable_provider_identity() {
        let provider_session_id = Uuid::new_v4();
        let first = CliSessionLaunchSpec {
            options: CLIAgentRuntimeSessionStartOptions::default(),
            mcp: CliMcpSessionLaunch::Disabled,
            continuation: CliProviderContinuation::ClaudeNew {
                provider_session_id,
            },
            native_event_budget: pioneer_cli_agent_runtime::NativeEventBudget::default(),
        };
        let resumed = CliSessionLaunchSpec {
            options: CLIAgentRuntimeSessionStartOptions::default(),
            mcp: CliMcpSessionLaunch::Disabled,
            continuation: CliProviderContinuation::ClaudeResume {
                provider_session_id,
            },
            native_event_budget: pioneer_cli_agent_runtime::NativeEventBudget::default(),
        };
        assert!(!requires_restart(&first, &resumed));
    }

    #[test]
    fn claude_session_identity_is_redacted_from_launch_debug() {
        let provider_session_id = Uuid::new_v4();
        let launch = CliSessionLaunchSpec::claude_new(
            CLIAgentRuntimeSessionStartOptions::default(),
            CliMcpSessionLaunch::Disabled,
            provider_session_id,
        );
        let debug = format!("{launch:?}");
        assert!(!debug.contains(provider_session_id.to_string().as_str()));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn claude_elevated_instruction_identity_controls_process_reuse() {
        let mut first_options = CLIAgentRuntimeSessionStartOptions::default();
        first_options.elevated_instructions = Some(elevated("first governing prompt"));
        let same_options = first_options.clone();
        let mut changed_options = first_options.clone();
        changed_options.elevated_instructions = Some(elevated("changed governing prompt"));
        let provider_session_id = Uuid::new_v4();

        let first = CliSessionLaunchSpec::claude_resume(
            first_options,
            CliMcpSessionLaunch::Disabled,
            provider_session_id,
        );
        let same = CliSessionLaunchSpec::claude_resume(
            same_options,
            CliMcpSessionLaunch::Disabled,
            provider_session_id,
        );
        let changed = CliSessionLaunchSpec::claude_resume(
            changed_options,
            CliMcpSessionLaunch::Disabled,
            provider_session_id,
        );

        assert!(!requires_restart(&first, &same));
        assert!(requires_restart(&first, &changed));
    }
}
