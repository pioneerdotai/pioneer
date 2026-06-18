//! Explicit CLI runtime turn steering helpers.

use pioneer_protocol::CLIRuntimeTurnSteerParams;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CLIRuntimeTurnSteerPlan {
    pub workspace_id: String,
    pub runtime_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub message: String,
}

pub fn plan_cli_runtime_turn_steer(
    workspace_id: impl Into<String>,
    runtime_id: impl Into<String>,
    thread_id: impl Into<String>,
    turn_id: impl Into<String>,
    message: impl Into<String>,
) -> Option<CLIRuntimeTurnSteerParams> {
    let plan = CLIRuntimeTurnSteerPlan {
        workspace_id: normalize_required(workspace_id.into())?,
        runtime_id: normalize_required(runtime_id.into())?,
        thread_id: normalize_required(thread_id.into())?,
        turn_id: normalize_required(turn_id.into())?,
        message: normalize_required(message.into())?,
    };
    Some(cli_runtime_turn_steer_params_from_plan(plan))
}

pub fn cli_runtime_turn_steer_params_from_plan(
    plan: CLIRuntimeTurnSteerPlan,
) -> CLIRuntimeTurnSteerParams {
    CLIRuntimeTurnSteerParams {
        workspace_id: plan.workspace_id,
        runtime_id: plan.runtime_id,
        thread_id: plan.thread_id,
        turn_id: plan.turn_id,
        message: plan.message,
    }
}

fn normalize_required(value: String) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_runtime_turn_steer_plan_trims_and_builds_params() {
        let params =
            plan_cli_runtime_turn_steer(" ws ", " codex ", " thread ", " turn ", " keep going ")
                .expect("valid steer params");

        assert_eq!(params.workspace_id, "ws");
        assert_eq!(params.runtime_id, "codex");
        assert_eq!(params.thread_id, "thread");
        assert_eq!(params.turn_id, "turn");
        assert_eq!(params.message, "keep going");
    }

    #[test]
    fn cli_runtime_turn_steer_plan_rejects_missing_message() {
        assert!(plan_cli_runtime_turn_steer("ws", "codex", "thread", "turn", "   ").is_none());
    }
}
