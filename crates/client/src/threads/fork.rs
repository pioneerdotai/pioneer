//! Planning helpers for CLI-runtime-backed thread forks.

use crate::threads::coordinator::ThreadCoordinator;
use pioneer_protocol::CLIRuntimeThreadForkParams;
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CLIRuntimeThreadForkPlan {
    Skip(CLIRuntimeThreadForkRejection),
    Request(CLIRuntimeThreadForkParams),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CLIRuntimeThreadForkRejection {
    MissingWorkspace,
    MissingRuntime,
    MissingSourceThread,
    MissingForkThread,
    SameThread,
    ForeignWorkspace,
}

pub fn plan_cli_runtime_thread_fork(
    coordinators: &HashMap<String, ThreadCoordinator>,
    workspace_id: Option<&str>,
    runtime_id: Option<&str>,
    source_thread_id: &str,
    fork_thread_id: &str,
    name: Option<&str>,
) -> CLIRuntimeThreadForkPlan {
    let Some(workspace_id) = workspace_id.and_then(non_empty) else {
        return CLIRuntimeThreadForkPlan::Skip(CLIRuntimeThreadForkRejection::MissingWorkspace);
    };
    let Some(runtime_id) = runtime_id.and_then(non_empty) else {
        return CLIRuntimeThreadForkPlan::Skip(CLIRuntimeThreadForkRejection::MissingRuntime);
    };
    let Some(source_thread_id) = non_empty(source_thread_id) else {
        return CLIRuntimeThreadForkPlan::Skip(CLIRuntimeThreadForkRejection::MissingSourceThread);
    };
    let Some(fork_thread_id) = non_empty(fork_thread_id) else {
        return CLIRuntimeThreadForkPlan::Skip(CLIRuntimeThreadForkRejection::MissingForkThread);
    };
    if source_thread_id == fork_thread_id {
        return CLIRuntimeThreadForkPlan::Skip(CLIRuntimeThreadForkRejection::SameThread);
    }
    let Some(coordinator) = coordinators.get(source_thread_id) else {
        return CLIRuntimeThreadForkPlan::Skip(CLIRuntimeThreadForkRejection::MissingSourceThread);
    };
    if coordinator.workspace_id != workspace_id {
        return CLIRuntimeThreadForkPlan::Skip(CLIRuntimeThreadForkRejection::ForeignWorkspace);
    }

    CLIRuntimeThreadForkPlan::Request(CLIRuntimeThreadForkParams {
        workspace_id: workspace_id.to_owned(),
        runtime_id: runtime_id.to_owned(),
        source_thread_id: source_thread_id.to_owned(),
        fork_thread_id: fork_thread_id.to_owned(),
        name: name.and_then(non_empty).map(str::to_owned),
    })
}

fn non_empty(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        Thread, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus,
    };

    fn coordinator(thread_id: &str, workspace_id: &str) -> ThreadCoordinator {
        ThreadCoordinator::new(Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: Some("Source".to_owned()),
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "gpt-5".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: 1,
            updated_at: 1,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        })
    }

    #[test]
    fn thread_fork_plan_builds_cli_runtime_request() {
        let coordinators = HashMap::from([("source".to_owned(), coordinator("source", "ws_a"))]);

        let params = match plan_cli_runtime_thread_fork(
            &coordinators,
            Some(" ws_a "),
            Some(" codex "),
            " source ",
            " fork ",
            Some(" Forked source "),
        ) {
            CLIRuntimeThreadForkPlan::Request(params) => params,
            other => panic!("unexpected fork plan: {other:?}"),
        };

        assert_eq!(params.workspace_id, "ws_a");
        assert_eq!(params.runtime_id, "codex");
        assert_eq!(params.source_thread_id, "source");
        assert_eq!(params.fork_thread_id, "fork");
        assert_eq!(params.name.as_deref(), Some("Forked source"));
    }

    #[test]
    fn thread_fork_plan_rejects_invalid_source_context() {
        let coordinators = HashMap::from([("source".to_owned(), coordinator("source", "ws_a"))]);

        assert_eq!(
            plan_cli_runtime_thread_fork(
                &coordinators,
                Some("ws_b"),
                Some("codex"),
                "source",
                "fork",
                None,
            ),
            CLIRuntimeThreadForkPlan::Skip(CLIRuntimeThreadForkRejection::ForeignWorkspace)
        );
        assert_eq!(
            plan_cli_runtime_thread_fork(
                &coordinators,
                Some("ws_a"),
                Some("codex"),
                "source",
                "source",
                None,
            ),
            CLIRuntimeThreadForkPlan::Skip(CLIRuntimeThreadForkRejection::SameThread)
        );
    }
}
