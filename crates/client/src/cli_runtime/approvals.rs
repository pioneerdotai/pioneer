//! Client-side pending request state for CLI runtime approvals.

use pioneer_protocol::{
    CLIRuntimePendingRequest, CLIRuntimeRequestOpenedNotification, CLIRuntimeRequestResolution,
    CLIRuntimeRequestResolvedNotification,
};

#[derive(Clone, Debug, PartialEq)]
pub struct CLIRuntimePendingRequestEntry {
    pub workspace_id: String,
    pub runtime_id: String,
    pub request_id: String,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub item_id: Option<String>,
    pub request: CLIRuntimePendingRequest,
}

impl CLIRuntimePendingRequestEntry {
    pub fn from_opened_notification(notification: CLIRuntimeRequestOpenedNotification) -> Self {
        Self {
            workspace_id: notification.workspace_id,
            runtime_id: notification.runtime_id,
            request_id: notification.request_id,
            thread_id: notification.thread_id,
            turn_id: notification.turn_id,
            item_id: notification.item_id,
            request: notification.request,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum CLIRuntimePendingRequestsReduction {
    Opened(CLIRuntimePendingRequestEntry),
    Resolved {
        request_id: String,
        resolution: CLIRuntimeRequestResolution,
    },
    TerminalTurn {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
    },
    ThreadClosed {
        workspace_id: String,
        thread_id: String,
    },
    ClearWorkspace {
        workspace_id: String,
    },
    ClearAll,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CLIRuntimePendingRequestState {
    requests: Vec<CLIRuntimePendingRequestEntry>,
}

impl CLIRuntimePendingRequestState {
    pub fn requests(&self) -> &[CLIRuntimePendingRequestEntry] {
        self.requests.as_slice()
    }

    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    pub fn pending_for_thread(
        &self,
        thread_id: Option<&str>,
    ) -> Vec<CLIRuntimePendingRequestEntry> {
        self.pending_for_scope(None, thread_id)
    }

    pub fn pending_for_scope(
        &self,
        workspace_id: Option<&str>,
        thread_id: Option<&str>,
    ) -> Vec<CLIRuntimePendingRequestEntry> {
        self.requests
            .iter()
            .filter(|entry| {
                workspace_id.map_or(true, |workspace_id| entry.workspace_id == workspace_id)
                    && match thread_id {
                        Some(thread_id) => entry.thread_id.as_deref() == Some(thread_id),
                        None => entry.thread_id.is_none(),
                    }
            })
            .cloned()
            .collect()
    }

    pub fn request(&self, request_id: &str) -> Option<&CLIRuntimePendingRequestEntry> {
        self.requests
            .iter()
            .find(|entry| entry.request_id == request_id)
    }

    pub fn apply(&mut self, reduction: CLIRuntimePendingRequestsReduction) -> bool {
        match reduction {
            CLIRuntimePendingRequestsReduction::Opened(entry) => {
                if let Some(existing) = self
                    .requests
                    .iter_mut()
                    .find(|existing| existing.request_id == entry.request_id)
                {
                    let changed = existing != &entry;
                    *existing = entry;
                    return changed;
                }

                self.requests.push(entry);
                true
            }
            CLIRuntimePendingRequestsReduction::Resolved { request_id, .. } => {
                remove_matching(&mut self.requests, |entry| entry.request_id == request_id)
            }
            CLIRuntimePendingRequestsReduction::TerminalTurn {
                workspace_id,
                thread_id,
                turn_id,
            } => remove_matching(&mut self.requests, |entry| {
                entry.workspace_id == workspace_id
                    && entry.thread_id.as_deref() == Some(thread_id.as_str())
                    && entry.turn_id.as_deref() == Some(turn_id.as_str())
            }),
            CLIRuntimePendingRequestsReduction::ThreadClosed {
                workspace_id,
                thread_id,
            } => remove_matching(&mut self.requests, |entry| {
                entry.workspace_id == workspace_id
                    && entry.thread_id.as_deref() == Some(thread_id.as_str())
            }),
            CLIRuntimePendingRequestsReduction::ClearWorkspace { workspace_id } => {
                remove_matching(&mut self.requests, |entry| {
                    entry.workspace_id == workspace_id
                })
            }
            CLIRuntimePendingRequestsReduction::ClearAll => {
                if self.requests.is_empty() {
                    false
                } else {
                    self.requests.clear();
                    true
                }
            }
        }
    }
}

pub fn reduce_cli_runtime_request_opened_notification(
    notification: CLIRuntimeRequestOpenedNotification,
) -> CLIRuntimePendingRequestsReduction {
    CLIRuntimePendingRequestsReduction::Opened(
        CLIRuntimePendingRequestEntry::from_opened_notification(notification),
    )
}

pub fn reduce_cli_runtime_request_resolved_notification(
    notification: CLIRuntimeRequestResolvedNotification,
) -> CLIRuntimePendingRequestsReduction {
    CLIRuntimePendingRequestsReduction::Resolved {
        request_id: notification.request_id,
        resolution: notification.resolution,
    }
}

pub fn reduce_cli_runtime_terminal_turn_cleanup(
    workspace_id: String,
    thread_id: String,
    turn_id: String,
) -> CLIRuntimePendingRequestsReduction {
    CLIRuntimePendingRequestsReduction::TerminalTurn {
        workspace_id,
        thread_id,
        turn_id,
    }
}

pub fn reduce_cli_runtime_thread_closed_cleanup(
    workspace_id: String,
    thread_id: String,
) -> CLIRuntimePendingRequestsReduction {
    CLIRuntimePendingRequestsReduction::ThreadClosed {
        workspace_id,
        thread_id,
    }
}

fn remove_matching(
    requests: &mut Vec<CLIRuntimePendingRequestEntry>,
    mut matches: impl FnMut(&CLIRuntimePendingRequestEntry) -> bool,
) -> bool {
    let initial_len = requests.len();
    requests.retain(|entry| !matches(entry));
    requests.len() != initial_len
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{CLIRuntimeRequestKind, CLIRuntimeRequestResolution};

    fn request_opened(
        request_id: &str,
        workspace_id: &str,
        runtime_id: &str,
        thread_id: Option<&str>,
        turn_id: Option<&str>,
        command: &str,
    ) -> CLIRuntimeRequestOpenedNotification {
        CLIRuntimeRequestOpenedNotification {
            workspace_id: workspace_id.to_owned(),
            runtime_id: runtime_id.to_owned(),
            request_id: request_id.to_owned(),
            thread_id: thread_id.map(str::to_owned),
            turn_id: turn_id.map(str::to_owned),
            item_id: None,
            request: CLIRuntimePendingRequest {
                kind: CLIRuntimeRequestKind::CommandApproval,
                title: Some(format!("Run {command}")),
                message: Some(command.to_owned()),
                native_request_id: Some(format!("native_{request_id}")),
                payload: None,
            },
        }
    }

    #[test]
    fn cli_runtime_approval_state_tracks_concurrent_requests() {
        let mut state = CLIRuntimePendingRequestState::default();

        assert!(state.apply(reduce_cli_runtime_request_opened_notification(
            request_opened(
                "req_a",
                "ws",
                "codex",
                Some("thread"),
                Some("turn_a"),
                "pwd"
            )
        )));
        assert!(state.apply(reduce_cli_runtime_request_opened_notification(
            request_opened("req_b", "ws", "codex", Some("thread"), Some("turn_b"), "ls")
        )));

        let requests = state.pending_for_thread(Some("thread"));
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].request_id, "req_a");
        assert_eq!(requests[1].request_id, "req_b");
    }

    #[test]
    fn cli_runtime_approval_state_ignores_stale_resolution() {
        let mut state = CLIRuntimePendingRequestState::default();

        assert!(
            !state.apply(reduce_cli_runtime_request_resolved_notification(
                CLIRuntimeRequestResolvedNotification {
                    workspace_id: "ws".to_owned(),
                    runtime_id: "codex".to_owned(),
                    request_id: "missing".to_owned(),
                    thread_id: Some("thread".to_owned()),
                    turn_id: Some("turn".to_owned()),
                    item_id: None,
                    resolution: CLIRuntimeRequestResolution::Cancelled,
                },
            ),)
        );
        assert!(state.is_empty());
    }

    #[test]
    fn cli_runtime_approval_state_removes_resolved_request_only() {
        let mut state = CLIRuntimePendingRequestState::default();
        state.apply(reduce_cli_runtime_request_opened_notification(
            request_opened("req_a", "ws", "codex", Some("thread"), Some("turn"), "pwd"),
        ));
        state.apply(reduce_cli_runtime_request_opened_notification(
            request_opened("req_b", "ws", "codex", Some("thread"), Some("turn"), "ls"),
        ));

        assert!(
            state.apply(reduce_cli_runtime_request_resolved_notification(
                CLIRuntimeRequestResolvedNotification {
                    workspace_id: "ws".to_owned(),
                    runtime_id: "codex".to_owned(),
                    request_id: "req_a".to_owned(),
                    thread_id: Some("thread".to_owned()),
                    turn_id: Some("turn".to_owned()),
                    item_id: None,
                    resolution: CLIRuntimeRequestResolution::Approved,
                },
            ),)
        );

        assert_eq!(state.requests().len(), 1);
        assert_eq!(state.requests()[0].request_id, "req_b");
    }

    #[test]
    fn cli_runtime_approval_state_cleans_up_terminal_turn_requests() {
        let mut state = CLIRuntimePendingRequestState::default();
        state.apply(reduce_cli_runtime_request_opened_notification(
            request_opened(
                "req_a",
                "ws",
                "codex",
                Some("thread"),
                Some("turn_a"),
                "pwd",
            ),
        ));
        state.apply(reduce_cli_runtime_request_opened_notification(
            request_opened("req_b", "ws", "codex", Some("thread"), Some("turn_b"), "ls"),
        ));

        assert!(state.apply(reduce_cli_runtime_terminal_turn_cleanup(
            "ws".to_owned(),
            "thread".to_owned(),
            "turn_a".to_owned(),
        )));

        assert_eq!(state.requests().len(), 1);
        assert_eq!(state.requests()[0].request_id, "req_b");
    }
}
