//! Typed thread opening over the process-local registry.
use crate::{core::ClientCore, rpc::JsonRpcRequestTransport};
use pioneer_protocol::{Thread, ThreadGetParams};

impl ClientCore {
    pub fn open_thread_by_id(
        &self,
        transport: &impl JsonRpcRequestTransport,
        id: &str,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !id.trim().is_empty() && !self.is_stopped(),
            "thread_open_cancelled"
        );
        let authorization = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned")
            .authorization_epoch();
        let connection = self.gateway_http_generation();
        let navigation = self.navigation_snapshot();
        let response = crate::transport::ws::command_sender::thread_get(
            transport,
            ThreadGetParams {
                thread_id: id.to_owned(),
            },
        )?;
        let workspace = response.thread.workspace_id.clone();
        let token = {
            let identity = self
                .identity_authorization
                .lock()
                .expect("identity owner poisoned");
            anyhow::ensure!(
                response.thread.id == id
                    && authorization == identity.authorization_epoch()
                    && connection == self.gateway_http_generation()
                    && !self.is_stopped(),
                "thread_open_stale"
            );
            self.admit_opened_thread(response.thread, navigation.as_ref())?
        };
        self.refresh_opened_thread_subscription(transport, token, &workspace)
    }

    /// A shell's cached record identifies the target; only the authoritative read
    /// may supply domain data for opening it in the current authorization epoch.
    pub fn open_thread(
        &self,
        transport: &impl JsonRpcRequestTransport,
        thread: Thread,
    ) -> anyhow::Result<()> {
        self.open_thread_by_id(transport, &thread.id)
    }
    /// Reuses the canonical Workspace draft before creating a new one.
    pub fn open_workspace_draft(
        &self,
        transport: &impl JsonRpcRequestTransport,
        workspace: &str,
        visibility: pioneer_protocol::ThreadVisibility,
    ) -> anyhow::Result<String> {
        anyhow::ensure!(
            !workspace.trim().is_empty() && !self.is_stopped(),
            "thread_draft_cancelled"
        );
        if let Some(id) = self.thread_workspace_draft(workspace) {
            self.activate_thread(Some(&id), Some(workspace));
            return Ok(id);
        }
        self.create_workspace_thread_draft(transport, workspace, visibility)
    }

    /// Retires all thread leases when a shell leaves the authenticated session.
    pub fn close_thread_sessions(&self, transport: &impl JsonRpcRequestTransport) -> Vec<String> {
        let ids: Vec<String> = self.thread_snapshots().into_keys().collect();
        self.clear_thread_stores();
        for id in &ids {
            let _ = crate::transport::ws::command_sender::thread_unsubscribe(transport, id.clone());
        }
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    struct ThreadRead<'a> {
        core: &'a ClientCore,
        replace_route: bool,
        revoke: bool,
        revoke_on_start: bool,
        returned_id: &'static str,
        calls: Cell<usize>,
    }
    impl JsonRpcRequestTransport for ThreadRead<'_> {
        fn send_json_rpc_request(
            &self,
            _: String,
            payload: String,
            response: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            self.calls.set(self.calls.get() + 1);
            let request: serde_json::Value = serde_json::from_str(&payload).unwrap();
            assert!(matches!(
                request["method"].as_str(),
                Some("thread/get" | "thread/start")
            ));
            if self.revoke_on_start && request["method"] == "thread/start" {
                self.core.begin_authorization_epoch(None);
            }
            if self.replace_route {
                self.core.navigate(
                    crate::navigation::NavigationIntent::SelectWorkspace {
                        workspace_id: Some("replacement".into()),
                    },
                    None,
                );
            }
            if self.revoke {
                self.core.invalidate_authorization_revision(9);
            }
            response
                .send(Ok(serde_json::json!({"thread": {
                    "id": self.returned_id, "workspace_id": "workspace", "preview": "Synthetic",
                    "mode": "Chat", "model": "model", "model_provider": "provider",
                    "created_at": 1, "updated_at": 1, "status": "Idle", "turns": []
                }, "sandbox": pioneer_protocol::SandboxPolicy::from_mode(pioneer_protocol::SandboxMode::FullAccess)})))
                .unwrap();
            Ok(())
        }
    }
    #[test]
    fn authoritative_open_selects_once_and_late_subscription_cannot_restore_revoked_data() {
        for revoke_on_start in [false, true] {
            let core = ClientCore::new();
            let transport = ThreadRead {
                core: &core,
                replace_route: false,
                revoke: false,
                revoke_on_start,
                returned_id: "thread",
                calls: Cell::new(0),
            };
            let result = core.open_thread_by_id(&transport, "thread");
            assert_eq!(transport.calls.get(), 2);
            assert_eq!(result.is_err(), revoke_on_start);
            if revoke_on_start {
                assert!(core.thread_snapshot("thread").is_none());
                assert!(core.navigation_snapshot().active_thread_id().is_none());
            } else {
                assert_eq!(
                    core.navigation_snapshot().active_thread_id(),
                    Some("thread")
                );
                assert_eq!(
                    core.thread_snapshot("thread")
                        .unwrap()
                        .coordinator()
                        .thread()
                        .unwrap()
                        .preview,
                    "Synthetic"
                );
            }
        }
    }

    #[test]
    fn thread_open_cannot_follow_a_replaced_route_or_accept_a_different_thread() {
        for (replace_route, revoke, returned_id) in [
            (true, false, "thread"),
            (false, false, "other"),
            (false, true, "thread"),
        ] {
            let core = ClientCore::new();
            let transport = ThreadRead {
                core: &core,
                replace_route,
                revoke,
                revoke_on_start: false,
                returned_id,
                calls: Cell::new(0),
            };
            let error = core.open_thread_by_id(&transport, "thread").unwrap_err();
            assert_eq!(error.to_string(), "thread_open_stale");
            assert_eq!(transport.calls.get(), 1);
            assert!(core.thread_snapshot("thread").is_none());
            assert!(core.thread_snapshot("other").is_none());
            assert_eq!(
                core.navigation_snapshot().workspace_id(),
                replace_route.then_some("replacement")
            );
        }
    }
}
