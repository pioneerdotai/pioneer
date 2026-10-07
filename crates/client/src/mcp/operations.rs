//! Typed MCP mutations. Configuration input is consumed, never published or logged.
use crate::core::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, mpsc},
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum McpIntent {
    RetryAuthorizationBrowser {
        server_id: String,
    },
    SignIn {
        server_id: String,
    },
    Disconnect {
        server_id: String,
    },
    CancelAuthorization {
        server_id: String,
        flow_id: String,
    },
    Policy {
        server_id: String,
        enabled: bool,
        allow_implicit_invocation: bool,
    },
    Restart {
        server_id: String,
    },
    Remove {
        server_id: String,
    },
    Configure {
        config_json: String,
    },
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpActionKind {
    RetryAuthorizationBrowser,
    SignIn,
    Disconnect,
    CancelAuthorization,
    Policy,
    Restart,
    Remove,
    Configure,
}
impl McpIntent {
    fn action_kind(&self) -> McpActionKind {
        match self {
            Self::RetryAuthorizationBrowser { .. } => McpActionKind::RetryAuthorizationBrowser,
            Self::SignIn { .. } => McpActionKind::SignIn,
            Self::Disconnect { .. } => McpActionKind::Disconnect,
            Self::CancelAuthorization { .. } => McpActionKind::CancelAuthorization,
            Self::Policy { .. } => McpActionKind::Policy,
            Self::Restart { .. } => McpActionKind::Restart,
            Self::Remove { .. } => McpActionKind::Remove,
            Self::Configure { .. } => McpActionKind::Configure,
        }
    }
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpActionState {
    Pending,
    Succeeded,
    Failed,
    Cancelled,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct McpActionPublication {
    pub operation_id: u64,
    pub revision: u64,
    pub state: McpActionState,
    pub kind: McpActionKind,
    pub field_error: Option<super::actions::McpInstallFieldError>,
}
struct Work {
    workspace: String,
    target: String,
    operation: u64,
    epoch: (u64, u64, Option<u64>),
    name: String,
    scope_kind: pioneer_protocol::McpScopeKind,
    intent: McpIntent,
    previous_policy: Option<(bool, bool)>,
}
#[derive(Default)]
pub(crate) struct McpController {
    pub(crate) fenced: bool,
    next: u64,
    publications: BTreeMap<(String, String), Arc<McpActionPublication>>,
    sender: Option<mpsc::SyncSender<Work>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl McpController {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
        self.publications.clear();
    }
}
impl Drop for McpController {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}
impl ClientCore {
    pub fn mcp_action_snapshot(
        &self,
        workspace: &str,
        target: &str,
    ) -> Option<Arc<McpActionPublication>> {
        self.snapshot(&ClientScope::McpAction {
            workspace_id: workspace.into(),
            target: target.into(),
        })
        .and_then(|p| p.snapshot().payload())
    }
    pub fn mcp_intent(&self, workspace: &str, intent: McpIntent) -> anyhow::Result<u64> {
        self.mcp_intent_admit(workspace, intent, None)
    }
    /// Plugin delegation shares native validation, queueing and completion. It
    /// must never recapture a Gateway selected after the originating read.
    pub(crate) fn mcp_intent_bound(
        &self,
        workspace: &str,
        intent: McpIntent,
        epoch: (u64, u64, Option<u64>),
    ) -> anyhow::Result<u64> {
        self.mcp_intent_admit(workspace, intent, Some(epoch))
    }
    fn mcp_intent_admit(
        &self,
        workspace: &str,
        intent: McpIntent,
        expected_epoch: Option<(u64, u64, Option<u64>)>,
    ) -> anyhow::Result<u64> {
        anyhow::ensure!(
            self.capability_management_allowed(workspace),
            "capability_access_denied"
        );
        let epoch = expected_epoch.unwrap_or_else(|| self.provider_runtime_epoch());
        anyhow::ensure!(
            expected_epoch.is_none() || self.provider_runtime_epoch() == epoch,
            "connection_changed"
        );
        anyhow::ensure!(epoch.2.is_some(), "gateway_not_connected");
        let (target, name, scope_kind) = match &intent {
            McpIntent::RetryAuthorizationBrowser { server_id }
            | McpIntent::SignIn { server_id }
            | McpIntent::Disconnect { server_id }
            | McpIntent::CancelAuthorization { server_id, .. }
            | McpIntent::Policy { server_id, .. }
            | McpIntent::Restart { server_id }
            | McpIntent::Remove { server_id } => {
                let snapshot = self
                    .mcp_catalog_snapshot(workspace)
                    .ok_or_else(|| anyhow::anyhow!("mcp_catalog_unavailable"))?;
                let server = snapshot
                    .servers()
                    .iter()
                    .find(|s| &s.id == server_id)
                    .ok_or_else(|| anyhow::anyhow!("mcp_server_unavailable"))?;
                (server_id.clone(), server.name.clone(), server.scope)
            }
            McpIntent::Configure { config_json } => {
                super::actions::validate_mcp_config_for_submit(config_json)
                    .map_err(|_| anyhow::anyhow!("mcp_config_invalid"))?;
                (
                    "configuration".into(),
                    String::new(),
                    pioneer_protocol::McpScopeKind::Workspace,
                )
            }
        };
        let kind = intent.action_kind();
        let previous_policy = self.mcp_catalog_snapshot(workspace).and_then(|p| {
            p.servers()
                .iter()
                .find(|s| s.id == target)
                .map(|s| (s.policy.enabled, s.policy.allow_implicit_invocation))
        });
        let optimistic = match &intent {
            McpIntent::Policy {
                enabled,
                allow_implicit_invocation,
                ..
            } => Some((*enabled, *allow_implicit_invocation)),
            _ => None,
        };
        let mut owner = self.mcp_controller.lock().expect("MCP controller poisoned");
        anyhow::ensure!(!owner.fenced, "capability_access_denied");
        anyhow::ensure!(
            !owner
                .publications
                .get(&(workspace.into(), target.clone()))
                .is_some_and(|p| p.state == McpActionState::Pending),
            "mcp_action_pending"
        );
        anyhow::ensure!(
            expected_epoch.is_none() || self.provider_runtime_epoch() == epoch,
            "connection_changed"
        );
        owner.next = owner
            .next
            .checked_add(1)
            .expect("MCP operation identity exhausted");
        let operation = owner.next;
        let work = Work {
            workspace: workspace.into(),
            target: target.clone(),
            operation,
            epoch,
            name,
            scope_kind,
            intent,
            previous_policy,
        };
        owner
            .sender
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("client_stopped"))?
            .try_send(work)
            .map_err(|_| anyhow::anyhow!("mcp_action_backpressure"))?;
        self.publish_mcp_action(
            &mut owner,
            workspace,
            &target,
            operation,
            McpActionState::Pending,
            kind,
        );
        if let Some((enabled, implicit)) = optimistic {
            self.apply_mcp_policy(workspace, &target, enabled, implicit);
        }
        Ok(operation)
    }
    pub(crate) fn capability_management_allowed(&self, workspace: &str) -> bool {
        !self.is_stopped()
            && self
                .authorization_snapshot(Some(workspace), None)
                .or_else(|| self.authorization_snapshot(None, None))
                .is_some_and(|p| {
                    crate::authorization::principal_presentation_capabilities(&p)
                        .can_manage_capabilities
                })
    }
    fn publish_mcp_action(
        &self,
        owner: &mut McpController,
        workspace: &str,
        target: &str,
        operation: u64,
        state: McpActionState,
        kind: McpActionKind,
    ) {
        let key = (workspace.into(), target.into());
        if owner
            .publications
            .get(&key)
            .is_some_and(|p| p.operation_id == operation && p.state == state)
        {
            return;
        }
        let scope = ClientScope::McpAction {
            workspace_id: workspace.into(),
            target: target.into(),
        };
        let revision = self
            .snapshot(&scope)
            .map_or(0, |p| p.revisions().scoped().get())
            .checked_add(1)
            .expect("MCP action revision exhausted");
        let p = Arc::new(McpActionPublication {
            operation_id: operation,
            revision,
            state,
            kind,
            field_error: None,
        });
        owner.publications.insert(key, p.clone());
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(revision),
            p,
            vec![],
        );
    }
    fn mcp_action_current(&self, work: &Work) -> bool {
        self.provider_runtime_epoch() == work.epoch
            && self.capability_management_allowed(&work.workspace)
            && {
                let owner = self.mcp_controller.lock().expect("MCP controller poisoned");
                !owner.fenced
                    && owner
                        .publications
                        .get(&(work.workspace.clone(), work.target.clone()))
                        .is_some_and(|p| {
                            p.operation_id == work.operation && p.state == McpActionState::Pending
                        })
            }
    }
    fn complete_mcp_action(&self, work: Work, result: anyhow::Result<()>) {
        if !self.mcp_action_current(&work) {
            return;
        }
        let mut owner = self.mcp_controller.lock().expect("MCP controller poisoned");
        if !owner
            .publications
            .get(&(work.workspace.clone(), work.target.clone()))
            .is_some_and(|p| p.operation_id == work.operation && p.state == McpActionState::Pending)
        {
            return;
        }
        if result.is_ok() {
            if matches!(
                work.intent,
                McpIntent::Disconnect { .. } | McpIntent::Remove { .. }
            ) {
                self.forget_mcp_oauth(&work.workspace, &work.target);
            }
            self.fence_mcp_reads_after_action(&work.workspace, &work.target);
        } else if matches!(work.intent, McpIntent::Policy { .. })
            && let Some((enabled, implicit)) = work.previous_policy
        {
            self.apply_mcp_policy(&work.workspace, &work.target, enabled, implicit);
        }
        self.publish_mcp_action(
            &mut owner,
            &work.workspace,
            &work.target,
            work.operation,
            if result.is_ok() {
                McpActionState::Succeeded
            } else {
                McpActionState::Failed
            },
            work.intent.action_kind(),
        );
        let preparation_error = result.as_ref().err().and_then(|error| {
            error
                .downcast_ref::<super::oauth::OAuthPreparationError>()
                .copied()
                // Retain compatibility with existing shells' stable occupied-port code.
                .or_else(|| {
                    (error.to_string() == "oauth_callback_port_unavailable")
                        .then_some(super::oauth::OAuthPreparationError::PortUnavailable)
                })
        });
        if let Some(reason) = preparation_error {
            let key = (work.workspace.clone(), work.target.clone());
            let current = owner.publications.get(&key).unwrap();
            let mut publication = (**current).clone();
            publication.revision += 1;
            publication.field_error = Some(super::actions::McpInstallFieldError::Failure {
                message: reason.code().into(),
            });
            let publication = Arc::new(publication);
            owner.publications.insert(key, publication.clone());
            self.publish(
                &ClientMutationAuthority { _private: () },
                ClientScope::McpAction {
                    workspace_id: work.workspace.clone(),
                    target: work.target.clone(),
                },
                crate::threads::registry::revisions(publication.revision),
                publication,
                vec![],
            );
        }
        let refresh_cleanup =
            matches!(work.intent, McpIntent::Disconnect { .. }) && result.is_err();
        drop(owner);
        if refresh_cleanup {
            self.refresh_mcp(&work.workspace);
        }
        self.refresh_plugin_publication(&work.workspace);
    }
    fn complete_mcp_configuration(
        &self,
        work: Work,
        result: anyhow::Result<pioneer_protocol::McpInstallResponse>,
    ) {
        if !self.mcp_action_current(&work) {
            return;
        }
        let mut owner = self.mcp_controller.lock().expect("MCP controller poisoned");
        if owner.fenced
            || !owner
                .publications
                .get(&(work.workspace.clone(), work.target.clone()))
                .is_some_and(|p| {
                    p.operation_id == work.operation && p.state == McpActionState::Pending
                })
        {
            return;
        }
        let (state, field_error, refresh) = match result {
            Ok(response) => {
                let reduction = super::actions::reduce_mcp_install_finish(
                    super::actions::McpInstallFinishOutcome::Response(response),
                );
                (
                    if reduction.close_dialog {
                        McpActionState::Succeeded
                    } else {
                        McpActionState::Failed
                    },
                    reduction.field_error,
                    reduction.queue_refresh,
                )
            }
            Err(_) => (McpActionState::Failed, None, false),
        };
        if refresh {
            self.fence_mcp_reads_after_action(&work.workspace, "configuration");
        }
        let scope = ClientScope::McpAction {
            workspace_id: work.workspace.clone(),
            target: work.target.clone(),
        };
        let revision = self
            .snapshot(&scope)
            .map_or(0, |p| p.revisions().scoped().get())
            + 1;
        let mut field_error = field_error;
        // Validation text may reference supplied values. Scrub all input string values
        // before it crosses the immutable publication boundary, then use the shared sanitizer.
        let mut values = vec![];
        fn strings(value: &serde_json::Value, out: &mut Vec<String>) {
            match value {
                serde_json::Value::String(s) if !s.is_empty() => out.push(s.clone()),
                serde_json::Value::Object(o) => o.values().for_each(|v| strings(v, out)),
                serde_json::Value::Array(a) => a.iter().for_each(|v| strings(v, out)),
                _ => {}
            }
        }
        if let McpIntent::Configure { config_json } = &work.intent
            && let Ok(value) = serde_json::from_str(config_json)
        {
            strings(&value, &mut values);
            values.sort_by_key(|v| std::cmp::Reverse(v.len()));
        }
        if let Some(super::actions::McpInstallFieldError::ValidationIssues(issues)) =
            &mut field_error
        {
            for issue in issues {
                if let super::actions::McpInstallFieldIssue::Diagnostic { message, .. } = issue {
                    for value in &values {
                        *message = message.replace(value, "[redacted]");
                    }
                    *message = pioneer_protocol::sanitize_runtime_diagnostic_line(message);
                }
            }
        }
        let p = Arc::new(McpActionPublication {
            operation_id: work.operation,
            revision,
            state,
            kind: McpActionKind::Configure,
            field_error,
        });
        owner
            .publications
            .insert((work.workspace.clone(), work.target), p.clone());
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(revision),
            p,
            vec![],
        );
        drop(owner);
        if refresh {
            self.refresh_mcp(&work.workspace);
        }
        self.refresh_plugin_publication(&work.workspace);
    }
    pub(crate) fn invalidate_mcp_operations(&self) {
        let mut owner = self.mcp_controller.lock().expect("MCP controller poisoned");
        owner.fenced = true;
        let pending = owner
            .publications
            .iter()
            .filter(|(_, p)| p.state == McpActionState::Pending)
            .map(|((w, t), p)| (w.clone(), t.clone(), p.operation_id, p.kind))
            .collect::<Vec<_>>();
        for (w, t, id, kind) in pending {
            self.publish_mcp_action(&mut owner, &w, &t, id, McpActionState::Cancelled, kind);
        }
    }
    pub(crate) fn start_mcp_operation_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<Work>(32);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-mcp-actions".into())
            .spawn(move || {
                while let Ok(work) = receiver.recv() {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if !core.mcp_action_current(&work) {
                        continue;
                    }
                    let sender = core.transport_runtime().ws_command_sender();
                    let oauth_redirect = if matches!(
                        &work.intent,
                        McpIntent::Configure { .. } | McpIntent::SignIn { .. }
                    ) {
                        core.prepare_mcp_oauth()
                    } else {
                        Ok(None)
                    };
                    drop(core);
                    let bound = sender.requests_for_connection(
                        work.epoch.2.expect("admitted MCP action has a connection"),
                    );
                    if let McpIntent::Configure { config_json } = &work.intent {
                        let params =
                            configuration_params(&work.workspace, config_json, oauth_redirect);
                        let result =
                            crate::transport::ws::command_sender::mcp_install(&bound, params);
                        if let Some(core) = weak.upgrade() {
                            core.complete_mcp_configuration(work, result);
                        }
                        continue;
                    }
                    let result = match &work.intent {
                        McpIntent::RetryAuthorizationBrowser { .. } => weak
                            .upgrade()
                            .ok_or_else(|| anyhow::anyhow!("client_stopped"))
                            .and_then(|core| {
                                anyhow::ensure!(
                                    core.mcp_action_current(&work),
                                    "connection_changed"
                                );
                                core.retry_mcp_oauth_browser_bound(
                                    &work.workspace,
                                    &work.target,
                                    work.epoch,
                                )
                            }),
                        McpIntent::SignIn { .. }
                        | McpIntent::Disconnect { .. }
                        | McpIntent::CancelAuthorization { .. } => {
                            use pioneer_protocol::{
                                McpOAuthAction, McpOAuthParams, McpOAuthResponse,
                                constants::methods,
                            };
                            oauth_redirect.and_then(|redirect| {
                                let action = match &work.intent {
                                    McpIntent::SignIn { .. } => McpOAuthAction::SignIn {
                                        redirect_uri: redirect.ok_or_else(|| {
                                            anyhow::anyhow!("oauth_shell_unavailable")
                                        })?,
                                    },
                                    McpIntent::Disconnect { .. } => McpOAuthAction::Disconnect,
                                    McpIntent::CancelAuthorization { flow_id, .. } => {
                                        McpOAuthAction::Cancel {
                                            flow_id: flow_id.clone(),
                                        }
                                    }
                                    _ => unreachable!(),
                                };
                                crate::rpc::send_json_rpc_request_typed::<McpOAuthResponse, _, _>(
                                    &sender.requests_for_connection(
                                        work.epoch
                                            .2
                                            .ok_or_else(|| anyhow::anyhow!("connection_changed"))?,
                                    ),
                                    methods::MCP_OAUTH,
                                    &McpOAuthParams {
                                        workspace_id: work.workspace.clone(),
                                        server_id: work.target.clone(),
                                        name: work.name.clone(),
                                        scope_kind: work.scope_kind,
                                        action,
                                    },
                                    std::time::Duration::from_secs(60),
                                )
                                .map(|_| ())
                            })
                        }
                        McpIntent::Policy {
                            enabled,
                            allow_implicit_invocation,
                            ..
                        } => crate::transport::ws::command_sender::mcp_policy_set(
                            &bound,
                            super::actions::mcp_policy_set_params(
                                &work.workspace,
                                &work.name,
                                *enabled,
                                *allow_implicit_invocation,
                            ),
                        )
                        .map(|_| ()),
                        McpIntent::Restart { .. } => {
                            crate::transport::ws::command_sender::mcp_server_restart(
                                &bound,
                                super::actions::mcp_server_restart_params(
                                    &work.workspace,
                                    &work.name,
                                ),
                            )
                            .map(|_| ())
                        }
                        McpIntent::Remove { .. } => {
                            crate::transport::ws::command_sender::mcp_uninstall(
                                &bound,
                                super::actions::mcp_uninstall_params(&work.workspace, &work.name),
                            )
                            .map(|_| ())
                        }
                        McpIntent::Configure { .. } => {
                            unreachable!("configuration uses typed validation completion")
                        }
                    };
                    if let Some(core) = weak.upgrade() {
                        core.complete_mcp_action(work, result);
                    }
                }
            })
            .expect("MCP action controller could not start");
        let mut owner = self.mcp_controller.lock().expect("MCP controller poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
}

/// Preparation happens before discovering whether any configured HTTP server
/// needs OAuth. Carry its outcome to the install challenge without failing a
/// public/stdio installation or offering a URL backed by no listener.
fn configuration_params(
    workspace: &str,
    config_json: &str,
    preparation: anyhow::Result<Option<String>>,
) -> pioneer_protocol::McpInstallParams {
    let mut params = super::actions::mcp_install_params(workspace, config_json);
    match preparation {
        Ok(redirect) => params.oauth_redirect_uri = redirect,
        Err(_) => params.oauth_callback_unavailable = true,
    }
    params
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog_test_support::{client, mcp};
    fn fixture() -> (Arc<ClientCore>, mpsc::Receiver<Work>) {
        let core = client();
        core.accept_mcp_catalog_for_test("workspace", mcp(&["a", "b"]));
        let (sender, receiver) = mpsc::sync_channel(2);
        core.mcp_controller.lock().unwrap().sender = Some(sender);
        (core, receiver)
    }
    // NOT_RUN / NOT_COMPILED. No provider, browser or MCP fixture is launched.
    #[test]
    fn plugin_native_admission_never_recaptures_replacement_with_copied_ids() {
        for intent in [
            McpIntent::Configure {
                config_json: r#"{"mcpServers":{"a":{"command":"fake"}}}"#.into(),
            },
            McpIntent::Policy {
                server_id: "a".into(),
                enabled: false,
                allow_implicit_invocation: false,
            },
            McpIntent::Restart {
                server_id: "a".into(),
            },
            McpIntent::Remove {
                server_id: "a".into(),
            },
            McpIntent::SignIn {
                server_id: "a".into(),
            },
            McpIntent::Disconnect {
                server_id: "a".into(),
            },
            McpIntent::CancelAuthorization {
                server_id: "a".into(),
                flow_id: "copied-flow".into(),
            },
            McpIntent::RetryAuthorizationBrowser {
                server_id: "a".into(),
            },
        ] {
            let (core, receiver) = fixture();
            let origin = core.provider_runtime_epoch();
            let pause = Arc::new(std::sync::Barrier::new(2));
            let resume = Arc::new(std::sync::Barrier::new(2));
            let worker_core = core.clone();
            let worker_pause = pause.clone();
            let worker_resume = resume.clone();
            let worker_intent = intent.clone();
            let worker = std::thread::spawn(move || {
                worker_pause.wait();
                worker_resume.wait();
                worker_core.mcp_intent_bound("workspace", worker_intent, origin)
            });
            pause.wait();
            crate::catalog_test_support::switch_connection(&core, 8);
            resume.wait();
            assert_eq!(
                worker.join().unwrap().unwrap_err().to_string(),
                "connection_changed"
            );
            assert!(receiver.try_recv().is_err());
            assert!(
                core.mcp_catalog_snapshot("workspace").unwrap().servers()[0]
                    .policy
                    .enabled
            );
            let admitted = core.provider_runtime_epoch();
            core.mcp_intent_bound("workspace", intent, admitted)
                .unwrap();
            let work = receiver.try_recv().unwrap();
            crate::catalog_test_support::switch_connection(&core, 9);
            assert_eq!(work.epoch, admitted);
            assert!(!core.mcp_action_current(&work));
        }
    }
    struct UnavailableListener;
    impl super::super::oauth::McpOAuthShell for UnavailableListener {
        fn prepare(&self) -> anyhow::Result<String> {
            anyhow::bail!("oauth_callback_port_unavailable")
        }
        fn authorize(
            &self,
            _: &super::super::oauth::OAuthPresentation,
            _: super::super::oauth::OAuthCallbackRelay,
            _: super::super::oauth::OAuthBrowserAdmission,
        ) -> super::super::oauth::OAuthBrowserEffectResult {
            panic!("no browser without a prepared listener")
        }
        fn release(&self, _: &str) {}
        fn shutdown(&self) {}
    }
    #[test]
    fn configure_preparation_failure_survives_install_until_protected_challenge() {
        use pioneer_protocol::*;
        let (core, queue) = fixture();
        core.set_mcp_oauth_shell(Arc::new(UnavailableListener));
        let config = r#"{"mcpServers":{"protected":{"url":"https://protected.example/mcp"},"public":{"url":"https://public.example/mcp"},"local":{"command":"fake"}}}"#;
        core.mcp_intent(
            "workspace",
            McpIntent::Configure {
                config_json: config.into(),
            },
        )
        .unwrap();
        let work = queue.try_recv().unwrap();
        let params = configuration_params(&work.workspace, config, core.prepare_mcp_oauth());
        assert!(params.oauth_callback_unavailable);
        assert!(params.oauth_redirect_uri.is_none());
        assert_eq!(params.config_json, config);
        // Configure itself remains a valid install. Only the protected server's
        // subsequent challenge publishes the actionable preparation failure.
        assert!(
            core.mcp_oauth_presentation("workspace", "protected")
                .is_none()
        );
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(
            McpOAuthNotification {
                workspace_id: "workspace".into(),
                server_id: "protected".into(),
                name: "protected".into(),
                scope_kind: McpScopeKind::Workspace,
                flow_id: Some("operation".into()),
                state: McpOAuthState::Failed,
                authorization_url: None,
                diagnostic: Some("oauth_callback_preparation_failed".into()),
            },
        ));
        let (failure, fallback) = core
            .mcp_oauth_presentation("workspace", "protected")
            .unwrap();
        assert_eq!(failure.state, McpOAuthState::Failed);
        assert!(failure.authorization_url.is_none());
        assert!(!fallback);
        assert!(core.mcp_oauth_presentation("workspace", "public").is_none());
        assert!(core.mcp_oauth_presentation("workspace", "local").is_none());
    }
    struct ConfigurationFailure(super::super::oauth::OAuthPreparationError);
    impl super::super::oauth::McpOAuthShell for ConfigurationFailure {
        fn configuration_error(&self) -> Option<super::super::oauth::OAuthPreparationError> {
            Some(self.0)
        }
        fn prepare(&self) -> anyhow::Result<String> {
            Err(self.0.into())
        }
        fn authorize(
            &self,
            _: &super::super::oauth::OAuthPresentation,
            _: super::super::oauth::OAuthCallbackRelay,
            _: super::super::oauth::OAuthBrowserAdmission,
        ) -> super::super::oauth::OAuthBrowserEffectResult {
            panic!("browser without listener")
        }
        fn release(&self, _: &str) {}
        fn shutdown(&self) {}
    }
    #[test]
    fn preparation_configuration_causes_reach_manual_and_protected_install_publications() {
        use super::super::oauth::OAuthPreparationError;
        use pioneer_protocol::*;
        for reason in [
            OAuthPreparationError::InvalidCallbackPort,
            OAuthPreparationError::ConfigurationLoad,
        ] {
            let (core, queue) = fixture();
            core.set_mcp_oauth_shell(Arc::new(ConfigurationFailure(reason)));
            core.mcp_intent(
                "workspace",
                McpIntent::SignIn {
                    server_id: "a".into(),
                },
            )
            .unwrap();
            let work = queue.try_recv().unwrap();
            let failure = core.prepare_mcp_oauth().unwrap_err();
            core.complete_mcp_action(work, Err(failure));
            let publication = core.mcp_action_snapshot("workspace", "a").unwrap();
            assert_eq!(publication.state, McpActionState::Failed);
            assert!(
                matches!(&publication.field_error, Some(super::super::actions::McpInstallFieldError::Failure { message }) if message == reason.code())
            );
            let config = r#"{"mcpServers":{"protected":{"url":"https://protected.example/mcp"},"public":{"url":"https://public.example/mcp"},"local":{"command":"fake"}}}"#;
            core.mcp_intent(
                "workspace",
                McpIntent::Configure {
                    config_json: config.into(),
                },
            )
            .unwrap();
            let work = queue.try_recv().unwrap();
            let params = configuration_params(&work.workspace, config, core.prepare_mcp_oauth());
            assert!(params.oauth_callback_unavailable);
            assert!(params.oauth_redirect_uri.is_none());
            core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(
                McpOAuthNotification {
                    workspace_id: "workspace".into(),
                    server_id: "protected".into(),
                    name: "protected".into(),
                    scope_kind: McpScopeKind::Workspace,
                    flow_id: Some("flow".into()),
                    state: McpOAuthState::Failed,
                    authorization_url: None,
                    diagnostic: Some("oauth_callback_preparation_failed".into()),
                },
            ));
            let (event, fallback) = core
                .mcp_oauth_presentation("workspace", "protected")
                .unwrap();
            assert_eq!(event.diagnostic.as_deref(), Some(reason.code()));
            assert!(event.authorization_url.is_none());
            assert!(!fallback);
            assert!(core.mcp_oauth_presentation("workspace", "public").is_none());
            assert!(core.mcp_oauth_presentation("workspace", "local").is_none());
        }
    }

    #[test]
    fn actions_have_one_generation_and_optimistic_policy_rolls_back_only_matching_server() {
        let (core, receiver) = fixture();
        let b = core.mcp_catalog_snapshot("workspace").unwrap().servers()[1].clone();
        let intent = || McpIntent::Policy {
            server_id: "a".into(),
            enabled: false,
            allow_implicit_invocation: false,
        };
        let id = core.mcp_intent("workspace", intent()).unwrap();
        assert!(core.mcp_intent("workspace", intent()).is_err());
        assert!(
            !core.mcp_catalog_snapshot("workspace").unwrap().servers()[0]
                .policy
                .enabled
        );
        assert!(Arc::ptr_eq(
            &b,
            &core.mcp_catalog_snapshot("workspace").unwrap().servers()[1]
        ));
        let work = receiver.try_recv().unwrap();
        assert_eq!(work.operation, id);
        core.complete_mcp_action(work, Err(anyhow::anyhow!("synthetic failure")));
        assert_eq!(
            core.mcp_action_snapshot("workspace", "a").unwrap().state,
            McpActionState::Failed
        );
        assert!(
            core.mcp_catalog_snapshot("workspace").unwrap().servers()[0]
                .policy
                .enabled
        );
        assert!(core.skills_catalog_snapshot("workspace").is_none());
        let retry = core.mcp_intent("workspace", intent()).unwrap();
        assert!(retry > id);
        core.complete_mcp_action(receiver.try_recv().unwrap(), Ok(()));
        assert_eq!(
            core.mcp_action_snapshot("workspace", "a").unwrap().state,
            McpActionState::Succeeded
        );
    }
    #[test]
    fn restart_remove_configuration_and_revoke_use_typed_queue_without_echoing_secrets() {
        let (core, receiver) = fixture();
        for intent in [McpIntent::Restart{server_id:"a".into()},McpIntent::Remove{server_id:"b".into()},McpIntent::Configure{config_json:r#"{"mcpServers":{"synthetic":{"command":"fake","env":{"TOKEN":"synthetic-secret"}}}}"#.into()}]{
    core.mcp_intent("workspace",intent).unwrap();let work=receiver.try_recv().unwrap();let target=work.target.clone();core.complete_mcp_action(work,Ok(()));let publication=core.snapshot(&ClientScope::McpAction{workspace_id:"workspace".into(),target}).unwrap();assert!(!publication.snapshot().serialized_payload().to_string().contains("synthetic-secret"));
  }
        core.mcp_intent(
            "workspace",
            McpIntent::Restart {
                server_id: "a".into(),
            },
        )
        .unwrap();
        let work = receiver.try_recv().unwrap();
        core.invalidate_mcp_operations();
        let before = core.mcp_action_snapshot("workspace", "a").unwrap();
        core.complete_mcp_action(work, Ok(()));
        assert!(Arc::ptr_eq(
            &before,
            &core.mcp_action_snapshot("workspace", "a").unwrap()
        ));
        assert!(
            core.mcp_intent(
                "workspace",
                McpIntent::Restart {
                    server_id: "a".into()
                }
            )
            .is_err()
        );
    }
    #[test]
    fn configuration_partial_result_keeps_validation_open_and_redacts_input_values() {
        use pioneer_protocol::*;
        let (core, receiver) = fixture();
        core.mcp_intent("workspace",McpIntent::Configure{config_json:r#"{"mcpServers":{"synthetic":{"command":"fake","env":{"TOKEN":"private-value"}}}}"#.into()}).unwrap();
        let work = receiver.try_recv().unwrap();
        core.complete_mcp_configuration(
            work,
            Ok(McpInstallResponse {
                status: McpInstallStatus::Partial,
                servers: vec![McpInstallResult {
                    name: "synthetic".into(),
                    status: McpInstallResultStatus::ValidationError,
                    diagnostics: vec![McpValidationDiagnostic {
                        code: "synthetic".into(),
                        level: McpDiagnosticLevel::Error,
                        message: "rejected private-value".into(),
                        field_path: Some("env.TOKEN".into()),
                    }],
                    server: None,
                }],
                audit: McpLifecycleAuditSummary { events_written: 0 },
            }),
        );
        let result = core
            .mcp_action_snapshot("workspace", "configuration")
            .unwrap();
        assert_eq!(result.state, McpActionState::Failed);
        assert!(result.field_error.is_some());
        let serialized = serde_json::to_string(&*result).unwrap();
        assert!(!serialized.contains("private-value"));
        assert!(serialized.contains("[redacted]"));
    }
}
