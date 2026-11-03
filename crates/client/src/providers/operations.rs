//! Provider mutations share one generation and permission gate per process.
use super::{cli_runtime_settings::*, runtime::ProviderRuntimeIntent, store::*};
use crate::core::*;
use pioneer_protocol::*;
use std::sync::Arc;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", content = "params", rename_all = "snake_case")]
pub enum ProviderCommand {
    Configure(ProviderConfigureParams),
    Disconnect(ProviderDeleteApiKeyParams),
    Connect(CLIRuntimeLoginStartParams),
    SetRuntimeProxy(CLIRuntimeProxySetParams),
    RemoveRuntimeProxy(CLIRuntimeProxyDeleteParams),
    SaveRuntime {
        workspace_id: String,
        draft: CLIRuntimeProviderDraft,
    },
    SetRuntimeEnabled {
        workspace_id: String,
        runtime_id: String,
        enabled: bool,
    },
}
impl std::fmt::Debug for ProviderCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderCommand")
            .field("kind", &self.kind())
            .finish_non_exhaustive()
    }
}
impl PartialEq for ProviderCommand {
    fn eq(&self, other: &Self) -> bool {
        serde_json::to_value(self).ok() == serde_json::to_value(other).ok()
    }
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderActionKind {
    Configure,
    Disconnect,
    Connect,
    SetRuntimeProxy,
    RemoveRuntimeProxy,
    SaveRuntime,
    SetRuntimeEnabled,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ProviderOperationPublication {
    revision: u64,
    generation: u64,
    workspace_id: String,
    target: String,
    action: ProviderActionKind,
    request: ProviderLoadState,
}
impl ProviderOperationPublication {
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }
    pub fn target(&self) -> &str {
        &self.target
    }
    pub fn action(&self) -> ProviderActionKind {
        self.action
    }
    pub fn request(&self) -> ProviderLoadState {
        self.request
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Identity {
    generation: u64,
    epoch: (u64, u64, Option<u64>),
    workspace_id: String,
    target: String,
    kind: ProviderActionKind,
}
pub struct ProviderOperation {
    identity: Identity,
    command: ProviderCommand,
}
// Login details are delivered once to the initiating operation, never published.
pub enum ProviderOperationCompletion {
    Completed,
    Login(CLIRuntimeLoginStartResponse),
}
struct ProviderWork {
    work_context: pioneer_observability::work::WorkCarrier,
    operation: ProviderOperation,
    reply: std::sync::mpsc::SyncSender<anyhow::Result<ProviderOperationCompletion>>,
}
#[derive(Default)]
pub(crate) struct ProviderController {
    pub(super) credentials: super::credentials::CredentialRequests,
    generation: u64,
    pub(super) effect_generation: u64,
    active: Option<Identity>,
    prepared: Option<ProviderOperation>,
    sender: Option<std::sync::mpsc::SyncSender<ProviderWork>>,
    observer: Option<pioneer_observability::queues::QueueObserver>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl ProviderController {
    pub(crate) fn stop(&mut self) {
        self.active = None;
        self.prepared = None;
        self.credentials.stop();
        self.sender.take();
    }
}
impl Drop for ProviderController {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}
impl ProviderCommand {
    fn workspace(&self) -> &str {
        match self {
            Self::Configure(p) => &p.workspace_id,
            Self::Disconnect(p) => &p.workspace_id,
            Self::Connect(p) => &p.workspace_id,
            Self::SetRuntimeProxy(p) => &p.workspace_id,
            Self::RemoveRuntimeProxy(p) => &p.workspace_id,
            Self::SaveRuntime { workspace_id, .. }
            | Self::SetRuntimeEnabled { workspace_id, .. } => workspace_id,
        }
    }
    fn target(&self) -> &str {
        match self {
            Self::Configure(p) => &p.provider,
            Self::Disconnect(p) => &p.provider,
            Self::Connect(p) => &p.runtime_id,
            Self::SetRuntimeProxy(p) => &p.runtime_id,
            Self::RemoveRuntimeProxy(p) => &p.runtime_id,
            Self::SaveRuntime { draft, .. } => &draft.id,
            Self::SetRuntimeEnabled { runtime_id, .. } => runtime_id,
        }
    }
    fn kind(&self) -> ProviderActionKind {
        match self {
            Self::Configure(_) => ProviderActionKind::Configure,
            Self::Disconnect(_) => ProviderActionKind::Disconnect,
            Self::Connect(_) => ProviderActionKind::Connect,
            Self::SetRuntimeProxy(_) => ProviderActionKind::SetRuntimeProxy,
            Self::RemoveRuntimeProxy(_) => ProviderActionKind::RemoveRuntimeProxy,
            Self::SaveRuntime { .. } => ProviderActionKind::SaveRuntime,
            Self::SetRuntimeEnabled { .. } => ProviderActionKind::SetRuntimeEnabled,
        }
    }
}
impl ClientCore {
    pub fn provider_operation_snapshot(
        &self,
        workspace_id: &str,
    ) -> Option<Arc<ProviderOperationPublication>> {
        self.snapshot(&ClientScope::ProviderOperation {
            workspace_id: workspace_id.into(),
        })
        .and_then(|p| p.snapshot().payload())
    }
    fn publish_provider_operation(&self, identity: &Identity, request: ProviderLoadState) {
        let scope = ClientScope::ProviderOperation {
            workspace_id: identity.workspace_id.clone(),
        };
        let revision = self
            .snapshot(&scope)
            .map_or(0, |p| p.revisions().scoped().get())
            .checked_add(1)
            .expect("provider operation revision exhausted");
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(revision),
            Arc::new(ProviderOperationPublication {
                revision,
                generation: identity.generation,
                workspace_id: identity.workspace_id.clone(),
                target: identity.target.clone(),
                action: identity.kind,
                request,
            }),
            vec![],
        );
    }
    pub fn prepare_provider_command(
        &self,
        command: ProviderCommand,
    ) -> anyhow::Result<ProviderOperation> {
        let capabilities = self
            .authorization_snapshot(Some(command.workspace()), None)
            .or_else(|| self.authorization_snapshot(None, None))
            .as_ref()
            .map(crate::authorization::principal_presentation_capabilities)
            .unwrap_or_default();
        anyhow::ensure!(
            !self.is_stopped()
                && capabilities.can_manage_capabilities
                && !command.workspace().is_empty()
                && !command.target().trim().is_empty(),
            "provider_action_forbidden"
        );
        let epoch = self.provider_runtime_epoch();
        // A prepared command may never have been claimed by its shell after a
        // transport change. Retire that exact owner before admitting a retry.
        let stale = self
            .provider_controller
            .lock()
            .expect("provider controller poisoned")
            .active
            .as_ref()
            .filter(|active| active.epoch != epoch)
            .cloned();
        if let Some(identity) = stale {
            let _ = self.finish_provider_operation(
                &identity,
                Err(anyhow::anyhow!("provider_action_cancelled")),
            );
        }
        let mut owner = self
            .provider_controller
            .lock()
            .expect("provider controller poisoned");
        anyhow::ensure!(owner.active.is_none(), "provider_action_pending");
        owner.generation = owner
            .generation
            .checked_add(1)
            .expect("provider action generation exhausted");
        let identity = Identity {
            generation: owner.generation,
            epoch,
            workspace_id: command.workspace().into(),
            target: command.target().into(),
            kind: command.kind(),
        };
        owner.active = Some(identity.clone());
        self.publish_provider_operation(&identity, ProviderLoadState::Loading);
        Ok(ProviderOperation { identity, command })
    }
    pub fn provider_command_intent(&self, command: ProviderCommand) -> ClientTransition {
        let Ok(prepared) = self.prepare_provider_command(command) else {
            return self.reject_intent();
        };
        let mut owner = self
            .provider_controller
            .lock()
            .expect("provider controller poisoned");
        if owner.active.as_ref() != Some(&prepared.identity) {
            return self.reject_intent();
        }
        owner.prepared = Some(prepared);
        self.navigation_outcome(ClientTransitionOutcome::Changed)
    }
    pub fn take_provider_operation(
        &self,
        workspace: &str,
        generation: u64,
    ) -> anyhow::Result<ProviderOperation> {
        let epoch = self.provider_runtime_epoch();
        let mut owner = self
            .provider_controller
            .lock()
            .expect("provider controller poisoned");
        if let Some(identity) = owner
            .active
            .as_ref()
            .filter(|identity| {
                identity.workspace_id == workspace
                    && identity.generation == generation
                    && (self.is_stopped() || identity.epoch != epoch)
            })
            .cloned()
        {
            drop(owner);
            let _ = self.finish_provider_operation(
                &identity,
                Err(anyhow::anyhow!("provider_action_cancelled")),
            );
            anyhow::bail!("provider_action_cancelled");
        }
        anyhow::ensure!(
            !self.is_stopped()
                && owner
                    .active
                    .as_ref()
                    .is_some_and(|active| active.workspace_id == workspace
                        && active.generation == generation
                        && active.epoch == epoch),
            "provider_action_cancelled"
        );
        owner
            .prepared
            .take()
            .ok_or_else(|| anyhow::anyhow!("provider_operation_already_claimed"))
    }
    pub fn execute_provider_command(
        self: &Arc<Self>,
        command: ProviderCommand,
    ) -> anyhow::Result<ProviderOperationCompletion> {
        self.prepare_provider_command(command)?
            .execute(Arc::downgrade(self))
    }
    pub fn execute_provider_operation(
        self: &Arc<Self>,
        operation: ProviderOperation,
    ) -> anyhow::Result<ProviderOperationCompletion> {
        operation.execute(Arc::downgrade(self))
    }
    pub(crate) fn start_provider_operation_controller(self: &Arc<Self>) {
        let (sender, receiver) = std::sync::mpsc::sync_channel::<ProviderWork>(1);
        let queue_observer = pioneer_observability::queues::QueueObserver::new(
            pioneer_observability::queues::QueueKind::ClientController,
            Some(1),
            "unsupported: controller request payload/shared references are not serialized",
        );
        let worker_queue = queue_observer.clone();
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-provider-actions".into())
            .spawn(move || {
                while let Ok(work) = receiver.recv() {
                    worker_queue.dequeued(pioneer_observability::queues::QueueBytes::Unsupported);
                    let mut processing = worker_queue.processing();
                    let carrier = work.work_context.clone().received(
                        pioneer_observability::work::Boundary::StdThread,
                        pioneer_observability::work::WorkKind::ClientCommand,
                        pioneer_observability::work::ExecutorRole::ClientController,
                    );
                    let _scope = carrier.enter();
                    let mut active_work = pioneer_observability::live::WorkGuard::start(
                        pioneer_observability::work::current(),
                        false,
                    );
                    let result = work.operation.execute_direct(weak.clone());
                    let outcome = if result.is_ok() {
                        pioneer_observability::work::Outcome::Completed
                    } else {
                        pioneer_observability::work::Outcome::Failed
                    };
                    let _ = work.reply.try_send(result);
                    processing.processed();
                    active_work.finish(outcome);
                }
            })
            .expect("provider action worker");
        let mut owner = self
            .provider_controller
            .lock()
            .expect("provider controller poisoned");
        owner.observer = Some(queue_observer);
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
    fn provider_operation_current(&self, identity: &Identity) -> bool {
        !self.is_stopped()
            && self.provider_runtime_epoch() == identity.epoch
            && self
                .provider_controller
                .lock()
                .expect("provider controller poisoned")
                .active
                .as_ref()
                == Some(identity)
    }
    fn finish_provider_operation(
        &self,
        identity: &Identity,
        result: anyhow::Result<ProviderOperationCompletion>,
    ) -> anyhow::Result<ProviderOperationCompletion> {
        let epoch = self.provider_runtime_epoch();
        let applicable = !self.is_stopped()
            && identity.epoch == epoch
            && self.capability_management_allowed(&identity.workspace_id);
        let mut owner = self
            .provider_controller
            .lock()
            .expect("provider controller poisoned");
        anyhow::ensure!(
            owner.active.as_ref() == Some(identity),
            "provider_action_cancelled"
        );
        owner.active = None;
        owner.prepared = None;
        self.publish_provider_operation(
            identity,
            if !applicable {
                ProviderLoadState::Cancelled
            } else if result.is_ok() {
                ProviderLoadState::Ready
            } else {
                ProviderLoadState::Failed
            },
        );
        drop(owner);
        if self.capability_management_allowed(&identity.workspace_id) {
            let provider = if matches!(
                identity.kind,
                ProviderActionKind::Configure | ProviderActionKind::Disconnect
            ) {
                identity.target.clone()
            } else {
                super::list::cli_runtime_provider_key(&identity.target)
            };
            // The accepted settings ACK is authoritative for this toggle. Keep
            // catalog/runtime reconciliation, but do not ask a disabled runtime
            // for models just because its collection already exists.
            let disabled = applicable
                && result.is_ok()
                && identity.kind == ProviderActionKind::SetRuntimeEnabled
                && self
                    .gateway_settings()
                    .settings
                    .as_ref()
                    .is_some_and(|settings| {
                        settings
                            .cli_runtimes
                            .instances
                            .iter()
                            .any(|runtime| runtime.id == identity.target && !runtime.enabled)
                    });
            if !disabled {
                self.refresh_provider_models(&identity.workspace_id, &provider);
            }
            self.provider_collection_intent(ProviderCollectionIntent::Refresh {
                key: ProviderCollectionKey::catalog(&identity.workspace_id),
            });
            self.provider_runtime_intent(ProviderRuntimeIntent::Refresh {
                workspace_id: identity.workspace_id.clone(),
            });
        }
        anyhow::ensure!(applicable, "provider_action_cancelled");
        result.map_err(|_| anyhow::anyhow!("provider_action_failed"))
    }
    pub(crate) fn invalidate_provider_operations(&self) {
        let mut owner = self
            .provider_controller
            .lock()
            .expect("provider controller poisoned");
        owner.active = None;
        owner.prepared = None;
        owner.credentials.invalidate();
    }
    pub fn cancel_provider_operations(&self, workspace: &str) {
        let epoch = self.provider_runtime_epoch();
        let mut owner = self
            .provider_controller
            .lock()
            .expect("provider controller poisoned");
        if let Some(identity) = owner
            .active
            .clone()
            .filter(|active| active.workspace_id == workspace)
        {
            owner.active = None;
            owner.prepared = None;
            self.publish_provider_operation(&identity, ProviderLoadState::Cancelled);
        } else if let Some(publication) = self.provider_operation_snapshot(workspace)
            && publication.request() == ProviderLoadState::Failed
        {
            // Releasing a surface retires its completed error too. Keep the
            // generation so a late completion cannot replace a newer command.
            self.publish_provider_operation(
                &Identity {
                    generation: publication.generation(),
                    epoch,
                    workspace_id: workspace.into(),
                    target: publication.target().into(),
                    kind: publication.action(),
                },
                ProviderLoadState::Cancelled,
            );
        }
    }
}

impl ProviderOperation {
    pub fn execute(
        self,
        client: std::sync::Weak<ClientCore>,
    ) -> anyhow::Result<ProviderOperationCompletion> {
        let core = client
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("provider_action_cancelled"))?;
        let identity = self.identity.clone();
        if !core.provider_operation_current(&identity) {
            return core.finish_provider_operation(
                &identity,
                Err(anyhow::anyhow!("provider_action_cancelled")),
            );
        }
        let (reply, receiver) = std::sync::mpsc::sync_channel(1);
        let queue_owner = core
            .provider_controller
            .lock()
            .expect("provider controller poisoned");
        let queued = queue_owner.sender.as_ref().is_some_and(|sender| {
            pioneer_observability::queues::observe_try_send(
                queue_owner.observer.as_ref(),
                pioneer_observability::queues::QueueBytes::Unsupported,
                || {
                    sender.try_send(ProviderWork {
                        work_context: pioneer_observability::work::WorkCarrier::child(
                            pioneer_observability::work::Boundary::StdThread,
                            pioneer_observability::work::WorkKind::ClientCommand,
                            pioneer_observability::work::Trigger::Event,
                            pioneer_observability::work::ExecutorRole::ClientController,
                        ),
                        operation: self,
                        reply,
                    })
                },
            )
            .is_ok()
        });
        drop(queue_owner);
        if !queued {
            return core.finish_provider_operation(
                &identity,
                Err(anyhow::anyhow!("provider_queue_unavailable")),
            );
        }
        drop(core);
        loop {
            match receiver.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(result) => return result,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    anyhow::ensure!(
                        client.upgrade().is_some_and(|core| !core.is_stopped()
                            && core.provider_runtime_epoch() == identity.epoch),
                        "provider_action_cancelled"
                    );
                }
                Err(_) => anyhow::bail!("provider_action_cancelled"),
            }
        }
    }
    fn execute_direct(
        self,
        client: std::sync::Weak<ClientCore>,
    ) -> anyhow::Result<ProviderOperationCompletion> {
        let core = client
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("provider_action_cancelled"))?;
        if !core.provider_operation_current(&self.identity) {
            return core.finish_provider_operation(
                &self.identity,
                Err(anyhow::anyhow!("provider_action_cancelled")),
            );
        }
        let sender = core.transport_runtime().ws_command_sender();
        let settings = match &self.command {
            ProviderCommand::SaveRuntime { draft, .. } => {
                Some(plan_cli_runtime_provider_draft_update(
                    core.gateway_settings().settings.as_ref(),
                    draft,
                ))
            }
            ProviderCommand::SetRuntimeEnabled {
                runtime_id,
                enabled,
                ..
            } => Some(plan_cli_runtime_provider_enabled_update(
                core.gateway_settings().settings.as_ref(),
                runtime_id,
                *enabled,
            )),
            _ => None,
        }
        .map(|plan| {
            let CLIRuntimeProviderSettingsPlan::Send(plan) = plan else {
                anyhow::bail!("provider_settings_rejected");
            };
            Ok((
                core.prepare_gateway_settings_update(Some(plan.snapshot))?,
                plan.update,
            ))
        });
        drop(core);
        let result = match self.command {
            ProviderCommand::Configure(params) => sender
                .provider_configure(params)
                .map(|_| ProviderOperationCompletion::Completed),
            ProviderCommand::Disconnect(params) => sender
                .provider_delete_api_key(params)
                .map(|_| ProviderOperationCompletion::Completed),
            ProviderCommand::Connect(params) => sender
                .cli_runtime_login_start(params)
                .map(ProviderOperationCompletion::Login),
            ProviderCommand::SetRuntimeProxy(params) => sender
                .cli_runtime_proxy_set(params)
                .map(|_| ProviderOperationCompletion::Completed),
            ProviderCommand::RemoveRuntimeProxy(params) => sender
                .cli_runtime_proxy_delete(params)
                .map(|_| ProviderOperationCompletion::Completed),
            ProviderCommand::SaveRuntime { .. } | ProviderCommand::SetRuntimeEnabled { .. } => {
                (|| {
                    let (generation, update) = settings.expect("settings command plan")?;
                    let mut result = sender
                        .gateway_settings_update(update)
                        .map(|response| response.settings);
                    let core = client
                        .upgrade()
                        .ok_or_else(|| anyhow::anyhow!("provider_action_cancelled"))?;
                    anyhow::ensure!(
                        core.finish_gateway_settings(generation, &mut result),
                        "provider_action_cancelled"
                    );
                    result.map(|_| ProviderOperationCompletion::Completed)
                })()
            }
        };
        client
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("provider_action_cancelled"))?
            .finish_provider_operation(&self.identity, result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> ClientCore {
        let core = ClientCore::new();
        core.accept_authorization_projection(
            0,
            None,
            AuthorizationCapabilitySnapshot {
                schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
                authorization_revision: 1,
                principal_id: PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").unwrap(),
                role_key: "admin".into(),
                role: AuthorizationRolePresentation {
                    key: "admin".into(),
                    display_name: "Administrator".into(),
                    description: String::new(),
                    built_in: false,
                },
                global: AuthorizationGlobalCapabilities {
                    can_manage_capabilities: true,
                    ..Default::default()
                },
                workspace: None,
                thread: None,
            },
        );
        core
    }
    fn configure() -> ProviderCommand {
        ProviderCommand::Configure(ProviderConfigureParams {
            workspace_id: "workspace".into(),
            provider: "openai".into(),
            api_key: Some("synthetic-form-value".into()),
            proxy_url: None,
            clear_proxy: false,
            base_url: None,
            clear_base_url: false,
        })
    }
    fn settings_snapshot() -> GatewaySettingsSnapshot {
        GatewaySettingsSnapshot {
            general: Default::default(),
            memory: Default::default(),
            self_improvement: Default::default(),
            self_improvement_status: None,
            thread_episodic: Default::default(),
            cli_runtimes: Default::default(),
            remote_access: Default::default(),
            voice_input: Default::default(),
        }
    }

    #[test]
    fn only_successful_disabled_toggle_skips_existing_model_collection_refresh() {
        use crate::rpc::{JsonRpcRequestTransport, JsonRpcResponseSender};
        use std::sync::Mutex;

        #[derive(Default)]
        struct CatalogTransport(Mutex<Vec<(String, String)>>);
        impl JsonRpcRequestTransport for CatalogTransport {
            fn send_json_rpc_request(
                &self,
                _: String,
                payload: String,
                reply: JsonRpcResponseSender,
            ) -> Result<(), String> {
                let request: JsonRpcRequest = serde_json::from_str(&payload).unwrap();
                let params = request.params.unwrap();
                let runtime = params["runtime_id"].as_str().unwrap_or_default();
                self.0
                    .lock()
                    .unwrap()
                    .push((request.method.clone(), runtime.into()));
                let result = match request.method.as_str() {
                    constants::methods::CLI_RUNTIME_LIST_MODELS => serde_json::json!({
                        "runtime_id": runtime, "models": [{"id": "fixture-model"}],
                    }),
                    constants::methods::PROVIDER_LIST => serde_json::json!({
                        "providers": [], "definitions": [],
                    }),
                    _ => panic!("unexpected catalog request: {}", request.method),
                };
                reply.send(Ok(result)).unwrap();
                Ok(())
            }
        }

        for runtime_id in ["codex", "claude", "opencode", "codex_two"] {
            // The proxy operation also retains refresh when settings say disabled.
            for (enabled, success, proxy) in [
                (false, true, false),
                (true, true, false),
                (false, false, false),
                (false, true, true),
            ] {
                let core = crate::catalog_test_support::settings_model_picker_client();
                let mut initial = settings_snapshot();
                let mut instance = initial.cli_runtimes.instances[0].clone();
                instance.id = runtime_id.into();
                instance.kind = match runtime_id {
                    "claude" => CLIAgentRuntimeKind::Claude,
                    "opencode" => CLIAgentRuntimeKind::OpenCode,
                    _ => CLIAgentRuntimeKind::Codex,
                };
                instance.enabled = !enabled && success && !proxy;
                let mut other_instance = instance.clone();
                other_instance.id = "unrelated".into();
                other_instance.display_name = "Unrelated CLI".into();
                other_instance.nickname = "unrelated".into();
                other_instance.enabled = true;
                initial.cli_runtimes.instances = vec![instance, other_instance];
                let load = core.request_gateway_settings().unwrap();
                assert!(core.finish_gateway_settings(load, &mut Ok(initial.clone())));
                let transport = Arc::new(CatalogTransport::default());
                *core
                    .transport_runtime()
                    .ws_command_sender()
                    .test_requests
                    .lock()
                    .unwrap() = Some(transport.clone());
                core.start_provider_controller();
                let key = ProviderCollectionKey::models(
                    "workspace",
                    super::super::list::cli_runtime_provider_key(runtime_id),
                    ProviderModelKind::Chat,
                );
                let other = ProviderCollectionKey::models(
                    "workspace",
                    "cli_runtime:unrelated",
                    ProviderModelKind::Chat,
                );
                let read = |key: ProviderCollectionKey| {
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
                    core.read_provider_collection(key, false)
                        .unwrap()
                        .wait_while(|| std::time::Instant::now() < deadline)
                        .unwrap()
                };
                assert_eq!(read(key.clone()).models()[0].id(), "fixture-model");
                let other_before = read(other.clone());
                transport.0.lock().unwrap().clear();
                let command = if proxy {
                    ProviderCommand::RemoveRuntimeProxy(CLIRuntimeProxyDeleteParams {
                        workspace_id: "workspace".into(),
                        runtime_id: runtime_id.into(),
                    })
                } else {
                    ProviderCommand::SetRuntimeEnabled {
                        workspace_id: "workspace".into(),
                        runtime_id: runtime_id.into(),
                        enabled,
                    }
                };
                let operation = core.prepare_provider_command(command).unwrap();
                let mut expected = initial;
                let result = if proxy {
                    Ok(ProviderOperationCompletion::Completed)
                } else {
                    let CLIRuntimeProviderSettingsPlan::Send(plan) =
                        plan_cli_runtime_provider_enabled_update(
                            Some(&expected),
                            runtime_id,
                            enabled,
                        )
                    else {
                        panic!("loaded CLI settings")
                    };
                    let save = core
                        .prepare_gateway_settings_update(Some(plan.snapshot.clone()))
                        .unwrap();
                    let mut ack = if success {
                        expected = plan.snapshot.clone();
                        Ok(plan.snapshot)
                    } else {
                        Err(anyhow::anyhow!("fixture settings save failed"))
                    };
                    assert!(core.finish_gateway_settings(save, &mut ack));
                    ack.map(|_| ProviderOperationCompletion::Completed)
                };
                assert_eq!(
                    core.finish_provider_operation(&operation.identity, result)
                        .is_ok(),
                    success
                );
                // Catalog completion is a FIFO barrier after any model request.
                let catalog = read(ProviderCollectionKey::catalog("workspace"));
                assert_eq!(catalog.request(), ProviderLoadState::Ready);
                let calls = transport.0.lock().unwrap().clone();
                let refresh_models = enabled || !success || proxy;
                assert_eq!(
                    calls,
                    if refresh_models {
                        vec![
                            (
                                constants::methods::CLI_RUNTIME_LIST_MODELS.into(),
                                runtime_id.into(),
                            ),
                            (constants::methods::PROVIDER_LIST.into(), String::new()),
                        ]
                    } else {
                        vec![(constants::methods::PROVIDER_LIST.into(), String::new())]
                    }
                );
                assert!(
                    core.provider_runtime_request_for_test("workspace")
                        .is_some()
                );
                assert_eq!(core.gateway_settings().settings, Some(expected));
                assert!(Arc::ptr_eq(
                    &other_before,
                    &core.provider_collection_snapshot(&other).unwrap()
                ));
                assert_eq!(
                    core.provider_collection_snapshot(&key).unwrap().models()[0].id(),
                    "fixture-model"
                );
            }
        }
    }
    #[test]
    fn retired_ticket_releases_exact_active_provider_operation() {
        for completed in [true, false] {
            let core = fixture();
            let first = core.prepare_provider_command(configure()).unwrap();
            crate::catalog_test_support::replace_connection(&core, 8);
            if completed {
                assert!(
                    core.finish_provider_operation(
                        &first.identity,
                        Ok(ProviderOperationCompletion::Completed)
                    )
                    .is_err()
                );
                assert_eq!(
                    core.provider_operation_snapshot("workspace")
                        .unwrap()
                        .request(),
                    ProviderLoadState::Cancelled
                );
            }
            let next = core.prepare_provider_command(configure()).unwrap();
            assert!(next.identity.generation > first.identity.generation);
            let pending = core.provider_operation_snapshot("workspace").unwrap();
            assert!(
                core.finish_provider_operation(&first.identity, Err(anyhow::anyhow!("late")))
                    .is_err()
            );
            assert!(Arc::ptr_eq(
                &pending,
                &core.provider_operation_snapshot("workspace").unwrap()
            ));
        }
    }

    #[test]
    fn command_claim_and_completion_have_one_owner_and_never_publish_credential_input() {
        let core = fixture();
        assert!(!format!("{:?}", configure()).contains("synthetic-form-value"));
        assert_eq!(
            core.provider_command_intent(configure()).outcome(),
            ClientTransitionOutcome::Changed
        );
        let loading = core.provider_operation_snapshot("workspace").unwrap();
        let serialized = serde_json::to_string(&loading).unwrap();
        assert!(!serialized.contains("synthetic-form-value"));
        assert!(!serialized.contains("api_key"));
        assert!(
            core.take_provider_operation("other", loading.generation())
                .is_err()
        );
        assert!(
            core.take_provider_operation("workspace", loading.generation() + 1)
                .is_err()
        );
        let operation = core
            .take_provider_operation("workspace", loading.generation())
            .unwrap();
        assert!(
            core.take_provider_operation("workspace", loading.generation())
                .is_err()
        );
        assert!(core.prepare_provider_command(configure()).is_err());
        core.finish_provider_operation(
            &operation.identity,
            Ok(ProviderOperationCompletion::Completed),
        )
        .unwrap();
        let ready = core.provider_operation_snapshot("workspace").unwrap();
        assert_eq!(ready.request(), ProviderLoadState::Ready);
        assert!(
            core.finish_provider_operation(
                &operation.identity,
                Ok(ProviderOperationCompletion::Completed)
            )
            .is_err()
        );
        assert!(Arc::ptr_eq(
            &ready,
            &core.provider_operation_snapshot("workspace").unwrap()
        ));
    }
    #[test]
    fn cli_save_ack_survives_queued_refresh_and_both_old_read_response_orders() {
        use crate::settings::runtime::SettingsIntent;
        let initial = settings_snapshot();
        // None: Refresh is queued but has not reserved a read ticket yet.
        // Some: the old read replies before/after the write ACK.
        for old_read_before_ack in [None, Some(true), Some(false)] {
            let core = crate::catalog_test_support::settings_model_picker_client();
            let load = core.request_gateway_settings().unwrap();
            assert!(core.finish_gateway_settings(load, &mut Ok(initial.clone())));
            assert_eq!(
                core.settings_intent(SettingsIntent::Refresh).outcome(),
                ClientTransitionOutcome::Changed
            );
            let epoch = core.take_settings_refresh_for_test().unwrap();
            let old_read = old_read_before_ack.map(|_| {
                core.request_gateway_settings_for_epoch(&epoch)
                    .unwrap()
                    .unwrap()
            });
            let operation = core
                .prepare_provider_command(ProviderCommand::SetRuntimeEnabled {
                    workspace_id: "workspace".into(),
                    runtime_id: "codex".into(),
                    enabled: false,
                })
                .unwrap();
            let CLIRuntimeProviderSettingsPlan::Send(plan) =
                plan_cli_runtime_provider_enabled_update(
                    core.gateway_settings().settings.as_ref(),
                    "codex",
                    false,
                )
            else {
                panic!("loaded CLI settings")
            };
            let disabled = plan.snapshot;
            let save = core
                .prepare_gateway_settings_update(Some(disabled.clone()))
                .unwrap();
            if old_read.is_none() {
                // The provider worker wins the gap between dequeue and RPC admission.
                assert!(
                    core.request_gateway_settings_for_epoch(&epoch)
                        .unwrap()
                        .is_none()
                );
                assert!(core.gateway_settings().saving);
                assert!(!core.gateway_settings().loading);
            }
            if old_read_before_ack == Some(true) {
                assert!(!core.finish_gateway_settings(old_read.unwrap(), &mut Ok(initial.clone())));
                core.complete_settings_refresh_for_test(&epoch, false);
            }
            assert_eq!(
                core.settings_intent(SettingsIntent::Refresh).outcome(),
                ClientTransitionOutcome::Noop
            );
            let mut ack = Ok(disabled.clone());
            let accepted = core.finish_gateway_settings(save, &mut ack);
            // Use the same ACK gate and terminal operation owner as the provider worker.
            let result = if accepted {
                ack.map(|_| ProviderOperationCompletion::Completed)
            } else {
                Err(anyhow::anyhow!("provider_action_cancelled"))
            };
            core.finish_provider_operation(&operation.identity, result)
                .unwrap();
            assert_eq!(
                core.provider_operation_snapshot("workspace")
                    .unwrap()
                    .request(),
                ProviderLoadState::Ready
            );
            if old_read_before_ack == Some(false) {
                assert!(!core.finish_gateway_settings(old_read.unwrap(), &mut Ok(initial.clone())));
                core.complete_settings_refresh_for_test(&epoch, false);
            }
            assert!(
                !core
                    .gateway_settings()
                    .settings
                    .unwrap()
                    .cli_runtimes
                    .instances[0]
                    .enabled
            );
            assert_eq!(
                core.settings_intent(SettingsIntent::Refresh).outcome(),
                ClientTransitionOutcome::Noop
            );
            let followup = core
                .take_settings_refresh_for_test()
                .expect("one coalesced refresh after save");
            assert!(core.take_settings_refresh_for_test().is_none());
            let load = core
                .request_gateway_settings_for_epoch(&followup)
                .unwrap()
                .unwrap();
            assert!(core.finish_gateway_settings(load, &mut Ok(disabled)));
            core.complete_settings_refresh_for_test(&followup, true);
            let settings = core.gateway_settings();
            assert!(!settings.settings.unwrap().cli_runtimes.instances[0].enabled);
            assert!(settings.error.is_none());
            assert!(!settings.loading && !settings.saving);
            assert_eq!(
                core.settings_intent(SettingsIntent::Refresh).outcome(),
                ClientTransitionOutcome::Changed
            );
        }

        // ACK arrives in B either without a replacement ticket or after a read of B.
        for replace_save_with_read in [false, true] {
            let core = crate::catalog_test_support::settings_model_picker_client();
            let select_workspace = |id: &str| {
                core.navigate(
                    crate::navigation::NavigationIntent::SelectWorkspace {
                        workspace_id: Some(id.into()),
                    },
                    None,
                );
            };
            let load = core.request_gateway_settings().unwrap();
            assert!(core.finish_gateway_settings(load, &mut Ok(initial.clone())));
            let mut disabled = initial.clone();
            disabled.cli_runtimes.instances[0].enabled = false;
            let save = core
                .prepare_gateway_settings_update(Some(disabled.clone()))
                .unwrap();
            if replace_save_with_read {
                core.settings_intent(SettingsIntent::Refresh);
                let deferred = core.take_settings_refresh_for_test().unwrap();
                assert!(
                    core.request_gateway_settings_for_epoch(&deferred)
                        .unwrap()
                        .is_none()
                );
            }
            select_workspace("other-workspace");
            let replacement = replace_save_with_read.then(|| {
                core.settings_intent(SettingsIntent::Refresh);
                let epoch = core.take_settings_refresh_for_test().unwrap();
                let read = core
                    .request_gateway_settings_for_epoch(&epoch)
                    .unwrap()
                    .unwrap();
                (epoch, read)
            });
            let before = core.gateway_settings();
            let mut stale_ack = disabled.clone();
            stale_ack.general.keepawake = !stale_ack.general.keepawake;
            assert!(!core.finish_gateway_settings(save, &mut Ok(stale_ack.clone())));
            if let Some((epoch, read)) = replacement {
                // An old ACK must not release the newer read or its coalescing owner.
                assert_eq!(core.gateway_settings(), before);
                assert_eq!(
                    core.settings_intent(SettingsIntent::Refresh).outcome(),
                    ClientTransitionOutcome::Noop
                );
                assert!(core.finish_gateway_settings(read, &mut Ok(initial.clone())));
                core.complete_settings_refresh_for_test(&epoch, true);
            } else {
                let released = core.gateway_settings();
                assert!(!released.saving && !released.loading);
                assert_eq!(released.settings, before.settings);
                assert_eq!(released.error, before.error);
            }

            select_workspace("workspace");
            assert_eq!(
                core.settings_intent(SettingsIntent::Refresh).outcome(),
                ClientTransitionOutcome::Changed
            );
            let epoch = core.take_settings_refresh_for_test().unwrap();
            let read = core
                .request_gateway_settings_for_epoch(&epoch)
                .unwrap()
                .expect("completed save cannot defer a new read");
            let before = core.gateway_settings();
            assert!(!core.finish_gateway_settings(save, &mut Ok(stale_ack.clone())));
            assert_eq!(core.gateway_settings(), before);
            assert_eq!(
                core.settings_intent(SettingsIntent::Refresh).outcome(),
                ClientTransitionOutcome::Noop
            );
            assert!(core.finish_gateway_settings(read, &mut Ok(disabled)));
            core.complete_settings_refresh_for_test(&epoch, true);
            assert_eq!(
                core.settings_intent(SettingsIntent::Refresh).outcome(),
                ClientTransitionOutcome::Changed
            );

            // A late ACK must also preserve a newer save and its deferred refresh.
            let epoch = core.take_settings_refresh_for_test().unwrap();
            let newer_save = core
                .prepare_gateway_settings_update(Some(initial.clone()))
                .unwrap();
            assert!(
                core.request_gateway_settings_for_epoch(&epoch)
                    .unwrap()
                    .is_none()
            );
            let before = core.gateway_settings();
            assert!(!core.finish_gateway_settings(save, &mut Ok(stale_ack)));
            assert_eq!(core.gateway_settings(), before);
            assert_eq!(
                core.settings_intent(SettingsIntent::Refresh).outcome(),
                ClientTransitionOutcome::Noop
            );
            assert!(core.finish_gateway_settings(newer_save, &mut Ok(initial.clone())));
            let followup = core
                .take_settings_refresh_for_test()
                .expect("new save keeps its refresh");
            assert!(core.take_settings_refresh_for_test().is_none());
            let read = core
                .request_gateway_settings_for_epoch(&followup)
                .unwrap()
                .unwrap();
            assert!(core.finish_gateway_settings(read, &mut Ok(initial.clone())));
            core.complete_settings_refresh_for_test(&followup, true);
            assert_eq!(
                core.settings_intent(SettingsIntent::Refresh).outcome(),
                ClientTransitionOutcome::Changed
            );
        }
    }

    #[test]
    fn cancellation_and_auth_fence_reject_late_mutation_completion_and_explicit_retry_gets_a_new_generation()
     {
        let core = fixture();
        let first = core.prepare_provider_command(configure()).unwrap();
        core.cancel_provider_operations("other");
        assert!(core.provider_operation_current(&first.identity));
        core.cancel_provider_operations("workspace");
        let cancelled = core.provider_operation_snapshot("workspace").unwrap();
        assert!(
            core.finish_provider_operation(
                &first.identity,
                Ok(ProviderOperationCompletion::Completed)
            )
            .is_err()
        );
        assert!(Arc::ptr_eq(
            &cancelled,
            &core.provider_operation_snapshot("workspace").unwrap()
        ));
        let failed = core.prepare_provider_command(configure()).unwrap();
        assert!(
            core.finish_provider_operation(&failed.identity, Err(anyhow::anyhow!("synthetic")))
                .is_err()
        );
        core.cancel_provider_operations("other");
        assert_eq!(
            core.provider_operation_snapshot("workspace")
                .unwrap()
                .request(),
            ProviderLoadState::Failed
        );
        core.cancel_provider_operations("workspace");
        assert_eq!(
            core.provider_operation_snapshot("workspace")
                .unwrap()
                .request(),
            ProviderLoadState::Cancelled
        );
        let retry = core.prepare_provider_command(configure()).unwrap();
        assert!(retry.identity.generation > first.identity.generation);
        let pending = core.provider_operation_snapshot("workspace").unwrap();
        assert!(
            core.finish_provider_operation(&failed.identity, Err(anyhow::anyhow!("late")))
                .is_err()
        );
        assert!(Arc::ptr_eq(
            &pending,
            &core.provider_operation_snapshot("workspace").unwrap()
        ));
        core.clear_authorization_projections();
        assert!(
            core.finish_provider_operation(
                &retry.identity,
                Ok(ProviderOperationCompletion::Completed)
            )
            .is_err()
        );
        assert!(core.provider_operation_snapshot("workspace").is_none());
        assert!(core.prepare_provider_command(configure()).is_err());
    }
}
