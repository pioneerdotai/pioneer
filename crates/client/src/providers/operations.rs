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
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-provider-actions".into())
            .spawn(move || {
                while let Ok(work) = receiver.recv() {
                    let result = work.operation.execute_direct(weak.clone());
                    let _ = work.reply.try_send(result);
                }
            })
            .expect("provider action worker");
        let mut owner = self
            .provider_controller
            .lock()
            .expect("provider controller poisoned");
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
        let mut owner = self
            .provider_controller
            .lock()
            .expect("provider controller poisoned");
        anyhow::ensure!(
            !self.is_stopped()
                && identity.epoch == epoch
                && owner.active.as_ref() == Some(identity),
            "provider_action_cancelled"
        );
        owner.active = None;
        owner.prepared = None;
        self.publish_provider_operation(
            identity,
            if result.is_ok() {
                ProviderLoadState::Ready
            } else {
                ProviderLoadState::Failed
            },
        );
        drop(owner);
        if result.is_ok() {
            let provider = if matches!(
                identity.kind,
                ProviderActionKind::Configure | ProviderActionKind::Disconnect
            ) {
                identity.target.clone()
            } else {
                super::list::cli_runtime_provider_key(&identity.target)
            };
            self.refresh_provider_models(&identity.workspace_id, &provider);
            self.provider_collection_intent(ProviderCollectionIntent::Refresh {
                key: ProviderCollectionKey::catalog(&identity.workspace_id),
            });
            self.provider_runtime_intent(ProviderRuntimeIntent::Refresh {
                workspace_id: identity.workspace_id.clone(),
            });
        }
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
        anyhow::ensure!(
            core.provider_operation_current(&identity),
            "provider_action_cancelled"
        );
        let (reply, receiver) = std::sync::mpsc::sync_channel(1);
        let queued = core
            .provider_controller
            .lock()
            .expect("provider controller poisoned")
            .sender
            .as_ref()
            .is_some_and(|sender| {
                sender
                    .try_send(ProviderWork {
                        operation: self,
                        reply,
                    })
                    .is_ok()
            });
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
        anyhow::ensure!(
            core.provider_operation_current(&self.identity),
            "provider_action_cancelled"
        );
        let sender = core.compatibility_runtime().ws_command_sender();
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
        })
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
        let retry = core.prepare_provider_command(configure()).unwrap();
        assert!(retry.identity.generation > first.identity.generation);
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
