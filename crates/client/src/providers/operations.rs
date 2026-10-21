//! Provider mutations share one generation and permission gate per process.
use super::{cli_runtime_settings::*, runtime::ProviderRuntimeIntent, store::*};
use crate::core::*;
use anyhow::Context as _;
use pioneer_protocol::*;
use std::{collections::BTreeMap, sync::Arc};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", content = "params", rename_all = "snake_case")]
pub enum ProviderCommand {
    AcpSession(AcpSessionParams),
    ReleaseAcpAuth(AcpSessionParams),
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
    AcpSession,
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
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(serde::Serialize)]
#[serde(tag = "kind", content = "result", rename_all = "snake_case")]
pub enum ProviderOperationCompletion {
    AcpSession(AcpSessionResponse),
    Completed,
    Login(CLIRuntimeLoginStartResponse),
}
enum ProviderWork {
    Execute {
        operation: ProviderOperation,
        reply: std::sync::mpsc::SyncSender<anyhow::Result<ProviderOperationCompletion>>,
    },
    LifecycleWake,
}
struct AuthInvocation {
    close: AcpSessionParams,
    epoch: (u64, u64, Option<u64>),
    // None is a release-before-start fence; Some binds the prepared action.
    operation_generation: Option<u64>,
    retired: bool,
    dispatched: bool,
    released: bool,
    next_cleanup: std::time::Instant,
}
#[derive(Default)]
pub(crate) struct ProviderController {
    auth_invocations: BTreeMap<String, AuthInvocation>,
    acp_sessions: BTreeMap<(String, String, String), AcpSessionSnapshot>,
    acp_epoch: Option<(u64, u64, Option<u64>)>,
    pub(super) credentials: super::credentials::CredentialRequests,
    generation: u64,
    pub(super) effect_generation: u64,
    active: Option<Identity>,
    prepared: Option<ProviderOperation>,
    sender: Option<std::sync::mpsc::SyncSender<ProviderWork>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl ProviderController {
    fn retire_auth(&mut self, id: &str) {
        if let Some(invocation) = self.auth_invocations.get_mut(id) {
            invocation.retired = true;
            invocation.dispatched = false;
            invocation.released = true;
            invocation.next_cleanup = std::time::Instant::now();
        }
        // Only acknowledged/no-dispatch records may be evicted. Early fences
        // and live remote obligations are never evicted to admit another start.
        while self.auth_invocations.values().filter(|i| i.retired).count() > 64 {
            let oldest = self
                .auth_invocations
                .iter()
                .filter(|(_, i)| i.retired)
                .min_by_key(|(_, i)| i.next_cleanup)
                .map(|(id, _)| id.clone())
                .expect("retired entry");
            self.auth_invocations.remove(&oldest);
        }
    }
    fn retire_undispatched_auth(&mut self, identity: &Identity) {
        let ids = self
            .auth_invocations
            .iter()
            .filter(|(_, i)| {
                !i.retired
                    && !i.dispatched
                    && i.operation_generation == Some(identity.generation)
                    && i.epoch == identity.epoch
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in ids {
            self.retire_auth(&id);
        }
    }
    fn release_auths(&mut self, workspace: Option<&str>) {
        let mut undispatched = Vec::new();
        for (id, invocation) in &mut self.auth_invocations {
            if workspace.is_some_and(|w| invocation.close.workspace_id != w) {
                continue;
            }
            invocation.released = true;
            if !invocation.dispatched
                && invocation.operation_generation.is_some()
                && !invocation.retired
            {
                undispatched.push(id.clone());
            }
        }
        for id in undispatched {
            self.retire_auth(&id);
        }
    }
    pub(crate) fn stop(&mut self) {
        self.active = None;
        self.prepared = None;
        self.credentials.stop();
        self.acp_sessions.clear();
        self.release_auths(None);
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
            Self::AcpSession(p) | Self::ReleaseAcpAuth(p) => &p.workspace_id,
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
            Self::AcpSession(p) | Self::ReleaseAcpAuth(p) => &p.runtime_id,
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
            Self::AcpSession(_) | Self::ReleaseAcpAuth(_) => ProviderActionKind::AcpSession,
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
    pub(crate) fn observe_acp_session_notification(
        &self,
        notification: &GatewayNotification,
    ) -> bool {
        let GatewayNotification::AcpSessionChanged(snapshot) = notification else {
            return false;
        };
        if !self.capability_management_allowed(&snapshot.workspace_id) {
            return true;
        }
        let epoch = self.provider_runtime_epoch();
        let mut owner = self
            .provider_controller
            .lock()
            .expect("provider controller poisoned");
        if owner.acp_epoch != Some(epoch) {
            owner.acp_sessions.clear();
            owner.acp_epoch = Some(epoch);
        }
        let key = (
            snapshot.workspace_id.clone(),
            snapshot.thread_id.clone(),
            snapshot.runtime_id.clone(),
        );
        if owner.acp_sessions.get(&key).is_some_and(|current| {
            current.generation > snapshot.generation
                || (current.generation == snapshot.generation
                    && current.presentation_revision >= snapshot.presentation_revision)
        }) {
            return true;
        }
        if owner.acp_sessions.len() >= 64 && !owner.acp_sessions.contains_key(&key) {
            if let Some(old) = owner.acp_sessions.keys().next().cloned() {
                owner.acp_sessions.remove(&old);
            }
        }
        owner.acp_sessions.insert(key, snapshot.clone());
        let previous = self.provider_operation_snapshot(&snapshot.workspace_id);
        let identity = Identity {
            generation: previous.as_ref().map_or(owner.generation, |p| p.generation),
            epoch: self.provider_runtime_epoch(),
            workspace_id: snapshot.workspace_id.clone(),
            target: previous
                .as_ref()
                .map_or_else(|| snapshot.runtime_id.clone(), |p| p.target.clone()),
            kind: previous
                .as_ref()
                .map_or(ProviderActionKind::AcpSession, |p| p.action),
        };
        self.publish_provider_operation(
            &identity,
            previous
                .as_ref()
                .map_or(ProviderLoadState::Ready, |p| p.request.clone()),
        );
        drop(owner);
        self.refresh_composer_acp_picker(snapshot);
        true
    }
    pub fn acp_session_snapshot(
        &self,
        workspace: &str,
        thread: &str,
        runtime: &str,
    ) -> Option<AcpSessionSnapshot> {
        // Read the existing owner before consulting authorization. Authorization
        // fencing clears this cache before publishing cancelled composer state,
        // while the identity owner is still locked.
        let (epoch, snapshot) = {
            let owner = self
                .provider_controller
                .lock()
                .expect("provider controller poisoned");
            let epoch = owner.acp_epoch?;
            let snapshot = owner
                .acp_sessions
                .get(&(workspace.into(), thread.into(), runtime.into()))?
                .clone();
            (epoch, snapshot)
        };
        if !self.capability_management_allowed(workspace) || epoch != self.provider_runtime_epoch()
        {
            return None;
        }
        Some(snapshot)
    }

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
        if let ProviderCommand::AcpSession(params) = &command {
            if let AcpSessionOperation::TerminalAuthStart {
                terminal_id,
                expected_generation,
                ..
            } = &params.operation
            {
                let mut close = params.clone();
                close.operation = AcpSessionOperation::TerminalAuthClose {
                    expected_generation: *expected_generation,
                    terminal_id: terminal_id.clone(),
                };
                if let Some(invocation) = owner.auth_invocations.get(terminal_id) {
                    anyhow::ensure!(invocation.close == close, "stale_auth_invocation");
                    if invocation.released && invocation.operation_generation.is_none() {
                        owner.retire_auth(terminal_id);
                    }
                    anyhow::bail!("auth_invocation_already_owned_or_released");
                }
                anyhow::ensure!(!owner.auth_invocations.values().any(|invocation|
                    !invocation.retired && invocation.operation_generation.is_some()
                    && invocation.epoch.2 == epoch.2
                    && invocation.close.workspace_id == params.workspace_id
                    && invocation.close.thread_id == params.thread_id
                    && invocation.close.runtime_id == params.runtime_id
                    && matches!(&invocation.close.operation, AcpSessionOperation::TerminalAuthClose { expected_generation: g, .. } if g == expected_generation)),
                    "auth_terminal_pending_cleanup");
                anyhow::ensure!(
                    terminal_id.len() == 32
                        && terminal_id
                            .bytes()
                            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
                        && owner
                            .auth_invocations
                            .values()
                            .filter(|i| !i.retired && i.operation_generation.is_some())
                            .count()
                            < 64,
                    "invalid_auth_invocation_or_active_budget_exceeded"
                );
                owner.auth_invocations.insert(
                    terminal_id.clone(),
                    AuthInvocation {
                        close,
                        epoch,
                        operation_generation: Some(identity.generation),
                        retired: false,
                        dispatched: false,
                        released: false,
                        next_cleanup: std::time::Instant::now(),
                    },
                );
            }
        }
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
        if let ProviderCommand::ReleaseAcpAuth(params) = command {
            self.release_acp_auth(params)?;
            return Ok(ProviderOperationCompletion::Completed);
        }
        self.prepare_provider_command(command)?
            .execute(Arc::downgrade(self))
    }
    pub fn execute_provider_operation(
        self: &Arc<Self>,
        operation: ProviderOperation,
    ) -> anyhow::Result<ProviderOperationCompletion> {
        operation.execute(Arc::downgrade(self))
    }
    /// Transfer the teardown obligation to the existing operation owner. This
    /// lifecycle intent is accepted while a normal provider action is busy.
    pub fn release_acp_auth(&self, params: AcpSessionParams) -> anyhow::Result<()> {
        let AcpSessionOperation::TerminalAuthClose { terminal_id, .. } = &params.operation else {
            anyhow::bail!("auth_release_requires_exact_terminal");
        };
        anyhow::ensure!(
            terminal_id.len() == 32
                && terminal_id
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-'),
            "invalid_auth_invocation"
        );
        let mut owner = self
            .provider_controller
            .lock()
            .expect("provider controller poisoned");
        if let Some(invocation) = owner.auth_invocations.get_mut(terminal_id) {
            anyhow::ensure!(invocation.close == params, "stale_auth_release");
            if invocation.retired {
                return Ok(());
            }
            invocation.released = true;
            if !invocation.dispatched && invocation.operation_generation.is_some() {
                let generation = invocation.operation_generation;
                let epoch = invocation.epoch;
                owner.retire_auth(terminal_id);
                // The shell may drop its prepared task before enqueue. Claim
                // and release share this lock, so this exact action can never
                // dispatch; it must not retain the normal provider busy gate.
                if let Some(identity) = owner
                    .active
                    .as_ref()
                    .filter(|identity| {
                        Some(identity.generation) == generation && identity.epoch == epoch
                    })
                    .cloned()
                {
                    owner.active = None;
                    owner.prepared = None;
                    self.publish_provider_operation(&identity, ProviderLoadState::Cancelled);
                }
            }
        } else {
            // Mobile release may cross the FFI worker before start preparation.
            // Retain a bounded tombstone so that late start cannot create it.
            anyhow::ensure!(
                owner
                    .auth_invocations
                    .values()
                    .filter(|i| !i.retired && i.operation_generation.is_none())
                    .count()
                    < 64,
                "auth_release_fence_budget_exceeded"
            );
            owner.auth_invocations.insert(
                terminal_id.clone(),
                AuthInvocation {
                    close: params,
                    epoch: self.provider_runtime_epoch(),
                    operation_generation: None,
                    retired: false,
                    dispatched: false,
                    released: true,
                    next_cleanup: std::time::Instant::now(),
                },
            );
        }
        let sender = owner.sender.clone();
        drop(owner);
        if let Some(sender) = sender {
            let _ = sender.try_send(ProviderWork::LifecycleWake);
        }
        Ok(())
    }
    fn finish_released_acp_auth(&self) {
        let epoch = self.provider_runtime_epoch();
        let releases = {
            let owner = self
                .provider_controller
                .lock()
                .expect("provider controller poisoned");
            owner
                .auth_invocations
                .iter()
                .filter(|(_, invocation)| {
                    invocation.released
                        && invocation.dispatched
                        && invocation.epoch.2 == epoch.2
                        && invocation.next_cleanup <= std::time::Instant::now()
                })
                .map(|(id, invocation)| (id.clone(), invocation.close.clone()))
                .collect::<Vec<_>>()
        };
        for (id, close) in releases {
            // This is the same serialized worker's completion path, independent
            // of a view or the next prepared operation's busy gate.
            let Some(connection) = epoch.2 else {
                continue;
            };
            if crate::transport::ws::command_sender::acp_session_control(
                &self
                    .transport_runtime()
                    .ws_command_sender()
                    .requests_for_connection(connection),
                close,
            )
            .is_ok()
            {
                self.provider_controller
                    .lock()
                    .expect("provider controller poisoned")
                    .retire_auth(&id);
            } else if let Some(invocation) = self
                .provider_controller
                .lock()
                .expect("provider controller poisoned")
                .auth_invocations
                .get_mut(&id)
            {
                invocation.next_cleanup =
                    std::time::Instant::now() + std::time::Duration::from_secs(1);
            }
        }
    }
    pub(crate) fn start_provider_operation_controller(self: &Arc<Self>) {
        let (sender, receiver) = std::sync::mpsc::sync_channel::<ProviderWork>(1);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-provider-actions".into())
            .spawn(move || {
                loop {
                    let Some(core) = weak.upgrade() else {
                        break;
                    };
                    core.finish_released_acp_auth();
                    let epoch = core.provider_runtime_epoch();
                    let pending_cleanup = core
                        .provider_controller
                        .lock()
                        .expect("provider controller poisoned")
                        .auth_invocations
                        .values()
                        .any(|invocation| {
                            invocation.released
                                && invocation.dispatched
                                && invocation.epoch.2 == epoch.2
                        });
                    drop(core);
                    let work = if pending_cleanup {
                        match receiver.recv_timeout(std::time::Duration::from_secs(1)) {
                            Ok(work) => work,
                            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                            Err(_) => break,
                        }
                    } else {
                        match receiver.recv() {
                            Ok(work) => work,
                            Err(_) => break,
                        }
                    };
                    match work {
                        ProviderWork::Execute { operation, reply } => {
                            let result = operation.execute_direct(weak.clone());
                            if let Some(core) = weak.upgrade() {
                                core.finish_released_acp_auth();
                            }
                            let _ = reply.try_send(result);
                        }
                        ProviderWork::LifecycleWake => {}
                    }
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
        let applicable = !self.is_stopped()
            && identity.epoch == epoch
            && self.capability_management_allowed(&identity.workspace_id);
        let mut owner = self
            .provider_controller
            .lock()
            .expect("provider controller poisoned");
        owner.retire_undispatched_auth(identity);
        if !applicable {
            for invocation in owner.auth_invocations.values_mut().filter(|i| {
                !i.retired
                    && i.operation_generation == Some(identity.generation)
                    && i.epoch == identity.epoch
            }) {
                invocation.released = true;
            }
        }
        anyhow::ensure!(
            owner.active.as_ref() == Some(identity),
            "provider_action_cancelled"
        );
        owner.active = None;
        owner.prepared = None;
        if applicable {
            if let Ok(ProviderOperationCompletion::AcpSession(response)) = &result {
                let snapshot = &response.snapshot;
                if owner.acp_epoch != Some(epoch) {
                    owner.acp_sessions.clear();
                    owner.acp_epoch = Some(epoch);
                }
                let key = (
                    snapshot.workspace_id.clone(),
                    snapshot.thread_id.clone(),
                    snapshot.runtime_id.clone(),
                );
                if owner.acp_sessions.len() >= 64 && !owner.acp_sessions.contains_key(&key) {
                    if let Some(old) = owner.acp_sessions.keys().next().cloned() {
                        owner.acp_sessions.remove(&old);
                    }
                }
                if owner.acp_sessions.get(&key).is_none_or(|current| {
                    current.generation < snapshot.generation
                        || (current.generation == snapshot.generation
                            && current.presentation_revision < snapshot.presentation_revision)
                }) {
                    owner.acp_sessions.insert(key, snapshot.clone());
                }
            }
        }
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
        if let Ok(ProviderOperationCompletion::AcpSession(response)) = &result {
            self.refresh_composer_acp_picker(&response.snapshot);
        }
        if identity.kind != ProviderActionKind::AcpSession
            && self.capability_management_allowed(&identity.workspace_id)
        {
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
        owner.acp_sessions.clear();
        owner.acp_epoch = None;
        owner.release_auths(None);
        if let Some(sender) = &owner.sender {
            let _ = sender.try_send(ProviderWork::LifecycleWake);
        }
    }
    pub fn cancel_provider_operations(&self, workspace: &str) {
        let epoch = self.provider_runtime_epoch();
        let mut owner = self
            .provider_controller
            .lock()
            .expect("provider controller poisoned");
        owner.release_auths(Some(workspace));
        if let Some(sender) = &owner.sender {
            let _ = sender.try_send(ProviderWork::LifecycleWake);
        }
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
        let queued = core
            .provider_controller
            .lock()
            .expect("provider controller poisoned")
            .sender
            .as_ref()
            .is_some_and(|sender| {
                sender
                    .try_send(ProviderWork::Execute {
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
                Err(_) => {
                    if let Some(core) = client.upgrade() {
                        return core.finish_provider_operation(
                            &identity,
                            Err(anyhow::anyhow!("provider_action_cancelled")),
                        );
                    }
                    anyhow::bail!("provider_action_cancelled");
                }
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
        let epoch = core.provider_runtime_epoch();
        let claimed = {
            let mut owner = core
                .provider_controller
                .lock()
                .expect("provider controller poisoned");
            let current = !core.is_stopped()
                && epoch == self.identity.epoch
                && owner.active.as_ref() == Some(&self.identity);
            if !current {
                false
            } else if let ProviderCommand::AcpSession(AcpSessionParams {
                operation: AcpSessionOperation::TerminalAuthStart { terminal_id, .. },
                ..
            }) = &self.command
            {
                if let Some(invocation) = owner.auth_invocations.get_mut(terminal_id).filter(|i| {
                    epoch.2.is_some()
                        && !i.retired
                        && !i.released
                        && !i.dispatched
                        && i.operation_generation == Some(self.identity.generation)
                        && i.epoch == epoch
                }) {
                    invocation.dispatched = true;
                    true
                } else {
                    false
                }
            } else {
                true
            }
        };
        if !claimed {
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
        let auth_close = match &self.command {
            ProviderCommand::AcpSession(params)
                if matches!(
                    &params.operation,
                    AcpSessionOperation::TerminalAuthClose { .. }
                ) =>
            {
                Some(params.clone())
            }
            _ => None,
        };
        let auth_start_id = match &self.command {
            ProviderCommand::AcpSession(AcpSessionParams {
                operation: AcpSessionOperation::TerminalAuthStart { terminal_id, .. },
                ..
            }) => Some(terminal_id.clone()),
            _ => None,
        };
        drop(core);
        let result = match self.command {
            ProviderCommand::ReleaseAcpAuth(params) => {
                client
                    .upgrade()
                    .ok_or_else(|| anyhow::anyhow!("provider_action_cancelled"))?
                    .release_acp_auth(params)?;
                Ok(ProviderOperationCompletion::Completed)
            }
            ProviderCommand::AcpSession(params) => {
                let connection = self.identity.epoch.2.context("provider_action_cancelled")?;
                crate::transport::ws::command_sender::acp_session_control(
                    &sender.requests_for_connection(connection),
                    params,
                )
                .map(ProviderOperationCompletion::AcpSession)
            }
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
        let core = client
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("provider_action_cancelled"))?;
        {
            let mut owner = core
                .provider_controller
                .lock()
                .expect("provider controller poisoned");
            if result.is_ok() {
                if let Some(params) = auth_close {
                    if let AcpSessionOperation::TerminalAuthClose { terminal_id, .. } =
                        &params.operation
                    {
                        if owner
                            .auth_invocations
                            .get(terminal_id)
                            .is_some_and(|i| i.close == params)
                        {
                            owner.retire_auth(terminal_id);
                        }
                    }
                }
            } else if let Some(id) = auth_start_id {
                if let Some(invocation) = owner.auth_invocations.get_mut(&id) {
                    invocation.released = true;
                }
            }
        }
        core.finish_provider_operation(&self.identity, result)
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
    fn acp_snapshot(generation: u64, revision: u64) -> AcpSessionSnapshot {
        serde_json::from_value(serde_json::json!({
            "workspace_id":"workspace", "thread_id":"parent", "runtime_id":"agent", "generation":generation,
            "connected":true, "presentation_revision":revision, "capabilities":{}, "auth_methods":[],
            "session_id":format!("session-{generation}"), "version":{"connection_id":format!("connection-{generation}"), "revision":revision},
            "has_config_options":true, "config_options":[], "modes":null, "commands":[], "session_info":null,
            "usage":null, "notice":null, "completed_elicitations":[], "additional_directories":[],
            "initial_descriptor_fingerprint":pioneer_protocol::acp_descriptor_fingerprint(&[],None).unwrap(),
            "configuration_replay":[], "continuation_requested":false
        })).unwrap()
    }

    #[test]
    fn execution_snapshot_replacement_second_submission_and_idle_disconnect_are_fenced() {
        let core = fixture();
        let prepared = acp_snapshot(1, 1);
        core.observe_acp_session_notification(&GatewayNotification::AcpSessionChanged(
            prepared.clone(),
        ));
        let executed = acp_snapshot(2, 2);
        core.observe_acp_session_notification(&GatewayNotification::AcpSessionChanged(
            executed.clone(),
        ));
        core.observe_acp_session_notification(&GatewayNotification::AcpSessionChanged(
            acp_snapshot(1, 999),
        ));
        let current = core
            .acp_session_snapshot("workspace", "parent", "agent")
            .unwrap();
        let next_submission = crate::cli_runtime::acp::capture_configuration(&current).unwrap();
        assert_eq!(next_submission.session_id, "session-2");
        assert_eq!(next_submission.expected, executed.version.unwrap());
        let mut disconnected = acp_snapshot(2, 3);
        disconnected.connected = false;
        disconnected.session_id = None;
        disconnected.version = None;
        core.observe_acp_session_notification(&GatewayNotification::AcpSessionChanged(
            disconnected,
        ));
        core.observe_acp_session_notification(&GatewayNotification::AcpSessionChanged(
            acp_snapshot(2, 2),
        ));
        assert!(
            !core
                .acp_session_snapshot("workspace", "parent", "agent")
                .unwrap()
                .connected
        );
    }

    fn terminal_start(id: String) -> AcpSessionParams {
        AcpSessionParams {
            workspace_id: "workspace".into(),
            thread_id: "parent".into(),
            runtime_id: "agent".into(),
            operation: AcpSessionOperation::TerminalAuthStart {
                expected_generation: 11,
                terminal_id: id,
                method_id: "first-method".into(),
                columns: 90,
                rows: 24,
            },
        }
    }
    #[test]
    fn live_auth_owner_survives_repeated_methods_until_exact_ack_and_stale_release() {
        let core = fixture();
        let a = terminal_start("A".repeat(32));
        let operation = core
            .prepare_provider_command(ProviderCommand::AcpSession(a.clone()))
            .unwrap();
        core.provider_controller
            .lock()
            .unwrap()
            .auth_invocations
            .get_mut(&"A".repeat(32))
            .unwrap()
            .dispatched = true;
        core.finish_provider_operation(
            &operation.identity,
            Ok(ProviderOperationCompletion::Completed),
        )
        .unwrap();
        let mut b = terminal_start("B".repeat(32));
        for method in ["first-method", "another-method"] {
            if let AcpSessionOperation::TerminalAuthStart { method_id, .. } = &mut b.operation {
                *method_id = method.into();
            }
            assert!(
                core.prepare_provider_command(ProviderCommand::AcpSession(b.clone()))
                    .is_err()
            );
        }
        let close_a = core.provider_controller.lock().unwrap().auth_invocations[&"A".repeat(32)]
            .close
            .clone();
        core.release_acp_auth(close_a.clone()).unwrap();
        assert!(
            core.prepare_provider_command(ProviderCommand::AcpSession(b.clone()))
                .is_err()
        );
        // Existing worker's exact acknowledgement; no UI result owns this state.
        core.provider_controller
            .lock()
            .unwrap()
            .retire_auth(&"A".repeat(32));
        let next = core
            .prepare_provider_command(ProviderCommand::AcpSession(b))
            .unwrap();
        core.release_acp_auth(close_a).unwrap();
        let owner = core.provider_controller.lock().unwrap();
        assert!(!owner.auth_invocations[&"B".repeat(32)].released);
        assert!(!owner.auth_invocations[&"B".repeat(32)].retired);
        assert_eq!(owner.active.as_ref(), Some(&next.identity));
    }

    #[test]
    fn completed_and_never_dispatched_auth_cycles_do_not_consume_the_active_budget() {
        let core = Arc::new(fixture());
        for cycle in 0..130 {
            let id = format!("{cycle:032}");
            let operation = core
                .prepare_provider_command(ProviderCommand::AcpSession(terminal_start(id.clone())))
                .unwrap();
            if cycle % 3 == 0 {
                core.cancel_provider_operations("workspace");
                assert!(operation.execute_direct(Arc::downgrade(&core)).is_err());
            } else if cycle % 3 == 1 {
                // There is no worker sender in this existing pure Core fixture.
                assert!(operation.execute(Arc::downgrade(&core)).is_err());
            } else {
                core.provider_controller
                    .lock()
                    .unwrap()
                    .auth_invocations
                    .get_mut(&id)
                    .unwrap()
                    .dispatched = true;
                core.finish_provider_operation(
                    &operation.identity,
                    Ok(ProviderOperationCompletion::Completed),
                )
                .unwrap();
                let close = core.provider_controller.lock().unwrap().auth_invocations[&id]
                    .close
                    .clone();
                core.release_acp_auth(close.clone()).unwrap();
                core.provider_controller.lock().unwrap().retire_auth(&id); // Exact worker acknowledgement.
                core.release_acp_auth(close).unwrap(); // Unmount then late success release.
            }
            let owner = core.provider_controller.lock().unwrap();
            assert!(owner.auth_invocations[&id].retired);
            assert!(owner.auth_invocations.len() <= 64);
            assert!(
                !owner
                    .auth_invocations
                    .values()
                    .any(|i| !i.retired && i.operation_generation.is_some())
            );
        }
        assert!(
            core.prepare_provider_command(ProviderCommand::AcpSession(terminal_start(
                "Z".repeat(32)
            )))
            .is_ok()
        );
    }

    #[test]
    fn auth_release_is_retained_while_busy_and_fences_a_late_start() {
        let core = Arc::new(fixture());
        let active = core.prepare_provider_command(configure()).unwrap();
        let id = "A".repeat(32);
        let close = AcpSessionParams {
            workspace_id: "workspace".into(),
            thread_id: "parent".into(),
            runtime_id: "agent".into(),
            operation: AcpSessionOperation::TerminalAuthClose {
                expected_generation: 1,
                terminal_id: id.clone(),
            },
        };
        core.release_acp_auth(close.clone()).unwrap();
        assert!(core.provider_operation_current(&active.identity));
        core.finish_provider_operation(
            &active.identity,
            Ok(ProviderOperationCompletion::Completed),
        )
        .unwrap();
        let start = AcpSessionParams {
            operation: AcpSessionOperation::TerminalAuthStart {
                expected_generation: 1,
                terminal_id: id.clone(),
                method_id: "opaque".into(),
                columns: 90,
                rows: 24,
            },
            ..close.clone()
        };
        assert!(
            core.prepare_provider_command(ProviderCommand::AcpSession(start.clone()))
                .is_err()
        );
        let id = "B".repeat(32);
        let start = AcpSessionParams {
            operation: AcpSessionOperation::TerminalAuthStart {
                expected_generation: 1,
                terminal_id: id.clone(),
                method_id: "opaque".into(),
                columns: 90,
                rows: 24,
            },
            ..close.clone()
        };
        let pending = core
            .prepare_provider_command(ProviderCommand::AcpSession(start))
            .unwrap();
        let close = AcpSessionParams {
            operation: AcpSessionOperation::TerminalAuthClose {
                expected_generation: 1,
                terminal_id: id.clone(),
            },
            ..close
        };
        core.release_acp_auth(close).unwrap();
        let owner = core.provider_controller.lock().unwrap();
        assert!(owner.auth_invocations[&id].released);
        assert!(owner.auth_invocations[&id].retired);
        assert!(owner.active.is_none());
        drop(owner);
        let next = core.prepare_provider_command(configure()).unwrap();
        assert!(pending.execute_direct(Arc::downgrade(&core)).is_err());
        assert!(core.provider_operation_current(&next.identity));
    }

    #[test]
    fn auth_cleanup_completion_survives_busy_owner_and_transient_failure() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct CloseTransport {
            calls: AtomicUsize,
            id: String,
        }
        impl crate::rpc::JsonRpcRequestTransport for CloseTransport {
            fn send_json_rpc_request(
                &self,
                _: String,
                payload: String,
                reply: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
                assert_eq!(payload["params"]["operation"]["terminal_id"], self.id);
                assert_eq!(payload["params"]["operation"]["expected_generation"], 11);
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    reply
                        .send(Err(crate::rpc::JsonRpcResponseError::transport(
                            "synthetic close failure",
                        )))
                        .unwrap();
                } else {
                    reply
                        .send(Ok(serde_json::to_value(AcpSessionResponse {
                            snapshot: acp_snapshot(11, 9),
                            result: Some(serde_json::json!({"already_retired":true})),
                        })
                        .unwrap()))
                        .unwrap();
                }
                Ok(())
            }
        }
        let core = crate::catalog_test_support::client();
        let id = "C".repeat(32);
        let params = AcpSessionParams {
            workspace_id: "workspace".into(),
            thread_id: "parent".into(),
            runtime_id: "agent".into(),
            operation: AcpSessionOperation::TerminalAuthStart {
                expected_generation: 11,
                terminal_id: id.clone(),
                method_id: "opaque".into(),
                columns: 90,
                rows: 24,
            },
        };
        let started = core
            .prepare_provider_command(ProviderCommand::AcpSession(params.clone()))
            .unwrap();
        core.provider_controller
            .lock()
            .unwrap()
            .auth_invocations
            .get_mut(&id)
            .unwrap()
            .dispatched = true;
        // The independent worker has created the side effect, but its initiating
        // view has gone away before consuming the late success.
        let close = AcpSessionParams {
            operation: AcpSessionOperation::TerminalAuthClose {
                terminal_id: id.clone(),
                expected_generation: 11,
            },
            ..params
        };
        core.release_acp_auth(close).unwrap();
        core.finish_provider_operation(
            &started.identity,
            Ok(ProviderOperationCompletion::Completed),
        )
        .unwrap();
        let busy = core.prepare_provider_command(configure()).unwrap();
        let transport = Arc::new(CloseTransport {
            calls: AtomicUsize::new(0),
            id: id.clone(),
        });
        *core
            .transport_runtime()
            .ws_command_sender()
            .test_requests
            .lock()
            .unwrap() = Some(transport.clone());
        core.finish_released_acp_auth();
        assert!(core.provider_controller.lock().unwrap().auth_invocations[&id].released);
        assert!(core.provider_operation_current(&busy.identity));
        core.provider_controller
            .lock()
            .unwrap()
            .auth_invocations
            .get_mut(&id)
            .unwrap()
            .next_cleanup = std::time::Instant::now();
        core.finish_released_acp_auth();
        assert!(core.provider_controller.lock().unwrap().auth_invocations[&id].retired);
        assert_eq!(transport.calls.load(Ordering::SeqCst), 2);
        assert!(core.provider_operation_current(&busy.identity));
        let close = core.provider_controller.lock().unwrap().auth_invocations[&id]
            .close
            .clone();
        core.release_acp_auth(close.clone()).unwrap(); // Late mobile promise after worker ack.
        core.finish_released_acp_auth();
        assert_eq!(transport.calls.load(Ordering::SeqCst), 2);
        assert!(core.provider_controller.lock().unwrap().auth_invocations[&id].retired);
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
        let initial = GatewaySettingsSnapshot {
            general: Default::default(),
            memory: Default::default(),
            self_improvement: Default::default(),
            self_improvement_status: None,
            thread_episodic: Default::default(),
            cli_runtimes: Default::default(),
            remote_access: Default::default(),
            voice_input: Default::default(),
        };
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
