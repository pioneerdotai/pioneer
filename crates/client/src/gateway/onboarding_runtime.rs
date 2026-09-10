//! Process-owned gateway registry and serialized onboarding commands.
use super::{
    onboarding_effects::*,
    runtime::GatewaySetupAction,
    session_refresh::{
        GatewaySessionPlatformStorage, GatewaySessionRefreshRequest, GatewaySessionStorage,
    },
    setup,
    setup_controller::*,
    types::*,
};
use crate::core::*;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    sync::{Arc, mpsc},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct GatewaySetupWarning {
    pub id: u64,
    pub message: String,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct GatewayWorkspaceOutcome {
    pub request_id: String,
    pub endpoint_id: String,
    pub succeeded: bool,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct GatewayDestinationOutcome {
    pub generation: u64,
    pub endpoint_id: String,
    pub succeeded: bool,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct GatewayDestinationsPublication {
    pub endpoints: Vec<GatewayEndpoint>,
    pub installation_id: Option<String>,
    pub registry_revision: u64,
    pub selected_endpoint: Option<String>,
    pub loading: bool,
    pub pending_endpoint: Option<String>,
    pub action_generation: u64,
    pub outcome: Option<GatewayDestinationOutcome>,
    pub workspace_outcomes: Vec<GatewayWorkspaceOutcome>,
    pub warnings: Vec<GatewaySetupWarning>,
    pub error: Option<String>,
    pub local_install_required: bool,
    pub local_update_required: bool,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OnboardingIntent {
    Initialize,
    Invitation {
        intent: super::invitation_controller::InvitationIntent,
    },
    RetryInitialization,
    Setup {
        intent: GatewaySetupIntent,
    },
    SelectGateway {
        endpoint_id: String,
    },
    DeleteGateway {
        endpoint_id: String,
        expected_registry_revision: u64,
    },
    AcknowledgeWorkspaceOutcome {
        request_id: String,
    },
    SetWorkspaceForRequest {
        endpoint_id: String,
        workspace_id: Option<String>,
        request_id: String,
    },
    SetWorkspace {
        endpoint_id: String,
        workspace_id: Option<String>,
    },
}
enum Work {
    Initialize,
    Bootstrap,
    Invitation(super::invitation_controller::InvitationRequest),
    RetryInvitation,
    Setup(GatewaySetupRequest),
    Select {
        endpoint_id: String,
        generation: u64,
    },
    Delete {
        endpoint_id: String,
        generation: u64,
    },
    Workspace {
        endpoint_id: String,
        workspace_id: Option<String>,
        request_id: Option<String>,
        authorization: u64,
        gateway: u64,
    },
}
#[derive(Default)]
pub(crate) struct OnboardingRuntime {
    environment: Option<OnboardingEnvironment>,
    pub(super) invitation: super::invitation_controller::InvitationController,
    pub(super) invitation_recovery: Option<super::onboarding_invitation::InvitationRecovery>,
    registry_revision: u64,
    setup: GatewaySetupController,
    destinations: GatewayDestinationsPublication,
    selection_generation: u64,
    warning_generation: u64,
    authorization_generation: Option<u64>,
    handoff: Option<(String, std::thread::ThreadId)>,
    workspace_requests: std::collections::BTreeSet<String>,
    queue: VecDeque<Work>,
    deferred: VecDeque<OnboardingIntent>,
    wake: Option<mpsc::SyncSender<()>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl OnboardingRuntime {
    pub(crate) fn stop(&mut self) {
        self.queue.clear();
        self.deferred.clear();
        self.wake.take();
    }
    fn wake(&self) {
        if let Some(wake) = &self.wake {
            let _ = wake.try_send(());
        }
    }
}
impl Drop for OnboardingRuntime {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}
struct OnboardingHandoff<'a> {
    core: &'a ClientCore,
    endpoint: String,
}
impl Drop for OnboardingHandoff<'_> {
    fn drop(&mut self) {
        let mut owner = self
            .core
            .onboarding
            .lock()
            .expect("onboarding owner poisoned");
        if owner
            .handoff
            .as_ref()
            .is_some_and(|(endpoint, _)| endpoint == &self.endpoint)
        {
            owner.handoff = None;
        }
    }
}
impl ClientCore {
    fn begin_onboarding_handoff(&self, id: &str) -> OnboardingHandoff<'_> {
        self.onboarding
            .lock()
            .expect("onboarding owner poisoned")
            .handoff = Some((id.to_owned(), std::thread::current().id()));
        OnboardingHandoff {
            core: self,
            endpoint: id.to_owned(),
        }
    }
    pub(crate) fn fence_onboarding_authorization(
        &self,
        projection: &super::identity_authorization::IdentityAuthorizationPublication,
    ) {
        let mut owner = self.onboarding.lock().expect("onboarding owner poisoned");
        let previous = owner
            .authorization_generation
            .replace(projection.connection_generation);
        if previous.is_none() || previous == Some(projection.connection_generation) {
            return;
        }
        let own_handoff = owner.handoff.as_ref().is_some_and(|(endpoint, thread)| {
            projection.endpoint_id.as_ref() == Some(endpoint)
                || projection.endpoint_id.is_none() && *thread == std::thread::current().id()
        });
        if own_handoff {
            return;
        }
        owner.queue.retain(|work| {
            !matches!(
                work,
                Work::Select { .. } | Work::Delete { .. } | Work::Bootstrap
            )
        });
        if let Some(endpoint_id) = owner.destinations.pending_endpoint.take() {
            owner.destinations.outcome = Some(GatewayDestinationOutcome {
                generation: owner.destinations.action_generation,
                endpoint_id,
                succeeded: false,
            });
            owner.destinations.error = Some("gateway_operation_cancelled".into());
            owner.selection_generation += 1;
        }
        if let Some(environment) = owner.environment.clone() {
            let revision = owner.registry_revision;
            owner.setup.intent(
                GatewaySetupIntent::Cancel,
                &environment.registry,
                revision,
                projection.connection_generation,
            );
            // Durable invitation recovery remains privately owned; a transient preview/accept is cancelled.
            owner.invitation.invalidate_authorization();
        }
        self.publish_onboarding(&owner);
    }
    pub fn onboarding_intent(&self, intent: OnboardingIntent) -> ClientTransition {
        let authorization = self.authorization_connection_generation();
        let gateway = self.gateway_operation_epoch();
        let connected = self
            .gateway_session()
            .connections
            .iter()
            .filter_map(|(id, state)| state.connected.as_ref().map(|_| id.clone()))
            .collect::<std::collections::BTreeSet<_>>();
        let mut owner = self.onboarding.lock().expect("onboarding owner poisoned");
        if self.is_stopped() {
            return self.reject_intent();
        }
        owner.authorization_generation.get_or_insert(authorization);
        if owner.environment.is_none()
            && matches!(intent, OnboardingIntent::SetWorkspaceForRequest { .. })
        {
            return self.reject_intent();
        }
        if owner.environment.is_none()
            && !matches!(
                intent,
                OnboardingIntent::Initialize | OnboardingIntent::RetryInitialization
            )
        {
            if owner.deferred.len() < 128 {
                owner.deferred.push_back(intent);
            } else {
                owner.destinations.error = Some("onboarding_request_capacity".into());
            }
            if !owner.destinations.loading {
                owner.destinations.loading = true;
                owner.queue.push_back(Work::Initialize);
                owner.wake();
            }
            return self.publish_onboarding(&owner);
        }
        match intent {
            OnboardingIntent::Initialize | OnboardingIntent::RetryInitialization => {
                if owner.environment.is_none() && !owner.destinations.loading {
                    owner.destinations.loading = true;
                    owner.destinations.error = None;
                    owner.queue.push_back(Work::Initialize);
                }
            }
            OnboardingIntent::Invitation { intent } => {
                if let Some(environment) = owner.environment.clone() {
                    let retry = match &intent {
                        super::invitation_controller::InvitationIntent::Submit => true,
                        super::invitation_controller::InvitationIntent::SubmitForOwner {
                            expected_owner,
                        } => *expected_owner == owner.invitation.publication().owner_generation,
                        _ => false,
                    };
                    if retry && owner.invitation_recovery.is_some() {
                        if owner.invitation.begin_commit_retry() {
                            owner.queue.push_back(Work::RetryInvitation);
                        }
                    } else if let Some(request) = owner.invitation.intent(
                        intent,
                        &environment.installation,
                        authorization,
                        gateway,
                    ) {
                        if matches!(
                            request.kind,
                            super::invitation_controller::InvitationRequestKind::Preview
                        ) {
                            let revision = owner.registry_revision;
                            owner.setup.intent(
                                GatewaySetupIntent::Cancel,
                                &environment.registry,
                                revision,
                                authorization,
                            );
                        }
                        owner.queue.push_back(Work::Invitation(request));
                    }
                }
            }
            OnboardingIntent::Setup { intent } => {
                if owner.destinations.pending_endpoint.is_some()
                    || owner.invitation.publication().submitting
                    || !owner.invitation.publication().can_cancel
                {
                    return self.reject_intent();
                }
                if let Some(environment) = owner.environment.clone() {
                    if matches!(intent, GatewaySetupIntent::Open { .. })
                        && owner.invitation.publication().active
                    {
                        owner.invitation.intent(
                            super::invitation_controller::InvitationIntent::Cancel,
                            &environment.installation,
                            authorization,
                            gateway,
                        );
                    }
                    let revision = owner.registry_revision;
                    if let Some(request) =
                        owner
                            .setup
                            .intent(intent, &environment.registry, revision, authorization)
                    {
                        owner.queue.push_back(Work::Setup(request));
                    }
                }
            }
            OnboardingIntent::SelectGateway { endpoint_id } => {
                if owner.destinations.pending_endpoint.is_none()
                    && !owner.setup.publication().pending
                    && !owner.invitation.publication().submitting
                    && owner.invitation.publication().can_cancel
                    && (owner.destinations.selected_endpoint.as_ref() != Some(&endpoint_id)
                        || !connected.contains(&endpoint_id))
                    && owner
                        .destinations
                        .endpoints
                        .iter()
                        .any(|e| e.id == endpoint_id)
                {
                    owner.selection_generation += 1;
                    let generation = owner.selection_generation;
                    owner.destinations.action_generation = generation;
                    owner.destinations.pending_endpoint = Some(endpoint_id.clone());
                    owner.destinations.error = None;
                    owner.queue.push_back(Work::Select {
                        endpoint_id,
                        generation,
                    });
                }
            }
            OnboardingIntent::DeleteGateway {
                endpoint_id,
                expected_registry_revision,
            } => {
                if owner.destinations.pending_endpoint.is_none()
                    && !owner.setup.publication().pending
                    && !owner.invitation.publication().submitting
                    && owner.invitation.publication().can_cancel
                    && owner.registry_revision == expected_registry_revision
                    && owner.environment.as_ref().is_some_and(|e| {
                        e.registry
                            .remotes
                            .iter()
                            .any(|endpoint| endpoint.id == endpoint_id)
                    })
                {
                    owner.selection_generation += 1;
                    let generation = owner.selection_generation;
                    owner.destinations.action_generation = generation;
                    owner.destinations.pending_endpoint = Some(endpoint_id.clone());
                    owner.destinations.error = None;
                    owner.queue.push_back(Work::Delete {
                        endpoint_id,
                        generation,
                    });
                }
            }
            OnboardingIntent::AcknowledgeWorkspaceOutcome { request_id } => {
                if owner
                    .destinations
                    .workspace_outcomes
                    .iter()
                    .any(|outcome| outcome.request_id == request_id)
                {
                    owner
                        .destinations
                        .workspace_outcomes
                        .retain(|outcome| outcome.request_id != request_id);
                    owner.workspace_requests.remove(&request_id);
                }
            }
            OnboardingIntent::SetWorkspaceForRequest {
                endpoint_id,
                workspace_id,
                request_id,
            } => {
                if request_id.is_empty()
                    || request_id.len() > 128
                    || owner.workspace_requests.len() >= 128
                    || !owner.workspace_requests.insert(request_id.clone())
                {
                    return self.reject_intent();
                }
                owner.queue.push_back(Work::Workspace {
                    endpoint_id,
                    workspace_id,
                    request_id: Some(request_id),
                    authorization,
                    gateway,
                });
            }
            OnboardingIntent::SetWorkspace {
                endpoint_id,
                workspace_id,
            } => {
                owner.queue.retain(|work| !matches!(work, Work::Workspace {endpoint_id:id,request_id:None,..} if id==&endpoint_id));
                owner.queue.push_back(Work::Workspace {
                    endpoint_id,
                    workspace_id,
                    request_id: None,
                    authorization,
                    gateway,
                });
            }
        }
        owner.wake();
        self.publish_onboarding(&owner)
    }
    pub(super) fn publish_onboarding(&self, owner: &OnboardingRuntime) -> ClientTransition {
        self.publish_settings_value(
            ClientScope::OnboardingInvitation,
            owner.invitation.publication().clone(),
        );
        self.publish_settings_value(ClientScope::GatewaySetup, owner.setup.publication().clone());
        self.publish_settings_value(ClientScope::GatewayDestinations, owner.destinations.clone())
    }
    /// Native adapters and compatibility launch consumers read the same committed registry.
    pub fn active_gateway_endpoint(&self) -> Option<GatewayEndpoint> {
        let registry = self.gateway_registry()?;
        super::runtime::active_gateway(&registry).cloned()
    }
    pub fn onboarding_loading(&self) -> bool {
        self.onboarding
            .lock()
            .expect("onboarding owner poisoned")
            .destinations
            .loading
    }
    pub fn onboarding_busy(&self) -> bool {
        let owner = self.onboarding.lock().expect("onboarding owner poisoned");
        owner.destinations.loading
            || owner.destinations.pending_endpoint.is_some()
            || owner.setup.publication().pending
            || owner.invitation.publication().submitting
    }
    pub fn onboarding_setup_required(&self) -> bool {
        self.gateway_registry()
            .is_some_and(|registry| super::registry::setup_required(&registry))
    }
    pub fn gateway_registry(&self) -> Option<GatewayRegistry> {
        self.onboarding
            .lock()
            .expect("onboarding owner poisoned")
            .environment
            .as_ref()
            .map(|e| e.registry.clone())
    }
    #[cfg(feature = "test-support")]
    pub fn install_onboarding_environment_for_test(&self, environment: OnboardingEnvironment) {
        self.onboarding
            .lock()
            .expect("onboarding owner poisoned")
            .environment = Some(environment.clone());
        self.adopt_onboarding_registry(environment.registry);
    }
    pub(crate) fn start_onboarding_controller(self: &Arc<Self>) {
        let (wake, received) = mpsc::sync_channel(1);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-onboarding".into())
            .spawn(move || {
                loop {
                    let Some(core) = weak.upgrade() else { return };
                    if core.is_stopped() {
                        return;
                    }
                    let work = core
                        .onboarding
                        .lock()
                        .expect("onboarding owner poisoned")
                        .queue
                        .pop_front();
                    if let Some(work) = work {
                        core.execute_onboarding(work);
                    } else {
                        drop(core);
                        if received.recv().is_err() {
                            return;
                        }
                    }
                }
            })
            .expect("onboarding worker could not start");
        let mut owner = self.onboarding.lock().expect("onboarding owner poisoned");
        owner.wake = Some(wake);
        owner.task = Some(task);
    }
    pub(super) fn persist_onboarding_registry(&self, registry: &GatewayRegistry) -> Result<()> {
        match self.request_onboarding_effect(OnboardingPlatformEffect::PersistGatewayRegistry {
            registry: registry.clone(),
        })? {
            ClientEffectResult::Completed => Ok(()),
            _ => anyhow::bail!("gateway_registry_write_failed"),
        }
    }
    pub(super) fn adopt_onboarding_registry(&self, registry: GatewayRegistry) {
        let mut owner = self.onboarding.lock().expect("onboarding owner poisoned");
        if let Some(environment) = owner.environment.as_mut() {
            if environment.registry != registry {
                environment.registry = registry.clone();
                owner.registry_revision += 1;
            }
            owner.destinations.installation_id = registry.installation_id.clone();
            owner.destinations.registry_revision = owner.registry_revision;
            owner.destinations.endpoints = registry
                .local
                .iter()
                .cloned()
                .chain(registry.remotes)
                .collect();
            owner.destinations.selected_endpoint = registry.active_gateway_id;
        }
        self.publish_onboarding(&owner);
    }
    fn setup_request_current(&self, request: &GatewaySetupRequest) -> bool {
        let authorization = self.authorization_connection_generation();
        let owner = self.onboarding.lock().expect("onboarding owner poisoned");
        !self.is_stopped()
            && owner
                .setup
                .accepts(request.ticket, owner.registry_revision, authorization)
    }
    fn execute_onboarding(&self, work: Work) {
        if matches!(work, Work::Initialize) {
            let result =
                self.request_onboarding_effect(OnboardingPlatformEffect::LoadGatewayEnvironment);
            let result = result.and_then(|result| match result {
                ClientEffectResult::GatewayEnvironmentLoaded { mut environment } => {
                    super::registry_recovery::recover_gateway_bindings(
                        &mut environment.registry,
                        &environment.binding_journals,
                        &GatewaySessionPlatformStorage(self),
                        |registry| self.persist_onboarding_registry(registry),
                        |gateway_id| match self.request_onboarding_effect(
                            OnboardingPlatformEffect::RemoveGatewayBindingJournal {
                                gateway_id: gateway_id.into(),
                            },
                        )? {
                            ClientEffectResult::Completed => Ok(()),
                            _ => anyhow::bail!("gateway_journal_remove_failed"),
                        },
                    );
                    if environment.discard_unbound_remote_candidates {
                        super::registry_recovery::recover_unbound_remote_candidates(
                            &mut environment.registry,
                            &GatewaySessionPlatformStorage(self),
                            |registry| self.persist_onboarding_registry(registry),
                        )?;
                    }
                    Ok(ClientEffectResult::GatewayEnvironmentLoaded { environment })
                }
                other => Ok(other),
            });

            let authorization = self.authorization_connection_generation();
            let mut owner = self.onboarding.lock().expect("onboarding owner poisoned");
            owner.destinations.loading = false;
            match result {
                Ok(ClientEffectResult::GatewayEnvironmentLoaded { environment }) => {
                    owner.destinations.local_install_required = environment.local_install_required;
                    owner.destinations.local_update_required = environment.local_update_required;
                    let mode = super::runtime::active_gateway(&environment.registry)
                        .filter(|endpoint| {
                            endpoint.kind == GatewayEndpointKind::Remote
                                && endpoint.session_ref.is_none()
                        })
                        .map(|endpoint| GatewaySetupMode::ReauthenticateGateway {
                            endpoint_id: endpoint.id.clone(),
                            close_on_success: false,
                        })
                        .unwrap_or(GatewaySetupMode::Initial {
                            allow_local: environment.registry.local.is_some()
                                && !environment.local_provisioned,
                        });
                    let revision = owner.registry_revision;
                    owner.setup.intent(
                        GatewaySetupIntent::Open { mode },
                        &environment.registry,
                        revision,
                        authorization,
                    );
                    let bootstrap = environment.registry.active_gateway_id.is_some()
                        || environment.registry.local.is_some()
                            && !environment.local_install_required;
                    owner.environment = Some(environment.clone());
                    let deferred = std::mem::take(&mut owner.deferred);
                    drop(owner);
                    self.adopt_onboarding_registry(environment.registry);
                    for intent in deferred {
                        self.onboarding_intent(intent);
                    }
                    let mut owner = self.onboarding.lock().expect("onboarding owner poisoned");
                    if bootstrap && !owner.invitation.publication().active {
                        owner.destinations.pending_endpoint =
                            owner.destinations.selected_endpoint.clone();
                        owner.queue.push_back(Work::Bootstrap);
                        self.publish_onboarding(&owner);
                    }
                    return;
                }
                _ => owner.destinations.error = Some("gateway_environment_load_failed".into()),
            }
            self.publish_onboarding(&owner);
            return;
        }
        let Some(mut environment) = self
            .onboarding
            .lock()
            .expect("onboarding owner poisoned")
            .environment
            .clone()
        else {
            return;
        };
        match work {
            Work::Bootstrap => {
                let result = (|| -> Result<()> {
                    if environment.registry.active_gateway_id.is_none()
                        && !environment.local_install_required
                    {
                        if let Some(local) = &environment.registry.local {
                            let plan = setup::plan_activate_gateway_registry(
                                &environment.registry,
                                &local.id,
                            )?;
                            self.persist_onboarding_registry(&plan.registry)?;
                            environment.registry = plan.registry;
                        }
                    }
                    let Some(endpoint) =
                        super::runtime::active_gateway(&environment.registry).cloned()
                    else {
                        return Ok(());
                    };
                    if endpoint.kind == GatewayEndpointKind::Local {
                        self.prepare_onboarding_local(&mut environment, false)?;
                    }
                    if !super::registry::setup_required(&environment.registry) {
                        self.connect_onboarding_endpoint(&mut environment, &endpoint.id)?;
                    }
                    Ok(())
                })();
                self.adopt_onboarding_registry(environment.registry);
                let mut owner = self.onboarding.lock().expect("onboarding owner poisoned");
                owner.destinations.pending_endpoint = None;
                owner.destinations.error = result.err().map(|_| "gateway_startup_failed".into());
                self.publish_onboarding(&owner);
            }
            Work::Invitation(request) => self.execute_onboarding_invitation(request, environment),
            Work::RetryInvitation => self.retry_onboarding_invitation(environment),
            Work::Setup(request) => {
                if !self.setup_request_current(&request) {
                    return;
                }
                let operation = self.gateway_operation_epoch();
                let result = self.execute_gateway_setup(&mut environment, &request);
                // Durable writes belong to the registry even if the form was replaced meanwhile.
                self.adopt_onboarding_registry(environment.registry);
                let authorization = self.authorization_connection_generation();
                let mut owner = self.onboarding.lock().expect("onboarding owner poisoned");
                owner.setup.complete(
                    request.ticket,
                    request.ticket.registry_revision,
                    if self.gateway_operation_epoch() == operation {
                        request.ticket.authorization_generation
                    } else {
                        authorization
                    },
                    result
                        .as_ref()
                        .map(|e| e.as_ref())
                        .map_err(|_| "gateway_setup_failed".into()),
                );
                self.publish_onboarding(&owner);
            }
            Work::Select {
                endpoint_id,
                generation,
            } => {
                let result = self.connect_onboarding_endpoint(&mut environment, &endpoint_id);
                self.adopt_onboarding_registry(environment.registry);
                let mut owner = self.onboarding.lock().expect("onboarding owner poisoned");
                if owner.selection_generation == generation {
                    owner.destinations.pending_endpoint = None;
                    owner.destinations.outcome = Some(GatewayDestinationOutcome {
                        generation,
                        endpoint_id,
                        succeeded: result.is_ok(),
                    });
                    owner.destinations.error =
                        result.err().map(|_| "gateway_connection_failed".into());
                }
                self.publish_onboarding(&owner);
            }
            Work::Delete {
                endpoint_id,
                generation,
            } => {
                let result = self.delete_onboarding_endpoint(&mut environment, &endpoint_id);
                self.adopt_onboarding_registry(environment.registry);
                let mut owner = self.onboarding.lock().expect("onboarding owner poisoned");
                if owner.selection_generation == generation {
                    owner.destinations.pending_endpoint = None;
                    owner.destinations.outcome = Some(GatewayDestinationOutcome {
                        generation,
                        endpoint_id,
                        succeeded: result.is_ok(),
                    });
                    owner.destinations.error = result.err().map(|_| "gateway_delete_failed".into());
                }
                self.publish_onboarding(&owner);
            }
            Work::Workspace {
                endpoint_id,
                workspace_id,
                request_id,
                authorization,
                gateway,
            } => {
                let result = (|| {
                    ensure!(
                        self.authorization_connection_generation() == authorization
                            && self.gateway_operation_epoch() == gateway
                            && environment.registry.active_gateway_id.as_ref()
                                == Some(&endpoint_id),
                        "gateway_workspace_scope_changed"
                    );
                    let plan = setup::plan_set_gateway_workspace_registry(
                        &environment.registry,
                        &endpoint_id,
                        workspace_id,
                    )?;
                    if plan.registry != environment.registry {
                        self.persist_onboarding_registry(&plan.registry)?;
                    }
                    Ok::<_, anyhow::Error>(plan.registry)
                })();
                let succeeded = result.is_ok();
                if let Ok(registry) = result {
                    self.adopt_onboarding_registry(registry);
                }
                let mut owner = self.onboarding.lock().expect("onboarding owner poisoned");
                if let Some(request_id) = request_id {
                    owner
                        .destinations
                        .workspace_outcomes
                        .push(GatewayWorkspaceOutcome {
                            request_id,
                            endpoint_id,
                            succeeded,
                        });
                }
                owner.destinations.error =
                    (!succeeded).then(|| "gateway_registry_write_failed".into());
                self.publish_onboarding(&owner);
            }
            Work::Initialize => unreachable!(),
        }
    }
    pub(super) fn connect_onboarding_endpoint(
        &self,
        environment: &mut OnboardingEnvironment,
        id: &str,
    ) -> Result<GatewayEndpoint> {
        let _handoff = self.begin_onboarding_handoff(id);
        let storage = GatewaySessionPlatformStorage(self);
        let retry = [
            Duration::from_millis(100),
            Duration::from_millis(250),
            Duration::from_millis(500),
        ];
        let mut endpoint = super::runtime::endpoint_by_id(&environment.registry, id)
            .context("gateway_not_found")?
            .clone();
        for attempt in 0..2 {
            let result = self.ensure_gateway_session(
                GatewaySessionRefreshRequest {
                    endpoint: &endpoint,
                    installation_id: &environment.installation.installation_id,
                    client_kind: environment.installation.client_kind,
                    now_unix: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
                    timeout: environment.timings.startup_timeout,
                    ws_timings: super::runtime::ws_timings_for_endpoint(
                        environment.ws_timings,
                        endpoint.kind,
                        environment.remote_connect_timeout_min,
                    ),
                    retry_delays: &retry,
                },
                &storage,
            );
            if attempt == 0
                && endpoint.kind == GatewayEndpointKind::Local
                && matches!(
                    result,
                    Err(
                        super::session_connection::GatewaySessionConnectionFailure::Terminal { .. }
                    )
                )
            {
                endpoint = self.prepare_onboarding_local(environment, true)?;
                continue;
            }
            result?;
            break;
        }
        let plan = setup::plan_activate_gateway_registry(&environment.registry, id)?;
        self.persist_onboarding_registry(&plan.registry)?;
        environment.registry = plan.registry;
        Ok(endpoint)
    }
    fn prepare_onboarding_local(
        &self,
        environment: &mut OnboardingEnvironment,
        recover: bool,
    ) -> Result<GatewayEndpoint> {
        let storage = GatewaySessionPlatformStorage(self);
        let local = environment
            .registry
            .local
            .as_ref()
            .context("local_gateway_unavailable")?;
        let recover = recover || self.gateway_session().terminal_reason(&local.id).is_some();
        let mutation = self.begin_gateway_session_mutation(&local.id)?;
        let prepared =
            match self.request_onboarding_effect(OnboardingPlatformEffect::PrepareLocalGateway {
                endpoint: environment
                    .registry
                    .local
                    .clone()
                    .context("local_gateway_unavailable")?,
                recover,
            })? {
                ClientEffectResult::LocalGatewayPrepared { prepared } => prepared,
                _ => anyhow::bail!("local_gateway_prepare_failed"),
            };
        if !prepared.warnings.is_empty() {
            let mut owner = self.onboarding.lock().expect("onboarding owner poisoned");
            for message in &prepared.warnings {
                owner.warning_generation += 1;
                let id = owner.warning_generation;
                owner.destinations.warnings.push(GatewaySetupWarning {
                    id,
                    message: message.clone(),
                });
                if owner.destinations.warnings.len() > 16 {
                    owner.destinations.warnings.remove(0);
                }
            }
            self.publish_onboarding(&owner);
        }
        let mut staged = environment.registry.clone();
        staged.local = Some(prepared.endpoint.clone());
        if prepared.activation.is_some() {
            super::provisioning::clear_endpoint_session_binding_durably(
                &mut staged,
                &prepared.endpoint.id,
                &storage,
                |registry| self.persist_onboarding_registry(registry),
            )?;
        } else if staged != environment.registry {
            self.persist_onboarding_registry(&staged)?;
        }
        environment.registry = staged;
        if let Some(activation) = prepared.activation {
            let timeout = environment.timings.startup_timeout;
            super::provisioning::provision_endpoint_session(
                &mut environment.registry,
                &environment.installation,
                &prepared.endpoint.id,
                activation.expose_secret(),
                &storage,
                |base, code, params| {
                    super::provisioning::activate_device_session(base, code, params, timeout)
                },
                |base, access, id| {
                    super::provisioning::revoke_session_best_effort(base, access, id, timeout)
                },
                |registry| self.persist_onboarding_registry(registry),
            )?;
        }
        drop(mutation);
        if recover {
            self.reduce_gateway_session_lifecycle(
                &prepared.endpoint.id,
                super::session_lifecycle::SessionLifecycleEvent::NoStoredSession,
            );
        }
        super::runtime::endpoint_by_id(&environment.registry, &prepared.endpoint.id)
            .cloned()
            .context("local_gateway_unavailable")
    }
    pub fn verify_configured_gateway_session(
        &self,
        id: &str,
    ) -> Result<Option<super::session_lifecycle::SessionTerminalReason>> {
        let environment = self
            .onboarding
            .lock()
            .expect("onboarding owner poisoned")
            .environment
            .clone()
            .context("gateway_environment_unavailable")?;
        let endpoint = super::runtime::endpoint_by_id(&environment.registry, id)
            .context("gateway_not_found")?;
        self.verify_gateway_session_identity(
            endpoint,
            &environment.installation.installation_id,
            environment.installation.client_kind,
        )
    }
    pub fn ensure_configured_gateway_session(
        &self,
        id: &str,
        recover: bool,
    ) -> Result<super::session_refresh::GatewaySessionPreparation> {
        self.with_gateway_session_refresh(id, || {
            let environment = self
                .onboarding
                .lock()
                .expect("onboarding owner poisoned")
                .environment
                .clone()
                .context("gateway_environment_unavailable")?;
            let endpoint = super::runtime::endpoint_by_id(&environment.registry, id)
                .context("gateway_not_found")?;
            let storage = GatewaySessionPlatformStorage(self);
            let retry = [
                Duration::from_millis(100),
                Duration::from_millis(250),
                Duration::from_millis(500),
            ];
            let request = GatewaySessionRefreshRequest {
                endpoint,
                installation_id: &environment.installation.installation_id,
                client_kind: environment.installation.client_kind,
                now_unix: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
                timeout: environment.timings.startup_timeout,
                ws_timings: super::runtime::ws_timings_for_endpoint(
                    environment.ws_timings,
                    endpoint.kind,
                    environment.remote_connect_timeout_min,
                ),
                retry_delays: &retry,
            };
            if recover {
                return self.prepare_gateway_session(
                    request,
                    &storage,
                    super::session_connection::exchange_refresh,
                    |base, access, id| {
                        super::provisioning::revoke_session_best_effort(
                            base,
                            access,
                            id,
                            environment.timings.startup_timeout,
                        )
                    },
                );
            }
            anyhow::bail!("direct preparation requires recovery mode")
        })
    }

    pub fn refresh_configured_gateway_session(
        &self,
        id: &str,
    ) -> Result<
        super::session_connection::GatewaySessionConnectionResult,
        super::session_connection::GatewaySessionConnectionFailure,
    > {
        self.refresh_configured_gateway_session_inner(id, None)
    }
    pub fn refresh_configured_gateway_session_after_unauthorized(
        &self,
        id: &str,
        generation: u64,
    ) -> Result<
        super::session_connection::GatewaySessionConnectionResult,
        super::session_connection::GatewaySessionConnectionFailure,
    > {
        self.refresh_configured_gateway_session_inner(id, Some(generation))
    }
    fn refresh_configured_gateway_session_inner(
        &self,
        id: &str,
        rejected: Option<u64>,
    ) -> Result<
        super::session_connection::GatewaySessionConnectionResult,
        super::session_connection::GatewaySessionConnectionFailure,
    > {
        let environment = self
            .onboarding
            .lock()
            .expect("onboarding owner poisoned")
            .environment
            .clone()
            .ok_or_else(|| {
                super::session_connection::GatewaySessionConnectionFailure::Unavailable {
                    code: "gateway_environment_unavailable".into(),
                }
            })?;
        let endpoint =
            super::runtime::endpoint_by_id(&environment.registry, id).ok_or_else(|| {
                super::session_connection::GatewaySessionConnectionFailure::Unavailable {
                    code: "gateway_not_found".into(),
                }
            })?;
        let retry = [
            Duration::from_millis(100),
            Duration::from_millis(250),
            Duration::from_millis(500),
        ];
        let request = GatewaySessionRefreshRequest {
            endpoint,
            installation_id: &environment.installation.installation_id,
            client_kind: environment.installation.client_kind,
            now_unix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            timeout: environment.timings.startup_timeout,
            ws_timings: super::runtime::ws_timings_for_endpoint(
                environment.ws_timings,
                endpoint.kind,
                environment.remote_connect_timeout_min,
            ),
            retry_delays: &retry,
        };
        let storage = GatewaySessionPlatformStorage(self);
        match rejected {
            Some(generation) => {
                self.refresh_gateway_session_after_unauthorized(request, &storage, generation)
            }
            None => self.ensure_gateway_session(request, &storage),
        }
    }
    fn delete_onboarding_endpoint(
        &self,
        environment: &mut OnboardingEnvironment,
        id: &str,
    ) -> Result<Option<GatewayEndpoint>> {
        let _mutation = self.begin_gateway_session_mutation(id)?;
        let storage = GatewaySessionPlatformStorage(self);
        let plan = setup::plan_delete_remote_gateway_registry(
            &environment.registry,
            setup::DeleteRemoteGatewayRegistryInput {
                gateway_id: id,
                local_gateway_id: environment.registry.local.as_ref().map(|e| e.id.as_str()),
            },
        )?;
        commit_gateway_deletion(&plan, &storage, |registry| {
            self.persist_onboarding_registry(registry)
        })?;
        environment.registry = plan.registry;
        self.clear_gateway_session(id)?;
        if plan.deleted_active {
            if let Some(endpoint) = plan.fallback_endpoint {
                return self
                    .connect_onboarding_endpoint(environment, &endpoint.id)
                    .map(Some);
            }
        }
        return Ok(None);
    }
    fn execute_gateway_setup(
        &self,
        environment: &mut OnboardingEnvironment,
        request: &GatewaySetupRequest,
    ) -> Result<Option<GatewayEndpoint>> {
        let storage = GatewaySessionPlatformStorage(self);
        if let Some(endpoint_id) = &request.recovery_endpoint {
            return self
                .connect_onboarding_endpoint(environment, endpoint_id)
                .map(Some);
        }
        if request.action == GatewaySetupAction::DeleteGateway {
            let GatewaySetupMode::EditGateway { endpoint_id } = &request.mode else {
                anyhow::bail!("gateway_delete_not_allowed")
            };
            return self.delete_onboarding_endpoint(environment, endpoint_id);
        }
        if request.action == GatewaySetupAction::SaveGateway {
            let GatewaySetupMode::EditGateway { endpoint_id } = &request.mode else {
                anyhow::bail!("gateway_edit_not_allowed")
            };
            let _mutation = self.begin_gateway_session_mutation(endpoint_id)?;
            let old = super::runtime::endpoint_by_id(&environment.registry, endpoint_id)
                .context("gateway_not_found")?;
            let address = request
                .address
                .as_ref()
                .context("gateway_address_missing")?;
            if environment.installation.client_kind == pioneer_protocol::ClientKind::Mobile
                && address != &old.gateway_base_url
            {
                require_reachable_remote(setup::validate_remote_gateway_base_url(
                    address.as_str(),
                    environment.timings.connect_timeout,
                )?)?;
                ensure!(
                    self.setup_request_current(request),
                    "gateway_setup_cancelled"
                );
            }
            let index = environment
                .registry
                .remotes
                .iter()
                .position(|endpoint| &endpoint.id == endpoint_id)
                .map_or(environment.registry.remotes.len() + 1, |index| index + 1);
            let plan = setup::plan_update_remote_gateway_registry(
                &environment.registry,
                setup::UpdateRemoteGatewayRegistryInput {
                    gateway_id: endpoint_id,
                    name: &request.name,
                    gateway_base_url: request
                        .address
                        .as_ref()
                        .context("gateway_address_missing")?
                        .as_str(),
                    default_remote_name: environment.remote_name(index),
                },
            )?;
            self.persist_onboarding_registry(&plan.registry)?;
            environment.registry = plan.registry;
            return Ok(Some(plan.endpoint));
        }
        let mut rollback = None;
        let (endpoint_id, activation) = if request.action == GatewaySetupAction::StartLocal {
            let endpoint = self.prepare_onboarding_local(environment, false)?;
            (endpoint.id, None)
        } else if let GatewaySetupMode::ReauthenticateGateway { endpoint_id, .. } = &request.mode {
            (endpoint_id.clone(), request.activation.clone())
        } else {
            let address = request
                .address
                .as_ref()
                .context("gateway_address_missing")?;
            require_reachable_remote(setup::validate_remote_gateway_base_url(
                address.as_str(),
                environment.timings.connect_timeout,
            )?)?;
            ensure!(
                self.setup_request_current(request),
                "gateway_setup_cancelled"
            );
            if let Some(existing) = environment
                .registry
                .remotes
                .iter()
                .find(|endpoint| &endpoint.gateway_base_url == address)
            {
                ensure!(
                    !request
                        .expected_gateway_id
                        .as_ref()
                        .is_some_and(|pin| existing
                            .server_gateway_id
                            .as_ref()
                            .is_some_and(|id| id != pin)),
                    "gateway_identity_mismatch"
                );
                (existing.id.clone(), request.activation.clone())
            } else {
                ensure!(
                    !request
                        .expected_gateway_id
                        .as_ref()
                        .is_some_and(|pin| environment
                            .registry
                            .remotes
                            .iter()
                            .any(|endpoint| endpoint.server_gateway_id.as_ref() == Some(pin))),
                    "gateway_identity_mismatch"
                );
                let change = setup::plan_add_remote_gateway(
                    &environment.registry,
                    setup::AddRemoteGatewayInput {
                        name: &request.name,
                        gateway_base_url: address.as_str(),
                        new_endpoint_id: setup::generated_remote_gateway_endpoint_id(),
                        default_remote_name: environment
                            .remote_name(environment.registry.remotes.len() + 1),
                    },
                )?;
                let mut staged = environment.registry.clone();
                let commit = change.apply_to_registry(
                    &mut staged,
                    setup::AddRemoteGatewayApplyMode::ProfileOnly,
                )?;
                self.persist_onboarding_registry(&staged)?;
                environment.registry = staged;
                let id = commit.endpoint.id.clone();
                rollback = Some((change, commit));
                (id, request.activation.clone())
            }
        };
        let endpoint = super::runtime::endpoint_by_id(&environment.registry, &endpoint_id)
            .context("gateway_not_found")?;
        ensure!(
            !request
                .expected_gateway_id
                .as_ref()
                .is_some_and(|pin| endpoint
                    .server_gateway_id
                    .as_ref()
                    .is_some_and(|actual| actual != pin)),
            "gateway_identity_mismatch"
        );
        let replacing_session = activation.is_some() && endpoint.session_ref.is_some();
        if replacing_session {
            let mutation = self.begin_gateway_session_mutation(&endpoint_id)?;
            let activation = activation.as_ref().context("gateway_activation_required")?;
            let timeout = environment.timings.startup_timeout;
            super::provisioning::replace_endpoint_session(
                endpoint,
                &environment.installation,
                activation.expose_secret(),
                &storage,
                |base, code, params| {
                    super::provisioning::activate_device_session(base, code, params, timeout)
                },
                |base, access, id| {
                    super::provisioning::revoke_session_best_effort(base, access, id, timeout)
                },
            )?;
            drop(mutation);
        }
        if endpoint.session_ref.is_none() {
            let activation = activation.context("gateway_activation_required")?;
            let timeout = environment.timings.startup_timeout;
            let result = super::provisioning::provision_endpoint_session_pinned(
                &mut environment.registry,
                &environment.installation,
                &endpoint_id,
                activation.expose_secret(),
                request.expected_gateway_id.as_ref(),
                &storage,
                |base, code, params| {
                    super::provisioning::activate_device_session(base, code, params, timeout)
                },
                |base, access, id| {
                    super::provisioning::revoke_session_best_effort(base, access, id, timeout)
                },
                |registry| self.persist_onboarding_registry(registry),
            );
            if let Err(error) = result {
                if let Some((change, commit)) = rollback {
                    super::provisioning::rollback_unprovisioned_remote(
                        &mut environment.registry,
                        &change,
                        &commit,
                        &storage,
                        |registry| self.persist_onboarding_registry(registry),
                    )?;
                }
                return Err(error);
            }
        }
        ensure!(
            self.setup_request_current(request),
            "gateway_setup_cancelled"
        );
        self.onboarding
            .lock()
            .expect("onboarding owner poisoned")
            .setup
            .retain_provisioned_endpoint(request.ticket, endpoint_id.clone());
        let _handoff = self.begin_onboarding_handoff(&endpoint_id);
        if replacing_session {
            self.clear_gateway_session(&endpoint_id)?;
        }
        self.connect_onboarding_endpoint(environment, &endpoint_id)
            .map(Some)
    }
}

fn require_reachable_remote(validation: setup::RemoteGatewayValidation) -> Result<()> {
    ensure!(
        matches!(validation, setup::RemoteGatewayValidation::Reachable { .. }),
        "gateway_unreachable"
    );
    Ok(())
}

fn commit_gateway_deletion(
    plan: &setup::DeleteRemoteGatewayRegistryPlan,
    storage: &dyn GatewaySessionStorage,
    save: impl FnOnce(&GatewayRegistry) -> Result<()>,
) -> Result<()> {
    // The registry remains a durable cleanup pointer until credential deletion succeeds.
    if plan.endpoint.session_ref.is_some() {
        storage.delete(&plan.endpoint)?;
    }
    save(&plan.registry)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unreachable_validation_is_a_failure_and_remote_names_use_current_registry_position() {
        let base =
            super::super::endpoint::GatewayBaseUrl::parse_presentation("https://gateway.invalid")
                .unwrap();
        let security = base.transport_security();
        assert!(
            require_reachable_remote(setup::RemoteGatewayValidation::Unreachable {
                gateway_base_url: base.clone(),
                transport_security: security
            })
            .is_err()
        );
        assert!(
            require_reachable_remote(setup::RemoteGatewayValidation::Reachable {
                gateway_base_url: base,
                transport_security: security
            })
            .is_ok()
        );
        let mut environment = environment();
        environment.default_remote_name = "Gateway {index}".into();
        assert_eq!(environment.remote_name(1), "Gateway 1");
        assert_eq!(environment.remote_name(2), "Gateway 2");
    }

    fn environment() -> OnboardingEnvironment {
        let mut registry = super::super::registry::default_registry(
            &super::super::registry::GatewayRegistryConfig { local: None },
        );
        registry.installation_id = Some("synthetic-installation".into());
        OnboardingEnvironment {
            registry,
            binding_journals: vec![],
            remote_connect_timeout_min: Duration::ZERO,
            discard_unbound_remote_candidates: false,
            default_remote_name: "Remote".into(),
            installation: pioneer_protocol::ClientInstallationDescriptor {
                installation_id: "synthetic-installation".into(),
                display_name: "Synthetic".into(),
                client_kind: pioneer_protocol::ClientKind::Desktop,
                platform: None,
                client_version: None,
            },
            timings: super::super::timings::GatewayTimings::from_millis(10, 10, 10).unwrap(),
            ws_timings: super::super::timings::GatewayWsTimings::from_millis(10, 10, 10, 10, 20, 0)
                .unwrap(),
            local_provisioned: false,
            local_install_required: false,
            local_update_required: false,
        }
    }
    #[test]
    fn failed_native_local_recovery_keeps_the_terminal_binding_for_retry() {
        let core = Arc::new(ClientCore::new());
        let mut env = environment();
        env.registry = super::super::provisioning::tests::registry();
        env.registry.local.as_mut().unwrap().session_ref = Some("synthetic-old-session".into());
        let before = env.registry.clone();
        core.onboarding.lock().unwrap().environment = Some(env.clone());
        core.reduce_gateway_session_lifecycle(
            "local",
            super::super::session_lifecycle::SessionLifecycleEvent::AuthFailed {
                reason: super::super::session_lifecycle::SessionTerminalReason::SessionRevoked,
            },
        );
        let worker_core = core.clone();
        let worker =
            std::thread::spawn(move || worker_core.prepare_onboarding_local(&mut env, true));
        let mut sequence = ClientChangeSequence::ZERO;
        let plan = loop {
            let batch = core.wait_for_publications(sequence);
            sequence = batch.sequence;
            if let Some(plan) = batch.effects.into_iter().next() {
                break plan;
            }
        };
        assert!(matches!(
            plan.effect(),
            ClientPlannedEffect::OnboardingPlatform(
                OnboardingPlatformEffect::PrepareLocalGateway { recover: true, .. }
            )
        ));
        core.complete_effect(ClientEffectCompletion::new(
            plan.operation_id().clone(),
            plan.generation(),
            ClientEffectResult::Failed {
                code: "synthetic_local_unavailable".into(),
            },
        ));
        assert!(worker.join().unwrap().is_err());
        assert_eq!(core.gateway_registry().unwrap(), before);
        assert_eq!(
            core.gateway_session().terminal_reason("local"),
            Some(super::super::session_lifecycle::SessionTerminalReason::SessionRevoked)
        );
        assert!(core.wait_for_publications(sequence).effects.is_empty());
        core.shutdown();
    }
    #[test]
    fn authorization_replacement_cancels_queued_setup_before_its_effects() {
        let core = ClientCore::new();
        core.onboarding.lock().unwrap().environment = Some(environment());
        core.onboarding_intent(OnboardingIntent::Setup {
            intent: GatewaySetupIntent::EditAddress {
                value: "https://gateway.invalid".into(),
            },
        });
        core.onboarding_intent(OnboardingIntent::Setup {
            intent: GatewaySetupIntent::EditActivation {
                value: pioneer_protocol::AuthSecretString::new("K7M4P9Q2"),
            },
        });
        core.onboarding_intent(OnboardingIntent::Setup {
            intent: GatewaySetupIntent::SubmitRemote,
        });
        assert!(core.onboarding.lock().unwrap().setup.publication().pending);
        core.fence_onboarding_authorization(
            &super::super::identity_authorization::IdentityAuthorizationPublication {
                connection_generation: 1,
                ..Default::default()
            },
        );
        assert!(!core.onboarding.lock().unwrap().setup.publication().pending);
        let sequence = core
            .wait_for_publications(ClientChangeSequence::ZERO)
            .sequence;
        let work = core.onboarding.lock().unwrap().queue.pop_front().unwrap();
        core.execute_onboarding(work);
        let batch = core.wait_for_publications(sequence);
        assert!(batch.changes.is_empty());
        assert!(batch.effects.is_empty());
    }
    #[test]
    fn workspace_completions_survive_batched_delivery_until_acknowledged() {
        let core = ClientCore::new();
        core.onboarding.lock().unwrap().environment = Some(environment());
        for id in ["first", "second"] {
            core.onboarding_intent(OnboardingIntent::SetWorkspaceForRequest {
                endpoint_id: "removed-endpoint".into(),
                workspace_id: None,
                request_id: id.into(),
            });
            let work = core.onboarding.lock().unwrap().queue.pop_front().unwrap();
            core.execute_onboarding(work);
        }
        let snapshot = core
            .snapshot(&ClientScope::GatewayDestinations)
            .unwrap()
            .typed::<GatewayDestinationsPublication>()
            .unwrap();
        assert_eq!(
            snapshot
                .payload()
                .workspace_outcomes
                .iter()
                .map(|outcome| outcome.request_id.as_str())
                .collect::<Vec<_>>(),
            vec!["first", "second"]
        );
        assert!(
            snapshot
                .payload()
                .workspace_outcomes
                .iter()
                .all(|outcome| !outcome.succeeded)
        );
        core.onboarding_intent(OnboardingIntent::AcknowledgeWorkspaceOutcome {
            request_id: "first".into(),
        });
        let owner = core.onboarding.lock().unwrap();
        assert_eq!(owner.destinations.workspace_outcomes.len(), 1);
        assert_eq!(
            owner.destinations.workspace_outcomes[0].request_id,
            "second"
        );
        assert!(!owner.workspace_requests.contains("first"));
    }
    #[test]
    fn workspace_requests_are_bounded_and_duplicate_ids_are_rejected() {
        let core = ClientCore::new();
        core.onboarding.lock().unwrap().environment = Some(environment());
        let request = |id: String| OnboardingIntent::SetWorkspaceForRequest {
            endpoint_id: "endpoint".into(),
            workspace_id: None,
            request_id: id,
        };
        for i in 0..128 {
            assert_ne!(
                core.onboarding_intent(request(i.to_string())).outcome(),
                ClientTransitionOutcome::Rejected
            );
        }
        assert_eq!(
            core.onboarding_intent(request("overflow".into())).outcome(),
            ClientTransitionOutcome::Rejected
        );
        assert_eq!(
            core.onboarding_intent(request("1".into())).outcome(),
            ClientTransitionOutcome::Rejected
        );
        assert_eq!(core.onboarding.lock().unwrap().queue.len(), 128);
        // An early acknowledgement cannot release an in-flight request's slot.
        core.onboarding_intent(OnboardingIntent::AcknowledgeWorkspaceOutcome {
            request_id: "1".into(),
        });
        assert_eq!(
            core.onboarding.lock().unwrap().workspace_requests.len(),
            128
        );
    }
    #[test]
    fn initialization_coalesces_and_failed_effect_can_be_explicitly_retried() {
        let core = Arc::new(ClientCore::new());
        core.onboarding_intent(OnboardingIntent::Initialize);
        core.onboarding_intent(OnboardingIntent::Initialize);
        assert_eq!(core.onboarding.lock().unwrap().queue.len(), 1);
        let worker_core = core.clone();
        let worker = std::thread::spawn(move || {
            let work = worker_core
                .onboarding
                .lock()
                .unwrap()
                .queue
                .pop_front()
                .unwrap();
            worker_core.execute_onboarding(work);
        });
        let mut sequence = ClientChangeSequence::ZERO;
        let plan = loop {
            let batch = core.wait_for_publications(sequence);
            sequence = batch.sequence;
            if let Some(plan) = batch.effects.into_iter().next() {
                break plan;
            }
        };
        core.complete_effect(ClientEffectCompletion::new(
            plan.operation_id().clone(),
            plan.generation(),
            ClientEffectResult::Failed {
                code: "synthetic_storage_unavailable".into(),
            },
        ));
        worker.join().unwrap();
        let publication = core
            .snapshot(&ClientScope::GatewayDestinations)
            .unwrap()
            .typed::<GatewayDestinationsPublication>()
            .unwrap();
        assert!(!publication.payload().loading);
        assert!(publication.payload().error.is_some());
        assert!(core.gateway_registry().is_none());
        core.onboarding_intent(OnboardingIntent::RetryInitialization);
        assert_eq!(core.onboarding.lock().unwrap().queue.len(), 1);
        core.shutdown();
    }
    #[test]
    fn cancelled_queued_setup_never_issues_a_native_or_network_effect() {
        let core = ClientCore::new();
        core.onboarding.lock().unwrap().environment = Some(environment());
        core.onboarding_intent(OnboardingIntent::Setup {
            intent: GatewaySetupIntent::EditAddress {
                value: "https://gateway.invalid".into(),
            },
        });
        core.onboarding_intent(OnboardingIntent::Setup {
            intent: GatewaySetupIntent::EditActivation {
                value: pioneer_protocol::AuthSecretString::new("K7M4P9Q2"),
            },
        });
        core.onboarding_intent(OnboardingIntent::Setup {
            intent: GatewaySetupIntent::SubmitRemote,
        });
        core.onboarding_intent(OnboardingIntent::Setup {
            intent: GatewaySetupIntent::SubmitRemote,
        });
        assert_eq!(core.onboarding.lock().unwrap().queue.len(), 1);
        core.onboarding_intent(OnboardingIntent::Setup {
            intent: GatewaySetupIntent::Cancel,
        });
        let sequence = core
            .wait_for_publications(ClientChangeSequence::ZERO)
            .sequence;
        let work = core.onboarding.lock().unwrap().queue.pop_front().unwrap();
        core.execute_onboarding(work);
        let after = core.wait_for_publications(sequence);
        assert!(after.effects.is_empty());
        assert!(after.changes.is_empty());
    }
    #[test]
    fn equal_registry_and_controlled_input_do_not_publish_visible_changes() {
        let core = ClientCore::new();
        let environment = environment();
        core.onboarding.lock().unwrap().environment = Some(environment.clone());
        core.adopt_onboarding_registry(environment.registry.clone());
        let sequence = core
            .wait_for_publications(ClientChangeSequence::ZERO)
            .sequence;
        core.adopt_onboarding_registry(environment.registry);
        core.onboarding_intent(OnboardingIntent::Setup {
            intent: GatewaySetupIntent::EditName {
                value: String::new(),
            },
        });
        assert!(core.wait_for_publications(sequence).changes.is_empty());
    }
    #[test]
    fn deletion_preserves_order_and_outward_failure_for_each_durable_step() {
        use std::cell::RefCell;
        struct Storage<'a> {
            order: &'a RefCell<Vec<&'static str>>,
            fail: bool,
        }
        impl GatewaySessionStorage for Storage<'_> {
            fn load(
                &self,
                _: &GatewayEndpoint,
            ) -> Result<Option<super::super::session_envelope::GatewaySessionEnvelope>>
            {
                panic!()
            }
            fn persist(
                &self,
                _: &GatewayEndpoint,
                _: &super::super::session_envelope::GatewaySessionEnvelope,
            ) -> Result<()> {
                panic!()
            }
            fn delete(&self, _: &GatewayEndpoint) -> Result<()> {
                self.order.borrow_mut().push("session");
                ensure!(!self.fail, "synthetic deletion failure");
                Ok(())
            }
        }
        for failed_step in [None, Some("session"), Some("registry")] {
            let mut registry = super::super::provisioning::tests::registry();
            let mut endpoint = registry.local.take().unwrap();
            endpoint.kind = GatewayEndpointKind::Remote;
            endpoint.session_ref = Some("synthetic".into());
            registry.active_gateway_id = Some(endpoint.id.clone());
            registry.remotes.push(endpoint.clone());
            let plan = setup::plan_delete_remote_gateway_registry(
                &registry,
                setup::DeleteRemoteGatewayRegistryInput {
                    gateway_id: &endpoint.id,
                    local_gateway_id: None,
                },
            )
            .unwrap();
            let order = RefCell::new(vec![]);
            let storage = Storage {
                order: &order,
                fail: failed_step == Some("session"),
            };
            let result = commit_gateway_deletion(&plan, &storage, |_| {
                order.borrow_mut().push("registry");
                ensure!(
                    failed_step != Some("registry"),
                    "synthetic registry failure"
                );
                Ok(())
            });
            assert_eq!(result.is_err(), failed_step.is_some());
            assert_eq!(
                *order.borrow(),
                if failed_step == Some("session") {
                    vec!["session"]
                } else {
                    vec!["session", "registry"]
                }
            );
        }
    }
}
