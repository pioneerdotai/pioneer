//! Startup diagnostics observe Client readiness without driving feature requests.
use gpui_kit::{App, AppContext, Task};
use pioneer_client::core::{ClientCore, ClientPublicationReference, ClientScope};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use pioneer_observability::{DesktopStartupOutcome, DesktopStartupStage, DesktopStartupTrace};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    sync::Arc,
};

struct ReadinessBinding {
    revisions: RefCell<HashMap<ClientScope, u64>>,
    changed: tokio::sync::watch::Sender<u64>,
}
impl ClientPublicationSink for ReadinessBinding {
    fn publish(&self, publication: ClientPublicationReference) {
        let revision = publication.revisions().scoped().get();
        let mut revisions = self.revisions.borrow_mut();
        if revisions
            .get(publication.scope())
            .is_some_and(|old| *old >= revision)
        {
            return;
        }
        revisions.insert(publication.scope().clone(), revision);
        self.changed.send_modify(|revision| *revision += 1);
    }
}
pub(super) struct DesktopStartupCoordinator {
    _task: Task<()>,
}
impl DesktopStartupCoordinator {
    pub(super) fn new(
        trace: DesktopStartupTrace,
        core: Arc<ClientCore>,
        registrar: Arc<dyn ClientBindingRegistrar>,
        cx: &mut App,
    ) -> Self {
        let binding = Arc::new(ReadinessBinding {
            revisions: RefCell::default(),
            changed: tokio::sync::watch::channel(0).0,
        });
        let mut changes = binding.changed.subscribe();
        let sink: Arc<dyn ClientPublicationSink> = binding.clone();
        let registrations: Vec<ClientBindingRegistration> = [
            ClientScope::Session,
            ClientScope::Navigation,
            ClientScope::GatewayDestinations,
            ClientScope::Administration { workspace_id: None },
            ClientScope::WorkspaceTree { workspace_id: None },
        ]
        .into_iter()
        .map(|scope| registrar.register(scope, Arc::downgrade(&sink)))
        .collect();
        let sink = Arc::downgrade(&sink);
        let input = Arc::downgrade(&binding);
        let task = cx.spawn(async move |cx| {
            // Completion and cancellation retire every publication registration,
            // even while the process retains this finished coordinator handle.
            let _registrations = registrations;
            let _binding = binding;
            let mut scope_registrations = Vec::new();
            let mut telemetry = StartupTelemetry::new(trace.clone());
            let mut scope = (None, None);
            loop {
                let navigation = core.navigation_snapshot();
                let next = (
                    navigation.workspace_id().map(str::to_owned),
                    navigation.active_thread_id().map(str::to_owned),
                );
                if scope != next {
                    scope_registrations.clear();
                    if let Some(input) = input.upgrade() {
                        input.revisions.borrow_mut().retain(|scope, _| {
                            matches!(
                                scope,
                                ClientScope::Session
                                    | ClientScope::Navigation
                                    | ClientScope::GatewayDestinations
                                    | ClientScope::Administration { workspace_id: None }
                                    | ClientScope::WorkspaceTree { workspace_id: None }
                            )
                        });
                    }
                    if let Some(workspace) = &next.0 {
                        scope_registrations.push(registrar.register(
                            ClientScope::WorkspaceTree {
                                workspace_id: Some(workspace.clone()),
                            },
                            sink.clone(),
                        ));
                    }
                    if let Some(thread) = &next.1 {
                        for scope in [
                            ClientScope::Thread {
                                thread_id: thread.clone(),
                            },
                            ClientScope::Composer {
                                thread_id: thread.clone(),
                            },
                            ClientScope::ThreadCapability {
                                thread_id: thread.clone(),
                            },
                        ] {
                            scope_registrations.push(registrar.register(scope, sink.clone()));
                        }
                    }
                    scope = next;
                }
                telemetry.observe(&core);
                if let Some(outcome) = readiness(&core) {
                    telemetry.finish();
                    let trace = trace.clone();
                    let _ = cx.update(|cx| {
                        if let Some(handle) = cx.windows().first().copied() {
                            let _ = handle.update(cx, |_, window, _| {
                                let frame = trace.stage(DesktopStartupStage::OperationalFrame);
                                window.on_next_frame(move |_, _| {
                                    frame.succeed();
                                    trace.finish(outcome);
                                    pioneer_observability::schedule_observability_flush();
                                });
                            });
                        }
                    });
                    break;
                }
                if changes.changed().await.is_err() || core.is_stopped() {
                    break;
                }
            }
        });
        Self { _task: task }
    }
}
fn readiness(core: &ClientCore) -> Option<DesktopStartupOutcome> {
    use pioneer_client::gateway::session_controller::StartupStage;
    let session = core.gateway_session();
    if session.startup.has_failed() {
        return Some(DesktopStartupOutcome::Degraded);
    }
    if core.gateway_registry().is_none() || core.onboarding_loading() {
        return None;
    }
    if core.onboarding_setup_required() {
        return Some(DesktopStartupOutcome::SetupRequired);
    }
    if !core.onboarding_busy()
        && session.status.as_ref().is_some_and(|status| {
            status.connection_state
                == pioneer_client::state::client_state::GatewayConnectionState::Disconnected
        })
    {
        return Some(DesktopStartupOutcome::Degraded);
    }
    let navigation = core.navigation_snapshot();
    let thread = navigation.active_thread_id()?;
    let workspace = navigation.workspace_id()?;
    let capability_ready = core.thread_capability_snapshot(thread).is_some_and(|p| {
        p.request == pioneer_client::threads::capabilities::ThreadCapabilityRequestState::Ready
    });
    let thread_ready = core
        .thread_snapshot(thread)
        .is_some_and(|p| !p.coordinator().history_loading && !p.subscription_failed());
    let composer_ready = core
        .composer_snapshot(thread)
        .is_some_and(|p| p.authorization_fingerprint().is_some());
    (session.startup.transport_ready
        && !session.startup.identity_pending
        && core.current_auth().is_some()
        && core.authorization_snapshot(Some(workspace), None).is_some()
        && !core.workspace_catalog().is_loading()
        && core
            .workspace_tree(workspace)
            .is_some_and(|p| !p.is_loading())
        && session.startup.stage_succeeded(StartupStage::Provider)
        && capability_ready
        && thread_ready
        && composer_ready)
        .then_some(DesktopStartupOutcome::Ready)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn uninitialized_client_is_not_reported_ready() {
        assert_eq!(readiness(&ClientCore::new()), None);
    }
    #[test]
    fn failed_required_stage_reports_degraded_without_starting_requests() {
        let core = ClientCore::new();
        core.update_startup_stage(
            pioneer_client::gateway::session_controller::StartupStage::Authorization,
            pioneer_client::gateway::session_controller::StartupStageState::Pending,
        );
        core.update_startup_stage(
            pioneer_client::gateway::session_controller::StartupStage::Authorization,
            pioneer_client::gateway::session_controller::StartupStageState::Failed,
        );
        assert_eq!(readiness(&core), Some(DesktopStartupOutcome::Degraded));
    }
    struct Registrar {
        active: std::rc::Rc<std::cell::Cell<usize>>,
        sink: RefCell<Option<std::sync::Weak<dyn ClientPublicationSink>>>,
    }
    impl ClientBindingRegistrar for Registrar {
        fn register(
            &self,
            _: ClientScope,
            sink: std::sync::Weak<dyn ClientPublicationSink>,
        ) -> ClientBindingRegistration {
            *self.sink.borrow_mut() = Some(sink);
            self.active.set(self.active.get() + 1);
            let active = self.active.clone();
            ClientBindingRegistration::new(move || active.set(active.get() - 1))
        }
    }
    #[gpui_kit::test]
    fn startup_completion_and_cancellation_retire_sinks_without_a_window(
        cx: &mut gpui_kit::TestAppContext,
    ) {
        use pioneer_client::gateway::session_controller::{StartupStage, StartupStageState};
        for complete in [false, true] {
            let core = Arc::new(ClientCore::new());
            if complete {
                core.update_startup_stage(StartupStage::Authorization, StartupStageState::Pending);
                core.update_startup_stage(StartupStage::Authorization, StartupStageState::Failed);
            }
            let active = std::rc::Rc::new(std::cell::Cell::new(0));
            let registrar = Arc::new(Registrar {
                active: active.clone(),
                sink: RefCell::default(),
            });
            let owner = cx.update(|cx| {
                DesktopStartupCoordinator::new(
                    DesktopStartupTrace::start(),
                    core.clone(),
                    registrar.clone(),
                    cx,
                )
            });
            assert_eq!(active.get(), 5);
            cx.run_until_parked();
            if complete {
                assert_eq!(
                    active.get(),
                    0,
                    "completed startup must not keep process-long subscriptions"
                );
            } else {
                assert_eq!(active.get(), 5);
            }
            drop(owner);
            cx.run_until_parked();
            assert_eq!(active.get(), 0);
            assert!(
                registrar
                    .sink
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .upgrade()
                    .is_none()
            );
            assert_eq!(Arc::strong_count(&core), 1);
        }
    }
}

// Diagnostic guards observe existing publications; they never advance shared
// startup stages, request work, or retain a window/feature owner.
struct StartupTelemetry {
    trace: DesktopStartupTrace,
    active: HashMap<DesktopStartupStage, pioneer_observability::DesktopStartupStageGuard>,
    completed: HashSet<DesktopStartupStage>,
}
impl StartupTelemetry {
    fn new(trace: DesktopStartupTrace) -> Self {
        let active = [
            DesktopStartupStage::GatewayRuntimeLoad,
            DesktopStartupStage::GatewaySessionConnect,
            DesktopStartupStage::AuthorizationLoad,
            DesktopStartupStage::WorkspaceLoad,
            DesktopStartupStage::ProviderLoad,
            DesktopStartupStage::ThreadTreeLoad,
            DesktopStartupStage::ThreadCapabilitiesLoad,
        ]
        .into_iter()
        .map(|stage| (stage, trace.stage(stage)))
        .collect();
        Self {
            trace,
            active,
            completed: HashSet::new(),
        }
    }
    fn observe(&mut self, core: &ClientCore) {
        use pioneer_client::gateway::session_controller::{
            SessionDiagnosticStage, StartupStage, StartupStageState,
        };
        let session = core.gateway_session();
        let nav = core.navigation_snapshot();
        let workspace = nav.workspace_id();
        let thread = nav.active_thread_id();
        let ready = [
            (DesktopStartupStage::GatewayRuntimeLoad, core.gateway_registry().is_some()),
            (DesktopStartupStage::GatewaySessionConnect, session.startup.transport_ready),
            (DesktopStartupStage::AuthorizationLoad, core.current_auth().is_some() && core.authorization_snapshot(workspace, None).is_some()),
            (DesktopStartupStage::WorkspaceLoad, workspace.is_some() && !core.workspace_catalog().is_loading()),
            (DesktopStartupStage::ProviderLoad, session.startup.stage_succeeded(StartupStage::Provider)),
            (DesktopStartupStage::ThreadTreeLoad, workspace.and_then(|w| core.workspace_tree(w)).is_some_and(|tree| !tree.is_loading()) && thread.is_some()),
            (DesktopStartupStage::ThreadCapabilitiesLoad, thread.and_then(|t| core.thread_capability_snapshot(t)).is_some_and(|p| p.request == pioneer_client::threads::capabilities::ThreadCapabilityRequestState::Ready)),
        ];
        for (stage, ready) in ready {
            if ready {
                if let Some(guard) = self.active.remove(&stage) {
                    guard.succeed();
                    self.completed.insert(stage);
                }
            }
        }
        for (stage, timing) in &session.startup.session_diagnostics {
            let target = match stage {
                SessionDiagnosticStage::ConnectAttempt => {
                    DesktopStartupStage::GatewaySessionAttempt
                }
                SessionDiagnosticStage::IdentityVerify => {
                    DesktopStartupStage::GatewaySessionIdentityVerify
                }
                _ => continue,
            };
            if let Some(duration) = timing.duration_ms {
                if self.completed.insert(target) {
                    self.trace.record_completed_stage(
                        target,
                        std::time::UNIX_EPOCH
                            + std::time::Duration::from_millis(timing.started_at_unix_ms),
                        std::time::Duration::from_millis(duration),
                        timing.state == StartupStageState::Succeeded,
                    );
                }
            }
        }
    }
    fn finish(&mut self) {
        for (_, guard) in self.active.drain() {
            guard.cancel();
        }
    }
}
impl Drop for StartupTelemetry {
    fn drop(&mut self) {
        self.finish();
    }
}
