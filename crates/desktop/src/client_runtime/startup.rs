//! Startup diagnostics observe Client readiness without driving feature requests.
use gpui_kit::{App, Entity, Subscription, Task, Window};
use pioneer_client::{core::ClientCore, navigation::ClientNavigationState};
use pioneer_observability::{DesktopStartupOutcome, DesktopStartupStage, DesktopStartupTrace};
use std::{cell::RefCell, collections::HashMap, rc::Rc, sync::Arc};

pub(super) struct DesktopStartupCoordinator {
    _task: Task<()>,
    #[cfg(test)]
    applications_for_test: tokio::sync::watch::Sender<u64>,
}
impl DesktopStartupCoordinator {
    pub(super) fn new(trace: DesktopStartupTrace, core: Arc<ClientCore>, cx: &mut App) -> Self {
        // Unlike feature subscriptions, this signal does not create or prolong demand.
        let mut publications = core.watch_publications();
        let (applied, mut applications) = tokio::sync::watch::channel(0u64);
        #[cfg(test)]
        let applications_for_test = applied.clone();
        let task = cx.spawn(async move |cx| {
            let mut telemetry = StartupTelemetry::new(trace.clone());
            loop {
                if core.is_stopped() {
                    break;
                }
                let mut candidate = None;
                let mut composer = None;
                // Observe the shell's mount/scope changes and the composer's own
                // application notifications, in addition to core publications.
                let subscriptions = cx.update(|cx| {
                    let mut subscriptions = Vec::new();
                    for handle in cx.windows() {
                        let _ = handle.update(cx, |_, window, cx| {
                            let Some(shell) = desktop_shell(window, cx) else {
                                return;
                            };
                            let changed = applied.clone();
                            subscriptions.push(cx.observe(&shell, move |_, _| {
                                changed.send_modify(|v| *v = v.wrapping_add(1));
                            }));
                            if let Some(thread) = shell.read(cx).startup_thread() {
                                let changed = applied.clone();
                                subscriptions.push(
                                    pioneer_desktop_thread::ThreadView::observe_composer_inputs(
                                        &thread,
                                        cx,
                                        move |_| {
                                            changed.send_modify(|v| *v = v.wrapping_add(1));
                                        },
                                    ),
                                );
                            }
                            composer = composer_outcome(&shell, cx);
                            if let Some(outcome) = readiness(&core, composer) {
                                candidate = Some((handle, FrameCandidate::new(&core, outcome)));
                            }
                        });
                    }
                    subscriptions
                });
                telemetry.observe(&core, composer);
                if let Some((handle, candidate)) = candidate {
                    let (sent, received) = tokio::sync::oneshot::channel();
                    let frame =
                        PendingFrame::new(trace.stage(DesktopStartupStage::OperationalFrame));
                    let outcome = candidate.outcome;
                    let scheduled = cx
                        .update(|cx| {
                            handle.update(cx, |_, window, _| {
                                frame.schedule(window, &core, candidate, sent);
                            })
                        })
                        .is_ok();
                    if scheduled && received.await == Ok(true) {
                        telemetry.finish();
                        trace.finish(outcome);
                        pioneer_observability::schedule_observability_flush();
                        break;
                    }
                    drop(frame);
                    // A stale frame retries on the publications/application which
                    // invalidated it. No timer or repeated frame polling is used.
                }
                let _subscriptions: Vec<Subscription> = subscriptions;
                tokio::select! {
                    changed = publications.changed() => { if changed.is_err() { break; } },
                    changed = applications.changed() => { if changed.is_err() { break; } },
                }
            }
        });
        Self {
            _task: task,
            #[cfg(test)]
            applications_for_test,
        }
    }
}

fn desktop_shell(
    window: &Window,
    cx: &App,
) -> Option<Entity<crate::desktop_shell::DesktopShellView>> {
    window
        .root::<gpui_kit::component::Root>()??
        .read(cx)
        .view()
        .clone()
        .downcast()
        .ok()
}
fn composer_outcome(
    shell: &Entity<crate::desktop_shell::DesktopShellView>,
    cx: &App,
) -> Option<DesktopStartupOutcome> {
    shell
        .read(cx)
        .startup_thread()?
        .read(cx)
        .composer_startup_outcome(cx)
}

// The task owns cancellation, including a callback queued in a surviving window.
struct PendingFrame(Rc<RefCell<Option<pioneer_observability::DesktopStartupStageGuard>>>);
impl PendingFrame {
    fn new(guard: pioneer_observability::DesktopStartupStageGuard) -> Self {
        Self(Rc::new(RefCell::new(Some(guard))))
    }
    fn schedule(
        &self,
        window: &mut Window,
        core: &Arc<ClientCore>,
        candidate: FrameCandidate,
        sent: tokio::sync::oneshot::Sender<bool>,
    ) {
        let guard = Rc::downgrade(&self.0);
        let core = Arc::downgrade(core);
        // GPUI drains next-frame callbacks before drawing. Refresh once, then
        // acknowledge that draw on the following signal; revalidate both phases.
        window.on_next_frame(move |window, cx| {
            let Some(guard) = guard.upgrade() else {
                return;
            };
            if !candidate.is_current_frame(&core, window, cx) {
                if let Some(guard) = guard.borrow_mut().take() {
                    guard.cancel();
                }
                let _ = sent.send(false);
                return;
            }
            window.refresh();
            let guard = Rc::downgrade(&guard);
            window.on_next_frame(move |window, cx| {
                let Some(guard) = guard.upgrade() else {
                    return;
                };
                let valid = candidate.is_current_frame(&core, window, cx);
                if let Some(guard) = guard.borrow_mut().take() {
                    if valid {
                        guard.succeed();
                    } else {
                        guard.cancel();
                    }
                }
                let _ = sent.send(valid);
            });
        });
    }
}
impl Drop for PendingFrame {
    fn drop(&mut self) {
        if let Some(guard) = self.0.borrow_mut().take() {
            guard.cancel();
        }
    }
}

struct FrameCandidate {
    outcome: DesktopStartupOutcome,
    navigation: Arc<ClientNavigationState>,
    navigation_revision: Option<u64>,
    composer: Option<Arc<pioneer_client::composer::store::ComposerPublication>>,
}
impl FrameCandidate {
    fn new(core: &ClientCore, outcome: DesktopStartupOutcome) -> Self {
        let navigation_revision = Self::navigation_revision(core);
        let navigation = core.navigation_snapshot();
        let composer = navigation
            .active_thread_id()
            .and_then(|id| core.composer_snapshot(id));
        Self {
            outcome,
            navigation,
            navigation_revision,
            composer,
        }
    }
    fn navigation_revision(core: &ClientCore) -> Option<u64> {
        core.snapshot(&pioneer_client::core::ClientScope::Navigation)
            .map(|p| p.revisions().scoped().get())
    }
    fn is_current_frame(
        &self,
        core: &std::sync::Weak<ClientCore>,
        window: &Window,
        cx: &App,
    ) -> bool {
        core.upgrade().is_some_and(|core| {
            let composer = desktop_shell(window, cx).and_then(|shell| composer_outcome(&shell, cx));
            self.is_current(&core, composer)
        })
    }
    fn is_current(&self, core: &ClientCore, composer: Option<DesktopStartupOutcome>) -> bool {
        !core.is_stopped()
            && self.navigation_revision == Self::navigation_revision(core)
            && match (
                &self.composer,
                self.navigation
                    .active_thread_id()
                    .and_then(|id| core.composer_snapshot(id)),
            ) {
                (Some(previous), Some(current)) => {
                    previous.has_same_submission_prerequisites(&current)
                }
                (None, None) => true,
                _ => false,
            }
            && readiness(core, composer) == Some(self.outcome)
            && self.navigation_revision == Self::navigation_revision(core)
    }
}

fn required_stage_failed(core: &ClientCore) -> bool {
    use pioneer_client::gateway::session_controller::{StartupStage, StartupStageState};
    // Provider is the demand-driven full catalog, not an operational dependency.
    // Keep the shared milestone contract intact for other consumers.
    core.gateway_session()
        .startup
        .stages
        .iter()
        .any(|(stage, state)| {
            !matches!(stage, StartupStage::Provider | StartupStage::ThreadTree)
                && *state == StartupStageState::Failed
        })
}
fn session_terminal_outcome(
    state: &pioneer_client::gateway::session_lifecycle::SessionLifecycleState,
) -> Option<DesktopStartupOutcome> {
    use pioneer_client::gateway::session_lifecycle::{
        SessionLifecycleState, SessionTerminalReason,
    };
    match state {
        SessionLifecycleState::NoSession | SessionLifecycleState::NeedsDeviceActivation => {
            Some(DesktopStartupOutcome::AuthenticationRequired)
        }
        SessionLifecycleState::Terminal { reason, .. } => Some(match reason {
            SessionTerminalReason::SecureStorageFailed
            | SessionTerminalReason::GatewayIdentityMismatch
            | SessionTerminalReason::RefreshOutcomeUnknown => DesktopStartupOutcome::Degraded,
            _ => DesktopStartupOutcome::AuthenticationRequired,
        }),
        _ => None,
    }
}

fn capability_read_failed(
    reads: &[pioneer_client::gateway::identity_authorization::CapabilityReadState],
    workspace: &str,
) -> bool {
    reads.iter().any(|read| {
        read.thread_id.is_none()
            && read
                .workspace_id
                .as_deref()
                .is_none_or(|id| id == workspace)
            && !read.loading
            && read.error.is_some()
    })
}

// The selected-directory read restores an absent active thread. Once a thread
// in this workspace exists, a failed background refresh cannot revoke composer
// readiness. Directory action errors live separately in `actions()`.
fn thread_tree_state(
    core: &ClientCore,
    workspace: &str,
) -> pioneer_client::gateway::session_controller::StartupStageState {
    use pioneer_client::gateway::session_controller::StartupStageState;
    let Some(tree) = core
        .workspace_tree(workspace)
        .filter(|p| p.snapshot().workspace_id == workspace)
    else {
        return StartupStageState::Pending;
    };
    if tree.is_loading() {
        return StartupStageState::Pending;
    }
    if tree.error().is_none() {
        return StartupStageState::Succeeded;
    }
    let restored = core
        .navigation_snapshot()
        .active_thread_id()
        .and_then(|thread| core.thread_snapshot(thread))
        .is_some_and(|thread| thread.coordinator().workspace_id == workspace);
    if restored {
        StartupStageState::Cancelled
    } else {
        StartupStageState::Failed
    }
}

fn readiness(
    core: &ClientCore,
    composer: Option<DesktopStartupOutcome>,
) -> Option<DesktopStartupOutcome> {
    use pioneer_client::{
        state::client_state::GatewayConnectionState,
        threads::capabilities::ThreadCapabilityRequestState,
    };
    let session = core.gateway_session();
    if required_stage_failed(core) {
        return Some(DesktopStartupOutcome::Degraded);
    }
    if !core.onboarding_busy() {
        if let Some(outcome) = core
            .active_gateway_endpoint()
            .as_ref()
            .and_then(|endpoint| session.session(&endpoint.id))
            .and_then(session_terminal_outcome)
        {
            return Some(outcome);
        }
    }
    if core.gateway_registry().is_none() || core.onboarding_loading() {
        return None;
    }
    if core.onboarding_setup_required() {
        return Some(DesktopStartupOutcome::SetupRequired);
    }
    if !core.onboarding_busy()
        && session
            .status
            .as_ref()
            .is_some_and(|status| status.connection_state == GatewayConnectionState::Disconnected)
    {
        return Some(DesktopStartupOutcome::Degraded);
    }
    if !session.startup.transport_ready
        || session.startup.identity_pending
        || !session
            .status
            .as_ref()
            .is_some_and(|s| s.connection_state == GatewayConnectionState::Connected)
    {
        return None;
    }
    if core.current_auth().is_none() {
        return Some(DesktopStartupOutcome::AuthenticationRequired);
    }
    let navigation = core.navigation_snapshot();
    let workspace = match navigation.workspace_id() {
        Some(workspace) => workspace,
        None => {
            let catalog = core.workspace_catalog();
            if catalog.is_loading() {
                return None;
            }
            if catalog.error().is_some() {
                return Some(DesktopStartupOutcome::Degraded);
            }
            return catalog
                .bootstrapped_connection_id
                .is_some()
                .then_some(DesktopStartupOutcome::SetupRequired);
        }
    };
    if thread_tree_state(core, workspace)
        == pioneer_client::gateway::session_controller::StartupStageState::Failed
    {
        return Some(DesktopStartupOutcome::Degraded);
    }
    let authorization = match core.authorization_snapshot(Some(workspace), None) {
        Some(authorization) => authorization,
        None => {
            let failed = core.snapshot(&pioneer_client::core::ClientScope::Administration { workspace_id: None })
                .and_then(|p| p.typed::<pioneer_client::gateway::identity_authorization::IdentityAuthorizationPublication>())
                .is_some_and(|p| capability_read_failed(&p.payload().capability_reads, workspace));
            return failed.then_some(DesktopStartupOutcome::Degraded);
        }
    };
    let can_create = authorization
        .workspace
        .as_ref()
        .is_some_and(|p| p.capabilities.can_create_thread);
    let thread = match navigation.active_thread_id() {
        Some(thread) => thread,
        None => {
            return (!can_create
                && core
                    .workspace_tree(workspace)
                    .is_some_and(|p| !p.is_loading()))
            .then_some(DesktopStartupOutcome::Degraded);
        }
    };
    let domain = match core.thread_snapshot(thread) {
        Some(domain) => domain,
        None => {
            return (!can_create
                && core.thread_start_snapshot().pending_thread_id.as_deref() == Some(thread))
            .then_some(DesktopStartupOutcome::Degraded);
        }
    };
    if domain.coordinator().workspace_id != workspace {
        return None;
    }
    if domain.subscription_failed() {
        return Some(DesktopStartupOutcome::Degraded);
    }
    if domain.coordinator().history_loading {
        return None;
    }
    // A reserved draft uses workspace create permission; no thread-scoped RPC
    // capability or existing history is required to compose its first message.
    if core.thread_workspace_draft(workspace).as_deref() != Some(thread) {
        let capabilities = core.thread_capability_snapshot(thread)?;
        if capabilities.workspace_id != workspace {
            return None;
        }
        match capabilities.request {
            ThreadCapabilityRequestState::Ready => {}
            ThreadCapabilityRequestState::Failed { .. }
            | ThreadCapabilityRequestState::Cancelled => {
                return Some(DesktopStartupOutcome::Degraded);
            }
            _ => return None,
        }
    }
    if core.workspace_catalog().is_loading()
        || thread_tree_state(core, workspace)
            == pioneer_client::gateway::session_controller::StartupStageState::Pending
    {
        return None;
    }
    composer
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_client::{
        gateway::session_controller::{StartupStage, StartupStageState},
        navigation::NavigationIntent,
        providers::store::ProviderCollectionKey,
    };

    #[test]
    fn catalog_milestones_never_fail_or_complete_startup_or_request_catalog() {
        for state in [
            None,
            Some(StartupStageState::Pending),
            Some(StartupStageState::Failed),
            Some(StartupStageState::Succeeded),
        ] {
            let core = ClientCore::new();
            if let Some(state) = state {
                core.update_startup_stage(StartupStage::Provider, StartupStageState::Pending);
                core.update_startup_stage(StartupStage::Provider, state);
            }
            assert!(!required_stage_failed(&core));
            for composer in [None, Some(DesktopStartupOutcome::Ready)] {
                assert_eq!(
                    readiness(&core, composer),
                    None,
                    "catalog alone cannot initialize a client"
                );
            }
            let mut telemetry = StartupTelemetry::new(DesktopStartupTrace::start());
            telemetry.observe(&core, Some(DesktopStartupOutcome::Ready));
            assert!(
                !telemetry
                    .active
                    .contains_key(&DesktopStartupStage::ProviderLoad)
            );
            assert!(
                !telemetry
                    .completed
                    .contains_key(&DesktopStartupStage::ProviderLoad)
            );
            assert!(
                core.provider_collection_snapshot(&ProviderCollectionKey::catalog("workspace"))
                    .is_none()
            );
        }
    }

    #[test]
    fn missing_authorization_finishes_as_auth_required_and_storage_errors_stay_degraded() {
        use pioneer_client::gateway::session_lifecycle::{
            SessionLifecycleState, SessionTerminalReason,
        };
        for state in [
            SessionLifecycleState::NoSession,
            SessionLifecycleState::NeedsDeviceActivation,
        ] {
            assert_eq!(
                session_terminal_outcome(&state),
                Some(DesktopStartupOutcome::AuthenticationRequired)
            );
        }
        assert_eq!(
            session_terminal_outcome(&SessionLifecycleState::Terminal {
                metadata: None,
                reason: SessionTerminalReason::SecureStorageFailed
            }),
            Some(DesktopStartupOutcome::Degraded)
        );
    }

    #[test]
    fn failed_authorization_read_is_terminal_only_for_the_required_scope() {
        use pioneer_client::gateway::identity_authorization::CapabilityReadState;
        let mut read = CapabilityReadState {
            workspace_id: Some("workspace".into()),
            thread_id: None,
            loading: false,
            error: Some("synthetic".into()),
        };
        assert!(capability_read_failed(&[read.clone()], "workspace"));
        assert!(!capability_read_failed(&[read.clone()], "other"));
        read.loading = true;
        assert!(!capability_read_failed(&[read], "workspace"));
    }

    #[test]
    fn required_error_survives_optional_catalog_failure() {
        let core = ClientCore::new();
        for stage in [StartupStage::Provider, StartupStage::Authorization] {
            core.update_startup_stage(stage, StartupStageState::Pending);
            core.update_startup_stage(stage, StartupStageState::Failed);
        }
        assert_eq!(
            readiness(&core, Some(DesktopStartupOutcome::Ready)),
            Some(DesktopStartupOutcome::Degraded)
        );
    }

    #[test]
    fn terminal_outcome_does_not_require_a_navigation_publication() {
        let core = ClientCore::new();
        core.update_startup_stage(StartupStage::Authorization, StartupStageState::Pending);
        core.update_startup_stage(StartupStage::Authorization, StartupStageState::Failed);
        let candidate = FrameCandidate::new(&core, DesktopStartupOutcome::Degraded);
        assert!(candidate.is_current(&core, None));
    }

    #[test]
    fn frame_revalidates_scope_even_after_navigation_returns_to_original_thread() {
        let core = ClientCore::new();
        core.update_startup_stage(StartupStage::Authorization, StartupStageState::Pending);
        core.update_startup_stage(StartupStage::Authorization, StartupStageState::Failed);
        let select = |workspace: &str, thread: &str| {
            core.navigate(
                NavigationIntent::SelectThread {
                    workspace_id: Some(workspace.into()),
                    thread_id: Some(thread.into()),
                },
                None,
            );
        };
        select("workspace", "a");
        let candidate = FrameCandidate::new(&core, DesktopStartupOutcome::Degraded);
        assert!(candidate.is_current(&core, None));
        select("other", "b");
        assert!(!candidate.is_current(&core, None));
        select("workspace", "a");
        assert!(!candidate.is_current(&core, None));
    }

    #[gpui_kit::test]
    fn passive_observer_cancellation_releases_core_without_feature_subscriptions(
        cx: &mut gpui_kit::TestAppContext,
    ) {
        let core = Arc::new(ClientCore::new());
        let owner = cx.update(|cx| {
            DesktopStartupCoordinator::new(DesktopStartupTrace::start(), core.clone(), cx)
        });
        cx.run_until_parked();
        assert!(
            core.provider_collection_snapshot(&ProviderCollectionKey::catalog("workspace"))
                .is_none()
        );
        drop(owner);
        cx.run_until_parked();
        assert_eq!(Arc::strong_count(&core), 1);
    }

    struct FrameHost;
    impl gpui_kit::Render for FrameHost {
        fn render(
            &mut self,
            _: &mut Window,
            _: &mut gpui_kit::Context<Self>,
        ) -> impl gpui_kit::IntoElement {
            gpui_kit::div()
        }
    }

    #[gpui_kit::test]
    fn frame_callbacks_revalidate_scope_and_acknowledge_once(cx: &mut gpui_kit::TestAppContext) {
        use tokio::sync::oneshot::error::TryRecvError;
        let (_host, window_cx) = cx.add_window_view(|_, _| FrameHost);
        for invalidate in [false, true] {
            let core = Arc::new(ClientCore::new());
            core.initialize_navigation();
            core.update_startup_stage(StartupStage::Authorization, StartupStageState::Pending);
            core.update_startup_stage(StartupStage::Authorization, StartupStageState::Failed);
            let candidate = FrameCandidate::new(&core, DesktopStartupOutcome::Degraded);
            let pending = PendingFrame::new(
                DesktopStartupTrace::start().stage(DesktopStartupStage::OperationalFrame),
            );
            let (sent, mut received) = tokio::sync::oneshot::channel();
            window_cx.update(|window, _| pending.schedule(window, &core, candidate, sent));
            assert_eq!(received.try_recv(), Err(TryRecvError::Empty));
            window_cx.update(|window, cx| assert_eq!(window.simulate_next_frame(cx), 1));
            assert_eq!(
                received.try_recv(),
                Err(TryRecvError::Empty),
                "pre-draw callback cannot finish startup"
            );
            if invalidate {
                core.navigate(
                    NavigationIntent::SelectWorkspace {
                        workspace_id: Some("other".into()),
                    },
                    None,
                );
            }
            window_cx.update(|window, cx| assert_eq!(window.simulate_next_frame(cx), 1));
            assert_eq!(received.try_recv(), Ok(!invalidate));
            assert!(pending.0.borrow().is_none());
            window_cx.update(|window, cx| assert_eq!(window.simulate_next_frame(cx), 0));
        }
    }

    #[gpui_kit::test]
    fn cancellation_retires_an_already_queued_frame(cx: &mut gpui_kit::TestAppContext) {
        let (_host, window_cx) = cx.add_window_view(|_, _| FrameHost);
        let core = Arc::new(ClientCore::new());
        core.initialize_navigation();
        let pending = PendingFrame::new(
            DesktopStartupTrace::start().stage(DesktopStartupStage::OperationalFrame),
        );
        let (sent, mut received) = tokio::sync::oneshot::channel();
        window_cx.update(|window, _| {
            pending.schedule(
                window,
                &core,
                FrameCandidate::new(&core, DesktopStartupOutcome::Ready),
                sent,
            )
        });
        drop(pending);
        window_cx.update(|window, cx| {
            window.simulate_next_frame(cx);
        });
        assert_eq!(
            received.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Closed)
        );
        assert_eq!(Arc::strong_count(&core), 1);
    }
}

// Diagnostic guards observe existing publications; they never advance shared
// startup stages, request work, or retain a window/feature owner.
struct StartupTelemetry {
    trace: DesktopStartupTrace,
    active: HashMap<DesktopStartupStage, pioneer_observability::DesktopStartupStageGuard>,
    completed: HashMap<
        DesktopStartupStage,
        pioneer_client::gateway::session_controller::StartupStageState,
    >,
    tree_workspace: Option<String>,
    composer_scope: Option<(
        String,
        String,
        Option<pioneer_client::composer::store::DraftId>,
    )>,
    runtime_key: Option<(
        String,
        String,
        pioneer_client::composer::store::DraftId,
        Option<String>,
    )>,
}
impl StartupTelemetry {
    fn new(trace: DesktopStartupTrace) -> Self {
        let active = [
            DesktopStartupStage::GatewayRuntimeLoad,
            DesktopStartupStage::GatewaySessionConnect,
            DesktopStartupStage::AuthorizationLoad,
            DesktopStartupStage::WorkspaceLoad,
            DesktopStartupStage::ComposerPrepare,
            DesktopStartupStage::ThreadTreeLoad,
            DesktopStartupStage::ThreadCapabilitiesLoad,
        ]
        .into_iter()
        .map(|stage| (stage, trace.stage(stage)))
        .collect();
        Self {
            trace,
            active,
            completed: HashMap::new(),
            tree_workspace: None,
            composer_scope: None,
            runtime_key: None,
        }
    }
    fn observe(&mut self, core: &ClientCore, composer: Option<DesktopStartupOutcome>) {
        use pioneer_client::gateway::session_controller::{
            SessionDiagnosticStage, StartupStage, StartupStageState,
        };
        let session = core.gateway_session();
        let nav = core.navigation_snapshot();
        let workspace = nav.workspace_id();
        let thread = nav.active_thread_id();
        if self.tree_workspace.as_deref() != workspace {
            let stage = DesktopStartupStage::ThreadTreeLoad;
            if self.tree_workspace.is_some() {
                if let Some(guard) = self.active.remove(&stage) {
                    guard.cancel();
                }
                self.completed.remove(&stage);
            }
            if workspace.is_some() {
                self.active
                    .entry(stage)
                    .or_insert_with(|| self.trace.stage(stage));
            }
            self.tree_workspace = workspace.map(str::to_owned);
        }
        if let Some(workspace) = workspace {
            match thread_tree_state(core, workspace) {
                StartupStageState::Succeeded => {
                    self.complete(DesktopStartupStage::ThreadTreeLoad, Some(true))
                }
                StartupStageState::Failed => {
                    self.complete(DesktopStartupStage::ThreadTreeLoad, Some(false))
                }
                StartupStageState::Cancelled => {
                    self.complete(DesktopStartupStage::ThreadTreeLoad, None)
                }
                StartupStageState::Pending => {}
            }
        }
        let composer_scope = workspace.zip(thread).map(|(workspace, thread)| {
            (
                workspace.to_owned(),
                thread.to_owned(),
                core.composer_snapshot(thread).map(|p| p.draft_id()),
            )
        });
        if self.composer_scope != composer_scope {
            let stage = DesktopStartupStage::ComposerPrepare;
            if self.composer_scope.is_some() {
                if let Some(guard) = self.active.remove(&stage) {
                    guard.cancel();
                }
            }
            if composer_scope.is_some() {
                self.active
                    .entry(stage)
                    .or_insert_with(|| self.trace.stage(stage));
            }
            self.completed.remove(&stage);
            self.composer_scope = composer_scope;
        }
        if workspace.is_some_and(|workspace| {
            core.authorization_snapshot(Some(workspace), None).is_none()
                && core.snapshot(&pioneer_client::core::ClientScope::Administration { workspace_id: None })
                    .and_then(|p| p.typed::<pioneer_client::gateway::identity_authorization::IdentityAuthorizationPublication>())
                    .is_some_and(|p| capability_read_failed(&p.payload().capability_reads, workspace))
        }) {
            self.complete(DesktopStartupStage::AuthorizationLoad, Some(false));
        }
        if thread.and_then(|id| core.thread_capability_snapshot(id)).is_some_and(|p| {
            matches!(p.request, pioneer_client::threads::capabilities::ThreadCapabilityRequestState::Failed { .. })
                && Some(p.workspace_id.as_str()) == workspace
        }) {
            self.complete(DesktopStartupStage::ThreadCapabilitiesLoad, Some(false));
        }
        for (source, target) in [
            (
                StartupStage::GatewayRuntime,
                DesktopStartupStage::GatewayRuntimeLoad,
            ),
            (
                StartupStage::GatewaySession,
                DesktopStartupStage::GatewaySessionConnect,
            ),
            (
                StartupStage::Authorization,
                DesktopStartupStage::AuthorizationLoad,
            ),
            (StartupStage::Workspace, DesktopStartupStage::WorkspaceLoad),
            (
                StartupStage::ThreadCapabilities,
                DesktopStartupStage::ThreadCapabilitiesLoad,
            ),
        ] {
            if session.startup.stages.get(&source) == Some(&StartupStageState::Failed) {
                self.complete(target, Some(false));
            }
        }
        let runtime = thread.and_then(|id| core.composer_snapshot(id)).filter(|p| {
            p.domain().selected_mode != pioneer_client::timeline::types::ThreadMode::Message
                && p.domain().selected_provider.as_deref().and_then(pioneer_client::providers::list::runtime_id_from_cli_runtime_provider_key).is_some()
        });
        let runtime_key = runtime.as_ref().map(|p| {
            (
                workspace.unwrap_or_default().to_owned(),
                p.thread_id().to_owned(),
                p.draft_id(),
                p.domain().selected_provider.clone(),
            )
        });
        if self.runtime_key != runtime_key {
            if let Some(guard) = self
                .active
                .remove(&DesktopStartupStage::ComposerRuntimeWait)
            {
                guard.cancel();
            }
            self.runtime_key = runtime_key;
            self.completed
                .remove(&DesktopStartupStage::ComposerRuntimeWait);
        }
        if let Some(runtime) = runtime {
            use pioneer_client::composer::catalog::ComposerCatalogRequestState;
            let stage = DesktopStartupStage::ComposerRuntimeWait;
            let selected = runtime.runtime_selection().filter(|p| {
                p.identity.draft_id == runtime.draft_id()
                    && p.selected_provider == runtime.domain().selected_provider
                    && Some(p.workspace_id.as_str()) == workspace
            });
            let selected_ready = runtime.selected_provider_ready() && selected.is_some();
            if !selected_ready && !self.completed.contains_key(&stage) {
                self.active
                    .entry(stage)
                    .or_insert_with(|| self.trace.stage(stage));
            }
            if selected_ready {
                if composer == Some(DesktopStartupOutcome::Ready) {
                    self.complete(stage, Some(true));
                }
            } else if selected.is_some_and(|p| {
                matches!(p.request.state, ComposerCatalogRequestState::Failed { .. })
            }) {
                self.complete(stage, Some(false));
            } else if selected
                .is_some_and(|p| matches!(p.request.state, ComposerCatalogRequestState::Cancelled))
            {
                self.complete(stage, None);
            }
        }
        match composer {
            Some(DesktopStartupOutcome::Ready) => {
                self.complete(DesktopStartupStage::ComposerPrepare, Some(true))
            }
            Some(DesktopStartupOutcome::Degraded) => {
                self.complete(DesktopStartupStage::ComposerPrepare, Some(false))
            }
            Some(_) => self.complete(DesktopStartupStage::ComposerPrepare, None),
            None => {}
        }
        let ready = [
            (DesktopStartupStage::GatewayRuntimeLoad, core.gateway_registry().is_some()),
            (DesktopStartupStage::GatewaySessionConnect, session.startup.transport_ready),
            (DesktopStartupStage::AuthorizationLoad, core.current_auth().is_some() && core.authorization_snapshot(workspace, None).is_some()),
            (DesktopStartupStage::WorkspaceLoad, workspace.is_some() && !core.workspace_catalog().is_loading()),
            (DesktopStartupStage::ThreadCapabilitiesLoad, thread.and_then(|t| core.thread_capability_snapshot(t)).is_some_and(|p| p.request == pioneer_client::threads::capabilities::ThreadCapabilityRequestState::Ready)),
        ];
        for (stage, ready) in ready {
            if ready {
                if let Some(guard) = self.active.remove(&stage) {
                    guard.succeed();
                    self.completed.insert(stage, StartupStageState::Succeeded);
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
                SessionDiagnosticStage::CredentialsLoad => {
                    DesktopStartupStage::GatewaySessionCredentialsLoad
                }
                SessionDiagnosticStage::RefreshIntentPersist => {
                    DesktopStartupStage::GatewaySessionRefreshIntentPersist
                }
                SessionDiagnosticStage::RefreshRequest => {
                    DesktopStartupStage::GatewaySessionRefreshRequest
                }
                SessionDiagnosticStage::CredentialsPersist => {
                    DesktopStartupStage::GatewaySessionCredentialsPersist
                }
            };
            if let Some(duration) = timing.duration_ms {
                if !self.completed.contains_key(&target) {
                    self.completed.insert(target, timing.state);
                    self.trace.record_session_diagnostic(
                        target,
                        std::time::UNIX_EPOCH
                            + std::time::Duration::from_millis(timing.started_at_unix_ms),
                        std::time::Duration::from_millis(duration),
                        timing.state == StartupStageState::Succeeded,
                        timing.state == StartupStageState::Cancelled,
                    );
                }
            }
        }
    }
    fn complete(&mut self, stage: DesktopStartupStage, succeeded: Option<bool>) {
        if let Some(guard) = self.active.remove(&stage) {
            match succeeded {
                Some(true) => guard.succeed(),
                Some(false) => drop(guard),
                None => guard.cancel(),
            }
            self.completed.insert(
                stage,
                match succeeded {
                    Some(true) => {
                        pioneer_client::gateway::session_controller::StartupStageState::Succeeded
                    }
                    Some(false) => {
                        pioneer_client::gateway::session_controller::StartupStageState::Failed
                    }
                    None => {
                        pioneer_client::gateway::session_controller::StartupStageState::Cancelled
                    }
                },
            );
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

#[cfg(test)]
mod review_tests;
