use super::PioneerDesktop;
use gpui::{Context, Window};
use pioneer_client::state::client_state::GatewayConnectionState;
use pioneer_observability::{
    DesktopStartupOutcome, DesktopStartupStage, DesktopStartupStageGuard, DesktopStartupTrace,
};
use std::collections::{HashMap, HashSet};

const fn operational_desktop_outcome() -> DesktopStartupOutcome {
    // Provider/model selection controls whether Composer can submit a turn;
    // it does not control whether the Desktop application itself is ready.
    DesktopStartupOutcome::Ready
}

pub(super) struct DesktopStartupCoordinator {
    trace: DesktopStartupTrace,
    active: HashMap<DesktopStartupStage, DesktopStartupStageGuard>,
    completed: HashSet<DesktopStartupStage>,
    failed: HashSet<DesktopStartupStage>,
    gateway_session_attempt: Option<DesktopStartupStageGuard>,
    gateway_session_backoff: Option<DesktopStartupStageGuard>,
    gateway_session_identity_verify: Option<DesktopStartupStageGuard>,
    frame_scheduled: bool,
    finalized: bool,
}

impl DesktopStartupCoordinator {
    pub(super) fn new(trace: DesktopStartupTrace) -> Self {
        Self {
            trace,
            active: HashMap::new(),
            completed: HashSet::new(),
            failed: HashSet::new(),
            gateway_session_attempt: None,
            gateway_session_backoff: None,
            gateway_session_identity_verify: None,
            frame_scheduled: false,
            finalized: false,
        }
    }

    pub(super) fn diagnostic_trace(&self) -> DesktopStartupTrace {
        self.trace.clone()
    }

    pub(super) fn begin(&mut self, stage: DesktopStartupStage) {
        if self.finalized || self.completed.contains(&stage) || self.active.contains_key(&stage) {
            return;
        }
        self.active.insert(stage, self.trace.stage(stage));
    }

    pub(super) fn succeed(&mut self, stage: DesktopStartupStage) {
        if let Some(guard) = self.active.remove(&stage) {
            guard.succeed();
            self.completed.insert(stage);
        }
    }

    pub(super) fn fail(&mut self, stage: DesktopStartupStage) {
        if self.active.remove(&stage).is_some() {
            self.completed.insert(stage);
            self.failed.insert(stage);
        }
    }

    fn terminal_failure_outcome(&self) -> Option<DesktopStartupOutcome> {
        if self
            .failed
            .contains(&DesktopStartupStage::AuthorizationLoad)
        {
            // The Desktop client currently exposes no stable machine code that
            // distinguishes expired credentials from transport/capability
            // failures, so do not mislabel every exhausted retry as auth.
            return Some(DesktopStartupOutcome::Degraded);
        }
        [
            DesktopStartupStage::GatewayRuntimeLoad,
            DesktopStartupStage::GatewaySessionConnect,
            DesktopStartupStage::WorkspaceLoad,
            DesktopStartupStage::ProviderLoad,
            DesktopStartupStage::ThreadTreeLoad,
            DesktopStartupStage::ActiveThreadBootstrap,
            DesktopStartupStage::ActiveThreadSubscribe,
            DesktopStartupStage::ThreadCapabilitiesLoad,
        ]
        .into_iter()
        .any(|stage| self.failed.contains(&stage))
        .then_some(DesktopStartupOutcome::Degraded)
    }

    pub(super) fn has_presented_operational_frame(&self) -> bool {
        self.finalized
    }

    fn stage_succeeded(&self, stage: DesktopStartupStage) -> bool {
        self.completed.contains(&stage) && !self.failed.contains(&stage)
    }

    fn cancel_active_stages(&mut self) {
        let active = std::mem::take(&mut self.active);
        for (stage, guard) in active {
            guard.cancel();
            self.completed.insert(stage);
        }
        self.cancel_gateway_session_diagnostics();
    }

    fn tracks_gateway_session_startup(&self) -> bool {
        !self.finalized
            && self
                .active
                .contains_key(&DesktopStartupStage::GatewaySessionConnect)
    }

    pub(super) fn gateway_session_attempt_started(&mut self) {
        if !self.tracks_gateway_session_startup() {
            return;
        }
        if let Some(backoff) = self.gateway_session_backoff.take() {
            backoff.succeed();
        }
        if self.gateway_session_attempt.is_none() {
            self.gateway_session_attempt =
                Some(self.trace.stage(DesktopStartupStage::GatewaySessionAttempt));
        }
    }

    pub(super) fn gateway_session_retry_scheduled(&mut self) {
        if !self.tracks_gateway_session_startup() {
            return;
        }
        // A retry event means the preceding attempt failed. Dropping the
        // guard deliberately records an error child span while the aggregate
        // session stage remains active and can still complete successfully.
        drop(self.gateway_session_attempt.take());
        if self.gateway_session_backoff.is_none() {
            self.gateway_session_backoff =
                Some(self.trace.stage(DesktopStartupStage::GatewaySessionBackoff));
        }
    }

    pub(super) fn gateway_session_transport_connected(&mut self) {
        if !self.tracks_gateway_session_startup() {
            return;
        }
        if let Some(backoff) = self.gateway_session_backoff.take() {
            backoff.succeed();
        }
        if let Some(attempt) = self.gateway_session_attempt.take() {
            attempt.succeed();
        }
        if self.gateway_session_identity_verify.is_none() {
            self.gateway_session_identity_verify = Some(
                self.trace
                    .stage(DesktopStartupStage::GatewaySessionIdentityVerify),
            );
        }
    }

    pub(super) fn gateway_session_transport_failed(&mut self) {
        if !self.tracks_gateway_session_startup() {
            return;
        }
        drop(self.gateway_session_attempt.take());
        if let Some(backoff) = self.gateway_session_backoff.take() {
            backoff.cancel();
        }
        drop(self.gateway_session_identity_verify.take());
    }

    pub(super) fn gateway_session_identity_verified(&mut self) {
        if let Some(stage) = self.gateway_session_identity_verify.take() {
            stage.succeed();
        }
    }

    pub(super) fn gateway_session_identity_failed(&mut self) {
        drop(self.gateway_session_identity_verify.take());
    }

    fn cancel_gateway_session_diagnostics(&mut self) {
        if let Some(stage) = self.gateway_session_attempt.take() {
            stage.cancel();
        }
        if let Some(stage) = self.gateway_session_backoff.take() {
            stage.cancel();
        }
        if let Some(stage) = self.gateway_session_identity_verify.take() {
            stage.cancel();
        }
    }

    fn schedule_finish(
        &mut self,
        outcome: DesktopStartupOutcome,
        window: &mut Window,
        cx: &mut Context<PioneerDesktop>,
    ) {
        if self.finalized || self.frame_scheduled {
            return;
        }
        if outcome == DesktopStartupOutcome::Ready && !self.active.is_empty() {
            return;
        }
        if outcome != DesktopStartupOutcome::Ready {
            // Setup/auth/degraded screens are valid terminal branches. Stages
            // that are no longer needed at that boundary are cancelled, not
            // mislabeled as failures; the actual failing stage (if any) was
            // already recorded through `fail`.
            self.cancel_active_stages();
        }
        self.begin(DesktopStartupStage::OperationalFrame);
        self.frame_scheduled = true;
        cx.on_next_frame(window, move |view, _, cx| {
            view.startup.succeed(DesktopStartupStage::OperationalFrame);
            view.startup.finalized = true;
            view.startup.trace.finish(outcome);
            pioneer_observability::schedule_observability_flush();

            // Gateway settings back controls in the title bar, but they are
            // not required to present an operational Desktop frame. Start
            // their initial fetch only after that readiness boundary so the
            // principal capability snapshot is already available.
            view.refresh_gateway_settings(cx);
        });
    }
}

impl PioneerDesktop {
    pub(super) fn reconcile_desktop_startup_readiness(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.startup.finalized || self.startup.frame_scheduled {
            return;
        }

        if self.gateway.runtime.is_some() {
            self.startup
                .succeed(DesktopStartupStage::GatewayRuntimeLoad);
        }
        if self.gateway.connection_state == GatewayConnectionState::Connected {
            self.startup
                .succeed(DesktopStartupStage::GatewaySessionConnect);
        }
        if self.gateway.current_auth.is_some() && self.gateway.capability_snapshot.is_some() {
            self.startup.succeed(DesktopStartupStage::AuthorizationLoad);
        }
        if !self.workspaces_loading {
            if self.active_workspace_id().is_some() {
                self.startup.succeed(DesktopStartupStage::WorkspaceLoad);
            } else if self.workspaces_error.is_some() {
                self.startup.fail(DesktopStartupStage::WorkspaceLoad);
            }
        }
        if !self.providers.loading() {
            if self.providers.error().is_some() {
                self.startup.fail(DesktopStartupStage::ProviderLoad);
            } else {
                self.startup.succeed(DesktopStartupStage::ProviderLoad);
            }
        }
        if !self.thread_list_loading && self.current_active_thread_id().is_some() {
            self.startup.succeed(DesktopStartupStage::ThreadTreeLoad);
        }
        let active_thread_id = self.current_active_thread_id();
        let thread_capabilities_ready = active_thread_id.is_some_and(|thread_id| {
            self.thread_scope_capabilities_thread_id.as_deref() == Some(thread_id)
                && self.thread_scope_capabilities_loading_thread_id.as_deref() != Some(thread_id)
        });
        if thread_capabilities_ready {
            self.startup
                .succeed(DesktopStartupStage::ThreadCapabilitiesLoad);
        }
        let active_thread_ready = thread_capabilities_ready
            && !self.active_thread_resubscribe_pending
            && self.composer_authorization_fingerprint.is_some();
        let providers_ready = self
            .startup
            .stage_succeeded(DesktopStartupStage::ProviderLoad);

        let outcome = if let Some(outcome) = self.startup.terminal_failure_outcome() {
            Some(outcome)
        } else if self.gateway.bootstrap_complete && self.is_gateway_setup_required() {
            Some(DesktopStartupOutcome::SetupRequired)
        } else if self.gateway.bootstrap_complete
            && !self.gateway.connecting
            && self.gateway.connection_state == GatewayConnectionState::Disconnected
        {
            Some(DesktopStartupOutcome::Degraded)
        } else {
            let initial_data_ready = self.gateway.connection_state
                == GatewayConnectionState::Connected
                && self.gateway.current_auth.is_some()
                && self.gateway.capability_snapshot.is_some()
                && self.active_workspace_id().is_some()
                && !self.workspaces_loading
                && !self.providers.loading()
                && providers_ready
                && !self.thread_list_loading
                && active_thread_ready;
            initial_data_ready.then_some(operational_desktop_outcome())
        };

        if let Some(outcome) = outcome {
            self.startup.schedule_finish(outcome, window, cx);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DesktopStartupCoordinator;
    use pioneer_observability::{
        DesktopStartupOutcome, DesktopStartupStage, DesktopStartupTrace, MobileStartupStage,
    };

    #[test]
    fn desktop_contract_does_not_reuse_mobile_stage_names() {
        assert_eq!(DesktopStartupStage::WindowOpen.as_str(), "window.open");
        assert_eq!(MobileStartupStage::NativeLaunch.as_str(), "native.launch");
        assert_ne!(
            DesktopStartupStage::GatewayRuntimeLoad.as_str(),
            MobileStartupStage::GatewayRegistryHydrate.as_str()
        );
    }

    #[test]
    fn mandatory_failures_produce_bounded_terminal_outcomes() {
        let mut startup = DesktopStartupCoordinator::new(DesktopStartupTrace::start());
        startup.begin(DesktopStartupStage::AuthorizationLoad);
        startup.fail(DesktopStartupStage::AuthorizationLoad);
        assert_eq!(
            startup.terminal_failure_outcome(),
            Some(DesktopStartupOutcome::Degraded)
        );

        let mut startup = DesktopStartupCoordinator::new(DesktopStartupTrace::start());
        startup.begin(DesktopStartupStage::WorkspaceLoad);
        startup.fail(DesktopStartupStage::WorkspaceLoad);
        assert_eq!(
            startup.terminal_failure_outcome(),
            Some(DesktopStartupOutcome::Degraded)
        );
    }

    #[test]
    fn composer_selection_is_not_part_of_desktop_readiness_outcome() {
        assert_eq!(
            super::operational_desktop_outcome(),
            DesktopStartupOutcome::Ready
        );
    }

    #[test]
    fn gateway_settings_failure_is_not_a_desktop_terminal_failure() {
        let mut startup = DesktopStartupCoordinator::new(DesktopStartupTrace::start());
        startup.begin(DesktopStartupStage::GatewaySettingsLoad);
        startup.fail(DesktopStartupStage::GatewaySettingsLoad);

        assert_eq!(startup.terminal_failure_outcome(), None);
    }

    #[test]
    fn terminal_branch_cancels_unneeded_stages_without_reporting_failure() {
        let mut startup = DesktopStartupCoordinator::new(DesktopStartupTrace::start());
        startup.begin(DesktopStartupStage::GatewaySessionConnect);

        startup.cancel_active_stages();

        assert!(
            startup
                .completed
                .contains(&DesktopStartupStage::GatewaySessionConnect)
        );
        assert!(
            !startup
                .failed
                .contains(&DesktopStartupStage::GatewaySessionConnect)
        );
    }

    #[test]
    fn gateway_session_diagnostics_follow_attempt_backoff_and_identity_phases() {
        let mut startup = DesktopStartupCoordinator::new(DesktopStartupTrace::start());
        startup.begin(DesktopStartupStage::GatewaySessionConnect);

        startup.gateway_session_attempt_started();
        assert!(startup.gateway_session_attempt.is_some());

        startup.gateway_session_retry_scheduled();
        assert!(startup.gateway_session_attempt.is_none());
        assert!(startup.gateway_session_backoff.is_some());

        startup.gateway_session_attempt_started();
        assert!(startup.gateway_session_backoff.is_none());
        assert!(startup.gateway_session_attempt.is_some());

        startup.gateway_session_transport_connected();
        assert!(startup.gateway_session_attempt.is_none());
        assert!(startup.gateway_session_identity_verify.is_some());

        startup.gateway_session_identity_verified();
        assert!(startup.gateway_session_identity_verify.is_none());
    }

    #[test]
    fn cancelling_startup_clears_gateway_session_diagnostics() {
        let mut startup = DesktopStartupCoordinator::new(DesktopStartupTrace::start());
        startup.begin(DesktopStartupStage::GatewaySessionConnect);
        startup.gateway_session_attempt_started();

        startup.cancel_active_stages();

        assert!(startup.gateway_session_attempt.is_none());
        assert!(startup.gateway_session_backoff.is_none());
        assert!(startup.gateway_session_identity_verify.is_none());
    }
}
