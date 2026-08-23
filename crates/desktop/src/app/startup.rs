use super::PioneerDesktop;
use gpui::{Context, Window};
use pioneer_client::state::client_state::GatewayConnectionState;
use pioneer_observability::{
    DesktopStartupOutcome, DesktopStartupStage, DesktopStartupStageGuard, DesktopStartupTrace,
};
use std::collections::{HashMap, HashSet};

pub(super) struct DesktopStartupCoordinator {
    trace: DesktopStartupTrace,
    active: HashMap<DesktopStartupStage, DesktopStartupStageGuard>,
    completed: HashSet<DesktopStartupStage>,
    failed: HashSet<DesktopStartupStage>,
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
            frame_scheduled: false,
            finalized: false,
        }
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
            DesktopStartupStage::ThreadTreeLoad,
            DesktopStartupStage::ThreadCapabilitiesLoad,
            DesktopStartupStage::ActiveThreadPrepare,
        ]
        .into_iter()
        .any(|stage| self.failed.contains(&stage))
        .then_some(DesktopStartupOutcome::Degraded)
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
        self.begin(DesktopStartupStage::OperationalFrame);
        self.frame_scheduled = true;
        cx.on_next_frame(window, move |view, _, _| {
            view.startup.succeed(DesktopStartupStage::OperationalFrame);
            view.startup.finalized = true;
            view.startup.trace.finish(outcome);
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
        if !self.gateway.settings_loading
            && (self.gateway.settings.is_some() || self.gateway.settings_error.is_some())
        {
            if self.gateway.settings.is_some() {
                self.startup
                    .succeed(DesktopStartupStage::GatewaySettingsLoad);
            } else {
                self.startup.fail(DesktopStartupStage::GatewaySettingsLoad);
            }
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
        if !self.providers.cli_loading() {
            if self.providers.cli_error().is_some() {
                self.startup.fail(DesktopStartupStage::CliRuntimeLoad);
            } else {
                self.startup.succeed(DesktopStartupStage::CliRuntimeLoad);
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
        let active_thread_ready =
            thread_capabilities_ready && self.composer_authorization_fingerprint.is_some();
        if active_thread_ready {
            self.startup
                .succeed(DesktopStartupStage::ActiveThreadPrepare);
        }

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
                && !self.providers.cli_loading()
                && !self.thread_list_loading
                && active_thread_ready;
            initial_data_ready.then_some(
                if self.composer_selected_provider.is_some()
                    && self.composer_selected_model.is_some()
                {
                    DesktopStartupOutcome::Ready
                } else {
                    DesktopStartupOutcome::SetupRequired
                },
            )
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
}
