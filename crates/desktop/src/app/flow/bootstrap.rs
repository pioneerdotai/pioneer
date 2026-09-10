use super::*;
impl PioneerDesktop {
    pub(crate) fn bootstrap_gateway_runtime(&mut self, cx: &mut Context<Self>) {
        #[cfg(test)]
        if cx
            .global::<crate::client_runtime::DesktopRuntimeCoordinator>()
            .skip_native_startup
        {
            return;
        }
        self.startup
            .begin(pioneer_observability::DesktopStartupStage::GatewayRuntimeLoad);
        self.gateway.client_runtime.client_core().onboarding_intent(
            pioneer_client::gateway::onboarding_runtime::OnboardingIntent::Initialize,
        );
        let _ = cx;
    }
}
