use super::*;

impl PioneerDesktop {
    pub(crate) fn is_gateway_setup_required(&self) -> bool {
        self.gateway
            .client_runtime
            .client_core()
            .onboarding_setup_required()
    }
}
