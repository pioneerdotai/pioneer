//! Typed native resources used by the onboarding executor.
use super::{
    timings::{GatewayTimings, GatewayWsTimings},
    types::{GatewayEndpoint, GatewayRegistry},
};
use pioneer_protocol::{AuthSecretString, ClientInstallationDescriptor};
use serde::{Deserialize, Serialize};
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingEnvironment {
    pub registry: GatewayRegistry,
    #[serde(default)]
    pub binding_journals: Vec<super::registry_recovery::GatewayBindingJournalRecord>,
    #[serde(default)]
    pub discard_unbound_remote_candidates: bool,
    pub default_remote_name: String,
    pub remote_connect_timeout_min: std::time::Duration,
    pub installation: ClientInstallationDescriptor,
    pub timings: GatewayTimings,
    pub ws_timings: GatewayWsTimings,
    pub local_provisioned: bool,
    pub local_install_required: bool,
    pub local_update_required: bool,
}
impl OnboardingEnvironment {
    pub(super) fn remote_name(&self, index: usize) -> String {
        self.default_remote_name
            .replace("{index}", &index.to_string())
    }
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OnboardingPlatformEffect {
    LoadGatewayEnvironment,
    RemoveGatewayBindingJournal {
        gateway_id: String,
    },
    PersistGatewayRegistry {
        registry: GatewayRegistry,
    },
    PrepareLocalGateway {
        endpoint: GatewayEndpoint,
        recover: bool,
    },
    CreateLocalDeviceActivation {
        endpoint_id: String,
    },
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalGatewayPreparation {
    pub endpoint: GatewayEndpoint,
    pub activation: Option<AuthSecretString>,
    pub warnings: Vec<String>,
}
impl crate::core::ClientCore {
    pub(crate) fn request_onboarding_effect(
        &self,
        effect: OnboardingPlatformEffect,
    ) -> anyhow::Result<crate::core::ClientEffectResult> {
        self.request_native_effect(effect.into())
    }
}
