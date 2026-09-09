use gpui_kit::App;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderEffectIdentity {
    operation_id: pioneer_client::core::ClientOperationId,
    generation: pioneer_client::core::ClientGeneration,
}
impl ProviderEffectIdentity {
    pub(crate) fn from_plan(plan: &pioneer_client::core::ClientEffectPlan) -> Self {
        Self {
            operation_id: plan.operation_id().clone(),
            generation: plan.generation(),
        }
    }
    pub fn operation_id(&self) -> &pioneer_client::core::ClientOperationId {
        &self.operation_id
    }
    pub fn generation(&self) -> pioneer_client::core::ClientGeneration {
        self.generation
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderEffectCompletion {
    identity: ProviderEffectIdentity,
    succeeded: bool,
}
impl ProviderEffectCompletion {
    pub fn new(identity: ProviderEffectIdentity, succeeded: bool) -> Self {
        Self {
            identity,
            succeeded,
        }
    }
    pub fn identity(&self) -> &ProviderEffectIdentity {
        &self.identity
    }
    pub fn succeeded(&self) -> bool {
        self.succeeded
    }
}
pub trait ProviderCredentialPort {
    fn copy(
        &self,
        identity: ProviderEffectIdentity,
        value: &str,
        cx: &mut App,
    ) -> ProviderEffectCompletion;
}
pub trait ProviderExternalNavigationPort {
    fn open_path(
        &self,
        identity: ProviderEffectIdentity,
        uri: &str,
        cx: &mut App,
    ) -> ProviderEffectCompletion;
}
