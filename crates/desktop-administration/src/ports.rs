use gpui_kit::{App, Task};
use pioneer_client::avatars::{AvatarCacheError, AvatarCacheRequest, AvatarCacheResult};
use tokio_util::sync::CancellationToken;

pub trait AdministrationAvatarPort {
    fn resolve(
        &self,
        request: AvatarCacheRequest,
        cancellation: CancellationToken,
        cx: &mut App,
    ) -> Task<Result<AvatarCacheResult, AvatarCacheError>>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdministrationEffectIdentity {
    operation_id: pioneer_client::core::ClientOperationId,
    generation: pioneer_client::core::ClientGeneration,
}
impl AdministrationEffectIdentity {
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
pub struct AdministrationEffectCompletion {
    identity: AdministrationEffectIdentity,
    succeeded: bool,
}
impl AdministrationEffectCompletion {
    pub fn new(identity: AdministrationEffectIdentity, succeeded: bool) -> Self {
        Self {
            identity,
            succeeded,
        }
    }
    pub fn identity(&self) -> &AdministrationEffectIdentity {
        &self.identity
    }
    pub fn succeeded(&self) -> bool {
        self.succeeded
    }
}
pub trait AdministrationActivationPort {
    fn copy(
        &self,
        identity: AdministrationEffectIdentity,
        value: &str,
        cx: &mut App,
    ) -> AdministrationEffectCompletion;
}
pub trait AdministrationExternalNavigationPort {
    fn open(
        &self,
        identity: AdministrationEffectIdentity,
        uri: &str,
        cx: &mut App,
    ) -> AdministrationEffectCompletion;
}
