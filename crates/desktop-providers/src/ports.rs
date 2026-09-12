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

/// Provider forms delegate the existing Desktop model dialog to shell composition.
pub struct ProviderModelPickerRequest {
    title: String,
    workspace_id: String,
    selection: pioneer_client::composer::model_selection::ModelSelectorSelection,
    on_save: std::rc::Rc<
        dyn Fn(pioneer_client::composer::model_selection::ModelSelectorSelection, &mut App) -> bool,
    >,
    on_refresh: std::rc::Rc<dyn Fn(&mut App)>,
}
impl ProviderModelPickerRequest {
    pub fn new(
        title: String,
        workspace_id: String,
        selection: pioneer_client::composer::model_selection::ModelSelectorSelection,
        on_save: std::rc::Rc<
            dyn Fn(
                pioneer_client::composer::model_selection::ModelSelectorSelection,
                &mut App,
            ) -> bool,
        >,
        on_refresh: std::rc::Rc<dyn Fn(&mut App)>,
    ) -> Self {
        Self {
            title,
            workspace_id,
            selection,
            on_save,
            on_refresh,
        }
    }
    pub fn title(&self) -> &str {
        &self.title
    }
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }
    pub fn selection(&self) -> &pioneer_client::composer::model_selection::ModelSelectorSelection {
        &self.selection
    }
    pub fn on_save(
        &self,
    ) -> std::rc::Rc<
        dyn Fn(pioneer_client::composer::model_selection::ModelSelectorSelection, &mut App) -> bool,
    > {
        self.on_save.clone()
    }
    pub fn on_refresh(&self) -> std::rc::Rc<dyn Fn(&mut App)> {
        self.on_refresh.clone()
    }
}
pub trait ProviderModelPickerPort {
    fn open(
        &self,
        request: ProviderModelPickerRequest,
        window: &mut gpui_kit::Window,
        cx: &mut App,
    );
}
