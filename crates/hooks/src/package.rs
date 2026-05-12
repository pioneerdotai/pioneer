use crate::{
    HookHandler, HookRegistry, HookRegistryError, HookRunStore, HookRuntime, HookRuntimeOptions,
    HookSubscription, HookSubscriptionRegistry,
};
use std::sync::Arc;

pub struct HookDefinition {
    pub handler: Arc<dyn HookHandler>,
    pub subscriptions: Vec<HookSubscription>,
    pub owner: &'static str,
}

impl HookDefinition {
    pub fn new(
        handler: Arc<dyn HookHandler>,
        subscriptions: impl IntoIterator<Item = HookSubscription>,
        owner: &'static str,
    ) -> Self {
        Self {
            handler,
            subscriptions: subscriptions.into_iter().collect(),
            owner,
        }
    }
}

pub trait HookPackage: Send + Sync {
    fn id(&self) -> &'static str;
    fn definitions(&self) -> Result<Vec<HookDefinition>, HookRegistryError>;
}

pub struct HookRuntimeBuilder {
    handlers: Arc<HookRegistry>,
    subscriptions: Arc<HookSubscriptionRegistry>,
    options: HookRuntimeOptions,
    run_store: Option<Arc<dyn HookRunStore>>,
}

impl Default for HookRuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl HookRuntimeBuilder {
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(HookRegistry::new()),
            subscriptions: Arc::new(HookSubscriptionRegistry::new()),
            options: HookRuntimeOptions::default(),
            run_store: None,
        }
    }

    pub fn from_runtime(runtime: &HookRuntime) -> Self {
        Self {
            handlers: runtime.handlers().clone(),
            subscriptions: runtime.subscriptions().clone(),
            options: runtime.options().clone(),
            run_store: runtime.run_store().cloned(),
        }
    }

    pub fn with_options(mut self, options: HookRuntimeOptions) -> Self {
        self.options = options;
        self
    }

    pub fn with_run_store(mut self, run_store: Arc<dyn HookRunStore>) -> Self {
        self.run_store = Some(run_store);
        self
    }

    pub fn without_run_store(mut self) -> Self {
        self.run_store = None;
        self
    }

    pub fn install<P: HookPackage>(self, package: P) -> Result<Self, HookRegistryError> {
        self.install_package(&package)
    }

    pub fn install_package(self, package: &dyn HookPackage) -> Result<Self, HookRegistryError> {
        for definition in package.definitions()? {
            self.install_definition(definition)?;
        }
        Ok(self)
    }

    pub fn build(self) -> Arc<HookRuntime> {
        Arc::new(HookRuntime::with_options_and_optional_run_store(
            self.handlers,
            self.subscriptions,
            self.options,
            self.run_store,
        ))
    }

    fn install_definition(&self, definition: HookDefinition) -> Result<(), HookRegistryError> {
        let hook_id = definition.handler.id();
        if !self.handlers.contains_handler(&hook_id)? {
            self.handlers.register_handler(definition.handler)?;
        }

        for subscription in definition.subscriptions {
            if self
                .subscriptions
                .get_subscription(&subscription.subscription_id)?
                .is_none()
            {
                self.subscriptions
                    .register_subscription(self.handlers.as_ref(), subscription)?;
            }
        }
        Ok(())
    }
}
