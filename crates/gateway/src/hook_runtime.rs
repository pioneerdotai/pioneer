use crate::hook_run_store::CrudHookRunStore;
use pioneer_crud::CrudStore;
use pioneer_hooks::{HookPackage, HookRegistryError, HookRuntime, HookRuntimeBuilder};
use std::sync::Arc;

pub(crate) struct GatewayHookRuntimeBuilder {
    crud_store: Arc<CrudStore>,
    inner: HookRuntimeBuilder,
}

impl GatewayHookRuntimeBuilder {
    pub(crate) fn new(crud_store: Arc<CrudStore>) -> Self {
        Self {
            crud_store,
            inner: HookRuntimeBuilder::new(),
        }
    }

    pub(crate) fn from_runtime(crud_store: Arc<CrudStore>, runtime: &HookRuntime) -> Self {
        Self {
            crud_store,
            inner: HookRuntimeBuilder::from_runtime(runtime),
        }
    }

    pub(crate) fn with_crud_run_store(self) -> Self {
        let Self { crud_store, inner } = self;
        Self {
            inner: inner.with_run_store(Arc::new(CrudHookRunStore::new(crud_store.clone()))),
            crud_store,
        }
    }

    pub(crate) fn install<P: HookPackage>(self, package: P) -> Result<Self, HookRegistryError> {
        let Self { crud_store, inner } = self;
        Ok(Self {
            crud_store,
            inner: inner.install(package)?,
        })
    }

    pub(crate) fn build(self) -> Arc<HookRuntime> {
        self.inner.build()
    }
}
