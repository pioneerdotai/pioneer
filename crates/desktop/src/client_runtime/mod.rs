mod binding_router;
#[cfg(test)]
pub(crate) use binding_router::test_binding_router;
mod platform_effect_router;

use platform_effect_router::{DesktopPlatformEffectRouter, DesktopSessionStorageAdapter};

use binding_router::DesktopClientBindingRouter;
use gpui_kit::{App, AppContext, Entity, Global, Subscription};
use pioneer_client::core::ClientCore;
use pioneer_desktop_foundation::ClientBindingRegistrar;
use std::sync::Arc;

pub(crate) struct DesktopRuntimeCoordinator {
    core: Arc<ClientCore>,
    binding_router: Entity<DesktopClientBindingRouter>,
    registrar: Arc<dyn ClientBindingRegistrar>,
    storage_adapter: Option<DesktopSessionStorageAdapter>,
    _effect_router: Arc<DesktopPlatformEffectRouter>,
    _quit: Subscription,
}

impl Global for DesktopRuntimeCoordinator {}

impl DesktopRuntimeCoordinator {
    pub(crate) fn install(cx: &mut App) {
        if cx.has_global::<Self>() {
            return;
        }
        let core = ClientCore::shared();
        let effect_router = Arc::new(DesktopPlatformEffectRouter);
        let storage_adapter = DesktopSessionStorageAdapter::start(core.clone(), &effect_router);
        let binding_router = cx.new(|cx| DesktopClientBindingRouter::new(core.clone(), cx));
        let registrar = DesktopClientBindingRouter::registrar(&binding_router, &core, cx);
        let quit = cx.on_app_quit(|cx| {
            for handle in cx.windows() {
                let _ = handle.update(cx, |root, window, cx| {
                    if let Ok(root) = root.clone().downcast::<gpui_kit::component::Root>() {
                        let content = root.read(cx).view().clone();
                        if let Ok(shell) =
                            content.downcast::<crate::desktop_shell::DesktopShellView>()
                        {
                            shell.update(cx, |shell, cx| shell.close(window, cx));
                        }
                    }
                });
            }
            let mut owner = cx.remove_global::<Self>();
            owner
                .binding_router
                .update(cx, |router, _| router.shutdown());
            owner.core.shutdown();
            let adapter = owner.storage_adapter.take();
            let cleanup = cx.background_spawn(async move {
                if let Some(adapter) = adapter {
                    adapter.join();
                }
            });
            async move {
                cleanup.await;
                drop(owner);
            }
        });
        cx.set_global(Self {
            core,
            binding_router,
            registrar,
            storage_adapter: Some(storage_adapter),
            _effect_router: effect_router,
            _quit: quit,
        });
    }

    /// Deliver already committed publications after a synchronous UI command.
    pub(crate) fn deliver_pending(cx: &App) {
        let owner = cx.global::<Self>();
        owner.binding_router.read(cx).deliver_pending(&owner.core);
    }

    pub(crate) fn core(&self) -> Arc<ClientCore> {
        self.core.clone()
    }
    pub(crate) fn registrar(&self) -> Arc<dyn ClientBindingRegistrar> {
        self.registrar.clone()
    }
}
