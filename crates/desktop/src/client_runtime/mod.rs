mod binding_router;
mod platform_effect_router;
mod session_demand;
mod startup;

use platform_effect_router::{DesktopPlatformEffectRouter, DesktopSessionStorageAdapter};

use binding_router::DesktopClientBindingRouter;
use gpui_kit::{App, AppContext, Entity, Global, Subscription};
use pioneer_client::core::ClientCore;
use pioneer_desktop_foundation::ClientBindingRegistrar;
use std::sync::Arc;

pub(crate) struct DesktopRuntimeCoordinator {
    startup: Option<startup::DesktopStartupCoordinator>,
    core: Arc<ClientCore>,
    binding_router: Entity<DesktopClientBindingRouter>,
    registrar: Arc<dyn ClientBindingRegistrar>,
    session_demand: Option<session_demand::DesktopSessionDemand>,
    storage_adapter: Option<DesktopSessionStorageAdapter>,
    _effect_router: Arc<DesktopPlatformEffectRouter>,
    _quit: Subscription,
    quit_task: Option<gpui_kit::Task<()>>,
}

impl Global for DesktopRuntimeCoordinator {}

impl DesktopRuntimeCoordinator {
    pub(crate) fn open_window(
        options: gpui_kit::WindowOptions,
        cx: &mut gpui_kit::AsyncApp,
    ) -> anyhow::Result<(
        gpui_kit::WindowHandle<gpui_kit::component::Root>,
        gpui_kit::WeakEntity<crate::desktop_shell::DesktopShellView>,
    )> {
        use gpui_kit::AppContext;
        use gpui_kit::component::Root;
        let mut desktop = None;
        let handle = cx.open_window(options, |window, cx| {
            Self::install(cx);
            let registrar = cx.global::<Self>().registrar();
            let navigation =
                crate::desktop_navigation::DesktopNavigationStore::new(registrar.as_ref());
            Self::deliver_pending(cx);
            let layout = cx.new(|cx| crate::shell_state::ShellStateStore::new(window, cx));
            let shell = cx.new(|cx| {
                crate::desktop_shell::DesktopShellView::new(navigation, layout, window, cx)
            });
            desktop = Some(shell.downgrade());
            shell.update(cx, |shell, cx| shell.start_desktop_update(window, cx));
            cx.new(|cx| Root::new(shell, window, cx))
        })?;
        Ok((
            handle,
            desktop.expect("window construction creates its shell"),
        ))
    }

    #[cfg(test)]
    pub(crate) fn install_for_test(cx: &mut App) {
        // Keep publications on GPUI's deterministic test executor; native client
        // workers and the session storage thread belong to application startup.
        let core = Arc::new(ClientCore::new());
        let binding_router = cx.new(|cx| DesktopClientBindingRouter::new(core.clone(), cx));
        let registrar = DesktopClientBindingRouter::registrar(&binding_router, &core, cx);
        let quit = cx.on_app_quit(|_| async {});
        cx.set_global(Self {
            startup: None,
            core,
            binding_router,
            registrar,
            session_demand: None,
            storage_adapter: None,
            _effect_router: Arc::new(DesktopPlatformEffectRouter),
            _quit: quit,
            quit_task: None,
        });
    }

    pub(crate) fn install(cx: &mut App) {
        if cx.has_global::<Self>() {
            return;
        }
        let core = ClientCore::shared();
        let effect_router = Arc::new(DesktopPlatformEffectRouter);
        let storage_adapter = DesktopSessionStorageAdapter::start(core.clone(), &effect_router);
        let binding_router = cx.new(|cx| DesktopClientBindingRouter::new(core.clone(), cx));
        let registrar = DesktopClientBindingRouter::registrar(&binding_router, &core, cx);
        let session_demand =
            session_demand::DesktopSessionDemand::new(core.clone(), registrar.as_ref(), cx);
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
            owner.startup.take();
            owner.session_demand.take();
            owner
                .binding_router
                .update(cx, |router, _| router.shutdown());
            let adapter = owner.storage_adapter.take();
            let core = owner.core.clone();
            let deadline = cx
                .background_executor()
                .timer(std::time::Duration::from_millis(100));
            let cleanup = cx.background_spawn(async move {
                // OS-forced quit has GPUI's 200 ms shutdown budget. Ordinary
                // menu/window closes already passed the unbounded save barrier.
                // Keep this final attempt bounded, and report cancellation
                // without logging document contents or claiming a successful save.
                let flush = Box::pin(core.flush_agents_documents_before_close(None));
                let deadline = Box::pin(deadline);
                let saved = matches!(
                    futures_util::future::select(flush, deadline).await,
                    futures_util::future::Either::Left((Ok(()), _))
                );
                if !saved {
                    tracing::warn!(
                        "Agents document save could not finish before forced process shutdown"
                    );
                }
                core.shutdown();
                if let Some(adapter) = adapter {
                    adapter.join();
                }
            });
            async move {
                cleanup.await;
                drop(owner);
            }
        });
        let startup_core = core.clone();
        cx.set_global(Self {
            startup: None,
            core,
            binding_router,
            registrar,
            session_demand: Some(session_demand),
            storage_adapter: Some(storage_adapter),
            _effect_router: effect_router,
            _quit: quit,
            quit_task: None,
        });
        startup_core.onboarding_intent(
            pioneer_client::gateway::onboarding_runtime::OnboardingIntent::Initialize,
        );
    }

    pub(crate) fn observe_startup(trace: pioneer_observability::DesktopStartupTrace, cx: &mut App) {
        let owner = cx.global::<Self>();
        let startup =
            startup::DesktopStartupCoordinator::new(trace, owner.core(), owner.registrar(), cx);
        cx.global_mut::<Self>().startup = Some(startup);
    }

    /// Menu/keyboard quit waits before entering GPUI's bounded final shutdown.
    pub(crate) fn request_quit(cx: &mut App) {
        if !cx.has_global::<Self>() {
            cx.quit();
            return;
        }
        let owner = cx.global::<Self>();
        if owner.quit_task.is_some() {
            return;
        }
        let core = owner.core.clone();
        let task = cx.spawn(async move |cx| {
            let result = core.flush_agents_documents_before_close(None).await;
            let _ = cx.update(|cx| {
                cx.global_mut::<Self>().quit_task.take();
                match result.and_then(|_| core.agents_documents_close_status(None)) {
                    Ok(true) => cx.quit(),
                    Ok(false) => Self::request_quit(cx),
                    Err(error) => {
                        for handle in cx.windows() {
                            let _ = handle.update(cx, |root, window, cx| {
                                if let Ok(root) =
                                    root.clone().downcast::<gpui_kit::component::Root>()
                                {
                                    let content = root.read(cx).view().clone();
                                    if let Ok(shell) =
                                        content.downcast::<crate::desktop_shell::DesktopShellView>()
                                    {
                                        shell.update(cx, |shell, cx| {
                                            shell.present_document_close_error(&error, window, cx)
                                        });
                                    }
                                }
                            });
                        }
                    }
                }
            });
        });
        cx.global_mut::<Self>().quit_task = Some(task);
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
