use std::{
    cell::RefCell,
    collections::HashMap,
    num::NonZeroUsize,
    rc::{Rc, Weak as LocalWeak},
    sync::{Arc, Weak},
};

use gpui_kit::{App, Context, Entity, Task};
use pioneer_client::core::{
    ClientCore, ClientPublicationReference, ClientScope, ClientSubscription,
    ClientSubscriptionEvent,
};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use tokio::sync::Notify;

struct RegisteredTarget {
    sink: Weak<dyn ClientPublicationSink>,
}

struct ScopeRoute {
    subscription: ClientSubscription,
    targets: HashMap<u64, Rc<RegisteredTarget>>,
}

#[derive(Default)]
struct Routes {
    active: bool,
    next_id: u64,
    scopes: HashMap<ClientScope, ScopeRoute>,
    initial: Vec<(u64, ClientPublicationReference)>,
}

pub(super) struct DesktopClientBindingRouter {
    routes: Rc<RefCell<Routes>>,
    wake: Arc<Notify>,
    task: Option<Task<()>>,
}

struct Registrar {
    core: Weak<ClientCore>,
    routes: LocalWeak<RefCell<Routes>>,
    wake: Weak<Notify>,
}

impl ClientBindingRegistrar for Registrar {
    fn register(
        &self,
        scope: ClientScope,
        sink: Weak<dyn ClientPublicationSink>,
    ) -> ClientBindingRegistration {
        let (Some(core), Some(routes)) = (self.core.upgrade(), self.routes.upgrade()) else {
            return ClientBindingRegistration::new(|| {});
        };
        let id = {
            let mut routes = routes.borrow_mut();
            if !routes.active {
                return ClientBindingRegistration::new(|| {});
            }
            routes.next_id = routes
                .next_id
                .checked_add(1)
                .expect("binding registration identity exhausted");
            let id = routes.next_id;
            let route = routes
                .scopes
                .entry(scope.clone())
                .or_insert_with(|| ScopeRoute {
                    subscription: core.subscribe(scope.clone(), NonZeroUsize::new(64).unwrap()),
                    targets: HashMap::new(),
                });
            route.targets.insert(id, Rc::new(RegisteredTarget { sink }));
            if let Some(publication) = core.snapshot(&scope) {
                routes.initial.push((id, publication));
            }
            id
        };
        if let Some(wake) = self.wake.upgrade() {
            wake.notify_one();
        }
        let routes = Rc::downgrade(&routes);
        ClientBindingRegistration::new(move || {
            let Some(routes) = routes.upgrade() else {
                return;
            };
            let mut routes = routes.borrow_mut();
            if let Some(route) = routes.scopes.get_mut(&scope) {
                route.targets.remove(&id);
                if route.targets.is_empty() {
                    routes.scopes.remove(&scope);
                }
            }
            routes.initial.retain(|(target, _)| *target != id);
        })
    }
}

impl DesktopClientBindingRouter {
    pub(super) fn new(core: Arc<ClientCore>, cx: &mut Context<Self>) -> Self {
        let routes = Rc::new(RefCell::new(Routes {
            active: true,
            ..Routes::default()
        }));
        let wake = Arc::new(Notify::new());
        let task_wake = wake.clone();
        let mut publications = core.watch_publications();
        let task = cx.spawn(async move |this, cx| {
            loop {
                tokio::select! {
                    changed = publications.changed() => { if changed.is_err() { break; } },
                    _ = task_wake.notified() => {},
                }
                let Ok(deliveries) = this.update(cx, |router, _| router.drain(&core)) else {
                    break;
                };
                // No router/entity/table borrow survives a callback. A recipient
                // may synchronously drop its own or another window's registration.
                for (target, publication) in deliveries {
                    if let Some(registration) = target.upgrade() {
                        if let Some(target) = registration.sink.upgrade() {
                            target.publish(publication);
                        }
                    }
                }
            }
        });
        Self {
            routes,
            wake,
            task: Some(task),
        }
    }

    fn drain(
        &self,
        core: &ClientCore,
    ) -> Vec<(LocalWeak<RegisteredTarget>, ClientPublicationReference)> {
        let mut routes = self.routes.borrow_mut();
        if !routes.active {
            return Vec::new();
        }
        let mut deliveries = Vec::new();
        for (id, publication) in std::mem::take(&mut routes.initial) {
            if let Some(target) = routes
                .scopes
                .get(publication.scope())
                .and_then(|route| route.targets.get(&id))
            {
                deliveries.push((Rc::downgrade(target), publication));
            }
        }
        routes.scopes.retain(|_, route| {
            route
                .targets
                .retain(|_, target| target.sink.strong_count() > 0);
            while let Some(event) = route.subscription.try_next() {
                let publication = match event {
                    ClientSubscriptionEvent::Publication { publication, .. } => Some(publication),
                    ClientSubscriptionEvent::ResnapshotRequired { scope, .. } => {
                        core.snapshot(&scope)
                    }
                };
                if let Some(publication) = publication {
                    deliveries.extend(
                        route
                            .targets
                            .values()
                            .map(|target| (Rc::downgrade(target), publication.clone())),
                    );
                }
            }
            !route.targets.is_empty()
        });
        deliveries.sort_by_key(|(_, publication)| publication.snapshot().sequence());
        deliveries
    }

    pub(super) fn registrar(
        entity: &Entity<Self>,
        core: &Arc<ClientCore>,
        cx: &App,
    ) -> Arc<dyn ClientBindingRegistrar> {
        let router = entity.read(cx);
        Arc::new(Registrar {
            core: Arc::downgrade(core),
            routes: Rc::downgrade(&router.routes),
            wake: Arc::downgrade(&router.wake),
        })
    }

    pub(super) fn shutdown(&mut self) {
        let mut routes = self.routes.borrow_mut();
        routes.active = false;
        routes.initial.clear();
        routes.scopes.clear();
        self.task.take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_client::gateway::session_lifecycle::SessionLifecycleEvent;

    #[derive(Default)]
    struct Sink(RefCell<Vec<u64>>);
    impl ClientPublicationSink for Sink {
        fn publish(&self, publication: ClientPublicationReference) {
            self.0
                .borrow_mut()
                .push(publication.revisions().scoped().get());
        }
    }

    #[test]
    fn different_scope_queues_preserve_process_publication_order() {
        let core = Arc::new(ClientCore::new());
        let routes = Rc::new(RefCell::new(Routes {
            active: true,
            ..Routes::default()
        }));
        let wake = Arc::new(Notify::new());
        let registrar = Registrar {
            core: Arc::downgrade(&core),
            routes: Rc::downgrade(&routes),
            wake: Arc::downgrade(&wake),
        };
        let router = DesktopClientBindingRouter {
            routes,
            wake,
            task: None,
        };
        let session: Arc<dyn ClientPublicationSink> = Arc::new(Sink::default());
        let identity: Arc<dyn ClientPublicationSink> = Arc::new(Sink::default());
        let _session = registrar.register(ClientScope::Session, Arc::downgrade(&session));
        let _identity = registrar.register(ClientScope::Settings, Arc::downgrade(&identity));
        use pioneer_client::gateway::session_controller::{StartupStage, StartupStageState};
        core.update_startup_stage(StartupStage::GatewayRuntime, StartupStageState::Pending);
        core.request_gateway_settings().unwrap();
        core.update_startup_stage(StartupStage::GatewayRuntime, StartupStageState::Succeeded);
        core.prepare_gateway_settings_update(None).unwrap();
        let sequences: Vec<_> = router
            .drain(&core)
            .into_iter()
            .map(|(_, publication)| publication.snapshot().sequence())
            .collect();
        assert!(sequences.len() >= 4);
        assert!(sequences.windows(2).all(|pair| pair[0] <= pair[1]));
    }

    #[test]
    fn exact_scope_windows_unregister_synchronously_even_after_batch_is_collected() {
        let core = Arc::new(ClientCore::new());
        let routes = Rc::new(RefCell::new(Routes {
            active: true,
            ..Routes::default()
        }));
        let wake = Arc::new(Notify::new());
        let registrar = Registrar {
            core: Arc::downgrade(&core),
            routes: Rc::downgrade(&routes),
            wake: Arc::downgrade(&wake),
        };
        let mut router = DesktopClientBindingRouter {
            routes: routes.clone(),
            wake,
            task: None,
        };
        let first = Arc::new(Sink::default());
        let second = Arc::new(Sink::default());
        let unrelated = Arc::new(Sink::default());
        let weak =
            |sink: &Arc<Sink>| Arc::downgrade(&(sink.clone() as Arc<dyn ClientPublicationSink>));
        let first_registration = registrar.register(ClientScope::Session, weak(&first));
        let second_registration = registrar.register(ClientScope::Session, weak(&second));
        let unrelated_registration = registrar.register(ClientScope::Settings, weak(&unrelated));
        core.reduce_gateway_session_lifecycle(
            "synthetic",
            SessionLifecycleEvent::DeviceActivationRequired,
        );
        let deliveries = router.drain(&core);
        drop(first_registration);
        for (registration, publication) in deliveries {
            if let Some(registration) = registration.upgrade() {
                if let Some(target) = registration.sink.upgrade() {
                    target.publish(publication);
                }
            }
        }
        assert!(first.0.borrow().is_empty());
        assert_eq!(*second.0.borrow(), vec![1]);
        assert!(unrelated.0.borrow().is_empty());
        drop(second_registration);
        assert!(!routes.borrow().scopes.contains_key(&ClientScope::Session));
        router.shutdown();
        assert!(routes.borrow().scopes.is_empty());
        let late = registrar.register(ClientScope::Session, weak(&second));
        assert!(routes.borrow().scopes.is_empty());
        drop((late, unrelated_registration));
    }
}
