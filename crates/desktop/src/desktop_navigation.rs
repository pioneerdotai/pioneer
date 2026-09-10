//! Window route mapping over immutable Client navigation publications.
use pioneer_client::{
    core::{ClientPublicationReference, ClientScope},
    navigation::{ClientNavigationState, SemanticDestination},
};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MainRoute {
    Threads,
    AgentsDoc,
    Providers,
    Administration,
    Mcp,
    McpDetails,
    Skills,
    SkillDetails,
    Settings,
}
impl MainRoute {
    fn from_destination(destination: &SemanticDestination) -> Self {
        match destination {
            SemanticDestination::Threads => Self::Threads,
            SemanticDestination::AgentsDocument => Self::AgentsDoc,
            SemanticDestination::Providers { .. } => Self::Providers,
            SemanticDestination::Administration { .. } => Self::Administration,
            SemanticDestination::Mcp { server_id: None } => Self::Mcp,
            SemanticDestination::Mcp { server_id: Some(_) } => Self::McpDetails,
            SemanticDestination::Skills { skill_id: None } => Self::Skills,
            SemanticDestination::Skills { skill_id: Some(_) } => Self::SkillDetails,
            SemanticDestination::Settings { .. } => Self::Settings,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RouteActivity {
    Active,
    Warm,
    Dormant,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum WindowRoute {
    #[default]
    Main,
    GatewaySetup,
    InvitationJoin,
}

pub(crate) struct DesktopRouteSnapshot {
    revision: u64,
    client_revision: u64,
    window_route: WindowRoute,
    route: MainRoute,
    navigation: Arc<ClientNavigationState>,
}
impl DesktopRouteSnapshot {
    pub(crate) fn window_route(&self) -> WindowRoute {
        self.window_route
    }
    pub(crate) fn route(&self) -> MainRoute {
        self.route
    }
    pub(crate) fn navigation(&self) -> &Arc<ClientNavigationState> {
        &self.navigation
    }
}

/// A binding owns the Desktop mapping, never a writable copy of Client selection.
pub(crate) struct DesktopNavigationStore {
    snapshot: RefCell<Arc<DesktopRouteSnapshot>>,
    closed: Cell<bool>,
    registration: RefCell<Option<ClientBindingRegistration>>,
    window_history: RefCell<Vec<WindowRoute>>,
    changed: tokio::sync::watch::Sender<u64>,
}
impl DesktopNavigationStore {
    pub(crate) fn new(registrar: &dyn ClientBindingRegistrar) -> Arc<Self> {
        let store = Arc::new(Self {
            closed: Cell::new(false),
            snapshot: RefCell::new(Arc::new(DesktopRouteSnapshot {
                revision: 0,
                client_revision: 0,
                window_route: WindowRoute::Main,
                route: MainRoute::Threads,
                navigation: Arc::default(),
            })),
            registration: RefCell::default(),
            window_history: RefCell::new(vec![WindowRoute::Main]),
            changed: tokio::sync::watch::channel(0).0,
        });
        let sink: Arc<dyn ClientPublicationSink> = store.clone();
        *store.registration.borrow_mut() =
            Some(registrar.register(ClientScope::Navigation, Arc::downgrade(&sink)));
        store
    }
    pub(crate) fn snapshot(&self) -> Arc<DesktopRouteSnapshot> {
        self.snapshot.borrow().clone()
    }
    pub(crate) fn watch(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changed.subscribe()
    }
    pub(crate) fn set_window_route(&self, route: WindowRoute) {
        if self.closed.get() {
            return;
        }
        let previous = self.snapshot();
        if previous.window_route == route {
            return;
        }
        let mut history = self.window_history.borrow_mut();
        if let Some(index) = history.iter().position(|entry| *entry == route) {
            history.truncate(index + 1);
        } else {
            history.push(route);
        }
        let revision = previous.revision + 1;
        *self.snapshot.borrow_mut() = Arc::new(DesktopRouteSnapshot {
            revision,
            client_revision: previous.client_revision,
            window_route: route,
            route: previous.route,
            navigation: previous.navigation.clone(),
        });
        self.changed.send_replace(revision);
    }
    pub(crate) fn activity(&self, route: MainRoute, window_active: bool) -> RouteActivity {
        let snapshot = self.snapshot();
        if self.closed.get() || snapshot.window_route == WindowRoute::GatewaySetup {
            RouteActivity::Dormant
        } else if window_active
            && snapshot.window_route == WindowRoute::Main
            && snapshot.route == route
        {
            RouteActivity::Active
        } else {
            RouteActivity::Warm
        }
    }
    pub(crate) fn is_visible(&self, route: MainRoute) -> bool {
        let snapshot = self.snapshot();
        !self.closed.get() && snapshot.window_route == WindowRoute::Main && snapshot.route == route
    }
    pub(crate) fn close(&self) {
        self.closed.set(true);
        self.registration.borrow_mut().take();
        self.window_history.borrow_mut().clear();
    }
}
impl ClientPublicationSink for DesktopNavigationStore {
    fn publish(&self, publication: ClientPublicationReference) {
        if self.closed.get() || publication.scope() != &ClientScope::Navigation {
            return;
        }
        let Some(publication) = publication.typed::<ClientNavigationState>() else {
            return;
        };
        let revision = publication.revisions().scoped().get();
        if revision <= self.snapshot.borrow().client_revision {
            return;
        }
        let navigation = publication.payload();
        let route = MainRoute::from_destination(navigation.destination());
        let previous = self.snapshot();
        let revision_local = previous.revision + 1;
        *self.snapshot.borrow_mut() = Arc::new(DesktopRouteSnapshot {
            revision: revision_local,
            client_revision: revision,
            window_route: previous.window_route,
            route,
            navigation,
        });
        self.changed.send_replace(revision_local);
    }
}

gpui_kit::actions!(
    desktop_navigation,
    [
        OpenThreads,
        OpenProviders,
        OpenMcp,
        OpenSkills,
        OpenAdministration,
        OpenSettings
    ]
);

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_client::{core::ClientCore, navigation::NavigationIntent};
    use std::rc::Rc;
    struct Registrar(Rc<Cell<usize>>);
    impl ClientBindingRegistrar for Registrar {
        fn register(
            &self,
            scope: ClientScope,
            _: std::sync::Weak<dyn ClientPublicationSink>,
        ) -> ClientBindingRegistration {
            assert_eq!(scope, ClientScope::Navigation);
            self.0.set(self.0.get() + 1);
            let count = self.0.clone();
            ClientBindingRegistration::new(move || count.set(count.get() - 1))
        }
    }
    #[test]
    fn sidebar_and_screen_share_one_revision_and_ignore_stale_delivery() {
        let count = Rc::new(Cell::new(0));
        let store = DesktopNavigationStore::new(&Registrar(count.clone()));
        let core = ClientCore::shared();
        let initial = core.snapshot(&ClientScope::Navigation).unwrap();
        store.publish(initial.clone());
        for destination in [
            SemanticDestination::Providers {
                filter: pioneer_client::providers::selectors::ProviderFilter::Cli,
            },
            SemanticDestination::Administration {
                route: pioneer_client::navigation::AdministrationRoute::Invitations,
            },
            SemanticDestination::Mcp {
                server_id: Some("server".into()),
            },
            SemanticDestination::Skills { skill_id: None },
            SemanticDestination::Settings {
                route: pioneer_client::navigation::SettingsRoute::Account,
            },
        ] {
            core.navigate(NavigationIntent::Navigate { destination }, None);
            store.publish(core.snapshot(&ClientScope::Navigation).unwrap());
            let sidebar = store.snapshot();
            let screen = store.snapshot();
            assert!(Arc::ptr_eq(&sidebar, &screen));
            assert_eq!(sidebar.revision, screen.revision);
            store.publish(initial.clone());
            assert!(Arc::ptr_eq(&screen, &store.snapshot()));
        }
        store.close();
        assert_eq!(count.get(), 0);
        let closed = store.snapshot();
        core.navigate(NavigationIntent::Reset, None);
        store.publish(core.snapshot(&ClientScope::Navigation).unwrap());
        assert!(Arc::ptr_eq(&closed, &store.snapshot()));
    }
    #[test]
    fn window_history_and_activity_do_not_mutate_client_selection() {
        let store = DesktopNavigationStore::new(&Registrar(Rc::new(Cell::new(0))));
        let core = ClientCore::shared();
        core.activate_thread(Some("thread"), Some("workspace"));
        store.publish(core.snapshot(&ClientScope::Navigation).unwrap());
        let selection = core.navigation_snapshot();
        assert_eq!(
            store.activity(MainRoute::Threads, true),
            RouteActivity::Active
        );
        assert_eq!(
            store.activity(MainRoute::Threads, false),
            RouteActivity::Warm
        );
        assert!(store.is_visible(MainRoute::Threads));
        assert!(!store.is_visible(MainRoute::Settings));
        assert_eq!(
            store.activity(MainRoute::Settings, true),
            RouteActivity::Warm
        );
        store.set_window_route(WindowRoute::InvitationJoin);
        assert!(!store.is_visible(MainRoute::Threads));
        store.set_window_route(WindowRoute::GatewaySetup);
        assert!(!store.is_visible(MainRoute::Threads));
        assert_eq!(
            store.activity(MainRoute::Threads, true),
            RouteActivity::Dormant
        );
        store.set_window_route(WindowRoute::Main);
        assert!(store.is_visible(MainRoute::Threads));
        assert_eq!(*store.window_history.borrow(), vec![WindowRoute::Main]);
        assert!(Arc::ptr_eq(&selection, &core.navigation_snapshot()));
        store.close();
        assert!(!store.is_visible(MainRoute::Threads));
        assert_eq!(
            store.activity(MainRoute::Threads, true),
            RouteActivity::Dormant
        );
    }
}
