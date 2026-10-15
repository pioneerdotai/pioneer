use pioneer_client::core::{ClientPublicationReference, ClientScope};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    sync::Arc,
};

/// Immutable-output registrations for exactly one mounted thread.
pub(crate) struct ThreadBindings {
    registrar: Arc<dyn ClientBindingRegistrar>,
    scopes: Vec<ClientScope>,
    active: Cell<bool>,
    registrations: RefCell<Vec<ClientBindingRegistration>>,
    latest: RefCell<HashMap<ClientScope, ClientPublicationReference>>,
    pending: RefCell<HashMap<ClientScope, ClientPublicationReference>>,
    changed: tokio::sync::watch::Sender<u64>,
    timeline_changes: RefCell<Vec<Arc<pioneer_client::timeline::presentation::TimelineChangeSet>>>,
    timeline: RefCell<Option<(String, crate::timeline::TimelineRenderModel)>>,
}
impl ThreadBindings {
    pub(crate) fn new(
        registrar: Arc<dyn ClientBindingRegistrar>,
        thread_id: &str,
        initial: Vec<ClientPublicationReference>,
    ) -> Arc<Self> {
        let id = thread_id.to_owned();
        let scopes = vec![
            ClientScope::Thread {
                thread_id: id.clone(),
            },
            ClientScope::Timeline {
                thread_id: id.clone(),
            },
            ClientScope::Composer {
                thread_id: id.clone(),
            },
            ClientScope::ComposerCatalog {
                thread_id: id.clone(),
            },
            ClientScope::TurnCancellation {
                thread_id: id.clone(),
            },
            ClientScope::ThreadCapability {
                thread_id: id.clone(),
            },
            ClientScope::Artifact {
                thread_id: id.clone(),
            },
            ClientScope::ThreadMember { thread_id: id },
            ClientScope::Navigation,
            ClientScope::Administration { workspace_id: None },
            ClientScope::Session,
        ];
        Self::scoped(registrar, scopes, initial)
    }
    pub(crate) fn scoped(
        registrar: Arc<dyn ClientBindingRegistrar>,
        scopes: Vec<ClientScope>,
        initial: Vec<ClientPublicationReference>,
    ) -> Arc<Self> {
        let binding = Arc::new(Self {
            registrar,
            scopes,
            active: Cell::new(true),
            registrations: RefCell::default(),
            latest: RefCell::default(),
            pending: RefCell::default(),
            changed: tokio::sync::watch::channel(0).0,
            timeline: RefCell::default(),
            timeline_changes: RefCell::default(),
        });
        let sink: Arc<dyn ClientPublicationSink> = binding.clone();
        for scope in &binding.scopes {
            let registration = binding
                .registrar
                .register(scope.clone(), Arc::downgrade(&sink));
            binding.registrations.borrow_mut().push(registration);
        }
        for publication in initial {
            binding.publish(publication);
        }
        binding
    }
    pub(crate) fn registrar(&self) -> Arc<dyn ClientBindingRegistrar> {
        self.registrar.clone()
    }
    pub(crate) fn publication(&self, scope: &ClientScope) -> Option<ClientPublicationReference> {
        self.latest.borrow().get(scope).cloned()
    }
    pub(crate) fn take_timeline_changes(
        &self,
    ) -> Vec<Arc<pioneer_client::timeline::presentation::TimelineChangeSet>> {
        self.timeline_changes.take()
    }
    pub(crate) fn timeline_model(
        &self,
        id: Option<&str>,
    ) -> Option<crate::timeline::TimelineRenderModel> {
        self.timeline
            .borrow()
            .as_ref()
            .filter(|(thread, _)| Some(thread.as_str()) == id)
            .map(|(_, model)| model.clone())
    }
    pub(crate) fn set_active(self: &Arc<Self>, active: bool) {
        if self.active.replace(active) == active {
            return;
        }
        self.registrations.borrow_mut().clear();
        self.pending.borrow_mut().clear();
        if active {
            let sink: Arc<dyn ClientPublicationSink> = self.clone();
            for scope in &self.scopes {
                let registration = self
                    .registrar
                    .register(scope.clone(), Arc::downgrade(&sink));
                self.registrations.borrow_mut().push(registration);
            }
        }
    }
    pub(crate) fn clear(&self) {
        self.active.set(false);
        self.timeline.borrow_mut().take();
        self.timeline_changes.borrow_mut().clear();
        self.registrations.borrow_mut().clear();
        self.pending.borrow_mut().clear();
        self.latest.borrow_mut().clear();
        self.timeline.borrow_mut().take();
        self.timeline_changes.borrow_mut().clear();
    }
    pub(crate) fn watch(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changed.subscribe()
    }
    pub(crate) fn drain(&self) -> Vec<ClientPublicationReference> {
        self.pending.borrow_mut().drain().map(|(_, p)| p).collect()
    }
}
impl ClientPublicationSink for ThreadBindings {
    fn publish(&self, publication: ClientPublicationReference) {
        let scope = publication.scope();
        if !self.active.get() || !self.scopes.contains(scope) {
            return;
        }
        if self
            .latest
            .borrow()
            .get(scope)
            .is_some_and(|old| old.snapshot().sequence() >= publication.snapshot().sequence())
        {
            return;
        }
        if let ClientScope::Timeline { thread_id } = scope {
            if let Some(change) = publication.timeline_change() {
                let mut pending = self.timeline_changes.borrow_mut();
                if pending.len() == 64 {
                    pending.clear();
                }
                pending.push(change);
            } else {
                self.timeline_changes.borrow_mut().clear();
            }
            *self.timeline.borrow_mut() = publication
                .typed::<pioneer_client::timeline::presentation::TimelineSnapshot>()
                .map(|snapshot| {
                    (
                        thread_id.clone(),
                        crate::timeline::TimelineRenderModel::from_snapshot(&snapshot.payload()),
                    )
                });
        }
        self.latest
            .borrow_mut()
            .insert(scope.clone(), publication.clone());
        self.pending.borrow_mut().insert(scope.clone(), publication);
        self.changed.send_modify(|v| *v = v.saturating_add(1));
    }
}
