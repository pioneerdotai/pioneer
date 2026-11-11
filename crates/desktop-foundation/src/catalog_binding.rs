use crate::{ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink};
use pioneer_client::core::{ClientPublicationReference, ClientScope};
use std::{cell::RefCell, collections::HashMap, sync::Arc};

/// Shared catalog registrations and ordered publication wakeups for native views.
pub struct CatalogBinding {
    registrar: Arc<dyn ClientBindingRegistrar>,
    registrations: RefCell<HashMap<ClientScope, ClientBindingRegistration>>,
    latest: RefCell<HashMap<ClientScope, ClientPublicationReference>>,
    changed: tokio::sync::watch::Sender<u64>,
}
impl CatalogBinding {
    pub fn watch(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changed.subscribe()
    }

    pub fn registrar(&self) -> Arc<dyn ClientBindingRegistrar> {
        self.registrar.clone()
    }
    pub fn new(registrar: Arc<dyn ClientBindingRegistrar>) -> Arc<Self> {
        Arc::new(Self {
            registrar,
            registrations: RefCell::default(),
            latest: RefCell::default(),
            changed: tokio::sync::watch::channel(0).0,
        })
    }
    pub fn set_scopes(self: &Arc<Self>, scopes: &[ClientScope]) {
        self.registrations
            .borrow_mut()
            .retain(|scope, _| scopes.contains(scope));
        self.latest
            .borrow_mut()
            .retain(|scope, _| scopes.contains(scope));
        let sink: Arc<dyn ClientPublicationSink> = self.clone();
        for scope in scopes {
            if self.registrations.borrow().contains_key(scope) {
                continue;
            }
            let registration = self
                .registrar
                .register(scope.clone(), Arc::downgrade(&sink));
            self.registrations
                .borrow_mut()
                .insert(scope.clone(), registration);
        }
    }
}
impl ClientPublicationSink for CatalogBinding {
    fn publish(&self, publication: ClientPublicationReference) {
        let mut latest = self.latest.borrow_mut();
        if latest
            .get(publication.scope())
            .is_some_and(|old| old.snapshot().sequence() >= publication.snapshot().sequence())
        {
            return;
        }
        latest.insert(publication.scope().clone(), publication);
        self.changed.send_modify(|revision| {
            *revision = revision
                .checked_add(1)
                .expect("catalog binding revision exhausted")
        });
    }
}
