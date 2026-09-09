use pioneer_client::core::{ClientPublicationReference, ClientScope};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{cell::RefCell, collections::HashMap, sync::Arc};

pub(crate) struct ProviderBinding {
    registrar: Arc<dyn ClientBindingRegistrar>,
    registrations: RefCell<HashMap<ClientScope, ClientBindingRegistration>>,
    latest: RefCell<HashMap<ClientScope, ClientPublicationReference>>,
    pub(crate) changed: tokio::sync::watch::Sender<u64>,
}
impl ProviderBinding {
    pub(crate) fn new(registrar: Arc<dyn ClientBindingRegistrar>) -> Arc<Self> {
        Arc::new(Self {
            registrar,
            registrations: RefCell::default(),
            latest: RefCell::default(),
            changed: tokio::sync::watch::channel(0).0,
        })
    }
    pub(crate) fn set_scopes(self: &Arc<Self>, scopes: &[ClientScope]) {
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
impl ClientPublicationSink for ProviderBinding {
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
                .expect("provider binding revision exhausted")
        });
    }
}
