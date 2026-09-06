use pioneer_client::{
    core::{ClientPublicationReference, ClientScope, ScopedPublication},
    gateway::identity_authorization::IdentityAuthorizationPublication,
};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{cell::RefCell, sync::Arc};

/// Immutable identity input and registration owned by one Desktop window.
pub(crate) struct IdentityAuthorizationBinding {
    publication: RefCell<Option<ScopedPublication<IdentityAuthorizationPublication>>>,
    registration: RefCell<Option<ClientBindingRegistration>>,
    changed: tokio::sync::watch::Sender<u64>,
}

impl IdentityAuthorizationBinding {
    pub(crate) fn new(registrar: &dyn ClientBindingRegistrar) -> Arc<Self> {
        let binding = Arc::new(Self {
            publication: RefCell::default(),
            registration: RefCell::default(),
            changed: tokio::sync::watch::channel(0).0,
        });
        let sink: Arc<dyn ClientPublicationSink> = binding.clone();
        *binding.registration.borrow_mut() = Some(registrar.register(
            ClientScope::Administration { workspace_id: None },
            Arc::downgrade(&sink),
        ));
        binding
    }
    pub(crate) fn watch(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changed.subscribe()
    }
    pub(crate) fn publication(&self) -> Option<Arc<IdentityAuthorizationPublication>> {
        self.publication
            .borrow()
            .as_ref()
            .map(|publication| publication.payload())
    }
}
impl ClientPublicationSink for IdentityAuthorizationBinding {
    fn publish(&self, publication: ClientPublicationReference) {
        if publication.scope() != &(ClientScope::Administration { workspace_id: None }) {
            return;
        }
        let Some(publication) = publication.typed::<IdentityAuthorizationPublication>() else {
            return;
        };
        let mut current = self.publication.borrow_mut();
        if current
            .as_ref()
            .is_some_and(|current| current.revisions().scoped() >= publication.revisions().scoped())
        {
            return;
        }
        let revision = publication.revisions().scoped().get();
        *current = Some(publication);
        drop(current);
        self.changed.send_replace(revision);
    }
}
