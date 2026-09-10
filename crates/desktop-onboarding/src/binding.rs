use pioneer_client::core::{ClientPublicationReference, ClientScope};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{cell::RefCell, collections::HashMap, sync::Arc};
pub(crate) struct Binding {
    revisions: RefCell<HashMap<ClientScope, u64>>,
    registrations: RefCell<Vec<ClientBindingRegistration>>,
    pub changed: tokio::sync::watch::Sender<()>,
}
impl Binding {
    pub fn new(scopes: Vec<ClientScope>, registrar: &Arc<dyn ClientBindingRegistrar>) -> Arc<Self> {
        let binding = Arc::new(Self {
            revisions: RefCell::new(scopes.iter().cloned().map(|scope| (scope, 0)).collect()),
            registrations: RefCell::default(),
            changed: tokio::sync::watch::channel(()).0,
        });
        let sink: Arc<dyn ClientPublicationSink> = binding.clone();
        *binding.registrations.borrow_mut() = scopes
            .into_iter()
            .map(|scope| registrar.register(scope, Arc::downgrade(&sink)))
            .collect();
        binding
    }
}
impl ClientPublicationSink for Binding {
    fn publish(&self, value: ClientPublicationReference) {
        let mut revisions = self.revisions.borrow_mut();
        let Some(old) = revisions.get_mut(value.scope()) else {
            return;
        };
        let revision = value.revisions().scoped().get();
        if *old >= revision {
            return;
        }
        *old = revision;
        self.changed.send_replace(());
    }
}
