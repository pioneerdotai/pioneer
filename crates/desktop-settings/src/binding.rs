use pioneer_client::core::{ClientPublicationReference, ClientScope};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{cell::RefCell, sync::Arc};
pub(crate) struct SettingsBinding {
    scopes: Vec<ClientScope>,
    avatar_scope: RefCell<Option<ClientScope>>,
    avatar_registration: RefCell<Option<ClientBindingRegistration>>,
    revisions: RefCell<std::collections::HashMap<ClientScope, u64>>,
    registrations: RefCell<Vec<ClientBindingRegistration>>,
    pub changed: tokio::sync::watch::Sender<()>,
}
impl SettingsBinding {
    pub fn set_avatar_scope(
        self: &Arc<Self>,
        principal: Option<&str>,
        registrar: &Arc<dyn ClientBindingRegistrar>,
    ) {
        let scope = principal.map(|principal| ClientScope::Avatar {
            principal_id: principal.into(),
        });
        if *self.avatar_scope.borrow() == scope {
            return;
        }
        *self.avatar_scope.borrow_mut() = scope.clone();
        let sink: Arc<dyn ClientPublicationSink> = self.clone();
        *self.avatar_registration.borrow_mut() =
            scope.map(|scope| registrar.register(scope, Arc::downgrade(&sink)));
    }

    pub fn new(scopes: Vec<ClientScope>, registrar: &Arc<dyn ClientBindingRegistrar>) -> Arc<Self> {
        let binding = Arc::new(Self {
            scopes: scopes.clone(),
            avatar_scope: RefCell::default(),
            avatar_registration: RefCell::default(),
            revisions: RefCell::default(),
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
impl ClientPublicationSink for SettingsBinding {
    fn publish(&self, reference: ClientPublicationReference) {
        if !self.scopes.contains(reference.scope())
            && self.avatar_scope.borrow().as_ref() != Some(reference.scope())
        {
            return;
        }
        let revision = reference.revisions().scoped().get();
        let mut revisions = self.revisions.borrow_mut();
        if revisions
            .get(reference.scope())
            .is_some_and(|old| *old >= revision)
        {
            return;
        }
        revisions.insert(reference.scope().clone(), revision);
        self.changed.send_replace(());
    }
}
