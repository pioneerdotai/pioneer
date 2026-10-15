use pioneer_client::{
    agents_doc::{
        controller::AgentsDocumentPublication, runtime::document_scope, scope::AgentsDocEditorScope,
    },
    core::ClientPublicationReference,
};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{cell::RefCell, sync::Arc};

pub(crate) struct DocumentBinding {
    scope: AgentsDocEditorScope,
    latest: RefCell<Option<Arc<AgentsDocumentPublication>>>,
    registration: RefCell<Option<ClientBindingRegistration>>,
    pub changed: tokio::sync::watch::Sender<()>,
}
impl DocumentBinding {
    pub fn new(
        scope: AgentsDocEditorScope,
        registrar: Arc<dyn ClientBindingRegistrar>,
    ) -> Arc<Self> {
        let binding = Arc::new(Self {
            scope,
            latest: RefCell::default(),
            registration: RefCell::default(),
            changed: tokio::sync::watch::channel(()).0,
        });
        let sink: Arc<dyn ClientPublicationSink> = binding.clone();
        *binding.registration.borrow_mut() =
            Some(registrar.register(document_scope(&binding.scope), Arc::downgrade(&sink)));
        binding
    }
    pub fn latest(&self) -> Option<Arc<AgentsDocumentPublication>> {
        self.latest.borrow().clone()
    }
}
impl ClientPublicationSink for DocumentBinding {
    fn publish(&self, reference: ClientPublicationReference) {
        if reference.scope() != &document_scope(&self.scope) {
            return;
        }
        let Some(next) = reference.snapshot().payload::<AgentsDocumentPublication>() else {
            return;
        };
        if next.scope() != &self.scope
            || self
                .latest
                .borrow()
                .as_ref()
                .is_some_and(|old| old.revision() >= next.revision())
        {
            return;
        }
        *self.latest.borrow_mut() = Some(next);
        self.changed.send_replace(());
    }
}
