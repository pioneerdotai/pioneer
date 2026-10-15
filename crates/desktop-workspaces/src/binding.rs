use pioneer_client::core::{ClientPublicationReference, ClientScope};
use pioneer_desktop_foundation::ClientPublicationSink;
use std::{cell::RefCell, collections::HashMap};
pub(super) struct Binding {
    pub publications: RefCell<HashMap<ClientScope, ClientPublicationReference>>,
    pub changed: tokio::sync::watch::Sender<u64>,
}
impl Default for Binding {
    fn default() -> Self {
        Self {
            publications: RefCell::default(),
            changed: tokio::sync::watch::channel(0).0,
        }
    }
}
impl ClientPublicationSink for Binding {
    fn publish(&self, publication: ClientPublicationReference) {
        let mut inputs = self.publications.borrow_mut();
        if inputs
            .get(publication.scope())
            .is_some_and(|p| p.revisions().scoped() >= publication.revisions().scoped())
        {
            return;
        }
        inputs.insert(publication.scope().clone(), publication);
        self.changed.send_modify(|value| *value += 1);
    }
}
