use pioneer_client::core::{ClientPublicationReference, ClientScope};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{cell::RefCell, collections::HashMap, sync::Arc};

/// Window registrations for the selected thread and independently displayed summaries.
pub(in crate::app) struct ThreadBindings {
    registrar: Arc<dyn ClientBindingRegistrar>,
    registrations: RefCell<HashMap<ClientScope, ClientBindingRegistration>>,
    latest: RefCell<HashMap<ClientScope, ClientPublicationReference>>,
    pending: RefCell<HashMap<ClientScope, ClientPublicationReference>>,
    changed: tokio::sync::watch::Sender<u64>,
}
impl ThreadBindings {
    pub(in crate::app) fn new(registrar: Arc<dyn ClientBindingRegistrar>) -> Arc<Self> {
        Arc::new(Self {
            registrar,
            registrations: RefCell::default(),
            latest: RefCell::default(),
            pending: RefCell::default(),
            changed: tokio::sync::watch::channel(0).0,
        })
    }
    fn register(self: &Arc<Self>, scope: ClientScope) {
        if self.registrations.borrow().contains_key(&scope) {
            return;
        }
        let sink: Arc<dyn ClientPublicationSink> = self.clone();
        let registration = self
            .registrar
            .register(scope.clone(), Arc::downgrade(&sink));
        self.registrations.borrow_mut().insert(scope, registration);
    }
    pub(in crate::app) fn select(self: &Arc<Self>, id: Option<&str>) {
        let keep = |scope: &ClientScope| !matches!(scope, ClientScope::Thread { thread_id } if Some(thread_id.as_str()) != id);
        self.registrations.borrow_mut().retain(|s, _| keep(s));
        self.pending.borrow_mut().retain(|s, _| keep(s));
        self.latest.borrow_mut().retain(|s, _| keep(s));
        if let Some(id) = id {
            self.register(ClientScope::Thread {
                thread_id: id.to_owned(),
            });
        }
    }
    pub(in crate::app) fn track_summary(self: &Arc<Self>, workspace: &str, id: &str) {
        self.register(ClientScope::SidebarSummary {
            workspace_id: workspace.to_owned(),
            thread_id: id.to_owned(),
        });
    }
    pub(in crate::app) fn remove(&self, id: &str) {
        let keep = |scope: &ClientScope| !matches!(scope, ClientScope::Thread {thread_id} | ClientScope::SidebarSummary {thread_id, ..} if thread_id == id);
        self.registrations.borrow_mut().retain(|s, _| keep(s));
        self.pending.borrow_mut().retain(|s, _| keep(s));
        self.latest.borrow_mut().retain(|s, _| keep(s));
    }
    pub(in crate::app) fn clear(&self) {
        self.registrations.borrow_mut().clear();
        self.pending.borrow_mut().clear();
        self.latest.borrow_mut().clear();
    }
    pub(in crate::app) fn watch(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changed.subscribe()
    }
    pub(in crate::app) fn drain(&self) -> Vec<ClientPublicationReference> {
        self.pending.borrow_mut().drain().map(|(_, p)| p).collect()
    }
}
impl ClientPublicationSink for ThreadBindings {
    fn publish(&self, publication: ClientPublicationReference) {
        let scope = publication.scope();
        if !self.registrations.borrow().contains_key(scope) {
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
        self.latest
            .borrow_mut()
            .insert(scope.clone(), publication.clone());
        self.pending.borrow_mut().insert(scope.clone(), publication);
        self.changed.send_modify(|v| *v = v.saturating_add(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashSet, rc::Rc};
    struct Registrar(Rc<RefCell<HashSet<ClientScope>>>);
    impl ClientBindingRegistrar for Registrar {
        fn register(
            &self,
            scope: ClientScope,
            _sink: std::sync::Weak<dyn ClientPublicationSink>,
        ) -> ClientBindingRegistration {
            self.0.borrow_mut().insert(scope.clone());
            let scopes = self.0.clone();
            ClientBindingRegistration::new(move || {
                scopes.borrow_mut().remove(&scope);
            })
        }
    }
    #[test]
    fn selection_and_teardown_reject_unrelated_and_late_publications() {
        let core = Arc::new(pioneer_client::core::ClientCore::new());
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../client-ffi/tests/fixtures/thread-registry-wire.json"
        ))
        .unwrap();
        for snapshot in fixture["initial"].as_array().unwrap() {
            core.upsert_thread(
                serde_json::from_value(snapshot["payload"]["thread"].clone()).unwrap(),
            );
        }
        let scopes = Rc::new(RefCell::new(HashSet::new()));
        let binding = ThreadBindings::new(Arc::new(Registrar(scopes.clone())));
        let a = ClientScope::Thread {
            thread_id: "a".into(),
        };
        let b = ClientScope::Thread {
            thread_id: "b".into(),
        };
        binding.select(Some("a"));
        binding.publish(core.snapshot(&b).unwrap());
        assert!(binding.drain().is_empty());
        binding.publish(core.snapshot(&a).unwrap());
        assert_eq!(binding.drain().len(), 1);
        binding.select(Some("b"));
        binding.publish(core.snapshot(&a).unwrap());
        assert!(binding.drain().is_empty());
        assert!(!scopes.borrow().contains(&a));
        binding.publish(core.snapshot(&b).unwrap());
        assert_eq!(binding.drain().len(), 1);
        binding.clear();
        binding.publish(core.snapshot(&b).unwrap());
        assert!(binding.drain().is_empty());
        assert!(scopes.borrow().is_empty());
    }
}
