use std::sync::Weak;

use pioneer_client::core::{ClientPublicationReference, ClientScope};

/// Weakly registered destination for one immutable scoped publication stream.
pub trait ClientPublicationSink {
    fn publish(&self, publication: ClientPublicationReference);
}

/// Router-facing registration protocol. Implementations remain in the shell.
pub trait ClientBindingRegistrar {
    fn register(
        &self,
        scope: ClientScope,
        sink: Weak<dyn ClientPublicationSink>,
    ) -> ClientBindingRegistration;
}

/// Synchronous unregister action owned by a capability binding.
pub struct ClientBindingRegistration {
    unregister: Option<Box<dyn FnOnce()>>,
}

impl ClientBindingRegistration {
    pub fn new(unregister: impl FnOnce() + 'static) -> Self {
        Self {
            unregister: Some(Box::new(unregister)),
        }
    }
}

impl Drop for ClientBindingRegistration {
    fn drop(&mut self) {
        if let Some(unregister) = self.unregister.take() {
            unregister();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_client::core::{
        ClientCore, ClientMutationAuthority, ClientRevisions, ContentRevision, DomainRevision,
        PresentationRevision, ScopedRevision,
    };
    use std::{
        cell::RefCell,
        rc::Rc,
        sync::{Arc, Mutex},
    };

    #[derive(Default)]
    struct TestSink {
        revisions: Mutex<Vec<u64>>,
    }

    impl ClientPublicationSink for TestSink {
        fn publish(&self, publication: ClientPublicationReference) {
            self.revisions
                .lock()
                .unwrap()
                .push(publication.revisions().scoped().get());
        }
    }

    type RegisteredSinks = Rc<RefCell<Vec<(ClientScope, Weak<dyn ClientPublicationSink>)>>>;

    struct TestRegistrar {
        registered: RegisteredSinks,
    }

    impl TestRegistrar {
        fn deliver(&self, publication: ClientPublicationReference) {
            for (scope, target) in self.registered.borrow().iter() {
                if scope == publication.scope()
                    && let Some(target) = target.upgrade()
                {
                    target.publish(publication.clone());
                }
            }
        }
    }

    impl ClientBindingRegistrar for TestRegistrar {
        fn register(
            &self,
            scope: ClientScope,
            sink: Weak<dyn ClientPublicationSink>,
        ) -> ClientBindingRegistration {
            self.registered
                .borrow_mut()
                .push((scope.clone(), sink.clone()));
            let registered = Rc::clone(&self.registered);
            ClientBindingRegistration::new(move || {
                registered
                    .borrow_mut()
                    .retain(|(registered_scope, registered_sink)| {
                        registered_scope != &scope || !registered_sink.ptr_eq(&sink)
                    });
            })
        }
    }

    #[test]
    fn registration_delivers_to_exact_weak_target_and_drop_preserves_other_targets() {
        let core = ClientCore::new();
        let authority = ClientMutationAuthority::for_test();
        let registered = Rc::new(RefCell::new(Vec::new()));
        let registrar = TestRegistrar {
            registered: Rc::clone(&registered),
        };
        let first = Arc::new(TestSink::default());
        let second = Arc::new(TestSink::default());
        let unrelated = Arc::new(TestSink::default());
        let weak = |sink: &Arc<TestSink>| {
            Arc::downgrade(&(sink.clone() as Arc<dyn ClientPublicationSink>))
        };
        let registration = registrar.register(ClientScope::Navigation, weak(&first));
        let second_registration = registrar.register(ClientScope::Navigation, weak(&second));
        let unrelated_registration = registrar.register(ClientScope::Settings, weak(&unrelated));
        assert_eq!(Arc::strong_count(&first), 1);
        for revision in 1..=3 {
            let transition = core.publish(
                &authority,
                ClientScope::Navigation,
                ClientRevisions::new(
                    DomainRevision::new(revision),
                    PresentationRevision::new(revision),
                    ContentRevision::new(revision),
                    ScopedRevision::new(revision),
                ),
                Arc::new(revision),
                vec![],
            );
            registrar.deliver(transition.changes().publications()[0].clone());
        }
        assert_eq!(*first.revisions.lock().unwrap(), [1, 2, 3]);
        assert_eq!(*second.revisions.lock().unwrap(), [1, 2, 3]);
        assert!(unrelated.revisions.lock().unwrap().is_empty());
        drop(first);
        registrar.deliver(core.snapshot(&ClientScope::Navigation).unwrap());
        assert_eq!(*second.revisions.lock().unwrap(), [1, 2, 3, 3]);
        drop(registration);
        assert_eq!(registered.borrow().len(), 2);
        drop(second_registration);
        assert_eq!(registered.borrow().len(), 1);
        drop(unrelated_registration);
        assert!(registered.borrow().is_empty());
    }
}
