use pioneer_client::{
    core::{ClientPublicationReference, ClientScope, ScopedPublication},
    gateway::session_controller::GatewaySessionPublication,
};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{cell::RefCell, sync::Arc, time::Duration};

/// A window's immutable session view and its exact-scope registration lifetime.
pub(crate) struct GatewaySessionBinding {
    publication: RefCell<Option<ScopedPublication<GatewaySessionPublication>>>,
    registration: RefCell<Option<ClientBindingRegistration>>,
    changed: tokio::sync::watch::Sender<u64>,
}

impl GatewaySessionBinding {
    pub(crate) fn new(registrar: &dyn ClientBindingRegistrar) -> Arc<Self> {
        let binding = Arc::new(Self {
            publication: RefCell::default(),
            registration: RefCell::default(),
            changed: tokio::sync::watch::channel(0).0,
        });
        let sink: Arc<dyn ClientPublicationSink> = binding.clone();
        *binding.registration.borrow_mut() =
            Some(registrar.register(ClientScope::Session, Arc::downgrade(&sink)));
        binding
    }

    pub(crate) fn watch(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changed.subscribe()
    }

    pub(crate) fn publication(&self) -> Option<ScopedPublication<GatewaySessionPublication>> {
        self.publication.borrow().clone()
    }

    pub(crate) fn synchronize(&self, publication: Option<ClientPublicationReference>) {
        if let Some(publication) = publication {
            self.publish(publication);
        }
    }

    pub(crate) fn refresh_delay(&self, endpoint: &str, now: u64, leeway: u64) -> Option<Duration> {
        self.publication
            .borrow()
            .as_ref()?
            .payload()
            .refresh_delay(endpoint, now, leeway)
    }
}

impl ClientPublicationSink for GatewaySessionBinding {
    fn publish(&self, publication: ClientPublicationReference) {
        if publication.scope() != &ClientScope::Session {
            return;
        }
        let Some(publication) = publication.typed::<GatewaySessionPublication>() else {
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

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_client::{core::ClientCore, gateway::session_lifecycle::SessionLifecycleEvent};

    struct Registrar;
    impl ClientBindingRegistrar for Registrar {
        fn register(
            &self,
            _: ClientScope,
            _: std::sync::Weak<dyn ClientPublicationSink>,
        ) -> ClientBindingRegistration {
            ClientBindingRegistration::new(|| {})
        }
    }

    #[test]
    fn session_binding_wakes_only_for_new_exact_scope_revisions() {
        let core = ClientCore::new();
        let binding = GatewaySessionBinding::new(&Registrar);
        let mut updates = binding.watch();
        core.reduce_gateway_session_lifecycle(
            "synthetic",
            SessionLifecycleEvent::DeviceActivationRequired,
        );
        let first = core.snapshot(&ClientScope::Session).unwrap();
        binding.publish(first.clone());
        assert!(updates.has_changed().unwrap());
        let _ = *updates.borrow_and_update();
        binding.publish(first.clone());
        assert!(!updates.has_changed().unwrap());
        core.begin_authorization_epoch(Some(("synthetic".into(), 1)));
        binding.publish(
            core.snapshot(&ClientScope::Administration { workspace_id: None })
                .unwrap(),
        );
        assert!(!updates.has_changed().unwrap());
        core.reduce_gateway_session_lifecycle("synthetic", SessionLifecycleEvent::Suspend);
        let second = core.snapshot(&ClientScope::Session).unwrap();
        binding.publish(second.clone());
        let _ = *updates.borrow_and_update();
        binding.publish(first);
        assert!(!updates.has_changed().unwrap());
        assert_eq!(
            binding.publication().unwrap().revisions(),
            second.revisions()
        );
    }
}
