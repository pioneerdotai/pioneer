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
