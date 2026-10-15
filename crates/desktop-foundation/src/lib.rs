//! Narrow shared seam between Desktop capabilities and shell composition.
//!
//! This crate has no concrete router, feature policy, platform adapter, store,
//! or cache. A future use of GPUI `.cached(...)` remains valid only when the
//! caller supplies definite bounds and explicitly updates the element and
//! notifies its retained owner whenever any application cache-key component
//! changes. GPUI itself observes entity dirtiness, bounds, content mask, and
//! `TextStyle`; it does not observe an application-defined key tuple.

#![forbid(unsafe_code)]

mod avatar;
mod binding;
mod identity;

pub use avatar::AvatarSurface;
pub use binding::{ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink};
pub use identity::ClientIdentityRef;
pub use pioneer_client::{
    core::{ClientPublicationReference, ClientScope, ScopedPublication},
    ids::{
        ClientControlRole, ClientDomainIdentity, ClientFeature, ClientIdentity,
        ClientIdentityNamespace,
    },
};
