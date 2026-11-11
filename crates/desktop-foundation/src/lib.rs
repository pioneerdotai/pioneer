//! Narrow shared seam between Desktop capabilities and shell composition.
//!
//! This crate has no concrete router, feature policy, platform adapter, store,
//! or cache. PackageUploadView retains only shared Client delivery presentation
//! and a delivery lease; feature adapters supply localized copy. CatalogBinding
//! owns scoped registrations, not domain requests. A future use of GPUI `.cached(...)` remains valid only when the
//! caller supplies definite bounds and explicitly updates the element and
//! notifies its retained owner whenever any application cache-key component
//! changes. GPUI itself observes entity dirtiness, bounds, content mask, and
//! `TextStyle`; it does not observe an application-defined key tuple.

#![forbid(unsafe_code)]

mod avatar;
mod image_surface;
pub use image_surface::ImageSurface;
mod binding;
mod catalog_binding;
mod identity;

pub use avatar::AvatarSurface;
pub use binding::{ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink};
pub use catalog_binding::CatalogBinding;
pub use identity::ClientIdentityRef;
pub use pioneer_client::{
    core::{ClientPublicationReference, ClientScope, ScopedPublication},
    ids::{
        ClientControlRole, ClientDomainIdentity, ClientFeature, ClientIdentity,
        ClientIdentityNamespace,
    },
};

mod package_upload;
pub use package_upload::{UploadText, UploadView as PackageUploadView, UploadVisibility};

mod catalog_presentation;
pub use catalog_presentation::{
    catalog_heading, catalog_sidebar_icon, catalog_sidebar_label, catalog_sidebar_menu_icon,
    catalog_status_badge,
};

mod catalog_details_header;
pub use catalog_details_header::CatalogDetailsHeader;

mod catalog_card;
pub use catalog_card::CatalogCard;
pub use catalog_presentation::{catalog_empty_state, catalog_list_header};

mod image_views;
pub use image_views::{ImageViewConfig, ImageViewPort, ImageViews};
