//! Retained provider presentation over immutable scoped Client publications.
#[macro_use]
extern crate rust_i18n;
rust_i18n::i18n!("locales", fallback = "en");
mod actions;
mod activity;
mod assets;
mod binding;
mod buttons;
mod catalog;
mod credential_form;
mod dialogs;
mod input;
mod ports;
mod providers;
mod queries;
mod sidebar;
mod view;
pub use ports::{
    ProviderCredentialPort, ProviderEffectCompletion, ProviderEffectIdentity,
    ProviderExternalNavigationPort,
};
pub use providers::{ProviderCatalogConfig, ProviderCatalogView};

mod dialog_lifetime;

#[cfg(test)]
mod boundary_tests;
