//! Retained administration presentation over the process-local Client.
#[macro_use]
extern crate rust_i18n;
rust_i18n::i18n!("locales", fallback = "en");
mod activity;
mod administration;
mod assets;
mod avatar;
mod binding;
mod buttons;
mod credential;
mod input;
mod invitations;
mod members;
mod ports;
mod sidebar;
mod view;
pub use administration::{AdministrationConfig, AdministrationView};
pub use ports::*;

mod dialog_lifetime;

#[cfg(test)]
mod boundary_tests;
