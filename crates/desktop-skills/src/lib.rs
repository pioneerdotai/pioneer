//! Retained catalog presentation over typed, immutable Client publications.
#[macro_use]
extern crate rust_i18n;
rust_i18n::i18n!("locales", fallback = "en");
mod actions;
mod assets;
mod catalog;
mod details;
mod dialogs;
mod list;
mod sidebar;
mod table;
pub use catalog::{SkillsCatalogConfig, SkillsCatalogView};

mod upload;
pub use upload::{UploadView as SkillsUploadView, UploadVisibility as SkillsUploadVisibility};

mod identity;
