//! Retained Settings surfaces and platform preference adapters.
#[macro_use]
extern crate rust_i18n;
rust_i18n::i18n!("locales", fallback = "en");
mod account;
mod binding;
mod buttons;
mod model_selector;
mod platform;
mod profile_presentation;
mod progress;
mod screen;
mod self_improvement_status;
mod sidebar;
mod view;
pub use platform::{
    SettingsPhotoError, SettingsPhotoPort, SettingsPhotoSelection, SettingsPlatform,
};
pub use screen::{SettingsConfig, SettingsView};
mod actions;
mod assets;
mod device_activation_form;
mod file_openers;
mod general_actions;

mod file_opener_types;
mod preferences_types;
pub use file_opener_types::FileOpenerId;
pub use preferences_types::{AppLanguagePreference, WindowThemePreference};
