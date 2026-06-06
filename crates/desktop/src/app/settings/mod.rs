mod lifecycle;
mod sidebar;
mod view;

pub(super) const SETTINGS_CONTENT_GENERAL_NODE_ID: &str = "settings:general";
pub(super) const SETTINGS_CONTENT_MEMORY_NODE_ID: &str = "settings:memory";

pub(super) use pioneer_client::settings::memory::{MemoryModelSetting, MemorySettingToggle};
