mod account;
mod lifecycle;
mod sidebar;
mod view;

pub(super) const SETTINGS_CONTENT_GENERAL_NODE_ID: &str = "settings:general";
pub(super) const SETTINGS_CONTENT_ACCOUNT_NODE_ID: &str = "settings:account";
pub(super) const SETTINGS_CONTENT_MEMORY_NODE_ID: &str = "settings:memory";
pub(super) const SETTINGS_CONTENT_SELF_IMPROVEMENT_NODE_ID: &str = "settings:self-improvement";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum VoiceInputEnableAction {
    Sent,
    NeedsSelection,
    Noop,
}

pub(super) use account::ProfileEditorState;
pub(super) use pioneer_client::settings::memory::{MemoryModelSetting, MemorySettingToggle};
pub(super) use pioneer_client::settings::self_improvement::SelfImprovementModelSetting;
