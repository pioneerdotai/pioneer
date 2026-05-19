mod lifecycle;
mod sidebar;
mod view;

pub(super) const SETTINGS_CONTENT_GENERAL_NODE_ID: &str = "settings:general";
pub(super) const SETTINGS_CONTENT_MEMORY_NODE_ID: &str = "settings:memory";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MemorySettingToggle {
    Enabled,
    ActiveRecall,
    ProactiveWrites,
    BackgroundExtraction,
    DebugTrace,
}
