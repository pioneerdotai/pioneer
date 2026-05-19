mod lifecycle;
mod sidebar;
mod view;

pub(super) const SETTINGS_CONTENT_GENERAL_NODE_ID: &str = "settings:general";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MemorySettingToggle {
    Enabled,
    ProactiveWrites,
    ActiveRecall,
    DebugTrace,
}
