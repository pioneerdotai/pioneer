pub(crate) mod download;
pub(crate) mod manifest;
pub(crate) mod plan;
pub(crate) mod platform;
pub(crate) mod relaunch;
pub(crate) mod release;
pub(crate) mod state;

pub(crate) use state::DesktopUpdateConfig;

pub(crate) fn desktop_current_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

pub(crate) fn desktop_update_config_from_env() -> DesktopUpdateConfig {
    DesktopUpdateConfig::from_env()
}
