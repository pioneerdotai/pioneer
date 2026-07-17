use std::path::PathBuf;

#[cfg(debug_assertions)]
const ENV_DESKTOP_UPDATE_STYLE_PREVIEW: &str = "PIONEER_DESKTOP_UPDATE_STYLE_PREVIEW";

#[cfg(debug_assertions)]
enum DesktopUpdateStylePreview {
    Ready,
    Downloading,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DesktopUpdateUiState {
    Idle,
    Checking,
    Downloading {
        style_preview: bool,
    },
    Ready {
        version: String,
        current_version: String,
        tag: String,
        asset_path: PathBuf,
        asset_name: String,
        sha256: String,
        os: String,
        arch: String,
        kind: String,
        size_bytes: u64,
        style_preview: bool,
    },
    Applying {
        version: String,
    },
    FailedSilent {
        checked_at_unix: u64,
        error_code: String,
    },
}

impl DesktopUpdateUiState {
    pub(crate) fn initial() -> Self {
        #[cfg(debug_assertions)]
        {
            match desktop_update_style_preview_mode() {
                Some(DesktopUpdateStylePreview::Ready) => Self::style_preview(),
                Some(DesktopUpdateStylePreview::Downloading) => Self::Downloading {
                    style_preview: true,
                },
                None => Self::Idle,
            }
        }

        #[cfg(not(debug_assertions))]
        {
            Self::Idle
        }
    }

    #[cfg(debug_assertions)]
    fn style_preview() -> Self {
        Self::Ready {
            version: "1.19367.0".to_owned(),
            current_version: env!("CARGO_PKG_VERSION").to_owned(),
            tag: "v1.19367.0".to_owned(),
            asset_path: PathBuf::from("/tmp/pioneer-app-updater-style-preview.zip"),
            asset_name: "Pioneer-style-preview.app.zip".to_owned(),
            sha256: "0000000000000000000000000000000000000000000000000000000000000000".to_owned(),
            os: std::env::consts::OS.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
            kind: "style_preview".to_owned(),
            size_bytes: 0,
            style_preview: true,
        }
    }

    pub(crate) fn is_style_preview(&self) -> bool {
        let is_ready_preview = matches!(
            self,
            Self::Ready {
                style_preview: true,
                ..
            }
        );

        #[cfg(debug_assertions)]
        {
            is_ready_preview
                || matches!(
                    self,
                    Self::Downloading {
                        style_preview: true,
                        ..
                    }
                )
        }

        #[cfg(not(debug_assertions))]
        {
            is_ready_preview
        }
    }

    pub(crate) fn should_render_sidebar_panel(&self) -> bool {
        matches!(self, Self::Downloading { .. } | Self::Ready { .. })
    }
}

#[cfg(debug_assertions)]
fn desktop_update_style_preview_mode() -> Option<DesktopUpdateStylePreview> {
    let value = std::env::var(ENV_DESKTOP_UPDATE_STYLE_PREVIEW).ok()?;
    match value.trim().to_ascii_lowercase().as_str() {
        "restart" | "ready" => Some(DesktopUpdateStylePreview::Ready),
        "downloading" | "checking" => Some(DesktopUpdateStylePreview::Downloading),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::DesktopUpdateUiState;
    use std::path::PathBuf;

    #[test]
    fn sidebar_panel_renders_for_downloading_and_ready() {
        let state = ready_state();

        assert!(state.should_render_sidebar_panel());
        assert!(DesktopUpdateUiState::Downloading {
            style_preview: false
        }
        .should_render_sidebar_panel());
    }

    #[test]
    fn checking_and_inactive_states_do_not_render_sidebar_panel() {
        let idle = DesktopUpdateUiState::Idle;
        let failed = DesktopUpdateUiState::FailedSilent {
            checked_at_unix: 1_789_200_000,
            error_code: "download".to_owned(),
        };

        assert!(!idle.should_render_sidebar_panel());
        assert!(!DesktopUpdateUiState::Checking.should_render_sidebar_panel());
        assert!(!failed.should_render_sidebar_panel());
    }

    fn ready_state() -> DesktopUpdateUiState {
        DesktopUpdateUiState::Ready {
            version: "0.26.0".to_owned(),
            current_version: "0.25.0".to_owned(),
            tag: "v0.26.0".to_owned(),
            asset_path: PathBuf::from("/tmp/Pioneer-aarch64.app.zip"),
            asset_name: "Pioneer-aarch64.app.zip".to_owned(),
            sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            os: "macos".to_owned(),
            arch: "aarch64".to_owned(),
            kind: "macos_app_zip".to_owned(),
            size_bytes: 123,
            style_preview: false,
        }
    }
}
