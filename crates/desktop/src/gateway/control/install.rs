use pioneer_config::{AppConfig, InstallManagedBy, load_install_state};
use std::path::{Path, PathBuf};
use tracing::info;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedGatewayInstall {
    pub managed_by: InstallManagedBy,
    pub installed_version: String,
    pub binary_path: PathBuf,
}

pub(crate) fn managed_gateway_install() -> Option<ManagedGatewayInstall> {
    let config = match AppConfig::load() {
        Ok(config) => config,
        Err(error) => {
            info!(
                error = %format!("{error:#}"),
                message = "failed to load app config while resolving managed gateway install"
            );
            return None;
        }
    };

    let install_state_path = match config.install_state_path() {
        Ok(path) => path,
        Err(error) => {
            info!(
                error = %format!("{error:#}"),
                message = "failed to resolve install-state path while resolving managed gateway install"
            );
            return None;
        }
    };

    managed_install_from_install_state(install_state_path.as_path())
}

fn managed_install_from_install_state(install_state_path: &Path) -> Option<ManagedGatewayInstall> {
    let install_state = match load_install_state(install_state_path) {
        Ok(Some(state)) => state,
        Ok(None) => return None,
        Err(error) => {
            info!(
                path = %install_state_path.display(),
                error = %format!("{error:#}"),
                message = "failed to load install-state while resolving managed gateway install"
            );
            return None;
        }
    };

    if install_state.binary_path.is_file() {
        return Some(ManagedGatewayInstall {
            managed_by: install_state.managed_by,
            installed_version: install_state.installed_version,
            binary_path: install_state.binary_path,
        });
    }

    info!(
        path = %install_state.binary_path.display(),
        message = "managed pioneer binary from install-state is missing"
    );
    None
}

#[cfg(test)]
mod tests {
    use super::managed_install_from_install_state;
    use pioneer_config::{InstallManagedBy, InstallState, save_install_state};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn managed_install_from_install_state_returns_none_when_state_is_missing() {
        let install_state_path = unique_temp_path("install-state-missing");
        assert!(managed_install_from_install_state(install_state_path.as_path()).is_none());
    }

    #[test]
    fn managed_install_from_install_state_returns_context_when_file_exists() {
        let install_state_path = unique_temp_path("install-state-existing");
        let binary_path = unique_temp_path(binary_file_prefix());
        fs::write(&binary_path, "pioneer").expect("failed to create test binary");

        write_install_state(&install_state_path, &binary_path, InstallManagedBy::Script);
        let resolved = managed_install_from_install_state(install_state_path.as_path())
            .expect("expected install context from install-state");
        assert_eq!(resolved.binary_path, binary_path);
        assert_eq!(resolved.managed_by, InstallManagedBy::Script);
        assert_eq!(resolved.installed_version, "0.1.0");

        let _ = fs::remove_file(install_state_path);
        let _ = fs::remove_file(binary_path);
    }

    #[test]
    fn managed_install_from_install_state_returns_none_when_binary_file_is_missing() {
        let install_state_path = unique_temp_path("install-state-missing-binary");
        let binary_path = unique_temp_path(binary_file_prefix());

        write_install_state(&install_state_path, &binary_path, InstallManagedBy::Desktop);
        assert!(managed_install_from_install_state(install_state_path.as_path()).is_none());

        let _ = fs::remove_file(install_state_path);
    }

    fn write_install_state(path: &PathBuf, binary_path: &PathBuf, managed_by: InstallManagedBy) {
        let state = InstallState {
            version: InstallState::CURRENT_VERSION,
            managed_by,
            installed_version: "0.1.0".to_owned(),
            install_root: binary_path.parent().map(PathBuf::from),
            binary_path: binary_path.clone(),
            updated_at_unix: 1_700_000_000,
        };

        save_install_state(path, &state).expect("failed to save install-state");
    }

    fn binary_file_prefix() -> &'static str {
        if cfg!(windows) {
            "pioneer.exe"
        } else {
            "pioneer"
        }
    }

    fn unique_temp_path(prefix: &str) -> PathBuf {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("{prefix}-{nanos}-{id}.tmp"))
    }
}
