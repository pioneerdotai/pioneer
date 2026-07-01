use super::model_catalog::{
    VoiceModelCatalogEntry, parakeet_v3_int8_catalog_entry, voice_model_install_layout,
};
use super::model_install::{
    ReqwestVoiceModelArchiveDownloader, VoiceModelArchiveDownloader, ensure_voice_model_installed,
    is_voice_model_installed_and_verified,
};
use anyhow::{Context, Result};
use pioneer_config::AppConfig;
use pioneer_protocol::{VoiceError, VoiceErrorKind, VoiceStatus, VoiceStatusResponse};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tracing::{info, warn};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VoiceModelBootstrapSnapshot {
    pub(crate) status: VoiceStatus,
    pub(crate) error: Option<VoiceError>,
}

#[derive(Debug)]
pub(crate) struct VoiceModelBootstrapHandle {
    state: Arc<RwLock<VoiceModelBootstrapSnapshot>>,
}

impl VoiceModelBootstrapHandle {
    pub(crate) fn unavailable(message: impl Into<String>) -> Self {
        Self::from_snapshot(VoiceModelBootstrapSnapshot {
            status: VoiceStatus::Unavailable,
            error: Some(VoiceError {
                kind: VoiceErrorKind::ModelUnavailable,
                message: message.into(),
            }),
        })
    }

    pub(crate) fn ready() -> Self {
        Self::from_snapshot(VoiceModelBootstrapSnapshot {
            status: VoiceStatus::Ready,
            error: None,
        })
    }

    fn downloading() -> Self {
        Self::from_snapshot(VoiceModelBootstrapSnapshot {
            status: VoiceStatus::ModelDownloading,
            error: None,
        })
    }

    fn from_snapshot(snapshot: VoiceModelBootstrapSnapshot) -> Self {
        Self {
            state: Arc::new(RwLock::new(snapshot)),
        }
    }

    pub(crate) fn snapshot(&self) -> VoiceModelBootstrapSnapshot {
        self.state
            .read()
            .map(|state| state.clone())
            .unwrap_or_else(|_| VoiceModelBootstrapSnapshot {
                status: VoiceStatus::Error,
                error: Some(VoiceError {
                    kind: VoiceErrorKind::Unknown,
                    message: "voice model bootstrap state is unavailable".to_owned(),
                }),
            })
    }

    pub(crate) fn status_response(&self) -> VoiceStatusResponse {
        let snapshot = self.snapshot();
        VoiceStatusResponse {
            status: snapshot.status,
            active_session_id: None,
            error: snapshot.error,
        }
    }

    fn set_snapshot(&self, snapshot: VoiceModelBootstrapSnapshot) {
        if let Ok(mut state) = self.state.write() {
            *state = snapshot;
        }
    }

    fn set_status(&self, status: VoiceStatus) {
        self.set_snapshot(VoiceModelBootstrapSnapshot {
            status,
            error: None,
        });
    }

    fn set_error(&self, error: VoiceError) {
        self.set_snapshot(VoiceModelBootstrapSnapshot {
            status: VoiceStatus::Error,
            error: Some(error),
        });
    }
}

pub(crate) fn start_parakeet_v3_int8_bootstrap(
    config: AppConfig,
    runtime_home: PathBuf,
) -> Result<Arc<VoiceModelBootstrapHandle>> {
    start_voice_model_bootstrap_with_downloader(
        parakeet_v3_int8_catalog_entry(),
        config,
        runtime_home,
        Arc::new(ReqwestVoiceModelArchiveDownloader::new()),
    )
}

pub(crate) fn start_voice_model_bootstrap_with_downloader<D>(
    entry: VoiceModelCatalogEntry,
    config: AppConfig,
    runtime_home: PathBuf,
    downloader: Arc<D>,
) -> Result<Arc<VoiceModelBootstrapHandle>>
where
    D: VoiceModelArchiveDownloader + 'static,
{
    let layout = voice_model_install_layout(&entry, &config, runtime_home.as_path())
        .context("failed to resolve voice model install layout")?;

    if is_voice_model_installed_and_verified(&entry, &layout) {
        info!(
            model_id = entry.id,
            model_dir = %layout.install_dir.display(),
            "local voice model is already installed"
        );
        return Ok(Arc::new(VoiceModelBootstrapHandle::ready()));
    }

    let handle = Arc::new(VoiceModelBootstrapHandle::downloading());
    let worker_handle = handle.clone();
    tokio::spawn(async move {
        info!(
            model_id = entry.id,
            model_dir = %layout.install_dir.display(),
            archive = %layout.archive_path.display(),
            "starting local voice model bootstrap"
        );
        match ensure_voice_model_installed(
            &entry,
            &config,
            runtime_home.as_path(),
            downloader.as_ref(),
        )
        .await
        {
            Ok(report) => {
                worker_handle.set_status(VoiceStatus::ModelLoading);
                if is_voice_model_installed_and_verified(&entry, &report.layout) {
                    worker_handle.set_status(VoiceStatus::Ready);
                    info!(
                        model_id = entry.id,
                        model_dir = %report.layout.install_dir.display(),
                        "local voice model bootstrap completed"
                    );
                } else {
                    let error = VoiceError {
                        kind: VoiceErrorKind::ModelUnavailable,
                        message: format!(
                            "local voice model {} installed but did not pass verification",
                            entry.id
                        ),
                    };
                    warn!(model_id = entry.id, "local voice model verification failed");
                    worker_handle.set_error(error);
                }
            }
            Err(error) => {
                warn!(
                    model_id = entry.id,
                    error = %format!("{error:#}"),
                    "local voice model bootstrap failed"
                );
                worker_handle.set_error(VoiceError {
                    kind: VoiceErrorKind::ModelUnavailable,
                    message: format!("local voice model {} bootstrap failed: {error:#}", entry.id),
                });
            }
        }
    });

    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::path::Path;
    use std::time::Duration;

    struct FailingDownloader;

    #[async_trait]
    impl VoiceModelArchiveDownloader for FailingDownloader {
        async fn download_archive(
            &self,
            _entry: &VoiceModelCatalogEntry,
            _destination: &Path,
        ) -> Result<()> {
            anyhow::bail!("network offline")
        }
    }

    #[tokio::test]
    async fn verified_installed_model_reports_ready_without_download() {
        let config = AppConfig::load().expect("config");
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let entry = parakeet_v3_int8_catalog_entry();
        let layout = voice_model_install_layout(&entry, &config, temp_dir.path()).expect("layout");
        std::fs::create_dir_all(layout.model_data_dir.as_path()).expect("model dir");
        std::fs::write(
            layout.ready_marker_path.as_path(),
            serde_json::to_vec(&serde_json::json!({
                "id": entry.id,
                "version": entry.version,
                "sha256": entry.sha256,
            }))
            .expect("marker"),
        )
        .expect("ready marker");

        let handle = start_voice_model_bootstrap_with_downloader(
            entry,
            config,
            temp_dir.path().to_path_buf(),
            Arc::new(FailingDownloader),
        )
        .expect("bootstrap");

        let response = handle.status_response();
        assert_eq!(response.status, VoiceStatus::Ready);
        assert!(response.error.is_none());
    }

    #[tokio::test]
    async fn download_error_is_exposed_as_voice_status_error() {
        let config = AppConfig::load().expect("config");
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let handle = start_voice_model_bootstrap_with_downloader(
            parakeet_v3_int8_catalog_entry(),
            config,
            temp_dir.path().to_path_buf(),
            Arc::new(FailingDownloader),
        )
        .expect("bootstrap");

        assert_eq!(
            handle.status_response().status,
            VoiceStatus::ModelDownloading
        );

        for _ in 0..20 {
            if handle.status_response().status == VoiceStatus::Error {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let response = handle.status_response();
        assert_eq!(response.status, VoiceStatus::Error);
        let error = response.error.expect("error");
        assert_eq!(error.kind, VoiceErrorKind::ModelUnavailable);
        assert!(error.message.contains("network offline"));
    }
}
