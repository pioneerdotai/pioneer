use crate::{
    app::root::{GatewayConnectionState, PioneerDesktop, ThreadArtifactActionStatus},
    gateway::GatewayWsCommandSender,
};
use anyhow::{Context as AnyhowContext, Result};
use gpui::{prelude::*, *};
use pioneer_client::artifacts::{
    actions as client_artifact_actions,
    download::{ArtifactDownloadRequest, ArtifactDownloadResult},
};
use pioneer_client::platform::{ArtifactFileOpener, ClientPath};
use pioneer_client::{ClientError, ClientResult};
use pioneer_protocol::{ArtifactRef, ArtifactSummary};
use std::{
    path::{Path, PathBuf},
    process::Command,
};
use tracing::warn;

impl client_artifact_actions::ArtifactCachedDownloadClient for GatewayWsCommandSender {
    fn download_artifact_to_cache(
        &self,
        request: ArtifactDownloadRequest,
    ) -> Result<ArtifactDownloadResult> {
        GatewayWsCommandSender::download_artifact_to_cache(self, request)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SystemArtifactFileOpener;

impl ArtifactFileOpener for SystemArtifactFileOpener {
    fn open_file(&self, path: &ClientPath) -> ClientResult<()> {
        spawn_open_file(path.as_path()).map_err(|error| ClientError::platform(format!("{error:#}")))
    }

    fn reveal_file(&self, path: &ClientPath) -> ClientResult<()> {
        spawn_reveal_file(path.as_path())
            .map_err(|error| ClientError::platform(format!("{error:#}")))
    }
}

impl PioneerDesktop {
    pub(in crate::app) fn choose_thread_artifact_download_destination(
        &mut self,
        summary: ArtifactSummary,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.artifact_action_can_download(&summary) {
            return;
        }

        self.thread_artifacts
            .set_action_status(&summary.artifact, ThreadArtifactActionStatus::Queued);
        cx.notify();

        let selection = cx.prompt_for_paths(PathPromptOptions {
            files: false,
            directories: true,
            multiple: false,
            prompt: None,
        });

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let selection = match selection.await {
                    Ok(selection) => selection,
                    Err(_) => {
                        let _ = this.update(&mut cx, |view, cx| {
                            view.thread_artifacts.clear_action_status(&summary.artifact);
                            cx.notify();
                        });
                        return;
                    }
                };

                let paths = match selection {
                    Ok(paths) => paths,
                    Err(error) => {
                        let _ = this.update(&mut cx, |view, cx| {
                            view.mark_thread_artifact_action_failed(
                                &summary.artifact,
                                format!("{error:#}"),
                                cx,
                            );
                        });
                        return;
                    }
                };

                let Some(destination_dir) = paths.and_then(|mut values| values.pop()) else {
                    let _ = this.update(&mut cx, |view, cx| {
                        view.thread_artifacts.clear_action_status(&summary.artifact);
                        cx.notify();
                    });
                    return;
                };

                let _ = this.update(&mut cx, |view, cx| {
                    view.start_thread_artifact_download_to_folder(summary, destination_dir, cx);
                });
            }
        })
        .detach();
    }

    pub(in crate::app) fn open_thread_artifact(
        &mut self,
        summary: ArtifactSummary,
        cx: &mut Context<Self>,
    ) {
        if !self.artifact_action_can_download(&summary) {
            return;
        }

        let Some(request) = self.artifact_download_request(&summary) else {
            return;
        };
        let existing_local_file = self.thread_artifacts.local_file(&summary.artifact).cloned();
        let initial_status = if existing_local_file.is_some() {
            ThreadArtifactActionStatus::Verifying
        } else {
            ThreadArtifactActionStatus::Downloading
        };
        self.thread_artifacts
            .set_action_status(&summary.artifact, initial_status);
        cx.notify();

        let ws_sender = self.gateway.ws_command_sender.clone();
        let artifact = summary.artifact.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let artifact_for_download = artifact.clone();
                let local_result = cx
                    .background_spawn(async move {
                        client_artifact_actions::ensure_artifact_local_copy_for_open(
                            &ws_sender,
                            request,
                            &artifact_for_download,
                            existing_local_file.as_ref(),
                        )
                    })
                    .await;

                let local_file = match local_result {
                    Ok(local_file) => local_file,
                    Err(error) => {
                        let _ = this.update(&mut cx, |view, cx| {
                            view.mark_thread_artifact_action_failed(
                                &artifact,
                                format!("{error:#}"),
                                cx,
                            );
                        });
                        return;
                    }
                };

                let local_file_for_state = local_file.clone();
                let _ = this.update(&mut cx, |view, cx| {
                    view.thread_artifacts
                        .set_local_file(&artifact, local_file_for_state);
                    view.thread_artifacts
                        .set_action_status(&artifact, ThreadArtifactActionStatus::Opening);
                    cx.notify();
                });

                let open_result = cx
                    .background_spawn(async move {
                        client_artifact_actions::open_artifact_local_file(
                            &SystemArtifactFileOpener,
                            local_file.path.as_path(),
                        )
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    match open_result {
                        Ok(()) => view.thread_artifacts.clear_action_status(&artifact),
                        Err(error) => view.mark_thread_artifact_action_failed(
                            &artifact,
                            format!("{error:#}"),
                            cx,
                        ),
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(in crate::app) fn reveal_thread_artifact(
        &mut self,
        artifact: ArtifactRef,
        cx: &mut Context<Self>,
    ) {
        let Some(local_file) = self.thread_artifacts.local_file(&artifact).cloned() else {
            return;
        };
        if !local_file.path.is_file() {
            self.thread_artifacts.clear_local_file(&artifact);
            cx.notify();
            return;
        }

        self.thread_artifacts
            .set_action_status(&artifact, ThreadArtifactActionStatus::Revealing);
        cx.notify();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let reveal_result = cx
                    .background_spawn(async move {
                        client_artifact_actions::reveal_artifact_local_file(
                            &SystemArtifactFileOpener,
                            local_file.path.as_path(),
                        )
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    match reveal_result {
                        Ok(()) => view.thread_artifacts.clear_action_status(&artifact),
                        Err(error) => view.mark_thread_artifact_action_failed(
                            &artifact,
                            format!("{error:#}"),
                            cx,
                        ),
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn start_thread_artifact_download_to_folder(
        &mut self,
        summary: ArtifactSummary,
        destination_dir: PathBuf,
        cx: &mut Context<Self>,
    ) {
        let Some(request) = self.artifact_download_request(&summary) else {
            return;
        };

        self.thread_artifacts
            .set_action_status(&summary.artifact, ThreadArtifactActionStatus::Downloading);
        cx.notify();

        let ws_sender = self.gateway.ws_command_sender.clone();
        let artifact = summary.artifact.clone();
        let display_name = summary.artifact.display_name.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let cache_result = cx
                    .background_spawn(async move { ws_sender.download_artifact_to_cache(request) })
                    .await;

                let cache_result = match cache_result {
                    Ok(cache_result) => cache_result,
                    Err(error) => {
                        let _ = this.update(&mut cx, |view, cx| {
                            view.mark_thread_artifact_action_failed(
                                &artifact,
                                format!("{error:#}"),
                                cx,
                            );
                        });
                        return;
                    }
                };

                let artifact_for_state = artifact.clone();
                let _ = this.update(&mut cx, |view, cx| {
                    view.thread_artifacts.set_action_status(
                        &artifact_for_state,
                        ThreadArtifactActionStatus::Verifying,
                    );
                    cx.notify();
                });

                let local_result = cx
                    .background_spawn(async move {
                        client_artifact_actions::copy_download_result_to_destination(
                            &cache_result,
                            display_name.as_str(),
                            destination_dir.as_path(),
                        )
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    match local_result {
                        Ok(local_file) => {
                            view.thread_artifacts.set_local_file(&artifact, local_file);
                            view.thread_artifacts.clear_action_status(&artifact);
                        }
                        Err(error) => {
                            view.mark_thread_artifact_action_failed(
                                &artifact,
                                format!("{error:#}"),
                                cx,
                            );
                        }
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn artifact_action_can_download(&mut self, summary: &ArtifactSummary) -> bool {
        let action_in_progress = self.thread_artifacts.action_in_progress(&summary.artifact);
        let connected = self.gateway.connection_state == GatewayConnectionState::Connected;
        match client_artifact_actions::artifact_download_block_reason(
            summary,
            action_in_progress,
            connected,
        ) {
            None => true,
            Some(client_artifact_actions::ArtifactFileActionBlockReason::NotConnected) => {
                self.thread_artifacts.set_action_status(
                    &summary.artifact,
                    ThreadArtifactActionStatus::Failed(
                        t!("artifacts.action.error.not_connected").to_string(),
                    ),
                );
                false
            }
            Some(
                client_artifact_actions::ArtifactFileActionBlockReason::NotReady
                | client_artifact_actions::ArtifactFileActionBlockReason::ActionInProgress,
            ) => false,
        }
    }

    fn artifact_download_request(
        &mut self,
        summary: &ArtifactSummary,
    ) -> Option<ArtifactDownloadRequest> {
        let gateway_profile_id = self
            .gateway
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.active_gateway().map(|gateway| gateway.id.clone()));
        match client_artifact_actions::plan_artifact_download_request(gateway_profile_id, summary) {
            Ok(request) => Some(request),
            Err(
                client_artifact_actions::ArtifactDownloadRequestPlanError::MissingGatewayProfile,
            ) => {
                self.thread_artifacts.set_action_status(
                    &summary.artifact,
                    ThreadArtifactActionStatus::Failed(
                        t!("artifacts.action.error.no_gateway").to_string(),
                    ),
                );
                None
            }
            Err(client_artifact_actions::ArtifactDownloadRequestPlanError::MissingWorkspaceId) => {
                self.thread_artifacts.set_action_status(
                    &summary.artifact,
                    ThreadArtifactActionStatus::Failed(
                        t!("artifacts.action.error.no_workspace").to_string(),
                    ),
                );
                None
            }
        }
    }

    fn mark_thread_artifact_action_failed(
        &mut self,
        artifact: &ArtifactRef,
        error: String,
        cx: &mut Context<Self>,
    ) {
        warn!(
            artifact_id = artifact.artifact_id.as_str(),
            version_id = artifact.version_id.as_deref(),
            error = %error,
            "artifact file action failed"
        );
        self.thread_artifacts
            .set_action_status(artifact, ThreadArtifactActionStatus::Failed(error));
        cx.notify();
    }
}

#[cfg(target_os = "macos")]
fn spawn_open_file(path: &Path) -> Result<()> {
    spawn_command(Command::new("open").arg(path), "open artifact file")
}

#[cfg(target_os = "macos")]
fn spawn_reveal_file(path: &Path) -> Result<()> {
    spawn_command(
        Command::new("open").arg("-R").arg(path),
        "reveal artifact file",
    )
}

#[cfg(all(unix, not(target_os = "macos")))]
fn spawn_open_file(path: &Path) -> Result<()> {
    spawn_command(Command::new("xdg-open").arg(path), "open artifact file")
}

#[cfg(all(unix, not(target_os = "macos")))]
fn spawn_reveal_file(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    spawn_command(Command::new("xdg-open").arg(parent), "reveal artifact file")
}

#[cfg(windows)]
fn spawn_open_file(path: &Path) -> Result<()> {
    spawn_command(
        Command::new("cmd").arg("/C").arg("start").arg("").arg(path),
        "open artifact file",
    )
}

#[cfg(windows)]
fn spawn_reveal_file(path: &Path) -> Result<()> {
    spawn_command(
        Command::new("explorer").arg("/select,").arg(path),
        "reveal artifact file",
    )
}

fn spawn_command(command: &mut Command, action: &str) -> Result<()> {
    command
        .spawn()
        .with_context(|| format!("failed to {action}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::app::root::{ThreadArtifactLocalFile, ThreadArtifactsState};
    use anyhow::Result;
    use pioneer_client::ClientResult;
    use pioneer_client::artifacts::{
        actions as client_artifact_actions,
        download::{ArtifactDownloadRequest, ArtifactDownloadResult},
    };
    use pioneer_client::platform::{ArtifactFileOpener, ClientPath};
    use pioneer_protocol::{ArtifactKind, ArtifactRef, ArtifactStatus};
    use sha2::{Digest, Sha256};
    use std::cell::RefCell;
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::Mutex,
    };

    #[derive(Clone)]
    struct FakeDownloadClient {
        result: ArtifactDownloadResult,
        calls: std::rc::Rc<RefCell<usize>>,
    }

    impl client_artifact_actions::ArtifactCachedDownloadClient for FakeDownloadClient {
        fn download_artifact_to_cache(
            &self,
            request: ArtifactDownloadRequest,
        ) -> Result<ArtifactDownloadResult> {
            assert_eq!(request.artifact_id, self.result.artifact.artifact_id);
            *self.calls.borrow_mut() += 1;
            Ok(self.result.clone())
        }
    }

    #[derive(Default)]
    struct FakeOpener {
        opened: Mutex<Vec<PathBuf>>,
        revealed: Mutex<Vec<PathBuf>>,
    }

    impl ArtifactFileOpener for FakeOpener {
        fn open_file(&self, path: &ClientPath) -> ClientResult<()> {
            self.opened
                .lock()
                .expect("opened lock")
                .push(path.as_path().to_owned());
            Ok(())
        }

        fn reveal_file(&self, path: &ClientPath) -> ClientResult<()> {
            self.revealed
                .lock()
                .expect("revealed lock")
                .push(path.as_path().to_owned());
            Ok(())
        }
    }

    #[test]
    fn artifact_download_writes_verified_bytes_to_selected_path_with_fake_client() {
        let temp = tempfile::tempdir().expect("tempdir");
        let bytes = b"artifact bytes";
        let cache_path = temp.path().join("cache.bin");
        fs::write(cache_path.as_path(), bytes).expect("write cache");
        let result = download_result(cache_path, bytes, "report?.txt");
        let client = FakeDownloadClient {
            result: result.clone(),
            calls: Default::default(),
        };

        let local_file = download_artifact_to_destination(
            &client,
            download_request("art_1"),
            "report?.txt",
            temp.path(),
        )
        .expect("download");

        assert_eq!(fs::read(local_file.path.as_path()).expect("read"), bytes);
        assert_eq!(
            local_file.path.file_name().and_then(|value| value.to_str()),
            Some("report_.txt")
        );
        assert_eq!(*client.calls.borrow(), 1);
    }

    #[test]
    fn failed_download_does_not_mark_file_downloaded_or_leave_final_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let bytes = b"artifact bytes";
        let cache_path = temp.path().join("cache.bin");
        fs::write(cache_path.as_path(), b"corrupt bytes").expect("write corrupt cache");
        let mut result = download_result(cache_path, bytes, "report.txt");
        result.size_bytes = bytes.len() as u64;

        let error = client_artifact_actions::copy_download_result_to_destination(
            &result,
            "report.txt",
            temp.path(),
        )
        .expect_err("should fail verification");

        assert!(
            error.to_string().contains("size mismatch") || error.to_string().contains("sha256")
        );
        assert!(!temp.path().join("report.txt").exists());
    }

    #[test]
    fn open_downloads_to_cache_before_invoking_opener() {
        let temp = tempfile::tempdir().expect("tempdir");
        let bytes = b"artifact bytes";
        let cache_path = temp.path().join("cache.bin");
        fs::write(cache_path.as_path(), bytes).expect("write cache");
        let client = FakeDownloadClient {
            result: download_result(cache_path.clone(), bytes, "report.txt"),
            calls: Default::default(),
        };
        let artifact = artifact_ref("art_1", "report.txt", Some(sha256_bytes(bytes)));

        let local_file = client_artifact_actions::ensure_artifact_local_copy_for_open(
            &client,
            download_request("art_1"),
            &artifact,
            None,
        )
        .expect("local copy");
        let opener = FakeOpener::default();
        client_artifact_actions::open_artifact_local_file(&opener, local_file.path.as_path())
            .expect("open");

        assert_eq!(*client.calls.borrow(), 1);
        assert_eq!(
            opener.opened.lock().expect("opened lock").as_slice(),
            &[cache_path]
        );
    }

    #[test]
    fn open_reuses_verified_cache() {
        let temp = tempfile::tempdir().expect("tempdir");
        let bytes = b"artifact bytes";
        let cache_path = temp.path().join("cache.bin");
        fs::write(cache_path.as_path(), bytes).expect("write cache");
        let client = FakeDownloadClient {
            result: download_result(cache_path.clone(), bytes, "report.txt"),
            calls: Default::default(),
        };
        let artifact = artifact_ref("art_1", "report.txt", Some(sha256_bytes(bytes)));
        let existing = ThreadArtifactLocalFile {
            path: cache_path.clone(),
            sha256: sha256_bytes(bytes),
            size_bytes: Some(bytes.len() as u64),
        };

        let local_file = client_artifact_actions::ensure_artifact_local_copy_for_open(
            &client,
            download_request("art_1"),
            &artifact,
            Some(&existing),
        )
        .expect("local copy");

        assert_eq!(local_file.path, cache_path);
        assert_eq!(*client.calls.borrow(), 0);
    }

    #[test]
    fn reveal_requires_existing_local_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let missing = temp.path().join("missing.txt");
        let opener = FakeOpener::default();

        let error = client_artifact_actions::reveal_artifact_local_file(&opener, missing.as_path())
            .expect_err("missing file should not reveal");

        assert!(error.to_string().contains("does not exist"));
        assert!(opener.revealed.lock().expect("revealed lock").is_empty());
    }

    #[test]
    fn artifact_state_reveal_disabled_until_local_path_exists() {
        let temp = tempfile::tempdir().expect("tempdir");
        let artifact = artifact_ref("art_1", "report.txt", None);
        let mut state = ThreadArtifactsState::default();

        assert!(state.local_file(&artifact).is_none());
        state.set_local_file(
            &artifact,
            ThreadArtifactLocalFile {
                path: temp.path().join("missing.txt"),
                sha256: "abc".to_owned(),
                size_bytes: None,
            },
        );
        assert!(
            state
                .local_file(&artifact)
                .is_some_and(|local| !local.path.is_file())
        );
    }

    fn download_artifact_to_destination<
        C: client_artifact_actions::ArtifactCachedDownloadClient,
    >(
        client: &C,
        request: ArtifactDownloadRequest,
        display_name: &str,
        destination_dir: &Path,
    ) -> Result<ThreadArtifactLocalFile> {
        let result = client.download_artifact_to_cache(request)?;
        client_artifact_actions::copy_download_result_to_destination(
            &result,
            display_name,
            destination_dir,
        )
    }

    fn download_result(
        local_path: PathBuf,
        bytes: &[u8],
        display_name: &str,
    ) -> ArtifactDownloadResult {
        ArtifactDownloadResult {
            local_path: ClientPath::new(local_path),
            artifact: artifact_ref("art_1", display_name, Some(sha256_bytes(bytes))),
            size_bytes: bytes.len() as u64,
            sha256: sha256_bytes(bytes),
        }
    }

    fn artifact_ref(artifact_id: &str, display_name: &str, sha256: Option<String>) -> ArtifactRef {
        ArtifactRef {
            artifact_id: artifact_id.to_owned(),
            version_id: Some("ver_1".to_owned()),
            display_name: display_name.to_owned(),
            kind: ArtifactKind::Text,
            mime_type: Some("text/plain".to_owned()),
            size_bytes: None,
            sha256,
            status: ArtifactStatus::Ready,
            preview: None,
        }
    }

    fn download_request(artifact_id: &str) -> ArtifactDownloadRequest {
        ArtifactDownloadRequest {
            gateway_profile_id: "remote".to_owned(),
            workspace_id: "ws_1".to_owned(),
            artifact_id: artifact_id.to_owned(),
            version_id: Some("ver_1".to_owned()),
        }
    }

    fn sha256_bytes(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        hex::encode(hasher.finalize())
    }
}
