use crate::{
    app::root::{
        GatewayConnectionState, PioneerDesktop, ThreadArtifactActionStatus, ThreadArtifactLocalFile,
    },
    gateway::{
        DesktopArtifactDownloadRequest, DesktopArtifactDownloadResult, GatewayWsCommandSender,
    },
};
use anyhow::{Context as AnyhowContext, Result, anyhow, bail};
use gpui::{prelude::*, *};
use pioneer_protocol::{ArtifactRef, ArtifactStatus, ArtifactSummary};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
    process::Command,
};
use tracing::warn;

pub(crate) trait ArtifactDownloadClient {
    fn download_artifact_to_cache(
        &self,
        request: DesktopArtifactDownloadRequest,
    ) -> Result<DesktopArtifactDownloadResult>;
}

impl ArtifactDownloadClient for GatewayWsCommandSender {
    fn download_artifact_to_cache(
        &self,
        request: DesktopArtifactDownloadRequest,
    ) -> Result<DesktopArtifactDownloadResult> {
        GatewayWsCommandSender::download_artifact_to_cache(self, request)
    }
}

pub(crate) trait ArtifactFileOpener {
    fn open_file(&self, path: &Path) -> Result<()>;
    fn reveal_file(&self, path: &Path) -> Result<()>;
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SystemArtifactFileOpener;

impl ArtifactFileOpener for SystemArtifactFileOpener {
    fn open_file(&self, path: &Path) -> Result<()> {
        spawn_open_file(path)
    }

    fn reveal_file(&self, path: &Path) -> Result<()> {
        spawn_reveal_file(path)
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
                        ensure_artifact_local_copy_for_open(
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
                        open_artifact_local_file(
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
                        reveal_artifact_local_file(
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
                        copy_cached_download_to_destination(
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
        if summary.artifact.status != ArtifactStatus::Ready {
            return false;
        }
        if self.thread_artifacts.action_in_progress(&summary.artifact) {
            return false;
        }
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.thread_artifacts.set_action_status(
                &summary.artifact,
                ThreadArtifactActionStatus::Failed(
                    t!("artifacts.action.error.not_connected").to_string(),
                ),
            );
            return false;
        }
        true
    }

    fn artifact_download_request(
        &mut self,
        summary: &ArtifactSummary,
    ) -> Option<DesktopArtifactDownloadRequest> {
        let gateway_profile_id = self
            .gateway
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.active_gateway().map(|gateway| gateway.id.clone()));
        let Some(gateway_profile_id) = gateway_profile_id else {
            self.thread_artifacts.set_action_status(
                &summary.artifact,
                ThreadArtifactActionStatus::Failed(
                    t!("artifacts.action.error.no_gateway").to_string(),
                ),
            );
            return None;
        };
        if summary.workspace_id.trim().is_empty() {
            self.thread_artifacts.set_action_status(
                &summary.artifact,
                ThreadArtifactActionStatus::Failed(
                    t!("artifacts.action.error.no_workspace").to_string(),
                ),
            );
            return None;
        }
        Some(DesktopArtifactDownloadRequest {
            gateway_profile_id,
            workspace_id: summary.workspace_id.clone(),
            artifact_id: summary.artifact.artifact_id.clone(),
            version_id: summary.artifact.version_id.clone(),
        })
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

pub(crate) fn ensure_artifact_local_copy_for_open<C: ArtifactDownloadClient>(
    client: &C,
    request: DesktopArtifactDownloadRequest,
    artifact: &ArtifactRef,
    existing: Option<&ThreadArtifactLocalFile>,
) -> Result<ThreadArtifactLocalFile> {
    if let Some(existing) = existing
        && existing_local_file_is_verified(existing, artifact)?
    {
        return Ok(existing.clone());
    }

    let result = client.download_artifact_to_cache(request)?;
    verify_download_result(&result)?;
    Ok(ThreadArtifactLocalFile {
        path: result.local_path,
        sha256: result.sha256,
        size_bytes: Some(result.size_bytes),
    })
}

pub(crate) fn copy_cached_download_to_destination(
    result: &DesktopArtifactDownloadResult,
    display_name: &str,
    destination_dir: &Path,
) -> Result<ThreadArtifactLocalFile> {
    if !destination_dir.is_dir() {
        bail!(
            "artifact destination `{}` is not a directory",
            destination_dir.display()
        );
    }
    verify_download_result(result)?;

    let final_path = unique_destination_path(destination_dir, display_name)?;
    let part_path = unique_part_path(&final_path)?;
    let copy_result = (|| -> Result<()> {
        if part_path.exists() {
            fs::remove_file(part_path.as_path()).with_context(|| {
                format!(
                    "failed to remove stale artifact download part `{}`",
                    part_path.display()
                )
            })?;
        }
        fs::copy(result.local_path.as_path(), part_path.as_path()).with_context(|| {
            format!(
                "failed to copy artifact download `{}` to `{}`",
                result.local_path.display(),
                part_path.display()
            )
        })?;
        verify_file(
            part_path.as_path(),
            result.sha256.as_str(),
            Some(result.size_bytes),
        )?;
        fs::rename(part_path.as_path(), final_path.as_path()).with_context(|| {
            format!(
                "failed to finalize artifact download `{}`",
                final_path.display()
            )
        })?;
        Ok(())
    })();

    if copy_result.is_err() {
        let _ = fs::remove_file(part_path.as_path());
    }
    copy_result?;

    Ok(ThreadArtifactLocalFile {
        path: final_path,
        sha256: result.sha256.clone(),
        size_bytes: Some(result.size_bytes),
    })
}

pub(crate) fn open_artifact_local_file<O: ArtifactFileOpener>(
    opener: &O,
    path: &Path,
) -> Result<()> {
    if !path.is_file() {
        bail!("artifact local file `{}` does not exist", path.display());
    }
    opener.open_file(path)
}

pub(crate) fn reveal_artifact_local_file<O: ArtifactFileOpener>(
    opener: &O,
    path: &Path,
) -> Result<()> {
    if !path.is_file() {
        bail!("artifact local file `{}` does not exist", path.display());
    }
    opener.reveal_file(path)
}

pub(crate) fn existing_local_file_is_verified(
    local_file: &ThreadArtifactLocalFile,
    artifact: &ArtifactRef,
) -> Result<bool> {
    if !local_file.path.is_file() {
        return Ok(false);
    }
    let expected_sha = artifact
        .sha256
        .as_deref()
        .unwrap_or(local_file.sha256.as_str());
    if expected_sha.trim().is_empty() {
        return Ok(false);
    }
    if let Some(expected_size) = artifact.size_bytes.or(local_file.size_bytes) {
        let actual_size = fs::metadata(local_file.path.as_path())
            .with_context(|| {
                format!(
                    "failed to stat artifact local file `{}`",
                    local_file.path.display()
                )
            })?
            .len();
        if actual_size != expected_size {
            return Ok(false);
        }
    }
    Ok(sha256_file(local_file.path.as_path())? == expected_sha)
}

pub(crate) fn sanitized_artifact_file_name(display_name: &str) -> String {
    let fallback = "artifact";
    let candidate = Path::new(display_name)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(display_name);
    let mut sanitized = String::with_capacity(candidate.len().max(fallback.len()));
    for ch in candidate.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }
    let trimmed = sanitized
        .trim_matches([' ', '\t', '\r', '\n'])
        .trim_matches('.');
    if trimmed.is_empty() {
        fallback.to_owned()
    } else {
        trimmed.to_owned()
    }
}

pub(crate) fn unique_destination_path(
    destination_dir: &Path,
    display_name: &str,
) -> Result<PathBuf> {
    reject_path_traversal(destination_dir)?;
    let safe_name = sanitized_artifact_file_name(display_name);
    let initial = destination_dir.join(safe_name.as_str());
    if !initial.exists() {
        return Ok(initial);
    }

    let safe_path = Path::new(safe_name.as_str());
    let stem = safe_path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("artifact");
    let extension = safe_path.extension().and_then(|value| value.to_str());
    for index in 1..10_000 {
        let candidate_name = if let Some(extension) = extension {
            format!("{stem} ({index}).{extension}")
        } else {
            format!("{stem} ({index})")
        };
        let candidate = destination_dir.join(candidate_name);
        if !candidate.exists() {
            return Ok(candidate);
        }
    }

    bail!("failed to choose a unique artifact destination file name")
}

fn unique_part_path(final_path: &Path) -> Result<PathBuf> {
    let file_name = final_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("artifact destination has no file name"))?;
    Ok(final_path.with_file_name(format!("{file_name}.part")))
}

fn verify_download_result(result: &DesktopArtifactDownloadResult) -> Result<()> {
    verify_file(
        result.local_path.as_path(),
        result.sha256.as_str(),
        Some(result.size_bytes),
    )
}

fn verify_file(path: &Path, expected_sha256: &str, expected_size: Option<u64>) -> Result<()> {
    if !path.is_file() {
        bail!("artifact file `{}` does not exist", path.display());
    }
    if let Some(expected_size) = expected_size {
        let actual_size = fs::metadata(path)
            .with_context(|| format!("failed to stat artifact file `{}`", path.display()))?
            .len();
        if actual_size != expected_size {
            bail!(
                "artifact file size mismatch for `{}`: expected {}, got {}",
                path.display(),
                expected_size,
                actual_size
            );
        }
    }
    let actual_sha256 = sha256_file(path)?;
    if actual_sha256 != expected_sha256 {
        bail!("artifact file sha256 mismatch for `{}`", path.display());
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file =
        fs::File::open(path).with_context(|| format!("failed to open `{}`", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("failed to read `{}`", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn reject_path_traversal(path: &Path) -> Result<()> {
    for component in path.components() {
        if matches!(component, Component::ParentDir) {
            bail!("artifact destination must not contain parent traversal");
        }
    }
    Ok(())
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
    use super::{
        ArtifactDownloadClient, ArtifactFileOpener, copy_cached_download_to_destination,
        ensure_artifact_local_copy_for_open, open_artifact_local_file, reveal_artifact_local_file,
        unique_destination_path,
    };
    use crate::{
        app::root::{ThreadArtifactLocalFile, ThreadArtifactsState},
        gateway::{DesktopArtifactDownloadRequest, DesktopArtifactDownloadResult},
    };
    use anyhow::Result;
    use pioneer_protocol::{ArtifactKind, ArtifactRef, ArtifactStatus};
    use sha2::{Digest, Sha256};
    use std::cell::RefCell;
    use std::{
        fs,
        path::{Path, PathBuf},
    };

    #[derive(Clone)]
    struct FakeDownloadClient {
        result: DesktopArtifactDownloadResult,
        calls: std::rc::Rc<RefCell<usize>>,
    }

    impl ArtifactDownloadClient for FakeDownloadClient {
        fn download_artifact_to_cache(
            &self,
            request: DesktopArtifactDownloadRequest,
        ) -> Result<DesktopArtifactDownloadResult> {
            assert_eq!(request.artifact_id, self.result.artifact.artifact_id);
            *self.calls.borrow_mut() += 1;
            Ok(self.result.clone())
        }
    }

    #[derive(Default)]
    struct FakeOpener {
        opened: RefCell<Vec<PathBuf>>,
        revealed: RefCell<Vec<PathBuf>>,
    }

    impl ArtifactFileOpener for FakeOpener {
        fn open_file(&self, path: &Path) -> Result<()> {
            self.opened.borrow_mut().push(path.to_owned());
            Ok(())
        }

        fn reveal_file(&self, path: &Path) -> Result<()> {
            self.revealed.borrow_mut().push(path.to_owned());
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

        let error = copy_cached_download_to_destination(&result, "report.txt", temp.path())
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

        let local_file = ensure_artifact_local_copy_for_open(
            &client,
            download_request("art_1"),
            &artifact,
            None,
        )
        .expect("local copy");
        let opener = FakeOpener::default();
        open_artifact_local_file(&opener, local_file.path.as_path()).expect("open");

        assert_eq!(*client.calls.borrow(), 1);
        assert_eq!(opener.opened.borrow().as_slice(), &[cache_path]);
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

        let local_file = ensure_artifact_local_copy_for_open(
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

        let error = reveal_artifact_local_file(&opener, missing.as_path())
            .expect_err("missing file should not reveal");

        assert!(error.to_string().contains("does not exist"));
        assert!(opener.revealed.borrow().is_empty());
    }

    #[test]
    fn artifact_file_names_are_sanitized_and_uniqued() {
        let temp = tempfile::tempdir().expect("tempdir");
        fs::write(temp.path().join("report_.txt"), b"existing").expect("write existing");

        let path = unique_destination_path(temp.path(), "../report?.txt").expect("path");

        assert_eq!(
            path.file_name().and_then(|value| value.to_str()),
            Some("report_ (1).txt")
        );
        assert!(path.starts_with(temp.path()));
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

    fn download_artifact_to_destination<C: ArtifactDownloadClient>(
        client: &C,
        request: DesktopArtifactDownloadRequest,
        display_name: &str,
        destination_dir: &Path,
    ) -> Result<ThreadArtifactLocalFile> {
        let result = client.download_artifact_to_cache(request)?;
        copy_cached_download_to_destination(&result, display_name, destination_dir)
    }

    fn download_result(
        local_path: PathBuf,
        bytes: &[u8],
        display_name: &str,
    ) -> DesktopArtifactDownloadResult {
        DesktopArtifactDownloadResult {
            local_path,
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

    fn download_request(artifact_id: &str) -> DesktopArtifactDownloadRequest {
        DesktopArtifactDownloadRequest {
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
