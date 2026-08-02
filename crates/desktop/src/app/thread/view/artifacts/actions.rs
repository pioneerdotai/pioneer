use crate::{
    app::root::{GatewayConnectionState, PioneerDesktop, ThreadArtifactActionStatus},
    gateway::DesktopGatewayHttpClient,
    state as desktop_state,
};
use anyhow::{Context as AnyhowContext, Result};
use gpui::{prelude::*, *};
use pioneer_client::artifacts::{
    actions as client_artifact_actions,
    http_download::{ArtifactHttpDownloadError, ArtifactHttpDownloadProgress},
};
use pioneer_client::platform::{ArtifactFileOpener, ClientPath};
use pioneer_client::rpc::{JsonRpcAuthorizationFailure, json_rpc_authorization_failure};
use pioneer_client::transport::http::{BrowserViewUrl, GatewayHttpError};
use pioneer_client::{ClientError, ClientResult};
use pioneer_protocol::{
    ArtifactRef, ArtifactSummary, ArtifactViewGrantCreateParams,
    ArtifactViewGrantDisposition,
};
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

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

trait ArtifactViewLauncher {
    fn open_view(&self, url: &BrowserViewUrl) -> ClientResult<()>;
}

#[derive(Clone, Copy, Debug)]
struct SystemArtifactViewLauncher;

impl ArtifactViewLauncher for SystemArtifactViewLauncher {
    fn open_view(&self, url: &BrowserViewUrl) -> ClientResult<()> {
        spawn_open_url(url.expose_url())
            .map_err(|error| ClientError::platform(format!("{error:#}")))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::app) enum DesktopArtifactActionError {
    Reconfigure,
    Authentication,
    RevokedOrUnavailable,
    GrantExpired,
    InvalidArtifact,
    DownloadCancelled,
    DownloadIntegrity,
    DiskFull,
    DownloadFailed,
    ViewerFailed,
    LocalCopyInvalid,
}

fn launch_artifact_view(
    launcher: &impl ArtifactViewLauncher,
    url: &BrowserViewUrl,
) -> std::result::Result<(), DesktopArtifactActionError> {
    launcher
        .open_view(url)
        .map_err(|_| DesktopArtifactActionError::ViewerFailed)
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
        let Some(params) = self.artifact_view_grant_params(&summary) else {
            cx.notify();
            return;
        };
        let http_client = match self.active_gateway_http_client() {
            Ok(client) => client,
            Err(error) => {
                let message = desktop_artifact_error_message(error);
                self.thread_artifacts.set_action_status(
                    &summary.artifact,
                    ThreadArtifactActionStatus::Failed(message),
                );
                cx.notify();
                return;
            }
        };
        self.thread_artifacts
            .set_action_status(&summary.artifact, ThreadArtifactActionStatus::Opening);
        cx.notify();

        let ws_sender = self.gateway.ws_command_sender.clone();
        let artifact = summary.artifact.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let view_result = cx
                    .background_spawn(async move { ws_sender.artifact_view_grant_create(params) })
                    .await;
                let open_result = match view_result {
                    Ok(grant) if grant.expires_at > unix_timestamp_secs() => http_client
                        .resolve_view_url(grant.relative_url.as_str())
                        .map_err(map_gateway_http_error)
                        .and_then(|url| {
                            launch_artifact_view(&SystemArtifactViewLauncher, &url)
                        }),
                    Ok(_) => Err(DesktopArtifactActionError::GrantExpired),
                    Err(error) => Err(map_view_grant_error(&error)),
                };

                let _ = this.update(&mut cx, |view, cx| {
                    match open_result {
                        Ok(()) => view.thread_artifacts.clear_action_status(&artifact),
                        Err(error) => view.mark_thread_artifact_action_failed(
                            &artifact,
                            desktop_artifact_error_message(error),
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
        if self.thread_artifacts.action_in_progress(&artifact) {
            return;
        }
        let Some(local_file) = self.thread_artifacts.local_file(&artifact).cloned() else {
            return;
        };

        self.thread_artifacts
            .set_action_status(&artifact, ThreadArtifactActionStatus::Verifying);
        cx.notify();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let local_file_for_verify = local_file.clone();
                let artifact_for_verify = artifact.clone();
                let verified = cx
                    .background_spawn(async move {
                        client_artifact_actions::existing_local_file_is_verified(
                            &local_file_for_verify,
                            &artifact_for_verify,
                        )
                    })
                    .await;
                let artifact_for_result = artifact.clone();
                let local_file = match verified {
                    Ok(true) => {
                        let updated = this
                            .update(&mut cx, |view, cx| {
                                view.thread_artifacts.set_action_status(
                                    &artifact,
                                    ThreadArtifactActionStatus::Revealing,
                                );
                                cx.notify();
                            })
                            .is_ok();
                        if !updated {
                            return;
                        }
                        local_file
                    }
                    Ok(false) | Err(_) => {
                        let _ = this.update(&mut cx, |view, cx| {
                            view.thread_artifacts.clear_local_file(&artifact);
                            view.mark_thread_artifact_action_failed(
                                &artifact,
                                desktop_artifact_error_message(
                                    DesktopArtifactActionError::LocalCopyInvalid,
                                ),
                                cx,
                            );
                        });
                        return;
                    }
                };
                let artifact_for_reveal = artifact.clone();
                let reveal_result = cx
                    .background_spawn(async move {
                        reveal_verified_local_file(
                            &SystemArtifactFileOpener,
                            &local_file,
                            &artifact_for_reveal,
                        )
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    match reveal_result {
                        Ok(()) => view
                            .thread_artifacts
                            .clear_action_status(&artifact_for_result),
                        Err(_error) => view.mark_thread_artifact_action_failed(
                            &artifact_for_result,
                            desktop_artifact_error_message(
                                DesktopArtifactActionError::ViewerFailed,
                            ),
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
        let Some(request) = self.artifact_http_download_request(&summary) else {
            cx.notify();
            return;
        };
        let http_client = match self.active_gateway_http_client() {
            Ok(client) => client,
            Err(error) => {
                self.mark_thread_artifact_action_failed(
                    &summary.artifact,
                    desktop_artifact_error_message(error),
                    cx,
                );
                return;
            }
        };
        let total_bytes = request.expected_size_bytes;
        let artifact = summary.artifact.clone();
        let artifact_key = client_artifact_actions::artifact_version_key(&artifact);
        let cancellation = CancellationToken::new();
        self.artifact_download_cancellations
            .insert(artifact_key.clone(), cancellation.clone());

        self.thread_artifacts.set_action_status(
            &artifact,
            ThreadArtifactActionStatus::Downloading {
                downloaded_bytes: 0,
                total_bytes,
            },
        );
        cx.notify();

        let display_name = summary.artifact.display_name.clone();
        let downloaded_bytes = Arc::new(AtomicU64::new(0));
        let download_finished = Arc::new(AtomicBool::new(false));
        self.poll_thread_artifact_download_progress(
            artifact.clone(),
            artifact_key.clone(),
            total_bytes,
            downloaded_bytes.clone(),
            download_finished.clone(),
            cx,
        );

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let progress_bytes = downloaded_bytes.clone();
                let cache_result = cx
                    .background_spawn(async move {
                        let progress = move |update: ArtifactHttpDownloadProgress| {
                            progress_bytes.store(update.downloaded_bytes, Ordering::Relaxed);
                        };
                        http_client.download(request, cancellation, Some(&progress))
                    })
                    .await;
                download_finished.store(true, Ordering::Release);

                let cache_result = match cache_result {
                    Ok(cache_result) => cache_result,
                    Err(error) => {
                        let _ = this.update(&mut cx, |view, cx| {
                            view.artifact_download_cancellations.remove(&artifact_key);
                            view.mark_thread_artifact_action_failed(
                                &artifact,
                                desktop_artifact_error_message(map_download_error(error)),
                                cx,
                            );
                        });
                        return;
                    }
                };

                let artifact_for_state = artifact.clone();
                let _ = this.update(&mut cx, |view, cx| {
                    view.artifact_download_cancellations.remove(&artifact_key);
                    view.thread_artifacts.set_action_status(
                        &artifact_for_state,
                        ThreadArtifactActionStatus::Verifying,
                    );
                    cx.notify();
                });

                let local_result = cx
                    .background_spawn(async move {
                        client_artifact_actions::copy_http_download_result_to_destination(
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
                        Err(_error) => {
                            view.mark_thread_artifact_action_failed(
                                &artifact,
                                desktop_artifact_error_message(
                                    DesktopArtifactActionError::DownloadFailed,
                                ),
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

    pub(in crate::app) fn cancel_thread_artifact_download(
        &mut self,
        artifact: ArtifactRef,
        cx: &mut Context<Self>,
    ) {
        let key = client_artifact_actions::artifact_version_key(&artifact);
        if let Some(cancellation) = self.artifact_download_cancellations.get(&key) {
            cancellation.cancel();
            self.thread_artifacts.set_action_status(
                &artifact,
                ThreadArtifactActionStatus::Failed(desktop_artifact_error_message(
                    DesktopArtifactActionError::DownloadCancelled,
                )),
            );
            cx.notify();
        }
    }

    fn poll_thread_artifact_download_progress(
        &self,
        artifact: ArtifactRef,
        artifact_key: client_artifact_actions::ArtifactVersionKey,
        total_bytes: u64,
        downloaded_bytes: Arc<AtomicU64>,
        download_finished: Arc<AtomicBool>,
        cx: &mut Context<Self>,
    ) {
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                loop {
                    cx.background_executor()
                        .timer(Duration::from_millis(100))
                        .await;
                    if download_finished.load(Ordering::Acquire) {
                        return;
                    }
                    let current = downloaded_bytes.load(Ordering::Relaxed).min(total_bytes);
                    let still_active = this
                        .update(&mut cx, |view, cx| {
                            if !view
                                .artifact_download_cancellations
                                .contains_key(&artifact_key)
                            {
                                return false;
                            }
                            view.thread_artifacts.set_action_status(
                                &artifact,
                                ThreadArtifactActionStatus::Downloading {
                                    downloaded_bytes: current,
                                    total_bytes,
                                },
                            );
                            cx.notify();
                            true
                        })
                        .unwrap_or(false);
                    if !still_active {
                        return;
                    }
                }
            }
        })
        .detach();
    }

    fn artifact_action_can_download(&mut self, summary: &ArtifactSummary) -> bool {
        let artifact_key = client_artifact_actions::artifact_version_key(&summary.artifact);
        let action_in_progress = self.thread_artifacts.action_in_progress(&summary.artifact)
            || self
                .artifact_download_cancellations
                .contains_key(&artifact_key);
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

    fn artifact_http_download_request(
        &mut self,
        summary: &ArtifactSummary,
    ) -> Option<pioneer_client::artifacts::http_download::ArtifactHttpDownloadRequest> {
        let gateway_profile_id = self
            .gateway
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.active_gateway().map(|gateway| gateway.id.clone()));
        match client_artifact_actions::plan_artifact_http_download_request(
            gateway_profile_id,
            summary,
        ) {
            Ok(request) => Some(request),
            Err(
                client_artifact_actions::ArtifactHttpDownloadRequestPlanError::MissingGatewayProfile,
            ) => {
                self.thread_artifacts.set_action_status(
                    &summary.artifact,
                    ThreadArtifactActionStatus::Failed(
                        t!("artifacts.action.error.no_gateway").to_string(),
                    ),
                );
                None
            }
            Err(client_artifact_actions::ArtifactHttpDownloadRequestPlanError::MissingWorkspaceId) => {
                self.thread_artifacts.set_action_status(
                    &summary.artifact,
                    ThreadArtifactActionStatus::Failed(
                        t!("artifacts.action.error.no_workspace").to_string(),
                    ),
                );
                None
            }
            Err(
                client_artifact_actions::ArtifactHttpDownloadRequestPlanError::MissingVersionId
                | client_artifact_actions::ArtifactHttpDownloadRequestPlanError::MissingSize
                | client_artifact_actions::ArtifactHttpDownloadRequestPlanError::MissingSha256,
            ) => {
                self.thread_artifacts.set_action_status(
                    &summary.artifact,
                    ThreadArtifactActionStatus::Failed(desktop_artifact_error_message(
                        DesktopArtifactActionError::InvalidArtifact,
                    )),
                );
                None
            }
        }
    }

    fn artifact_view_grant_params(
        &mut self,
        summary: &ArtifactSummary,
    ) -> Option<ArtifactViewGrantCreateParams> {
        let version_id = summary
            .artifact
            .version_id
            .clone()
            .filter(|value| !value.trim().is_empty());
        if summary.workspace_id.trim().is_empty() || version_id.is_none() {
            self.thread_artifacts.set_action_status(
                &summary.artifact,
                ThreadArtifactActionStatus::Failed(desktop_artifact_error_message(
                    DesktopArtifactActionError::InvalidArtifact,
                )),
            );
            return None;
        }
        Some(ArtifactViewGrantCreateParams {
            workspace_id: summary.workspace_id.clone(),
            artifact_id: summary.artifact.artifact_id.clone(),
            version_id: version_id.expect("checked exact artifact version"),
            projection_kind: None,
            disposition: ArtifactViewGrantDisposition::Inline,
        })
    }

    pub(in crate::app) fn active_gateway_http_client(
        &mut self,
    ) -> std::result::Result<DesktopGatewayHttpClient, DesktopArtifactActionError> {
        let endpoint = self
            .gateway
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.active_gateway().cloned())
            .ok_or(DesktopArtifactActionError::Reconfigure)?;
        let access = self
            .gateway
            .ws_command_sender
            .current_gateway_http_access()
            .map_err(|_| DesktopArtifactActionError::Authentication)?;
        if let Some(client) = self.gateway.http_client.as_ref()
            && client.matches(&endpoint, &access)
        {
            return Ok(client.clone());
        }
        let runtime_home = desktop_state::runtime_home_dir()
            .map_err(|_| DesktopArtifactActionError::DownloadFailed)?;
        let client = DesktopGatewayHttpClient::for_endpoint(
            &endpoint,
            self.gateway.ws_command_sender.clone(),
            runtime_home,
        )
        .map_err(map_gateway_http_error)?;
        self.gateway.http_client = Some(client.clone());
        Ok(client)
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

fn reveal_verified_local_file(
    opener: &impl ArtifactFileOpener,
    local_file: &client_artifact_actions::ArtifactLocalFile,
    artifact: &ArtifactRef,
) -> std::result::Result<(), DesktopArtifactActionError> {
    if !client_artifact_actions::existing_local_file_is_verified(local_file, artifact)
        .map_err(|_| DesktopArtifactActionError::LocalCopyInvalid)?
    {
        return Err(DesktopArtifactActionError::LocalCopyInvalid);
    }
    client_artifact_actions::reveal_artifact_local_file(opener, local_file.path.as_path())
        .map_err(|_| DesktopArtifactActionError::ViewerFailed)
}

fn map_view_grant_error(error: &anyhow::Error) -> DesktopArtifactActionError {
    match json_rpc_authorization_failure(error) {
        Some(JsonRpcAuthorizationFailure::AuthenticationTerminal) => {
            DesktopArtifactActionError::Authentication
        }
        Some(
            JsonRpcAuthorizationFailure::Forbidden
            | JsonRpcAuthorizationFailure::InaccessibleResource,
        ) => DesktopArtifactActionError::RevokedOrUnavailable,
        None => DesktopArtifactActionError::DownloadFailed,
    }
}

fn map_gateway_http_error(error: GatewayHttpError) -> DesktopArtifactActionError {
    match error {
        GatewayHttpError::InvalidEndpoint
        | GatewayHttpError::GatewayPinMismatch
        | GatewayHttpError::SessionMismatch => DesktopArtifactActionError::Reconfigure,
        GatewayHttpError::AuthenticationTerminal(_)
        | GatewayHttpError::AuthenticationUnavailable
        | GatewayHttpError::Unauthorized => DesktopArtifactActionError::Authentication,
        GatewayHttpError::Forbidden | GatewayHttpError::NotFound => {
            DesktopArtifactActionError::RevokedOrUnavailable
        }
        GatewayHttpError::Cancelled => DesktopArtifactActionError::DownloadCancelled,
        GatewayHttpError::InvalidStoragePath
        | GatewayHttpError::InvalidHeader
        | GatewayHttpError::InvalidResponse
        | GatewayHttpError::Conflict
        | GatewayHttpError::RangeNotSatisfiable => DesktopArtifactActionError::InvalidArtifact,
        GatewayHttpError::Transport
        | GatewayHttpError::TooManyRequests
        | GatewayHttpError::ServiceUnavailable
        | GatewayHttpError::Server => DesktopArtifactActionError::DownloadFailed,
    }
}

fn map_download_error(error: ArtifactHttpDownloadError) -> DesktopArtifactActionError {
    match error {
        ArtifactHttpDownloadError::InvalidRequest
        | ArtifactHttpDownloadError::InvalidResponse => DesktopArtifactActionError::InvalidArtifact,
        ArtifactHttpDownloadError::Authentication => DesktopArtifactActionError::Authentication,
        ArtifactHttpDownloadError::RevokedOrUnavailable => {
            DesktopArtifactActionError::RevokedOrUnavailable
        }
        ArtifactHttpDownloadError::Integrity => DesktopArtifactActionError::DownloadIntegrity,
        ArtifactHttpDownloadError::DiskFull => DesktopArtifactActionError::DiskFull,
        ArtifactHttpDownloadError::Cancelled => DesktopArtifactActionError::DownloadCancelled,
        ArtifactHttpDownloadError::Transport | ArtifactHttpDownloadError::DiskWrite => {
            DesktopArtifactActionError::DownloadFailed
        }
    }
}

fn desktop_artifact_error_message(error: DesktopArtifactActionError) -> String {
    match error {
        DesktopArtifactActionError::Reconfigure => {
            t!("artifacts.action.error.reconfigure").to_string()
        }
        DesktopArtifactActionError::Authentication => {
            t!("artifacts.action.error.authentication").to_string()
        }
        DesktopArtifactActionError::RevokedOrUnavailable => {
            t!("artifacts.action.error.revoked").to_string()
        }
        DesktopArtifactActionError::GrantExpired => {
            t!("artifacts.action.error.grant_expired").to_string()
        }
        DesktopArtifactActionError::InvalidArtifact => {
            t!("artifacts.action.error.invalid_artifact").to_string()
        }
        DesktopArtifactActionError::DownloadCancelled => {
            t!("artifacts.action.error.cancelled").to_string()
        }
        DesktopArtifactActionError::DownloadIntegrity => {
            t!("artifacts.action.error.integrity").to_string()
        }
        DesktopArtifactActionError::DiskFull => {
            t!("artifacts.action.error.disk_full").to_string()
        }
        DesktopArtifactActionError::DownloadFailed => {
            t!("artifacts.action.error.download_failed").to_string()
        }
        DesktopArtifactActionError::ViewerFailed => {
            t!("artifacts.action.error.viewer_failed").to_string()
        }
        DesktopArtifactActionError::LocalCopyInvalid => {
            t!("artifacts.action.error.local_copy_invalid").to_string()
        }
    }
}

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(target_os = "macos")]
fn spawn_open_url(url: &str) -> Result<()> {
    spawn_command(Command::new("open").arg(url), "open artifact view")
}

#[cfg(all(unix, not(target_os = "macos")))]
fn spawn_open_url(url: &str) -> Result<()> {
    spawn_command(Command::new("xdg-open").arg(url), "open artifact view")
}

#[cfg(windows)]
fn spawn_open_url(url: &str) -> Result<()> {
    shell_open(std::ffi::OsStr::new(url), "open artifact view")
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
    shell_open(path.as_os_str(), "open artifact file")
}

#[cfg(windows)]
fn spawn_reveal_file(path: &Path) -> Result<()> {
    spawn_command(
        Command::new("explorer").arg("/select,").arg(path),
        "reveal artifact file",
    )
}

#[cfg(windows)]
fn shell_open(target: &std::ffi::OsStr, action: &str) -> Result<()> {
    use std::os::windows::ffi::OsStrExt as _;
    use windows_sys::Win32::UI::Shell::ShellExecuteW;
    use windows_sys::Win32::UI::WindowsAndMessaging::SW_SHOWNORMAL;

    let mut target = target.encode_wide().collect::<Vec<_>>();
    if target.contains(&0) {
        anyhow::bail!("failed to {action}: target contains a NUL character");
    }
    target.push(0);
    let operation = "open\0".encode_utf16().collect::<Vec<_>>();
    let result = unsafe {
        ShellExecuteW(
            std::ptr::null_mut(),
            operation.as_ptr(),
            target.as_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            SW_SHOWNORMAL,
        )
    };
    if result as isize <= 32 {
        anyhow::bail!("failed to {action}: ShellExecuteW returned {result:?}");
    }
    Ok(())
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
        ArtifactFileOpener, ArtifactRef, ArtifactViewLauncher, BrowserViewUrl,
        ClientPath, ClientResult, DesktopArtifactActionError,
        desktop_artifact_error_message, launch_artifact_view,
        reveal_verified_local_file,
    };
    use pioneer_client::{
        artifacts::actions::ArtifactLocalFile,
        gateway::endpoint::GatewayBaseUrl,
    };
    use pioneer_protocol::{ArtifactKind, ArtifactStatus};
    use sha2::{Digest, Sha256};
    use std::{
        fs,
        sync::atomic::{AtomicUsize, Ordering},
    };

    #[derive(Default)]
    struct FakeViewLauncher {
        calls: AtomicUsize,
    }

    impl ArtifactViewLauncher for FakeViewLauncher {
        fn open_view(&self, url: &BrowserViewUrl) -> ClientResult<()> {
            assert!(url.expose_url().starts_with("https://relay.test/storage/views/"));
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeFileOpener {
        open_calls: AtomicUsize,
        reveal_calls: AtomicUsize,
    }

    impl ArtifactFileOpener for FakeFileOpener {
        fn open_file(&self, _path: &ClientPath) -> ClientResult<()> {
            self.open_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        fn reveal_file(&self, _path: &ClientPath) -> ClientResult<()> {
            self.reveal_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[test]
    fn open_uses_ephemeral_view_url_and_injected_launcher() {
        let base = GatewayBaseUrl::parse_presentation("https://relay.test").expect("base URL");
        let token = "a".repeat(43);
        let view_url = BrowserViewUrl::resolve(&base, format!("/storage/views/{token}").as_str())
            .expect("view URL");
        let launcher = FakeViewLauncher::default();

        launch_artifact_view(&launcher, &view_url).expect("launch through fake port");

        assert_eq!(launcher.calls.load(Ordering::Relaxed), 1);
        assert_eq!(format!("{view_url:?}"), "BrowserViewUrl([redacted])");
    }

    #[test]
    fn reveal_accepts_only_a_verified_downloaded_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("artifact.txt");
        let bytes = b"verified artifact";
        fs::write(path.as_path(), bytes).expect("write artifact");
        let digest = sha256(bytes);
        let artifact = artifact_ref(bytes.len() as u64, digest.clone());
        let local_file = ArtifactLocalFile {
            path: path.clone(),
            sha256: digest,
            size_bytes: Some(bytes.len() as u64),
        };
        let opener = FakeFileOpener::default();

        reveal_verified_local_file(&opener, &local_file, &artifact).expect("verified reveal");
        assert_eq!(opener.reveal_calls.load(Ordering::Relaxed), 1);
        assert_eq!(opener.open_calls.load(Ordering::Relaxed), 0);

        fs::write(path, b"tampered artifact").expect("tamper artifact");
        assert_eq!(
            reveal_verified_local_file(&opener, &local_file, &artifact),
            Err(DesktopArtifactActionError::LocalCopyInvalid)
        );
        assert_eq!(opener.reveal_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn desktop_artifact_actions_have_no_ws_download_or_open_fallback() {
        let source = include_str!("actions.rs");
        assert!(!source.contains(&["ensure_artifact_local_copy", "_for_open("].concat()));
        assert!(!source.contains(&["download_artifact", "_to_cache("].concat()));
        assert!(!source.contains(&["ArtifactDownload", "Request"].concat()));
        assert!(
            !source.contains("Command::new(\"cmd\")"),
            "artifact URLs and paths must never pass through cmd.exe"
        );
        assert!(source.contains("artifact_view_grant_create(params)"));
        assert!(source.contains("DesktopGatewayHttpClient"));
    }

    #[test]
    fn user_facing_failures_are_typed_and_secret_free() {
        for error in [
            DesktopArtifactActionError::Reconfigure,
            DesktopArtifactActionError::Authentication,
            DesktopArtifactActionError::RevokedOrUnavailable,
            DesktopArtifactActionError::GrantExpired,
        ] {
            let message = desktop_artifact_error_message(error);
            assert!(!message.trim().is_empty());
            assert!(!message.contains("Bearer"));
            assert!(!message.contains("storage/views"));
        }
    }

    fn artifact_ref(size_bytes: u64, digest: String) -> ArtifactRef {
        ArtifactRef {
            artifact_id: "artifact-1".to_owned(),
            version_id: Some("version-1".to_owned()),
            display_name: "artifact.txt".to_owned(),
            kind: ArtifactKind::Text,
            mime_type: Some("text/plain".to_owned()),
            size_bytes: Some(size_bytes),
            sha256: Some(digest),
            status: ArtifactStatus::Ready,
            preview: None,
        }
    }

    fn sha256(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        hex::encode(hasher.finalize())
    }
}
