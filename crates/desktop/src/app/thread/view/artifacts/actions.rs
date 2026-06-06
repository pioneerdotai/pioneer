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
