use super::archive::build_skill_upload_archive;
use crate::{
    app::root::{GatewayConnectionState, PioneerDesktop, SkillUploadProgress},
    gateway::GatewayWsCommandSender,
};
use anyhow::{Context as AnyhowContext, Result, anyhow, bail};
use gpui::{prelude::*, *};
use pioneer_protocol::{
    SkillArchiveFormat, SkillLifecycleSource, SkillsInstallParams, SkillsPolicySetParams,
    SkillsUninstallParams, SkillsUpdateParams, SkillsUploadAbortParams, SkillsUploadFinishParams,
    SkillsUploadStartParams,
};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tracing::warn;

impl PioneerDesktop {
    pub(super) fn install_skill_from_path(&mut self, source_path: String, cx: &mut Context<Self>) {
        let source_path = source_path.trim().to_owned();
        if source_path.is_empty() {
            self.skills_error = Some(t!("skills.error.path_required").to_string());
            return;
        }

        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.skills_error = Some(t!("skills.error.gateway_not_connected").to_string());
            return;
        }

        let Some(connection_id) = self.gateway.ws_connection_id else {
            self.skills_error = Some(t!("skills.error.gateway_not_connected").to_string());
            return;
        };

        let Some(workspace_id) = self.skills_workspace_scope() else {
            self.skills_error = Some(t!("skills.error.workspace_not_selected").to_string());
            return;
        };

        self.skills_loading = true;
        self.skills_error = None;
        let cancel_token = Arc::new(AtomicBool::new(false));
        self.skills_upload_cancel_token = Some(cancel_token.clone());
        self.skills_upload_progress = Some(SkillUploadProgress {
            label: t!("skills.upload.preparing").to_string(),
            sent_bytes: 0,
            total_bytes: 0,
        });

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let upload_workspace_id = workspace_id.clone();
            let upload_source_path = source_path.clone();
            let upload_cancel_token = cancel_token.clone();
            async move {
                let result: Result<()> = async {
                    let upload_id = upload_selected_skill_directory(
                        this.clone(),
                        &mut cx,
                        ws_sender.clone(),
                        upload_workspace_id,
                        upload_source_path,
                        t!("skills.upload.installing").to_string(),
                        upload_cancel_token,
                    )
                    .await?;

                    cx.background_spawn(async move {
                        ws_sender.skills_install(SkillsInstallParams {
                            workspace_id,
                            source: SkillLifecycleSource::UploadedArchive { upload_id },
                            target_source_kind: "user".to_owned(),
                        })
                    })
                    .await?;
                    Ok(())
                }
                .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    view.skills_loading = false;
                    view.skills_upload_progress = None;
                    view.skills_upload_cancel_token = None;
                    match result {
                        Ok(_) => {
                            view.skills_error = None;
                            view.queue_skills_refresh();
                        }
                        Err(error) => {
                            view.skills_error =
                                Some(format!("{}: {error:#}", t!("skills.error.install_failed")));
                            warn!(error = %format!("{error:#}"), "failed to install skill");
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn update_skill_from_path(
        &mut self,
        slug: String,
        source_kind: String,
        source_path: String,
        cx: &mut Context<Self>,
    ) {
        let slug = slug.trim().to_owned();
        let source_kind = source_kind.trim().to_owned();
        let source_path = source_path.trim().to_owned();

        if slug.is_empty() || source_kind.is_empty() {
            self.skills_error = Some(t!("skills.error.invalid_skill_target").to_string());
            return;
        }
        if source_path.is_empty() {
            self.skills_error = Some(t!("skills.error.path_required").to_string());
            return;
        }
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.skills_error = Some(t!("skills.error.gateway_not_connected").to_string());
            return;
        }

        let Some(connection_id) = self.gateway.ws_connection_id else {
            self.skills_error = Some(t!("skills.error.gateway_not_connected").to_string());
            return;
        };
        let Some(workspace_id) = self.skills_workspace_scope() else {
            self.skills_error = Some(t!("skills.error.workspace_not_selected").to_string());
            return;
        };

        self.skills_error = None;
        let cancel_token = Arc::new(AtomicBool::new(false));
        self.skills_upload_cancel_token = Some(cancel_token.clone());
        self.skills_upload_progress = Some(SkillUploadProgress {
            label: t!("skills.upload.preparing").to_string(),
            sent_bytes: 0,
            total_bytes: 0,
        });
        self.mark_skill_pending(slug.as_str(), source_kind.as_str(), true);

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let slug_for_request = slug.clone();
            let source_kind_for_request = source_kind.clone();
            let upload_workspace_id = workspace_id.clone();
            let upload_source_path = source_path.clone();
            let upload_cancel_token = cancel_token.clone();
            async move {
                let result: Result<()> = async {
                    let upload_id = upload_selected_skill_directory(
                        this.clone(),
                        &mut cx,
                        ws_sender.clone(),
                        upload_workspace_id,
                        upload_source_path,
                        t!("skills.upload.updating").to_string(),
                        upload_cancel_token,
                    )
                    .await?;

                    cx.background_spawn(async move {
                        ws_sender.skills_update(SkillsUpdateParams {
                            workspace_id,
                            slug: slug_for_request,
                            source_kind: source_kind_for_request,
                            source: SkillLifecycleSource::UploadedArchive { upload_id },
                            expected_previous_fingerprint: None,
                        })
                    })
                    .await?;
                    Ok(())
                }
                .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    view.skills_upload_progress = None;
                    view.skills_upload_cancel_token = None;
                    view.mark_skill_pending(slug.as_str(), source_kind.as_str(), false);
                    match result {
                        Ok(_) => {
                            view.skills_error = None;
                            view.queue_skills_refresh();
                        }
                        Err(error) => {
                            view.skills_error =
                                Some(format!("{}: {error:#}", t!("skills.error.update_failed")));
                            warn!(
                                slug = slug.as_str(),
                                source_kind = source_kind.as_str(),
                                error = %format!("{error:#}"),
                                "failed to update skill"
                            );
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn uninstall_skill(
        &mut self,
        slug: String,
        source_kind: String,
        cx: &mut Context<Self>,
    ) {
        let slug = slug.trim().to_owned();
        let source_kind = source_kind.trim().to_owned();
        if slug.is_empty() || source_kind.is_empty() {
            self.skills_error = Some(t!("skills.error.invalid_skill_target").to_string());
            return;
        }
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.skills_error = Some(t!("skills.error.gateway_not_connected").to_string());
            return;
        }

        let Some(connection_id) = self.gateway.ws_connection_id else {
            self.skills_error = Some(t!("skills.error.gateway_not_connected").to_string());
            return;
        };
        let Some(workspace_id) = self.skills_workspace_scope() else {
            self.skills_error = Some(t!("skills.error.workspace_not_selected").to_string());
            return;
        };

        self.skills_error = None;
        self.mark_skill_pending(slug.as_str(), source_kind.as_str(), true);

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let slug_for_request = slug.clone();
            let source_kind_for_request = source_kind.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.skills_uninstall(SkillsUninstallParams {
                            workspace_id,
                            slug: slug_for_request,
                            source_kind: source_kind_for_request,
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    view.mark_skill_pending(slug.as_str(), source_kind.as_str(), false);
                    match result {
                        Ok(_) => {
                            view.skills_error = None;
                            view.queue_skills_refresh();
                        }
                        Err(error) => {
                            view.skills_error = Some(format!(
                                "{}: {error:#}",
                                t!("skills.error.uninstall_failed")
                            ));
                            warn!(
                                slug = slug.as_str(),
                                source_kind = source_kind.as_str(),
                                error = %format!("{error:#}"),
                                "failed to uninstall skill"
                            );
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn set_skill_policy(
        &mut self,
        slug: String,
        source_kind: String,
        enabled: bool,
        allow_implicit_invocation: bool,
        cx: &mut Context<Self>,
    ) {
        let slug = slug.trim().to_owned();
        let source_kind = source_kind.trim().to_owned();

        if slug.is_empty() || source_kind.is_empty() {
            self.skills_error = Some(t!("skills.error.invalid_skill_target").to_string());
            return;
        }
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.skills_error = Some(t!("skills.error.gateway_not_connected").to_string());
            return;
        }

        let Some(connection_id) = self.gateway.ws_connection_id else {
            self.skills_error = Some(t!("skills.error.gateway_not_connected").to_string());
            return;
        };
        let Some(workspace_id) = self.skills_workspace_scope() else {
            self.skills_error = Some(t!("skills.error.workspace_not_selected").to_string());
            return;
        };

        let previous_policy = self.skill_policy_values(slug.as_str(), source_kind.as_str());

        self.skills_error = None;
        self.mark_skill_pending(slug.as_str(), source_kind.as_str(), true);
        self.apply_local_skill_policy(
            slug.as_str(),
            source_kind.as_str(),
            enabled,
            allow_implicit_invocation,
        );

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let slug_for_request = slug.clone();
            let source_kind_for_request = source_kind.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.skills_policy_set(SkillsPolicySetParams {
                            workspace_id,
                            skill_slug: slug_for_request,
                            source_kind: source_kind_for_request,
                            enabled: Some(enabled),
                            allow_implicit_invocation: Some(allow_implicit_invocation),
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    view.mark_skill_pending(slug.as_str(), source_kind.as_str(), false);
                    match result {
                        Ok(_) => {
                            view.skills_error = None;
                            view.queue_skills_refresh();
                        }
                        Err(error) => {
                            if let Some((prev_enabled, prev_implicit)) = previous_policy {
                                view.apply_local_skill_policy(
                                    slug.as_str(),
                                    source_kind.as_str(),
                                    prev_enabled,
                                    prev_implicit,
                                );
                            }

                            view.skills_error =
                                Some(format!("{}: {error:#}", t!("skills.error.policy_failed")));
                            warn!(
                                slug = slug.as_str(),
                                source_kind = source_kind.as_str(),
                                error = %format!("{error:#}"),
                                "failed to set skill policy"
                            );
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn skill_policy_values(&self, slug: &str, source_kind: &str) -> Option<(bool, bool)> {
        self.skills_catalog
            .iter()
            .find(|skill| skill.slug == slug && skill.source_kind == source_kind)
            .map(|skill| (skill.policy.enabled, skill.policy.allow_implicit_invocation))
    }

    fn apply_local_skill_policy(
        &mut self,
        slug: &str,
        source_kind: &str,
        enabled: bool,
        allow_implicit_invocation: bool,
    ) {
        for skill in &mut self.skills_catalog {
            if skill.slug == slug && skill.source_kind == source_kind {
                skill.policy.enabled = enabled;
                skill.policy.allow_implicit_invocation = allow_implicit_invocation;
                skill.status = if enabled {
                    if skill.health.status == "blocked" {
                        "blocked".to_owned()
                    } else {
                        "active".to_owned()
                    }
                } else {
                    "disabled".to_owned()
                };
            }
        }

        for skill in &mut self.installed_skills {
            if skill.slug == slug && skill.source_kind == source_kind {
                skill.policy.enabled = enabled;
                skill.policy.allow_implicit_invocation = allow_implicit_invocation;
                skill.status = if enabled {
                    if skill.health.status == "blocked" {
                        "blocked".to_owned()
                    } else {
                        "active".to_owned()
                    }
                } else {
                    "disabled".to_owned()
                };
            }
        }
    }

    pub(super) fn cancel_skill_upload(&mut self, cx: &mut Context<Self>) {
        if let Some(cancel_token) = self.skills_upload_cancel_token.as_ref() {
            cancel_token.store(true, Ordering::Relaxed);
        }
        if let Some(progress) = self.skills_upload_progress.as_mut() {
            progress.label = t!("skills.upload.cancelling").to_string();
        }
        cx.notify();
    }
}

async fn upload_selected_skill_directory(
    this: WeakEntity<PioneerDesktop>,
    cx: &mut AsyncApp,
    ws_sender: GatewayWsCommandSender,
    workspace_id: String,
    source_path: String,
    progress_label: String,
    cancel_token: Arc<AtomicBool>,
) -> Result<String> {
    let source_path = PathBuf::from(source_path);
    let archive = cx
        .background_spawn(async move { build_skill_upload_archive(source_path.as_path()) })
        .await?;
    let total_bytes = u64::try_from(archive.bytes.len()).context("skill archive size overflow")?;

    update_skill_upload_progress(
        &this,
        cx,
        SkillUploadProgress {
            label: progress_label.clone(),
            sent_bytes: 0,
            total_bytes,
        },
    );
    ensure_upload_not_cancelled(cancel_token.as_ref())?;

    let start = {
        let ws_sender = ws_sender.clone();
        let workspace_id = workspace_id.clone();
        let archive_file_name = archive.file_name.clone();
        let archive_sha256 = archive.sha256.clone();
        let uncompressed_size = archive.uncompressed_size_bytes;
        cx.background_spawn(async move {
            ws_sender.skills_upload_start(SkillsUploadStartParams {
                workspace_id,
                file_name: archive_file_name,
                archive_format: SkillArchiveFormat::TarGz,
                compressed_size_bytes: total_bytes,
                uncompressed_size_hint_bytes: Some(uncompressed_size),
                sha256: archive_sha256,
            })
        })
        .await?
    };

    let upload_id = start.upload_id.clone();
    let chunk_size = usize::try_from(
        start
            .recommended_chunk_size_bytes
            .min(start.max_chunk_size_bytes)
            .max(1),
    )
    .context("gateway upload chunk size overflow")?;

    let upload_result: Result<()> = async {
        let mut offset = 0usize;
        while offset < archive.bytes.len() {
            ensure_upload_not_cancelled(cancel_token.as_ref())?;
            let end = offset.saturating_add(chunk_size).min(archive.bytes.len());
            let chunk = archive.bytes[offset..end].to_vec();
            let chunk_offset = u64::try_from(offset).context("skill upload offset overflow")?;
            let ack = {
                let ws_sender = ws_sender.clone();
                let workspace_id = workspace_id.clone();
                let upload_id = upload_id.clone();
                cx.background_spawn(async move {
                    ws_sender.send_skill_upload_chunk(workspace_id, upload_id, chunk_offset, chunk)
                })
                .await?
            };

            if ack.next_offset != u64::try_from(end).context("skill upload next offset overflow")? {
                bail!(
                    "gateway acknowledged unexpected skill upload offset {}",
                    ack.next_offset
                );
            }
            offset = end;
            update_skill_upload_progress(
                &this,
                cx,
                SkillUploadProgress {
                    label: progress_label.clone(),
                    sent_bytes: u64::try_from(offset).context("skill upload progress overflow")?,
                    total_bytes,
                },
            );
        }

        ensure_upload_not_cancelled(cancel_token.as_ref())?;
        let finish = {
            let ws_sender = ws_sender.clone();
            let workspace_id = workspace_id.clone();
            let upload_id = upload_id.clone();
            cx.background_spawn(async move {
                ws_sender.skills_upload_finish(SkillsUploadFinishParams {
                    workspace_id,
                    upload_id,
                })
            })
            .await?
        };
        if finish.status != "finalized" {
            bail!(
                "gateway returned unexpected skill upload status {}",
                finish.status
            );
        }
        Ok(())
    }
    .await;

    if let Err(error) = upload_result {
        let _ = cx
            .background_spawn({
                let ws_sender = ws_sender.clone();
                let workspace_id = workspace_id.clone();
                let upload_id = upload_id.clone();
                async move {
                    ws_sender.skills_upload_abort(SkillsUploadAbortParams {
                        workspace_id,
                        upload_id,
                    })
                }
            })
            .await;
        return Err(error);
    }

    Ok(upload_id)
}

fn update_skill_upload_progress(
    this: &WeakEntity<PioneerDesktop>,
    cx: &mut AsyncApp,
    progress: SkillUploadProgress,
) {
    let _ = this.update(cx, |view, cx| {
        view.skills_upload_progress = Some(progress);
        cx.notify();
    });
}

fn ensure_upload_not_cancelled(cancel_token: &AtomicBool) -> Result<()> {
    if cancel_token.load(Ordering::Relaxed) {
        return Err(anyhow!("skill upload cancelled"));
    }
    Ok(())
}
