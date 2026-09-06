use crate::{
    app::root::{GatewayConnectionState, PioneerDesktop, SkillUploadProgress},
    gateway::GatewayWsCommandSender,
};
use anyhow::Result;
use gpui_kit::{prelude::*, *};
use pioneer_client::skills::{
    actions as skill_actions,
    archive::{
        SkillUploadArchive, SkillUploadSourceKind, build_skill_pack_upload_archive,
        build_skill_upload_archive, classify_skill_upload_source,
    },
    upload as skill_upload,
};
use pioneer_protocol::{SkillId, SkillPackId};
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
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return;
        }
        let Some(source_path) = skill_actions::normalize_skill_source_path(source_path.as_str())
        else {
            self.skills_error = Some(t!("skills.error.path_required").to_string());
            return;
        };

        let scope = match skill_actions::plan_skill_action_scope(
            matches!(
                self.gateway.connection_state,
                GatewayConnectionState::Connected
            ),
            self.gateway.ws_connection_id,
            self.skills_workspace_scope(),
        ) {
            skill_actions::SkillActionScopePlan::Send(scope) => scope,
            skill_actions::SkillActionScopePlan::Unavailable(reason) => {
                self.apply_skill_action_unavailable(reason);
                return;
            }
        };
        let connection_id = scope.connection_id;
        let workspace_id = scope.workspace_id;

        self.skills_loading = true;
        self.skills_error = None;
        let cancel_token = Arc::new(AtomicBool::new(false));
        self.skills_upload_cancel_token = Some(cancel_token.clone());
        self.skills_upload_progress = Some(skill_upload::skill_upload_progress(
            t!("skills.upload.preparing").to_string(),
            0,
            0,
        ));

        let ws_sender = self.gateway.client_runtime.ws_command_sender().clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let classification_path = PathBuf::from(source_path.as_str());
                let source_kind = cx
                    .background_spawn(async move {
                        classify_skill_upload_source(classification_path.as_path())
                    })
                    .await;
                let (source_kind, result): (Option<SkillUploadSourceKind>, Result<()>) =
                    match source_kind {
                        Ok(source_kind) => {
                            let archive_builder: fn(
                                &std::path::Path,
                            ) -> Result<SkillUploadArchive> = match source_kind {
                                SkillUploadSourceKind::Skill => build_skill_upload_archive,
                                SkillUploadSourceKind::Pack => build_skill_pack_upload_archive,
                            };
                            let progress_label = match source_kind {
                                SkillUploadSourceKind::Skill => {
                                    t!("skills.upload.installing").to_string()
                                }
                                SkillUploadSourceKind::Pack => {
                                    t!("skills.upload.installing_pack").to_string()
                                }
                            };
                            let upload_workspace_id = workspace_id.clone();
                            let result = async {
                                let upload_id = upload_selected_skill_directory(
                                    this.clone(),
                                    &mut cx,
                                    ws_sender.clone(),
                                    upload_workspace_id,
                                    source_path,
                                    progress_label,
                                    cancel_token,
                                    archive_builder,
                                )
                                .await?;

                                cx.background_spawn(async move {
                                    match source_kind {
                                        SkillUploadSourceKind::Skill => ws_sender
                                            .skills_install(
                                                skill_actions::skills_install_uploaded_archive_params(
                                                    workspace_id,
                                                    upload_id,
                                                ),
                                            )
                                            .map(|_| ()),
                                        SkillUploadSourceKind::Pack => ws_sender
                                            .skills_pack_install(
                                                skill_actions::skills_pack_install_uploaded_archive_params(
                                                    workspace_id,
                                                    upload_id,
                                                ),
                                            )
                                            .map(|_| ()),
                                    }
                                })
                                .await?;
                                Ok(())
                            }
                            .await;
                            (Some(source_kind), result)
                        }
                        Err(error) => (None, Err(error)),
                    };

                let _ = this.update(&mut cx, |view, cx| {
                    if !skill_actions::skill_action_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    let outcome = match result {
                        Ok(_) => skill_actions::SkillActionFinishOutcome::Success,
                        Err(error) => {
                            let label = match source_kind {
                                Some(SkillUploadSourceKind::Pack) => {
                                    t!("skills.error.install_pack_failed")
                                }
                                _ => t!("skills.error.install_failed"),
                            };
                            let error = format!("{label}: {error:#}");
                            warn!(source_kind = ?source_kind, error = %format!("{error:#}"), "failed to install skill source");
                            skill_actions::SkillActionFinishOutcome::Failure { error }
                        }
                    };
                    match source_kind {
                        Some(SkillUploadSourceKind::Pack) => {
                            let reduction = skill_actions::reduce_skill_pack_action_finish(
                                skill_actions::SkillPackActionFinishKind::Install,
                                outcome,
                            );
                            view.apply_skill_pack_action_finish_reduction(reduction);
                        }
                        _ => {
                            let reduction = skill_actions::reduce_skill_action_finish(
                                skill_actions::SkillActionFinishKind::Install,
                                outcome,
                            );
                            view.apply_skill_action_finish_reduction(reduction);
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn update_skill_pack_from_path(
        &mut self,
        pack_id: SkillPackId,
        source_path: String,
        cx: &mut Context<Self>,
    ) {
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return;
        }
        if !self
            .skills_management
            .packs
            .iter()
            .any(|row| row.pack.id == pack_id)
        {
            self.skills_error = Some(t!("skills.error.invalid_pack_target").to_string());
            return;
        }
        let Some(source_path) = skill_actions::normalize_skill_source_path(source_path.as_str())
        else {
            self.skills_error = Some(t!("skills.error.path_required").to_string());
            return;
        };
        let scope = match skill_actions::plan_skill_action_scope(
            matches!(
                self.gateway.connection_state,
                GatewayConnectionState::Connected
            ),
            self.gateway.ws_connection_id,
            self.skills_workspace_scope(),
        ) {
            skill_actions::SkillActionScopePlan::Send(scope) => scope,
            skill_actions::SkillActionScopePlan::Unavailable(reason) => {
                self.apply_skill_action_unavailable(reason);
                return;
            }
        };
        let connection_id = scope.connection_id;
        let workspace_id = scope.workspace_id;

        self.skills_error = None;
        let cancel_token = Arc::new(AtomicBool::new(false));
        self.skills_upload_cancel_token = Some(cancel_token.clone());
        self.skills_upload_progress = Some(skill_upload::skill_upload_progress(
            t!("skills.upload.preparing").to_string(),
            0,
            0,
        ));
        self.mark_skill_pack_pending(&pack_id, true);

        let ws_sender = self.gateway.client_runtime.ws_command_sender().clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let pack_id_for_request = pack_id.clone();
            let upload_workspace_id = workspace_id.clone();
            async move {
                let result: Result<()> = async {
                    let upload_id = upload_selected_skill_directory(
                        this.clone(),
                        &mut cx,
                        ws_sender.clone(),
                        upload_workspace_id,
                        source_path,
                        t!("skills.upload.updating_pack").to_string(),
                        cancel_token,
                        build_skill_pack_upload_archive,
                    )
                    .await?;

                    cx.background_spawn(async move {
                        ws_sender.skills_pack_update(
                            skill_actions::skills_pack_update_uploaded_archive_params(
                                workspace_id,
                                pack_id_for_request,
                                upload_id,
                            ),
                        )
                    })
                    .await?;
                    Ok(())
                }
                .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !skill_actions::skill_action_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    let outcome = match result {
                        Ok(_) => skill_actions::SkillActionFinishOutcome::Success,
                        Err(error) => {
                            let error =
                                format!("{}: {error:#}", t!("skills.error.update_pack_failed"));
                            warn!(pack_id = %pack_id, error = %format!("{error:#}"), "failed to update skill pack");
                            skill_actions::SkillActionFinishOutcome::Failure { error }
                        }
                    };
                    let reduction = skill_actions::reduce_skill_pack_action_finish(
                        skill_actions::SkillPackActionFinishKind::Update(
                            skill_actions::SkillPackActionTarget::new(pack_id.clone()),
                        ),
                        outcome,
                    );
                    view.apply_skill_pack_action_finish_reduction(reduction);
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn uninstall_skill_pack(&mut self, pack_id: SkillPackId, cx: &mut Context<Self>) {
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return;
        }
        if !self
            .skills_management
            .packs
            .iter()
            .any(|row| row.pack.id == pack_id)
        {
            self.skills_error = Some(t!("skills.error.invalid_pack_target").to_string());
            return;
        }
        let scope = match skill_actions::plan_skill_action_scope(
            matches!(
                self.gateway.connection_state,
                GatewayConnectionState::Connected
            ),
            self.gateway.ws_connection_id,
            self.skills_workspace_scope(),
        ) {
            skill_actions::SkillActionScopePlan::Send(scope) => scope,
            skill_actions::SkillActionScopePlan::Unavailable(reason) => {
                self.apply_skill_action_unavailable(reason);
                return;
            }
        };
        let connection_id = scope.connection_id;
        let workspace_id = scope.workspace_id;

        self.skills_error = None;
        self.mark_skill_pack_pending(&pack_id, true);
        let ws_sender = self.gateway.client_runtime.ws_command_sender().clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let pack_id_for_request = pack_id.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.skills_pack_uninstall(skill_actions::skills_pack_uninstall_params(
                            workspace_id,
                            pack_id_for_request,
                        ))
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !skill_actions::skill_action_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }
                    let outcome = match result {
                        Ok(_) => skill_actions::SkillActionFinishOutcome::Success,
                        Err(error) => {
                            let error = format!(
                                "{}: {error:#}",
                                t!("skills.error.uninstall_pack_failed")
                            );
                            warn!(pack_id = %pack_id, error = %format!("{error:#}"), "failed to uninstall skill pack");
                            skill_actions::SkillActionFinishOutcome::Failure { error }
                        }
                    };
                    let reduction = skill_actions::reduce_skill_pack_action_finish(
                        skill_actions::SkillPackActionFinishKind::Uninstall(
                            skill_actions::SkillPackActionTarget::new(pack_id.clone()),
                        ),
                        outcome,
                    );
                    view.apply_skill_pack_action_finish_reduction(reduction);
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn update_skill_from_path(
        &mut self,
        skill_id: SkillId,
        source_path: String,
        cx: &mut Context<Self>,
    ) {
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return;
        }
        if skill_actions::skill_lifecycle_editable(
            self.installed_skills.as_slice(),
            self.skills_catalog.as_slice(),
            &skill_id,
        ) == Some(false)
        {
            self.skills_error = Some(t!("skills.error.invalid_skill_target").to_string());
            return;
        }
        let Some(source_path) = skill_actions::normalize_skill_source_path(source_path.as_str())
        else {
            self.skills_error = Some(t!("skills.error.path_required").to_string());
            return;
        };
        let scope = match skill_actions::plan_skill_action_scope(
            matches!(
                self.gateway.connection_state,
                GatewayConnectionState::Connected
            ),
            self.gateway.ws_connection_id,
            self.skills_workspace_scope(),
        ) {
            skill_actions::SkillActionScopePlan::Send(scope) => scope,
            skill_actions::SkillActionScopePlan::Unavailable(reason) => {
                self.apply_skill_action_unavailable(reason);
                return;
            }
        };
        let connection_id = scope.connection_id;
        let workspace_id = scope.workspace_id;

        self.skills_error = None;
        let cancel_token = Arc::new(AtomicBool::new(false));
        self.skills_upload_cancel_token = Some(cancel_token.clone());
        self.skills_upload_progress = Some(skill_upload::skill_upload_progress(
            t!("skills.upload.preparing").to_string(),
            0,
            0,
        ));
        self.mark_skill_pending(&skill_id, true);

        let ws_sender = self.gateway.client_runtime.ws_command_sender().clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let skill_id_for_request = skill_id.clone();
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
                        build_skill_upload_archive,
                    )
                    .await?;

                    cx.background_spawn(async move {
                        ws_sender.skills_update(
                            skill_actions::skills_update_uploaded_archive_params(
                                workspace_id,
                                skill_id_for_request,
                                upload_id,
                                None,
                            ),
                        )
                    })
                    .await?;
                    Ok(())
                }
                .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !skill_actions::skill_action_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    let outcome = match result {
                        Ok(_) => skill_actions::SkillActionFinishOutcome::Success,
                        Err(error) => {
                            let error = format!("{}: {error:#}", t!("skills.error.update_failed"));
                            warn!(
                                skill_id = %skill_id,
                                error = %format!("{error:#}"),
                                "failed to update skill"
                            );
                            skill_actions::SkillActionFinishOutcome::Failure { error }
                        }
                    };
                    let reduction = skill_actions::reduce_skill_action_finish(
                        skill_actions::SkillActionFinishKind::Update(
                            skill_actions::SkillActionTarget::new(skill_id.clone()),
                        ),
                        outcome,
                    );
                    view.apply_skill_action_finish_reduction(reduction);

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn uninstall_skill(&mut self, skill_id: SkillId, cx: &mut Context<Self>) {
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return;
        }
        if skill_actions::skill_lifecycle_editable(
            self.installed_skills.as_slice(),
            self.skills_catalog.as_slice(),
            &skill_id,
        ) == Some(false)
        {
            self.skills_error = Some(t!("skills.error.invalid_skill_target").to_string());
            return;
        }
        let scope = match skill_actions::plan_skill_action_scope(
            matches!(
                self.gateway.connection_state,
                GatewayConnectionState::Connected
            ),
            self.gateway.ws_connection_id,
            self.skills_workspace_scope(),
        ) {
            skill_actions::SkillActionScopePlan::Send(scope) => scope,
            skill_actions::SkillActionScopePlan::Unavailable(reason) => {
                self.apply_skill_action_unavailable(reason);
                return;
            }
        };
        let connection_id = scope.connection_id;
        let workspace_id = scope.workspace_id;

        self.skills_error = None;
        self.mark_skill_pending(&skill_id, true);

        let ws_sender = self.gateway.client_runtime.ws_command_sender().clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let skill_id_for_request = skill_id.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.skills_uninstall(skill_actions::skills_uninstall_params(
                            workspace_id,
                            skill_id_for_request,
                        ))
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !skill_actions::skill_action_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    let outcome = match result {
                        Ok(_) => skill_actions::SkillActionFinishOutcome::Success,
                        Err(error) => {
                            let error =
                                format!("{}: {error:#}", t!("skills.error.uninstall_failed"));
                            warn!(
                                skill_id = %skill_id,
                                error = %format!("{error:#}"),
                                "failed to uninstall skill"
                            );
                            skill_actions::SkillActionFinishOutcome::Failure { error }
                        }
                    };
                    let reduction = skill_actions::reduce_skill_action_finish(
                        skill_actions::SkillActionFinishKind::Uninstall(
                            skill_actions::SkillActionTarget::new(skill_id.clone()),
                        ),
                        outcome,
                    );
                    view.apply_skill_action_finish_reduction(reduction);

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn set_skill_policy(
        &mut self,
        skill_id: SkillId,
        enabled: bool,
        allow_implicit_invocation: bool,
        cx: &mut Context<Self>,
    ) {
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return;
        }
        let allow_implicit_invocation = skill_actions::effective_allow_implicit_invocation(
            allow_implicit_invocation,
            skill_actions::skill_policy_implicit_editable(
                self.installed_skills.as_slice(),
                self.skills_catalog.as_slice(),
                &skill_id,
            ),
        );

        let scope = match skill_actions::plan_skill_action_scope(
            matches!(
                self.gateway.connection_state,
                GatewayConnectionState::Connected
            ),
            self.gateway.ws_connection_id,
            self.skills_workspace_scope(),
        ) {
            skill_actions::SkillActionScopePlan::Send(scope) => scope,
            skill_actions::SkillActionScopePlan::Unavailable(reason) => {
                self.apply_skill_action_unavailable(reason);
                return;
            }
        };
        let connection_id = scope.connection_id;
        let workspace_id = scope.workspace_id;

        let previous_policy =
            skill_actions::skill_policy_values(self.skills_catalog.as_slice(), &skill_id);

        self.skills_error = None;
        self.mark_skill_pending(&skill_id, true);
        skill_actions::apply_local_skill_policy(
            self.skills_catalog.as_mut_slice(),
            self.installed_skills.as_mut_slice(),
            &skill_id,
            enabled,
            allow_implicit_invocation,
        );
        self.reproject_skill_management();

        let ws_sender = self.gateway.client_runtime.ws_command_sender().clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let skill_id_for_request = skill_id.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.skills_policy_set(skill_actions::skills_policy_set_params(
                            workspace_id,
                            skill_id_for_request,
                            enabled,
                            allow_implicit_invocation,
                        ))
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !skill_actions::skill_action_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    let outcome = match result {
                        Ok(_) => skill_actions::SkillActionFinishOutcome::Success,
                        Err(error) => {
                            let error = format!("{}: {error:#}", t!("skills.error.policy_failed"));
                            warn!(
                                skill_id = %skill_id,
                                error = %format!("{error:#}"),
                                "failed to set skill policy"
                            );
                            skill_actions::SkillActionFinishOutcome::Failure { error }
                        }
                    };
                    let reduction = skill_actions::reduce_skill_action_finish(
                        skill_actions::SkillActionFinishKind::Policy(
                            skill_actions::SkillActionTarget::new(skill_id.clone()),
                        ),
                        outcome,
                    );
                    if reduction.rollback_policy
                        && let Some((prev_enabled, prev_implicit)) = previous_policy
                    {
                        skill_actions::apply_local_skill_policy(
                            view.skills_catalog.as_mut_slice(),
                            view.installed_skills.as_mut_slice(),
                            &skill_id,
                            prev_enabled,
                            prev_implicit,
                        );
                        view.reproject_skill_management();
                    }
                    view.apply_skill_action_finish_reduction(reduction);

                    cx.notify();
                });
            }
        })
        .detach();
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

    fn apply_skill_action_unavailable(&mut self, reason: skill_actions::SkillActionUnavailable) {
        self.skills_error = Some(match reason {
            skill_actions::SkillActionUnavailable::GatewayNotConnected => {
                t!("skills.error.gateway_not_connected").to_string()
            }
            skill_actions::SkillActionUnavailable::WorkspaceNotSelected => {
                t!("skills.error.workspace_not_selected").to_string()
            }
        });
    }

    fn apply_skill_action_finish_reduction(
        &mut self,
        reduction: skill_actions::SkillActionFinishReduction,
    ) {
        if let Some(loading) = reduction.loading {
            self.skills_loading = loading;
        }
        if reduction.clear_upload_state {
            self.skills_upload_progress = None;
            self.skills_upload_cancel_token = None;
        }
        if let Some(pending) = reduction.pending {
            self.mark_skill_pending(&pending.target.skill_id, pending.pending);
        }
        self.skills_error = reduction.error;
        if reduction.queue_refresh {
            self.queue_skills_refresh();
        }
    }

    fn apply_skill_pack_action_finish_reduction(
        &mut self,
        reduction: skill_actions::SkillPackActionFinishReduction,
    ) {
        if let Some(loading) = reduction.loading {
            self.skills_loading = loading;
        }
        if reduction.clear_upload_state {
            self.skills_upload_progress = None;
            self.skills_upload_cancel_token = None;
        }
        if let Some(pending) = reduction.pending {
            self.mark_skill_pack_pending(&pending.target.pack_id, pending.pending);
        }
        self.skills_error = reduction.error;
        if reduction.queue_refresh {
            self.queue_skills_refresh();
        }
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
    archive_builder: fn(&std::path::Path) -> Result<SkillUploadArchive>,
) -> Result<String> {
    let source_path = PathBuf::from(source_path);
    let archive = cx
        .background_spawn(async move { archive_builder(source_path.as_path()) })
        .await?;
    let total_bytes = skill_upload::archive_compressed_size(&archive)?;

    update_skill_upload_progress(
        &this,
        cx,
        skill_upload::skill_upload_progress(progress_label.clone(), 0, total_bytes),
    );
    skill_upload::ensure_skill_upload_not_cancelled(cancel_token.load(Ordering::Relaxed))?;

    let start = {
        let ws_sender = ws_sender.clone();
        let workspace_id = workspace_id.clone();
        let params = skill_upload::skills_upload_start_params(workspace_id, &archive)?;
        cx.background_spawn(async move { ws_sender.skills_upload_start(params) })
            .await?
    };

    let upload_id = start.upload_id.clone();
    let chunk_size = skill_upload::skill_upload_chunk_size(&start)?;

    let upload_result: Result<()> = async {
        let mut offset = 0usize;
        while let Some(chunk) =
            skill_upload::next_skill_upload_chunk(archive.bytes.as_slice(), offset, chunk_size)?
        {
            skill_upload::ensure_skill_upload_not_cancelled(cancel_token.load(Ordering::Relaxed))?;
            let ack = {
                let ws_sender = ws_sender.clone();
                let workspace_id = workspace_id.clone();
                let upload_id = upload_id.clone();
                let chunk_offset = chunk.offset_bytes;
                let chunk_bytes = chunk.bytes.clone();
                cx.background_spawn(async move {
                    ws_sender.send_skill_upload_chunk(
                        workspace_id,
                        upload_id,
                        chunk_offset,
                        chunk_bytes,
                    )
                })
                .await?
            };

            skill_upload::validate_skill_upload_chunk_ack(&ack, chunk.next_offset_bytes)?;
            offset = chunk.next_offset;
            update_skill_upload_progress(
                &this,
                cx,
                skill_upload::skill_upload_progress(
                    progress_label.clone(),
                    chunk.next_offset_bytes,
                    total_bytes,
                ),
            );
        }

        skill_upload::ensure_skill_upload_not_cancelled(cancel_token.load(Ordering::Relaxed))?;
        let finish = {
            let ws_sender = ws_sender.clone();
            let params =
                skill_upload::skills_upload_finish_params(workspace_id.clone(), upload_id.clone());
            cx.background_spawn(async move { ws_sender.skills_upload_finish(params) })
                .await?
        };
        skill_upload::validate_skill_upload_finish_response(&finish)?;
        Ok(())
    }
    .await;

    if let Err(error) = upload_result {
        let _ = cx
            .background_spawn({
                let ws_sender = ws_sender.clone();
                let params = skill_upload::skills_upload_abort_params(
                    workspace_id.clone(),
                    upload_id.clone(),
                );
                async move { ws_sender.skills_upload_abort(params) }
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
