use crate::catalog::SkillsCatalogView;
use gpui_kit::*;
use pioneer_client::{
    navigation::{NavigationIntent, SemanticDestination},
    skills::{operations::*, types::*},
};
impl SkillsCatalogView {
    pub(crate) fn refresh_installed_skills(&mut self, _: &mut Context<Self>) {
        if let Some(w) = self.input.navigation_input.workspace_id() {
            self.client.refresh_skills(w);
        }
    }
    pub(crate) fn open_skill_from_sidebar(&mut self, id: SkillId, _: &mut Context<Self>) {
        if self.principal_presentation_capabilities().can_use_skills {
            self.client.navigate(
                NavigationIntent::Navigate {
                    destination: SemanticDestination::Skills { skill_id: Some(id) },
                },
                None,
            );
        }
    }
    pub(crate) fn close_skill_details_screen(&mut self, _: &mut Context<Self>) {
        if self.is_plugin_component() {
            self.client.navigate(NavigationIntent::BackFromPlugin, None);
            return;
        }
        self.client.navigate(
            NavigationIntent::Navigate {
                destination: if self
                    .principal_presentation_capabilities()
                    .can_manage_capabilities
                {
                    SemanticDestination::Skills { skill_id: None }
                } else {
                    SemanticDestination::Threads
                },
            },
            None,
        );
    }
    pub(crate) fn toggle_skill_pack_expanded(&mut self, id: SkillPackId, cx: &mut Context<Self>) {
        if !self.skills_expanded_pack_ids.remove(&id) {
            self.skills_expanded_pack_ids.insert(id);
        }
        self.sync_rows(cx);
        cx.notify();
    }
    pub(crate) fn set_skill_policy(
        &mut self,
        skill_id: SkillId,
        enabled: bool,
        allow_implicit_invocation: bool,
        cx: &mut Context<Self>,
    ) {
        self.send(
            SkillsIntent::Policy {
                skill_id,
                enabled,
                allow_implicit_invocation,
            },
            cx,
        );
    }
    pub(crate) fn uninstall_skill(&mut self, skill_id: SkillId, cx: &mut Context<Self>) {
        self.send(SkillsIntent::Remove { skill_id }, cx);
    }
    pub(crate) fn uninstall_skill_pack(&mut self, pack_id: SkillPackId, cx: &mut Context<Self>) {
        self.send(SkillsIntent::RemovePack { pack_id }, cx);
    }
    fn send(&mut self, intent: SkillsIntent, cx: &mut Context<Self>) {
        if let Some(w) = self.input.navigation_input.workspace_id() {
            let failed = if let Some((workspace, parent)) = self.plugin_parent() {
                use pioneer_client::plugins::runtime::PluginIntent;
                let intent = match intent {
                    SkillsIntent::Policy {
                        skill_id,
                        enabled,
                        allow_implicit_invocation,
                    } => Some(PluginIntent::Skill {
                        skill_id,
                        enabled,
                        allow_implicit_invocation,
                    }),
                    SkillsIntent::Remove { skill_id } => {
                        Some(PluginIntent::RemoveSkill { skill_id })
                    }
                    _ => None,
                };
                intent.is_none_or(|intent| {
                    self.client
                        .plugin_intent_for(workspace, parent, intent)
                        .outcome()
                        != pioneer_client::core::ClientTransitionOutcome::Changed
                })
            } else {
                self.client.skills_intent(w, intent).is_err()
            };
            if failed {
                self.presentation_error =
                    Some(t!("skills.error.gateway_not_connected").to_string());
                cx.notify();
            }
        }
    }
    pub(crate) fn install_skill_from_path(&mut self, path: String, cx: &mut Context<Self>) {
        self.start_upload(SkillUploadTarget::Install, path, cx);
    }
    pub(crate) fn update_skill_from_path(
        &mut self,
        skill_id: SkillId,
        path: String,
        cx: &mut Context<Self>,
    ) {
        self.start_upload(SkillUploadTarget::Update { skill_id }, path, cx);
    }
    pub(crate) fn update_skill_pack_from_path(
        &mut self,
        pack_id: SkillPackId,
        path: String,
        cx: &mut Context<Self>,
    ) {
        self.start_upload(SkillUploadTarget::UpdatePack { pack_id }, path, cx);
    }
    fn start_upload(&mut self, target: SkillUploadTarget, path: String, cx: &mut Context<Self>) {
        if self.upload.read(cx).active() {
            return;
        }
        if let Some(workspace) = self
            .input
            .navigation_input
            .workspace_id()
            .map(str::to_owned)
        {
            let upload =
                crate::upload::UploadView::new(self.client.clone(), self.binding.registrar(), cx);
            self._upload_visibility = cx.subscribe(
                &upload,
                |view, upload, _: &crate::upload::UploadVisibility, cx| {
                    view.presentation_error = upload.read(cx).error();
                    cx.notify();
                },
            );
            self.upload = upload;
            if let Err(failure) = self.upload.update(cx, |view, cx| {
                view.start(workspace, target, path.into(), cx)
            }) {
                self.presentation_error = Some(crate::upload::UploadView::failure_message(failure));
                cx.notify();
            }
        }
    }
}
