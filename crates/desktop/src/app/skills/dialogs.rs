use crate::app::root::PioneerDesktop;
use gpui_kit::{prelude::*, *};
use pioneer_protocol::{SkillId, SkillPackId};

#[derive(Clone)]
enum SkillSourcePickerTarget {
    Install,
    Update { skill_id: SkillId },
    UpdatePack { pack_id: SkillPackId },
}

#[derive(Copy, Clone)]
enum SkillSourcePickerKind {
    Directory,
}

impl PioneerDesktop {
    pub(super) fn open_skill_install_dialog(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return;
        }
        self.open_skill_source_picker(SkillSourcePickerTarget::Install, window, cx);
    }

    pub(super) fn open_skill_update_dialog(
        &mut self,
        skill_id: SkillId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return;
        }
        self.open_skill_source_picker(SkillSourcePickerTarget::Update { skill_id }, window, cx);
    }

    pub(super) fn open_skill_pack_update_dialog(
        &mut self,
        pack_id: SkillPackId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return;
        }
        self.open_skill_source_picker(SkillSourcePickerTarget::UpdatePack { pack_id }, window, cx);
    }

    pub(super) fn confirm_uninstall_skill_pack(
        &mut self,
        pack_id: SkillPackId,
        name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return;
        }
        let title = t!("skills.dialog.uninstall_pack_title", name = name.as_str()).to_string();
        let description = t!("skills.dialog.uninstall_pack_description").to_string();
        let answer = window.prompt(
            PromptLevel::Warning,
            title.as_str(),
            Some(description.as_str()),
            &[
                PromptButton::new(t!("skills.button.uninstall").to_string()),
                PromptButton::cancel(t!("buttons.cancel").to_string()),
            ],
            cx,
        );

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                if answer.await != Ok(0) {
                    return;
                }
                let _ = this.update(&mut cx, |view, cx| {
                    view.uninstall_skill_pack(pack_id, cx);
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn open_skill_source_picker(
        &mut self,
        target: SkillSourcePickerTarget,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_native_skill_source_picker(target, SkillSourcePickerKind::Directory, cx);
    }

    fn open_native_skill_source_picker(
        &mut self,
        target: SkillSourcePickerTarget,
        picker_kind: SkillSourcePickerKind,
        cx: &mut Context<Self>,
    ) {
        let selection = cx.prompt_for_paths(PathPromptOptions {
            files: picker_kind.allows_files(),
            directories: picker_kind.allows_directories(),
            multiple: false,
            prompt: None,
        });

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let selection = match selection.await {
                    Ok(selection) => selection,
                    Err(_) => return,
                };

                let paths = match selection {
                    Ok(paths) => paths,
                    Err(error) => {
                        let _ = this.update(&mut cx, |view, cx| {
                            view.skills_error = Some(format!(
                                "{}: {error:#}",
                                t!("skills.error.path_picker_failed")
                            ));
                            cx.notify();
                        });
                        return;
                    }
                };

                let Some(source_path) = paths
                    .and_then(|mut values| values.pop())
                    .map(|path| path.to_string_lossy().into_owned())
                else {
                    return;
                };

                let _ = this.update(&mut cx, |view, cx| {
                    view.apply_selected_skill_source_path(target, source_path, cx);
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn apply_selected_skill_source_path(
        &mut self,
        target: SkillSourcePickerTarget,
        source_path: String,
        cx: &mut Context<Self>,
    ) {
        match target {
            SkillSourcePickerTarget::Install => self.install_skill_from_path(source_path, cx),
            SkillSourcePickerTarget::Update { skill_id } => {
                self.update_skill_from_path(skill_id, source_path, cx)
            }
            SkillSourcePickerTarget::UpdatePack { pack_id } => {
                self.update_skill_pack_from_path(pack_id, source_path, cx)
            }
        }
    }
}

impl SkillSourcePickerKind {
    fn allows_files(self) -> bool {
        false
    }

    fn allows_directories(self) -> bool {
        true
    }
}
