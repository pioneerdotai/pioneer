use crate::app::root::PioneerDesktop;
use gpui::{prelude::*, *};
use pioneer_protocol::SkillId;

#[derive(Clone)]
enum SkillSourcePickerTarget {
    Install,
    Update { skill_id: SkillId },
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
        self.open_skill_source_picker(SkillSourcePickerTarget::Install, window, cx);
    }

    pub(super) fn open_skill_update_dialog(
        &mut self,
        skill_id: SkillId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_skill_source_picker(SkillSourcePickerTarget::Update { skill_id }, window, cx);
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
