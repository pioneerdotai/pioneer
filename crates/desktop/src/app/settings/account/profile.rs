use std::path::PathBuf;

use gpui::{prelude::*, *};
use gpui_component::{
    input::{InputEvent, InputState},
    menu::{ContextMenuExt, PopupMenuItem},
    *,
};
use pioneer_protocol::{
    AuthProfileAvatarUpdate, AuthProfileUpdateParams, PROFILE_AVATAR_MAX_DECODED_BYTES,
};

use crate::{
    app::{invitation_join::load_desktop_profile_avatar, root::PioneerDesktop},
    assets::PioneerIconName,
    components::profile_editor::{
        profile_avatar, profile_editor_header, profile_editor_page, profile_identity_group,
        profile_username_editor, profile_username_field,
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProfileEditorPhase {
    Account,
    Username,
}

#[derive(Clone, Debug, Default)]
enum ProfileAvatarEdit {
    #[default]
    Unchanged,
    Remove,
    Set(PathBuf),
}

pub(in crate::app) struct ProfileEditorState {
    first_name: Entity<InputState>,
    last_name: Entity<InputState>,
    nickname: Entity<InputState>,
    current_avatar_path: Option<PathBuf>,
    current_avatar_present: bool,
    avatar: ProfileAvatarEdit,
    phase: ProfileEditorPhase,
    username_before_edit: Option<String>,
    saving: bool,
    error: Option<String>,
}

impl ProfileEditorState {
    fn display_name(&self, cx: &App) -> String {
        [
            self.first_name.read(cx).value().trim(),
            self.last_name.read(cx).value().trim(),
        ]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
    }

    fn avatar_path(&self) -> Option<PathBuf> {
        match &self.avatar {
            ProfileAvatarEdit::Unchanged => self.current_avatar_path.clone(),
            ProfileAvatarEdit::Remove => None,
            ProfileAvatarEdit::Set(path) => Some(path.clone()),
        }
    }

    fn has_avatar(&self) -> bool {
        match &self.avatar {
            ProfileAvatarEdit::Unchanged => self.current_avatar_present,
            ProfileAvatarEdit::Remove => false,
            ProfileAvatarEdit::Set(_) => true,
        }
    }

    fn can_complete(&self, phase: ProfileEditorPhase, cx: &App) -> bool {
        let display_name = match phase {
            ProfileEditorPhase::Account => self.display_name(cx),
            ProfileEditorPhase::Username => "Member".to_owned(),
        };
        AuthProfileUpdateParams::new(
            display_name,
            self.nickname.read(cx).value().trim().to_owned(),
            AuthProfileAvatarUpdate::Unchanged,
        )
        .is_ok()
    }

    fn params(&self, cx: &App) -> anyhow::Result<AuthProfileUpdateParams> {
        let avatar = match &self.avatar {
            ProfileAvatarEdit::Unchanged => AuthProfileAvatarUpdate::Unchanged,
            ProfileAvatarEdit::Remove => AuthProfileAvatarUpdate::Remove,
            ProfileAvatarEdit::Set(path) => AuthProfileAvatarUpdate::Set {
                avatar: load_desktop_profile_avatar(path.as_path())?,
            },
        };
        AuthProfileUpdateParams::new(
            self.display_name(cx),
            self.nickname.read(cx).value().trim().to_owned(),
            avatar,
        )
        .map_err(anyhow::Error::new)
    }
}

impl PioneerDesktop {
    pub(super) fn open_profile_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(auth) = self.gateway.current_auth.as_ref() else {
            return;
        };
        let (first_name, last_name) = split_display_name(auth.principal.display_name.as_str());
        let nickname = auth.principal.nickname.clone();
        let current_avatar_present = auth.principal.avatar_revision.is_some();
        let current_avatar_path = self
            .member_avatar_state
            .presentation(&auth.principal.id)
            .and_then(|avatar| avatar.cached_image_path.clone());

        let first_name = cx.new(|cx| {
            let mut state = InputState::new(window, cx)
                .placeholder(t!("settings.profile.first_name").to_string());
            state.set_value(first_name, window, cx);
            state
        });
        let last_name = cx.new(|cx| {
            let mut state = InputState::new(window, cx)
                .placeholder(t!("settings.profile.last_name").to_string());
            state.set_value(last_name, window, cx);
            state
        });
        let nickname = cx.new(|cx| {
            let mut state = InputState::new(window, cx)
                .placeholder(t!("settings.profile.username").to_string());
            state.set_value(nickname, window, cx);
            state
        });

        let editor = cx.new(|_| ProfileEditorState {
            first_name: first_name.clone(),
            last_name: last_name.clone(),
            nickname: nickname.clone(),
            current_avatar_path,
            current_avatar_present,
            avatar: ProfileAvatarEdit::Unchanged,
            phase: ProfileEditorPhase::Account,
            username_before_edit: None,
            saving: false,
            error: None,
        });
        self.profile_editor_input_subscriptions = [&first_name, &last_name, &nickname]
            .into_iter()
            .map(|input| {
                cx.subscribe(input, |view, _, event: &InputEvent, cx| {
                    if !matches!(event, InputEvent::Change) {
                        return;
                    }
                    if let Some(editor) = view.profile_editor.as_ref() {
                        editor.update(cx, |state, _| state.error = None);
                    }
                    cx.notify();
                })
            })
            .collect();
        self.profile_editor = Some(editor);
        first_name.update(cx, |state, cx| state.focus(window, cx));
        cx.notify();
    }

    pub(in crate::app::settings) fn render_profile_editor(
        &self,
        editor: Entity<ProfileEditorState>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let (
            phase,
            saving,
            error,
            first_name,
            last_name,
            nickname,
            display_name,
            avatar_path,
            has_avatar,
            can_complete,
        ) = {
            let state = editor.read(cx);
            (
                state.phase,
                state.saving,
                state.error.clone(),
                state.first_name.clone(),
                state.last_name.clone(),
                state.nickname.clone(),
                state.display_name(cx),
                state.avatar_path(),
                state.has_avatar(),
                state.can_complete(state.phase, cx),
            )
        };
        let desktop = cx.entity().clone();
        let title = match phase {
            ProfileEditorPhase::Account => t!("settings.profile.edit_title"),
            ProfileEditorPhase::Username => t!("settings.profile.edit_username_title"),
        }
        .to_string();
        let header = profile_editor_header(
            title,
            "profile-editor-back",
            "profile-editor-done",
            t!("settings.profile.done").to_string(),
            saving,
            can_complete,
            {
                let editor = editor.clone();
                let desktop = desktop.clone();
                move |_, window, cx| {
                    let _ = desktop.update(cx, |view, cx| match phase {
                        ProfileEditorPhase::Account => view.close_profile_editor(cx),
                        ProfileEditorPhase::Username => {
                            view.cancel_profile_username_editor(editor.clone(), window, cx)
                        }
                    });
                }
            },
            {
                let editor = editor.clone();
                let desktop = desktop.clone();
                move |_, _, cx| {
                    let _ = desktop.update(cx, |view, cx| match phase {
                        ProfileEditorPhase::Account => view.save_profile_editor(editor.clone(), cx),
                        ProfileEditorPhase::Username => {
                            view.complete_profile_username_editor(editor.clone(), cx)
                        }
                    });
                }
            },
            cx,
        );
        let content = match phase {
            ProfileEditorPhase::Account => {
                let avatar = div()
                    .id("profile-avatar-edit")
                    .relative()
                    .flex_none()
                    .cursor_pointer()
                    .on_click({
                        let editor = editor.clone();
                        let desktop = desktop.clone();
                        move |_, _, cx| {
                            let _ = desktop.update(cx, |view, cx| {
                                view.pick_profile_avatar(editor.clone(), cx)
                            });
                        }
                    })
                    .context_menu({
                        let editor = editor.clone();
                        let desktop = desktop.clone();
                        move |menu, _, _| {
                            let change_editor = editor.clone();
                            let change_desktop = desktop.clone();
                            let remove_editor = editor.clone();
                            menu.min_w(px(200.))
                                .item(
                                    PopupMenuItem::new(
                                        t!("settings.profile.change_photo").to_string(),
                                    )
                                    .icon(PioneerIconName::Pen)
                                    .disabled(saving)
                                    .on_click(
                                        move |_, _, cx| {
                                            let _ = change_desktop.update(cx, |view, cx| {
                                                view.pick_profile_avatar(change_editor.clone(), cx)
                                            });
                                        },
                                    ),
                                )
                                .item(
                                    PopupMenuItem::new(
                                        t!("settings.profile.remove_photo").to_string(),
                                    )
                                    .icon(PioneerIconName::Trash)
                                    .disabled(saving || !has_avatar)
                                    .on_click(
                                        move |_, _, cx| {
                                            let _ = remove_editor.update(cx, |state, cx| {
                                                state.avatar = ProfileAvatarEdit::Remove;
                                                state.error = None;
                                                cx.notify();
                                            });
                                        },
                                    ),
                                )
                        }
                    })
                    .child(profile_avatar(display_name, avatar_path))
                    .into_any_element();
                v_flex()
                    .w_full()
                    .gap_6()
                    .child(profile_identity_group(
                        avatar, first_name, last_name, None, cx,
                    ))
                    .child(profile_username_field(
                        "profile-username-row",
                        nickname.read(cx).value().to_string(),
                        None,
                        {
                            let editor = editor.clone();
                            let desktop = desktop.clone();
                            move |_, window, cx| {
                                let _ = desktop.update(cx, |view, cx| {
                                    view.open_profile_username_editor(editor.clone(), window, cx)
                                });
                            }
                        },
                        cx,
                    ))
                    .into_any_element()
            }
            ProfileEditorPhase::Username => profile_username_editor(nickname, None, cx),
        };

        profile_editor_page(
            match phase {
                ProfileEditorPhase::Account => "profile-account-scroll",
                ProfileEditorPhase::Username => "profile-username-scroll",
            },
            header,
            content,
            error,
            cx,
        )
    }

    fn close_profile_editor(&mut self, cx: &mut Context<Self>) {
        self.profile_editor = None;
        self.profile_editor_input_subscriptions.clear();
        cx.notify();
    }

    fn open_profile_username_editor(
        &mut self,
        editor: Entity<ProfileEditorState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let nickname = editor.update(cx, |state, cx| {
            state.username_before_edit = Some(state.nickname.read(cx).value().trim().to_owned());
            state.phase = ProfileEditorPhase::Username;
            state.error = None;
            state.nickname.clone()
        });
        nickname.update(cx, |state, cx| state.focus(window, cx));
        cx.notify();
    }

    fn cancel_profile_username_editor(
        &mut self,
        editor: Entity<ProfileEditorState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let (nickname, previous) = editor.update(cx, |state, _| {
            state.phase = ProfileEditorPhase::Account;
            state.error = None;
            (state.nickname.clone(), state.username_before_edit.take())
        });
        if let Some(previous) = previous {
            nickname.update(cx, |state, cx| state.set_value(previous, window, cx));
        }
        cx.notify();
    }

    fn complete_profile_username_editor(
        &mut self,
        editor: Entity<ProfileEditorState>,
        cx: &mut Context<Self>,
    ) {
        if !editor
            .read(cx)
            .can_complete(ProfileEditorPhase::Username, cx)
        {
            editor.update(cx, |state, cx| {
                state.error = Some(t!("settings.profile.error_invalid").to_string());
                cx.notify();
            });
            cx.notify();
            return;
        }
        editor.update(cx, |state, cx| {
            state.phase = ProfileEditorPhase::Account;
            state.username_before_edit = None;
            state.error = None;
            cx.notify();
        });
        cx.notify();
    }

    fn pick_profile_avatar(&mut self, editor: Entity<ProfileEditorState>, cx: &mut Context<Self>) {
        let selection = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            directories: false,
            multiple: false,
            prompt: None,
        });
        cx.spawn(move |_this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let Ok(Ok(Some(paths))) = selection.await else {
                    return;
                };
                let Some(path) = paths.into_iter().next() else {
                    return;
                };
                let valid_size = std::fs::metadata(path.as_path())
                    .ok()
                    .is_some_and(|metadata| {
                        metadata.is_file()
                            && metadata.len() <= PROFILE_AVATAR_MAX_DECODED_BYTES as u64
                    });
                let valid = valid_size && load_desktop_profile_avatar(path.as_path()).is_ok();
                let _ = editor.update(&mut cx, |state, cx| {
                    if valid {
                        state.avatar = ProfileAvatarEdit::Set(path);
                        state.error = None;
                    } else {
                        state.error = Some(t!("settings.profile.error_avatar").to_string());
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn save_profile_editor(&mut self, editor: Entity<ProfileEditorState>, cx: &mut Context<Self>) {
        let params = editor.update(cx, |state, cx| {
            if state.saving {
                return None;
            }
            match state.params(cx) {
                Ok(params) => {
                    state.saving = true;
                    state.error = None;
                    cx.notify();
                    Some(params)
                }
                Err(_) => {
                    state.error = Some(t!("settings.profile.error_invalid").to_string());
                    cx.notify();
                    None
                }
            }
        });
        let Some(params) = params else {
            return;
        };
        let sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move { sender.auth_profile_update(params) })
                    .await;
                match result {
                    Ok(response) => {
                        let _ = this.update(&mut cx, |view, cx| {
                            if let Some(auth) = view.gateway.current_auth.as_mut() {
                                auth.principal = response.principal;
                            }
                            view.profile_editor = None;
                            view.profile_editor_input_subscriptions.clear();
                            view.resolve_current_principal_avatar(cx);
                            view.refresh_members(false, cx);
                            view.refresh_all_workspace_members(cx);
                            cx.notify();
                        });
                    }
                    Err(error) => {
                        let message = profile_save_error_message(error.to_string().as_str());
                        let _ = editor.update(&mut cx, |state, cx| {
                            state.saving = false;
                            state.error = Some(message);
                            cx.notify();
                        });
                    }
                }
            }
        })
        .detach();
    }
}

fn split_display_name(display_name: &str) -> (String, String) {
    let normalized = display_name.split_whitespace().collect::<Vec<_>>();
    match normalized.split_first() {
        Some((first, rest)) => ((*first).to_owned(), rest.join(" ")),
        None => (String::new(), String::new()),
    }
}

fn profile_save_error_message(error: &str) -> String {
    if error.contains("nickname_unavailable") {
        t!("settings.profile.error_username_unavailable").to_string()
    } else if error.contains("avatar_invalid") {
        t!("settings.profile.error_avatar").to_string()
    } else {
        t!("settings.profile.error_save").to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::MEMBER_DISPLAY_NAME_MAX_SCALARS;

    #[::core::prelude::v1::test]
    fn display_name_is_split_into_first_and_remaining_names() {
        assert_eq!(
            split_display_name("  Alexander  Oskin Junior "),
            ("Alexander".to_owned(), "Oskin Junior".to_owned())
        );
    }

    #[::core::prelude::v1::test]
    fn display_name_limit_matches_protocol_contract() {
        assert_eq!(MEMBER_DISPLAY_NAME_MAX_SCALARS, 128);
    }

    #[::core::prelude::v1::test]
    fn profile_editing_uses_nested_settings_screens_instead_of_a_dialog() {
        let source = include_str!("profile.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .unwrap();
        assert!(source.contains("ProfileEditorPhase::Account"));
        assert!(source.contains("ProfileEditorPhase::Username"));
        assert!(source.contains("profile_editor_header"));
        assert!(source.contains("profile_identity_group"));
        assert!(source.contains("profile_username_field"));
        assert!(source.contains("profile_username_editor"));
        assert!(!source.contains("open_dialog"));
        assert!(!source.contains("close_dialog"));
    }
}
