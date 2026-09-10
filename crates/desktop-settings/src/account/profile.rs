use crate::{
    binding::SettingsBinding,
    profile_presentation::*,
    screen::{SettingsConfig, SettingsScreenView},
};
use gpui_kit::component::{
    input::{InputEvent, InputState},
    menu::{ContextMenuExt, PopupMenuItem},
    *,
};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    core::ClientScope,
    settings::profile::{
        ProfileAvatarSelection, ProfileEditorSection, ProfileIntent, ProfilePublication,
    },
};
use std::sync::Arc;
#[derive(Clone, Copy, PartialEq, Eq)]
enum ProfileEditorPhase {
    Account,
    Username,
}
pub(crate) struct ProfileEditor {
    config: SettingsConfig,
    input: ProfilePublication,
    first_name: Entity<InputState>,
    last_name: Entity<InputState>,
    nickname: Entity<InputState>,
    phase: ProfileEditorPhase,
    _binding: Arc<SettingsBinding>,
    _task: Task<()>,
    _inputs: Vec<Subscription>,
    selection: Option<Task<()>>,
}
pub(crate) struct ProfileEditorClosed;
impl EventEmitter<ProfileEditorClosed> for ProfileEditor {}
impl SettingsScreenView {
    pub fn open_profile_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let editor = ProfileEditor::new(self.config.clone(), window, cx);
        self.profile_subscription = Some(cx.subscribe(
            &editor,
            |view, _, _: &ProfileEditorClosed, cx| {
                view.profile_editor = None;
                view.profile_subscription = None;
                cx.notify();
            },
        ));
        self.profile_editor = Some(editor);
        cx.notify();
    }
}
impl ProfileEditor {
    fn new(config: SettingsConfig, window: &mut Window, cx: &mut App) -> Entity<Self> {
        config.client.profile_intent(ProfileIntent::Open {
            section: ProfileEditorSection::Account,
        });
        cx.new(|cx| {
            let input = config.client.profile();
            let first_name = cx.new(|cx| {
                let mut state = InputState::new(window, cx)
                    .placeholder(t!("settings.profile.first_name").to_string());
                state.set_value(input.first_name.clone(), window, cx);
                state
            });
            let last_name = cx.new(|cx| {
                let mut state = InputState::new(window, cx)
                    .placeholder(t!("settings.profile.last_name").to_string());
                state.set_value(input.last_name.clone(), window, cx);
                state
            });
            let nickname = cx.new(|cx| {
                let mut state = InputState::new(window, cx)
                    .placeholder(t!("settings.profile.username").to_string());
                state.set_value(input.nickname.clone(), window, cx);
                state
            });
            let inputs = [&first_name, &last_name, &nickname]
                .into_iter()
                .enumerate()
                .map(|(role, input)| {
                    cx.subscribe(
                        input,
                        move |view: &mut Self, input, event: &InputEvent, cx| {
                            if !matches!(event, InputEvent::Change) {
                                return;
                            }
                            let value = input.read(cx).value().to_string();
                            let (field, current) = match role {
                                0 => (
                                    pioneer_client::settings::profile::ProfileField::FirstName,
                                    &view.input.first_name,
                                ),
                                1 => (
                                    pioneer_client::settings::profile::ProfileField::LastName,
                                    &view.input.last_name,
                                ),
                                _ => (
                                    pioneer_client::settings::profile::ProfileField::Nickname,
                                    &view.input.nickname,
                                ),
                            };
                            if &value != current {
                                view.config.client.profile_intent(ProfileIntent::EditField {
                                    expected_owner: view.input.owner_generation,
                                    field,
                                    value,
                                });
                                view.input = view.config.client.profile();
                                cx.notify();
                            }
                        },
                    )
                })
                .collect();
            first_name.update(cx, |state, cx| state.focus(window, cx));
            let binding = SettingsBinding::new(
                vec![
                    ClientScope::Profile,
                    ClientScope::Administration { workspace_id: None },
                ],
                &config.bindings,
            );
            binding.set_avatar_scope(input.principal_id.as_deref(), &config.bindings);
            let mut changed = binding.changed.subscribe();
            let handle = window.window_handle();
            let task = cx.spawn(async move |view: WeakEntity<Self>, cx| {
                while changed.changed().await.is_ok() {
                    if handle
                        .update(cx, |_, window, cx| {
                            view.update(cx, |view, cx| view.sync(window, cx))
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            });
            Self {
                config,
                input,
                first_name,
                last_name,
                nickname,
                phase: ProfileEditorPhase::Account,
                _binding: binding,
                _task: task,
                _inputs: inputs,
                selection: None,
            }
        })
    }
    fn sync(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let next = self.config.client.profile();
        self._binding
            .set_avatar_scope(next.principal_id.as_deref(), &self.config.bindings);
        if next == self.input {
            cx.notify();
            return;
        }
        let saved = next.saved_revision > self.input.saved_revision
            && next.saved_revision == next.edit_revision;
        for (input, value) in [
            (&self.first_name, &next.first_name),
            (&self.last_name, &next.last_name),
            (&self.nickname, &next.nickname),
        ] {
            if input.read(cx).value().as_ref() != value {
                input.update(cx, |input, cx| input.set_value(value.clone(), window, cx));
            }
        }
        self.phase = if next.nickname_editing {
            ProfileEditorPhase::Username
        } else {
            ProfileEditorPhase::Account
        };
        self.input = next;
        if saved || self.input.principal_id.is_none() {
            cx.emit(ProfileEditorClosed);
        }
        cx.notify();
    }
    fn pick_avatar(&mut self, cx: &mut Context<Self>) {
        let selection = self.config.photos.select(cx);
        let expected_owner = self.input.owner_generation;
        self.selection = Some(cx.spawn(async move |view: WeakEntity<Self>, cx| {
            let result = selection.await;
            let _ = view.update(cx, |view, cx| {
                let intent = match result {
                    Ok(Some(photo)) => ProfileIntent::SelectAvatar {
                        expected_owner,
                        preview: photo.preview,
                        avatar: photo.avatar,
                    },
                    Ok(None) => return,
                    Err(error) => ProfileIntent::SelectionFailed {
                        expected_owner,
                        error: match error {
                            crate::platform::SettingsPhotoError::Picker => "photo_picker_failed",
                            crate::platform::SettingsPhotoError::InvalidAvatar => "avatar_invalid",
                        }
                        .into(),
                    },
                };
                view.config.client.profile_intent(intent);
                view.input = view.config.client.profile();
                cx.notify();
            });
        }));
    }
    fn back(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.phase == ProfileEditorPhase::Account {
            self.config
                .client
                .profile_intent(ProfileIntent::CloseForOwner {
                    expected_owner: self.input.owner_generation,
                    section: self.input.section,
                });
            cx.emit(ProfileEditorClosed);
        } else {
            self.config
                .client
                .profile_intent(ProfileIntent::CancelNicknameEdit);
            self.sync(window, cx);
        }
        cx.notify();
    }
    fn done(&mut self, cx: &mut Context<Self>) {
        let previous_saved = self.input.saved_revision;
        let intent = if self.phase == ProfileEditorPhase::Username {
            ProfileIntent::AcceptNicknameEdit
        } else {
            ProfileIntent::SaveForOwner {
                expected_owner: self.input.owner_generation,
                section: self.input.section,
            }
        };
        self.config.client.profile_intent(intent);
        self.input = self.config.client.profile();
        self.phase = if self.input.nickname_editing {
            ProfileEditorPhase::Username
        } else {
            ProfileEditorPhase::Account
        };
        if self.input.saved_revision > previous_saved
            && self.input.saved_revision == self.input.edit_revision
        {
            cx.emit(ProfileEditorClosed);
        }
        cx.notify();
    }
}
impl Render for ProfileEditor {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let title = if self.phase == ProfileEditorPhase::Account {
            t!("settings.profile.edit_title")
        } else {
            t!("settings.profile.edit_username_title")
        }
        .to_string();
        let header = profile_editor_header(
            title,
            "profile-editor-back",
            "profile-editor-done",
            t!("settings.profile.done").to_string(),
            self.input.pending,
            if self.phase == ProfileEditorPhase::Username {
                self.input.nickname_valid
            } else {
                self.input.valid
            },
            cx.listener(|view, _, window, cx| view.back(window, cx)),
            cx.listener(|view, _, _, cx| view.done(cx)),
            cx,
        );
        let content = if self.phase == ProfileEditorPhase::Username {
            profile_username_editor(self.nickname.clone(), None, cx)
        } else {
            let display_name = [self.input.first_name.trim(), self.input.last_name.trim()]
                .into_iter()
                .filter(|p| !p.is_empty())
                .collect::<Vec<_>>()
                .join(" ");
            let avatar_path = match &self.input.avatar {
                ProfileAvatarSelection::Selected { preview } => {
                    Some(std::path::PathBuf::from(preview))
                }
                ProfileAvatarSelection::Remove => None,
                ProfileAvatarSelection::Unchanged => self
                    .input
                    .principal_id
                    .as_ref()
                    .and_then(|p| self.config.platform.avatar_path(p, cx)),
            };
            let has_avatar = match self.input.avatar {
                ProfileAvatarSelection::Unchanged => self.input.has_saved_avatar,
                ProfileAvatarSelection::Remove => false,
                ProfileAvatarSelection::Selected { .. } => true,
            };
            let entity = cx.entity();
            let saving = self.input.pending;
            let menu_owner = self.input.owner_generation;
            let avatar = div()
                .id("profile-avatar-edit")
                .relative()
                .flex_none()
                .cursor_pointer()
                .on_click(cx.listener(|view, _, _, cx| view.pick_avatar(cx)))
                .context_menu(move |menu, _, _| {
                    let change = entity.clone();
                    let remove = entity.clone();
                    menu.min_w(px(200.))
                        .item(
                            PopupMenuItem::new(t!("settings.profile.change_photo").to_string())
                                .icon(crate::assets::PioneerIconName::Pen)
                                .disabled(saving)
                                .on_click(move |_, _, cx| {
                                    change.update(cx, |view, cx| {
                                        if view.input.owner_generation == menu_owner {
                                            view.pick_avatar(cx);
                                        }
                                    })
                                }),
                        )
                        .item(
                            PopupMenuItem::new(t!("settings.profile.remove_photo").to_string())
                                .icon(crate::assets::PioneerIconName::Trash)
                                .disabled(saving || !has_avatar)
                                .on_click(move |_, _, cx| {
                                    remove.update(cx, |view, cx| {
                                        view.config.client.profile_intent(
                                            ProfileIntent::RemoveAvatarForOwner {
                                                expected_owner: menu_owner,
                                            },
                                        );
                                        view.input = view.config.client.profile();
                                        cx.notify();
                                    })
                                }),
                        )
                })
                .child(profile_avatar(display_name, avatar_path))
                .into_any_element();
            v_flex()
                .w_full()
                .gap_6()
                .child(profile_identity_group(
                    avatar,
                    self.first_name.clone(),
                    self.last_name.clone(),
                    None,
                    cx,
                ))
                .child(profile_username_field(
                    "profile-username-row",
                    self.input.nickname.clone(),
                    None,
                    cx.listener(|view, _, window, cx| {
                        view.config
                            .client
                            .profile_intent(ProfileIntent::BeginNicknameEdit);
                        view.input = view.config.client.profile();
                        view.phase = ProfileEditorPhase::Username;
                        view.nickname
                            .update(cx, |input, cx| input.focus(window, cx));
                        cx.notify();
                    }),
                    cx,
                ))
                .into_any_element()
        };
        let error = self.input.error.as_deref().map(|e| {
            if e == "nickname_unavailable" {
                t!("settings.profile.error_username_unavailable")
            } else if e == "avatar_invalid" {
                t!("settings.profile.error_avatar")
            } else {
                t!("settings.profile.error_save")
            }
            .to_string()
        });
        profile_editor_page(
            if self.phase == ProfileEditorPhase::Account {
                "profile-account-scroll"
            } else {
                "profile-username-scroll"
            },
            header,
            content,
            error,
            cx,
        )
    }
}

#[cfg(test)]
mod retained_tests {
    use super::{ProfileEditor, SettingsConfig};
    use crate::platform::{
        SettingsPhotoError, SettingsPhotoPort, SettingsPhotoSelection, SettingsPlatform,
    };
    use gpui_kit::{App, AppContext, Entity, Task, TestAppContext, Window};
    use pioneer_desktop_foundation::{
        ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink, ClientScope,
        file_opener::FileOpenerId,
        preferences::{AppLanguagePreference, WindowThemePreference},
    };
    use std::{
        rc::Rc,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };
    pub(crate) struct Native;
    impl SettingsPlatform for Native {
        fn telemetry(&self, _: bool) {
            panic!("unexpected native effect");
        }
        fn language(&self, _: &App) -> AppLanguagePreference {
            Default::default()
        }
        fn theme(&self, _: &App) -> WindowThemePreference {
            Default::default()
        }
        fn set_language(&self, _: AppLanguagePreference, _: &mut App) -> anyhow::Result<()> {
            panic!("unexpected native effect");
        }
        fn set_theme(
            &self,
            _: WindowThemePreference,
            _: &mut Window,
            _: &mut App,
        ) -> anyhow::Result<()> {
            panic!("unexpected native effect");
        }
        fn file_opener(&self, _: Option<&str>, _: &App) -> FileOpenerId {
            Default::default()
        }
        fn available_file_openers(&self) -> Vec<FileOpenerId> {
            vec![]
        }
        fn set_file_opener(
            &self,
            _: Option<&str>,
            _: FileOpenerId,
            _: &mut App,
        ) -> anyhow::Result<()> {
            panic!("unexpected native effect");
        }
        fn avatar_path(&self, _: &str, _: &App) -> Option<std::path::PathBuf> {
            None
        }
    }
    impl SettingsPhotoPort for Native {
        fn select(
            &self,
            _: &mut App,
        ) -> Task<Result<Option<SettingsPhotoSelection>, SettingsPhotoError>> {
            panic!("unexpected native effect");
        }
    }
    pub(crate) struct Registrar(pub(crate) Arc<AtomicUsize>);
    impl ClientBindingRegistrar for Registrar {
        fn register(
            &self,
            _: ClientScope,
            _: std::sync::Weak<dyn ClientPublicationSink>,
        ) -> ClientBindingRegistration {
            self.0.fetch_add(1, Ordering::SeqCst);
            let count = self.0.clone();
            ClientBindingRegistration::new(move || {
                count.fetch_sub(1, Ordering::SeqCst);
            })
        }
    }
    struct Host {
        editor: Option<Entity<ProfileEditor>>,
    }
    impl gpui_kit::Render for Host {
        fn render(
            &mut self,
            _: &mut Window,
            _: &mut gpui_kit::Context<Self>,
        ) -> impl gpui_kit::IntoElement {
            use gpui_kit::ParentElement;
            gpui_kit::div().children(self.editor.clone())
        }
    }
    #[gpui_kit::test]
    fn profile_keeps_inputs_selection_and_releases_scoped_bindings(cx: &mut TestAppContext) {
        cx.update(gpui_kit::init);
        let client = pioneer_client::catalog_test_support::settings_client();
        let count = Arc::new(AtomicUsize::new(0));
        let config = SettingsConfig {
            client: client.clone(),
            bindings: Arc::new(Registrar(count.clone())),
            platform: Rc::new(Native),
            photos: Rc::new(Native),
        };
        let (root, cx) = cx.add_window_view(|window, cx| {
            let editor = ProfileEditor::new(config, window, cx);
            let host = cx.new(|_| Host {
                editor: Some(editor),
            });
            gpui_kit::component::Root::new(host, window, cx)
        });
        let host: Entity<Host> =
            root.read_with(cx, |root, _| root.view().clone().downcast().unwrap());
        let editor = host.read_with(cx, |host, _| host.editor.clone().unwrap());
        let input = editor.read_with(cx, |view, _| view.first_name.clone());
        cx.update(|_, cx| input.update(cx, |input, cx| input.set_selected_range(0..9, cx)));
        cx.simulate_input("Edited");
        cx.run_until_parked();
        assert_eq!(client.profile().first_name, "Edited");
        assert_eq!(client.profile().last_name, "User");
        cx.update(|_, cx| input.update(cx, |input, cx| input.set_selected_range(1..3, cx)));
        let revision = client.profile().edit_revision;
        cx.update(|window, cx| editor.update(cx, |view, cx| view.sync(window, cx)));
        cx.run_until_parked();
        assert_eq!(client.profile().edit_revision, revision);
        assert_eq!(input.read_with(cx, |input, _| input.selected_range()), 1..3);
        assert_eq!(
            input.entity_id(),
            editor.read_with(cx, |view, _| view.first_name.entity_id())
        );
        assert_eq!(count.load(Ordering::SeqCst), 3);
        cx.update(|_, cx| {
            host.update(cx, |host, cx| {
                host.editor.take();
                cx.notify();
            })
        });
        drop(editor);
        cx.update(|_, _| {});
        cx.run_until_parked();
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }
}

#[cfg(test)]
pub(crate) use retained_tests::{Native, Registrar};
