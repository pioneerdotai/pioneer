//! Profile editing baseline and save ownership, independent of editor widgets.
use pioneer_protocol::{AuthPrincipalSnapshot, AuthProfileAvatarUpdate, AuthProfileUpdateParams};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileEditorSection {
    #[default]
    Account,
    Profile,
    Username,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct ProfilePublication {
    pub principal_id: Option<String>,
    pub owner_generation: u64,
    pub first_name: String,
    pub last_name: String,
    pub nickname: String,
    pub avatar: ProfileAvatarSelection,
    pub has_saved_avatar: bool,
    pub edit_revision: u64,
    pub saved_revision: u64,
    pub pending: bool,
    pub dirty: bool,
    pub valid: bool,
    pub nickname_valid: bool,
    pub nickname_editing: bool,
    pub error: Option<String>,
    pub section: ProfileEditorSection,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProfileAvatarSelection {
    #[default]
    Unchanged,
    Remove,
    Selected {
        preview: String,
    },
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileField {
    FirstName,
    LastName,
    Nickname,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProfileIntent {
    SaveForOwner {
        expected_owner: u64,
        section: ProfileEditorSection,
    },
    CloseForOwner {
        expected_owner: u64,
        section: ProfileEditorSection,
    },
    RemoveAvatarForOwner {
        expected_owner: u64,
    },
    EditField {
        expected_owner: u64,
        field: ProfileField,
        value: String,
    },
    Open {
        section: ProfileEditorSection,
    },
    EditName {
        first_name: String,
        last_name: String,
    },
    EditNickname {
        nickname: String,
    },
    SelectAvatar {
        expected_owner: u64,
        preview: String,
        avatar: pioneer_protocol::ProfileAvatarInput,
    },
    RemoveAvatar,
    BeginNicknameEdit,
    CancelNicknameEdit,
    AcceptNicknameEdit,
    SelectionFailed {
        expected_owner: u64,
        error: String,
    },
    Save,
    Cancel,
}
#[derive(Default)]
pub struct ProfileStore {
    pub(crate) publication: ProfilePublication,
    baseline: Option<AuthPrincipalSnapshot>,
    avatar: AuthProfileAvatarUpdate,
    generation: u64,
    nickname_checkpoint: Option<String>,
    pending: Option<(u64, u64, AuthProfileUpdateParams)>,
}
pub(crate) struct ProfileSave {
    pub generation: u64,
    pub params: AuthProfileUpdateParams,
}
impl ProfileStore {
    pub(crate) fn synchronize(&mut self, principal: Option<&AuthPrincipalSnapshot>) {
        if self.baseline.as_ref().map(|v| &v.id) != principal.map(|v| &v.id) {
            self.reset(principal);
        } else if self.baseline.as_ref() != principal {
            if !self.publication.dirty && self.pending.is_none() {
                self.reset(principal);
            } else {
                self.baseline = principal.cloned();
                self.validate();
            }
        }
    }
    pub(crate) fn invalidate(&mut self) {
        self.reset(None);
    }
    fn reset(&mut self, principal: Option<&AuthPrincipalSnapshot>) {
        self.generation += 1;
        self.pending = None;
        self.avatar = AuthProfileAvatarUpdate::Unchanged;
        self.nickname_checkpoint = None;
        let section = self.publication.section;
        let revision = self.publication.edit_revision + 1;
        self.publication = ProfilePublication {
            section,
            owner_generation: self.generation,
            edit_revision: revision,
            ..Default::default()
        };
        self.baseline = principal.cloned();
        if let Some(principal) = principal {
            self.publication.principal_id = Some(principal.id.to_string());
            let (first, last) = principal
                .display_name
                .split_once(' ')
                .unwrap_or((&principal.display_name, ""));
            self.publication.first_name = first.into();
            self.publication.last_name = last.into();
            self.publication.nickname = principal.nickname.clone();
            self.publication.has_saved_avatar = principal.avatar_revision.is_some();
        }
        self.validate();
    }
    fn params(&self) -> Option<AuthProfileUpdateParams> {
        let baseline = self.baseline.as_ref()?;
        let display_name = if self.publication.section == ProfileEditorSection::Username {
            baseline.display_name.clone()
        } else {
            [
                self.publication.first_name.trim(),
                self.publication.last_name.trim(),
            ]
            .into_iter()
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join(" ")
        };
        let nickname = if self.publication.section == ProfileEditorSection::Profile {
            &baseline.nickname
        } else {
            &self.publication.nickname
        };
        let avatar = if self.publication.section == ProfileEditorSection::Username {
            AuthProfileAvatarUpdate::Unchanged
        } else {
            self.avatar.clone()
        };
        AuthProfileUpdateParams::new(display_name, nickname, avatar).ok()
    }
    fn validate(&mut self) {
        let params = self.params();
        self.publication.valid = params.is_some();
        self.publication.nickname_valid = AuthProfileUpdateParams::new(
            "Member",
            &self.publication.nickname,
            AuthProfileAvatarUpdate::Unchanged,
        )
        .is_ok();
        self.publication.dirty = self.baseline.as_ref().is_some_and(|baseline| {
            let display_name = [
                self.publication.first_name.trim(),
                self.publication.last_name.trim(),
            ]
            .into_iter()
            .filter(|p| !p.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
            (self.publication.section != ProfileEditorSection::Username
                && (display_name != baseline.display_name
                    || self.avatar != AuthProfileAvatarUpdate::Unchanged))
                || (self.publication.section != ProfileEditorSection::Profile
                    && self.publication.nickname.trim() != baseline.nickname)
        });
    }
    pub(crate) fn intent(&mut self, intent: ProfileIntent) -> Option<ProfileSave> {
        if self.baseline.is_none() {
            return None;
        }
        let before = self.publication.clone();
        match intent {
            ProfileIntent::SaveForOwner {
                expected_owner,
                section,
            } => {
                if expected_owner != self.publication.owner_generation
                    || section != self.publication.section
                {
                    return None;
                }
                return self.intent(ProfileIntent::Save);
            }
            ProfileIntent::CloseForOwner {
                expected_owner,
                section,
            } => {
                if expected_owner != self.publication.owner_generation
                    || section != self.publication.section
                {
                    return None;
                }
                if section == ProfileEditorSection::Username {
                    self.publication.nickname = self.baseline.as_ref()?.nickname.clone();
                    self.publication.nickname_editing = false;
                    self.nickname_checkpoint = None;
                } else {
                    return self.intent(ProfileIntent::Cancel);
                }
            }
            ProfileIntent::RemoveAvatarForOwner { expected_owner } => {
                if expected_owner != self.publication.owner_generation {
                    return None;
                }
                return self.intent(ProfileIntent::RemoveAvatar);
            }
            ProfileIntent::EditField {
                expected_owner,
                field,
                value,
            } => {
                if expected_owner != self.publication.owner_generation {
                    return None;
                }
                match field {
                    ProfileField::FirstName => self.publication.first_name = value,
                    ProfileField::LastName => self.publication.last_name = value,
                    ProfileField::Nickname => self.publication.nickname = value,
                }
            }
            ProfileIntent::Open { section } => {
                if section == ProfileEditorSection::Username && self.publication.section != section
                {
                    self.publication.nickname = self
                        .baseline
                        .as_ref()
                        .map(|p| p.nickname.clone())
                        .unwrap_or_default();
                }
                self.publication.section = section;
                self.publication.error = None;
            }
            ProfileIntent::EditName {
                first_name,
                last_name,
            } => {
                self.publication.first_name = first_name;
                self.publication.last_name = last_name;
            }
            ProfileIntent::EditNickname { nickname } => self.publication.nickname = nickname,
            ProfileIntent::SelectAvatar {
                expected_owner,
                preview,
                avatar,
            } => {
                if expected_owner != self.publication.owner_generation {
                    return None;
                }
                self.avatar = AuthProfileAvatarUpdate::Set { avatar };
                self.publication.avatar = ProfileAvatarSelection::Selected { preview };
            }
            ProfileIntent::BeginNicknameEdit => {
                self.nickname_checkpoint = Some(self.publication.nickname.clone());
                self.publication.nickname_editing = true;
            }
            ProfileIntent::CancelNicknameEdit => {
                if let Some(previous) = self.nickname_checkpoint.take() {
                    self.publication.nickname = previous;
                }
                self.publication.nickname_editing = false;
            }
            ProfileIntent::AcceptNicknameEdit => {
                if !self.publication.nickname_valid {
                    self.publication.error = Some("invalid_profile".into());
                    return None;
                }
                self.nickname_checkpoint = None;
                self.publication.nickname_editing = false;
            }
            ProfileIntent::RemoveAvatar => {
                let present = self
                    .baseline
                    .as_ref()
                    .is_some_and(|b| b.avatar_revision.is_some());
                self.avatar = if present {
                    AuthProfileAvatarUpdate::Remove
                } else {
                    AuthProfileAvatarUpdate::Unchanged
                };
                self.publication.avatar = if present {
                    ProfileAvatarSelection::Remove
                } else {
                    ProfileAvatarSelection::Unchanged
                };
            }
            ProfileIntent::SelectionFailed {
                expected_owner,
                error,
            } => {
                if expected_owner != self.publication.owner_generation {
                    return None;
                }
                self.publication.error = Some(error);
                return None;
            }
            ProfileIntent::Save => {
                self.validate();
                if self.pending.is_some() {
                    return None;
                }
                if !self.publication.dirty {
                    self.publication.saved_revision = self.publication.edit_revision;
                    return None;
                }
                let params = self.params()?;
                self.generation += 1;
                self.pending = Some((
                    self.generation,
                    self.publication.edit_revision,
                    params.clone(),
                ));
                self.publication.pending = true;
                self.publication.error = None;
                return Some(ProfileSave {
                    generation: self.generation,
                    params,
                });
            }
            ProfileIntent::Cancel => {
                let baseline = self.baseline.clone();
                self.reset(baseline.as_ref());
                return None;
            }
        }
        if self.publication != before {
            self.publication.edit_revision += 1;
            self.publication.error = None;
            self.validate();
        }
        None
    }
    pub(crate) fn accepts(&self, generation: u64) -> bool {
        self.pending.as_ref().is_some_and(|p| p.0 == generation)
    }
    pub(crate) fn complete(
        &mut self,
        generation: u64,
        result: Result<&AuthPrincipalSnapshot, String>,
    ) -> bool {
        let Some((pending, revision, submitted)) = self.pending.clone() else {
            return false;
        };
        if pending != generation {
            return false;
        }
        if result
            .as_ref()
            .is_ok_and(|principal| self.baseline.as_ref().is_none_or(|p| p.id != principal.id))
        {
            return false;
        }
        self.pending = None;
        self.publication.pending = false;
        match result {
            Ok(principal) => {
                self.baseline = Some(principal.clone());
                self.publication.has_saved_avatar = principal.avatar_revision.is_some();
                if self.avatar == submitted.avatar {
                    self.avatar = AuthProfileAvatarUpdate::Unchanged;
                    self.publication.avatar = ProfileAvatarSelection::Unchanged;
                }
                self.publication.saved_revision = revision;
                self.publication.error = None;
            }
            Err(error) => self.publication.error = Some(error),
        }
        self.validate();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn principal() -> AuthPrincipalSnapshot {
        AuthPrincipalSnapshot {
            id: pioneer_protocol::PrincipalId::new("P00000000000000000001").unwrap(),
            kind: pioneer_protocol::PrincipalKind::User,
            display_name: "Alice Smith".into(),
            nickname: "alice".into(),
            avatar_revision: None,
        }
    }
    fn store() -> ProfileStore {
        let mut store = ProfileStore::default();
        store.synchronize(Some(&principal()));
        store
    }
    #[test]
    fn scoped_field_edits_preserve_siblings_and_stale_save_close_are_noops() {
        let mut store = store();
        let owner = store.publication.owner_generation;
        store.intent(ProfileIntent::EditField {
            expected_owner: owner,
            field: ProfileField::FirstName,
            value: "Alicia".into(),
        });
        store.intent(ProfileIntent::EditField {
            expected_owner: owner,
            field: ProfileField::LastName,
            value: "Jones".into(),
        });
        assert_eq!(store.publication.first_name, "Alicia");
        assert_eq!(store.publication.last_name, "Jones");
        let before = store.publication.clone();
        store.intent(ProfileIntent::EditField {
            expected_owner: owner,
            field: ProfileField::LastName,
            value: "Jones".into(),
        });
        assert_eq!(store.publication, before);
        store.invalidate();
        store.synchronize(Some(&principal()));
        let before = store.publication.clone();
        for intent in [
            ProfileIntent::SaveForOwner {
                expected_owner: owner,
                section: ProfileEditorSection::Account,
            },
            ProfileIntent::CloseForOwner {
                expected_owner: owner,
                section: ProfileEditorSection::Account,
            },
        ] {
            assert!(store.intent(intent).is_none());
            assert_eq!(store.publication, before);
        }
    }
    #[test]
    fn username_close_discards_only_username_and_preserves_parent_profile_draft() {
        let mut store = store();
        let owner = store.publication.owner_generation;
        store.intent(ProfileIntent::Open {
            section: ProfileEditorSection::Profile,
        });
        store.intent(ProfileIntent::EditField {
            expected_owner: owner,
            field: ProfileField::FirstName,
            value: "Alicia".into(),
        });
        store.intent(ProfileIntent::Open {
            section: ProfileEditorSection::Username,
        });
        store.intent(ProfileIntent::EditField {
            expected_owner: owner,
            field: ProfileField::Nickname,
            value: "changed".into(),
        });
        assert!(
            store
                .intent(ProfileIntent::SaveForOwner {
                    expected_owner: owner,
                    section: ProfileEditorSection::Profile
                })
                .is_none()
        );
        store.intent(ProfileIntent::CloseForOwner {
            expected_owner: owner,
            section: ProfileEditorSection::Username,
        });
        assert_eq!(store.publication.nickname, "alice");
        assert_eq!(store.publication.first_name, "Alicia");
        store.intent(ProfileIntent::Open {
            section: ProfileEditorSection::Profile,
        });
        assert!(store.publication.dirty);
    }
    #[test]
    fn equal_controlled_input_has_no_edit_revision_and_invalid_draft_survives_identity_refresh() {
        let mut store = store();
        let revision = store.publication.edit_revision;
        store.intent(ProfileIntent::EditName {
            first_name: "Alice".into(),
            last_name: "Smith".into(),
        });
        assert_eq!(store.publication.edit_revision, revision);
        store.intent(ProfileIntent::EditName {
            first_name: String::new(),
            last_name: String::new(),
        });
        assert!(store.publication.dirty);
        assert!(!store.publication.valid);
        assert!(store.intent(ProfileIntent::Save).is_none());
        let mut changed = principal();
        changed.nickname = "alice.new".into();
        store.synchronize(Some(&changed));
        assert_eq!(store.publication.first_name, "");
        assert!(store.publication.dirty);
    }
    #[test]
    fn single_submit_failure_retry_and_late_completion_preserve_newer_edits() {
        let mut store = store();
        store.intent(ProfileIntent::EditName {
            first_name: "Alicia".into(),
            last_name: "Smith".into(),
        });
        let first = store.intent(ProfileIntent::Save).unwrap();
        assert!(store.intent(ProfileIntent::Save).is_none());
        assert!(store.complete(first.generation, Err("offline".into())));
        assert!(store.publication.dirty);
        assert_eq!(store.publication.saved_revision, 0);
        let retry = store.intent(ProfileIntent::Save).unwrap();
        assert!(!store.complete(first.generation, Err("late".into())));
        store.intent(ProfileIntent::EditName {
            first_name: "Ally".into(),
            last_name: "Smith".into(),
        });
        let mut saved = principal();
        saved.display_name = "Alicia Smith".into();
        assert!(store.complete(retry.generation, Ok(&saved)));
        assert_eq!(store.publication.first_name, "Ally");
        assert!(store.publication.dirty);
        assert!(store.publication.saved_revision < store.publication.edit_revision);
    }
    #[test]
    fn wrong_principal_and_access_loss_cannot_commit_profile() {
        let mut store = store();
        store.intent(ProfileIntent::EditNickname {
            nickname: "another".into(),
        });
        let request = store.intent(ProfileIntent::Save).unwrap();
        let mut wrong = principal();
        wrong.id = pioneer_protocol::PrincipalId::new("P00000000000000000002").unwrap();
        assert!(!store.complete(request.generation, Ok(&wrong)));
        assert!(store.publication.pending);
        store.invalidate();
        assert!(!store.complete(request.generation, Ok(&principal())));
        assert!(store.publication.principal_id.is_none());
        assert_eq!(store.publication.first_name, "");
        assert!(
            store
                .intent(ProfileIntent::EditNickname {
                    nickname: "late".into()
                })
                .is_none()
        );
        assert_eq!(store.publication.nickname, "");
    }
    #[test]
    fn avatar_bytes_never_enter_publication_and_success_does_not_resend_submitted_avatar() {
        let mut store = store();
        let avatar = pioneer_protocol::ProfileAvatarInput::new(
            pioneer_protocol::ProfileAvatarMediaType::Png,
            "aGVsbG8=",
        )
        .unwrap();
        store.intent(ProfileIntent::SelectAvatar {
            expected_owner: store.publication.owner_generation,
            preview: "synthetic-avatar.png".into(),
            avatar,
        });
        let request = store.intent(ProfileIntent::Save).unwrap();
        assert!(
            !serde_json::to_string(&store.publication)
                .unwrap()
                .contains("aGVsbG8=")
        );
        store.intent(ProfileIntent::EditName {
            first_name: "New".into(),
            last_name: "Name".into(),
        });
        let mut saved = principal();
        saved.avatar_revision = Some("avatar1".into());
        assert!(store.complete(request.generation, Ok(&saved)));
        assert_eq!(store.publication.avatar, ProfileAvatarSelection::Unchanged);
        assert!(store.publication.dirty);
        assert_eq!(
            store.intent(ProfileIntent::Save).unwrap().params.avatar,
            AuthProfileAvatarUpdate::Unchanged
        );
    }
    #[test]
    fn profile_name_and_nickname_validation_preserves_unicode_and_boundary_contracts() {
        let mut store = store();
        for invalid in [String::new(), "A\0B".into(), "a".repeat(129)] {
            store.intent(ProfileIntent::EditName {
                first_name: invalid,
                last_name: String::new(),
            });
            assert!(!store.publication.valid);
            assert!(store.intent(ProfileIntent::Save).is_none());
        }
        store.intent(ProfileIntent::EditName {
            first_name: " Александр ".into(),
            last_name: " Оскин ".into(),
        });
        assert!(store.publication.valid);
        for invalid in [String::new(), "_member".into(), "a".repeat(33)] {
            store.intent(ProfileIntent::EditNickname { nickname: invalid });
            assert!(!store.publication.valid);
        }
        store.intent(ProfileIntent::EditNickname {
            nickname: "member_name".into(),
        });
        assert!(store.publication.valid);
    }
}
