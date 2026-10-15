//! Invitation preview, profile and acceptance request ownership.
use super::invitation::{
    InvitationJoinFlow, InvitationJoinPhase, InvitationJoinSafeProfile, InvitationQrPresentation,
    InvitationSessionCommit,
};
use pioneer_protocol::{
    AuthSecretString, ClientInstallationDescriptor, InvitationAcceptParams,
    InvitationAcceptResponse, InvitationPreviewResponse, NewMemberProfile, ProfileAvatarInput,
};
use serde::{Deserialize, Serialize};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct InvitationPublication {
    pub active: bool,
    pub can_cancel: bool,
    pub owner_generation: u64,
    pub revision: u64,
    pub phase: InvitationJoinPhase,
    pub preview: Option<InvitationPreviewResponse>,
    pub first_name: String,
    pub last_name: String,
    pub nickname: String,
    pub avatar_preview: Option<String>,
    pub username_editing: bool,
    pub name_valid: bool,
    pub nickname_valid: bool,
    pub name_error: Option<String>,
    pub nickname_error: Option<String>,
    pub avatar_error: Option<String>,
    pub error: Option<String>,
    pub preview_pending: bool,
    pub submitting: bool,
    pub completed_endpoint: Option<String>,
}
impl Default for InvitationPublication {
    fn default() -> Self {
        Self {
            active: false,
            can_cancel: true,
            owner_generation: 0,
            revision: 0,
            phase: InvitationJoinPhase::Parsing,
            preview: None,
            first_name: String::new(),
            last_name: String::new(),
            nickname: String::new(),
            avatar_preview: None,
            username_editing: false,
            name_valid: false,
            nickname_valid: false,
            name_error: None,
            nickname_error: None,
            avatar_error: None,
            error: None,
            preview_pending: false,
            submitting: false,
            completed_endpoint: None,
        }
    }
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InvitationIntent {
    RemoveAvatarForOwner {
        expected_owner: u64,
    },
    PreviewRetryForOwner {
        expected_owner: u64,
    },
    OpenUsernameForOwner {
        expected_owner: u64,
    },
    AcceptUsernameForOwner {
        expected_owner: u64,
    },
    CancelUsernameForOwner {
        expected_owner: u64,
    },
    EditField {
        expected_owner: u64,
        field: crate::settings::profile::ProfileField,
        value: String,
    },
    Open {
        uri: AuthSecretString,
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
        avatar: ProfileAvatarInput,
    },
    RemoveAvatar,
    AvatarFailed {
        expected_owner: u64,
    },
    OpenUsername,
    AcceptUsername,
    CancelUsername,
    PreviewRetry,
    SubmitForOwner {
        expected_owner: u64,
    },
    Close {
        expected_owner: u64,
    },
    Submit,
    Cancel,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvitationTicket {
    pub owner_generation: u64,
    pub request_generation: u64,
    pub authorization_generation: u64,
    pub gateway_generation: u64,
}
pub enum InvitationRequestKind {
    Preview,
    Accept { params: InvitationAcceptParams },
}
pub struct InvitationRequest {
    pub ticket: InvitationTicket,
    pub presentation: InvitationQrPresentation,
    pub kind: InvitationRequestKind,
}
pub enum InvitationAcceptCompletion {
    Applied,
    Stale(Option<pioneer_protocol::AuthSessionGrant>),
    InvalidGrant(pioneer_protocol::AuthSessionGrant),
}
#[derive(Default)]
pub struct InvitationController {
    publication: InvitationPublication,
    flow: Option<InvitationJoinFlow>,
    avatar: Option<ProfileAvatarInput>,
    username_checkpoint: Option<String>,
    request_generation: u64,
    pending: Option<InvitationTicket>,
    pub(crate) commit: Option<InvitationSessionCommit>,
    commit_in_flight: bool,
}
impl InvitationController {
    pub(crate) fn invalidate_authorization(&mut self) {
        if !self.publication.active || self.commit.is_some() || self.commit_in_flight {
            return;
        }
        let before = self.publication.clone();
        if let Some(flow) = &mut self.flow {
            flow.cancel();
        }
        self.pending = None;
        self.avatar = None;
        self.username_checkpoint = None;
        self.publication = InvitationPublication {
            owner_generation: before.owner_generation + 1,
            revision: before.revision,
            ..Default::default()
        };
        self.project(before);
    }
    pub(crate) fn take_commit(&mut self) -> Option<InvitationSessionCommit> {
        let commit = self.commit.take()?;
        self.commit_in_flight = true;
        Some(commit)
    }
    pub(crate) fn begin_commit_retry(&mut self) -> bool {
        if !self.commit_in_flight || self.publication.submitting {
            return false;
        }
        let before = self.publication.clone();
        self.publication.submitting = true;
        self.publication.error = None;
        self.project(before);
        true
    }
    pub(crate) fn finish_commit(
        &mut self,
        owner: u64,
        result: Result<String, String>,
        retryable_commit: bool,
    ) {
        if self.publication.owner_generation != owner {
            return;
        }
        let before = self.publication.clone();
        self.publication.submitting = false;
        self.commit_in_flight = retryable_commit;
        match result {
            Ok(endpoint) => {
                self.publication.completed_endpoint = Some(endpoint);
                self.publication.error = None;
                self.avatar = None;
                if let Some(flow) = &mut self.flow {
                    let _ = flow.complete();
                }
            }
            Err(code) => {
                self.publication.error = Some(code);
                if !retryable_commit {
                    if let Some(flow) = &mut self.flow {
                        let _ = flow.retryable_failure();
                    }
                }
            }
        }
        self.project(before);
    }
    pub fn publication(&self) -> &InvitationPublication {
        &self.publication
    }
    fn display_name(&self) -> String {
        [
            self.publication.first_name.trim(),
            self.publication.last_name.trim(),
        ]
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
    }
    fn project(&mut self, before: InvitationPublication) {
        self.publication.can_cancel =
            !self.publication.submitting && self.commit.is_none() && !self.commit_in_flight;
        if let Some(flow) = &self.flow {
            self.publication.phase = flow.safe_state().phase;
        }
        self.publication.name_valid =
            NewMemberProfile::new(self.display_name(), "member", None).is_ok();
        self.publication.nickname_valid =
            NewMemberProfile::new("Member", &self.publication.nickname, None).is_ok();
        if self.publication != before {
            self.publication.revision = before.revision + 1;
        }
    }
    pub fn intent(
        &mut self,
        intent: InvitationIntent,
        installation: &ClientInstallationDescriptor,
        authorization_generation: u64,
        gateway_generation: u64,
    ) -> Option<InvitationRequest> {
        let before = self.publication.clone();
        let request = self.reduce(
            intent,
            installation,
            authorization_generation,
            gateway_generation,
        );
        self.project(before);
        request
    }
    fn request(
        &mut self,
        kind: InvitationRequestKind,
        authorization_generation: u64,
        gateway_generation: u64,
    ) -> Option<InvitationRequest> {
        let presentation = self.flow.as_ref()?.presentation().ok()?.clone();
        self.request_generation += 1;
        let ticket = InvitationTicket {
            owner_generation: self.publication.owner_generation,
            request_generation: self.request_generation,
            authorization_generation,
            gateway_generation,
        };
        self.pending = Some(ticket);
        Some(InvitationRequest {
            ticket,
            presentation,
            kind,
        })
    }
    fn reduce(
        &mut self,
        intent: InvitationIntent,
        installation: &ClientInstallationDescriptor,
        authorization_generation: u64,
        gateway_generation: u64,
    ) -> Option<InvitationRequest> {
        match intent {
            InvitationIntent::RemoveAvatarForOwner { expected_owner }
            | InvitationIntent::PreviewRetryForOwner { expected_owner } => {
                if expected_owner != self.publication.owner_generation {
                    return None;
                }
                let action = if matches!(intent, InvitationIntent::RemoveAvatarForOwner { .. }) {
                    InvitationIntent::RemoveAvatar
                } else {
                    InvitationIntent::PreviewRetry
                };
                return self.reduce(
                    action,
                    installation,
                    authorization_generation,
                    gateway_generation,
                );
            }
            InvitationIntent::OpenUsernameForOwner { expected_owner }
            | InvitationIntent::AcceptUsernameForOwner { expected_owner }
            | InvitationIntent::CancelUsernameForOwner { expected_owner } => {
                if expected_owner != self.publication.owner_generation {
                    return None;
                }
                let action = match intent {
                    InvitationIntent::OpenUsernameForOwner { .. } => InvitationIntent::OpenUsername,
                    InvitationIntent::AcceptUsernameForOwner { .. } => {
                        InvitationIntent::AcceptUsername
                    }
                    _ => InvitationIntent::CancelUsername,
                };
                return self.reduce(
                    action,
                    installation,
                    authorization_generation,
                    gateway_generation,
                );
            }
            InvitationIntent::EditField {
                expected_owner,
                field,
                value,
            } => {
                if expected_owner != self.publication.owner_generation
                    || self.publication.submitting
                {
                    return None;
                }
                use crate::settings::profile::ProfileField;
                match field {
                    ProfileField::FirstName => {
                        self.publication.first_name = value;
                        self.publication.name_error = None;
                    }
                    ProfileField::LastName => {
                        self.publication.last_name = value;
                        self.publication.name_error = None;
                    }
                    ProfileField::Nickname => {
                        self.publication.nickname = value;
                        self.publication.nickname_error = None;
                    }
                }
            }
            InvitationIntent::Close { expected_owner } => {
                if expected_owner == self.publication.owner_generation {
                    return self.reduce(
                        InvitationIntent::Cancel,
                        installation,
                        authorization_generation,
                        gateway_generation,
                    );
                }
                return None;
            }
            InvitationIntent::SubmitForOwner { expected_owner } => {
                if expected_owner == self.publication.owner_generation {
                    return self.reduce(
                        InvitationIntent::Submit,
                        installation,
                        authorization_generation,
                        gateway_generation,
                    );
                }
                return None;
            }
            InvitationIntent::Open { uri } => {
                if self
                    .flow
                    .as_ref()
                    .and_then(|flow| flow.presentation().ok())
                    .is_some_and(|p| p.deep_link() == uri.expose_secret())
                    && self.publication.phase != InvitationJoinPhase::Terminal
                {
                    return None;
                }
                // A durable commit must be resolved before replacing its invitation.
                if self.commit.is_some() || self.commit_in_flight {
                    return None;
                }
                self.pending = None;
                self.avatar = None;
                self.username_checkpoint = None;
                self.publication = InvitationPublication {
                    active: true,
                    owner_generation: self.publication.owner_generation + 1,
                    revision: self.publication.revision,
                    ..Default::default()
                };
                match InvitationJoinFlow::from_uri(uri.expose_secret()) {
                    Ok((flow, _)) => {
                        self.flow = Some(flow);
                        self.publication.preview_pending = true;
                        return self.request(
                            InvitationRequestKind::Preview,
                            authorization_generation,
                            gateway_generation,
                        );
                    }
                    Err(_) => {
                        self.flow = None;
                        self.publication.phase = InvitationJoinPhase::Terminal;
                        self.publication.error = Some("invalid_invitation".into());
                    }
                }
            }
            InvitationIntent::Cancel => {
                if self.commit.is_some() || self.commit_in_flight {
                    return None;
                }
                if let Some(flow) = &mut self.flow {
                    flow.cancel();
                }
                self.pending = None;
                self.avatar = None;
                self.publication.active = false;
                self.publication.owner_generation += 1;
                self.publication.preview_pending = false;
                self.publication.submitting = false;
                self.publication.avatar_preview = None;
            }
            InvitationIntent::PreviewRetry => {
                if self.pending.is_some()
                    || self
                        .flow
                        .as_ref()
                        .is_none_or(|flow| flow.presentation().is_err())
                    || self.commit.is_some()
                    || self.commit_in_flight
                {
                    return None;
                }
                self.publication.error = None;
                self.publication.preview_pending = true;
                return self.request(
                    InvitationRequestKind::Preview,
                    authorization_generation,
                    gateway_generation,
                );
            }
            InvitationIntent::EditName {
                first_name,
                last_name,
            } => {
                if self.publication.submitting {
                    return None;
                }
                self.publication.first_name = first_name;
                self.publication.last_name = last_name;
                self.publication.name_error = None;
            }
            InvitationIntent::EditNickname { nickname } => {
                if self.publication.submitting {
                    return None;
                }
                self.publication.nickname = nickname;
                self.publication.nickname_error = None;
            }
            InvitationIntent::SelectAvatar {
                expected_owner,
                preview,
                avatar,
            } => {
                if expected_owner != self.publication.owner_generation
                    || self.publication.submitting
                {
                    return None;
                }
                self.avatar = Some(avatar);
                self.publication.avatar_preview = Some(preview);
                self.publication.avatar_error = None;
            }
            InvitationIntent::RemoveAvatar => {
                if self.publication.submitting {
                    return None;
                }
                self.avatar = None;
                self.publication.avatar_preview = None;
                self.publication.avatar_error = None;
            }
            InvitationIntent::AvatarFailed { expected_owner } => {
                if expected_owner == self.publication.owner_generation {
                    self.publication.avatar_error = Some("avatar_invalid".into());
                }
            }
            InvitationIntent::OpenUsername => {
                if self.publication.submitting {
                    return None;
                }
                self.username_checkpoint = Some(self.publication.nickname.clone());
                self.publication.username_editing = true;
            }
            InvitationIntent::CancelUsername => {
                if self.publication.submitting {
                    return None;
                }
                if let Some(nickname) = self.username_checkpoint.take() {
                    self.publication.nickname = nickname;
                }
                self.publication.username_editing = false;
                self.publication.nickname_error = None;
            }
            InvitationIntent::AcceptUsername => {
                if self.publication.submitting || !self.publication.nickname_valid {
                    return None;
                }
                self.username_checkpoint = None;
                self.publication.username_editing = false;
            }
            InvitationIntent::Submit => {
                if self.pending.is_some()
                    || self.publication.preview.is_none()
                    || self.commit.is_some()
                    || self.commit_in_flight
                {
                    return None;
                }
                let display_name = self.display_name();
                let profile = match NewMemberProfile::new(
                    display_name.clone(),
                    self.publication.nickname.clone(),
                    self.avatar.clone(),
                ) {
                    Ok(profile) => profile,
                    Err(_) => {
                        if !self.publication.name_valid {
                            self.publication.name_error = Some("invalid_profile".into());
                        }
                        if !self.publication.nickname_valid {
                            self.publication.nickname_error = Some("invalid_nickname".into());
                        }
                        return None;
                    }
                };
                let flow = self.flow.as_mut()?;
                flow.update_safe_profile(InvitationJoinSafeProfile {
                    display_name,
                    nickname: self.publication.nickname.clone(),
                    has_avatar: self.avatar.is_some(),
                });
                if flow.submit().is_err() {
                    return None;
                }
                self.publication.submitting = true;
                self.publication.error = None;
                return self.request(
                    InvitationRequestKind::Accept {
                        params: InvitationAcceptParams {
                            profile,
                            installation: installation.clone(),
                        },
                    },
                    authorization_generation,
                    gateway_generation,
                );
            }
        }
        None
    }
    pub fn accepts(
        &self,
        ticket: InvitationTicket,
        authorization_generation: u64,
        gateway_generation: u64,
    ) -> bool {
        self.pending == Some(ticket)
            && self.publication.owner_generation == ticket.owner_generation
            && ticket.authorization_generation == authorization_generation
            && ticket.gateway_generation == gateway_generation
    }
    pub fn complete_preview(
        &mut self,
        ticket: InvitationTicket,
        authorization_generation: u64,
        gateway_generation: u64,
        result: Result<InvitationPreviewResponse, String>,
    ) -> bool {
        if !self.accepts(ticket, authorization_generation, gateway_generation)
            || !self.publication.preview_pending
        {
            return false;
        }
        let before = self.publication.clone();
        self.pending = None;
        self.publication.preview_pending = false;
        match result {
            Ok(preview)
                if self
                    .flow
                    .as_ref()
                    .and_then(|flow| flow.presentation().ok())
                    .is_some_and(|p| p.verify_gateway_id(&preview.gateway_id).is_ok()) =>
            {
                self.publication.preview = Some(preview);
                self.publication.error = None;
                if let Some(flow) = &mut self.flow {
                    flow.preview_succeeded();
                }
            }
            Ok(_) => self.publication.error = Some("gateway_identity_mismatch".into()),
            Err(code) => self.publication.error = Some(code),
        }
        if self.publication.error.as_deref().is_some_and(|code| {
            matches!(code, "invitation_unavailable" | "gateway_identity_mismatch")
        }) {
            if let Some(flow) = &mut self.flow {
                flow.terminal_failure(false);
            }
        }
        self.project(before);
        true
    }
    pub fn complete_accept(
        &mut self,
        ticket: InvitationTicket,
        authorization_generation: u64,
        gateway_generation: u64,
        installation_id: &str,
        result: Result<InvitationAcceptResponse, String>,
    ) -> InvitationAcceptCompletion {
        if !self.accepts(ticket, authorization_generation, gateway_generation)
            || !self.publication.submitting
        {
            return InvitationAcceptCompletion::Stale(result.ok().map(|accepted| accepted.grant));
        }
        let before = self.publication.clone();
        self.pending = None;
        let cleanup = result.as_ref().ok().map(|accepted| accepted.grant.clone());
        let mut invalid_grant = false;
        let result = result.and_then(|accepted| {
            let presentation = self
                .flow
                .as_ref()
                .and_then(|flow| flow.presentation().ok())
                .ok_or_else(|| "invalid_invitation".to_string())?;
            InvitationSessionCommit::new(presentation, accepted, installation_id).map_err(|_| {
                invalid_grant = true;
                "invalid_invitation_grant".into()
            })
        });
        match result {
            Ok(commit) => {
                self.commit = Some(commit);
                if let Some(flow) = &mut self.flow {
                    let _ = flow.accept_succeeded();
                }
            }
            Err(code) => {
                self.publication.submitting = false;
                let terminal = code == "invitation_unavailable";
                match code.as_str() {
                    "invalid_profile" => self.publication.name_error = Some(code),
                    "nickname_unavailable" => self.publication.nickname_error = Some(code),
                    "avatar_invalid" => self.publication.avatar_error = Some(code),
                    _ => self.publication.error = Some(code),
                }
                if let Some(flow) = &mut self.flow {
                    if terminal {
                        flow.terminal_failure(false);
                    } else {
                        let _ = flow.retryable_failure();
                    }
                }
            }
        }
        self.project(before);
        if invalid_grant {
            InvitationAcceptCompletion::InvalidGrant(
                cleanup.expect("invalid grant retained for cleanup"),
            )
        } else {
            InvitationAcceptCompletion::Applied
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn uri(character: char) -> AuthSecretString {
        use pioneer_protocol::*;
        AuthSecretString::new(
            InvitationPresentation::new(
                GatewayBaseUrl::parse_presentation("https://gateway.invalid").unwrap(),
                GatewayId::new("G00000000000000000001").unwrap(),
                InvitationCredential::parse(format!(
                    "{}{}",
                    INVITATION_CREDENTIAL_PREFIX,
                    character.to_string().repeat(INVITATION_CREDENTIAL_BODY_LEN)
                ))
                .unwrap(),
            )
            .unwrap()
            .deep_link(),
        )
    }
    fn installation() -> ClientInstallationDescriptor {
        ClientInstallationDescriptor {
            installation_id: "synthetic".into(),
            display_name: "Synthetic".into(),
            client_kind: pioneer_protocol::ClientKind::Desktop,
            platform: None,
            client_version: None,
        }
    }
    #[test]
    fn unavailable_preview_is_terminal_and_cannot_start_an_empty_retry() {
        let mut owner = InvitationController::default();
        let installation = installation();
        let request = owner
            .intent(
                InvitationIntent::Open { uri: uri('A') },
                &installation,
                1,
                1,
            )
            .unwrap();
        assert!(owner.complete_preview(request.ticket, 1, 1, Err("invitation_unavailable".into())));
        assert_eq!(owner.publication().phase, InvitationJoinPhase::Terminal);
        let before = owner.publication().clone();
        assert!(
            owner
                .intent(InvitationIntent::PreviewRetry, &installation, 1, 1)
                .is_none()
        );
        assert_eq!(owner.publication(), &before);
        assert!(
            owner
                .intent(
                    InvitationIntent::Open { uri: uri('E') },
                    &installation,
                    1,
                    1
                )
                .is_some()
        );
    }
    #[test]
    fn username_cleanup_and_field_edits_cannot_mutate_a_replacement_owner() {
        use crate::settings::profile::ProfileField;
        let mut owner = InvitationController::default();
        let installation = installation();
        owner.intent(
            InvitationIntent::Open { uri: uri('A') },
            &installation,
            1,
            1,
        );
        let old = owner.publication().owner_generation;
        owner.intent(
            InvitationIntent::Open { uri: uri('E') },
            &installation,
            1,
            1,
        );
        let current = owner.publication().owner_generation;
        owner.intent(
            InvitationIntent::EditField {
                expected_owner: current,
                field: ProfileField::Nickname,
                value: "new_member".into(),
            },
            &installation,
            1,
            1,
        );
        owner.intent(
            InvitationIntent::OpenUsernameForOwner {
                expected_owner: current,
            },
            &installation,
            1,
            1,
        );
        owner.intent(
            InvitationIntent::EditField {
                expected_owner: current,
                field: ProfileField::Nickname,
                value: "updated_member".into(),
            },
            &installation,
            1,
            1,
        );
        let before = owner.publication().clone();
        for intent in [
            InvitationIntent::RemoveAvatarForOwner {
                expected_owner: old,
            },
            InvitationIntent::PreviewRetryForOwner {
                expected_owner: old,
            },
            InvitationIntent::CancelUsernameForOwner {
                expected_owner: old,
            },
            InvitationIntent::AcceptUsernameForOwner {
                expected_owner: old,
            },
            InvitationIntent::EditField {
                expected_owner: old,
                field: ProfileField::Nickname,
                value: "stale".into(),
            },
        ] {
            owner.intent(intent, &installation, 1, 1);
            assert_eq!(owner.publication(), &before);
        }
        owner.intent(
            InvitationIntent::CancelUsernameForOwner {
                expected_owner: current,
            },
            &installation,
            1,
            1,
        );
        assert_eq!(owner.publication().nickname, "new_member");
        assert!(!owner.publication().username_editing);
    }
    #[test]
    fn preview_duplicate_retry_cancel_and_replacement_are_generation_scoped() {
        let mut owner = InvitationController::default();
        let installation = installation();
        let first = owner
            .intent(
                InvitationIntent::Open { uri: uri('A') },
                &installation,
                1,
                1,
            )
            .unwrap();
        let before = owner.publication().clone();
        assert!(
            owner
                .intent(
                    InvitationIntent::Open { uri: uri('A') },
                    &installation,
                    1,
                    1
                )
                .is_none()
        );
        assert_eq!(owner.publication(), &before);
        assert!(!owner.complete_preview(first.ticket, 2, 1, Err("wrong session".into())));
        assert!(owner.complete_preview(first.ticket, 1, 1, Err("preview_failed".into())));
        let retry = owner
            .intent(InvitationIntent::PreviewRetry, &installation, 1, 1)
            .unwrap();
        let replacement = owner
            .intent(
                InvitationIntent::Open { uri: uri('E') },
                &installation,
                1,
                1,
            )
            .unwrap();
        let before = owner.publication().clone();
        assert!(!owner.complete_preview(retry.ticket, 1, 1, Err("late preview".into())));
        assert_eq!(owner.publication(), &before);
        owner.intent(InvitationIntent::Cancel, &installation, 1, 1);
        assert!(!owner.complete_preview(replacement.ticket, 1, 1, Err("late preview".into())));
        assert!(
            !serde_json::to_string(owner.publication())
                .unwrap()
                .contains(&"E".repeat(32))
        );
    }
}
