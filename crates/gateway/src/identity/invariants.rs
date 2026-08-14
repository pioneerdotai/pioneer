use pioneer_crud::{
    IdentityInvariantRows, actor_ref_from_db, principal_kind_from_db, principal_status_from_db,
};
use pioneer_protocol::{
    GatewayId, PersistedActorRef, PrincipalId, PrincipalKind, PrincipalStatus, RoleKey,
};
use std::collections::HashSet;
use std::fmt;

const MAX_IDENTITY_DIAGNOSTICS: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum IdentityInvariantKind {
    MissingGateway,
    MultipleGateways,
    InvalidGatewaySingleton,
    InvalidGatewayId,
    UnsupportedBootstrapVersion,
    InvalidPrincipalId,
    InvalidPrincipalGatewayId,
    CrossGatewayPrincipal,
    UnknownPrincipalKind,
    UnknownPrincipalStatus,
    InvalidPrincipalRemovalState,
    InvalidNickname,
    DuplicateNickname,
    MissingSuperuser,
    MultipleSuperusers,
    InvalidSuperuserState,
    InvalidUserRole,
    InvalidUserState,
    MissingActor,
    InvalidActorPair,
    DanglingPrincipalActor,
}

impl IdentityInvariantKind {
    fn code(self) -> &'static str {
        match self {
            Self::MissingGateway => "missing_gateway",
            Self::MultipleGateways => "multiple_gateways",
            Self::InvalidGatewaySingleton => "invalid_gateway_singleton",
            Self::InvalidGatewayId => "invalid_gateway_id",
            Self::UnsupportedBootstrapVersion => "unsupported_bootstrap_version",
            Self::InvalidPrincipalId => "invalid_principal_id",
            Self::InvalidPrincipalGatewayId => "invalid_principal_gateway_id",
            Self::CrossGatewayPrincipal => "cross_gateway_principal",
            Self::UnknownPrincipalKind => "unknown_principal_kind",
            Self::UnknownPrincipalStatus => "unknown_principal_status",
            Self::InvalidPrincipalRemovalState => "invalid_principal_removal_state",
            Self::InvalidNickname => "invalid_nickname",
            Self::DuplicateNickname => "duplicate_nickname",
            Self::MissingSuperuser => "missing_superuser",
            Self::MultipleSuperusers => "multiple_superusers",
            Self::InvalidSuperuserState => "invalid_superuser_state",
            Self::InvalidUserRole => "invalid_user_role",
            Self::InvalidUserState => "invalid_user_state",
            Self::MissingActor => "missing_actor",
            Self::InvalidActorPair => "invalid_actor_pair",
            Self::DanglingPrincipalActor => "dangling_principal_actor",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdentityInvariantError {
    violations: Vec<IdentityInvariantKind>,
    omitted: usize,
}

impl IdentityInvariantError {
    #[cfg(test)]
    pub(crate) fn violations(&self) -> &[IdentityInvariantKind] {
        &self.violations
    }
}

impl fmt::Display for IdentityInvariantError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("identity invariant violation: ")?;
        for (index, violation) in self.violations.iter().enumerate() {
            if index > 0 {
                formatter.write_str(",")?;
            }
            formatter.write_str(violation.code())?;
        }
        if self.omitted > 0 {
            write!(formatter, ",omitted={}", self.omitted)?;
        }
        Ok(())
    }
}

impl std::error::Error for IdentityInvariantError {}

pub(crate) fn validate_identity_invariants(
    rows: &IdentityInvariantRows,
    supported_bootstrap_version: i64,
) -> Result<(), IdentityInvariantError> {
    let mut violations = Vec::new();
    let gateway = match rows.gateways.as_slice() {
        [] => {
            push_violation(&mut violations, IdentityInvariantKind::MissingGateway);
            None
        }
        [gateway] => Some(gateway),
        gateways => {
            push_violation(&mut violations, IdentityInvariantKind::MultipleGateways);
            gateways.first()
        }
    };

    let canonical_gateway_id = gateway.and_then(|gateway| {
        if gateway.singleton_key != 1 {
            push_violation(
                &mut violations,
                IdentityInvariantKind::InvalidGatewaySingleton,
            );
        }
        if gateway.identity_bootstrap_version < 0
            || gateway.identity_bootstrap_version > supported_bootstrap_version
        {
            push_violation(
                &mut violations,
                IdentityInvariantKind::UnsupportedBootstrapVersion,
            );
        }
        match GatewayId::new(gateway.id.as_str()) {
            Ok(id) => Some(id),
            Err(_) => {
                push_violation(&mut violations, IdentityInvariantKind::InvalidGatewayId);
                None
            }
        }
    });

    let mut principal_ids = HashSet::new();
    let mut nickname_keys = HashSet::new();
    let mut superuser_count = 0usize;
    for principal in &rows.principals {
        let principal_id = match PrincipalId::new(principal.id.as_str()) {
            Ok(id) => {
                principal_ids.insert(id.clone());
                Some(id)
            }
            Err(_) => {
                push_violation(&mut violations, IdentityInvariantKind::InvalidPrincipalId);
                None
            }
        };
        let principal_gateway_id = match GatewayId::new(principal.gateway_id.as_str()) {
            Ok(id) => Some(id),
            Err(_) => {
                push_violation(
                    &mut violations,
                    IdentityInvariantKind::InvalidPrincipalGatewayId,
                );
                None
            }
        };
        if let (Some(expected), Some(actual)) =
            (canonical_gateway_id.as_ref(), principal_gateway_id.as_ref())
            && expected != actual
        {
            push_violation(
                &mut violations,
                IdentityInvariantKind::CrossGatewayPrincipal,
            );
        }

        let kind = match principal_kind_from_db(&principal.kind) {
            Ok(kind) => Some(kind),
            Err(_) => {
                push_violation(&mut violations, IdentityInvariantKind::UnknownPrincipalKind);
                None
            }
        };
        let status = match principal_status_from_db(&principal.status) {
            Ok(status) => Some(status),
            Err(_) => {
                push_violation(
                    &mut violations,
                    IdentityInvariantKind::UnknownPrincipalStatus,
                );
                None
            }
        };

        if matches!(status, Some(PrincipalStatus::Removed)) != principal.removed_at.is_some() {
            push_violation(
                &mut violations,
                IdentityInvariantKind::InvalidPrincipalRemovalState,
            );
        }
        let nickname = principal.nickname.trim();
        let nickname_key = principal.nickname_key.trim();
        if principal.display_name.trim().is_empty()
            || nickname.is_empty()
            || nickname_key.is_empty()
            || principal.nickname != nickname
            || principal.nickname_key != nickname_key
            || nickname.to_lowercase() != nickname_key
        {
            push_violation(&mut violations, IdentityInvariantKind::InvalidNickname);
        }
        if !nickname_keys.insert((principal.gateway_id.as_str(), nickname_key)) {
            push_violation(&mut violations, IdentityInvariantKind::DuplicateNickname);
        }

        if kind == Some(PrincipalKind::Superuser) {
            superuser_count += 1;
            if principal_id.is_none()
                || principal_gateway_id.as_ref() != canonical_gateway_id.as_ref()
                || principal.role_key.is_some()
                || status != Some(PrincipalStatus::Active)
                || principal.removed_at.is_some()
            {
                push_violation(
                    &mut violations,
                    IdentityInvariantKind::InvalidSuperuserState,
                );
            }
        } else if kind == Some(PrincipalKind::User) {
            let role = principal
                .role_key
                .as_deref()
                .and_then(|value| RoleKey::new(value).ok());
            if crate::authorization::AuthorizationService::new()
                .resolved_role_key(PrincipalKind::User, role.as_ref())
                .is_none()
            {
                push_violation(&mut violations, IdentityInvariantKind::InvalidUserRole);
            }
            if principal_id.is_none()
                || principal_gateway_id.as_ref() != canonical_gateway_id.as_ref()
                || status.is_none()
            {
                push_violation(&mut violations, IdentityInvariantKind::InvalidUserState);
            }
        }
    }

    match superuser_count {
        0 => push_violation(&mut violations, IdentityInvariantKind::MissingSuperuser),
        1 => {}
        _ => push_violation(&mut violations, IdentityInvariantKind::MultipleSuperusers),
    }

    for actor in &rows.actor_references {
        match actor_ref_from_db(actor.actor_kind.as_deref(), actor.actor_id.as_deref()) {
            Ok(None) => push_violation(&mut violations, IdentityInvariantKind::MissingActor),
            Ok(Some(PersistedActorRef::System)) => {}
            Ok(Some(PersistedActorRef::Principal(id))) => {
                if !principal_ids.contains(&id) {
                    push_violation(
                        &mut violations,
                        IdentityInvariantKind::DanglingPrincipalActor,
                    );
                }
            }
            Err(_) => push_violation(&mut violations, IdentityInvariantKind::InvalidActorPair),
        }
    }

    if violations.is_empty() {
        Ok(())
    } else {
        let omitted = violations.len().saturating_sub(MAX_IDENTITY_DIAGNOSTICS);
        violations.truncate(MAX_IDENTITY_DIAGNOSTICS);
        Err(IdentityInvariantError {
            violations,
            omitted,
        })
    }
}

fn push_violation(violations: &mut Vec<IdentityInvariantKind>, violation: IdentityInvariantKind) {
    if !violations.contains(&violation) {
        violations.push(violation);
    }
}

#[cfg(test)]
mod tests {
    use super::{IdentityInvariantKind, validate_identity_invariants};
    use chrono::Utc;
    use pioneer_crud::{ActorReferenceRow, ActorResourceKind, IdentityInvariantRows};
    use pioneer_entity::{gateway_identity, gateway_principal};

    const GATEWAY_ID: &str = "G00000000000000000001";
    const PRINCIPAL_ID: &str = "P00000000000000000001";

    fn valid_rows() -> IdentityInvariantRows {
        let now = Utc::now().fixed_offset();
        IdentityInvariantRows {
            gateways: vec![gateway_identity::Model {
                id: GATEWAY_ID.to_owned(),
                singleton_key: 1,
                identity_bootstrap_version: 0,
                auth_schema_version: 0,
                auth_ready_at: None,
                created_at: now,
                updated_at: now,
            }],
            principals: vec![gateway_principal::Model {
                id: PRINCIPAL_ID.to_owned(),
                gateway_id: GATEWAY_ID.to_owned(),
                kind: "superuser".to_owned(),
                role_key: None,
                status: "active".to_owned(),
                display_name: "Superuser".to_owned(),
                nickname: "superuser".to_owned(),
                nickname_key: "superuser".to_owned(),
                created_at: now,
                updated_at: now,
                removed_at: None,
                authorization_guard: 1,
            }],
            actor_references: vec![
                ActorReferenceRow {
                    resource_kind: ActorResourceKind::Thread,
                    resource_id: "thread-1".to_owned(),
                    actor_kind: Some("principal".to_owned()),
                    actor_id: Some(PRINCIPAL_ID.to_owned()),
                },
                ActorReferenceRow {
                    resource_kind: ActorResourceKind::Turn,
                    resource_id: "turn-1".to_owned(),
                    actor_kind: Some("system".to_owned()),
                    actor_id: None,
                },
            ],
        }
    }

    #[test]
    fn valid_identity_and_actor_state_passes() {
        validate_identity_invariants(&valid_rows(), 1).expect("valid rows must pass");
    }

    #[test]
    fn valid_edited_superuser_profile_does_not_change_the_identity_invariant() {
        let mut rows = valid_rows();
        rows.principals[0].display_name = "Gateway administrator".to_owned();
        rows.principals[0].nickname = "gateway-admin".to_owned();
        rows.principals[0].nickname_key = "gateway-admin".to_owned();

        validate_identity_invariants(&rows, 1)
            .expect("a valid profile edit must not invalidate the stable Superuser identity");
    }

    #[test]
    fn every_corrupt_state_fails_closed_with_a_bounded_safe_code() {
        let cases: Vec<(
            IdentityInvariantKind,
            Box<dyn Fn(&mut IdentityInvariantRows)>,
        )> = vec![
            (
                IdentityInvariantKind::MissingGateway,
                Box::new(|rows| rows.gateways.clear()),
            ),
            (
                IdentityInvariantKind::MultipleGateways,
                Box::new(|rows| rows.gateways.push(rows.gateways[0].clone())),
            ),
            (
                IdentityInvariantKind::InvalidGatewaySingleton,
                Box::new(|rows| rows.gateways[0].singleton_key = 2),
            ),
            (
                IdentityInvariantKind::InvalidGatewayId,
                Box::new(|rows| rows.gateways[0].id = "gateway".to_owned()),
            ),
            (
                IdentityInvariantKind::UnsupportedBootstrapVersion,
                Box::new(|rows| rows.gateways[0].identity_bootstrap_version = 2),
            ),
            (
                IdentityInvariantKind::InvalidPrincipalId,
                Box::new(|rows| rows.principals[0].id = "principal".to_owned()),
            ),
            (
                IdentityInvariantKind::InvalidPrincipalGatewayId,
                Box::new(|rows| rows.principals[0].gateway_id = "gateway".to_owned()),
            ),
            (
                IdentityInvariantKind::CrossGatewayPrincipal,
                Box::new(|rows| rows.principals[0].gateway_id = "G00000000000000000002".to_owned()),
            ),
            (
                IdentityInvariantKind::UnknownPrincipalKind,
                Box::new(|rows| rows.principals[0].kind = "owner".to_owned()),
            ),
            (
                IdentityInvariantKind::UnknownPrincipalStatus,
                Box::new(|rows| rows.principals[0].status = "enabled".to_owned()),
            ),
            (
                IdentityInvariantKind::InvalidPrincipalRemovalState,
                Box::new(|rows| rows.principals[0].status = "removed".to_owned()),
            ),
            (
                IdentityInvariantKind::InvalidNickname,
                Box::new(|rows| rows.principals[0].nickname_key = "SUPERUSER".to_owned()),
            ),
            (
                IdentityInvariantKind::InvalidNickname,
                Box::new(|rows| rows.principals[0].nickname = " superuser ".to_owned()),
            ),
            (
                IdentityInvariantKind::DuplicateNickname,
                Box::new(|rows| {
                    let mut user = rows.principals[0].clone();
                    user.id = "P00000000000000000002".to_owned();
                    user.kind = "user".to_owned();
                    user.role_key = Some("member".to_owned());
                    rows.principals.push(user);
                }),
            ),
            (
                IdentityInvariantKind::MissingSuperuser,
                Box::new(|rows| {
                    rows.principals[0].kind = "user".to_owned();
                    rows.principals[0].role_key = Some("member".to_owned());
                }),
            ),
            (
                IdentityInvariantKind::MultipleSuperusers,
                Box::new(|rows| {
                    let mut duplicate = rows.principals[0].clone();
                    duplicate.id = "P00000000000000000002".to_owned();
                    duplicate.nickname = "second".to_owned();
                    duplicate.nickname_key = "second".to_owned();
                    rows.principals.push(duplicate);
                }),
            ),
            (
                IdentityInvariantKind::InvalidSuperuserState,
                Box::new(|rows| rows.principals[0].role_key = Some("member".to_owned())),
            ),
            (
                IdentityInvariantKind::InvalidUserRole,
                Box::new(|rows| {
                    rows.principals[0].kind = "user".to_owned();
                    rows.principals[0].role_key = Some("viewer".to_owned());
                }),
            ),
            (
                IdentityInvariantKind::InvalidUserState,
                Box::new(|rows| {
                    rows.principals[0].kind = "user".to_owned();
                    rows.principals[0].role_key = Some("member".to_owned());
                    rows.principals[0].gateway_id = "G00000000000000000002".to_owned();
                }),
            ),
            (
                IdentityInvariantKind::MissingActor,
                Box::new(|rows| {
                    rows.actor_references[0].actor_kind = None;
                    rows.actor_references[0].actor_id = None;
                }),
            ),
            (
                IdentityInvariantKind::InvalidActorPair,
                Box::new(|rows| rows.actor_references[0].actor_kind = Some("owner".to_owned())),
            ),
            (
                IdentityInvariantKind::DanglingPrincipalActor,
                Box::new(|rows| {
                    rows.actor_references[0].actor_id = Some("P00000000000000000002".to_owned())
                }),
            ),
        ];

        for (expected, mutate) in cases {
            let mut rows = valid_rows();
            mutate(&mut rows);
            let error = validate_identity_invariants(&rows, 1)
                .expect_err("corrupt identity state must fail");
            assert!(
                error.violations().contains(&expected),
                "expected {expected:?}, got {:?}",
                error.violations()
            );
            let diagnostic = error.to_string();
            assert!(diagnostic.len() <= 512);
            assert!(!diagnostic.contains(GATEWAY_ID));
            assert!(!diagnostic.contains(PRINCIPAL_ID));
            assert!(!diagnostic.contains("Superuser"));
        }
    }
}
