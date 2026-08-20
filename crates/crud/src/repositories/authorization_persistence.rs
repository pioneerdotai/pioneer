use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use pioneer_entity::{
    agent_skill, agent_skill_version, gateway_principal, self_improvement_source_turn, thread,
    thread_lineage, thread_membership, turn, workspace, workspace_membership,
};
use pioneer_protocol::{
    GatewayId, PrincipalKind, PrincipalStatus, RoleKey, ThreadOriginKind, ThreadSidebarVisibility,
    TurnKind, TurnOrigin, TurnStatus,
};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};

use super::identity::{actor_ref_from_db, principal_kind_from_db, principal_status_from_db};
use super::membership::{PersistedThreadAccessClass, persisted_thread_access_class_from_db};
use crate::convention::{
    thread_origin_kind_from_db, thread_sidebar_visibility_from_db, turn_kind_from_db,
    turn_origin_from_db, turn_status_from_db,
};

const MAX_AUTHORIZATION_DIAGNOSTICS: usize = 8;
const MAX_SOURCE_TURN_IDS_JSON_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthorizationPersistenceInvariantKind {
    MembershipPrincipalMissing,
    MembershipPrincipalCrossGateway,
    MembershipPrincipalInvalid,
    MembershipWorkspaceMissing,
    MembershipActorInvalid,
    ThreadWorkspaceMissing,
    ThreadAccessClassInvalid,
    ThreadAccessClassInconsistent,
    ThreadMembershipThreadMissing,
    ThreadMembershipInvalidParentAccessClass,
    ThreadMembershipParentMissing,
    PrivateCreatorWorkspaceMembershipMissing,
    PrivateCreatorThreadMembershipMissing,
}

impl AuthorizationPersistenceInvariantKind {
    pub const fn code(self) -> &'static str {
        match self {
            Self::MembershipPrincipalMissing => "membership_principal_missing",
            Self::MembershipPrincipalCrossGateway => "membership_principal_cross_gateway",
            Self::MembershipPrincipalInvalid => "membership_principal_invalid",
            Self::MembershipWorkspaceMissing => "membership_workspace_missing",
            Self::MembershipActorInvalid => "membership_actor_invalid",
            Self::ThreadWorkspaceMissing => "thread_workspace_missing",
            Self::ThreadAccessClassInvalid => "thread_access_class_invalid",
            Self::ThreadAccessClassInconsistent => "thread_access_class_inconsistent",
            Self::ThreadMembershipThreadMissing => "thread_membership_thread_missing",
            Self::ThreadMembershipInvalidParentAccessClass => {
                "thread_membership_invalid_parent_access_class"
            }
            Self::ThreadMembershipParentMissing => "thread_membership_parent_missing",
            Self::PrivateCreatorWorkspaceMembershipMissing => {
                "private_creator_workspace_membership_missing"
            }
            Self::PrivateCreatorThreadMembershipMissing => {
                "private_creator_thread_membership_missing"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationPersistenceInvariantReport {
    pub violations: Vec<AuthorizationPersistenceInvariantKind>,
    pub omitted_violations: usize,
    pub ineligible_active_learned_versions: usize,
}

impl AuthorizationPersistenceInvariantReport {
    pub fn is_valid(&self) -> bool {
        self.violations.is_empty()
    }

    pub fn safe_diagnostic(&self) -> String {
        let mut output = self
            .violations
            .iter()
            .map(|violation| violation.code())
            .collect::<Vec<_>>()
            .join(",");
        if self.omitted_violations > 0 {
            if !output.is_empty() {
                output.push(',');
            }
            output.push_str(format!("omitted={}", self.omitted_violations).as_str());
        }
        output
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LearnedVersionIneligibleReason {
    VersionMissing,
    SkillMissing,
    WorkspaceMismatch,
    EmptyProvenance,
    MalformedProvenance,
    MissingSourceLedgerRow,
    MixedWorkspace,
    MissingSourceThread,
    SourceThreadScopeMismatch,
    SourceNotWorkspaceVisible,
    MissingSourceTurn,
    SourceTurnScopeMismatch,
    SourceTurnIneligible,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberLearnedVersionEligibility {
    Eligible,
    Ineligible(LearnedVersionIneligibleReason),
}

pub async fn derive_member_learned_version_eligibility<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    version_id: &str,
) -> Result<MemberLearnedVersionEligibility> {
    let Some(version) = agent_skill_version::Entity::find_by_id(version_id.to_owned())
        .one(db)
        .await
        .context("failed to load learned version eligibility target")?
    else {
        return Ok(MemberLearnedVersionEligibility::Ineligible(
            LearnedVersionIneligibleReason::VersionMissing,
        ));
    };
    let Some(skill) = agent_skill::Entity::find_by_id(version.skill_id)
        .one(db)
        .await
        .context("failed to load learned version skill owner")?
    else {
        return Ok(MemberLearnedVersionEligibility::Ineligible(
            LearnedVersionIneligibleReason::SkillMissing,
        ));
    };
    if skill.workspace_id != workspace_id {
        return Ok(MemberLearnedVersionEligibility::Ineligible(
            LearnedVersionIneligibleReason::WorkspaceMismatch,
        ));
    }
    if version.source_turn_ids_json.len() > MAX_SOURCE_TURN_IDS_JSON_BYTES {
        return Ok(MemberLearnedVersionEligibility::Ineligible(
            LearnedVersionIneligibleReason::MalformedProvenance,
        ));
    }
    let Ok(source_turn_ids) =
        serde_json::from_str::<Vec<String>>(version.source_turn_ids_json.as_str())
    else {
        return Ok(MemberLearnedVersionEligibility::Ineligible(
            LearnedVersionIneligibleReason::MalformedProvenance,
        ));
    };
    if source_turn_ids.is_empty() {
        return Ok(MemberLearnedVersionEligibility::Ineligible(
            LearnedVersionIneligibleReason::EmptyProvenance,
        ));
    }
    let source_turn_id_set = source_turn_ids.iter().collect::<HashSet<_>>();
    if source_turn_id_set.len() != source_turn_ids.len()
        || source_turn_ids
            .iter()
            .any(|id| id.is_empty() || id != id.trim())
    {
        return Ok(MemberLearnedVersionEligibility::Ineligible(
            LearnedVersionIneligibleReason::MalformedProvenance,
        ));
    }

    let source_rows = self_improvement_source_turn::Entity::find()
        .filter(self_improvement_source_turn::Column::TurnId.is_in(source_turn_ids.clone()))
        .order_by_asc(self_improvement_source_turn::Column::TurnId)
        .all(db)
        .await
        .context("failed to load immutable learned-version source ledger")?;
    if source_rows.len() != source_turn_ids.len() {
        return Ok(MemberLearnedVersionEligibility::Ineligible(
            LearnedVersionIneligibleReason::MissingSourceLedgerRow,
        ));
    }
    if source_rows
        .iter()
        .any(|source| source.workspace_id != workspace_id)
    {
        return Ok(MemberLearnedVersionEligibility::Ineligible(
            LearnedVersionIneligibleReason::MixedWorkspace,
        ));
    }

    let thread_ids = source_rows
        .iter()
        .map(|source| source.thread_id.clone())
        .collect::<HashSet<_>>();
    let threads = thread::Entity::find()
        .filter(thread::Column::Id.is_in(thread_ids.iter().cloned()))
        .all(db)
        .await
        .context("failed to load learned-version source threads")?
        .into_iter()
        .map(|model| (model.id.clone(), model))
        .collect::<HashMap<_, _>>();
    let turns = turn::Entity::find()
        .filter(turn::Column::Id.is_in(source_turn_ids.clone()))
        .all(db)
        .await
        .context("failed to load learned-version source turns")?
        .into_iter()
        .map(|model| (model.id.clone(), model))
        .collect::<HashMap<_, _>>();

    for source in source_rows {
        let Some(source_thread) = threads.get(source.thread_id.as_str()) else {
            return Ok(MemberLearnedVersionEligibility::Ineligible(
                LearnedVersionIneligibleReason::MissingSourceThread,
            ));
        };
        if source_thread.workspace_id != workspace_id {
            return Ok(MemberLearnedVersionEligibility::Ineligible(
                LearnedVersionIneligibleReason::SourceThreadScopeMismatch,
            ));
        }
        if persisted_thread_access_class_from_db(source_thread.access_class.as_str()).ok()
            != Some(PersistedThreadAccessClass::Workspace)
            || thread_sidebar_visibility_from_db(source_thread.sidebar_visibility.as_str())
                != Some(ThreadSidebarVisibility::Visible)
            || !thread_origin_kind_from_db(source_thread.origin_kind.as_str()).is_some_and(
                |origin| {
                    matches!(
                        origin,
                        ThreadOriginKind::Collaborative
                            | ThreadOriginKind::DirectMessage
                            | ThreadOriginKind::User
                    )
                },
            )
        {
            return Ok(MemberLearnedVersionEligibility::Ineligible(
                LearnedVersionIneligibleReason::SourceNotWorkspaceVisible,
            ));
        }
        let Some(source_turn) = turns.get(source.turn_id.as_str()) else {
            return Ok(MemberLearnedVersionEligibility::Ineligible(
                LearnedVersionIneligibleReason::MissingSourceTurn,
            ));
        };
        if source_turn.thread_id != source.thread_id {
            return Ok(MemberLearnedVersionEligibility::Ineligible(
                LearnedVersionIneligibleReason::SourceTurnScopeMismatch,
            ));
        }
        if turn_status_from_db(source_turn.status.as_str()) != Some(TurnStatus::Completed)
            || turn_kind_from_db(source_turn.turn_kind.as_str()) != Some(TurnKind::Conversation)
            || turn_origin_from_db(source_turn.origin.as_str()) != Some(TurnOrigin::User)
        {
            return Ok(MemberLearnedVersionEligibility::Ineligible(
                LearnedVersionIneligibleReason::SourceTurnIneligible,
            ));
        }
    }

    Ok(MemberLearnedVersionEligibility::Eligible)
}

pub async fn scan_authorization_persistence_invariants<C: ConnectionTrait>(
    db: &C,
    gateway_id: &GatewayId,
) -> Result<AuthorizationPersistenceInvariantReport> {
    let principals = gateway_principal::Entity::find()
        .all(db)
        .await
        .context("failed to scan authorization principals")?;
    let principal_by_id = principals
        .iter()
        .map(|principal| (principal.id.as_str(), principal))
        .collect::<HashMap<_, _>>();
    let workspaces = workspace::Entity::find()
        .select_only()
        .column(workspace::Column::Id)
        .into_tuple::<String>()
        .all(db)
        .await
        .context("failed to scan authorization workspaces")?
        .into_iter()
        .collect::<HashSet<_>>();
    let workspace_memberships = workspace_membership::Entity::find()
        .all(db)
        .await
        .context("failed to scan workspace memberships")?;
    let workspace_membership_keys = workspace_memberships
        .iter()
        .map(|membership| {
            (
                membership.principal_id.as_str(),
                membership.workspace_id.as_str(),
            )
        })
        .collect::<HashSet<_>>();
    let threads = thread::Entity::find()
        .all(db)
        .await
        .context("failed to scan authorization threads")?;
    let thread_by_id = threads
        .iter()
        .map(|model| (model.id.as_str(), model))
        .collect::<HashMap<_, _>>();
    let lineage_children = thread_lineage::Entity::find()
        .select_only()
        .column(thread_lineage::Column::ChildThreadId)
        .into_tuple::<String>()
        .all(db)
        .await
        .context("failed to scan thread lineage children")?
        .into_iter()
        .collect::<HashSet<_>>();
    let thread_memberships = thread_membership::Entity::find()
        .all(db)
        .await
        .context("failed to scan thread memberships")?;
    let thread_membership_keys = thread_memberships
        .iter()
        .map(|membership| {
            (
                membership.thread_id.as_str(),
                membership.principal_id.as_str(),
            )
        })
        .collect::<HashSet<_>>();

    let mut violations = Vec::new();
    for membership in &workspace_memberships {
        validate_membership_principal(
            &mut violations,
            &principal_by_id,
            gateway_id,
            membership.principal_id.as_str(),
        );
        if !workspaces.contains(membership.workspace_id.as_str()) {
            push_violation(
                &mut violations,
                AuthorizationPersistenceInvariantKind::MembershipWorkspaceMissing,
            );
        }
        validate_membership_actor(
            &mut violations,
            &principal_by_id,
            gateway_id,
            membership.granted_by_actor_kind.as_str(),
            membership.granted_by_actor_id.as_deref(),
        );
    }

    for model in &threads {
        if !workspaces.contains(model.workspace_id.as_str()) {
            push_violation(
                &mut violations,
                AuthorizationPersistenceInvariantKind::ThreadWorkspaceMissing,
            );
        }
        let access_class = match persisted_thread_access_class_from_db(model.access_class.as_str())
        {
            Ok(access_class) => Some(access_class),
            Err(_) => {
                push_violation(
                    &mut violations,
                    AuthorizationPersistenceInvariantKind::ThreadAccessClassInvalid,
                );
                None
            }
        };
        let is_user_origin = matches!(
            model.origin_kind.as_str(),
            "collaborative" | "direct_message" | "user"
        );
        let has_internal_signal = matches!(model.origin_kind.as_str(), "task_run" | "system")
            || model.sidebar_visibility == "hidden"
            || lineage_children.contains(model.id.as_str());
        let consistent = match access_class {
            Some(PersistedThreadAccessClass::Private | PersistedThreadAccessClass::Workspace) => {
                is_user_origin
                    && model.sidebar_visibility == "visible"
                    && !lineage_children.contains(model.id.as_str())
            }
            Some(PersistedThreadAccessClass::Internal) => has_internal_signal,
            None => false,
        };
        if !consistent {
            push_violation(
                &mut violations,
                AuthorizationPersistenceInvariantKind::ThreadAccessClassInconsistent,
            );
        }

        if access_class == Some(PersistedThreadAccessClass::Private)
            && let Ok(Some(pioneer_protocol::PersistedActorRef::Principal(creator_id))) =
                actor_ref_from_db(
                    model.created_by_actor_kind.as_deref(),
                    model.created_by_actor_id.as_deref(),
                )
            && let Some(creator) = principal_by_id.get(creator_id.as_str())
            && principal_kind_from_db(creator.kind.as_str()).ok() == Some(PrincipalKind::User)
        {
            if !workspace_membership_keys
                .contains(&(creator_id.as_str(), model.workspace_id.as_str()))
            {
                push_violation(
                    &mut violations,
                    AuthorizationPersistenceInvariantKind::PrivateCreatorWorkspaceMembershipMissing,
                );
            }
            if !thread_membership_keys.contains(&(model.id.as_str(), creator_id.as_str())) {
                push_violation(
                    &mut violations,
                    AuthorizationPersistenceInvariantKind::PrivateCreatorThreadMembershipMissing,
                );
            }
        }
    }

    for membership in &thread_memberships {
        validate_membership_principal(
            &mut violations,
            &principal_by_id,
            gateway_id,
            membership.principal_id.as_str(),
        );
        validate_membership_actor(
            &mut violations,
            &principal_by_id,
            gateway_id,
            membership.added_by_actor_kind.as_str(),
            membership.added_by_actor_id.as_deref(),
        );
        let Some(parent_thread) = thread_by_id.get(membership.thread_id.as_str()) else {
            push_violation(
                &mut violations,
                AuthorizationPersistenceInvariantKind::ThreadMembershipThreadMissing,
            );
            continue;
        };
        if !matches!(
            persisted_thread_access_class_from_db(parent_thread.access_class.as_str()).ok(),
            Some(PersistedThreadAccessClass::Private | PersistedThreadAccessClass::Workspace)
        ) {
            push_violation(
                &mut violations,
                AuthorizationPersistenceInvariantKind::ThreadMembershipInvalidParentAccessClass,
            );
        }
        if !workspace_membership_keys.contains(&(
            membership.principal_id.as_str(),
            parent_thread.workspace_id.as_str(),
        )) {
            push_violation(
                &mut violations,
                AuthorizationPersistenceInvariantKind::ThreadMembershipParentMissing,
            );
        }
    }

    let active_versions = agent_skill::Entity::find()
        .filter(agent_skill::Column::ActiveVersionId.is_not_null())
        .all(db)
        .await
        .context("failed to scan active learned versions")?;
    let mut ineligible_active_learned_versions = 0usize;
    for skill in active_versions {
        let Some(version_id) = skill.active_version_id.as_deref() else {
            continue;
        };
        if derive_member_learned_version_eligibility(db, skill.workspace_id.as_str(), version_id)
            .await?
            != MemberLearnedVersionEligibility::Eligible
        {
            ineligible_active_learned_versions += 1;
        }
    }

    let omitted_violations = violations
        .len()
        .saturating_sub(MAX_AUTHORIZATION_DIAGNOSTICS);
    violations.truncate(MAX_AUTHORIZATION_DIAGNOSTICS);
    Ok(AuthorizationPersistenceInvariantReport {
        violations,
        omitted_violations,
        ineligible_active_learned_versions,
    })
}

fn validate_membership_principal(
    violations: &mut Vec<AuthorizationPersistenceInvariantKind>,
    principals: &HashMap<&str, &gateway_principal::Model>,
    gateway_id: &GatewayId,
    principal_id: &str,
) {
    let Some(principal) = principals.get(principal_id) else {
        push_violation(
            violations,
            AuthorizationPersistenceInvariantKind::MembershipPrincipalMissing,
        );
        return;
    };
    if principal.gateway_id != gateway_id.as_str() {
        push_violation(
            violations,
            AuthorizationPersistenceInvariantKind::MembershipPrincipalCrossGateway,
        );
    }
    let role_is_valid = principal
        .role_key
        .as_deref()
        .and_then(|value| RoleKey::new(value).ok())
        .is_some();
    if principal_kind_from_db(principal.kind.as_str()).ok() != Some(PrincipalKind::User)
        || principal_status_from_db(principal.status.as_str()).ok()
            == Some(PrincipalStatus::Removed)
        || !role_is_valid
    {
        push_violation(
            violations,
            AuthorizationPersistenceInvariantKind::MembershipPrincipalInvalid,
        );
    }
}

fn validate_membership_actor(
    violations: &mut Vec<AuthorizationPersistenceInvariantKind>,
    principals: &HashMap<&str, &gateway_principal::Model>,
    gateway_id: &GatewayId,
    actor_kind: &str,
    actor_id: Option<&str>,
) {
    let valid = match actor_ref_from_db(Some(actor_kind), actor_id) {
        Ok(Some(pioneer_protocol::PersistedActorRef::System)) => true,
        Ok(Some(pioneer_protocol::PersistedActorRef::Principal(actor_id))) => principals
            .get(actor_id.as_str())
            .is_some_and(|principal| principal.gateway_id == gateway_id.as_str()),
        _ => false,
    };
    if !valid {
        push_violation(
            violations,
            AuthorizationPersistenceInvariantKind::MembershipActorInvalid,
        );
    }
}

fn push_violation(
    violations: &mut Vec<AuthorizationPersistenceInvariantKind>,
    violation: AuthorizationPersistenceInvariantKind,
) {
    if !violations.contains(&violation) {
        violations.push(violation);
    }
}

#[cfg(test)]
mod tests {
    use migration::{Migrator, MigratorTrait};
    use pioneer_entity::{
        agent_skill, agent_skill_version, gateway_identity, gateway_principal,
        self_improvement_source_turn, thread, thread_membership, turn, workspace,
        workspace_membership,
    };
    use pioneer_protocol::GatewayId;
    use sea_orm::{
        ActiveModelTrait, ActiveValue::NotSet, ColumnTrait, ConnectionTrait, Database,
        DatabaseConnection, EntityTrait, QueryFilter, Set,
    };

    use super::*;

    const GATEWAY_ID: &str = "G00000000000000000001";
    const SUPERUSER_ID: &str = "P00000000000000000001";
    const MEMBER_ID: &str = "P00000000000000000002";
    const WORKSPACE_ID: &str = "W00000000000000000001";
    const THREAD_ID: &str = "T00000000000000000001";
    const TURN_ID: &str = "U00000000000000000001";
    const SKILL_ID: &str = "K00000000000000000001";
    const VERSION_ID: &str = "V00000000000000000001";

    async fn base_database(include_member: bool) -> DatabaseConnection {
        let database = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&database, None).await.unwrap();
        let now = chrono::Utc::now().fixed_offset();
        gateway_identity::ActiveModel {
            id: Set(GATEWAY_ID.to_owned()),
            singleton_key: Set(1),
            identity_bootstrap_version: Set(1),
            created_at: Set(now),
            updated_at: Set(now),
            auth_schema_version: Set(0),
            auth_ready_at: Set(None),
        }
        .insert(&database)
        .await
        .unwrap();
        gateway_principal::ActiveModel {
            id: Set(SUPERUSER_ID.to_owned()),
            gateway_id: Set(GATEWAY_ID.to_owned()),
            kind: Set("superuser".to_owned()),
            role_key: Set(None),
            status: Set("active".to_owned()),
            display_name: Set("Superuser".to_owned()),
            nickname: Set("superuser".to_owned()),
            nickname_key: Set("superuser".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            removed_at: Set(None),
            authorization_guard: Set(1),
        }
        .insert(&database)
        .await
        .unwrap();
        if include_member {
            gateway_principal::ActiveModel {
                id: Set(MEMBER_ID.to_owned()),
                gateway_id: Set(GATEWAY_ID.to_owned()),
                kind: Set("user".to_owned()),
                role_key: Set(Some("member".to_owned())),
                status: Set("active".to_owned()),
                display_name: Set("Member".to_owned()),
                nickname: Set("member".to_owned()),
                nickname_key: Set("member".to_owned()),
                created_at: Set(now),
                updated_at: Set(now),
                removed_at: Set(None),
                authorization_guard: Set(1),
            }
            .insert(&database)
            .await
            .unwrap();
        }
        workspace::ActiveModel {
            id: Set(WORKSPACE_ID.to_owned()),
            name: Set("Workspace".to_owned()),
            is_active: Set(true),
            is_current: Set(true),
            created_at: Set(now),
            updated_at: Set(now),
        }
        .insert(&database)
        .await
        .unwrap();
        thread::ActiveModel {
            id: Set(THREAD_ID.to_owned()),
            workspace_id: Set(WORKSPACE_ID.to_owned()),
            name: Set(Some("Private legacy thread".to_owned())),
            preview: Set(String::new()),
            preview_author_json: Set(None),
            mode: Set("chat".to_owned()),
            model: Set("test".to_owned()),
            model_provider: Set("test".to_owned()),
            status: Set("active".to_owned()),
            origin_kind: Set("user".to_owned()),
            sidebar_visibility: Set("visible".to_owned()),
            access_class: Set("private".to_owned()),
            agent_nickname: Set(None),
            agent_role: Set(None),
            summary: Set(None),
            summary_turn_count: Set(Some(0)),
            created_at: Set(now),
            updated_at: Set(now),
            created_by_actor_id: Set(Some(SUPERUSER_ID.to_owned())),
            created_by_actor_kind: Set(Some("principal".to_owned())),
        }
        .insert(&database)
        .await
        .unwrap();
        database
    }

    #[tokio::test]
    async fn superuser_only_legacy_state_is_ready() {
        let database = base_database(false).await;
        let report = scan_authorization_persistence_invariants(
            &database,
            &GatewayId::new(GATEWAY_ID).unwrap(),
        )
        .await
        .unwrap();
        assert!(report.is_valid(), "{}", report.safe_diagnostic());
        assert_eq!(report.ineligible_active_learned_versions, 0);
    }

    #[tokio::test]
    async fn thread_membership_without_parent_workspace_grant_fails_closed() {
        let database = base_database(true).await;
        let now = chrono::Utc::now().fixed_offset();
        thread_membership::ActiveModel {
            thread_id: Set(THREAD_ID.to_owned()),
            principal_id: Set(MEMBER_ID.to_owned()),
            added_by_actor_kind: Set("system".to_owned()),
            added_by_actor_id: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        }
        .insert(&database)
        .await
        .unwrap();
        let report = scan_authorization_persistence_invariants(
            &database,
            &GatewayId::new(GATEWAY_ID).unwrap(),
        )
        .await
        .unwrap();
        assert!(
            report
                .violations
                .contains(&AuthorizationPersistenceInvariantKind::ThreadMembershipParentMissing)
        );
        let diagnostic = report.safe_diagnostic();
        assert!(!diagnostic.contains(THREAD_ID));
        assert!(!diagnostic.contains(MEMBER_ID));
        assert!(diagnostic.len() <= 512);
    }

    #[tokio::test]
    async fn workspace_visibility_preserves_explicit_thread_memberships_as_valid_state() {
        let database = base_database(true).await;
        let now = chrono::Utc::now().fixed_offset();
        workspace_membership::ActiveModel {
            principal_id: Set(MEMBER_ID.to_owned()),
            workspace_id: Set(WORKSPACE_ID.to_owned()),
            granted_by_actor_kind: Set("system".to_owned()),
            granted_by_actor_id: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        }
        .insert(&database)
        .await
        .unwrap();
        thread_membership::ActiveModel {
            thread_id: Set(THREAD_ID.to_owned()),
            principal_id: Set(MEMBER_ID.to_owned()),
            added_by_actor_kind: Set("system".to_owned()),
            added_by_actor_id: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        }
        .insert(&database)
        .await
        .unwrap();
        let thread = thread::Entity::find_by_id(THREAD_ID.to_owned())
            .one(&database)
            .await
            .unwrap()
            .unwrap();
        let mut thread: thread::ActiveModel = thread.into();
        thread.access_class = Set("workspace".to_owned());
        thread.update(&database).await.unwrap();

        let report = scan_authorization_persistence_invariants(
            &database,
            &GatewayId::new(GATEWAY_ID).unwrap(),
        )
        .await
        .unwrap();
        assert!(
            report.is_valid(),
            "private/workspace/private transitions preserve explicit participants: {}",
            report.safe_diagnostic()
        );
    }

    async fn add_learned_version_fixture(database: &DatabaseConnection) {
        let now = chrono::Utc::now().fixed_offset();
        let thread_model = thread::Entity::find_by_id(THREAD_ID.to_owned())
            .one(database)
            .await
            .unwrap()
            .unwrap();
        let mut active: thread::ActiveModel = thread_model.into();
        active.access_class = Set("workspace".to_owned());
        active.update(database).await.unwrap();
        turn::ActiveModel {
            id: Set(TURN_ID.to_owned()),
            thread_id: Set(THREAD_ID.to_owned()),
            status: Set("completed".to_owned()),
            error: Set(None),
            prompt_manifest_json: Set("{}".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            turn_kind: Set("conversation".to_owned()),
            origin: Set("user".to_owned()),
            initiated_by_actor_id: Set(Some(SUPERUSER_ID.to_owned())),
            initiated_by_actor_kind: Set(Some("principal".to_owned())),
            ..Default::default()
        }
        .insert(database)
        .await
        .unwrap();
        self_improvement_source_turn::ActiveModel {
            id: NotSet,
            workspace_id: Set(WORKSPACE_ID.to_owned()),
            thread_id: Set(THREAD_ID.to_owned()),
            turn_id: Set(TURN_ID.to_owned()),
            task_delivery_id: Set(None),
            terminal_event_id: Set("E00000000000000000001".to_owned()),
            terminal_at: Set(now),
            created_at: Set(now),
        }
        .insert(database)
        .await
        .unwrap();
        agent_skill::ActiveModel {
            id: Set(SKILL_ID.to_owned()),
            workspace_id: Set(WORKSPACE_ID.to_owned()),
            slug: Set("learned".to_owned()),
            active_version_id: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        }
        .insert(database)
        .await
        .unwrap();
        agent_skill_version::ActiveModel {
            id: Set(VERSION_ID.to_owned()),
            skill_id: Set(SKILL_ID.to_owned()),
            version_number: Set(1),
            source_run_id: Set(None),
            parent_version_id: Set(None),
            candidate_key: Set("candidate".to_owned()),
            display_name: Set("Learned".to_owned()),
            skill_markdown: Set("# Learned".to_owned()),
            instruction_body: Set("Instruction".to_owned()),
            when_to_use: Set("Use".to_owned()),
            when_not_to_use: Set("Do not use".to_owned()),
            fingerprint: Set("fingerprint".to_owned()),
            source_turn_ids_json: Set(format!("[\"{TURN_ID}\"]")),
            created_at: Set(now),
        }
        .insert(database)
        .await
        .unwrap();
        agent_skill::Entity::update_many()
            .col_expr(
                agent_skill::Column::ActiveVersionId,
                sea_orm::sea_query::Expr::value(VERSION_ID),
            )
            .filter(agent_skill::Column::Id.eq(SKILL_ID))
            .exec(database)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn learned_eligibility_requires_complete_workspace_visible_provenance() {
        let database = base_database(false).await;
        add_learned_version_fixture(&database).await;
        assert_eq!(
            derive_member_learned_version_eligibility(&database, WORKSPACE_ID, VERSION_ID)
                .await
                .unwrap(),
            MemberLearnedVersionEligibility::Eligible
        );

        thread::Entity::update_many()
            .col_expr(
                thread::Column::AccessClass,
                sea_orm::sea_query::Expr::value("private"),
            )
            .filter(thread::Column::Id.eq(THREAD_ID))
            .exec(&database)
            .await
            .unwrap();
        assert_eq!(
            derive_member_learned_version_eligibility(&database, WORKSPACE_ID, VERSION_ID)
                .await
                .unwrap(),
            MemberLearnedVersionEligibility::Ineligible(
                LearnedVersionIneligibleReason::SourceNotWorkspaceVisible
            )
        );

        thread::Entity::update_many()
            .col_expr(
                thread::Column::AccessClass,
                sea_orm::sea_query::Expr::value("internal"),
            )
            .col_expr(
                thread::Column::OriginKind,
                sea_orm::sea_query::Expr::value("system"),
            )
            .col_expr(
                thread::Column::SidebarVisibility,
                sea_orm::sea_query::Expr::value("hidden"),
            )
            .filter(thread::Column::Id.eq(THREAD_ID))
            .exec(&database)
            .await
            .unwrap();
        assert_eq!(
            derive_member_learned_version_eligibility(&database, WORKSPACE_ID, VERSION_ID)
                .await
                .unwrap(),
            MemberLearnedVersionEligibility::Ineligible(
                LearnedVersionIneligibleReason::SourceNotWorkspaceVisible
            )
        );

        thread::Entity::update_many()
            .col_expr(
                thread::Column::AccessClass,
                sea_orm::sea_query::Expr::value("workspace"),
            )
            .col_expr(
                thread::Column::OriginKind,
                sea_orm::sea_query::Expr::value("user"),
            )
            .col_expr(
                thread::Column::SidebarVisibility,
                sea_orm::sea_query::Expr::value("visible"),
            )
            .filter(thread::Column::Id.eq(THREAD_ID))
            .exec(&database)
            .await
            .unwrap();
        agent_skill_version::Entity::update_many()
            .col_expr(
                agent_skill_version::Column::SourceTurnIdsJson,
                sea_orm::sea_query::Expr::value("[\"U00000000000000000999\"]"),
            )
            .filter(agent_skill_version::Column::Id.eq(VERSION_ID))
            .exec(&database)
            .await
            .unwrap();
        assert_eq!(
            derive_member_learned_version_eligibility(&database, WORKSPACE_ID, VERSION_ID)
                .await
                .unwrap(),
            MemberLearnedVersionEligibility::Ineligible(
                LearnedVersionIneligibleReason::MissingSourceLedgerRow
            )
        );

        database
            .execute_unprepared(
                "INSERT INTO workspace (id,name,is_active,is_current) VALUES \
                    ('W00000000000000000002','Other workspace',1,0); \
                 INSERT INTO thread (
                    id,workspace_id,preview,mode,model,model_provider,status,origin_kind,
                    sidebar_visibility,access_class,created_at,updated_at
                 ) VALUES (
                    'T00000000000000000002','W00000000000000000002','','chat','test','test',
                    'active','user','visible','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP
                 ); \
                 INSERT INTO turn (
                    id,thread_id,status,prompt_manifest_json,turn_kind,origin,created_at,updated_at
                 ) VALUES (
                    'U00000000000000000002','T00000000000000000002','completed','{}',
                    'conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP
                 ); \
                 INSERT INTO self_improvement_source_turn (
                    workspace_id,thread_id,turn_id,terminal_event_id,terminal_at,created_at
                 ) VALUES (
                    'W00000000000000000002','T00000000000000000002',
                    'U00000000000000000002','E00000000000000000002',
                    CURRENT_TIMESTAMP,CURRENT_TIMESTAMP
                 );",
            )
            .await
            .unwrap();
        agent_skill_version::Entity::update_many()
            .col_expr(
                agent_skill_version::Column::SourceTurnIdsJson,
                sea_orm::sea_query::Expr::value(format!(
                    "[\"{TURN_ID}\",\"U00000000000000000002\"]"
                )),
            )
            .filter(agent_skill_version::Column::Id.eq(VERSION_ID))
            .exec(&database)
            .await
            .unwrap();
        assert_eq!(
            derive_member_learned_version_eligibility(&database, WORKSPACE_ID, VERSION_ID)
                .await
                .unwrap(),
            MemberLearnedVersionEligibility::Ineligible(
                LearnedVersionIneligibleReason::MixedWorkspace
            )
        );

        agent_skill_version::Entity::update_many()
            .col_expr(
                agent_skill_version::Column::SourceTurnIdsJson,
                sea_orm::sea_query::Expr::value("[]"),
            )
            .filter(agent_skill_version::Column::Id.eq(VERSION_ID))
            .exec(&database)
            .await
            .unwrap();
        assert_eq!(
            derive_member_learned_version_eligibility(&database, WORKSPACE_ID, VERSION_ID)
                .await
                .unwrap(),
            MemberLearnedVersionEligibility::Ineligible(
                LearnedVersionIneligibleReason::EmptyProvenance
            )
        );
    }
}
