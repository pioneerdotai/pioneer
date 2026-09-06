use anyhow::{Context, Result};
use pioneer_crud::{AgentSkillVersionSnapshotRecord, CrudStore};
use pioneer_skills::{
    AgentSkillRuntimeEntry, agent_skill_runtime_description, ensure_agent_skill_overlay_capacity,
};

pub(crate) async fn load_active_agent_skill_overlay(
    store: &CrudStore,
    workspace_id: &str,
) -> Result<Vec<AgentSkillRuntimeEntry>> {
    let state = store
        .get_self_improvement_workspace_state(workspace_id)
        .await
        .with_context(|| {
            format!("failed to load self-improvement state for workspace `{workspace_id}`")
        })?;
    if !state.is_some_and(|state| state.effective_enabled_at_unix.is_some()) {
        return Ok(Vec::new());
    }

    let active = store
        .list_active_agent_skill_versions(workspace_id)
        .await
        .with_context(|| {
            format!("failed to load active Agent skills for workspace `{workspace_id}`")
        })?;
    let entries = active
        .into_iter()
        .map(agent_skill_runtime_entry)
        .collect::<Vec<_>>();
    ensure_agent_skill_overlay_capacity(entries.as_slice())
        .context("active Agent skill overlay violates its production prompt capacity")?;
    Ok(entries)
}

pub(crate) async fn load_scoped_agent_skill_overlay(
    store: &CrudStore,
    principal_id: &pioneer_protocol::PrincipalId,
    workspace_id: &str,
) -> Result<Vec<AgentSkillRuntimeEntry>> {
    let database = store.database_connection();
    if pioneer_crud::find_workspace_membership(&database, principal_id, workspace_id)
        .await
        .with_context(|| {
            format!(
                "failed to verify Member workspace membership for Agent skill overlay in \
                 `{workspace_id}`"
            )
        })?
        .is_none()
    {
        return Ok(Vec::new());
    }
    let active = load_active_agent_skill_overlay(store, workspace_id).await?;
    let policies = store
        .list_workspace_skill_policies(workspace_id)
        .await
        .with_context(|| {
            format!("failed to load Agent skill policies for workspace `{workspace_id}`")
        })?
        .into_iter()
        .map(|policy| (policy.skill_id, policy.enabled))
        .collect::<std::collections::HashMap<_, _>>();
    let mut eligible = Vec::with_capacity(active.len());
    for entry in active {
        if policies.get(&entry.skill_id) != Some(&Some(true)) {
            continue;
        }
        let eligibility = pioneer_crud::derive_member_learned_version_eligibility(
            &database,
            workspace_id,
            entry.version_id.as_str(),
        )
        .await
        .with_context(|| {
            format!(
                "failed to derive Member eligibility for Agent skill version `{}`",
                entry.version_id
            )
        })?;
        match eligibility {
            pioneer_crud::MemberLearnedVersionEligibility::Eligible => eligible.push(entry),
            pioneer_crud::MemberLearnedVersionEligibility::Ineligible(
                pioneer_crud::LearnedVersionIneligibleReason::SourceThreadIneligible,
            ) => {
                crate::authorization::record_ineligible_self_improvement_source_rejection();
            }
            pioneer_crud::MemberLearnedVersionEligibility::Ineligible(_) => {}
        }
    }
    Ok(eligible)
}

pub(crate) fn agent_skill_runtime_entry(
    snapshot: AgentSkillVersionSnapshotRecord,
) -> AgentSkillRuntimeEntry {
    AgentSkillRuntimeEntry {
        skill_id: snapshot.skill_id,
        slug: snapshot.slug,
        version_id: snapshot.version.id,
        version_number: snapshot.version.version_number,
        display_name: snapshot.version.display_name,
        runtime_description: agent_skill_runtime_description(
            snapshot.version.when_to_use.as_str(),
            snapshot.version.when_not_to_use.as_str(),
        ),
        body: snapshot.version.instruction_body,
        fingerprint: snapshot.version.fingerprint,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{
        AcceptedAgentSkillCreate, FinalizeSelfImprovementRunInput,
        FinalizeSelfImprovementRunResult, NewSelfImprovementRun, SelfImprovementFinalOutcome,
        SelfImprovementFinalizationAuthority,
    };
    use pioneer_protocol::{PrincipalId, SkillId};
    use sea_orm::{ConnectionTrait, Database};

    const NOW: i64 = 1_900_400_000;
    const WORKSPACE: &str = "W0000000000000000000O";
    const MEMBER_ID: &str = "P00000000000000000002";

    fn member_id() -> PrincipalId {
        PrincipalId::new(MEMBER_ID).expect("Member fixture ID must be valid")
    }

    async fn active_store() -> CrudStore {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("in-memory SQLite must open");
        Migrator::up(&database, None)
            .await
            .expect("migrations must apply");
        database
            .execute_unprepared(
                "INSERT INTO gateway_identity (
                    id, singleton_key, identity_bootstrap_version, auth_schema_version
                 ) VALUES (
                    'G00000000000000000001', 1, 1, 0
                 ); \
                 INSERT INTO gateway_principal (
                    id, gateway_id, kind, role_key, status, display_name, nickname, nickname_key
                 ) VALUES (
                    'P00000000000000000002', 'G00000000000000000001', 'user', 'member',
                    'active', 'Member', 'member', 'member'
                 ); \
                 INSERT INTO workspace (id, name, is_active, is_current) VALUES \
                 ('W0000000000000000000O', 'Overlay', 1, 1); \
                 INSERT INTO workspace_membership (
                    principal_id, workspace_id, granted_by_actor_kind, created_at, updated_at
                 ) VALUES (
                    'P00000000000000000002', 'W0000000000000000000O', 'system',
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                 ); \
                 INSERT INTO thread (
                     id, workspace_id, preview, mode, model, model_provider, status, origin_kind,
                     access_class
                 ) VALUES (
                     'thread_overlay', 'W0000000000000000000O', '', 'agent', 'model', 'provider',
                     'active', 'user', 'workspace'
                 ); \
                 INSERT INTO turn (id, thread_id, status, turn_kind, origin) VALUES \
                     ('turn_context', 'thread_overlay', 'completed', 'conversation', 'user'), \
                     ('turn_anchor', 'thread_overlay', 'completed', 'conversation', 'user');",
            )
            .await
            .expect("history fixtures must insert");
        let store = CrudStore::new(database.clone());
        store
            .activate_self_improvement_workspace(WORKSPACE, NOW)
            .await
            .expect("workspace must activate");
        database
            .execute_unprepared(
                "INSERT INTO self_improvement_source_turn (
                    workspace_id, thread_id, turn_id, terminal_event_id, terminal_at, created_at
                 ) VALUES (
                    'W0000000000000000000O', 'thread_overlay', 'turn_anchor', 'event_anchor',
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                 ), (
                    'W0000000000000000000O', 'thread_overlay', 'turn_context', 'event_context',
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                 )",
            )
            .await
            .expect("source fixture must insert");
        let run = store
            .create_or_get_self_improvement_run(
                NewSelfImprovementRun {
                    workspace_id: WORKSPACE.to_owned(),
                    activation_epoch: 1,
                    scheduled_date_utc: "2030-03-04".to_owned(),
                    source_lower_exclusive: 0,
                    source_upper_inclusive: 1,
                    learner_provider: "provider".to_owned(),
                    learner_model: "model".to_owned(),
                    reviewer_provider: "provider".to_owned(),
                    reviewer_model: "model".to_owned(),
                    pipeline_contract_version: "self-improvement-v1".to_owned(),
                },
                NOW + 1,
            )
            .await
            .expect("run must create");
        let claimed = store
            .claim_available_self_improvement_run(
                WORKSPACE,
                run.id.as_str(),
                1,
                "gateway-overlay",
                NOW + 2,
                NOW + 100,
            )
            .await
            .expect("claim must execute")
            .expect("claim must win");
        let result = store
            .finalize_self_improvement_run(
                FinalizeSelfImprovementRunInput {
                    fence: claimed.fence().expect("running run must have fence"),
                    authority: SelfImprovementFinalizationAuthority {
                        effective_enabled: true,
                        learner_provider: "provider".to_owned(),
                        learner_model: "model".to_owned(),
                        reviewer_provider: "provider".to_owned(),
                        reviewer_model: "model".to_owned(),
                        pipeline_contract_version: "self-improvement-v1".to_owned(),
                    },
                    outcome: SelfImprovementFinalOutcome::AcceptedCreate(
                        AcceptedAgentSkillCreate {
                            skill_id: SkillId::new("AAAAAAAAAAAAAAAAAAAAA")
                                .expect("valid skill ID"),
                            version_id: "111111111111111111111".to_owned(),
                            slug: "stable-procedure".to_owned(),
                            candidate_key: "candidate".to_owned(),
                            display_name: "Stable procedure".to_owned(),
                            skill_markdown: "---\nname: Stable procedure\n---\nBody".to_owned(),
                            instruction_body: "Exact immutable body".to_owned(),
                            when_to_use: "Use for the stable procedure".to_owned(),
                            when_not_to_use: "The procedure does not apply".to_owned(),
                            fingerprint: "a".repeat(64),
                            source_turn_ids: vec![
                                "turn_context".to_owned(),
                                "turn_anchor".to_owned(),
                            ],
                        },
                    ),
                },
                NOW + 3,
            )
            .await
            .expect("finalization must execute");
        assert!(matches!(
            result,
            FinalizeSelfImprovementRunResult::Applied { .. }
        ));
        store
    }

    #[tokio::test]
    async fn materializes_exact_active_version_only_while_workspace_is_enabled() {
        let store = active_store().await;
        let restarted_store = CrudStore::new(store.database_connection());
        drop(store);
        let entries = load_active_agent_skill_overlay(&restarted_store, WORKSPACE)
            .await
            .expect("overlay must load after service restart");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].version_id, "111111111111111111111");
        assert_eq!(entries[0].body, "Exact immutable body");
        assert_eq!(
            entries[0].runtime_description,
            "Use for the stable procedure. Do not use when: The procedure does not apply."
        );

        restarted_store
            .deactivate_self_improvement_workspace(WORKSPACE, NOW + 4)
            .await
            .expect("workspace must deactivate");
        assert!(
            load_active_agent_skill_overlay(&restarted_store, WORKSPACE)
                .await
                .expect("disabled overlay lookup must succeed")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn member_overlay_accepts_private_sources_with_workspace_membership_and_policy() {
        use pioneer_crud::WorkspaceSkillPolicyRecord;
        use sea_orm::ConnectionTrait;

        let store = active_store().await;
        let principal_id = member_id();
        store
            .database_connection()
            .execute_unprepared(
                "DELETE FROM workspace_membership \
                 WHERE principal_id = 'P00000000000000000002' \
                   AND workspace_id = 'W0000000000000000000O'",
            )
            .await
            .expect("Member membership must be removable");
        assert!(
            load_scoped_agent_skill_overlay(&store, &principal_id, WORKSPACE)
                .await
                .expect("missing workspace membership must fail closed without error")
                .is_empty()
        );
        store
            .database_connection()
            .execute_unprepared(
                "INSERT INTO workspace_membership (
                    principal_id, workspace_id, granted_by_actor_kind, created_at, updated_at
                 ) VALUES (
                    'P00000000000000000002', 'W0000000000000000000O', 'system',
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                 )",
            )
            .await
            .expect("Member membership must be restorable");
        store
            .database_connection()
            .execute_unprepared(
                "UPDATE thread SET access_class = 'workspace' WHERE id = 'thread_overlay'",
            )
            .await
            .expect("source thread must become workspace-visible");
        assert!(
            load_scoped_agent_skill_overlay(&store, &principal_id, WORKSPACE)
                .await
                .expect("missing policy must fail closed without error")
                .is_empty()
        );
        store
            .upsert_workspace_skill_policy(
                &WorkspaceSkillPolicyRecord {
                    id: "member_overlay_policy".to_owned(),
                    workspace_id: WORKSPACE.to_owned(),
                    skill_id: SkillId::new("AAAAAAAAAAAAAAAAAAAAA").expect("skill id"),
                    enabled: Some(true),
                    allow_implicit_invocation: Some(true),
                },
                NOW + 5,
            )
            .await
            .expect("explicit workspace policy must persist");
        let eligible = load_scoped_agent_skill_overlay(&store, &principal_id, WORKSPACE)
            .await
            .expect("workspace-only learned version must resolve");
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0].version_id, "111111111111111111111");

        store
            .database_connection()
            .execute_unprepared(
                "UPDATE thread SET access_class = 'private' WHERE id = 'thread_overlay'",
            )
            .await
            .expect("source thread must become private");
        assert_eq!(
            load_scoped_agent_skill_overlay(&store, &principal_id, WORKSPACE)
                .await
                .expect("private source skills must remain available to workspace members")
                .len(),
            1
        );
        store
            .database_connection()
            .execute_unprepared(
                "UPDATE thread SET access_class = 'workspace' WHERE id = 'thread_overlay'; \
                 DELETE FROM workspace_membership \
                 WHERE principal_id = 'P00000000000000000002' \
                   AND workspace_id = 'W0000000000000000000O'",
            )
            .await
            .expect("Member membership revoke must persist");
        assert!(
            load_scoped_agent_skill_overlay(&store, &principal_id, WORKSPACE)
                .await
                .expect("revoked membership must invalidate overlay without error")
                .is_empty()
        );
    }
}
