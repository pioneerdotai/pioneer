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
    use pioneer_protocol::SkillId;
    use sea_orm::{ConnectionTrait, Database};

    const NOW: i64 = 1_900_400_000;
    const WORKSPACE: &str = "ws_overlay";

    async fn active_store() -> CrudStore {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("in-memory SQLite must open");
        Migrator::up(&database, None)
            .await
            .expect("migrations must apply");
        database
            .execute_unprepared(
                "INSERT INTO workspace (id, name, is_active, is_current) VALUES \
                 ('ws_overlay', 'Overlay', 1, 1); \
                 INSERT INTO thread (
                     id, workspace_id, preview, mode, model, model_provider, status, origin_kind
                 ) VALUES (
                     'thread_overlay', 'ws_overlay', '', 'agent', 'model', 'provider',
                     'active', 'user'
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
                    'ws_overlay', 'thread_overlay', 'turn_anchor', 'event_anchor',
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
        let entries = load_active_agent_skill_overlay(&store, WORKSPACE)
            .await
            .expect("overlay must load");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].version_id, "111111111111111111111");
        assert_eq!(entries[0].body, "Exact immutable body");
        assert_eq!(
            entries[0].runtime_description,
            "Use for the stable procedure. Do not use when: The procedure does not apply."
        );

        store
            .deactivate_self_improvement_workspace(WORKSPACE, NOW + 4)
            .await
            .expect("workspace must deactivate");
        assert!(
            load_active_agent_skill_overlay(&store, WORKSPACE)
                .await
                .expect("disabled overlay lookup must succeed")
                .is_empty()
        );
    }
}
