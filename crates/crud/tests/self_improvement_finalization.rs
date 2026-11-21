use migration::MigratorTrait;
use pioneer_crud::{
    AcceptedAgentSkillCreate, AcceptedAgentSkillRollback, AcceptedAgentSkillUpdate, CrudStore,
    FinalizeSelfImprovementRunInput, FinalizeSelfImprovementRunResult, NewSelfImprovementRun,
    SelfImprovementFinalOutcome, SelfImprovementFinalizationAuthority,
    SelfImprovementFinalizationConflict, SelfImprovementNoChangeReason,
};
use pioneer_protocol::SkillId;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};

const NOW: i64 = 1_900_300_000;
const WORKSPACE: &str = "ws_finalize";
const CONTEXT_TURN: &str = "turn_context_history";
const ANCHOR_TURN: &str = "turn_new_anchor";
const SKILL_ID: &str = "AAAAAAAAAAAAAAAAAAAAA";
const VERSION_ID: &str = "111111111111111111111";
const VERSION_2_ID: &str = "222222222222222222222";
const VERSION_3_ID: &str = "333333333333333333333";

#[tokio::test]
async fn old_discovery_cannot_overwrite_processed_chronology_and_rejection_is_idempotent() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (database, store) = setup(fixture).await;
        let store = store.with_maintenance_access();
        database
            .execute_unprepared(&format!(
                "UPDATE turn SET created_at = to_timestamp({})",
                NOW - 10
            ))
            .await
            .unwrap();
        let first = claimed_input(&store).await;
        store
            .finalize_self_improvement_run(first, NOW + 3)
            .await
            .unwrap();
        let active = store
            .list_active_agent_skill_versions(WORKSPACE)
            .await
            .unwrap();
        assert_eq!(active[0].version.evidence_latest_at_unix, Some(NOW));
        assert_eq!(active[0].lifecycle_evidence_latest_at_unix, Some(NOW));
        assert_ne!(active[0].version.created_at_unix, NOW);
        assert_eq!(
            store
                .self_improvement_processed_history_date(WORKSPACE)
                .await
                .unwrap(),
            Some(NOW - 10)
        );
        assert_eq!(
            store
                .self_improvement_processed_history_date("other-workspace")
                .await
                .unwrap(),
            None
        );
        let old_id = append_anchor(&database, "late_old_turn").await;
        database
            .execute_unprepared(&format!(
                "UPDATE turn SET created_at = to_timestamp({}) WHERE id = 'late_old_turn'",
                NOW - 1_000
            ))
            .await
            .unwrap();
        let mut create = accepted_create();
        create.skill_id = SkillId::new("BBBBBBBBBBBBBBBBBBBBB").unwrap();
        create.version_id = VERSION_2_ID.into();
        create.slug = "obsolete-practice".into();
        create.candidate_key = "late-old-candidate".into();
        create.fingerprint = "b".repeat(64);
        create.source_turn_ids = vec![CONTEXT_TURN.into(), "late_old_turn".into()];
        create.evidence_time = Some(pioneer_crud::AgentSkillEvidenceTime {
            confirmed_at_unix: NOW - 1_000,
            latest_at_unix: NOW - 1_000,
        });
        let input = claimed_action_input(
            &store,
            "2030-03-04",
            1,
            old_id,
            SelfImprovementFinalOutcome::AcceptedCreate(create),
            NOW + 4,
        )
        .await;
        assert_eq!(
            store
                .finalize_self_improvement_run(input.clone(), NOW + 6)
                .await
                .unwrap(),
            FinalizeSelfImprovementRunResult::NoChange {
                reason: SelfImprovementNoChangeReason::HostValidationRejected
            }
        );
        assert_eq!(
            store
                .finalize_self_improvement_run(input, NOW + 7)
                .await
                .unwrap(),
            FinalizeSelfImprovementRunResult::AlreadyFinalized
        );
        assert_eq!(
            store
                .list_active_agent_skill_versions(WORKSPACE)
                .await
                .unwrap(),
            active
        );
        assert_eq!(
            store
                .self_improvement_processed_history_date(WORKSPACE)
                .await
                .unwrap(),
            Some(NOW - 10)
        );
        let context = store
            .recent_processed_self_improvement_sources(WORKSPACE)
            .await
            .unwrap();
        assert_eq!(context.len(), 3);
        assert!(
            store
                .recent_processed_self_improvement_sources("other-workspace")
                .await
                .unwrap()
                .is_empty()
        );
        for _ in 0..2 {
            migration::Migrator::up(&database, None).await.unwrap();
        }
        assert_eq!(
            store
                .list_active_agent_skill_versions(WORKSPACE)
                .await
                .unwrap(),
            active
        );
        Ok(())
    })
    .await
    .unwrap();
}

async fn setup(fixture: &pioneer_postgres_test::TestDatabase) -> (DatabaseConnection, CrudStore) {
    let database = fixture.connection().clone();
    database
        .execute_unprepared("DELETE FROM workspace")
        .await
        .unwrap();
    database
        .execute_unprepared(&format!(
            r#"
            INSERT INTO workspace (id, name, is_active, is_current)
            VALUES ('{WORKSPACE}', 'Finalize', TRUE, TRUE);
            INSERT INTO thread (
                id, workspace_id, preview, mode, model, model_provider, status, origin_kind,
                access_class
            ) VALUES (
                'thread_finalize', '{WORKSPACE}', '', 'agent', 'gpt-5.4', 'openai',
                'active', 'user', 'workspace'
            );
            INSERT INTO turn (id, thread_id, status, turn_kind, origin)
            VALUES
                ('{CONTEXT_TURN}', 'thread_finalize', 'completed', 'conversation', 'user'),
                ('{ANCHOR_TURN}', 'thread_finalize', 'completed', 'conversation', 'user');
            "#
        ))
        .await
        .expect("workspace history fixtures must insert");
    let store = CrudStore::new(database.clone());
    store
        .activate_self_improvement_workspace(WORKSPACE, NOW)
        .await
        .expect("workspace must activate");
    database
        .execute_unprepared(&format!(
            r#"
            INSERT INTO self_improvement_source_turn (
                workspace_id, thread_id, turn_id, terminal_event_id, terminal_at, created_at
            ) VALUES (
                '{WORKSPACE}', 'thread_finalize', '{ANCHOR_TURN}', 'event_new_anchor',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            ), (
                '{WORKSPACE}', 'thread_finalize', '{CONTEXT_TURN}', 'event_context',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            );
            "#
        ))
        .await
        .expect("new anchor must insert");
    (database, store)
}

fn run_input() -> NewSelfImprovementRun {
    NewSelfImprovementRun {
        workspace_id: WORKSPACE.to_owned(),
        activation_epoch: 1,
        scheduled_date_utc: "2030-03-03".to_owned(),
        source_lower_exclusive: 0,
        source_upper_inclusive: 1,
        learner_provider: "openai".to_owned(),
        learner_model: "gpt-5.4".to_owned(),
        learner_reasoning_effort: None,
        reviewer_provider: "openai".to_owned(),
        reviewer_model: "gpt-5.4".to_owned(),
        reviewer_reasoning_effort: None,
        pipeline_contract_version: "self-improvement-v1".to_owned(),
    }
}

fn authority() -> SelfImprovementFinalizationAuthority {
    SelfImprovementFinalizationAuthority {
        effective_enabled: true,
        learner_provider: "openai".to_owned(),
        learner_model: "gpt-5.4".to_owned(),
        learner_reasoning_effort: None,
        reviewer_provider: "openai".to_owned(),
        reviewer_model: "gpt-5.4".to_owned(),
        reviewer_reasoning_effort: None,
        pipeline_contract_version: "self-improvement-v1".to_owned(),
    }
}

fn accepted_create() -> AcceptedAgentSkillCreate {
    AcceptedAgentSkillCreate {
        evidence_time: Some(pioneer_crud::AgentSkillEvidenceTime {
            confirmed_at_unix: NOW,
            latest_at_unix: NOW,
        }),
        skill_id: SkillId::new(SKILL_ID).expect("valid skill id"),
        version_id: VERSION_ID.to_owned(),
        slug: "stable-procedure".to_owned(),
        candidate_key: "create-stable-procedure".to_owned(),
        display_name: "Stable procedure".to_owned(),
        skill_markdown: concat!(
            "---\n",
            "name: \"Stable procedure\"\n",
            "slug: \"stable-procedure\"\n",
            "description: \"Use it. Do not use when: It does not apply.\"\n",
            "---\n",
            "Follow the stable procedure.\n"
        )
        .to_owned(),
        instruction_body: "Follow the stable procedure.".to_owned(),
        when_to_use: "Use it".to_owned(),
        when_not_to_use: "It does not apply".to_owned(),
        fingerprint: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        source_turn_ids: vec![CONTEXT_TURN.to_owned(), ANCHOR_TURN.to_owned()],
    }
}

async fn claimed_input(store: &CrudStore) -> FinalizeSelfImprovementRunInput {
    let run = store
        .create_or_get_self_improvement_run(run_input(), NOW + 1)
        .await
        .expect("run must create");
    let claimed = store
        .claim_available_self_improvement_run(
            WORKSPACE,
            run.id.as_str(),
            1,
            "gateway-finalizer",
            NOW + 2,
            NOW + 100,
        )
        .await
        .expect("claim must execute")
        .expect("claim must win");
    FinalizeSelfImprovementRunInput {
        fence: claimed.fence().expect("running run must expose fence"),
        authority: authority(),
        outcome: SelfImprovementFinalOutcome::AcceptedCreate(accepted_create()),
    }
}

async fn append_anchor(database: &DatabaseConnection, turn_id: &str) -> i64 {
    database
        .execute_unprepared(&format!(
            "INSERT INTO turn (id, thread_id, status, turn_kind, origin) VALUES \
             ('{turn_id}', 'thread_finalize', 'completed', 'conversation', 'user'); \
             INSERT INTO self_improvement_source_turn (
                 workspace_id, thread_id, turn_id, terminal_event_id, terminal_at, created_at
             ) VALUES (
                 '{WORKSPACE}', 'thread_finalize', '{turn_id}', 'event_{turn_id}',
                 CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
             );"
        ))
        .await
        .expect("new anchor fixture must insert");
    scalar_i64(
        database,
        "SELECT MAX(id) AS value FROM self_improvement_source_turn",
    )
    .await
}

async fn claimed_action_input(
    store: &CrudStore,
    scheduled_date_utc: &str,
    source_lower_exclusive: i64,
    source_upper_inclusive: i64,
    outcome: SelfImprovementFinalOutcome,
    now: i64,
) -> FinalizeSelfImprovementRunInput {
    let run = store
        .create_or_get_self_improvement_run(
            NewSelfImprovementRun {
                workspace_id: WORKSPACE.to_owned(),
                activation_epoch: 1,
                scheduled_date_utc: scheduled_date_utc.to_owned(),
                source_lower_exclusive,
                source_upper_inclusive,
                learner_provider: "openai".to_owned(),
                learner_model: "gpt-5.4".to_owned(),
                learner_reasoning_effort: None,
                reviewer_provider: "openai".to_owned(),
                reviewer_model: "gpt-5.4".to_owned(),
                reviewer_reasoning_effort: None,
                pipeline_contract_version: "self-improvement-v1".to_owned(),
            },
            now,
        )
        .await
        .expect("action run must create");
    let claimed = store
        .claim_available_self_improvement_run(
            WORKSPACE,
            run.id.as_str(),
            1,
            "gateway-finalizer",
            now + 1,
            now + 100,
        )
        .await
        .expect("action claim must execute")
        .expect("action claim must win");
    FinalizeSelfImprovementRunInput {
        fence: claimed.fence().expect("action run must expose fence"),
        authority: authority(),
        outcome,
    }
}

async fn scalar_i64(database: &DatabaseConnection, sql: &str) -> i64 {
    database
        .query_one_raw(Statement::from_string(
            DatabaseBackend::Postgres,
            sql.to_owned(),
        ))
        .await
        .expect("query must execute")
        .expect("query must return one row")
        .try_get("", "value")
        .expect("value must decode")
}
