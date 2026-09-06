use migration::{Migrator, MigratorTrait};
use pioneer_crud::{
    AcceptedAgentSkillCreate, AcceptedAgentSkillRollback, AcceptedAgentSkillUpdate, CrudStore,
    FinalizeSelfImprovementRunInput, FinalizeSelfImprovementRunResult, NewSelfImprovementRun,
    SelfImprovementFinalOutcome, SelfImprovementFinalizationAuthority,
    SelfImprovementFinalizationConflict, SelfImprovementNoChangeReason,
};
use pioneer_protocol::SkillId;
use sea_orm::{ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement};

const NOW: i64 = 1_900_300_000;
const WORKSPACE: &str = "ws_finalize";
const CONTEXT_TURN: &str = "turn_context_history";
const ANCHOR_TURN: &str = "turn_new_anchor";
const SKILL_ID: &str = "AAAAAAAAAAAAAAAAAAAAA";
const VERSION_ID: &str = "111111111111111111111";
const VERSION_2_ID: &str = "222222222222222222222";
const VERSION_3_ID: &str = "333333333333333333333";

async fn setup() -> (DatabaseConnection, CrudStore) {
    let database = Database::connect("sqlite::memory:")
        .await
        .expect("in-memory SQLite must open");
    Migrator::up(&database, None)
        .await
        .expect("migrations must apply");
    database
        .execute_unprepared(&format!(
            r#"
            INSERT INTO workspace (id, name, is_active, is_current)
            VALUES ('{WORKSPACE}', 'Finalize', 1, 1);
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
        reviewer_provider: "openai".to_owned(),
        reviewer_model: "gpt-5.4".to_owned(),
        pipeline_contract_version: "self-improvement-v1".to_owned(),
    }
}

fn authority() -> SelfImprovementFinalizationAuthority {
    SelfImprovementFinalizationAuthority {
        effective_enabled: true,
        learner_provider: "openai".to_owned(),
        learner_model: "gpt-5.4".to_owned(),
        reviewer_provider: "openai".to_owned(),
        reviewer_model: "gpt-5.4".to_owned(),
        pipeline_contract_version: "self-improvement-v1".to_owned(),
    }
}

fn accepted_create() -> AcceptedAgentSkillCreate {
    AcceptedAgentSkillCreate {
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
                reviewer_provider: "openai".to_owned(),
                reviewer_model: "gpt-5.4".to_owned(),
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
            DatabaseBackend::Sqlite,
            sql.to_owned(),
        ))
        .await
        .expect("query must execute")
        .expect("query must return one row")
        .try_get("", "value")
        .expect("value must decode")
}

#[tokio::test]
async fn accepted_create_from_private_source_is_atomic_active_and_idempotent() {
    let (database, store) = setup().await;
    let input = claimed_input(&store).await;
    database
        .execute_unprepared(
            "UPDATE thread SET access_class = 'private' WHERE id = 'thread_finalize'",
        )
        .await
        .expect("changing the source to private must not prevent publication");
    assert_eq!(
        store
            .finalize_self_improvement_run(input.clone(), NOW + 3)
            .await
            .expect("finalization must commit"),
        FinalizeSelfImprovementRunResult::Applied {
            skill_id: SkillId::new(SKILL_ID).expect("valid skill id"),
            version_id: VERSION_ID.to_owned(),
        }
    );
    let active = store
        .list_active_agent_skill_versions(WORKSPACE)
        .await
        .expect("active skill must load");
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].version.id, VERSION_ID);
    assert_eq!(
        active[0].version.source_turn_ids,
        vec![CONTEXT_TURN, ANCHOR_TURN]
    );
    let run = store
        .get_self_improvement_run(WORKSPACE, input.fence.run_id.as_str())
        .await
        .expect("run must load")
        .expect("run must exist");
    assert_eq!(run.status, "completed");
    assert_eq!(run.outcome.as_deref(), Some("applied"));
    assert_eq!(run.applied_action.as_deref(), Some("create"));
    assert_eq!(run.claim_token, None);
    assert_eq!(
        store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("state must load")
            .expect("state must exist")
            .cursor_source_id,
        1
    );
    assert_eq!(
        store
            .finalize_self_improvement_run(input.clone(), NOW + 4)
            .await
            .expect("replay must be harmless"),
        FinalizeSelfImprovementRunResult::AlreadyFinalized
    );
    let mut altered_replay = input;
    let SelfImprovementFinalOutcome::AcceptedCreate(create) = &mut altered_replay.outcome else {
        unreachable!("fixture is an accepted create");
    };
    create.source_turn_ids.reverse();
    assert_eq!(
        store
            .finalize_self_improvement_run(altered_replay, NOW + 5)
            .await
            .expect("non-exact replay must be rejected"),
        FinalizeSelfImprovementRunResult::Stale
    );
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM agent_skill_version"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn accepted_version_fails_closed_if_source_becomes_internal() {
    let (database, store) = setup().await;
    let input = claimed_input(&store).await;
    database
        .execute_unprepared(
            "UPDATE thread SET access_class = 'internal' WHERE id = 'thread_finalize'",
        )
        .await
        .expect("source thread must become internal");

    assert_eq!(
        store
            .finalize_self_improvement_run(input, NOW + 3)
            .await
            .expect("visibility loss must be a safe stale outcome"),
        FinalizeSelfImprovementRunResult::Stale
    );
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM agent_skill_version"
        )
        .await,
        0,
        "a new version must not persist after source visibility loss"
    );
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM self_improvement_source_turn"
        )
        .await,
        2,
        "historical source provenance must remain inspectable"
    );
}

#[tokio::test]
async fn accepted_update_and_exact_parent_rollback_share_terminal_transaction() {
    let (database, store) = setup().await;
    let create = claimed_input(&store).await;
    store
        .finalize_self_improvement_run(create, NOW + 3)
        .await
        .expect("create must finalize");

    let update_anchor = "turn_update_anchor";
    let update_upper = append_anchor(&database, update_anchor).await;
    let update = AcceptedAgentSkillUpdate {
        skill_id: SkillId::new(SKILL_ID).expect("valid skill id"),
        expected_active_version_id: VERSION_ID.to_owned(),
        version_id: VERSION_2_ID.to_owned(),
        version_number: 2,
        slug: "stable-procedure".to_owned(),
        candidate_key: "update-stable-procedure".to_owned(),
        display_name: "Stable procedure v2".to_owned(),
        skill_markdown: concat!(
            "---\n",
            "name: \"Stable procedure v2\"\n",
            "slug: \"stable-procedure\"\n",
            "description: \"Use v2. Do not use when: V2 does not apply.\"\n",
            "---\n",
            "Follow the improved stable procedure.\n"
        )
        .to_owned(),
        instruction_body: "Follow the improved stable procedure.".to_owned(),
        when_to_use: "Use v2".to_owned(),
        when_not_to_use: "V2 does not apply".to_owned(),
        fingerprint: "b".repeat(64),
        source_turn_ids: vec![CONTEXT_TURN.to_owned(), update_anchor.to_owned()],
    };
    let update_input = claimed_action_input(
        &store,
        "2030-03-04",
        1,
        update_upper,
        SelfImprovementFinalOutcome::AcceptedUpdate(update),
        NOW + 4,
    )
    .await;
    let update_run_id = update_input.fence.run_id.clone();
    assert_eq!(
        store
            .finalize_self_improvement_run(update_input.clone(), NOW + 6)
            .await
            .expect("update finalization"),
        FinalizeSelfImprovementRunResult::Applied {
            skill_id: SkillId::new(SKILL_ID).expect("valid skill id"),
            version_id: VERSION_2_ID.to_owned(),
        }
    );
    assert_eq!(
        store
            .finalize_self_improvement_run(update_input, NOW + 7)
            .await
            .expect("exact update replay"),
        FinalizeSelfImprovementRunResult::AlreadyFinalized
    );
    let active = store
        .list_active_agent_skill_versions(WORKSPACE)
        .await
        .expect("updated active version");
    assert_eq!(active[0].version.id, VERSION_2_ID);
    assert_eq!(
        active[0].version.parent_version_id.as_deref(),
        Some(VERSION_ID)
    );
    assert_eq!(active[0].version.display_name, "Stable procedure v2");
    let update_run = store
        .get_self_improvement_run(WORKSPACE, update_run_id.as_str())
        .await
        .expect("update run query")
        .expect("update run");
    assert_eq!(update_run.applied_action.as_deref(), Some("update"));
    assert_eq!(update_run.previous_version_id.as_deref(), Some(VERSION_ID));
    assert_eq!(
        update_run.resulting_version_id.as_deref(),
        Some(VERSION_2_ID)
    );

    let rollback_anchor = "turn_rollback_anchor";
    let rollback_upper = append_anchor(&database, rollback_anchor).await;
    let rollback_input = claimed_action_input(
        &store,
        "2030-03-05",
        update_upper,
        rollback_upper,
        SelfImprovementFinalOutcome::AcceptedRollback(AcceptedAgentSkillRollback {
            skill_id: SkillId::new(SKILL_ID).expect("valid skill id"),
            expected_active_version_id: VERSION_2_ID.to_owned(),
            target_parent_version_id: VERSION_ID.to_owned(),
            candidate_key: "rollback-stable-procedure".to_owned(),
            source_turn_ids: vec![rollback_anchor.to_owned()],
        }),
        NOW + 8,
    )
    .await;
    let rollback_run_id = rollback_input.fence.run_id.clone();
    assert_eq!(
        store
            .finalize_self_improvement_run(rollback_input.clone(), NOW + 10)
            .await
            .expect("rollback finalization"),
        FinalizeSelfImprovementRunResult::Applied {
            skill_id: SkillId::new(SKILL_ID).expect("valid skill id"),
            version_id: VERSION_ID.to_owned(),
        }
    );
    assert_eq!(
        store
            .finalize_self_improvement_run(rollback_input, NOW + 11)
            .await
            .expect("exact rollback replay"),
        FinalizeSelfImprovementRunResult::AlreadyFinalized
    );
    let restored = store
        .list_active_agent_skill_versions(WORKSPACE)
        .await
        .expect("restored active version");
    assert_eq!(restored[0].version.id, VERSION_ID);
    assert_eq!(restored[0].version.display_name, "Stable procedure");
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM agent_skill_version"
        )
        .await,
        2,
        "rollback must not create a third version"
    );
    let rollback_run = store
        .get_self_improvement_run(WORKSPACE, rollback_run_id.as_str())
        .await
        .expect("rollback run query")
        .expect("rollback run");
    assert_eq!(rollback_run.applied_action.as_deref(), Some("rollback"));
    assert_eq!(
        rollback_run.previous_version_id.as_deref(),
        Some(VERSION_2_ID)
    );
    assert_eq!(
        rollback_run.resulting_version_id.as_deref(),
        Some(VERSION_ID)
    );
    assert_eq!(
        store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("workspace state")
            .expect("workspace state row")
            .cursor_source_id,
        rollback_upper
    );

    let post_rollback_anchor = "turn_post_rollback_update_anchor";
    let post_rollback_upper = append_anchor(&database, post_rollback_anchor).await;
    let post_rollback_update = claimed_action_input(
        &store,
        "2030-03-06",
        rollback_upper,
        post_rollback_upper,
        SelfImprovementFinalOutcome::AcceptedUpdate(AcceptedAgentSkillUpdate {
            skill_id: SkillId::new(SKILL_ID).expect("valid skill id"),
            expected_active_version_id: VERSION_ID.to_owned(),
            version_id: VERSION_3_ID.to_owned(),
            version_number: 3,
            slug: "stable-procedure".to_owned(),
            candidate_key: "update-after-rollback".to_owned(),
            display_name: "Stable procedure v3".to_owned(),
            skill_markdown: concat!(
                "---\n",
                "name: \"Stable procedure v3\"\n",
                "slug: \"stable-procedure\"\n",
                "description: \"Use v3. Do not use when: V3 does not apply.\"\n",
                "---\n",
                "Follow the post-rollback stable procedure.\n"
            )
            .to_owned(),
            instruction_body: "Follow the post-rollback stable procedure.".to_owned(),
            when_to_use: "Use v3".to_owned(),
            when_not_to_use: "V3 does not apply".to_owned(),
            fingerprint: "c".repeat(64),
            source_turn_ids: vec![post_rollback_anchor.to_owned()],
        }),
        NOW + 12,
    )
    .await;
    assert_eq!(
        store
            .finalize_self_improvement_run(post_rollback_update, NOW + 14)
            .await
            .expect("post-rollback update finalization"),
        FinalizeSelfImprovementRunResult::Applied {
            skill_id: SkillId::new(SKILL_ID).expect("valid skill id"),
            version_id: VERSION_3_ID.to_owned(),
        }
    );
    let post_rollback_active = store
        .list_active_agent_skill_versions(WORKSPACE)
        .await
        .expect("post-rollback active version");
    assert_eq!(post_rollback_active[0].version.id, VERSION_3_ID);
    assert_eq!(post_rollback_active[0].version.version_number, 3);
    assert_eq!(
        post_rollback_active[0].version.parent_version_id.as_deref(),
        Some(VERSION_ID),
        "a post-rollback update branches from the exact restored active version"
    );
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM agent_skill_version"
        )
        .await,
        3,
        "the historical version number remains occupied and the new update advances the sequence"
    );
}

#[tokio::test]
async fn late_terminal_failure_rolls_back_update_version_pointer_run_and_cursor() {
    let (database, store) = setup().await;
    store
        .finalize_self_improvement_run(claimed_input(&store).await, NOW + 3)
        .await
        .expect("create must finalize");
    let update_anchor = "turn_failed_update_anchor";
    let update_upper = append_anchor(&database, update_anchor).await;
    let input = claimed_action_input(
        &store,
        "2030-03-04",
        1,
        update_upper,
        SelfImprovementFinalOutcome::AcceptedUpdate(AcceptedAgentSkillUpdate {
            skill_id: SkillId::new(SKILL_ID).expect("valid skill id"),
            expected_active_version_id: VERSION_ID.to_owned(),
            version_id: VERSION_2_ID.to_owned(),
            version_number: 2,
            slug: "stable-procedure".to_owned(),
            candidate_key: "failed-update".to_owned(),
            display_name: "Never committed".to_owned(),
            skill_markdown: "# Never committed".to_owned(),
            instruction_body: "Never committed body".to_owned(),
            when_to_use: "Never".to_owned(),
            when_not_to_use: "Always".to_owned(),
            fingerprint: "b".repeat(64),
            source_turn_ids: vec![update_anchor.to_owned()],
        }),
        NOW + 4,
    )
    .await;
    database
        .execute_unprepared(&format!(
            "CREATE TRIGGER reject_terminal_run_update \
             BEFORE UPDATE ON self_improvement_run \
             WHEN OLD.id = '{}' \
             BEGIN SELECT RAISE(ABORT, 'forced terminal failure'); END;",
            input.fence.run_id
        ))
        .await
        .expect("terminal failure trigger must install");
    store
        .finalize_self_improvement_run(input, NOW + 6)
        .await
        .expect_err("late terminal failure must roll back every write");

    let active = store
        .list_active_agent_skill_versions(WORKSPACE)
        .await
        .expect("active version after failed transaction");
    assert_eq!(active[0].version.id, VERSION_ID);
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM agent_skill_version"
        )
        .await,
        1
    );
    assert_eq!(
        store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("workspace state")
            .expect("workspace state row")
            .cursor_source_id,
        1
    );
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM self_improvement_run \
             WHERE status = 'completed'"
        )
        .await,
        1,
        "only the earlier create run may be completed"
    );
}

#[tokio::test]
async fn current_fingerprint_update_is_atomic_terminal_no_change() {
    let (database, store) = setup().await;
    store
        .finalize_self_improvement_run(claimed_input(&store).await, NOW + 3)
        .await
        .expect("create must finalize");
    let anchor = "turn_current_fingerprint_anchor";
    let upper = append_anchor(&database, anchor).await;
    let input = claimed_action_input(
        &store,
        "2030-03-04",
        1,
        upper,
        SelfImprovementFinalOutcome::AcceptedUpdate(AcceptedAgentSkillUpdate {
            skill_id: SkillId::new(SKILL_ID).expect("valid skill id"),
            expected_active_version_id: VERSION_ID.to_owned(),
            version_id: VERSION_2_ID.to_owned(),
            version_number: 2,
            slug: "stable-procedure".to_owned(),
            candidate_key: "current-fingerprint".to_owned(),
            display_name: "Stable procedure".to_owned(),
            skill_markdown: accepted_create().skill_markdown,
            instruction_body: "Follow the stable procedure.".to_owned(),
            when_to_use: "Use it".to_owned(),
            when_not_to_use: "It does not apply".to_owned(),
            fingerprint: "a".repeat(64),
            source_turn_ids: vec![anchor.to_owned()],
        }),
        NOW + 4,
    )
    .await;
    assert_eq!(
        store
            .finalize_self_improvement_run(input.clone(), NOW + 6)
            .await
            .expect("current fingerprint decision"),
        FinalizeSelfImprovementRunResult::NoChange {
            reason: SelfImprovementNoChangeReason::HostValidationRejected,
        }
    );
    assert_eq!(
        store
            .finalize_self_improvement_run(input, NOW + 7)
            .await
            .expect("current fingerprint replay"),
        FinalizeSelfImprovementRunResult::AlreadyFinalized
    );
    assert_eq!(
        store
            .list_active_agent_skill_versions(WORKSPACE)
            .await
            .expect("unchanged active skill")[0]
            .version
            .id,
        VERSION_ID
    );
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM agent_skill_version"
        )
        .await,
        1
    );
    assert_eq!(
        store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("workspace state")
            .expect("workspace state row")
            .cursor_source_id,
        upper
    );
}

#[tokio::test]
async fn stale_claim_authority_epoch_and_cursor_write_nothing() {
    for mutate in 0..6 {
        let (database, store) = setup().await;
        let mut input = claimed_input(&store).await;
        let mut finalization_now = NOW + 3;
        match mutate {
            0 => input.fence.claim_token.push_str("stale"),
            1 => input.authority.effective_enabled = false,
            2 => input.fence.activation_epoch += 1,
            3 => {
                database
                    .execute_unprepared(&format!(
                        "UPDATE self_improvement_workspace_state SET cursor_source_id = 1 \
                         WHERE workspace_id = '{WORKSPACE}'"
                    ))
                    .await
                    .expect("cursor fixture must change");
            }
            4 => input.authority.learner_model = "another-model".to_owned(),
            5 => finalization_now = NOW + 100,
            _ => unreachable!(),
        }
        assert_eq!(
            store
                .finalize_self_improvement_run(input, finalization_now)
                .await
                .expect("stale finalization must be reported"),
            FinalizeSelfImprovementRunResult::Stale
        );
        assert_eq!(
            scalar_i64(&database, "SELECT COUNT(*) AS value FROM agent_skill").await,
            0
        );
        assert_eq!(
            scalar_i64(
                &database,
                "SELECT COUNT(*) AS value FROM self_improvement_run WHERE status = 'completed'"
            )
            .await,
            0
        );
    }
}

#[tokio::test]
async fn expired_owner_and_takeover_have_one_terminal_writer() {
    let (database, gateway_a) = setup().await;
    let gateway_b = CrudStore::new(database.clone());
    let old_input = claimed_input(&gateway_a).await;
    let reclaimed = gateway_b
        .claim_available_self_improvement_run(
            WORKSPACE,
            old_input.fence.run_id.as_str(),
            old_input.fence.activation_epoch,
            "gateway-takeover",
            NOW + 100,
            NOW + 200,
        )
        .await
        .expect("takeover claim must execute")
        .expect("expired owner must be reclaimed");
    let mut takeover_input = old_input.clone();
    takeover_input.fence = reclaimed
        .fence()
        .expect("takeover owner must expose a fence");

    let (old_result, takeover_result) = tokio::join!(
        gateway_a.finalize_self_improvement_run(old_input, NOW + 101),
        gateway_b.finalize_self_improvement_run(takeover_input, NOW + 101),
    );
    assert_eq!(
        old_result.expect("old finalization must report authority"),
        FinalizeSelfImprovementRunResult::Stale
    );
    assert_eq!(
        takeover_result.expect("takeover finalization must execute"),
        FinalizeSelfImprovementRunResult::Applied {
            skill_id: SkillId::new(SKILL_ID).expect("valid skill id"),
            version_id: VERSION_ID.to_owned(),
        }
    );
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM agent_skill_version"
        )
        .await,
        1
    );
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM self_improvement_run WHERE status = 'completed'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn no_change_completes_range_without_skill_rows() {
    let (database, store) = setup().await;
    let mut input = claimed_input(&store).await;
    input.outcome = SelfImprovementFinalOutcome::NoChange {
        reason: SelfImprovementNoChangeReason::ReviewerRejected,
        reason_codes: vec!["insufficient_generality".to_owned()],
    };
    assert_eq!(
        store
            .finalize_self_improvement_run(input, NOW + 3)
            .await
            .expect("no-change must commit"),
        FinalizeSelfImprovementRunResult::NoChange {
            reason: SelfImprovementNoChangeReason::ReviewerRejected,
        }
    );
    assert_eq!(
        scalar_i64(&database, "SELECT COUNT(*) AS value FROM agent_skill").await,
        0
    );
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM self_improvement_run \
             WHERE status = 'completed' AND outcome = 'no_change'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn conflict_and_late_constraint_failure_leave_every_write_rolled_back() {
    let (database, store) = setup().await;
    let input = claimed_input(&store).await;
    database
        .execute_unprepared(&format!(
            "INSERT INTO agent_skill (id, workspace_id, slug, created_at, updated_at) VALUES \
             ('CCCCCCCCCCCCCCCCCCCCC', '{WORKSPACE}', 'stable-procedure', \
              CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
        ))
        .await
        .expect("conflict fixture must insert");
    assert_eq!(
        store
            .finalize_self_improvement_run(input, NOW + 3)
            .await
            .expect("conflict must be explicit"),
        FinalizeSelfImprovementRunResult::Conflict(SelfImprovementFinalizationConflict::Slug)
    );
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM self_improvement_run WHERE status = 'completed'"
        )
        .await,
        0
    );

    let (database, store) = setup().await;
    let input = claimed_input(&store).await;
    database
        .execute_unprepared(&format!(
            r#"
            INSERT INTO agent_skill (id, workspace_id, slug, created_at, updated_at)
            VALUES (
                'CCCCCCCCCCCCCCCCCCCCC', '{WORKSPACE}', 'other-procedure',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            );
            INSERT INTO agent_skill_version (
                id, skill_id, version_number, source_run_id, candidate_key, display_name,
                skill_markdown, instruction_body, when_to_use, when_not_to_use, fingerprint,
                source_turn_ids_json, created_at
            ) VALUES (
                '222222222222222222222', 'CCCCCCCCCCCCCCCCCCCCC', 1,
                '{}', 'create-stable-procedure', 'Existing', '# Existing', 'Existing body',
                'Use it', 'Do not use it',
                'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                '["existing_turn"]', CURRENT_TIMESTAMP
            );
            "#,
            input.fence.run_id
        ))
        .await
        .expect("candidate conflict fixture must insert");
    assert_eq!(
        store
            .finalize_self_improvement_run(input, NOW + 3)
            .await
            .expect("candidate conflict must be explicit"),
        FinalizeSelfImprovementRunResult::Conflict(SelfImprovementFinalizationConflict::Candidate)
    );
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM self_improvement_run WHERE status = 'completed'"
        )
        .await,
        0
    );

    let (database, store) = setup().await;
    let input = claimed_input(&store).await;
    database
        .execute_unprepared(&format!(
            r#"
            INSERT INTO agent_skill (id, workspace_id, slug, created_at, updated_at)
            VALUES (
                'CCCCCCCCCCCCCCCCCCCCC', '{WORKSPACE}', 'other-procedure',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            );
            INSERT INTO agent_skill_version (
                id, skill_id, version_number, candidate_key, display_name, skill_markdown,
                instruction_body, when_to_use, when_not_to_use, fingerprint,
                source_turn_ids_json, created_at
            ) VALUES (
                '222222222222222222222', 'CCCCCCCCCCCCCCCCCCCCC', 1, 'existing',
                'Existing', '# Existing', 'Existing body', 'Use it', 'Do not use it',
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                '["existing_turn"]', CURRENT_TIMESTAMP
            );
            "#
        ))
        .await
        .expect("fingerprint conflict fixture must insert");
    assert_eq!(
        store
            .finalize_self_improvement_run(input, NOW + 3)
            .await
            .expect("duplicate fingerprint must be terminal no-change"),
        FinalizeSelfImprovementRunResult::NoChange {
            reason: SelfImprovementNoChangeReason::HostValidationRejected,
        }
    );
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM self_improvement_run WHERE status = 'completed'"
        )
        .await,
        1
    );
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM agent_skill_version"
        )
        .await,
        1,
        "duplicate candidate must not create another version"
    );

    let (database, store) = setup().await;
    let input = claimed_input(&store).await;
    database
        .execute_unprepared(
            "CREATE TRIGGER reject_agent_skill_version \
             BEFORE INSERT ON agent_skill_version \
             BEGIN SELECT RAISE(ABORT, 'forced version failure'); END;",
        )
        .await
        .expect("failure trigger must install");
    store
        .finalize_self_improvement_run(input, NOW + 3)
        .await
        .expect_err("late version failure must roll back transaction");
    assert_eq!(
        scalar_i64(&database, "SELECT COUNT(*) AS value FROM agent_skill").await,
        0
    );
    assert_eq!(
        scalar_i64(
            &database,
            "SELECT COUNT(*) AS value FROM self_improvement_run WHERE status = 'completed'"
        )
        .await,
        0
    );
    assert_eq!(
        store
            .get_self_improvement_workspace_state(WORKSPACE)
            .await
            .expect("state must load")
            .expect("state must exist")
            .cursor_source_id,
        0
    );
}
