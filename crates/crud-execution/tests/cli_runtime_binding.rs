use anyhow::Result;
use pioneer_crud_execution::cli_attempt_commits::{activate_attempt, prepare_initial_attempt};
use pioneer_crud_execution::cli_runtime_binding::{
    CliRuntimeExecutionSegmentStatus, CliRuntimeTurnAttemptStatus, FrozenBindingRoot,
    NewCliRuntimeThreadBinding, NewCliRuntimeTurnBinding, confirm_marked_fork_intent,
    discard_rejected_fork_intent, find_thread_binding, insert_fork_intent_if_absent,
    update_thread_resume_cursor, upsert_thread_binding,
};
use pioneer_crud_execution::cli_runtime_store::CliRuntimeStore;
use pioneer_crud_execution::input_mapping::InputMapping;
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_postgres_pools::PostgresDatabase;
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};

struct InlineRoot;

impl FrozenBindingRoot for InlineRoot {
    fn locator(&self) -> Option<String> {
        None
    }

    async fn verify<'a, C: ConnectionTrait>(&'a self, _: &'a C, _: &'a str) -> Result<()> {
        Ok(())
    }
}

async fn seed_workspace(db: &impl ConnectionTrait, workspace: &str) -> Result<()> {
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO workspace (id, name) VALUES ($1, 'Workspace')",
        [workspace.into()],
    ))
    .await?;
    Ok(())
}

#[tokio::test]
async fn marked_cli_fork_intent_is_insert_once_and_confirmed_once() {
    pioneer_postgres_test::with_database(async |fixture| {
    let db = fixture.connection();
    seed_workspace(db, "ws_marked_fork").await?;
    let now = pioneer_crud_execution::time::unix_to_datetime(1_700_010_000);
    let pending = NewCliRuntimeThreadBinding {
        thread_id: "child_marked_fork".to_owned(),
        workspace_id: "ws_marked_fork".to_owned(),
        runtime_id: "codex".to_owned(),
        runtime_kind: "codex".to_owned(),
        native_thread_id: "source_provider".to_owned(),
        native_session_id: None,
        native_root_thread_id: Some("source_provider".to_owned()),
        native_cwd: Some("/tmp".to_owned()),
        native_model: Some("gpt-5".to_owned()),
        resume_cursor_json: r#"{"forkSourceTurnId":"child_first","forkBoundaryTurnId":"provider_turn","forkMarker":"pioneer-cli-fork:nonce"}"#.to_owned(),
        status: "fork_pending".to_owned(),
        created_at: now,
        updated_at: now,
    };
    assert!(
        insert_fork_intent_if_absent(db, pending.clone(), &InlineRoot)
            .await
            .unwrap()
    );
    assert!(
        !insert_fork_intent_if_absent(db, pending.clone(), &InlineRoot)
            .await
            .unwrap()
    );
    let durable = find_thread_binding(db, "child_marked_fork")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(durable.resume_cursor_json, pending.resume_cursor_json);
    let committed = confirm_marked_fork_intent(db, &durable, "provider_fork", Some("/tmp".to_owned()), Some("gpt-5".to_owned()),
        r#"{"threadId":"provider_fork","forkSourceTurnId":"child_first","forkBoundaryTurnId":"provider_turn","forkMarker":"pioneer-cli-fork:nonce"}"#.to_owned(), now,
     &InlineRoot).await.unwrap();
    assert_eq!(committed.status, "active");
    assert_eq!(committed.native_thread_id, "provider_fork");
    assert!(
        confirm_marked_fork_intent(db, &durable,
                "duplicate_fork",
                None,
                None,
                "{}".to_owned(),
                now,
             &InlineRoot)
            .await
            .is_err()
    );
    assert!(
        !discard_rejected_fork_intent(db, "child_marked_fork",
                "codex",
                "source_provider",
                pending.resume_cursor_json.as_str(),
            )
            .await
            .unwrap()
    );
    assert!(
        update_thread_resume_cursor(db, "child_marked_fork",
                "provider_fork",
                pending.resume_cursor_json.as_str(),
                "{}".to_owned(),
                now,
             &InlineRoot)
            .await
            .is_err(),
        "a stale receipt writer must not erase the confirmed fork cursor"
    );
    assert_eq!(
        find_thread_binding(db, "child_marked_fork")
            .await
            .unwrap()
            .unwrap()
            .native_thread_id,
        "provider_fork"
    );
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn cli_resume_cursor_compares_and_preserves_exact_json_text() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = fixture.connection();
        seed_workspace(db, "ws_cursor").await?;
        let now = pioneer_crud_execution::time::unix_to_datetime(1_700_010_001);
        let original = r#"{ "threadId":"native", "providerOpaque":{"marker":"🧪\u0000"} }"#;
        let equivalent =
            serde_json::to_string(&serde_json::from_str::<serde_json::Value>(original)?)?;
        assert_ne!(original, equivalent);
        let binding = NewCliRuntimeThreadBinding {
            thread_id: "thread".into(),
            workspace_id: "ws_cursor".into(),
            runtime_id: "codex".into(),
            runtime_kind: "codex".into(),
            native_thread_id: "native".into(),
            native_session_id: None,
            native_root_thread_id: None,
            native_cwd: None,
            native_model: None,
            resume_cursor_json: original.into(),
            status: "active".into(),
            created_at: now,
            updated_at: now,
        };
        let inserted = upsert_thread_binding(db, binding, &InlineRoot).await?;
        assert_eq!(inserted.resume_cursor_json, original);
        let next = r#"{ "threadId":"native", "sequence":2 }"#;
        let rejected = update_thread_resume_cursor(
            db,
            "thread",
            "native",
            &equivalent,
            next.into(),
            now,
            &InlineRoot,
        )
        .await
        .expect_err("an equivalent JSON spelling must lose the CAS");
        assert!(
            rejected
                .to_string()
                .contains("changed before context receipt")
        );
        let saved = find_thread_binding(db, "thread")
            .await?
            .expect("the rejected replacement must preserve the binding");
        assert_eq!(saved.resume_cursor_json, original);
        let updated = update_thread_resume_cursor(
            db,
            "thread",
            "native",
            &saved.resume_cursor_json,
            next.into(),
            now,
            &InlineRoot,
        )
        .await?;
        assert_eq!(updated.resume_cursor_json, next);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn concurrent_cli_fork_intents_have_one_winner_and_stale_cleanup_cannot_delete_it() {
    pioneer_postgres_test::with_database(async |fixture| {
    let db = fixture.connection();
    seed_workspace(db, "ws_fork_intent_race").await?;
    let now = pioneer_crud_execution::time::unix_to_datetime(1_700_010_001);
    let pending = NewCliRuntimeThreadBinding {
        thread_id: "child_fork_race".to_owned(),
        workspace_id: "ws_fork_intent_race".to_owned(),
        runtime_id: "codex".to_owned(),
        runtime_kind: "codex".to_owned(),
        native_thread_id: "source".to_owned(),
        native_session_id: None,
        native_root_thread_id: Some("source".to_owned()),
        native_cwd: None,
        native_model: None,
        resume_cursor_json: r#"{"forkSourceTurnId":"source_turn","forkBoundaryTurnId":"boundary","forkMarker":"pioneer-cli-fork:race-a"}"#.to_owned(),
        status: "fork_pending".to_owned(),
        created_at: now,
        updated_at: now,
    };
    let mut other = pending.clone();
    other.resume_cursor_json = r#"{"forkSourceTurnId":"source_turn","forkBoundaryTurnId":"boundary","forkMarker":"pioneer-cli-fork:race-b"}"#.to_owned();
    let contender = fixture.connect().await?;
    let (first, second) = tokio::join!(
        insert_fork_intent_if_absent(db, pending.clone(), &InlineRoot),
        insert_fork_intent_if_absent(&contender, other.clone(), &InlineRoot),
    );
    assert_eq!(
        usize::from(first.unwrap()) + usize::from(second.unwrap()),
        1
    );
    let winner = find_thread_binding(db, "child_fork_race")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(winner.status, "fork_pending");
    let stale = if winner.resume_cursor_json == pending.resume_cursor_json {
        other.resume_cursor_json.as_str()
    } else {
        pending.resume_cursor_json.as_str()
    };
    assert!(
        !discard_rejected_fork_intent(db, "child_fork_race", "codex", "source", stale,)
            .await
            .unwrap()
    );
    assert_eq!(
        find_thread_binding(db, "child_fork_race")
            .await
            .unwrap()
            .unwrap()
            .resume_cursor_json,
        winner.resume_cursor_json
    );
    assert!(
        discard_rejected_fork_intent(db, "child_fork_race",
                "codex",
                "source",
                winner.resume_cursor_json.as_str(),
            )
            .await
            .unwrap()
    );
    assert!(
        find_thread_binding(db, "child_fork_race")
            .await
            .unwrap()
            .is_none()
    );
        contender.close().await?;
    Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn cli_runtime_execution_segments_enforce_attempt_ownership_and_sequence() {
    pioneer_postgres_test::with_database(async |fixture| {
        seed_workspace(fixture.connection(), "ws_cli_segments").await?;
        let db = PostgresDatabase::new(
            fixture.connect_read_only().await?,
            fixture.connection().clone(),
        );
        let store = CliRuntimeStore::new(db.clone());
        let now = unix_to_datetime(1_700_015_000);
        let turn_id = "turn_cli_segments";
        let native_thread_id = "codex-thread-segments";
        let submission_id = "codex-submission-segments";
        let binding = NewCliRuntimeTurnBinding {
            turn_id: turn_id.to_owned(),
            thread_id: "thread_cli_segments".to_owned(),
            continuation_thread_id: "thread_cli_segments".to_owned(),
            workspace_id: "ws_cli_segments".to_owned(),
            runtime_id: "codex".to_owned(),
            runtime_kind: "codex".to_owned(),
            native_thread_id: native_thread_id.to_owned(),
            native_turn_id: None,
            request_id: None,
            status: "starting".to_owned(),
            model: Some("gpt-5".to_owned()),
            cwd: Some("/tmp/project".to_owned()),
            sandbox_json: None,
            approval_policy: Some("on-request".to_owned()),
            input_mapping_json: "{}".to_owned(),
            created_at: now,
            updated_at: now,
        };
        let mapping = InputMapping::prepare(&binding.input_mapping_json, false)?;
        let tx = db.begin().await?;
        let ((_, attempt), _) = prepare_initial_attempt(
            &tx,
            &binding,
            "cli-attempt-segments",
            1,
            mapping,
            &InlineRoot,
        )
        .await
        .expect("initial CLI runtime attempt should prepare");
        tx.commit().await?;

        let tx = db.begin().await?;
        activate_attempt(
            &tx,
            turn_id,
            attempt.id.as_str(),
            submission_id,
            None,
            now,
            None,
            None,
        )
        .await
        .expect("initial CLI runtime attempt should activate");
        tx.commit().await?;

        let (_, _, first) = store
            .register_cli_runtime_execution_segment(
                turn_id,
                native_thread_id,
                "codex-segment-1",
                now,
            )
            .await
            .expect("first execution segment should register");
        assert_eq!(first.segment_index, 1);
        assert_eq!(first.attempt_id, attempt.id);
        let (_, _, duplicate) = store
            .register_cli_runtime_execution_segment(
                turn_id,
                native_thread_id,
                "codex-segment-1",
                now,
            )
            .await
            .expect("execution segment registration should be idempotent");
        assert_eq!(duplicate.id, first.id);
        assert!(
            store
                .register_cli_runtime_execution_segment(
                    turn_id,
                    native_thread_id,
                    "codex-segment-2",
                    now,
                )
                .await
                .expect_err("a second running execution segment must be rejected")
                .to_string()
                .contains("already has active execution segment")
        );
        assert!(
            store
                .register_cli_runtime_execution_segment(
                    turn_id,
                    "codex-subagent-thread",
                    "codex-subagent-segment",
                    now,
                )
                .await
                .expect_err("a subagent thread must not own a root execution segment")
                .to_string()
                .contains("cannot accept this execution segment")
        );

        let (_, owner_attempt, completed_first) = store
            .terminalize_cli_runtime_execution_segment(
                "codex",
                "codex-segment-1",
                CliRuntimeExecutionSegmentStatus::Completed,
                None,
                now,
            )
            .await
            .expect("first execution segment should terminalize")
            .expect("first execution segment should exist");
        assert_eq!(owner_attempt.status, CliRuntimeTurnAttemptStatus::Running);
        assert_eq!(
            completed_first.status,
            CliRuntimeExecutionSegmentStatus::Completed
        );

        let (_, _, second) = store
            .register_cli_runtime_execution_segment(
                turn_id,
                native_thread_id,
                "codex-segment-2",
                now,
            )
            .await
            .expect("next execution segment should register after terminal completion");
        assert_eq!(second.segment_index, 2);
        let segment_owner = store
            .resolve_cli_runtime_native_turn_owner("codex", "codex-segment-2")
            .await
            .expect("segment owner lookup should succeed")
            .expect("segment owner should exist");
        assert_eq!(segment_owner.attempt.id, attempt.id);
        assert_eq!(
            segment_owner
                .segment
                .as_ref()
                .map(|segment| segment.id.as_str()),
            Some(second.id.as_str())
        );
        let submission_owner = store
            .resolve_cli_runtime_native_turn_owner("codex", submission_id)
            .await
            .expect("submission owner lookup should succeed")
            .expect("submission owner should exist");
        assert!(submission_owner.segment.is_none());
        assert_eq!(submission_owner.attempt.id, attempt.id);

        assert!(
            store
                .mark_cli_runtime_turn_attempt_terminal(
                    attempt.id.as_str(),
                    CliRuntimeTurnAttemptStatus::Interrupted,
                    Some("user cancelled".to_owned()),
                    now,
                )
                .await
                .expect("attempt terminalization should succeed")
        );
        let terminal_owner = store
            .resolve_cli_runtime_native_turn_owner("codex", "codex-segment-2")
            .await
            .expect("terminal segment owner lookup should succeed")
            .expect("terminal segment owner should exist");
        assert_eq!(
            terminal_owner.attempt.status,
            CliRuntimeTurnAttemptStatus::Interrupted
        );
        let terminal_segment = terminal_owner
            .segment
            .expect("terminal segment should remain durable");
        assert_eq!(
            terminal_segment.status,
            CliRuntimeExecutionSegmentStatus::Interrupted
        );
        assert_eq!(
            terminal_segment.failure_reason.as_deref(),
            Some("user cancelled")
        );
        Ok(())
    })
    .await
    .unwrap();
}
