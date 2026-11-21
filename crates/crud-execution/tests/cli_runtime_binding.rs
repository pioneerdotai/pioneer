use anyhow::Result;
use pioneer_crud_execution::cli_runtime_binding::{
    FrozenBindingRoot, NewCliRuntimeThreadBinding, confirm_marked_fork_intent,
    discard_rejected_fork_intent, find_thread_binding, insert_fork_intent_if_absent,
    update_thread_resume_cursor, upsert_thread_binding,
};
use sea_orm::{ConnectionTrait, DbBackend, Statement};

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
