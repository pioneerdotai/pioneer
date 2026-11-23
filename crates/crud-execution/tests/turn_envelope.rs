use anyhow::{Context, Result};
use pioneer_crud_execution::turn_envelope::{
    authorization_context_as_text, find_turn_execution_security_snapshot, pin_native_context,
    set_turn_execution_authorization_context, set_turn_execution_envelope,
    set_turn_execution_security_snapshot,
};
use pioneer_entity::turn;
use pioneer_postgres_test::with_database;
use pioneer_protocol::{
    NativeContextBudget, NativeServiceModePlan, ServiceModeParameters,
    TurnExecutionSecuritySnapshot, TurnFilesystemAccess, TurnFilesystemSandboxEntry,
    TurnPermissionMode, TurnPermissionProfileSnapshot, TurnPermissionProfileSource,
    TurnSandboxMode,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, DbBackend, EntityTrait, QueryFilter,
    QuerySelect, Statement, TransactionTrait,
};
use serde_json::Value;

#[tokio::test]
async fn native_context_cas_keeps_existing_cap_and_authorization_json_keeps_its_bytes() -> Result<()>
{
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            r#"
            INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
            INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                VALUES ('thread', 'w', '', 'agent', 'model', 'provider', 'active');
            INSERT INTO "turn" (id, thread_id, status, execution_security_snapshot_json)
                VALUES ('turn', 'thread', 'in_progress',
                    '{ "backend": { "execution_backend": "native" }, "unknown": "\u0000" }');
            "#,
        )
        .await?;
        let authorization = r#"{ "z": 1, "a": "\u0000" }"#;
        assert!(set_turn_execution_authorization_context(db, "turn", authorization).await?);
        let saved = turn::Entity::find_by_id("turn")
            .select_only()
            .column_as(authorization_context_as_text(), "authorization")
            .into_tuple::<String>()
            .one(db)
            .await?
            .context("missing authorization envelope")?;
        assert_eq!(saved.as_bytes(), authorization.as_bytes());

        let budget = NativeContextBudget {
            service_mode: None,
            instance: "provider".into(),
            authority: Some("account".into()),
            model: "model".into(),
            manual_window: None,
            context_window: 272_000,
            max_input: None,
            max_output: Some(16_000),
        };
        assert!(!pin_native_context(db, "foreign", "turn", &budget, None).await?);
        let other = fixture.connect().await?;
        let (first, second) = tokio::try_join!(
            pin_native_context(db, "w", "turn", &budget, None),
            pin_native_context(&other, "w", "turn", &budget, None),
        )?;
        assert_ne!(first, second);
        let mut plan = budget.clone();
        plan.context_window = 1_050_000;
        plan.service_mode = Some(NativeServiceModePlan {
            id: "standard".into(),
            parameters: ServiceModeParameters::OpenAiStandard,
        });
        assert!(pin_native_context(db, "w", "turn", &plan, None).await?);
        assert!(!pin_native_context(db, "w", "turn", &plan, None).await?);
        let snapshot = turn::Entity::find_by_id("turn")
            .one(db)
            .await?
            .context("missing turn")?
            .execution_security_snapshot_json
            .context("missing security envelope")?;
        let value: Value = serde_json::from_str(&snapshot)?;
        assert_eq!(value["native_context"]["context_window"], 272_000);
        assert_eq!(value["native_context"]["service_mode"]["id"], "standard");
        assert_eq!(value["unknown"], "\0");
        let replacement = TurnExecutionSecuritySnapshot::unrestricted_full_access(
            "/workspace/owner-admission",
            1_700,
        );
        let transaction = db.begin().await?;
        assert!(
            set_turn_execution_envelope(
                &transaction,
                "turn",
                &replacement,
                "{ \"replacement\": true }",
            )
            .await?
        );
        let stored = find_turn_execution_security_snapshot(&transaction, "turn")
            .await?
            .context("missing replacement security snapshot")?;
        assert_eq!(stored.snapshot, replacement);
        assert_eq!(stored.version, i64::from(replacement.version));
        assert_eq!(stored.turn_id, "turn");
        transaction.rollback().await?;
        let persisted_authorization = turn::Entity::find_by_id("turn")
            .select_only()
            .column_as(authorization_context_as_text(), "authorization")
            .into_tuple::<String>()
            .one(&other)
            .await?
            .context("missing original authorization after rollback")?;
        assert_eq!(persisted_authorization.as_bytes(), authorization.as_bytes());
        assert_eq!(
            turn::Entity::find_by_id("turn")
                .one(&other)
                .await?
                .context("missing original turn after rollback")?
                .execution_security_snapshot_json
                .as_deref(),
            Some(snapshot.as_str()),
        );
        other.close().await?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn turn_execution_security_snapshot_missing_old_row_returns_none() {
    with_database(async |fixture| {
        let db = fixture.connection();
        let turn = "turn";
        seed_envelope_turn(db, turn).await?;

        let record = find_turn_execution_security_snapshot(db, turn)
            .await
            .expect("security snapshot read should succeed");
        assert_eq!(record, None);

        let persisted_turn = pioneer_entity::turn::Entity::find_by_id(turn)
            .one(db)
            .await
            .expect("turn query should succeed")
            .expect("turn should exist");
        assert_eq!(persisted_turn.execution_security_snapshot_version, None);
        assert_eq!(persisted_turn.execution_security_snapshot_json, None);

        let snapshot =
            TurnExecutionSecuritySnapshot::unrestricted_full_access("/workspace/security", 1);
        let updated = set_turn_execution_security_snapshot(db, "missing_turn", &snapshot)
            .await
            .expect("missing turn update should not fail");
        assert!(!updated, "missing turn should not be silently inserted");
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn turn_execution_security_snapshot_roundtrips_restricted_snapshot() {
    with_database(async |fixture| {
        let db = fixture.connection();
        let turn = "turn";
        seed_envelope_turn(db, turn).await?;
        let permission_profile = TurnPermissionProfileSnapshot::from_mode(
            TurnPermissionMode::AutoAcceptEdits,
            TurnPermissionProfileSource::Composer,
        );
        let snapshot = TurnExecutionSecuritySnapshot::workspace_write(
            permission_profile.clone(),
            "/workspace/security-restricted",
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Write,
                "/workspace/security-restricted",
            )],
            1_700_000_000_001,
        );

        let updated = set_turn_execution_security_snapshot(db, turn, &snapshot)
            .await
            .expect("security snapshot update should succeed");
        assert!(updated, "existing turn must be updated");

        let record = find_turn_execution_security_snapshot(db, turn)
            .await
            .expect("security snapshot read should succeed")
            .expect("security snapshot should exist");
        assert_eq!(record.version, 1);
        assert_eq!(record.snapshot.permission_profile, permission_profile);
        assert_eq!(
            record.snapshot.sandbox.mode,
            TurnSandboxMode::WorkspaceWrite
        );
        assert_eq!(
            record.snapshot.sandbox.filesystem.entries,
            snapshot.sandbox.filesystem.entries
        );
        assert_eq!(record.snapshot, snapshot);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn security_snapshot_gap_query_finds_turns_missing_execution_snapshot() {
    with_database(async |fixture| {
        let db = fixture.connection();
        let missing_turn = "turn";
        seed_envelope_turn(db, missing_turn).await?;
        let complete_turn = "complete-turn";
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO turn (id, thread_id, status) VALUES ($1, 'thread', 'in_progress')",
            [complete_turn.into()],
        ))
        .await?;
        let snapshot =
            TurnExecutionSecuritySnapshot::unrestricted_full_access("/workspace/security-gap", 1);
        set_turn_execution_security_snapshot(db, complete_turn, &snapshot)
            .await
            .expect("security snapshot update should succeed");

        let missing = pioneer_entity::turn::Entity::find()
            .filter(pioneer_entity::turn::Column::ExecutionSecuritySnapshotJson.is_null())
            .filter(pioneer_entity::turn::Column::Status.eq("in_progress"))
            .all(db)
            .await
            .expect("gap query should run");
        let missing_ids = missing
            .iter()
            .map(|turn| turn.id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(missing_ids, vec![missing_turn]);
        Ok(())
    })
    .await
    .unwrap();
}

async fn seed_envelope_turn(db: &DatabaseConnection, id: &str) -> Result<()> {
    db.execute_unprepared(
        "INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
         INSERT INTO thread(id, workspace_id, preview, mode, model, model_provider, status)
         VALUES ('thread', 'w', '', 'agent', 'model', 'provider', 'active');",
    )
    .await?;
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO turn (id, thread_id, status) VALUES ($1, 'thread', 'in_progress')",
        [id.into()],
    ))
    .await?;
    Ok(())
}
