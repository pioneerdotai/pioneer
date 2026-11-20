use anyhow::Result;
use pioneer_crud_execution::outbox_discovery::{discover, next_deadline};
use pioneer_postgres_test::with_database;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ConnectionTrait, DbBackend, Statement};

#[tokio::test]
async fn exhausted_start_discovery_and_deadline_share_the_canonical_action_filter() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared("
            INSERT INTO workspace (id, name) VALUES ('workspace', 'Workspace');
            INSERT INTO agent_identity (id, workspace_id, source_kind, source_id, source_fingerprint)
                VALUES ('Identity0000000000000', 'workspace', 'ephemeral', 'identity', 'fingerprint');
            INSERT INTO agent_execution (
                id, workspace_id, agent_identity_id, identity_source_revision, identity_source_fingerprint,
                home_root_thread_id, work_graph_root_execution_id, requested_identity_selection_json,
                requested_profile_selection_json, authorization_context_fingerprint
            ) VALUES (
                'Execution000000000000', 'workspace', 'Identity0000000000000', 1, 'fingerprint',
                'thread', 'Execution000000000000', '{}', '{}', 'authorization'
            );
        ").await?;

        for (id, action, payload, due) in [
            ("missing-kind", "start_agent", r#"{"nested":{"text":"\u0000"}}"#, None),
            ("future-start", "start_agent", r#"{"kind":"start_agent","input":{"text":"\u0000"}}"#, Some("2026-11-19T10:00:05.100Z")),
            ("wrong-action", "stop_agent", r#"{"kind":"start_agent"}"#, Some("2026-11-19T10:00:02Z")),
            ("wrong-kind", "start_agent", r#"{"kind":"stop_agent"}"#, Some("2026-11-19T10:00:01Z")),
        ] {
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "INSERT INTO agent_action (id, execution_id, action_kind, idempotency_key, request_fingerprint) VALUES ($1, 'Execution000000000000', $2, $1, 'fingerprint')",
                [id.into(), action.into()],
            )).await?;
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "INSERT INTO agent_action_outbox (id, action_id, owner_execution_id, payload_json, status, attempts, next_attempt_at) VALUES ($1, $1, 'Execution000000000000', $2, 'failed', 8, $3::text::timestamptz)",
                [id.into(), payload.into(), due.into()],
            )).await?;
        }

        let now = DateTimeWithTimeZone::parse_from_rfc3339("2026-11-19T10:00:00Z")?;
        assert_eq!(discover(db, now, 8).await?, ["missing-kind"]);
        assert_eq!(next_deadline(db, now).await?, Some(now.timestamp() + 6));
        let later = DateTimeWithTimeZone::parse_from_rfc3339("2026-11-19T10:00:06Z")?;
        assert_eq!(discover(db, later, 8).await?, ["missing-kind", "future-start"]);
        assert_eq!(next_deadline(db, later).await?, None);
        Ok(())
    }).await
}
