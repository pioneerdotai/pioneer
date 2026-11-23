use anyhow::Result;
use pioneer_crud_conversations::turn_reads::find_terminal_turns_for_thread;
use pioneer_postgres_test::with_database;
use sea_orm::ConnectionTrait;

#[tokio::test]
async fn terminal_history_keeps_the_cutoff_timestamp_bucket_and_its_byte_order() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            r#"
            INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
            INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                VALUES ('thread', 'w', '', 'chat', 'model', 'provider', 'active'),
                       ('other', 'w', '', 'chat', 'model', 'provider', 'active');
            INSERT INTO turn (id, thread_id, status, created_at) VALUES
                ('older', 'thread', 'failed', '2026-01-01T00:00:00.000001Z'),
                ('z', 'thread', 'completed', '2026-01-01T00:00:00.000002Z'),
                ('a', 'thread', 'failed', '2026-01-01T00:00:00.000002Z'),
                ('A', 'thread', 'interrupted', '2026-01-01T00:00:00.000002Z'),
                ('latest', 'thread', 'completed', '2026-01-01T00:00:00.000003Z'),
                ('active', 'thread', 'in_progress', '2026-01-01T00:00:00.000004Z'),
                ('foreign', 'other', 'completed', '2026-01-02T00:00:00.000004Z');
            "#,
        )
        .await?;
        for (limit, expected) in [
            (0, vec![]),
            (1, vec!["latest"]),
            (2, vec!["A", "a", "z", "latest"]),
            (4, vec!["A", "a", "z", "latest"]),
            (5, vec!["older", "A", "a", "z", "latest"]),
            (100, vec!["older", "A", "a", "z", "latest"]),
        ] {
            let rows = find_terminal_turns_for_thread(db, "thread", limit).await?;
            assert_eq!(
                rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
                expected
            );
        }
        assert!(
            find_terminal_turns_for_thread(db, "missing", 10)
                .await?
                .is_empty()
        );
        Ok(())
    })
    .await
}
