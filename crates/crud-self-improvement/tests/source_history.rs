use anyhow::{Context, Result};
use chrono::DateTime;
use pioneer_crud_self_improvement::source_history::{
    prepare_source_turn_check, processed_history_date,
};
use pioneer_crud_self_improvement::source_turn::{
    list_after_cursor, list_frozen_range, recent_processed_sources,
};
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, TransactionTrait};

#[tokio::test]
async fn finalization_requires_a_new_visible_anchor_and_keeps_the_original_history_date()
-> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            r#"
            INSERT INTO workspace (id, name) VALUES ('w', 'Workspace'), ('other', 'Other');
            INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                VALUES ('thread', 'w', '', 'agent', 'model', 'provider', 'active'),
                       ('other-thread', 'other', '', 'agent', 'model', 'provider', 'active');
            INSERT INTO "turn" (id, thread_id, status, created_at)
                VALUES ('old', 'thread', 'completed', '2026-11-15T23:50:00.123+03:00'),
                       ('new', 'thread', 'completed', '2026-11-14T19:00:00Z'),
                       ('foreign', 'other-thread', 'completed', '2026-11-19T19:00:00Z');
            INSERT INTO self_improvement_source_turn
                (id, workspace_id, thread_id, turn_id, terminal_event_id, terminal_at)
                VALUES (1, 'w', 'thread', 'old', 'event-old', '2026-11-16T13:00:00Z'),
                       (2, 'w', 'thread', 'new', 'event-new', '2026-11-17T13:00:00Z'),
                       (3, 'w', 'other-thread', 'foreign', 'event-foreign', '2026-11-19T20:00:00Z');
            INSERT INTO self_improvement_run (
                id, workspace_id, activation_epoch, scheduled_date_utc, source_lower_exclusive,
                source_upper_inclusive, status, learner_provider, learner_model, reviewer_provider,
                reviewer_model, pipeline_contract_version
            ) VALUES ('past', 'w', 1, '2026-11-16', 0, 1, 'completed', 'p', 'l', 'p', 'r', 'v1');
            "#,
        )
        .await?;

        let check = |ids: &[&str]| {
            let ids = ids.iter().map(|id| (*id).to_owned()).collect::<Vec<_>>();
            prepare_source_turn_check("w", &ids, Some((0, 3)))?.context("missing source check")
        };
        assert!(!check(&["old"])?.matches(db).await?);
        assert!(check(&["old", "new"])?.matches(db).await?);
        assert!(!check(&["new", "foreign"])?.matches(db).await?);
        let expected_date = DateTime::parse_from_rfc3339("2026-11-15T20:50:00Z")?.timestamp();
        assert_eq!(processed_history_date(db, "w").await?, Some(expected_date));
        assert_eq!(processed_history_date(db, "other").await?, None);

        let anchors = list_after_cursor(db, "w", 0, 0, 2).await?;
        assert_eq!(anchors.len(), 1);
        assert_eq!(anchors[0].turn_id, "new");
        assert_eq!(anchors[0].terminal_event_id, "event-new");
        assert_eq!(list_frozen_range(db, "w", 0, 2, 0).await?, anchors);
        assert!(list_after_cursor(db, "w", 2, 0, 2).await?.is_empty());
        let processed = recent_processed_sources(db, "w").await?;
        assert_eq!(processed.len(), 1);
        assert_eq!(processed[0].turn_id, "old");
        assert_eq!(processed[0].parent_turn_created_at_unix, expected_date);

        let writer = fixture.connect().await?;
        let tx = writer.begin().await?;
        tx.execute_unprepared("UPDATE self_improvement_run SET source_upper_inclusive = 2")
            .await?;
        assert!(!check(&["old", "new"])?.matches(&tx).await?);
        assert!(check(&["old", "new"])?.matches(db).await?);
        tx.commit().await?;
        assert!(!check(&["old", "new"])?.matches(db).await?);
        assert_eq!(processed_history_date(db, "w").await?, Some(expected_date));
        writer.close().await?;
        Ok(())
    })
    .await
}
