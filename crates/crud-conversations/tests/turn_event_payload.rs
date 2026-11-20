use anyhow::{Context, Result};
use pioneer_crud_conversations::turn_event::{NewEvent, insert, lock_stream, next_sequence};
use pioneer_crud_payloads::BLOCK_BYTES;
use pioneer_crud_payloads::storage::{PreparedPayload, Selection, capture, metadata};
use pioneer_entity::{compaction_event_revision as event_revision, turn_event as event};
use pioneer_postgres_test::with_database;
use sea_orm::{AccessMode, ConnectionTrait, EntityTrait, IsolationLevel, TransactionTrait};

#[tokio::test]
async fn canonical_event_append_and_delete_publish_the_payload_and_revision_together() -> Result<()>
{
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            r#"
            INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
            INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                VALUES ('thread', 'w', '', 'chat', 'model', 'provider', 'active');
            INSERT INTO turn (id, thread_id, status) VALUES ('turn', 'thread', 'in_progress');
        "#,
        )
        .await?;
        let now = chrono::Utc::now().fixed_offset();
        let json = format!(
            r#"{{
  "data": "{}\u0000"
}}"#,
            "🙂".repeat(BLOCK_BYTES / 2)
        );
        let prepared = PreparedPayload::new(&json)?;
        let tx = db.begin().await?;
        lock_stream(&tx, "turn").await?;
        let sequence = next_sequence(&tx, "turn").await?;
        assert_eq!(sequence, 1);
        insert(
            &tx,
            NewEvent {
                id: "event",
                thread_id: "thread",
                turn_id: "turn",
                sequence,
                event_type: "test/probe",
                idempotency_key: "key",
                created_at: now,
            },
            &prepared,
        )
        .await?;
        assert!(event::Entity::find_by_id("event").one(db).await?.is_none());
        tx.commit().await?;

        let reader = db
            .begin_with_config(
                Some(IsolationLevel::RepeatableRead),
                Some(AccessMode::ReadOnly),
            )
            .await?;
        let row = event::Entity::find_by_id("event")
            .one(&reader)
            .await?
            .context("missing event")?;
        assert_eq!(row.payload, None);
        assert_eq!(row.payload_payload_id.as_deref(), Some(prepared.id()));
        assert!(!row.portable_identity.is_empty());
        let revision = event_revision::Entity::find_by_id("event")
            .one(&reader)
            .await?
            .context("missing event revision")?;
        assert_eq!(revision.present, 1);

        let writer = db.begin().await?;
        assert_eq!(
            event::Entity::delete_by_id("event")
                .exec(&writer)
                .await?
                .rows_affected,
            1
        );
        writer.commit().await?;
        assert!(metadata(db, prepared.id()).await?.is_none());
        let body = capture(&reader, prepared.id(), Selection::Full)
            .await?
            .context("snapshot lost committed event")?;
        reader.commit().await?;
        assert_eq!(body.into_text()?.as_bytes(), json.as_bytes());
        Ok(())
    })
    .await
}
