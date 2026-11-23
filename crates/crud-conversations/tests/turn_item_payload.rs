use anyhow::{Context, Result};
use pioneer_crud_conversations::turn_item::{ItemWrite, upsert};
use pioneer_crud_payloads::BLOCK_BYTES;
use pioneer_crud_payloads::storage::{PreparedPayload, Selection, capture, metadata};
use pioneer_entity::turn_item as item;
use pioneer_postgres_test::with_database;
use sea_orm::{AccessMode, ConnectionTrait, EntityTrait, IsolationLevel, TransactionTrait};

#[tokio::test]
async fn item_upsert_preserves_raw_json_and_releases_the_replaced_version_on_commit() -> Result<()>
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
        let draft = format!(
            r#"{{ "text": "{}\u0000" }}"#,
            "界🙂".repeat(BLOCK_BYTES / 3)
        );
        let first = PreparedPayload::new(&draft)?;
        let second = PreparedPayload::new(r#"{"text":"\u0000"}"#)?;
        let write = |id: &str, status: &str| ItemWrite {
            id: id.into(),
            turn_id: "turn".into(),
            item_id: "item".into(),
            item_type: "agent_message".into(),
            status: Some(status.into()),
            created_at: now,
            updated_at: now + chrono::Duration::seconds(1),
        };
        let tx = db.begin().await?;
        upsert(&tx, write("row", "in_progress"), &first).await?;
        tx.commit().await?;
        let read = db
            .begin_with_config(
                Some(IsolationLevel::RepeatableRead),
                Some(AccessMode::ReadOnly),
            )
            .await?;
        let row = item::Entity::find_by_id("row")
            .one(&read)
            .await?
            .context("missing item")?;
        assert_eq!(row.payload, None);
        assert_eq!(row.payload_payload_id.as_deref(), Some(first.id()));

        let tx = db.begin().await?;
        upsert(&tx, write("ignored", "completed"), &second).await?;
        assert!(metadata(&tx, first.id()).await?.is_none());
        assert!(metadata(db, first.id()).await?.is_some());
        tx.rollback().await?;
        assert!(metadata(db, first.id()).await?.is_some());
        assert!(metadata(db, second.id()).await?.is_none());

        let tx = db.begin().await?;
        upsert(&tx, write("ignored", "completed"), &second).await?;
        tx.commit().await?;
        assert!(metadata(db, first.id()).await?.is_none());
        let earlier = capture(&read, first.id(), Selection::Full)
            .await?
            .context("old snapshot lost item")?;
        read.commit().await?;
        assert_eq!(earlier.into_text()?.as_bytes(), draft.as_bytes());
        let row = item::Entity::find_by_id("row")
            .one(db)
            .await?
            .context("item changed identity")?;
        assert_eq!(row.status.as_deref(), Some("completed"));
        assert_eq!(row.payload_payload_id.as_deref(), Some(second.id()));
        assert!(row.portable_revision > 0);

        let tx = db.begin().await?;
        item::Entity::delete_by_id("row").exec(&tx).await?;
        tx.commit().await?;
        assert!(metadata(db, second.id()).await?.is_none());
        Ok(())
    })
    .await
}
