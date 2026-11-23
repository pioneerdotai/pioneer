use anyhow::Result;
use pioneer_crud_execution::tool_output::{MAX_CHUNK_BYTES, page, record, record_output};
use pioneer_entity::tool_output_chunk as chunk;
use pioneer_postgres_test::with_database;
use pioneer_protocol::{ItemDeltaNotification, ItemDeltaStream};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter, sea_query::Expr,
};
use sha2::{Digest, Sha256};

async fn seed(db: &impl ConnectionTrait) -> Result<()> {
    db.execute_unprepared(
        "INSERT INTO workspace (id, name) VALUES ('workspace', 'Workspace');
         INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
             VALUES ('thread', 'workspace', '', 'chat', 'model', 'provider', 'active');
         INSERT INTO turn (id, thread_id, status) VALUES ('turn', 'thread', 'running');",
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn tool_output_round_trips_raw_metadata_and_whole_chunk_with_concurrent_retry() -> Result<()>
{
    with_database(async |fixture| {
        let db = fixture.connection();
        seed(db).await?;
        let mut notification = ItemDeltaNotification {
            workspace_id: "workspace".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            item_id: "item".into(),
            delta: "🧪\0".repeat(23_000),
            stream: Some(ItemDeltaStream::Stdout),
            payload: Some(serde_json::json!({"detailed": "🧪".repeat(2000), "nul": "\0"})),
            markdown: None,
            markdown_version: None,
        };
        let other = fixture.connect().await?;
        tokio::try_join!(
            record(db, "same", &notification),
            record(&other, "same", &notification),
        )?;
        let compressed = chunk::Entity::find()
            .filter(chunk::Column::Id.eq("same"))
            .one(db)
            .await?
            .expect("persisted tool output");
        assert_eq!(compressed.text_encoding, 1);
        assert_eq!(
            compressed.text_original_bytes,
            notification.delta.len() as i64
        );
        assert!(compressed.text.len() + 2 < notification.delta.len());
        assert_eq!(compressed.stream.as_deref(), Some("stdout"));
        assert_eq!(
            compressed.metadata,
            notification
                .payload
                .as_ref()
                .map(serde_json::Value::to_string)
        );

        let rows = page(&other, "workspace", "thread", "turn", "item", 0).await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.text, notification.delta);
        notification.delta = "short".into();
        assert!(record(db, "same", &notification).await.is_err());

        notification.stream = Some(ItemDeltaStream::Stderr);
        notification.payload = None;
        record(db, "next", &notification).await?;
        let rows = page(db, "workspace", "thread", "turn", "item", rows[0].0).await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.text, "short");
        assert_eq!(rows[0].1.metadata, None);
        assert_eq!(rows[0].1.stream.as_deref(), Some("stderr"));
        assert!(
            page(db, "elsewhere", "thread", "turn", "item", 0)
                .await?
                .is_empty()
        );

        notification.delta.clear();
        record(db, "empty", &notification).await?;
        let empty = chunk::Entity::find()
            .filter(chunk::Column::Id.eq("empty"))
            .one(db)
            .await?
            .expect("persisted empty tool output");
        let empty_digest = Sha256::digest([]).to_vec();
        assert_eq!(empty.text_encoding, 0);
        assert_eq!(empty.text_sha256, empty_digest);
        for invalid in [vec![0_u8; 31], vec![0_u8; 33], vec![b'0'; 64], Vec::new()] {
            assert!(
                chunk::Entity::update_many()
                    .col_expr(chunk::Column::TextSha256, Expr::val(invalid))
                    .filter(chunk::Column::Id.eq("empty"))
                    .exec(db)
                    .await
                    .is_err()
            );
        }
        chunk::Entity::update_many()
            .col_expr(chunk::Column::TextSha256, Expr::val(vec![0_u8; 32]))
            .filter(chunk::Column::Id.eq("empty"))
            .exec(db)
            .await?;
        assert!(
            page(db, "workspace", "thread", "turn", "item", rows[0].0)
                .await
                .is_err()
        );
        chunk::Entity::update_many()
            .col_expr(chunk::Column::TextSha256, Expr::val(empty_digest))
            .filter(chunk::Column::Id.eq("empty"))
            .exec(db)
            .await?;
        let rows = page(db, "workspace", "thread", "turn", "item", rows[0].0).await?;
        assert_eq!(rows.len(), 1);
        assert!(rows[0].1.text.is_empty());
        assert!(rows[0].1.metadata.is_none());

        db.execute_unprepared("DELETE FROM turn WHERE id = 'turn'")
            .await?;
        assert!(chunk::Entity::find().all(db).await?.is_empty());
        other.close().await?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn large_unicode_output_keeps_immutable_chunks_and_retry_identity() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        seed(db).await?;
        let original = "вывод🦀\n".repeat(30_000);
        let mut notification = ItemDeltaNotification {
            workspace_id: "workspace".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            item_id: "large".into(),
            delta: original.clone(),
            stream: Some(ItemDeltaStream::Stdout),
            payload: Some(serde_json::json!({"truncated":false})),
            markdown: None,
            markdown_version: None,
        };
        record_output(db, "large", &notification).await?;
        let reopened = fixture.connect().await?;
        record_output(&reopened, "large", &notification).await?;
        let rows = page(&reopened, "workspace", "thread", "turn", "large", 0).await?;
        assert!(rows.len() > 1);
        let stored = chunk::Entity::find().filter(chunk::Column::ItemId.eq("large"));
        assert_eq!(stored.clone().count(db).await?, rows.len() as u64);
        assert_eq!(
            stored
                .filter(chunk::Column::TextEncoding.eq(1))
                .count(db)
                .await?,
            rows.len() as u64
        );
        notification.delta.truncate(195_000);
        assert!(
            record_output(&reopened, "large", &notification)
                .await
                .is_err()
        );
        assert!(
            rows.iter()
                .all(|(_, row)| row.text.len() <= MAX_CHUNK_BYTES)
        );
        assert_eq!(
            rows.iter()
                .map(|(_, row)| row.text.as_str())
                .collect::<String>(),
            original
        );
        assert_eq!(
            rows.iter()
                .filter(|(_, row)| row.metadata.is_some())
                .count(),
            1
        );
        assert!(
            page(
                &reopened,
                "workspace",
                "thread",
                "turn",
                "large",
                rows.last().unwrap().0
            )
            .await?
            .is_empty()
        );
        reopened.close().await?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn tool_output_chunks_are_scoped_ordered_and_idempotent() -> Result<()> {
    with_database(async |fixture| {
        let writer = fixture.connection();
        seed(writer).await?;
        let mut n = pioneer_protocol::ItemDeltaNotification {
            workspace_id: "workspace".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            item_id: "command".into(),
            delta: "same text\n".into(),
            stream: Some(pioneer_protocol::ItemDeltaStream::Stdout),
            payload: None,
            markdown: None,
            markdown_version: None,
        };
        record_output(writer, "chunk-a", &n).await.unwrap();
        record_output(writer, "chunk-a", &n).await.unwrap();
        record_output(writer, "chunk-b", &n).await.unwrap();
        n.stream = Some(pioneer_protocol::ItemDeltaStream::Stderr);
        n.delta = "failure detail".into();
        record_output(writer, "chunk-c", &n).await.unwrap();
        assert!(record_output(writer, "chunk-a", &n).await.is_err());
        n.thread_id = "other".into();
        assert!(record_output(writer, "bad", &n).await.is_err());
        let reopened = fixture.connect_read_only().await?;
        let rows = page(&reopened, "workspace", "thread", "turn", "command", 0)
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].1.text, "same text\n");
        assert_eq!(rows[1].1.text, "same text\n");
        assert_eq!(rows[2].1.text, "failure detail");
        assert!(
            page(&reopened, "foreign", "thread", "turn", "command", 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            page(
                &reopened,
                "workspace",
                "thread",
                "turn",
                "command",
                rows[2].0
            )
            .await
            .unwrap()
            .is_empty()
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn tool_output_large_unicode_chunks_survive_retry_without_snapshot_copies() -> Result<()> {
    with_database(async |fixture| {
        let writer = fixture.connection();
        seed(writer).await?;
        let original = "вывод🦀\n".repeat(30000);
        let n = pioneer_protocol::ItemDeltaNotification {
            workspace_id: "workspace".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            item_id: "large".into(),
            delta: original.clone(),
            stream: Some(pioneer_protocol::ItemDeltaStream::Stdout),
            payload: Some(serde_json::json!({"truncated":false})),
            markdown: None,
            markdown_version: None,
        };
        record_output(writer, "large", &n).await.unwrap();
        assert!(
            chunk::Entity::find()
                .filter(chunk::Column::TextEncoding.eq(1_i16))
                .filter(Expr::cust("octet_length(text) < text_original_bytes"))
                .count(writer)
                .await?
                > 0
        );
        record_output(writer, "large", &n).await.unwrap();
        let rows = page(writer, "workspace", "thread", "turn", "large", 0)
            .await
            .unwrap();
        assert!(rows.len() > 1);
        let mut conflicting = n.clone();
        conflicting.delta.truncate(195000);
        assert!(record_output(writer, "large", &conflicting).await.is_err());
        assert!(rows.iter().all(|(_, row)| row.text.len() <= 128 * 1024));
        assert_eq!(
            rows.iter()
                .map(|(_, row)| row.text.as_str())
                .collect::<String>(),
            original
        );
        assert_eq!(
            rows.iter()
                .filter(|(_, row)| row.metadata.is_some())
                .count(),
            1
        );
        assert!(
            page(
                writer,
                "workspace",
                "thread",
                "turn",
                "large",
                rows.last().unwrap().0
            )
            .await
            .unwrap()
            .is_empty()
        );
        Ok(())
    })
    .await
}
