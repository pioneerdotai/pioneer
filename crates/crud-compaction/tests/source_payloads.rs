use anyhow::Result;
use pioneer_crud_compaction::source_payloads;
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, TransactionTrait};
use serde_json::json;

#[tokio::test]
async fn input_and_provider_context_batches_preserve_order_exact_bytes_and_revision() -> Result<()>
{
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            r#"INSERT INTO workspace (id, name) VALUES ('workspace', 'Workspace');
               INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
               VALUES ('thread', 'workspace', '', 'agent', 'model', 'provider', 'active');
               INSERT INTO turn (id, thread_id, status) VALUES ('turn', 'thread', 'completed');
               INSERT INTO turn_input (id, turn_id, input_index, input_type, payload)
               VALUES ('input', 'turn', 0, 'text', '{ "text": "界" }');
               INSERT INTO turn_llm_context (id, turn_id, sequence, source, payload)
               VALUES ('context', 'turn', 1, 'assistant_round', '{ "large": "🧪" }');"#,
        )
        .await?;
        let references = json!([
            {"scope": "context:turn", "id": "context", "version": "revision:1"},
            {"scope": "input:turn", "id": "input", "version": "input-revision:1"},
            {"scope": "context:turn", "id": "context", "version": "revision:1"},
        ])
        .to_string();
        let original_context = r#"{ "large": "🧪" }"#;
        let original_input = r#"{ "text": "界" }"#;

        let read = db
            .begin_with_config(
                Some(sea_orm::IsolationLevel::RepeatableRead),
                Some(sea_orm::AccessMode::ReadOnly),
            )
            .await?;
        let lengths = source_payloads::sizes(&read, "workspace", "thread", &references, 3).await?;
        assert_eq!(
            lengths
                .iter()
                .map(|row| (row.ordinal, row.bytes))
                .collect::<Vec<_>>(),
            [
                (0, i64::try_from(original_context.len())?),
                (1, i64::try_from(original_input.len())?),
                (2, i64::try_from(original_context.len())?)
            ],
        );
        let writer = fixture.connect().await?;
        writer
            .execute_unprepared(
                r#"UPDATE turn_llm_context SET payload = '{"new":1}' WHERE id = 'context'"#,
            )
            .await?;
        let saved = source_payloads::capture(&read, "workspace", "thread", &references, 2).await?;
        read.commit().await?;
        let bodies = saved
            .into_iter()
            .map(|(ordinal, body)| Ok((ordinal, body.into_text()?)))
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(
            bodies,
            [
                (0, original_context.to_owned()),
                (1, original_input.to_owned())
            ]
        );
        let current = source_payloads::capture(db, "workspace", "thread", &references, 3).await?;
        assert_eq!(current.iter().map(|row| row.0).collect::<Vec<_>>(), [1]);
        assert!(
            source_payloads::capture(db, "other", "thread", &references, 3)
                .await?
                .is_empty()
        );
        writer.close().await?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn event_and_item_capture_keep_exact_block_bytes_after_concurrent_replacement() -> Result<()>
{
    use pioneer_crud_payloads::storage::PreparedPayload;
    use sea_orm::{DbBackend, Statement};

    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            "INSERT INTO workspace(id,name) VALUES ('workspace','Workspace');
             INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
             VALUES ('thread','workspace','','agent','model','provider','active');
             INSERT INTO turn(id,thread_id,status) VALUES ('turn','thread','completed');",
        )
        .await?;
        let event_wire = format!(r#"{{ "text": "{}" }}"#, "界🧪".repeat(11_000));
        let item_wire = r#"{ "text": "literal \u0000" }"#;
        let event = PreparedPayload::new(&event_wire)?;
        let item = PreparedPayload::new_turn_item(item_wire)?;
        let insert = db.begin().await?;
        event.insert(&insert).await?;
        item.insert(&insert).await?;
        insert
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload_payload_id)
                 VALUES ('event','thread','turn',1,'item/completed',$1)",
                [event.id().into()],
            ))
            .await?;
        insert
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload_payload_id)
                 VALUES ('row','turn','item','agent_message','completed',$1)",
                [item.id().into()],
            ))
            .await?;
        insert.commit().await?;

        let references = json!([
            {"scope":"event:turn", "id":"event", "version":"event-revision:1"},
            {"scope":"item:turn", "id":"row", "version":"item-revision:1"}
        ])
        .to_string();
        let snapshot = db
            .begin_with_config(
                Some(sea_orm::IsolationLevel::RepeatableRead),
                Some(sea_orm::AccessMode::ReadOnly),
            )
            .await?;
        let sizes = source_payloads::sizes(&snapshot, "workspace", "thread", &references, 2).await?;
        assert_eq!(
            sizes.iter().map(|size| (size.ordinal, size.bytes)).collect::<Vec<_>>(),
            [(0, event_wire.len() as i64), (1, item_wire.len() as i64)]
        );

        let writer = fixture.connect().await?;
        writer.execute_unprepared(
            "UPDATE turn_event SET payload_payload_id=NULL,payload='{\"replacement\":true}'::json WHERE id='event';",
        ).await?;
        let first = source_payloads::capture(&snapshot, "workspace", "thread", &references, 1).await?;
        assert_eq!(first.len(), 1);
        let both = source_payloads::capture(&snapshot, "workspace", "thread", &references, 2).await?;
        snapshot.commit().await?;
        let bodies = both.into_iter()
            .map(|(ordinal, body)| Ok((ordinal, body.into_text()?)))
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(bodies, [(0, event_wire), (1, item_wire.to_owned())]);
        let current = source_payloads::capture(&writer, "workspace", "thread", &references, 2).await?;
        assert_eq!(current.into_iter().map(|(ordinal, _)| ordinal).collect::<Vec<_>>(), [1]);
        writer.close().await?;
        Ok(())
    })
    .await
}
