use anyhow::{Context, Result};
use pioneer_crud_conversations::saved_history::{anchor, page, page_direction, revision, snapshot};
use pioneer_crud_conversations::turn_item::{ItemWrite, upsert};
use pioneer_crud_payloads::storage::{PreparedPayload, Selection, capture};
use pioneer_entity::{turn_input, turn_item};
use pioneer_postgres_test::with_database;
use pioneer_protocol::{AgentMessagePhase, TurnItem, UserInput};
use sea_orm::{
    AccessMode, ConnectionTrait, DatabaseConnection, EntityTrait, IsolationLevel, Set,
    TransactionTrait,
};
use std::time::Duration;

#[tokio::test]
async fn tuple_cursor_pins_equal_timestamp_sources_and_detects_logical_edits() -> Result<()> {
    with_database(async |database| {
        let reader = database.connection();
        reader.execute_unprepared(
            "INSERT INTO workspace (id, name) VALUES ('workspace', 'Workspace');
             INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
             VALUES ('thread', 'workspace', '', 'agent', 'model', 'provider', 'active');
             INSERT INTO turn (id, thread_id, status, created_at)
             VALUES ('turn-A', 'thread', 'completed', '2026-11-19 15:00:00.123456Z'),
                    ('turn-a', 'thread', 'completed', '2026-11-19 15:00:00.123456Z');
             INSERT INTO turn_input (id, turn_id, input_index, input_type, created_at)
             VALUES ('input', 'turn-A', 0, 'text', '2026-11-19 15:00:00.123456Z');
             INSERT INTO turn_item (id, turn_id, item_id, item_type, payload, created_at)
             VALUES ('item', 'turn-a', 'saved-item', 'message', '{}', '2026-11-19 15:00:00.123456Z');"
        ).await?;

        let frozen_read = reader.begin_with_config(
            Some(IsolationLevel::RepeatableRead), Some(AccessMode::ReadOnly),
        ).await?;
        let frontier = snapshot(&frozen_read, "thread").await?;
        let first = page(&frozen_read, "thread", None, None, 2, &frontier).await?;
        assert_eq!(first.iter().map(|row| row.key.id.as_str()).collect::<Vec<_>>(), ["turn-a", "turn-A"]);
        let newest = &first[0].key;
        assert_eq!(newest.created_at, "2026-11-19T15:00:00.123456Z");
        let at_start = revision(&frozen_read, "thread", newest, &frontier).await?;

        let writer = database.connect().await?;
        writer.execute_unprepared(
            "INSERT INTO turn (id, thread_id, status, created_at)
             VALUES ('turn-0', 'thread', 'completed', '2026-11-19 15:00:00.123456Z');
             INSERT INTO turn_input (id, turn_id, input_index, input_type, created_at)
             VALUES ('after-snapshot', 'turn-A', 1, 'text', '2026-11-19 15:00:00.123456Z');"
        ).await?;
        let second = page(&frozen_read, "thread", Some(newest), Some(&first[1].key), 2, &frontier).await?;
        assert_eq!(second.iter().map(|row| row.key.id.as_str()).collect::<Vec<_>>(), ["item", "input"]);
        let reverse = page_direction(&frozen_read, "thread", Some(newest), Some(&second[0].key), false, true, 2, &frontier).await?;
        assert_eq!(reverse.iter().map(|row| row.key.id.as_str()).collect::<Vec<_>>(), ["turn-A", "turn-a"]);
        let exact = anchor(&frozen_read, "thread", "turn-a", Some("saved-item"), &frontier)
            .await?.context("canonical anchor is missing")?;
        assert_eq!(exact.key, second[0].key);
        assert!(anchor(&frozen_read, "thread", "turn-A", Some("saved-item"), &frontier).await?.is_none());
        frozen_read.commit().await?;
        assert_eq!(revision(reader, "thread", newest, &frontier).await?, at_start);

        writer.execute_unprepared(
            "UPDATE turn SET message_revision=message_revision+1, message_deleted_at=CURRENT_TIMESTAMP WHERE id='turn-a'"
        ).await?;
        assert_ne!(revision(reader, "thread", newest, &frontier).await?, at_start);
        assert!(anchor(reader, "thread", "turn-a", Some("saved-item"), &frontier).await?.is_none());
        writer.close().await?;
        Ok(())
    }).await
}

#[tokio::test]
async fn resumed_history_excludes_an_earlier_insert_that_committed_after_capture() -> Result<()> {
    with_database(async |database| {
        let reader = database.connection();
        reader
            .execute_unprepared(
                "INSERT INTO workspace (id, name) VALUES ('workspace', 'Workspace');
                 INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
                 VALUES ('thread', 'workspace', '', 'agent', 'model', 'provider', 'active');
                 INSERT INTO turn (id, thread_id, status, created_at)
                 VALUES ('turn', 'thread', 'completed', '2026-11-19 15:00:00Z');",
            )
            .await?;

        let earlier_writer = database.connect().await?;
        let earlier = earlier_writer.begin().await?;
        earlier
            .execute_unprepared(
                "INSERT INTO turn_item (id, turn_id, item_id, item_type, payload, created_at)
                 VALUES ('item-a', 'turn', 'saved-a', 'message', '{}', '2026-11-19 15:00:00Z')",
            )
            .await?;
        let later_writer = database.connect().await?;
        tokio::time::timeout(
            Duration::from_secs(5),
            later_writer.execute_unprepared(
                "INSERT INTO turn_item (id, turn_id, item_id, item_type, payload, created_at)
                 VALUES ('item-b', 'turn', 'saved-b', 'message', '{}', '2026-11-19 15:00:00Z')",
            ),
        )
        .await
        .context("the later writer could not commit independently")??;

        let capture = reader
            .begin_with_config(
                Some(IsolationLevel::RepeatableRead),
                Some(AccessMode::ReadOnly),
            )
            .await?;
        let frontier = snapshot(&capture, "thread").await?;
        let first = page(&capture, "thread", None, None, 2, &frontier).await?;
        assert_eq!(
            first
                .iter()
                .map(|row| row.key.id.as_str())
                .collect::<Vec<_>>(),
            ["turn", "item-b"],
        );
        let saved_revision = revision(&capture, "thread", &first[0].key, &frontier).await?;
        capture.commit().await?;
        earlier.commit().await?;

        let resumed = page(
            reader,
            "thread",
            Some(&first[0].key),
            Some(&first[1].key),
            2,
            &frontier,
        )
        .await?;
        assert!(resumed.is_empty());
        assert!(
            anchor(reader, "thread", "turn", Some("saved-a"), &frontier)
                .await?
                .is_none()
        );
        assert_eq!(
            revision(reader, "thread", &first[0].key, &frontier).await?,
            saved_revision
        );

        let fresh = snapshot(reader, "thread").await?;
        let rows = page(reader, "thread", None, None, 3, &fresh).await?;
        assert_eq!(
            rows.iter()
                .map(|row| row.key.id.as_str())
                .collect::<Vec<_>>(),
            ["turn", "item-b", "item-a"],
        );
        assert!(
            anchor(reader, "thread", "turn", Some("saved-a"), &fresh)
                .await?
                .is_some()
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn saved_payload_history_plain_storage() -> Result<()> {
    saved_payload_history(false).await
}

#[tokio::test]
async fn saved_payload_history_reads_compressed_items_and_tracks_edits_and_deletions() -> Result<()>
{
    saved_payload_history(true).await
}

fn message(item_id: &str, text: &str) -> TurnItem {
    TurnItem::AgentMessage {
        id: item_id.into(),
        text: text.into(),
        phase: AgentMessagePhase::FinalAnswer,
        markdown: None,
        markdown_version: None,
    }
}

async fn save_item(
    db: &DatabaseConnection,
    item_id: &str,
    text: &str,
    compressed: bool,
) -> Result<()> {
    let at = "2026-01-01T00:00:00Z".parse()?;
    let id = format!("row-{item_id}");
    let item = message(item_id, text);
    if compressed {
        let body = PreparedPayload::new(&serde_json::to_string(&item)?)?;
        let tx = db.begin().await?;
        upsert(
            &tx,
            ItemWrite {
                id,
                turn_id: "turn".into(),
                item_id: item_id.into(),
                item_type: "agent_message".into(),
                status: Some("completed".into()),
                created_at: at,
                updated_at: at,
            },
            &body,
        )
        .await?;
        tx.commit().await?;
    } else {
        turn_item::Entity::insert(turn_item::ActiveModel {
            id: Set(id),
            turn_id: Set("turn".into()),
            item_id: Set(item_id.into()),
            item_type: Set("agent_message".into()),
            payload: Set(Some(serde_json::to_value(item)?)),
            created_at: Set(at),
            updated_at: Set(at),
            ..Default::default()
        })
        .exec_without_returning(db)
        .await?;
    }
    Ok(())
}

async fn saved_payload_history(compressed: bool) -> Result<()> {
    with_database(async |database| {
        let db = database.connection();
        let at = "2026-01-01T00:00:00Z".parse()?;
        db.execute_unprepared(
            "INSERT INTO workspace (id, name) VALUES ('workspace', 'Workspace');
             INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
               VALUES ('thread', 'workspace', '', 'agent', 'model', 'provider', 'active');
             INSERT INTO turn (id, thread_id, status, created_at)
               VALUES ('turn', 'thread', 'completed', '2026-01-01 00:00:00Z')",
        )
        .await?;
        let input_text = "Вход 🦀 日本語";
        let input = UserInput::Text {
            text: input_text.into(),
            text_elements: vec![],
        };
        let save_input = async |id: &str, index: i64| -> Result<()> {
            turn_input::Entity::insert(turn_input::ActiveModel {
                id: Set(id.into()),
                turn_id: Set("turn".into()),
                input_index: Set(index),
                input_type: Set("text".into()),
                text: Set(Some(input_text.into())),
                payload: Set(serde_json::to_string(&input)?),
                created_at: Set(at),
                ..Default::default()
            })
            .exec_without_returning(db)
            .await?;
            Ok(())
        };
        let answer = "Ответ 😀 中文".repeat(if compressed { 5000 } else { 1 });
        for (index, suffix) in ["a", "b", "c"].into_iter().enumerate() {
            save_input(&format!("input-{suffix}"), i64::try_from(index)?).await?;
            save_item(db, &format!("item-{suffix}"), &answer, compressed).await?;
        }

        let frontier = snapshot(db, "thread").await?;
        let mut rows = page(db, "thread", None, None, 2, &frontier).await?;
        let boundary = rows[0].key.clone();
        let original_revision = revision(db, "thread", &boundary, &frontier).await?;
        let stored_input = turn_input::Entity::find_by_id("input-a")
            .one(db)
            .await?
            .context("missing input")?;
        assert_eq!(serde_json::from_str::<UserInput>(&stored_input.payload)?, input);
        let stored_item = turn_item::Entity::find_by_id("row-item-a")
            .one(db)
            .await?
            .context("missing item")?;
        let decoded: TurnItem = if compressed {
            let body_id = stored_item.payload_payload_id.context("missing portable value")?;
            let stored = db.query_one_raw(sea_orm::Statement::from_sql_and_values(
                sea_orm::DbBackend::Postgres,
                "SELECT count(*) AS blocks FROM portable_payload_part WHERE payload_id=$1 AND codec=1",
                [body_id.clone().into()],
            )).await?.context("missing block count")?;
            assert!(stored.try_get::<i64>("", "blocks")? > 1);
            let body = capture(db, &body_id, Selection::Full).await?.context("missing item body")?;
            serde_json::from_slice(&body.into_bytes()?)?
        } else {
            assert_eq!(stored_item.payload_payload_id, None);
            serde_json::from_value(stored_item.payload.context("missing inline JSON")?)?
        };
        assert!(matches!(decoded, TurnItem::AgentMessage { text, .. } if text == answer));
        let reread = snapshot(db, "thread").await?;
        assert_eq!(
            (reread.turns, reread.inputs, reread.items, reread.children),
            (frontier.turns, frontier.inputs, frontier.items, frontier.children),
        );
        assert_eq!(revision(db, "thread", &boundary, &frontier).await?, original_revision);

        let exact = anchor(db, "thread", "turn", Some("item-a"), &frontier)
            .await?
            .context("missing item anchor")?;
        let inclusive = page_direction(db, "thread", Some(&boundary), Some(&exact.key),
            true, false, 1, &frontier).await?;
        assert_eq!(inclusive[0].item_id.as_deref(), Some("item-a"));
        let newer = page_direction(db, "thread", Some(&boundary), Some(&exact.key),
            false, true, 100, &frontier).await?;
        assert!(!newer.iter().any(|row| row.key == exact.key));
        assert!(anchor(db, "unrelated", "turn", Some("item-a"), &frontier).await?.is_none());

        save_input("input-aa", 9).await?;
        save_item(db, "item-aa", &answer, compressed).await?;
        loop {
            let next = page(db, "thread", Some(&boundary), Some(&rows.last().unwrap().key),
                2, &frontier).await?;
            if next.is_empty() {
                break;
            }
            rows.extend(next);
        }
        assert_eq!(rows.len(), 7);
        assert_eq!(
            rows.iter().filter(|row| row.key.kind == "input")
                .map(|row| row.key.id.as_str()).collect::<Vec<_>>(),
            ["input-c", "input-b", "input-a"],
        );
        assert_eq!(
            rows.iter().filter_map(|row| row.item_id.as_deref()).collect::<Vec<_>>(),
            ["item-c", "item-b", "item-a"],
        );
        assert!(rows.windows(2).all(|pair| {
            let [a, b] = [&pair[0].key, &pair[1].key];
            (&a.created_at, &a.kind, &a.id) > (&b.created_at, &b.kind, &b.id)
        }));
        assert_eq!(revision(db, "thread", &boundary, &frontier).await?, original_revision);

        let updated_input = UserInput::Text {
            text: "Изменено 🦀".into(),
            text_elements: vec![],
        };
        turn_input::Entity::update(turn_input::ActiveModel {
            id: Set("input-a".into()),
            payload: Set(serde_json::to_string(&updated_input)?),
            ..Default::default()
        }).exec(db).await?;
        let edited_input = revision(db, "thread", &boundary, &frontier).await?;
        assert_ne!(edited_input, original_revision);
        turn_input::Entity::delete_by_id("input-a").exec(db).await?;
        let deleted_input = revision(db, "thread", &boundary, &frontier).await?;
        assert_ne!(deleted_input, edited_input);

        if compressed {
            save_item(db, "item-a", "Изменено 🦀", true).await?;
        } else {
            turn_item::Entity::update(turn_item::ActiveModel {
                id: Set("row-item-a".into()),
                payload: Set(Some(serde_json::to_value(message("item-a", "Изменено 🦀"))?)),
                ..Default::default()
            }).exec(db).await?;
        }
        let edited_item = revision(db, "thread", &boundary, &frontier).await?;
        assert_ne!(edited_item, deleted_input);
        turn_item::Entity::delete_by_id("row-item-a").exec(db).await?;
        assert_ne!(revision(db, "thread", &boundary, &frontier).await?, edited_item);
        let remaining = page(db, "thread", Some(&boundary), None, 100, &frontier).await?;
        assert!(!remaining.iter().any(|row| row.key.id == "input-a"
            || row.item_id.as_deref() == Some("item-a")));
        Ok(())
    }).await
}
