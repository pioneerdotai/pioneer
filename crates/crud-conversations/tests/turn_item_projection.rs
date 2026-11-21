use pioneer_crud_conversations::turn_item::{
    prepare_turn_item_projection, upsert_prepared_turn_item,
};
use pioneer_crud_conversations::turn_payload;
use pioneer_entity::turn_item;
use pioneer_protocol::{AgentMessagePhase, TurnItem};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, DbBackend, EntityTrait, QueryFilter,
    Statement, entity::prelude::DateTimeWithTimeZone,
};

#[tokio::test]
async fn upsert_turn_item_replaces_only_its_portable_payload_version() -> anyhow::Result<()> {
    use sea_orm::TransactionTrait;

    pioneer_postgres_test::with_database(async |fixture| {
        let writer = fixture.connection();
        writer
            .execute_unprepared(
                "INSERT INTO workspace(id, name) VALUES ('ws-item', 'Item fixture');
             INSERT INTO thread(id, workspace_id, preview, mode, model, model_provider, status)
                 VALUES ('thread-item', 'ws-item', '', 'agent', 'fixture', 'fixture', 'active');
             INSERT INTO turn(id, thread_id, status)
                 VALUES ('turn-item', 'thread-item', 'in_progress')",
            )
            .await?;
        let db =
            pioneer_sqlite::SqliteDatabase::new(fixture.connect_read_only().await?, writer.clone());
        let created_at = fixed_test_datetime();
        let row_query = || {
            turn_item::Entity::find()
                .filter(turn_item::Column::TurnId.eq("turn-item"))
                .filter(turn_item::Column::ItemId.eq("item"))
        };

        let first = agent_message_item("item", "first payload");
        let insert = prepare_turn_item_projection(
            "turn-item",
            &first,
            Some("running"),
            created_at,
            created_at,
        )?;
        let tx = writer.begin().await?;
        upsert_prepared_turn_item(&tx, insert).await?;
        tx.commit().await?;
        let inserted = turn_payload::items(&db, row_query())
            .await?
            .pop()
            .expect("inserted item");
        assert_eq!(inserted.status.as_deref(), Some("running"));
        assert_eq!(turn_payload::decode_item(&inserted)?, first);
        let first_locator = inserted
            .payload_payload_id
            .expect("inserted portable value");

        let updated_at = fixed_later_test_datetime();
        let updated = agent_message_item("item", "updated payload");
        let update = prepare_turn_item_projection(
            "turn-item",
            &updated,
            Some("completed"),
            created_at,
            updated_at,
        )?;
        let tx = writer.begin().await?;
        upsert_prepared_turn_item(&tx, update).await?;
        tx.commit().await?;
        let stored = turn_payload::items(&db, row_query())
            .await?
            .pop()
            .expect("updated item");
        assert_eq!(stored.status.as_deref(), Some("completed"));
        assert_eq!(stored.created_at, created_at);
        assert_eq!(stored.updated_at, updated_at);
        assert_eq!(turn_payload::decode_item(&stored)?, updated);

        let backing_rows = query_i64(
            writer,
            "SELECT count(*) AS value FROM portable_payload_value",
        )
        .await;
        assert_eq!(backing_rows, 1);
        assert!(
            pioneer_entity::portable_payload_value::Entity::find_by_id(first_locator)
                .one(writer)
                .await?
                .is_none()
        );
        Ok(())
    })
    .await
}

fn agent_message_item(item_id: &str, marker: &str) -> TurnItem {
    TurnItem::AgentMessage {
        id: item_id.to_owned(),
        text: format!("{marker} {}", "abc123xyz ".repeat(64)),
        phase: AgentMessagePhase::FinalAnswer,
        markdown: None,
        markdown_version: None,
    }
}

fn fixed_test_datetime() -> DateTimeWithTimeZone {
    chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z").expect("valid fixed test date")
}

fn fixed_later_test_datetime() -> DateTimeWithTimeZone {
    chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:01Z").expect("valid fixed test date")
}

async fn query_i64(connection: &DatabaseConnection, sql: &str) -> i64 {
    let row = connection
        .query_one_raw(Statement::from_string(DbBackend::Postgres, sql.to_owned()))
        .await
        .expect("query should execute")
        .expect("query should return row");
    row.try_get::<i64>("", "value")
        .expect("value should decode")
}
