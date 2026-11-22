use pioneer_entity::turn;
use pioneer_protocol::UserInput;
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};

use pioneer_crud_conversations::{
    saved_history::{page, revision, snapshot},
    turn_input,
};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_postgres_test::TestDatabase;
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement, TransactionTrait};

fn unix_to_datetime(value: i64) -> sea_orm::prelude::DateTimeWithTimeZone {
    chrono::DateTime::from_timestamp(value, 0)
        .unwrap()
        .fixed_offset()
}

async fn seed(
    fixture: &TestDatabase,
    workspace: &str,
    thread: &str,
) -> anyhow::Result<PostgresDatabase> {
    let db = PostgresDatabase::new(
        fixture.connect_read_only().await?,
        fixture.connection().clone(),
    );
    db.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "INSERT INTO workspace (id, name) VALUES ($1,'History workspace')",
        [workspace.into()],
    ))
    .await?;
    db.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Postgres,
        "INSERT INTO thread (id,workspace_id,preview,mode,model,model_provider,status) VALUES ($1,$2,'','chat','test','test','active')",
        [thread.into(), workspace.into()])).await?;
    Ok(db)
}

async fn new_turn(db: &PostgresDatabase, thread: &str, id: &str) -> anyhow::Result<()> {
    db.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Postgres,
        "INSERT INTO turn (id,thread_id,turn_kind,origin,status) VALUES ($1,$2,'conversation','user','in_progress')",
        [id.into(), thread.into()])).await?;
    Ok(())
}

async fn change_input(
    db: &PostgresDatabase,
    id: &str,
    input: &[UserInput],
    number: i64,
    at: i64,
) -> anyhow::Result<()> {
    let prepared = turn_input::prepare(id, input, unix_to_datetime(at))?;
    let tx = db.begin().await?;
    turn_input::replace(&tx, prepared).await?;
    let affected = turn::Entity::update_many()
        .col_expr(turn::Column::MessageRevision, Expr::value(number))
        .filter(turn::Column::Id.eq(id))
        .exec(&tx)
        .await?;
    anyhow::ensure!(affected.rows_affected == 1, "replacement Turn is missing");
    tx.commit().await?;
    Ok(())
}

#[tokio::test]
async fn canonical_history_keyset_handles_timestamp_ties_snapshot_appends_and_edits() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = seed(fixture, "saved-history", "parent").await?;
        let at = unix_to_datetime(1_700_000_000);
        for id in ["turn-a", "turn-c", "turn-b"] {
            new_turn(&db, "parent", id).await?;
            turn::Entity::update_many()
                .col_expr(turn::Column::CreatedAt, Expr::value(at))
                .filter(turn::Column::Id.eq(id))
                .exec(&db)
                .await
                .unwrap();
        }
        let snapshot = snapshot(&db, "parent").await.unwrap();
        let first = page(&db, "parent", None, None, 2, &snapshot).await.unwrap();
        assert_eq!(
            first.iter().map(|r| r.turn_id.as_str()).collect::<Vec<_>>(),
            ["turn-c", "turn-b"]
        );
        let boundary = first[0].key.clone();
        let before = first[1].key.clone();
        let previous_revision = revision(&db, "parent", &boundary, &snapshot).await.unwrap();
        new_turn(&db, "parent", "new-turn").await?;
        // Even a newly saved record at the same timestamp and inside the old
        // lexicographic boundary is outside this traversal's insertion watermark.
        turn::Entity::update_many()
            .col_expr(turn::Column::CreatedAt, Expr::value(at))
            .filter(turn::Column::Id.eq("new-turn"))
            .exec(&db)
            .await
            .unwrap();
        let old = page(&db, "parent", Some(&boundary), Some(&before), 2, &snapshot)
            .await
            .unwrap();
        assert_eq!(
            old.iter().map(|r| r.turn_id.as_str()).collect::<Vec<_>>(),
            ["turn-a"]
        );
        assert_eq!(
            revision(&db, "parent", &boundary, &snapshot).await.unwrap(),
            previous_revision
        );
        assert!(
            page(&db, "child", Some(&boundary), None, 2, &snapshot)
                .await
                .unwrap()
                .is_empty()
        );
        change_input(
            &db,
            "turn-a",
            &[UserInput::Text {
                text: "edited".into(),
                text_elements: vec![],
            }],
            1,
            1_700_000_010,
        )
        .await
        .unwrap();
        assert_ne!(
            revision(&db, "parent", &boundary, &snapshot).await.unwrap(),
            previous_revision
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn deleted_turn_inputs_are_not_returned_by_canonical_history_scan() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = seed(fixture, "deleted-history", "thread").await?;
        new_turn(&db, "thread", "turn").await?;
        change_input(
            &db,
            "turn",
            &[UserInput::Text {
                text: "removed".into(),
                text_elements: vec![],
            }],
            1,
            1_700_000_010,
        )
        .await
        .unwrap();
        let snapshot = snapshot(&db, "thread").await.unwrap();
        assert!(
            page(&db, "thread", None, None, 100, &snapshot)
                .await
                .unwrap()
                .iter()
                .any(|r| r.key.kind == "input")
        );
        turn::Entity::update_many()
            .col_expr(
                turn::Column::MessageDeletedAt,
                Expr::value(unix_to_datetime(1_700_000_020)),
            )
            .filter(turn::Column::Id.eq("turn"))
            .exec(&db)
            .await
            .unwrap();
        let rows = page(&db, "thread", None, None, 100, &snapshot)
            .await
            .unwrap();
        assert!(
            !rows
                .iter()
                .any(|r| r.key.kind == "input" || r.key.kind == "item")
        );
        assert_eq!(
            rows.len(),
            1,
            "the authorized deleted-turn tombstone remains readable"
        );
        Ok(())
    })
    .await
    .unwrap();
}
