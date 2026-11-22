use crate::{
    canonical_history::PagedSource, plan_test_support::seed_revision_fixture,
    store::CompactionStore,
};
use pioneer_postgres_pools::PostgresDatabase;
use sea_orm::{ConnectionTrait, Statement};

async fn scalar(store: &CompactionStore, sql: &str) -> i64 {
    store
        .database_connection()
        .query_one_raw(Statement::from_string(
            store.database_connection().get_database_backend(),
            sql,
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

#[pioneer_postgres_test::postgres_test]
async fn canonical_revisions_preserve_independent_edits_deletes_and_compressed_sources() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connect().await.unwrap();
    seed_revision_fixture(fixture.connection(), true).await;
    let reader = fixture.connect_read_only().await.unwrap();
    let store =
        CompactionStore::for_test(PostgresDatabase::new(reader, fixture.connection().clone()));
    writer
        .execute_unprepared(
            "UPDATE turn_input SET payload='{\"edited\":true}' WHERE id='input-250'",
        )
        .await
        .unwrap();
    writer
        .execute_unprepared("DELETE FROM turn_input WHERE id='input-251'")
        .await
        .unwrap();
    writer.execute_unprepared("INSERT INTO turn_input(id,turn_id,input_index,input_type,payload,created_at) VALUES('new','turn',300,'text','{}',CURRENT_TIMESTAMP)").await.unwrap();
    assert!(
        store
            .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
            .await
            .unwrap()
            .entries
            .iter()
            .any(|event| event.reference.id == "event-1")
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT revision AS n FROM compaction_input_revision WHERE source_id='input-250'"
        )
        .await,
        2
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT present AS n FROM compaction_input_revision WHERE source_id='input-251'"
        )
        .await,
        0
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) AS n FROM compaction_input_revision WHERE source_id='new'"
        )
        .await,
        1
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) AS n FROM compaction_event_revision WHERE present=1"
        )
        .await,
        300
    );
    assert_eq!(scalar(&store,"SELECT count(*) AS n FROM turn_input WHERE id='input-250' AND payload::text='{\"edited\":true}'").await,1);
}
