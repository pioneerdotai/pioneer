use crate::{plan_test_support::seed_payload_sources, store::CompactionStore};
use pioneer_crud_payloads::storage::PreparedPayload;
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_postgres_test::{TestDatabase, with_database};
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};

async fn open(fixture: &TestDatabase) -> CompactionStore {
    CompactionStore::for_test(PostgresDatabase::new(
        fixture.connect_read_only().await.unwrap(),
        fixture.connect().await.unwrap(),
    ))
    .with_maintenance_access()
}

async fn scalar(store: &CompactionStore, sql: &str) -> i64 {
    store
        .database_connection()
        .query_one_raw(Statement::from_string(DbBackend::Postgres, sql))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

async fn fixture(db: &TestDatabase) -> CompactionStore {
    seed_payload_sources(db.connection()).await;
    open(db).await
}

async fn replace_event(fixture: &TestDatabase, text: &str) -> String {
    let body = PreparedPayload::new(text).unwrap();
    let tx = fixture.connection().begin().await.unwrap();
    body.insert(&tx).await.unwrap();
    let result = tx
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE turn_event SET payload=NULL, payload_payload_id=$1 WHERE id='event'",
            [body.id().into()],
        ))
        .await
        .unwrap();
    assert_eq!(result.rows_affected(), 1);
    tx.commit().await.unwrap();
    body.id().to_owned()
}

#[tokio::test]
async fn a_canonical_edit_invalidates_the_compaction_reference_for_a_stored_payload() {
    with_database(async |db| {
        let store = fixture(db).await;
        let text = serde_json::json!({"text": "сжатие 🦀".repeat(10_000)}).to_string();
        let value = replace_event(db, &text).await;
        let source = pioneer_compaction::SourceRef {
            scope: "event:turn".into(),
            id: "event".into(),
            version: "event-revision:2".into(),
        };
        let epoch = store.compaction_projection_version("ws", "thread").await?;
        let original = store
            .compaction_reference_fragment("ws", "thread", &source, 0)
            .await?
            .expect("current event reference");
        assert_eq!(original.text, text.chars().take(16_384).collect::<String>());
        assert!(original.next_character.is_some());
        assert_eq!(original.reference, source);
        assert!(
            store
                .compaction_references_current("ws", "thread", std::slice::from_ref(&source))
                .await?
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT count(DISTINCT payload_id) n FROM portable_payload_part WHERE codec=1"
            )
            .await,
            1
        );

        store
            .database_connection()
            .execute_unprepared("UPDATE turn_event SET sequence=sequence+1 WHERE id='event'")
            .await?;
        let updated = pioneer_compaction::SourceRef {
            version: "event-revision:3".into(),
            ..source.clone()
        };
        assert!(store.compaction_projection_version("ws", "thread").await? > epoch);
        assert!(
            !store
                .compaction_references_current("ws", "thread", std::slice::from_ref(&source))
                .await?
        );
        assert!(
            store
                .compaction_reference_fragment("ws", "thread", &source, 0)
                .await
                .is_err()
        );
        assert_eq!(
            store
                .compaction_reference_payload("ws", "thread", &updated)
                .await?,
            Some(text)
        );
        assert!(
            pioneer_crud_payloads::storage::metadata(db.connection(), &value)
                .await?
                .is_some()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn whole_and_batched_payload_loading_preserves_unicode_large_values_and_order() {
    with_database(async |db| {
    let store = fixture(db).await;
    let large = serde_json::json!({"text": "🧪漢字".repeat(90_000)}).to_string();
    replace_event(db, &large).await;
    let event = pioneer_compaction::SourceRef {
        scope: "event:turn".into(),
        id: "event".into(),
        version: "event-revision:2".into(),
    };
    assert_eq!(
        store
            .compaction_reference_payload("ws", "thread", &event)
            .await
            .unwrap()
            .as_deref(),
        Some(large.as_str())
    );

    let first = serde_json::to_string(&"a".repeat(130_000)).unwrap();
    let second = serde_json::to_string(&"б".repeat(65_000)).unwrap();
    let third = serde_json::to_string(&"c".repeat(130_000)).unwrap();
    for (index, (id, payload)) in [("input", &first), ("input-2", &second), ("input-3", &third)]
        .into_iter()
        .enumerate()
    {
        if index == 0 {
            store
                .database_connection()
                .execute_raw(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "UPDATE turn_input SET payload=$1::text::json WHERE id=$2",
                    [payload.clone().into(), id.into()],
                ))
                .await
                .unwrap();
        } else {
            store.database_connection().execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES($1, 'turn', $2::bigint::int, 'text', '', $3::text::json, CURRENT_TIMESTAMP)",
                [id.into(), (index as i64).into(), payload.clone().into()],
            )).await.unwrap();
        }
    }
    let refs = [
        pioneer_compaction::SourceRef {
            scope: "input:turn".into(),
            id: "input".into(),
            version: "input-revision:2".into(),
        },
        pioneer_compaction::SourceRef {
            scope: "input:turn".into(),
            id: "input-2".into(),
            version: "input-revision:1".into(),
        },
        pioneer_compaction::SourceRef {
            scope: "input:turn".into(),
            id: "input-3".into(),
            version: "input-revision:1".into(),
        },
    ];
    let (consumed, payloads) = store
        .compaction_reference_payload_batch("ws", "thread", &refs)
        .await
        .unwrap();
    assert_eq!(consumed, 2);
    assert_eq!(payloads, [first, second]);
    let (consumed, payloads) = store
        .compaction_reference_payload_batch("ws", "thread", &refs[2..])
        .await
        .unwrap();
    assert_eq!(consumed, 1);
    assert_eq!(payloads, [third]);
    store
        .database_connection()
        .execute_unprepared("UPDATE turn_input SET payload='null' WHERE id='input-2'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_reference_payload_batch("ws", "thread", &refs[..2])
            .await
            .is_err(),
        "a stale member cannot disappear from a successful batch"
    );
    store
        .database_connection()
        .execute_unprepared("DELETE FROM turn_input WHERE id='input-3'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_reference_payload_batch("ws", "thread", &refs[2..])
            .await
            .is_err(),
        "a tombstoned member cannot disappear from a successful batch"
    );
        Ok(())
    }).await.unwrap();
}

/// Counts the operations and retained bytes for a mixed history page.

#[tokio::test]
#[ignore = "manual history preparation benchmark; run only in the dedicated benchmark stage"]
async fn benchmark_history_payload_loading_many_small_and_one_large_compressed_result() {
    with_database(async |db| {
    let store = fixture(db).await;
    for index in 1..=1_000_i64 {
        store.database_connection().execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES($1, 'turn', $2::bigint::int, 'text', '', $3::text::json, CURRENT_TIMESTAMP)",
            [format!("bench-{index}").into(), index.into(), serde_json::json!({"type":"text","text":format!("message-{index}")}).to_string().into()],
        )).await.unwrap();
    }
    let large = serde_json::json!({"result":"z".repeat(4 * 1024 * 1024)}).to_string();
    replace_event(db, &large).await;

    let mut refs = (1..=1_000_i64)
        .map(|index| pioneer_compaction::SourceRef {
            scope: "input:turn".into(),
            id: format!("bench-{index}"),
            version: "input-revision:1".into(),
        })
        .collect::<Vec<_>>();
    refs.push(pioneer_compaction::SourceRef {
        scope: "event:turn".into(),
        id: "event".into(),
        version: "event-revision:2".into(),
    });
    let started = std::time::Instant::now();
    let mut offset = 0usize;
    let mut batches = 0usize;
    let mut retained_bytes = 0usize;
    while offset < refs.len() {
        let (consumed, payloads) = store
            .compaction_reference_payload_batch("ws", "thread", &refs[offset..])
            .await
            .unwrap();
        retained_bytes = retained_bytes.max(payloads.iter().map(String::len).sum());
        offset += consumed;
        batches += 1;
    }
    eprintln!(
        "history_payload_crud_microbenchmark rows={} measured_payload_calls={} estimated_reader_queries={} measured_max_returned_bytes={} elapsed_ms={}",
        refs.len(),
        batches,
        batches * 2 - 1,
        retained_bytes,
        started.elapsed().as_millis()
    );
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn stale_compaction_references_remain_stale_after_compression_and_restart() {
    with_database(async |db| {
        let store = fixture(db).await;
        let reference = pioneer_compaction::SourceRef {
            scope: "event:turn".into(),
            id: "event".into(),
            version: "event-revision:1".into(),
        };
        store
            .compaction_reference_fragment("ws", "thread", &reference, 0)
            .await
            .unwrap()
            .unwrap();
        let old_value = replace_event(
            db,
            &serde_json::json!({"text":"edited".repeat(20_000)}).to_string(),
        )
        .await;
        assert!(
            store
                .compaction_reference_fragment("ws", "thread", &reference, 0)
                .await
                .unwrap_err()
                .to_string()
                .contains("stale source revision")
        );
        assert!(
            store
                .compaction_reference_payload("ws", "thread", &reference)
                .await
                .unwrap()
                .is_none(),
            "a stale exact revision must not return a payload"
        );
        assert!(
            store
                .compaction_reference_payload_batch(
                    "ws",
                    "thread",
                    std::slice::from_ref(&reference)
                )
                .await
                .is_err(),
            "a stale batch must not return a partial success"
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT revision n FROM compaction_event_revision WHERE source_id='event'"
            )
            .await,
            2
        );
        drop(store);
        let store = open(db).await;
        assert!(
            store
                .compaction_reference_fragment("ws", "thread", &reference, 0)
                .await
                .unwrap_err()
                .to_string()
                .contains("stale source revision")
        );
        assert!(
            store
                .compaction_reference_payload("ws", "thread", &reference)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .compaction_reference_payload_batch(
                    "ws",
                    "thread",
                    std::slice::from_ref(&reference)
                )
                .await
                .is_err()
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT revision n FROM compaction_event_revision WHERE source_id='event'"
            )
            .await,
            2
        );
        let current_text = serde_json::json!({"text":"edited again".repeat(15_000)}).to_string();
        let current_value = replace_event(db, &current_text).await;
        assert!(
            pioneer_crud_payloads::storage::metadata(db.connection(), &old_value)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT revision n FROM compaction_event_revision WHERE source_id='event'"
            )
            .await,
            3
        );
        let current = pioneer_compaction::SourceRef {
            scope: "event:turn".into(),
            id: "event".into(),
            version: "event-revision:3".into(),
        };
        assert_eq!(
            store
                .compaction_reference_payload("ws", "thread", &current)
                .await
                .unwrap()
                .as_deref(),
            Some(current_text.as_str()),
        );
        store
            .database_connection()
            .execute_unprepared("DELETE FROM turn_event WHERE id='event'")
            .await
            .unwrap();
        assert!(
            pioneer_crud_payloads::storage::metadata(db.connection(), &current_value)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .compaction_reference_payload("ws", "thread", &current)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .compaction_reference_payload_batch("ws", "thread", std::slice::from_ref(&current))
                .await
                .is_err()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn whole_and_batch_reads_reject_noncanonical_revision_aliases() {
    with_database(async |db| {
        let store = fixture(db).await;
        for (scope, id, version) in [
            ("input:turn", "input", "input-revision:01"),
            ("context:turn", "context", "revision:+1"),
            ("event:turn", "event", "event-revision:01"),
            ("item:turn", "item", "item-revision:+1"),
        ] {
            let reference = pioneer_compaction::SourceRef {
                scope: scope.into(),
                id: id.into(),
                version: version.into(),
            };
            assert!(
                store
                    .compaction_reference_payload("ws", "thread", &reference)
                    .await
                    .is_err(),
                "whole read accepted noncanonical version {version}"
            );
            assert!(
                store
                    .compaction_reference_payload_batch(
                        "ws",
                        "thread",
                        std::slice::from_ref(&reference),
                    )
                    .await
                    .is_err(),
                "batch read accepted noncanonical version {version}"
            );
        }
        Ok(())
    })
    .await
    .unwrap();
}
