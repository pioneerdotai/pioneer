use super::*;
use crate::compaction::{frozen, history};
use pioneer_compaction::frozen::FrozenMessageRef;
use pioneer_provider::ChatMessage;
use pioneer_sqlite::{SqliteReadClass, SqliteReadEvent, SqliteReadOutcome};
use sha2::{Digest, Sha256};

fn reference(source: SourceRef, message: &ChatMessage) -> FrozenMessageRef {
    FrozenMessageRef {
        logical_turn_id: None,
        source_thread: "thread".into(),
        context_thread: None,
        unit_id: source.id.clone(),
        sources: vec![source],
        event_input_role: None,
        source_aliases: vec![],
        ambiguous_input_aliases: vec![],
        publication_aliases: None,
        inherited: false,
        complete: true,
        protected_input: false,
        wire_sha256: hex::encode(Sha256::digest(serde_json::to_vec(message).unwrap())),
        replay_source: None,
        tool_item_id: None,
        tool_call_id: None,
        tool_name: None,
    }
}

async fn scenario() -> (Fixture, Arc<HistoryReadObserver>, Vec<FrozenMessageRef>) {
    let payloads = (0..3)
        .map(|index| {
            serde_json::to_string(&pioneer_crud::CanonicalTurnEventPayload::ItemCompleted(
                pioneer_protocol::ItemCompletedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: "turn".into(),
                    item: pioneer_protocol::TurnItem::AgentMessage {
                        id: format!("answer-{index}"),
                        text: format!("answer {index}"),
                        phase: Default::default(),
                        markdown: None,
                        markdown_version: None,
                    },
                },
            ))
            .unwrap()
        })
        .collect::<Vec<_>>();
    let observer = Arc::new(HistoryReadObserver::default());
    let f = fixture_with_canonical_payloads_and_observer(
        payloads.clone(),
        vec![],
        true,
        false,
        Some(observer.clone()),
    )
    .await;
    let sources = f
        .store
        .compaction_source_metadata_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries;
    let mut references = sources
        .into_iter()
        .zip(payloads)
        .map(|(entry, payload)| {
            let message = history::event_message(serde_json::from_str(&payload).unwrap())
                .unwrap()
                .unwrap();
            reference(entry.reference, &message)
        })
        .collect::<Vec<_>>();

    let db = f.store.database_connection();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,created_at,updated_at) VALUES('precheck-item','turn','historical-command','command_execution','completed',?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [serde_json::to_string(&historical_command_fixture("retained output")).unwrap().into()],
    )).await.unwrap();
    let message = ChatMessage::user("exact bounded replay");
    let replay_payload = serde_json::to_string(&pioneer_tools::ToolResultView::Json {
        value: serde_json::to_value(&message).unwrap(),
        truncated: false,
    })
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO turn_llm_context(id,turn_id,item_id,sequence,source,payload,output_policy_snapshot,created_at) VALUES('precheck-replay','turn','historical-command',1,'tool_result_v2',?,'{}',CURRENT_TIMESTAMP)",
        [replay_payload.into()],
    )).await.unwrap();
    let mut item = reference(
        SourceRef {
            scope: "item:turn".into(),
            id: "precheck-item".into(),
            version: "item-revision:1".into(),
        },
        &message,
    );
    item.replay_source = Some(SourceRef {
        scope: "context:turn".into(),
        id: "precheck-replay".into(),
        version: "revision:1".into(),
    });
    references.insert(1, item);
    let sources = references
        .iter()
        .flat_map(|entry| entry.sources.iter().chain(entry.replay_source.iter()))
        .cloned()
        .collect::<Vec<_>>();
    history::prepare_references(&f.store, "ws", "thread", &sources)
        .await
        .unwrap();
    (f, observer, references)
}

fn take_reads(observer: &HistoryReadObserver, class: SqliteReadClass) -> usize {
    let reads = std::mem::take(&mut *observer.reads.lock().unwrap());
    assert!(reads.iter().all(|event| !matches!(event,
        SqliteReadEvent::OperationFinished { class: actual, outcome, .. }
            if *actual != class || *outcome != SqliteReadOutcome::Ok
    )));
    reads
        .iter()
        .filter(|event| matches!(event, SqliteReadEvent::OperationFinished { .. }))
        .count()
}

#[tokio::test]
async fn frozen_prepared_event_item_group_saves_metadata_reads_for_both_projections() {
    let (f, observer, references) = scenario().await;
    for maintenance in [false, true] {
        let store = if maintenance {
            f.store.with_maintenance_access()
        } else {
            f.store.clone()
        };
        let class = if maintenance {
            SqliteReadClass::Maintenance
        } else {
            SqliteReadClass::Interactive
        };
        for execution in [false, true] {
            observer.reads.lock().unwrap().clear();
            observer.writes.lock().unwrap().clear();
            let mut expected = Vec::new();
            for entry in &references {
                expected.extend(
                    frozen::restore_entries_page_for_test(
                        &store,
                        "ws",
                        std::slice::from_ref(entry),
                        execution,
                    )
                    .await
                    .unwrap(),
                );
            }
            let individual_reads = take_reads(&observer, class);
            let actual =
                frozen::restore_entries_page_for_test(&store, "ws", &references, execution)
                    .await
                    .unwrap();
            let grouped_reads = take_reads(&observer, class);
            assert_eq!(actual, expected);
            assert_eq!(individual_reads - grouped_reads, references.len() - 1);
            assert_eq!(
                actual
                    .iter()
                    .map(|message| message.as_ref().unwrap().content.as_str())
                    .collect::<Vec<_>>(),
                ["answer 0", "exact bounded replay", "answer 1", "answer 2"]
            );
            assert!(
                observer.writes().is_empty(),
                "prepared restore queued a writer"
            );
        }
    }
}

#[tokio::test]
async fn frozen_unprepared_group_keeps_legacy_registration_fallback() {
    let (f, observer, references) = scenario().await;
    let expected = frozen::restore_entries_page_for_test(&f.store, "ws", &references, true)
        .await
        .unwrap();
    for sql in [
        "DELETE FROM compaction_event_revision",
        "DELETE FROM compaction_item_revision",
        "DELETE FROM compaction_source_revision",
    ] {
        f.store
            .database_connection()
            .execute_unprepared(sql)
            .await
            .unwrap();
    }
    observer.writes.lock().unwrap().clear();
    let actual = frozen::restore_entries_page_for_test(&f.store, "ws", &references, true)
        .await
        .unwrap();
    assert_eq!(actual, expected);
    assert!(
        !observer.writes().is_empty(),
        "legacy metadata was not registered"
    );
    let sources = references
        .iter()
        .flat_map(|entry| entry.sources.iter().chain(entry.replay_source.iter()))
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        f.store
            .compaction_references_current("ws", "thread", &sources)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn frozen_precheck_keeps_exact_reads_and_fallback_rejects_stale_sources() {
    for change in ["edit", "delete-reinsert", "tombstone", "replay"] {
        let (f, observer, references) = scenario().await;
        let store = f.store.with_maintenance_access();
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        observer.reads.lock().unwrap().clear();
        let restore = frozen::with_preparation_precheck_pause(
            frozen::restore_entries_page_for_test(&store, "ws", &references, true),
            reached_tx,
            release_rx,
        );
        let mutate = async {
            reached_rx
                .await
                .expect("actual grouped precheck did not return true");
            assert_eq!(
                take_reads(&observer, SqliteReadClass::Maintenance),
                1,
                "payload read started before the precheck pause"
            );
            // The same maintenance admission must be free while restore is
            // paused. This read completes before any source is changed.
            store
                .compaction_projection_version("ws", "thread")
                .await
                .unwrap();
            let db = store.database_connection();
            match change {
                "edit" => {
                    db.execute_unprepared(
                        "UPDATE turn_event SET payload=payload||' ' WHERE id='source'",
                    )
                    .await
                    .unwrap();
                }
                "delete-reinsert" => {
                    db.execute_unprepared("DELETE FROM turn_event WHERE id='source'")
                        .await
                        .unwrap();
                    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES('source','thread','turn',1,'item/completed',?,CURRENT_TIMESTAMP)",
                        [f.canonical_payload.clone().into()],
                    )).await.unwrap();
                }
                "tombstone" => {
                    db.execute_unprepared(
                        "UPDATE compaction_event_revision SET present=0 WHERE source_id='source'",
                    )
                    .await
                    .unwrap();
                }
                "replay" => {
                    db.execute_unprepared(
                        "UPDATE turn_llm_context SET payload=payload||' ' WHERE id='precheck-replay'",
                    )
                    .await
                    .unwrap();
                }
                _ => unreachable!(),
            }
            observer.reads.lock().unwrap().clear();
            observer.writes.lock().unwrap().clear();
            release_tx.send(()).expect("restore ended while paused");
        };
        let (result, ()) = tokio::join!(restore, mutate);
        let error = result.expect_err(change);
        assert!(
            error
                .to_string()
                .contains("selected history source disappeared"),
            "{change}: {error:#}"
        );
        // No prepare_references call was reintroduced after release: event
        // failure takes one exact payload read; replay failure reads the first
        // event, the item, then its exact replay dependency.
        assert_eq!(
            take_reads(&observer, SqliteReadClass::Maintenance),
            if change == "replay" { 3 } else { 1 }
        );
        assert!(observer.writes().is_empty());
        // A fresh grouped precheck is false. Legacy seeding must not overwrite
        // the authoritative revision or tombstone to make this restore pass.
        assert!(
            frozen::restore_entries_page_for_test(&f.store, "ws", &references, true)
                .await
                .is_err(),
            "{change}"
        );
    }
}

#[tokio::test]
async fn frozen_precheck_pause_is_operation_scoped_and_drop_releases_restore() {
    let (f, _, references) = scenario().await;
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let restore = frozen::with_preparation_precheck_pause(
        frozen::restore_entries_page_for_test(&f.store, "ws", &references, true),
        reached_tx,
        release_rx,
    );
    let other_operation = async {
        reached_rx.await.expect("grouped precheck did not pause");
        // Even the same store, workspace, entries and Tokio task cannot consume
        // another operation's pause. Only the scoped restore is suspended.
        let result = frozen::restore_entries_page_for_test(&f.store, "ws", &references, true)
            .await
            .unwrap();
        drop(release_tx);
        result
    };
    let (restored, other) = tokio::join!(restore, other_operation);
    assert_eq!(restored.unwrap(), other);
}

#[tokio::test]
async fn frozen_precheck_pause_cancellation_drops_channels_and_leaves_store_usable() {
    let (f, _, references) = scenario().await;
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let mut restore = Box::pin(frozen::with_preparation_precheck_pause(
        frozen::restore_entries_page_for_test(&f.store, "ws", &references, true),
        reached_tx,
        release_rx,
    ));
    tokio::select! {
        result = &mut restore => panic!("restore ended before cancellation: {result:?}"),
        reached = reached_rx => reached.expect("grouped precheck did not pause"),
    }
    drop(restore);
    assert!(
        release_tx.send(()).is_err(),
        "cancelled restore kept its receiver"
    );
    assert!(
        frozen::restore_entries_page_for_test(&f.store, "ws", &references, true)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn frozen_precheck_does_not_hide_database_errors_or_precheck_malformed_revisions() {
    let (f, observer, mut references) = scenario().await;
    references[0].sources[0].version = "event-revision:01".into();
    observer.reads.lock().unwrap().clear();
    assert!(
        frozen::restore_entries_page_for_test(&f.store, "ws", &references, true)
            .await
            .is_err()
    );
    assert_eq!(take_reads(&observer, SqliteReadClass::Interactive), 0);
    references[0].sources[0].version = "event-revision:1".into();
    f.store
        .database_connection()
        .execute_unprepared(
            "ALTER TABLE compaction_event_revision RENAME TO unavailable_event_revision",
        )
        .await
        .unwrap();
    assert!(
        frozen::restore_entries_page_for_test(&f.store, "ws", &references, true)
            .await
            .is_err()
    );
}
