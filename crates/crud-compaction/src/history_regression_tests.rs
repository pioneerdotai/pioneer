use crate::{
    SOURCE_PAGE_BYTES, SOURCE_PAGE_ROWS,
    canonical_history::PagedSource,
    checkpoint_apply::{CommitOutcome, SourceAssertion},
    frozen_import::EMPTY_FROZEN_IMPORT_SHA256,
    frozen_maintenance::FrozenStorageLifetimeProgress,
    runner_repository::ManifestEntry,
    runner_writes::CanonicalSource,
    store::CompactionStore,
};
use pioneer_compaction::*;
use sea_orm::{ConnectionTrait, Database, DbBackend, Statement, TransactionTrait};

type RecordedStatements = std::sync::Arc<std::sync::Mutex<Vec<Statement>>>;

async fn store() -> CompactionStore {
    store_recording_statements(None).await
}

async fn store_recording_statements(statements: Option<RecordedStatements>) -> CompactionStore {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let mut writer = fixture.connection().clone();
    let mut reader = fixture.connect_read_only().await.unwrap();
    if let Some(statements) = statements {
        let reads = statements.clone();
        reader.set_metric_callback(move |info| {
            reads.lock().unwrap().push(info.statement.clone());
        });
        writer.set_metric_callback(move |info| {
            statements.lock().unwrap().push(info.statement.clone());
        });
    }
    let store = CompactionStore::for_test(pioneer_postgres_pools::PostgresDatabase::new(
        reader, writer,
    ))
    .with_maintenance_access();
    let db = store.database_connection();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',TRUE,FALSE)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    store
}

#[pioneer_postgres_test::postgres_test]
async fn checkpoint_publication_preserves_compressed_event() {
    let store = store().await;
    let wire = format!(r#"{{"text":"{}"}}"#, "retained original 🌍".repeat(4096));
    let source = source_with_storage(&store, "first", 1, &wire, true).await;
    let checkpoint = candidate(&store, "compressed-history", None, &source).await;
    assert_eq!(
        store
            .compaction_apply(&checkpoint, None, &[source])
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    assert_eq!(
        store
            .compaction_head(&checkpoint.owner)
            .await
            .unwrap()
            .unwrap(),
        checkpoint.id
    );
}

async fn source(
    store: &CompactionStore,
    id: &str,
    sequence: i64,
    payload: &str,
) -> SourceAssertion {
    source_with_storage(store, id, sequence, payload, false).await
}

async fn source_with_storage(
    store: &CompactionStore,
    id: &str,
    sequence: i64,
    payload: &str,
    portable: bool,
) -> SourceAssertion {
    let wire = if serde_json::from_str::<serde_json::Value>(payload).is_ok() {
        payload.to_owned()
    } else {
        serde_json::to_string(payload).unwrap()
    };
    let db = store.database_connection();
    if portable {
        crate::plan_test_support::seed_portable_event(&db, id, sequence, &wire).await;
    } else {
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ($1,'thread','turn',$2,'fixture',$3::text::json,CURRENT_TIMESTAMP)",
            [id.into(), sequence.into(), wire.clone().into()])).await.unwrap();
    }
    let revision = store
        .database_connection()
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT revision FROM compaction_event_revision WHERE source_id=$1",
            [id.into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "revision")
        .unwrap();
    SourceAssertion {
        revision: Some(revision),
        kind: CanonicalSource::Event,
        turn_id: "turn".into(),
        id: id.into(),
        payload: wire,
    }
}

async fn candidate(
    store: &CompactionStore,
    op: &str,
    expected: Option<&str>,
    assertion: &SourceAssertion,
) -> Checkpoint {
    candidate_with_epochs(
        store,
        op,
        expected,
        assertion,
        std::collections::BTreeMap::new(),
    )
    .await
}

async fn candidate_with_epochs(
    store: &CompactionStore,
    op: &str,
    expected: Option<&str>,
    assertion: &SourceAssertion,
    source_epochs: std::collections::BTreeMap<String, u64>,
) -> Checkpoint {
    candidate_fixture(store, op, expected, assertion, source_epochs, false).await
}

async fn candidate_with_manifest(
    store: &CompactionStore,
    op: &str,
    expected: Option<&str>,
    assertion: &SourceAssertion,
) -> Checkpoint {
    candidate_fixture(
        store,
        op,
        expected,
        assertion,
        std::collections::BTreeMap::new(),
        true,
    )
    .await
}

async fn candidate_fixture(
    store: &CompactionStore,
    op: &str,
    expected: Option<&str>,
    assertion: &SourceAssertion,
    source_epochs: std::collections::BTreeMap<String, u64>,
    prepare_manifest: bool,
) -> Checkpoint {
    let checkpoint = candidate_admission_fixture(
        store,
        op,
        expected,
        assertion,
        source_epochs,
        prepare_manifest,
    )
    .await;
    store
        .compaction_save_candidate(&checkpoint, 0)
        .await
        .unwrap();
    checkpoint
}

async fn candidate_admission_fixture(
    store: &CompactionStore,
    op: &str,
    expected: Option<&str>,
    assertion: &SourceAssertion,
    source_epochs: std::collections::BTreeMap<String, u64>,
    prepare_manifest: bool,
) -> Checkpoint {
    let selection = ModelSelection {
        service_mode: None,
        context_authority: None,
        context_window: None,
        transport: Transport::Api,
        instance: "p".into(),
        model: "m".into(),
        effort: None,
    };
    let snapshot = OperationSnapshot {
        id: op.into(),
        owner: "owner".into(),
        expected_checkpoint: expected.map(str::to_owned),
        projection_version: 1,
        source_epochs,
        admission: CompactionSettings::default()
            .admit(&selection, None, 10)
            .unwrap(),
        plan: CompactionPlan {
            mode: CompactionMode::Normal,
            coverage_domain: pioneer_compaction::CoverageDomain::OwnContribution,
            compact: vec![0],
            retain: vec![],
            coverage: vec![assertion.reference()],
            fingerprint: op.into(),
        },
    };
    store
        .compaction_admit("ws", "thread", &snapshot)
        .await
        .unwrap();
    if prepare_manifest {
        store
            .compaction_prepare_runner(op, &ModelBudget::new(None, None, None), 1, 0)
            .await
            .unwrap();
        store
            .compaction_append_manifest(
                op,
                &[ManifestEntry {
                    ordinal: 0,
                    unit: 0,
                    reference_only: false,
                    thread_id: "thread".into(),
                    source: assertion.reference(),
                }],
            )
            .await
            .unwrap();
    }
    let checkpoint = Checkpoint {
        id: format!("cp-{op}"),
        operation_id: op.into(),
        format_version: 1,
        owner: "owner".into(),
        previous: expected.map(str::to_owned),
        coverage: vec![assertion.reference()],
        summary: "A saved summary".into(),
        selection,
        projection_version: 1,
    };
    checkpoint
}

#[pioneer_postgres_test::postgres_test]
async fn append_survives_atomic_apply_and_restart_does_not_regenerate_or_reapply() {
    let store = store().await;
    let first = source(&store, "source-a", 1, "original one").await;
    let cp = candidate(&store, "op-a", None, &first).await;
    assert!(
        store
            .compaction_manifest_page("op-a", false, 0, 0)
            .await
            .unwrap()
            .len()
            == 1,
        "raw assertion publication saves exact historical ownership"
    );
    source(&store, "source-b", 2, "appended after snapshot").await;
    assert_eq!(
        store
            .compaction_apply(&cp, None, &[first.clone()])
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    let restarted = CompactionStore::for_test(store.database_connection());
    assert_eq!(
        restarted.compaction_head("owner").await.unwrap().as_deref(),
        Some(cp.id.as_str())
    );
    assert_eq!(
        restarted
            .compaction_apply(&cp, None, &[first])
            .await
            .unwrap(),
        CommitOutcome::AlreadyApplied
    );
    let page = restarted
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 2);
    assert!(page.entries.iter().all(|s| !s.incomplete));
    assert_eq!(
        restarted
            .compaction_checkpoint(&cp.id)
            .await
            .unwrap()
            .unwrap()
            .coverage
            .len(),
        1
    );
}

#[pioneer_postgres_test::postgres_test]
async fn edits_competing_head_and_stop_reject_candidates_without_losing_summaries() {
    let store = store().await;
    let s = source(&store, "source", 1, "original").await;
    let a = candidate(&store, "a", None, &s).await;
    let b = candidate(&store, "b", None, &s).await;
    assert_eq!(
        store
            .compaction_apply(&a, None, &[s.clone()])
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    assert_eq!(
        store
            .compaction_apply(&b, None, &[s.clone()])
            .await
            .unwrap(),
        CommitOutcome::Stale
    );
    let c = candidate(&store, "c", Some(&a.id), &s).await;
    store
        .database_connection()
        .execute_unprepared(
            "UPDATE turn_event SET payload=to_json('changed'::text) WHERE id='source'",
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_apply(&c, Some(&a.id), &[s.clone()])
            .await
            .unwrap(),
        CommitOutcome::Stale
    );
    store
        .compaction_finish("c", "cancelled", "user_stop")
        .await
        .unwrap();
    assert_eq!(
        store.compaction_apply(&c, Some(&a.id), &[s]).await.unwrap(),
        CommitOutcome::Cancelled
    );
    for cp in [&a, &b, &c] {
        assert!(store.compaction_checkpoint(&cp.id).await.unwrap().is_some());
    }
    assert_eq!(
        store.compaction_head("owner").await.unwrap().as_deref(),
        Some(a.id.as_str())
    );
}

#[pioneer_postgres_test::postgres_test]
async fn legacy_apply_ignores_coarse_epoch_changes_behind_previous_checkpoint() {
    for delete in [false, true] {
        let store = store().await;
        let a = source(&store, "independent-a", 1, "A").await;
        let s1 = candidate(&store, "independent-s1", None, &a).await;
        assert_eq!(
            store.compaction_apply(&s1, None, &[a]).await.unwrap(),
            CommitOutcome::Applied
        );

        let b = source(&store, "independent-b", 2, "B").await;
        let epoch = store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap();
        let s2 = candidate_with_epochs(
            &store,
            "independent-s2",
            Some(&s1.id),
            &b,
            std::collections::BTreeMap::from([("thread".into(), epoch)]),
        )
        .await;
        store
            .database_connection()
            .execute_unprepared(if delete {
                "DELETE FROM turn_event WHERE id='independent-a'"
            } else {
                "UPDATE turn_event SET payload=to_json('edited A'::text) WHERE id='independent-a'"
            })
            .await
            .unwrap();

        assert_eq!(
            store
                .compaction_apply(&s2, Some(&s1.id), &[b])
                .await
                .unwrap(),
            CommitOutcome::Applied
        );
    }
}

#[pioneer_postgres_test::postgres_test]
async fn exact_candidate_idempotency() {
    let store = store().await;
    let s = source(&store, "source", 1, "original").await;
    let cp = candidate(&store, "op", None, &s).await;
    store.compaction_save_candidate(&cp, 0).await.unwrap();
    let mut forged = cp.clone();
    forged.summary = "different result".into();
    assert!(store.compaction_save_candidate(&forged, 0).await.is_err());
    assert!(store.compaction_apply(&forged, None, &[s]).await.is_err());
}

#[pioneer_postgres_test::postgres_test]
async fn cancellation_releases_writer_and_reader_is_physically_read_only() {
    use std::time::Duration;
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture
        .connect_with_options(|options| {
            options
                .max_connections(1)
                .map_sqlx_postgres_opts(|options| {
                    options.options([("default_transaction_read_only", "on")])
                });
        })
        .await
        .unwrap();
    let database = pioneer_postgres_pools::PostgresDatabase::new(reader, writer);
    assert!(database.reader_is_read_only().await.unwrap());
    let store = CompactionStore::for_test(database.clone()).with_maintenance_access();
    let hold = database.begin().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), store.compaction_head("none"))
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
    let waiting = tokio::spawn({
        let store = store.clone();
        async move { store.compaction_finish("none", "cancelled", "stop").await }
    });
    tokio::task::yield_now().await;
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    hold.rollback().await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        store.compaction_finish("none", "failed", "done"),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(database.reader_is_read_only().await.unwrap());
    database.close().await.unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn tool_result_lookup_never_crosses_workspace_thread_or_turn() {
    let store = store().await;
    store.database_connection().execute_unprepared("INSERT INTO turn_llm_context(id,turn_id,item_id,sequence,source,payload,output_policy_snapshot,created_at) VALUES ('result','turn','call-item',1,'tool_result_v2','{}','{}',CURRENT_TIMESTAMP)").await.unwrap();
    assert_eq!(
        store
            .compaction_tool_result_id("ws", "thread", "turn", "call-item")
            .await
            .unwrap()
            .as_deref(),
        Some("result")
    );
    for (workspace, thread, turn) in [
        ("other", "thread", "turn"),
        ("ws", "other", "turn"),
        ("ws", "thread", "other"),
    ] {
        assert!(
            store
                .compaction_tool_result_id(workspace, thread, turn, "call-item")
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[pioneer_postgres_test::postgres_test]
async fn full_shell_result_reuses_terminal_item_and_binds_revision() {
    let store = store().await;
    let payload = serde_json::json!({"storage":{"kind":"shell","stdout":"🌍漢字".repeat(20_000),"truncated":true}}).to_string();
    store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO turn_item(id,turn_id,item_id,item_type,payload,created_at,updated_at) VALUES ('shell-row','turn','shell-item','command_execution',$1::text::json,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", [payload.clone().into()])).await.unwrap();
    assert!(
        store
            .compaction_tool_result_id("ws", "thread", "turn", "shell-item")
            .await
            .unwrap()
            .is_none()
    );
    let first = store
        .compaction_tool_result_fragment("ws", "thread", "turn", "shell-item", None, 0)
        .await
        .unwrap()
        .unwrap();
    let version = first.reference.version.clone();
    assert!(version.starts_with("item-revision:"));
    let mut restored = first.text;
    let mut next = first.next_character;
    while let Some(offset) = next {
        let fragment = store
            .compaction_tool_result_fragment(
                "ws",
                "thread",
                "turn",
                "shell-item",
                Some(&version),
                offset,
            )
            .await
            .unwrap()
            .unwrap();
        restored.push_str(&fragment.text);
        next = fragment.next_character;
    }
    assert_eq!(restored, payload);
    assert!(
        store
            .compaction_tool_result_fragment("other", "thread", "turn", "shell-item", None, 0)
            .await
            .unwrap()
            .is_none()
    );
    let assertion = SourceAssertion {
        kind: CanonicalSource::ToolItem,
        turn_id: "turn".into(),
        id: "shell-row".into(),
        payload: String::new(),
        revision: Some(
            version
                .strip_prefix("item-revision:")
                .unwrap()
                .parse()
                .unwrap(),
        ),
    };
    let checkpoint = candidate(&store, "shell-op", None, &assertion).await;
    store.database_connection().execute_unprepared("UPDATE turn_item SET payload=jsonb_set(payload::jsonb,'{storage,truncated}','false'::jsonb)::json WHERE id='shell-row'").await.unwrap();
    assert_eq!(
        store
            .compaction_apply(&checkpoint, None, &[assertion])
            .await
            .unwrap(),
        CommitOutcome::Stale
    );
    assert!(
        store
            .compaction_tool_result_fragment(
                "ws",
                "thread",
                "turn",
                "shell-item",
                Some(&version),
                0
            )
            .await
            .is_err()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn durable_runner_admission_preserves_overflow_and_ordinary_allowances() {
    use pioneer_compaction::runner::{AttemptObservation, FailureKind, RunnerPhase, RunnerState};
    let store = store().await;
    let source = source(&store, "retry-source", 1, "retained source").await;
    candidate_admission_fixture(
        &store,
        "retry-runner",
        None,
        &source,
        Default::default(),
        true,
    )
    .await;
    let deadline = u64::try_from(
        store
            .compaction_operation("retry-runner")
            .await
            .unwrap()
            .unwrap()
            .deadline_ms,
    )
    .unwrap();
    let initial =
        RunnerState::new(deadline, &ModelBudget::new(None, None, None), 1000, None).unwrap();
    for (overflow, ceiling) in [(1, None), (0, Some(0)), (0, Some(8500))] {
        let mut invalid = initial.clone();
        invalid.overflow_rejections = overflow;
        invalid.overflow_input_ceiling = ceiling;
        assert!(
            store
                .compaction_activate_runner("retry-runner", &invalid)
                .await
                .unwrap_err()
                .to_string()
                .contains("invalid initial runner state")
        );
        assert!(
            store
                .compaction_runner_state("retry-runner")
                .await
                .unwrap()
                .is_none()
        );
    }
    // Decode a pre-estimation JSON shape at the actual activation boundary.
    let mut legacy = serde_json::to_value(initial).unwrap();
    for field in [
        "portion_attempts",
        "overflow_rejections",
        "overflow_input_ceiling",
    ] {
        legacy.as_object_mut().unwrap().remove(field);
    }
    let initial: RunnerState = serde_json::from_value(legacy).unwrap();
    assert_eq!(initial.portion_attempts, 0);
    assert_eq!(initial.overflow_rejections, 0);
    assert_eq!(initial.overflow_input_ceiling, None);
    store
        .compaction_activate_runner("retry-runner", &initial)
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_runner_state("retry-runner")
            .await
            .unwrap()
            .unwrap(),
        initial
    );

    // Independent invalid storage proposals. Total<=7 alone would admit the
    // first four; no saturating subtraction may accept overflow>portion.
    for (portion, overflow, retries, corrections) in [
        (4, 0, 0, 0),
        (7, 3, 0, 0),
        (5, 5, 0, 0),
        (1, 2, 0, 0),
        (1, 0, 3, 0),
        (1, 0, 0, 2),
    ] {
        let mut invalid = initial.claim(1).unwrap();
        invalid.portion_attempts = portion;
        invalid.overflow_rejections = overflow;
        invalid.retries = retries;
        invalid.corrections = corrections;
        assert!(
            store
                .compaction_runner_transition("retry-runner", initial.generation, &invalid, None)
                .await
                .unwrap_err()
                .to_string()
                .contains("invalid state transition")
        );
        assert_eq!(
            store
                .compaction_runner_state("retry-runner")
                .await
                .unwrap()
                .unwrap(),
            initial
        );
    }

    // Exercise one mixed execution through the real persistence owner. The
    // shrink arithmetic itself is covered by the pure runner; here every claim
    // and failure must survive a new CompactionStore handle and reload, including 4..7.
    let mut current = initial;
    let mut now = 1_u64;
    for kind in [
        FailureKind::InputOverflow,
        FailureKind::Transient,
        FailureKind::InputOverflow,
        FailureKind::InvalidCompletion,
        FailureKind::InputOverflow,
        FailureKind::InputOverflow,
        FailureKind::Permanent,
    ] {
        if let RunnerPhase::Backoff { not_before_ms, .. } = current.phase {
            now = now.max(not_before_ms);
        }
        let mut claim = current.claim(now).unwrap();
        claim.observation = Some(AttemptObservation {
            number: claim.attempts,
            started_ms: now,
            finished_ms: None,
            estimated_input_tokens: claim.input_ceiling().unwrap_or(10_000),
            output_cap: claim.target_tokens,
            input_tokens: None,
            output_tokens: None,
            completion: None,
            failure: None,
            diagnostic: None,
        });
        assert!(
            store
                .compaction_runner_transition("retry-runner", current.generation, &claim, None)
                .await
                .unwrap()
        );
        let reloaded =
            CompactionStore::for_test(store.database_connection()).with_maintenance_access();
        let saved_claim = reloaded
            .compaction_runner_state("retry-runner")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved_claim, claim);
        let failed = saved_claim.attempt_failed(kind, now + 1, None).unwrap();
        assert!(
            reloaded
                .compaction_runner_transition("retry-runner", saved_claim.generation, &failed, None)
                .await
                .unwrap()
        );
        current = store
            .compaction_runner_state("retry-runner")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current, failed);
        now += 2;
    }
    assert_eq!(current.attempts, 7);
    assert_eq!(current.portion_attempts, 7);
    assert_eq!(current.overflow_rejections, 4);
    assert!(current.overflow_input_ceiling.is_some());
    assert_eq!(current.retries, 1);
    assert_eq!(current.cursor, Default::default());
    assert_eq!(current.previous_checkpoint, None);
}

#[pioneer_postgres_test::postgres_test]
async fn projection_epoch_changes_for_history_edits_without_invalidating_active_parent_appends() {
    let store = store().await;
    let db = store.database_connection();
    db.execute_unprepared("INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,created_at,updated_at) VALUES ('live-row','turn','live-item','command_execution','in_progress','{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared(
        "UPDATE turn_item SET payload='{\"stdout\":\"progress\"}' WHERE id='live-row'",
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        0
    );
    db.execute_unprepared("UPDATE turn_item SET status='completed',payload='{\"stdout\":\"finished\"}' WHERE id='live-row'").await.unwrap();
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        0
    );
    source(&store, "appended-parent-event", 1, "completed new work").await;
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        0
    );
    db.execute_unprepared("UPDATE turn_item SET payload='{\"stdout\":\"edited historical result\"}' WHERE id='live-row'").await.unwrap();
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        1
    );
}

#[pioneer_postgres_test::postgres_test]
async fn user_input_revisions_bind_edits_and_deletes_without_invalidating_appends() {
    let store = store().await;
    let db = store.database_connection();
    db.execute_unprepared("INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('original-input','turn',0,'text','original','{\"type\":\"text\",\"text\":\"original\"}',CURRENT_TIMESTAMP)").await.unwrap();
    let page = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Input, 0)
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    let original = page.entries[0].reference.clone();
    assert_eq!(original.version, "input-revision:1");
    assert!(
        store
            .compaction_reference_fragment("ws", "thread", &original, 0)
            .await
            .unwrap()
            .unwrap()
            .text
            .contains("original")
    );
    db.execute_unprepared("INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('new-input','turn',1,'text','new','{\"type\":\"text\",\"text\":\"new\"}',CURRENT_TIMESTAMP)").await.unwrap();
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        0
    );
    db.execute_unprepared("UPDATE turn_input SET text='edited',payload='{\"type\":\"text\",\"text\":\"edited\"}' WHERE id='original-input'").await.unwrap();
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        1
    );
    assert!(
        store
            .compaction_reference_fragment("ws", "thread", &original, 0)
            .await
            .is_err()
    );
    let changed = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Input, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    assert_eq!(changed.version, "input-revision:2");
    assert!(
        store
            .compaction_reference_fragment("other", "thread", &changed, 0)
            .await
            .unwrap()
            .is_none()
    );
    db.execute_unprepared("DELETE FROM turn_input WHERE id='original-input'")
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        2
    );
    assert!(
        store
            .compaction_reference_fragment("ws", "thread", &changed, 0)
            .await
            .unwrap()
            .is_none()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn source_high_water_is_scoped_and_append_does_not_move_a_captured_boundary() {
    let store = store().await;
    source(&store, "first", 1, "first payload").await;
    let high_water = store
        .compaction_source_high_water("ws", "thread", "turn", PagedSource::Event)
        .await
        .unwrap();
    assert_eq!(high_water, 1);
    assert_eq!(
        store
            .compaction_source_high_water("other", "thread", "turn", PagedSource::Event)
            .await
            .unwrap(),
        0
    );
    source(&store, "appended", 2, "later payload").await;
    let page = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap();
    let pinned: Vec<_> = page
        .entries
        .into_iter()
        .filter(|row| row.sequence <= high_water)
        .collect();
    assert_eq!(pinned.len(), 1);
    assert_eq!(pinned[0].reference.id, "first");
    assert_eq!(pinned[0].payload.as_deref(), Some(r#""first payload""#));
    assert_eq!(
        store
            .compaction_source_high_water("ws", "thread", "turn", PagedSource::Event)
            .await
            .unwrap(),
        2
    );
}

#[pioneer_postgres_test::postgres_test]
async fn metadata_projection_keeps_exact_sources_without_loading_covered_payloads() {
    let store = store().await;
    source(&store, "small-metadata", 1, "small body").await;
    source(
        &store,
        "large-metadata",
        2,
        &"x".repeat(SOURCE_PAGE_BYTES + 1),
    )
    .await;
    let metadata = store
        .compaction_source_metadata_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap();
    assert_eq!(metadata.entries.len(), 2);
    assert_eq!(metadata.next_sequence, 2);
    assert!(metadata.entries.iter().all(|row| row.payload.is_none()));
    let materialized = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap();
    assert_eq!(
        metadata.entries[0].reference,
        materialized.entries[0].reference
    );
    assert_eq!(
        metadata.entries[1].reference,
        materialized.entries[1].reference
    );
    assert_eq!(
        materialized.entries[0].payload.as_deref(),
        Some(r#""small body""#)
    );
    assert!(materialized.entries[1].payload.is_none());
    assert!(
        store
            .compaction_source_metadata_page("other", "thread", "turn", PagedSource::Event, 0)
            .await
            .unwrap()
            .entries
            .is_empty()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn fitting_request_validation_rejects_edited_deleted_and_cross_scope_sources() {
    let store = store().await;
    source(&store, "current-source", 1, "original").await;
    let reference = store
        .compaction_source_metadata_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    assert!(
        store
            .compaction_sources_current("ws", "thread", &[reference.clone()])
            .await
            .unwrap()
    );
    assert!(
        !store
            .compaction_sources_current("other", "thread", &[reference.clone()])
            .await
            .unwrap()
    );
    store
        .database_connection()
        .execute_unprepared(
            "UPDATE turn_event SET payload=to_json('edited'::text) WHERE id='current-source'",
        )
        .await
        .unwrap();
    assert!(
        !store
            .compaction_sources_current("ws", "thread", &[reference])
            .await
            .unwrap()
    );
    let updated = store
        .compaction_source_metadata_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    assert!(
        store
            .compaction_sources_current("ws", "thread", &[updated.clone()])
            .await
            .unwrap()
    );
    store
        .database_connection()
        .execute_unprepared("DELETE FROM turn_event WHERE id='current-source'")
        .await
        .unwrap();
    assert!(
        !store
            .compaction_sources_current("ws", "thread", &[updated])
            .await
            .unwrap()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn input_projection_uses_accepted_input_order_instead_of_insertion_order() {
    let store = store().await;
    let mut insertion_fence = None;
    for (id, index) in [("later-input", 1_i64), ("first-input", 0_i64)] {
        store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ($1,'turn',$2,'text','fixture','{}',CURRENT_TIMESTAMP)", [id.into(),index.into()])).await.unwrap();
        if index == 1 {
            insertion_fence = Some(store.compaction_history_read_fence().await.unwrap());
        }
    }
    let frozen = store
        .compaction_source_metadata_page_at_fence(
            "ws",
            "thread",
            "turn",
            PagedSource::Input,
            0,
            insertion_fence.unwrap().input_order,
        )
        .await
        .unwrap();
    assert_eq!(
        frozen.entries.len(),
        1,
        "a late lower input index must not enter the captured snapshot"
    );
    assert_eq!(frozen.entries[0].reference.id, "later-input");
    let page = store
        .compaction_source_metadata_page("ws", "thread", "turn", PagedSource::Input, 0)
        .await
        .unwrap();
    assert_eq!(
        page.entries
            .iter()
            .map(|row| row.reference.id.as_str())
            .collect::<Vec<_>>(),
        vec!["first-input", "later-input"]
    );
    let next = store
        .compaction_source_metadata_page(
            "ws",
            "thread",
            "turn",
            PagedSource::Input,
            page.entries[0].sequence,
        )
        .await
        .unwrap();
    assert_eq!(next.entries.len(), 1);
    assert_eq!(next.entries[0].reference.id, "later-input");
}

#[pioneer_postgres_test::postgres_test]
async fn frozen_history_is_paged_immutable_scoped_and_restart_safe() {
    use pioneer_compaction::frozen::{FrozenHistoryRef, FrozenMessageRef};
    let store = store().await;
    let descriptor = FrozenHistoryRef {
        format: 1,
        manifest_id: "frozen-manifest".into(),
        messages: 130,
        identity_sha256: "a".repeat(64),
    };
    let messages = (0..130)
        .map(|i| FrozenMessageRef {
            logical_turn_id: Some("command-turn".into()),
            context_thread: None,
            source_thread: "thread".into(),
            unit_id: format!("unit-{i}"),
            sources: vec![SourceRef {
                scope: "event:turn".into(),
                id: format!("event-{i}"),
                version: "event-revision:1".into(),
            }],
            event_input_role: None,
            source_aliases: vec![],
            ambiguous_input_aliases: vec![],
            publication_aliases: None,
            inherited: false,
            complete: true,
            protected_input: false,
            wire_sha256: "b".repeat(64),
            replay_source: None,
            tool_item_id: None,
            tool_call_id: None,
            tool_name: None,
        })
        .collect::<Vec<_>>();
    store
        .compaction_begin_frozen_history("ws", "thread", &descriptor)
        .await
        .unwrap();
    assert!(
        !store
            .compaction_finish_frozen_history("ws", "thread", &descriptor)
            .await
            .unwrap()
    );
    store
        .compaction_append_frozen_history(
            "ws",
            "thread",
            &descriptor.manifest_id,
            0,
            &messages[..128],
        )
        .await
        .unwrap();
    // R1: the committed prefix is visible in the logical view before ready;
    // physical allocation beyond next is not part of the logical stream.
    let visible = store
        .database_connection()
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT COUNT(*) AS n FROM compaction_frozen_message WHERE manifest_id=$1",
            [descriptor.manifest_id.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "n")
        .unwrap();
    assert_eq!(visible, 128);
    assert!(
        store
            .compaction_frozen_history_owner("ws", &descriptor)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_frozen_history_page("ws", "thread", &descriptor.manifest_id, 0)
            .await
            .unwrap()
            .is_empty()
    );
    // Restart retries the same immutable metadata, then finishes the remaining quantum.
    store
        .compaction_begin_frozen_history("ws", "thread", &descriptor)
        .await
        .unwrap();
    store
        .compaction_append_frozen_history(
            "ws",
            "thread",
            &descriptor.manifest_id,
            0,
            &messages[..128],
        )
        .await
        .unwrap();
    store
        .compaction_append_frozen_history(
            "ws",
            "thread",
            &descriptor.manifest_id,
            128,
            &messages[128..],
        )
        .await
        .unwrap();
    assert!(
        store
            .compaction_finish_frozen_history("ws", "thread", &descriptor)
            .await
            .unwrap()
    );
    assert!(
        store
            .compaction_finish_frozen_history("ws", "thread", &descriptor)
            .await
            .unwrap()
    );
    let page = store
        .compaction_frozen_history_page("ws", "thread", &descriptor.manifest_id, 0)
        .await
        .unwrap();
    assert_eq!(page, messages[..128]);
    assert_eq!(
        store
            .compaction_frozen_history_page("ws", "thread", &descriptor.manifest_id, 128)
            .await
            .unwrap(),
        messages[128..]
    );
    assert!(
        store
            .compaction_frozen_history_page("other", "thread", &descriptor.manifest_id, 0)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .compaction_frozen_history_owner("other", &descriptor)
            .await
            .unwrap()
            .is_none()
    );
    let mut changed = messages[0].clone();
    changed.wire_sha256 = "c".repeat(64);
    assert!(
        store
            .compaction_append_frozen_history(
                "ws",
                "thread",
                &descriptor.manifest_id,
                0,
                &[changed]
            )
            .await
            .is_err()
    );
    assert_eq!(
        store
            .compaction_frozen_history_page("ws", "thread", &descriptor.manifest_id, 0)
            .await
            .unwrap(),
        page
    );
    let mut collision = descriptor.clone();
    collision.messages = 1;
    assert!(
        store
            .compaction_begin_frozen_history("ws", "thread", &collision)
            .await
            .is_err()
    );
    assert!(
        store
            .compaction_append_frozen_history(
                "ws",
                "thread",
                &descriptor.manifest_id,
                130,
                &messages[..1]
            )
            .await
            .is_err()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn exact_reference_scope_requires_workspace_and_current_revision() {
    let store = store().await;
    source(&store, "lookup-source", 1, "original").await;
    let reference = store
        .compaction_source_metadata_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    assert_eq!(
        store
            .compaction_reference_thread("ws", &reference)
            .await
            .unwrap()
            .as_deref(),
        Some("thread")
    );
    assert!(
        store
            .compaction_reference_thread("other-workspace", &reference)
            .await
            .unwrap()
            .is_none()
    );
    store
        .database_connection()
        .execute_unprepared(
            "UPDATE turn_event SET payload=to_json('edited'::text) WHERE id='lookup-source'",
        )
        .await
        .unwrap();
    assert!(
        store
            .compaction_reference_thread("ws", &reference)
            .await
            .unwrap()
            .is_none()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn inherited_source_epoch_fences_commit_but_parent_append_does_not() {
    for edited in [false, true] {
        let store = store().await;
        let db = store.database_connection();
        db.execute_unprepared("INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('parent','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
        db.execute_unprepared("INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('parent-turn','parent','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
        db.execute_unprepared("INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('parent-source','parent','parent-turn',1,'fixture',to_json('reference fact'::text),CURRENT_TIMESTAMP)").await.unwrap();
        let own = source(&store, "own", 1, "own completed work").await;
        let parent_epoch = store
            .compaction_projection_version("ws", "parent")
            .await
            .unwrap();
        let cp = candidate_with_epochs(
            &store,
            "guarded",
            None,
            &own,
            std::collections::BTreeMap::from([("parent".into(), parent_epoch)]),
        )
        .await;
        if edited {
            db.execute_unprepared(
                "UPDATE turn_event SET payload=to_json('edited fact'::text) WHERE id='parent-source'",
            )
            .await
            .unwrap();
        } else {
            db.execute_unprepared("INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('parent-append','parent','parent-turn',2,'fixture',to_json('later work'::text),CURRENT_TIMESTAMP)").await.unwrap();
        }
        let result = store.compaction_apply(&cp, None, &[own]).await.unwrap();
        assert_eq!(
            result,
            if edited {
                CommitOutcome::Stale
            } else {
                CommitOutcome::Applied
            }
        );
        assert_eq!(
            store.compaction_head("owner").await.unwrap().is_some(),
            !edited
        );
        assert!(store.compaction_checkpoint(&cp.id).await.unwrap().is_some());
    }
}

#[pioneer_postgres_test::postgres_test]
async fn source_epoch_admission_rejects_changed_missing_and_foreign_scope() {
    let store = store().await;
    let own = source(&store, "own", 1, "completed work").await;
    candidate(&store, "original", None, &own).await;
    store
        .database_connection()
        .execute_unprepared(
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('other','other',TRUE,FALSE)",
        )
        .await
        .unwrap();
    store.database_connection().execute_unprepared("INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('foreign','other','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    let original: OperationSnapshot = serde_json::from_str(
        &store
            .compaction_operation("original")
            .await
            .unwrap()
            .unwrap()
            .snapshot,
    )
    .unwrap();
    for (index, (thread, epoch)) in [("missing", 0), ("thread", 99), ("foreign", 0)]
        .into_iter()
        .enumerate()
    {
        let mut snapshot = original.clone();
        snapshot.id = format!("invalid-{index}");
        snapshot.plan.fingerprint = snapshot.id.clone();
        snapshot.source_epochs = std::collections::BTreeMap::from([(thread.into(), epoch)]);
        assert!(
            store
                .compaction_admit("ws", "thread", &snapshot)
                .await
                .is_err()
        );
        assert!(
            store
                .compaction_operation(&snapshot.id)
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[pioneer_postgres_test::postgres_test]
async fn task_basis_scope_requires_exact_execution_snapshot_and_lineage() {
    let store = store().await;
    let db = store.database_connection();
    for sql in [
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('child','ws','','agent','m','p','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task','ws','thread','thread','thread','turn','agent','running','Task','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run','task','run',1,1,'running','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('rt','task','run','child','child-turn','initial',0,1,'in_progress',CURRENT_TIMESTAMP)",
        "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES ('child','thread','thread',1,CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    assert_eq!(
        store
            .compaction_task_basis_thread("ws", "child", "child-turn")
            .await
            .unwrap(),
        None
    );
    db.execute_unprepared("WITH frozen_root_fixture(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at) AS (VALUES ('run','task','ws','thread','[]',CURRENT_TIMESTAMP)) INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,frozen_manifest_id) SELECT run_id,task_id,workspace_id,conversation_thread_id,history_json,created_at,CASE WHEN history_json IS JSON OBJECT THEN (history_json)::json->>'manifest_id' END FROM frozen_root_fixture").await.unwrap();
    assert_eq!(
        store
            .compaction_task_basis_thread("ws", "child", "child-turn")
            .await
            .unwrap()
            .as_deref(),
        Some("thread")
    );
    let before_input = store.compaction_history_read_fence().await.unwrap();
    db.execute_unprepared("INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('child-turn','child','in_progress','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('child-input','child-turn',0,'text','own','{}',CURRENT_TIMESTAMP)").await.unwrap();
    let after_input = store.compaction_history_read_fence().await.unwrap();
    assert!(
        store
            .compaction_latest_task_basis_turn("ws", "child", &before_input)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .compaction_latest_task_basis_turn("ws", "child", &after_input)
            .await
            .unwrap()
            .as_deref(),
        Some("child-turn")
    );
    assert!(
        store
            .compaction_latest_task_basis_turn("other", "child", &after_input)
            .await
            .unwrap()
            .is_none()
    );
    let basis = store
        .compaction_task_basis_snapshot("ws", "child", "child-turn")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(basis.parent_thread, "thread");
    assert_eq!(basis.run_id, "run");
    assert_eq!(basis.history_json, "[]");
    // A UTF-8 scalar crosses the 256KiB fragment boundary; decode only after
    // releasing the reader and joining complete bounded byte fragments.
    let large = format!(
        "{}🦀{}",
        "x".repeat(SOURCE_PAGE_BYTES - 1),
        "история".repeat(50_000)
    );
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "WITH root_replacement(history_json) AS (VALUES ($1)) UPDATE task_run_conversation_snapshot SET history_json=(SELECT history_json FROM root_replacement),frozen_manifest_id=(SELECT CASE WHEN history_json IS JSON OBJECT THEN (history_json)::json->>'manifest_id' END FROM root_replacement) WHERE run_id='run'",
        [large.clone().into()],
    ))
    .await
    .unwrap();
    assert_eq!(
        store
            .compaction_task_basis_snapshot("ws", "child", "child-turn")
            .await
            .unwrap()
            .unwrap()
            .history_json,
        large
    );
    for (workspace, thread, turn) in [
        ("other", "child", "child-turn"),
        ("ws", "thread", "child-turn"),
        ("ws", "child", "other-turn"),
    ] {
        assert!(
            store
                .compaction_task_basis_snapshot(workspace, thread, turn)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .compaction_task_basis_thread(workspace, thread, turn)
                .await
                .unwrap(),
            None
        );
    }
    db.execute_unprepared(
        "UPDATE thread_lineage SET parent_thread_id='child' WHERE child_thread_id='child'",
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .compaction_task_basis_thread("ws", "child", "child-turn")
            .await
            .unwrap(),
        None
    );
    db.execute_unprepared(
        "UPDATE thread_lineage SET parent_thread_id='thread' WHERE child_thread_id='child'",
    )
    .await
    .unwrap();
    db.execute_unprepared(
        "UPDATE task_run_conversation_snapshot SET task_id='unrelated' WHERE run_id='run'",
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .compaction_task_basis_thread("ws", "child", "child-turn")
            .await
            .unwrap(),
        None
    );
}

async fn admit_import_operation(
    store: &CompactionStore,
    id: &str,
    thread: &str,
    turn: &str,
) -> OperationSnapshot {
    let snapshot = OperationSnapshot {
        id: id.into(),
        owner: format!("owner-{id}"),
        expected_checkpoint: None,
        projection_version: store
            .compaction_projection_version("ws", thread)
            .await
            .unwrap(),
        source_epochs: std::collections::BTreeMap::new(),
        admission: CompactionSettings::default()
            .admit(
                &ModelSelection {
                    service_mode: None,
                    context_authority: None,
                    context_window: None,
                    transport: Transport::Api,
                    instance: "p".into(),
                    model: "m".into(),
                    effort: None,
                },
                None,
                0,
            )
            .unwrap(),
        plan: CompactionPlan {
            mode: CompactionMode::Normal,
            coverage_domain: pioneer_compaction::CoverageDomain::OwnContribution,
            compact: vec![],
            retain: vec![],
            coverage: vec![],
            fingerprint: id.into(),
        },
    };
    store
        .compaction_admit("ws", thread, &snapshot)
        .await
        .unwrap();
    store
        .compaction_bind_execution_turn(id, turn)
        .await
        .unwrap();
    snapshot
}

async fn ready_import_operation(
    store: &CompactionStore,
    snapshot: &OperationSnapshot,
    source_thread: &str,
    source: &SourceRef,
) -> pioneer_compaction::runner::RunnerState {
    ready_operation(
        store,
        snapshot,
        &[(source_thread.to_owned(), source.clone())],
    )
    .await
}

async fn ready_operation(
    store: &CompactionStore,
    snapshot: &OperationSnapshot,
    sources: &[(String, SourceRef)],
) -> pioneer_compaction::runner::RunnerState {
    ready_operation_with_references(store, snapshot, sources, &[]).await
}

async fn ready_operation_with_references(
    store: &CompactionStore,
    snapshot: &OperationSnapshot,
    sources: &[(String, SourceRef)],
    references: &[(String, SourceRef)],
) -> pioneer_compaction::runner::RunnerState {
    use pioneer_compaction::runner::{RunnerState, SourceCursor};
    let op = &snapshot.id;
    // Seed the exact original legacy raw plan before runner execution. An
    // empty frozen-style plan is not a valid missing-binding contract.
    if !sources.is_empty()
        && snapshot.plan.compact.is_empty()
        && store
            .compaction_bound_source_projection(op)
            .await
            .unwrap()
            .is_none()
    {
        let mut raw = snapshot.clone();
        raw.plan.compact = (0..sources.len()).collect();
        raw.plan.coverage = sources.iter().map(|(_, source)| source.clone()).collect();
        store
            .database_connection()
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE compaction_operation SET snapshot=$2 WHERE id=$1",
                [
                    op.clone().into(),
                    serde_json::to_string(&raw).unwrap().into(),
                ],
            ))
            .await
            .unwrap();
    }
    let budget = ModelBudget::new(None, None, None);
    store
        .compaction_prepare_runner(op, &budget, sources.len() as u64, references.len() as u64)
        .await
        .unwrap();
    let manifest = sources
        .iter()
        .map(|source| (false, source))
        .chain(references.iter().map(|source| (true, source)))
        .enumerate()
        .map(
            |(ordinal, (reference_only, (thread_id, source)))| ManifestEntry {
                ordinal: ordinal as u64,
                unit: ordinal as u64,
                reference_only,
                thread_id: thread_id.clone(),
                source: source.clone(),
            },
        )
        .collect::<Vec<_>>();
    store
        .compaction_append_manifest(op, &manifest)
        .await
        .unwrap();
    let initial = RunnerState::new(snapshot.admission.deadline_ms, &budget, 1000, None).unwrap();
    store
        .compaction_activate_runner(op, &initial)
        .await
        .unwrap();
    let attempt = initial.claim(1).unwrap();
    assert!(
        store
            .compaction_runner_transition(op, initial.generation, &attempt, None)
            .await
            .unwrap()
    );
    let checkpoint = Checkpoint {
        id: format!("checkpoint-{op}"),
        operation_id: op.clone(),
        format_version: 1,
        owner: snapshot.owner.clone(),
        previous: None,
        coverage: sources.iter().map(|(_, source)| source.clone()).collect(),
        summary: "fixture summary".into(),
        selection: snapshot.admission.selection.clone(),
        projection_version: snapshot.projection_version,
    };
    let next = attempt
        .candidate(
            1,
            checkpoint.id.clone(),
            SourceCursor {
                unit: sources.len() as u64,
                ..Default::default()
            },
            true,
            2,
        )
        .unwrap();
    assert!(
        store
            .compaction_runner_transition(op, attempt.generation, &next, Some(&checkpoint))
            .await
            .unwrap()
    );
    let ready = next.candidate_checked(true).unwrap();
    assert!(
        store
            .compaction_runner_transition(op, next.generation, &ready, None)
            .await
            .unwrap()
    );
    ready
}

#[pioneer_postgres_test::postgres_test]
async fn runner_commit_treats_published_checkpoint_as_independent_after_admission() {
    for delete in [false, true] {
        let store = store().await;
        let mut leaf = source(&store, "checkpoint-leaf", 1, "accepted H").await;
        leaf.revision = Some(1);
        let checkpoint = candidate_with_manifest(&store, "source-checkpoint", None, &leaf).await;
        assert_eq!(
            store
                .compaction_apply(&checkpoint, None, std::slice::from_ref(&leaf))
                .await
                .unwrap(),
            CommitOutcome::Applied
        );
        let mut later_leaf =
            source(&store, "later-checkpoint-leaf", 2, "later accepted work").await;
        later_leaf.revision = Some(1);
        let head = candidate_with_manifest(
            &store,
            "source-checkpoint-head",
            Some(&checkpoint.id),
            &later_leaf,
        )
        .await;
        assert_eq!(
            store
                .compaction_apply(
                    &head,
                    Some(&checkpoint.id),
                    std::slice::from_ref(&later_leaf)
                )
                .await
                .unwrap(),
            CommitOutcome::Applied
        );
        let checkpoint_source = store
            .compaction_checkpoint_source("ws", "thread", &head.id)
            .await
            .unwrap()
            .unwrap();
        let operation =
            admit_import_operation(&store, "own-checkpoint-target", "thread", "turn").await;
        let ready = ready_import_operation(&store, &operation, "thread", &checkpoint_source).await;

        store
            .database_connection()
            .execute_unprepared(if delete {
                "DELETE FROM turn_event WHERE id='checkpoint-leaf'"
            } else {
                "UPDATE turn_event SET payload=to_json('changed H'::text) WHERE id='checkpoint-leaf'"
            })
            .await
            .unwrap();
        assert!(
            store
                .compaction_manifest_sources_current(&operation.id)
                .await
                .unwrap(),
            "a published checkpoint must not depend on its historical leaf"
        );
        assert_eq!(
            store
                .compaction_apply_runner(&operation.id, &ready, None)
                .await
                .unwrap(),
            CommitOutcome::Applied
        );
        let third = store
            .compaction_checkpoint(&format!("checkpoint-{}", operation.id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            third.coverage,
            vec![checkpoint_source],
            "S3 must record S2 as its atomic direct input"
        );
        let s3_edges = store
            .compaction_checkpoint_edges(&third.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(s3_edges.coverage.len(), 1);
        assert_eq!(s3_edges.coverage[0].source.id, head.id);
        assert_eq!(
            store
                .compaction_checkpoint_edges(&head.id)
                .await
                .unwrap()
                .unwrap()
                .previous
                .as_deref(),
            Some(checkpoint.id.as_str())
        );
        assert!(
            store
                .compaction_checkpoint_source("ws", "thread", &third.id)
                .await
                .unwrap()
                .is_some(),
            "S3 was not published after its predecessor's old leaf changed"
        );
    }
}

#[pioneer_postgres_test::postgres_test]
async fn reference_only_checkpoint_does_not_revalidate_historical_leaves() {
    let store = store().await;
    let mut leaf = source(&store, "reference-leaf", 1, "reference H").await;
    leaf.revision = Some(1);
    let checkpoint = candidate(&store, "reference-checkpoint", None, &leaf).await;
    assert_eq!(
        store
            .compaction_apply(&checkpoint, None, std::slice::from_ref(&leaf))
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    let checkpoint_source = store
        .compaction_checkpoint_source("ws", "thread", &checkpoint.id)
        .await
        .unwrap()
        .unwrap();
    let mut selected = source(&store, "selected-leaf", 2, "selected work").await;
    selected.revision = Some(1);
    let selected_source = selected.reference();
    let operation = admit_import_operation(&store, "reference-target", "thread", "turn").await;
    let ready = ready_operation_with_references(
        &store,
        &operation,
        &[("thread".into(), selected_source.clone())],
        &[("thread".into(), checkpoint_source)],
    )
    .await;
    assert!(
        store
            .compaction_manifest_sources_current(&operation.id)
            .await
            .unwrap(),
        "valid selected work plus a current reference-only checkpoint must reach commit validation"
    );
    let candidate = store
        .compaction_checkpoint(&format!("checkpoint-{}", operation.id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(candidate.coverage, vec![selected_source]);
    store
        .database_connection()
        .execute_unprepared("DELETE FROM turn_event WHERE id='reference-leaf'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_manifest_sources_current(&operation.id)
            .await
            .unwrap(),
        "reference-only published checkpoint must survive historical deletion"
    );
    assert_eq!(
        store
            .compaction_apply_runner(&operation.id, &ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
}

#[pioneer_postgres_test::postgres_test]
async fn compaction_schema_preserves_byte_checks_keys_and_creation_sequence() {
    let store = store().await;
    let db = store.database_connection();
    db.execute_unprepared(
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count) VALUES ('manifest','ws','thread','fixture',1)",
    ).await.unwrap();
    // The CHECK measures UTF-8 bytes, not characters, and enforces the upper bound.
    for (table, columns, values) in [
        (
            "compaction_frozen_message_data",
            "manifest_id,ordinal,reference_json,bytes",
            "'manifest',0,$1,$2",
        ),
        (
            "compaction_frozen_import_data",
            "manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes",
            "'manifest',0,0,'scope','source','v1','thread',$1,$2",
        ),
    ] {
        let insert = format!("INSERT INTO {table}({columns}) VALUES ({values})");
        for (payload, bytes) in [
            ("я".to_owned(), 1_i64),
            ("".into(), -1),
            ("x".repeat(262145), 262145),
        ] {
            assert!(
                db.execute_raw(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    insert.clone(),
                    [payload.into(), bytes.into()],
                ))
                .await
                .is_err()
            );
        }
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            insert.clone(),
            ["я".into(), 2_i64.into()],
        ))
        .await
        .unwrap();
        assert!(
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                insert,
                ["я".into(), 2_i64.into()],
            ))
            .await
            .is_err(),
            "composite primary key must reject duplicate ordinals"
        );
    }
    assert!(db.execute_unprepared("INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES ('manifest',1,0,'scope','source','v1','thread','{}',2)").await.is_err(), "source identity must remain unique within the manifest message");
    assert!(db.execute_unprepared("INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES ('missing',0,'{}',2)").await.is_err());
    db.execute_unprepared("DELETE FROM compaction_frozen_history WHERE id='manifest'")
        .await
        .unwrap();
    for table in [
        "compaction_frozen_message_data",
        "compaction_frozen_import_data",
    ] {
        let row = db
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                format!("SELECT count(*) AS n FROM {table}"),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.try_get::<i64>("", "n").unwrap(),
            0,
            "manifest deletion must cascade"
        );
    }
    let previous = db
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT sequence FROM compaction_turn_creation WHERE turn_id='turn'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "sequence")
        .unwrap();
    db.execute_unprepared("DELETE FROM compaction_turn_creation WHERE turn_id='turn'")
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO compaction_turn_creation(turn_id) VALUES ('turn')")
        .await
        .unwrap();
    let next = db
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT sequence FROM compaction_turn_creation WHERE turn_id='turn'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "sequence")
        .unwrap();
    assert!(
        next > previous,
        "AUTOINCREMENT must not reuse deleted creation ordinals"
    );
}

fn shared_refs(count: usize) -> Vec<pioneer_compaction::frozen::FrozenMessageRef> {
    (0..count)
        .map(|i| pioneer_compaction::frozen::FrozenMessageRef {
            logical_turn_id: Some("turn".into()),
            context_thread: None,
            source_thread: "thread".into(),
            unit_id: format!("u{i}"),
            sources: vec![SourceRef {
                scope: "event:turn".into(),
                id: format!("e{i}"),
                version: "event-revision:1".into(),
            }],
            event_input_role: None,
            source_aliases: vec![],
            ambiguous_input_aliases: vec![],
            publication_aliases: None,
            inherited: false,
            complete: true,
            protected_input: false,
            wire_sha256: "a".repeat(64),
            replay_source: None,
            tool_item_id: None,
            tool_call_id: None,
            tool_name: None,
        })
        .collect()
}

fn shared_descriptor(
    id: &str,
    refs: &[pioneer_compaction::frozen::FrozenMessageRef],
) -> pioneer_compaction::frozen::FrozenHistoryRef {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    for r in refs {
        let b = serde_json::to_vec(r).unwrap();
        digest.update((b.len() as u64).to_be_bytes());
        digest.update(b);
    }
    pioneer_compaction::frozen::FrozenHistoryRef {
        format: 1,
        manifest_id: id.into(),
        messages: refs.len() as u64,
        identity_sha256: hex::encode(digest.finalize()),
    }
}

async fn shared_capture(
    store: &CompactionStore,
    id: &str,
    refs: &[pioneer_compaction::frozen::FrozenMessageRef],
    shared: bool,
) -> pioneer_compaction::frozen::FrozenHistoryRef {
    let d = shared_descriptor(id, refs);
    store
        .compaction_begin_frozen_history("ws", "thread", &d)
        .await
        .unwrap();
    let start = if shared {
        store
            .compaction_share_frozen_prefix("ws", "thread", id, refs, &[])
            .await
            .unwrap()
            .0 as usize
    } else {
        0
    };
    for (i, page) in refs[start..].chunks(128).enumerate() {
        store
            .compaction_append_frozen_history("ws", "thread", id, (start + i * 128) as u64, page)
            .await
            .unwrap();
    }
    assert!(
        store
            .compaction_finish_frozen_history("ws", "thread", &d)
            .await
            .unwrap()
    );
    d
}

async fn shared_read(
    store: &CompactionStore,
    id: &str,
) -> Vec<pioneer_compaction::frozen::FrozenMessageRef> {
    let mut result = Vec::new();
    loop {
        let page = store
            .compaction_frozen_history_page("ws", "thread", id, result.len() as u64)
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        result.extend(page);
    }
    result
}

async fn frozen_count(store: &CompactionStore, table: &str) -> i64 {
    store
        .database_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            format!("SELECT count(*) AS n FROM {table}"),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

#[pioneer_postgres_test::postgres_test]
async fn shared_frozen_growth_is_linear_and_old_revisions_are_unchanged() {
    let store = store().await;
    let refs = shared_refs(400);
    for n in (10..=400).step_by(10) {
        shared_capture(&store, &format!("s{n:04}"), &refs[..n], true).await;
    }
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        400
    );
    assert_eq!(frozen_count(&store, "compaction_frozen_span").await, 40);
    assert_eq!(shared_read(&store, "s0010").await, refs[..10]);
    assert_eq!(shared_read(&store, "s0400").await, refs);
    let plan = store.database_connection().query_all_raw(Statement::from_string(DbBackend::Postgres,
        "EXPLAIN (COSTS OFF) SELECT reference_json FROM compaction_frozen_message WHERE manifest_id='s0400' AND ordinal>=200 AND ordinal<210 ORDER BY ordinal"))
        .await.unwrap().into_iter().map(|row|row.try_get::<String>("","QUERY PLAN").unwrap()).collect::<Vec<_>>();
    assert!(
        !plan
            .iter()
            .any(|line| line.contains("Seq Scan on compaction_frozen_message_data")),
        "range read must not scan the full payload table: {plan:?}"
    );

    let d = shared_descriptor("unused", &refs);
    let found = store
        .compaction_equivalent_frozen_history("ws", "thread", &d, 0, EMPTY_FROZEN_IMPORT_SHA256)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.manifest_id, "s0400");
    assert!(
        store
            .compaction_equivalent_frozen_history(
                "foreign",
                "thread",
                &d,
                0,
                EMPTY_FROZEN_IMPORT_SHA256
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_equivalent_frozen_history("ws", "thread", &d, 1, &"b".repeat(64))
            .await
            .unwrap()
            .is_none()
    );
    let mut changed = refs.clone();
    changed[200].wire_sha256 = "b".repeat(64);
    shared_capture(&store, "branch", &changed, true).await;
    assert_eq!(shared_read(&store, "branch").await, changed);
    assert_eq!(shared_read(&store, "s0400").await, refs);
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        600
    );
    store
        .database_connection()
        .execute_unprepared("DELETE FROM thread WHERE id='thread'")
        .await
        .unwrap();
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        0
    );
    assert_eq!(frozen_count(&store, "compaction_frozen_span").await, 0);
}

#[pioneer_postgres_test::postgres_test]
async fn legacy_frozen_duplicates_convert_incrementally_without_changing_ids_or_reads() {
    let mut frozen_conversion_progress = FrozenStorageLifetimeProgress::default();
    let store = store().await;
    let refs = shared_refs(300);
    for id in ["a", "b", "c"] {
        shared_capture(&store, id, &refs, false).await;
    }
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        900
    );
    // Reusing a published legacy capture must not hide it from conversion.
    assert_eq!(
        store
            .compaction_share_frozen_prefix("ws", "thread", "b", &refs, &[])
            .await
            .unwrap(),
        (300, 0)
    );

    let mut quanta = 0;
    while store
        .compact_frozen_storage_quantum(&mut frozen_conversion_progress)
        .await
        .unwrap()
    {
        quanta += 1;
        assert!(quanta < 512);
        for id in ["a", "b", "c"] {
            assert_eq!(shared_read(&store, id).await, refs);
        }
    }
    assert!(quanta > 10);
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        300
    );
    assert_eq!(frozen_count(&store, "compaction_frozen_history").await, 3);
    let descriptor = shared_descriptor("b", &refs);
    assert_eq!(
        store
            .compaction_frozen_history_owner("ws", &descriptor)
            .await
            .unwrap(),
        Some("thread".into())
    );
    assert!(
        !store
            .compact_frozen_storage_quantum(&mut frozen_conversion_progress)
            .await
            .unwrap()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn old_layout_sizes_and_new_shared_payload_keep_exact_logical_stream() {
    let store = store().await;
    let refs = shared_refs(260);
    shared_capture(&store, "base", &refs, true).await;
    let descriptor = shared_capture(&store, "legacy", &refs, false).await;
    let sizes = store.database_connection().query_all_raw(Statement::from_string(DbBackend::Postgres,"SELECT ordinal,bytes FROM compaction_frozen_message WHERE manifest_id='legacy' ORDER BY ordinal")).await.unwrap();
    assert_eq!(sizes.len(), refs.len());
    let mut progress = FrozenStorageLifetimeProgress::default();
    for _ in 0..512 {
        store
            .compact_frozen_storage_quantum(&mut progress)
            .await
            .unwrap();
    }
    assert_eq!(shared_read(&store, "legacy").await, refs);
    let after = store.database_connection().query_all_raw(Statement::from_string(DbBackend::Postgres,"SELECT ordinal,bytes FROM compaction_frozen_message WHERE manifest_id='legacy' ORDER BY ordinal")).await.unwrap();
    assert_eq!(
        sizes
            .iter()
            .map(|r| (
                r.try_get::<i64>("", "ordinal").unwrap(),
                r.try_get::<i64>("", "bytes").unwrap()
            ))
            .collect::<Vec<_>>(),
        after
            .iter()
            .map(|r| (
                r.try_get::<i64>("", "ordinal").unwrap(),
                r.try_get::<i64>("", "bytes").unwrap()
            ))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        260
    );
    assert_eq!(
        store
            .compaction_frozen_history_owner("ws", &descriptor)
            .await
            .unwrap(),
        Some("thread".into())
    );
}

#[pioneer_postgres_test::postgres_test]
async fn duplicate_cleanup_keeps_understated_physical_bytes_and_advances_after_layout_switch() {
    let store = store().await;
    let refs = shared_refs(2);
    shared_capture(&store, "base", &refs, true).await;
    shared_capture(&store, "legacy", &refs, false).await;
    let mut progress = FrozenStorageLifetimeProgress::default();
    let mut activated = false;
    for _ in 0..128 {
        store
            .compact_frozen_storage_quantum(&mut progress)
            .await
            .unwrap();
        let row=store.database_connection().query_one_raw(Statement::from_string(DbBackend::Postgres,"SELECT active,cleanup_next,cleanup_to FROM compaction_frozen_layout WHERE manifest_id='legacy' AND kind=0")).await.unwrap();
        if let Some(row) = row {
            if row.try_get::<i64>("", "active").unwrap() == 1
                && row.try_get::<i64>("", "cleanup_next").unwrap() == 0
                && row.try_get::<i64>("", "cleanup_to").unwrap() == 2
            {
                activated = true;
                break;
            }
        }
    }
    assert!(
        activated,
        "fixture must pause after real activation and before cleanup"
    );
    // Seed legacy corruption only; tested cleanup runs with CHECKs restored.
    let oversized = serde_json::json!({"padding":"x".repeat(300_000)}).to_string();
    let corrupted_payload = oversized.clone();
    let tx = store.database_connection().begin().await.unwrap();
    tx.execute_unprepared("ALTER TABLE compaction_frozen_message_data DROP CONSTRAINT compaction_frozen_message_data_check")
        .await
        .unwrap();
    tx.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,"UPDATE compaction_frozen_message_data SET reference_json=$1,bytes=2 WHERE manifest_id='legacy' AND ordinal=0",[corrupted_payload.into()])).await.unwrap();
    tx.execute_unprepared("ALTER TABLE compaction_frozen_message_data ADD CONSTRAINT compaction_frozen_message_data_check CHECK (bytes BETWEEN 0 AND 262144 AND octet_length(reference_json) = bytes) NOT VALID")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(
        store
            .compact_frozen_storage_quantum(&mut progress)
            .await
            .unwrap()
    );
    let state = store.database_connection().query_one_raw(Statement::from_string(DbBackend::Postgres,
        "SELECT active,failed,pending,cleanup_next,cleanup_to FROM compaction_frozen_layout WHERE manifest_id='legacy' AND kind=0")).await.unwrap().unwrap();
    assert_eq!(
        (
            state.try_get::<i64>("", "active").unwrap(),
            state.try_get::<i64>("", "failed").unwrap(),
            state.try_get::<i64>("", "pending").unwrap(),
            state.try_get::<i64>("", "cleanup_next").unwrap(),
            state.try_get::<i64>("", "cleanup_to").unwrap(),
        ),
        (1, 0, 0, 2, 2),
        "a bad redundant row must not quarantine the published layout"
    );
    let mut finished = false;
    for _ in 0..128 {
        if !store
            .compact_frozen_storage_quantum(&mut progress)
            .await
            .unwrap()
        {
            finished = true;
            break;
        }
    }
    assert!(finished, "the poison row must not cause endless retries");
    let remaining = store.database_connection().query_all_raw(Statement::from_string(DbBackend::Postgres,
        "SELECT ordinal,reference_json,bytes FROM compaction_frozen_message_data WHERE manifest_id='legacy' ORDER BY ordinal")).await.unwrap();
    assert_eq!(remaining.len(), 1, "the healthy duplicate must be deleted");
    assert_eq!(remaining[0].try_get::<i64>("", "ordinal").unwrap(), 0);
    assert_eq!(remaining[0].try_get::<i64>("", "bytes").unwrap(), 2);
    assert_eq!(
        remaining[0]
            .try_get::<String>("", "reference_json")
            .unwrap(),
        oversized,
        "cleanup must preserve the unsuitable physical row exactly"
    );
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        3
    );
    assert_eq!(shared_read(&store, "base").await, refs);
    assert_eq!(shared_read(&store, "legacy").await, refs);
}

#[pioneer_postgres_test::postgres_test]
async fn shared_frozen_append_rollback_and_concurrent_retry_preserve_one_sequence() {
    let store = store().await;
    let refs = shared_refs(25);
    let d = shared_descriptor("concurrent", &refs);
    store
        .compaction_begin_frozen_history("ws", "thread", &d)
        .await
        .unwrap();
    store
        .compaction_share_frozen_prefix("ws", "thread", &d.manifest_id, &refs, &[])
        .await
        .unwrap();
    let db = store.database_connection();
    db.execute_unprepared("CREATE FUNCTION reject_shared_append() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture rollback'; END $$; CREATE TRIGGER reject_shared_append BEFORE INSERT ON compaction_frozen_message_data FOR EACH ROW EXECUTE FUNCTION reject_shared_append()").await.unwrap();
    assert!(
        store
            .compaction_append_frozen_history("ws", "thread", &d.manifest_id, 0, &refs)
            .await
            .is_err()
    );
    assert_eq!(frozen_count(&store, "compaction_frozen_span").await, 0);
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        0
    );
    assert!(
        !store
            .compaction_finish_frozen_history("ws", "thread", &d)
            .await
            .unwrap()
    );
    db.execute_unprepared("DROP TRIGGER reject_shared_append ON compaction_frozen_message_data")
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        shared_capture(&store, "concurrent", &refs, true),
        shared_capture(&store, "concurrent", &refs, true)
    );
    assert_eq!(a, b);
    assert_eq!(shared_read(&store, "concurrent").await, refs);
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        25
    );
    assert_eq!(frozen_count(&store, "compaction_frozen_span").await, 1);
}

#[pioneer_postgres_test::postgres_test]
async fn shared_frozen_cleanup_rollback_and_poison_row_do_not_lose_other_history() {
    let mut frozen_conversion_progress = FrozenStorageLifetimeProgress::default();
    let store = store().await;
    let refs = shared_refs(20);
    shared_capture(&store, "a", &refs, true).await;
    shared_capture(&store, "b", &refs, false).await;
    let db = store.database_connection();
    db.execute_unprepared("CREATE FUNCTION reject_shared_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture cleanup rollback'; END $$; CREATE TRIGGER reject_shared_cleanup BEFORE DELETE ON compaction_frozen_message_data FOR EACH ROW EXECUTE FUNCTION reject_shared_cleanup()").await.unwrap();
    let mut failed = false;
    for _ in 0..128 {
        if store
            .compact_frozen_storage_quantum(&mut frozen_conversion_progress)
            .await
            .is_err()
        {
            failed = true;
            break;
        }
    }
    assert!(failed);
    assert_eq!(shared_read(&store, "b").await, refs);
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        40
    );
    db.execute_unprepared("DROP TRIGGER reject_shared_cleanup ON compaction_frozen_message_data")
        .await
        .unwrap();
    for _ in 0..128 {
        if !store
            .compact_frozen_storage_quantum(&mut frozen_conversion_progress)
            .await
            .unwrap()
        {
            break;
        }
    }
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        20
    );
    shared_capture(&store, "c-broken", &refs, false).await;
    shared_capture(&store, "d-good", &refs, false).await;
    db.execute_unprepared(
        "DELETE FROM compaction_frozen_message_data WHERE manifest_id='c-broken' AND ordinal=3",
    )
    .await
    .unwrap();
    let mut rejected = 0;
    for _ in 0..128 {
        match store
            .compact_frozen_storage_quantum(&mut frozen_conversion_progress)
            .await
        {
            Ok(false) => break,
            Ok(true) => {}
            Err(_) => rejected += 1,
        }
    }
    assert_eq!(rejected, 1);
    assert_eq!(shared_read(&store, "d-good").await, refs);
    assert_eq!(
        frozen_count(&store, "compaction_frozen_message_data").await,
        39
    );
}

#[pioneer_postgres_test::postgres_test]
async fn history_check_legacy_failures_remain_retained_without_optional_admission() {
    use pioneer_entity::compaction_history_check as check;
    use sea_orm::EntityTrait;
    let store = store().await.with_maintenance_access();
    let db = store.database_connection();
    db.execute_unprepared("UPDATE turn SET status='in_progress' WHERE id='turn'")
        .await
        .unwrap();
    store
        .compaction_enqueue_native_history_check("ws", "thread", "turn", r#""retained descriptor""#)
        .await
        .unwrap();
    db.execute_unprepared("UPDATE turn SET status='completed' WHERE id='turn'")
        .await
        .unwrap();
    for outcome in ["failed", "cancelled"] {
        db.execute_unprepared(&format!("UPDATE compaction_history_check SET managed=0,state='finished',outcome='{outcome}' WHERE turn_id='turn'")).await.unwrap();
        let before = check::Entity::find_by_id("turn")
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert!(
            store
                .compaction_due_history_checks(i64::MAX)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .compaction_claim_history_check("turn", before.revision, 0)
                .await
                .unwrap()
                .is_none()
        );
        let after = check::Entity::find_by_id("turn")
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            before, after,
            "policy upgrade must not spend or rewrite the legacy lifecycle budget"
        );
    }
}

#[pioneer_postgres_test::postgres_test]
async fn history_check_legacy_pages_do_not_reenter_optional_discovery() {
    use pioneer_entity::compaction_history_check as check;
    use sea_orm::EntityTrait;
    let store = store().await.with_maintenance_access();
    let db = store.database_connection();
    for n in 0..33 {
        db.execute_unprepared(&format!("INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('legacy-{n:02}','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)")).await.unwrap();
        db.execute_unprepared(&format!("INSERT INTO compaction_history_check(turn_id,state,outcome) VALUES ('legacy-{n:02}','finished','failed')")).await.unwrap();
    }
    let before = check::Entity::find().all(&db).await.unwrap();
    for _ in 0..2 {
        assert!(
            store
                .compaction_due_history_checks(i64::MAX)
                .await
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(
        check::Entity::find().all(&db).await.unwrap(),
        before,
        "discovery must leave legacy checks available to lifecycle inspection"
    );
}

// Frozen history lifetime regressions.
async fn checkpoint_proof_version(store: &CompactionStore, id: &str) -> i64 {
    store
        .database_connection()
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT proof_version FROM compaction_checkpoint WHERE id=$1",
            [id.into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "proof_version")
        .unwrap()
}

#[pioneer_postgres_test::postgres_test]
async fn raw_assertion_publication_seals_exact_empty_proofs_and_immutable_ownership() {
    let store = store().await;
    let assertion = source(&store, "raw-proof-source", 1, "original").await;
    let cp = candidate(&store, "raw-proof-op", None, &assertion).await;
    assert_eq!(checkpoint_proof_version(&store, &cp.id).await, 0);
    let ownership = store
        .compaction_manifest_page(&cp.operation_id, false, 0, 0)
        .await
        .unwrap();
    assert_eq!(ownership.len(), 1);
    assert_eq!(ownership[0].source, assertion.reference());
    assert_eq!(ownership[0].thread_id, "thread");
    // Genuine no-runner/no-manifest bootstrap still supports exact candidate
    // retry once historical ownership was established in the first save.
    store.compaction_save_candidate(&cp, 0).await.unwrap();
    let retried = store
        .compaction_manifest_page(&cp.operation_id, false, 0, 0)
        .await
        .unwrap();
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].ordinal, ownership[0].ordinal);
    assert_eq!(retried[0].unit, ownership[0].unit);
    assert_eq!(retried[0].source, ownership[0].source);
    assert_eq!(retried[0].thread_id, ownership[0].thread_id);
    assert_eq!(checkpoint_proof_version(&store, &cp.id).await, 0);
    assert_eq!(
        store
            .compaction_apply(&cp, None, &[assertion.clone()])
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    assert_eq!(checkpoint_proof_version(&store, &cp.id).await, 1);
    let db = store.database_connection();
    for table in [
        "compaction_checkpoint_replay_alias",
        "compaction_checkpoint_event_input",
        "compaction_checkpoint_import",
    ] {
        let n: i64 = db
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                format!("SELECT count(*) AS n FROM {table} WHERE checkpoint_id=$1"),
                [cp.id.clone().into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "n")
            .unwrap();
        assert_eq!(n, 0, "empty set is proven by marker, not row presence");
    }
    for sql in [
        "UPDATE compaction_checkpoint SET proof_version=0 WHERE id=$1",
        "UPDATE compaction_checkpoint SET previous='forged' WHERE id=$1",
        "DELETE FROM compaction_coverage WHERE checkpoint_id=$1",
        "INSERT INTO compaction_checkpoint_event_input(checkpoint_id,source_thread,source_scope,source_id,source_version,role) VALUES($1,'thread','event:turn','forged','event-revision:1','deleted')",
    ] {
        assert!(
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                sql,
                [cp.id.clone().into()]
            ))
            .await
            .is_err(),
            "sealed evidence/links must be immutable"
        );
    }
    assert!(
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "DELETE FROM compaction_manifest WHERE operation_id=$1",
            [cp.operation_id.clone().into()]
        ))
        .await
        .is_err()
    );
    assert_eq!(
        store
            .compaction_apply(&cp, None, &[assertion])
            .await
            .unwrap(),
        CommitOutcome::AlreadyApplied
    );
}

#[pioneer_postgres_test::postgres_test]
async fn raw_assertion_next_candidate_adds_unclaimed_original_source_and_publishes() {
    let store = store().await;
    let db = store.database_connection();
    let a = source(&store, "A", 1, "original A").await;
    let b = source(&store, "B", 2, "original B").await;
    let selection = ModelSelection {
        service_mode: None,
        context_authority: None,
        context_window: None,
        transport: Transport::Api,
        instance: "p".into(),
        model: "m".into(),
        effort: None,
    };
    let snapshot = OperationSnapshot {
        id: "incremental-raw-op".into(),
        owner: "owner".into(),
        expected_checkpoint: None,
        projection_version: 1,
        source_epochs: Default::default(),
        admission: CompactionSettings::default()
            .admit(&selection, None, 10)
            .unwrap(),
        plan: CompactionPlan {
            mode: CompactionMode::Normal,
            coverage_domain: CoverageDomain::OwnContribution,
            compact: vec![0, 1],
            retain: vec![],
            coverage: vec![a.reference(), b.reference()],
            fingerprint: "incremental-raw-op".into(),
        },
    };
    let admitted = store
        .compaction_admit("ws", "thread", &snapshot)
        .await
        .unwrap();
    // One genuine raw admission: no runner preparation, seeded ownership,
    // frozen binding, or later snapshot replacement supplies B.
    assert!(
        store
            .compaction_runner_plan(&snapshot.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_runner_state(&snapshot.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_bound_source_projection(&snapshot.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_manifest_page(&snapshot.id, false, 0, 0)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(admitted.snapshot, serde_json::to_string(&snapshot).unwrap());
    let k0 = Checkpoint {
        id: "raw-K0".into(),
        operation_id: snapshot.id.clone(),
        owner: snapshot.owner.clone(),
        previous: None,
        summary: "First portion, only A".into(),
        selection,
        coverage: vec![a.reference()],
        projection_version: 1,
        format_version: 1,
    };
    store.compaction_save_candidate(&k0, 0).await.unwrap();
    let first = store
        .compaction_manifest_page(&snapshot.id, false, 0, 0)
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].source, a.reference());
    assert_eq!(first[0].ordinal, 0);
    assert_eq!(first[0].unit, 0);
    assert_eq!(first[0].thread_id, "thread");
    assert!(!first[0].reference_only);
    store.compaction_save_candidate(&k0, 0).await.unwrap();
    let k1 = Checkpoint {
        id: "raw-K1".into(),
        previous: None, // independent of unpublished-previous publication
        summary: "Next portion, original A and B".into(),
        coverage: snapshot.plan.coverage.clone(),
        ..k0.clone()
    };
    // B's fresh insert and the candidate belong to one atomic write set.
    // A conflicting closed portion rolls that insert back too.
    assert!(store.compaction_save_candidate(&k1, 0).await.is_err());
    assert!(store.compaction_checkpoint(&k1.id).await.unwrap().is_none());
    let rolled_back = store
        .compaction_manifest_page(&snapshot.id, false, 0, 0)
        .await
        .unwrap();
    assert_eq!(rolled_back.len(), first.len());
    assert_eq!(rolled_back[0].source, first[0].source);
    assert_eq!(rolled_back[0].ordinal, first[0].ordinal);
    assert_eq!(rolled_back[0].unit, first[0].unit);
    assert_eq!(rolled_back[0].thread_id, first[0].thread_id);
    assert_eq!(
        store
            .compaction_operation(&snapshot.id)
            .await
            .unwrap()
            .unwrap()
            .next_portion,
        1
    );
    // Corrupt unclaimed staging is not absence, even without a runner. The
    // bootstrap above was genuine; these negative rows never seed a valid
    // manifest for K1 and must survive each rejected save unchanged.
    for (role, version) in [
        ("1", b.reference().version),
        ("2", b.reference().version),
        ("0.5", b.reference().version),
        ("0", "event-revision:999".into()),
    ] {
        let r = b.reference();
        let affected = db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES($1,7,7,$2::text::bigint,'thread',$3,$4,$5)",
            [snapshot.id.clone().into(), role.into(), r.scope.into(), r.id.into(), version.clone().into()],
        )).await;
        if role == "0.5" {
            assert!(
                affected.is_err(),
                "PostgreSQL must reject a fractional role before publication"
            );
            assert!(store.compaction_checkpoint(&k1.id).await.unwrap().is_none());
            assert_eq!(checkpoint_proof_version(&store, &k0.id).await, 0);
            continue;
        }
        affected.unwrap();
        let error = store.compaction_save_candidate(&k1, 1).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("raw candidate missing expected manifest ownership"),
            "{role}/{version}: {error:#}"
        );
        assert!(store.compaction_checkpoint(&k1.id).await.unwrap().is_none());
        assert_eq!(store.compaction_head(&k1.owner).await.unwrap(), None);
        assert_eq!(checkpoint_proof_version(&store, &k0.id).await, 0);
        let operation = store
            .compaction_operation(&snapshot.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(operation.snapshot, admitted.snapshot);
        assert_eq!(operation.next_portion, 1);
        assert_eq!(operation.status, "running");
        let row = db.query_one_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "SELECT CAST(reference_only AS TEXT) AS role,source_version FROM compaction_manifest WHERE operation_id=$1 AND ordinal=7",
            [snapshot.id.clone().into()])).await.unwrap().unwrap();
        assert_eq!(row.try_get::<String>("", "role").unwrap(), role);
        assert_eq!(
            row.try_get::<String>("", "source_version").unwrap(),
            version
        );
        // B is not covered by K0; removal of this corrupt unsealed staging
        // row uses the existing permitted path, with all guards still active.
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "DELETE FROM compaction_manifest WHERE operation_id=$1 AND ordinal=7",
            [snapshot.id.clone().into()],
        ))
        .await
        .unwrap();
    }
    for _ in 0..2 {
        for assertion in [&a, &b] {
            assert_eq!(
                store
                    .compaction_reference_thread("ws", &assertion.reference())
                    .await
                    .unwrap(),
                Some("thread".into())
            );
        }
        store.compaction_save_candidate(&k1, 1).await.unwrap();
        let saved = store
            .compaction_manifest_page(&snapshot.id, false, 0, 0)
            .await
            .unwrap();
        assert_eq!(saved.len(), 2);
        for (entry, expected) in saved.iter().zip([a.reference(), b.reference()]) {
            assert_eq!(entry.source, expected);
            assert_eq!(entry.thread_id, "thread");
            assert!(!entry.reference_only);
        }
        assert_eq!(
            (saved[0].ordinal, saved[0].unit),
            (first[0].ordinal, first[0].unit)
        );
        assert_eq!((saved[1].ordinal, saved[1].unit), (1, 1));
        let operation = store
            .compaction_operation(&snapshot.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(operation.snapshot, admitted.snapshot);
        assert_eq!(operation.status, "running");
        assert_eq!(operation.next_portion, 2);
        assert_eq!(store.compaction_head(&k1.owner).await.unwrap(), None);
        assert_eq!(checkpoint_proof_version(&store, &k0.id).await, 0);
        assert_eq!(checkpoint_proof_version(&store, &k1.id).await, 0);
    }
    assert_eq!(
        store
            .compaction_apply(&k1, None, &[a.clone(), b.clone()])
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    assert_eq!(checkpoint_proof_version(&store, &k1.id).await, 1);
    assert_eq!(
        store.compaction_head(&k1.owner).await.unwrap(),
        Some(k1.id.clone())
    );
    for (cp, expected) in [
        (&k0, vec![a.reference()]),
        (&k1, vec![a.reference(), b.reference()]),
    ] {
        let edges = store
            .compaction_checkpoint_edges(&cp.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            edges
                .coverage
                .iter()
                .map(|entry| entry.source.clone())
                .collect::<std::collections::BTreeSet<_>>(),
            expected
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
        );
        assert!(
            edges
                .coverage
                .iter()
                .all(|entry| entry.source_thread == "thread")
        );
        assert!(edges.replay_aliases.is_empty());
        assert!(edges.event_input_evidence.is_empty());
        for table in [
            "compaction_checkpoint_replay_alias",
            "compaction_checkpoint_event_input",
            "compaction_checkpoint_import",
        ] {
            let n: i64 = db
                .query_one_raw(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    format!("SELECT count(*) AS n FROM {table} WHERE checkpoint_id=$1"),
                    [cp.id.clone().into()],
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get("", "n")
                .unwrap();
            assert_eq!(n, 0, "raw proof set stays empty; K0 does not claim B");
        }
    }
    let status: String = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT status FROM compaction_checkpoint WHERE id=$1",
            [k1.id.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "status")
        .unwrap();
    assert_eq!(status, "applied");
    let operation = store
        .compaction_operation(&snapshot.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.status, "completed");
    assert_eq!(operation.snapshot, admitted.snapshot);
    assert!(
        store
            .compaction_runner_plan(&snapshot.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_runner_state(&snapshot.id)
            .await
            .unwrap()
            .is_none()
    );
    let saved = store
        .compaction_manifest_page(&snapshot.id, false, 0, 0)
        .await
        .unwrap();
    assert_eq!(saved.len(), 2);
    for (ordinal, (entry, expected)) in saved.iter().zip([a.reference(), b.reference()]).enumerate()
    {
        assert_eq!(entry.source, expected);
        assert_eq!(entry.ordinal, ordinal as u64);
        assert_eq!(entry.unit, ordinal as u64);
        assert_eq!(entry.thread_id, "thread");
        assert!(!entry.reference_only);
    }
    assert_eq!(
        store.compaction_apply(&k1, None, &[a, b]).await.unwrap(),
        CommitOutcome::AlreadyApplied
    );
}

#[pioneer_postgres_test::postgres_test]
async fn raw_assertion_api_saves_permuted_prebuilt_manifest_and_publishes_exactly() {
    let store = store().await;
    let a1 = source(&store, "A1", 1, "first A").await;
    let b1 = source(&store, "B1", 2, "only B").await;
    let a2 = source(&store, "A2", 3, "second A").await;
    let selection = ModelSelection {
        service_mode: None,
        context_authority: None,
        context_window: None,
        transport: Transport::Api,
        instance: "p".into(),
        model: "m".into(),
        effort: None,
    };
    let coverage = vec![a1.reference(), a2.reference(), b1.reference()];
    let snapshot = OperationSnapshot {
        id: "permuted-raw-op".into(),
        owner: "owner".into(),
        expected_checkpoint: None,
        projection_version: 1,
        source_epochs: Default::default(),
        admission: CompactionSettings::default()
            .admit(&selection, None, 10)
            .unwrap(),
        plan: CompactionPlan {
            mode: CompactionMode::Normal,
            coverage_domain: CoverageDomain::OwnContribution,
            compact: vec![0, 1],
            retain: vec![],
            coverage: coverage.clone(),
            fingerprint: "permuted-raw-op".into(),
        },
    };
    store
        .compaction_admit("ws", "thread", &snapshot)
        .await
        .unwrap();
    store
        .compaction_prepare_runner(&snapshot.id, &ModelBudget::new(None, None, None), 3, 0)
        .await
        .unwrap();
    let ordered = vec![a1.reference(), b1.reference(), a2.reference()];
    let manifest = ordered
        .iter()
        .enumerate()
        .map(|(ordinal, source)| ManifestEntry {
            ordinal: ordinal as u64,
            unit: 5,
            reference_only: false,
            thread_id: "thread".into(),
            source: source.clone(),
        })
        .collect::<Vec<_>>();
    store
        .compaction_append_manifest(&snapshot.id, &manifest)
        .await
        .unwrap();
    assert_ne!(coverage, ordered);
    assert!(
        store
            .compaction_manifest_sources_current(&snapshot.id)
            .await
            .unwrap()
    );
    assert!(
        store
            .compaction_bound_source_projection(&snapshot.id)
            .await
            .unwrap()
            .is_none()
    );
    let cp = Checkpoint {
        id: "permuted-raw-checkpoint".into(),
        operation_id: snapshot.id.clone(),
        owner: snapshot.owner.clone(),
        previous: None,
        summary: "Exact complete units A and B".into(),
        selection,
        coverage: coverage.clone(),
        projection_version: 1,
        format_version: 1,
    };
    for _ in 0..2 {
        // This is the first public candidate save, not just a runner's retry.
        store.compaction_save_candidate(&cp, 0).await.unwrap();
        let saved = store
            .compaction_manifest_page(&snapshot.id, false, 0, 0)
            .await
            .unwrap();
        assert_eq!(saved.len(), 3);
        assert_eq!(
            saved
                .iter()
                .map(|entry| entry.source.clone())
                .collect::<Vec<_>>(),
            ordered
        );
        for (entry, expected) in saved.iter().zip(&manifest) {
            assert_eq!(entry.ordinal, expected.ordinal);
            assert_eq!(entry.unit, expected.unit);
            assert_eq!(entry.thread_id, expected.thread_id);
            assert!(!entry.reference_only);
        }
        assert_eq!(checkpoint_proof_version(&store, &cp.id).await, 0);
        assert_eq!(store.compaction_head(&cp.owner).await.unwrap(), None);
    }
    assert_eq!(
        store
            .compaction_apply(&cp, None, &[a1, a2, b1])
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    assert_eq!(checkpoint_proof_version(&store, &cp.id).await, 1);
    assert_eq!(
        store.compaction_head(&cp.owner).await.unwrap(),
        Some(cp.id.clone())
    );
    let edges = store
        .compaction_checkpoint_edges(&cp.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        edges
            .coverage
            .iter()
            .map(|entry| entry.source.clone())
            .collect::<std::collections::BTreeSet<_>>(),
        coverage
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
    );
    assert!(
        edges
            .coverage
            .iter()
            .all(|entry| entry.source_thread == "thread")
    );
}

#[pioneer_postgres_test::postgres_test]
async fn raw_candidate_rejects_corrupt_or_missing_prepared_ownership_without_bootstrap() {
    async fn manifest_identity(store: &CompactionStore, op: &str) -> Vec<String> {
        let mut rows = store
            .compaction_manifest_page(op, false, 0, 0)
            .await
            .unwrap();
        rows.extend(
            store
                .compaction_manifest_page(op, true, 0, 0)
                .await
                .unwrap(),
        );
        rows.sort_by_key(|row| row.ordinal);
        rows.iter()
            .map(|row| {
                serde_json::to_string(&(
                    row.ordinal,
                    row.unit,
                    row.reference_only,
                    &row.thread_id,
                    &row.source,
                ))
                .unwrap()
            })
            .collect()
    }
    for ready in [false, true] {
        for defect in [
            "thread",
            "version",
            "reference-only",
            "ambiguous",
            "missing",
            "operation",
        ] {
            let store = store().await;
            let db = store.database_connection();
            let assertion = source(&store, "prepared-owner-source", 1, "original").await;
            let cp = candidate_admission_fixture(
                &store,
                "prepared-owner-op",
                None,
                &assertion,
                Default::default(),
                true,
            )
            .await;
            let record = store
                .compaction_operation(&cp.operation_id)
                .await
                .unwrap()
                .unwrap();
            if ready {
                let snapshot: OperationSnapshot = serde_json::from_str(&record.snapshot).unwrap();
                // Only activate. ready_operation also saves a candidate and
                // closes ownership, which would make this corruption fixture
                // hit the immutable guard before the action under test.
                let budget = ModelBudget::new(None, None, None);
                let initial = pioneer_compaction::runner::RunnerState::new(
                    snapshot.admission.deadline_ms,
                    &budget,
                    1000,
                    None,
                )
                .unwrap();
                store
                    .compaction_activate_runner(&cp.operation_id, &initial)
                    .await
                    .unwrap();
            }
            // Corrupt the unsealed fixture before candidate creation. No DDL or
            // production guard is disabled; prepared/ready state remains intact.
            if defect == "operation" {
                // The real trigger prevents an orphan. Move the entry to an
                // existing but unrelated operation to exercise missing ownership.
                let error = db.execute_unprepared(
                    "UPDATE compaction_manifest SET operation_id='orphan-operation' WHERE operation_id='prepared-owner-op'",
                ).await.unwrap_err();
                assert!(error.to_string().contains("manifest operation missing"));
                db.execute_unprepared(
                    "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms)
                     SELECT 'unrelated-operation',owner,'unrelated-fingerprint','running','{}',deadline_ms
                     FROM compaction_operation WHERE id='prepared-owner-op'",
                ).await.unwrap();
            }
            let sql = match defect {
                "thread" => {
                    "UPDATE compaction_manifest SET source_thread='wrong-thread' WHERE operation_id='prepared-owner-op'"
                }
                "version" => {
                    "UPDATE compaction_manifest SET source_version='event-revision:999' WHERE operation_id='prepared-owner-op'"
                }
                "reference-only" => {
                    "UPDATE compaction_manifest SET reference_only=1 WHERE operation_id='prepared-owner-op'"
                }
                "ambiguous" => {
                    "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) SELECT operation_id,7,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version FROM compaction_manifest WHERE operation_id='prepared-owner-op'"
                }
                "missing" => {
                    "DELETE FROM compaction_manifest WHERE operation_id='prepared-owner-op'"
                }
                "operation" => {
                    "UPDATE compaction_manifest SET operation_id='unrelated-operation' WHERE operation_id='prepared-owner-op'"
                }
                _ => unreachable!(),
            };
            db.execute_unprepared(sql).await.unwrap();
            assert_eq!(
                store
                    .compaction_reference_thread("ws", &assertion.reference())
                    .await
                    .unwrap(),
                Some("thread".into()),
                "current assertion itself must remain valid"
            );
            let before = manifest_identity(&store, &cp.operation_id).await;
            let orphan_before = manifest_identity(&store, "orphan-operation").await;
            let state = serde_json::to_string(
                &store
                    .compaction_runner_state(&cp.operation_id)
                    .await
                    .unwrap(),
            )
            .unwrap();
            for _ in 0..2 {
                let error = store.compaction_save_candidate(&cp, 0).await.unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("raw historical ownership conflict")
                        || error
                            .to_string()
                            .contains("raw candidate missing expected manifest ownership"),
                    "{ready}/{defect}: {error:#}"
                );
                assert!(store.compaction_checkpoint(&cp.id).await.unwrap().is_none());
                assert_eq!(store.compaction_head(&cp.owner).await.unwrap(), None);
                let after = store
                    .compaction_operation(&cp.operation_id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(after.status, record.status);
                assert_eq!(after.next_portion, record.next_portion);
                assert_eq!(after.snapshot, record.snapshot);
                assert_eq!(
                    serde_json::to_string(
                        &store
                            .compaction_runner_state(&cp.operation_id)
                            .await
                            .unwrap()
                    )
                    .unwrap(),
                    state
                );
                assert_eq!(
                    store
                        .compaction_runner_plan(&cp.operation_id)
                        .await
                        .unwrap()
                        .unwrap()
                        .ready,
                    ready
                );
                assert_eq!(manifest_identity(&store, &cp.operation_id).await, before);
                assert_eq!(
                    manifest_identity(&store, "orphan-operation").await,
                    orphan_before
                );
            }
        }
    }
}

#[pioneer_postgres_test::postgres_test]
async fn raw_assertion_missing_historical_ownership_is_not_empty_proofs() {
    let store = store().await;
    let assertion = source(&store, "lost-owner-source", 1, "original").await;
    // Model an unsealed legacy checkpoint whose ownership was already lost,
    // before the new candidate writers' atomic immutable portion boundary.
    // A post-commit DELETE is now correctly rejected by the production guard.
    let cp = candidate_admission_fixture(
        &store,
        "lost-owner-op",
        None,
        &assertion,
        Default::default(),
        false,
    )
    .await;
    use sha2::{Digest, Sha256};
    let identity = hex::encode(Sha256::digest(serde_json::to_vec(&cp).unwrap()));
    let db = store.database_connection();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO compaction_checkpoint(id,operation_id,owner,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES($1,$2,$3,0,$4,$5,$6::text::json,$7,1,'candidate')",
        [cp.id.clone().into(),cp.operation_id.clone().into(),cp.owner.clone().into(),cp.summary.clone().into(),identity.into(),serde_json::to_string(&cp.selection).unwrap().into(),(cp.projection_version as i64).into()])).await.unwrap();
    let r = assertion.reference();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO compaction_coverage(checkpoint_id,source_scope,source_id,source_version) VALUES($1,$2,$3,$4)",
        [cp.id.clone().into(),r.scope.into(),r.id.into(),r.version.into()])).await.unwrap();
    // Candidate retry must not reconstruct a historical node from current
    // assertions, even when neither a runner nor any manifest row survived.
    assert!(store.compaction_save_candidate(&cp, 0).await.is_err());
    assert!(
        store
            .compaction_manifest_page(&cp.operation_id, false, 0, 0)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .compaction_apply(&cp, None, &[assertion])
            .await
            .is_err()
    );
    assert_eq!(checkpoint_proof_version(&store, &cp.id).await, 0);
    assert_eq!(store.compaction_head("owner").await.unwrap(), None);
}

#[pioneer_postgres_test::postgres_test]
async fn extra_staging_evidence_prevents_seal_and_publication_without_overwrite() {
    let store = store().await;
    let assertion = source(&store, "staging-source", 1, "original").await;
    let cp = candidate(&store, "staging-op", None, &assertion).await;
    store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,"INSERT INTO compaction_checkpoint_event_input(checkpoint_id,source_thread,source_scope,source_id,source_version,role) VALUES($1,'thread','event:turn','poison','event-revision:1','deleted')",[cp.id.clone().into()])).await.unwrap();
    for _ in 0..2 {
        assert!(
            store
                .compaction_apply(&cp, None, &[assertion.clone()])
                .await
                .is_err()
        );
        assert_eq!(checkpoint_proof_version(&store, &cp.id).await, 0);
    }
    assert_eq!(store.compaction_head("owner").await.unwrap(), None);
    let n: i64 = store
        .database_connection()
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT count(*) AS n FROM compaction_checkpoint_event_input WHERE checkpoint_id=$1",
            [cp.id.into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap();
    assert_eq!(n, 1, "conflicting staging is not silently repaired");
}

#[pioneer_postgres_test::postgres_test]
async fn checkpoint_alias_null_identity_is_distinct_from_empty_tool_identity() {
    let store = store().await;
    let assertion = source(&store, "alias-source", 1, "original").await;
    let cp = candidate(&store, "alias-op", None, &assertion).await;
    let sql = "INSERT INTO compaction_checkpoint_replay_alias(checkpoint_id,covered_thread,covered_scope,covered_id,covered_version,replay_thread,replay_scope,replay_id,replay_version,tool_item_id) VALUES($1,'thread','event:turn','source','event-revision:1','thread','context:turn','replay','revision:1',$2)";
    for tool in [None, Some(String::new())] {
        store
            .database_connection()
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                sql,
                [cp.id.clone().into(), tool.into()],
            ))
            .await
            .unwrap();
    }
    assert!(
        store
            .database_connection()
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                sql,
                [cp.id.clone().into(), Option::<String>::None.into()]
            ))
            .await
            .is_err()
    );
    let n: i64 = store
        .database_connection()
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT count(*) AS n FROM compaction_checkpoint_replay_alias WHERE checkpoint_id=$1",
            [cp.id.into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap();
    assert_eq!(n, 2);
}

#[pioneer_postgres_test::postgres_test]
async fn deadline_saved_ready_candidate_commit_expiry_refuses_resume_without_changing_generation() {
    use pioneer_compaction::runner::{AttemptPurpose, FailureKind, RunnerPhase, RunnerState};
    for phase_name in ["Ready", "Candidate", "Commit"] {
        let store = store().await;
        let assertion = source(&store, "resume-source", 1, "original").await;
        let cp = candidate_fixture(
            &store,
            "resume-op",
            None,
            &assertion,
            std::collections::BTreeMap::new(),
            true,
        )
        .await;
        let descriptor = shared_descriptor("resume-origin", &[]);
        store
            .compaction_begin_frozen_history("ws", "thread", &descriptor)
            .await
            .unwrap();
        let hold = store
            .compaction_finish_frozen_history_held("ws", "thread", &descriptor)
            .await
            .unwrap();
        let db = store.database_connection();
        db.execute_unprepared("INSERT INTO compaction_operation_projection(operation_id,manifest_id,identity_sha256,imports_sha256,import_count) SELECT 'resume-op',id,identity_sha256,imports_sha256,import_count FROM compaction_frozen_history WHERE id='resume-origin'").await.unwrap();
        let phase = match phase_name {
            "Ready" => RunnerPhase::Ready {
                purpose: AttemptPurpose::Portion,
            },
            "Candidate" => RunnerPhase::Candidate {
                checkpoint: cp.id.clone(),
                final_portion: true,
            },
            _ => RunnerPhase::Commit {
                checkpoint: cp.id.clone(),
            },
        };
        let state = RunnerState {
            generation: 0,
            deadline_ms: 1000,
            attempts: 1,
            retries: 0,
            portion_attempts: 0,
            overflow_rejections: 0,
            overflow_input_ceiling: None,
            corrections: 0,
            target_tokens: 10,
            source_text_projection_version: 0,
            cursor: pioneer_compaction::runner::SourceCursor {
                unit: 1,
                ..Default::default()
            },
            previous_checkpoint: Some(cp.id.clone()),
            phase: RunnerPhase::Failed {
                kind: FailureKind::Deadline,
            },
            resume_phase: Some(phase),
            observation: None,
            diagnostic: None,
        };
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,"INSERT INTO compaction_runner_state(operation_id,generation,state) VALUES('resume-op',0,$1::text::json)",[serde_json::to_string(&state).unwrap().into()])).await.unwrap();
        db.execute_unprepared("UPDATE compaction_operation SET status='failed',outcome='deadline',deadline_ms=1000 WHERE id='resume-op'").await.unwrap();
        drop(hold);
        if phase_name == "Commit" {
            assert!(
                store
                    .compaction_resume_deadline("resume-op", "turn", 2000)
                    .await
                    .unwrap(),
                "resume first creates a running origin root"
            );
            let mut p = FrozenStorageLifetimeProgress::default();
            for _ in 0..500 {
                let _ = p.quantum(&store).await;
            }
            assert!(
                store
                    .compaction_bound_source_projection("resume-op")
                    .await
                    .unwrap()
                    .is_some()
            );
            // Return to proven deadline terminal state at the new generation.
            let mut terminal = store
                .compaction_runner_state("resume-op")
                .await
                .unwrap()
                .unwrap();
            terminal.generation += 1;
            terminal.phase = RunnerPhase::Failed {
                kind: FailureKind::Deadline,
            };
            terminal.resume_phase = Some(RunnerPhase::Commit {
                checkpoint: cp.id.clone(),
            });
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,"UPDATE compaction_runner_state SET generation=$1,state=$2::text::json WHERE operation_id='resume-op'",[(terminal.generation as i64).into(),serde_json::to_string(&terminal).unwrap().into()])).await.unwrap();
            db.execute_unprepared("UPDATE compaction_operation SET status='failed',outcome='deadline' WHERE id='resume-op'").await.unwrap();
        }
        // Isolated fixture expiry models the durable winner before dispatch.
        db.execute_unprepared(
            "UPDATE compaction_frozen_history SET expired=1 WHERE id='resume-origin'",
        )
        .await
        .unwrap();
        let before = store
            .compaction_runner_state("resume-op")
            .await
            .unwrap()
            .unwrap();
        let error = store
            .compaction_resume_deadline("resume-op", "turn", 4000)
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("expired"), "{phase_name}");
        assert_eq!(
            store
                .compaction_runner_state("resume-op")
                .await
                .unwrap()
                .unwrap(),
            before
        );
        assert_eq!(
            store
                .compaction_operation("resume-op")
                .await
                .unwrap()
                .unwrap()
                .status,
            "failed"
        );
    }
}

#[pioneer_postgres_test::postgres_test]
async fn legacy_large_raw_admission_and_coverage_are_paged_not_poisoned_by_aggregate_bytes() {
    let recorded: RecordedStatements = Default::default();
    let store = store_recording_statements(Some(recorded.clone())).await;
    let mut assertions = Vec::new();
    for i in 0..128 {
        assertions.push(
            source(
                &store,
                &format!("raw-{i:04}-{}", "x".repeat(3072)),
                i + 1,
                "{}",
            )
            .await,
        );
    }
    let mut cp = candidate_admission_fixture(
        &store,
        "raw-large-op",
        None,
        &assertions[0],
        Default::default(),
        false,
    )
    .await;
    cp.coverage = assertions.iter().map(SourceAssertion::reference).collect();
    let db = store.database_connection();
    let row = db
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT snapshot FROM compaction_operation WHERE id='raw-large-op'",
        ))
        .await
        .unwrap()
        .unwrap();
    let mut snapshot: OperationSnapshot =
        serde_json::from_str(&row.try_get::<String>("", "snapshot").unwrap()).unwrap();
    snapshot.plan.coverage = cp.coverage.clone();
    snapshot.plan.compact = (0..cp.coverage.len()).collect();
    let json = serde_json::to_string(&snapshot).unwrap();
    assert!(json.len() > SOURCE_PAGE_BYTES);
    // Original legacy admission predates the small current admission envelope.
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "UPDATE compaction_operation SET snapshot=$1 WHERE id='raw-large-op'",
        [json.into()],
    ))
    .await
    .unwrap();
    recorded.lock().unwrap().clear();
    store.compaction_save_candidate(&cp, 0).await.unwrap();
    assert_eq!(
        store
            .compaction_checkpoint(&cp.id)
            .await
            .unwrap()
            .unwrap()
            .coverage,
        cp.coverage
    );
    assert_eq!(
        store
            .compaction_checkpoint_edges(&cp.id)
            .await
            .unwrap()
            .unwrap()
            .coverage
            .len(),
        128
    );
    assert_eq!(
        store
            .compaction_apply(&cp, None, &assertions)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    assert_eq!(checkpoint_proof_version(&store, &cp.id).await, 1);
    let statements = recorded.lock().unwrap();
    let fragments = statements
        .iter()
        .filter(|s| s.sql.contains("substr(convert_to(o.snapshot::text"))
        .count();
    assert!(
        fragments >= 4,
        "both raw preparation and proof extraction must read multiple fragments"
    );
    assert!(
        !statements
            .iter()
            .any(|s| s.sql.starts_with("SELECT owner,snapshot")),
        "no full snapshot payload read"
    );
}

#[path = "delivered_output_tests.rs"]
mod delivered_output_tests;

#[path = "history_retry_tests.rs"]
mod history_retry_tests;

#[pioneer_postgres_test::postgres_test]
async fn old_summary_is_not_a_canonical_source_or_projection_dependency() {
    let store = store().await;
    let epoch = store
        .compaction_projection_version("ws", "thread")
        .await
        .unwrap();
    pioneer_crud_conversations::thread_metadata::update_thread_summary(
        &store.database_connection(),
        "thread",
        "obsolete text",
        900,
        chrono::Utc::now().fixed_offset(),
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        epoch
    );
    let row = store
        .database_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT count(*) AS n FROM compaction_live_sources WHERE source_scope LIKE 'legacy:%'",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "n").unwrap(), 0);
    let source = SourceRef {
        scope: "legacy:thread".into(),
        id: "thread".into(),
        version: "legacy-revision:1".into(),
    };
    assert!(
        store
            .compaction_reference_fragment("ws", "thread", &source, 0)
            .await
            .is_err()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn selected_source_edit_does_not_invalidate_completed_runner_summary() {
    let store = store().await;
    let (_, ready) = crate::plan_test_support::prepare_checked_candidate(&store).await;
    verify_edited_source_runner_commit(&store, ready).await;
}

async fn verify_edited_source_runner_commit(
    store: &CompactionStore,
    ready: pioneer_compaction::runner::RunnerState,
) {
    store
        .database_connection()
        .execute_unprepared(
            "UPDATE turn_event SET payload=to_json('edited selected source'::text) WHERE id='runner-source'",
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_apply_runner("runner", &ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    assert_eq!(
        store
            .compaction_head("runner-owner")
            .await
            .unwrap()
            .as_deref(),
        Some("runner-candidate")
    );
    assert_eq!(
        store
            .compaction_checkpoint("runner-candidate")
            .await
            .unwrap()
            .unwrap()
            .summary,
        "saved complete candidate"
    );
}

#[pioneer_postgres_test::postgres_test]
async fn large_canonical_source_reads_are_version_bound_and_cleanup_preserves_original() {
    let store = store().await;
    let text = "🌍漢字abcdef".repeat(40_000);
    store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO turn_llm_context(id,turn_id,sequence,source,payload,output_policy_snapshot,created_at) VALUES ('large','turn',1,'tool_result_v2',$1,'{}',CURRENT_TIMESTAMP)", [text.clone().into()])).await.unwrap();
    let mut offset = 0;
    let mut rebuilt = String::new();
    let mut revision = None;
    loop {
        let fragment = store
            .compaction_source_fragment(
                "ws",
                "thread",
                "turn",
                "large",
                revision.as_deref(),
                offset,
            )
            .await
            .unwrap()
            .unwrap();
        assert!(fragment.text.len() <= 64 * 1024);
        revision = Some(fragment.reference.version);
        rebuilt.push_str(&fragment.text);
        let Some(next) = fragment.next_character else {
            break;
        };
        offset = next;
    }
    assert_eq!(rebuilt, text);
    assert_eq!(
        pioneer_crud_conversations::context_cleanup::delete_turn_llm_context_for_terminal_turns(
            &store.database_connection()
        )
        .await
        .unwrap(),
        0
    );
    let source = SourceAssertion {
        revision: Some(1),
        kind: CanonicalSource::ProviderContext,
        turn_id: "turn".into(),
        id: "large".into(),
        payload: String::new(),
    };
    let cp = candidate(&store, "large-op", None, &source).await;
    store
        .database_connection()
        .execute_unprepared("UPDATE turn_llm_context SET payload='changed' WHERE id='large'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_source_fragment("ws", "thread", "turn", "large", revision.as_deref(), 0)
            .await
            .is_err()
    );
    assert_eq!(
        store.compaction_apply(&cp, None, &[source]).await.unwrap(),
        CommitOutcome::Stale
    );
}

#[pioneer_postgres_test::postgres_test]
async fn compaction_post_terminal_stop_survives_worker_loss_and_fences_new_admission() {
    let store = store().await;
    source(&store, "stop-source", 1, "original source").await;
    let reference = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    let op = admit_import_operation(&store, "durable-stop", "thread", "turn").await;
    let ready = ready_import_operation(&store, &op, "thread", &reference).await;
    let db = store.database_connection();
    assert!(
        store
            .compaction_stop_execution("wrong-workspace", "thread", &op.owner, "turn")
            .await
            .is_err()
    );
    db.execute_unprepared("CREATE FUNCTION abort_context_stop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'Stop rollback'; END $$; CREATE TRIGGER abort_context_stop BEFORE INSERT ON compaction_execution_stop FOR EACH ROW EXECUTE FUNCTION abort_context_stop()").await.unwrap();
    assert!(
        store
            .compaction_stop_execution("ws", "thread", &op.owner, "turn")
            .await
            .is_err()
    );
    assert!(!store.compaction_execution_cancelled(&op.id).await.unwrap());
    db.execute_unprepared("DROP TRIGGER abort_context_stop ON compaction_execution_stop")
        .await
        .unwrap();
    let mut stopped = db.work_signal("compaction").subscribe();
    stopped.begin_pass(&db).await.unwrap();
    store
        .compaction_stop_execution("ws", "thread", &op.owner, "turn")
        .await
        .unwrap();
    assert!(futures_util::FutureExt::now_or_never(stopped.wait(None)).is_some());
    stopped.begin_pass(&db).await.unwrap();
    store
        .compaction_stop_execution("ws", "thread", &op.owner, "turn")
        .await
        .unwrap();
    assert!(futures_util::FutureExt::now_or_never(stopped.wait(None)).is_none());
    // No service reconciliation runs: model a lost worker with a ready candidate.
    assert!(store.compaction_execution_cancelled(&op.id).await.unwrap());
    assert_eq!(
        store
            .compaction_operation(&op.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "running"
    );
    assert_eq!(
        store
            .compaction_apply_runner(&op.id, &ready, None)
            .await
            .unwrap(),
        CommitOutcome::Cancelled
    );
    assert!(store.compaction_head(&op.owner).await.unwrap().is_none());
    assert_eq!(
        pioneer_crud_conversations::turn_reads::get_turn_for_thread(&db, "thread", "turn")
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        pioneer_protocol::TurnStatus::Completed
    );
    let mut replacement = op.clone();
    replacement.id = "after-stop-new-fingerprint".into();
    replacement.plan.fingerprint = replacement.id.clone();
    assert!(
        store
            .compaction_admit_for_turn("ws", "thread", &replacement, Some("turn"))
            .await
            .is_err()
    );
    assert!(
        store
            .compaction_operation(&replacement.id)
            .await
            .unwrap()
            .is_none()
    );
    db.execute_unprepared("INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('next-turn','thread','in_progress','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    store
        .compaction_admit_for_turn("ws", "thread", &replacement, Some("next-turn"))
        .await
        .unwrap();
    store
        .compaction_stop_execution("ws", "thread", &op.owner, "next-turn")
        .await
        .unwrap();
    assert!(
        store.compaction_execution_cancelled(&op.id).await.unwrap(),
        "later Stop must not erase an earlier stopped execution"
    );
    assert!(
        store
            .compaction_execution_cancelled(&replacement.id)
            .await
            .unwrap()
    );
    // Stop before any admission is also durable and cannot leave a running row.
    let mut unborn = replacement.clone();
    unborn.id = "never-admitted".into();
    unborn.owner = "never-admitted-owner".into();
    unborn.plan.fingerprint = unborn.id.clone();
    store
        .compaction_stop_execution("ws", "thread", &unborn.owner, "turn")
        .await
        .unwrap();
    assert!(
        store
            .compaction_admit_for_turn("ws", "thread", &unborn, Some("turn"))
            .await
            .is_err()
    );
    assert!(
        store
            .compaction_operation(&unborn.id)
            .await
            .unwrap()
            .is_none()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn task_output_snapshot_is_bound_to_completed_turn_and_never_recaptured() {
    let store = store().await;
    let db = store.database_connection();
    for sql in [
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task','ws','thread','thread','thread','turn','agent','running','Task','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run','task','run',1,1,'running','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('rt','task','run','thread','turn','initial',0,1,'in_progress',CURRENT_TIMESTAMP)",
        "UPDATE turn SET status='in_progress' WHERE id='turn'",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    assert_eq!(
        pioneer_crud_tasks::view_store::TaskViewStore::new(&db)
            .get_task_run_turn("rt")
            .await
            .unwrap()
            .unwrap()
            .kind,
        pioneer_protocol::TaskRunTurnKind::Initial
    );
    let history = pioneer_compaction::frozen::FrozenHistoryRef {
        format: 1,
        manifest_id: "output-history".into(),
        messages: 0,
        identity_sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
    };
    store
        .compaction_begin_frozen_history("ws", "thread", &history)
        .await
        .unwrap();
    store
        .compaction_finish_frozen_history("ws", "thread", &history)
        .await
        .unwrap();
    assert!(
        store
            .compaction_record_task_output("ws", "rt", &history)
            .await
            .is_err()
    );
    db.execute_unprepared("UPDATE turn SET status='completed' WHERE id='turn'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_record_task_output("other", "rt", &history)
            .await
            .is_err()
    );
    let output = store
        .compaction_record_task_output("ws", "rt", &history)
        .await
        .unwrap();
    assert_eq!(output.task_id, "task");
    assert_eq!(output.run_id, "run");
    assert_eq!(output.source_thread, "thread");
    assert_eq!(output.source_turn, "turn");
    assert_eq!(output.history, history);
    let other = pioneer_compaction::frozen::FrozenHistoryRef {
        manifest_id: "later-history".into(),
        ..history.clone()
    };
    store
        .compaction_begin_frozen_history("ws", "thread", &other)
        .await
        .unwrap();
    store
        .compaction_finish_frozen_history("ws", "thread", &other)
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_record_task_output("ws", "rt", &other)
            .await
            .unwrap(),
        output
    );
    assert!(
        store
            .compaction_task_output("other", "rt")
            .await
            .unwrap()
            .is_none()
    );
    db.execute_unprepared("UPDATE task_run_turn SET turn_id='other-turn' WHERE id='rt'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_task_output("ws", "rt")
            .await
            .unwrap()
            .is_none()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn compaction_successful_apply_fences_prepared_lifecycle_timeout() {
    use pioneer_entity::compaction_lifecycle_pending as pending;
    use sea_orm::EntityTrait;
    let store = store().await;
    source(&store, "apply-race-source", 1, "original source").await;
    let reference = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    let op = admit_import_operation(&store, "apply-race", "thread", "turn").await;
    let ready = ready_import_operation(&store, &op, "thread", &reference).await;
    let candidate = pending::Entity::find_by_id(&op.id)
        .one(&store.database_connection())
        .await
        .unwrap()
        .unwrap();
    let now = i64::try_from(op.admission.deadline_ms).unwrap();
    let claim = store
        .compaction_claim_lifecycle(&candidate, &|| now)
        .await
        .unwrap()
        .unwrap();
    let timeout = store
        .compaction_prepare_lifecycle(&claim, now)
        .await
        .unwrap()
        .unwrap();
    assert!(!timeout.needs_publication());
    // Interleave the real atomic apply after timeout preparation, before its
    // writer admission. The old timeout must neither finish nor ACK this apply.
    assert_eq!(
        store
            .compaction_apply_runner(&op.id, &ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    let after_apply = pending::Entity::find_by_id(&op.id)
        .one(&store.database_connection())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !store
            .repair_lifecycle_without_publication(timeout)
            .await
            .unwrap()
            .0
    );
    assert_eq!(
        store
            .compaction_operation(&op.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "completed"
    );
    assert_eq!(
        store.compaction_head(&op.owner).await.unwrap().as_deref(),
        Some("checkpoint-apply-race")
    );
    assert_eq!(
        pending::Entity::find_by_id(&op.id)
            .one(&store.database_connection())
            .await
            .unwrap()
            .unwrap(),
        after_apply
    );
    assert!(matches!(
        store
            .compaction_runner_state(&op.id)
            .await
            .unwrap()
            .unwrap()
            .phase,
        pioneer_compaction::runner::RunnerPhase::Applied { .. }
    ));
}

#[path = "frozen_import_regressions.rs"]
mod frozen_import_regressions;
