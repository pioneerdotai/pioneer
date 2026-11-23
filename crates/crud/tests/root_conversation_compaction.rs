//! Root publication, revision fencing and durable history-check ownership.
use migration::{Migrator, MigratorTrait};
use pioneer_crud::{
    CrudStore,
    compaction::{HistoryCheckDiagnostic, HistoryCheckOutcome},
};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_postgres_test::TestDatabase;
use pioneer_protocol::{
    ItemCompletedNotification, TurnCompletedNotification, TurnItem, TurnStatus,
};
use sea_orm::{ConnectionTrait, DbBackend, Statement};

const READY: i64 = 1_792_838_000;

async fn open_store(fixture: &TestDatabase) -> CrudStore {
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let database = PostgresDatabase::new(reader, writer);
    database.validate_reader().await.unwrap();
    CrudStore::new(database).with_maintenance_access()
}

#[pioneer_postgres_test::postgres_test]
async fn canonical_policy_preserves_execution_intents_and_root_publication_survives_restart() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let store = open_store(&fixture).await;
    let db = store.database_connection();
    // A persisted execution descriptor must never schedule a proactive root check.
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',TRUE,FALSE)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('root','ws','','agent','m','p','active','user','workspace','2026-10-24T00:00:00Z','2026-10-24T00:00:00Z')",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('child','ws','','agent','m','p','active','system','workspace','2026-10-24T00:00:00Z','2026-10-24T00:00:00Z')",
        "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES('child','root','root',1,'2026-10-24T00:00:00Z')",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('executor','child','in_progress','conversation','user','2026-10-24T00:00:00Z','2026-10-24T00:00:00Z')",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,initiated_by_actor_kind,initiated_by_actor_id,created_at,updated_at) VALUES('old-human','root','in_progress','conversation','user','principal','P00000000000000000001','2026-10-23T00:00:00Z','2026-10-23T00:00:00Z')",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,initiated_by_actor_kind,initiated_by_actor_id,created_at,updated_at) VALUES('late-human','root','in_progress','conversation','user','principal','P00000000000000000001','2026-10-23T01:00:00Z','2026-10-23T01:00:00Z')",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,initiated_by_actor_kind,initiated_by_actor_id,created_at,updated_at) VALUES('human','root','in_progress','conversation','user','principal','P00000000000000000001','2026-10-24T00:00:00Z','2026-10-24T00:00:00Z')",
        "INSERT INTO compaction_history_check(turn_id,descriptor,managed,revision,failures) VALUES('executor','{\"fixture\":\"retained-execution-descriptor\"}',1,9,2)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    // Persist a completed answer without a live root publication. A duplicate
    // completion after startup must neither activate it nor redesignate it.
    let historical_answer = serde_json::to_string(&TurnItem::AgentMessage {
        id: "old-ready".into(),
        text: "fixture original publication".into(),
        phase: Default::default(),
        markdown: None,
        markdown_version: None,
    })
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,created_at,updated_at) VALUES('old-ready-row','old-human','old-ready','agent_message','completed',$1::text::json,$2,$2)",
        [historical_answer.into(), chrono::DateTime::from_timestamp(READY + 10, 0).unwrap().fixed_offset().into()],
    ))
    .await
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "UPDATE turn SET status='completed',updated_at=$1 WHERE id='old-human'",
        [chrono::DateTime::from_timestamp(READY + 10, 0)
            .unwrap()
            .fixed_offset()
            .into()],
    ))
    .await
    .unwrap();
    Migrator::up(fixture.connection(), None).await.unwrap();
    assert!(
        store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty()
    );
    let initial_activity = store
        .compaction_root_history_activity("ws", "root", "human")
        .await
        .unwrap();
    assert!(
        initial_activity.is_some(),
        "the most recently accepted human turn owns the activity"
    );
    complete(&store, "root", "old-human", READY + 100_000).await;
    assert!(
        store
            .with_maintenance_access()
            .compaction_root_history_activity("ws", "root", "old-human")
            .await
            .unwrap()
            .is_none(),
        "replaying an old ready event cannot activate a cold root"
    );
    let retained = db.query_one_raw(Statement::from_string(DbBackend::Postgres,
        "SELECT descriptor,revision,failures,purpose FROM compaction_history_check WHERE turn_id='executor'".to_owned())).await.unwrap().unwrap();
    assert_eq!(
        retained.try_get::<String>("", "descriptor").unwrap(),
        r#"{"fixture":"retained-execution-descriptor"}"#
    );
    assert_eq!(retained.try_get::<i64>("", "revision").unwrap(), 9);
    assert_eq!(retained.try_get::<i64>("", "failures").unwrap(), 2);
    assert_eq!(
        retained.try_get::<String>("", "purpose").unwrap(),
        "execution"
    );

    // A real completed child remains retained work, without proactive eligibility.
    complete(&store, "child", "executor", READY).await;
    assert!(
        store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty()
    );
    store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "root".into(),
                turn_id: "human".into(),
                item: TurnItem::AgentMessage {
                    id: "answer".into(),
                    text: "ready answer".into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            READY,
        )
        .await
        .unwrap();
    assert!(
        store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty(),
        "candidate item alone is not a ready response"
    );
    // Publication and its queue debt must commit together. A storage rejection
    // cannot leave a completed answer whose preparation request disappeared.
    db.execute_unprepared("CREATE FUNCTION reject_root_debt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture queue failure'; END $$; CREATE TRIGGER reject_root_debt BEFORE INSERT ON compaction_history_check FOR EACH ROW WHEN (NEW.turn_id='human') EXECUTE FUNCTION reject_root_debt()").await.unwrap();
    let (_, mut answer) = store.get_turn("root", "human").await.unwrap().unwrap();
    answer.status = TurnStatus::Completed;
    assert!(
        store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "root".into(),
                    turn: answer,
                },
                READY
            )
            .await
            .is_err()
    );
    assert_eq!(
        store
            .get_turn("root", "human")
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::InProgress
    );
    assert!(
        store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .compaction_root_history_activity("ws", "root", "human")
            .await
            .unwrap(),
        initial_activity,
        "failed publication must not change the activity or expose its debt",
    );
    db.execute_unprepared("DROP TRIGGER reject_root_debt ON compaction_history_check; DROP FUNCTION reject_root_debt()")
        .await
        .unwrap();
    // The canonical append survived the failed projection. Retry its saved
    // projection through the durable replay owner, preserving original time.
    let replayed = store
        .replay_due_turn_event_projections(READY + 30, 10)
        .await
        .unwrap();
    assert_eq!(replayed.projected, 1);
    assert_eq!(replayed.failed, 0);
    complete(&store, "root", "human", READY).await;
    let due = store.compaction_pending_history_checks().await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!((&*due[0].thread_id, &*due[0].turn_id), ("root", "human"));
    assert_eq!(
        store
            .compaction_root_history_activity("ws", "root", "human")
            .await
            .unwrap(),
        Some(READY * 1000)
    );
    // Reprocessing completion at a later wall clock must not rejuvenate root
    // activity or manufacture a new publication request.
    complete(&store, "root", "human", READY + 100_000).await;
    assert_eq!(
        store
            .compaction_root_history_activity("ws", "root", "human")
            .await
            .unwrap(),
        Some(READY * 1000)
    );
    let replay = store.compaction_pending_history_checks().await.unwrap();
    assert_eq!(replay[0].revision, due[0].revision);

    let claim = store
        .compaction_claim_history_check("human", due[0].revision, READY * 1000)
        .await
        .unwrap()
        .unwrap();
    let deadline = store
        .compaction_begin_history_attempt("human", claim.revision, "{}", "fixture", READY * 1000)
        .await
        .unwrap()
        .unwrap();
    // A fresh scoped owner has no in-memory job state; durable revision and
    // deadline alone determine resumed ownership.
    let restarted = open_store(&fixture).await;
    let resumed = restarted
        .compaction_claim_history_check("human", claim.revision, READY * 1000 + 10)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        restarted
            .compaction_begin_history_attempt(
                "human",
                resumed.revision,
                "{}",
                "fixture",
                READY * 1000 + 10
            )
            .await
            .unwrap(),
        Some(deadline)
    );
    Migrator::up(fixture.connection(), None).await.unwrap();
    assert_eq!(
        restarted
            .compaction_root_history_activity("ws", "root", "human")
            .await
            .unwrap(),
        Some(READY * 1000),
        "new publication activity survives a fresh owner"
    );
    let diagnostic = HistoryCheckDiagnostic::default();
    assert!(
        !store
            .compaction_record_history_result(
                "human",
                claim.revision,
                0,
                HistoryCheckOutcome::Fits,
                &diagnostic,
                READY * 1000 + 20
            )
            .await
            .unwrap()
    );
    assert_eq!(
        restarted
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .len(),
        1
    );
    // Historical input has already created the owner. No operation has
    // stopped the current anchor yet; the next input must fence it.
    let stops = db
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT count(*) AS n FROM compaction_execution_stop s JOIN compaction_context c ON c.owner=s.owner WHERE c.thread_id='root' AND s.turn_id='human'".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stops.try_get::<i64>("", "n").unwrap(), 0);
    let mut thread = store.get_thread_model("root").await.unwrap().unwrap();
    thread.updated_at = READY + 1;
    let (_, mut next) = store.get_turn("root", "human").await.unwrap().unwrap();
    next.id = "next-human".into();
    next.status = TurnStatus::InProgress;
    store
        .materialize_turn_start(
            &thread,
            pioneer_protocol::SandboxMode::FullAccess,
            &next,
            &[],
            pioneer_protocol::PersistedActorRef::Principal(
                pioneer_protocol::PrincipalId::new("P00000000000000000001").unwrap(),
            ),
        )
        .await
        .unwrap();
    // A newly accepted input owns the anchor; a late answer to the older
    // earlier question can extend activity but must not reclaim ownership.
    answer_item(&store, "late-human", "late-ready", READY + 5).await;
    complete(&store, "root", "late-human", READY + 5).await;
    assert_eq!(
        restarted
            .compaction_root_history_activity("ws", "root", "human")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        restarted
            .compaction_root_history_activity("ws", "root", "next-human")
            .await
            .unwrap(),
        Some((READY + 5) * 1000)
    );
    let fence = db.query_one_raw(Statement::from_string(DbBackend::Postgres,
        "SELECT s.turn_id FROM compaction_execution_stop s JOIN compaction_context c ON c.owner=s.owner WHERE c.thread_id='root' AND s.turn_id='human'".to_owned())).await.unwrap();
    assert!(
        fence.is_some(),
        "new input must fence an older root even before first operation admission"
    );
    assert!(
        !restarted
            .compaction_record_history_result(
                "human",
                resumed.revision,
                0,
                HistoryCheckOutcome::Fits,
                &diagnostic,
                READY * 1000 + 20
            )
            .await
            .unwrap()
    );
    assert!(
        restarted
            .compaction_begin_history_attempt(
                "human",
                resumed.revision,
                "{}",
                "fixture",
                READY * 1000 + 30
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        restarted
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty()
    );
    let retired = db
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT state,outcome FROM compaction_history_check WHERE turn_id='human'".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retired.try_get::<String>("", "state").unwrap(), "finished");
    assert_eq!(
        retired.try_get::<String>("", "outcome").unwrap(),
        "cancelled"
    );
}

async fn complete(store: &CrudStore, thread: &str, turn: &str, at: i64) {
    let (_, mut model) = store.get_turn(thread, turn).await.unwrap().unwrap();
    model.status = TurnStatus::Completed;
    store
        .materialize_turn_completed(
            TurnCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: thread.into(),
                turn: model,
            },
            at,
        )
        .await
        .unwrap();
}

async fn answer_item(store: &CrudStore, turn: &str, id: &str, at: i64) {
    store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "root".into(),
                turn_id: turn.into(),
                item: TurnItem::AgentMessage {
                    id: id.into(),
                    text: "fixture original publication".into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            at,
        )
        .await
        .unwrap();
}
