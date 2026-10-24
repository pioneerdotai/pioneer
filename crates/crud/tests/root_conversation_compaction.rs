//! Policy upgrade and durable ownership regressions. Written for external review;
//! no execution is authorized until the reviewer opens the validation gate.
use migration::{Migrator, MigratorTrait};
use pioneer_crud::{
    CrudStore,
    compaction::{HistoryCheckDiagnostic, HistoryCheckOutcome},
};
use pioneer_protocol::{
    ItemCompletedNotification, TurnCompletedNotification, TurnItem, TurnStatus,
};
use pioneer_sqlite::{SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor};
use sea_orm::{ConnectionTrait, Database, DbBackend, Statement};

const POLICY: &str = "m20261024_000001_root_conversation_compaction";
const READY: i64 = 1_792_838_000;

#[tokio::test]
async fn policy_upgrade_preserves_execution_intents_and_root_publication_survives_restart() {
    let connection = Database::connect("sqlite::memory:").await.unwrap();
    let writer = SqliteWriteExecutor::new(connection.clone());
    let before = Migrator::migrations()
        .iter()
        .position(|m| m.name() == POLICY)
        .unwrap();
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, Some(before as u32))
        .await
        .unwrap();
    let store = CrudStore::new(SqliteDatabase::from_executor(connection, writer.clone()))
        .with_maintenance_access();
    let db = store.database_connection();
    // Pre-upgrade rows deliberately have no policy discriminator. Neither a
    // retained descriptor nor managed=1 is permission to warm an executor.
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',1,1)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('root','ws','','agent','m','p','active','user','workspace','2026-10-24T00:00:00Z','2026-10-24T00:00:00Z')",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('child','ws','','agent','m','p','active','system','workspace','2026-10-24T00:00:00Z','2026-10-24T00:00:00Z')",
        "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES('child','root','root',1,'2026-10-24T00:00:00Z')",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,initiated_by_actor_kind,initiated_by_actor_id,created_at,updated_at) VALUES('human','root','in_progress','conversation','user','principal','P00000000000000000001','2026-10-24T00:00:00Z','2026-10-24T00:00:00Z')",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('executor','child','in_progress','conversation','user','2026-10-24T00:00:00Z','2026-10-24T00:00:00Z')",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,initiated_by_actor_kind,initiated_by_actor_id,created_at,updated_at) VALUES('old-human','root','in_progress','conversation','user','principal','P00000000000000000001','2026-10-23T00:00:00Z','2026-10-23T00:00:00Z')",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,initiated_by_actor_kind,initiated_by_actor_id,created_at,updated_at) VALUES('late-human','root','in_progress','conversation','user','principal','P00000000000000000001','2026-10-23T01:00:00Z','2026-10-23T01:00:00Z')",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('monitor','root','completed','conversation','user','2026-10-24T00:00:00Z','2026-10-24T00:00:00Z')",
        "INSERT INTO compaction_history_check(turn_id,descriptor,managed,revision,failures) VALUES('executor','retained-legacy-descriptor',1,9,2)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    // Original ready response predates upgrade. A newer stream of ordinary
    // monitor items must neither renew activity nor force publication to scan
    // the stream. More than one recovery quantum makes cursor progress and a
    // fresh owner (without in-memory state) observable, including equal times.
    answer_item(&store, "old-human", "old-ready", READY + 10).await;
    complete(&store, "root", "old-human", READY + 10).await;
    for n in 0..65 {
        answer_item(&store, "monitor", &format!("report-{n}"), READY + 20).await;
    }
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    // Repeat through the registered migrator: the saved completion marker
    // must make a second startup harmless to retained rows and trigger policy.
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    assert!(
        store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty()
    );
    let legacy = db.query_one_raw(Statement::from_string(DbBackend::Sqlite,
        "SELECT descriptor,revision,failures,purpose FROM compaction_history_check WHERE turn_id='executor'".to_owned())).await.unwrap().unwrap();
    assert_eq!(
        legacy.try_get::<String>("", "descriptor").unwrap(),
        "retained-legacy-descriptor"
    );
    assert_eq!(legacy.try_get::<i64>("", "revision").unwrap(), 9);
    assert_eq!(legacy.try_get::<i64>("", "failures").unwrap(), 2);
    assert_eq!(
        legacy.try_get::<String>("", "purpose").unwrap(),
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
    db.execute_unprepared("CREATE TEMP TRIGGER reject_root_debt BEFORE INSERT ON compaction_history_check WHEN NEW.turn_id='human' BEGIN SELECT RAISE(ABORT,'fixture queue failure'); END").await.unwrap();
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
    db.execute_unprepared("DROP TRIGGER reject_root_debt")
        .await
        .unwrap();
    complete(&store, "root", "human", READY).await;
    assert!(
        store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty(),
        "upgrade debt must wait for bounded activity restoration"
    );
    let cursor = || {
        Statement::from_string(DbBackend::Sqlite,
        "SELECT recovery_pending,recovery_before_ms FROM compaction_root_activity WHERE thread_id='root'".to_owned())
    };
    let seeded = db.query_one_raw(cursor()).await.unwrap().unwrap();
    assert_eq!(seeded.try_get::<i64>("", "recovery_pending").unwrap(), 1);
    assert_eq!(
        seeded.try_get::<i64>("", "recovery_before_ms").unwrap(),
        i64::MAX
    );
    store
        .compaction_recover_root_activity_quantum()
        .await
        .unwrap();
    assert!(
        store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty()
    );
    let progress = db.query_one_raw(cursor()).await.unwrap().unwrap();
    assert_eq!(progress.try_get::<i64>("", "recovery_pending").unwrap(), 1);
    assert!(progress.try_get::<i64>("", "recovery_before_ms").unwrap() < i64::MAX);
    // An independent publication arrives behind the cursor. Its original time
    // is recorded atomically, without resetting progress; the older, later-ready
    // historical answer still wins MAX when another owner resumes recovery.
    answer_item(&store, "late-human", "late-ready", READY + 5).await;
    complete(&store, "root", "late-human", READY + 5).await;
    let interrupted = db.query_one_raw(cursor()).await.unwrap().unwrap();
    assert_eq!(
        interrupted
            .try_get::<i64>("", "recovery_before_ms")
            .unwrap(),
        progress.try_get::<i64>("", "recovery_before_ms").unwrap()
    );
    let recovery_owner = store.with_maintenance_access();
    recovery_owner
        .compaction_recover_root_activity_quantum()
        .await
        .unwrap();
    assert_eq!(
        db.query_one_raw(cursor())
            .await
            .unwrap()
            .unwrap()
            .try_get::<i64>("", "recovery_pending")
            .unwrap(),
        0
    );
    let due = store.compaction_pending_history_checks().await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!((&*due[0].thread_id, &*due[0].turn_id), ("root", "human"));
    assert_eq!(
        store
            .compaction_root_history_activity("ws", "root", "human")
            .await
            .unwrap(),
        Some((READY + 10) * 1000)
    );
    // Reprocessing completion at a later wall clock must not rejuvenate root
    // activity or manufacture a new publication request.
    complete(&store, "root", "human", READY + 100_000).await;
    assert_eq!(
        store
            .compaction_root_history_activity("ws", "root", "human")
            .await
            .unwrap(),
        Some((READY + 10) * 1000)
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
    let restarted = store.with_maintenance_access();
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
    // No operation has been admitted and no native context exists yet.
    // New input must still create the owner and fence the earlier anchor.
    let contexts = db
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT count(*) AS n FROM compaction_context WHERE thread_id='root'".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(contexts.try_get::<i64>("", "n").unwrap(), 0);
    let thread = store.get_thread_model("root").await.unwrap().unwrap();
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
    let fence = db.query_one_raw(Statement::from_string(DbBackend::Sqlite,
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
            DbBackend::Sqlite,
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
