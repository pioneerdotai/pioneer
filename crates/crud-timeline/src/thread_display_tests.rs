use super::*;
use crate::test_support::{
    assert_thread_turn_snapshot_statement, assert_thread_turn_snapshot_statements,
};
use pioneer_crud_access::authorization_scope::list_running_task_scopes;
use pioneer_crud_access::{
    membership::{
        NewThreadMembership, NewWorkspaceMembership, find_accessible_thread_for_principal,
        insert_thread_membership, insert_workspace_membership,
        list_accessible_threads_for_principal,
    },
    test_support::membership_fixture,
};
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_entity::{gateway_principal, thread};
use pioneer_protocol::{AUTH_DOMAIN_ID_LEN, PRINCIPAL_ID_LEN, PrincipalId, generate_id};
use pioneer_protocol::{
    PersistedActorRef, ThreadMode, ThreadStatus, TurnMention, TurnOrigin, TurnPermissionMode,
    TurnPermissionProfileSnapshot, TurnPermissionProfileSource,
};
use sea_orm::{ActiveModelTrait, Set};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement, TransactionTrait};
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Default)]
struct ThreadSnapshotWriteObserver {
    events: std::sync::Mutex<Vec<pioneer_sqlite::SqliteWriteEvent>>,
}

impl pioneer_sqlite::SqliteWriteObserver for ThreadSnapshotWriteObserver {
    fn observe(&self, event: pioneer_sqlite::SqliteWriteEvent) {
        self.events.lock().unwrap().push(event);
    }
}

#[tokio::test]
async fn thread_list_batches_preserve_order_limit_effort_and_snapshot_metadata() {
    pioneer_postgres_test::with_database(async |fixture| {
        let mut connection = fixture.connection().clone();
        let statements = Arc::new(std::sync::Mutex::new(Vec::<Statement>::new()));
        connection.set_metric_callback({
            let statements = statements.clone();
            move |info| statements.lock().unwrap().push(info.statement.clone())
        });
        let db = SqliteDatabase::from(connection);
        let store = ThreadDisplayStore::new(&db);
        db.execute_unprepared(
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws_batch','Batch',TRUE,FALSE)"
        ).await.unwrap();
        for index in 0..231 {
            let thread_id = format!("H{index:020}");
            db.execute_raw(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,created_at,updated_at) \
                 VALUES ($1,'ws_batch','preview','agent','model','provider','idle',$2,$3)",
                vec![thread_id.clone().into(), unix_to_datetime(1_700_000_000 + index).into(),
                     unix_to_datetime(1_700_000_000 + index).into()],
            )).await.unwrap();
            let turns: Vec<(&str, &str, Option<&str>, i64)> = match index % 6 {
                0 => vec![],
                1 => vec![
                    ("z", "conversation", Some("low"), 0),
                    ("a", "conversation", Some("high"), 0),
                    ("t", "task_run", Some("high"), 2),
                ],
                2 => vec![
                    ("a", "conversation", Some("high"), 0),
                    ("b", "conversation", None, 1),
                    ("t", "task_run", Some("high"), 2),
                ],
                3 => vec![("t", "task_run", Some("high"), 2)],
                // Insert z before a: equal timestamps must use ID DESC, not insertion order.
                4 => vec![
                    ("z", "conversation", Some("low"), 1),
                    ("a", "conversation", Some("high"), 1),
                ],
                _ => vec![("a", "conversation", Some("high"), 0)],
            };
            for (suffix, kind, effort, offset) in turns {
                db.execute_raw(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    "INSERT INTO turn(id,thread_id,status,turn_kind,origin,reasoning_effort,created_at,updated_at) \
                     VALUES ($1,$2,'in_progress',$3,'user',$4,$5,$6)",
                    vec![format!("T{index:019}{suffix}").into(), thread_id.clone().into(), kind.into(), effort.into(),
                         unix_to_datetime(1_700_000_000 + offset).into(), unix_to_datetime(1_700_000_000 + offset).into()],
                )).await.unwrap();
            }
        }
        let permission_profile = TurnPermissionProfileSnapshot::from_mode(
            TurnPermissionMode::Supervised,
            TurnPermissionProfileSource::Composer,
        );
        let rich_turn_id = format!("T{:019}t", 229);
        let reply_id = format!("T{:019}a", 229);
        let mention = TurnMention {
            principal_id: PrincipalId::new("P00000000000000000001").unwrap(),
            nickname: "member".to_owned(),
        };
        db.execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "UPDATE turn SET origin='detached_task', send_mode='message', \
             initiated_by_actor_kind='system', author_display_name_snapshot='Batch Author', \
             author_nickname_snapshot='batch_author', author_avatar_revision_snapshot='avatar-7', \
             reply_to_turn_id=$1, mentions_json=$2, message_revision=3, message_deleted_at=created_at, \
             error='snapshot error', prompt_manifest_json='123', \
             permission_profile_snapshot_json=$3 WHERE id=$4",
            vec![reply_id.clone().into(), serde_json::to_string(&vec![mention.clone()]).unwrap().into(),
                 serde_json::to_string(&permission_profile).unwrap().into(), rich_turn_id.clone().into()],
        )).await.unwrap();

        // At 129, the second batch contains only thread 102, which has no Turns.
        // At 130, it also contains thread 101, whose latest Turn is Conversation.
        // Neither needs a fallback query; at 132, TaskRun thread 99 does.
        for (limit, expected_turn_queries) in [
            (0, 0),
            (1, 2),
            (128, 2),
            (129, 3),
            (130, 3),
            (132, 4),
            (231, 4),
        ] {
            statements.lock().unwrap().clear();
            let threads = store
                .list_threads_for_workspace("ws_batch", limit)
                .await
                .unwrap();
            assert_eq!(threads.len(), limit as usize);
            let recorded = statements.lock().unwrap().clone();
            assert_eq!(
                recorded.len(),
                1 + expected_turn_queries,
                "unexpected query count for limit {limit}"
            );
            assert_thread_turn_snapshot_statements(&recorded, &threads);
            for (position, thread) in threads.iter().enumerate() {
                let index = 230 - position;
                assert_eq!(thread.id, format!("H{index:020}"));
                assert_eq!(thread.model, "model");
                assert_eq!(thread.model_provider, "provider");
                assert_eq!(
                    thread.status,
                    ThreadStatus::Idle,
                    "TaskRun must not activate foreground"
                );
                let (suffix, effort, kind) = match index % 6 {
                    0 => {
                        assert!(thread.turns.is_empty());
                        assert!(thread.reasoning_effort.is_none());
                        continue;
                    }
                    1 => ("t", Some("low"), TurnKind::TaskRun),
                    2 | 3 => ("t", None, TurnKind::TaskRun),
                    4 => ("z", Some("low"), TurnKind::Conversation),
                    _ => ("a", Some("high"), TurnKind::Conversation),
                };
                assert_eq!(thread.reasoning_effort.as_deref(), effort);
                assert_eq!(thread.turns.len(), 1);
                assert_eq!(thread.turns[0].id, format!("T{index:019}{suffix}"));
                assert_eq!(thread.turns[0].turn_kind, kind);
                assert!(thread.turns[0].prompt_manifest.is_none());
                if index == 229 {
                    let snapshot = &thread.turns[0];
                    assert_eq!(snapshot.permission_profile, permission_profile);
                    assert_eq!(snapshot.origin, TurnOrigin::DetachedTask);
                    assert_eq!(snapshot.mode, ThreadMode::Message);
                    let author = snapshot.author.as_ref().unwrap();
                    assert_eq!(author.actor, PersistedActorRef::System);
                    assert_eq!(author.display_name, "Batch Author");
                    assert_eq!(author.nickname, "batch_author");
                    assert_eq!(author.avatar_revision.as_deref(), Some("avatar-7"));
                    assert_eq!(
                        snapshot.reply_to_turn_id.as_deref(),
                        Some(reply_id.as_str())
                    );
                    assert_eq!(snapshot.mentions, vec![mention.clone()]);
                    assert_eq!(snapshot.message_revision, 3);
                    assert!(snapshot.message_deleted);
                    assert_eq!(snapshot.error.as_deref(), Some("snapshot error"));
                }
            }
        }
        statements.lock().unwrap().clear();
        assert!(
            store
                .list_threads_for_workspace("missing", 231)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(statements.lock().unwrap().len(), 1);

        // A skipped thread model must never be included in the selected-ID batch.
        db
            .execute_unprepared("UPDATE thread SET mode='unknown' WHERE id='H00000000000000000230'")
            .await
            .unwrap();
        statements.lock().unwrap().clear();
        let threads = store
            .list_threads_for_workspace("ws_batch", 1)
            .await
            .unwrap();
        assert!(threads.is_empty());
        assert_eq!(statements.lock().unwrap().len(), 1);

        // Unknown Turn status still suppresses its snapshot, including effort;
        // malformed selected snapshot metadata still fails through the old decoder.
        db
            .execute_raw(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "UPDATE turn SET status='unknown' WHERE id=$1",
                [rich_turn_id.clone().into()],
            ))
            .await
            .unwrap();
        let threads = store
            .list_threads_for_workspace("ws_batch", 2)
            .await
            .unwrap();
        assert_eq!(threads.len(), 1);
        assert!(threads[0].turns.is_empty());
        assert!(threads[0].reasoning_effort.is_none());
        for corrupt_column in ["permission_profile_snapshot_json", "mentions_json"] {
            db
                .execute_raw(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    format!(
                        "UPDATE turn SET status='in_progress', {corrupt_column}='123' WHERE id=$1"
                    ),
                    [rich_turn_id.clone().into()],
                ))
                .await
                .unwrap();
            assert!(
                store
                    .list_threads_for_workspace("ws_batch", 2)
                    .await
                    .is_err()
            );
            db.execute_raw(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "UPDATE turn SET permission_profile_snapshot_json=$1, mentions_json='[]' WHERE id=$2",
                [serde_json::to_string(&permission_profile).unwrap().into(), rich_turn_id.clone().into()],
            )).await.unwrap();
        }
        Ok(())
    })
    .await
    .unwrap();
}

#[derive(Default)]
struct ThreadSnapshotReadObserver {
    events: std::sync::Mutex<Vec<pioneer_sqlite::SqliteReadEvent>>,
    enqueued: tokio::sync::Notify,
}

impl pioneer_sqlite::SqliteReadObserver for ThreadSnapshotReadObserver {
    fn observe(&self, event: pioneer_sqlite::SqliteReadEvent) {
        self.events.lock().unwrap().push(event);
        if matches!(
            event,
            pioneer_sqlite::SqliteReadEvent::AdmissionEnqueued { .. }
        ) {
            self.enqueued.notify_one();
        }
    }
}

#[pioneer_postgres_test::postgres_test]
async fn thread_turn_snapshot_batches_use_scoped_readers_and_release_cancelled_capacity() {
    use pioneer_sqlite::{
        SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteReadOutcome, SqliteWriteExecutor,
    };
    use sea_orm::StreamTrait;
    use std::time::Duration;
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let mut writer = fixture.connect_with_limit(1).await.unwrap();
    writer.execute_unprepared(
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','Snapshot',TRUE,FALSE); \
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status) \
         VALUES ('thread','ws','','agent','model','provider','idle'); \
         INSERT INTO turn(id,thread_id,status,turn_kind,origin,reasoning_effort,created_at,updated_at) \
         VALUES ('conversation','thread','completed','conversation','user','low','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'), \
                ('task','thread','in_progress','task_run','scheduled_task','high','2026-01-01T00:00:01Z','2026-01-01T00:00:01Z')"
    ).await.unwrap();
    let reader_statements = Arc::new(std::sync::Mutex::new(Vec::<Statement>::new()));
    let writer_statements = Arc::new(std::sync::Mutex::new(Vec::<Statement>::new()));
    writer.set_metric_callback({
        let statements = writer_statements.clone();
        move |info| statements.lock().unwrap().push(info.statement.clone())
    });
    let mut reader = fixture.connect_read_only().await.unwrap();
    reader.set_metric_callback({
        let statements = reader_statements.clone();
        move |info| statements.lock().unwrap().push(info.statement.clone())
    });
    let reads = Arc::new(ThreadSnapshotReadObserver::default());
    let writes = Arc::new(ThreadSnapshotWriteObserver::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, writes.clone()),
        reads.clone(),
    );
    database.validate_reader().await.unwrap();
    let interactive = database.clone();
    let maintenance = interactive.maintenance();
    let critical = interactive.maintenance().with_critical_writes();
    let held_writer = database.begin().await.unwrap();
    let mut selected = Vec::new();
    for (store, class) in [
        (&interactive, SqliteReadClass::Interactive),
        (&maintenance, SqliteReadClass::Maintenance),
        (&critical, SqliteReadClass::Maintenance),
    ] {
        reader_statements.lock().unwrap().clear();
        writer_statements.lock().unwrap().clear();
        writes.events.lock().unwrap().clear();
        reads.events.lock().unwrap().clear();
        selected = tokio::time::timeout(
            Duration::from_secs(5),
            ThreadDisplayStore::new(store).list_threads_for_workspace("ws", 1),
        )
        .await
        .expect("list must finish while the writer is occupied")
        .unwrap();
        assert_eq!(selected[0].turns[0].id, "task");
        assert_eq!(selected[0].reasoning_effort.as_deref(), Some("low"));
        assert_eq!(selected[0].status, ThreadStatus::Idle);
        assert_thread_turn_snapshot_statements(&reader_statements.lock().unwrap(), &selected);
        assert!(writer_statements.lock().unwrap().is_empty());
        assert!(
            writes.events.lock().unwrap().is_empty(),
            "no writer reservation"
        );
        let events = reads.events.lock().unwrap();
        let finished = events
            .iter()
            .filter_map(|event| match event {
                SqliteReadEvent::OperationFinished { class, outcome, .. } => {
                    Some((*class, *outcome))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(finished, vec![(class, SqliteReadOutcome::Ok); 3]);
        if class == SqliteReadClass::Maintenance {
            assert_eq!(
                events
                    .iter()
                    .filter(|event| matches!(
                        event,
                        SqliteReadEvent::AdmissionReleased {
                            active: 0,
                            queue_depth: 0,
                            ..
                        }
                    ))
                    .count(),
                3,
                "each bounded read must release capacity"
            );
        }
    }
    // Tree task activity uses the same physical readers and scheduling
    // classes, even while the writer is occupied by another operation.
    for (store, class) in [
        (&interactive, SqliteReadClass::Interactive),
        (&maintenance, SqliteReadClass::Maintenance),
        (&critical, SqliteReadClass::Maintenance),
    ] {
        reader_statements.lock().unwrap().clear();
        writer_statements.lock().unwrap().clear();
        reads.events.lock().unwrap().clear();
        let page = tokio::time::timeout(
            Duration::from_secs(5),
            list_running_task_scopes(
                store,
                "ws",
                &["thread".into()],
                &PrincipalId::new("P00000000000000000001").unwrap(),
                "",
            ),
        )
        .await
        .expect("task activity must not wait for the writer")
        .unwrap();
        assert!(page.scopes.is_empty());
        assert_eq!(reader_statements.lock().unwrap().len(), 1);
        assert!(writer_statements.lock().unwrap().is_empty());
        assert_eq!(
            reads
                .events
                .lock()
                .unwrap()
                .iter()
                .filter_map(|event| match event {
                    SqliteReadEvent::OperationFinished { class, outcome, .. } =>
                        Some((*class, *outcome)),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![(class, SqliteReadOutcome::Ok)]
        );
    }
    held_writer.rollback().await.unwrap();

    for thread in &mut selected {
        thread.turns.clear();
        thread.reasoning_effort = None;
    }
    let scoped = database.maintenance();
    let held_reader = scoped
        .stream_raw(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT 1 AS value".to_owned(),
        ))
        .await
        .unwrap();
    reads.events.lock().unwrap().clear();
    reader_statements.lock().unwrap().clear();
    let cancelled = tokio::spawn({
        let store = critical.clone();
        let mut selected = selected.clone();
        async move {
            ThreadDisplayStore::new(&store)
                .attach_latest_thread_turn_snapshots(&mut selected, &HashMap::new())
                .await
        }
    });
    // Wait for actual admission state, not a sleep or performance threshold.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if reads.events.lock().unwrap().iter().any(|event| {
                matches!(
                    event,
                    SqliteReadEvent::AdmissionEnqueued {
                        active: 1,
                        queue_depth: 1,
                        ..
                    }
                )
            }) {
                break;
            }
            reads.enqueued.notified().await;
        }
    })
    .await
    .expect("batch selector should queue behind the held reader");
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    assert!(
        reader_statements.lock().unwrap().is_empty(),
        "queued batch must not execute SQL"
    );
    {
        let events = reads.events.lock().unwrap();
        assert!(events.iter().any(|event| matches!(
            event,
            SqliteReadEvent::AdmissionCancelled {
                class: SqliteReadClass::Maintenance,
                active: 1,
                queue_depth: 0,
                ..
            }
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            SqliteReadEvent::OperationFinished {
                class: SqliteReadClass::Maintenance,
                outcome: SqliteReadOutcome::Cancelled,
                ..
            }
        )));
    }
    drop(held_reader);
    // SeaORM emits the held stream's metric on drop; isolate the resumed
    // batch statements from that deliberately occupied test reader.
    reader_statements.lock().unwrap().clear();
    reads.events.lock().unwrap().clear();
    tokio::time::timeout(
        Duration::from_secs(5),
        ThreadDisplayStore::new(&critical)
            .attach_latest_thread_turn_snapshots(&mut selected, &HashMap::new()),
    )
    .await
    .expect("cancelled batch must not leak reader capacity")
    .unwrap();
    assert_eq!(selected[0].turns[0].id, "task");
    assert_eq!(selected[0].reasoning_effort.as_deref(), Some("low"));
    let recorded = reader_statements.lock().unwrap();
    assert_eq!(recorded.len(), 2);
    assert_thread_turn_snapshot_statement(&recorded[0], false);
    assert_thread_turn_snapshot_statement(&recorded[1], true);
    assert_eq!(
        reads
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| matches!(
                event,
                SqliteReadEvent::AdmissionReleased {
                    active: 0,
                    queue_depth: 0,
                    ..
                }
            ))
            .count(),
        2
    );
}

#[tokio::test]
async fn member_thread_collection_filters_private_workspace_and_internal_rows_in_sql() {
    pioneer_postgres_test::with_database(async |test_database| {
            let mut fixture = membership_fixture(test_database.connection()).await;
            let member_b_id =
                PrincipalId::new(generate_id(PRINCIPAL_ID_LEN)).expect("second member id");
            let now = chrono::Utc::now().fixed_offset();
            gateway_principal::ActiveModel {
                id: Set(member_b_id.to_string()),
                gateway_id: Set(fixture.gateway_id.to_string()),
                kind: Set("user".to_owned()),
                role_key: Set(Some("member".to_owned())),
                status: Set("active".to_owned()),
                display_name: Set("Member B".to_owned()),
                nickname: Set("member_b_matrix".to_owned()),
                nickname_key: Set("member_b_matrix".to_owned()),
                created_at: Set(now),
                updated_at: Set(now),
                removed_at: Set(None),
                authorization_guard: Set(1),
                ..Default::default()
            }
            .insert(&fixture.database)
            .await
            .expect("create second Member");

            let transaction = fixture.database.begin().await.expect("begin grants");
            let completion_sequence = pioneer_crud_tasks::completion_inbox::current_completion_sequence(&transaction).await?;
            for (principal_id, workspace_id) in [
                (&fixture.member_id, &fixture.red_workspace_id),
                (&fixture.member_id, &fixture.blue_workspace_id),
                (&member_b_id, &fixture.red_workspace_id),
            ] {
                insert_workspace_membership(
                    &transaction,
                    &NewWorkspaceMembership {
                        gateway_id: fixture.gateway_id.clone(),
                        principal_id: (*principal_id).clone(),
                        workspace_id: (*workspace_id).clone(),
                        granted_by: PersistedActorRef::Principal(fixture.superuser_id.clone()),
                        now,
                    },
                    completion_sequence,
                )
                .await
                .expect("grant overlapping workspace matrix");
            }
            insert_thread_membership(
                &transaction,
                &NewThreadMembership {
                    gateway_id: fixture.gateway_id.clone(),
                    principal_id: fixture.member_id.clone(),
                    workspace_id: fixture.red_workspace_id.clone(),
                    thread_id: fixture.private_thread_id.clone(),
                    added_by: PersistedActorRef::Principal(fixture.superuser_id.clone()),
                    now,
                },
                    completion_sequence,
            )
            .await
            .expect("grant private thread");
            transaction.commit().await.expect("commit grants");

            let workspace_thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
            let internal_thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
            let peer_private_thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
            let blue_workspace_thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
            for (thread_id, workspace_id, access_class, origin_kind, creator_id) in [
                (
                    workspace_thread_id.as_str(),
                    fixture.red_workspace_id.as_str(),
                    "workspace",
                    "user",
                    fixture.superuser_id.to_string(),
                ),
                (
                    internal_thread_id.as_str(),
                    fixture.red_workspace_id.as_str(),
                    "internal",
                    "task_run",
                    fixture.superuser_id.to_string(),
                ),
                (
                    peer_private_thread_id.as_str(),
                    fixture.red_workspace_id.as_str(),
                    "private",
                    "user",
                    member_b_id.to_string(),
                ),
                (
                    blue_workspace_thread_id.as_str(),
                    fixture.blue_workspace_id.as_str(),
                    "workspace",
                    "user",
                    fixture.member_id.to_string(),
                ),
            ] {
                thread::ActiveModel {
                    workspace_unread_started_at: sea_orm::NotSet,
                    id: Set(thread_id.to_owned()),
                    workspace_id: Set(workspace_id.to_owned()),
                    name: Set(None),
                    preview: Set(String::new()),
                    preview_author_json: Set(None),
                    mode: Set("chat".to_owned()),
                    model: Set("test".to_owned()),
                    model_provider: Set("test".to_owned()),
                    status: Set("idle".to_owned()),
                    origin_kind: Set(origin_kind.to_owned()),
                    // Deliberately visible to exercise access_class fail-closed filtering.
                    sidebar_visibility: Set("visible".to_owned()),
                    access_class: Set(access_class.to_owned()),
                    agent_nickname: Set(None),
                    agent_role: Set(None),
                    summary: Set(None),
                    summary_turn_count: Set(None),
                    created_at: Set(now),
                    updated_at: Set(now),
                    created_by_actor_id: Set(Some(creator_id)),
                    created_by_actor_kind: Set(Some("principal".to_owned())),
                    ..Default::default()
                }
                .insert(&fixture.database)
                .await
                .expect("insert collection fixture thread");
            }

            let transaction = fixture
                .database
                .begin()
                .await
                .expect("begin peer private grant");
            let completion_sequence = pioneer_crud_tasks::completion_inbox::current_completion_sequence(&transaction).await?;
            insert_thread_membership(
                &transaction,
                &NewThreadMembership {
                    gateway_id: fixture.gateway_id.clone(),
                    principal_id: member_b_id.clone(),
                    workspace_id: fixture.red_workspace_id.clone(),
                    thread_id: peer_private_thread_id.clone(),
                    added_by: PersistedActorRef::Principal(member_b_id.clone()),
                    now,
                },
                    completion_sequence,
            )
            .await
            .expect("grant peer private thread");
            transaction
                .commit()
                .await
                .expect("commit peer private grant");

            let member_a_red = list_accessible_threads_for_principal(
                &fixture.database,
                &fixture.member_id,
                fixture.red_workspace_id.as_str(),
                100,
            )
            .await
            .expect("list Member A red threads")
            .into_iter()
            .map(|thread| thread.id)
            .collect::<BTreeSet<_>>();
            assert_eq!(
                member_a_red,
                BTreeSet::from([
                    fixture.private_thread_id.clone(),
                    workspace_thread_id.clone()
                ])
            );
            assert!(!member_a_red.contains(&internal_thread_id));
            assert!(!member_a_red.contains(&peer_private_thread_id));
            assert_eq!(
                find_accessible_thread_for_principal(
                    &fixture.database,
                    &fixture.member_id,
                    fixture.red_workspace_id.as_str(),
                    workspace_thread_id.as_str(),
                )
                .await
                .expect("resolve accessible workspace thread")
                .map(|thread| thread.id),
                Some(workspace_thread_id.clone())
            );
            assert!(
                find_accessible_thread_for_principal(
                    &fixture.database,
                    &fixture.member_id,
                    fixture.red_workspace_id.as_str(),
                    peer_private_thread_id.as_str(),
                )
                .await
                .expect("hide peer private thread")
                .is_none()
            );
            assert!(
                find_accessible_thread_for_principal(
                    &fixture.database,
                    &fixture.member_id,
                    fixture.blue_workspace_id.as_str(),
                    workspace_thread_id.as_str(),
                )
                .await
                .expect("reject exact parent mismatch")
                .is_none()
            );

            let member_b_red = list_accessible_threads_for_principal(
                &fixture.database,
                &member_b_id,
                fixture.red_workspace_id.as_str(),
                100,
            )
            .await
            .expect("list Member B red threads")
            .into_iter()
            .map(|thread| thread.id)
            .collect::<BTreeSet<_>>();
            assert_eq!(
                member_b_red,
                BTreeSet::from([peer_private_thread_id.clone(), workspace_thread_id.clone()])
            );
            assert!(!member_b_red.contains(&fixture.private_thread_id));
            assert!(!member_b_red.contains(&internal_thread_id));

            let member_a_blue = list_accessible_threads_for_principal(
                &fixture.database,
                &fixture.member_id,
                fixture.blue_workspace_id.as_str(),
                100,
            )
            .await
            .expect("list Member A blue threads")
            .into_iter()
            .map(|thread| thread.id)
            .collect::<BTreeSet<_>>();
            assert_eq!(
                member_a_blue,
                BTreeSet::from([blue_workspace_thread_id.clone()])
            );

            let member_b_blue = list_accessible_threads_for_principal(
                &fixture.database,
                &member_b_id,
                fixture.blue_workspace_id.as_str(),
                100,
            )
            .await
            .expect("list ungranted Member B blue threads")
            .into_iter()
            .map(|thread| thread.id)
            .collect::<BTreeSet<_>>();
            assert!(member_b_blue.is_empty());

            // Exercise the ThreadDisplayStore attachment path with the same ACL matrix.
            // Turns on excluded rows must never enter the selected-ID batches.
            for id in [
                &fixture.private_thread_id,
                &workspace_thread_id,
                &internal_thread_id,
                &peer_private_thread_id,
                &blue_workspace_thread_id,
            ] {
                fixture
                    .database
                    .execute_raw(Statement::from_sql_and_values(
                        DatabaseBackend::Postgres,
                        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) \
                     VALUES ($1,$2,'in_progress','task_run','scheduled_task',$3,$4)",
                        vec![
                            generate_id(AUTH_DOMAIN_ID_LEN).into(),
                            id.clone().into(),
                            now.into(),
                            now.into(),
                        ],
                    ))
                    .await
                    .unwrap();
            }
            let statements = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Statement>::new()));
            fixture.database.set_metric_callback({
                let statements = statements.clone();
                move |info| statements.lock().unwrap().push(info.statement.clone())
            });
            let connection = SqliteDatabase::from(fixture.database.clone());
            let store = ThreadDisplayStore::new(&connection);
            for (principal, workspace, expected) in [
                (&fixture.member_id, &fixture.red_workspace_id, &member_a_red),
                (&member_b_id, &fixture.red_workspace_id, &member_b_red),
                (
                    &fixture.member_id,
                    &fixture.blue_workspace_id,
                    &member_a_blue,
                ),
                (&member_b_id, &fixture.blue_workspace_id, &member_b_blue),
            ] {
                statements.lock().unwrap().clear();
                let listed = store
                    .list_accessible_threads_for_principal(principal, workspace, 100)
                    .await
                    .unwrap();
                assert_eq!(
                    listed
                        .iter()
                        .map(|thread| thread.id.clone())
                        .collect::<BTreeSet<_>>(),
                    *expected
                );
                crate::test_support::assert_thread_turn_snapshot_statements(
                    &statements.lock().unwrap(),
                    &listed,
                );
                for thread in listed {
                    assert_eq!(thread.turns.len(), 1);
                    assert_eq!(
                        thread.turns[0].turn_kind,
                        pioneer_protocol::TurnKind::TaskRun
                    );
                    assert!(thread.reasoning_effort.is_none());
                }
            }

            // 229 public rows plus the two original authorized rows = 231. Equal
            // updated_at values also exercise the membership selector's ID ASC tie.
            for index in 0..229 {
                let id = format!("B{index:020}");
                fixture.database.execute_raw(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,access_class,created_at,updated_at) \
                     VALUES ($1,$2,'','chat','test','test','idle','workspace',$3,$4)",
                    vec![id.clone().into(), fixture.red_workspace_id.clone().into(), now.into(),
                         (now + chrono::Duration::seconds(1)).into()],
                )).await.unwrap();
                fixture
                    .database
                    .execute_raw(Statement::from_sql_and_values(
                        DatabaseBackend::Postgres,
                        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) \
                     VALUES ($1,$2,'in_progress','task_run','scheduled_task',$3,$4)",
                        vec![
                            format!("C{index:020}").into(),
                            id.into(),
                            now.into(),
                            now.into(),
                        ],
                    ))
                    .await
                    .unwrap();
            }
            for (limit, turn_queries) in [(0, 0), (128, 2), (129, 4), (231, 4)] {
                let expected = list_accessible_threads_for_principal(
                    &fixture.database,
                    &fixture.member_id,
                    &fixture.red_workspace_id,
                    limit,
                )
                .await
                .unwrap()
                .into_iter()
                .map(|model| model.id)
                .collect::<Vec<_>>();
                statements.lock().unwrap().clear();
                let listed = store
                    .list_accessible_threads_for_principal(
                        &fixture.member_id,
                        &fixture.red_workspace_id,
                        limit,
                    )
                    .await
                    .unwrap();
                assert_eq!(listed.len(), limit as usize);
                assert_eq!(
                    listed
                        .iter()
                        .map(|thread| thread.id.clone())
                        .collect::<Vec<_>>(),
                    expected
                );
                let recorded = statements.lock().unwrap().clone();
                assert_eq!(recorded.len(), 1 + turn_queries);
                crate::test_support::assert_thread_turn_snapshot_statements(&recorded, &listed);
                assert!(
                    listed
                        .iter()
                        .all(|thread| thread.workspace_id == fixture.red_workspace_id)
                );
                assert!(listed.iter().all(
                    |thread| thread.id != peer_private_thread_id && thread.id != internal_thread_id
                ));
                if limit == 231 {
                    assert!(
                        listed
                            .iter()
                            .any(|thread| thread.id == fixture.private_thread_id)
                    );
                    assert!(listed.iter().any(|thread| thread.id == workspace_thread_id));
                }
            }
            Ok(())
        }).await.unwrap();
}

#[tokio::test]
async fn list_threads_for_workspace_includes_latest_turn_marker() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = SqliteDatabase::new(fixture.connect_read_only().await?, fixture.connection().clone());
        let store = ThreadDisplayStore::new(&db);
        let workspace_id = "ws_000000000000000001";
        let thread_id = "thr_000000000000000003";
        let task_run_turn_id = "turn_000000000000000005";
        let default_effort_turn_id = "turn_000000000000000006";
        let latest_task_run_turn_id = "turn_000000000000000007";

        db.execute_unprepared(
            "INSERT INTO workspace (id, name) VALUES ('ws_000000000000000001', 'Thread list');
             INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status, updated_at)
                 VALUES ('thr_000000000000000003', 'ws_000000000000000001', '', 'agent',
                         'o3', 'custom-provider', 'active', to_timestamp(1700000100));
             INSERT INTO turn (id,thread_id,status,turn_kind,origin,reasoning_effort,created_at,updated_at)
                 VALUES ('turn_000000000000000003','thr_000000000000000003','in_progress',
                         'conversation','user','low',to_timestamp(1700000000),to_timestamp(1700000000)),
                        ('turn_000000000000000004','thr_000000000000000003','in_progress',
                         'conversation','user','high',to_timestamp(1700000100),to_timestamp(1700000100)),
                        ('turn_000000000000000005','thr_000000000000000003','in_progress',
                         'task_run','detached_task',NULL,to_timestamp(1700000101),to_timestamp(1700000101))",
        ).await?;
        let threads = store
            .list_threads_for_workspace(workspace_id, 10)
            .await
            .expect("list threads should succeed");
        let listed = threads
            .iter()
            .find(|thread| thread.id == thread_id)
            .expect("thread should be listed");

        assert_eq!(listed.model, "o3");
        assert_eq!(listed.model_provider, "custom-provider");
        assert_eq!(listed.reasoning_effort.as_deref(), Some("high"));
        assert_eq!(listed.turns.len(), 1);
        assert_eq!(listed.turns[0].id, task_run_turn_id);
        assert_eq!(listed.turns[0].turn_kind, TurnKind::TaskRun);

        let fetched = store
            .get_thread_model(thread_id)
            .await
            .expect("get thread model should succeed")
            .expect("thread should exist");
        assert_eq!(fetched.reasoning_effort.as_deref(), Some("high"));
        assert_eq!(fetched.turns.len(), 1);
        assert_eq!(fetched.turns[0].id, task_run_turn_id);
        assert_eq!(fetched.turns[0].turn_kind, TurnKind::TaskRun);

        db.execute_unprepared(
            "INSERT INTO turn (id,thread_id,status,turn_kind,origin,reasoning_effort,created_at,updated_at)
                 VALUES ('turn_000000000000000006','thr_000000000000000003','completed',
                         'conversation','user',NULL,to_timestamp(1700000102),to_timestamp(1700000102))",
        ).await?;
        let listed = store
            .list_threads_for_workspace(workspace_id, 10)
            .await
            .unwrap();
        assert!(listed[0].reasoning_effort.is_none());
        assert_eq!(listed[0].turns[0].id, default_effort_turn_id);

        db.execute_unprepared(
            "INSERT INTO turn (id,thread_id,status,turn_kind,origin,reasoning_effort,created_at,updated_at)
                 VALUES ('turn_000000000000000007','thr_000000000000000003','in_progress',
                         'task_run','detached_task',NULL,to_timestamp(1700000103),to_timestamp(1700000103))",
        ).await?;
        let fetched = store
            .get_thread_model(thread_id)
            .await
            .expect("thread should load after default effort")
            .expect("thread should exist after default effort");
        assert!(
            fetched.reasoning_effort.is_none(),
            "the latest Conversation turn must be allowed to clear an older effort"
        );
        assert_eq!(fetched.turns.len(), 1);
        assert_eq!(fetched.turns[0].id, latest_task_run_turn_id);
        assert_eq!(fetched.turns[0].turn_kind, TurnKind::TaskRun);
        let listed = store
            .list_threads_for_workspace(workspace_id, 10)
            .await
            .unwrap();
        assert!(listed[0].reasoning_effort.is_none());
        assert_eq!(listed[0].turns[0].id, latest_task_run_turn_id);
        assert_eq!(listed[0].turns[0].turn_kind, TurnKind::TaskRun);
        Ok(())
    })
    .await
    .unwrap();
}
