//! Real ordinary subscriber boundary, with durable Member leases. NOT RUN.
use super::*;
use pioneer_crud::{
    AUTHORIZATION_FACTS_BATCH_LIMIT, NewActiveDeviceSessionRow, NewThreadMembership,
    NewWorkspaceMembership, insert_active_device_session, insert_thread_membership,
    insert_workspace_membership,
};
use pioneer_protocol::{
    AuthSessionId, ClientInstallationDescriptor, ClientKind, DeviceId, PrincipalId, TokenFamilyId,
};
use pioneer_sqlite::{
    SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteReadOutcome, SqliteWriteExecutor,
};
use sea_orm::{ConnectOptions, Statement};

const THREAD: &str = "T0000000000000000009A";
const METHOD: &str = "test/bounded-fanout";

type Principal = Arc<crate::auth::AuthenticatedSessionPrincipal>;

struct Fixture {
    _directory: tempfile::TempDir,
    database: SqliteDatabase,
    processor: MessageProcessor,
    workspace: String,
    statements: Arc<std::sync::Mutex<Vec<Statement>>>,
    observer: Arc<NativeSchedulingObserver>,
}

impl Fixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("fanout.sqlite");
        let mut options = ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
        options
            .max_connections(1)
            .sqlx_logging(false)
            .map_sqlx_sqlite_opts(|o| o.pragma("foreign_keys", "OFF"));
        let writer = Database::connect(options).await.unwrap();
        let (_, _, workspace) = setup_workspace_manager_with_connection(writer.clone()).await;
        writer
            .execute_unprepared("PRAGMA journal_mode=WAL")
            .await
            .unwrap();
        let mut options =
            ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
        options
            .max_connections(2)
            .sqlx_logging(false)
            .map_sqlx_sqlite_opts(|o| {
                o.read_only(true)
                    .create_if_missing(false)
                    .pragma("query_only", "ON")
                    .pragma("foreign_keys", "OFF")
            });
        let mut reader = Database::connect(options).await.unwrap();
        let statements = Arc::new(std::sync::Mutex::new(Vec::new()));
        reader.set_metric_callback({
            let statements = statements.clone();
            move |info| statements.lock().unwrap().push(info.statement.clone())
        });
        let observer = Arc::new(NativeSchedulingObserver::default());
        let database = SqliteDatabase::from_executor_with_read_observer(
            reader,
            SqliteWriteExecutor::with_observer(writer, observer.clone()),
            observer.clone(),
        );
        database.validate_reader().await.unwrap();
        assert!(database.reader_query_only_enabled().await.unwrap());
        assert_eq!(
            database
                .query_one_raw(Statement::from_string(
                    sea_orm::DbBackend::Sqlite,
                    "SELECT foreign_keys AS value FROM pragma_foreign_keys"
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get::<i64>("", "value")
                .unwrap(),
            0
        );
        let identity = Arc::new(
            crate::identity::bootstrap_identity(&database)
                .await
                .unwrap()
                .snapshot,
        );
        let auth = Arc::new(
            crate::auth::GatewayAuthService::new(
                database.clone(),
                pioneer_config::GatewayAuthConfig::default(),
                identity,
                &crate::secrets::AuthKeyMaterial::from_test_bytes(vec![8; 64]),
                &crate::secrets::AuthKeyMaterial::from_test_bytes(vec![9; 64]),
            )
            .unwrap(),
        );
        let store = Arc::new(CrudStore::new(database.clone()));
        let manager = Arc::new(ThreadManager::new("o4-mini", "openai"));
        let started = manager
            .system_thread_start_seeded(
                workspace.clone(),
                serde_json::from_value(json!({"thread_id": THREAD, "workspace_id": workspace, "visibility": "private"}))
                    .unwrap(),
                None,
                None,
            )
            .await
            .unwrap();
        store
            .create_superuser_thread(
                &started.response.thread,
                PersistedActorRef::Principal(authenticated_test_superuser().principal_id.clone()),
                pioneer_crud::PersistedThreadAccessClass::Private,
            )
            .await
            .unwrap();
        let processor = MessageProcessor::new(
            manager,
            test_provider(),
            Arc::new(SessionManager::new()),
            Arc::new(WorkspaceManager::new(database.clone())),
            store,
            test_gateway_secrets(),
            test_summary_config(),
            test_tool_loop_config(),
        )
        .with_auth_service(auth);
        Self {
            _directory: directory,
            database,
            processor,
            workspace,
            statements,
            observer,
        }
    }

    async fn member(&self, n: usize, participant: bool) -> Principal {
        let now = chrono::Utc::now().fixed_offset();
        let mut principal = (*authenticated_test_superuser()).clone();
        principal.principal_id = PrincipalId::new(format!("P{n:020}")).unwrap();
        principal.kind = pioneer_protocol::PrincipalKind::User;
        principal.role_key = Some(RoleKey::member());
        pioneer_crud::create_member_principal(
            &self.database,
            pioneer_crud::NewMemberPrincipalRow {
                id: principal.principal_id.clone(),
                gateway_id: principal.gateway_id.clone(),
                role_key: RoleKey::member(),
                display_name: format!("Member {n}"),
                nickname: format!("member-{n}"),
                nickname_key: format!("member-{n}"),
                now,
            },
        )
        .await
        .unwrap();
        let tx = self.database.begin().await.unwrap();
        insert_workspace_membership(
            &tx,
            &NewWorkspaceMembership {
                gateway_id: principal.gateway_id.clone(),
                principal_id: principal.principal_id.clone(),
                workspace_id: self.workspace.clone(),
                granted_by: PersistedActorRef::System,
                now,
            },
        )
        .await
        .unwrap();
        if participant {
            insert_thread_membership(
                &tx,
                &NewThreadMembership {
                    gateway_id: principal.gateway_id.clone(),
                    principal_id: principal.principal_id.clone(),
                    workspace_id: self.workspace.clone(),
                    thread_id: THREAD.into(),
                    added_by: PersistedActorRef::System,
                    now,
                },
            )
            .await
            .unwrap();
        }
        tx.commit().await.unwrap();
        self.device(Arc::new(principal), n).await
    }

    async fn device(&self, owner: Principal, n: usize) -> Principal {
        let now = chrono::Utc::now().fixed_offset();
        let mut principal = (*owner).clone();
        principal.device_id = DeviceId::new(format!("D{n:020}")).unwrap();
        principal.session_id = AuthSessionId::new(format!("S{n:020}")).unwrap();
        insert_active_device_session(
            &self.database,
            NewActiveDeviceSessionRow {
                device_id: principal.device_id.clone(),
                session_id: principal.session_id.clone(),
                gateway_id: principal.gateway_id.clone(),
                principal_id: principal.principal_id.clone(),
                token_family_id: TokenFamilyId::new(format!("F{n:020}")).unwrap(),
                installation: ClientInstallationDescriptor {
                    installation_id: format!("fanout-{n}"),
                    display_name: "Fanout".into(),
                    client_kind: ClientKind::Desktop,
                    platform: None,
                    client_version: None,
                },
                activation_token_hash: [0; 32],
                activation_locator_hash: [0; 32],
                refresh_expires_at: now + chrono::Duration::days(1),
                now,
            },
        )
        .await
        .unwrap();
        Arc::new(principal)
    }

    async fn connect(&self, principal: Principal) -> (ConnectionId, mpsc::Receiver<Message>) {
        let (tx, rx) = mpsc::channel(4);
        let identity = crate::thread::ThreadSubscriptionIdentity::new(
            principal.principal_id.clone(),
            principal.session_id.clone(),
        );
        let connection = self
            .processor
            .session_manager
            .register_connection(tx, principal)
            .await
            .unwrap();
        self.processor
            .thread_manager
            .thread_start_seeded_authenticated(
                connection,
                identity,
                self.workspace.clone(),
                serde_json::from_value(
                    json!({"thread_id": THREAD, "workspace_id": self.workspace}),
                )
                .unwrap(),
                None,
                None,
            )
            .await
            .unwrap();
        (connection, rx)
    }

    async fn emit<F: Fn()>(&self, on_serialize: F) -> usize {
        let serializations = Arc::new(AtomicUsize::new(0));
        self.processor
            .send_notification_to_thread_subscribers(
                THREAD,
                METHOD,
                &NotificationSerializationProbe {
                    serializations: serializations.clone(),
                    on_serialize,
                },
            )
            .await;
        serializations.load(Ordering::SeqCst)
    }

    async fn assert_candidate_denied_before_serialization(&self, connection_id: ConnectionId) {
        let subscribers = self
            .processor
            .thread_manager
            .subscribed_connections(THREAD)
            .await
            .into_iter()
            .filter(|subscriber| subscriber.connection_id == connection_id)
            .collect::<Vec<_>>();
        assert_eq!(
            subscribers.len(),
            1,
            "the sole original candidate must be an actual subscriber"
        );
        let serializations = Arc::new(AtomicUsize::new(0));
        self.processor
            .send_notification_to_authorized_thread_subscribers(
                THREAD,
                METHOD,
                &NotificationSerializationProbe {
                    serializations: serializations.clone(),
                    on_serialize: || {},
                },
                subscribers,
            )
            .await;
        assert_eq!(
            serializations.load(Ordering::SeqCst),
            0,
            "the sole candidate must be rejected before serialization"
        );
    }

    fn clear_observation(&self) {
        self.statements.lock().unwrap().clear();
        self.observer.reads.lock().unwrap().clear();
        self.observer.writes.lock().unwrap().clear();
    }
}

fn delivered(rx: &mut mpsc::Receiver<Message>) {
    let Message::Text(text) = rx.try_recv().expect("authorized notification") else {
        panic!("text notification expected")
    };
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&text).unwrap()["method"],
        METHOD
    );
    assert!(
        rx.try_recv().is_err(),
        "duplicate candidate must not duplicate delivery"
    );
}

#[tokio::test]
async fn ordinary_fanout_shares_bounded_prefix_on_scoped_query_only_readers() {
    let f = Fixture::new().await;
    let principal = f.member(10, true).await;
    let mut receivers = Vec::new();
    for _ in 0..AUTHORIZATION_FACTS_BATCH_LIMIT + 1 {
        receivers.push(f.connect(principal.clone()).await.1);
    }
    for (processor, class) in [
        (f.processor.clone(), SqliteReadClass::Interactive),
        (
            f.processor.scoped_for_background_reconciliation(),
            SqliteReadClass::Maintenance,
        ),
    ] {
        let held_writer = f.database.begin().await.unwrap();
        f.clear_observation();
        let serializations = Arc::new(AtomicUsize::new(0));
        let subscribers = processor
            .thread_manager
            .subscribed_connections(THREAD)
            .await;
        let mut duplicate_input = subscribers.clone();
        duplicate_input.extend(subscribers);
        // This callback runs before final per-recipient SQL. Its observations
        // assert only the new stage's cost, never the whole notification cost.
        let payload = NotificationSerializationProbe {
            serializations: serializations.clone(),
            on_serialize: || {
                let statements = f.statements.lock().unwrap();
                for (table, expected) in [
                    ("thread", 1),
                    ("workspace", 1),
                    ("workspace_membership", 2),
                    ("thread_membership", 2),
                    ("auth_session", 2),
                ] {
                    assert_eq!(
                        statements
                            .iter()
                            .filter(|s| s.sql.contains(&format!("FROM \"{table}\"")))
                            .count(),
                        expected,
                        "pre-stage {table}"
                    );
                }
                for statement in statements.iter() {
                    assert!(
                        statement.values.as_ref().map_or(0, |v| v.0.len())
                            <= AUTHORIZATION_FACTS_BATCH_LIMIT + 2
                    );
                }
                let reads = f.observer.reads.lock().unwrap();
                let finished = reads
                    .iter()
                    .filter_map(|event| match event {
                        SqliteReadEvent::OperationFinished { class, outcome, .. } => {
                            Some((*class, *outcome))
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                assert_eq!(finished, vec![(class, SqliteReadOutcome::Ok); 8]);
                assert_eq!(
                    reads
                        .iter()
                        .filter(|event| matches!(event, SqliteReadEvent::AdmissionAcquired { .. }))
                        .count(),
                    reads
                        .iter()
                        .filter(|event| matches!(event, SqliteReadEvent::AdmissionReleased { .. }))
                        .count()
                );
                assert!(
                    reads
                        .iter()
                        .filter_map(|event| match event {
                            SqliteReadEvent::AdmissionReleased {
                                active,
                                queue_depth,
                                ..
                            } => Some((*active, *queue_depth)),
                            _ => None,
                        })
                        .all(|state| state == (0, 0)),
                    "no reader admission survives into serialization"
                );
                assert!(f.observer.writes.lock().unwrap().is_empty());
            },
        };
        processor
            .send_notification_to_authorized_thread_subscribers(
                THREAD,
                METHOD,
                &payload,
                duplicate_input,
            )
            .await;
        assert_eq!(serializations.load(Ordering::SeqCst), 1);
        for rx in &mut receivers {
            delivered(rx);
        }
        held_writer.rollback().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_fanout_durable_leases_private_acl_and_fresh_final_subset() {
    let f = Fixture::new().await;
    let allowed = f.member(10, true).await;
    let revoked = f.device(allowed.clone(), 11).await;
    let denied = f.member(12, false).await;
    let (_, mut allowed_rx) = f.connect(allowed.clone()).await;
    let (_, mut revoked_rx) = f.connect(revoked.clone()).await;
    let (denied_connection, mut denied_rx) = f.connect(denied.clone()).await;
    pioneer_entity::device::Entity::update_many()
        .col_expr(
            pioneer_entity::device::Column::Status,
            Expr::value("revoked"),
        )
        .filter(pioneer_entity::device::Column::Id.eq(revoked.device_id.to_string()))
        .exec(&f.database)
        .await
        .unwrap();
    // Even the creator needs private participant proof.
    pioneer_entity::thread::Entity::update_many()
        .col_expr(
            pioneer_entity::thread::Column::CreatedByActorKind,
            Expr::value("principal"),
        )
        .col_expr(
            pioneer_entity::thread::Column::CreatedByActorId,
            Expr::value(denied.principal_id.to_string()),
        )
        .filter(pioneer_entity::thread::Column::Id.eq(THREAD))
        .exec(&f.database)
        .await
        .unwrap();
    assert_eq!(f.emit(|| {}).await, 1);
    delivered(&mut allowed_rx);
    assert!(revoked_rx.try_recv().is_err());
    assert!(denied_rx.try_recv().is_err());

    f.assert_candidate_denied_before_serialization(denied_connection).await;
    assert!(denied_rx.try_recv().is_err());
    assert!(
        allowed_rx.try_recv().is_err(),
        "valid connection outside the original list must receive nothing"
    );
    assert!(revoked_rx.try_recv().is_err());

    // A fresh event observes workspace visibility through the same canonical
    // policy, while the second device's revoked lease still excludes it.
    pioneer_entity::thread::Entity::update_many()
        .col_expr(
            pioneer_entity::thread::Column::AccessClass,
            Expr::value("workspace"),
        )
        .filter(pioneer_entity::thread::Column::Id.eq(THREAD))
        .exec(&f.database)
        .await
        .unwrap();
    assert_eq!(f.emit(|| {}).await, 1);
    delivered(&mut allowed_rx);
    delivered(&mut denied_rx);
    assert!(revoked_rx.try_recv().is_err());
    pioneer_entity::thread::Entity::update_many()
        .col_expr(
            pioneer_entity::thread::Column::AccessClass,
            Expr::value("private"),
        )
        .filter(pioneer_entity::thread::Column::Id.eq(THREAD))
        .exec(&f.database)
        .await
        .unwrap();

    // Actual durable mutations at serialization: revoke the allowed lease and
    // grant the denied Member. Final fresh reads must cut the former and must
    // not expand the pre-allowed subset to the latter.
    let db = f.database.clone();
    let workspace = f.workspace.clone();
    let allowed_session = allowed.session_id.to_string();
    let denied_id = denied.principal_id.clone();
    assert_eq!(
        f.emit(|| {
            let db = db.clone();
            let workspace = workspace.clone();
            let allowed_session = allowed_session.clone();
            let denied_id = denied_id.clone();
            std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async move {
                        pioneer_entity::auth_session::Entity::update_many()
                            .col_expr(
                                pioneer_entity::auth_session::Column::Status,
                                Expr::value("revoked"),
                            )
                            .filter(pioneer_entity::auth_session::Column::Id.eq(allowed_session))
                            .exec(&db)
                            .await
                            .unwrap();
                        let tx = db.begin().await.unwrap();
                        insert_thread_membership(
                            &tx,
                            &NewThreadMembership {
                                gateway_id: authenticated_test_superuser().gateway_id.clone(),
                                principal_id: denied_id,
                                workspace_id: workspace,
                                thread_id: THREAD.into(),
                                added_by: PersistedActorRef::System,
                                now: chrono::Utc::now().fixed_offset(),
                            },
                        )
                        .await
                        .unwrap();
                        tx.commit().await.unwrap();
                    })
            })
            .join()
            .unwrap();
        })
        .await,
        1
    );
    assert!(allowed_rx.try_recv().is_err());
    assert!(revoked_rx.try_recv().is_err());
    assert!(denied_rx.try_recv().is_err());
    assert_eq!(f.emit(|| {}).await, 1);
    delivered(&mut denied_rx);
}

#[tokio::test]
async fn ordinary_fanout_rejects_missing_corrupt_and_cross_linked_lease_facts_before_serializing() {
    let f = Fixture::new().await;
    let principal = f.member(10, true).await;
    let other = f.device(principal.clone(), 11).await;
    let foreign = f.member(12, true).await;
    let (_, mut rx) = f.connect(principal.clone()).await;
    // One selected session row cannot validate a different actual device,
    // principal, or expired access credential in another connection.
    for (device, principal_id, expiry) in [
        (
            other.device_id.clone(),
            principal.principal_id.clone(),
            u64::MAX,
        ),
        (
            principal.device_id.clone(),
            foreign.principal_id.clone(),
            u64::MAX,
        ),
        (
            principal.device_id.clone(),
            principal.principal_id.clone(),
            0,
        ),
    ] {
        let mut captured = (*principal).clone();
        captured.device_id = device;
        captured.principal_id = principal_id;
        captured.access_expires_at_unix = expiry;
        let (connection, mut invalid_rx) = f.connect(Arc::new(captured)).await;
        assert_eq!(f.emit(|| {}).await, 1);
        delivered(&mut rx);
        assert!(invalid_rx.try_recv().is_err());
        f.assert_candidate_denied_before_serialization(connection).await;
        assert!(invalid_rx.try_recv().is_err());
        assert!(
            rx.try_recv().is_err(),
            "valid connection outside the original list must receive nothing"
        );
        f.processor
            .session_manager
            .unregister_connection(connection)
            .await;
        f.processor
            .thread_manager
            .thread_unsubscribe(connection, THREAD)
            .await;
    }
    // Corrupt persisted rows deliberately bypass CHECK constraints as well as
    // unenforced foreign keys; normal fixture creation uses valid typed CRUD.
    f.database
        .execute_unprepared("PRAGMA ignore_check_constraints=ON")
        .await
        .unwrap();
    // Restore each mutation through the runtime writer. No missing/error case
    // can serialize when this is the only remaining actual candidate.
    for (mutation, restore) in [
        (
            "UPDATE auth_session SET device_id='D00000000000000000999' WHERE id='S00000000000000000010'",
            "UPDATE auth_session SET device_id='D00000000000000000010' WHERE id='S00000000000000000010'",
        ),
        (
            "UPDATE device SET principal_id='P00000000000000000999' WHERE id='D00000000000000000010'",
            "UPDATE device SET principal_id='P00000000000000000010' WHERE id='D00000000000000000010'",
        ),
        (
            "UPDATE auth_session SET refresh_generation=-1 WHERE id='S00000000000000000010'",
            "UPDATE auth_session SET refresh_generation=0 WHERE id='S00000000000000000010'",
        ),
        (
            "UPDATE auth_session SET refresh_expires_at='2000-01-01T00:00:00+00:00' WHERE id='S00000000000000000010'",
            "UPDATE auth_session SET refresh_expires_at='2100-01-01T00:00:00+00:00' WHERE id='S00000000000000000010'",
        ),
        (
            "UPDATE gateway_principal SET kind='malformed' WHERE id='P00000000000000000010'",
            "UPDATE gateway_principal SET kind='user' WHERE id='P00000000000000000010'",
        ),
    ] {
        f.database.execute_unprepared(mutation).await.unwrap();
        assert_eq!(f.emit(|| {}).await, 0, "{mutation}");
        assert!(rx.try_recv().is_err());
        f.database.execute_unprepared(restore).await.unwrap();
        assert_eq!(f.emit(|| {}).await, 1);
        delivered(&mut rx);
    }
    f.database
        .execute_unprepared("DROP TABLE thread_membership")
        .await
        .unwrap();
    assert_eq!(
        f.emit(|| {}).await,
        0,
        "membership SQL failure must fail closed"
    );
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn authorization_fact_queries_enforce_exact_scopes_rows_and_input_limits() {
    use pioneer_crud::{
        load_session_leases, load_thread_membership_principal_ids,
        load_workspace_membership_principal_ids,
    };
    let f = Fixture::new().await;
    let participant = f.member(10, true).await;
    let nonparticipant = f.member(12, false).await;
    // An existing participant outside the requested IDs must never be loaded.
    let one = [nonparticipant.principal_id.clone()];
    assert!(
        load_thread_membership_principal_ids(&f.database, THREAD, &one)
            .await
            .unwrap()
            .is_empty()
    );
    let ids = [
        participant.principal_id.clone(),
        nonparticipant.principal_id.clone(),
    ];
    assert_eq!(
        load_thread_membership_principal_ids(&f.database, THREAD, &ids)
            .await
            .unwrap(),
        vec![participant.principal_id.to_string()]
    );
    assert!(
        load_thread_membership_principal_ids(&f.database, "other-thread", &ids)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        load_workspace_membership_principal_ids(&f.database, "other-workspace", &ids)
            .await
            .unwrap()
            .is_empty()
    );
    let repeated = vec![participant.principal_id.clone(); AUTHORIZATION_FACTS_BATCH_LIMIT];
    assert_eq!(
        load_thread_membership_principal_ids(&f.database, THREAD, &repeated)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        load_workspace_membership_principal_ids(&f.database, &f.workspace, &repeated)
            .await
            .unwrap()
            .len(),
        1
    );
    let sessions = vec![participant.session_id.clone(); AUTHORIZATION_FACTS_BATCH_LIMIT];
    let rows = load_session_leases(&f.database, &sessions).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].session_id, participant.session_id.as_str());
    let over = vec![participant.principal_id.clone(); AUTHORIZATION_FACTS_BATCH_LIMIT + 1];
    let sessions_over = vec![participant.session_id.clone(); AUTHORIZATION_FACTS_BATCH_LIMIT + 1];
    f.clear_observation();
    assert!(
        load_thread_membership_principal_ids(&f.database, THREAD, &over)
            .await
            .is_err()
    );
    assert!(
        load_workspace_membership_principal_ids(&f.database, &f.workspace, &over)
            .await
            .is_err()
    );
    assert!(
        load_session_leases(&f.database, &sessions_over)
            .await
            .is_err()
    );
    assert!(
        f.statements.lock().unwrap().is_empty(),
        "oversized input must not reach SQLite"
    );
    f.database
        .execute_unprepared("DROP TABLE auth_session")
        .await
        .unwrap();
    let (_, mut rx) = f.connect(participant).await;
    assert_eq!(f.emit(|| {}).await, 0, "lease SQL failure must fail closed");
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn fanout_keeps_exact_runtime_drafts_and_internal_lineage_as_special_domains() {
    let f = Fixture::new().await;
    let principal = f.member(10, true).await;
    let (connection, mut rx) = f.connect(principal.clone()).await;
    let draft_id = "T0000000000000000009B";
    f.processor
        .thread_manager
        .thread_start_draft_authenticated(
            connection,
            crate::thread::ThreadSubscriptionIdentity::new(
                principal.principal_id.clone(),
                principal.session_id.clone(),
            ),
            f.workspace.clone(),
            serde_json::from_value(json!({"thread_id": draft_id, "workspace_id": f.workspace}))
                .unwrap(),
            None,
            None,
        )
        .await
        .unwrap();
    let serializations = Arc::new(AtomicUsize::new(0));
    f.processor
        .send_notification_to_authorized_thread_subscribers(
            draft_id,
            METHOD,
            &NotificationSerializationProbe {
                serializations: serializations.clone(),
                on_serialize: || {},
            },
            f.processor
                .thread_manager
                .subscribed_connections(draft_id)
                .await,
        )
        .await;
    assert_eq!(serializations.load(Ordering::SeqCst), 1);
    delivered(&mut rx);

    let absolute = f.device(authenticated_test_superuser(), 20).await;
    let (_, mut absolute_rx) = f.connect(absolute).await;
    pioneer_entity::thread::Entity::update_many()
        .col_expr(
            pioneer_entity::thread::Column::AccessClass,
            Expr::value("internal"),
        )
        .filter(pioneer_entity::thread::Column::Id.eq(THREAD))
        .exec(&f.database)
        .await
        .unwrap();
    assert_eq!(
        f.emit(|| {}).await,
        0,
        "an internal child without durable lineage cannot be treated as an ordinary root, even for Superuser"
    );
    assert!(rx.try_recv().is_err());
    assert!(absolute_rx.try_recv().is_err());
}
