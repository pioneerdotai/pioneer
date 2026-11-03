use super::*;
use pioneer_entity::{agent_action, agent_action_outbox, agent_execution, agent_identity};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, Set, TransactionTrait};

async fn outbox_fixture(
    rows: &[(&str, &str, i64)],
) -> (
    tempfile::TempDir,
    Arc<MessageProcessor>,
    Arc<NativeSchedulingObserver>,
    chrono::DateTime<chrono::FixedOffset>,
) {
    let observer = Arc::new(NativeSchedulingObserver::default());
    let (directory, workspace_manager, store, workspace) =
        setup_pooled_file_workspace_manager_with_observers(
            Some(observer.clone()),
            Some(observer.clone()),
        )
        .await;
    let database = store.database_connection();
    let identity = agent_identity::Entity::find()
        .filter(agent_identity::Column::WorkspaceId.eq(&workspace))
        .one(&database)
        .await
        .unwrap()
        .unwrap();
    let now = pioneer_crud::utc_now();
    let execution = "E00000000000000000007";
    // Persist a real owner tuple under the same FK-OFF runtime contract as Gateway.
    agent_execution::Entity::insert(agent_execution::ActiveModel {
        id: Set(execution.to_owned()),
        workspace_id: Set(workspace),
        agent_identity_id: Set(identity.id),
        identity_source_revision: Set(identity.source_revision),
        identity_source_fingerprint: Set(identity.source_fingerprint),
        parent_execution_id: Set(None),
        parent_task_id: Set(None),
        parent_thread_id: Set(None),
        home_root_thread_id: Set("outbox-root".to_owned()),
        work_graph_root_execution_id: Set(execution.to_owned()),
        requested_identity_selection_json: Set("{}".to_owned()),
        requested_profile_selection_json: Set("{}".to_owned()),
        resolved_profile_id: Set(None),
        resolved_profile_fingerprint: Set(None),
        presentation_snapshot_id: Set(None),
        authorization_context_fingerprint: Set("fixture".to_owned()),
        execution_generation: Set(1),
        status: Set("running".to_owned()),
        created_at: Set(now),
        updated_at: Set(now),
        finished_at: Set(None),
    })
    .exec(&database)
    .await
    .unwrap();
    for &(id, payload, attempts) in rows {
        agent_action::Entity::insert(agent_action::ActiveModel {
            id: Set(id.to_owned()),
            execution_id: Set(execution.to_owned()),
            action_kind: Set("send_message".to_owned()),
            idempotency_key: Set(id.to_owned()),
            request_fingerprint: Set("fixture".to_owned()),
            status: Set("committed".to_owned()),
            created_at: Set(now),
            committed_at: Set(Some(now)),
            response_json: Set(Some("{}".to_owned())),
        })
        .exec(&database)
        .await
        .unwrap();
        agent_action_outbox::Entity::insert(agent_action_outbox::ActiveModel {
            id: Set(id.to_owned()),
            action_id: Set(id.to_owned()),
            owner_execution_id: Set(execution.to_owned()),
            payload_json: Set(payload.to_owned()),
            status: Set("pending".to_owned()),
            attempts: Set(attempts),
            next_attempt_at: Set(None),
            delivered_at: Set(None),
            last_error: Set(None),
            created_at: Set(now),
        })
        .exec(&database)
        .await
        .unwrap();
    }
    let processor = Arc::new(MessageProcessor::new(
        Arc::new(ThreadManager::new("test-model", "openai")),
        test_provider(),
        Arc::new(SessionManager::new()),
        workspace_manager,
        store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_tool_loop_config(),
    ));
    (directory, processor, observer, now)
}

#[tokio::test]
async fn agent_action_outbox_poison_payloads_keep_retry_contract_and_process_remainder() {
    let (_directory, processor, observer, now) = outbox_fixture(&[
        ("a-malformed", "{", 0),
        ("b-final-invalid-start", r#"{"kind":"start_agent"}"#, 7),
        ("c-healthy", r#"{"kind":"send_message","original":true}"#, 0),
    ])
    .await;
    let database = processor.crud_store.database_connection();
    // Even a foreground caller must use Maintenance for outbox bookkeeping.
    observer.writes.lock().unwrap().clear();
    assert_eq!(
        crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
            .await
            .unwrap()
            .delivered,
        1
    );
    let writes = observer.writes.lock().unwrap().clone();
    assert!(
        writes
            .iter()
            .any(|event| matches!(event, pioneer_sqlite::SqliteWriteEvent::Acquired { .. }))
    );
    assert!(
        writes
            .iter()
            .filter_map(|event| match event {
                pioneer_sqlite::SqliteWriteEvent::Acquired { class, .. } => Some(*class),
                _ => None,
            })
            .all(|class| class == pioneer_sqlite::SqliteWriteClass::Maintenance)
    );
    let bad = agent_action_outbox::Entity::find_by_id("a-malformed")
        .one(&database)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        bad.payload_json, "{",
        "the original payload remains authoritative"
    );
    assert_eq!((bad.status.as_str(), bad.attempts), ("failed", 1));
    assert!(bad.next_attempt_at.unwrap() >= now + chrono::Duration::seconds(30));
    assert_eq!(
        bad.last_error.as_deref(),
        // Undecodable JSON cannot prove a rejected StartAgent binding.
        Some("outbox_start_outcome_unknown")
    );
    let final_row = agent_action_outbox::Entity::find_by_id("b-final-invalid-start")
        .one(&database)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            final_row.status.as_str(),
            final_row.attempts,
            final_row.next_attempt_at
        ),
        ("failed", 8, None)
    );
    let healthy = agent_action_outbox::Entity::find_by_id("c-healthy")
        .one(&database)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (healthy.status.as_str(), healthy.attempts),
        ("delivered", 1)
    );
    assert!(healthy.delivered_at.is_some());
    assert_eq!(
        crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
            .await
            .unwrap()
            .delivered,
        0
    );
    // No DB capacity survives the handler/ack boundary.
    let foreground = tokio::time::timeout(Duration::from_secs(1), database.begin())
        .await
        .unwrap()
        .unwrap();
    foreground.rollback().await.unwrap();
}

#[tokio::test]
async fn agent_action_outbox_local_claim_failure_between_healthy_rows_dispatches_final_attempt() {
    let (_directory, processor, observer, _) = outbox_fixture(&[
        ("a", r#"{"kind":"send_message"}"#, 7),
        ("b", r#"{"kind":"send_message"}"#, 0),
        ("c", r#"{"kind":"send_message"}"#, 0),
    ])
    .await;
    let database = processor
        .crud_store
        .with_maintenance_access()
        .database_connection();
    // Only the fixture adds this fault trigger; no production schema mechanism.
    database.execute_unprepared("CREATE TRIGGER reject_b_claim BEFORE UPDATE OF attempts ON agent_action_outbox WHEN OLD.id='b' BEGIN SELECT RAISE(ABORT,'local storage failure'); END").await.unwrap();
    observer.writes.lock().unwrap().clear();
    let batch = crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
        .await
        .unwrap();
    assert_eq!(batch.delivered, 2);
    assert_eq!(batch.errors.len(), 1);
    for (id, attempts) in [("a", 8), ("c", 1)] {
        let row = agent_action_outbox::Entity::find_by_id(id)
            .one(&database)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((row.status.as_str(), row.attempts), ("delivered", attempts));
    }
    let failed_claim = agent_action_outbox::Entity::find_by_id("b")
        .one(&database)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed_claim.attempts, 0);
    assert!(failed_claim.next_attempt_at.is_some());
    assert!(
        observer
            .writes
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                pioneer_sqlite::SqliteWriteEvent::Acquired { class, .. } => Some(*class),
                _ => None,
            })
            .all(|class| class == pioneer_sqlite::SqliteWriteClass::Maintenance)
    );
}

#[tokio::test]
async fn agent_action_outbox_ack_and_both_deferral_errors_preserve_remainder_and_diagnostics() {
    use crate::message::agent_action_tools::{
        AgentActionOutboxDispatch, process_claimed_agent_action_outbox,
    };
    for outcome in 0..4 {
        let (_directory, processor, _, _) = outbox_fixture(&[
            ("a", r#"{"kind":"send_message"}"#, 7),
            ("b", r#"{"kind":"send_message"}"#, 0),
            ("c", r#"{"kind":"send_message"}"#, 0),
        ])
        .await;
        let database = processor
            .crud_store
            .with_maintenance_access()
            .database_connection();
        let batch = pioneer_crud::claim_agent_action_outbox(&database, 64)
            .await
            .unwrap();
        assert!(batch.errors.is_empty());
        database.execute_unprepared("CREATE TRIGGER reject_b_ack BEFORE UPDATE ON agent_action_outbox WHEN OLD.id='b' BEGIN SELECT RAISE(ABORT,'local callback failure'); END").await.unwrap();
        let dispatched = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = dispatched.clone();
        let result = process_claimed_agent_action_outbox(&database, batch, move |row| {
            let seen = seen.clone();
            async move {
                seen.lock().unwrap().push(row.id.clone());
                if row.id != "b" {
                    return Ok(AgentActionOutboxDispatch::Delivered);
                }
                match outcome {
                    0 => Ok(AgentActionOutboxDispatch::Delivered),
                    1 => Err(anyhow::anyhow!("malformed dispatch")),
                    2 => Ok(AgentActionOutboxDispatch::AwaitingPermit),
                    _ => Ok(AgentActionOutboxDispatch::AwaitingRuntime),
                }
            }
        })
        .await;
        assert_eq!(*dispatched.lock().unwrap(), vec!["a", "b", "c"]);
        assert_eq!(result.delivered, 2);
        assert_eq!(result.errors.len(), 1);
        for (id, status, attempts) in [
            ("a", "delivered", 8),
            ("b", "pending", 1),
            ("c", "delivered", 1),
        ] {
            let row = agent_action_outbox::Entity::find_by_id(id)
                .one(&database)
                .await
                .unwrap()
                .unwrap();
            assert_eq!((row.status.as_str(), row.attempts), (status, attempts));
            if id == "b" {
                assert!(
                    row.next_attempt_at.is_some(),
                    "failed ACK retains durable claim lease"
                );
            }
        }
        let foreground = tokio::time::timeout(Duration::from_secs(1), database.begin())
            .await
            .unwrap()
            .unwrap();
        foreground.rollback().await.unwrap();
    }
}

#[tokio::test]
async fn agent_action_outbox_creation_and_polling_panics_preserve_remainder_and_final_dispatch() {
    use crate::message::agent_action_tools::{
        AgentActionOutboxDispatch, process_claimed_agent_action_outbox,
    };
    for panic_on_creation in [false, true] {
        let (_directory, processor, observer, _) = outbox_fixture(&[
            ("a", r#"{"kind":"send_message"}"#, 0),
            ("b", r#"{"kind":"send_message"}"#, 0),
            ("c", r#"{"kind":"send_message"}"#, 7),
        ])
        .await;
        let database = processor
            .crud_store
            .with_maintenance_access()
            .database_connection();
        observer.writes.lock().unwrap().clear();
        let batch = pioneer_crud::claim_agent_action_outbox(&database, 64)
            .await
            .unwrap();
        assert_eq!(batch.rows.len(), 3);
        let polled = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = polled.clone();
        let result = process_claimed_agent_action_outbox(&database, batch, move |row| {
            if panic_on_creation && row.id == "b" {
                panic!("injected synchronous dispatch creation unwind");
            }
            let seen = seen.clone();
            async move {
                seen.lock().unwrap().push(row.id.clone());
                if row.id == "b" {
                    panic!("injected dispatch polling unwind");
                }
                Ok(AgentActionOutboxDispatch::Delivered)
            }
        })
        .await;
        assert_eq!(
            *polled.lock().unwrap(),
            if panic_on_creation {
                vec!["a", "c"]
            } else {
                vec!["a", "b", "c"]
            }
        );
        assert_eq!(result.delivered, 2);
        assert_eq!(result.errors.len(), 1);
        assert_eq!(
            result.errors[0].to_string(),
            "outbox_dispatch_callback_panic_outcome_unknown"
        );
        for (id, status, attempts) in [
            ("a", "delivered", 1),
            ("b", "pending", 1),
            ("c", "delivered", 8),
        ] {
            let row = agent_action_outbox::Entity::find_by_id(id)
                .one(&database)
                .await
                .unwrap()
                .unwrap();
            assert_eq!((row.status.as_str(), row.attempts), (status, attempts));
            assert_eq!(row.last_error, None);
            if id == "b" {
                assert!(
                    row.next_attempt_at.is_some(),
                    "only original durable lease survives"
                );
            }
        }
        assert_eq!(
            observer
                .writes
                .lock()
                .unwrap()
                .iter()
                .filter_map(|event| match event {
                    pioneer_sqlite::SqliteWriteEvent::Acquired { class, .. } => Some(*class),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![pioneer_sqlite::SqliteWriteClass::Maintenance; 5]
        );
        assert!(database.reader_query_only_enabled().await.unwrap());
        let writer = tokio::time::timeout(
            Duration::from_secs(1),
            processor.crud_store.database_connection().begin(),
        )
        .await
        .unwrap()
        .unwrap();
        writer.rollback().await.unwrap();
    }
}

// The existing claimed-row seam accepts a scoped connection. Inject an unwind
// after the actual ACK commits, without changing the real writer or its routes.
struct PanicAckConnection<C>(C);
#[sea_orm::prelude::async_trait::async_trait]
impl<C: ConnectionTrait> ConnectionTrait for PanicAckConnection<C> {
    fn get_database_backend(&self) -> sea_orm::DatabaseBackend {
        self.0.get_database_backend()
    }
    async fn execute_raw(
        &self,
        statement: sea_orm::Statement,
    ) -> std::result::Result<sea_orm::ExecResult, sea_orm::DbErr> {
        let panic_after_commit = statement.values.as_ref().is_some_and(|values| {
            values.0.iter().any(
                |value| matches!(value, sea_orm::Value::String(Some(id)) if id.as_str() == "b"),
            )
        });
        let result = self.0.execute_raw(statement).await?;
        if panic_after_commit {
            panic!("injected ACK unwind after commit");
        }
        Ok(result)
    }
    async fn execute_unprepared(
        &self,
        sql: &str,
    ) -> std::result::Result<sea_orm::ExecResult, sea_orm::DbErr> {
        self.0.execute_unprepared(sql).await
    }
    async fn query_one_raw(
        &self,
        statement: sea_orm::Statement,
    ) -> std::result::Result<Option<sea_orm::QueryResult>, sea_orm::DbErr> {
        self.0.query_one_raw(statement).await
    }
    async fn query_all_raw(
        &self,
        statement: sea_orm::Statement,
    ) -> std::result::Result<Vec<sea_orm::QueryResult>, sea_orm::DbErr> {
        self.0.query_all_raw(statement).await
    }
    fn support_returning(&self) -> bool {
        self.0.support_returning()
    }
    fn is_mock_connection(&self) -> bool {
        self.0.is_mock_connection()
    }
}

#[tokio::test]
async fn agent_action_outbox_ack_panic_after_commit_does_not_compensate_or_stop_remainder() {
    use crate::message::agent_action_tools::{
        AgentActionOutboxDispatch, process_claimed_agent_action_outbox,
    };
    let (_directory, processor, observer, _) = outbox_fixture(&[
        ("a", r#"{"kind":"send_message"}"#, 0),
        ("b", r#"{"kind":"send_message"}"#, 0),
        ("c", r#"{"kind":"send_message"}"#, 7),
    ])
    .await;
    let database = processor
        .crud_store
        .with_maintenance_access()
        .database_connection();
    observer.writes.lock().unwrap().clear();
    let batch = pioneer_crud::claim_agent_action_outbox(&database, 64)
        .await
        .unwrap();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let dispatched = seen.clone();
    let result = process_claimed_agent_action_outbox(
        &PanicAckConnection(database.clone()),
        batch,
        move |row| {
            let dispatched = dispatched.clone();
            async move {
                dispatched.lock().unwrap().push(row.id);
                Ok(AgentActionOutboxDispatch::Delivered)
            }
        },
    )
    .await;
    assert_eq!(*seen.lock().unwrap(), vec!["a", "b", "c"]);
    assert_eq!(
        result.delivered, 2,
        "B's acknowledgement did not return a known outcome"
    );
    assert_eq!(result.errors.len(), 1);
    assert_eq!(
        result.errors[0].to_string(),
        "outbox_dispatch_callback_panic_outcome_unknown"
    );
    for (id, attempts) in [("a", 1), ("b", 1), ("c", 8)] {
        let row = agent_action_outbox::Entity::find_by_id(id)
            .one(&database)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((row.status.as_str(), row.attempts), ("delivered", attempts));
        assert!(row.delivered_at.is_some());
        assert_eq!(row.next_attempt_at, None);
        assert_eq!(
            row.last_error, None,
            "no blind mark_failed after the unknown ACK"
        );
    }
    assert_eq!(
        observer
            .writes
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                pioneer_sqlite::SqliteWriteEvent::Acquired { class, .. } => Some(*class),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![pioneer_sqlite::SqliteWriteClass::Maintenance; 6],
        "three claims + three ACKs, no compensation/retry"
    );
    assert!(database.reader_query_only_enabled().await.unwrap());
    let writer = tokio::time::timeout(
        Duration::from_secs(1),
        processor.crud_store.database_connection().begin(),
    )
    .await
    .unwrap()
    .unwrap();
    writer.rollback().await.unwrap();
}

#[tokio::test]
async fn start_outbox_refusal_stops_delivery_without_inflating_attempts_and_unknown_error_is_safe()
{
    use crate::message::agent_action_tools::{
        AgentActionOutboxDispatch, process_claimed_agent_action_outbox,
    };
    use pioneer_crud::AgentActionOutboxFailure as Failure;
    let (_directory, processor, _observer, now) = outbox_fixture(&[
        ("refused", r#"{"kind":"start_agent"}"#, 0),
        ("temporary", r#"{"kind":"start_agent"}"#, 0),
        ("unknown", r#"{"kind":"start_agent"}"#, 0),
        ("stale-refusal", r#"{"kind":"start_agent"}"#, 7),
    ])
    .await;
    let db = processor
        .crud_store
        .with_maintenance_access()
        .database_connection();
    let batch = pioneer_crud::claim_agent_action_outbox(&db, 64)
        .await
        .unwrap();
    let result = process_claimed_agent_action_outbox(&db, batch, |row| {
        let db = db.clone();
        async move {
            match row.id.as_str() {
                "stale-refusal" => {
                    // Another delivery already acknowledged this exact claim. The
                    // losing CAS must not count a second successful acknowledgement.
                    assert!(
                        pioneer_crud::mark_agent_action_outbox_delivered(
                            &db,
                            &row.id,
                            row.attempts,
                            pioneer_crud::utc_now(),
                        )
                        .await
                        .unwrap()
                    );
                    Ok(AgentActionOutboxDispatch::Rejected(Failure::OutcomeUnknown))
                }
                "refused" => Ok(AgentActionOutboxDispatch::Rejected(
                    Failure::ForegroundConflict,
                )),
                "temporary" => Err(Failure::ControlUnavailable.into()),
                _ => Err(anyhow::anyhow!(
                    "provider secret and private input must not be persisted"
                )),
            }
        }
    })
    .await;
    assert!(result.errors.is_empty());
    assert_eq!(result.delivered, 0);
    let refused = agent_action_outbox::Entity::find_by_id("refused")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            refused.status.as_str(),
            refused.attempts,
            refused.next_attempt_at
        ),
        ("failed", 1, None)
    );
    assert_eq!(
        refused.last_error.as_deref(),
        Some("outbox_foreground_conflict")
    );
    assert!(refused.delivered_at.is_none());
    for (id, class) in [
        ("temporary", "outbox_runtime_control_unavailable"),
        ("unknown", "outbox_start_outcome_unknown"),
    ] {
        let row = agent_action_outbox::Entity::find_by_id(id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.attempts, 1);
        assert!(row.next_attempt_at.unwrap() >= now + chrono::Duration::seconds(30));
        assert_eq!(row.last_error.as_deref(), Some(class));
    }
    // Make the retry deadline due. A new claim must contain only retryable rows.
    let due = now - chrono::Duration::seconds(1);
    use sea_orm::{ActiveModelTrait, Set};
    for id in ["temporary", "unknown"] {
        let mut row: agent_action_outbox::ActiveModel = agent_action_outbox::Entity::find_by_id(id)
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .into();
        row.next_attempt_at = Set(Some(due));
        row.update(&db).await.unwrap();
    }
    let batch = pioneer_crud::claim_agent_action_outbox(&db, 64)
        .await
        .unwrap();
    let ids = batch
        .rows
        .into_iter()
        .map(|row| row.id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        ids,
        ["temporary".to_owned(), "unknown".to_owned()]
            .into_iter()
            .collect()
    );
}

#[test]
fn agent_start_proof_preserves_typed_storage_faults_without_retrying_semantic_denials() {
    use crate::message::agent_action_tools::agent_start_proof_error;
    use pioneer_crud::AgentActionOutboxFailure as Failure;
    use sea_orm::{DbErr, RuntimeErr, SqlxError};
    use std::borrow::Cow;

    // SQLx's typed DatabaseError boundary, as used by the SQLite driver. The
    // text is deliberately unrelated to the numeric IOERR code: classification
    // must preserve the driver failure without inspecting its Display payload.
    #[derive(Debug)]
    struct DriverIo;
    impl std::fmt::Display for DriverIo {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("private provider payload must not become diagnostics")
        }
    }
    impl std::error::Error for DriverIo {}
    impl sea_orm::sqlx::error::DatabaseError for DriverIo {
        fn message(&self) -> &str {
            "private provider payload must not become diagnostics"
        }
        fn code(&self) -> Option<Cow<'_, str>> {
            Some("10".into()) // SQLITE_IOERR
        }
        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }
        fn kind(&self) -> sea_orm::sqlx::error::ErrorKind {
            sea_orm::sqlx::error::ErrorKind::Other
        }
    }
    let driver = RuntimeErr::SqlxError(Arc::new(SqlxError::Database(Box::new(DriverIo))));
    for storage in [
        DbErr::Conn(driver.clone()),
        DbErr::Query(driver.clone()),
        DbErr::Exec(driver),
        DbErr::ConnectionAcquire(sea_orm::ConnAcquireErr::Timeout),
    ] {
        for semantic in [Failure::AuthorityDenied, Failure::InvalidStart] {
            let source =
                || anyhow::Error::new(storage.clone()).context("context proof unavailable");
            // CLI preparation completes through TaskStartFailure before the
            // outbox adds its own semantic context. Its source chain must be
            // classified just like the direct API proof failure.
            for error in [
                source(),
                anyhow::Error::new(pioneer_tasks::TaskStartFailure::from_error(
                    pioneer_tasks::TaskStartStage::CliPreparation,
                    source(),
                ))
                .context(Failure::PreparationUnavailable),
            ] {
                let proof_error = agent_start_proof_error(error, semantic);
                assert_eq!(
                    proof_error.downcast_ref::<Failure>(),
                    Some(&Failure::StorageUnavailable)
                );
                assert!(proof_error.chain().any(|cause| cause.is::<DbErr>()));
                assert_eq!(
                    proof_error.downcast_ref::<Failure>().unwrap().safe_name(),
                    "outbox_storage_unavailable"
                );
            }
        }
    }
    for semantic in [Failure::AuthorityDenied, Failure::InvalidStart] {
        let malformed = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        for error in [
            anyhow::anyhow!("confirmed permission denial"),
            anyhow::Error::new(DbErr::RecordNotFound("missing admission".into())),
            anyhow::Error::new(DbErr::Type("invalid persisted field".into())),
            anyhow::Error::new(malformed),
            anyhow::anyhow!("SQLITE_IOERR is merely untrusted text here"),
        ] {
            let proof_error = agent_start_proof_error(error, semantic);
            assert_eq!(proof_error.downcast_ref::<Failure>(), Some(&semantic));
        }
    }
}

#[tokio::test]
async fn exhausted_start_reconciliation_is_bounded_and_yields_to_an_ordinary_due_row() {
    use sea_orm::ActiveModelTrait;
    let (_directory, processor, _observer, now) = outbox_fixture(&[
        (
            "a-exhausted-start",
            r#"{"kind":"start_agent","spawned_execution_id":"E00000000000000000007","dispatch":{"thread_id":"poison-destination","turn_id":"poison-turn"}}"#,
            pioneer_crud::AGENT_ACTION_OUTBOX_MAX_ATTEMPTS,
        ),
        ("b-ordinary-due", r#"{"kind":"send_message"}"#, 0),
    ])
    .await;
    let database = processor.crud_store.database_connection();
    // This intentionally malformed StartAgent has no response binding. Its
    // durable obligation must stay unresolved rather than masquerade as cleanup.
    agent_action::Entity::update_many()
        .col_expr(
            agent_action::Column::ActionKind,
            sea_orm::sea_query::Expr::value("start_agent"),
        )
        .filter(agent_action::Column::Id.eq("a-exhausted-start"))
        .exec(&database)
        .await
        .unwrap();
    for id in ["a-exhausted-start", "b-ordinary-due"] {
        let mut row: agent_action_outbox::ActiveModel = agent_action_outbox::Entity::find_by_id(id)
            .one(&database)
            .await
            .unwrap()
            .unwrap()
            .into();
        row.status = Set("failed".to_owned());
        row.next_attempt_at = Set(if id == "a-exhausted-start" {
            None // legacy exhausted delivery without a scheduled reconciliation
        } else {
            Some(now - chrono::Duration::seconds(1))
        });
        row.update(&database).await.unwrap();
    }
    // A limit of two supplies one timed quantum. The exhausted row is first;
    // rescheduling its reconciliation must give the older ordinary row progress
    // on the very next quantum, without spending a ninth delivery attempt.
    let first = crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 2)
        .await
        .unwrap();
    assert_eq!(first.delivered, 0);
    assert!(first.errors.len() <= 2);
    let exhausted = agent_action_outbox::Entity::find_by_id("a-exhausted-start")
        .one(&database)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exhausted.status, "failed");
    assert_eq!(
        exhausted.attempts,
        pioneer_crud::AGENT_ACTION_OUTBOX_MAX_ATTEMPTS
    );
    assert!(exhausted.next_attempt_at.is_some_and(|next| next > now));
    assert!(exhausted.delivered_at.is_none());
    let second = crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 2)
        .await
        .unwrap();
    assert_eq!(second.delivered, 1);
    assert!(second.errors.len() <= 2);
    let ordinary = agent_action_outbox::Entity::find_by_id("b-ordinary-due")
        .one(&database)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (ordinary.status.as_str(), ordinary.attempts),
        ("delivered", 1)
    );
    let unchanged = agent_action_outbox::Entity::find_by_id("a-exhausted-start")
        .one(&database)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        unchanged.attempts,
        pioneer_crud::AGENT_ACTION_OUTBOX_MAX_ATTEMPTS
    );
    assert_eq!(unchanged.next_attempt_at, exhausted.next_attempt_at);
    assert!(
        !processor
            .agent_manager
            .has_thread("poison-destination")
            .await
    );
    assert!(
        processor
            .crud_store
            .get_turn_runtime_snapshot("poison-turn")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn ledger_owner_parks_terminal_prefix_and_compacts_after_real_ack_and_retention_deadline() {
    use pioneer_entity::agent_action_receipt as receipt;
    use sea_orm::sea_query::Expr;
    let execution = "E00000000000000000007";
    let mut rows = Vec::new();
    for index in 0..65 {
        let id = format!("parked-{index:03}");
        rows.push((id.clone(), json!({"action_id":id,"execution_id":execution,"kind":"start_agent",
            "spawned_execution_id":"absent-execution","dispatch":{"turn_id":"absent-turn","execution_generation":1}}).to_string(), 8));
    }
    for index in 0..65 {
        let id = format!("aaa-compacted-{index:03}");
        rows.push((id.clone(), json!({"action_id":id,"execution_id":execution,"kind":"send_message","bulky":"x".repeat(4096)}).to_string(),0));
    }
    for id in ["closed-old", "closed-future"] {
        rows.push((id.into(), json!({"action_id":id,"execution_id":execution,"kind":"send_message","bulky":"x".repeat(4096)}).to_string(),0));
    }
    let refs = rows
        .iter()
        .map(|(id, payload, attempts)| (id.as_str(), payload.as_str(), *attempts))
        .collect::<Vec<_>>();
    let (_directory, processor, observer, _) = outbox_fixture(&refs).await;
    let database = processor.crud_store.database_connection();
    let old = pioneer_crud::utc_now() - chrono::Duration::days(31);
    let parked = rows
        .iter()
        .filter(|(id, _, _)| id.starts_with("parked-"))
        .map(|(id, _, _)| id.clone())
        .collect::<Vec<_>>();
    agent_action::Entity::update_many()
        .col_expr(agent_action::Column::ActionKind, Expr::value("start_agent"))
        .filter(agent_action::Column::Id.is_in(parked.clone()))
        .exec(&database)
        .await
        .unwrap();
    agent_action_outbox::Entity::update_many()
        .col_expr(agent_action_outbox::Column::CreatedAt, Expr::value(old))
        .col_expr(agent_action_outbox::Column::Status, Expr::value("failed"))
        .col_expr(
            agent_action_outbox::Column::LastError,
            Expr::value("outbox_detached_binding_rejected"),
        )
        .filter(agent_action_outbox::Column::Id.is_in(parked.clone()))
        .exec(&database)
        .await
        .unwrap();
    let compacted_ids = rows
        .iter()
        .filter(|(id, _, _)| id.starts_with("aaa-compacted-"))
        .map(|(id, _, _)| id.clone())
        .collect::<Vec<_>>();
    for id in &compacted_ids {
        agent_action::Entity::update_many()
            .col_expr(agent_action::Column::CreatedAt, Expr::value(old))
            .col_expr(agent_action::Column::CommittedAt, Expr::value(Some(old)))
            .filter(agent_action::Column::Id.eq(id.clone()))
            .exec(&database)
            .await
            .unwrap();
        agent_action_outbox::Entity::update_many()
            .col_expr(agent_action_outbox::Column::CreatedAt, Expr::value(old))
            .col_expr(
                agent_action_outbox::Column::Status,
                Expr::value("delivered"),
            )
            .col_expr(
                agent_action_outbox::Column::DeliveredAt,
                Expr::value(Some(old)),
            )
            .filter(agent_action_outbox::Column::Id.eq(id.clone()))
            .exec(&database)
            .await
            .unwrap();
        receipt::Entity::insert(receipt::ActiveModel {
            id: Set(format!("receipt-{id}")),
            action_id: Set(id.clone()),
            actor_kind: Set("agent_execution".into()),
            actor_id: Set(Some(execution.into())),
            decision: Set("allowed".into()),
            policy_fingerprint: Set("a".repeat(64)),
            committed_at: Set(old),
            response_json: Set(Some("large response".repeat(100))),
            ..Default::default()
        })
        .exec(&database)
        .await
        .unwrap();
    }
    // Use the production compactor to obtain genuine markers, then restart
    // reconstruction over more than one page of already compacted history.
    let upper = processor
        .crud_store
        .terminal_agent_action_ledger_upper()
        .await
        .unwrap()
        .unwrap();
    let mut cursor = None;
    loop {
        let page = processor
            .crud_store
            .compact_terminal_agent_action_ledger_page(
                pioneer_crud::utc_now(),
                cursor.as_ref(),
                &upper,
            )
            .await
            .unwrap();
        assert!(page.errors.is_empty());
        cursor = page.after;
        if !page.more {
            break;
        }
    }
    let prefix = processor
        .crud_store
        .compact_terminal_agent_action_ledger_page(pioneer_crud::utc_now(), None, &upper)
        .await
        .unwrap();
    assert!(
        prefix.more,
        "excluded history consumes a raw page before the useful tail"
    );
    assert_eq!(prefix.after.as_deref(), Some("aaa-compacted-063"));
    assert_eq!(prefix.summary.compacted_rows, 0);
    assert!(prefix.errors.is_empty());
    // The StartAgent tuple is genuinely parked on absent terminal provenance.
    // Its unmodified payloads must not repeatedly reopen compaction at EOF.
    let label = "__agent_ledger_settled__";
    processor.arm_completed_history_preparation_barrier(label);
    observer.writes.lock().unwrap().clear();
    let worker = tokio::spawn(MessageProcessor::run_agent_action_ledger_worker(
        Arc::downgrade(&processor),
    ));
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert!(
        observer.writes.lock().unwrap().is_empty(),
        "parked raw pages do not require a writer"
    );
    processor.release_completed_history_preparation_barrier();
    tokio::task::yield_now().await;
    observer.reads.lock().unwrap().clear();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    tokio::task::yield_now().await;
    assert!(observer.reads.lock().unwrap().is_empty());
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::resume();
    // Establish a real Empty handoff before the supported action dispatcher
    // publishes terminal ACKs. Test-only tuple dates retain exact retention.
    processor.arm_completed_history_preparation_barrier(label);
    processor
        .crud_store
        .background_work("recovery.ledger")
        .invalidate();
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    let now = pioneer_crud::utc_now();
    for (id, created) in [
        ("closed-old", old),
        (
            "closed-future",
            now - chrono::Duration::days(30) + chrono::Duration::seconds(3),
        ),
    ] {
        agent_action::Entity::update_many()
            .col_expr(agent_action::Column::CreatedAt, Expr::value(created))
            .col_expr(
                agent_action::Column::CommittedAt,
                Expr::value(Some(created)),
            )
            .filter(agent_action::Column::Id.eq(id))
            .exec(&database)
            .await
            .unwrap();
        agent_action_outbox::Entity::update_many()
            .col_expr(agent_action_outbox::Column::CreatedAt, Expr::value(created))
            .filter(agent_action_outbox::Column::Id.eq(id))
            .exec(&database)
            .await
            .unwrap();
        receipt::Entity::insert(receipt::ActiveModel {
            id: Set(format!("receipt-{id}")),
            action_id: Set(id.into()),
            actor_kind: Set("agent_execution".into()),
            actor_id: Set(Some(execution.into())),
            decision: Set("allowed".into()),
            policy_fingerprint: Set("a".repeat(64)),
            committed_at: Set(created),
            response_json: Set(Some("large response".repeat(100))),
            ..Default::default()
        })
        .exec(&database)
        .await
        .unwrap();
    }
    let batch = crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
        .await
        .unwrap();
    assert!(batch.errors.is_empty());
    assert_eq!(batch.delivered, 2);
    processor.arm_completed_history_preparation_barrier(label);
    processor.release_completed_history_preparation_barrier();
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    let old_row = agent_action_outbox::Entity::find_by_id("closed-old")
        .one(&database)
        .await
        .unwrap()
        .unwrap();
    let future_row = agent_action_outbox::Entity::find_by_id("closed-future")
        .one(&database)
        .await
        .unwrap()
        .unwrap();
    assert!(
        serde_json::from_str::<JsonValue>(&old_row.payload_json)
            .unwrap()
            .get("_pioneer_compacted")
            .is_some()
    );
    assert!(
        serde_json::from_str::<JsonValue>(&future_row.payload_json)
            .unwrap()
            .get("_pioneer_compacted")
            .is_none()
    );
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    processor.arm_completed_history_preparation_barrier(label);
    processor.release_completed_history_preparation_barrier();
    timeout(
        Duration::from_secs(7),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert!(observer.reads.lock().unwrap().iter().all(|event| !matches!(event,
        pioneer_sqlite::SqliteReadEvent::AdmissionEnqueued {class,..} if *class!=pioneer_sqlite::SqliteReadClass::Maintenance)));
    assert!(observer.writes.lock().unwrap().iter().any(|event| matches!(
        event,
        pioneer_sqlite::SqliteWriteEvent::Acquired {
            class: pioneer_sqlite::SqliteWriteClass::Maintenance,
            ..
        }
    )));
    let future_row = agent_action_outbox::Entity::find_by_id("closed-future")
        .one(&database)
        .await
        .unwrap()
        .unwrap();
    assert!(
        serde_json::from_str::<JsonValue>(&future_row.payload_json)
            .unwrap()
            .get("_pioneer_compacted")
            .is_some(),
        "actual retention deadline compacts without another commit/client"
    );
    let retained = agent_action_outbox::Entity::find()
        .filter(agent_action_outbox::Column::Id.is_in(parked))
        .all(&database)
        .await
        .unwrap();
    assert_eq!(retained.len(), 65);
    assert!(retained.iter().all(|row| {
        serde_json::from_str::<JsonValue>(&row.payload_json)
            .unwrap()
            .get("_pioneer_compacted")
            .is_none()
    }));
    // Remove fixture-only parked/tail tuples after their assertions, leaving
    // more than one raw page of exclusively production-compacted history.
    agent_action_outbox::Entity::delete_many()
        .filter(agent_action_outbox::Column::Id.is_not_in(compacted_ids.clone()))
        .exec(&database)
        .await
        .unwrap();
    processor
        .crud_store
        .background_work("recovery.ledger")
        .invalidate();
    processor.arm_completed_history_preparation_barrier(label);
    processor.release_completed_history_preparation_barrier();
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    let all_upper = processor
        .crud_store
        .terminal_agent_action_ledger_upper()
        .await
        .unwrap()
        .unwrap();
    let page = processor
        .crud_store
        .compact_terminal_agent_action_ledger_page(pioneer_crud::utc_now(), None, &all_upper)
        .await
        .unwrap();
    assert!(page.more);
    assert!(page.errors.is_empty());
    assert_eq!(page.summary.compacted_rows, 0);
    assert!(page.next_at.is_none());
    processor.release_completed_history_preparation_barrier();
    tokio::task::yield_now().await;
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    tokio::task::yield_now().await;
    assert!(observer.reads.lock().unwrap().is_empty());
    assert!(observer.writes.lock().unwrap().is_empty());
    processor.cli_history_shutdown.cancel();
    worker.await.unwrap();
}

#[tokio::test]
async fn ledger_raw_page_keeps_tuple_progress_through_datetime_decode_poison() {
    use pioneer_entity::agent_action_receipt as receipt;
    use sea_orm::sea_query::Expr;
    for poison_due in [false, true] {
        let execution = "E00000000000000000007";
        let rows = (0..66).map(|i| {
            let id = format!("ledger-{i:03}");
            (id.clone(), json!({"action_id":id,"execution_id":execution,"kind":"send_message","bulky":"x".repeat(4096)}).to_string(), 0)
        }).collect::<Vec<_>>();
        let refs = rows
            .iter()
            .map(|(id, p, n)| (id.as_str(), p.as_str(), *n))
            .collect::<Vec<_>>();
        let (_directory, processor, _observer, _) = outbox_fixture(&refs).await;
        let store = processor.crud_store.with_maintenance_access();
        let db = store.database_connection();
        let now = pioneer_crud::utc_now();
        let old = now - chrono::Duration::days(31);
        let later = if poison_due {
            old
        } else {
            now - chrono::Duration::days(30) + chrono::Duration::seconds(3)
        };
        for (index, (id, _, _)) in rows.iter().enumerate() {
            let created = if index < 63 { old } else { later };
            agent_action::Entity::update_many()
                .col_expr(agent_action::Column::CreatedAt, Expr::value(created))
                .col_expr(
                    agent_action::Column::CommittedAt,
                    Expr::value(Some(created)),
                )
                .filter(agent_action::Column::Id.eq(id))
                .exec(&db)
                .await
                .unwrap();
            agent_action_outbox::Entity::update_many()
                .col_expr(agent_action_outbox::Column::CreatedAt, Expr::value(created))
                .col_expr(
                    agent_action_outbox::Column::Status,
                    Expr::value("delivered"),
                )
                .col_expr(
                    agent_action_outbox::Column::DeliveredAt,
                    Expr::value(Some(created)),
                )
                .filter(agent_action_outbox::Column::Id.eq(id))
                .exec(&db)
                .await
                .unwrap();
            receipt::Entity::insert(receipt::ActiveModel {
                id: Set(format!("receipt-{id}")),
                action_id: Set(id.clone()),
                actor_kind: Set("agent_execution".into()),
                actor_id: Set(Some(execution.into())),
                decision: Set("allowed".into()),
                policy_fingerprint: Set("a".repeat(64)),
                committed_at: Set(created),
                response_json: Set(Some("large response".repeat(100))),
                ..Default::default()
            })
            .exec(&db)
            .await
            .unwrap();
        }
        // Valid tuple keys and payload JSON, malformed non-key timestamp. This
        // exercises ORM decoding, rather than the older JSON poison matrix.
        agent_action_outbox::Entity::update_many()
            .col_expr(
                agent_action_outbox::Column::DeliveredAt,
                Expr::value("not-a-date"),
            )
            .filter(agent_action_outbox::Column::Id.eq("ledger-063"))
            .exec(&db)
            .await
            .unwrap();
        let upper = store
            .terminal_agent_action_ledger_upper()
            .await
            .unwrap()
            .unwrap();
        let first = store
            .compact_terminal_agent_action_ledger_page(now, None, &upper)
            .await
            .unwrap();
        assert!(first.more);
        assert_eq!(first.after.as_ref().unwrap(), "ledger-063");
        assert_eq!(first.summary.compacted_rows, 63);
        assert_eq!(first.errors.len(), 1, "future poison is visible too");
        let tail = store
            .compact_terminal_agent_action_ledger_page(
                now + chrono::Duration::seconds(4),
                first.after.as_ref(),
                &upper,
            )
            .await
            .unwrap();
        assert!(!tail.more);
        assert_eq!(
            tail.summary.compacted_rows, 2,
            "healthy tail beyond raw64 is serviced"
        );
        assert!(tail.errors.is_empty());
        assert!(
            agent_action_outbox::Entity::find_by_id("ledger-063")
                .one(&db)
                .await
                .is_err(),
            "poison is preserved, never ACKed"
        );
        // A subsequent finite error-retry pass observes the same poison, with
        // no healthy-row replay or loss. The existing real-owner test covers
        // retained error/deadline waiting; this is its raw adapter boundary.
        let retry = store
            .compact_terminal_agent_action_ledger_page(
                now + chrono::Duration::hours(1),
                None,
                &upper,
            )
            .await
            .unwrap();
        assert_eq!(retry.summary.compacted_rows, 0);
        assert_eq!(retry.errors.len(), 1);
    }
}

#[tokio::test]
async fn control_action_raw_pages_keep_future_and_excluded_history_then_old_key_release() {
    let owned = (0..131)
        .map(|i| {
            (
                format!("f05-action-{i:03}"),
                r#"{"kind":"send_message","original":true}"#,
                0,
            )
        })
        .collect::<Vec<_>>();
    let inputs = owned
        .iter()
        .map(|(id, p, a)| (id.as_str(), *p, *a))
        .collect::<Vec<_>>();
    let (_directory, processor, observer, now) = outbox_fixture(&inputs).await;
    let db = processor.crud_store.database_connection();
    let future = now + chrono::Duration::hours(1) + chrono::Duration::nanoseconds(1);
    for i in 0..130 {
        let id = format!("f05-action-{i:03}");
        let mut row: agent_action_outbox::ActiveModel =
            agent_action_outbox::Entity::find_by_id(&id)
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .into();
        if i < 65 {
            row.next_attempt_at = Set(Some(future));
        } else {
            row.status = Set("failed".into());
            row.attempts = Set(8);
            row.last_error = Set(Some("outbox_detached_binding_rejected".into()));
        }
        sea_orm::ActiveModelTrait::update(row, &db).await.unwrap();
    }
    processor.arm_completed_history_preparation_barrier("__control_admission_raw_page__");
    let stop = tokio_util::sync::CancellationToken::new();
    let worker = tokio::spawn(MessageProcessor::run_recovery_control_worker(
        Arc::downgrade(&processor),
        stop.clone(),
    ));
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(
        agent_action_outbox::Entity::find_by_id("f05-action-130")
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .status,
        "pending",
        "raw frontier has not reached useful tail"
    );
    processor.arm_completed_history_preparation_barrier("__control_owner_settled__");
    processor.release_completed_history_preparation_barrier();
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(
        agent_action_outbox::Entity::find_by_id("f05-action-130")
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .status,
        "delivered"
    );
    let mut pass = pioneer_crud::ControlAdmissionPass::default();
    timeout(Duration::from_secs(10), async {
        while !processor
            .crud_store
            .control_admission_quantum(&mut pass)
            .await
            .unwrap()
        {}
    })
    .await
    .unwrap();
    let mut admission = pioneer_crud::RecoveryAdmission::unknown();
    pass.apply(&mut admission, now.timestamp());
    assert_eq!(admission.action_next, Some(future));
    assert!(!admission.action_outbox);
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    processor.release_completed_history_preparation_barrier();
    tokio::task::yield_now().await;
    let ticker = tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    ticker.abort();
    assert!(observer.reads.lock().unwrap().is_empty());
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::resume();
    // Restored retry-date transition at a key behind the completed cursor.
    let change = processor
        .crud_store
        .background_work("recovery.control")
        .changing();
    let mut row: agent_action_outbox::ActiveModel =
        agent_action_outbox::Entity::find_by_id("f05-action-000")
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .into();
    row.next_attempt_at = Set(None);
    processor.arm_completed_history_preparation_barrier("__control_owner_settled__");
    sea_orm::ActiveModelTrait::update(row, &db).await.unwrap();
    change.finish(true);
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(
        agent_action_outbox::Entity::find_by_id("f05-action-000")
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .status,
        "delivered"
    );
    // All-excluded history: remove only genuine timed obligations, then prove
    // settled Empty has no recurring physical discovery.
    let change = processor
        .crud_store
        .background_work("recovery.control")
        .changing();
    agent_action_outbox::Entity::update_many()
        .col_expr(
            agent_action_outbox::Column::Status,
            sea_orm::sea_query::Expr::value("delivered"),
        )
        .filter(agent_action_outbox::Column::NextAttemptAt.is_not_null())
        .exec(&db)
        .await
        .unwrap();
    change.finish(true);
    processor.arm_completed_history_preparation_barrier("__control_owner_settled__");
    processor.release_completed_history_preparation_barrier();
    timeout(
        Duration::from_secs(10),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    processor.release_completed_history_preparation_barrier();
    tokio::task::yield_now().await;
    let ticker = tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(300)).await;
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    ticker.abort();
    assert!(observer.reads.lock().unwrap().is_empty());
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::resume();
    stop.cancel();
    timeout(Duration::from_secs(10), worker)
        .await
        .unwrap()
        .unwrap();
}
