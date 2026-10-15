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
        setup_pooled_file_workspace_manager_with_observer(Some(observer.clone())).await;
    let database = store.database_connection();
    let identity = agent_identity::Entity::find()
        .filter(agent_identity::Column::WorkspaceId.eq(&workspace))
        .one(&database)
        .await
        .unwrap()
        .unwrap();
    let now = pioneer_crud::utc_now();
    let execution = "E00000000000000000007";
    // A valid FK-backed owner: no disabled integrity checks in this worker test.
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
    assert_eq!(bad.last_error.as_deref(), Some("outbox_delivery_failed"));
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
