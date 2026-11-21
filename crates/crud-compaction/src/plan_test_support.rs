use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};

#[derive(Debug, serde::Deserialize)]
pub struct PlanNode {
    #[serde(rename = "Node Type")]
    pub kind: String,
    #[serde(rename = "Relation Name")]
    pub relation: Option<String>,
    #[serde(rename = "Alias")]
    pub alias: Option<String>,
    #[serde(rename = "Subplan Name")]
    pub subplan: Option<String>,
    #[serde(rename = "Index Name")]
    pub index: Option<String>,
    #[serde(rename = "Index Cond")]
    index_condition: Option<String>,
    #[serde(rename = "Plans", default)]
    pub children: Vec<PlanNode>,
}

impl PlanNode {
    pub fn walk(&self) -> Vec<&Self> {
        let mut nodes = vec![self];
        let mut cursor = 0;
        while cursor < nodes.len() {
            nodes.extend(&nodes[cursor].children);
            cursor += 1;
        }
        nodes
    }

    pub fn has_index_constraint(&self, column: &str, operator: &str) -> bool {
        self.index_condition.as_deref().is_some_and(|condition| {
            condition.split('(').any(|expression| {
                let mut tokens = expression.split_whitespace();
                let left = tokens.next().map(|term| term.rsplit('.').next().unwrap());
                left.is_some_and(|term| term.trim_matches('"') == column)
                    && tokens.next() == Some(operator)
            })
        })
    }

    pub fn scoped_index(&self, column: &str) -> bool {
        self.index.is_some() && self.has_index_constraint(column, "=")
    }
}

pub async fn explain<C: ConnectionTrait>(db: &C, mut statement: Statement) -> PlanNode {
    statement
        .sql
        .insert_str(0, "EXPLAIN (FORMAT JSON, COSTS OFF) ");
    let result = db.query_one_raw(statement).await.unwrap().unwrap();
    let document = result
        .try_get::<serde_json::Value>("", "QUERY PLAN")
        .unwrap();
    serde_json::from_value(document[0]["Plan"].clone()).unwrap()
}

pub fn assert_exact_branch_search(branch: &PlanNode, subject: &str, column: &str, by_alias: bool) {
    let nodes = branch.walk();
    let matches = |node: &PlanNode| {
        let label = if by_alias {
            &node.alias
        } else {
            &node.relation
        };
        label.as_deref() == Some(subject)
    };
    assert!(
        nodes
            .iter()
            .any(|node| matches(node) && node.scoped_index(column)),
        "missing exact indexed {column} lookup for {subject}: {branch:#?}"
    );
    assert!(
        !nodes
            .iter()
            .any(|node| matches(node) && node.kind == "Seq Scan"),
        "unexpected sequential scan for {subject}: {branch:#?}"
    );
}

pub async fn seed_compressed_sources(db: &pioneer_sqlite::SqliteDatabase) {
    use pioneer_crud_conversations::{turn_event, turn_item};
    use pioneer_crud_payloads::storage::PreparedPayload;

    let wire = format!(r#"{{"value":"{}"}}"#, "canonical 🌍".repeat(8192));
    let event = PreparedPayload::new(&wire).unwrap();
    let item = PreparedPayload::new(&wire).unwrap();
    let now = sea_orm::prelude::DateTimeUtc::from(std::time::SystemTime::now()).fixed_offset();
    let tx = db.begin().await.unwrap();
    turn_event::lock_stream(&tx, "source-turn").await.unwrap();
    turn_event::insert(
        &tx,
        turn_event::NewEvent {
            id: "event-source",
            thread_id: "source-thread",
            turn_id: "source-turn",
            sequence: 1,
            event_type: "fixture",
            idempotency_key: "event-source",
            created_at: now,
        },
        &event,
    )
    .await
    .unwrap();
    turn_item::upsert(
        &tx,
        turn_item::ItemWrite {
            id: "item-source".into(),
            turn_id: "source-turn".into(),
            item_id: "item-source".into(),
            item_type: "command_execution".into(),
            status: Some("completed".into()),
            created_at: now,
            updated_at: now,
        },
        &item,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

pub async fn seed_canonical_plan_noise(db: &pioneer_sqlite::SqliteDatabase) {
    // PostgreSQL can prefer a sequential scan on tiny tables. Populate other
    // canonical scopes so this checks the plan relevant to retained histories.
    db.execute_unprepared(
        "INSERT INTO turn_llm_context(id,turn_id,sequence,source,payload,created_at)
         SELECT 'plan-context-'||n,'other-turn',n,'noise','{}'::json,CURRENT_TIMESTAMP FROM generate_series(100,4095) n;
         INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at)
         SELECT 'plan-event-'||n,'other-thread','other-turn',n,'noise','{}'::json,CURRENT_TIMESTAMP FROM generate_series(100,4095) n;
         INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,created_at,updated_at)
         SELECT 'plan-item-'||n,'other-turn','plan-item-'||n,'command_execution','completed','{}'::json,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM generate_series(100,4095) n;
         INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at)
         SELECT 'plan-input-'||n,'other-turn',n,'text','noise','{}'::json,CURRENT_TIMESTAMP FROM generate_series(100,4095) n;
         ANALYZE turn_llm_context;
         ANALYZE turn_event;
         ANALYZE turn_item;
         ANALYZE turn_input;
         ANALYZE compaction_source_revision;
         ANALYZE compaction_event_revision;
         ANALYZE compaction_item_revision;
         ANALYZE compaction_input_revision;",
    ).await.unwrap();
}

pub async fn seed_task_basis(db: &pioneer_sqlite::SqliteDatabase) {
    use pioneer_crud_tasks::conversation_snapshot::{
        NewTaskRunConversationSnapshot, insert_if_absent,
    };

    let snapshot = NewTaskRunConversationSnapshot {
        run_id: "basis-run".into(),
        task_id: "task".into(),
        workspace_id: "ws".into(),
        conversation_thread_id: "source-thread".into(),
        source_turn_id: None,
        history_json: "[]".into(),
        created_at: sea_orm::prelude::DateTimeUtc::from(std::time::SystemTime::now())
            .fixed_offset(),
    };
    let prepared =
        pioneer_crud_payloads::storage::PreparedPayload::new(&snapshot.history_json).unwrap();
    let tx = db.begin().await.unwrap();
    let (_, inserted) = insert_if_absent(&tx, &snapshot, None, Some(&prepared))
        .await
        .unwrap();
    assert!(inserted);
    tx.commit().await.unwrap();
}

pub async fn replace_task_basis(db: &pioneer_sqlite::SqliteDatabase, text: &str) {
    let prepared = pioneer_crud_payloads::storage::PreparedPayload::new(text).unwrap();
    let tx = db.begin().await.unwrap();
    prepared.insert(&tx).await.unwrap();
    let result = tx
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE task_run_conversation_snapshot
         SET history_json_payload_id=$1, history_json_has_array_prefix=$2
         WHERE run_id='basis-run'",
            [
                prepared.id().into(),
                text.trim_start_matches(' ').starts_with('[').into(),
            ],
        ))
        .await
        .unwrap();
    assert_eq!(result.rows_affected(), 1);
    tx.commit().await.unwrap();
}

pub async fn seed_portable_event(
    db: &pioneer_sqlite::SqliteDatabase,
    id: &str,
    sequence: i64,
    wire: &str,
) {
    use pioneer_crud_conversations::turn_event;
    let prepared = pioneer_crud_payloads::storage::PreparedPayload::new(wire).unwrap();
    let tx = db.begin().await.unwrap();
    turn_event::lock_stream(&tx, "turn").await.unwrap();
    turn_event::insert(
        &tx,
        turn_event::NewEvent {
            id,
            thread_id: "thread",
            turn_id: "turn",
            sequence,
            event_type: "fixture",
            idempotency_key: id,
            created_at: sea_orm::prelude::DateTimeUtc::from(std::time::SystemTime::now())
                .fixed_offset(),
        },
        &prepared,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

// These existing reader-predicate fixtures intentionally model corrupt legacy
// headers that predate the complete-identity guard. Seed that historical state
// atomically and restore the exact production DDL before any repository read;
// no production transition or race fixture runs with a weakened guard.
pub async fn seed_legacy_frozen_header(db: &pioneer_sqlite::SqliteDatabase, sql: &str) {
    let tx = db.begin().await.unwrap();
    tx.execute_unprepared(
        "ALTER TABLE compaction_frozen_history DISABLE TRIGGER frozen_complete_identity",
    )
    .await
    .unwrap();
    tx.execute_unprepared(sql).await.unwrap();
    tx.execute_unprepared(
        "ALTER TABLE compaction_frozen_history ENABLE TRIGGER frozen_complete_identity",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

pub async fn seed_revision_fixture(db: &sea_orm::DatabaseConnection, compressed: bool) {
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',TRUE,FALSE)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('other','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('other-turn','other','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "WITH RECURSIVE nums(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM nums WHERE n<299) INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) SELECT 'input-'||n,'turn',n,'text','legacy','{\"type\":\"text\",\"text\":\"legacy\"}',CURRENT_TIMESTAMP FROM nums",
        "WITH RECURSIVE nums(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM nums WHERE n<300) INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) SELECT 'event-'||n,'thread','turn',n,'fixture','{}',CURRENT_TIMESTAMP FROM nums",
        "WITH RECURSIVE nums(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM nums WHERE n<300) INSERT INTO turn_llm_context(id,turn_id,sequence,source,payload,output_policy_snapshot,created_at) SELECT 'context-'||n,'turn',n,'tool_result_v2','{}','{}',CURRENT_TIMESTAMP FROM nums",
        "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES('unrelated','other-turn',0,'text','untouched','{}',CURRENT_TIMESTAMP)",
        "INSERT INTO turn_llm_context(id,turn_id,sequence,source,payload,output_policy_snapshot,created_at) VALUES('unrelated-context','other-turn',1,'tool_result_v2','{}','{}',CURRENT_TIMESTAMP)",
        "INSERT INTO turn_item(id,turn_id,item_id,item_type,status,active_attempt_number,payload,created_at,updated_at) VALUES('legacy-item','turn','legacy-item','command_execution','completed',0,'{\"text\":\"legacy 🧪\"}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    if compressed {
        let wire = serde_json::json!({"source":"🔧".repeat(32_768)}).to_string();
        let payload = pioneer_crud_payloads::storage::PreparedPayload::new(&wire).unwrap();
        let transaction = db.begin().await.unwrap();
        payload.insert(&transaction).await.unwrap();
        transaction
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE turn_event SET payload=NULL,payload_payload_id=$1 WHERE id='event-1'",
                [payload.id().into()],
            ))
            .await
            .unwrap();
        transaction.commit().await.unwrap();
    }
}

pub async fn seed_payload_sources(db: &sea_orm::DatabaseConnection) {
    db.execute_unprepared(
        r#"
        INSERT INTO workspace(id,name) VALUES ('ws','fixture');
        INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
            VALUES ('thread','ws','','agent','m','p','active');
        INSERT INTO turn(id,thread_id,status,turn_kind,origin)
            VALUES ('turn','thread','completed','conversation','user');
        INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload)
            VALUES ('event','thread','turn',1,'fixture','{"text":"original"}');
        INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload)
            VALUES ('item','turn','item','command_execution','completed','{"text":"original"}');
        INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload)
            VALUES ('input','turn',0,'text','original','{"text":"original"}');
        INSERT INTO turn_llm_context(id,turn_id,sequence,source,payload,output_policy_snapshot)
            VALUES ('context','turn',1,'assistant_round','{"text":"original"}','{}');
    "#,
    )
    .await
    .unwrap();
}

/// Prepare a checked runner candidate and verify the shared admission/revision fences.
pub async fn prepare_checked_candidate(
    store: &crate::store::CompactionStore,
) -> (
    pioneer_compaction::Checkpoint,
    pioneer_compaction::runner::RunnerState,
) {
    use crate::{canonical_history::PagedSource, runner_repository::ManifestEntry};
    use pioneer_compaction::{
        Checkpoint, CompactionMode, CompactionPlan, CompactionSettings, ModelBudget,
        ModelSelection, OperationSnapshot, Transport,
        runner::{RunnerState, SourceCursor},
    };

    store.database_connection().execute_unprepared("INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('parent','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    store.database_connection().execute_unprepared("INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('parent-turn','parent','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    store.database_connection().execute_unprepared("INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('parent-source','parent','parent-turn',1,'fixture',to_json('reference fact'::text),CURRENT_TIMESTAMP)").await.unwrap();
    store.database_connection().execute_unprepared(
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at)
         VALUES ('runner-source','thread','turn',1,'fixture',to_json('original source'::text),CURRENT_TIMESTAMP)",
    ).await.unwrap();
    // Simulate a source predating the migration: admission must seed revision
    // metadata even if this source is never read by the service materializer.
    store
        .database_connection()
        .execute_unprepared("DELETE FROM compaction_event_revision WHERE source_id='runner-source'")
        .await
        .unwrap();
    let page = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap();
    let reference = page.entries[0].reference.clone();
    assert_eq!(reference.version, "event-revision:1");
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        0
    );
    let snapshot = OperationSnapshot {
        id: "runner".into(),
        owner: "runner-owner".into(),
        expected_checkpoint: None,
        projection_version: 0,
        source_epochs: std::collections::BTreeMap::from([("parent".into(), 0)]),
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
            compact: vec![0],
            retain: vec![],
            coverage: vec![reference.clone()],
            fingerprint: "runner-plan".into(),
        },
    };
    store
        .compaction_admit("ws", "thread", &snapshot)
        .await
        .unwrap();
    let budget = ModelBudget::new(None, None, None);
    store
        .compaction_prepare_runner("runner", &budget, 1, 0)
        .await
        .unwrap();
    let initial = RunnerState::new(snapshot.admission.deadline_ms, &budget, 1000, None).unwrap();
    assert!(
        store
            .compaction_activate_runner("runner", &initial)
            .await
            .is_err()
    );
    let manifest = ManifestEntry {
        ordinal: 0,
        unit: 0,
        reference_only: false,
        thread_id: "thread".into(),
        source: reference.clone(),
    };
    store
        .compaction_append_manifest("runner", &[manifest.clone()])
        .await
        .unwrap();
    store
        .compaction_append_manifest("runner", &[manifest])
        .await
        .unwrap();
    store
        .compaction_activate_runner("runner", &initial)
        .await
        .unwrap();
    let attempt = initial.claim(1).unwrap();
    assert!(
        store
            .compaction_runner_transition("runner", initial.generation, &attempt, None)
            .await
            .unwrap()
    );
    assert!(
        !store
            .compaction_runner_transition("runner", initial.generation, &attempt, None)
            .await
            .unwrap()
    );
    let next = attempt
        .candidate(
            1,
            "runner-candidate".into(),
            SourceCursor {
                unit: 1,
                ..Default::default()
            },
            true,
            2,
        )
        .unwrap();
    let checkpoint = Checkpoint {
        id: "runner-candidate".into(),
        operation_id: "runner".into(),
        format_version: 1,
        owner: snapshot.owner.clone(),
        previous: None,
        coverage: vec![reference],
        summary: "saved complete candidate".into(),
        selection: snapshot.admission.selection.clone(),
        projection_version: 0,
    };
    let mut wrong_selection = checkpoint.clone();
    wrong_selection.selection.model = "changed-after-admission".into();
    assert!(
        store
            .compaction_runner_transition(
                "runner",
                attempt.generation,
                &next,
                Some(&wrong_selection)
            )
            .await
            .is_err()
    );
    assert_eq!(
        store
            .compaction_runner_state("runner")
            .await
            .unwrap()
            .unwrap(),
        attempt
    );
    let mut wrong_basis = checkpoint.clone();
    wrong_basis.previous = Some("unrelated-checkpoint".into());
    assert!(
        !store
            .compaction_runner_transition("runner", attempt.generation, &next, Some(&wrong_basis))
            .await
            .unwrap()
    );
    assert!(
        store
            .compaction_checkpoint("runner-candidate")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .compaction_runner_transition("runner", attempt.generation, &next, Some(&checkpoint))
            .await
            .unwrap()
    );
    let restored = store
        .compaction_runner_state("runner")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored, next);
    assert_eq!(
        store
            .compaction_checkpoint("runner-candidate")
            .await
            .unwrap()
            .unwrap()
            .summary,
        "saved complete candidate"
    );
    let ready = restored.candidate_checked(true).unwrap();
    store
        .compaction_runner_transition("runner", restored.generation, &ready, None)
        .await
        .unwrap();
    (checkpoint, ready)
}
