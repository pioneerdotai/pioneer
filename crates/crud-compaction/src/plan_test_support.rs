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
