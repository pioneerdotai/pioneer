//! Finite, restartable protocol work on the existing storage maintenance owner.
//! No constructor enables the operational permit or performs database work.
use super::compaction_frozen_root::FrozenRoot;
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_entity::compaction_frozen_maintenance_progress as progress;
use sea_orm::entity::prelude::*;
use sea_orm::{
    ConnectionTrait, DbBackend, QueryOrder, QuerySelect, Statement, TransactionTrait, Value,
};

#[derive(Debug)]
struct InventoryIntegrity(&'static str);
impl std::fmt::Display for InventoryIntegrity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for InventoryIntegrity {}
macro_rules! integrity {
    ($condition:expr,$message:literal $(,)?) => {
        if !$condition {
            return Err(InventoryIntegrity($message).into());
        }
    };
}

#[derive(Default)]
pub(crate) struct Cursor {
    pub(crate) workspace: Option<String>,
    pub(crate) phase: usize,
    pub(crate) storage_turn: bool,
    pub(crate) physical_turn: bool,
    pub(crate) cleanup_after: Option<(String, i64)>,
}
const PHASES: [&str; 8] = [
    "root_task",
    "root_runtime",
    "root_cli_thread",
    "root_cli_turn",
    "proof_backfill",
    "cleanup_seed",
    "detach",
    "logical_reclaim",
];
#[derive(Clone, Copy)]
struct CarrierSpec {
    table: &'static str,
    key: &'static str,
    owner: &'static str,
    body: &'static str,
    identity: &'static [&'static str],
}
const CARRIERS: [CarrierSpec; 4] = [
    CarrierSpec {
        table: "task_run_conversation_snapshot",
        key: "run_id",
        owner: "conversation_thread_id",
        body: "history_json",
        identity: &["task_id", "conversation_thread_id", "source_turn_id"],
    },
    CarrierSpec {
        table: "turn_runtime_snapshot",
        key: "turn_id",
        owner: "thread_id",
        body: "history_json",
        identity: &["thread_id"],
    },
    CarrierSpec {
        table: "thread_cli_runtime_binding",
        key: "thread_id",
        owner: "thread_id",
        body: "resume_cursor_json",
        identity: &["runtime_id", "runtime_kind", "native_thread_id"],
    },
    CarrierSpec {
        table: "turn_cli_runtime_binding",
        key: "turn_id",
        owner: "thread_id",
        body: "input_mapping_json",
        identity: &[
            "thread_id",
            "runtime_id",
            "runtime_kind",
            "native_thread_id",
        ],
    },
];
fn sql(s: impl Into<String>, values: impl IntoIterator<Item = Value>) -> Statement {
    Statement::from_sql_and_values(DbBackend::Sqlite, s, values)
}

pub(crate) async fn quantum(store: &CrudStore) -> Result<bool> {
    if !store.frozen_history_protocol_enabled() {
        return Ok(false);
    }
    let (workspace, phase, storage_turn) = {
        let mut cursor = store
            .frozen_maintenance_cursor
            .lock()
            .map_err(|_| anyhow::anyhow!("maintenance cursor poisoned"))?;
        cursor.storage_turn = !cursor.storage_turn;
        (cursor.workspace.clone(), cursor.phase, cursor.storage_turn)
    };
    // Alternate existing prefix work with protocol work; no lock spans awaits.
    if storage_turn {
        return Ok(false);
    }
    let workspace = if phase == 0 {
        let row = match workspace.as_deref() {
            None => {
                store
                    .connection
                    .query_one_raw(sql("SELECT id FROM workspace ORDER BY id LIMIT 1", []))
                    .await?
            }
            Some(after) => {
                store
                    .connection
                    .query_one_raw(sql(
                        "SELECT id FROM workspace WHERE id>? ORDER BY id LIMIT 1",
                        [after.into()],
                    ))
                    .await?
            }
        };
        let row = match row {
            Some(row) => Some(row),
            None => {
                store
                    .connection
                    .query_one_raw(sql("SELECT id FROM workspace ORDER BY id LIMIT 1", []))
                    .await?
            }
        };
        let Some(row) = row else { return Ok(false) };
        row.try_get::<String>("", "id")?
    } else {
        let Some(workspace) = workspace else {
            return Ok(false);
        };
        workspace
    };
    {
        let mut cursor = store
            .frozen_maintenance_cursor
            .lock()
            .map_err(|_| anyhow::anyhow!("maintenance cursor poisoned"))?;
        cursor.workspace = Some(workspace.clone());
        cursor.phase = (phase + 1) % PHASES.len();
    }
    store.run_serialized_write(||async {
        let tx=store.connection.begin().await?;
        for phase in PHASES {
            tx.execute_raw(sql("INSERT INTO compaction_frozen_maintenance_progress(workspace_id,phase) SELECT ?,? WHERE EXISTS(SELECT 1 FROM workspace WHERE id=?) ON CONFLICT(workspace_id,phase) DO NOTHING",[workspace.clone().into(),phase.into(),workspace.clone().into()])).await?;
        }
        tx.commit().await?;Ok(())
    }).await?;
    let Some(p) = progress::Entity::find_by_id((workspace.clone(), PHASES[phase].to_owned()))
        .one(&store.connection)
        .await?
    else {
        return Ok(false);
    };
    match phase {
        0..=3 if p.state == "pending" => root_quantum(store, &p, CARRIERS[phase], phase).await,
        4 => proof_quantum(store, &p).await,
        5 if p.state == "pending" => seed_quantum(store, &p).await,
        6 | 7 if store.frozen_history_reclamation_enabled() => {
            let result = if phase == 6 {
                super::compaction_frozen_reclaim::detach(store, &workspace, p.after_key.as_deref())
                    .await?
            } else {
                super::compaction_frozen_reclaim::logical(store, &workspace, p.after_key.as_deref())
                    .await?
            };
            store
                .run_serialized_write(|| async {
                    let tx = store.connection.begin().await?;
                    progress_matches(&tx, &p).await?;
                    advance(&tx, &p, result.as_deref(), false).await?;
                    tx.commit().await?;
                    Ok(result.is_some())
                })
                .await
        }
        _ => Ok(false),
    }
}

async fn progress_matches<C: ConnectionTrait>(db: &C, p: &progress::Model) -> Result<()> {
    let current = progress::Entity::find_by_id((p.workspace_id.clone(), p.phase.clone()))
        .one(db)
        .await?;
    ensure!(current.as_ref() == Some(p), "maintenance progress changed");
    ensure!(
        db.query_one_raw(sql(
            "SELECT id FROM workspace WHERE id=?",
            [p.workspace_id.clone().into()]
        ))
        .await?
        .is_some(),
        "maintenance workspace unavailable"
    );
    Ok(())
}
async fn advance<C: ConnectionTrait>(
    db: &C,
    p: &progress::Model,
    after: Option<&str>,
    complete: bool,
) -> Result<()> {
    let changed=db.execute_raw(sql("UPDATE compaction_frozen_maintenance_progress SET after_key=?,state=? WHERE workspace_id=? AND phase=? AND after_key IS ? AND state=? AND protocol_version=1",[after.into(),if complete{"complete"}else{"pending"}.into(),p.workspace_id.clone().into(),p.phase.clone().into(),p.after_key.clone().into(),p.state.clone().into()])).await?.rows_affected();
    ensure!(changed == 1, "maintenance cursor CAS changed");
    Ok(())
}

async fn read_text(
    store: &CrudStore,
    spec: CarrierSpec,
    rid: i64,
    workspace: &str,
    size: i64,
) -> Result<String> {
    ensure!(size >= 0, "invalid carrier byte size");
    let mut bytes = Vec::new();
    let mut offset = 0i64;
    while offset < size {
        let length = (size - offset).min(super::compaction::SOURCE_PAGE_BYTES as i64);
        let row=store.connection.query_one_raw(sql(format!("SELECT substr(CAST({} AS BLOB),?,?) AS fragment FROM {} WHERE rowid=? AND workspace_id=? AND length(CAST({} AS BLOB))=?",spec.body,spec.table,spec.body),[(offset+1).into(),length.into(),rid.into(),workspace.into(),size.into()])).await?.ok_or_else(||anyhow::anyhow!("carrier disappeared during inventory"))?;
        let fragment = row.try_get::<Vec<u8>>("", "fragment")?;
        ensure!(fragment.len() as i64 == length, "carrier fragment changed");
        bytes.extend(fragment);
        offset += length;
    }
    Ok(String::from_utf8(bytes)?)
}
async fn classify_in<C: ConnectionTrait>(
    db: &C,
    root: FrozenRoot,
    workspace: &str,
    owner: Option<&str>,
    orphan: bool,
) -> Result<FrozenRoot> {
    let FrozenRoot::Manifest(ref descriptor) = root else {
        return Ok(root);
    };
    if orphan {
        return Ok(root);
    }
    let Some(owner) = owner else {
        return Ok(FrozenRoot::Blocked);
    };
    // Accepted orphan carrier is still a durable root. No join through Task/run
    // may drop it and no missing header is replaced with fresh live history.
    let domain=db.query_one_raw(sql("SELECT t.id FROM thread t JOIN workspace w ON w.id=t.workspace_id WHERE t.id=? AND t.workspace_id=?",[owner.into(),workspace.into()])).await?;
    if domain.is_none() {
        return Ok(root);
    }
    let exact=db.query_one_raw(sql("SELECT id FROM compaction_frozen_history WHERE id=? AND workspace_id=? AND owner_thread=? AND identity_sha256=? AND message_count=? AND availability='resident' AND ready=1 AND next_ordinal=message_count AND next_import=import_count",[descriptor.manifest_id.clone().into(),workspace.into(),owner.into(),descriptor.identity_sha256.clone().into(),i64::try_from(descriptor.messages)?.into()])).await?;
    Ok(if exact.is_some() {
        root
    } else {
        FrozenRoot::Blocked
    })
}
async fn root_quantum(
    store: &CrudStore,
    p: &progress::Model,
    spec: CarrierSpec,
    kind: usize,
) -> Result<bool> {
    let select = match p.after_key.as_deref() {
        None => sql(
            format!(
                "SELECT rowid AS rid,length(CAST({} AS BLOB)) AS bytes FROM {} WHERE workspace_id=? ORDER BY {} LIMIT 1",
                spec.body, spec.table, spec.key
            ),
            [p.workspace_id.clone().into()],
        ),
        Some(after) => sql(
            format!(
                "SELECT rowid AS rid,length(CAST({} AS BLOB)) AS bytes FROM {} WHERE workspace_id=? AND {}>? ORDER BY {} LIMIT 1",
                spec.body, spec.table, spec.key, spec.key
            ),
            [p.workspace_id.clone().into(), after.into()],
        ),
    };
    let Some(row) = store.connection.query_one_raw(select).await? else {
        return store
            .run_serialized_write(|| async {
                let tx = store.connection.begin().await?;
                progress_matches(&tx, p).await?;
                let eof = match p.after_key.as_deref() {
                    None => sql(
                        format!("SELECT 1 FROM {} WHERE workspace_id=? LIMIT 1", spec.table),
                        [p.workspace_id.clone().into()],
                    ),
                    Some(after) => sql(
                        format!(
                            "SELECT 1 FROM {} WHERE workspace_id=? AND {}>? LIMIT 1",
                            spec.table, spec.key
                        ),
                        [p.workspace_id.clone().into(), after.into()],
                    ),
                };
                ensure!(
                    tx.query_one_raw(eof).await?.is_none(),
                    "root inventory EOF changed"
                );
                advance(&tx, p, p.after_key.as_deref(), true).await?;
                tx.commit().await?;
                Ok(true)
            })
            .await;
    };
    let rid = row.try_get::<i64>("", "rid")?;
    let size = row.try_get::<i64>("", "bytes")?;
    let fields = spec.identity.join(",");
    let metadata = store
        .connection
        .query_one_raw(sql(
            format!(
                "SELECT {} AS key,{} AS owner,{fields} FROM {} WHERE rowid=? AND workspace_id=?",
                spec.key, spec.owner, spec.table
            ),
            [rid.into(), p.workspace_id.clone().into()],
        ))
        .await?
        .ok_or_else(|| anyhow::anyhow!("carrier metadata unavailable"))?;
    let key = metadata.try_get::<String>("", "key")?;
    let owner = metadata.try_get::<String>("", "owner")?;
    let identity = spec
        .identity
        .iter()
        .map(|column| metadata.try_get::<Option<String>>("", column))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let body = read_text(store, spec, rid, &p.workspace_id, size).await?;
    let (root, root_owner) = match kind {
        0 | 1 => (FrozenRoot::history(&body), Some(owner)),
        2 => FrozenRoot::cli_thread(&body),
        3 => FrozenRoot::cli_turn(&body),
        _ => unreachable!(),
    };
    let mut values: Vec<Value> = vec![
        p.workspace_id.clone().into(),
        key.clone().into(),
        rid.into(),
        body.clone().into(),
    ];
    let mut exact = format!(
        "workspace_id=? AND {}=? AND rowid=? AND CAST({} AS BLOB)=CAST(? AS BLOB)",
        spec.key, spec.body
    );
    for (column, value) in spec.identity.iter().zip(identity) {
        exact.push_str(&format!(" AND {column} IS ?"));
        values.push(value.into());
    }
    #[cfg(any(test, feature = "test-support"))]
    super::compaction_runner::trigger_publication_test_hook(
        store,
        &key,
        super::compaction_runner::PublicationTestPause::RootInventoryBeforeWriter,
    )
    .await;
    store
        .run_serialized_write(|| async {
            let tx = store.connection.begin().await?;
            progress_matches(&tx, p).await?;
            let domain_sql=match kind {
                0=>"SELECT s.run_id FROM task_run_conversation_snapshot s JOIN task_run r ON r.id=s.run_id AND r.task_id=s.task_id JOIN task t ON t.id=s.task_id AND t.workspace_id=s.workspace_id JOIN thread th ON th.id=s.conversation_thread_id AND th.workspace_id=s.workspace_id WHERE s.run_id=? AND s.workspace_id=?",
                1=>"SELECT s.turn_id FROM turn_runtime_snapshot s JOIN turn v ON v.id=s.turn_id AND v.thread_id=s.thread_id JOIN thread th ON th.id=s.thread_id AND th.workspace_id=s.workspace_id WHERE s.turn_id=? AND s.workspace_id=?",
                2=>"SELECT s.thread_id FROM thread_cli_runtime_binding s JOIN thread th ON th.id=s.thread_id AND th.workspace_id=s.workspace_id WHERE s.thread_id=? AND s.workspace_id=?",
                3=>"SELECT s.turn_id FROM turn_cli_runtime_binding s JOIN turn v ON v.id=s.turn_id AND v.thread_id=s.thread_id JOIN thread th ON th.id=s.thread_id AND th.workspace_id=s.workspace_id WHERE s.turn_id=? AND s.workspace_id=?",
                _=>unreachable!(),
            };
            let orphan=tx.query_one_raw(sql(domain_sql,[key.clone().into(),p.workspace_id.clone().into()])).await?.is_none();
            let classified = classify_in(&tx, root.clone(), &p.workspace_id, root_owner.as_deref(),orphan).await?;
            let mut update_values = vec![classified.id().into(), classified.state().into()];
            update_values.extend(values.clone());
            let changed = tx
                .execute_raw(sql(
                    format!(
                        "UPDATE {} SET frozen_manifest_id=?,frozen_root_state=? WHERE {exact}",
                        spec.table
                    ),
                    update_values,
                ))
                .await?
                .rows_affected();
            ensure!(changed == 1, "root carrier bytes/identity changed");
            advance(&tx, p, Some(&key), false).await?;
            tx.commit().await?;
            Ok(true)
        })
        .await
}

pub(crate) async fn workspace_ready<C: ConnectionTrait>(db: &C, workspace: &str) -> Result<bool> {
    for (phase, spec) in PHASES[..4].iter().zip(CARRIERS) {
        if db.query_one_raw(sql("SELECT workspace_id FROM compaction_frozen_maintenance_progress WHERE workspace_id=? AND phase=? AND state='complete' AND protocol_version=1",[workspace.into(),(*phase).into()])).await?.is_none(){return Ok(false);}
        for state in ["unknown", "blocked"] {
            if db
                .query_one_raw(sql(
                    format!(
                        "SELECT 1 FROM {} WHERE workspace_id=? AND frozen_root_state=? LIMIT 1",
                        spec.table
                    ),
                    [workspace.into(), state.into()],
                ))
                .await?
                .is_some()
            {
                return Ok(false);
            }
        }
    }
    Ok(db
        .query_one_raw(sql(
            "SELECT id FROM workspace WHERE id=?",
            [workspace.into()],
        ))
        .await?
        .is_some())
}

async fn seed_quantum(store: &CrudStore, p: &progress::Model) -> Result<bool> {
    let select = match p.after_key.as_deref() {
        None => sql(
            "SELECT rowid AS rid,length(CAST(id AS BLOB)) AS bytes,storage_generation FROM compaction_frozen_history WHERE workspace_id=? ORDER BY id LIMIT 64",
            [p.workspace_id.clone().into()],
        ),
        Some(after) => sql(
            "SELECT rowid AS rid,length(CAST(id AS BLOB)) AS bytes,storage_generation FROM compaction_frozen_history WHERE workspace_id=? AND id>? ORDER BY id LIMIT 64",
            [p.workspace_id.clone().into(), after.into()],
        ),
    };
    let rows = store.connection.query_all_raw(select).await?;
    let mut headers = Vec::new();
    let mut bytes = 0usize;
    for row in rows {
        let size = row.try_get::<i64>("", "bytes")?;
        if size < 0 || size > super::compaction::SOURCE_PAGE_BYTES as i64 / 3 {
            // An oversized metadata key is conservative debt, not an endless
            // seed retry and not authority to rewrite a public identity.
            return store.run_serialized_write(||async {
                let tx=store.connection.begin().await?;progress_matches(&tx,p).await?;
                let changed=tx.execute_raw(sql("UPDATE compaction_frozen_maintenance_progress SET state='blocked' WHERE workspace_id=? AND phase='cleanup_seed' AND state='pending' AND after_key IS ?",[p.workspace_id.clone().into(),p.after_key.clone().into()])).await?.rows_affected();
                ensure!(changed==1,"blocked seed cursor changed");tx.commit().await?;Ok(true)
            }).await;
        }
        if bytes + size as usize * 3 > super::compaction::SOURCE_PAGE_BYTES {
            break;
        }
        let spec = CarrierSpec {
            table: "compaction_frozen_history",
            key: "id",
            owner: "owner_thread",
            body: "id",
            identity: &[],
        };
        let id = read_text(store, spec, row.try_get("", "rid")?, &p.workspace_id, size).await?;
        bytes += id.len() * 3;
        headers.push((id, row.try_get::<i64>("", "storage_generation")?));
    }
    store.run_serialized_write(||async {
        let tx=store.connection.begin().await?;progress_matches(&tx,p).await?;
        for (id,generation) in &headers {
            for kind in [0i64,1] {
                let inserted=tx.execute_raw(sql("INSERT INTO compaction_frozen_cleanup(manifest_id,kind,storage_generation) SELECT id,?,storage_generation FROM compaction_frozen_history WHERE id=? AND workspace_id=? AND storage_generation=? ON CONFLICT(manifest_id,kind) DO NOTHING",[kind.into(),id.clone().into(),p.workspace_id.clone().into(),(*generation).into()])).await?;
                if inserted.rows_affected()==0 {
                    ensure!(tx.query_one_raw(sql("SELECT h.id FROM compaction_frozen_history h JOIN compaction_frozen_cleanup c ON c.manifest_id=h.id WHERE h.id=? AND h.workspace_id=? AND h.storage_generation=? AND c.kind=?",[id.clone().into(),p.workspace_id.clone().into(),(*generation).into(),kind.into()])).await?.is_some(),"cleanup seed header changed");
                }
            }
        }
        let complete=headers.is_empty();
        if complete {
            let eof=match p.after_key.as_deref(){None=>sql("SELECT 1 FROM compaction_frozen_history WHERE workspace_id=? LIMIT 1",[p.workspace_id.clone().into()]),Some(after)=>sql("SELECT 1 FROM compaction_frozen_history WHERE workspace_id=? AND id>? LIMIT 1",[p.workspace_id.clone().into(),after.into()])};
            ensure!(tx.query_one_raw(eof).await?.is_none(),"cleanup seed EOF changed");
        }
        advance(&tx,p,headers.last().map(|h|h.0.as_str()).or(p.after_key.as_deref()),complete).await?;tx.commit().await?;Ok(true)
    }).await
}

#[derive(Clone)]
struct CompletedBoundary {
    operation: pioneer_entity::compaction_operation::Model,
    projection: pioneer_entity::compaction_operation_projection::Model,
    context: pioneer_entity::compaction_context::Model,
}
impl CompletedBoundary {
    async fn load(store: &CrudStore, id: &str) -> Result<Self> {
        use pioneer_entity::{
            compaction_context as ctx, compaction_operation as op,
            compaction_operation_projection as origin,
        };
        let operation = op::Entity::find_by_id(id)
            .one(&store.connection)
            .await?
            .ok_or_else(|| anyhow::anyhow!("completed operation unavailable"))?;
        let projection = origin::Entity::find_by_id(id)
            .one(&store.connection)
            .await?
            .ok_or_else(|| anyhow::anyhow!("completed origin unavailable"))?;
        let context = ctx::Entity::find_by_id(&operation.owner)
            .one(&store.connection)
            .await?
            .ok_or_else(|| anyhow::anyhow!("completed context unavailable"))?;
        ensure!(
            operation.status == "completed"
                && operation.outcome.as_deref() == Some("applied")
                && operation.frozen_publication_contract != "assertion_compat"
                && projection.storage_state == "bound",
            "operation not eligible for completed inventory"
        );
        Ok(Self {
            operation,
            projection,
            context,
        })
    }
    async fn validate<C: ConnectionTrait>(&self, db: &C) -> Result<()> {
        use pioneer_entity::{
            compaction_context as ctx, compaction_operation as op,
            compaction_operation_projection as origin,
        };
        ensure!(
            op::Entity::find_by_id(&self.operation.id)
                .one(db)
                .await?
                .as_ref()
                == Some(&self.operation),
            "completed operation boundary changed"
        );
        ensure!(
            origin::Entity::find_by_id(&self.operation.id)
                .one(db)
                .await?
                .as_ref()
                == Some(&self.projection),
            "completed original projection changed"
        );
        let actual = ctx::Entity::find_by_id(&self.operation.owner)
            .one(db)
            .await?;
        ensure!(
            actual
                .as_ref()
                .is_some_and(|c| c.workspace_id == self.context.workspace_id
                    && c.thread_id == self.context.thread_id
                    && c.format_version == self.context.format_version),
            "completed historical domain changed"
        );
        ensure!(db.query_one_raw(sql("SELECT t.id FROM thread t JOIN workspace w ON w.id=t.workspace_id WHERE t.id=? AND t.workspace_id=?",[self.context.thread_id.clone().into(),self.context.workspace_id.clone().into()])).await?.is_some(),"completed domain unavailable");
        Ok(())
    }
}

async fn proof_quantum(store: &CrudStore, p: &progress::Model) -> Result<bool> {
    let mut values: Vec<Value> = vec![p.workspace_id.clone().into()];
    let lower = if let Some(after) = &p.after_key {
        values.push(after.clone().into());
        " AND o.id>?"
    } else {
        ""
    };
    let selected=store.connection.query_one_raw(sql(format!("SELECT o.id FROM compaction_operation o JOIN compaction_context c ON c.owner=o.owner JOIN thread t ON t.id=c.thread_id AND t.workspace_id=c.workspace_id JOIN compaction_operation_projection x ON x.operation_id=o.id \
        WHERE c.workspace_id=? AND o.status='completed' AND o.outcome='applied' AND o.frozen_publication_contract<>'assertion_compat' AND x.storage_state='bound' \
        AND o.frozen_proof_state IN ('legacy','pending','prepared') {lower} ORDER BY o.id LIMIT 1"),values)).await?;
    let Some(selected) = selected else {
        // Completed discovery is dynamic, unlike the stable per-operation set.
        // Wrap the fairness cursor, retaining all authoritative operation cursors.
        if p.after_key.is_none() {
            return Ok(false);
        }
        return store
            .run_serialized_write(|| async {
                let tx = store.connection.begin().await?;
                progress_matches(&tx, p).await?;
                advance(&tx, p, None, false).await?;
                tx.commit().await?;
                Ok(true)
            })
            .await;
    };
    let id = selected.try_get::<String>("", "id")?;
    let boundary = CompletedBoundary::load(store, &id).await?;
    let guard = match store.compaction_pin_operation_projection(&id).await {
        Ok(Some(guard)) => guard,
        Ok(None) => {
            quarantine_completed(store, p, &boundary).await?;
            return Ok(true);
        }
        Err(error) => {
            if error.is::<sea_orm::DbErr>() || crate::is_anyhow_sqlite_lock(&error) {
                return Err(error);
            }
            quarantine_completed(store, p, &boundary).await?;
            return Ok(true);
        }
    };
    let result = completed_step(store, p, &boundary, &guard).await;
    // P3 preparation records integrity quarantine itself. Other temporary
    // invalidations cannot be turned into corruption by this maintenance owner.
    let result = match result {
        Err(error)
            if error.is::<super::compaction_frozen_verify::FrozenIntegrityError>()
                || error.is::<serde_json::Error>()
                || error.is::<InventoryIntegrity>() =>
        {
            quarantine_completed(store, p, &boundary)
                .await
                .map(|()| true)
        }
        result => result,
    };
    guard.complete(result).await
}
async fn quarantine_completed(
    store: &CrudStore,
    p: &progress::Model,
    b: &CompletedBoundary,
) -> Result<()> {
    store.run_serialized_write(||async {
        let tx=store.connection.begin().await?;progress_matches(&tx,p).await?;b.validate(&tx).await?;
        let changed=tx.execute_raw(sql("UPDATE compaction_operation SET frozen_proof_state='quarantined' WHERE id=? AND status='completed' AND outcome='applied' AND frozen_publication_contract<>'assertion_compat'",[b.operation.id.clone().into()])).await?.rows_affected();
        ensure!(changed==1,"completed quarantine changed");advance(&tx,p,Some(&b.operation.id),false).await?;tx.commit().await?;Ok(())
    }).await
}
async fn completed_step(
    store: &CrudStore,
    p: &progress::Model,
    b: &CompletedBoundary,
    guard: &crate::FrozenUseGuard,
) -> Result<bool> {
    use pioneer_entity::compaction_checkpoint as cp;
    let o = &b.operation;
    let snapshot: pioneer_compaction::OperationSnapshot = serde_json::from_str(&o.snapshot)?;
    integrity!(
        snapshot.owner == o.owner,
        "completed snapshot owner mismatch"
    );
    if o.frozen_accounting_mode == "legacy_bound" {
        super::compaction_frozen_verify::verify(store, guard).await?;
        return store.run_serialized_write(||async {
            let tx=store.connection.begin().await?;progress_matches(&tx,p).await?;b.validate(&tx).await?;guard.validate_in(&tx,true).await?;
            for state in ["counted","prepared"] {
                integrity!(tx.query_one_raw(sql("SELECT id FROM compaction_checkpoint WHERE operation_id=? AND frozen_accounting_state=? LIMIT 1",[o.id.clone().into(),state.into()])).await?.is_none(),"legacy operation has unexpected accounting flags");
            }
            let changed=tx.execute_raw(sql("UPDATE compaction_operation SET frozen_accounting_mode='completed_inventory',frozen_inventory_state='pending',frozen_checkpoint_count=0,frozen_prepared_checkpoint_count=0,frozen_backfill_after_checkpoint=NULL,frozen_proof_state='pending' WHERE id=? AND status='completed' AND outcome='applied' AND frozen_publication_contract<>'assertion_compat' AND frozen_accounting_mode='legacy_bound' AND frozen_inventory_state='unknown' AND frozen_checkpoint_count IS NULL AND frozen_prepared_checkpoint_count IS NULL AND frozen_backfill_after_checkpoint IS NULL",[o.id.clone().into()])).await?.rows_affected();
            ensure!(changed==1,"completed inventory start CAS changed");tx.commit().await?;Ok(true)
        }).await;
    }
    integrity!(
        matches!(
            o.frozen_accounting_mode.as_str(),
            "completed_inventory" | "live_known"
        ) && o.frozen_checkpoint_count.is_some()
            && o.frozen_prepared_checkpoint_count.is_some(),
        "completed inventory accounting invalid"
    );
    let pending = cp::Entity::find()
        .select_only()
        .column(cp::Column::Id)
        .filter(cp::Column::OperationId.eq(&o.id))
        .filter(cp::Column::FrozenAccountingState.eq("counted"))
        .order_by_asc(cp::Column::Id)
        .limit(1)
        .into_tuple::<String>()
        .one(&store.connection)
        .await?;
    if let Some(id) = pending {
        // Independent of inventory cursor: a crash after counted/cursor commit
        // cannot hide its pending preparation, including a preprepared header.
        super::compaction_checkpoint_proof::prepare(store, &id).await?;
        return Ok(true);
    }
    if o.frozen_inventory_state == "pending" {
        let ids = super::compaction_checkpoint_proof::checkpoint_ids_page(
            store,
            &o.id,
            o.frozen_backfill_after_checkpoint.as_deref(),
        )
        .await?;
        let mut checkpoints = Vec::new();
        let mut checked_ids = Vec::new();
        let mut page_bytes = 0usize;
        for id in &ids {
            let row=store.connection.query_one_raw(sql("SELECT id,operation_id,owner,identity_sha256,previous,projection_version,format_version,frozen_accounting_state FROM compaction_checkpoint WHERE id=? AND operation_id=?",[id.clone().into(),o.id.clone().into()])).await?.ok_or_else(||anyhow::anyhow!("inventory checkpoint unavailable"))?;
            let owner = row.try_get::<String>("", "owner")?;
            let identity = row.try_get::<String>("", "identity_sha256")?;
            let previous = row.try_get::<Option<String>>("", "previous")?;
            let bytes = id.len()
                + owner.len()
                + identity.len()
                + previous.as_ref().map_or(0, String::len)
                + o.id.len();
            integrity!(
                bytes <= super::compaction::SOURCE_PAGE_BYTES,
                "inventory checkpoint metadata oversized"
            );
            if page_bytes + bytes > super::compaction::SOURCE_PAGE_BYTES {
                break;
            }
            page_bytes += bytes;
            checked_ids.push(id.clone());
            checkpoints.push((
                id.clone(),
                row.try_get::<String>("", "owner")?,
                row.try_get::<String>("", "identity_sha256")?,
                row.try_get::<Option<String>>("", "previous")?,
                row.try_get::<i64>("", "projection_version")?,
                row.try_get::<i64>("", "format_version")?,
                row.try_get::<String>("", "frozen_accounting_state")?,
            ));
        }
        #[cfg(any(test, feature = "test-support"))]
        super::compaction_runner::trigger_publication_test_hook(
            store,
            &o.id,
            super::compaction_runner::PublicationTestPause::CompletedInventoryBeforeWriter,
        )
        .await;
        return store.run_serialized_write(||async {
            let tx=store.connection.begin().await?;progress_matches(&tx,p).await?;b.validate(&tx).await?;guard.validate_in(&tx,true).await?;
            let mut added=0i64;
            for (id,owner,identity,previous,projection,format,state) in &checkpoints {
                integrity!(owner==&o.owner,"inventory checkpoint owner mismatch");
                ensure!(tx.query_one_raw(sql("SELECT id FROM compaction_checkpoint WHERE id=? AND operation_id=? AND owner=? AND identity_sha256=? AND previous IS ? AND projection_version=? AND format_version=? AND frozen_accounting_state=?",[id.clone().into(),o.id.clone().into(),owner.clone().into(),identity.clone().into(),previous.clone().into(),(*projection).into(),(*format).into(),state.clone().into()])).await?.is_some(),"inventory immutable checkpoint tuple changed");
                let changed=tx.execute_raw(sql("UPDATE compaction_checkpoint SET frozen_accounting_state='counted' WHERE id=? AND operation_id=? AND frozen_accounting_state='uncounted'",[id.clone().into(),o.id.clone().into()])).await?.rows_affected();
                added+=i64::try_from(changed)?;
            }
            let eof=checked_ids.is_empty();
            if eof {
                let statement=match o.frozen_backfill_after_checkpoint.as_deref(){None=>sql("SELECT id FROM compaction_checkpoint WHERE operation_id=? LIMIT 1",[o.id.clone().into()]),Some(after)=>sql("SELECT id FROM compaction_checkpoint WHERE operation_id=? AND id>? LIMIT 1",[o.id.clone().into(),after.into()])};
                ensure!(tx.query_one_raw(statement).await?.is_none(),"completed inventory EOF changed");
            }
            let after=checked_ids.last().cloned().or(o.frozen_backfill_after_checkpoint.clone());
            let changed=tx.execute_raw(sql("UPDATE compaction_operation SET frozen_checkpoint_count=frozen_checkpoint_count+?,frozen_backfill_after_checkpoint=?,frozen_inventory_state=? WHERE id=? AND status='completed' AND outcome='applied' AND frozen_accounting_mode='completed_inventory' AND frozen_inventory_state='pending' AND frozen_checkpoint_count IS ? AND frozen_prepared_checkpoint_count IS ? AND frozen_backfill_after_checkpoint IS ?",[added.into(),after.into(),if eof{"known"}else{"pending"}.into(),o.id.clone().into(),o.frozen_checkpoint_count.into(),o.frozen_prepared_checkpoint_count.into(),o.frozen_backfill_after_checkpoint.clone().into()])).await?.rows_affected();
            ensure!(changed==1,"completed inventory flags/count/cursor CAS changed");tx.commit().await?;Ok(true)
        }).await;
    }
    integrity!(
        o.frozen_inventory_state == "known"
            && o.frozen_checkpoint_count == o.frozen_prepared_checkpoint_count,
        "completed full proof accounting incomplete"
    );
    super::compaction_frozen_verify::verify(store, guard).await?;
    let finals = cp::Entity::find()
        .select_only()
        .column(cp::Column::Id)
        .filter(cp::Column::OperationId.eq(&o.id))
        .filter(cp::Column::Status.eq("applied"))
        .order_by_asc(cp::Column::Id)
        .limit(2)
        .into_tuple::<String>()
        .all(&store.connection)
        .await?;
    integrity!(
        finals.len() == usize::from(o.frozen_checkpoint_count != Some(0)),
        "completed final checkpoint binding unavailable"
    );
    if let Some(final_cp) = finals.first() {
        if let Err(error) =
            super::compaction_runner::validate_publication_checkpoint_graph(store, final_cp).await
        {
            if error.is::<sea_orm::DbErr>() || crate::is_anyhow_sqlite_lock(&error) {
                return Err(error);
            }
            return Err(InventoryIntegrity("completed checkpoint graph corrupt").into());
        }
    }
    store.run_serialized_write(||async {
        let tx=store.connection.begin().await?;progress_matches(&tx,p).await?;b.validate(&tx).await?;guard.validate_in(&tx,true).await?;
        if let Some(final_cp)=finals.first() {
            ensure!(tx.query_one_raw(sql("SELECT p.id FROM compaction_checkpoint p JOIN compaction_checkpoint_proof h ON h.checkpoint_id=p.id AND h.operation_id=p.operation_id WHERE p.id=? AND p.operation_id=? AND p.owner=? AND p.status='applied' AND p.frozen_accounting_state='prepared' AND h.state='prepared' AND h.owner=p.owner AND h.checkpoint_identity_sha256=p.identity_sha256 AND h.previous IS p.previous AND h.projection_version=p.projection_version AND h.format_version=p.format_version",[final_cp.clone().into(),o.id.clone().into(),o.owner.clone().into()])).await?.is_some(),"completed final proof binding changed");
        }
        let prepared=tx.execute_raw(sql("UPDATE compaction_operation SET frozen_proof_state='prepared' WHERE id=? AND status='completed' AND outcome='applied' AND frozen_publication_contract<>'assertion_compat' AND frozen_inventory_state='known' AND frozen_checkpoint_count=? AND frozen_prepared_checkpoint_count=? AND frozen_proof_state IN ('pending','prepared')",[o.id.clone().into(),o.frozen_checkpoint_count.into(),o.frozen_prepared_checkpoint_count.into()])).await?.rows_affected();ensure!(prepared==1,"completed proof-ready CAS changed");
        let complete=tx.execute_raw(sql("UPDATE compaction_operation SET frozen_proof_state='complete' WHERE id=? AND status='completed' AND outcome='applied' AND frozen_proof_state='prepared' AND frozen_inventory_state='known' AND frozen_checkpoint_count IS NOT NULL AND frozen_checkpoint_count=frozen_prepared_checkpoint_count",[o.id.clone().into()])).await?.rows_affected();ensure!(complete==1,"completed complete seal changed");
        advance(&tx,p,Some(&o.id),false).await?;tx.commit().await?;Ok(true)
    }).await
}
