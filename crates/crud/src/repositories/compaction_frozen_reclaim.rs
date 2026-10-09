//! Logical reclamation only. Header/data deletion is never part of this module.
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_entity::{
    compaction_frozen_history as history, compaction_operation as operation,
    compaction_operation_projection as projection, compaction_runner_state as runner,
};
use sea_orm::{DbBackend, Statement, TransactionTrait, entity::prelude::*};
fn sql(s: impl Into<String>, v: impl IntoIterator<Item = Value>) -> Statement {
    Statement::from_sql_and_values(DbBackend::Sqlite, s, v)
}
/// The same exclusion is repeated in every transition and rebuild transaction.
pub(crate) async fn unrooted<C: ConnectionTrait>(db: &C, id: &str) -> Result<bool> {
    for statement in [
        "SELECT 1 FROM compaction_frozen_use WHERE manifest_id=? LIMIT 1",
        "SELECT 1 FROM task_run_conversation_snapshot WHERE frozen_manifest_id=? LIMIT 1",
        "SELECT 1 FROM turn_runtime_snapshot WHERE frozen_manifest_id=? LIMIT 1",
        "SELECT 1 FROM thread_cli_runtime_binding WHERE frozen_manifest_id=? LIMIT 1",
        "SELECT 1 FROM turn_cli_runtime_binding WHERE frozen_manifest_id=? LIMIT 1",
        "SELECT 1 FROM compaction_task_output WHERE manifest_id=? LIMIT 1",
        "SELECT 1 FROM compaction_operation_projection WHERE manifest_id=? AND storage_state='bound' LIMIT 1",
        "SELECT 1 FROM compaction_frozen_layout WHERE candidate=? AND active=0 LIMIT 1",
    ] {
        if db
            .query_one_raw(sql(statement, [id.into()]))
            .await?
            .is_some()
        {
            return Ok(false);
        }
    }
    Ok(true)
}
pub(crate) async fn detach(
    store: &CrudStore,
    workspace: &str,
    after: Option<&str>,
) -> Result<Option<String>> {
    if !store.frozen_history_reclamation_enabled() {
        return Ok(None);
    }
    let mut values: Vec<Value> = vec![workspace.into()];
    let lower = if let Some(after) = after {
        values.push(after.into());
        " AND o.id>?"
    } else {
        ""
    };
    let row=store.connection.query_one_raw(sql(format!("SELECT o.id FROM compaction_operation o JOIN compaction_context c ON c.owner=o.owner JOIN compaction_operation_projection x ON x.operation_id=o.id WHERE c.workspace_id=? AND o.status='completed' AND o.outcome='applied' AND o.frozen_publication_contract<>'assertion_compat' AND o.frozen_accounting_mode IN ('live_known','completed_inventory') AND o.frozen_inventory_state='known' AND o.frozen_proof_state='complete' AND x.storage_state='bound' {lower} ORDER BY o.id LIMIT 1"),values)).await?;
    let Some(row) = row else { return Ok(None) };
    let id: String = row.try_get("", "id")?;
    // Semantic corruption before pin acquisition must also advance discovery.
    // Database/lock failures remain retryable and never certify detach.
    let result = async {
        let op = operation::Entity::find_by_id(&id)
            .one(&store.connection)
            .await?
            .ok_or_else(|| anyhow::anyhow!("detach operation unavailable"))?;
        let origin = projection::Entity::find_by_id(&id)
            .one(&store.connection)
            .await?
            .ok_or_else(|| anyhow::anyhow!("detach origin unavailable"))?;
        let context = pioneer_entity::compaction_context::Entity::find_by_id(&op.owner)
            .one(&store.connection)
            .await?
            .ok_or_else(|| anyhow::anyhow!("detach context unavailable"))?;
        let state = runner::Entity::find_by_id(&id)
            .one(&store.connection)
            .await?;
        if let Some(state) = &state {
            let Ok(parsed) =
                serde_json::from_str::<pioneer_compaction::runner::RunnerState>(&state.state)
            else {
                return Ok(Some(id.clone()));
            };
            let pioneer_compaction::runner::RunnerPhase::Applied { checkpoint } = parsed.phase else {
                return Ok(Some(id.clone()));
            };
            ensure!(store.connection.query_one_raw(sql("SELECT id FROM compaction_checkpoint WHERE id=? AND operation_id=? AND status='applied'",[checkpoint.into(),id.clone().into()])).await?.is_some(),"detach runner Applied identity mismatch");
        }
        let Some(guard) = store.compaction_pin_operation_projection(&id).await? else {
            return Err(anyhow::anyhow!("detach real origin unavailable"));
        };
        let result=async {
            super::compaction_frozen_verify::verify(store,&guard).await?;
            let mut cursor=None;
            loop {
                let ids=super::compaction_checkpoint_proof::checkpoint_ids_page(store,&id,cursor.as_deref()).await?;
                if ids.is_empty(){break;}
                for cp in &ids {store.compaction_checkpoint_edges(cp).await?.ok_or_else(||anyhow::anyhow!("detach promised proof missing"))?;}
                cursor=ids.last().cloned();
            }
            store.run_serialized_write(||async {
                let tx=store.connection.begin().await?;
                ensure!(store.frozen_history_reclamation_enabled(),"reclamation gate closed");
                ensure!(operation::Entity::find_by_id(&id).one(&tx).await?.as_ref()==Some(&op) && projection::Entity::find_by_id(&id).one(&tx).await?.as_ref()==Some(&origin) && runner::Entity::find_by_id(&id).one(&tx).await?==state,"detach immutable boundary changed");
                guard.validate_in(&tx,true).await?;
                ensure!(op.status=="completed" && op.outcome.as_deref()==Some("applied") && op.frozen_publication_contract!="assertion_compat" && matches!(op.frozen_accounting_mode.as_str(),"live_known"|"completed_inventory") && op.frozen_inventory_state=="known" && op.frozen_proof_state=="complete" && origin.storage_state=="bound","detach eligibility changed");
                ensure!(tx.query_one_raw(sql("SELECT c.owner FROM compaction_context c JOIN thread t ON t.id=c.thread_id AND t.workspace_id=c.workspace_id JOIN workspace w ON w.id=c.workspace_id WHERE c.owner=? AND c.workspace_id=? AND c.thread_id=? AND c.format_version=?",[op.owner.clone().into(),context.workspace_id.clone().into(),context.thread_id.clone().into(),context.format_version.into()])).await?.is_some(),"detach context boundary changed");

                ensure!(op.frozen_checkpoint_count.is_some() && op.frozen_checkpoint_count==op.frozen_prepared_checkpoint_count,"detach accounting incomplete");
                let complete=tx.query_one_raw(sql("SELECT 1 WHERE (SELECT COUNT(*) FROM compaction_checkpoint WHERE operation_id=?1)=?2 AND NOT EXISTS(SELECT 1 FROM compaction_checkpoint p LEFT JOIN compaction_checkpoint_proof h ON h.checkpoint_id=p.id WHERE p.operation_id=?1 AND (p.frozen_accounting_state<>'prepared' OR h.checkpoint_id IS NULL OR h.state<>'prepared' OR h.operation_id<>p.operation_id OR h.owner<>p.owner OR h.checkpoint_identity_sha256<>p.identity_sha256 OR h.previous IS NOT p.previous OR h.projection_version<>p.projection_version OR h.format_version<>p.format_version))",[id.clone().into(),op.frozen_checkpoint_count.into()])).await?;
                ensure!(complete.is_some(),"detach full proof set changed");
                let changed=tx.execute_raw(sql("UPDATE compaction_operation_projection SET storage_state='proof_only' WHERE operation_id=? AND manifest_id=? AND identity_sha256=? AND imports_sha256=? AND import_count=? AND storage_state='bound'",[id.clone().into(),origin.manifest_id.clone().into(),origin.identity_sha256.clone().into(),origin.imports_sha256.clone().into(),origin.import_count.into()])).await?.rows_affected();
                ensure!(changed==1,"detach projection CAS changed");tx.commit().await?;Ok(Some(id.clone()))
            }).await
        }.await;
        guard.complete(result).await
    }.await;
    match result {
        Err(error) if !error.is::<sea_orm::DbErr>() && !crate::is_anyhow_sqlite_lock(&error) => {
            tracing::warn!(
                phase = "detach",
                outcome = "retained",
                "Frozen history reclaim deferred"
            );
            Ok(Some(id))
        }
        result => result,
    }
}

pub(crate) async fn logical(
    store: &CrudStore,
    workspace: &str,
    after: Option<&str>,
) -> Result<Option<String>> {
    if !store.frozen_history_reclamation_enabled() {
        return Ok(None);
    }
    let mut values: Vec<Value> = vec![workspace.into()];
    let lower = if let Some(after) = after {
        values.push(after.into());
        " AND id>?"
    } else {
        ""
    };
    let row=store.connection.query_one_raw(sql(format!("SELECT id FROM compaction_frozen_history WHERE workspace_id=? AND availability IN ('resident','releasing') {lower} ORDER BY id LIMIT 1"),values)).await?;
    let Some(row) = row else { return Ok(None) };
    let id: String = row.try_get("", "id")?;
    let h = history::Entity::find_by_id(&id)
        .one(&store.connection)
        .await?
        .ok_or_else(|| anyhow::anyhow!("reclaim header unavailable"))?;
    let result=store.run_serialized_write(||async {
        let tx=store.connection.begin().await?;
        ensure!(history::Entity::find_by_id(&id).one(&tx).await?.as_ref()==Some(&h),"logical generation/cursor changed");
        if !super::compaction_frozen_maintenance::workspace_ready(&tx,workspace).await? || !unrooted(&tx,&id).await? {tx.commit().await?;return Ok(Some(id.clone()));}
        ensure!(tx.query_one_raw(sql("SELECT t.id FROM thread t JOIN workspace w ON w.id=t.workspace_id WHERE t.id=? AND t.workspace_id=?",[h.owner_thread.clone().into(),workspace.into()])).await?.is_some(),"reclaim domain unavailable");
        if h.availability=="resident" {
            let changed=tx.execute_raw(sql("UPDATE compaction_frozen_history SET availability='releasing',ready=0,storage_generation=storage_generation+1,release_kind=0,release_after_span=NULL WHERE id=? AND availability='resident' AND storage_generation=?",[id.clone().into(),h.storage_generation.into()])).await?.rows_affected();ensure!(changed==1,"retirement CAS changed");
        } else if h.release_kind<2 {
            let mut values:Vec<Value>=vec![id.clone().into(),h.release_kind.into()];
            let lower=if let Some(after)=h.release_after_span{values.push(after.into());" AND start>?"}else{""};
            let rows=tx.query_all_raw(sql(format!("SELECT start,length(CAST(manifest_id AS BLOB))+length(CAST(source_manifest AS BLOB))+32 AS bytes FROM compaction_frozen_span WHERE manifest_id=? AND kind=? {lower} ORDER BY start LIMIT 128"),values)).await?;
            let mut starts=Vec::new();let mut bytes=0i64;
            for row in rows {let size:i64=row.try_get("","bytes")?;ensure!((0..=256*1024).contains(&size),"own span metadata oversized");if bytes+size>256*1024{break;}bytes+=size;starts.push(row.try_get::<i64>("","start")?);}
            for start in &starts {let changed=tx.execute_raw(sql("DELETE FROM compaction_frozen_span WHERE manifest_id=? AND kind=? AND start=?",[id.clone().into(),h.release_kind.into(),(*start).into()])).await?.rows_affected();ensure!(changed==1,"logical span disappeared");}
            if starts.is_empty() {
                // Matching EOF plus no earlier own spans proves completeness.
                ensure!(tx.query_one_raw(sql("SELECT 1 FROM compaction_frozen_span WHERE manifest_id=? AND kind=? LIMIT 1",[id.clone().into(),h.release_kind.into()])).await?.is_none(),"logical own mapping EOF mismatch");
                tx.execute_raw(sql("DELETE FROM compaction_frozen_layout WHERE manifest_id=? AND kind=?",[id.clone().into(),h.release_kind.into()])).await?;
            }
            let next=if starts.is_empty(){h.release_kind+1}else{h.release_kind};
            let cursor=if starts.is_empty(){None}else{starts.last().copied()};
            let changed=tx.execute_raw(sql("UPDATE compaction_frozen_history SET release_kind=?,release_after_span=? WHERE id=? AND availability='releasing' AND storage_generation=? AND release_kind=? AND release_after_span IS ?",[next.into(),cursor.into(),id.clone().into(),h.storage_generation.into(),h.release_kind.into(),h.release_after_span.into()])).await?.rows_affected();ensure!(changed==1,"logical cursor CAS changed");
        } else {
            ensure!(tx.query_one_raw(sql("SELECT 1 FROM compaction_frozen_span WHERE manifest_id=? LIMIT 1",[id.clone().into()])).await?.is_none() && tx.query_one_raw(sql("SELECT 1 FROM compaction_frozen_layout WHERE manifest_id=? LIMIT 1",[id.clone().into()])).await?.is_none(),"logical release mappings remain");
            let changed=tx.execute_raw(sql("UPDATE compaction_frozen_history SET availability='released' WHERE id=? AND availability='releasing' AND storage_generation=? AND release_kind=2 AND release_after_span IS ?",[id.clone().into(),h.storage_generation.into(),h.release_after_span.into()])).await?.rows_affected();ensure!(changed==1,"logical done CAS changed");
            dirty(&tx,&id,h.storage_generation).await?;
        }
        tx.commit().await?;Ok(Some(id.clone()))
    }).await;
    match result {
        Err(error) if !error.is::<sea_orm::DbErr>() && !crate::is_anyhow_sqlite_lock(&error) => {
            tracing::warn!(
                phase = "logical",
                outcome = "retained",
                "Frozen history reclaim deferred"
            );
            Ok(Some(id))
        }
        result => result,
    }
}
pub(crate) async fn dirty<C: ConnectionTrait>(db: &C, id: &str, generation: i64) -> Result<()> {
    for kind in [0i64, 1] {
        db.execute_raw(sql("INSERT INTO compaction_frozen_cleanup(manifest_id,kind,storage_generation) VALUES(?,?,?) ON CONFLICT(manifest_id,kind) DO UPDATE SET dirty_seq=dirty_seq+1,state=CASE WHEN state='quarantined' THEN state WHEN storage_generation<>excluded.storage_generation OR state='idle' THEN 'queued' ELSE state END,storage_generation=excluded.storage_generation",[id.into(),kind.into(),generation.into()])).await?;
    }
    Ok(())
}
pub(crate) async fn rebuild<C: ConnectionTrait>(store: &CrudStore, db: &C, id: &str) -> Result<()> {
    let h = history::Entity::find_by_id(id)
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("rebuild header unavailable"))?;
    if h.availability == "resident" {
        return Ok(());
    }
    ensure!(
        h.availability == "released" && h.release_kind == 2,
        "frozen history logical retirement requires progress"
    );
    ensure!(
        store.frozen_history_reclamation_enabled()
            && super::compaction_frozen_maintenance::workspace_ready(db, &h.workspace_id).await?
            && unrooted(db, id).await?,
        "released history rebuild roots/gate unavailable"
    );
    ensure!(
        db.query_one_raw(sql(
            "SELECT 1 FROM compaction_frozen_span WHERE manifest_id=? LIMIT 1",
            [id.into()]
        ))
        .await?
        .is_none()
            && db
                .query_one_raw(sql(
                    "SELECT 1 FROM compaction_frozen_layout WHERE manifest_id=? LIMIT 1",
                    [id.into()]
                ))
                .await?
                .is_none(),
        "released own mappings remain"
    );
    let changed=db.execute_raw(sql("UPDATE compaction_frozen_history SET availability='resident',storage_generation=storage_generation+1,ready=0,next_ordinal=0,next_import=0,storage_registered=0,release_kind=0,release_after_span=NULL WHERE id=? AND availability='released' AND storage_generation=? AND release_kind=2",[id.into(),h.storage_generation.into()])).await?.rows_affected();
    ensure!(changed == 1, "same-ID rebuild CAS changed");
    dirty(db, id, h.storage_generation + 1).await?;
    Ok(())
}
