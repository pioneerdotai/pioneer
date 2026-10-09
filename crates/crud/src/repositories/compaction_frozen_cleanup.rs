//! Durable repeated physical passes. Only these checked static tables are deleted.
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_entity::{compaction_frozen_cleanup as job, compaction_frozen_history as history};
use sea_orm::{DbBackend, Statement, TransactionTrait, entity::prelude::*};
fn sql(s: impl Into<String>, v: impl IntoIterator<Item = Value>) -> Statement {
    Statement::from_sql_and_values(DbBackend::Sqlite, s, v)
}
struct Statements {
    max: &'static str,
    initial: &'static str,
    continuation: &'static str,
    eof_initial: &'static str,
    eof_continuation: &'static str,
    point: &'static str,
    delete: &'static str,
}
fn statements(kind: i64) -> Result<Statements> {
    // Table names and kind/count columns cannot come from a caller.
    match kind {
        0 => Ok(Statements {
            max: "SELECT ordinal FROM compaction_frozen_message_data WHERE manifest_id=? ORDER BY ordinal DESC LIMIT 1",
            initial: "SELECT ordinal,bytes FROM compaction_frozen_message_data WHERE manifest_id=? AND ordinal>=0 AND ordinal<=? ORDER BY ordinal LIMIT 128",
            continuation: "SELECT ordinal,bytes FROM compaction_frozen_message_data WHERE manifest_id=? AND ordinal>? AND ordinal<=? ORDER BY ordinal LIMIT 128",
            eof_initial: "SELECT 1 FROM compaction_frozen_message_data WHERE manifest_id=? AND ordinal>=0 AND ordinal<=? LIMIT 1",
            eof_continuation: "SELECT 1 FROM compaction_frozen_message_data WHERE manifest_id=? AND ordinal>? AND ordinal<=? LIMIT 1",
            point: "SELECT bytes FROM compaction_frozen_message_data WHERE manifest_id=? AND ordinal=?",
            delete: "DELETE FROM compaction_frozen_message_data WHERE manifest_id=?1 AND ordinal=?2 AND bytes=?3 AND NOT EXISTS(SELECT 1 FROM compaction_frozen_span WHERE source_manifest=?1 AND kind=0 AND start<=?2 AND end>?2) AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=?1 AND h.availability='resident' AND ?2>=0 AND ?2<h.message_count AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout WHERE manifest_id=?1 AND kind=0 AND active=1)) AND NOT EXISTS(SELECT 1 FROM compaction_frozen_use WHERE manifest_id=?1) AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout WHERE candidate=?1 AND active=0)",
        }),
        1 => Ok(Statements {
            max: "SELECT ordinal FROM compaction_frozen_import_data WHERE manifest_id=? ORDER BY ordinal DESC LIMIT 1",
            initial: "SELECT ordinal,bytes FROM compaction_frozen_import_data WHERE manifest_id=? AND ordinal>=0 AND ordinal<=? ORDER BY ordinal LIMIT 128",
            continuation: "SELECT ordinal,bytes FROM compaction_frozen_import_data WHERE manifest_id=? AND ordinal>? AND ordinal<=? ORDER BY ordinal LIMIT 128",
            eof_initial: "SELECT 1 FROM compaction_frozen_import_data WHERE manifest_id=? AND ordinal>=0 AND ordinal<=? LIMIT 1",
            eof_continuation: "SELECT 1 FROM compaction_frozen_import_data WHERE manifest_id=? AND ordinal>? AND ordinal<=? LIMIT 1",
            point: "SELECT bytes FROM compaction_frozen_import_data WHERE manifest_id=? AND ordinal=?",
            delete: "DELETE FROM compaction_frozen_import_data WHERE manifest_id=?1 AND ordinal=?2 AND bytes=?3 AND NOT EXISTS(SELECT 1 FROM compaction_frozen_span WHERE source_manifest=?1 AND kind=1 AND start<=?2 AND end>?2) AND NOT EXISTS(SELECT 1 FROM compaction_frozen_history h WHERE h.id=?1 AND h.availability='resident' AND ?2>=0 AND ?2<h.import_count AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout WHERE manifest_id=?1 AND kind=1 AND active=1)) AND NOT EXISTS(SELECT 1 FROM compaction_frozen_use WHERE manifest_id=?1) AND NOT EXISTS(SELECT 1 FROM compaction_frozen_layout WHERE candidate=?1 AND active=0)",
        }),
        _ => Err(anyhow::anyhow!("invalid physical cleanup kind")),
    }
}
async fn allowed<C: ConnectionTrait>(
    store: &CrudStore,
    db: &C,
    h: &history::Model,
) -> Result<bool> {
    if !store.frozen_history_physical_cleanup_enabled()
        || !matches!(
            h.availability.as_str(),
            "resident" | "releasing" | "released"
        )
        || !super::compaction_frozen_maintenance::workspace_ready(db, &h.workspace_id).await?
    {
        return Ok(false);
    }
    if db.query_one_raw(sql("SELECT t.id FROM thread t JOIN workspace w ON w.id=t.workspace_id WHERE t.id=? AND t.workspace_id=?",[h.owner_thread.clone().into(),h.workspace_id.clone().into()])).await?.is_none(){return Ok(false);}
    // Globally held jobs defer without a false EOF; per-row span holds advance.
    for statement in [
        "SELECT 1 FROM compaction_frozen_use WHERE manifest_id=? LIMIT 1",
        "SELECT 1 FROM compaction_frozen_layout WHERE candidate=? AND active=0 LIMIT 1",
    ] {
        if db
            .query_one_raw(sql(statement, [h.id.clone().into()]))
            .await?
            .is_some()
        {
            return Ok(false);
        }
    }
    Ok(true)
}
async fn matches<C: ConnectionTrait>(db: &C, h: &history::Model, j: &job::Model) -> Result<bool> {
    Ok(
        history::Entity::find_by_id(&h.id).one(db).await?.as_ref() == Some(h)
            && job::Entity::find_by_id((j.manifest_id.clone(), j.kind))
                .one(db)
                .await?
                .as_ref()
                == Some(j)
            && h.storage_generation == j.storage_generation,
    )
}
pub(crate) async fn quantum(store: &CrudStore) -> Result<bool> {
    if !store.frozen_history_physical_cleanup_enabled() {
        return Ok(false);
    }
    let after = store
        .frozen_maintenance_cursor
        .lock()
        .map_err(|_| anyhow::anyhow!("maintenance cursor poisoned"))?
        .cleanup_after
        .clone();
    let select = match after {
        None => sql(
            "SELECT manifest_id,kind FROM compaction_frozen_cleanup WHERE state IN ('queued','running') ORDER BY manifest_id,kind LIMIT 1",
            [],
        ),
        Some((id, kind)) => sql(
            "SELECT manifest_id,kind FROM compaction_frozen_cleanup WHERE state IN ('queued','running') AND (manifest_id,kind)>(?,?) ORDER BY manifest_id,kind LIMIT 1",
            [id.into(), kind.into()],
        ),
    };
    let row = store.connection.query_one_raw(select).await?;
    let row=match row {Some(row)=>Some(row),None=>store.connection.query_one_raw(sql("SELECT manifest_id,kind FROM compaction_frozen_cleanup WHERE state IN ('queued','running') ORDER BY manifest_id,kind LIMIT 1",[])).await?};
    let Some(row) = row else { return Ok(false) };
    let id: String = row.try_get("", "manifest_id")?;
    let kind: i64 = row.try_get("", "kind")?;
    store
        .frozen_maintenance_cursor
        .lock()
        .map_err(|_| anyhow::anyhow!("maintenance cursor poisoned"))?
        .cleanup_after = Some((id.clone(), kind));
    let Some(h) = history::Entity::find_by_id(&id)
        .one(&store.connection)
        .await?
    else {
        return Ok(false);
    };
    let Some(j) = job::Entity::find_by_id((id.clone(), kind))
        .one(&store.connection)
        .await?
    else {
        return Ok(false);
    };
    let statements = statements(kind)?;
    if !allowed(store, &store.connection, &h).await? {
        return Ok(false);
    }
    if j.state == "queued" {
        return store.run_serialized_write(||async {
            let tx=store.connection.begin().await?;
            if !matches(&tx,&h,&j).await? || !allowed(store,&tx,&h).await? {tx.commit().await?;return Ok(false);}
            let ceiling=tx.query_one_raw(sql(statements.max,[id.clone().into()])).await?.map(|r|r.try_get::<i64>("","ordinal")).transpose()?;
            if ceiling.is_some_and(|n|n<0) {tx.execute_raw(sql("UPDATE compaction_frozen_cleanup SET state='quarantined' WHERE manifest_id=? AND kind=? AND storage_generation=? AND state='queued'",[id.clone().into(),kind.into(),h.storage_generation.into()])).await?;tx.commit().await?;return Ok(true);}
            let changed=tx.execute_raw(sql("UPDATE compaction_frozen_cleanup SET state='running',pass_no=pass_no+1,pass_seq=dirty_seq,after_ordinal=NULL,ceiling_ordinal=? WHERE manifest_id=? AND kind=? AND state='queued' AND storage_generation=? AND pass_no=? AND after_ordinal IS ? AND ceiling_ordinal IS ?",[ceiling.unwrap_or(-1).into(),id.clone().into(),kind.into(),h.storage_generation.into(),j.pass_no.into(),j.after_ordinal.into(),j.ceiling_ordinal.into()])).await?.rows_affected();ensure!(changed==1,"physical pass start CAS changed");tx.commit().await?;Ok(true)
        }).await;
    }
    if j.state != "running" {
        return Ok(false);
    }
    let ceiling = j
        .ceiling_ordinal
        .ok_or_else(|| anyhow::anyhow!("running physical ceiling missing"))?;
    let select = match j.after_ordinal {
        None => sql(statements.initial, [id.clone().into(), ceiling.into()]),
        Some(after) => sql(
            statements.continuation,
            [id.clone().into(), after.into(), ceiling.into()],
        ),
    };
    let rows = store.connection.query_all_raw(select).await?;
    let mut keys = Vec::new();
    let mut bytes = 0i64;
    let mut poison = false;
    for row in rows {
        let ordinal: i64 = row.try_get("", "ordinal")?;
        let size: i64 = row.try_get("", "bytes")?;
        if ordinal < 0 || !(0..=256 * 1024).contains(&size) {
            poison = true;
            break;
        }
        if bytes + size > 256 * 1024 {
            break;
        }
        bytes += size;
        keys.push((ordinal, size));
    }
    #[cfg(any(test, feature = "test-support"))]
    super::compaction_runner::trigger_publication_test_hook(
        store,
        &id,
        super::compaction_runner::PublicationTestPause::PhysicalBeforeWriter,
    )
    .await;
    store.run_serialized_write(||async {
        let tx=store.connection.begin().await?;
        if !matches(&tx,&h,&j).await? || !allowed(store,&tx,&h).await? {tx.commit().await?;return Ok(false);}
        if poison {
            let changed=tx.execute_raw(sql("UPDATE compaction_frozen_cleanup SET state='quarantined' WHERE manifest_id=? AND kind=? AND state='running' AND storage_generation=? AND pass_no=? AND after_ordinal IS ? AND ceiling_ordinal IS ?",[id.clone().into(),kind.into(),h.storage_generation.into(),j.pass_no.into(),j.after_ordinal.into(),j.ceiling_ordinal.into()])).await?.rows_affected();ensure!(changed==1,"physical quarantine CAS changed");tx.commit().await?;return Ok(true);
        }
        for (ordinal,size) in &keys {
            let row=tx.query_one_raw(sql(statements.point,[id.clone().into(),(*ordinal).into()])).await?.ok_or_else(||anyhow::anyhow!("physical key changed"))?;
            ensure!(row.try_get::<i64>("","bytes")?==*size,"physical size changed");
            // Pending copies have actual reserved spans and a candidate/read
            // use. Implicit direct counts protect an unconverted target. No
            // additional pending flag hold requires an unmodelled dirty event.
            // All dependency predicates are part of this exact DELETE statement.
            tx.execute_raw(sql(statements.delete,[id.clone().into(),(*ordinal).into(),(*size).into()])).await?;
        }
        let state=if keys.is_empty() {
            let eof=match j.after_ordinal {None=>sql(statements.eof_initial,[id.clone().into(),ceiling.into()]),Some(after)=>sql(statements.eof_continuation,[id.clone().into(),after.into(),ceiling.into()])};
            ensure!(tx.query_one_raw(eof).await?.is_none(),"physical matching EOF changed");
            if j.dirty_seq==j.pass_seq.ok_or_else(||anyhow::anyhow!("physical pass sequence missing"))? {"idle"}else{"queued"}
        }else{"running"};
        let after=keys.last().map(|k|k.0).or(j.after_ordinal);
        let changed=tx.execute_raw(sql("UPDATE compaction_frozen_cleanup SET state=?,after_ordinal=? WHERE manifest_id=? AND kind=? AND state='running' AND storage_generation=? AND pass_no=? AND pass_seq IS ? AND dirty_seq=? AND after_ordinal IS ? AND ceiling_ordinal IS ?",[state.into(),after.into(),id.clone().into(),kind.into(),h.storage_generation.into(),j.pass_no.into(),j.pass_seq.into(),j.dirty_seq.into(),j.after_ordinal.into(),j.ceiling_ordinal.into()])).await?.rows_affected();ensure!(changed==1,"physical batch/cursor/EOF CAS changed");tx.commit().await?;Ok(true)
    }).await
}
