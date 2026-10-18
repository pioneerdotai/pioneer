//! Durable eligibility only. These tokens never authorize physical deletion.
//! All helpers inherit the maintenance handle and release capacity per quantum.
use super::frozen_storage_lifetime::{frozen_scope_query, physical_table};
use crate::CrudStore;
use anyhow::{Result, ensure};
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};

#[derive(Clone)]
pub(super) struct Work {
    pub manifest: String,
    pub kind: i64,
    pub revision: i64,
}

fn sql(text: impl Into<String>, values: impl IntoIterator<Item = sea_orm::Value>) -> Statement {
    Statement::from_sql_and_values(DbBackend::Sqlite, text, values)
}

/// Single group per quantum, with a persisted upper bound for each family.
/// Concurrent groups on either side of the cursor are covered by INSERT triggers.
pub(super) async fn bootstrap_step(store: &CrudStore) -> Result<bool> {
    let row = store.connection.query_one_raw(sql(
        "SELECT kind,started,upper_manifest,after_manifest FROM frozen_sweep_bootstrap WHERE complete=0 ORDER BY kind LIMIT 1", []
    )).await?;
    let Some(row) = row else { return Ok(false) };
    let kind: i64 = row.try_get("", "kind")?;
    let started: i64 = row.try_get("", "started")?;
    let upper: Option<String> = row.try_get("", "upper_manifest")?;
    let after: Option<String> = row.try_get("", "after_manifest")?;
    let (table, _) = physical_table(kind);
    // Bounded indexed seeks outside the writer. Cursor CAS below permits
    // duplicate discovery after cancellation, never a skipped enqueue.
    let next = if started == 0 {
        store
            .connection
            .query_one_raw(sql(
                format!("SELECT manifest_id FROM {table} ORDER BY manifest_id DESC LIMIT 1"),
                [],
            ))
            .await?
    } else {
        let mut query = format!("SELECT manifest_id FROM {table} WHERE manifest_id<=?1");
        let mut values = vec![upper.clone().into()];
        if let Some(after) = &after {
            query += " AND manifest_id>?2";
            values.push(after.clone().into());
        }
        query += " ORDER BY manifest_id LIMIT 1";
        store.connection.query_one_raw(sql(query, values)).await?
    };
    let next: Option<String> = next.map(|r| r.try_get("", "manifest_id")).transpose()?;
    // Find a witness with the original oracle on the reader. Reverse scope
    // discovery can inspect many endpoints; the writer must only recheck the
    // selected header/link row key, not repeat that discovery under capacity.
    let scope = if let Some(id) = next.as_deref().filter(|_| started != 0) {
        let witness = store
            .connection
            .query_one(&frozen_scope_query(id))
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("legacy frozen dependency scope unknown; cleanup waits for repair")
            })?;
        Some(scope_witness_guard(id, &witness)?)
    } else {
        None
    };
    store.run_serialized_write(|| async {
        let tx = store.connection.begin().await?;
        // Scope authority is the current witness, not enqueue or an old reader
        // result. Link/header validation and cursor/work commit are atomic.
        if let Some(scope) = &scope {
            ensure!(tx.query_one_raw(scope.clone()).await?.is_some(),
                "legacy frozen dependency scope unknown; cleanup waits for repair");
        }
        let changed = tx.execute_raw(sql(
            "UPDATE frozen_sweep_bootstrap SET started=1,upper_manifest=CASE WHEN started=0 THEN ?4 ELSE upper_manifest END,after_manifest=CASE WHEN started=0 THEN NULL ELSE ?4 END,complete=CASE WHEN ?4 IS NULL THEN 1 ELSE 0 END WHERE kind=?1 AND complete=0 AND started=?2 AND after_manifest IS ?3",
            [kind.into(), started.into(), after.clone().into(), next.clone().into()]
        )).await?.rows_affected();
        if changed == 1 && started != 0 {
            if let Some(id) = &next {
                tx.execute_raw(sql("INSERT INTO frozen_sweep_work(manifest_id,kind) VALUES(?1,?2) ON CONFLICT(manifest_id,kind) DO NOTHING", [id.clone().into(), kind.into()])).await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }).await?;
    Ok(true)
}

fn scope_witness_guard(id: &str, row: &sea_orm::QueryResult) -> Result<Statement> {
    let via: i64 = row.try_get("", "scope_via")?;
    if via == 0 {
        return Ok(sql(
            "SELECT 1 FROM compaction_frozen_history WHERE id=?1 AND workspace_id<>''",
            [id.into()],
        ));
    }
    let witness: i64 = row.try_get("", "scope_row")?;
    let (table, header, predicate) = match via {
        1 => ("compaction_frozen_layout", "manifest_id", "candidate"),
        2 => ("compaction_frozen_layout", "candidate", "manifest_id"),
        3 => ("compaction_frozen_span", "manifest_id", "source_manifest"),
        4 => ("compaction_frozen_span", "source_manifest", "manifest_id"),
        _ => anyhow::bail!("invalid frozen scope witness"),
    };
    // rowid only locates a witness; current link/header predicates authorize
    // scope. Reuse cannot forge known scope, and malformed legacy kind/start
    // values do not introduce a stricter prerequisite than the original oracle.
    Ok(sql(
        format!(
            "SELECT 1 FROM {table} s JOIN compaction_frozen_history h ON h.id=s.{header} WHERE s.rowid=?2 AND s.{predicate}=?1 AND h.workspace_id<>''"
        ),
        [id.into(), witness.into()],
    ))
}

/// A consumer may reference arbitrarily many backing groups. The covering
/// index seeks past the previous source, coalescing repeated spans per source.
pub(super) async fn fanout_step(store: &CrudStore) -> Result<bool> {
    // Consumer tokens are permanent rows: INSERT/UPSERT never rekeys/deletes
    // them. Their explicit INTEGER PRIMARY KEY rowids supply a stable service
    // order (also across VACUUM), not work revisions.
    // Freeze an insertion frontier per round so new consumers cannot extend a
    // round indefinitely. pending index includes rowid as its final SQLite key.
    let cursor = store
        .connection
        .query_one_raw(sql(
            "SELECT revision,after_row,until_row FROM frozen_sweep_fanout WHERE id=1",
            [],
        ))
        .await?
        .ok_or_else(|| anyhow::anyhow!("fanout cursor missing"))?;
    let turn: i64 = cursor.try_get("", "revision")?;
    let after_row: i64 = cursor.try_get("", "after_row")?;
    let mut until_row: i64 = cursor.try_get("", "until_row")?;
    let mut row = store.connection.query_one_raw(sql(
        "SELECT rowid AS consumer_row,manifest_id,kind,revision,after_source FROM frozen_sweep_consumer WHERE pending=1 AND rowid>?1 AND rowid<=?2 ORDER BY rowid LIMIT 1",
        [after_row.into(),until_row.into()]
    )).await?;
    if row.is_none() {
        until_row = store
            .connection
            .query_one_raw(sql(
                "SELECT COALESCE(MAX(rowid),0) AS until_row FROM frozen_sweep_consumer",
                [],
            ))
            .await?
            .ok_or_else(|| anyhow::anyhow!("consumer frontier missing"))?
            .try_get("", "until_row")?;
        row = store.connection.query_one_raw(sql(
            "SELECT rowid AS consumer_row,manifest_id,kind,revision,after_source FROM frozen_sweep_consumer WHERE pending=1 AND rowid<=?1 ORDER BY rowid LIMIT 1",[until_row.into()]
        )).await?;
    }
    let Some(row) = row else { return Ok(false) };
    let consumer_row: i64 = row.try_get("", "consumer_row")?;
    let id: String = row.try_get("", "manifest_id")?;
    let kind: i64 = row.try_get("", "kind")?;
    let revision: i64 = row.try_get("", "revision")?;
    let after: Option<String> = row.try_get("", "after_source")?;
    let mut query =
        "SELECT source_manifest FROM compaction_frozen_span WHERE manifest_id=?1 AND kind=?2"
            .to_owned();
    let mut values = vec![id.clone().into(), kind.into()];
    if let Some(after) = &after {
        query += " AND source_manifest>?3";
        values.push(after.clone().into());
    }
    query += " ORDER BY source_manifest LIMIT 1";
    let next = store.connection.query_one_raw(sql(query, values)).await?;
    let next: Option<String> = next.map(|r| r.try_get("", "source_manifest")).transpose()?;
    store.run_serialized_write(|| async {
        let tx = store.connection.begin().await?;
        // Rotation is durable and advances even if the consumer was reset while
        // waiting. A hot low ID cannot pin later consumers or the other kind.
        let rotated = tx.execute_raw(sql(
            "UPDATE frozen_sweep_fanout SET revision=revision+1,after_row=?2,until_row=?3 WHERE id=1 AND revision=?1",
            [turn.into(),consumer_row.into(),until_row.into()]
        )).await?.rows_affected();
        if rotated == 0 { tx.commit().await?; return Ok(()) }
        let changed = tx.execute_raw(sql(
            "UPDATE frozen_sweep_consumer SET after_source=?4,pending=CASE WHEN ?4 IS NULL THEN 0 ELSE 1 END WHERE manifest_id=?1 AND kind=?2 AND revision=?3 AND pending=1 AND after_source IS ?5",
            [id.clone().into(), kind.into(), revision.into(), next.clone().into(), after.clone().into()]
        )).await?.rows_affected();
        if changed == 1 {
            if let Some(source) = &next {
                tx.execute_raw(sql("INSERT INTO frozen_sweep_work(manifest_id,kind) VALUES(?1,?2) ON CONFLICT(manifest_id,kind) DO UPDATE SET revision=revision+1,pending=1,retry_at=0", [source.clone().into(), kind.into()])).await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }).await?;
    Ok(true)
}

/// Advisory service order only: losing it cannot acknowledge durable work.
#[derive(Default)]
pub(super) struct ServiceRound {
    pub(super) after_row: i64,
    pub(super) until_row: i64,
}

pub(super) enum Admission {
    Selected(Work),
    Continue,
    Idle,
}

pub(super) async fn discover(
    store: &CrudStore,
    active: &[(&str, i64)],
    kind: i64,
    round: &mut ServiceRound,
) -> Result<Admission> {
    let mut values: Vec<sea_orm::Value> = Vec::with_capacity(active.len() * 2 + 1);
    values.push(kind.into());
    let mut due =
        "SELECT 1 FROM frozen_sweep_work INDEXED BY frozen_sweep_pending WHERE pending=1 AND kind=?1 AND retry_at<=unixepoch()"
            .to_owned();
    for (id, kind) in active {
        let index = values.len();
        due += &format!(
            " AND NOT (manifest_id=?{} AND kind=?{})",
            index + 1,
            index + 2
        );
        values.extend([(*id).into(), (*kind).into()]);
    }
    due += " LIMIT 1";
    // The due index avoids walking deferred work every idle tick. Excluding
    // the at most four active keys also prevents a false continuation loop.
    if store
        .connection
        .query_one_raw(sql(due, values))
        .await?
        .is_none()
    {
        return Ok(Admission::Idle);
    }
    if round.after_row >= round.until_row {
        round.until_row = store
            .connection
            .query_one_raw(sql(
                "SELECT COALESCE(MAX(work_row),0) AS until_row FROM frozen_sweep_work",
                [],
            ))
            .await?
            .ok_or_else(|| anyhow::anyhow!("work frontier missing"))?
            .try_get("", "until_row")?;
        round.after_row = 0;
    }
    // Stable insertion order, not mutable retry_at or TEXT order. New rows
    // cannot enter this round; re-dirty/retry cannot move a visited row ahead.
    // Visiting future retries is bounded too, with no geometry or writer work.
    let rows = store.connection.query_all_raw(sql(
        "SELECT work_row,manifest_id,kind,revision,retry_at<=unixepoch() AS due FROM frozen_sweep_work INDEXED BY frozen_sweep_service WHERE pending=1 AND kind=?1 AND work_row>?2 AND work_row<=?3 ORDER BY work_row LIMIT 16",
        [kind.into(),round.after_row.into(),round.until_row.into()]
    )).await?;
    if rows.is_empty() {
        round.after_row = round.until_row;
    }
    for row in rows {
        round.after_row = row.try_get("", "work_row")?;
        let id: String = row.try_get("", "manifest_id")?;
        if row.try_get::<i64>("", "due")? == 0 || active.contains(&(id.as_str(), kind)) {
            continue;
        }
        return Ok(Admission::Selected(Work {
            manifest: id,
            kind,
            revision: row.try_get("", "revision")?,
        }));
    }
    // Due work can be behind the cursor or beyond this round's frontier.
    // Keep the continuation, wrapping on the next admission of this kind.
    Ok(Admission::Continue)
}

impl Work {
    /// Fanout is an immediate barrier: a consumer mutation invalidates even a
    /// prepared key whose own generation has not yet been expanded.
    /// Own and current reverse consumers only, through indexed dependencies.
    /// A disconnected OLD source was dirtied directly by the span trigger;
    /// losing the current link therefore cannot evade the revision predicate.
    pub(super) fn fence(&self) -> Statement {
        sql(
            "SELECT 1 FROM frozen_sweep_work WHERE manifest_id=?1 AND kind=?2 AND revision=?3 AND pending=1 AND NOT EXISTS(SELECT 1 FROM frozen_sweep_consumer WHERE manifest_id=?1 AND kind=?2 AND pending=1) AND NOT EXISTS(SELECT 1 FROM compaction_frozen_span s WHERE s.source_manifest=?1 AND s.kind=?2 AND EXISTS(SELECT 1 FROM frozen_sweep_consumer c WHERE c.manifest_id=s.manifest_id AND c.kind=s.kind AND c.pending=1))",
            [
                self.manifest.clone().into(),
                self.kind.into(),
                self.revision.into(),
            ],
        )
    }

    /// Completion validates geometry again in this same short writer boundary.
    /// Refusal persists a delayed retry; CAS never clears newer domain work.
    /// Wall time schedules eligibility only, and is never a revision token.
    pub(super) async fn finish(
        &self,
        store: &CrudStore,
        validation: Option<Statement>,
    ) -> Result<()> {
        store.run_serialized_write(|| async {
            let tx = store.connection.begin().await?;
            let valid = if let Some(guard) = &validation {
                tx.query_one_raw(self.fence()).await?.is_some() && tx.query_one_raw(guard.clone()).await?.is_some()
            } else { false };
            tx.execute_raw(sql(
                "UPDATE frozen_sweep_work SET pending=CASE WHEN ?4 THEN 0 ELSE 1 END,retry_at=CASE WHEN ?4 THEN 0 ELSE unixepoch()+60 END WHERE manifest_id=?1 AND kind=?2 AND revision=?3 AND pending=1",
                [self.manifest.clone().into(), self.kind.into(), self.revision.into(), valid.into()]
            )).await?;
            tx.commit().await?;
            Ok(())
        }).await
    }
}
