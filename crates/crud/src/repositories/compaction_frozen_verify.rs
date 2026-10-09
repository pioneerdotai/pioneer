//! Capture-local full verification, independent of historical checkpoint proofs.
use super::compaction_frozen_use::FrozenUseGuard;
use crate::{
    CrudStore,
    compaction::{FrozenImportRecord, SOURCE_PAGE_BYTES, SOURCE_PAGE_ROWS},
};
use anyhow::Result;

#[derive(Debug)]
pub(crate) struct FrozenIntegrityError(pub(crate) &'static str);
impl std::fmt::Display for FrozenIntegrityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for FrozenIntegrityError {}
macro_rules! ensure {
    ($condition:expr,$message:literal $(,)?) => {
        if !$condition {
            return Err(FrozenIntegrityError($message).into());
        }
    };
}

use pioneer_compaction::frozen::FrozenMessageRef;
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};
use sha2::{Digest, Sha256};

pub(crate) struct VerifiedFrozenCapture {
    use_id: String,
    generation: i64,
}

/// Builder access is next-bounded separately from count-bounded logical views.
/// Every fetch validates the use/domain, including an empty or zero-count page.
pub(crate) async fn rows<C: ConnectionTrait>(
    db: &C,
    guard: &FrozenUseGuard,
    kind: i64,
    start: i64,
    ordinary: bool,
) -> Result<Vec<(i64, String)>> {
    ensure!(
        start >= 0 && matches!(kind, 0 | 1),
        "invalid frozen builder page"
    );
    guard.validate_in(db, ordinary).await?;
    let (table, next) = if kind == 0 {
        ("compaction_frozen_message", "next_ordinal")
    } else {
        ("compaction_frozen_import", "next_import")
    };
    let limit=db.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        format!("SELECT {next} AS n FROM compaction_frozen_history WHERE id=? AND storage_generation=? AND availability='resident'"),
        [guard.header().id.clone().into(),guard.header().storage_generation.into()])).await?
        .ok_or_else(||anyhow::anyhow!("frozen history is unavailable"))?.try_get::<i64>("","n")?;
    let sizes=db.query_all_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        format!("SELECT ordinal,bytes FROM {table} WHERE manifest_id=? AND ordinal>=? AND ordinal<? ORDER BY ordinal LIMIT ?"),
        [guard.header().id.clone().into(),start.into(),limit.into(),(SOURCE_PAGE_ROWS as i64).into()])).await?;
    let mut end = start;
    let mut bytes = 0_i64;
    for row in sizes {
        let ordinal = row.try_get::<i64>("", "ordinal")?;
        let size = row.try_get::<i64>("", "bytes")?;
        ensure!(
            (0..=SOURCE_PAGE_BYTES as i64).contains(&size),
            "invalid frozen row size"
        );
        if bytes + size > SOURCE_PAGE_BYTES as i64 {
            break;
        }
        ensure!(ordinal == end, "frozen logical ordinal gap");
        end += 1;
        bytes += size;
    }
    guard.validate_in(db, ordinary).await?;
    let fields = if kind == 0 {
        "ordinal,reference_json AS text,bytes"
    } else {
        "ordinal,proof_json AS text,bytes,message_ordinal,source_thread,source_scope,source_id,source_version"
    };
    let fetched=db.query_all_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        format!("SELECT {fields} FROM {table} WHERE manifest_id=? AND ordinal>=? AND ordinal<? ORDER BY ordinal"),
        [guard.header().id.clone().into(),start.into(),end.into()])).await?;
    let mut out = Vec::with_capacity(fetched.len());
    for row in fetched {
        let ordinal = row.try_get::<i64>("", "ordinal")?;
        let json = row.try_get::<String>("", "text")?;
        ensure!(
            ordinal == start + out.len() as i64
                && json.len() as i64 == row.try_get::<i64>("", "bytes")?,
            "frozen page changed or lost a row"
        );
        if kind == 1 {
            let r: FrozenImportRecord = serde_json::from_str(&json)?;
            ensure!(
                i64::try_from(r.message_ordinal)? == row.try_get::<i64>("", "message_ordinal")?
                    && r.source_thread == row.try_get::<String>("", "source_thread")?
                    && r.source.scope == row.try_get::<String>("", "source_scope")?
                    && r.source.id == row.try_get::<String>("", "source_id")?
                    && r.source.version == row.try_get::<String>("", "source_version")?,
                "frozen import source index mismatch"
            );
        }
        out.push((ordinal, json));
    }
    ensure!(
        out.len() as i64 == end - start,
        "frozen page lost an immutable row"
    );
    guard.validate_in(db, ordinary).await?;
    Ok(out)
}

pub(crate) async fn verify(
    store: &CrudStore,
    guard: &FrozenUseGuard,
) -> Result<VerifiedFrozenCapture> {
    let h = guard.header();
    for (kind, count, expected) in [
        (0, h.message_count, &h.identity_sha256),
        (1, h.import_count, &h.imports_sha256),
    ] {
        ensure!(count >= 0, "invalid frozen count");
        let mut after = 0_i64;
        let mut digest = Sha256::new();
        while after < count {
            let page = rows(&store.connection, guard, kind, after, false).await?;
            ensure!(!page.is_empty(), "frozen capture is incomplete");
            for (ordinal, json) in page {
                ensure!(
                    ordinal == after && after < count,
                    "frozen capture ordinal mismatch"
                );
                let bytes = if kind == 0 {
                    let r: FrozenMessageRef = serde_json::from_str(&json)?;
                    r.validate()
                        .map_err(|_| FrozenIntegrityError("invalid frozen reference"))?;
                    serde_json::to_vec(&r)?
                } else {
                    let r: FrozenImportRecord = serde_json::from_str(&json)?;
                    ensure!(
                        r.message_ordinal < u64::try_from(h.message_count)?,
                        "frozen import target out of range"
                    );
                    serde_json::to_vec(&r)?
                };
                // Original reference framing is BE; original import framing is LE.
                digest.update(if kind == 0 {
                    (bytes.len() as u64).to_be_bytes()
                } else {
                    (bytes.len() as u64).to_le_bytes()
                });
                digest.update(bytes);
                after += 1;
            }
        }
        ensure!(
            hex::encode(digest.finalize()) == *expected,
            "frozen capture digest mismatch"
        );
    }
    guard.validate_in(&store.connection, false).await?;
    Ok(VerifiedFrozenCapture {
        use_id: guard.token().into(),
        generation: h.storage_generation,
    })
}

pub(crate) async fn finish(
    store: &CrudStore,
    guard: &FrozenUseGuard,
    receipt: &VerifiedFrozenCapture,
) -> Result<bool> {
    ensure!(
        receipt.use_id == guard.token() && receipt.generation == guard.header().storage_generation,
        "frozen finish receipt mismatch"
    );
    store.run_serialized_write(||async {
        let tx=store.connection.begin().await?;guard.validate_in(&tx,false).await?;
        let updated=tx.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "UPDATE compaction_frozen_history SET ready=1 WHERE id=? AND storage_generation=? AND availability='resident' AND next_ordinal=message_count AND next_import=import_count",
            [guard.header().id.clone().into(),receipt.generation.into()])).await?.rows_affected()==1;
        tx.commit().await?;Ok(updated)
    }).await
}
