//! Immutable shared ranges. A range addresses physical rows directly: reads
//! never traverse parent snapshots. Publication preserves the logical views.
use crate::frozen_geometry::{PhysicalDeleteResult, duplicate_cleanup_statements};
use crate::frozen_views::{import, message};
use anyhow::{Result, ensure};
use pioneer_sqlite::SqliteDatabase;

use crate::frozen_copy::{
    CopyRevisions, StagedCopyCheck, checked_copy_dependencies, lock_copy_headers, span_revision,
};
use pioneer_entity::{
    compaction_frozen_history as history, compaction_frozen_import_data as import_data,
    compaction_frozen_layout as layout, compaction_frozen_message_data as message_data,
    compaction_frozen_span as span,
};
use sea_orm::sea_query::{Expr, ExprTrait, OnConflict};
use sea_orm::{ActiveValue::Set, QueryOrder, QuerySelect, TransactionTrait, entity::prelude::*};
const ROWS: u64 = 128;
const BYTES: i64 = 256 * 1024;

// Sizes first, then a bounded payload query. Parsing/comparison happens after
// each query has released its reader. Logical ordinals are physical ordinals.
pub async fn rows<C: ConnectionTrait>(
    db: &C,
    id: &str,
    kind: i64,
    start: i64,
) -> Result<Vec<(i64, String)>> {
    let h = history::Entity::find_by_id(id)
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("frozen comparison input missing"))?;
    let bounds = crate::frozen::logical_bounds(&h)?;
    let bound = if kind == 0 { bounds.0 } else { bounds.1 };
    macro_rules! read {
        ($e:ident, $field:ident, $json:literal) => {{
            let q = $e::Entity::find().filter($e::Column::ManifestId.eq(id)).filter($e::Column::Ordinal.gte(start));
            let sizes = q.clone().select_only().column($e::Column::Ordinal).column($e::Column::Bytes)
                .order_by_asc($e::Column::Ordinal).limit(ROWS).into_tuple::<(i64,i64)>().all(db).await?;
            ensure!(start>=bound || !sizes.is_empty(), "frozen comparison page missing before declared bound");
            let mut end = start; let mut bytes = 0;
            for (ordinal, size) in sizes {
                ensure!((0..=BYTES).contains(&size), "invalid frozen row size");
                if bytes + size > BYTES { break; }
                ensure!(ordinal == end, "frozen range has an ordinal gap");
                end += 1; bytes += size;
            }
            let loaded = q.select_only().column($e::Column::Ordinal).column($e::Column::$field)
                .filter($e::Column::Ordinal.lt(end))
                .filter(Expr::cust(format!("octet_length({})::bigint=bytes AND bytes BETWEEN 0 AND {}", $json, BYTES)))
                .order_by_asc($e::Column::Ordinal)
                .into_tuple::<(i64,String)>().all(db).await?;
            ensure!(loaded.len() == usize::try_from(end-start)?, "frozen range readback incomplete");
            ensure!(loaded.iter().map(|(_, json)| json.len()).sum::<usize>() <= usize::try_from(bytes)?, "frozen range byte size mismatch");
            loaded
        }};
    }
    Ok(if kind == 0 {
        read!(message, ReferenceJson, "reference_json")
    } else {
        read!(import, ProofJson, "proof_json")
    })
}

async fn candidate<C: ConnectionTrait>(
    db: &C,
    h: &history::Model,
    kind: i64,
) -> Result<Option<String>> {
    let ready_layout = layout::Entity::find()
        .select_only()
        .column(layout::Column::ManifestId)
        .filter(layout::Column::Kind.eq(kind))
        .filter(layout::Column::Active.eq(1_i64))
        .filter(layout::Column::Failed.eq(0))
        .filter(layout::Column::Pending.eq(0))
        .filter(
            Expr::col((layout::Entity, layout::Column::ManifestId))
                .eq(Expr::col((history::Entity, history::Column::Id))),
        );
    use sea_orm::QueryTrait;
    Ok(history::Entity::find()
        .filter(history::Column::WorkspaceId.eq(&h.workspace_id))
        .filter(history::Column::OwnerThread.eq(&h.owner_thread))
        .filter(history::Column::Ready.eq(1_i64))
        .filter(history::Column::Expired.eq(0_i64))
        .filter(
            Expr::col((history::Entity, history::Column::NextOrdinal))
                .eq(Expr::col((history::Entity, history::Column::MessageCount))),
        )
        .filter(
            Expr::col((history::Entity, history::Column::NextImport))
                .eq(Expr::col((history::Entity, history::Column::ImportCount))),
        )
        .filter(history::Column::Id.ne(&h.id))
        .filter(Expr::exists(ready_layout.into_query()))
        .order_by_desc(if kind == 0 {
            history::Column::MessageCount
        } else {
            history::Column::ImportCount
        })
        .order_by_asc(history::Column::Id)
        .select_only()
        .column(history::Column::Id)
        .into_tuple::<String>()
        .one(db)
        .await?)
}

pub async fn equivalent(
    db: &SqliteDatabase,
    workspace: &str,
    owner: &str,
    descriptor: &pioneer_compaction::frozen::FrozenHistoryRef,
    imports: u64,
    digest: &str,
) -> Result<Option<pioneer_compaction::frozen::FrozenHistoryRef>> {
    let found = history::Entity::find()
        .filter(history::Column::WorkspaceId.eq(workspace))
        .filter(history::Column::OwnerThread.eq(owner))
        .filter(history::Column::Ready.eq(1_i64))
        .filter(history::Column::Expired.eq(0_i64))
        .filter(
            Expr::col((history::Entity, history::Column::NextOrdinal))
                .eq(Expr::col((history::Entity, history::Column::MessageCount))),
        )
        .filter(
            Expr::col((history::Entity, history::Column::NextImport))
                .eq(Expr::col((history::Entity, history::Column::ImportCount))),
        )
        .filter(history::Column::IdentitySha256.eq(&descriptor.identity_sha256))
        .filter(history::Column::MessageCount.eq(i64::try_from(descriptor.messages)?))
        .filter(history::Column::ImportsSha256.eq(digest))
        .filter(history::Column::ImportCount.eq(i64::try_from(imports)?))
        .order_by_asc(history::Column::Id)
        .one(db)
        .await?;
    Ok(found.map(|h| pioneer_compaction::frozen::FrozenHistoryRef {
        manifest_id: h.id,
        ..descriptor.clone()
    }))
}

// Register metadata only. Ready manifests and candidate rows are immutable;
// candidate scope is revalidated when publishing the prepared ranges.
async fn register(
    db: &SqliteDatabase,
    h: &history::Model,
    kind: i64,
    base: Option<String>,
) -> Result<layout::Model> {
    let tx = db.begin().await?;
    let current = history::Entity::find_by_id(&h.id)
        .one(&tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("capture removed"))?;
    crate::frozen::logical_bounds(&current)?;
    ensure!(
        current.identity_sha256 == h.identity_sha256 && current.imports_sha256 == h.imports_sha256,
        "capture changed"
    );
    if let Some(base) = &base {
        let candidate = history::Entity::find_by_id(base)
            .one(&tx)
            .await?
            .ok_or_else(|| anyhow::anyhow!("base missing"))?;
        crate::frozen::logical_bounds(&candidate)?;
        ensure!(
            candidate.ready == 1
                && candidate.workspace_id == h.workspace_id
                && candidate.owner_thread == h.owner_thread,
            "base scope unavailable"
        );
        let layout = crate::frozen::find_layout(base.clone(), kind)
            .one(&tx)
            .await?
            .ok_or_else(|| anyhow::anyhow!("base layout missing"))?;
        ensure!(
            layout.active == 1 && layout.failed == 0 && layout.pending == 0,
            "base layout unavailable"
        );
    }
    layout::Entity::insert(layout::ActiveModel {
        manifest_id: Set(h.id.clone()),
        kind: Set(kind),
        active: Set(0),
        pending: Set(1),
        candidate: Set(base),
        compared: Set(0),
        copy_to: Set(None),
        copy_next: Set(0),
        cleanup_to: Set(0),
        cleanup_next: Set(0),
        failed: Set(0),
        ..Default::default()
    })
    .on_conflict(
        OnConflict::columns([layout::Column::ManifestId, layout::Column::Kind])
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(&tx)
    .await?;
    let state = crate::frozen::find_layout(h.id.clone(), kind)
        .one(&tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("capture layout removed during registration"))?;
    tx.commit().await?;
    Ok(state)
}

// Import proofs bind a message ordinal as well as a source. Reuse requires
// the exact target reference, not only identical proof JSON.
async fn same_import_target<C: ConnectionTrait>(
    db: &C,
    a: &str,
    b: &str,
    json: &str,
) -> Result<bool> {
    let proof: crate::frozen_import::FrozenImportRecord = serde_json::from_str(json)?;
    let ordinal = i64::try_from(proof.message_ordinal)?;
    let mut values = Vec::new();
    for id in [a, b] {
        let text = message::Entity::find_by_id((id.to_owned(), ordinal))
            .filter(message::Column::Bytes.lte(BYTES))
            .select_only()
            .column(message::Column::ReferenceJson)
            .filter(Expr::cust(
                "octet_length(reference_json)::bigint=bytes AND bytes>=0",
            ))
            .into_tuple::<String>()
            .one(db)
            .await?;
        values.push(text.ok_or_else(|| anyhow::anyhow!("import target reference missing"))?);
    }
    Ok(values[0] == values[1])
}

/// New captures supply their already-prepared references. Only compare against
/// a ready range layout; an unconverted old manifest is never borrowed from.
pub async fn prepare(
    db: &SqliteDatabase,
    workspace: &str,
    owner: &str,
    id: &str,
    kind: i64,
    entries: &[String],
) -> Result<u64> {
    let h = history::Entity::find_by_id(id)
        .filter(history::Column::WorkspaceId.eq(workspace))
        .filter(history::Column::OwnerThread.eq(owner))
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("frozen capture scope missing"))?;
    crate::frozen::logical_bounds(&h)?;
    if h.ready != 0 {
        return Ok(u64::try_from(if kind == 0 {
            h.message_count
        } else {
            h.import_count
        })?);
    }
    let l = register(db, &h, kind, candidate(db, &h, kind).await?).await?;
    if l.active != 0 {
        return Ok(u64::try_from(if kind == 0 {
            h.next_ordinal
        } else {
            h.next_import
        })?);
    }
    let mut matched = 0_usize;
    if let Some(base) = &l.candidate {
        'pages: while matched < entries.len() {
            let page = rows(db, base, kind, matched as i64).await?;
            if page.is_empty() {
                break;
            }
            for (_, text) in page {
                if entries.get(matched) != Some(&text) {
                    break 'pages;
                }
                if kind == 1 {
                    let proof: crate::frozen_import::FrozenImportRecord =
                        serde_json::from_str(&text)?;
                    // The message tail is appended after prefix preparation.
                    // Imports targeting it must follow the normal append path.
                    if i64::try_from(proof.message_ordinal)? >= h.next_ordinal
                        || !same_import_target(db, id, base, &text).await?
                    {
                        break 'pages;
                    }
                }
                matched += 1;
                if matched == entries.len() {
                    break 'pages;
                }
            }
        }
    }
    let mut state = l;
    loop {
        let verified = if state.copy_to.is_some_and(|end| end == state.copy_next) {
            let mut check = StagedCopyCheck::new(h.clone(), state.clone())?;
            while !check.step(db).await? {}
            check.revisions
        } else {
            None
        };
        if publish(db, &h, &state, matched as i64, verified).await? {
            break;
        }
        state = crate::frozen::find_layout(id.to_owned(), kind)
            .one(db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("capture layout removed during preparation"))?;
    }
    Ok(matched as u64)
}

/// Flatten a prefix to physical ranges. Incomplete range copies are invisible;
/// each batch is restart-safe. A constant-size transaction publishes the layout.
async fn publish(
    db: &SqliteDatabase,
    h: &history::Model,
    l: &layout::Model,
    prefix: i64,
    verified: Option<CopyRevisions>,
) -> Result<bool> {
    let changes = [
        db.work_signal("maintenance.portable").changing(),
        db.work_signal("maintenance.html").changing(),
    ];
    let frozen_change = db.work_signal("maintenance.frozen").changing();
    let mut published = false;
    let result = async {
        let prefix = l.copy_to.unwrap_or(prefix);
        if l.copy_to.is_none() {
            layout::Entity::update_many()
                .col_expr(layout::Column::CopyTo, Expr::val(prefix))
                .filter(layout::Column::ManifestId.eq(&h.id))
                .filter(layout::Column::Kind.eq(l.kind))
                .filter(layout::Column::CopyTo.is_null())
                .exec(db)
                .await?;
        }
        let mut next = l.copy_next;
        if let Some(base) = &l.candidate {
            if next < prefix {
                let spans = span::Entity::find()
                    .filter(span::Column::ManifestId.eq(base))
                    .filter(span::Column::Kind.eq(l.kind))
                    .filter(span::Column::Start.gte(next))
                    .filter(span::Column::Start.lt(prefix))
                    .order_by_asc(span::Column::Start)
                    .limit(ROWS)
                    .all(db)
                    .await?;
                ensure!(!spans.is_empty(), "shared prefix range missing");
                let mut prepared = Vec::new();
                for s in spans {
                    ensure!(
                        s.start == next && s.end > s.start,
                        "shared prefix range gap"
                    );
                    let end = std::cmp::min(s.end, prefix);
                    prepared.push((next, end, s.source_manifest));
                    next = end;
                }
                let tx = db.begin().await?;
                lock_copy_headers(&tx, h, l).await?;
                checked_copy_dependencies(&tx, h, l).await?;
                let state = crate::frozen::find_layout(h.id.clone(), l.kind)
                    .one(&tx)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("copy state removed"))?;
                ensure!(
                    state.active == 0
                        && state.copy_next == l.copy_next
                        && state.copy_to == Some(prefix)
                        && state.candidate == l.candidate,
                    "copy progress changed"
                );
                for (start, end, source) in prepared {
                    exact_span_insert(&tx, &h.id, l.kind, start, end, &source).await?;
                }
                layout::Entity::update_many()
                    .col_expr(layout::Column::CopyNext, Expr::val(next))
                    .filter(layout::Column::ManifestId.eq(&h.id))
                    .filter(layout::Column::Kind.eq(l.kind))
                    .filter(layout::Column::CopyNext.eq(l.copy_next))
                    .exec(&tx)
                    .await?;
                tx.commit().await?;
                if next < prefix {
                    return Ok(false);
                }
            }
        }
        let Some(verified) = verified else {
            return Ok(false);
        };
        let count = if l.kind == 0 {
            h.message_count
        } else {
            h.import_count
        };
        ensure!(prefix <= count, "shared prefix exceeds capture");
        let tx = db.begin().await?;
        lock_copy_headers(&tx, h, l).await?;
        let current = history::Entity::find_by_id(&h.id)
            .one(&tx)
            .await?
            .ok_or_else(|| anyhow::anyhow!("capture removed"))?;
        let state = crate::frozen::find_layout(h.id.clone(), l.kind)
            .one(&tx)
            .await?
            .ok_or_else(|| anyhow::anyhow!("capture layout removed"))?;
        crate::frozen::logical_bounds(&current)?;
        if state.active != 0 {
            tx.commit().await?;
            return Ok(true);
        }
        if CopyRevisions::read(&tx, h, l).await? != verified {
            tx.rollback().await?;
            return Ok(false);
        }
        ensure!(
            current.workspace_id == h.workspace_id
                && current.owner_thread == h.owner_thread
                && current.identity_sha256 == h.identity_sha256
                && current.imports_sha256 == h.imports_sha256
                && current.message_count == h.message_count
                && current.import_count == h.import_count
                && current.ready == h.ready,
            "capture changed before range publication"
        );
        if let Some(base) = &l.candidate {
            ensure!(
                history::Entity::find_by_id(base)
                    .filter(history::Column::WorkspaceId.eq(&h.workspace_id))
                    .filter(history::Column::OwnerThread.eq(&h.owner_thread))
                    .filter(history::Column::Ready.eq(1_i64))
                    .filter(history::Column::Expired.eq(0_i64))
                    .filter(
                        Expr::col((history::Entity, history::Column::NextOrdinal))
                            .eq(Expr::col((history::Entity, history::Column::MessageCount)))
                    )
                    .filter(
                        Expr::col((history::Entity, history::Column::NextImport))
                            .eq(Expr::col((history::Entity, history::Column::ImportCount)))
                    )
                    .one(&tx)
                    .await?
                    .is_some(),
                "shared prefix scope changed"
            );
        }
        ensure!(
            state.candidate == l.candidate
                && state.copy_to == Some(prefix)
                && state.copy_next == prefix
                && state.failed == 0
                && state.pending == 1,
            "range activation progress changed"
        );
        if h.ready != 0 && prefix < count {
            exact_span_insert(&tx, &h.id, l.kind, prefix, count, &h.id).await?;
        }
        let revision = span_revision(&tx, &h.id, l.kind).await?;
        layout::Entity::update_many()
            .col_expr(layout::Column::Active, Expr::val(1))
            .col_expr(layout::Column::VerifiedSpanRevision, Expr::val(revision))
            .col_expr(
                layout::Column::Pending,
                Expr::val(i64::from(h.ready != 0 && prefix > 0)),
            )
            .col_expr(layout::Column::Compared, Expr::val(prefix))
            .col_expr(
                layout::Column::CleanupTo,
                Expr::val(if h.ready != 0 { prefix } else { 0 }),
            )
            .filter(layout::Column::ManifestId.eq(&h.id))
            .filter(layout::Column::Kind.eq(l.kind))
            .filter(layout::Column::Active.eq(0_i64))
            .exec(&tx)
            .await?;
        if h.ready == 0 {
            history::Entity::update_many()
                .col_expr(
                    if l.kind == 0 {
                        history::Column::NextOrdinal
                    } else {
                        history::Column::NextImport
                    },
                    Expr::val(prefix),
                )
                .filter(history::Column::Id.eq(&h.id))
                .exec(&tx)
                .await?;
        }
        tx.commit().await?;
        published = true;
        Ok(true)
    }
    .await;
    if result.is_ok() {
        // Only activation without remaining cleanup releases lifetime protection.
        let keys = Some(
            std::iter::once(h.id.clone())
                .chain(l.candidate.iter().cloned())
                .map(|id| format!("m:{id}"))
                .collect::<Vec<_>>(),
        );
        crate::frozen_lifetime::complete_keys(
            frozen_change,
            &keys,
            published && (h.ready == 0 || l.copy_to.unwrap_or(prefix) == 0),
        );
        for change in changes {
            change.completed(published);
        }
    }
    result
}

async fn exact_span_insert<C: ConnectionTrait>(
    db: &C,
    id: &str,
    kind: i64,
    start: i64,
    end: i64,
    source: &str,
) -> Result<()> {
    ensure!(
        start >= 0 && end > start && !source.is_empty(),
        "invalid prepared span"
    );
    span::Entity::insert(span::ActiveModel {
        manifest_id: Set(id.into()),
        kind: Set(kind),
        start: Set(start),
        end: Set(end),
        source_manifest: Set(source.into()),
        ..Default::default()
    })
    .on_conflict(
        OnConflict::columns([
            span::Column::ManifestId,
            span::Column::Kind,
            span::Column::Start,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(db)
    .await?;
    let actual = crate::frozen::find_span(id.to_owned(), kind, start)
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("span readback missing"))?;
    ensure!(
        actual.end == end && actual.source_manifest == source,
        "immutable span conflict"
    );
    Ok(())
}

/// Called inside the append transaction: reserve a physical tail and update its
/// logical range atomically with the insert. Published ranges never grow.
pub async fn append_source<C: ConnectionTrait>(
    db: &C,
    id: &str,
    kind: i64,
    ordinal: i64,
) -> Result<String> {
    if crate::frozen::find_layout(id.to_owned(), kind)
        .filter(layout::Column::Active.eq(1_i64))
        .one(db)
        .await?
        .is_none()
    {
        return Ok(id.to_owned());
    }
    let last = span::Entity::find()
        .filter(span::Column::ManifestId.eq(id))
        .filter(span::Column::Kind.eq(kind))
        .order_by_desc(span::Column::Start)
        .one(db)
        .await?;
    if let Some(s) = last {
        if ordinal < s.end {
            return Ok(s.source_manifest);
        }
        ensure!(ordinal == s.end, "range append is not sequential");
        let max = if kind == 0 {
            message_data::Entity::find()
                .filter(message_data::Column::ManifestId.eq(&s.source_manifest))
                .select_only()
                .column(message_data::Column::Ordinal)
                .order_by_desc(message_data::Column::Ordinal)
                .into_tuple::<i64>()
                .one(db)
                .await?
        } else {
            import_data::Entity::find()
                .filter(import_data::Column::ManifestId.eq(&s.source_manifest))
                .select_only()
                .column(import_data::Column::Ordinal)
                .order_by_desc(import_data::Column::Ordinal)
                .into_tuple::<i64>()
                .one(db)
                .await?
        };
        if max == Some(ordinal - 1) {
            span::Entity::update_many()
                .col_expr(span::Column::End, Expr::val(ordinal + 1))
                .filter(span::Column::ManifestId.eq(id))
                .filter(span::Column::Kind.eq(kind))
                .filter(span::Column::Start.eq(s.start))
                .exec(db)
                .await?;
            return Ok(s.source_manifest);
        }
    } else {
        ensure!(ordinal == 0, "initial range ordinal mismatch");
    }
    span::Entity::insert(span::ActiveModel {
        manifest_id: Set(id.into()),
        kind: Set(kind),
        start: Set(ordinal),
        end: Set(ordinal + 1),
        source_manifest: Set(id.into()),
        ..Default::default()
    })
    .exec(db)
    .await?;
    Ok(id.into())
}

#[derive(Default)]
pub struct FrozenConversionProgress {
    copy: Option<StagedCopyCheck>,
    upper: Option<String>,
    after: [String; 2],
    register_after: String,
}

/// One resumable maintenance quantum; a corrupt manifest is quarantined so it
/// cannot prevent subsequent manifests from being converted.
pub async fn maintain(
    db: &SqliteDatabase,
    progress: &mut FrozenConversionProgress,
    target_manifests: Option<&[String]>,
) -> Result<(bool, Option<(i64, PhysicalDeleteResult)>)> {
    if progress.upper.is_none() {
        progress.upper = Some(
            history::Entity::find()
                .select_only()
                .column(history::Column::Id)
                .order_by_desc(history::Column::Id)
                .into_tuple::<String>()
                .one(db)
                .await?
                .unwrap_or_default(),
        );
    }
    if let Some(check) = progress.copy.as_mut() {
        let result = check.step(db).await;
        match result {
            Ok(false) => return Ok((true, None)),
            Ok(true) => {
                let check = progress.copy.take().expect("copy validation");
                let deleted = maintain_layout(db, &check.layout, check.revisions).await?;
                return Ok((true, deleted.map(|result| (check.layout.kind, result))));
            }
            Err(error) => {
                // Resource/DB errors are retryable, never durable corruption.
                if error.downcast_ref::<sea_orm::DbErr>().is_some() {
                    return Err(error);
                }
                let check = progress.copy.take().expect("copy validation");
                let current =
                    crate::frozen::find_layout(check.layout.manifest_id.clone(), check.layout.kind)
                        .one(db)
                        .await?;
                let header = history::Entity::find_by_id(&check.header().id)
                    .one(db)
                    .await?;
                if current.as_ref() != Some(&check.layout)
                    || header.as_ref() != Some(check.header())
                {
                    return Ok((true, None));
                }
                layout::Entity::update_many()
                    .col_expr(layout::Column::Failed, Expr::val(1))
                    .col_expr(layout::Column::Pending, Expr::val(0))
                    .filter(layout::Column::ManifestId.eq(check.layout.manifest_id))
                    .filter(layout::Column::Kind.eq(check.layout.kind))
                    .exec(db)
                    .await?;
                return Err(error);
            }
        }
    }
    use sea_orm::QueryTrait;
    for kind in [0, 1] {
        let pending = layout::Entity::find()
            .filter(match target_manifests {
                Some(ids) => sea_orm::Condition::all()
                    .add(layout::Column::ManifestId.is_in(ids.iter().cloned())),
                None => sea_orm::Condition::all(),
            })
            .filter(layout::Column::ManifestId.gte(&progress.after[kind as usize]))
            .filter(layout::Column::ManifestId.lte(progress.upper.as_deref().unwrap_or_default()))
            .filter(layout::Column::Kind.eq(kind))
            .filter(layout::Column::Pending.eq(1))
            .filter(layout::Column::Failed.eq(0))
            .filter(Expr::col(layout::Column::Active).eq(0).or(
                Expr::col(layout::Column::CleanupNext).lt(Expr::col(layout::Column::CleanupTo)),
            ))
            .filter(Expr::exists(
                history::Entity::find()
                    .select_only()
                    .column(history::Column::Id)
                    .filter(
                        Expr::col((history::Entity, history::Column::Id))
                            .eq(Expr::col((layout::Entity, layout::Column::ManifestId))),
                    )
                    .filter(history::Column::Ready.eq(1_i64))
                    .filter(history::Column::Expired.eq(0_i64))
                    .filter(
                        Expr::col((history::Entity, history::Column::NextOrdinal))
                            .eq(Expr::col((history::Entity, history::Column::MessageCount))),
                    )
                    .filter(
                        Expr::col((history::Entity, history::Column::NextImport))
                            .eq(Expr::col((history::Entity, history::Column::ImportCount))),
                    )
                    .into_query(),
            ))
            .order_by_asc(layout::Column::ManifestId)
            .one(db)
            .await?;
        if let Some(l) = pending {
            progress.after[kind as usize] = l.manifest_id.clone();
            if l.active == 0 && l.copy_to.is_some_and(|end| end == l.copy_next) {
                let h = history::Entity::find_by_id(&l.manifest_id)
                    .one(db)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("copy header missing"))?;
                progress.copy = Some(StagedCopyCheck::new(h, l)?);
                return Ok((true, None));
            }
            let deleted = match maintain_layout(db, &l, None).await {
                Ok(deleted) => deleted,
                Err(error) => {
                    if error.downcast_ref::<sea_orm::DbErr>().is_some() {
                        return Err(error);
                    }
                    layout::Entity::update_many()
                        .col_expr(layout::Column::Failed, Expr::val(1))
                        .col_expr(layout::Column::Pending, Expr::val(0))
                        .filter(layout::Column::ManifestId.eq(&l.manifest_id))
                        .filter(layout::Column::Kind.eq(kind))
                        .exec(db)
                        .await?;
                    return Err(error);
                }
            };
            return Ok((true, deleted.map(|result| (kind, result))));
        }
    }
    let h = history::Entity::find()
        .filter(match target_manifests {
            Some(ids) => {
                sea_orm::Condition::all().add(history::Column::Id.is_in(ids.iter().cloned()))
            }
            None => sea_orm::Condition::all(),
        })
        .filter(history::Column::Id.gt(&progress.register_after))
        .filter(history::Column::Id.lte(progress.upper.as_deref().unwrap_or_default()))
        .filter(history::Column::Ready.eq(1_i64))
        .filter(history::Column::Expired.eq(0_i64))
        .filter(
            Expr::col((history::Entity, history::Column::NextOrdinal))
                .eq(Expr::col((history::Entity, history::Column::MessageCount))),
        )
        .filter(
            Expr::col((history::Entity, history::Column::NextImport))
                .eq(Expr::col((history::Entity, history::Column::ImportCount))),
        )
        .filter(history::Column::StorageRegistered.eq(0))
        .order_by_asc(history::Column::Id)
        .one(db)
        .await?;
    if let Some(h) = h {
        for kind in [0, 1] {
            register(db, &h, kind, candidate(db, &h, kind).await?).await?;
        }
        history::Entity::update_many()
            .col_expr(history::Column::StorageRegistered, Expr::val(1))
            .filter(history::Column::Id.eq(&h.id))
            .exec(db)
            .await?;
        progress.register_after = h.id;
        // Registration can create work below either layout cursor. Existing
        // completed layouts are excluded by their durable pending predicate.
        progress.after = Default::default();
        return Ok((true, None));
    }
    progress.upper = None;
    progress.after = Default::default();
    progress.register_after.clear();
    Ok((false, None))
}

async fn maintain_layout(
    db: &SqliteDatabase,
    l: &layout::Model,
    verified: Option<CopyRevisions>,
) -> Result<Option<PhysicalDeleteResult>> {
    let h = history::Entity::find_by_id(&l.manifest_id)
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("capture removed"))?;
    if l.active == 0 {
        if let Some(prefix) = l.copy_to {
            publish(db, &h, l, prefix, verified).await?;
            return Ok(None);
        }
        let mut matched = l.compared;
        let count = if l.kind == 0 {
            h.message_count
        } else {
            h.import_count
        };
        let mut done = matched == count || l.candidate.is_none();
        if !done {
            let a = rows(db, &h.id, l.kind, matched).await?;
            let b = rows(db, l.candidate.as_ref().unwrap(), l.kind, matched).await?;
            ensure!(!a.is_empty(), "legacy frozen history is incomplete");
            if b.is_empty() {
                done = true;
            }
            for ((_, x), (_, y)) in a.iter().zip(&b) {
                if x != y
                    || (l.kind == 1
                        && !same_import_target(db, &h.id, l.candidate.as_ref().unwrap(), x).await?)
                {
                    done = true;
                    break;
                }
                matched += 1;
            }
            done |= matched == count;
        }
        if done {
            publish(db, &h, l, matched, None).await?;
        } else {
            layout::Entity::update_many()
                .col_expr(layout::Column::Compared, Expr::val(matched))
                .filter(layout::Column::ManifestId.eq(&h.id))
                .filter(layout::Column::Kind.eq(l.kind))
                .filter(layout::Column::Compared.eq(l.compared))
                .exec(db)
                .await?;
        }
    } else {
        let sizes = if l.kind == 0 {
            message_data::Entity::find()
                .filter(message_data::Column::ManifestId.eq(&h.id))
                .filter(message_data::Column::Ordinal.gte(l.cleanup_next))
                .filter(message_data::Column::Ordinal.lt(l.cleanup_to))
                .select_only()
                .column(message_data::Column::Ordinal)
                .column(message_data::Column::Bytes)
                .expr(Expr::cust("octet_length(reference_json)::bigint"))
                .order_by_asc(message_data::Column::Ordinal)
                .limit(ROWS)
                .into_tuple::<(i64, i64, i64)>()
                .all(db)
                .await?
        } else {
            import_data::Entity::find()
                .filter(import_data::Column::ManifestId.eq(&h.id))
                .filter(import_data::Column::Ordinal.gte(l.cleanup_next))
                .filter(import_data::Column::Ordinal.lt(l.cleanup_to))
                .select_only()
                .column(import_data::Column::Ordinal)
                .column(import_data::Column::Bytes)
                .expr(Expr::cust("octet_length(proof_json)::bigint"))
                .order_by_asc(import_data::Column::Ordinal)
                .limit(ROWS)
                .into_tuple::<(i64, i64, i64)>()
                .all(db)
                .await?
        };
        let mut end = if sizes.is_empty() {
            l.cleanup_to
        } else {
            l.cleanup_next
        };
        let mut bytes = 0;
        let mut keys = Vec::new();
        for (ordinal, size, actual) in sizes {
            if size != actual || !(0..=BYTES).contains(&size) {
                // Bad redundant payload is not evidence of a bad logical
                // layout. Keep it and advance this finite cleanup visit;
                // healthy rows above this local cap belong to lifetime sweep.
                end = ordinal + 1;
                continue;
            }
            if bytes + size > BYTES {
                break;
            }
            bytes += size;
            end = ordinal + 1;
            keys.push((ordinal, size));
        }
        let statements = duplicate_cleanup_statements(&h, l, &keys)?;
        let frozen_change = db.work_signal("maintenance.frozen").changing();
        let tx = db.begin().await?;
        let current = crate::frozen::find_layout(h.id.clone(), l.kind)
            .one(&tx)
            .await?
            .ok_or_else(|| anyhow::anyhow!("cleanup layout removed"))?;
        if current.cleanup_next != l.cleanup_next {
            tx.commit().await?;
            frozen_change.completed(false);
            return Ok(None);
        }
        // The generic lifetime sweep separately validates all reverse layouts
        // in paged progress. Duplicate cleanup never guesses the health of a
        // foreign layout; it skips such a container and still finishes its own
        // finite cleanup cursor. Expired spans cannot become permanent roots:
        // the lifetime sweep later evaluates their exact logical union.
        let foreign = span::Entity::find()
            .filter(span::Column::SourceManifest.eq(&h.id))
            .filter(span::Column::Kind.eq(l.kind))
            .one(&tx)
            .await?
            .is_some();
        let mut deleted = None;
        if let Some((guard, delete)) = statements {
            deleted = Some(if !foreign && tx.query_one_raw(guard).await?.is_some() {
                let rows = tx.query_all_raw(delete).await?;
                PhysicalDeleteResult::from_rows(&rows)?
            } else {
                PhysicalDeleteResult::refused()
            });
        }
        let changed = layout::Entity::update_many()
            .col_expr(layout::Column::CleanupNext, Expr::val(end))
            .col_expr(
                layout::Column::Pending,
                Expr::val(i64::from(end < l.cleanup_to)),
            )
            .filter(layout::Column::ManifestId.eq(&h.id))
            .filter(layout::Column::Kind.eq(l.kind))
            .exec(&tx)
            .await?;
        tx.commit().await?;
        let keys = Some(
            std::iter::once(h.id.clone())
                .chain(l.candidate.iter().cloned())
                .map(|id| format!("m:{id}"))
                .collect::<Vec<_>>(),
        );
        crate::frozen_lifetime::complete_keys(
            frozen_change,
            &keys,
            changed.rows_affected > 0 && current.pending != 0 && end >= l.cleanup_to,
        );
        return Ok(deleted);
    }
    Ok(None)
}
