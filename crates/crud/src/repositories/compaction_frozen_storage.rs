//! Immutable shared ranges. A range addresses physical rows directly: reads
//! never traverse parent snapshots. Publication preserves the logical views.
use super::compaction_frozen_views::{import, message};
use crate::CrudStore;
use anyhow::{Result, ensure};
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
pub(crate) async fn rows<C: ConnectionTrait>(
    db: &C,
    id: &str,
    kind: i64,
    start: i64,
) -> Result<Vec<(i64, String)>> {
    let h = history::Entity::find_by_id(id)
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("frozen comparison input missing"))?;
    let bounds = crate::frozen_lifetime::logical_bounds(&h)?;
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
                .filter(Expr::cust(format!("length(CAST({} AS BLOB))=bytes AND bytes BETWEEN 0 AND {}", $json, BYTES)))
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
        .filter(layout::Column::Active.eq(1))
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
        .filter(history::Column::Ready.eq(1))
        .filter(history::Column::Expired.eq(0))
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

pub(crate) async fn equivalent(
    store: &CrudStore,
    workspace: &str,
    owner: &str,
    descriptor: &pioneer_compaction::frozen::FrozenHistoryRef,
    imports: u64,
    digest: &str,
) -> Result<Option<pioneer_compaction::frozen::FrozenHistoryRef>> {
    let found = history::Entity::find()
        .filter(history::Column::WorkspaceId.eq(workspace))
        .filter(history::Column::OwnerThread.eq(owner))
        .filter(history::Column::Ready.eq(1))
        .filter(history::Column::Expired.eq(0))
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
        .one(&store.connection)
        .await?;
    Ok(found.map(|h| pioneer_compaction::frozen::FrozenHistoryRef {
        manifest_id: h.id,
        ..descriptor.clone()
    }))
}

// Register metadata only. Ready manifests and candidate rows are immutable;
// candidate scope is revalidated when publishing the prepared ranges.
async fn register(
    store: &CrudStore,
    h: &history::Model,
    kind: i64,
    base: Option<String>,
) -> Result<layout::Model> {
    let tx = store.connection.begin().await?;
    let current = history::Entity::find_by_id(&h.id)
        .one(&tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("capture removed"))?;
    crate::frozen_lifetime::logical_bounds(&current)?;
    ensure!(
        current.identity_sha256 == h.identity_sha256 && current.imports_sha256 == h.imports_sha256,
        "capture changed"
    );
    if let Some(base) = &base {
        let candidate = history::Entity::find_by_id(base)
            .one(&tx)
            .await?
            .ok_or_else(|| anyhow::anyhow!("base missing"))?;
        crate::frozen_lifetime::logical_bounds(&candidate)?;
        ensure!(
            candidate.ready == 1
                && candidate.workspace_id == h.workspace_id
                && candidate.owner_thread == h.owner_thread,
            "base scope unavailable"
        );
        let layout = layout::Entity::find_by_id((base.clone(), kind))
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
    })
    .on_conflict(
        OnConflict::columns([layout::Column::ManifestId, layout::Column::Kind])
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(&tx)
    .await?;
    let state = layout::Entity::find_by_id((h.id.clone(), kind))
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
    let proof: super::compaction_frozen_import::FrozenImportRecord = serde_json::from_str(json)?;
    let ordinal = i64::try_from(proof.message_ordinal)?;
    let mut values = Vec::new();
    for id in [a, b] {
        let text = message::Entity::find_by_id((id.to_owned(), ordinal))
            .filter(message::Column::Bytes.lte(BYTES))
            .select_only()
            .column(message::Column::ReferenceJson)
            .filter(Expr::cust(
                "length(CAST(reference_json AS BLOB))=bytes AND bytes>=0",
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
pub(crate) async fn prepare(
    store: &CrudStore,
    workspace: &str,
    owner: &str,
    id: &str,
    kind: i64,
    entries: &[String],
) -> Result<u64> {
    let h = history::Entity::find_by_id(id)
        .filter(history::Column::WorkspaceId.eq(workspace))
        .filter(history::Column::OwnerThread.eq(owner))
        .one(&store.connection)
        .await?
        .ok_or_else(|| anyhow::anyhow!("frozen capture scope missing"))?;
    crate::frozen_lifetime::logical_bounds(&h)?;
    if h.ready != 0 {
        return Ok(u64::try_from(if kind == 0 {
            h.message_count
        } else {
            h.import_count
        })?);
    }
    let l = register(
        store,
        &h,
        kind,
        candidate(&store.connection, &h, kind).await?,
    )
    .await?;
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
            let page = rows(&store.connection, base, kind, matched as i64).await?;
            if page.is_empty() {
                break;
            }
            for (_, text) in page {
                if entries.get(matched) != Some(&text) {
                    break 'pages;
                }
                if kind == 1 {
                    let proof: super::compaction_frozen_import::FrozenImportRecord =
                        serde_json::from_str(&text)?;
                    // The message tail is appended after prefix preparation.
                    // Imports targeting it must follow the normal append path.
                    if i64::try_from(proof.message_ordinal)? >= h.next_ordinal
                        || !same_import_target(&store.connection, id, base, &text).await?
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
            while !check.step(store).await? {}
            true
        } else {
            false
        };
        if publish(store, &h, &state, matched as i64, verified).await? {
            break;
        }
        state = layout::Entity::find_by_id((id.to_owned(), kind))
            .one(&store.connection)
            .await?
            .ok_or_else(|| anyhow::anyhow!("capture layout removed during preparation"))?;
    }
    Ok(matched as u64)
}

/// Flatten a prefix to physical ranges. Incomplete range copies are invisible;
/// each batch is restart-safe. A constant-size transaction publishes the layout.
async fn publish(
    store: &CrudStore,
    h: &history::Model,
    l: &layout::Model,
    prefix: i64,
    verified: bool,
) -> Result<bool> {
    let prefix = l.copy_to.unwrap_or(prefix);
    if l.copy_to.is_none() {
        layout::Entity::update_many()
            .col_expr(layout::Column::CopyTo, Expr::val(prefix))
            .filter(layout::Column::ManifestId.eq(&h.id))
            .filter(layout::Column::Kind.eq(l.kind))
            .filter(layout::Column::CopyTo.is_null())
            .exec(&store.connection)
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
                .all(&store.connection)
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
            let tx = store.connection.begin().await?;
            checked_copy_dependencies(&tx, h, l).await?;
            let state = layout::Entity::find_by_id((h.id.clone(), l.kind))
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
    if !verified {
        return Ok(false);
    }
    let count = if l.kind == 0 {
        h.message_count
    } else {
        h.import_count
    };
    ensure!(prefix <= count, "shared prefix exceeds capture");
    let tx = store.connection.begin().await?;
    let current = history::Entity::find_by_id(&h.id)
        .one(&tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("capture removed"))?;
    let state = layout::Entity::find_by_id((h.id.clone(), l.kind))
        .one(&tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("capture layout removed"))?;
    crate::frozen_lifetime::logical_bounds(&current)?;
    if state.active != 0 {
        tx.commit().await?;
        return Ok(true);
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
                .filter(history::Column::Ready.eq(1))
                .filter(history::Column::Expired.eq(0))
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
    layout::Entity::update_many()
        .col_expr(layout::Column::Active, Expr::val(1))
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
        .filter(layout::Column::Active.eq(0))
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
    Ok(true)
}

async fn checked_copy_dependencies<C: ConnectionTrait>(
    db: &C,
    h: &history::Model,
    l: &layout::Model,
) -> Result<()> {
    let current = history::Entity::find_by_id(&h.id)
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("copy logical capture missing"))?;
    crate::frozen_lifetime::logical_bounds(&current)?;
    ensure!(
        current.workspace_id == h.workspace_id
            && current.owner_thread == h.owner_thread
            && current.identity_sha256 == h.identity_sha256
            && current.imports_sha256 == h.imports_sha256
            && current.message_count == h.message_count
            && current.import_count == h.import_count
            && current.ready == h.ready,
        "copy logical identity changed"
    );
    if let Some(base) = &l.candidate {
        let b = history::Entity::find_by_id(base)
            .one(db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("copy logical base missing"))?;
        crate::frozen_lifetime::logical_bounds(&b)?;
        ensure!(
            b.ready == 1 && b.workspace_id == h.workspace_id && b.owner_thread == h.owner_thread,
            "copy logical base unavailable"
        );
        let bl = layout::Entity::find_by_id((base.clone(), l.kind))
            .one(db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("copy base layout missing"))?;
        ensure!(
            bl.active == 1 && bl.failed == 0 && bl.pending == 0,
            "copy base layout changed"
        );
    }
    Ok(())
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
    let actual = span::Entity::find_by_id((id.to_owned(), kind, start))
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
pub(crate) async fn append_source<C: ConnectionTrait>(
    db: &C,
    id: &str,
    kind: i64,
    ordinal: i64,
) -> Result<String> {
    if layout::Entity::find_by_id((id.to_owned(), kind))
        .filter(layout::Column::Active.eq(1))
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
    })
    .exec(db)
    .await?;
    Ok(id.into())
}

/// One resumable maintenance quantum; a corrupt manifest is quarantined so it
/// cannot prevent subsequent manifests from being converted.
pub(crate) async fn maintain(
    store: &CrudStore,
    progress: &mut super::frozen_storage_lifetime::FrozenStorageLifetimeProgress,
) -> Result<bool> {
    if !progress.conversion_scope(store).await? {
        return Ok(true);
    }
    if let Some(check) = progress.conversion.as_mut() {
        let result = check.step(store).await;
        match result {
            Ok(false) => return Ok(true),
            Ok(true) => {
                let check = progress.conversion.take().expect("copy validation");
                maintain_layout(store, &check.layout, true).await?;
                return Ok(true);
            }
            Err(error) => {
                let check = progress.conversion.take().expect("copy validation");
                layout::Entity::update_many()
                    .col_expr(layout::Column::Failed, Expr::val(1))
                    .col_expr(layout::Column::Pending, Expr::val(0))
                    .filter(layout::Column::ManifestId.eq(check.layout.manifest_id))
                    .filter(layout::Column::Kind.eq(check.layout.kind))
                    .exec(&store.connection)
                    .await?;
                return Err(error);
            }
        }
    }
    use sea_orm::QueryTrait;
    for kind in [0, 1] {
        let pending = layout::Entity::find()
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
                    .filter(history::Column::Ready.eq(1))
                    .filter(history::Column::Expired.eq(0))
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
            .one(&store.connection)
            .await?;
        if let Some(l) = pending {
            if l.active == 0 && l.copy_to.is_some_and(|end| end == l.copy_next) {
                let h = history::Entity::find_by_id(&l.manifest_id)
                    .one(&store.connection)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("copy header missing"))?;
                progress.conversion = Some(StagedCopyCheck::new(h, l)?);
                return Ok(true);
            }
            if let Err(error) = maintain_layout(store, &l, false).await {
                if error.downcast_ref::<sea_orm::DbErr>().is_some() {
                    return Err(error);
                }
                layout::Entity::update_many()
                    .col_expr(layout::Column::Failed, Expr::val(1))
                    .col_expr(layout::Column::Pending, Expr::val(0))
                    .filter(layout::Column::ManifestId.eq(&l.manifest_id))
                    .filter(layout::Column::Kind.eq(kind))
                    .exec(&store.connection)
                    .await?;
                return Err(error);
            }
            return Ok(true);
        }
    }
    let h = history::Entity::find()
        .filter(history::Column::Ready.eq(1))
        .filter(history::Column::Expired.eq(0))
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
        .one(&store.connection)
        .await?;
    if let Some(h) = h {
        for kind in [0, 1] {
            register(
                store,
                &h,
                kind,
                candidate(&store.connection, &h, kind).await?,
            )
            .await?;
        }
        history::Entity::update_many()
            .col_expr(history::Column::StorageRegistered, Expr::val(1))
            .filter(history::Column::Id.eq(&h.id))
            .exec(&store.connection)
            .await?;
        return Ok(true);
    }
    Ok(false)
}

async fn maintain_layout(store: &CrudStore, l: &layout::Model, verified: bool) -> Result<()> {
    let h = history::Entity::find_by_id(&l.manifest_id)
        .one(&store.connection)
        .await?
        .ok_or_else(|| anyhow::anyhow!("capture removed"))?;
    if l.active == 0 {
        if let Some(prefix) = l.copy_to {
            publish(store, &h, l, prefix, verified).await?;
            return Ok(());
        }
        let mut matched = l.compared;
        let count = if l.kind == 0 {
            h.message_count
        } else {
            h.import_count
        };
        let mut done = matched == count || l.candidate.is_none();
        if !done {
            let a = rows(&store.connection, &h.id, l.kind, matched).await?;
            let b = rows(
                &store.connection,
                l.candidate.as_ref().unwrap(),
                l.kind,
                matched,
            )
            .await?;
            ensure!(!a.is_empty(), "legacy frozen history is incomplete");
            if b.is_empty() {
                done = true;
            }
            for ((_, x), (_, y)) in a.iter().zip(&b) {
                if x != y
                    || (l.kind == 1
                        && !same_import_target(
                            &store.connection,
                            &h.id,
                            l.candidate.as_ref().unwrap(),
                            x,
                        )
                        .await?)
                {
                    done = true;
                    break;
                }
                matched += 1;
            }
            done |= matched == count;
        }
        if done {
            publish(store, &h, l, matched, false).await?;
        } else {
            layout::Entity::update_many()
                .col_expr(layout::Column::Compared, Expr::val(matched))
                .filter(layout::Column::ManifestId.eq(&h.id))
                .filter(layout::Column::Kind.eq(l.kind))
                .filter(layout::Column::Compared.eq(l.compared))
                .exec(&store.connection)
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
                .expr(Expr::cust("length(CAST(reference_json AS BLOB))"))
                .order_by_asc(message_data::Column::Ordinal)
                .limit(ROWS)
                .into_tuple::<(i64, i64, i64)>()
                .all(&store.connection)
                .await?
        } else {
            import_data::Entity::find()
                .filter(import_data::Column::ManifestId.eq(&h.id))
                .filter(import_data::Column::Ordinal.gte(l.cleanup_next))
                .filter(import_data::Column::Ordinal.lt(l.cleanup_to))
                .select_only()
                .column(import_data::Column::Ordinal)
                .column(import_data::Column::Bytes)
                .expr(Expr::cust("length(CAST(proof_json AS BLOB))"))
                .order_by_asc(import_data::Column::Ordinal)
                .limit(ROWS)
                .into_tuple::<(i64, i64, i64)>()
                .all(&store.connection)
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
            ensure!(size == actual, "duplicate row declared bytes mismatch");
            ensure!((0..=BYTES).contains(&size), "invalid duplicate row size");
            if bytes + size > BYTES {
                break;
            }
            bytes += size;
            end = ordinal + 1;
            keys.push((ordinal, size));
        }
        let tx = store.connection.begin().await?;
        let current = layout::Entity::find_by_id((h.id.clone(), l.kind))
            .one(&tx)
            .await?
            .ok_or_else(|| anyhow::anyhow!("cleanup layout removed"))?;
        if current.cleanup_next != l.cleanup_next {
            tx.commit().await?;
            return Ok(());
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
        if !foreign {
            for (ordinal, size) in keys {
                super::frozen_storage_lifetime::delete_free_key(&tx, &h.id, l.kind, ordinal, size)
                    .await?;
            }
        }
        layout::Entity::update_many()
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
    }
    Ok(())
}

/// Private existing-worker progress for restart verification. Pending layout
/// dependencies protect all backing sets; no connection/permit or reader is
/// retained during backoff. New captures use the identical bounded validator.
pub(super) struct StagedCopyCheck {
    header: history::Model,
    pub(super) layout: layout::Model,
    after: i64,
    next: i64,
    base_layout: Option<crate::frozen_lifetime::LayoutScan>,
    base_started: bool,
}
impl StagedCopyCheck {
    fn new(header: history::Model, layout: layout::Model) -> Result<Self> {
        ensure!(
            layout.active == 0
                && layout.failed == 0
                && layout.pending == 1
                && layout.copy_to == Some(layout.copy_next)
                && layout.copy_next >= 0,
            "staged copy not ready for verification"
        );
        Ok(Self {
            header,
            layout,
            after: -1,
            next: 0,
            base_layout: None,
            base_started: false,
        })
    }
    async fn step(&mut self, store: &CrudStore) -> Result<bool> {
        checked_copy_dependencies(&store.connection, &self.header, &self.layout).await?;
        if !self.base_started {
            self.base_started = true;
            if let Some(base) = &self.layout.candidate {
                let header = history::Entity::find_by_id(base)
                    .one(&store.connection)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("copy base removed"))?;
                self.base_layout = Some(crate::frozen_lifetime::LayoutScan::new(header, 0, 2)?);
                return Ok(false);
            }
        }
        if let Some(scan) = self.base_layout.as_mut() {
            scan.step(store).await?;
            if scan.done() {
                self.base_layout = None;
            }
            return Ok(false);
        }
        let current = layout::Entity::find_by_id((self.header.id.clone(), self.layout.kind))
            .one(&store.connection)
            .await?
            .ok_or_else(|| anyhow::anyhow!("staged copy removed"))?;
        ensure!(
            current == self.layout,
            "staged copy changed during verification"
        );
        let page = span::Entity::find()
            .filter(span::Column::ManifestId.eq(&self.header.id))
            .filter(span::Column::Kind.eq(self.layout.kind))
            .filter(span::Column::Start.gt(self.after))
            .order_by_asc(span::Column::Start)
            .limit(ROWS)
            .all(&store.connection)
            .await?;
        for s in &page {
            ensure!(
                s.start == self.next && s.end > s.start && s.end <= self.layout.copy_next,
                "staged prefix geometry corrupt"
            );
            let base = self
                .layout
                .candidate
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("staged prefix has no base"))?;
            let original = span::Entity::find()
                .filter(span::Column::ManifestId.eq(base))
                .filter(span::Column::Kind.eq(s.kind))
                .filter(span::Column::Start.eq(s.start))
                .one(&store.connection)
                .await?
                .ok_or_else(|| anyhow::anyhow!("staged prefix base range missing"))?;
            ensure!(
                s.source_manifest == original.source_manifest
                    && s.end == std::cmp::min(original.end, self.layout.copy_next),
                "staged prefix exact backing conflict"
            );
            self.next = s.end;
            self.after = s.start;
        }
        if page.len() < ROWS as usize {
            ensure!(
                self.next == self.layout.copy_next,
                "staged prefix incomplete"
            );
            return Ok(true);
        }
        Ok(false)
    }
}
