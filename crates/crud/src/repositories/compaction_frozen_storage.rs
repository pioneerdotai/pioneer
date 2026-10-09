//! Immutable shared ranges. A range addresses physical rows directly: reads
//! never traverse parent snapshots. Publication preserves the logical views.
use super::compaction_frozen_views::message;
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_entity::{
    compaction_frozen_history as history, compaction_frozen_import_data as import_data,
    compaction_frozen_layout as layout, compaction_frozen_message_data as message_data,
    compaction_frozen_span as span,
};
use sea_orm::sea_query::{Expr, ExprTrait, OnConflict};
use sea_orm::{
    ActiveValue::Set, DbBackend, QueryOrder, QuerySelect, Statement, TransactionTrait,
    entity::prelude::*,
};
const ROWS: u64 = 128;
const BYTES: i64 = 256 * 1024;

#[derive(Debug, PartialEq, Eq)]
enum CopyOutcome {
    Complete,
    Progress,
    /// Preserve direct backing and inactive reservations. Optional conversion
    /// cannot transfer even one legal legacy row within its metadata budget.
    Retained,
}

fn span_copy_bytes(base: &str, target: &str, source_bytes: i64) -> Result<usize> {
    // Input model + inserted model: UTF-8 IDs, plus kind/start/end on each side.
    // Overflow is an oversized row, never EOF or a truncated identity.
    let source = usize::try_from(source_bytes)?;
    Ok(base
        .len()
        .saturating_add(target.len())
        .saturating_add(source.saturating_mul(2))
        .saturating_add(48))
}

// Logical access always names a borrowed target use. Builder reads are next-bounded.
pub(crate) async fn rows<C: ConnectionTrait>(
    db: &C,
    guard: &crate::FrozenUseGuard,
    kind: i64,
    start: i64,
) -> Result<Vec<(i64, String)>> {
    super::compaction_frozen_verify::rows(db, guard, kind, start, guard.header().ready != 0).await
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
        .filter(
            Expr::col((layout::Entity, layout::Column::ManifestId))
                .eq(Expr::col((history::Entity, history::Column::Id))),
        );
    use sea_orm::QueryTrait;
    Ok(history::Entity::find()
        .filter(history::Column::WorkspaceId.eq(&h.workspace_id))
        .filter(history::Column::OwnerThread.eq(&h.owner_thread))
        .filter(history::Column::Ready.eq(1))
        .filter(history::Column::Availability.eq("resident"))
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
) -> Result<Option<crate::FrozenUseGuard>> {
    let use_id = uuid::Uuid::new_v4().to_string();
    store
        .run_serialized_write(|| async {
            let tx = store.connection.begin().await?;
            let found = history::Entity::find()
                .filter(history::Column::WorkspaceId.eq(workspace))
                .filter(history::Column::OwnerThread.eq(owner))
                .filter(history::Column::Ready.eq(1))
                .filter(history::Column::Availability.eq("resident"))
                .filter(history::Column::IdentitySha256.eq(&descriptor.identity_sha256))
                .filter(history::Column::MessageCount.eq(i64::try_from(descriptor.messages)?))
                .filter(history::Column::ImportsSha256.eq(digest))
                .filter(history::Column::ImportCount.eq(i64::try_from(imports)?))
                .filter(
                    Expr::col((history::Entity, history::Column::NextOrdinal))
                        .eq(Expr::col(history::Column::MessageCount)),
                )
                .filter(
                    Expr::col((history::Entity, history::Column::NextImport))
                        .eq(Expr::col(history::Column::ImportCount)),
                )
                .order_by_asc(history::Column::Id)
                .one(&tx)
                .await?;
            let guard = match found {
                Some(h) => {
                    let exact = pioneer_compaction::frozen::FrozenHistoryRef {
                        manifest_id: h.id,
                        ..descriptor.clone()
                    };
                    Some(
                        super::compaction_frozen_use::acquire_in(
                            store,
                            &tx,
                            workspace,
                            &exact,
                            Some(owner),
                            "capture",
                            true,
                            &use_id,
                        )
                        .await?,
                    )
                }
                None => None,
            };
            tx.commit().await?;
            Ok(guard)
        })
        .await
}

// Register metadata only. Ready manifests and candidate rows are immutable;
// candidate scope is revalidated when publishing the prepared ranges.
async fn register(
    store: &CrudStore,
    h: &history::Model,
    kind: i64,
    base: Option<String>,
    guard: &crate::FrozenUseGuard,
) -> Result<layout::Model> {
    store.run_serialized_write(|| async {
    let tx = store.connection.begin().await?;
    guard.validate_in(&tx, h.ready != 0).await?;
    if let Some(base) = &base {
        let actual = tx.query_one_raw(sea_orm::Statement::from_sql_and_values(sea_orm::DbBackend::Sqlite,
            "SELECT b.id FROM compaction_frozen_history b JOIN thread t ON t.id=b.owner_thread AND t.workspace_id=b.workspace_id \
             JOIN workspace w ON w.id=b.workspace_id WHERE b.id=? AND b.workspace_id=? AND b.owner_thread=? \
             AND b.availability='resident' AND b.ready=1 AND b.next_ordinal=b.message_count AND b.next_import=b.import_count",
            [base.clone().into(), h.workspace_id.clone().into(), h.owner_thread.clone().into()])).await?;
        ensure!(actual.is_some(), "prefix candidate domain is unavailable");
    }
    layout::Entity::insert(layout::ActiveModel {
        manifest_id: Set(h.id.clone()),
        kind: Set(kind),
        active: Set(0),
        pending: Set(1),
        candidate: Set(base.clone()),
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
    let actual = layout::Entity::find_by_id((h.id.clone(), kind))
        .one(&tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("capture layout removed during registration"))?;
    tx.commit().await?;
    Ok(actual)
    }).await
}

// Import proofs bind a message ordinal as well as a source. Reuse requires
// the exact target reference, not only identical proof JSON.
async fn same_import_target<C: ConnectionTrait>(
    db: &C,
    a: &crate::FrozenUseGuard,
    b: &crate::FrozenUseGuard,
    json: &str,
) -> Result<bool> {
    let proof: super::compaction_frozen_import::FrozenImportRecord = serde_json::from_str(json)?;
    let ordinal = i64::try_from(proof.message_ordinal)?;
    let mut values = Vec::new();
    for guard in [a, b] {
        guard.validate_in(db, guard.header().ready != 0).await?;
        let next = history::Entity::find_by_id(&guard.header().id)
            .one(db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("import target is unavailable"))?
            .next_ordinal;
        ensure!(
            ordinal >= 0 && ordinal < next,
            "import target is outside captured next bound"
        );
        let text = message::Entity::find_by_id((guard.header().id.clone(), ordinal))
            .filter(message::Column::Bytes.lte(BYTES))
            .select_only()
            .column(message::Column::ReferenceJson)
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
    guard: &crate::FrozenUseGuard,
) -> Result<u64> {
    ensure!(
        guard.workspace() == workspace && guard.owner() == owner && guard.header().id == id,
        "prefix use scope mismatch"
    );
    guard.validate_in(&store.connection, false).await?;
    let h = history::Entity::find_by_id(id)
        .filter(history::Column::WorkspaceId.eq(workspace))
        .filter(history::Column::OwnerThread.eq(owner))
        .one(&store.connection)
        .await?
        .ok_or_else(|| anyhow::anyhow!("frozen capture scope missing"))?;
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
        guard,
    )
    .await?;
    if l.active != 0 || l.failed != 0 {
        return Ok(u64::try_from(if kind == 0 {
            h.next_ordinal
        } else {
            h.next_import
        })?);
    }
    let candidate_guard = match &l.candidate {
        Some(base) => Some(
            store
                .compaction_acquire_frozen_builder_use(workspace, owner, base)
                .await?,
        ),
        None => None,
    };
    let result = async {
        let mut matched = 0_usize;
        if l.candidate.is_some() {
            'pages: while matched < entries.len() {
                let page = rows(
                    &store.connection,
                    candidate_guard.as_ref().unwrap(),
                    kind,
                    matched as i64,
                )
                .await?;
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
                            || !same_import_target(
                                &store.connection,
                                guard,
                                candidate_guard.as_ref().unwrap(),
                                &text,
                            )
                            .await?
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
            match publish(
                store,
                &h,
                &state,
                matched as i64,
                guard,
                candidate_guard.as_ref(),
            )
            .await?
            {
                CopyOutcome::Complete | CopyOutcome::Retained => break,
                CopyOutcome::Progress => {
                    state = layout::Entity::find_by_id((id.to_owned(), kind))
                        .one(&store.connection)
                        .await?
                        .ok_or_else(|| {
                            anyhow::anyhow!("capture layout removed during preparation")
                        })?;
                }
            }
        }
        guard.validate_in(&store.connection, false).await?;
        let current = history::Entity::find_by_id(id)
            .one(&store.connection)
            .await?
            .ok_or_else(|| anyhow::anyhow!("capture removed after prefix publication"))?;
        Ok(u64::try_from(if kind == 0 {
            current.next_ordinal
        } else {
            current.next_import
        })?)
    }
    .await;
    match candidate_guard {
        Some(candidate) => candidate.complete(result).await,
        None => result,
    }
}

/// Flatten a prefix to physical ranges. Incomplete range copies are invisible;
/// each batch is restart-safe. A constant-size transaction publishes the layout.
async fn publish(
    store: &CrudStore,
    h: &history::Model,
    l: &layout::Model,
    requested_prefix: i64,
    guard: &crate::FrozenUseGuard,
    candidate_guard: Option<&crate::FrozenUseGuard>,
) -> Result<CopyOutcome> {
    store
        .run_serialized_write(|| async {
            let tx = store.connection.begin().await?;
            guard.validate_in(&tx, h.ready != 0).await?;
            let current = history::Entity::find_by_id(&h.id)
                .one(&tx)
                .await?
                .ok_or_else(|| anyhow::anyhow!("capture removed"))?;
            let state = layout::Entity::find_by_id((h.id.clone(), l.kind))
                .one(&tx)
                .await?
                .ok_or_else(|| anyhow::anyhow!("capture layout removed"))?;
            if state.active != 0 {
                tx.commit().await?;
                return Ok(CopyOutcome::Complete);
            }
            ensure!(
                current.ready == h.ready,
                "capture readiness changed before range publication"
            );
            if state != *l {
                tx.commit().await?;
                return Ok(CopyOutcome::Progress);
            }
            if state.failed != 0 {
                tx.commit().await?;
                return Ok(CopyOutcome::Retained);
            }
            let prefix = state.copy_to.unwrap_or(requested_prefix);
            let count = if l.kind == 0 {
                current.message_count
            } else {
                current.import_count
            };
            let appended = if l.kind == 0 {
                current.next_ordinal
            } else {
                current.next_import
            };
            ensure!(
                prefix >= 0 && prefix <= count && state.copy_next >= 0 && state.copy_next <= prefix,
                "invalid shared prefix cursor"
            );
            match (&state.candidate, candidate_guard) {
                (Some(id), Some(candidate)) => {
                    ensure!(candidate.header().id == *id, "shared candidate changed");
                    candidate.validate_in(&tx, true).await?;
                    ensure!(layout::Entity::find_by_id((id.clone(), l.kind))
                        .filter(layout::Column::Active.eq(1)).one(&tx).await?.is_some(),
                        "shared candidate layout is unavailable");
                }
                (None, None) => ensure!(prefix == 0, "shared prefix has no candidate"),
                _ => anyhow::bail!("shared candidate use mismatch"),
            }
            let suffix_bytes = if prefix < appended { h.id.len().saturating_mul(2).saturating_add(24) } else { 0 };
            if suffix_bytes > BYTES as usize {
                retain_copy(&tx, &state).await?;
                tx.commit().await?;
                return Ok(CopyOutcome::Retained);
            }
            let mut next = state.copy_next;
            if let Some(base) = &state.candidate {
                if next < prefix {
                    // Sizes first: only numeric scalars cross the DB boundary.
                    // The selected full models form a contiguous <=128-row,
                    // <=256-KiB prefix; a LIMIT on full models alone is insufficient.
                    let sizes = tx.query_all_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                        "SELECT start,end,length(CAST(source_manifest AS BLOB)) AS source_bytes FROM compaction_frozen_span WHERE manifest_id=? AND kind=? AND start>=? AND start<? ORDER BY start LIMIT ?",
                        [base.clone().into(),l.kind.into(),next.into(),prefix.into(),(ROWS as i64).into()])).await?;
                    ensure!(!sizes.is_empty(), "shared prefix range missing");
                    let mut bytes = suffix_bytes;
                    let mut selected_end = next;
                    let mut selected_rows = 0_u64;
                    for size in sizes {
                        if selected_rows == ROWS - u64::from(suffix_bytes != 0) { break; }
                        let start = size.try_get::<i64>("", "start")?;
                        let end = size.try_get::<i64>("", "end")?;
                        ensure!(start == selected_end && end > start, "shared prefix range gap");
                        let size = span_copy_bytes(base, &h.id, size.try_get("", "source_bytes")?)?;
                        if bytes.saturating_add(size) > BYTES as usize { break; }
                        bytes += size;
                        selected_end = std::cmp::min(end, prefix);
                        selected_rows += 1;
                    }
                    if selected_rows == 0 {
                        // This is a retention/progress outcome, not an invalid
                        // descriptor. Captures may append direct own backing;
                        // existing ready histories keep their direct body.
                        // Keep candidate/partial spans to preserve every hold.
                        retain_copy(&tx, &state).await?;
                        tx.commit().await?;
                        return Ok(CopyOutcome::Retained);
                    }
                    let spans = span::Entity::find()
                        .filter(span::Column::ManifestId.eq(base))
                        .filter(span::Column::Kind.eq(l.kind))
                        .filter(span::Column::Start.gte(next))
                        .filter(span::Column::Start.lt(selected_end))
                        .order_by_asc(span::Column::Start)
                        .limit(selected_rows)
                        .all(&tx).await?;
                    ensure!(spans.len() as u64 == selected_rows, "shared prefix sizes changed");
                    for source in spans {
                        ensure!(
                            source.start == next && source.end > next,
                            "shared prefix range gap"
                        );
                        let end = std::cmp::min(source.end, prefix);
                        span::Entity::insert(span::ActiveModel {
                            manifest_id: Set(h.id.clone()),
                            kind: Set(l.kind),
                            start: Set(next),
                            end: Set(end),
                            source_manifest: Set(source.source_manifest.clone()),
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
                        .exec_without_returning(&tx)
                        .await?;
                        // Check exact physical pointer without fetching another
                        // complete model (which would duplicate the byte budget).
                        let actual = tx.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                            "SELECT start FROM compaction_frozen_span WHERE manifest_id=? AND kind=? AND start=? AND end=? AND source_manifest=?",
                            [h.id.clone().into(),l.kind.into(),next.into(),end.into(),source.source_manifest.into()])).await?;
                        ensure!(actual.is_some(), "copied span retry changed physical pointer");
                        next = end;
                    }
                }
            }
            // Span insertion and copy cursor advance are one atomic bounded quantum.
            let advanced = layout::Entity::update_many()
                .col_expr(layout::Column::CopyTo, Expr::val(prefix))
                .col_expr(layout::Column::CopyNext, Expr::val(next))
                .filter(layout::Column::ManifestId.eq(&h.id))
                .filter(layout::Column::Kind.eq(l.kind))
                .filter(layout::Column::Active.eq(0))
                .filter(layout::Column::CopyNext.eq(state.copy_next))
                .exec(&tx)
                .await?;
            ensure!(advanced.rows_affected == 1, "shared prefix cursor changed");
            if next < prefix {
                tx.commit().await?;
                return Ok(CopyOutcome::Progress);
            }
            // A restarted incomplete capture may already have an own suffix. Preserve its next bound.
            if prefix < appended {
                span::Entity::insert(span::ActiveModel {
                    manifest_id: Set(h.id.clone()),
                    kind: Set(l.kind),
                    start: Set(prefix),
                    end: Set(appended),
                    source_manifest: Set(h.id.clone()),
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
                .exec_without_returning(&tx)
                .await?;
                let actual = tx.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                    "SELECT start FROM compaction_frozen_span WHERE manifest_id=? AND kind=? AND start=? AND end=? AND source_manifest=?",
                    [h.id.clone().into(),l.kind.into(),prefix.into(),appended.into(),h.id.clone().into()])).await?;
                ensure!(actual.is_some(), "capture suffix pointer collision");
            }
            layout::Entity::update_many()
                .col_expr(layout::Column::Active, Expr::val(1))
                .col_expr(
                    layout::Column::Pending,
                    Expr::val(i64::from(current.ready != 0 && prefix > 0)),
                )
                .col_expr(layout::Column::Compared, Expr::val(prefix))
                .col_expr(
                    layout::Column::CleanupTo,
                    Expr::val(if current.ready != 0 { prefix } else { 0 }),
                )
                .filter(layout::Column::ManifestId.eq(&h.id))
                .filter(layout::Column::Kind.eq(l.kind))
                .filter(layout::Column::Active.eq(0))
                .exec(&tx)
                .await?;
            if current.ready == 0 {
                history::Entity::update_many()
                    .col_expr(
                        if l.kind == 0 {
                            history::Column::NextOrdinal
                        } else {
                            history::Column::NextImport
                        },
                        Expr::val(std::cmp::max(appended, prefix)),
                    )
                    .filter(history::Column::Id.eq(&h.id))
                    .filter(
                        history::Column::StorageGeneration.eq(guard.header().storage_generation),
                    )
                    .filter(history::Column::Availability.eq("resident"))
                    .filter(history::Column::Ready.eq(0))
                    .exec(&tx)
                    .await?;
            }
            tx.commit().await?;
            Ok(CopyOutcome::Complete)
        })
        .await
}

/// Caller has revalidated header/use/generation and the entire layout inside
/// this same writer transaction. Keep its reservations; only stop conversion.
async fn retain_copy<C: ConnectionTrait>(db: &C, state: &layout::Model) -> Result<()> {
    let changed = layout::Entity::update_many()
        .col_expr(layout::Column::Failed, Expr::val(1))
        .col_expr(layout::Column::Pending, Expr::val(0))
        .filter(layout::Column::ManifestId.eq(&state.manifest_id))
        .filter(layout::Column::Kind.eq(state.kind))
        .filter(layout::Column::Active.eq(0))
        .filter(layout::Column::CopyNext.eq(state.copy_next))
        .exec(db)
        .await?;
    ensure!(changed.rows_affected == 1, "retained prefix cursor changed");
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
pub(crate) async fn maintain(store: &CrudStore) -> Result<bool> {
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
                    .filter(history::Column::Availability.eq("resident"))
                    .into_query(),
            ))
            .order_by_asc(layout::Column::ManifestId)
            .one(&store.connection)
            .await?;
        if let Some(l) = pending {
            if let Err(error) = maintain_layout(store, &l).await {
                if error.downcast_ref::<sea_orm::DbErr>().is_some() {
                    return Err(error);
                }
                store.run_serialized_write(|| async {
                    let tx = store.connection.begin().await?;
                    let current = layout::Entity::find_by_id((l.manifest_id.clone(), l.kind)).one(&tx).await?;
                    let domain = tx.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                        "SELECT h.id FROM compaction_frozen_history h JOIN thread t ON t.id=h.owner_thread AND t.workspace_id=h.workspace_id JOIN workspace w ON w.id=h.workspace_id WHERE h.id=? AND h.ready=1 AND h.availability='resident'",
                        [l.manifest_id.clone().into()])).await?;
                    if current.as_ref() == Some(&l) && domain.is_some() {
                        layout::Entity::update_many().col_expr(layout::Column::Failed, Expr::val(1))
                            .col_expr(layout::Column::Pending, Expr::val(0))
                            .filter(layout::Column::ManifestId.eq(&l.manifest_id)).filter(layout::Column::Kind.eq(kind)).exec(&tx).await?;
                    }
                    tx.commit().await?;
                    Ok(())
                }).await?;
                return Err(error);
            }
            return Ok(true);
        }
    }
    let h = history::Entity::find()
        .filter(history::Column::Ready.eq(1))
        .filter(history::Column::Availability.eq("resident"))
        .filter(history::Column::StorageRegistered.eq(0))
        .order_by_asc(history::Column::Id)
        .one(&store.connection)
        .await?;
    if let Some(h) = h {
        let guard = store
            .compaction_acquire_frozen_builder_use(&h.workspace_id, &h.owner_thread, &h.id)
            .await?;
        let result = async {
            for kind in [0, 1] {
                register(
                    store,
                    &h,
                    kind,
                    candidate(&store.connection, &h, kind).await?,
                    &guard,
                )
                .await?;
            }
            store
                .run_serialized_write(|| async {
                    let tx = store.connection.begin().await?;
                    guard.validate_in(&tx, true).await?;
                    history::Entity::update_many()
                        .col_expr(history::Column::StorageRegistered, Expr::val(1))
                        .filter(history::Column::Id.eq(&h.id))
                        .exec(&tx)
                        .await?;
                    tx.commit().await?;
                    Ok(())
                })
                .await?;
            Ok(true)
        }
        .await;
        return guard.complete(result).await;
    }
    Ok(false)
}

async fn maintain_layout(store: &CrudStore, l: &layout::Model) -> Result<()> {
    let h = history::Entity::find_by_id(&l.manifest_id)
        .one(&store.connection)
        .await?
        .ok_or_else(|| anyhow::anyhow!("capture removed"))?;
    if l.active == 1 {
        return store.run_serialized_write(||async {
            let tx=store.connection.begin().await?;
            if history::Entity::find_by_id(&h.id).one(&tx).await?.as_ref()!=Some(&h) || layout::Entity::find_by_id((h.id.clone(),l.kind)).one(&tx).await?.as_ref()!=Some(l) {tx.commit().await?;return Ok(());}
            ensure!(tx.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"SELECT t.id FROM thread t JOIN workspace w ON w.id=t.workspace_id WHERE t.id=? AND t.workspace_id=?",[h.owner_thread.clone().into(),h.workspace_id.clone().into()])).await?.is_some(),"cleanup metadata domain unavailable");
            ensure!(h.availability=="resident" && h.ready==1,"cleanup metadata header unavailable");
            layout::Entity::update_many().col_expr(layout::Column::CleanupNext,Expr::val(l.cleanup_to)).col_expr(layout::Column::Pending,Expr::val(0)).filter(layout::Column::ManifestId.eq(&h.id)).filter(layout::Column::Kind.eq(l.kind)).exec(&tx).await?;
            super::compaction_frozen_reclaim::dirty(&tx,&h.id,h.storage_generation).await?;
            tx.commit().await?;Ok(())
        }).await;
    }
    let guard = store
        .compaction_acquire_frozen_builder_use(&h.workspace_id, &h.owner_thread, &h.id)
        .await?;
    let candidate_guard = if l.active == 0 {
        match &l.candidate {
            Some(base) => {
                match store
                    .compaction_acquire_frozen_builder_use(&h.workspace_id, &h.owner_thread, base)
                    .await
                {
                    Ok(candidate) => Some(candidate),
                    Err(error) => return guard.complete(Err(error)).await,
                }
            }
            None => None,
        }
    } else {
        None
    };
    let result = async {
        if l.active == 0 {
            if let Some(prefix) = l.copy_to {
                publish(store, &h, l, prefix, &guard, candidate_guard.as_ref()).await?;
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
                let a = rows(&store.connection, &guard, l.kind, matched).await?;
                let b = rows(
                    &store.connection,
                    candidate_guard.as_ref().unwrap(),
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
                                &guard,
                                candidate_guard.as_ref().unwrap(),
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
                publish(store, &h, l, matched, &guard, candidate_guard.as_ref()).await?;
            } else {
                store
                    .run_serialized_write(|| async {
                        let tx = store.connection.begin().await?;
                        guard.validate_in(&tx, true).await?;
                        if let Some(candidate) = &candidate_guard {
                            candidate.validate_in(&tx, true).await?;
                        }
                        let current = layout::Entity::find_by_id((h.id.clone(), l.kind))
                            .one(&tx)
                            .await?
                            .ok_or_else(|| anyhow::anyhow!("frozen layout unavailable"))?;
                        if current != *l {
                            tx.commit().await?;
                            return Ok(());
                        }
                        layout::Entity::update_many()
                            .col_expr(layout::Column::Compared, Expr::val(matched))
                            .filter(layout::Column::ManifestId.eq(&h.id))
                            .filter(layout::Column::Kind.eq(l.kind))
                            .filter(layout::Column::Compared.eq(l.compared))
                            .exec(&tx)
                            .await?;
                        tx.commit().await?;
                        Ok(())
                    })
                    .await?;
            }
        }

        Ok(())
    }
    .await;
    let result = match candidate_guard {
        Some(candidate) => candidate.complete(result).await,
        None => result,
    };
    guard.complete(result).await
}

#[cfg(test)]
#[path = "compaction_frozen_storage_tests.rs"]
mod tests;
