//! Read-side verification of bounded frozen-copy publication.

use anyhow::{Result, ensure};
use pioneer_entity::{
    compaction_frozen_history as history, compaction_frozen_layout as layout,
    compaction_frozen_span as span, compaction_frozen_sweep_consumer as consumer,
};
use sea_orm::sea_query::LockType;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};

const ROWS: u64 = 128;

pub async fn lock_copy_headers<C: ConnectionTrait>(
    db: &C,
    h: &history::Model,
    l: &layout::Model,
) -> Result<()> {
    let ids = std::iter::once(h.id.as_str()).chain(l.candidate.as_deref());
    let locked = history::Entity::find()
        .select_only()
        .column(history::Column::Id)
        .filter(history::Column::Id.is_in(ids))
        .order_by_asc(history::Column::Id)
        .lock(LockType::NoKeyUpdate)
        .into_tuple::<String>()
        .all(db)
        .await?;
    ensure!(
        locked.len() == 1 + usize::from(l.candidate.is_some()),
        "frozen copy header disappeared"
    );
    Ok(())
}

pub async fn span_revision<C: ConnectionTrait>(db: &C, id: &str, kind: i64) -> Result<i64> {
    consumer::Entity::find()
        .select_only()
        .column(consumer::Column::Revision)
        .filter(consumer::Column::ManifestId.eq(id))
        .filter(consumer::Column::Kind.eq(kind))
        .into_tuple::<i64>()
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("frozen span revision missing"))
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct CopyRevisions {
    target: i64,
    candidate: Option<i64>,
}

impl CopyRevisions {
    pub async fn read<C: ConnectionTrait>(
        db: &C,
        h: &history::Model,
        l: &layout::Model,
    ) -> Result<Self> {
        let target = span_revision(db, &h.id, l.kind).await?;
        let candidate = match &l.candidate {
            Some(base) => Some(span_revision(db, base, l.kind).await?),
            None => None,
        };
        Ok(Self { target, candidate })
    }
}

pub async fn checked_copy_dependencies<C: ConnectionTrait>(
    db: &C,
    h: &history::Model,
    l: &layout::Model,
) -> Result<()> {
    let current = history::Entity::find_by_id(&h.id)
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("copy logical capture missing"))?;
    crate::frozen::logical_bounds(&current)?;
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
        crate::frozen::logical_bounds(&b)?;
        ensure!(
            b.ready == 1 && b.workspace_id == h.workspace_id && b.owner_thread == h.owner_thread,
            "copy logical base unavailable"
        );
        let bl = crate::frozen::find_layout(base.clone(), l.kind)
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
/// Private existing-worker progress for restart verification. Pending layout
/// dependencies protect all backing sets; no connection/permit or reader is
/// retained during backoff. New captures use the identical bounded validator.
pub struct StagedCopyCheck {
    header: history::Model,
    pub layout: layout::Model,
    after: i64,
    next: i64,
    base_layout: Option<crate::frozen::LayoutScan>,
    base_started: bool,
    pub revisions: Option<CopyRevisions>,
}
impl StagedCopyCheck {
    pub fn new(header: history::Model, layout: layout::Model) -> Result<Self> {
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
            revisions: None,
        })
    }
    pub fn header(&self) -> &history::Model {
        &self.header
    }

    pub async fn step(&mut self, db: &impl ConnectionTrait) -> Result<bool> {
        if self.revisions.is_none() {
            let revisions = CopyRevisions::read(db, &self.header, &self.layout).await?;
            self.revisions = Some(revisions);
        }
        checked_copy_dependencies(db, &self.header, &self.layout).await?;
        if !self.base_started {
            if let Some(base) = &self.layout.candidate {
                let header = history::Entity::find_by_id(base)
                    .one(db)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("copy base removed"))?;
                self.base_layout = Some(crate::frozen::LayoutScan::new(header, 0, 2)?);
            }
            // Publish this stage only after every fallible base read/setup.
            // No candidate is a separate completed (empty) base stage.
            self.base_started = true;
            return Ok(false);
        }
        if let Some(scan) = self.base_layout.as_mut() {
            scan.step(db).await?;
            if scan.done() {
                self.base_layout = None;
            }
            return Ok(false);
        }
        let current = crate::frozen::find_layout(self.header.id.clone(), self.layout.kind)
            .one(db)
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
            .all(db)
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
                .one(db)
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
