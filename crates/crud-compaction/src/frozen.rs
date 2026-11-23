use anyhow::{Result, ensure};
use pioneer_entity::compaction_frozen_history as history;
use pioneer_entity::{compaction_frozen_layout as layout, compaction_frozen_span as span};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, Select};

pub fn find_layout(manifest: impl AsRef<str>, kind: i64) -> Select<layout::Entity> {
    layout::Entity::find()
        .filter(layout::Column::ManifestId.eq(manifest.as_ref()))
        .filter(layout::Column::Kind.eq(kind))
}

pub fn find_span(manifest: impl AsRef<str>, kind: i64, start: i64) -> Select<span::Entity> {
    span::Entity::find()
        .filter(span::Column::ManifestId.eq(manifest.as_ref()))
        .filter(span::Column::Kind.eq(kind))
        .filter(span::Column::Start.eq(start))
}

/// R1: both streams must be valid, even when the caller reads only one.
pub fn logical_bounds(h: &history::Model) -> Result<(i64, i64)> {
    let bounds = header_bounds(h)?;
    ensure!(
        h.expired == 0,
        "frozen_input_expired: сохранённый input освобождён; точный replay недоступен"
    );
    Ok(bounds)
}

/// Counter validation also applies to expired physical backing metadata.
pub fn header_bounds(h: &history::Model) -> Result<(i64, i64)> {
    ensure!(
        matches!(h.expired, 0 | 1)
            && matches!(h.ready, 0 | 1)
            && h.message_count >= 0
            && h.import_count >= 0
            && (0..=h.message_count).contains(&h.next_ordinal)
            && (0..=h.import_count).contains(&h.next_import)
            && (h.ready == 0
                || (h.next_ordinal == h.message_count && h.next_import == h.import_count)),
        "invalid frozen history counters"
    );
    Ok(if h.ready == 1 {
        (h.message_count, h.import_count)
    } else {
        (h.next_ordinal, h.next_import)
    })
}

/// One layout metadata page per step; reused by foreground acquire, proof
/// extraction and lifetime maintenance. No DB capacity is retained.
pub struct LayoutScan {
    h: history::Model,
    bounds: (i64, i64),
    kind: i64,
    end_kind: i64,
    layout_read: bool,
    after: i64,
    next: i64,
}
impl LayoutScan {
    pub fn new(h: history::Model, start_kind: i64, end_kind: i64) -> Result<Self> {
        ensure!(
            0 <= start_kind && start_kind < end_kind && end_kind <= 2,
            "invalid layout scan kinds"
        );
        let bounds = header_bounds(&h)?;
        Ok(Self {
            h,
            bounds,
            kind: start_kind,
            end_kind,
            layout_read: false,
            after: -1,
            next: 0,
        })
    }
    pub fn done(&self) -> bool {
        self.kind == self.end_kind
    }
    pub async fn step(&mut self, db: &impl ConnectionTrait) -> Result<()> {
        use pioneer_entity::compaction_frozen_span as span;
        use sea_orm::QueryOrder;
        if self.done() {
            return Ok(());
        }
        let bound = if self.kind == 0 {
            self.bounds.0
        } else {
            self.bounds.1
        };
        if !self.layout_read {
            let l = find_layout(self.h.id.clone(), self.kind).one(db).await?;
            let active = if let Some(l) = l {
                ensure!(
                    matches!(l.active, 0 | 1)
                        && matches!(l.pending, 0 | 1)
                        && l.failed == 0
                        && (0..=bound).contains(&l.compared)
                        && (0..=bound).contains(&l.copy_next)
                        && l.copy_to.is_none_or(|end| (0..=bound).contains(&end))
                        && (0..=bound).contains(&l.cleanup_to)
                        && (0..=l.cleanup_to).contains(&l.cleanup_next),
                    "invalid frozen layout state"
                );
                l.active == 1
            } else {
                ensure!(
                    self.h.storage_registered == 0,
                    "registered frozen layout missing"
                );
                false
            };
            self.layout_read = active;
            if !active {
                self.advance();
            }
            return Ok(());
        }
        let page = span::Entity::find()
            .filter(span::Column::ManifestId.eq(&self.h.id))
            .filter(span::Column::Kind.eq(self.kind))
            .filter(span::Column::Start.gt(self.after))
            .order_by_asc(span::Column::Start)
            .limit(64)
            .all(db)
            .await?;
        for s in &page {
            ensure!(
                s.start == self.next
                    && s.end > s.start
                    && s.end <= bound
                    && !s.source_manifest.is_empty(),
                "invalid frozen logical span"
            );
            self.next = s.end;
            self.after = s.start;
        }
        if page.len() < 64 {
            ensure!(self.next == bound, "frozen layout is incomplete");
            self.advance();
        }
        Ok(())
    }
    fn advance(&mut self) {
        self.kind += 1;
        self.layout_read = false;
        self.after = -1;
        self.next = 0;
    }
}
