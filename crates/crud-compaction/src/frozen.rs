use anyhow::{Result, ensure};
use pioneer_entity::compaction_frozen_history as history;
use pioneer_entity::{compaction_frozen_layout as layout, compaction_frozen_span as span};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Select};

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
