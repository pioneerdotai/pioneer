use pioneer_entity::html_preview_frozen_result as receipt;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Select};

pub fn find_frozen_result(manifest: impl AsRef<str>, ordinal: i64) -> Select<receipt::Entity> {
    receipt::Entity::find()
        .filter(receipt::Column::ManifestId.eq(manifest.as_ref()))
        .filter(receipt::Column::Ordinal.eq(ordinal))
}
