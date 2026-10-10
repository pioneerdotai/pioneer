//! Entity key represents the existing logical expression UNIQUE identity.
//! The nullable tool component must be filtered with IS NULL / equality; generic
//! find/update/delete-by-id uses = NULL and must not be used for this table.
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "compaction_checkpoint_replay_alias")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub checkpoint_id: String,
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub covered_thread: String,
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub covered_scope: String,
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub covered_id: String,
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub covered_version: String,
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub replay_thread: String,
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub replay_scope: String,
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub replay_id: String,
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub replay_version: String,
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text", nullable)]
    pub tool_item_id: NullableToolItemId,
    #[serde(skip)]
    #[sea_orm(
        belongs_to,
        from = "checkpoint_id",
        to = "id",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    pub compaction_checkpoint: HasOne<super::compaction_checkpoint::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}

// SeaORM's composite PrimaryKey requires TryFromU64 for every component;
// Option<String> lacks that trait. This transparent nullable value preserves
// SQL NULL and the existing serialized column without a surrogate key.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NullableToolItemId(pub Option<String>);

impl sea_orm::TryFromU64 for NullableToolItemId {
    fn try_from_u64(_: u64) -> Result<Self, DbErr> {
        Err(DbErr::ConvertFromU64("NullableToolItemId"))
    }
}

impl From<NullableToolItemId> for sea_orm::Value {
    fn from(value: NullableToolItemId) -> Self {
        value.0.into()
    }
}
impl sea_orm::TryGetable for NullableToolItemId {
    fn try_get_by<I: sea_orm::ColIdx>(
        row: &sea_orm::QueryResult,
        index: I,
    ) -> Result<Self, sea_orm::TryGetError> {
        <Option<String> as sea_orm::TryGetable>::try_get_by(row, index).map(Self)
    }
}
impl sea_orm::sea_query::ValueType for NullableToolItemId {
    fn try_from(value: sea_orm::Value) -> Result<Self, sea_orm::sea_query::ValueTypeErr> {
        <Option<String> as sea_orm::sea_query::ValueType>::try_from(value).map(Self)
    }
    fn type_name() -> String {
        "NullableToolItemId".into()
    }
    fn array_type() -> sea_orm::sea_query::ArrayType {
        sea_orm::sea_query::ArrayType::String
    }
    fn column_type() -> sea_orm::sea_query::ColumnType {
        sea_orm::sea_query::ColumnType::Text
    }
}
impl sea_orm::sea_query::Nullable for NullableToolItemId {
    fn null() -> sea_orm::Value {
        sea_orm::Value::String(None)
    }
}
