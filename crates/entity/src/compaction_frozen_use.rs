//! Frozen-history protocol metadata.
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "compaction_frozen_use")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub use_id: String,
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub manifest_id: String,
    pub storage_generation: i64,
    #[sea_orm(column_type = "Text")]
    pub purpose: String,
    #[sea_orm(column_type = "Text")]
    pub workspace_id: String,
    #[sea_orm(
        belongs_to,
        from = "manifest_id",
        to = "id",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    pub compaction_frozen_history: HasOne<super::compaction_frozen_history::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
