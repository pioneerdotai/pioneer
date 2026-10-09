//! Frozen-history protocol metadata.
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "compaction_checkpoint_event_input_proof")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub checkpoint_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub ordinal: i64,
    #[sea_orm(column_type = "Text")]
    pub source_thread: String,
    #[sea_orm(column_type = "Text")]
    pub source_scope: String,
    #[sea_orm(column_type = "Text")]
    pub source_id: String,
    #[sea_orm(column_type = "Text")]
    pub source_version: String,
    #[sea_orm(column_type = "Text")]
    pub role: String,
    #[sea_orm(
        belongs_to,
        from = "checkpoint_id",
        to = "checkpoint_id",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    pub compaction_checkpoint_proof: HasOne<super::compaction_checkpoint_proof::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
