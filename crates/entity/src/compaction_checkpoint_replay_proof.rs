//! Frozen-history protocol metadata.
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "compaction_checkpoint_replay_proof")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub checkpoint_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub ordinal: i64,
    #[sea_orm(column_type = "Text")]
    pub covered_thread: String,
    #[sea_orm(column_type = "Text")]
    pub covered_scope: String,
    #[sea_orm(column_type = "Text")]
    pub covered_id: String,
    #[sea_orm(column_type = "Text")]
    pub covered_version: String,
    #[sea_orm(column_type = "Text")]
    pub replay_thread: String,
    #[sea_orm(column_type = "Text")]
    pub replay_scope: String,
    #[sea_orm(column_type = "Text")]
    pub replay_id: String,
    #[sea_orm(column_type = "Text")]
    pub replay_version: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub tool_item_id: Option<String>,
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
