use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "compaction_checkpoint_event_input")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub checkpoint_id: String,
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub source_thread: String,
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub source_scope: String,
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub source_id: String,
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub source_version: String,
    #[sea_orm(column_type = "Text")]
    pub role: String,
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
