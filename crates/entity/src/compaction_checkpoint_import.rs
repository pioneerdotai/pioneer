use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "compaction_checkpoint_import")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub checkpoint_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub import_ordinal: i64,
    pub message_ordinal: i64,
    #[sea_orm(column_type = "Text")]
    pub target_source_thread: String,
    #[sea_orm(column_type = "Text")]
    pub context_thread: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub target_checkpoint_json: Option<String>,
    #[sea_orm(column_type = "Text")]
    pub proof_json: String,
    pub bytes: i64,
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
