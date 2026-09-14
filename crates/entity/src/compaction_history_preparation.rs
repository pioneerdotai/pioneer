use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "compaction_history_preparation")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub thread_id: String,
    #[sea_orm(column_type = "Text")]
    pub turn_id: Option<String>,
    pub upper_turn_rowid: i64,
    pub turn_rowid: i64,
    pub source_kind: i64,
    pub after_sequence: i64,
    pub step: i64,
    pub inserted: i64,
    pub input_order: i64,
    pub event_order: i64,
    pub context_order: i64,
    pub ready: i64,
    #[sea_orm(
        belongs_to,
        from = "thread_id",
        to = "id",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    pub thread: HasOne<super::thread::Entity>,
}
impl ActiveModelBehavior for ActiveModel {}
