use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
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
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}
