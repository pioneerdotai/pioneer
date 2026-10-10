use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
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
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}
