use sea_orm::entity::prelude::*;
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "compaction_frozen_layout")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub manifest_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub kind: i64,
    pub active: i64,
    pub pending: i64,
    pub candidate: Option<String>,
    pub compared: i64,
    pub copy_to: Option<i64>,
    pub copy_next: i64,
    pub cleanup_to: i64,
    pub cleanup_next: i64,
    pub failed: i64,
}
impl ActiveModelBehavior for ActiveModel {}
