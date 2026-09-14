use sea_orm::entity::prelude::*;
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "compaction_frozen_span")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub manifest_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub kind: i64,
    #[sea_orm(primary_key, auto_increment = false)]
    pub start: i64,
    pub end: i64,
    pub source_manifest: String,
}
impl ActiveModelBehavior for ActiveModel {}
