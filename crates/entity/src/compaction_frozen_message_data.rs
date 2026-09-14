use sea_orm::entity::prelude::*;
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "compaction_frozen_message_data")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub manifest_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub ordinal: i64,
    pub reference_json: String,
    pub bytes: i64,
}
impl ActiveModelBehavior for ActiveModel {}
