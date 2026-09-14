use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "tool_output_chunk")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub ordinal: i64,
    #[sea_orm(unique, column_type = "Text")]
    pub id: String,
    pub turn_id: String,
    pub item_id: String,
    pub stream: String,
    pub text: String,
    pub metadata: Option<String>,
}
impl ActiveModelBehavior for ActiveModel {}
