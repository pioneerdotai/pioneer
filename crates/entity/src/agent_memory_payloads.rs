//! `SeaORM` Entity, aligned manually while execution/codegen is prohibited.
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "agent_memory_payloads")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub memory_id: String,
    pub scope_kind: String,
    pub scope_key_hash: String,
    #[sea_orm(column_type = "Text")]
    pub content: String,
    pub revision: i64,
}
impl ActiveModelBehavior for ActiveModel {}
