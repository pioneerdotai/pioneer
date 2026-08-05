//! Durable accepted final response and its terminal commit state.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "turn_finalization")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub turn_id: String,
    pub thread_id: String,
    pub workspace_id: String,
    pub generation: i64,
    #[sea_orm(unique)]
    pub item_id: String,
    #[sea_orm(column_type = "Text")]
    pub item_json: String,
    pub item_digest: String,
    pub status: String,
    pub prepared_at: DateTimeWithTimeZone,
    pub committed_at: Option<DateTimeWithTimeZone>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

impl ActiveModelBehavior for ActiveModel {}
