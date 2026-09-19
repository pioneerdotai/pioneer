//! No Turn foreign key: persisted native events may refer to a missing Turn.
//! Such jobs wait without authorizing deletion, preserving the retention policy.
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "native_event_cleanup_job")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub turn_id: String,
    #[sea_orm(column_type = "Text")]
    pub state: String,
    pub available_at: i64,
    pub last_served_at: i64,
    pub revision: i64,
    #[sea_orm(column_type = "Text", nullable)]
    pub last_error: Option<String>,
}

impl ActiveModelBehavior for ActiveModel {}
