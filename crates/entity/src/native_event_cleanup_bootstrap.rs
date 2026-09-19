use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "native_event_cleanup_bootstrap")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub singleton: i64,
    #[sea_orm(column_type = "Text", nullable)]
    pub cursor_id: Option<String>,
    pub complete: bool,
}

impl ActiveModelBehavior for ActiveModel {}
