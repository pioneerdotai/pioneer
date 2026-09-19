use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "native_event_cleanup_scheduler")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub singleton: i64,
    pub new_jobs_since_served: i64,
}

impl ActiveModelBehavior for ActiveModel {}
