//! Durable idempotency identity for native provider Turn admission.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "turn_admission")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub turn_id: String,
    pub thread_id: String,
    pub workspace_id: String,
    pub request_digest: String,
    pub created_at: DateTimeWithTimeZone,
}

impl ActiveModelBehavior for ActiveModel {}
