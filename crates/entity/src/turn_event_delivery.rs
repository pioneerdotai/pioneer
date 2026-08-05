//! `SeaORM` Entity for the durable optional-consumer outbox.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "turn_event_delivery")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub event_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub sequence: i64,
    pub consumer: String,
    pub status: String,
    pub attempt_count: i64,
    #[sea_orm(column_type = "Text", nullable)]
    pub last_error: Option<String>,
    pub next_run_at: DateTimeWithTimeZone,
    pub claim_token: Option<String>,
    pub claim_expires_at: Option<DateTimeWithTimeZone>,
    pub delivered_at: Option<DateTimeWithTimeZone>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

impl ActiveModelBehavior for ActiveModel {}
