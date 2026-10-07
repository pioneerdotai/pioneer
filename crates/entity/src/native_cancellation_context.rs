//! Immutable originating cancellation description; one row per turn.
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "native_cancellation_context")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub turn_id: String,
    pub thread_id: String,
    pub workspace_id: String,
    pub execution_owner_id: String,
    #[sea_orm(column_type = "Text")]
    pub context_json: String,
    pub context_sha256: String,
    pub accepted_event_id: Option<String>,
    pub created_at: DateTimeWithTimeZone,
    #[sea_orm(
        belongs_to,
        from = "turn_id",
        to = "id",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    pub turn: HasOne<super::turn::Entity>,
}
impl ActiveModelBehavior for ActiveModel {}
