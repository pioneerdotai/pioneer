//! Durable ledger row for an eligible completed conversation exchange.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "self_improvement_source_turn")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub workspace_id: String,
    pub thread_id: String,
    #[sea_orm(unique)]
    pub turn_id: String,
    #[sea_orm(unique)]
    pub task_delivery_id: Option<String>,
    #[sea_orm(unique)]
    pub terminal_event_id: String,
    pub terminal_at: DateTimeWithTimeZone,
    pub created_at: DateTimeWithTimeZone,
    #[sea_orm(
        belongs_to,
        from = "workspace_id",
        to = "id",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    pub workspace: HasOne<super::workspace::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
