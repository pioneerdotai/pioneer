//! `SeaORM` entity for durable native-agent terminal obligations.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "native_terminal_effect_outbox")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub effect_id: String,
    pub batch_id: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub runtime_generation: i64,
    pub effect_kind: String,
    pub gate_kind: String,
    #[sea_orm(column_type = "Text")]
    pub payload_json: String,
    pub payload_sha256: String,
    pub payload_identity_sha256: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub handler_checkpoint_json: Option<String>,
    pub handler_checkpoint_sha256: Option<String>,
    pub status: String,
    pub accepted_candidate_id: Option<String>,
    pub attempt_count: i64,
    pub max_attempts: i64,
    pub last_error_code: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub last_error_message: Option<String>,
    pub next_run_at: Option<DateTimeWithTimeZone>,
    pub claim_token: Option<String>,
    pub claim_expires_at: Option<DateTimeWithTimeZone>,
    pub terminal_committed_at: Option<DateTimeWithTimeZone>,
    pub completed_at: Option<DateTimeWithTimeZone>,
    pub prepared_at: DateTimeWithTimeZone,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
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
