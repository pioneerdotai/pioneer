//! Derived HTML preview lifetime metadata; no media payloads.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "html_preview_frozen_index")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub manifest_id: String,
    #[sea_orm(column_type = "Text")]
    pub workspace_id: String,
    #[sea_orm(column_type = "Text")]
    pub identity_sha256: String,
    pub message_count: i64,
    pub ready: i64,
    pub expired: i64,
    pub next_ordinal: i64,
    pub complete: i64,
    pub unknown: i64,
}

impl ActiveModelBehavior for ActiveModel {}
