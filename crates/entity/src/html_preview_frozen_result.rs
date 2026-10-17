//! Derived HTML preview lifetime metadata; no media payloads.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "html_preview_frozen_result")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub manifest_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub ordinal: i64,
    #[sea_orm(column_type = "Text")]
    pub reference_json: String,
    #[sea_orm(column_type = "Text")]
    pub turn_id: String,
    #[sea_orm(column_type = "Text")]
    pub source_id: String,
    #[sea_orm(column_type = "Text")]
    pub source_version: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub archive_wire_sha256: Option<String>,
}

impl ActiveModelBehavior for ActiveModel {}
