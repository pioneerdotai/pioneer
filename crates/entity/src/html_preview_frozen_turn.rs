//! Derived HTML preview lifetime metadata; no media payloads.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "html_preview_frozen_turn")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub manifest_id: String,
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub turn_id: String,
}

impl ActiveModelBehavior for ActiveModel {}
