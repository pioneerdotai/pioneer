//! Derived HTML preview lifetime metadata; no media payloads.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "html_preview_index_backfill")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub singleton: i64,
    pub complete: i64,
    pub inputs_complete: i64,
}

impl ActiveModelBehavior for ActiveModel {}
