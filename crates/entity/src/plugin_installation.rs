use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "plugin_installation")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub version: Option<String>,
    #[sea_orm(unique)]
    pub source_upload_id: String,
    pub package_path: String,
    pub data_path: String,
    pub package_fingerprint: String,
    pub enabled: bool,
    pub state: String,
    pub revision: i64,
    pub pending_json: Option<String>,
    pub last_error: Option<String>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}
impl ActiveModelBehavior for ActiveModel {}
