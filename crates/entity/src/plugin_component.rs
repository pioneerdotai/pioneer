use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "plugin_component")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub plugin_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub kind: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub member_key: String,
    pub member_path: Option<String>,
    #[sea_orm(unique)]
    pub skill_id: Option<String>,
    #[sea_orm(unique)]
    pub mcp_installation_id: Option<String>,
    pub package_fingerprint: Option<String>,
    pub status: String,
    pub diagnostic: Option<String>,
    pub override_fields_json: String,
}
impl ActiveModelBehavior for ActiveModel {}
