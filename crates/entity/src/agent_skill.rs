//! Logical Agent skill with a pointer to its active immutable version.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "agent_skill")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    #[sea_orm(unique_key = "uq_agent_skill_workspace_slug")]
    pub workspace_id: String,
    #[sea_orm(unique_key = "uq_agent_skill_workspace_slug")]
    pub slug: String,
    pub active_version_id: Option<String>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
    #[sea_orm(
        belongs_to,
        relation_enum = "Workspace",
        from = "workspace_id",
        to = "id",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    pub workspace: HasOne<super::workspace::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ActiveVersion",
        from = "active_version_id",
        to = "id",
        on_update = "NoAction",
        on_delete = "SetNull"
    )]
    pub active_version: HasOne<super::agent_skill_version::Entity>,
    #[sea_orm(has_many, relation_enum = "Versions", via_rel = "Skill")]
    pub versions: HasMany<super::agent_skill_version::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
