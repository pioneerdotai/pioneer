//! Immutable runtime-visible version of one Agent skill.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "agent_skill_version")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    #[sea_orm(unique_key = "uq_agent_skill_version_number")]
    #[sea_orm(unique_key = "uq_agent_skill_version_fingerprint")]
    pub skill_id: String,
    #[sea_orm(unique_key = "uq_agent_skill_version_number")]
    pub version_number: i64,
    #[sea_orm(unique_key = "uq_agent_skill_version_run_candidate")]
    pub source_run_id: Option<String>,
    pub parent_version_id: Option<String>,
    #[sea_orm(unique_key = "uq_agent_skill_version_run_candidate")]
    pub candidate_key: String,
    pub display_name: String,
    #[sea_orm(column_type = "Text")]
    pub skill_markdown: String,
    #[sea_orm(column_type = "Text")]
    pub instruction_body: String,
    #[sea_orm(column_type = "Text")]
    pub when_to_use: String,
    #[sea_orm(column_type = "Text")]
    pub when_not_to_use: String,
    #[sea_orm(unique_key = "uq_agent_skill_version_fingerprint")]
    pub fingerprint: String,
    #[sea_orm(column_type = "Text")]
    pub source_turn_ids_json: String,
    pub created_at: DateTimeWithTimeZone,
    #[sea_orm(
        belongs_to,
        relation_enum = "Skill",
        relation_reverse = "Versions",
        from = "skill_id",
        to = "id",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    pub skill: HasOne<super::agent_skill::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "SourceRun",
        from = "source_run_id",
        to = "id",
        on_update = "NoAction",
        on_delete = "SetNull"
    )]
    pub source_run: HasOne<super::self_improvement_run::Entity>,
    #[sea_orm(
        self_ref,
        relation_enum = "Parent",
        relation_reverse = "Children",
        from = "parent_version_id",
        to = "id",
        on_update = "NoAction",
        on_delete = "SetNull"
    )]
    pub parent: HasOne<Entity>,
    #[sea_orm(self_ref, relation_enum = "Children", relation_reverse = "Parent")]
    pub children: HasMany<Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
