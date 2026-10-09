//! Frozen-history protocol metadata.
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "compaction_checkpoint_proof")]
pub struct Model {
    #[sea_orm(has_many)]
    pub compaction_checkpoint_replay_proofs:
        HasMany<super::compaction_checkpoint_replay_proof::Entity>,
    #[sea_orm(has_many)]
    pub compaction_checkpoint_event_input_proofs:
        HasMany<super::compaction_checkpoint_event_input_proof::Entity>,

    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub checkpoint_id: String,
    #[sea_orm(column_type = "Text")]
    pub operation_id: String,
    #[sea_orm(column_type = "Text")]
    pub owner: String,
    #[sea_orm(column_type = "Text")]
    pub workspace_id: String,
    #[sea_orm(column_type = "Text")]
    pub thread_id: String,
    #[sea_orm(column_type = "Text")]
    pub checkpoint_identity_sha256: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub previous: Option<String>,
    pub projection_version: i64,
    pub format_version: i64,
    #[sea_orm(column_type = "Text")]
    pub coverage_domain: String,
    #[sea_orm(column_type = "Text")]
    pub origin_manifest_id: String,
    pub origin_message_count: i64,
    #[sea_orm(column_type = "Text")]
    pub origin_identity_sha256: String,
    pub origin_import_count: i64,
    #[sea_orm(column_type = "Text")]
    pub origin_imports_sha256: String,
    pub coverage_count: i64,
    #[sea_orm(column_type = "Text")]
    pub coverage_sha256: String,
    pub alias_count: i64,
    #[sea_orm(column_type = "Text")]
    pub aliases_sha256: String,
    pub next_alias: i64,
    pub evidence_count: i64,
    #[sea_orm(column_type = "Text")]
    pub evidence_sha256: String,
    pub next_evidence: i64,
    pub proof_format: i64,
    #[sea_orm(column_type = "Text")]
    pub state: String,
    #[sea_orm(
        belongs_to,
        from = "checkpoint_id",
        to = "id",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    pub compaction_checkpoint: HasOne<super::compaction_checkpoint::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
