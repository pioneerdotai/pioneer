use anyhow::{Context, Result};
use pioneer_entity::turn_cli_runtime_instruction;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ConnectionTrait, EntityTrait, Set};
use std::fmt;

#[derive(Clone, PartialEq, Eq)]
pub struct CliRuntimeInstructionProjectionRecord {
    pub turn_id: String,
    pub runtime_kind: String,
    pub transport_kind: String,
    pub instruction_text: String,
    pub instruction_fingerprint: String,
    pub section_ids_json: String,
    pub compiler_version: String,
    pub created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    pub updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
}

impl fmt::Debug for CliRuntimeInstructionProjectionRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CliRuntimeInstructionProjectionRecord")
            .field("turn_id", &self.turn_id)
            .field("runtime_kind", &self.runtime_kind)
            .field("transport_kind", &self.transport_kind)
            .field("instruction_text", &"[REDACTED]")
            .field("instruction_fingerprint", &self.instruction_fingerprint)
            .field("section_ids_json", &self.section_ids_json)
            .field("compiler_version", &self.compiler_version)
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct NewCliRuntimeInstructionProjection {
    pub turn_id: String,
    pub runtime_kind: String,
    pub transport_kind: String,
    pub instruction_text: String,
    pub instruction_fingerprint: String,
    pub section_ids_json: String,
    pub compiler_version: String,
    pub created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    pub updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
}

impl fmt::Debug for NewCliRuntimeInstructionProjection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NewCliRuntimeInstructionProjection")
            .field("turn_id", &self.turn_id)
            .field("runtime_kind", &self.runtime_kind)
            .field("transport_kind", &self.transport_kind)
            .field("instruction_text", &"[REDACTED]")
            .field("instruction_fingerprint", &self.instruction_fingerprint)
            .field("section_ids_json", &self.section_ids_json)
            .field("compiler_version", &self.compiler_version)
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .finish()
    }
}

pub async fn insert_cli_runtime_instruction_projection_if_absent<C: ConnectionTrait>(
    db: &C,
    projection: NewCliRuntimeInstructionProjection,
) -> Result<CliRuntimeInstructionProjectionRecord> {
    let turn_id = projection.turn_id.clone();
    turn_cli_runtime_instruction::Entity::insert(turn_cli_runtime_instruction::ActiveModel {
        turn_id: Set(projection.turn_id),
        runtime_kind: Set(projection.runtime_kind),
        transport_kind: Set(projection.transport_kind),
        instruction_text: Set(projection.instruction_text),
        instruction_fingerprint: Set(projection.instruction_fingerprint),
        section_ids_json: Set(projection.section_ids_json),
        compiler_version: Set(projection.compiler_version),
        created_at: Set(projection.created_at),
        updated_at: Set(projection.updated_at),
    })
    .on_conflict(
        OnConflict::column(turn_cli_runtime_instruction::Column::TurnId)
            .do_nothing()
            .to_owned(),
    )
    .try_insert()
    .exec(db)
    .await
    .context("failed to persist CLI runtime instruction projection")?;

    find_cli_runtime_instruction_projection(db, turn_id.as_str())
        .await?
        .context("persisted CLI runtime instruction projection is missing")
}

pub async fn find_cli_runtime_instruction_projection<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<CliRuntimeInstructionProjectionRecord>> {
    let row = turn_cli_runtime_instruction::Entity::find_by_id(turn_id.to_owned())
        .one(db)
        .await
        .context("failed to query CLI runtime instruction projection")?;
    Ok(row.map(record_from_model))
}

fn record_from_model(
    model: turn_cli_runtime_instruction::Model,
) -> CliRuntimeInstructionProjectionRecord {
    CliRuntimeInstructionProjectionRecord {
        turn_id: model.turn_id,
        runtime_kind: model.runtime_kind,
        transport_kind: model.transport_kind,
        instruction_text: model.instruction_text,
        instruction_fingerprint: model.instruction_fingerprint,
        section_ids_json: model.section_ids_json,
        compiler_version: model.compiler_version,
        created_at: model.created_at,
        updated_at: model.updated_at,
    }
}
