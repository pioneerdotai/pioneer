use crate::skill_pack_installation::unix_to_datetime;
use anyhow::{Context, Result};
use pioneer_entity::skill_dependency_snapshot;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set, sea_query::OnConflict,
};

const SKILL_DEPENDENCY_INSERT_BATCH_SIZE: usize = 32;

#[derive(Debug, Clone)]
pub struct PreparedSkillDependencySnapshot {
    id: String,
    turn_id: Option<String>,
    skill_id: String,
    skill_owner: Option<String>,
    skill_slug: String,
    source_kind: String,
    diagnostics_json: String,
    created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
}

pub fn prepare_skill_dependency_snapshot_idempotent(
    id: &str,
    turn_id: &str,
    record: &SkillDependencySnapshotRecord,
) -> PreparedSkillDependencySnapshot {
    PreparedSkillDependencySnapshot {
        id: id.to_owned(),
        turn_id: Some(turn_id.to_owned()),
        skill_id: record.skill_id.to_string(),
        skill_owner: record.skill_owner.clone(),
        skill_slug: record.skill_slug.clone(),
        source_kind: record.source_kind.clone(),
        diagnostics_json: record.diagnostics_json.clone(),
        created_at: unix_to_datetime(record.created_at_unix),
    }
}

pub async fn insert_skill_dependency_snapshot<C: ConnectionTrait>(
    db: &C,
    record: &SkillDependencySnapshotRecord,
) -> Result<()> {
    skill_dependency_snapshot::Entity::insert(skill_dependency_snapshot::ActiveModel {
        id: Set(pioneer_protocol::generate_id(21)),
        turn_id: Set(record.turn_id.clone()),
        skill_id: Set(record.skill_id.to_string()),
        skill_owner: Set(record.skill_owner.clone()),
        skill_slug: Set(record.skill_slug.clone()),
        source_kind: Set(record.source_kind.clone()),
        diagnostics_json: Set(record.diagnostics_json.clone()),
        created_at: Set(unix_to_datetime(record.created_at_unix)),
    })
    .exec(db)
    .await
    .with_context(|| {
        format!(
            "failed to insert skill dependency snapshot `{}` ({})",
            record.skill_id, record.source_kind
        )
    })?;

    Ok(())
}

pub async fn insert_prepared_skill_dependency_snapshots_idempotent<C: ConnectionTrait>(
    db: &C,
    prepared: Vec<PreparedSkillDependencySnapshot>,
) -> Result<()> {
    for batch in prepared.chunks(SKILL_DEPENDENCY_INSERT_BATCH_SIZE) {
        skill_dependency_snapshot::Entity::insert_many(batch.iter().map(|prepared| {
            skill_dependency_snapshot::ActiveModel {
                id: Set(prepared.id.clone()),
                turn_id: Set(prepared.turn_id.clone()),
                skill_id: Set(prepared.skill_id.clone()),
                skill_owner: Set(prepared.skill_owner.clone()),
                skill_slug: Set(prepared.skill_slug.clone()),
                source_kind: Set(prepared.source_kind.clone()),
                diagnostics_json: Set(prepared.diagnostics_json.clone()),
                created_at: Set(prepared.created_at),
            }
        }))
        .on_conflict(
            OnConflict::column(skill_dependency_snapshot::Column::Id)
                .do_nothing()
                .to_owned(),
        )
        .exec_without_returning(db)
        .await
        .context("failed to insert prepared skill dependency snapshot batch")?;
    }
    Ok(())
}

pub async fn list_turn_skill_dependency_snapshots<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Vec<skill_dependency_snapshot::Model>> {
    skill_dependency_snapshot::Entity::find()
        .filter(skill_dependency_snapshot::Column::TurnId.eq(turn_id.to_owned()))
        .order_by_asc(skill_dependency_snapshot::Column::SkillId)
        .all(db)
        .await
        .with_context(|| format!("failed to query skill dependency snapshots for turn `{turn_id}`"))
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SkillDependencySnapshotRecord {
    pub turn_id: Option<String>,
    pub skill_id: pioneer_protocol::SkillId,
    pub skill_owner: Option<String>,
    pub skill_slug: String,
    pub source_kind: String,
    pub diagnostics_json: String,
    pub created_at_unix: i64,
}

pub fn skill_dependency_snapshot_record_from_model(
    model: pioneer_entity::skill_dependency_snapshot::Model,
) -> Result<SkillDependencySnapshotRecord> {
    let skill_id = pioneer_protocol::SkillId::new(model.skill_id.clone()).with_context(|| {
        format!(
            "skill dependency snapshot key `{}` is not a valid SkillId",
            model.skill_id
        )
    })?;
    Ok(SkillDependencySnapshotRecord {
        turn_id: model.turn_id,
        skill_id,
        skill_owner: model.skill_owner,
        skill_slug: model.skill_slug,
        source_kind: model.source_kind,
        diagnostics_json: model.diagnostics_json,
        created_at_unix: model.created_at.timestamp(),
    })
}
