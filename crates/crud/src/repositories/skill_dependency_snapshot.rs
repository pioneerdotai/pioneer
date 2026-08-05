use crate::util::unix_to_datetime;
use anyhow::{Context, Result, bail};
use pioneer_entity::skill_dependency_snapshot;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

pub async fn insert_skill_dependency_snapshot<C: ConnectionTrait>(
    db: &C,
    record: &crate::SkillDependencySnapshotRecord,
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

pub async fn insert_skill_dependency_snapshot_idempotent<C: ConnectionTrait>(
    db: &C,
    id: &str,
    turn_id: &str,
    record: &crate::SkillDependencySnapshotRecord,
) -> Result<()> {
    let created_at = unix_to_datetime(record.created_at_unix);
    if let Some(existing) = skill_dependency_snapshot::Entity::find_by_id(id.to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to load skill dependency delivery `{id}`"))?
    {
        if existing.turn_id.as_deref() == Some(turn_id)
            && existing.skill_id == record.skill_id.to_string()
            && existing.skill_owner == record.skill_owner
            && existing.skill_slug == record.skill_slug
            && existing.source_kind == record.source_kind
            && existing.diagnostics_json == record.diagnostics_json
            && existing.created_at == created_at
        {
            return Ok(());
        }
        bail!("skill dependency delivery id `{id}` conflicts with an existing record");
    }

    skill_dependency_snapshot::Entity::insert(skill_dependency_snapshot::ActiveModel {
        id: Set(id.to_owned()),
        turn_id: Set(Some(turn_id.to_owned())),
        skill_id: Set(record.skill_id.to_string()),
        skill_owner: Set(record.skill_owner.clone()),
        skill_slug: Set(record.skill_slug.clone()),
        source_kind: Set(record.source_kind.clone()),
        diagnostics_json: Set(record.diagnostics_json.clone()),
        created_at: Set(created_at),
    })
    .exec(db)
    .await
    .with_context(|| format!("failed to insert skill dependency delivery `{id}`"))?;

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
