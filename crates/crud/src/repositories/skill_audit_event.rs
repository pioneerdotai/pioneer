use crate::util::unix_to_datetime;
use anyhow::{Context, Result, bail};
use pioneer_entity::skill_audit_event;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

pub async fn insert_skill_audit_events<C: ConnectionTrait>(
    db: &C,
    turn_id: Option<&str>,
    records: &[crate::SkillAuditEventRecord],
) -> Result<()> {
    for record in records {
        skill_audit_event::Entity::insert(skill_audit_event::ActiveModel {
            id: Set(pioneer_protocol::generate_id(21)),
            turn_id: Set(turn_id
                .map(str::to_owned)
                .or_else(|| record.turn_id.clone())),
            skill_id: Set(record.skill_id.to_string()),
            skill_owner: Set(record.skill_owner.clone()),
            skill_slug: Set(record.skill_slug.clone()),
            source_kind: Set(record.source_kind.clone()),
            action: Set(record.action.clone()),
            decision: Set(record.decision.clone()),
            reason_code: Set(record.reason_code.clone()),
            details_json: Set(record.details_json.clone()),
            created_at: Set(unix_to_datetime(record.created_at_unix)),
        })
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to insert skill audit event `{}` ({})",
                record.skill_id, record.action
            )
        })?;
    }

    Ok(())
}

pub async fn insert_skill_audit_event_idempotent<C: ConnectionTrait>(
    db: &C,
    id: &str,
    turn_id: &str,
    record: &crate::SkillAuditEventRecord,
) -> Result<()> {
    let created_at = unix_to_datetime(record.created_at_unix);
    if let Some(existing) = skill_audit_event::Entity::find_by_id(id.to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to load skill audit delivery `{id}`"))?
    {
        if existing.turn_id.as_deref() == Some(turn_id)
            && existing.skill_id == record.skill_id.to_string()
            && existing.skill_owner == record.skill_owner
            && existing.skill_slug == record.skill_slug
            && existing.source_kind == record.source_kind
            && existing.action == record.action
            && existing.decision == record.decision
            && existing.reason_code == record.reason_code
            && existing.details_json == record.details_json
            && existing.created_at == created_at
        {
            return Ok(());
        }
        bail!("skill audit delivery id `{id}` conflicts with an existing record");
    }

    skill_audit_event::Entity::insert(skill_audit_event::ActiveModel {
        id: Set(id.to_owned()),
        turn_id: Set(Some(turn_id.to_owned())),
        skill_id: Set(record.skill_id.to_string()),
        skill_owner: Set(record.skill_owner.clone()),
        skill_slug: Set(record.skill_slug.clone()),
        source_kind: Set(record.source_kind.clone()),
        action: Set(record.action.clone()),
        decision: Set(record.decision.clone()),
        reason_code: Set(record.reason_code.clone()),
        details_json: Set(record.details_json.clone()),
        created_at: Set(created_at),
    })
    .exec(db)
    .await
    .with_context(|| format!("failed to insert skill audit delivery `{id}`"))?;

    Ok(())
}

pub async fn list_skill_audit_events<C: ConnectionTrait>(
    db: &C,
    skill_id: &pioneer_protocol::SkillId,
    limit: u64,
) -> Result<Vec<skill_audit_event::Model>> {
    skill_audit_event::Entity::find()
        .filter(skill_audit_event::Column::SkillId.eq(skill_id.to_string()))
        .order_by_desc(skill_audit_event::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| format!("failed to query skill audit events for `{skill_id}`"))
}

pub async fn list_turn_skill_audit_events<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Vec<skill_audit_event::Model>> {
    skill_audit_event::Entity::find()
        .filter(skill_audit_event::Column::TurnId.eq(turn_id.to_owned()))
        .order_by_asc(skill_audit_event::Column::SkillId)
        .order_by_asc(skill_audit_event::Column::CreatedAt)
        .all(db)
        .await
        .with_context(|| format!("failed to query skill audit events for turn `{turn_id}`"))
}
