use crate::util::unix_to_datetime;
use anyhow::{Context, Result};
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
                record.skill_slug, record.action
            )
        })?;
    }

    Ok(())
}

pub async fn list_skill_audit_events<C: ConnectionTrait>(
    db: &C,
    skill_slug: &str,
    limit: u64,
) -> Result<Vec<skill_audit_event::Model>> {
    skill_audit_event::Entity::find()
        .filter(skill_audit_event::Column::SkillSlug.eq(skill_slug.to_owned()))
        .order_by_desc(skill_audit_event::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| format!("failed to query skill audit events for `{skill_slug}`"))
}

pub async fn list_skill_audit_events_for_source<C: ConnectionTrait>(
    db: &C,
    skill_slug: &str,
    source_kind: &str,
    limit: u64,
) -> Result<Vec<skill_audit_event::Model>> {
    skill_audit_event::Entity::find()
        .filter(skill_audit_event::Column::SkillSlug.eq(skill_slug.to_owned()))
        .filter(skill_audit_event::Column::SourceKind.eq(source_kind.to_owned()))
        .order_by_desc(skill_audit_event::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to query skill audit events for `{skill_slug}` ({source_kind})")
        })
}

pub async fn list_turn_skill_audit_events<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Vec<skill_audit_event::Model>> {
    skill_audit_event::Entity::find()
        .filter(skill_audit_event::Column::TurnId.eq(turn_id.to_owned()))
        .order_by_asc(skill_audit_event::Column::SkillSlug)
        .order_by_asc(skill_audit_event::Column::CreatedAt)
        .all(db)
        .await
        .with_context(|| format!("failed to query skill audit events for turn `{turn_id}`"))
}
