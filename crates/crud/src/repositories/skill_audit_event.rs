use crate::util::unix_to_datetime;
use anyhow::{Context, Result};
use pioneer_entity::skill_audit_event;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
    sea_query::OnConflict,
};

const SKILL_AUDIT_INSERT_BATCH_SIZE: usize = 32;

#[derive(Debug, Clone)]
pub struct PreparedSkillAuditEvent {
    id: String,
    turn_id: Option<String>,
    skill_id: String,
    skill_owner: Option<String>,
    skill_slug: String,
    source_kind: String,
    action: String,
    decision: String,
    reason_code: Option<String>,
    details_json: String,
    created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
}

pub fn prepare_skill_audit_events(
    turn_id: Option<&str>,
    records: &[crate::SkillAuditEventRecord],
) -> Vec<PreparedSkillAuditEvent> {
    records
        .iter()
        .map(|record| PreparedSkillAuditEvent {
            id: pioneer_protocol::generate_id(21),
            turn_id: turn_id
                .map(str::to_owned)
                .or_else(|| record.turn_id.clone()),
            skill_id: record.skill_id.to_string(),
            skill_owner: record.skill_owner.clone(),
            skill_slug: record.skill_slug.clone(),
            source_kind: record.source_kind.clone(),
            action: record.action.clone(),
            decision: record.decision.clone(),
            reason_code: record.reason_code.clone(),
            details_json: record.details_json.clone(),
            created_at: unix_to_datetime(record.created_at_unix),
        })
        .collect()
}

pub fn prepare_skill_audit_event_idempotent(
    id: &str,
    turn_id: &str,
    record: &crate::SkillAuditEventRecord,
) -> PreparedSkillAuditEvent {
    PreparedSkillAuditEvent {
        id: id.to_owned(),
        turn_id: Some(turn_id.to_owned()),
        skill_id: record.skill_id.to_string(),
        skill_owner: record.skill_owner.clone(),
        skill_slug: record.skill_slug.clone(),
        source_kind: record.source_kind.clone(),
        action: record.action.clone(),
        decision: record.decision.clone(),
        reason_code: record.reason_code.clone(),
        details_json: record.details_json.clone(),
        created_at: unix_to_datetime(record.created_at_unix),
    }
}

pub async fn insert_skill_audit_events<C: ConnectionTrait>(
    db: &C,
    turn_id: Option<&str>,
    records: &[crate::SkillAuditEventRecord],
) -> Result<()> {
    insert_prepared_skill_audit_events(db, prepare_skill_audit_events(turn_id, records)).await
}

pub async fn insert_prepared_skill_audit_events<C: ConnectionTrait>(
    db: &C,
    records: Vec<PreparedSkillAuditEvent>,
) -> Result<()> {
    for batch in records.chunks(SKILL_AUDIT_INSERT_BATCH_SIZE) {
        skill_audit_event::Entity::insert_many(
            batch.iter().map(prepared_skill_audit_event_active_model),
        )
        .exec_without_returning(db)
        .await
        .context("failed to insert prepared skill audit event batch")?;
    }

    Ok(())
}

pub async fn insert_prepared_skill_audit_events_idempotent<C: ConnectionTrait>(
    db: &C,
    prepared: Vec<PreparedSkillAuditEvent>,
) -> Result<()> {
    for batch in prepared.chunks(SKILL_AUDIT_INSERT_BATCH_SIZE) {
        skill_audit_event::Entity::insert_many(
            batch.iter().map(prepared_skill_audit_event_active_model),
        )
        .on_conflict(
            OnConflict::column(skill_audit_event::Column::Id)
                .do_nothing()
                .to_owned(),
        )
        .exec_without_returning(db)
        .await
        .context("failed to insert prepared skill audit event batch")?;
    }
    Ok(())
}

fn prepared_skill_audit_event_active_model(
    prepared: &PreparedSkillAuditEvent,
) -> skill_audit_event::ActiveModel {
    skill_audit_event::ActiveModel {
        id: Set(prepared.id.clone()),
        turn_id: Set(prepared.turn_id.clone()),
        skill_id: Set(prepared.skill_id.clone()),
        skill_owner: Set(prepared.skill_owner.clone()),
        skill_slug: Set(prepared.skill_slug.clone()),
        source_kind: Set(prepared.source_kind.clone()),
        action: Set(prepared.action.clone()),
        decision: Set(prepared.decision.clone()),
        reason_code: Set(prepared.reason_code.clone()),
        details_json: Set(prepared.details_json.clone()),
        created_at: Set(prepared.created_at),
    }
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
