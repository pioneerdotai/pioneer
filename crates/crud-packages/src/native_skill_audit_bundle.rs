//! Atomic, replayable delivery of native skill audit and dependency evidence.
use anyhow::{Context, Result, bail};
use pioneer_postgres_pools::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, is_retryable_database_conflict,
    retry_with_backoff,
};
use sea_orm::{ConnectionTrait, TransactionSession, TransactionTrait};
use sha2::{Digest, Sha256};
use std::time::Duration;

use crate::skill_audit_event::{self, SkillAuditEventRecord, skill_audit_event_record_from_model};
use crate::skill_dependency_snapshot::{
    self, SkillDependencySnapshotRecord, skill_dependency_snapshot_record_from_model,
};

fn native_skill_delivery_id<T: serde::Serialize>(
    record_kind: &str,
    turn_id: &str,
    index: usize,
    record: &T,
) -> Result<String> {
    let encoded = serde_json::to_vec(&(record_kind, turn_id, index, record))
        .context("failed to encode native skill delivery identity")?;
    let digest = hex::encode(Sha256::digest(encoded));
    Ok(format!("N{}", &digest[..20]))
}

pub async fn persist_native_skill_audit_bundle<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    turn_id: &str,
    records: &[SkillAuditEventRecord],
    dependency_snapshots: &[SkillDependencySnapshotRecord],
) -> Result<()> {
    const MAX_NATIVE_SKILL_AUDIT_RECORDS: usize = 512;
    if records.len() > MAX_NATIVE_SKILL_AUDIT_RECORDS
        || dependency_snapshots.len() > MAX_NATIVE_SKILL_AUDIT_RECORDS
    {
        bail!(
            "native skill audit bundle exceeds {MAX_NATIVE_SKILL_AUDIT_RECORDS} records per kind"
        );
    }
    for record in records {
        if record
            .turn_id
            .as_deref()
            .is_some_and(|value| value != turn_id)
        {
            bail!("native skill audit record belongs to a different turn");
        }
    }
    for snapshot in dependency_snapshots {
        if snapshot
            .turn_id
            .as_deref()
            .is_some_and(|value| value != turn_id)
        {
            bail!("native skill dependency snapshot belongs to a different turn");
        }
    }

    let audit_deliveries = records
        .iter()
        .enumerate()
        .map(|(index, record)| {
            let id = native_skill_delivery_id("audit", turn_id, index, record)?;
            Ok(skill_audit_event::prepare_skill_audit_event_idempotent(
                id.as_str(),
                turn_id,
                record,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let dependency_deliveries = dependency_snapshots
        .iter()
        .enumerate()
        .map(|(index, record)| {
            let id = native_skill_delivery_id("dependency", turn_id, index, record)?;
            Ok(
                skill_dependency_snapshot::prepare_skill_dependency_snapshot_idempotent(
                    id.as_str(),
                    turn_id,
                    record,
                ),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    retry_with_backoff(
        || {
            let audit_deliveries = audit_deliveries.clone();
            let dependency_deliveries = dependency_deliveries.clone();
            async move {
                let transaction = db
                    .begin()
                    .await
                    .context("failed to begin native skill audit transaction")?;
                skill_audit_event::insert_prepared_skill_audit_events_idempotent(
                    &transaction,
                    audit_deliveries,
                )
                .await?;
                skill_dependency_snapshot::insert_prepared_skill_dependency_snapshots_idempotent(
                    &transaction,
                    dependency_deliveries,
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit native skill audit transaction")
            }
        },
        is_retryable_database_conflict,
        DEFAULT_LOCK_RETRY_ATTEMPTS,
        Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
    )
    .await
}

pub async fn list_turn_skill_audit_event_records<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Vec<SkillAuditEventRecord>> {
    let rows = skill_audit_event::list_turn_skill_audit_events(db, turn_id).await?;
    Ok(rows
        .into_iter()
        .map(skill_audit_event_record_from_model)
        .collect::<Result<Vec<_>>>()?)
}

pub async fn list_turn_skill_dependency_snapshot_records<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Vec<SkillDependencySnapshotRecord>> {
    let rows = skill_dependency_snapshot::list_turn_skill_dependency_snapshots(db, turn_id).await?;
    Ok(rows
        .into_iter()
        .map(skill_dependency_snapshot_record_from_model)
        .collect::<Result<Vec<_>>>()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::SkillId;

    #[pioneer_postgres_test::postgres_test]
    async fn native_skill_audit_bundle_is_atomic_and_idempotent() {
        let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
        let db = fixture.connection();
        let turn_id = "turn_native_skill_audit";
        let skill_id = SkillId::new("N".repeat(21)).expect("valid skill id");
        let audit = SkillAuditEventRecord {
            turn_id: Some(turn_id.to_owned()),
            skill_id: skill_id.clone(),
            skill_owner: Some("pioneer".to_owned()),
            skill_slug: "durable-skill".to_owned(),
            source_kind: "registry".to_owned(),
            action: "resolve".to_owned(),
            decision: "accepted".to_owned(),
            reason_code: None,
            details_json: r#"{"dependency_diagnostics":[{"name":"tool"}]}"#.to_owned(),
            created_at_unix: 1_700_000_200,
        };
        let dependency = SkillDependencySnapshotRecord {
            turn_id: Some(turn_id.to_owned()),
            skill_id,
            skill_owner: audit.skill_owner.clone(),
            skill_slug: audit.skill_slug.clone(),
            source_kind: audit.source_kind.clone(),
            diagnostics_json: r#"[{"name":"tool"}]"#.to_owned(),
            created_at_unix: audit.created_at_unix,
        };

        db.execute_unprepared(
                "CREATE FUNCTION reject_native_skill_dependency() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fault injection'; END $$; CREATE TRIGGER reject_native_skill_dependency BEFORE INSERT ON skill_dependency_snapshot FOR EACH ROW EXECUTE FUNCTION reject_native_skill_dependency()",
            )
            .await
            .expect("fault trigger should install");
        assert!(
            persist_native_skill_audit_bundle(
                db,
                turn_id,
                std::slice::from_ref(&audit),
                std::slice::from_ref(&dependency),
            )
            .await
            .is_err(),
            "dependency failure must reject the whole durable delivery"
        );
        assert!(
            list_turn_skill_audit_event_records(db, turn_id)
                .await
                .expect("audit rows should list after rollback")
                .is_empty(),
            "audit rows must roll back with the dependency snapshot"
        );

        db.execute_unprepared("DROP TRIGGER reject_native_skill_dependency ON skill_dependency_snapshot; DROP FUNCTION reject_native_skill_dependency()")
            .await
            .expect("fault trigger should drop");
        for _ in 0..2 {
            persist_native_skill_audit_bundle(
                db,
                turn_id,
                std::slice::from_ref(&audit),
                std::slice::from_ref(&dependency),
            )
            .await
            .expect("native skill delivery should commit idempotently");
        }
        assert_eq!(
            list_turn_skill_audit_event_records(db, turn_id)
                .await
                .expect("audit rows should list"),
            vec![audit]
        );
        assert_eq!(
            list_turn_skill_dependency_snapshot_records(db, turn_id)
                .await
                .expect("dependency snapshots should list"),
            vec![dependency]
        );
    }
}
