//! Fences for publishing filesystem-prepared skill metadata.

use anyhow::{Context, Result, bail};
use pioneer_protocol::SkillId;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::sea_query::LockType;
use sea_orm::{ConnectionTrait, EntityTrait, QuerySelect, TransactionTrait};

use crate::skill_installation;
use crate::skill_lifecycle::{
    prepare_generic_skill_update_fence, revalidate_generic_skill_update_fence,
};
use crate::skill_pack_installation::unix_to_datetime;
use crate::skill_pack_transactions::{retry_write, skill_installation_record_from_model};
use crate::skill_records::{SkillInstallationPatch, SkillInstallationRecord};

/// Exact row facts captured before filesystem preparation, including subsecond
/// timestamps. Ordinary foreground records retain their existing wire shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillReconciliationSnapshot {
    pub record: SkillInstallationRecord,
    model: pioneer_entity::skill_installation::Model,
}

/// Outcome of a failed prepared publication. An error before a transaction's
/// mutation or after acknowledged rollback proves that no commit took place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillReconciliationFailure {
    NotCommitted,
    Unknown,
}

#[derive(Debug)]
pub struct SkillReconciliationError {
    pub outcome: SkillReconciliationFailure,
    pub error: anyhow::Error,
}
impl std::fmt::Display for SkillReconciliationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "skill reconciliation {:?}: {}", self.outcome, self.error)
    }
}
impl std::error::Error for SkillReconciliationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.error.as_ref())
    }
}
fn reconciliation_error(
    error: impl Into<anyhow::Error>,
    outcome: SkillReconciliationFailure,
) -> anyhow::Error {
    SkillReconciliationError {
        outcome,
        error: error.into(),
    }
    .into()
}

impl SkillReconciliationSnapshot {
    pub fn from_model(model: pioneer_entity::skill_installation::Model) -> Result<Self> {
        Ok(Self {
            record: skill_installation_record_from_model(model.clone())?,
            model,
        })
    }
}

async fn revalidate_skill_source_workspace<C: ConnectionTrait>(
    db: &C,
    record: &SkillInstallationRecord,
    expected: Option<&pioneer_entity::workspace::Model>,
) -> Result<bool> {
    if record.source_kind == "system" {
        return Ok(record.scope_key == "system" && expected.is_none());
    }
    if !matches!(record.source_kind.as_str(), "user" | "registry") {
        return Ok(false);
    }
    let Some(expected) = expected else {
        return Ok(false);
    };
    if !expected.is_active || expected.id != record.scope_key {
        return Ok(false);
    }
    let current = pioneer_entity::workspace::Entity::find_by_id(record.scope_key.clone())
        .lock(LockType::Share)
        .one(db)
        .await?;
    Ok(current.as_ref() == Some(expected))
}

pub async fn update_skill_installation(
    db: &SqliteDatabase,
    skill_id: &SkillId,
    patch: &SkillInstallationPatch,
    event_timestamp_secs: i64,
) -> Result<bool> {
    update_skill_installation_guarded(db, skill_id, patch, event_timestamp_secs, None, true).await
}

pub async fn reconcile_skill_installation_with_progress(
    db: &SqliteDatabase,
    expected: &SkillReconciliationSnapshot,
    patch: &SkillInstallationPatch,
    now: i64,
    workspace: Option<&pioneer_entity::workspace::Model>,
    still_current: &(dyn Fn() -> bool + Send + Sync),
    mutation_started: &std::sync::atomic::AtomicBool,
) -> Result<bool> {
    // The public view cannot substitute scope/metadata for the exact facts
    // captured by this snapshot. Validate it before acquiring DB capacity.
    if skill_installation_record_from_model(expected.model.clone())
        .map_err(|e| reconciliation_error(e, SkillReconciliationFailure::NotCommitted))?
        != expected.record
    {
        return Err(reconciliation_error(
            anyhow::anyhow!("prepared skill snapshot view changed"),
            SkillReconciliationFailure::NotCommitted,
        ));
    }
    update_skill_installation_guarded(
        db,
        &expected.record.skill_id,
        patch,
        now,
        Some((expected, workspace, still_current, mutation_started)),
        true,
    )
    .await
}

pub async fn confirm_skill_reconciliation(
    db: &SqliteDatabase,
    expected: &SkillReconciliationSnapshot,
    workspace: Option<&pioneer_entity::workspace::Model>,
    still_current: &(dyn Fn() -> bool + Send + Sync),
    transaction_started: &std::sync::atomic::AtomicBool,
) -> Result<bool> {
    if skill_installation_record_from_model(expected.model.clone())
        .map_err(|e| reconciliation_error(e, SkillReconciliationFailure::NotCommitted))?
        != expected.record
    {
        return Err(reconciliation_error(
            anyhow::anyhow!("prepared skill snapshot view changed"),
            SkillReconciliationFailure::NotCommitted,
        ));
    }
    update_skill_installation_guarded(
        db,
        &expected.record.skill_id,
        &SkillInstallationPatch::default(),
        expected.record.updated_at_unix,
        Some((expected, workspace, still_current, transaction_started)),
        false,
    )
    .await
}

pub async fn register_skill_import_pending(
    db: &SqliteDatabase,
    record: &SkillInstallationRecord,
    workspace: Option<&pioneer_entity::workspace::Model>,
    now: i64,
) -> Result<Option<SkillReconciliationSnapshot>> {
    if record.pack_id.is_some() || record.pack_member_key.is_some() {
        bail!("pack members must be inserted through a pack transaction");
    }
    let created_at = unix_to_datetime(now);
    let prepared = skill_installation::prepare_skill_installation(record, created_at, created_at);
    async {
        let transaction = db.begin().await?;
        if !revalidate_skill_source_workspace(&transaction, record, workspace).await? {
            transaction.rollback().await?;
            return Ok(None);
        }
        if skill_installation::has_skill_import_provenance(&transaction, record).await? {
            transaction.rollback().await?;
            return Ok(None);
        }
        skill_installation::insert_prepared_skill_installations(
            &transaction,
            std::slice::from_ref(&prepared),
        )
        .await?;
        let snapshot = skill_installation::find_skill_installation(&transaction, &record.skill_id)
            .await?
            .map(SkillReconciliationSnapshot::from_model)
            .transpose()?;
        transaction.commit().await?;
        Ok(snapshot)
    }
    .await
}

pub async fn find_skill_import_provenance(
    db: &SqliteDatabase,
    kind: &str,
    scope: &str,
    reference: &str,
) -> Result<Option<SkillReconciliationSnapshot>> {
    let mut rows = skill_installation::find_import_provenance(db, kind, scope, reference).await?;
    if rows.len() > 1 {
        bail!("ambiguous exact skill import provenance");
    }
    rows.pop()
        .map(SkillReconciliationSnapshot::from_model)
        .transpose()
}

pub async fn list_skill_reconciliation_page(
    db: &SqliteDatabase,
    source_kind: &str,
    scope_key: &str,
    after: Option<&str>,
    limit: u64,
) -> Result<Vec<SkillReconciliationSnapshot>> {
    skill_installation::list_skill_installations_scope_page(
        db,
        source_kind,
        scope_key,
        after,
        limit,
    )
    .await?
    .into_iter()
    .map(SkillReconciliationSnapshot::from_model)
    .collect()
}

async fn update_skill_installation_guarded(
    db: &SqliteDatabase,
    skill_id: &SkillId,
    patch: &SkillInstallationPatch,
    event_timestamp_secs: i64,
    expected: Option<(
        &SkillReconciliationSnapshot,
        Option<&pioneer_entity::workspace::Model>,
        &(dyn Fn() -> bool + Send + Sync),
        &std::sync::atomic::AtomicBool,
    )>,
    write: bool,
) -> Result<bool> {
    let failure = |error: anyhow::Error, outcome| {
        if expected.is_some() {
            reconciliation_error(error, outcome)
        } else {
            error
        }
    };
    let fence = prepare_generic_skill_update_fence(db, skill_id, patch)
        .await
        .map_err(|e| failure(e, SkillReconciliationFailure::NotCommitted))?;
    let now = unix_to_datetime(event_timestamp_secs);
    let operation = || {
        let fence = fence.clone();
        async move {
            let transaction = db
                .begin()
                .await
                .context("failed to begin skill installation update transaction")
                .map_err(|e| failure(e, SkillReconciliationFailure::NotCommitted))?;
            let result: Result<bool> = async {
                if !revalidate_generic_skill_update_fence(&transaction, skill_id, &fence).await? {
                    return Ok(false);
                }
                if let Some((expected, workspace, still_current, _)) = expected {
                    if !still_current() {
                        return Ok(false);
                    }
                    let current =
                        skill_installation::find_skill_installation(&transaction, skill_id).await?;
                    if current.as_ref() != Some(&expected.model) {
                        return Ok(false);
                    }
                    if !revalidate_skill_source_workspace(&transaction, &expected.record, workspace)
                        .await?
                    {
                        return Ok(false);
                    }
                    if patch
                        .source_kind
                        .as_ref()
                        .is_some_and(|kind| kind != &expected.record.source_kind)
                        || patch
                            .scope_key
                            .as_ref()
                            .is_some_and(|scope| scope != &expected.record.scope_key)
                    {
                        bail!("reconciliation cannot change skill source scope");
                    }
                }
                if !write {
                    return Ok(true);
                }
                if let Some((_, _, _, started)) = expected {
                    started.store(true, std::sync::atomic::Ordering::Release);
                }
                skill_installation::update_skill_installation(&transaction, skill_id, patch, now)
                    .await
            }
            .await;
            match result {
                Ok(updated) if expected.is_none_or(|(_, _, current, _)| current()) && updated => {
                    if let Some((_, _, _, started)) = expected {
                        started.store(true, std::sync::atomic::Ordering::Release);
                    }
                    transaction
                        .commit()
                        .await
                        .context("failed to acknowledge skill installation commit")
                        .map_err(|e| failure(e, SkillReconciliationFailure::Unknown))?;
                    Ok(true)
                }
                Ok(_) => {
                    transaction
                        .rollback()
                        .await
                        .context("failed to acknowledge stale skill rollback")
                        .map_err(|e| failure(e, SkillReconciliationFailure::Unknown))?;
                    Ok(false)
                }
                Err(error) => match transaction.rollback().await {
                    Ok(()) => Err(failure(error, SkillReconciliationFailure::NotCommitted)),
                    Err(rollback) => Err(failure(
                        error.context(format!("rollback acknowledgement failed: {rollback}")),
                        SkillReconciliationFailure::Unknown,
                    )),
                },
            }
        }
    };
    if expected.is_some() {
        operation().await
    } else {
        retry_write(operation).await
    }
}
