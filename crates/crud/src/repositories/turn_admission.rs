use anyhow::{Context, Result};
use pioneer_entity::turn_admission;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait, Set};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewTurnAdmission {
    pub turn_id: String,
    pub thread_id: String,
    pub workspace_id: String,
    pub request_digest: String,
    pub policy_generation: Option<u64>,
    pub role_key: Option<String>,
    pub policy_fingerprint: Option<String>,
    pub execution_lease: Option<super::execution_admission_lease::NewExecutionAdmissionLease>,
}

pub async fn insert<C: ConnectionTrait>(
    db: &C,
    admission: NewTurnAdmission,
    created_at: DateTimeWithTimeZone,
) -> Result<()> {
    if let Some(admitted_generation) = admission.policy_generation {
        let current_generation = super::policy_generation::current_policy_generation_on(db).await?;
        if admitted_generation != current_generation.get() {
            anyhow::bail!(
                "Turn admission generation {admitted_generation} is stale; current authorization generation is {}",
                current_generation.get()
            );
        }
    }
    let execution_lease = admission.execution_lease.clone();
    turn_admission::ActiveModel {
        turn_id: Set(admission.turn_id),
        thread_id: Set(admission.thread_id),
        workspace_id: Set(admission.workspace_id),
        request_digest: Set(admission.request_digest),
        policy_generation: Set(admission
            .policy_generation
            .map(i64::try_from)
            .transpose()
            .context("Turn admission policy generation exceeds SQLite INTEGER")?),
        role_key: Set(admission.role_key),
        policy_fingerprint: Set(admission.policy_fingerprint),
        created_at: Set(created_at),
    }
    .insert(db)
    .await
    .context("failed to insert native Turn admission")?;
    if let Some(execution_lease) = execution_lease {
        super::execution_admission_lease::reserve(db, execution_lease, created_at).await?;
    }
    Ok(())
}

pub async fn find<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<turn_admission::Model>> {
    turn_admission::Entity::find_by_id(turn_id.to_owned())
        .one(db)
        .await
        .context("failed to query native Turn admission")
}
