use anyhow::{Context, Result, bail};
use pioneer_entity::execution_admission_lease;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter, Set,
};

pub const EXECUTION_LEASE_STATUS_ACTIVE: &str = "active";
pub const EXECUTION_LEASE_STATUS_RELEASED: &str = "released";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionAdmissionClass {
    InteractiveTurn,
    CliProcess,
    AttachedChild,
    QueuedTask,
    ScheduledTask,
}

impl ExecutionAdmissionClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InteractiveTurn => "interactive_turn",
            Self::CliProcess => "cli_process",
            Self::AttachedChild => "attached_child",
            Self::QueuedTask => "queued_task",
            Self::ScheduledTask => "scheduled_task",
        }
    }

    pub const fn bucket(self) -> ExecutionQuotaBucket {
        match self {
            Self::InteractiveTurn | Self::CliProcess | Self::AttachedChild => {
                ExecutionQuotaBucket::Active
            }
            Self::QueuedTask => ExecutionQuotaBucket::Queued,
            Self::ScheduledTask => ExecutionQuotaBucket::Scheduled,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionQuotaBucket {
    Active,
    Queued,
    Scheduled,
}

impl ExecutionQuotaBucket {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Queued => "queued",
            Self::Scheduled => "scheduled",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionQuotaCeilings {
    pub per_principal: u32,
    pub per_role: u32,
    pub per_workspace: u32,
    pub gateway: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionAdmissionQuotaPolicy {
    pub active: ExecutionQuotaCeilings,
    pub queued: ExecutionQuotaCeilings,
    pub scheduled: ExecutionQuotaCeilings,
}

impl ExecutionAdmissionQuotaPolicy {
    pub const fn ceilings(self, bucket: ExecutionQuotaBucket) -> ExecutionQuotaCeilings {
        match bucket {
            ExecutionQuotaBucket::Active => self.active,
            ExecutionQuotaBucket::Queued => self.queued,
            ExecutionQuotaBucket::Scheduled => self.scheduled,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewExecutionAdmissionLease {
    pub id: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub operation_class: ExecutionAdmissionClass,
    pub principal_id: String,
    pub role_key: String,
    pub workspace_id: String,
    pub policy_fingerprint: String,
    pub policy: ExecutionAdmissionQuotaPolicy,
}

pub async fn reserve<C: ConnectionTrait>(
    db: &C,
    lease: NewExecutionAdmissionLease,
    created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
) -> Result<execution_admission_lease::Model> {
    if let Some(existing) = find_by_subject(db, &lease.subject_kind, &lease.subject_id).await? {
        ensure_same_immutable_lease(&existing, &lease)?;
        return Ok(existing);
    }

    let bucket = lease.operation_class.bucket();
    let ceilings = lease.policy.ceilings(bucket);
    enforce_scope_ceiling(
        db,
        bucket,
        execution_admission_lease::Column::PrincipalId,
        lease.principal_id.as_str(),
        ceilings.per_principal,
        "principal",
    )
    .await?;
    enforce_scope_ceiling(
        db,
        bucket,
        execution_admission_lease::Column::RoleKey,
        lease.role_key.as_str(),
        ceilings.per_role,
        "role",
    )
    .await?;
    enforce_scope_ceiling(
        db,
        bucket,
        execution_admission_lease::Column::WorkspaceId,
        lease.workspace_id.as_str(),
        ceilings.per_workspace,
        "workspace",
    )
    .await?;
    let gateway_count = active_bucket_query(bucket).count(db).await?;
    if gateway_count >= u64::from(ceilings.gateway) {
        bail!(
            "Gateway {} execution quota is exhausted (limit {})",
            bucket.as_str(),
            ceilings.gateway
        );
    }

    execution_admission_lease::ActiveModel {
        id: Set(lease.id.clone()),
        subject_kind: Set(lease.subject_kind.clone()),
        subject_id: Set(lease.subject_id.clone()),
        operation_class: Set(lease.operation_class.as_str().to_owned()),
        quota_bucket: Set(bucket.as_str().to_owned()),
        principal_id: Set(lease.principal_id.clone()),
        role_key: Set(lease.role_key.clone()),
        workspace_id: Set(lease.workspace_id.clone()),
        policy_fingerprint: Set(lease.policy_fingerprint.clone()),
        status: Set(EXECUTION_LEASE_STATUS_ACTIVE.to_owned()),
        created_at: Set(created_at),
        released_at: Set(None),
    }
    .insert(db)
    .await
    .context("failed to reserve durable execution admission lease")?;

    find_by_subject(db, &lease.subject_kind, &lease.subject_id)
        .await?
        .context("execution admission lease is missing after reservation")
}

/// Reacquires an immutable lease after an explicit blocked-Task readmission.
///
/// A released lease is never reopened by the ordinary `reserve` path: that
/// would let a replayed creation write resurrect terminal work. This separate
/// operation is used only by the transactional Task resume path after Gateway
/// has validated an explicit authorization readmission.
pub async fn reacquire<C: ConnectionTrait>(
    db: &C,
    lease: NewExecutionAdmissionLease,
    reacquired_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
) -> Result<execution_admission_lease::Model> {
    let Some(existing) = find_by_subject(db, &lease.subject_kind, &lease.subject_id).await? else {
        return reserve(db, lease, reacquired_at).await;
    };
    ensure_same_immutable_lease(&existing, &lease)?;
    if existing.status == EXECUTION_LEASE_STATUS_ACTIVE {
        return Ok(existing);
    }
    if existing.status != EXECUTION_LEASE_STATUS_RELEASED {
        bail!(
            "execution admission lease `{}` has unknown status `{}`",
            existing.id,
            existing.status
        );
    }

    let bucket = lease.operation_class.bucket();
    let ceilings = lease.policy.ceilings(bucket);
    enforce_scope_ceiling(
        db,
        bucket,
        execution_admission_lease::Column::PrincipalId,
        lease.principal_id.as_str(),
        ceilings.per_principal,
        "principal",
    )
    .await?;
    enforce_scope_ceiling(
        db,
        bucket,
        execution_admission_lease::Column::RoleKey,
        lease.role_key.as_str(),
        ceilings.per_role,
        "role",
    )
    .await?;
    enforce_scope_ceiling(
        db,
        bucket,
        execution_admission_lease::Column::WorkspaceId,
        lease.workspace_id.as_str(),
        ceilings.per_workspace,
        "workspace",
    )
    .await?;
    let gateway_count = active_bucket_query(bucket).count(db).await?;
    if gateway_count >= u64::from(ceilings.gateway) {
        bail!(
            "Gateway {} execution quota is exhausted (limit {})",
            bucket.as_str(),
            ceilings.gateway
        );
    }

    let mut active: execution_admission_lease::ActiveModel = existing.into();
    active.status = Set(EXECUTION_LEASE_STATUS_ACTIVE.to_owned());
    active.released_at = Set(None);
    active
        .update(db)
        .await
        .context("failed to reacquire durable execution admission lease")?;

    find_by_subject(db, &lease.subject_kind, &lease.subject_id)
        .await?
        .context("execution admission lease is missing after reacquisition")
}

pub async fn release_by_subject<C: ConnectionTrait>(
    db: &C,
    subject_kind: &str,
    subject_id: &str,
    released_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
) -> Result<bool> {
    let Some(model) = find_by_subject(db, subject_kind, subject_id).await? else {
        return Ok(false);
    };
    if model.status == EXECUTION_LEASE_STATUS_RELEASED {
        return Ok(false);
    }
    let mut active: execution_admission_lease::ActiveModel = model.into();
    active.status = Set(EXECUTION_LEASE_STATUS_RELEASED.to_owned());
    active.released_at = Set(Some(released_at));
    active
        .update(db)
        .await
        .context("failed to release durable execution admission lease")?;
    Ok(true)
}

pub async fn find_by_subject<C: ConnectionTrait>(
    db: &C,
    subject_kind: &str,
    subject_id: &str,
) -> Result<Option<execution_admission_lease::Model>> {
    execution_admission_lease::Entity::find()
        .filter(execution_admission_lease::Column::SubjectKind.eq(subject_kind))
        .filter(execution_admission_lease::Column::SubjectId.eq(subject_id))
        .one(db)
        .await
        .context("failed to query durable execution admission lease")
}

pub async fn list_active<C: ConnectionTrait>(
    db: &C,
) -> Result<Vec<execution_admission_lease::Model>> {
    execution_admission_lease::Entity::find()
        .filter(execution_admission_lease::Column::Status.eq(EXECUTION_LEASE_STATUS_ACTIVE))
        .all(db)
        .await
        .context("failed to list active execution admission leases")
}

async fn enforce_scope_ceiling<C: ConnectionTrait>(
    db: &C,
    bucket: ExecutionQuotaBucket,
    column: execution_admission_lease::Column,
    scope_id: &str,
    limit: u32,
    scope_name: &str,
) -> Result<()> {
    let count = active_bucket_query(bucket)
        .filter(column.eq(scope_id))
        .count(db)
        .await?;
    if count >= u64::from(limit) {
        bail!(
            "{scope_name} {} execution quota is exhausted (limit {limit})",
            bucket.as_str()
        );
    }
    Ok(())
}

fn active_bucket_query(
    bucket: ExecutionQuotaBucket,
) -> sea_orm::Select<execution_admission_lease::Entity> {
    execution_admission_lease::Entity::find()
        .filter(execution_admission_lease::Column::Status.eq(EXECUTION_LEASE_STATUS_ACTIVE))
        .filter(execution_admission_lease::Column::QuotaBucket.eq(bucket.as_str()))
}

fn ensure_same_immutable_lease(
    existing: &execution_admission_lease::Model,
    requested: &NewExecutionAdmissionLease,
) -> Result<()> {
    if existing.id != requested.id
        || existing.operation_class != requested.operation_class.as_str()
        || existing.quota_bucket != requested.operation_class.bucket().as_str()
        || existing.principal_id != requested.principal_id
        || existing.role_key != requested.role_key
        || existing.workspace_id != requested.workspace_id
        || existing.policy_fingerprint != requested.policy_fingerprint
    {
        bail!("execution admission lease conflicts with its immutable reservation");
    }
    Ok(())
}
