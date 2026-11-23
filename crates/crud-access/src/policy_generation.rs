use anyhow::{Context, Result, anyhow, bail};
use pioneer_entity::{authorization_change_feed as feed, authorization_policy_state as state};
use pioneer_protocol::{
    AuthorizationChangeKind, AuthorizationChangeScope, AuthorizationProjectionChangedNotification,
    PolicyGeneration,
};
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
    TransactionSession, TransactionTrait,
};

pub async fn current_policy_generation<C: ConnectionTrait>(db: &C) -> Result<PolicyGeneration> {
    current_policy_generation_on(db).await
}

pub async fn current_policy_generation_on<C: ConnectionTrait>(db: &C) -> Result<PolicyGeneration> {
    let generation = state::Entity::find_by_id(1)
        .select_only()
        .column(state::Column::Generation)
        .into_tuple::<i64>()
        .one(db)
        .await
        .context("failed to load durable authorization policy generation")?
        .context("authorization policy state singleton is missing")?;
    generation_from_i64(generation)
}

pub async fn ensure_code_policy_generation<D>(
    db: &D,
    fingerprint: &str,
) -> Result<AuthorizationProjectionChangedNotification>
where
    D: TransactionTrait,
{
    if fingerprint.len() != 64 || !fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("authorization policy fingerprint must be a SHA-256 hex digest");
    }
    let affected = AuthorizationChangeScope::Global;
    let affected_scope_json = serde_json::to_value(&affected)
        .context("failed to serialize authorization change scope")?;
    let transaction = db
        .begin()
        .await
        .context("failed to begin code-policy generation transaction")?;
    // Update the singleton in one statement so concurrent changes serialize
    // on this row and evaluate the previous fingerprint under its row lock.
    let generation_column = Expr::col(state::Column::Generation);
    let next_generation = Expr::case(
        state::Column::CodePolicyFingerprint
            .eq("")
            .or(state::Column::CodePolicyFingerprint.eq(fingerprint)),
        generation_column.clone(),
    )
    .finally(generation_column.add(1_i64));
    let generation = advance_generation(&transaction, next_generation.into(), Some(fingerprint))
        .await
        .context("failed to advance changed code policy")?;
    let notification = AuthorizationProjectionChangedNotification {
        policy_generation: generation,
        change: AuthorizationChangeKind::CodePolicy,
        affected,
    };
    insert_change_if_absent(&transaction, &notification, affected_scope_json).await?;
    transaction
        .commit()
        .await
        .context("failed to commit code-policy generation")?;
    Ok(notification)
}

pub async fn append_authorization_change<D>(
    db: &D,
    change: AuthorizationChangeKind,
    affected: AuthorizationChangeScope,
) -> Result<AuthorizationProjectionChangedNotification>
where
    D: TransactionTrait,
{
    let affected_scope_json = serde_json::to_value(&affected)
        .context("failed to serialize authorization change scope")?;
    let transaction = db
        .begin()
        .await
        .context("failed to begin authorization change transaction")?;
    let generation = advance_generation(
        &transaction,
        Expr::col(state::Column::Generation).add(1_i64),
        None,
    )
    .await?;
    let notification = AuthorizationProjectionChangedNotification {
        policy_generation: generation,
        change,
        affected,
    };
    insert_change_if_absent(&transaction, &notification, affected_scope_json).await?;
    transaction
        .commit()
        .await
        .context("failed to commit authorization change")?;
    Ok(notification)
}

/// Reads the durable, ordered change feed for missed-event reconciliation.
/// The feed carries only typed, payload-safe invalidation scopes.
pub async fn list_authorization_changes_after<C: ConnectionTrait>(
    db: &C,
    after: Option<PolicyGeneration>,
    limit: usize,
) -> Result<Vec<AuthorizationProjectionChangedNotification>> {
    if !(1..=1024).contains(&limit) {
        bail!("authorization change feed limit must be between 1 and 1024");
    }
    let after = after.map_or(0, generation_i64);
    let rows = feed::Entity::find()
        .filter(feed::Column::Generation.gt(after))
        .order_by_asc(feed::Column::Generation)
        .limit(limit as u64)
        .all(db)
        .await
        .context("failed to list durable authorization changes")?;
    rows.into_iter()
        .map(|row| {
            let generation = generation_from_i64(row.generation)?;
            let change = parse_change_kind(&row.change_kind)?;
            let affected = serde_json::from_value(row.affected_scope_json)
                .context("invalid durable authorization change scope")?;
            Ok(AuthorizationProjectionChangedNotification {
                policy_generation: generation,
                change,
                affected,
            })
        })
        .collect()
}

async fn advance_generation<C: ConnectionTrait>(
    db: &C,
    next_generation: Expr,
    fingerprint: Option<&str>,
) -> Result<PolicyGeneration> {
    let mut update = state::Entity::update_many()
        .col_expr(state::Column::Generation, next_generation)
        .col_expr(
            state::Column::UpdatedAt,
            Expr::cust("statement_timestamp()"),
        )
        .filter(state::Column::SingletonId.eq(1_i64))
        .filter(state::Column::Generation.lt(i64::MAX));
    if let Some(fingerprint) = fingerprint {
        update = update.col_expr(state::Column::CodePolicyFingerprint, Expr::val(fingerprint));
    }
    let row = update
        .exec_with_returning(db)
        .await
        .context("failed to advance authorization generation")?
        .pop()
        .context("authorization policy state is missing or exhausted")?;
    generation_from_i64(row.generation)
}

async fn insert_change_if_absent<C: ConnectionTrait>(
    db: &C,
    notification: &AuthorizationProjectionChangedNotification,
    scope_json: serde_json::Value,
) -> Result<()> {
    feed::Entity::insert(feed::ActiveModel {
        generation: Set(generation_i64(notification.policy_generation)),
        change_kind: Set(change_kind_name(notification.change).to_owned()),
        affected_scope_json: Set(scope_json),
        ..Default::default()
    })
    .on_conflict_do_nothing_on([feed::Column::Generation])
    .exec_without_returning(db)
    .await
    .context("failed to append durable authorization change")?;
    Ok(())
}

fn generation_from_i64(value: i64) -> Result<PolicyGeneration> {
    let value = u64::try_from(value).context("authorization generation is negative")?;
    PolicyGeneration::new(value).ok_or_else(|| anyhow!("authorization generation is zero"))
}

fn generation_i64(generation: PolicyGeneration) -> i64 {
    i64::try_from(generation.get()).expect("persisted policy generation exceeds PostgreSQL BIGINT")
}

fn parse_change_kind(value: &str) -> Result<AuthorizationChangeKind> {
    match value {
        "code_policy" => Ok(AuthorizationChangeKind::CodePolicy),
        "role_policy" => Ok(AuthorizationChangeKind::RolePolicy),
        "role_assignment" => Ok(AuthorizationChangeKind::RoleAssignment),
        "workspace_acl" => Ok(AuthorizationChangeKind::WorkspaceAcl),
        "thread_acl" => Ok(AuthorizationChangeKind::ThreadAcl),
        "resource_selector" => Ok(AuthorizationChangeKind::ResourceSelector),
        _ => bail!("unknown durable authorization change kind `{value}`"),
    }
}

const fn change_kind_name(kind: AuthorizationChangeKind) -> &'static str {
    match kind {
        AuthorizationChangeKind::CodePolicy => "code_policy",
        AuthorizationChangeKind::RolePolicy => "role_policy",
        AuthorizationChangeKind::RoleAssignment => "role_assignment",
        AuthorizationChangeKind::WorkspaceAcl => "workspace_acl",
        AuthorizationChangeKind::ThreadAcl => "thread_acl",
        AuthorizationChangeKind::ResourceSelector => "resource_selector",
    }
}
