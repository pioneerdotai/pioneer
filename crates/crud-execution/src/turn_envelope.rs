use anyhow::{Context, Result};
use pioneer_entity::{thread, turn};
use pioneer_protocol::{NativeContextBudget, NativeSummaryContext, TurnExecutionSecuritySnapshot};
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, QueryTrait};
use serde_json::Value;

pub use pioneer_crud_access::turn_execution::{
    authorization_context_as_text, authorization_context_value,
    set_turn_execution_authorization_context,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnExecutionSecuritySnapshotRecord {
    pub turn_id: String,
    pub version: i64,
    pub snapshot: TurnExecutionSecuritySnapshot,
}

pub async fn set_turn_execution_security_snapshot<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    snapshot: &TurnExecutionSecuritySnapshot,
) -> Result<bool> {
    let version = i64::from(snapshot.version);
    let snapshot_json = serde_json::to_string(snapshot)
        .context("failed to serialize turn execution security snapshot to json")?;

    let update_result = turn::Entity::update_many()
        .filter(turn::Column::Id.eq(turn_id.to_owned()))
        .col_expr(
            turn::Column::ExecutionSecuritySnapshotVersion,
            Expr::value(version),
        )
        .col_expr(
            turn::Column::ExecutionSecuritySnapshotJson,
            Expr::value(snapshot_json),
        )
        .exec(db)
        .await
        .context("failed to update turn execution security snapshot")?;

    Ok(update_result.rows_affected > 0)
}

pub async fn set_turn_execution_envelope<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    snapshot: &TurnExecutionSecuritySnapshot,
    authorization_context_json: &str,
) -> Result<bool> {
    let version = i64::from(snapshot.version);
    let snapshot_json = serde_json::to_string(snapshot)
        .context("failed to serialize turn execution security snapshot to json")?;
    let update_result = turn::Entity::update_many()
        .filter(turn::Column::Id.eq(turn_id.to_owned()))
        .col_expr(
            turn::Column::ExecutionSecuritySnapshotVersion,
            Expr::value(version),
        )
        .col_expr(
            turn::Column::ExecutionSecuritySnapshotJson,
            Expr::value(snapshot_json),
        )
        .col_expr(
            turn::Column::ExecutionAuthorizationContextJson,
            authorization_context_value(authorization_context_json)?,
        )
        .exec(db)
        .await
        .context("failed to update turn execution envelope")?;
    Ok(update_result.rows_affected > 0)
}

pub async fn find_turn_execution_security_snapshot<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<TurnExecutionSecuritySnapshotRecord>> {
    let Some((version, body)) = turn::Entity::find_by_id(turn_id)
        .select_only()
        .columns([
            turn::Column::ExecutionSecuritySnapshotVersion,
            turn::Column::ExecutionSecuritySnapshotJson,
        ])
        .into_tuple::<(Option<i64>, Option<String>)>()
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    let Some(snapshot_json) = body
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != "{}" && *value != "null")
    else {
        return Ok(None);
    };

    let snapshot = serde_json::from_str::<TurnExecutionSecuritySnapshot>(snapshot_json)
        .with_context(|| {
            format!("failed to decode execution security snapshot for turn `{turn_id}`")
        })?;
    let version = version.unwrap_or_else(|| i64::from(snapshot.version));

    Ok(Some(TurnExecutionSecuritySnapshotRecord {
        turn_id: turn_id.to_owned(),
        version,
        snapshot,
    }))
}

/// Pin an uninitialized Native context or supply its missing service mode.
/// Build the replacement before the write and compare exact stored text at
/// publication so an unrelated concurrent security change cannot be erased.
pub async fn pin_native_context<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    turn_id: &str,
    budget: &NativeContextBudget,
    summary: Option<&NativeSummaryContext>,
) -> Result<bool> {
    use turn::Column as Field;
    let scope = thread::Entity::find()
        .select_only()
        .column(thread::Column::Id)
        .filter(thread::Column::WorkspaceId.eq(workspace_id))
        .into_query();
    let budget = serde_json::to_value(budget)?;
    let summary = serde_json::to_value(summary)?;
    for _ in 0..8 {
        let Some(expected) = turn::Entity::find_by_id(turn_id)
            .select_only()
            .column(Field::ExecutionSecuritySnapshotJson)
            .filter(Field::ThreadId.in_subquery(scope.clone()))
            .into_tuple::<Option<String>>()
            .one(db)
            .await?
            .flatten()
        else {
            return Ok(false);
        };
        let Some(next) = prepare_pinned_context(&expected, &budget, &summary)? else {
            return Ok(false);
        };
        let updated = turn::Entity::update_many()
            .filter(Field::Id.eq(turn_id))
            .filter(Field::ThreadId.in_subquery(scope.clone()))
            .filter(Field::ExecutionSecuritySnapshotJson.eq(expected))
            .col_expr(Field::ExecutionSecuritySnapshotJson, Expr::val(next))
            .exec(db)
            .await
            .context("failed to publish Native execution context")?;
        if updated.rows_affected > 0 {
            return Ok(true);
        }
    }
    anyhow::bail!("execution security snapshot changed repeatedly while pinning Native context")
}

fn prepare_pinned_context(
    expected: &str,
    budget: &Value,
    summary: &Value,
) -> Result<Option<String>> {
    let mut snapshot: Value = serde_json::from_str(expected)?;
    if snapshot
        .pointer("/backend/execution_backend")
        .and_then(Value::as_str)
        != Some("native")
    {
        return Ok(None);
    }
    let Some(snapshot) = snapshot.as_object_mut() else {
        return Ok(None);
    };
    if snapshot.get("native_context").is_none_or(Value::is_null) {
        snapshot.insert("native_context".to_owned(), budget.clone());
        snapshot.insert("native_summary".to_owned(), summary.clone());
    } else {
        let Some(mode) = budget.get("service_mode").filter(|value| !value.is_null()) else {
            return Ok(None);
        };
        let Some(context) = snapshot
            .get_mut("native_context")
            .and_then(Value::as_object_mut)
        else {
            return Ok(None);
        };
        if context
            .get("service_mode")
            .is_some_and(|value| !value.is_null())
            || !["instance", "model"].into_iter().all(|field| {
                context.get(field).and_then(Value::as_str)
                    == budget.get(field).and_then(Value::as_str)
                    && context.get(field).and_then(Value::as_str).is_some()
            })
            || context.get("authority").unwrap_or(&Value::Null)
                != budget.get("authority").unwrap_or(&Value::Null)
        {
            return Ok(None);
        }
        context.insert("service_mode".to_owned(), mode.clone());
        if let Some(summary_budget) = snapshot
            .get_mut("native_summary")
            .and_then(|stored| stored.get_mut("budget"))
            .and_then(Value::as_object_mut)
        {
            if summary_budget
                .get("service_mode")
                .is_none_or(Value::is_null)
            {
                summary_budget.insert(
                    "service_mode".to_owned(),
                    summary
                        .pointer("/budget/service_mode")
                        .unwrap_or(&Value::Null)
                        .clone(),
                );
            }
        }
    }
    Ok(Some(serde_json::to_string(snapshot)?))
}
