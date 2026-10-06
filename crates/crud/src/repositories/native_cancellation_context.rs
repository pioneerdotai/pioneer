//! Immutable cancellation input; never reconstructed from current settings.
use anyhow::{Result, ensure};
use pioneer_entity::{native_cancellation_context as context, thread, turn, turn_execution};
use pioneer_protocol::NativeTerminalEffectPreparation;
use sea_orm::sea_query::{Alias, Condition, Expr, ExprTrait, Func, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, QueryFilter, QuerySelect, Set, Statement,
};
use sha2::{Digest, Sha256};

pub const MAX_CONTEXT_BYTES: usize = 516 * 1024;
#[derive(Debug)]
pub struct NativeCancellationContextUnavailable;
impl std::fmt::Display for NativeCancellationContextUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("required immutable native cancellation context is unavailable")
    }
}
impl std::error::Error for NativeCancellationContextUnavailable {}

#[derive(Clone, Debug)]
pub struct NativeCancellationContext {
    pub preparation: NativeTerminalEffectPreparation,
    pub(crate) context_sha256: String,
    pub accepted_event_id: Option<String>,
}

pub fn digest(json: &str) -> String {
    hex::encode(Sha256::digest(json.as_bytes()))
}

pub fn prepare_registration(
    preparation: &NativeTerminalEffectPreparation,
    owner: &str,
    now: sea_orm::prelude::DateTimeWithTimeZone,
) -> Result<context::ActiveModel> {
    let json = serde_json::to_string(preparation)?;
    ensure!(
        json.len() <= MAX_CONTEXT_BYTES,
        "native cancellation context exceeds durable byte limit"
    );
    let sha256 = digest(&json);
    Ok(context::ActiveModel {
        turn_id: Set(preparation.turn_id.clone()),
        thread_id: Set(preparation.thread_id.clone()),
        workspace_id: Set(preparation.workspace_id.clone()),
        execution_owner_id: Set(owner.to_owned()),
        context_json: Set(json),
        context_sha256: Set(sha256),
        accepted_event_id: Set(None),
        created_at: Set(now),
        ..Default::default()
    })
}

pub async fn load<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<NativeCancellationContext>> {
    // Bounded owned row; .one releases the reader before digest/JSON processing.
    let row = context::Entity::find_by_id(turn_id.to_owned())
        .filter(
            Expr::expr(
                Func::cust(Alias::new("length"))
                    .arg(Expr::col(context::Column::ContextJson).cast_as(Alias::new("blob"))),
            )
            .lte(MAX_CONTEXT_BYTES as i64),
        )
        .one(db)
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    ensure!(
        row.context_json.len() <= MAX_CONTEXT_BYTES,
        "native cancellation context exceeds durable byte limit"
    );
    ensure!(
        digest(&row.context_json) == row.context_sha256,
        "immutable native cancellation context digest mismatch"
    );
    let preparation: NativeTerminalEffectPreparation = serde_json::from_str(&row.context_json)?;
    ensure!(
        preparation.turn_id == row.turn_id
            && preparation.thread_id == row.thread_id
            && preparation.workspace_id == row.workspace_id,
        "immutable cancellation context scope mismatch"
    );
    Ok(Some(NativeCancellationContext {
        preparation,
        context_sha256: row.context_sha256,
        accepted_event_id: row.accepted_event_id,
    }))
}

/// Compare immutable identity and policy, allowing only cancellation reason text.
/// This runs on owned bounded descriptions before acquiring database capacity.
pub fn validate_derived_preparation(
    context: &NativeCancellationContext,
    preparation: &NativeTerminalEffectPreparation,
) -> Result<()> {
    fn normalize(
        mut preparation: NativeTerminalEffectPreparation,
    ) -> Result<NativeTerminalEffectPreparation> {
        for effect in &mut preparation.effects {
            match &mut effect.payload {
                pioneer_protocol::NativeTerminalEffectPayload::AttachedTaskCleanup {
                    reason,
                    ..
                } => reason.clear(),
                pioneer_protocol::NativeTerminalEffectPayload::PostTurnHook { request, .. } => {
                    let input = request
                        .pointer_mut("/input/payload/value")
                        .and_then(serde_json::Value::as_object_mut)
                        .ok_or_else(|| {
                            anyhow::anyhow!("immutable cancellation hook input is missing")
                        })?;
                    input.remove("error");
                }
                pioneer_protocol::NativeTerminalEffectPayload::PostTurnHookPreparationFailed {
                    ..
                } => {}
            }
        }
        Ok(preparation)
    }
    ensure!(
        normalize(context.preparation.clone())? == normalize(preparation.clone())?,
        "native cancellation preparation changed immutable identity or obligations"
    );
    Ok(())
}

/// Writer holds only bounded scope/status/owner checks and the immutable insert.
/// Recovery confirms an existing row without fetching its JSON or updating it.
pub async fn insert_once<C: ConnectionTrait>(
    db: &C,
    preparation: &NativeTerminalEffectPreparation,
    candidate: context::ActiveModel,
    initial_turn: bool,
) -> Result<()> {
    let exists = context::Entity::find_by_id(preparation.turn_id.clone())
        .filter(context::Column::ThreadId.eq(preparation.thread_id.clone()))
        .filter(context::Column::WorkspaceId.eq(preparation.workspace_id.clone()))
        .select_only()
        .column(context::Column::TurnId)
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_some();
    if exists {
        return Ok(());
    }
    if !initial_turn {
        return Err(NativeCancellationContextUnavailable.into());
    }
    let turn_scope = turn::Entity::find_by_id(preparation.turn_id.clone())
        .filter(turn::Column::ThreadId.eq(preparation.thread_id.clone()))
        .filter(turn::Column::Status.eq("in_progress"))
        .select_only()
        .column(turn::Column::Id)
        .into_tuple::<String>()
        .one(db)
        .await?;
    let workspace = thread::Entity::find_by_id(preparation.thread_id.clone())
        .filter(thread::Column::WorkspaceId.eq(preparation.workspace_id.clone()))
        .select_only()
        .column(thread::Column::Id)
        .into_tuple::<String>()
        .one(db)
        .await?;
    ensure!(
        turn_scope.is_some() && workspace.is_some(),
        "native cancellation context registration lost its turn scope"
    );
    context::Entity::insert(candidate)
        .exec_without_returning(db)
        .await?;
    Ok(())
}

fn owned_query(turn_id: &str, owner: &str) -> sea_orm::sea_query::SelectStatement {
    Query::select()
        .from(context::Entity)
        .left_join(
            turn_execution::Entity,
            Expr::col((turn_execution::Entity, turn_execution::Column::TurnId))
                .eq(Expr::col((context::Entity, context::Column::TurnId))),
        )
        .and_where(Expr::col((context::Entity, context::Column::TurnId)).eq(turn_id))
        .cond_where(
            Condition::any()
                .add(Expr::col((turn_execution::Entity, turn_execution::Column::OwnerId)).eq(owner))
                .add(
                    Condition::all()
                        .add(
                            Expr::col((turn_execution::Entity, turn_execution::Column::TurnId))
                                .is_null(),
                        )
                        .add(
                            Expr::col((context::Entity, context::Column::ExecutionOwnerId))
                                .eq(owner),
                        ),
                ),
        )
        .to_owned()
}

/// Only compact digest/scope/owner binds enter writer admission, never JSON.
pub fn prepare_revalidation(snapshot: &NativeCancellationContext, owner: &str) -> Statement {
    let mut query = owned_query(&snapshot.preparation.turn_id, owner);
    query
        .expr(Expr::val(1))
        .inner_join(
            turn::Entity,
            Expr::col((turn::Entity, turn::Column::Id))
                .eq(Expr::col((context::Entity, context::Column::TurnId))),
        )
        .and_where(Expr::col((turn::Entity, turn::Column::Status)).eq("in_progress"))
        .and_where(
            Expr::col((context::Entity, context::Column::ContextSha256))
                .eq(&snapshot.context_sha256),
        )
        .and_where(
            Expr::col((context::Entity, context::Column::ThreadId))
                .eq(&snapshot.preparation.thread_id),
        )
        .and_where(
            Expr::col((context::Entity, context::Column::WorkspaceId))
                .eq(&snapshot.preparation.workspace_id),
        )
        .and_where(Expr::col((context::Entity, context::Column::AcceptedEventId)).is_null());
    DbBackend::Sqlite.build(&query)
}

pub async fn revalidate<C: ConnectionTrait>(db: &C, fence: Statement) -> Result<()> {
    ensure!(
        db.query_one_raw(fence).await?.is_some(),
        "native cancellation immutable context or execution owner changed"
    );
    Ok(())
}

pub async fn mark_accepted<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    event_id: &str,
) -> Result<()> {
    ensure!(
        context::Entity::update_many()
            .col_expr(
                context::Column::AcceptedEventId,
                Expr::value(event_id.to_owned())
            )
            .filter(context::Column::TurnId.eq(turn_id))
            .filter(context::Column::AcceptedEventId.is_null())
            .exec(db)
            .await?
            .rows_affected
            == 1,
        "native cancellation receipt changed during append"
    );
    Ok(())
}

pub async fn has_accepted<C: ConnectionTrait>(db: &C, turn_id: &str) -> Result<bool> {
    Ok(context::Entity::find_by_id(turn_id.to_owned())
        .filter(context::Column::AcceptedEventId.is_not_null())
        .select_only()
        .column(context::Column::TurnId)
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_some())
}

pub async fn has_accepted_owned<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    owner: &str,
) -> Result<bool> {
    let mut query = owned_query(turn_id, owner);
    query
        .expr(Expr::val(1))
        .and_where(Expr::col((context::Entity, context::Column::AcceptedEventId)).is_not_null());
    Ok(db
        .query_one_raw(DbBackend::Sqlite.build(&query))
        .await?
        .is_some())
}

pub async fn receipt_owned<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    owner: &str,
) -> Result<Option<pioneer_protocol::NativeDurableCancellationReceipt>> {
    let mut query = owned_query(turn_id, owner);
    for column in [
        context::Column::WorkspaceId,
        context::Column::ThreadId,
        context::Column::TurnId,
        context::Column::AcceptedEventId,
    ] {
        query.column((context::Entity, column));
    }
    query.and_where(Expr::col((context::Entity, context::Column::AcceptedEventId)).is_not_null());
    let Some(row) = db.query_one_raw(DbBackend::Sqlite.build(&query)).await? else {
        return Ok(None);
    };
    Ok(Some(pioneer_protocol::NativeDurableCancellationReceipt {
        workspace_id: row.try_get("", "workspace_id")?,
        thread_id: row.try_get("", "thread_id")?,
        turn_id: row.try_get("", "turn_id")?,
        execution_owner_id: owner.to_owned(),
        canonical_event_id: row.try_get("", "accepted_event_id")?,
    }))
}
