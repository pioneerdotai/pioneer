use anyhow::{Context, Result};
use pioneer_entity::turn;
use pioneer_protocol::{PersistedActorRef, TurnKind, TurnStatus, TurnWorkOwner};
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect};

use crate::identity::actor_ref_from_db;

pub fn authorization_context_as_text() -> Expr {
    Expr::col((
        turn::Entity,
        turn::Column::ExecutionAuthorizationContextJson,
    ))
    .cast_as("text")
}

/// Durable facts used by execution binding checks, without presentation or
/// permission snapshots. Unknown authority enums must never become defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnExecutionBinding {
    pub id: String,
    pub turn_kind: TurnKind,
    pub origin: pioneer_protocol::TurnOrigin,
    pub work_owner: TurnWorkOwner,
}

pub async fn find_turn_execution_binding<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<TurnExecutionBinding>> {
    let row = turn::Entity::find_by_id(turn_id.to_owned())
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .select_only()
        .columns([
            turn::Column::Id,
            turn::Column::Status,
            turn::Column::TurnKind,
            turn::Column::Origin,
            turn::Column::WorkOwner,
        ])
        .into_tuple::<(String, String, String, String, String)>()
        .one(db)
        .await
        .context("failed to query turn execution binding")?;
    row.map(|(id, status, kind, origin, work_owner)| {
        // Preserve the full Turn decoder's rejection of an unknown status even
        // for ancestor bindings, whose liveness is not otherwise required.
        turn_status_from_db(&status).context("unknown execution binding turn status")?;
        Ok(TurnExecutionBinding {
            id,
            turn_kind: turn_kind_from_db(&kind).context("unknown execution binding turn kind")?,
            origin: turn_origin_from_db(&origin)
                .context("unknown execution binding turn origin")?,
            work_owner: TurnWorkOwner::from_db_str(&work_owner).map_err(anyhow::Error::msg)?,
        })
    })
    .transpose()
}

pub async fn find_turn_execution_status<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<TurnStatus>> {
    let status = turn::Entity::find_by_id(turn_id.to_owned())
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .select_only()
        .column(turn::Column::Status)
        .into_tuple::<String>()
        .one(db)
        .await
        .context("failed to query turn execution status")?;
    status
        .map(|status| turn_status_from_db(&status).context("unknown execution turn status"))
        .transpose()
}

pub async fn find_turn_initiator<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<PersistedActorRef>> {
    let Some((actor_kind, actor_id)) = turn::Entity::find_by_id(turn_id.to_owned())
        .select_only()
        .columns([
            turn::Column::InitiatedByActorKind,
            turn::Column::InitiatedByActorId,
        ])
        .into_tuple::<(Option<String>, Option<String>)>()
        .one(db)
        .await
        .context("failed to query turn by id")?
    else {
        return Ok(None);
    };
    actor_ref_from_db(actor_kind.as_deref(), actor_id.as_deref())
        .with_context(|| format!("turn `{turn_id}` has an invalid persisted initiator pair"))
}

pub async fn find_turn_execution_authorization_context<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<String>> {
    Ok(turn::Entity::find_by_id(turn_id.to_owned())
        .select_only()
        .column_as(
            authorization_context_as_text(),
            "authorization_context_json",
        )
        .into_tuple::<Option<String>>()
        .one(db)
        .await
        .context("failed to query turn by id")?
        .flatten()
        .filter(|json| !json.trim().is_empty()))
}

pub fn turn_status_from_db(value: &str) -> Option<TurnStatus> {
    match value {
        "in_progress" => Some(TurnStatus::InProgress),
        "completed" => Some(TurnStatus::Completed),
        "failed" => Some(TurnStatus::Failed),
        "interrupted" => Some(TurnStatus::Interrupted),
        "blocked" => Some(TurnStatus::Blocked),
        _ => None,
    }
}

pub fn turn_kind_from_db(value: &str) -> Option<pioneer_protocol::TurnKind> {
    match value {
        "conversation" => Some(pioneer_protocol::TurnKind::Conversation),
        "task_run" => Some(pioneer_protocol::TurnKind::TaskRun),
        _ => None,
    }
}

pub fn turn_origin_from_db(value: &str) -> Option<pioneer_protocol::TurnOrigin> {
    match value {
        "user" => Some(pioneer_protocol::TurnOrigin::User),
        "scheduled_task" => Some(pioneer_protocol::TurnOrigin::ScheduledTask),
        "detached_task" => Some(pioneer_protocol::TurnOrigin::DetachedTask),
        "attached_task" => Some(pioneer_protocol::TurnOrigin::AttachedTask),
        "task_delivery" => Some(pioneer_protocol::TurnOrigin::TaskDelivery),
        _ => None,
    }
}

pub async fn find_turn_execution_authorization_contexts<C: ConnectionTrait>(
    db: &C,
    turn_ids: &[String],
) -> Result<std::collections::HashMap<String, String>> {
    use pioneer_entity::turn as entity;
    let mut result = std::collections::HashMap::new();
    for ids in turn_ids.chunks(64) {
        let rows = entity::Entity::find()
            .select_only()
            .column(entity::Column::Id)
            .column_as(
                authorization_context_as_text(),
                "authorization_context_json",
            )
            .filter(entity::Column::Id.is_in(ids.iter().cloned()))
            .into_tuple::<(String, Option<String>)>()
            .all(db)
            .await?;
        for (id, encoded) in rows {
            if let Some(encoded) = encoded.filter(|s| !s.trim().is_empty()) {
                result.insert(id, encoded);
            }
        }
    }
    Ok(result)
}
