//! Reconcile foreground state and select missing canonical completion events.
use crate::{
    collaboration::collaboration_from_model,
    events::TurnEventPayload,
    protocol_projection::{parse_turn_permission_profile, parse_turn_prompt_manifest},
    thread_metadata, turn_event_reads,
    values::{thread_status_from_db, turn_kind_to_db, turn_status_to_db, unix_to_datetime},
};
use anyhow::{Context, Result};
use pioneer_crud_access::turn_execution::{turn_kind_from_db, turn_origin_from_db};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_protocol::{ThreadStatus, Turn, TurnItem, TurnKind, TurnStatus, TurnWorkOwner};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use std::{collections::HashSet, future::Future};

pub async fn complete_in_progress_turns_after_final_agent_message<Publish, Published>(
    connection: &PostgresDatabase,
    event_timestamp_secs: i64,
    mut publish: Publish,
) -> Result<u64>
where
    Publish: FnMut(pioneer_protocol::TurnCompletedNotification, i64) -> Published,
    Published: Future<Output = Result<()>>,
{
    let turns = pioneer_entity::turn::Entity::find()
        .filter(pioneer_entity::turn::Column::Status.eq(turn_status_to_db(TurnStatus::InProgress)))
        .all(connection)
        .await
        .context("failed to query in-progress turns for final-agent-message repair")?;

    let mut completed = 0u64;
    for turn_model in turns {
        let Some(latest_event) =
            turn_event_reads::latest_event_for_turn(connection, turn_model.id.as_str()).await?
        else {
            continue;
        };
        let TurnEventPayload::ItemCompleted(notification) = latest_event.payload else {
            continue;
        };
        if !matches!(&notification.item, TurnItem::AgentMessage { .. }) {
            continue;
        }
        let Some(thread_model) =
            thread_metadata::find_thread_by_id(connection, turn_model.thread_id.as_str()).await?
        else {
            continue;
        };

        let prompt_manifest = parse_turn_prompt_manifest(&turn_model)?;
        let permission_profile = parse_turn_permission_profile(&turn_model)?;
        let collaboration = collaboration_from_model(&turn_model)?;
        publish(
            pioneer_protocol::TurnCompletedNotification {
                workspace_id: thread_model.workspace_id,
                thread_id: turn_model.thread_id.clone(),
                turn: Turn {
                    work_owner: TurnWorkOwner::from_db_str(&turn_model.work_owner)
                        .map_err(anyhow::Error::msg)?,
                    id: turn_model.id,
                    status: TurnStatus::Completed,
                    turn_kind: turn_kind_from_db(turn_model.turn_kind.as_str()).unwrap_or_default(),
                    origin: turn_origin_from_db(turn_model.origin.as_str()).unwrap_or_default(),
                    mode: collaboration.mode.effective_mode,
                    author: collaboration.author,
                    reply_to_turn_id: collaboration.reply_to_turn_id,
                    mentions: collaboration.mentions,
                    message_revision: collaboration.message_revision,
                    message_deleted: collaboration.message_deleted,
                    error: None,
                    prompt_manifest,
                    permission_profile,
                },
            },
            event_timestamp_secs,
        )
        .await
        .with_context(|| {
            format!(
                "failed to materialize completion repair for turn `{}`",
                notification.turn_id
            )
        })?;
        completed = completed.saturating_add(1);
    }

    Ok(completed)
}

pub async fn reconcile_thread_foreground_statuses(
    connection: &PostgresDatabase,
    event_timestamp_secs: i64,
) -> Result<u64> {
    let active_conversation_thread_ids = pioneer_entity::turn::Entity::find()
        .filter(pioneer_entity::turn::Column::Status.eq(turn_status_to_db(TurnStatus::InProgress)))
        .filter(pioneer_entity::turn::Column::TurnKind.eq(turn_kind_to_db(TurnKind::Conversation)))
        .all(connection)
        .await
        .context("failed to query in-progress conversation turns for status repair")?
        .into_iter()
        .map(|turn| turn.thread_id)
        .collect::<HashSet<_>>();
    let threads = pioneer_entity::thread::Entity::find()
        .all(connection)
        .await
        .context("failed to query threads for foreground status repair")?;
    let updated_at = unix_to_datetime(event_timestamp_secs);
    let mut repaired = 0u64;

    for thread_model in threads {
        let desired = if active_conversation_thread_ids.contains(thread_model.id.as_str()) {
            ThreadStatus::Active
        } else {
            ThreadStatus::Idle
        };
        let Some(current) = thread_status_from_db(thread_model.status.as_str()) else {
            continue;
        };
        if current == desired {
            continue;
        }
        thread_metadata::update_thread_status(
            connection,
            thread_model.id.as_str(),
            desired,
            updated_at,
        )
        .await?;
        repaired = repaired.saturating_add(1);
    }

    Ok(repaired)
}
