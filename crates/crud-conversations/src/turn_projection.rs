//! Conversation identity and foreground projections over the caller's connection.
use crate::{events::TurnStartedEventPayload, thread_metadata as thread, thread_mutations};
use anyhow::{Context, Result};
use pioneer_crud_access::policy;
use pioneer_protocol::{PersistedActorRef, ThreadMode, ThreadStatus};
use sea_orm::ConnectionTrait;
use std::{future::Future, pin::Pin};

type ProjectFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

fn project_future<'a>(future: impl Future<Output = Result<()>> + Send + 'a) -> ProjectFuture<'a> {
    Box::pin(future)
}

pub fn project_turn_started_identity<'a, C: ConnectionTrait + Sync>(
    db: &'a C,
    payload: &'a TurnStartedEventPayload,
    event_sequence: i64,
) -> ProjectFuture<'a> {
    project_future(async move {
        // Sequence 1 can replay an in-progress projection after state loss.
        // A later TurnStarted would reuse the identity of the existing turn.
        if let Some(existing) =
            crate::turn_reads::find_turn_by_id(db, payload.turn.id.as_str()).await?
            && (event_sequence != 1 || existing.status != "in_progress")
        {
            anyhow::bail!(
                "turn `{}` already exists with status `{}`; TurnStarted identity reuse is forbidden",
                payload.turn.id,
                existing.status
            );
        }

        Ok(())
    })
}

#[allow(clippy::too_many_arguments)]
pub fn project_turn_started_thread<'a, C, Publish, Published>(
    db: &'a C,
    payload: &'a TurnStartedEventPayload,
    actor: Option<&'a PersistedActorRef>,
    thread_created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    thread_updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    is_task_run_occurrence: bool,
    thread_preview_author_json: Option<String>,
    message_preview_author_json: Option<String>,
    message_thread_preview: String,
    task_run_parent_thread: Option<pioneer_protocol::Thread>,
    publish_thread_change: Publish,
) -> ProjectFuture<'a>
where
    C: ConnectionTrait + Sync + 'a,
    Publish: FnOnce(String) -> Published + Send + 'a,
    Published: Future<Output = Result<()>> + Send + 'a,
{
    project_future(async move {
        if is_task_run_occurrence {
            // A task-run occurrence is only a parent-timeline projection. The
            // parent thread and its foreground policy already exist, and a
            // stale task payload must not overwrite metadata or foreground
            // state written by a concurrent conversation turn.
            if thread::find_thread_by_id(db, payload.thread.id.as_str())
                .await?
                .is_none()
            {
                let parent = task_run_parent_thread
                    .context("task-run parent Thread projection was not prepared")?;
                if thread_mutations::upsert_projected_thread(
                    db,
                    &parent,
                    actor,
                    thread_preview_author_json,
                    thread_created_at,
                    thread_updated_at,
                )
                .await?
                {
                    publish_thread_change(parent.id.clone()).await?;
                }
                policy::upsert_thread_sandbox_policy(
                    db,
                    parent.id.as_str(),
                    payload.sandbox_mode,
                    thread_created_at,
                    thread_updated_at,
                )
                .await?;
            }
        } else if payload.turn.mode == ThreadMode::Message
            && thread::find_thread_by_id(db, payload.thread.id.as_str())
                .await?
                .is_some()
        {
            // Message reuses the canonical TurnStarted envelope, but it must
            // not replay the envelope's full Thread snapshot over concurrent
            // execution or management changes. New threads still take the
            // normal insertion path below.
            thread::touch_thread_for_completed_message_prepared(
                db,
                payload.thread.id.as_str(),
                message_thread_preview.as_str(),
                message_preview_author_json,
                thread_updated_at,
            )
            .await?;
            if policy::find_thread_sandbox_mode(db, payload.thread.id.as_str())
                .await?
                .is_none()
            {
                // Atomic first-Message materialization inserts the user thread
                // before this event is projected. Seed its existing policy row
                // without replacing a policy changed by another operation.
                policy::upsert_thread_sandbox_policy(
                    db,
                    payload.thread.id.as_str(),
                    payload.sandbox_mode,
                    thread_created_at,
                    thread_updated_at,
                )
                .await?;
            }
        } else {
            let existing_thread = thread::find_thread_by_id(db, payload.thread.id.as_str())
                .await?
                .is_some();
            let creator = (!existing_thread).then_some(actor).flatten();
            if thread_mutations::upsert_projected_thread(
                db,
                &payload.thread,
                creator,
                thread_preview_author_json,
                thread_created_at,
                thread_updated_at,
            )
            .await?
            {
                publish_thread_change(payload.thread.id.clone()).await?;
            }
            policy::upsert_thread_sandbox_policy(
                db,
                payload.thread.id.as_str(),
                payload.sandbox_mode,
                thread_created_at,
                thread_updated_at,
            )
            .await?;
        }

        Ok(())
    })
}

pub async fn project_thread_foreground_status<C: ConnectionTrait + Sync>(
    db: &C,
    thread_id: &str,
    updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
) -> Result<()> {
    let status = if crate::turn_reads::has_in_progress_conversation_turn(db, thread_id).await? {
        ThreadStatus::Active
    } else {
        ThreadStatus::Idle
    };
    thread::update_thread_status(db, thread_id, status, updated_at).await
}
