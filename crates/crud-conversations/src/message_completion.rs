//! Validate a Message snapshot before its atomic publication.
use crate::events::{TurnEventPayload, TurnStartedEventPayload};
use anyhow::{Result, bail};
use pioneer_protocol::{PersistedActorRef, SandboxMode, Thread, Turn, TurnStatus, UserInput};

pub struct CompletedMessageTurnWrite<'a> {
    pub thread: &'a Thread,
    pub sandbox_mode: SandboxMode,
    pub started_turn: &'a Turn,
    pub input: &'a [UserInput],
    pub actor: PersistedActorRef,
    pub completed: pioneer_protocol::TurnCompletedNotification,
    pub audit_event: pioneer_protocol::TurnPermissionAuditEvent,
}

pub fn completed_message_turn_events(
    write: CompletedMessageTurnWrite<'_>,
) -> Result<Vec<TurnEventPayload>> {
    if write.started_turn.mode != pioneer_protocol::ThreadMode::Message
        || write.started_turn.status != TurnStatus::InProgress
    {
        bail!("instant Message batch requires an in-progress Message start snapshot");
    }
    if write.completed.workspace_id != write.thread.workspace_id
        || write.completed.thread_id != write.thread.id
        || write.completed.turn.id != write.started_turn.id
        || write.completed.turn.mode != pioneer_protocol::ThreadMode::Message
        || write.completed.turn.status != TurnStatus::Completed
        || write.completed.turn.author != write.started_turn.author
        || write.completed.turn.reply_to_turn_id != write.started_turn.reply_to_turn_id
        || write.completed.turn.mentions != write.started_turn.mentions
        || write.completed.turn.message_revision != write.started_turn.message_revision
        || write.completed.turn.message_deleted != write.started_turn.message_deleted
    {
        bail!("instant Message completion differs from its start snapshot");
    }
    if write.audit_event.workspace_id != write.thread.workspace_id
        || write.audit_event.thread_id != write.thread.id
        || write.audit_event.turn_id != write.started_turn.id
    {
        bail!("instant Message permission audit scope differs from Turn scope");
    }
    Ok(vec![
        TurnEventPayload::TurnStarted(TurnStartedEventPayload {
            thread: write.thread.clone(),
            sandbox_mode: write.sandbox_mode,
            turn: write.started_turn.clone(),
            input: write.input.to_vec(),
            // An agent-authored Message does not select or run a model. Preserve
            // the destination's configuration, including an intentional None,
            // so single-thread and list snapshots restore the same selection.
            reasoning_effort: if matches!(&write.actor, PersistedActorRef::AgentExecution(_)) {
                write.thread.reasoning_effort.clone()
            } else {
                None
            },
            actor: Some(write.actor),
            work_owner: Default::default(),
        }),
        TurnEventPayload::TurnCompleted(write.completed),
        TurnEventPayload::TurnPermissionAudit(write.audit_event),
    ])
}
