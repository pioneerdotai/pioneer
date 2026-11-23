use anyhow::{Context, Result, bail};
use pioneer_entity::turn_event;
use pioneer_protocol::{PersistedActorRef, ThreadMode, UserInput};

use crate::events::CanonicalTurnEventPayload;

#[derive(Debug, Clone, PartialEq)]
pub struct CanonicalTurnEventRecord {
    pub event_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub sequence: i64,
    pub created_at: chrono::DateTime<chrono::FixedOffset>,
    pub payload: CanonicalTurnEventPayload,
}

impl CanonicalTurnEventRecord {
    /// Returns the durable initiator carried by this turn/start event.
    ///
    /// Older canonical envelopes can decode without an actor. A new
    /// PostgreSQL projection requires explicit attribution.
    pub fn turn_start_actor(&self) -> Option<&PersistedActorRef> {
        match &self.payload {
            CanonicalTurnEventPayload::TurnStarted(started) => started.actor.as_ref(),
            _ => None,
        }
    }
}

pub fn project_current_message_input(
    record: &mut CanonicalTurnEventRecord,
    input: &[UserInput],
) -> Result<()> {
    let CanonicalTurnEventPayload::TurnStarted(started) = &mut record.payload else {
        return Ok(());
    };
    if started.turn.mode != ThreadMode::Message {
        bail!(
            "canonical Message turn-start event `{}` has non-Message mode",
            record.event_id
        );
    }
    started.input = input.to_vec();
    Ok(())
}

pub fn hydrate_legacy_turn_start_actor(
    record: &mut CanonicalTurnEventRecord,
    owning_actor: Option<&pioneer_protocol::PersistedActorRef>,
) -> Result<()> {
    let CanonicalTurnEventPayload::TurnStarted(started) = &mut record.payload else {
        return Ok(());
    };
    match (started.actor.as_ref(), owning_actor) {
        (Some(event_actor), Some(owning_actor)) if event_actor != owning_actor => bail!(
            "canonical turn-start event `{}` actor does not match owning turn",
            record.event_id
        ),
        (Some(_), Some(_)) | (None, None) => {}
        (None, Some(owning_actor)) => {
            // Canonical history derives the actor in this decoded copy of an
            // append-only event without changing the stored envelope.
            started.actor = Some(owning_actor.clone());
        }
        (Some(_), None) => bail!(
            "canonical turn-start event `{}` has an actor but owning turn does not",
            record.event_id
        ),
    }
    Ok(())
}

pub fn decode_event(
    row: turn_event::Model,
    workspace_id: &str,
) -> Result<CanonicalTurnEventRecord> {
    let mut payload = serde_json::from_value::<CanonicalTurnEventPayload>(
        row.payload.context("missing canonical event payload")?,
    )
    .with_context(|| format!("failed to decode canonical turn event `{}`", row.id))?;
    if payload.workspace_id() != workspace_id
        || payload.thread_id() != row.thread_id
        || payload.turn_id() != row.turn_id
    {
        bail!(
            "canonical turn event `{}` payload identity mismatch",
            row.id
        );
    }
    if payload.event_type() != row.event_type {
        bail!("canonical turn event `{}` payload type mismatch", row.id);
    }
    if let CanonicalTurnEventPayload::TurnStarted(started) = &mut payload {
        started.turn.work_owner = started.work_owner;
        // The persisted event carries a client-facing Thread snapshot. Its preview and turn list
        // can contain unrelated concurrent exchanges, while the canonical history record is
        // scoped to the exact `started.turn` and `started.input` below.
        started.thread.preview.clear();
        started.thread.turns.clear();
    }
    Ok(CanonicalTurnEventRecord {
        event_id: row.id,
        thread_id: row.thread_id,
        turn_id: row.turn_id,
        sequence: row.sequence,
        created_at: row.created_at,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use pioneer_protocol::{
        SandboxMode, Thread, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus, Turn,
        TurnKind, TurnOrigin, TurnStatus, default_turn_permission_profile_snapshot,
    };

    use super::*;
    use crate::events::CanonicalTurnStartedEventPayload;

    fn message_start_record(mode: ThreadMode) -> CanonicalTurnEventRecord {
        let turn = Turn {
            work_owner: Default::default(),
            id: "turn_message".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: TurnKind::Conversation,
            origin: TurnOrigin::User,
            mode,
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: default_turn_permission_profile_snapshot(),
        };
        CanonicalTurnEventRecord {
            event_id: "event_start".to_owned(),
            thread_id: "thread_message".to_owned(),
            turn_id: turn.id.clone(),
            sequence: 1,
            created_at: chrono::FixedOffset::east_opt(0)
                .expect("UTC offset")
                .timestamp_opt(1_900_000_000, 0)
                .single()
                .expect("timestamp"),
            payload: CanonicalTurnEventPayload::TurnStarted(CanonicalTurnStartedEventPayload {
                thread: Thread {
                    context_window: None,
                    service_mode: None,
                    context_authority: None,
                    workspace_id: "workspace_message".to_owned(),
                    id: "thread_message".to_owned(),
                    name: None,
                    preview: String::new(),
                    preview_author: None,
                    mode: ThreadMode::Message,
                    model: "model".to_owned(),
                    model_provider: "provider".to_owned(),
                    reasoning_effort: None,
                    created_at: 1_900_000_000,
                    updated_at: 1_900_000_000,
                    status: ThreadStatus::Idle,
                    origin_kind: ThreadOriginKind::Collaborative,
                    sidebar_visibility: ThreadSidebarVisibility::Visible,
                    agent_nickname: None,
                    agent_role: None,
                    visibility: None,
                    turns: Vec::new(),
                },
                sandbox_mode: SandboxMode::FullAccess,
                turn,
                input: vec![UserInput::Text {
                    text: "append-only original".to_owned(),
                    text_elements: Vec::new(),
                }],
                actor: None,
                reasoning_effort: None,
                work_owner: Default::default(),
            }),
        }
    }

    #[test]
    fn canonical_message_history_uses_current_input_and_rejects_mode_mismatch() {
        let current = vec![UserInput::Text {
            text: "current edited text".to_owned(),
            text_elements: Vec::new(),
        }];
        let mut message = message_start_record(ThreadMode::Message);
        project_current_message_input(&mut message, current.as_slice())
            .expect("Message projection should accept current input");
        let CanonicalTurnEventPayload::TurnStarted(started) = message.payload else {
            panic!("fixture must remain a TurnStarted event");
        };
        assert_eq!(started.input, current);

        let mut inconsistent = message_start_record(ThreadMode::Agent);
        assert!(project_current_message_input(&mut inconsistent, &[]).is_err());
    }
}
