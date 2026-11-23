//! Canonical projection of edits and deletions to terminal messages.
use crate::{thread_metadata, turn_message_revision, turn_reads, turn_writes};
use anyhow::{Context, Result};
use pioneer_protocol::{
    ThreadMode, TurnKind, TurnMessageDeletedEvent, TurnMessageEditedEvent,
    TurnMessageRevisionChangeKind, TurnOrigin, TurnStatus, UserInput,
};
use sea_orm::ConnectionTrait;
use std::future::Future;

pub fn message_thread_preview(input: &[UserInput]) -> String {
    input
        .iter()
        .find_map(|input| match input {
            UserInput::Text { text, .. } => Some(text.trim()),
            _ => None,
        })
        .filter(|text| !text.is_empty())
        .unwrap_or_default()
        .to_owned()
}

pub enum TurnMessageMutationProjection<'a> {
    Edit(&'a TurnMessageEditedEvent),
    Delete(&'a TurnMessageDeletedEvent),
}

#[derive(Clone, Debug)]
pub enum PreparedTurnMessageMutationProjection {
    Superseded {
        expected_turn: pioneer_entity::turn::Model,
    },
    Apply {
        expected_turn: pioneer_entity::turn::Model,
        expected_input_rows: Vec<pioneer_entity::turn_input::Model>,
        expected_previous_revision: Option<pioneer_entity::turn_message_revision::Model>,
        previous_preview: String,
        replacement_preview: String,
        preview_updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
        already_projected: bool,
        revision: Option<turn_message_revision::PreparedTurnMessageRevision>,
        mutation_state: turn_writes::PreparedMessageTurnMutationState,
    },
}

pub async fn prepare_turn_message_mutation<C: ConnectionTrait>(
    db: &C,
    mutation: TurnMessageMutationProjection<'_>,
    updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
) -> Result<PreparedTurnMessageMutationProjection> {
    let (thread_id, event_turn, projected_input, changed_by, change_kind, deleted_by) =
        match mutation {
            TurnMessageMutationProjection::Edit(payload) => (
                payload.thread_id.as_str(),
                &payload.turn,
                payload.input.as_slice(),
                &payload.changed_by,
                TurnMessageRevisionChangeKind::Edit,
                None,
            ),
            TurnMessageMutationProjection::Delete(payload) => (
                payload.thread_id.as_str(),
                &payload.turn,
                &[] as &[UserInput],
                &payload.deleted_by,
                TurnMessageRevisionChangeKind::Delete,
                Some(&payload.deleted_by),
            ),
        };
    let is_delete = deleted_by.is_some();
    if event_turn.mode != ThreadMode::Message
        || event_turn.status != TurnStatus::Completed
        || event_turn.turn_kind != TurnKind::Conversation
        || event_turn.origin != TurnOrigin::User
        || event_turn.message_revision == 0
        || event_turn.message_deleted != is_delete
        || (is_delete && (!projected_input.is_empty() || !event_turn.mentions.is_empty()))
    {
        anyhow::bail!("invalid post-terminal Turn message mutation event");
    }

    let (turn_model, collaboration) =
        turn_writes::find_turn_collaboration(db, thread_id, event_turn.id.as_str())
            .await?
            .context("message mutation event targets a missing Turn")?;

    if !collaboration.mode.message_mutation_eligible
        || turn_model.status != "completed"
        || turn_model.turn_kind != "conversation"
        || turn_model.origin != "user"
    {
        anyhow::bail!("message mutation event targets an immutable Turn");
    }

    let current_revision = collaboration.message_revision;
    if current_revision > event_turn.message_revision {
        return Ok(PreparedTurnMessageMutationProjection::Superseded {
            expected_turn: turn_model,
        });
    }
    if current_revision < event_turn.message_revision
        && current_revision.checked_add(1) != Some(event_turn.message_revision)
    {
        anyhow::bail!("Turn message mutation replay has a revision gap");
    }

    let current_input_rows = turn_reads::find_turn_inputs(db, event_turn.id.as_str()).await?;
    let current_input = current_input_rows
        .iter()
        .map(|row| {
            serde_json::from_str::<UserInput>(row.payload.as_str())
                .context("failed to decode current Turn input during mutation replay")
        })
        .collect::<Result<Vec<_>>>()?;
    let (expected_previous_revision, previous_input) =
        if current_revision < event_turn.message_revision {
            (None, current_input.clone())
        } else {
            let previous_revision_model = turn_message_revision::list_turn_message_revisions(
                db,
                event_turn.id.as_str(),
                Some(event_turn.message_revision),
                1,
            )
            .await?
            .into_iter()
            .next()
            .context("message mutation replay is missing its previous revision")?;
            let previous_revision = turn_message_revision::turn_message_revision_from_model(
                previous_revision_model.clone(),
            )?;
            if previous_revision.revision.checked_add(1) != Some(event_turn.message_revision) {
                anyhow::bail!("message mutation replay is missing its previous revision");
            }
            let previous_input = previous_revision
                .input
                .context("message mutation replay previous revision has no input")?;
            (Some(previous_revision_model), previous_input)
        };
    let previous_preview = message_thread_preview(previous_input.as_slice());
    let replacement_preview = if is_delete {
        String::new()
    } else {
        message_thread_preview(projected_input)
    };
    let already_projected = current_revision == event_turn.message_revision
        && collaboration.mentions == event_turn.mentions
        && collaboration.message_deleted == event_turn.message_deleted
        && current_input == projected_input;

    let revision = if current_revision < event_turn.message_revision {
        Some(turn_message_revision::prepare_turn_message_revision(
            turn_message_revision::NewTurnMessageRevision {
                turn_id: event_turn.id.as_str(),
                revision: current_revision,
                input: current_input.as_slice(),
                mentions: collaboration.mentions.as_slice(),
                changed_by,
                change_kind,
                created_at: updated_at,
            },
        )?)
    } else {
        None
    };
    let mutation_state = turn_writes::prepare_message_turn_mutation_state(
        turn_model.thread_id.as_str(),
        event_turn.id.as_str(),
        event_turn.message_revision,
        event_turn.mentions.as_slice(),
        deleted_by,
        updated_at,
    )?;

    Ok(PreparedTurnMessageMutationProjection::Apply {
        expected_turn: turn_model,
        expected_input_rows: current_input_rows,
        expected_previous_revision,
        previous_preview,
        replacement_preview,
        preview_updated_at: updated_at,
        already_projected,
        revision,
        mutation_state,
    })
}

pub async fn project_turn_message_mutation<
    C: ConnectionTrait + Sync,
    F: Future<Output = Result<()>>,
>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    event_revision: u64,
    prepared: PreparedTurnMessageMutationProjection,
    replace_input: impl FnOnce() -> F,
) -> Result<()> {
    let expected_revision = i64::try_from(event_revision)
        .context("Turn message revision exceeds database integer range")?;
    match prepared {
        PreparedTurnMessageMutationProjection::Superseded { expected_turn } => {
            let current = turn_reads::find_turn_by_thread_and_id(db, thread_id, turn_id)
                .await?
                .context("message mutation event targets a missing Turn")?;
            if current != expected_turn && current.message_revision <= expected_revision {
                anyhow::bail!("Turn message mutation changed during projection preparation");
            }
            Ok(())
        }
        PreparedTurnMessageMutationProjection::Apply {
            expected_turn,
            expected_input_rows,
            expected_previous_revision,
            previous_preview,
            replacement_preview,
            preview_updated_at,
            already_projected,
            revision,
            mutation_state,
        } => {
            let current_turn = turn_reads::find_turn_by_thread_and_id(db, thread_id, turn_id)
                .await?
                .context("message mutation event targets a missing Turn")?;
            if current_turn != expected_turn {
                anyhow::bail!("Turn message mutation changed during projection preparation");
            }
            let current_input_rows = turn_reads::find_turn_inputs(db, turn_id).await?;
            if current_input_rows != expected_input_rows {
                anyhow::bail!("Turn message input changed during projection preparation");
            }
            if let Some(expected_previous_revision) = expected_previous_revision {
                let current_previous_revision = turn_message_revision::list_turn_message_revisions(
                    db,
                    turn_id,
                    Some(event_revision),
                    1,
                )
                .await?
                .into_iter()
                .next();
                if current_previous_revision.as_ref() != Some(&expected_previous_revision) {
                    anyhow::bail!("Turn message revision changed during projection preparation");
                }
            }
            thread_metadata::replace_thread_preview_if_matches(
                db,
                thread_id,
                previous_preview.as_str(),
                replacement_preview.as_str(),
                preview_updated_at,
            )
            .await?;
            if already_projected {
                return Ok(());
            }
            if let Some(revision) = revision {
                turn_message_revision::insert_prepared_turn_message_revision(db, revision, true)
                    .await?;
            }
            if !turn_writes::project_prepared_message_turn_mutation_state(db, mutation_state)
                .await?
            {
                anyhow::bail!("Turn message mutation replay lost its target");
            }
            replace_input().await
        }
    }
}
