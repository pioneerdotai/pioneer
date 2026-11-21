//! Durable identity and replay fences for confirmed CLI MCP outcomes.
use crate::{
    cli_mcp_item::{cli_mcp_item_metadata, is_managed_cli_mcp_item},
    cli_runtime_binding, event_append, turn_execution, turn_item_attempt,
};
use anyhow::{Context, Result};
use pioneer_crud_conversations::{
    events::TurnEventPayload, turn_event_reads, turn_item, turn_payload, turn_reads,
};
use pioneer_protocol::{ItemCompletedNotification, TurnItemAttemptStatus};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ConnectionTrait, DatabaseTransaction, entity::prelude::DateTimeWithTimeZone};

#[derive(Debug, Clone)]
pub struct CliMcpCallIdentity {
    pub runtime_id: String,
    pub session_generation: i64,
    pub native_thread_id: String,
    pub native_turn_id: String,
    pub native_item_id: String,
    pub manifest_hash: String,
    pub projection_activation_generation: i64,
    pub execution_owner: Option<(String, u64)>,
}

// A durable tool receipt can replay after the facade ledger is lost.
// Its response/duration may differ, while the original canonical
// execution fact must remain unchanged. Decode outside the writer and
// carry the immutable event bytes back as a commit-time fence.
pub async fn find_confirmed_completion(
    db: &SqliteDatabase,
    notification: &ItemCompletedNotification,
    identity: &CliMcpCallIdentity,
) -> Result<Option<pioneer_entity::turn_event::Model>> {
    let accepted = turn_event_reads::find_accepted_item_completion(
        db,
        &notification.turn_id,
        &identity.native_item_id,
    )
    .await?;
    Ok(if let Some(event) = accepted {
        let event = turn_payload::hydrate_events(db, vec![event])
            .await?
            .pop()
            .unwrap();
        let payload: TurnEventPayload = serde::Deserialize::deserialize(
            event
                .payload
                .as_ref()
                .context("missing canonical event payload")?,
        )?;
        match payload {
            TurnEventPayload::ItemCompleted(saved)
                if saved.workspace_id == notification.workspace_id
                    && saved.thread_id == notification.thread_id
                    && saved.turn_id == notification.turn_id
                    && saved.item.item_id() == notification.item.item_id()
                    && is_managed_cli_mcp_item(&saved.item) =>
            {
                let saved_metadata =
                    cli_mcp_item_metadata(&saved.item).expect("managed MCP completion metadata");
                let incoming_metadata = cli_mcp_item_metadata(&notification.item)
                    .context("CLI MCP completion metadata is missing")?;
                [
                    "canonicalCallableName",
                    "providerCallId",
                    "sessionGeneration",
                    "manifestHash",
                    "invocationCorrelationId",
                    "responseDelivery",
                    "serverInstallationId",
                    "canonicalSchemaFingerprint",
                    "providerSchemaFingerprint",
                    "arguments",
                ]
                .iter()
                .all(|key| {
                    saved_metadata
                        .get(key)
                        .is_some_and(|value| Some(value) == incoming_metadata.get(key))
                })
                .then_some(event)
            }
            _ => None,
        }
    } else {
        None
    })
}

/// Revalidate execution ownership and the exact prior completion inside the
/// caller's canonical publication transaction.
pub async fn completion_already_confirmed(
    tx: &DatabaseTransaction,
    notification: &ItemCompletedNotification,
    identity: &CliMcpCallIdentity,
    prepared: &event_append::PreparedTurnEvent,
    confirmed: Option<&pioneer_entity::turn_event::Model>,
) -> Result<bool> {
    let binding = cli_runtime_binding::find_turn_binding(tx, &notification.turn_id)
        .await?
        .context("CLI MCP turn binding is missing")?;
    anyhow::ensure!(
        binding.workspace_id == notification.workspace_id
            && binding.thread_id == notification.thread_id
            && binding.runtime_id == identity.runtime_id
            && binding.native_thread_id == identity.native_thread_id
            && binding
                .mcp
                .as_ref()
                .is_some_and(|mcp| mcp.session_generation == identity.session_generation
                    && mcp.manifest_hash == identity.manifest_hash
                    && mcp.projection_activation_generation
                        == identity.projection_activation_generation),
        "CLI MCP completion binding or generation changed"
    );
    let owner = cli_runtime_binding::resolve_native_turn_owner(
        tx,
        &identity.runtime_id,
        &identity.native_turn_id,
    )
    .await?
    .context("CLI MCP native execution is missing")?;
    anyhow::ensure!(
        owner.binding.turn_id == notification.turn_id
            && owner.attempt.native_thread_id == identity.native_thread_id,
        "CLI MCP completion native execution mismatch"
    );
    // Exact durable replay uses the existing event idempotency key;
    // it never executes the operation again or repairs another attempt.
    let replay = event_append::prepared_event_already_exists(tx, prepared).await?;
    if !replay {
        let execution = turn_execution::find(tx, &notification.turn_id).await?;
        anyhow::ensure!(
            execution
                .as_ref()
                .map(|execution| (execution.owner_id.clone(), execution.owner_generation))
                == identity.execution_owner,
            "CLI MCP execution lease owner changed"
        );
        let latest = cli_runtime_binding::latest_turn_attempt(tx, &notification.turn_id)
            .await?
            .context("CLI MCP current native attempt is missing")?;
        let turn = turn_reads::find_turn_by_thread_and_id(
            tx,
            &notification.thread_id,
            &notification.turn_id,
        )
        .await?
        .context("CLI MCP turn is missing")?;
        anyhow::ensure!(
            latest.id == owner.attempt.id && turn.thread_id == notification.thread_id,
            "CLI MCP completion execution is no longer current"
        );
        if let Some(confirmed) = confirmed {
            let current = turn_event_reads::find_accepted_item_completion(
                tx,
                &notification.turn_id,
                &identity.native_item_id,
            )
            .await?;
            anyhow::ensure!(
                current.is_some_and(|event| event.id == confirmed.id
                    && event.portable_identity == confirmed.portable_identity
                    && event.portable_revision == confirmed.portable_revision),
                "CLI MCP canonical completion changed during replay"
            );
            return Ok(true);
        }
        if let Some(item) =
            turn_item::find_turn_item_metadata(tx, &notification.turn_id, &identity.native_item_id)
                .await?
        {
            if item.active_attempt_status.as_deref() == Some("interrupted") {
                // Turn closure may have won the writer race. It recorded
                // an unknown outcome, not an execution fact. Correct only
                // that exact original attempt; never retry the operation.
                anyhow::ensure!(
                    turn_item_attempt::is_unconfirmed_cli_mcp_attempt(
                        tx,
                        &notification.turn_id,
                        &identity.native_item_id,
                        item.active_attempt_id
                            .as_deref()
                            .context("CLI MCP attempt is missing")?,
                    )
                    .await?,
                    "CLI MCP item attempt is already terminal"
                );
            } else {
                anyhow::ensure!(
                    item.status.as_deref() == Some("in_progress")
                        && item.active_attempt_status.as_deref() == Some("running"),
                    "CLI MCP item attempt is already terminal"
                );
                let attempt = turn_item_attempt::find_latest_running_attempt(
                    tx,
                    &notification.turn_id,
                    &identity.native_item_id,
                )
                .await?
                .context("CLI MCP running attempt is missing")?;
                anyhow::ensure!(
                    item.active_attempt_id.as_deref() == Some(attempt.id.as_str()),
                    "CLI MCP completion does not own the original item attempt"
                );
            }
        }
    }
    Ok(false)
}

/// Late native lifecycle observations must not append a new in-progress fact
/// after the managed invocation's canonical terminal result.
pub async fn cli_mcp_observation_is_superseded<C: ConnectionTrait>(
    db: &C,
    event: &TurnEventPayload,
    before_sequence: Option<i64>,
) -> Result<bool> {
    let (turn_id, item) = match event {
        TurnEventPayload::ItemStarted(n) => (&n.turn_id, &n.item),
        TurnEventPayload::ItemUpdated(n) => (&n.turn_id, &n.item),
        _ => return Ok(false),
    };
    if !is_managed_cli_mcp_item(item) {
        return Ok(false);
    }
    if turn_event_reads::has_accepted_item_completion(db, turn_id, item.item_id(), before_sequence)
        .await?
    {
        return Ok(true);
    }
    // A replayed observation preceding completion must still create/update its
    // original attempt. Only admission of a new observation uses this fallback.
    Ok(before_sequence.is_none()
        && turn_item::find_turn_item_metadata(db, turn_id, item.item_id())
            .await?
            .is_some_and(|item| item.status.as_deref() != Some("in_progress")))
}

/// Revalidate the attempt observed by the canonical MCP completion before projection.
pub async fn fence_projected_completion_attempt(
    db: &impl ConnectionTrait,
    turn_id: &str,
    item_id: &str,
    status: TurnItemAttemptStatus,
    failure_reason: Option<String>,
    sequence: i64,
    created_at: DateTimeWithTimeZone,
) -> Result<()> {
    let Some(item) = turn_item::find_turn_item_metadata(db, turn_id, item_id).await? else {
        return Ok(());
    };
    if item.active_attempt_status.as_deref() == Some("interrupted") {
        anyhow::ensure!(
            turn_item_attempt::finish_unconfirmed_cli_mcp_attempt(
                db,
                turn_id,
                item_id,
                item.active_attempt_id
                    .as_deref()
                    .context("CLI MCP attempt is missing")?,
                status,
                failure_reason,
                sequence,
                created_at,
            )
            .await?,
            "CLI MCP outcome no longer owns the unconfirmed attempt"
        );
    } else {
        let attempt = turn_item_attempt::find_latest_running_attempt(db, turn_id, item_id)
            .await?
            .context("CLI MCP outcome has no running attempt")?;
        anyhow::ensure!(
            item.active_attempt_id.as_deref() == Some(attempt.id.as_str())
                && attempt
                    .started_event_sequence
                    .is_some_and(|started| started < sequence),
            "CLI MCP outcome no longer owns the item attempt"
        );
    }
    Ok(())
}
