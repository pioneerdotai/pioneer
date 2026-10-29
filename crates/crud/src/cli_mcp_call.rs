//! Finalize a managed CLI MCP item from its confirmed invocation outcome.
use crate::*;

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

/// Recognizes host-enriched MCP lifecycle items, not tool names or arguments.
pub fn is_managed_cli_mcp_item(item: &TurnItem) -> bool {
    let TurnItem::DynamicToolCall {
        id,
        tool_name,
        storage,
        ..
    } = item
    else {
        return false;
    };
    let metadata = match storage {
        ToolStoragePayload::Summary(summary) => &summary.metadata,
        ToolStoragePayload::Metadata { metadata } => metadata,
        _ => return false,
    };
    metadata
        .get("canonicalCallableName")
        .and_then(|v| v.as_str())
        == Some(tool_name.as_str())
        && metadata.get("providerCallId").and_then(|v| v.as_str()) == Some(id.as_str())
        && metadata
            .get("sessionGeneration")
            .and_then(|v| v.as_u64())
            .is_some()
        && metadata
            .get("manifestHash")
            .and_then(|v| v.as_str())
            .is_some()
        && metadata
            .get("invocationCorrelationId")
            .and_then(|v| v.as_str())
            .is_some()
}

impl CrudStore {
    /// The checked native claim is supplied by the managed adapter. Recheck its
    /// durable execution/generation and the current item attempt in the same
    /// writer transaction as canonical acceptance. Projection uses the existing
    /// replay lane if an accepted predecessor has not yet been projected.
    pub async fn materialize_cli_mcp_call_completed(
        &self,
        notification: pioneer_protocol::ItemCompletedNotification,
        identity: CliMcpCallIdentity,
        event_timestamp_secs: i64,
    ) -> Result<()> {
        anyhow::ensure!(
            notification.item.item_id() == identity.native_item_id,
            "CLI MCP completion item identity mismatch"
        );
        let created_at = unix_to_datetime(event_timestamp_secs);
        let claim_expires_at =
            unix_to_datetime(event_timestamp_secs.saturating_add(TURN_EVENT_PROJECTION_LEASE_SECS));
        // No response serialization or policy preparation holds the writer.
        let prepared = prepare_projected_turn_event_for_permanent_storage(
            &self.connection,
            TurnEventPayload::ItemCompleted(notification.clone()),
            created_at,
        )
        .await?;
        self.run_serialized_write(|| async {
            let prepared = prepared.clone();
            let transaction = self.connection.begin().await?;
            let binding =
                cli_runtime_binding::find_turn_binding(&transaction, &notification.turn_id)
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
                &transaction,
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
            let replay =
                turn_event::prepared_event_already_exists(&transaction, &prepared.event).await?;
            if !replay {
                let execution = turn_execution::find(&transaction, &notification.turn_id).await?;
                anyhow::ensure!(
                    execution
                        .as_ref()
                        .map(|execution| (execution.owner_id.clone(), execution.owner_generation))
                        == identity.execution_owner,
                    "CLI MCP execution lease owner changed"
                );
                let latest =
                    cli_runtime_binding::latest_turn_attempt(&transaction, &notification.turn_id)
                        .await?
                        .context("CLI MCP current native attempt is missing")?;
                let turn = turn::find_turn_by_thread_and_id(
                    &transaction,
                    &notification.thread_id,
                    &notification.turn_id,
                )
                .await?
                .context("CLI MCP turn is missing")?;
                anyhow::ensure!(
                    latest.id == owner.attempt.id && turn.thread_id == notification.thread_id,
                    "CLI MCP completion execution is no longer current"
                );
                if let Some(item) = turn::find_turn_item(
                    &transaction,
                    &notification.turn_id,
                    &identity.native_item_id,
                )
                .await?
                {
                    if item.active_attempt_status.as_deref() == Some("interrupted") {
                        // Turn closure may have won the writer race. It recorded
                        // an unknown outcome, not an execution fact. Correct only
                        // that exact original attempt; never retry the operation.
                        anyhow::ensure!(
                            turn_item_attempt::is_unconfirmed_cli_mcp_attempt(
                                &transaction,
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
                            &transaction,
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
            self.append_and_project_turn_event_with_replay_in_transaction(
                &transaction,
                prepared,
                created_at,
                claim_expires_at,
                true,
                false,
                true,
            )
            .await?;
            transaction.commit().await?;
            Ok(())
        })
        .await
    }
}

/// Late native lifecycle observations must not append a new in-progress fact
/// after the managed invocation's canonical terminal result.
pub(crate) async fn cli_mcp_observation_is_superseded<C: ConnectionTrait>(
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
    if turn_event::has_accepted_item_completion(db, turn_id, item.item_id(), before_sequence)
        .await?
    {
        return Ok(true);
    }
    // A replayed observation preceding completion must still create/update its
    // original attempt. Only admission of a new observation uses this fallback.
    Ok(before_sequence.is_none()
        && turn::find_turn_item(db, turn_id, item.item_id())
            .await?
            .is_some_and(|item| item.status.as_deref() != Some("in_progress")))
}
