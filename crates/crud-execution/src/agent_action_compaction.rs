//! Bounded retention of exact replay identities for terminal agent actions.

use crate::execution_reads::{load_agent_execution_resource_state, load_agent_turn_response};
use crate::outbox_claim::AGENT_START_TERMINAL_FAILURE_CLASSES;
use crate::outbox_retention::{
    AGENT_ACTION_LEDGER_PAYLOAD_RETENTION_DAYS, payload_not_compacted, terminal_ledger_condition,
};
use anyhow::{Context, Result, bail};
use chrono::Duration;
use pioneer_entity::{agent_action, agent_action_outbox, agent_action_receipt};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
    TransactionSession, TransactionTrait,
};
use sha2::{Digest, Sha256};

const AGENT_BACKGROUND_BATCH_LIMIT: u64 = 512;
pub const AGENT_ACTION_COMPACTION_FORMAT: &str = "agent_action_response_v1";
pub const AGENT_ACTION_RECEIPT_COMPACTION_FORMAT: &str = "agent_action_receipt_response_v1";
pub const AGENT_ACTION_OUTBOX_COMPACTION_FORMAT: &str = "agent_action_outbox_payload_v1";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentActionLedgerCompactionSummary {
    pub candidate_rows: u64,
    pub compacted_rows: u64,
    pub payload_bytes_released: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentLedgerCursor {
    pub created_at: DateTimeWithTimeZone,
    pub id: String,
    pub upper_created_at: DateTimeWithTimeZone,
    pub upper_id: String,
}

pub async fn compact_terminal_agent_action_ledger<C>(
    db: &C,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> Result<AgentActionLedgerCompactionSummary>
where
    C: ConnectionTrait + TransactionTrait,
{
    compact_terminal_agent_action_ledger_page(db, now, limit, None)
        .await
        .map(|(summary, _)| summary)
}

/// Continue `next` until None, even when compacted_rows == 0. A skipped proof
/// is not the end of the finite scan. Restart at None after source invalidation.
pub async fn compact_terminal_agent_action_ledger_page<C>(
    db: &C,
    now: DateTimeWithTimeZone,
    limit: u64,
    after: Option<&AgentLedgerCursor>,
) -> Result<(
    AgentActionLedgerCompactionSummary,
    Option<AgentLedgerCursor>,
)>
where
    C: ConnectionTrait + TransactionTrait,
{
    if limit == 0 {
        return Ok((AgentActionLedgerCompactionSummary::default(), None));
    }
    if limit > AGENT_BACKGROUND_BATCH_LIMIT {
        bail!("Agent action compaction batch exceeds its bounded limit");
    }
    let cutoff = now - Duration::days(AGENT_ACTION_LEDGER_PAYLOAD_RETENTION_DAYS);
    let terminal = terminal_ledger_condition();
    let after_condition = match after {
        Some(after) => Condition::any()
            .add(agent_action_outbox::Column::CreatedAt.gt(after.created_at))
            .add(
                Condition::all()
                    .add(agent_action_outbox::Column::CreatedAt.eq(after.created_at))
                    .add(agent_action_outbox::Column::Id.gt(after.id.clone())),
            ),
        None => Condition::all(),
    };
    let (upper_created_at, upper_id) = if let Some(cursor) = after {
        (cursor.upper_created_at, cursor.upper_id.clone())
    } else {
        let Some(upper) = agent_action_outbox::Entity::find()
            .select_only()
            .column(agent_action_outbox::Column::CreatedAt)
            .column(agent_action_outbox::Column::Id)
            .filter(terminal.clone())
            .filter(agent_action_outbox::Column::CreatedAt.lte(cutoff))
            .filter(payload_not_compacted())
            .order_by_desc(agent_action_outbox::Column::CreatedAt)
            .order_by_desc(agent_action_outbox::Column::Id)
            .into_tuple::<(DateTimeWithTimeZone, String)>()
            .one(db)
            .await?
        else {
            return Ok((AgentActionLedgerCompactionSummary::default(), None));
        };
        upper
    };
    let upper_condition = Condition::any()
        .add(agent_action_outbox::Column::CreatedAt.lt(upper_created_at))
        .add(
            Condition::all()
                .add(agent_action_outbox::Column::CreatedAt.eq(upper_created_at))
                .add(agent_action_outbox::Column::Id.lte(upper_id.clone())),
        );
    let candidates = agent_action_outbox::Entity::find()
        .filter(after_condition)
        .filter(upper_condition)
        .filter(terminal)
        .filter(agent_action_outbox::Column::CreatedAt.lte(cutoff))
        .filter(payload_not_compacted())
        .order_by_asc(agent_action_outbox::Column::CreatedAt)
        .order_by_asc(agent_action_outbox::Column::Id)
        .limit(limit)
        .all(db)
        .await
        .context("failed to select terminal agent domain action ledger rows")?;
    let next = if candidates.len() as u64 == limit {
        candidates.last().map(|row| AgentLedgerCursor {
            created_at: row.created_at,
            id: row.id.clone(),
            upper_created_at,
            upper_id: upper_id.clone(),
        })
    } else {
        None
    };
    let mut summary = AgentActionLedgerCompactionSummary {
        candidate_rows: u64::try_from(candidates.len()).unwrap_or(u64::MAX),
        ..Default::default()
    };
    for candidate in candidates {
        let action = agent_action::Entity::find_by_id(candidate.action_id.clone())
            .one(db)
            .await
            .context("failed to load compactable agent domain action")?
            .context("compactable agent domain outbox has no action")?;
        if action.action_kind == "start_agent"
            && candidate.status == "failed"
            && !candidate
                .last_error
                .as_deref()
                .is_some_and(|cause| AGENT_START_TERMINAL_FAILURE_CLASSES.contains(&cause))
        {
            continue; // exhaustion is a reconciliation obligation, not a terminal receipt
        }
        if action.action_kind == "start_agent" && candidate.status == "failed" {
            let payload: serde_json::Value = serde_json::from_str(&candidate.payload_json)?;
            let Some(turn_id) = payload
                .pointer("/dispatch/turn_id")
                .and_then(serde_json::Value::as_str)
            else {
                continue;
            };
            let Some(execution_id) = payload
                .get("spawned_execution_id")
                .and_then(serde_json::Value::as_str)
            else {
                continue;
            };
            let response = load_agent_turn_response(db, turn_id).await?;
            let resource = load_agent_execution_resource_state(db, execution_id).await?;
            if response.is_none_or(|response| response.execution_id != execution_id)
                || !pioneer_crud_conversations::projection_receipts::has_accepted_terminal(
                    db, turn_id,
                )
                .await?
                || resource.is_none_or(|resource| {
                    !matches!(
                        resource.status.as_str(),
                        "completed" | "failed" | "blocked" | "cancelled" | "timed_out"
                    )
                })
            {
                continue;
            }
        }
        let receipt = agent_action_receipt::Entity::find()
            .filter(agent_action_receipt::Column::ActionId.eq(candidate.action_id.clone()))
            .one(db)
            .await
            .context("failed to load compactable agent domain action receipt")?
            .context("compactable agent domain outbox has no receipt")?;
        if action.status != "committed"
            || action
                .committed_at
                .is_none_or(|committed_at| committed_at > cutoff)
            || receipt.committed_at > cutoff
            || receipt.decision != "allowed"
            || receipt.actor_kind != "agent_execution"
            || receipt.actor_id.as_deref() != Some(action.execution_id.as_str())
            || candidate.owner_execution_id != action.execution_id
        {
            bail!("terminal agent domain action ledger tuple is not safe to compact");
        }

        let compacted_outbox = compacted_agent_action_outbox_payload(&candidate, &action)?;
        let mut released = compacted_bytes(candidate.payload_json.as_str(), &compacted_outbox);
        let compacted_action_response = action
            .response_json
            .as_deref()
            .filter(|original| {
                !is_agent_action_compaction_marker(original, AGENT_ACTION_COMPACTION_FORMAT)
            })
            .map(|original| {
                let compacted = agent_action_compaction_marker(
                    AGENT_ACTION_COMPACTION_FORMAT,
                    original,
                    retained_thread_action_result(original),
                );
                released = released.saturating_add(compacted_bytes(original, &compacted));
                (original.to_owned(), compacted)
            });
        let compacted_receipt_response = receipt
            .response_json
            .as_deref()
            .filter(|original| {
                !is_agent_action_compaction_marker(original, AGENT_ACTION_RECEIPT_COMPACTION_FORMAT)
            })
            .map(|original| {
                let compacted = agent_action_compaction_marker(
                    AGENT_ACTION_RECEIPT_COMPACTION_FORMAT,
                    original,
                    retained_thread_action_result(original),
                );
                released = released.saturating_add(compacted_bytes(original, &compacted));
                (original.to_owned(), compacted)
            });

        let transaction = db
            .begin()
            .await
            .context("failed to begin agent domain action ledger compaction")?;
        if let Some((original, compacted)) = compacted_action_response {
            let updated = agent_action::Entity::update_many()
                .col_expr(
                    agent_action::Column::ResponseJson,
                    sea_orm::sea_query::Expr::value(Some(compacted)),
                )
                .filter(agent_action::Column::Id.eq(action.id.clone()))
                .filter(agent_action::Column::Status.eq("committed"))
                .filter(agent_action::Column::ExecutionId.eq(action.execution_id.clone()))
                .filter(agent_action::Column::ResponseJson.eq(Some(original)))
                .exec(&transaction)
                .await
                .context("failed to compact agent domain action response")?;
            if updated.rows_affected != 1 {
                bail!("agent domain action response changed during compaction");
            }
        }
        if let Some((original, compacted)) = compacted_receipt_response {
            let updated = agent_action_receipt::Entity::update_many()
                .col_expr(
                    agent_action_receipt::Column::ResponseJson,
                    sea_orm::sea_query::Expr::value(Some(compacted)),
                )
                .filter(agent_action_receipt::Column::Id.eq(receipt.id.clone()))
                .filter(agent_action_receipt::Column::ActionId.eq(action.id.clone()))
                .filter(agent_action_receipt::Column::Decision.eq("allowed"))
                .filter(agent_action_receipt::Column::ActorKind.eq("agent_execution"))
                .filter(agent_action_receipt::Column::ActorId.eq(Some(action.execution_id.clone())))
                .filter(agent_action_receipt::Column::ResponseJson.eq(Some(original)))
                .exec(&transaction)
                .await
                .context("failed to compact agent domain receipt response")?;
            if updated.rows_affected != 1 {
                bail!("agent domain receipt response changed during compaction");
            }
        }
        let updated = agent_action_outbox::Entity::update_many()
            .col_expr(
                agent_action_outbox::Column::PayloadJson,
                sea_orm::sea_query::Expr::value(compacted_outbox),
            )
            .filter(agent_action_outbox::Column::Id.eq(candidate.id.clone()))
            .filter(agent_action_outbox::Column::PayloadJson.eq(candidate.payload_json.clone()))
            .filter(agent_action_outbox::Column::Status.eq(candidate.status.clone()))
            .filter(agent_action_outbox::Column::Attempts.eq(candidate.attempts))
            .filter(agent_action_outbox::Column::OwnerExecutionId.eq(action.execution_id.clone()))
            .exec(&transaction)
            .await
            .context("failed to compact agent domain action outbox payload")?;
        if updated.rows_affected != 1 {
            bail!("agent domain action outbox changed during compaction");
        }
        transaction
            .commit()
            .await
            .context("failed to commit agent domain action ledger compaction")?;
        summary.compacted_rows = summary.compacted_rows.saturating_add(1);
        summary.payload_bytes_released = summary.payload_bytes_released.saturating_add(released);
    }
    Ok((summary, next))
}

fn compacted_agent_action_outbox_payload(
    outbox: &agent_action_outbox::Model,
    action: &agent_action::Model,
) -> Result<String> {
    let original: serde_json::Value = serde_json::from_str(outbox.payload_json.as_str())
        .context("terminal agent domain action outbox payload is invalid")?;
    if original
        .get("action_id")
        .and_then(serde_json::Value::as_str)
        != Some(action.id.as_str())
        || original
            .get("execution_id")
            .and_then(serde_json::Value::as_str)
            != Some(action.execution_id.as_str())
        || original.get("kind").and_then(serde_json::Value::as_str)
            != Some(action.action_kind.as_str())
    {
        bail!("terminal agent domain action outbox payload differs from its action");
    }
    let mut retained = serde_json::Map::new();
    retained.insert(
        "action_id".to_owned(),
        serde_json::Value::String(action.id.clone()),
    );
    retained.insert(
        "execution_id".to_owned(),
        serde_json::Value::String(action.execution_id.clone()),
    );
    retained.insert(
        "kind".to_owned(),
        serde_json::Value::String(action.action_kind.clone()),
    );
    if let Some(spawned_execution_id) = original.get("spawned_execution_id") {
        retained.insert(
            "spawned_execution_id".to_owned(),
            spawned_execution_id.clone(),
        );
    }
    if original
        .get("execution_mode")
        .and_then(serde_json::Value::as_str)
        == Some("detached_task")
    {
        // Preserve the accepted execution strategy and Task binding. Replaying
        // this action must never reinterpret it as a direct model launch.
        for field in ["execution_mode", "task_id", "thread_id", "turn_id"] {
            retained.insert(
                field.to_owned(),
                original
                    .get(field)
                    .context("detached action binding is missing")?
                    .clone(),
            );
        }
    } else if action.action_kind == "start_agent" {
        // Keep only the immutable binding required for terminal reconciliation;
        // input, launch parameters and the runtime dispatch payload are released.
        let turn_id = original
            .pointer("/dispatch/turn_id")
            .and_then(serde_json::Value::as_str)
            .context("compactable StartAgent has no terminal Turn binding")?;
        let generation = original
            .pointer("/dispatch/execution_generation")
            .and_then(serde_json::Value::as_i64)
            .context("compactable StartAgent has no execution generation")?;
        retained.insert(
            "dispatch".to_owned(),
            serde_json::json!({"turn_id":turn_id, "execution_generation":generation}),
        );
    }
    // Replay must distinguish the original launch choice from its resolved
    // profile even after the dispatch envelope has been released.
    if let Some(fingerprint) = original.get("launch_request_fingerprint") {
        retained.insert("launch_request_fingerprint".to_owned(), fingerprint.clone());
    }
    Ok(agent_action_compaction_marker(
        AGENT_ACTION_OUTBOX_COMPACTION_FORMAT,
        outbox.payload_json.as_str(),
        retained,
    ))
}

fn retained_thread_action_result(original: &str) -> serde_json::Map<String, serde_json::Value> {
    let mut retained = serde_json::Map::new();
    if let Some(result) = serde_json::from_str::<serde_json::Value>(original)
        .ok()
        .and_then(|response| response.get("threadResult").cloned())
    {
        // Small immutable navigation references must outlive bulky response
        // material. Keep the exact issued value, without later enrichment.
        retained.insert("threadResult".to_owned(), result);
    }
    retained
}

fn agent_action_compaction_marker(
    format: &str,
    original: &str,
    mut retained: serde_json::Map<String, serde_json::Value>,
) -> String {
    retained.insert(
        "_pioneer_compacted".to_owned(),
        serde_json::json!({
            "format": format,
            "original_bytes": original.len(),
            "original_sha256": hex::encode(Sha256::digest(original.as_bytes())),
        }),
    );
    serde_json::Value::Object(retained).to_string()
}

fn compacted_bytes(original: &str, compacted: &str) -> u64 {
    u64::try_from(original.len().saturating_sub(compacted.len())).unwrap_or(u64::MAX)
}

fn is_agent_action_compaction_marker(value: &str, expected_format: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(value)
        .ok()
        .and_then(|value| value.get("_pioneer_compacted").cloned())
        .is_some_and(|marker| {
            marker.get("format").and_then(serde_json::Value::as_str) == Some(expected_format)
                && marker
                    .get("original_bytes")
                    .and_then(serde_json::Value::as_u64)
                    .is_some()
                && marker
                    .get("original_sha256")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(is_sha256_hex)
        })
}

pub fn compacted_agent_action_value_matches(
    persisted: Option<&str>,
    expected: Option<&str>,
    format: &str,
) -> bool {
    match (persisted, expected) {
        (None, None) => true,
        (Some(persisted), Some(expected)) if persisted == expected => true,
        (Some(persisted), Some(expected)) => {
            let Ok(value) = serde_json::from_str::<serde_json::Value>(persisted) else {
                return false;
            };
            let Some(marker) = value.get("_pioneer_compacted") else {
                return false;
            };
            let expected_sha256 = hex::encode(Sha256::digest(expected.as_bytes()));
            marker.get("format").and_then(serde_json::Value::as_str) == Some(format)
                && marker
                    .get("original_bytes")
                    .and_then(serde_json::Value::as_u64)
                    == u64::try_from(expected.len()).ok()
                && marker
                    .get("original_sha256")
                    .and_then(serde_json::Value::as_str)
                    == Some(expected_sha256.as_str())
        }
        _ => false,
    }
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests;
