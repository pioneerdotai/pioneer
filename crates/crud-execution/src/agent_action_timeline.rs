//! Binds committed actions to immutable timeline targets and projects authorship.

use anyhow::{Context, Result, bail};
use pioneer_crud_conversations::collaboration::collaboration_from_model;
use pioneer_entity::{
    agent_action, agent_action_receipt, agent_action_timeline_target, agent_execution,
    agent_identity, agent_presentation_snapshot, turn, turn_item,
};
use pioneer_protocol::{PersistedActorRef, SafeRouteProvenance};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, Set,
};
use std::collections::BTreeMap;

use crate::agent_actions::{load_agent_action_receipt, safe_route_provenance_from_receipt};
use crate::agent_identity::{AGENT_PROJECTION_BATCH_LIMIT, agent_presentation_snapshot_from_rows};
use crate::outbox_claim::ACTION_TIMELINE_TARGET_TURN_INPUT;

const ACTION_TIMELINE_TARGET_TURN_ITEM: &str = "turn_item";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentActionTimelineProjection {
    pub author: pioneer_protocol::TurnAuthorSnapshot,
    pub route: Option<SafeRouteProvenance>,
}

fn exact_timeline_target_keys(
    targets: &[(String, Option<String>)],
    item_rows: &[(String, String, String)],
) -> Result<Vec<String>> {
    let mut item_rows_by_target = BTreeMap::new();
    for (row_id, turn_id, item_id) in item_rows {
        let key = (turn_id.clone(), item_id.clone());
        if item_rows_by_target.insert(key, row_id.clone()).is_some() {
            bail!("multiple Turn items share one exact timeline target key");
        }
    }
    Ok(targets
        .iter()
        .filter_map(|(turn_id, item_id)| match item_id {
            Some(item_id) => item_rows_by_target
                .get(&(turn_id.clone(), item_id.clone()))
                .map(|row_id| format!("turn_item:{row_id}")),
            None => Some(format!("turn_input:{turn_id}")),
        })
        .collect())
}

pub async fn action_has_timeline_target<C: ConnectionTrait>(
    db: &C,
    action_id: &str,
) -> Result<bool> {
    Ok(agent_action_timeline_target::Entity::find()
        .filter(agent_action_timeline_target::Column::ActionId.eq(action_id))
        .one(db)
        .await?
        .is_some())
}

pub async fn bind_agent_action_timeline_target<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: Option<&str>,
    action_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<agent_action_timeline_target::Model> {
    let action = agent_action::Entity::find_by_id(action_id.to_owned())
        .one(db)
        .await
        .context("failed to validate timeline target action")?
        .context("agent action timeline target action does not exist")?;
    if action.status != "committed" {
        bail!("agent action timeline target requires a committed action");
    }
    let (target_key, target_kind, turn_item_id) = if let Some(item_id) = item_id {
        if action.action_kind != "deliver_result" {
            bail!("only a delivery action may target a result item");
        }
        let item = turn_item::Entity::find()
            .filter(turn_item::Column::TurnId.eq(turn_id.to_owned()))
            .filter(turn_item::Column::ItemId.eq(item_id.to_owned()))
            .one(db)
            .await
            .context("failed to resolve agent action timeline item target")?
            .context("agent action timeline item target does not exist")?;
        if item.item_type != "agent_message" {
            let receipt = load_agent_action_receipt(db, action_id)
                .await?
                .context("routed delivery diagnostic has no exact action receipt")?;
            if item.item_type != "system_event" || receipt.route_receipt_json.is_none() {
                bail!("agent action timeline item target is not a deliverable result item");
            }
        }
        (
            format!("turn_item:{}", item.id),
            ACTION_TIMELINE_TARGET_TURN_ITEM,
            Some(item.id),
        )
    } else {
        if !matches!(action.action_kind.as_str(), "send_message" | "start_agent") {
            bail!("only a message or Agent start action may target a Turn input");
        }
        let turn = turn::Entity::find_by_id(turn_id.to_owned())
            .one(db)
            .await
            .context("failed to validate agent action timeline Turn target")?
            .context("agent action timeline Turn target does not exist")?;
        let collaboration = collaboration_from_model(&turn)
            .context("agent action timeline Turn target has invalid collaboration facts")?;
        let exact_author = collaboration.author.as_ref().is_some_and(|author| {
            matches!(
                &author.actor,
                PersistedActorRef::AgentExecution(execution_id)
                    if execution_id.as_str() == action.execution_id
            ) && author.agent.is_some()
        });
        if !exact_author {
            bail!("agent action timeline Turn target has no exact immutable action author");
        }
        (
            format!("turn_input:{turn_id}"),
            ACTION_TIMELINE_TARGET_TURN_INPUT,
            None,
        )
    };
    agent_action_timeline_target::Entity::insert(agent_action_timeline_target::ActiveModel {
        target_key: Set(target_key.clone()),
        action_id: Set(action_id.to_owned()),
        turn_id: Set(turn_id.to_owned()),
        turn_item_id: Set(turn_item_id.clone()),
        target_kind: Set(target_kind.to_owned()),
        created_at: Set(now),
    })
    .on_conflict(OnConflict::new().do_nothing().to_owned())
    .exec_without_returning(db)
    .await
    .context("failed to bind agent action to its exact timeline target")?;
    let row = agent_action_timeline_target::Entity::find_by_id(target_key)
        .one(db)
        .await
        .context("failed to reload agent action timeline target")?
        .context("agent action timeline target disappeared after insert")?;
    if row.action_id != action_id
        || row.turn_id != turn_id
        || row.turn_item_id != turn_item_id
        || row.target_kind != target_kind
    {
        bail!("timeline target was reused by a different immutable agent action");
    }
    let action_rows = agent_action_timeline_target::Entity::find()
        .filter(agent_action_timeline_target::Column::ActionId.eq(action_id.to_owned()))
        .limit(2)
        .all(db)
        .await
        .context("failed to validate unique agent action timeline target")?;
    if action_rows.len() != 1 || action_rows[0] != row {
        bail!("agent action was reused by a different immutable timeline target");
    }
    Ok(row)
}

/// Batch-projects exact immutable authorship and non-disclosing route
/// provenance for action-produced Turn inputs and items. Source titles and raw
/// route IDs are deliberately absent; an authorized detail surface may enrich
/// this only after a separate current source-read check.
pub async fn load_agent_action_timeline_projections_for_targets<C: ConnectionTrait>(
    db: &C,
    targets: &[(String, Option<String>)],
) -> Result<BTreeMap<(String, Option<String>), AgentActionTimelineProjection>> {
    if targets.is_empty() {
        return Ok(BTreeMap::new());
    }
    if targets.len() > AGENT_PROJECTION_BATCH_LIMIT {
        bail!("Agent timeline projection batch exceeds its bounded limit");
    }
    let requested_item_targets = targets
        .iter()
        .filter_map(|(turn_id, item_id)| {
            item_id
                .as_ref()
                .map(|item_id| (turn_id.clone(), item_id.clone()))
        })
        .collect::<Vec<_>>();
    let candidate_items = if requested_item_targets.is_empty() {
        Vec::new()
    } else {
        let exact_item_targets = requested_item_targets.iter().fold(
            Condition::any(),
            |condition, (turn_id, item_id)| {
                condition.add(
                    Condition::all()
                        .add(turn_item::Column::TurnId.eq(turn_id.clone()))
                        .add(turn_item::Column::ItemId.eq(item_id.clone())),
                )
            },
        );
        let rows = turn_item::Entity::find()
            .filter(exact_item_targets)
            .limit((AGENT_PROJECTION_BATCH_LIMIT + 1) as u64)
            .all(db)
            .await
            .context("failed to resolve exact timeline target item rows")?;
        if rows.len() > AGENT_PROJECTION_BATCH_LIMIT {
            bail!("Agent timeline item target resolution exceeds its bounded limit");
        }
        rows
    };
    let item_rows = candidate_items
        .iter()
        .map(|item| (item.id.clone(), item.turn_id.clone(), item.item_id.clone()))
        .collect::<Vec<_>>();
    let target_keys = exact_timeline_target_keys(targets, item_rows.as_slice())?;
    if target_keys.is_empty() {
        return Ok(BTreeMap::new());
    }
    let bindings = agent_action_timeline_target::Entity::find()
        .filter(agent_action_timeline_target::Column::TargetKey.is_in(target_keys))
        .all(db)
        .await
        .context("failed to load agent action timeline targets")?;
    if bindings.is_empty() {
        return Ok(BTreeMap::new());
    }
    let action_ids = bindings
        .iter()
        .map(|binding| binding.action_id.clone())
        .collect::<Vec<_>>();
    let actions = agent_action::Entity::find()
        .filter(agent_action::Column::Id.is_in(action_ids.clone()))
        .all(db)
        .await
        .context("failed to load timeline target actions")?;
    let actions_by_id = actions
        .iter()
        .map(|action| (action.id.as_str(), action))
        .collect::<BTreeMap<_, _>>();
    let receipts = agent_action_receipt::Entity::find()
        .filter(agent_action_receipt::Column::ActionId.is_in(action_ids))
        .all(db)
        .await
        .context("failed to load timeline target action receipts")?;
    let receipts_by_action = receipts
        .iter()
        .map(|receipt| (receipt.action_id.as_str(), receipt))
        .collect::<BTreeMap<_, _>>();
    let items_by_id = candidate_items
        .iter()
        .map(|item| (item.id.as_str(), item))
        .collect::<BTreeMap<_, _>>();
    let execution_ids = actions
        .iter()
        .map(|action| action.execution_id.clone())
        .collect::<Vec<_>>();
    let executions = agent_execution::Entity::find()
        .filter(agent_execution::Column::Id.is_in(execution_ids))
        .all(db)
        .await
        .context("failed to load timeline target AgentExecutions")?;
    let executions_by_id = executions
        .iter()
        .map(|execution| (execution.id.as_str(), execution))
        .collect::<BTreeMap<_, _>>();
    let identity_ids = executions
        .iter()
        .map(|execution| execution.agent_identity_id.clone())
        .collect::<Vec<_>>();
    let identities = agent_identity::Entity::find()
        .filter(agent_identity::Column::Id.is_in(identity_ids))
        .all(db)
        .await
        .context("failed to load timeline target AgentIdentities")?;
    let identities_by_id = identities
        .iter()
        .map(|identity| (identity.id.as_str(), identity))
        .collect::<BTreeMap<_, _>>();
    let snapshot_ids = executions
        .iter()
        .filter_map(|execution| execution.presentation_snapshot_id.clone())
        .collect::<Vec<_>>();
    let snapshots = agent_presentation_snapshot::Entity::find()
        .filter(agent_presentation_snapshot::Column::Id.is_in(snapshot_ids))
        .all(db)
        .await
        .context("failed to load timeline target presentation snapshots")?;
    let snapshots_by_id = snapshots
        .iter()
        .map(|snapshot| (snapshot.id.as_str(), snapshot))
        .collect::<BTreeMap<_, _>>();

    let mut result = BTreeMap::new();
    for binding in bindings {
        let action = actions_by_id
            .get(binding.action_id.as_str())
            .context("agent action timeline target has no action")?;
        if action.status != "committed" {
            bail!("agent action timeline target references an uncommitted action");
        }
        let execution = executions_by_id
            .get(action.execution_id.as_str())
            .context("timeline target action has no exact AgentExecution")?;
        let identity = identities_by_id
            .get(execution.agent_identity_id.as_str())
            .context("timeline target AgentExecution has no identity")?;
        let snapshot_id = execution
            .presentation_snapshot_id
            .as_deref()
            .context("timeline target AgentExecution has no presentation snapshot")?;
        let snapshot = snapshots_by_id
            .get(snapshot_id)
            .context("timeline target AgentExecution presentation snapshot is missing")?;
        let author = agent_presentation_snapshot_from_rows(identity, execution, snapshot)?
            .to_turn_author_snapshot();
        let item_id = match (
            binding.target_kind.as_str(),
            binding.turn_item_id.as_deref(),
        ) {
            (ACTION_TIMELINE_TARGET_TURN_INPUT, None) => None,
            (ACTION_TIMELINE_TARGET_TURN_ITEM, Some(row_id)) => {
                let item = items_by_id
                    .get(row_id)
                    .context("agent action timeline target item is missing")?;
                if item.turn_id != binding.turn_id || item.item_type != "agent_message" {
                    bail!("agent action timeline target item has inconsistent immutable facts");
                }
                Some(item.item_id.clone())
            }
            _ => bail!("agent action timeline target has an invalid shape"),
        };
        let receipt = receipts_by_action
            .get(binding.action_id.as_str())
            .context("timeline target action has no exact receipt")?;
        let route = receipt
            .route_receipt_json
            .as_deref()
            .map(|json| safe_route_provenance_from_receipt(json, action.action_kind.as_str()))
            .transpose()?;
        let key = (binding.turn_id, item_id);
        if result
            .insert(key, AgentActionTimelineProjection { author, route })
            .is_some()
        {
            bail!("multiple actions claim one immutable timeline target");
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeline_item_targets_use_database_row_ids_not_protocol_item_ids() {
        let targets = vec![
            ("turn-a".to_owned(), Some("protocol-item".to_owned())),
            ("turn-b".to_owned(), None),
        ];
        let item_rows = vec![(
            "database-row".to_owned(),
            "turn-a".to_owned(),
            "protocol-item".to_owned(),
        )];

        let keys = exact_timeline_target_keys(targets.as_slice(), item_rows.as_slice()).unwrap();

        assert_eq!(
            keys,
            vec![
                "turn_item:database-row".to_owned(),
                "turn_input:turn-b".to_owned(),
            ]
        );
    }
}
