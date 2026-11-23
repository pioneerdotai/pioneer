//! Replay and authorization checks for the atomic agent action ledger.
use crate::agent_action_compaction::{
    AGENT_ACTION_COMPACTION_FORMAT, AGENT_ACTION_OUTBOX_COMPACTION_FORMAT,
    AGENT_ACTION_RECEIPT_COMPACTION_FORMAT, compacted_agent_action_value_matches,
};
use crate::agent_actions::{
    AgentActionInput, insert_agent_action_idempotent, load_agent_action_outbox,
    load_agent_action_receipt,
};
use crate::agent_grant::upgrade_agent_execution_grant_model_to_current;
use crate::canonical_id::canonical_agent_id;
use crate::execution_reads::load_agent_execution_resource_state;
use anyhow::{Context, Result, bail};
use pioneer_crud_access::policy_generation::current_policy_generation_on;
use pioneer_entity::{
    agent_action, agent_action_outbox, agent_action_receipt, agent_delegation_route,
    agent_domain_commit, agent_execution, agent_execution_grant, agent_identity,
    agent_running_permit, task_delivery, task_occurrence_contract, task_run_execution, thread,
    thread_lineage,
};
use pioneer_protocol::AgentRouteDisclosurePolicy;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, QuerySelect, Set,
};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalTaskDeliveryCommitError {
    OccurrenceActorMismatch,
    OccurrenceTerminalStatusMismatch,
}

impl std::fmt::Display for TerminalTaskDeliveryCommitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OccurrenceActorMismatch => {
                formatter.write_str("terminal Task delivery occurrence actor mismatch")
            }
            Self::OccurrenceTerminalStatusMismatch => {
                formatter.write_str("terminal Task delivery occurrence status mismatch")
            }
        }
    }
}

impl std::error::Error for TerminalTaskDeliveryCommitError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCommitInput {
    pub mutation_kind: String,
    pub idempotency_key: String,
    pub request_fingerprint: String,
    pub actor_identity_id: String,
    pub action: AgentActionInput,
    pub receipt_id: String,
    pub outbox_id: String,
    pub action_response_json: Option<String>,
    pub receipt_response_json: Option<String>,
    pub route_receipt_json: Option<String>,
    pub outbox_payload_json: String,
    pub policy_fingerprint: String,
    pub execution_grant_fingerprint: String,
    pub execution_grant_policy_generation: i64,
    pub source_scope_id: String,
    pub destination_scope_id: Option<String>,
    pub subject_role_key: String,
    pub authorized_resource_action: String,
    pub source_policy_generation: i64,
    pub destination_policy_generation: Option<i64>,
    pub route_generation: Option<i64>,
    pub disclosure_class: String,
    /// Exact immutable execution generation bound into the runtime adapter.
    /// A recovered/replaced execution must not commit through a stale adapter.
    pub expected_execution_generation: i64,
    /// Current mutable source revision observed immediately before commit.
    /// This is separate from the execution's pinned authorship revision: a
    /// rename may advance the current source while the execution keeps its
    /// immutable snapshot, but a concurrent disable/retire/settings change
    /// must fence the prepared side effect.
    pub expected_current_identity_source_revision: i64,
    pub expected_current_identity_source_fingerprint: String,
    /// Exact resource-attempt generation that currently owns the held permit.
    /// This fences an old provider/runtime process after replacement even
    /// though the durable AgentExecution identity itself is unchanged.
    pub expected_attempt_generation: i64,
    /// Exact policy generation admitted by the execution-bound adapter. CRUD
    /// compares it with the current generation in the write transaction.
    pub expected_policy_generation: i64,
    /// Marks a cross-capsule write so CRUD revalidates the exact durable route
    /// inside the caller-owned transaction.
    pub requires_cross_capsule_route: bool,
    /// Optional root-scoped resource transition. When present, the resource
    /// state and either its permit or durable queue entry are committed with
    /// the action, receipt and outbox rather than in a second write path.
    pub resource: Option<AgentResourceCommitInput>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentResourceCommitInput {
    pub root_execution_id: String,
    pub execution_id: String,
    pub attempt_generation: i64,
    pub branch_key: String,
    pub fair_order: i64,
    /// Stable server-owned resource-state identifier for this action attempt.
    /// It must be reused on response-loss retries instead of being generated
    /// inside the transaction.
    pub resource_state_id: String,
    pub permit_id: Option<String>,
    pub queue_id: Option<String>,
    pub enqueue_sequence: Option<i64>,
    /// Server-owned execution-local liveness windows. They are applied only
    /// when a permit starts an attempt; queued work remains without a running
    /// deadline until it is actually admitted.
    pub idle_timeout_secs: Option<i64>,
    pub hard_timeout_secs: Option<i64>,
}

fn agent_action_decision_fingerprint(input: &AgentCommitInput) -> String {
    let mut digest = Sha256::new();
    for value in [
        input.action.execution_id.as_str(),
        input.mutation_kind.as_str(),
        input.request_fingerprint.as_str(),
        input.policy_fingerprint.as_str(),
        input.execution_grant_fingerprint.as_str(),
        input.source_scope_id.as_str(),
        input.destination_scope_id.as_deref().unwrap_or(""),
        input.subject_role_key.as_str(),
        input.authorized_resource_action.as_str(),
        input.disclosure_class.as_str(),
    ] {
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    for generation in [
        Some(input.expected_execution_generation),
        Some(input.execution_grant_policy_generation),
        Some(input.source_policy_generation),
        input.destination_policy_generation,
        input.route_generation,
    ] {
        digest.update(generation.unwrap_or_default().to_be_bytes());
        digest.update([generation.is_some() as u8]);
    }
    hex::encode(digest.finalize())
}

/// Read under the same writer transaction as the compound mutation. Exact
/// replay returns before inserting the action receipt or outbox.
pub async fn agent_action_commit_exists(
    db: &DatabaseTransaction,
    input: &AgentCommitInput,
) -> Result<bool> {
    Ok(agent_domain_commit::Entity::find()
        .filter(agent_domain_commit::Column::IdempotencyKey.eq(input.idempotency_key.clone()))
        .filter(agent_domain_commit::Column::ExecutionId.eq(input.action.execution_id.clone()))
        .one(db)
        .await?
        .is_some())
}

/// Commit action, receipt, domain commit and outbox in one caller-owned transaction.
pub async fn commit_agent_action(
    db: &DatabaseTransaction,
    input: &AgentCommitInput,
    commit_resources: impl AsyncFnOnce(
        &DatabaseTransaction,
        &AgentResourceCommitInput,
        DateTimeWithTimeZone,
    ) -> Result<()>,
) -> Result<agent_domain_commit::Model> {
    if input.idempotency_key != input.action.idempotency_key
        || input.request_fingerprint != input.action.request_fingerprint
        || input.mutation_kind != input.action.action_kind
    {
        bail!("domain commit differs from its exact action intent");
    }
    if input.source_scope_id.trim().is_empty()
        || input.subject_role_key.trim().is_empty()
        || input.authorized_resource_action.trim().is_empty()
        || input.policy_fingerprint.len() != 64
        || !input
            .policy_fingerprint
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || input.execution_grant_fingerprint.len() != 64
        || !input
            .execution_grant_fingerprint
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || input.execution_grant_policy_generation < 1
        || input.source_policy_generation < 1
        || input.expected_execution_generation < 1
        || input
            .destination_policy_generation
            .is_some_and(|generation| generation < 1)
        || input
            .route_generation
            .is_some_and(|generation| generation < 1)
        || (!matches!(
            input.disclosure_class.as_str(),
            "source_capsule" | "same_capsule" | "delegated_root"
        ) && !is_routed_disclosure_class(input.disclosure_class.as_str()))
    {
        bail!("agent action receipt facts are invalid");
    }
    // An exact replay of a previously committed domain mutation is returned
    // before current admission policy is evaluated again.
    if let Some(existing) = agent_domain_commit::Entity::find()
        .filter(agent_domain_commit::Column::IdempotencyKey.eq(input.idempotency_key.clone()))
        .filter(agent_domain_commit::Column::ExecutionId.eq(input.action.execution_id.clone()))
        .one(db)
        .await
        .context("failed to load idempotent domain commit")?
    {
        validate_committed_agent_action_replay(db, input, &existing).await?;
        return Ok(existing);
    }
    let current_policy_generation = current_policy_generation_on(db).await?;
    if i64::try_from(current_policy_generation.get())
        .context("current policy generation exceeds persistence bounds")?
        != input.expected_policy_generation
        || input.source_policy_generation != input.expected_policy_generation
    {
        bail!("Agent action policy changed before commit");
    }
    let actor_execution = revalidate_agent_execution_for_commit(db, input).await?;
    revalidate_agent_receipt_authority(db, input, &actor_execution).await?;
    revalidate_agent_route_for_commit(db, input, &actor_execution).await?;
    // The canonical commit boundary derives the actor from the pinned
    // execution.  Actor fields are intentionally absent from
    // `AgentCommitInput`, so no adapter/model input can substitute a
    // principal or System actor.
    let actor_kind = "agent_execution";
    let actor_id = Some(input.action.execution_id.clone());
    let action = insert_agent_action_idempotent(db, &input.action).await?;
    if action.request_fingerprint != input.request_fingerprint {
        bail!("action and domain commit fingerprints do not match");
    }
    if action.status == "committed"
        && !compacted_agent_action_value_matches(
            action.response_json.as_deref(),
            input.action_response_json.as_deref(),
            AGENT_ACTION_COMPACTION_FORMAT,
        )
    {
        bail!("committed agent action was replayed with a different response");
    }
    if !matches!(action.status.as_str(), "prepared" | "committed") {
        bail!("agent action is not in a committable state");
    }
    let now = input.action.now;
    let decision_fingerprint = agent_action_decision_fingerprint(input);
    agent_action::Entity::update_many()
        .col_expr(
            agent_action::Column::Status,
            sea_orm::sea_query::Expr::value("committed"),
        )
        .col_expr(
            agent_action::Column::CommittedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .col_expr(
            agent_action::Column::ResponseJson,
            sea_orm::sea_query::Expr::value(input.action_response_json.clone()),
        )
        .filter(agent_action::Column::Id.eq(action.id.clone()))
        .exec(db)
        .await
        .context("failed to commit agent action")?;
    if let Some(resource) = input.resource.as_ref() {
        commit_resources(db, resource, now).await?;
    }
    agent_action_receipt::Entity::insert(agent_action_receipt::ActiveModel {
        id: Set(input.receipt_id.clone()),
        action_id: Set(action.id.clone()),
        actor_kind: Set(actor_kind.to_owned()),
        actor_id: Set(actor_id),
        decision: Set("allowed".to_owned()),
        policy_fingerprint: Set(input.policy_fingerprint.clone()),
        execution_grant_fingerprint: Set(Some(input.execution_grant_fingerprint.clone())),
        execution_grant_policy_generation: Set(Some(input.execution_grant_policy_generation)),
        source_scope_id: Set(Some(input.source_scope_id.clone())),
        destination_scope_id: Set(input.destination_scope_id.clone()),
        action_kind: Set(Some(input.mutation_kind.clone())),
        authorized_resource_action: Set(Some(input.authorized_resource_action.clone())),
        subject_role_key: Set(Some(input.subject_role_key.clone())),
        execution_generation: Set(Some(input.expected_execution_generation)),
        source_policy_generation: Set(Some(input.source_policy_generation)),
        destination_policy_generation: Set(input.destination_policy_generation),
        route_generation: Set(input.route_generation),
        disclosure_class: Set(Some(input.disclosure_class.clone())),
        decision_fingerprint: Set(Some(decision_fingerprint.clone())),
        committed_at: Set(now),
        response_json: Set(input.receipt_response_json.clone()),
        route_receipt_json: Set(input.route_receipt_json.clone()),
    })
    .on_conflict(
        OnConflict::column(agent_action_receipt::Column::ActionId)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist action receipt")?;
    let receipt = load_agent_action_receipt(db, action.id.as_str())
        .await?
        .context("action receipt disappeared after idempotent insert")?;
    if receipt.id != input.receipt_id
        || receipt.actor_kind != actor_kind
        || receipt.actor_id.as_deref() != Some(input.action.execution_id.as_str())
        || receipt.decision != "allowed"
        || receipt.policy_fingerprint != input.policy_fingerprint
        || receipt.execution_grant_fingerprint.as_deref()
            != Some(input.execution_grant_fingerprint.as_str())
        || receipt.execution_grant_policy_generation
            != Some(input.execution_grant_policy_generation)
        || receipt.source_scope_id.as_deref() != Some(input.source_scope_id.as_str())
        || receipt.destination_scope_id != input.destination_scope_id
        || receipt.action_kind.as_deref() != Some(input.mutation_kind.as_str())
        || receipt.authorized_resource_action.as_deref()
            != Some(input.authorized_resource_action.as_str())
        || receipt.subject_role_key.as_deref() != Some(input.subject_role_key.as_str())
        || receipt.execution_generation != Some(input.expected_execution_generation)
        || receipt.source_policy_generation != Some(input.source_policy_generation)
        || receipt.destination_policy_generation != input.destination_policy_generation
        || receipt.route_generation != input.route_generation
        || receipt.disclosure_class.as_deref() != Some(input.disclosure_class.as_str())
        || receipt.decision_fingerprint.as_deref() != Some(decision_fingerprint.as_str())
        || !compacted_agent_action_value_matches(
            receipt.response_json.as_deref(),
            input.receipt_response_json.as_deref(),
            AGENT_ACTION_RECEIPT_COMPACTION_FORMAT,
        )
        || receipt.route_receipt_json != input.route_receipt_json
    {
        bail!("action receipt was reused with different immutable commit facts");
    }
    agent_action_outbox::Entity::insert(agent_action_outbox::ActiveModel {
        id: Set(input.outbox_id.clone()),
        action_id: Set(action.id.clone()),
        owner_execution_id: Set(input.action.execution_id.clone()),
        payload_json: Set(input.outbox_payload_json.clone()),
        status: Set("pending".to_owned()),
        attempts: Set(0),
        next_attempt_at: Set(None),
        delivered_at: Set(None),
        last_error: Set(None),
        created_at: Set(now),
    })
    .on_conflict(
        OnConflict::column(agent_action_outbox::Column::ActionId)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist action outbox")?;
    let outbox = load_agent_action_outbox(db, action.id.as_str())
        .await?
        .context("action outbox disappeared after idempotent insert")?;
    if outbox.id != input.outbox_id
        || outbox.owner_execution_id != input.action.execution_id
        || !compacted_agent_action_value_matches(
            Some(outbox.payload_json.as_str()),
            Some(input.outbox_payload_json.as_str()),
            AGENT_ACTION_OUTBOX_COMPACTION_FORMAT,
        )
    {
        bail!("action outbox was reused with different immutable commit facts");
    }
    let model = agent_domain_commit::ActiveModel {
        id: Set(canonical_agent_id(
            'D',
            &format!(
                "domain-commit\0{}\0{}",
                input.action.execution_id, input.idempotency_key
            ),
        )),
        mutation_kind: Set(input.mutation_kind.clone()),
        idempotency_key: Set(input.idempotency_key.clone()),
        execution_id: Set(input.action.execution_id.clone()),
        request_fingerprint: Set(input.request_fingerprint.clone()),
        actor_identity_id: Set(input.actor_identity_id.clone()),
        receipt_id: Set(input.receipt_id.clone()),
        outbox_id: Set(input.outbox_id.clone()),
        status: Set("committed".to_owned()),
        committed_at: Set(now),
    };
    agent_domain_commit::Entity::insert(model)
        .on_conflict(
            OnConflict::columns([
                agent_domain_commit::Column::ExecutionId,
                agent_domain_commit::Column::IdempotencyKey,
            ])
            .do_nothing()
            .to_owned(),
        )
        .exec_without_returning(db)
        .await
        .context("failed to persist domain commit")?;
    let commit = agent_domain_commit::Entity::find()
        .filter(agent_domain_commit::Column::ExecutionId.eq(input.action.execution_id.clone()))
        .filter(agent_domain_commit::Column::IdempotencyKey.eq(input.idempotency_key.clone()))
        .one(db)
        .await
        .context("failed to reload idempotent domain commit")?
        .context("domain commit disappeared after idempotent insert")?;
    validate_committed_agent_action_replay(db, input, &commit).await?;
    Ok(commit)
}

async fn validate_committed_agent_action_replay<C: ConnectionTrait>(
    db: &C,
    input: &AgentCommitInput,
    commit: &agent_domain_commit::Model,
) -> Result<()> {
    if commit.mutation_kind != input.mutation_kind
        || commit.request_fingerprint != input.request_fingerprint
        || commit.actor_identity_id != input.actor_identity_id
        || commit.receipt_id != input.receipt_id
        || commit.outbox_id != input.outbox_id
        || commit.status != "committed"
    {
        bail!("domain idempotency key was reused with different immutable commit facts");
    }
    let action = agent_action::Entity::find()
        .filter(agent_action::Column::ExecutionId.eq(input.action.execution_id.clone()))
        .filter(agent_action::Column::IdempotencyKey.eq(input.action.idempotency_key.clone()))
        .one(db)
        .await
        .context("failed to reload replayed agent action")?
        .context("domain commit has no exact agent action")?;
    if action.id != input.action.id
        || action.action_kind != input.action.action_kind
        || action.request_fingerprint != input.action.request_fingerprint
        || action.status != "committed"
        || !compacted_agent_action_value_matches(
            action.response_json.as_deref(),
            input.action_response_json.as_deref(),
            AGENT_ACTION_COMPACTION_FORMAT,
        )
    {
        bail!("domain commit replay differs from its exact agent action");
    }
    let receipt = load_agent_action_receipt(db, action.id.as_str())
        .await?
        .context("domain commit has no exact action receipt")?;
    let decision_fingerprint = agent_action_decision_fingerprint(input);
    if receipt.id != input.receipt_id
        || receipt.actor_kind != "agent_execution"
        || receipt.actor_id.as_deref() != Some(input.action.execution_id.as_str())
        || receipt.decision != "allowed"
        || receipt.policy_fingerprint != input.policy_fingerprint
        || receipt.execution_grant_fingerprint.as_deref()
            != Some(input.execution_grant_fingerprint.as_str())
        || receipt.execution_grant_policy_generation
            != Some(input.execution_grant_policy_generation)
        || receipt.source_scope_id.as_deref() != Some(input.source_scope_id.as_str())
        || receipt.destination_scope_id != input.destination_scope_id
        || receipt.action_kind.as_deref() != Some(input.mutation_kind.as_str())
        || receipt.authorized_resource_action.as_deref()
            != Some(input.authorized_resource_action.as_str())
        || receipt.subject_role_key.as_deref() != Some(input.subject_role_key.as_str())
        || receipt.execution_generation != Some(input.expected_execution_generation)
        || receipt.source_policy_generation != Some(input.source_policy_generation)
        || receipt.destination_policy_generation != input.destination_policy_generation
        || receipt.route_generation != input.route_generation
        || receipt.disclosure_class.as_deref() != Some(input.disclosure_class.as_str())
        || receipt.decision_fingerprint.as_deref() != Some(decision_fingerprint.as_str())
        || !compacted_agent_action_value_matches(
            receipt.response_json.as_deref(),
            input.receipt_response_json.as_deref(),
            AGENT_ACTION_RECEIPT_COMPACTION_FORMAT,
        )
        || receipt.route_receipt_json != input.route_receipt_json
    {
        bail!("domain commit replay differs from its exact action receipt");
    }
    let outbox = load_agent_action_outbox(db, action.id.as_str())
        .await?
        .context("domain commit has no exact action outbox")?;
    if outbox.id != input.outbox_id
        || outbox.owner_execution_id != input.action.execution_id
        || !compacted_agent_action_value_matches(
            Some(outbox.payload_json.as_str()),
            Some(input.outbox_payload_json.as_str()),
            AGENT_ACTION_OUTBOX_COMPACTION_FORMAT,
        )
    {
        bail!("domain commit replay differs from its exact action outbox");
    }
    Ok(())
}

async fn revalidate_agent_execution_for_commit(
    db: &DatabaseTransaction,
    input: &AgentCommitInput,
) -> Result<agent_execution::Model> {
    let execution = agent_execution::Entity::find_by_id(input.action.execution_id.clone())
        .one(db)
        .await
        .context("failed to revalidate committing AgentExecution")?
        .context("committing AgentExecution disappeared")?;
    let terminal_status = matches!(
        execution.status.as_str(),
        "completed" | "succeeded" | "failed" | "blocked" | "cancelled" | "timed_out"
    );
    if terminal_status != execution.finished_at.is_some() {
        bail!("committing AgentExecution has an inconsistent terminal fence");
    }
    if input.actor_identity_id != execution.agent_identity_id
        || input.expected_execution_generation != execution.execution_generation
        || input.source_scope_id != execution.home_root_thread_id
    {
        bail!("committing AgentExecution is stale or has a different identity");
    }
    if terminal_status {
        revalidate_terminal_task_delivery_action(db, input, &execution).await?;
    }
    let identity = agent_identity::Entity::find_by_id(execution.agent_identity_id.clone())
        .one(db)
        .await
        .context("failed to revalidate committing Agent identity")?
        .context("committing Agent identity disappeared")?;
    if identity.workspace_id != execution.workspace_id
        || identity.status != "active"
        || identity.retired_at.is_some()
    {
        bail!("committing Agent identity is unavailable");
    }
    if identity.source_revision != input.expected_current_identity_source_revision
        || identity.source_fingerprint != input.expected_current_identity_source_fingerprint
    {
        bail!("committing Agent identity source changed before commit");
    }
    let resource_state = load_agent_execution_resource_state(db, execution.id.as_str())
        .await?
        .context("committing AgentExecution has no current resource attempt")?;
    if input.expected_attempt_generation < 1
        || resource_state.attempt_generation != input.expected_attempt_generation
    {
        bail!("committing AgentExecution resource attempt is stale");
    }
    if terminal_status {
        if resource_state.status != "completed" {
            bail!("terminal Task delivery has no exact terminal resource attempt");
        }
        let permit_id = resource_state
            .permit_id
            .as_deref()
            .context("terminal Task delivery resource attempt has no original permit")?;
        let permit = agent_running_permit::Entity::find_by_id(permit_id.to_owned())
            .one(db)
            .await
            .context("failed to revalidate terminal Task delivery permit")?
            .context("terminal Task delivery permit disappeared")?;
        if permit.status != "released"
            || permit.execution_id != execution.id
            || permit.root_execution_id != execution.work_graph_root_execution_id
            || permit.attempt_generation != resource_state.attempt_generation
            || permit.released_at.is_none()
        {
            bail!("terminal Task delivery has no exact released execution permit");
        }
        return Ok(execution);
    }
    if resource_state.status != "running" {
        bail!("committing AgentExecution resource attempt is not running");
    }
    let permit_id = resource_state
        .permit_id
        .as_deref()
        .context("committing AgentExecution resource attempt has no permit")?;
    let permit = agent_running_permit::Entity::find_by_id(permit_id.to_owned())
        .one(db)
        .await
        .context("failed to revalidate committing AgentExecution permit")?
        .context("committing AgentExecution permit disappeared")?;
    if permit.status != "held"
        || permit.execution_id != execution.id
        || permit.root_execution_id != execution.work_graph_root_execution_id
        || permit.attempt_generation != resource_state.attempt_generation
    {
        bail!("committing AgentExecution no longer owns its exact permit");
    }
    Ok(execution)
}

/// Result delivery is the only post-terminal Agent action. The Task runtime
/// releases its attempt permit as part of the terminal run transaction, while
/// the independently retried delivery must still retain the exact occurrence
/// actor. Treating every terminal execution as live would be an authority
/// bypass, so bind this exception to the deterministic TaskDelivery/action
/// tuple and its already-terminal resource attempt.
async fn revalidate_terminal_task_delivery_action(
    db: &DatabaseTransaction,
    input: &AgentCommitInput,
    execution: &agent_execution::Model,
) -> Result<()> {
    if input.mutation_kind != "deliver_result"
        || input.action.action_kind != "deliver_result"
        || input.resource.is_some()
        || !matches!(execution.status.as_str(), "completed" | "succeeded")
    {
        bail!("terminal AgentExecution cannot commit this delivery action");
    }
    let parent_task_id = execution
        .parent_task_id
        .as_deref()
        .context("terminal delivery execution has no parent Task")?;
    let result_reference = agent_delivery_result_reference(input)?;
    let delivery_id = pioneer_protocol::task_delivery_id_from_result_item_id(&result_reference)
        .context("terminal Task delivery action has an invalid result reference")?;
    let delivery = task_delivery::Entity::find_by_id(delivery_id.to_owned())
        .one(db)
        .await
        .context("failed to revalidate terminal Task delivery")?
        .context("terminal Task delivery disappeared before action commit")?;
    let expected_action_id =
        canonical_agent_id('A', &format!("task-delivery-action\0{}", delivery.id));
    if delivery.task_id != parent_task_id
        || delivery.status != "delivering"
        || delivery.error_snapshot_json.is_some()
        || delivery.delivery_key != input.idempotency_key
        || input.action.id != expected_action_id
    {
        bail!("terminal Task delivery action differs from its durable delivery attempt");
    }
    let occurrence = task_occurrence_contract::Entity::find()
        .filter(task_occurrence_contract::Column::RunId.eq(delivery.run_id.clone()))
        .one(db)
        .await
        .context("failed to revalidate terminal Task occurrence")?
        .context("terminal Task delivery has no occurrence contract")?;
    if occurrence.task_id != parent_task_id
        || occurrence.agent_execution_id.as_deref() != Some(execution.id.as_str())
    {
        return Err(TerminalTaskDeliveryCommitError::OccurrenceActorMismatch.into());
    }
    if occurrence.status != "delivered" {
        return Err(TerminalTaskDeliveryCommitError::OccurrenceTerminalStatusMismatch.into());
    }
    let task_execution = task_run_execution::Entity::find_by_id(execution.id.clone())
        .one(db)
        .await
        .context("failed to revalidate terminal Task run execution")?
        .context("terminal Task delivery has no TaskRunExecution")?;
    if task_execution.task_id != parent_task_id
        || task_execution.task_run_id != delivery.run_id
        || task_execution.status != "succeeded"
    {
        bail!("terminal Task delivery differs from its exact terminal TaskRunExecution");
    }
    Ok(())
}

pub fn agent_delivery_result_reference(input: &AgentCommitInput) -> Result<String> {
    if input.mutation_kind != "deliver_result" || input.action.action_kind != "deliver_result" {
        bail!("Agent action is not a result delivery");
    }
    let payload: serde_json::Value = serde_json::from_str(input.outbox_payload_json.as_str())
        .context("Agent result delivery outbox is invalid")?;
    let normalized: pioneer_protocol::NormalizedAgentAction = serde_json::from_value(
        payload
            .get("normalized")
            .cloned()
            .context("Agent result delivery outbox has no normalized action")?,
    )
    .context("Agent result delivery normalized action is invalid")?;
    if normalized.kind != pioneer_protocol::AgentActionKind::DeliverResult
        || normalized.action_id.as_str() != input.action.id
        || normalized.execution_id.as_str() != input.action.execution_id
        || normalized.idempotency_key != input.idempotency_key
    {
        bail!("Agent result delivery outbox differs from its exact action");
    }
    normalized
        .opaque_resource_id
        .context("Agent result delivery action has no result reference")
}

async fn revalidate_agent_receipt_authority(
    db: &DatabaseTransaction,
    input: &AgentCommitInput,
    execution: &agent_execution::Model,
) -> Result<()> {
    let grants = agent_execution_grant::Entity::find()
        .filter(agent_execution_grant::Column::ExecutionId.eq(execution.id.clone()))
        .limit(2)
        .all(db)
        .await
        .context("failed to revalidate Agent action execution grant")?;
    let grants = grants
        .into_iter()
        .map(upgrade_agent_execution_grant_model_to_current)
        .collect::<Result<Vec<_>>>()
        .context("failed to upcast Agent action execution grant")?;
    if grants.len() != 1 {
        bail!("Agent action execution must own one exact immutable grant");
    }
    let grant: serde_json::Value = serde_json::from_str(grants[0].grant_json.as_str())
        .context("Agent action execution grant is invalid")?;
    let expected_policy_fingerprint = grant
        .get("agent_authorization_fingerprint")
        .and_then(serde_json::Value::as_str)
        .context("Agent action execution grant has no exact authorization fingerprint")?;
    if input.execution_grant_fingerprint != expected_policy_fingerprint {
        bail!("Agent action execution grant fingerprint differs from its immutable grant");
    }
    let expected_role = grant
        .get("role_key")
        .and_then(serde_json::Value::as_str)
        .context("Agent action execution grant has no exact subject role")?;
    if input.subject_role_key != expected_role {
        bail!("Agent action subject role differs from its immutable execution grant");
    }
    let grant_policy_generation = grant
        .get("agent_policy_generation")
        .and_then(serde_json::Value::as_u64)
        .and_then(|generation| i64::try_from(generation).ok())
        .context("Agent action execution grant has no exact policy generation")?;
    if input.execution_grant_policy_generation != grant_policy_generation {
        bail!("Agent action execution grant generation differs from its immutable grant");
    }
    let allowed_actions = grant
        .get("allowed_actions")
        .and_then(serde_json::Value::as_array)
        .context("Agent action execution grant has no exact action ceiling")?;
    if !allowed_actions
        .iter()
        .any(|action| action.as_str() == Some(input.authorized_resource_action.as_str()))
    {
        bail!("Agent action resource action exceeds its immutable execution grant");
    }
    match input.disclosure_class.as_str() {
        "source_capsule"
            if input.destination_scope_id.is_none()
                && input.route_receipt_json.is_none()
                && input.destination_policy_generation.is_none()
                && input.route_generation.is_none() => {}
        "same_capsule"
            if input.destination_scope_id.as_deref() == Some(input.source_scope_id.as_str())
                && input.route_receipt_json.is_none()
                && input.destination_policy_generation.is_none()
                && input.route_generation.is_none() => {}
        "delegated_root"
            if input.mutation_kind == "create_thread"
                && input
                    .destination_scope_id
                    .as_deref()
                    .is_some_and(|destination| destination != input.source_scope_id)
                && input.route_receipt_json.is_none()
                && input.destination_policy_generation.is_none()
                && input.route_generation.is_none() => {}
        disclosure_class
            if is_routed_disclosure_class(disclosure_class)
                && input.route_receipt_json.is_some() => {}
        _ => bail!("Agent action receipt scope and disclosure facts are inconsistent"),
    }
    Ok(())
}

pub async fn revalidate_agent_route_for_commit(
    db: &DatabaseTransaction,
    input: &AgentCommitInput,
    execution: &agent_execution::Model,
) -> Result<()> {
    let Some(receipt_json) = input.route_receipt_json.as_deref() else {
        if input.requires_cross_capsule_route {
            bail!("cross-capsule Agent action has no exact route receipt");
        }
        if input.route_generation.is_some()
            || input.destination_policy_generation.is_some()
            || is_routed_disclosure_class(input.disclosure_class.as_str())
        {
            bail!("same-capsule Agent action has forged routed receipt facts");
        }
        return Ok(());
    };
    let receipt: serde_json::Value =
        serde_json::from_str(receipt_json).context("Agent route receipt is invalid")?;
    let route_id = receipt
        .get("routeId")
        .and_then(serde_json::Value::as_str)
        .context("Agent route receipt has no route id")?;
    let expected_generation = receipt
        .get("routeGeneration")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| i64::try_from(value).ok())
        .context("Agent route receipt has an invalid generation")?;
    let expected_source_generation = receipt
        .get("sourcePolicyGeneration")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| i64::try_from(value).ok())
        .context("Agent route receipt has an invalid source policy generation")?;
    let expected_destination_generation = receipt
        .get("destinationPolicyGeneration")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| i64::try_from(value).ok())
        .context("Agent route receipt has an invalid destination policy generation")?;
    let current_policy_generation = current_policy_generation_on(db).await?;
    let current_policy_generation = i64::try_from(current_policy_generation.get())
        .context("current policy generation exceeds persistence bounds")?;
    if expected_source_generation != current_policy_generation
        || expected_destination_generation != current_policy_generation
    {
        bail!("Agent route policy changed before commit");
    }
    let receipt_action = receipt
        .get("action")
        .and_then(serde_json::Value::as_str)
        .context("Agent route receipt has no action")?;
    if receipt_action != input.mutation_kind {
        bail!("Agent route receipt action differs from the committed mutation");
    }
    let route = agent_delegation_route::Entity::find_by_id(route_id.to_owned())
        .one(db)
        .await
        .context("failed to revalidate Agent route at commit")?
        .context("Agent route disappeared before commit")?;
    let now = input.action.now;
    if route.status != "active"
        || route.route_generation != expected_generation
        || route.source_policy_generation != expected_source_generation
        || route.destination_policy_generation != expected_destination_generation
        || route
            .expires_at
            .as_ref()
            .is_some_and(|expires_at| expires_at <= &now)
    {
        bail!("Agent route changed before commit");
    }
    if input.destination_scope_id.as_deref() != route.destination_capsule_id.as_deref()
        || input.destination_policy_generation != Some(expected_destination_generation)
        || input.route_generation != Some(expected_generation)
        || !is_routed_disclosure_class(input.disclosure_class.as_str())
    {
        bail!("Agent route receipt facts differ from the committed route");
    }
    if route.route_kind != "capsule_bound"
        && route.source_identity_id.as_deref() != Some(execution.agent_identity_id.as_str())
    {
        bail!("Agent route source identity differs from the committing execution");
    }
    match route.route_kind.as_str() {
        "execution_bound" if route.source_execution_id != execution.id => {
            bail!("Agent route is bound to a different source execution")
        }
        "execution_bound" | "identity_bound" | "capsule_bound" => {}
        _ => bail!("Agent route has an invalid binding kind"),
    }
    let allowed_actions: Vec<pioneer_protocol::AgentRouteAction> =
        serde_json::from_str(route.allowed_actions_json.as_str())
            .context("Agent route action subset is invalid")?;
    let required_action = match receipt_action {
        "send_message" => pioneer_protocol::AgentRouteAction::SendMessage,
        "start_agent" => pioneer_protocol::AgentRouteAction::StartAgent,
        "create_task" => pioneer_protocol::AgentRouteAction::CreateTask,
        "schedule_task" => pioneer_protocol::AgentRouteAction::ScheduleTask,
        "review_task_result" => pioneer_protocol::AgentRouteAction::ReviewTaskResult,
        "deliver_result" => pioneer_protocol::AgentRouteAction::DeliverResult,
        _ => bail!("Agent route receipt names a non-routable action"),
    };
    if !allowed_actions.contains(&required_action) {
        bail!("Agent route no longer permits the committed action");
    }
    let disclosure: AgentRouteDisclosurePolicy =
        serde_json::from_str(route.disclosure_json.as_str())
            .context("Agent route disclosure policy is invalid")?;
    if !route_disclosure_allows_commit_class(
        receipt_action,
        input.disclosure_class.as_str(),
        disclosure,
    ) {
        bail!("Agent route no longer permits the committed disclosure class");
    }
    if route.source_workspace_id.as_deref() != Some(execution.workspace_id.as_str())
        || route.source_capsule_id.as_deref() != Some(execution.home_root_thread_id.as_str())
    {
        bail!("Agent route source capsule differs from the committing execution");
    }
    let destination = thread::Entity::find_by_id(route.destination_thread_id.clone())
        .one(db)
        .await
        .context("failed to revalidate Agent route destination")?
        .context("Agent route destination disappeared before commit")?;
    if route.destination_workspace_id.as_deref() != Some(destination.workspace_id.as_str())
        || destination.workspace_id != execution.workspace_id
    {
        bail!("Agent route destination workspace changed before commit");
    }
    if route.source_capsule_id != route.destination_capsule_id {
        let destination_capsule = if destination.access_class == "internal" {
            thread_lineage::Entity::find_by_id(destination.id.clone())
                .one(db)
                .await
                .context("failed to revalidate route destination lineage")?
                .context("routed internal destination lost its lineage")?
                .root_thread_id
        } else {
            destination.id.clone()
        };
        if route.destination_capsule_id.as_deref() != Some(destination_capsule.as_str()) {
            bail!("Agent route destination capsule changed before commit");
        }
    }
    if let Some(destination_identity_id) = route.destination_agent_identity_id.as_deref() {
        let destination_identity = agent_identity::Entity::find_by_id(destination_identity_id)
            .one(db)
            .await
            .context("failed to revalidate route destination identity")?
            .context("route destination identity disappeared before commit")?;
        if destination_identity.workspace_id != execution.workspace_id
            || destination_identity.status != "active"
        {
            bail!("route destination identity is unavailable");
        }
    }
    let outbox: serde_json::Value = serde_json::from_str(input.outbox_payload_json.as_str())
        .context("routed Agent outbox payload is invalid")?;
    if outbox.get("route_id").and_then(serde_json::Value::as_str) != Some(route.id.as_str())
        || outbox
            .get("destination_thread_id")
            .and_then(serde_json::Value::as_str)
            != Some(route.destination_thread_id.as_str())
    {
        bail!("Agent route target differs from the committed outbox");
    }
    Ok(())
}

fn is_routed_disclosure_class(value: &str) -> bool {
    matches!(
        value,
        "routed_text"
            | "routed_artifacts"
            | "routed_text_artifacts"
            | "routed_empty"
            | "routed_task_input"
            | "routed_result_summary"
            | "routed_result_full"
            | "routed_control"
    )
}

fn route_disclosure_allows_commit_class(
    action: &str,
    class: &str,
    disclosure: AgentRouteDisclosurePolicy,
) -> bool {
    match (action, class) {
        ("send_message" | "start_agent", "routed_text") => disclosure.text,
        ("send_message" | "start_agent", "routed_artifacts") => disclosure.artifacts,
        ("send_message" | "start_agent", "routed_text_artifacts") => {
            disclosure.text && disclosure.artifacts
        }
        ("send_message" | "start_agent", "routed_empty") => true,
        ("create_task" | "schedule_task", "routed_task_input") => disclosure.user_input,
        ("deliver_result", "routed_result_summary") => matches!(
            disclosure.result_return,
            pioneer_protocol::AgentResultReturnPolicy::SummaryOnly
                | pioneer_protocol::AgentResultReturnPolicy::FullResult
        ),
        ("deliver_result", "routed_result_full") => matches!(
            disclosure.result_return,
            pioneer_protocol::AgentResultReturnPolicy::FullResult
        ),
        ("review_task_result", "routed_control") => disclosure.allows_anything(),
        _ => false,
    }
}
