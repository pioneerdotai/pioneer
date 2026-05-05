use anyhow::{Context, Result};
use pioneer_entity::agent_memory_candidate;
use pioneer_protocol::{MemoryCandidateDecision, MemoryCandidateStatus, MemoryScopeKind};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

use crate::convention::{
    DB_ID_LEN, memory_candidate_status_to_db, memory_category_to_db, memory_scope_kind_to_db,
    memory_source_kind_to_db,
};
use crate::memory::{
    AgentMemoryCandidateDecisionRecord, AgentMemoryCandidateListFilter, MemoryScopeResolution,
    MemoryWorkspaceGuard, NewAgentMemoryCandidate, actor_id_to_db, actor_kind_to_db,
    normalized_memory_namespace, normalized_optional_memory_key,
};

pub async fn insert_candidate<C: ConnectionTrait>(
    db: &C,
    candidate: NewAgentMemoryCandidate,
    resolved_scope: MemoryScopeResolution,
    now: DateTimeWithTimeZone,
) -> Result<agent_memory_candidate::Model> {
    let namespace = normalized_memory_namespace(candidate.namespace.as_deref())?;
    let key = normalized_optional_memory_key(candidate.key.clone())?;

    if let Some(dedupe_key) = candidate.dedupe_key.as_deref() {
        if let Some(existing) =
            find_candidate_by_dedupe(db, &resolved_scope, namespace.as_str(), dedupe_key).await?
        {
            return Ok(existing);
        }
    }

    let id = candidate
        .id
        .clone()
        .unwrap_or_else(|| pioneer_protocol::generate_id(DB_ID_LEN));

    agent_memory_candidate::Entity::insert(agent_memory_candidate::ActiveModel {
        id: Set(id.clone()),
        scope_kind: Set(memory_scope_kind_to_db(resolved_scope.scope.kind).to_owned()),
        scope_key: Set(resolved_scope.scope.key),
        scope_key_hash: Set(resolved_scope.scope_key_hash),
        workspace_id: Set(resolved_scope.workspace_id),
        namespace: Set(namespace),
        category: Set(memory_category_to_db(candidate.category).to_owned()),
        key: Set(key),
        candidate_text: Set(candidate.candidate_text),
        confidence: Set(candidate.confidence),
        reason: Set(candidate.reason),
        source_kind: Set(memory_source_kind_to_db(candidate.source_kind).to_owned()),
        source_thread_id: Set(candidate.source_thread_id),
        source_turn_id: Set(candidate.source_turn_id),
        source_item_id: Set(candidate.source_item_id),
        created_by_kind: Set(actor_kind_to_db(&candidate.created_by)),
        created_by_id: Set(actor_id_to_db(&candidate.created_by)),
        status: Set(memory_candidate_status_to_db(MemoryCandidateStatus::Pending).to_owned()),
        dedupe_key: Set(candidate.dedupe_key),
        created_at: Set(now),
        decided_at: Set(None),
        decided_by_kind: Set(None),
        decided_by_id: Set(None),
        decision_reason: Set(None),
        promoted_memory_id: Set(None),
        metadata_json: Set(candidate.metadata_json),
    })
    .exec(db)
    .await
    .with_context(|| format!("failed to insert memory candidate `{id}`"))?;

    agent_memory_candidate::Entity::find_by_id(id)
        .one(db)
        .await
        .context("failed to reload inserted memory candidate")?
        .context("inserted memory candidate missing")
}

pub async fn list_candidates<C: ConnectionTrait>(
    db: &C,
    filter: AgentMemoryCandidateListFilter,
    resolved_scopes: Vec<MemoryScopeResolution>,
) -> Result<Vec<agent_memory_candidate::Model>> {
    let mut query = agent_memory_candidate::Entity::find();

    if !resolved_scopes.is_empty() {
        query = query.filter(scope_condition(&resolved_scopes));
    }
    if let Some(guard) = &filter.workspace_guard {
        query = query.filter(workspace_guard_condition(guard));
    }

    let statuses = if filter.statuses.is_empty() {
        vec![memory_candidate_status_to_db(MemoryCandidateStatus::Pending).to_owned()]
    } else {
        filter
            .statuses
            .iter()
            .map(|status| memory_candidate_status_to_db(*status).to_owned())
            .collect::<Vec<_>>()
    };
    query = query.filter(agent_memory_candidate::Column::Status.is_in(statuses));

    if let Some(limit) = filter.limit {
        query = query.limit(limit);
    }

    query
        .order_by_desc(agent_memory_candidate::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list memory candidates")
}

pub async fn decide_candidate<C: ConnectionTrait>(
    db: &C,
    decision: AgentMemoryCandidateDecisionRecord,
) -> Result<Option<agent_memory_candidate::Model>> {
    let status = match decision.decision {
        MemoryCandidateDecision::Approve => MemoryCandidateStatus::Approved,
        MemoryCandidateDecision::Reject => MemoryCandidateStatus::Rejected,
        MemoryCandidateDecision::Expire => MemoryCandidateStatus::Expired,
    };

    let decided_at = crate::util::unix_to_datetime(decision.decided_at_unix);
    let affected = agent_memory_candidate::Entity::update_many()
        .col_expr(
            agent_memory_candidate::Column::Status,
            Expr::value(memory_candidate_status_to_db(status).to_owned()),
        )
        .col_expr(
            agent_memory_candidate::Column::DecidedAt,
            Expr::value(Some(decided_at)),
        )
        .col_expr(
            agent_memory_candidate::Column::DecidedByKind,
            Expr::value(actor_kind_to_db(&decision.decided_by)),
        )
        .col_expr(
            agent_memory_candidate::Column::DecidedById,
            Expr::value(actor_id_to_db(&decision.decided_by)),
        )
        .col_expr(
            agent_memory_candidate::Column::DecisionReason,
            Expr::value(decision.decision_reason),
        )
        .col_expr(
            agent_memory_candidate::Column::PromotedMemoryId,
            Expr::value(decision.promoted_memory_id),
        )
        .filter(agent_memory_candidate::Column::Id.eq(decision.candidate_id.clone()))
        .filter(
            agent_memory_candidate::Column::Status.eq(memory_candidate_status_to_db(
                MemoryCandidateStatus::Pending,
            )),
        )
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to decide memory candidate `{}`",
                decision.candidate_id
            )
        })?
        .rows_affected;

    if affected == 0 {
        return Ok(None);
    }

    agent_memory_candidate::Entity::find_by_id(decision.candidate_id)
        .one(db)
        .await
        .context("failed to reload decided memory candidate")
}

async fn find_candidate_by_dedupe<C: ConnectionTrait>(
    db: &C,
    resolved_scope: &MemoryScopeResolution,
    namespace: &str,
    dedupe_key: &str,
) -> Result<Option<agent_memory_candidate::Model>> {
    agent_memory_candidate::Entity::find()
        .filter(
            agent_memory_candidate::Column::ScopeKind
                .eq(memory_scope_kind_to_db(resolved_scope.scope.kind)),
        )
        .filter(
            agent_memory_candidate::Column::ScopeKeyHash.eq(resolved_scope.scope_key_hash.clone()),
        )
        .filter(agent_memory_candidate::Column::Namespace.eq(namespace.to_owned()))
        .filter(agent_memory_candidate::Column::DedupeKey.eq(dedupe_key.to_owned()))
        .one(db)
        .await
        .with_context(|| format!("failed to find memory candidate by dedupe key `{dedupe_key}`"))
}

fn scope_condition(scopes: &[MemoryScopeResolution]) -> Condition {
    let mut condition = Condition::any();
    for scope in scopes {
        condition = condition.add(
            Condition::all()
                .add(
                    agent_memory_candidate::Column::ScopeKind
                        .eq(memory_scope_kind_to_db(scope.scope.kind)),
                )
                .add(agent_memory_candidate::Column::ScopeKeyHash.eq(scope.scope_key_hash.clone())),
        );
    }
    condition
}

fn workspace_guard_condition(guard: &MemoryWorkspaceGuard) -> Condition {
    Condition::any()
        .add(agent_memory_candidate::Column::WorkspaceId.eq(guard.workspace_id.clone()))
        .add_option(guard.allow_global_user.then(|| {
            Condition::all()
                .add(agent_memory_candidate::Column::WorkspaceId.is_null())
                .add(
                    agent_memory_candidate::Column::ScopeKind
                        .eq(memory_scope_kind_to_db(MemoryScopeKind::User)),
                )
        }))
        .add_option(guard.allow_global_agent.then(|| {
            Condition::all()
                .add(agent_memory_candidate::Column::WorkspaceId.is_null())
                .add(
                    agent_memory_candidate::Column::ScopeKind
                        .eq(memory_scope_kind_to_db(MemoryScopeKind::Agent)),
                )
        }))
}
