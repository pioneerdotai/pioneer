//! Immutable, scoped operation revisions. Financial evidence is retained apart
//! from transcript payloads; all access uses the caller's database class.
use crate::CrudStore;
use anyhow::{Context, Result, ensure};
use pioneer_entity::{
    agent_execution, execution_observation as fact, execution_tariff as tariff, task, thread, turn,
    workspace,
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect, Set, TransactionTrait,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionObservationInput {
    pub workspace_id: String,
    pub operation_id: String,
    pub revision: i64,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub owner_kind: String,
    pub owner_id: String,
    pub parent_operation_id: Option<String>,
    pub kind: String,
    pub purpose: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub status: String,
    pub recorded_at_ms: i64,
    pub facts: Value,
    pub tariff_evidence: Option<Value>,
}

#[derive(Clone, Debug, Default)]
struct TimingInput {
    start: Option<i64>,
    end: Option<i64>,
    start_mono: Option<i64>,
    end_mono: Option<i64>,
    elapsed: Option<i64>,
    process: Option<String>,
}
impl TimingInput {
    fn from_facts(input: &ExecutionObservationInput) -> Self {
        let timing = &input.facts["timing"];
        let wall = |value: &Value| {
            value
                .as_str()
                .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
                .map(|v| v.timestamp_millis())
        };
        let mut result = Self {
            start: timing["prepared_at_unix_ms"]
                .as_i64()
                .or_else(|| wall(&timing["prepared_at"]))
                .or_else(|| {
                    timing["prepared_at_unix_seconds"]
                        .as_i64()
                        .and_then(|n| n.checked_mul(1000))
                }),
            end: wall(&timing["observation_finished_at"]),
            elapsed: timing["elapsed_ns"].as_i64().or_else(|| {
                timing["elapsed_ms"]
                    .as_i64()
                    .and_then(|n| n.checked_mul(1_000_000))
            }),
            start_mono: timing["prepared_mono_ns"].as_i64(),
            end_mono: timing["observation_finished_mono_ns"].as_i64(),
            process: timing["process_run_id"].as_str().map(str::to_owned),
            ..Default::default()
        };
        if matches!(input.kind.as_str(), "provider_phase" | "runtime_phase") {
            result.start = input.facts["ts_unix_ms"].as_i64();
            result.end = result.start;
            result.process = input.facts["process_run_id"].as_str().map(str::to_owned);
        }
        if input.kind == "tool_invocation" {
            let observed = &input.facts["observation"];
            result.process = observed["processRunId"].as_str().map(str::to_owned);
            if input.status == "running" {
                result.start = observed["tsUnixMs"].as_i64();
                result.start_mono = observed["monoNs"].as_i64();
            }
            if input.status == "settled" {
                result.end = observed["tsUnixMs"].as_i64();
                result.end_mono = observed["monoNs"].as_i64();
            }
        }
        result
    }
}

/// Serialization/hash work is deliberately separated from transactional commit.
#[derive(Clone, Debug)]
pub struct PreparedExecutionObservation {
    input: ExecutionObservationInput,
    facts: String,
    tariff: Option<(String, String)>,
    tariff_reference: Option<String>,
    expected_previous_sequence: Option<i64>,
    expected_live_usage: Option<(String, Option<String>)>,
    timing: TimingInput,
}
impl ExecutionObservationInput {
    pub fn prepare(mut self) -> Result<PreparedExecutionObservation> {
        ensure!(self.revision >= 0, "negative observation revision");
        for id in [&self.workspace_id, &self.operation_id, &self.owner_id] {
            ensure!(
                !id.is_empty() && id.len() <= 256,
                "invalid observation identity"
            );
        }
        ensure!(
            self.facts.is_object(),
            "observation facts must be an object"
        );
        let existing_reference = self
            .tariff_evidence
            .as_ref()
            .and_then(|v| v["retained_evidence_hash"].as_str())
            .map(str::to_owned);
        if let Some(hash) = &existing_reference {
            ensure!(
                hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()),
                "invalid tariff reference"
            );
            self.tariff_evidence = None;
        }
        let tariff = self
            .tariff_evidence
            .take()
            .map(|evidence| -> Result<_> {
                let json = serde_json::to_string(&evidence)?;
                ensure!(json.len() <= 1024 * 1024, "tariff evidence exceeds bound");
                Ok((hex::encode(Sha256::digest(json.as_bytes())), json))
            })
            .transpose()?;
        let tariff_reference = tariff
            .as_ref()
            .map(|(hash, _)| hash.clone())
            .or(existing_reference);
        if let Some(hash) = &tariff_reference {
            self.facts["tariff_snapshot_ref"] =
                serde_json::json!({"content_hash":hash,"storage":"execution_tariff"});
        }
        let facts = serde_json::to_string(&self.facts)?;
        ensure!(facts.len() <= 64 * 1024, "observation facts exceed bound");
        let timing = TimingInput::from_facts(&self);
        Ok(PreparedExecutionObservation {
            input: self,
            facts,
            tariff,
            tariff_reference,
            expected_previous_sequence: None,
            expected_live_usage: None,
            timing,
        })
    }
}

pub async fn commit<C: ConnectionTrait>(
    db: &C,
    prepared: &PreparedExecutionObservation,
) -> Result<()> {
    commit_revision(db, prepared, prepared.input.revision).await
}
async fn commit_revision<C: ConnectionTrait>(
    db: &C,
    prepared: &PreparedExecutionObservation,
    revision: i64,
) -> Result<()> {
    let i = &prepared.input;
    validate_prepared_owner(db, prepared).await?;
    let parent_scope = if (i.kind == "provider_attempt"
        && i.operation_id.starts_with("provider:journal:"))
        || matches!(i.kind.as_str(), "tool_invocation" | "provider_phase")
    {
        if let Some(parent) = &i.parent_operation_id {
            if let Some(turn) = parent.strip_prefix("turn:") {
                ensure!(i.turn_id.as_deref() == Some(turn), "causal turn conflict");
                None
            } else {
                let parent = fact::Entity::find()
                    .filter(fact::Column::WorkspaceId.eq(&i.workspace_id))
                    .filter(fact::Column::OperationId.eq(parent))
                    .order_by_desc(fact::Column::Revision)
                    .one(db)
                    .await?
                    .context("causal parent removed")?;
                ensure!(
                    parent.thread_id == i.thread_id && parent.turn_id == i.turn_id,
                    "causal parent scope conflict"
                );
                Some(parent)
            }
        } else {
            None
        }
    } else {
        None
    };

    if let Some((id, expected)) = &prepared.expected_live_usage {
        let current = pioneer_entity::provider_usage_observation::Entity::find_by_id(id)
            .one(db)
            .await?;
        ensure!(
            current.as_ref().map(|r| r.usage_json.as_str()) == expected.as_deref(),
            "live usage changed during reconciliation"
        );
    }
    let previous = fact::Entity::find()
        .filter(fact::Column::WorkspaceId.eq(&i.workspace_id))
        .filter(fact::Column::OperationId.eq(&i.operation_id))
        .order_by_desc(fact::Column::Revision)
        .one(db)
        .await?;
    if let Some(expected) = prepared.expected_previous_sequence {
        ensure!(
            previous.as_ref().is_some_and(|r| r.sequence == expected),
            "observation reconciliation source changed"
        );
    }
    if let Some(previous) = &previous {
        ensure!(
            previous.thread_id == i.thread_id
                && previous.turn_id == i.turn_id
                && previous.owner_kind == i.owner_kind
                && previous.owner_id == i.owner_id
                && previous.kind == i.kind
                && previous.parent_operation_id == i.parent_operation_id,
            "observation immutable ownership conflict"
        );
        if prepared.expected_previous_sequence.is_none() && previous.source_revision >= revision {
            if previous.source_revision == revision {
                // A recalculation is a separate immutable revision. Redelivery
                // compares to the original producer fact, not its derived view.
                let original = fact::Entity::find()
                    .filter(fact::Column::WorkspaceId.eq(&i.workspace_id))
                    .filter(fact::Column::OperationId.eq(&i.operation_id))
                    .filter(fact::Column::SourceRevision.eq(revision))
                    .filter(fact::Column::ResolutionOrigin.eq("producer"))
                    .one(db)
                    .await?
                    .context("original producer revision missing")?;
                ensure!(
                    original.facts_json == prepared.facts && original.status == i.status,
                    "observation source revision conflict"
                );
            }
            return Ok(());
        }
    }
    let source_revision = if prepared.expected_previous_sequence.is_some() {
        previous.as_ref().map_or(revision, |v| v.source_revision)
    } else {
        revision
    };
    let resolution_origin = if prepared.expected_previous_sequence.is_some() {
        "historical_reconciliation"
    } else {
        "producer"
    };
    let revision = previous
        .as_ref()
        .map(|v| {
            v.revision
                .checked_add(1)
                .context("observation revision exhausted")
        })
        .transpose()?
        .unwrap_or(0);

    let hash = retain_prepared_tariff(db, prepared).await?;
    let mut execution_id = previous
        .as_ref()
        .and_then(|p| p.execution_id.clone())
        .or_else(|| parent_scope.as_ref().and_then(|p| p.execution_id.clone()));
    if execution_id.is_none() {
        if let Some(turn) = &i.turn_id {
            if let Some(binding) =
                pioneer_entity::agent_turn_response_execution::Entity::find_by_id(turn)
                    .one(db)
                    .await?
            {
                let owner = agent_execution::Entity::find_by_id(&binding.execution_id)
                    .select_only()
                    .column(agent_execution::Column::WorkspaceId)
                    .into_tuple::<String>()
                    .one(db)
                    .await?;
                if owner.as_deref() == Some(i.workspace_id.as_str()) {
                    execution_id = Some(binding.execution_id);
                }
            }
        } else if i.owner_kind == "execution" {
            execution_id = Some(i.owner_id.clone());
        }
    }
    let mut task_id = previous
        .as_ref()
        .and_then(|p| p.task_id.clone())
        .or_else(|| parent_scope.as_ref().and_then(|p| p.task_id.clone()));
    let mut task_run_id = previous
        .as_ref()
        .and_then(|p| p.task_run_id.clone())
        .or_else(|| parent_scope.as_ref().and_then(|p| p.task_run_id.clone()));
    let mut task_execution_id = previous
        .as_ref()
        .and_then(|p| p.task_execution_id.clone())
        .or_else(|| {
            parent_scope
                .as_ref()
                .and_then(|p| p.task_execution_id.clone())
        });
    if task_id.is_none() {
        if let Some(turn) = &i.turn_id {
            use pioneer_entity::{
                task_run as run, task_run_execution as execution, task_run_turn as binding,
            };
            if let Some(binding) = binding::Entity::find()
                .filter(binding::Column::TurnId.eq(turn))
                .filter(binding::Column::ThreadId.eq(i.thread_id.as_deref().unwrap_or("")))
                .one(db)
                .await?
            {
                let owner = task::Entity::find_by_id(&binding.task_id)
                    .select_only()
                    .column(task::Column::WorkspaceId)
                    .into_tuple::<String>()
                    .one(db)
                    .await?;
                let source_run = run::Entity::find_by_id(&binding.run_id)
                    .select_only()
                    .column(run::Column::TaskId)
                    .into_tuple::<String>()
                    .one(db)
                    .await?;
                if owner.as_deref() == Some(i.workspace_id.as_str())
                    && source_run.as_deref() == Some(binding.task_id.as_str())
                {
                    if let Some(id) = binding.execution_id {
                        let source = execution::Entity::find_by_id(&id).one(db).await?;
                        if source.is_some_and(|e| {
                            e.task_id == binding.task_id && e.task_run_id == binding.run_id
                        }) {
                            task_execution_id = Some(id);
                        }
                    }
                    task_id = Some(binding.task_id);
                    task_run_id = Some(binding.run_id);
                }
            }
        } else if i.owner_kind == "task" {
            task_id = Some(i.owner_id.clone());
        }
    }
    let mut root_thread_id = previous
        .as_ref()
        .and_then(|p| p.root_thread_id.clone())
        .or_else(|| parent_scope.as_ref().and_then(|p| p.root_thread_id.clone()));
    if root_thread_id.is_none() {
        if let Some(id) = &i.thread_id {
            if let Some(lineage) = pioneer_entity::thread_lineage::Entity::find_by_id(id)
                .one(db)
                .await?
            {
                let root = thread::Entity::find_by_id(&lineage.root_thread_id)
                    .select_only()
                    .column(thread::Column::WorkspaceId)
                    .into_tuple::<String>()
                    .one(db)
                    .await?;
                if root.as_deref() == Some(i.workspace_id.as_str()) {
                    root_thread_id = Some(lineage.root_thread_id);
                }
            }
        }
    }
    let mut timing = prepared.timing.clone();
    let start_process =
        if let Some(previous) = previous.as_ref().filter(|p| p.start_wall_ms.is_some()) {
            previous.start_process_run_id.clone()
        } else {
            prepared
                .timing
                .start
                .and_then(|_| prepared.timing.process.clone())
        };
    if let Some(previous) = &previous {
        timing.start = previous.start_wall_ms.or(timing.start);
        if timing.process.is_some() && timing.process == previous.process_run_id {
            timing.start_mono = previous.start_mono_ns.or(timing.start_mono);
            if timing.elapsed.is_none() {
                timing.elapsed = timing
                    .start_mono
                    .zip(timing.end_mono)
                    .and_then(|(a, b)| b.checked_sub(a))
                    .filter(|v| *v >= 0);
            }
        }
    }
    if start_process.is_some() && timing.process.is_some() && start_process != timing.process {
        timing.elapsed = None;
    }
    let retained_window = previous
        .as_ref()
        .and_then(|p| p.window_id.clone())
        .or_else(|| parent_scope.as_ref().and_then(|p| p.window_id.clone()));
    let window_id = if retained_window.is_some() {
        retained_window
    } else if let Some(turn) = &i.turn_id {
        use pioneer_entity::turn_execution_window as window;
        window::Entity::find()
            .filter(window::Column::WorkspaceId.eq(&i.workspace_id))
            .filter(window::Column::ThreadId.eq(i.thread_id.as_deref().unwrap_or("")))
            .filter(window::Column::TurnId.eq(turn))
            .order_by_desc(window::Column::WindowIndex)
            .one(db)
            .await?
            .map(|r| r.id)
    } else {
        None
    };
    fact::ActiveModel {
        sequence: Default::default(),
        workspace_id: Set(i.workspace_id.clone()),
        operation_id: Set(i.operation_id.clone()),
        revision: Set(revision),
        source_revision: Set(source_revision),
        resolution_origin: Set(resolution_origin.into()),
        thread_id: Set(i.thread_id.clone()),
        root_thread_id: Set(root_thread_id),
        turn_id: Set(i.turn_id.clone()),
        task_id: Set(task_id),
        task_run_id: Set(task_run_id),
        task_execution_id: Set(task_execution_id),
        execution_id: Set(execution_id),
        window_id: Set(window_id),
        owner_kind: Set(i.owner_kind.clone()),
        owner_id: Set(i.owner_id.clone()),
        parent_operation_id: Set(i.parent_operation_id.clone()),
        kind: Set(i.kind.clone()),
        purpose: Set(i.purpose.clone()),
        provider: Set(i.provider.clone()),
        model: Set(i.model.clone()),
        status: Set(i.status.clone()),
        recorded_at_ms: Set(i.recorded_at_ms),
        start_wall_ms: Set(timing.start),
        end_wall_ms: Set(timing.end),
        start_mono_ns: Set(timing.start_mono),
        end_mono_ns: Set(timing.end_mono),
        elapsed_ns: Set(timing.elapsed),
        process_run_id: Set(timing.process),
        start_process_run_id: Set(start_process),
        tariff_hash: Set(hash),
        facts_json: Set(prepared.facts.clone()),
    }
    .insert(db)
    .await?;
    Ok(())
}

fn latest_at_frontier(through: i64) -> sea_orm::sea_query::Expr {
    use sea_orm::sea_query::{Alias, Expr, ExprTrait, Query};
    let newer = Alias::new("newer_observation");
    Expr::exists(
        Query::select()
            .expr(Expr::val(1_i64))
            .from_as(fact::Entity, newer.clone())
            .and_where(
                Expr::col((newer.clone(), fact::Column::WorkspaceId))
                    .eq(Expr::col((fact::Entity, fact::Column::WorkspaceId))),
            )
            .and_where(
                Expr::col((newer.clone(), fact::Column::OperationId))
                    .eq(Expr::col((fact::Entity, fact::Column::OperationId))),
            )
            .and_where(
                Expr::col((newer.clone(), fact::Column::Sequence))
                    .gt(Expr::col((fact::Entity, fact::Column::Sequence))),
            )
            .and_where(Expr::col((newer, fact::Column::Sequence)).lte(through))
            .to_owned(),
    )
    .not()
}

impl CrudStore {
    pub async fn record_execution_observation(
        &self,
        prepared: PreparedExecutionObservation,
    ) -> Result<()> {
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            commit(&tx, &prepared).await?;
            tx.commit().await?;
            Ok(())
        })
        .await
    }
    pub async fn execution_observation_frontier(
        &self,
        workspace: &str,
        thread: &str,
    ) -> Result<i64> {
        Ok(fact::Entity::find()
            .filter(fact::Column::WorkspaceId.eq(workspace))
            .filter(fact::Column::ThreadId.eq(thread))
            .order_by_desc(fact::Column::Sequence)
            .one(&self.connection)
            .await?
            .map_or(0, |r| r.sequence))
    }
    pub async fn execution_workspace_frontier(&self, workspace: &str) -> Result<i64> {
        Ok(fact::Entity::find()
            .select_only()
            .column_as(fact::Column::Sequence.max(), "maximum")
            .filter(fact::Column::WorkspaceId.eq(workspace))
            .into_tuple::<Option<i64>>()
            .one(&self.connection)
            .await?
            .flatten()
            .unwrap_or(0))
    }
    /// Immutable revisions at a fixed upper frontier. Latest-only selects one
    /// leaf per operation at that frontier; appends cannot move its page position.
    pub async fn execution_observation_page(
        &self,
        workspace: &str,
        thread: &str,
        after: i64,
        through: i64,
        limit: u64,
        latest_only: bool,
    ) -> Result<Vec<fact::Model>> {
        ensure!(
            (1..=100).contains(&limit) && after >= 0 && through >= after,
            "invalid observation page"
        );
        let mut query = fact::Entity::find()
            .filter(fact::Column::WorkspaceId.eq(workspace))
            .filter(fact::Column::ThreadId.eq(thread))
            .filter(fact::Column::Sequence.gt(after))
            .filter(fact::Column::Sequence.lte(through));
        if latest_only {
            query = query.filter(latest_at_frontier(through));
        }
        Ok(query
            .order_by_asc(fact::Column::Sequence)
            .limit(limit)
            .all(&self.connection)
            .await?)
    }
    pub async fn workspace_owned_execution_observation_page(
        &self,
        workspace: &str,
        after: i64,
        through: i64,
        limit: u64,
    ) -> Result<Vec<fact::Model>> {
        ensure!(
            (1..=100).contains(&limit) && after >= 0 && through >= after,
            "invalid observation page"
        );
        Ok(fact::Entity::find()
            .filter(fact::Column::WorkspaceId.eq(workspace))
            .filter(fact::Column::ThreadId.is_null())
            .filter(fact::Column::Sequence.gt(after))
            .filter(fact::Column::Sequence.lte(through))
            .filter(latest_at_frontier(through))
            .order_by_asc(fact::Column::Sequence)
            .limit(limit)
            .all(&self.connection)
            .await?)
    }
    pub async fn execution_observation_latest(
        &self,
        workspace: &str,
        thread: &str,
        operation: &str,
    ) -> Result<Option<fact::Model>> {
        Ok(fact::Entity::find()
            .filter(fact::Column::WorkspaceId.eq(workspace))
            .filter(fact::Column::ThreadId.eq(thread))
            .filter(fact::Column::OperationId.eq(operation))
            .order_by_desc(fact::Column::Revision)
            .one(&self.connection)
            .await?)
    }
    pub async fn workspace_owned_execution_observation_latest(
        &self,
        workspace: &str,
        operation: &str,
    ) -> Result<Option<fact::Model>> {
        Ok(fact::Entity::find()
            .filter(fact::Column::WorkspaceId.eq(workspace))
            .filter(fact::Column::ThreadId.is_null())
            .filter(fact::Column::OperationId.eq(operation))
            .order_by_desc(fact::Column::Revision)
            .one(&self.connection)
            .await?)
    }
    /// Verified task ancestry only; no title/goal/model metadata participates.
    /// None is incomplete/cyclic legacy ownership, never an invented match.
    pub async fn execution_task_ancestry_matches(
        &self,
        workspace: &str,
        candidate: &str,
        ancestor: &str,
    ) -> Result<Option<bool>> {
        let mut cursor = Some(candidate.to_owned());
        let mut seen = std::collections::HashSet::new();
        for _ in 0..64 {
            let Some(id) = cursor else {
                return Ok(Some(false));
            };
            if !seen.insert(id.clone()) {
                return Ok(None);
            }
            let row = task::Entity::find_by_id(&id)
                .select_only()
                .columns([
                    task::Column::Id,
                    task::Column::WorkspaceId,
                    task::Column::ParentTaskId,
                ])
                .into_tuple::<(String, String, Option<String>)>()
                .one(&self.connection)
                .await?;
            let Some((id, owner, parent)) = row else {
                return Ok(None);
            };
            if owner != workspace {
                return Ok(None);
            }
            if id == ancestor {
                return Ok(Some(true));
            }
            cursor = parent;
        }
        Ok(None)
    }
    pub async fn execution_turn_owned_by(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        execution: &str,
    ) -> Result<bool> {
        let owner = thread::Entity::find_by_id(thread)
            .select_only()
            .column(thread::Column::WorkspaceId)
            .into_tuple::<String>()
            .one(&self.connection)
            .await?;
        if owner.as_deref() != Some(workspace) {
            return Ok(false);
        }
        let row = pioneer_entity::agent_turn_response_execution::Entity::find_by_id(turn)
            .one(&self.connection)
            .await?;
        if !row.is_some_and(|row| row.execution_id == execution) {
            return Ok(false);
        }
        Ok(turn::Entity::find_by_id(turn)
            .select_only()
            .column(turn::Column::ThreadId)
            .into_tuple::<String>()
            .one(&self.connection)
            .await?
            .as_deref()
            == Some(thread))
    }
    pub async fn execution_children_page(
        &self,
        workspace: &str,
        thread: Option<&str>,
        parent: &str,
        after: i64,
        through: i64,
        limit: u64,
    ) -> Result<Vec<fact::Model>> {
        ensure!(
            (1..=32).contains(&limit),
            "operation detail page exceeds bound"
        );
        let mut query = fact::Entity::find()
            .filter(fact::Column::WorkspaceId.eq(workspace))
            .filter(fact::Column::ParentOperationId.eq(parent))
            .filter(fact::Column::Sequence.gt(after))
            .filter(fact::Column::Sequence.lte(through))
            .filter(latest_at_frontier(through));
        query = if let Some(thread) = thread {
            query.filter(fact::Column::ThreadId.eq(thread))
        } else {
            query.filter(fact::Column::ThreadId.is_null())
        };
        Ok(query
            .order_by_asc(fact::Column::Sequence)
            .limit(limit)
            .all(&self.connection)
            .await?)
    }
    pub async fn execution_operation_at_frontier(
        &self,
        workspace: &str,
        thread: Option<&str>,
        operation: &str,
        through: i64,
    ) -> Result<Option<fact::Model>> {
        let mut query = fact::Entity::find()
            .filter(fact::Column::WorkspaceId.eq(workspace))
            .filter(fact::Column::OperationId.eq(operation))
            .filter(fact::Column::Sequence.lte(through));
        query = if let Some(thread) = thread {
            query.filter(fact::Column::ThreadId.eq(thread))
        } else {
            query.filter(fact::Column::ThreadId.is_null())
        };
        Ok(query
            .order_by_desc(fact::Column::Sequence)
            .one(&self.connection)
            .await?)
    }
    pub async fn execution_tariff(
        &self,
        workspace: &str,
        hash: &str,
    ) -> Result<Option<tariff::Model>> {
        Ok(
            tariff::Entity::find_by_id((workspace.to_owned(), hash.to_owned()))
                .one(&self.connection)
                .await?,
        )
    }
}

/// Content-free projection prepared with the canonical event envelope, before
/// writer acquisition. Ownership is revalidated by `commit` in the append tx.
pub fn prepare_event(
    payload: &crate::events::TurnEventPayload,
) -> Result<Option<PreparedExecutionObservation>> {
    use crate::events::TurnEventPayload as E;
    use pioneer_protocol::TurnItem;
    let (item, terminal) = match payload {
        E::ItemStarted(n) => (Some(&n.item), false),
        E::ItemCompleted(n) => (Some(&n.item), true),
        E::ItemUpdated(n) => (Some(&n.item), false),
        _ => (None, false),
    };
    let Some(item) = item else {
        return prepare_lifecycle_event(payload);
    };
    let mut facts = serde_json::json!({"schema_version":1,"event_type":payload.event_type(),
        "timing":{"basis":"legacy_untimed","coverage":"missing_source_measurement"}});
    let mut tariff_evidence = None;
    let mut operation_id = format!("item:{}:{}", payload.turn_id(), item.item_id());
    let mut kind = "item".to_owned();
    let mut purpose = "runtime".to_owned();
    let mut provider = None;
    let mut model = None;
    let mut status = if terminal { "settled" } else { "running" }.to_owned();
    if let Some((observation, tool_name, storage)) = match item {
        TurnItem::CommandExecution {
            observation,
            tool_name,
            storage,
            ..
        }
        | TurnItem::FileChange {
            observation,
            tool_name,
            storage,
            ..
        }
        | TurnItem::WebSearch {
            observation,
            tool_name,
            storage,
            ..
        }
        | TurnItem::WebFetch {
            observation,
            tool_name,
            storage,
            ..
        }
        | TurnItem::Download {
            observation,
            tool_name,
            storage,
            ..
        }
        | TurnItem::DynamicToolCall {
            observation,
            tool_name,
            storage,
            ..
        } => Some((observation, tool_name, storage)),
        _ => None,
    } {
        kind = "tool_invocation".into();
        if let Some(observation) = observation {
            operation_id = format!(
                "tool:{}:{}:{}",
                payload.turn_id(),
                item.item_id(),
                observation.attempt_id
            );
            facts["logical_tool_invocation_id"] =
                serde_json::json!(format!("item:{}:{}", payload.turn_id(), item.item_id()));
        }
        purpose = "tool".into();
        facts["tool_name"] = serde_json::json!(tool_name);
        facts["observation"] = serde_json::to_value(observation)?;
        facts["timing"] = serde_json::json!({"clock_domain":"tool_event_instant_epoch_v1","basis":"source_tool_lifecycle","coverage":if observation.as_ref().is_some_and(|v|v.process_run_id.is_some()){"measured_source"}else{"legacy_process_unknown"}});

        if let Some(observation) = observation {
            facts["tool_sizes"] = serde_json::json!({"produced_captured_text_bytes":observation.output_sizes.as_ref().map(|v|v.captured_text_bytes),
                "prepared_source_bytes":observation.output_sizes.as_ref().map(|v|v.prepared_source_bytes),
                "retained_source_bytes":null,"retained_source_basis":"separate_source_commit_fact",
                "model_projection_text_bytes":observation.output_sizes.as_ref().map(|v|v.model_projection_text_bytes),
                "request_inclusion_bytes":null,"inclusion_basis":"measured_separately_per_request_manifest_occurrence",
                "source_reconstruction":"subject_to_source_retention_not_archived_here"});
        }

        facts["direct_external_charge"] =
            serde_json::json!({"status":"unobserved","reason":"no_metered_service_evidence"});
        facts["generated_arguments_cost_basis"] = "parent_model_output_subset".into();
        if let pioneer_protocol::ToolStoragePayload::Shell {
            stdout,
            stderr,
            aggregated_output,
            duration_ms,
            truncated,
            ..
        } = storage
        {
            facts["local_duration_ms"] = serde_json::json!(duration_ms);
            if facts.get("tool_sizes").is_none() {
                facts["tool_sizes"] = serde_json::json!({"produced_captured_text_bytes":null,
                    "prepared_source_bytes":null,"model_projection_text_bytes":null,"request_inclusion_bytes":null});
            }
            // This storage payload commits atomically with the terminal item.
            // Count stored fields separately: aggregate can duplicate stdout/stderr.
            facts["tool_sizes"]["retained_item_fields_bytes"] = serde_json::json!(
                stdout.as_ref().map_or(0, String::len)
                    + stderr.as_ref().map_or(0, String::len)
                    + aggregated_output.as_ref().map_or(0, String::len)
            );
            facts["tool_sizes"]["retained_item_basis"] =
                "committed_shell_storage_fields_utf8".into();
            facts["tool_sizes"]["retained_truncated"] = serde_json::json!(truncated);
        }
    }
    if let TurnItem::SystemEvent {
        code: Some(code),
        details: Some(details),
        ..
    } = item
    {
        if code == "runtime_operation" && details["operation_kind"] == "context_preparation" {
            facts = details.clone();
            kind = "context_preparation".into();
            purpose = "preparation".into();
            status = details["status"]
                .as_str()
                .filter(|s| matches!(*s, "started" | "completed" | "failed"))
                .unwrap_or("unconfirmed")
                .into();
        }
        if code == "runtime_phase" {
            facts = details.clone();
            kind = "runtime_phase".into();
            purpose = "wait".into();
        }
        if code == "provider_phase" {
            facts = details.clone();
            kind = "provider_phase".into();
            purpose = facts["purpose"].as_str().unwrap_or("transport").into();
            provider = facts["provider"].as_str().map(str::to_owned);
            model = facts["model"].as_str().map(str::to_owned);
        }
        if code == "cli_native_turn_timing" {
            kind = "external_timing".into();
            facts = details.clone();
            facts["coverage"] = "external_source_reported_no_internal_phases".into();
        }
        if code == "provider_usage" {
            facts = details.clone();
            let usage = &details["usage"];
            operation_id = format!("provider:{}:{}", payload.turn_id(), item.item_id());
            kind = "provider_attempt".into();
            purpose = details["purpose"].as_str().unwrap_or("unknown").into();
            provider = details["provider"].as_str().map(str::to_owned);
            model = details["model"].as_str().map(str::to_owned);
            status = details["status"].as_str().unwrap_or("unconfirmed").into();
            if details["observation_kind"] == "cli_native_statistics" {
                kind = "external_usage_snapshot".into();
                purpose = "external_runtime".into();
                facts["aggregation"] = serde_json::json!({"additive":false,
                    "reason":"external_snapshot_baseline_and_unique_leaf_coverage_unconfirmed",
                    "preserved_scope":usage["scope"],"cost_scope":usage["cost_scope"],"cost_basis":usage["cost_basis"]});
            }
            tariff_evidence = usage["accounting"]["pricing"]
                .as_object()
                .map(|_| usage["accounting"]["pricing"].clone());
            // Deduplicated evidence remains available after transcript cleanup.
            if let Some(accounting) = facts
                .get_mut("usage")
                .and_then(|v| v.get_mut("accounting"))
                .and_then(Value::as_object_mut)
            {
                accounting.remove("pricing");
            }
        }
    }
    Ok(Some(
        ExecutionObservationInput {
            workspace_id: payload.workspace_id().into(),
            operation_id,
            revision: 0,
            thread_id: Some(payload.thread_id().into()),
            turn_id: Some(payload.turn_id().into()),
            owner_kind: "turn".into(),
            owner_id: payload.turn_id().into(),
            parent_operation_id: if matches!(kind.as_str(), "provider_phase" | "runtime_phase") {
                details_parent(&facts)
            } else if kind == "tool_invocation" {
                facts["observation"]["parentOperationId"]
                    .as_str()
                    .map(str::to_owned)
                    .or_else(|| Some(format!("turn:{}", payload.turn_id())))
            } else {
                Some(format!("turn:{}", payload.turn_id()))
            },
            kind,
            purpose,
            provider,
            model,
            status,
            recorded_at_ms: chrono::Utc::now().timestamp_millis(),
            facts,
            tariff_evidence,
        }
        .prepare()?,
    ))
}

// Reuse durable domain events. No raw arguments, permission scopes, retry error
// text or checkpoint payloads enter this compact timing projection.
fn prepare_lifecycle_event(
    payload: &crate::events::TurnEventPayload,
) -> Result<Option<PreparedExecutionObservation>> {
    use crate::events::TurnEventPayload as E;
    let (parent, details, source_time) = match payload {
        E::ItemRetryScheduled(n) => (
            Some(n.item_id.as_str()),
            serde_json::json!({"recovery_job_id":n.recovery_job_id,"attempt_number":n.attempt_number,"due_at_unix_seconds":n.next_run_at_unix,"phase":"backoff_scheduled"}),
            None,
        ),
        E::ItemRetryAttemptStarted(n) => (
            Some(n.item_id.as_str()),
            serde_json::json!({"recovery_job_id":n.recovery_job_id,"attempt_number":n.attempt_number,"phase":"retry_execution_started"}),
            None,
        ),
        E::ItemRecoveryOpened(n) => (
            Some(n.item_id.as_str()),
            serde_json::json!({"recovery_job_id":n.recovery_job_id,"attempt_number":n.attempt_number,"phase":"recovery_opened"}),
            None,
        ),
        E::ItemRecoverySucceeded(n) => (
            Some(n.item_id.as_str()),
            serde_json::json!({"recovery_job_id":n.recovery_job_id,"attempt_number":n.attempt_number,"phase":"recovery_succeeded"}),
            None,
        ),
        E::ItemRecoveryExhausted(n) => (
            Some(n.item_id.as_str()),
            serde_json::json!({"recovery_job_id":n.recovery_job_id,"attempt_number":n.attempt_number,"phase":"recovery_exhausted"}),
            None,
        ),
        E::ItemTimeoutDetected(n) => (
            Some(n.item_id.as_str()),
            serde_json::json!({"attempt_number":n.attempt_number,"phase":"timeout_detected","reason":n.reason}),
            None,
        ),
        E::ItemToolRetryScheduled(n) => (
            Some(n.item_id.as_str()),
            serde_json::json!({"episode_id":n.tool_retry_episode_id,"attempt_number":n.attempt_number,"phase":"tool_retry_scheduled"}),
            None,
        ),
        E::ItemToolRetryResolved(n) => (
            Some(n.item_id.as_str()),
            serde_json::json!({"episode_id":n.tool_retry_episode_id,"attempt_number":n.attempt_number,"phase":"tool_retry_resolved"}),
            None,
        ),
        E::ItemToolRetryExhausted(n) => (
            Some(n.item_id.as_str()),
            serde_json::json!({"episode_id":n.tool_retry_episode_id,"attempt_number":n.attempt_number,"phase":"tool_retry_exhausted"}),
            None,
        ),
        E::TurnPermissionAudit(n) => (
            n.item_id.as_deref(),
            serde_json::json!({"phase":"permission_audit","event_kind":n.event_kind,"tool_call_id":n.tool_call_id,"tool_name":n.tool_name,"decision":n.decision,"cached":n.cached}),
            None,
        ),
        E::TurnExecutionWindowStarted(n) => (
            None,
            serde_json::json!({"window_id":n.window_id,"phase":"execution_window_started","window_index":n.window_index}),
            Some(n.started_at_unix_ms),
        ),
        E::TurnExecutionWindowExhausted(n) => (
            None,
            serde_json::json!({"window_id":n.window_id,"phase":"execution_window_exhausted","window_index":n.window_index,"reason":n.exhaustion_reason}),
            Some(n.exhausted_at_unix_ms),
        ),
        E::TurnExecutionWindowCheckpointed(n) => (
            None,
            serde_json::json!({"window_id":n.window_id,"phase":"execution_window_checkpointed","checkpoint_id":n.checkpoint_id,"payload_bytes":n.payload_bytes}),
            Some(n.created_at_unix_ms),
        ),
        E::TurnExecutionWindowContinued(n) => (
            None,
            serde_json::json!({"window_id":n.window_id,"phase":"execution_window_continued","previous_window_id":n.previous_window_id}),
            Some(n.continued_at_unix_ms),
        ),
        E::TurnExecutionWindowBlocked(n) => (
            None,
            serde_json::json!({"window_id":n.window_id,"phase":"execution_window_blocked"}),
            Some(n.blocked_at_unix_ms),
        ),
        _ => return Ok(None),
    };
    let identity = payload.idempotency_key()?;
    ExecutionObservationInput {
        workspace_id:payload.workspace_id().into(),operation_id:format!("phase:{}:{identity}",payload.turn_id()),revision:0,
        thread_id:Some(payload.thread_id().into()),turn_id:Some(payload.turn_id().into()),owner_kind:"turn".into(),owner_id:payload.turn_id().into(),
        parent_operation_id:Some(parent.map(|id|format!("item:{}:{id}",payload.turn_id())).unwrap_or_else(||format!("turn:{}",payload.turn_id()))),
        kind:"runtime_phase".into(),purpose:"runtime".into(),provider:None,model:None,status:"observed".into(),recorded_at_ms:chrono::Utc::now().timestamp_millis(),
        facts:serde_json::json!({"event_type":payload.event_type(),"details":details,"ts_unix_ms":source_time,
            "timing":{"basis":if source_time.is_some(){"source_wall_point"}else{"recorded_event_order_only"},"coverage":"no_inferred_wait_duration"}}),tariff_evidence:None,
    }.prepare().map(Some)
}

pub async fn commit_event<C: ConnectionTrait>(
    db: &C,
    prepared: &PreparedExecutionObservation,
    sequence: i64,
) -> Result<()> {
    commit_revision(db, prepared, sequence).await
}

fn details_parent(facts: &Value) -> Option<String> {
    facts["parent_operation_id"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| {
            facts["physical_attempt_id"]
                .as_str()
                .map(|id| format!("provider:{id}"))
        })
}
impl CrudStore {
    pub async fn record_provider_phase(
        &self,
        workspace: &str,
        owner: &str,
        mut facts: Value,
    ) -> Result<()> {
        let parent = details_parent(&facts).context("provider phase requires durable parent")?;
        let source = fact::Entity::find()
            .filter(fact::Column::WorkspaceId.eq(workspace))
            .filter(fact::Column::OperationId.eq(&parent))
            .order_by_desc(fact::Column::Revision)
            .one(&self.connection)
            .await?
            .context("provider phase parent unavailable")?;
        // Parent identity is runtime supplied and its owner was durably checked
        // before dispatch. Deletion cannot reattribute late facts to workspace.
        let source_facts: Value = serde_json::from_str(&source.facts_json)?;
        ensure!(
            source.owner_id == owner || source_facts["legacy_owner_id"].as_str() == Some(owner),
            "provider phase owner conflict"
        );
        facts["owner_coverage"] = "inherited_verified_durable_parent".into();
        self.record_execution_observation(
            ExecutionObservationInput {
                workspace_id: workspace.into(),
                operation_id: pioneer_protocol::generate_id(21),
                revision: 0,
                thread_id: source.thread_id,
                turn_id: source.turn_id,
                owner_kind: source.owner_kind,
                owner_id: source.owner_id,
                parent_operation_id: Some(parent),
                kind: "provider_phase".into(),
                purpose: source.purpose,
                provider: source.provider,
                model: source.model,
                status: "observed".into(),
                recorded_at_ms: chrono::Utc::now().timestamp_millis(),
                facts,
                tariff_evidence: None,
            }
            .prepare()?,
        )
        .await
    }
}

impl PreparedExecutionObservation {
    pub fn expecting_live_usage(mut self, id: String, value: Option<String>) -> Self {
        self.expected_live_usage = Some((id, value));
        self
    }

    pub fn expecting_previous_sequence(mut self, sequence: i64) -> Self {
        self.expected_previous_sequence = Some(sequence);
        self
    }

    pub fn tariff_hash(&self) -> Option<&str> {
        self.tariff_reference.as_deref()
    }
}
/// Replace only newly prepared inline tariffs. Already stored legacy public
/// records are untouched; detail queries resolve the retained workspace hash.
pub fn reference_event_tariff(
    payload: &mut crate::events::TurnEventPayload,
    prepared: &PreparedExecutionObservation,
) {
    use crate::events::TurnEventPayload as E;
    use pioneer_protocol::TurnItem;
    let Some(hash) = prepared.tariff_hash() else {
        return;
    };
    let item = match payload {
        E::ItemStarted(n) => &mut n.item,
        E::ItemCompleted(n) => &mut n.item,
        E::ItemUpdated(n) => &mut n.item,
        _ => return,
    };
    if let TurnItem::SystemEvent {
        details: Some(details),
        ..
    } = item
    {
        if let Some(accounting) = details
            .get_mut("usage")
            .and_then(|v| v.get_mut("accounting"))
            .and_then(Value::as_object_mut)
        {
            accounting.insert(
                "pricing".into(),
                serde_json::json!({"retained_evidence_hash":hash,"storage":"execution_tariff"}),
            );
        }
    }
}

const BACKFILL_KEY: &str = "execution_observation_legacy_v1";
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExecutionObservationBackfillProgress {
    pub after: String,
    pub complete: bool,
    pub scanned: u64,
    pub unavailable: u64,
}
impl CrudStore {
    pub async fn execution_observation_backfill_progress(
        &self,
    ) -> Result<ExecutionObservationBackfillProgress> {
        self.execution_backfill_progress_for(BACKFILL_KEY).await
    }
    pub async fn execution_aux_observation_backfill_progress(
        &self,
    ) -> Result<ExecutionObservationBackfillProgress> {
        self.execution_backfill_progress_for(AUX_BACKFILL_KEY).await
    }
    async fn execution_backfill_progress_for(
        &self,
        key: &str,
    ) -> Result<ExecutionObservationBackfillProgress> {
        let row = pioneer_entity::thread_timeline_projection_meta::Entity::find_by_id(key)
            .one(&self.connection)
            .await?;
        row.and_then(|r| r.projection_config_json)
            .map(|json| serde_json::from_str(&json).map_err(Into::into))
            .unwrap_or_else(|| Ok(Default::default()))
    }
    /// Finite, restartable data-migration quantum. Preparation is outside the
    /// writer; progress and all accepted facts commit together. No provider calls.
    pub async fn backfill_execution_observations_quantum(
        &self,
    ) -> Result<ExecutionObservationBackfillProgress> {
        use pioneer_entity::{thread_timeline_projection_meta as meta, turn_event as source};
        use sea_orm::sea_query::{Alias, Expr, Func};
        use sea_orm::{ExprTrait, FromQueryResult};
        #[derive(FromQueryResult)]
        struct Row {
            id: String,
            thread_id: String,
            turn_id: String,
            sequence: i64,
            payload: Option<String>,
            created_at: sea_orm::prelude::DateTimeWithTimeZone,
        }
        let predecessor = meta::Entity::find_by_id(BACKFILL_KEY)
            .one(&self.connection)
            .await?;
        let previous_json = predecessor
            .as_ref()
            .and_then(|row| row.projection_config_json.clone());
        let before: ExecutionObservationBackfillProgress = previous_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?
            .unwrap_or_default();
        if before.complete {
            return Ok(before);
        }
        let length = Func::cust(Alias::new("length"))
            .arg(Expr::col(source::Column::Payload).cast_as(Alias::new("BLOB")));
        let rows = source::Entity::find()
            .select_only()
            .columns([
                source::Column::Id,
                source::Column::ThreadId,
                source::Column::TurnId,
                source::Column::Sequence,
                source::Column::CreatedAt,
            ])
            .column_as(
                Expr::case(
                    Expr::expr(length).lte(2 * 1024 * 1024),
                    Expr::col(source::Column::Payload),
                )
                .finally(Option::<String>::None),
                "payload",
            )
            .filter(source::Column::Id.gt(&before.after))
            .filter(source::Column::EventType.is_in([
                pioneer_protocol::constants::events::ITEM_STARTED,
                pioneer_protocol::constants::events::ITEM_COMPLETED,
                pioneer_protocol::constants::events::ITEM_UPDATED,
                pioneer_protocol::constants::events::ITEM_RETRY_SCHEDULED,
                pioneer_protocol::constants::events::ITEM_RETRY_ATTEMPT_STARTED,
                pioneer_protocol::constants::events::ITEM_RECOVERY_OPENED,
                pioneer_protocol::constants::events::ITEM_RECOVERY_SUCCEEDED,
                pioneer_protocol::constants::events::ITEM_RECOVERY_EXHAUSTED,
                pioneer_protocol::constants::events::ITEM_TIMEOUT_DETECTED,
                pioneer_protocol::constants::events::ITEM_TOOL_RETRY_SCHEDULED,
                pioneer_protocol::constants::events::ITEM_TOOL_RETRY_RESOLVED,
                pioneer_protocol::constants::events::ITEM_TOOL_RETRY_EXHAUSTED,
                pioneer_protocol::constants::events::TURN_PERMISSION_AUDIT,
                pioneer_protocol::constants::events::TURN_EXECUTION_WINDOW_STARTED,
                pioneer_protocol::constants::events::TURN_EXECUTION_WINDOW_EXHAUSTED,
                pioneer_protocol::constants::events::TURN_EXECUTION_WINDOW_CHECKPOINTED,
                pioneer_protocol::constants::events::TURN_EXECUTION_WINDOW_CONTINUED,
                pioneer_protocol::constants::events::TURN_EXECUTION_WINDOW_BLOCKED,
            ]))
            .order_by_asc(source::Column::Id)
            .limit(32)
            .into_model::<Row>()
            .all(&self.connection)
            .await?;
        let count = rows.len();
        let mut progress = before.clone();
        let mut prepared = Vec::new();
        for row in rows {
            progress.after = row.id.clone();
            progress.scanned += 1;
            let Some(body) = row.payload else {
                progress.unavailable += 1;
                continue;
            };
            let parsed = serde_json::from_str::<crate::CanonicalTurnEventPayload>(&body);
            let Ok(payload) = parsed else {
                progress.unavailable += 1;
                continue;
            };
            if payload.thread_id() != row.thread_id || payload.turn_id() != row.turn_id {
                progress.unavailable += 1;
                continue;
            }
            match prepare_event(&payload) {
                Ok(Some(mut fact)) => {
                    // Preserve source-owned observations; projection time is not
                    // a historical start/end and never substitutes updated_at.
                    fact.input.recorded_at_ms = row.created_at.timestamp_millis();
                    // Quarantine unavailable legacy owners/evidence by advancing
                    // durable progress. A malformed old row must not poison EOF.
                    let owner_thread = thread::Entity::find_by_id(&row.thread_id)
                        .one(&self.connection)
                        .await?;
                    let owner_turn = turn::Entity::find_by_id(&row.turn_id)
                        .one(&self.connection)
                        .await?;
                    let valid = owner_thread
                        .as_ref()
                        .is_some_and(|t| t.workspace_id == fact.input.workspace_id)
                        && owner_turn
                            .as_ref()
                            .is_some_and(|t| t.thread_id == row.thread_id)
                        && workspace::Entity::find_by_id(&fact.input.workspace_id)
                            .one(&self.connection)
                            .await?
                            .is_some();
                    let missing_evidence = if fact.tariff.is_none() {
                        if let Some(hash) = &fact.tariff_reference {
                            self.execution_tariff(&fact.input.workspace_id, hash)
                                .await?
                                .is_none()
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if !valid || missing_evidence {
                        progress.unavailable += 1;
                        continue;
                    }
                    if let Some(existing) = self
                        .execution_observation_latest(
                            &fact.input.workspace_id,
                            &row.thread_id,
                            &fact.input.operation_id,
                        )
                        .await?
                    {
                        if existing.owner_id != fact.input.owner_id
                            || existing.owner_kind != fact.input.owner_kind
                            || existing.kind != fact.input.kind
                        {
                            progress.unavailable += 1;
                            continue;
                        }
                    }
                    prepared.push((row.id, row.sequence, fact));
                }
                Ok(None) => {}
                Err(_) => progress.unavailable += 1,
            }
        }
        progress.complete = count < 32;
        let next_json = serde_json::to_string(&progress)?;
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let current = meta::Entity::find_by_id(BACKFILL_KEY).one(&tx).await?;
            ensure!(
                current
                    .as_ref()
                    .and_then(|r| r.projection_config_json.as_deref())
                    == previous_json.as_deref(),
                "observation backfill progress changed"
            );
            for (id, sequence, fact) in &prepared {
                // Source deletion during preparation must not resurrect a
                // removed thread's diagnostic facts. Owners are checked again.
                if source::Entity::find_by_id(id)
                    .select_only()
                    .column(source::Column::Id)
                    .into_tuple::<String>()
                    .one(&tx)
                    .await?
                    .is_some()
                    && thread::Entity::find_by_id(fact.input.thread_id.as_deref().unwrap_or(""))
                        .filter(thread::Column::WorkspaceId.eq(&fact.input.workspace_id))
                        .one(&tx)
                        .await?
                        .is_some()
                    && turn::Entity::find_by_id(fact.input.turn_id.as_deref().unwrap_or(""))
                        .filter(
                            turn::Column::ThreadId.eq(fact
                                .input
                                .thread_id
                                .as_deref()
                                .unwrap_or("")),
                        )
                        .one(&tx)
                        .await?
                        .is_some()
                {
                    commit_revision(&tx, fact, *sequence).await?;
                }
            }
            commit_backfill_checkpoint(&tx, BACKFILL_KEY, current, &next_json, progress.complete)
                .await?;
            tx.commit().await?;
            Ok(())
        })
        .await?;
        Ok(progress)
    }
}

pub(crate) async fn retain_prepared_tariff<C: ConnectionTrait>(
    db: &C,
    prepared: &PreparedExecutionObservation,
) -> Result<Option<String>> {
    let i = &prepared.input;
    let hash = if let Some((hash, evidence)) = &prepared.tariff {
        if let Some(existing) = tariff::Entity::find_by_id((i.workspace_id.clone(), hash.clone()))
            .one(db)
            .await?
        {
            ensure!(
                existing.evidence_json == *evidence,
                "tariff content hash conflict"
            );
        } else {
            tariff::ActiveModel {
                workspace_id: Set(i.workspace_id.clone()),
                content_hash: Set(hash.clone()),
                evidence_json: Set(evidence.clone()),
                captured_at_ms: Set(i.recorded_at_ms),
            }
            .insert(db)
            .await?;
        }
        Some(hash.clone())
    } else if let Some(hash) = &prepared.tariff_reference {
        ensure!(
            tariff::Entity::find_by_id((i.workspace_id.clone(), hash.clone()))
                .one(db)
                .await?
                .is_some(),
            "retained tariff missing"
        );
        Some(hash.clone())
    } else {
        None
    };
    Ok(hash)
}

pub(crate) async fn validate_prepared_owner<C: ConnectionTrait>(
    db: &C,
    prepared: &PreparedExecutionObservation,
) -> Result<()> {
    let i = &prepared.input;
    ensure!(
        workspace::Entity::find_by_id(&i.workspace_id)
            .one(db)
            .await?
            .is_some(),
        "observation workspace missing"
    );
    if let Some(id) = &i.thread_id {
        let row = thread::Entity::find_by_id(id)
            .one(db)
            .await?
            .context("observation thread missing")?;
        ensure!(
            row.workspace_id == i.workspace_id,
            "observation workspace conflict"
        );
    }
    if let Some(id) = &i.turn_id {
        let row = turn::Entity::find_by_id(id)
            .one(db)
            .await?
            .context("observation turn missing")?;
        ensure!(
            Some(row.thread_id) == i.thread_id,
            "observation turn owner conflict"
        );
    }
    match i.owner_kind.as_str() {
        "thread" => ensure!(
            i.thread_id.as_deref() == Some(i.owner_id.as_str()),
            "observation thread owner conflict"
        ),
        "turn" => ensure!(
            i.turn_id.as_deref() == Some(i.owner_id.as_str()),
            "observation turn owner conflict"
        ),
        "task" => {
            let owner = task::Entity::find_by_id(&i.owner_id)
                .one(db)
                .await?
                .context("observation task missing")?;
            ensure!(
                owner.workspace_id == i.workspace_id,
                "observation task owner conflict"
            );
        }
        "execution" => {
            let owner = agent_execution::Entity::find_by_id(&i.owner_id)
                .one(db)
                .await?
                .context("observation execution missing")?;
            ensure!(
                owner.workspace_id == i.workspace_id,
                "observation execution owner conflict"
            );
        }
        "workspace" => ensure!(
            i.owner_id == i.workspace_id,
            "observation workspace owner conflict"
        ),
        "self_improvement_run" => {
            let run = pioneer_entity::self_improvement_run::Entity::find_by_id(&i.owner_id)
                .one(db)
                .await?
                .context("self improvement observation owner missing")?;
            ensure!(
                run.workspace_id == i.workspace_id && i.thread_id.is_none() && i.turn_id.is_none(),
                "self improvement observation owner conflict"
            );
        }
        _ => anyhow::bail!("unsupported observation owner"),
    }
    Ok(())
}

impl CrudStore {
    pub async fn execution_recent_owner_turns(
        &self,
        workspace: &str,
        thread_id: &str,
        task_id: Option<&str>,
        execution_id: Option<&str>,
    ) -> Result<Vec<String>> {
        use sea_orm::QueryTrait;
        let owner = thread::Entity::find_by_id(thread_id)
            .select_only()
            .column(thread::Column::WorkspaceId)
            .into_tuple::<String>()
            .one(&self.connection)
            .await?;
        ensure!(
            owner.as_deref() == Some(workspace),
            "owner timeline workspace conflict"
        );
        let mut query = turn::Entity::find()
            .select_only()
            .column(turn::Column::Id)
            .filter(turn::Column::ThreadId.eq(thread_id));
        if let Some(id) = task_id {
            use pioneer_entity::task_run_turn as mapping;
            query = query.filter(
                turn::Column::Id.in_subquery(
                    mapping::Entity::find()
                        .select_only()
                        .column(mapping::Column::TurnId)
                        .filter(mapping::Column::ThreadId.eq(thread_id))
                        .filter(mapping::Column::TaskId.eq(id))
                        .into_query(),
                ),
            );
        }
        if let Some(id) = execution_id {
            use pioneer_entity::agent_turn_response_execution as mapping;
            query = query.filter(
                turn::Column::Id.in_subquery(
                    mapping::Entity::find()
                        .select_only()
                        .column(mapping::Column::TurnId)
                        .filter(mapping::Column::ExecutionId.eq(id))
                        .into_query(),
                ),
            );
        }
        Ok(query
            .order_by_desc(turn::Column::CreatedAt)
            .order_by_desc(turn::Column::Id)
            .limit(100)
            .into_tuple::<String>()
            .all(&self.connection)
            .await?)
    }
    /// Existing lifecycle owners remain authoritative. No updated_at-to-finish
    /// conversion and no second lifecycle publisher. This is a bounded live
    /// domain projection alongside the immutable diagnostic revision page.
    pub async fn execution_scope_owners(
        &self,
        workspace: &str,
        thread: &str,
        turns: Vec<String>,
    ) -> Result<Vec<Value>> {
        use pioneer_entity::{turn_execution as execution, turn_execution_window as window};
        use sea_orm::FromQueryResult;
        #[derive(FromQueryResult)]
        struct Execution {
            turn_id: String,
            status: String,
            owner_generation: i64,
            started_at: Option<sea_orm::prelude::DateTimeWithTimeZone>,
            completed_at: Option<sea_orm::prelude::DateTimeWithTimeZone>,
        }
        #[derive(FromQueryResult)]
        struct Window {
            id: String,
            turn_id: String,
            status: String,
            window_index: i64,
            started_at: sea_orm::prelude::DateTimeWithTimeZone,
            completed_at: Option<sea_orm::prelude::DateTimeWithTimeZone>,
        }
        #[derive(FromQueryResult)]
        struct AgentOwner {
            id: String,
            status: String,
            parent_execution_id: Option<String>,
            home_root_thread_id: String,
            work_graph_root_execution_id: String,
            created_at: sea_orm::prelude::DateTimeWithTimeZone,
            finished_at: Option<sea_orm::prelude::DateTimeWithTimeZone>,
        }
        ensure!(turns.len() <= 100, "owner projection exceeds bound");
        let scope = thread::Entity::find_by_id(thread)
            .select_only()
            .column(thread::Column::WorkspaceId)
            .into_tuple::<String>()
            .one(&self.connection)
            .await?;
        ensure!(
            scope.as_deref() == Some(workspace),
            "owner projection workspace conflict"
        );
        if turns.is_empty() {
            return Ok(Vec::new());
        }
        // FK=OFF: only turn IDs belonging to the requested thread may grant
        // access to their execution owner metadata.
        let turns = turn::Entity::find()
            .select_only()
            .column(turn::Column::Id)
            .filter(turn::Column::ThreadId.eq(thread))
            .filter(turn::Column::Id.is_in(turns))
            .limit(100)
            .into_tuple::<String>()
            .all(&self.connection)
            .await?;
        let agent_ids = pioneer_entity::agent_turn_response_execution::Entity::find()
            .select_only()
            .column(pioneer_entity::agent_turn_response_execution::Column::ExecutionId)
            .filter(
                pioneer_entity::agent_turn_response_execution::Column::TurnId.is_in(turns.clone()),
            )
            .limit(100)
            .into_tuple::<String>()
            .all(&self.connection)
            .await?;
        let agents = agent_execution::Entity::find()
            .select_only()
            .columns([
                agent_execution::Column::Id,
                agent_execution::Column::Status,
                agent_execution::Column::ParentExecutionId,
                agent_execution::Column::HomeRootThreadId,
                agent_execution::Column::WorkGraphRootExecutionId,
                agent_execution::Column::CreatedAt,
                agent_execution::Column::FinishedAt,
            ])
            .filter(agent_execution::Column::WorkspaceId.eq(workspace))
            .filter(agent_execution::Column::Id.is_in(agent_ids))
            .order_by_asc(agent_execution::Column::Id)
            .limit(100)
            .into_model::<AgentOwner>()
            .all(&self.connection)
            .await?;
        let executions = execution::Entity::find()
            .select_only()
            .columns([
                execution::Column::TurnId,
                execution::Column::Status,
                execution::Column::OwnerGeneration,
                execution::Column::StartedAt,
                execution::Column::CompletedAt,
            ])
            .filter(execution::Column::WorkspaceId.eq(workspace))
            .filter(execution::Column::ThreadId.eq(thread))
            .filter(execution::Column::TurnId.is_in(turns.clone()))
            .order_by_asc(execution::Column::TurnId)
            .limit(100)
            .into_model::<Execution>()
            .all(&self.connection)
            .await?;
        let windows = window::Entity::find()
            .select_only()
            .columns([
                window::Column::Id,
                window::Column::TurnId,
                window::Column::Status,
                window::Column::WindowIndex,
                window::Column::StartedAt,
                window::Column::CompletedAt,
            ])
            .filter(window::Column::WorkspaceId.eq(workspace))
            .filter(window::Column::ThreadId.eq(thread))
            .filter(window::Column::TurnId.is_in(turns))
            .order_by_asc(window::Column::TurnId)
            .order_by_asc(window::Column::WindowIndex)
            .limit(100)
            .into_model::<Window>()
            .all(&self.connection)
            .await?;
        let mut out = Vec::new();
        for row in agents {
            out.push(serde_json::json!({"operation_id":row.id,"kind":"agent_execution","status":row.status,
                "parent_operation_id":row.parent_execution_id,"root_thread_id":row.home_root_thread_id,"work_graph_root_execution_id":row.work_graph_root_execution_id,
                "created_wall_ms":row.created_at.timestamp_millis(),"start_wall_ms":null,"end_wall_ms":row.finished_at.map(|v|v.timestamp_millis()),
                "elapsed_ns":null,"basis":"agent_execution_source_created_and_finished_not_inferred_start","revision_semantics":"live_domain_owner_not_frozen_at_diagnostic_frontier"}));
        }
        for row in executions {
            out.push(serde_json::json!({"operation_id":format!("turn:{}",row.turn_id),"kind":"execution","status":row.status,"generation":row.owner_generation,
            "start_wall_ms":row.started_at.map(|v|v.timestamp_millis()),"end_wall_ms":row.completed_at.map(|v|v.timestamp_millis()),
            "elapsed_ns":null,"basis":"turn_execution_source_wall","revision_semantics":"live_domain_owner_not_frozen_at_diagnostic_frontier"}));
        }
        for row in windows {
            out.push(serde_json::json!({"operation_id":row.id,"parent_operation_id":format!("turn:{}",row.turn_id),"kind":"execution_window","status":row.status,"window_index":row.window_index,
            "start_wall_ms":row.started_at.timestamp_millis(),"end_wall_ms":row.completed_at.map(|v|v.timestamp_millis()),"elapsed_ns":null,
            "basis":"turn_execution_window_source_wall","revision_semantics":"live_domain_owner_not_frozen_at_diagnostic_frontier"}));
        }
        Ok(out)
    }
}

async fn commit_backfill_checkpoint<C: ConnectionTrait>(
    db: &C,
    key: &str,
    current: Option<pioneer_entity::thread_timeline_projection_meta::Model>,
    json: &str,
    complete: bool,
) -> Result<()> {
    use pioneer_entity::thread_timeline_projection_meta as meta;
    let now = chrono::Utc::now().fixed_offset();
    let existed = current.is_some();
    let model = if let Some(current) = current {
        let mut m: meta::ActiveModel = current.into();
        m.projection_config_json = Set(Some(json.to_owned()));
        m.status = Set(if complete { "complete" } else { "backfilling" }.into());
        m.updated_at = Set(now);
        m
    } else {
        meta::ActiveModel {
            projection_key: Set(key.into()),
            projection_version: Set(1),
            status: Set(if complete { "complete" } else { "backfilling" }.into()),
            source_thread_count: Set(0),
            source_turn_count: Set(0),
            source_turn_item_count: Set(0),
            source_turn_event_count: Set(0),
            last_error: Set(None),
            backfill_started_at: Set(Some(now)),
            backfilled_at: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
            projection_config_hash: Set(None),
            projection_config_json: Set(Some(json.to_owned())),
        }
    };
    if existed {
        model.update(db).await?;
    } else {
        model.insert(db).await?;
    }
    Ok(())
}

const AUX_BACKFILL_KEY: &str = "execution_aux_usage_legacy_v1";
impl CrudStore {
    pub async fn backfill_aux_execution_observations_quantum(
        &self,
    ) -> Result<ExecutionObservationBackfillProgress> {
        use pioneer_entity::{
            provider_usage_observation as source, thread_timeline_projection_meta as meta,
        };
        let checkpoint = meta::Entity::find_by_id(AUX_BACKFILL_KEY)
            .one(&self.connection)
            .await?;
        let previous_json = checkpoint
            .as_ref()
            .and_then(|row| row.projection_config_json.clone());
        let before: ExecutionObservationBackfillProgress = previous_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?
            .unwrap_or_default();
        if before.complete {
            return Ok(before);
        }
        use sea_orm::sea_query::{Alias, Expr, Func};
        use sea_orm::{ExprTrait, FromQueryResult};
        #[derive(FromQueryResult)]
        struct Row {
            id: String,
            workspace_id: String,
            owner_id: String,
            operation_kind: String,
            status: String,
            usage_json: Option<String>,
            started_at: i64,
        }
        let length = Func::cust(Alias::new("length"))
            .arg(Expr::col(source::Column::UsageJson).cast_as(Alias::new("BLOB")));
        let rows = source::Entity::find()
            .select_only()
            .columns([
                source::Column::Id,
                source::Column::WorkspaceId,
                source::Column::OwnerId,
                source::Column::OperationKind,
                source::Column::Status,
                source::Column::StartedAt,
            ])
            .column_as(
                Expr::case(
                    Expr::expr(length).lte(32 * 1024),
                    Expr::col(source::Column::UsageJson),
                )
                .finally(Option::<String>::None),
                "usage_json",
            )
            .filter(source::Column::Id.gt(&before.after))
            .order_by_asc(source::Column::Id)
            .limit(32)
            .into_model::<Row>()
            .all(&self.connection)
            .await?;
        let count = rows.len();
        let mut progress = before;
        let mut prepared = Vec::new();
        for row in rows {
            progress.after = row.id.clone();
            progress.scanned += 1;
            let Some(body) = row.usage_json else {
                progress.unavailable += 1;
                continue;
            };
            let Ok(mut usage) = serde_json::from_str::<Value>(&body) else {
                progress.unavailable += 1;
                continue;
            };
            // New producers already projected atomically. Main numeric refinements
            // share the native attempt leaf and must never create a second charge.
            if usage["_pioneer_observation"].is_object() || row.operation_kind == "main" {
                continue;
            }
            if workspace::Entity::find_by_id(&row.workspace_id)
                .one(&self.connection)
                .await?
                .is_none()
            {
                progress.unavailable += 1;
                continue;
            }
            let owner = thread::Entity::find_by_id(&row.owner_id)
                .one(&self.connection)
                .await?
                .filter(|t| t.workspace_id == row.workspace_id);
            let run_owner = if owner.is_none() && row.operation_kind == "self_improvement" {
                pioneer_entity::self_improvement_run::Entity::find_by_id(&row.owner_id)
                    .one(&self.connection)
                    .await?
                    .filter(|run| run.workspace_id == row.workspace_id)
            } else {
                None
            };
            if owner.is_none() && run_owner.is_none() && row.owner_id != row.workspace_id {
                // A deleted/private legacy thread cannot be reassigned to a
                // broader WorkspaceRead scope. Preserve its original journal,
                // mark unavailable progress, and never invent an owner.
                progress.unavailable += 1;
                continue;
            }
            let evidence = usage["accounting"]
                .get("pricing")
                .filter(|v| v.is_object())
                .cloned();
            if let Some(accounting) = usage.get_mut("accounting").and_then(Value::as_object_mut) {
                accounting.remove("pricing");
            }
            let thread_id = owner.map(|t| t.id);
            let input = ExecutionObservationInput {
                workspace_id: row.workspace_id.clone(),
                operation_id: format!("provider:journal:{}", row.id),
                revision: 0,
                thread_id: thread_id.clone(),
                turn_id: None,
                owner_kind: if thread_id.is_some() {
                    "thread"
                } else if run_owner.is_some() {
                    "self_improvement_run"
                } else {
                    "workspace"
                }
                .into(),
                owner_id: thread_id.unwrap_or_else(|| {
                    if run_owner.is_some() {
                        row.owner_id.clone()
                    } else {
                        row.workspace_id.clone()
                    }
                }),
                parent_operation_id: None,
                kind: if matches!(
                    usage["provider"].as_str(),
                    Some("codex-cli" | "claude-cli" | "opencode-cli")
                ) {
                    "external_usage_snapshot"
                } else {
                    "provider_attempt"
                }
                .into(),
                purpose: row.operation_kind.clone(),
                provider: usage["provider"].as_str().map(str::to_owned),
                model: usage["model"].as_str().map(str::to_owned),
                status: row.status.clone(),
                recorded_at_ms: row.started_at.saturating_mul(1000),
                facts: serde_json::json!({"usage":usage,"legacy_owner_id":row.owner_id,
                    "usage_source_fingerprint":hex::encode(Sha256::digest(body.as_bytes())),
                    "coverage":"legacy_journal_no_dispatch_or_final_manifest_evidence","timing":{"prepared_at_unix_seconds":row.started_at,"basis":"legacy_source_preparation_only"}}),
                tariff_evidence: evidence,
            };
            match input.prepare() {
                Ok(fact) => {
                    let missing = if fact.tariff.is_none() {
                        if let Some(hash) = &fact.tariff_reference {
                            self.execution_tariff(&row.workspace_id, hash)
                                .await?
                                .is_none()
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if missing {
                        progress.unavailable += 1;
                    } else {
                        prepared.push((row.id, body, fact));
                    }
                }
                Err(_) => progress.unavailable += 1,
            }
        }
        progress.complete = count < 32;
        let next_json = serde_json::to_string(&progress)?;
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let current = meta::Entity::find_by_id(AUX_BACKFILL_KEY).one(&tx).await?;
            ensure!(
                current
                    .as_ref()
                    .and_then(|row| row.projection_config_json.as_deref())
                    == previous_json.as_deref(),
                "auxiliary backfill progress changed"
            );
            for (id, expected, fact) in &prepared {
                let source = source::Entity::find_by_id(id)
                    .select_only()
                    .column(source::Column::Id)
                    .filter(source::Column::UsageJson.eq(expected.clone()))
                    .into_tuple::<String>()
                    .one(&tx)
                    .await?;
                if source.is_none() {
                    continue;
                }
                // A concurrent deletion is retried from the same durable cursor;
                // the next preparation classifies the now-missing source/owner.
                commit(&tx, fact).await?;
            }
            commit_backfill_checkpoint(
                &tx,
                AUX_BACKFILL_KEY,
                current,
                &next_json,
                progress.complete,
            )
            .await?;
            tx.commit().await?;
            Ok(())
        })
        .await?;
        Ok(progress)
    }
}

impl CrudStore {
    /// Prepare from the already serialized source outside writer admission. The
    /// same transaction as the source append revalidates ownership and commits
    /// the content-free measurement; no independent delivery or recorder exists.
    pub(crate) async fn prepare_retained_tool_source(
        &self,
        entry: &super::turn_llm_context::NewTurnLlmContextEntry,
        delivery_key: &str,
    ) -> Result<Option<PreparedExecutionObservation>> {
        if entry.source != "tool_result_v2" {
            return Ok(None);
        }
        let turn = turn::Entity::find_by_id(&entry.turn_id)
            .one(&self.connection)
            .await?
            .context("retained source turn missing")?;
        let thread = thread::Entity::find_by_id(&turn.thread_id)
            .one(&self.connection)
            .await?
            .context("retained source thread missing")?;
        ExecutionObservationInput {
            workspace_id:thread.workspace_id,
            operation_id:format!("tool-source:{}:{}",entry.turn_id,hex::encode(Sha256::digest(delivery_key.as_bytes()))),
            revision:0,thread_id:Some(turn.thread_id),turn_id:Some(entry.turn_id.clone()),
            owner_kind:"turn".into(),owner_id:entry.turn_id.clone(),
            parent_operation_id:Some(format!("turn:{}",entry.turn_id)),
            kind:"tool_result_retention".into(),purpose:"tool".into(),provider:None,model:None,status:"committed".into(),
            recorded_at_ms:entry.created_at.timestamp_millis(),
            facts:serde_json::json!({"source_kind":entry.source,"source_item_id":entry.item_id,
                "source_attempt_id":entry.attempt_id,"tool_name":entry.tool_name,
                "logical_tool_invocation_id":entry.item_id.as_ref().map(|id|format!("item:{}:{id}",entry.turn_id)),
                "retained_source_serialized_bytes":entry.payload.len(),
                "source_version_fingerprint":hex::encode(Sha256::digest(entry.payload.as_bytes())),
                "basis":"exact_committed_turn_llm_context_serialized_payload",
                "source_reconstruction":"subject_to_existing_source_acl_and_retention",
                "timing":{"coverage":"source_created_timestamp_only_commit_time_unmeasured"},
                "source_created_at_unix_ms":entry.created_at.timestamp_millis()}),tariff_evidence:None,
        }.prepare().map(Some)
    }
}
