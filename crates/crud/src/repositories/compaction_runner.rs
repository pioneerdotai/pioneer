use super::compaction::{
    CHECKPOINT_SOURCE_LIMIT, SOURCE_PAGE_BYTES, SOURCE_PAGE_ROWS, checkpoint_identity,
};
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_compaction::runner::{FailureKind, RunnerPhase, RunnerState};
use pioneer_compaction::{Checkpoint, ModelBudget, SourceRef};
use pioneer_crud_compaction::runner_state::{
    exact_json, insert_initial, observation, update_runner_state,
};
use pioneer_entity::{
    compaction_attempt_observation, compaction_checkpoint, compaction_context, compaction_coverage,
    compaction_execution_stop, compaction_manifest, compaction_operation, compaction_runner_plan,
    compaction_runner_state, thread, turn,
};
use sea_orm::sea_query::{Alias, Asterisk, Expr, ExprTrait, Func, JoinType, OnConflict, Query};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use sea_orm::{ConnectionTrait, TransactionTrait};

fn json_text(column: Expr, path: &[&str]) -> Expr {
    Expr::expr(Func::cust(Alias::new("json_extract_path_text")).args(
        std::iter::once(column.cast_as("json")).chain(path.iter().map(|key| Expr::val(*key))),
    ))
}

fn null_safe_eq(left: Expr, right: Expr) -> Expr {
    // SeaQuery does not expose the PostgreSQL null-safe equality operator.
    Expr::cust_with_exprs("$1 IS NOT DISTINCT FROM $2", [left, right])
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PublicationTestPause {
    ReaderPreflight,
    BeforeWriter,
    ProofSeal,
    CheckpointOriginAcquire,
}

#[cfg(any(test, feature = "test-support"))]
struct RegisteredPublicationTestHook {
    token: std::sync::Arc<()>,
    reached: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

#[cfg(any(test, feature = "test-support"))]
static PUBLICATION_TEST_HOOKS: std::sync::OnceLock<
    std::sync::Mutex<
        std::collections::BTreeMap<
            (usize, String, PublicationTestPause),
            RegisteredPublicationTestHook,
        >,
    >,
> = std::sync::OnceLock::new();

#[cfg(any(test, feature = "test-support"))]
pub struct PublicationTestHookHandle {
    key: (usize, String, PublicationTestPause),
    token: std::sync::Arc<()>,
    _store: CrudStore,
    reached: Option<tokio::sync::oneshot::Receiver<()>>,
    release: Option<tokio::sync::oneshot::Sender<()>>,
}

#[cfg(any(test, feature = "test-support"))]
impl PublicationTestHookHandle {
    pub async fn reached(&mut self) {
        let reached = self
            .reached
            .take()
            .expect("publication hook already awaited");
        tokio::time::timeout(std::time::Duration::from_secs(10), reached)
            .await
            .expect("publication hook was not reached before the diagnostic timeout")
            .expect("publication hook participant ended before reaching the pause");
    }

    pub fn release(mut self) {
        self.release
            .take()
            .expect("publication hook already released")
            .send(())
            .expect("publication hook participant ended before release");
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Drop for PublicationTestHookHandle {
    fn drop(&mut self) {
        let mut hooks = PUBLICATION_TEST_HOOKS
            .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if hooks
            .get(&self.key)
            .is_some_and(|hook| std::sync::Arc::ptr_eq(&hook.token, &self.token))
        {
            hooks.remove(&self.key);
        }
        // Dropping the release sender also unblocks a participant that already
        // took this registration and is waiting at the pause.
    }
}
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct PublicationTestMetrics {
    coverage_checks: usize,
    manifest_checks: usize,
    heavy_checks_while_writer: usize,
    writer_entries: usize,
    proof_seal_writer_entries: usize,
    topology_pages: usize,
    topology_page_rows: usize,
    topology_page_bytes: usize,
    dependency_checks: usize,
    writer_depth: usize,
}

#[cfg(test)]
static PUBLICATION_TEST_METRICS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::BTreeMap<String, PublicationTestMetrics>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
pub(super) fn reset_publication_test_metrics(operation: &str) {
    PUBLICATION_TEST_METRICS
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(operation.to_owned(), PublicationTestMetrics::default());
}

#[cfg(test)]
pub(super) fn publication_test_metrics(operation: &str) -> PublicationTestMetrics {
    PUBLICATION_TEST_METRICS
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(operation)
        .copied()
        .unwrap_or_default()
}

#[cfg(test)]
fn record_publication_heavy_check(operation: &str, manifest: bool) {
    let mut all = PUBLICATION_TEST_METRICS
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(metrics) = all.get_mut(operation) else {
        return;
    };
    if manifest {
        metrics.manifest_checks += 1;
    } else {
        metrics.coverage_checks += 1;
    }
    if metrics.writer_depth != 0 {
        metrics.heavy_checks_while_writer += 1;
    }
}

#[cfg(test)]
pub(super) fn record_proof_preparation_check(operation: &str, topology: bool) {
    let mut all = PUBLICATION_TEST_METRICS
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(metrics) = all.get_mut(operation) {
        if topology {
            metrics.topology_pages += 1;
        } else {
            metrics.dependency_checks += 1;
        }
        if metrics.writer_depth != 0 {
            metrics.heavy_checks_while_writer += 1;
        }
    }
}

#[cfg(test)]
pub(super) fn record_topology_page(operation: &str, rows: usize, bytes: usize) {
    let mut all = PUBLICATION_TEST_METRICS
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(metrics) = all.get_mut(operation) {
        metrics.topology_page_rows = metrics.topology_page_rows.max(rows);
        metrics.topology_page_bytes = metrics.topology_page_bytes.max(bytes);
    }
}

#[cfg(test)]
pub(super) struct PublicationWriterTestGuard(String);

#[cfg(test)]
impl PublicationWriterTestGuard {
    pub(super) fn enter_seal(operation: &str) -> Self {
        let guard = Self::enter(operation);
        let mut all = PUBLICATION_TEST_METRICS
            .get()
            .expect("metrics initialized by enter")
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(metrics) = all.get_mut(operation) {
            metrics.proof_seal_writer_entries += 1;
        }
        guard
    }
    fn enter(operation: &str) -> Self {
        let mut all = PUBLICATION_TEST_METRICS
            .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(metrics) = all.get_mut(operation) {
            metrics.writer_entries += 1;
            metrics.writer_depth += 1;
        }
        Self(operation.to_owned())
    }
}

#[cfg(test)]
impl Drop for PublicationWriterTestGuard {
    fn drop(&mut self) {
        let mut all = PUBLICATION_TEST_METRICS
            .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(metrics) = all.get_mut(&self.0) {
            metrics.writer_depth -= 1;
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn arm_publication_test_hook(
    store: &CrudStore,
    operation: &str,
    phase: PublicationTestPause,
) -> PublicationTestHookHandle {
    let key = (
        store.connection.runtime_identity(),
        operation.to_owned(),
        phase,
    );
    let token = std::sync::Arc::new(());
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let hook = RegisteredPublicationTestHook {
        token: token.clone(),
        reached: reached_tx,
        release: release_rx,
    };
    let mut hooks = PUBLICATION_TEST_HOOKS
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(!hooks.contains_key(&key), "publication hook already armed");
    hooks.insert(key.clone(), hook);
    PublicationTestHookHandle {
        key,
        token,
        _store: store.clone(),
        reached: Some(reached_rx),
        release: Some(release_tx),
    }
}

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub async fn trigger_publication_test_hook(
    store: &CrudStore,
    operation: &str,
    phase: PublicationTestPause,
) {
    pause_publication_test_hook(store.connection.runtime_identity(), operation, phase).await;
}

#[cfg(any(test, feature = "test-support"))]
pub(super) async fn pause_publication_test_hook(
    runtime_identity: usize,
    operation: &str,
    phase: PublicationTestPause,
) {
    let key = (runtime_identity, operation.to_owned(), phase);
    let hook = PUBLICATION_TEST_HOOKS
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&key);
    if let Some(hook) = hook {
        let _ = hook.reached.send(());
        let _ = hook.release.await;
    }
}

#[derive(Clone, Debug)]
pub struct ManifestEntry {
    pub ordinal: u64,
    pub unit: u64,
    pub reference_only: bool,
    pub thread_id: String,
    pub source: SourceRef,
}
#[derive(Clone, Debug)]
pub struct RunnerPlanRecord {
    pub source_count: u64,
    pub reference_count: u64,
    pub budget: ModelBudget,
    pub ready: bool,
}

/// Manifest admission contains metadata only and resumes in bounded batches.
/// No provider may run until activate_runner has checked the whole manifest.
/// The execution turn is an immutable admission boundary. Its terminal
/// interruption fences checkpoint publication even before service cleanup.
pub(crate) async fn compaction_execution_cancelled<C: ConnectionTrait>(
    db: &C,
    operation: &str,
) -> Result<bool> {
    Ok(compaction_operation::Entity::find()
        .select_only()
        .join(
            JoinType::InnerJoin,
            compaction_operation::Entity::belongs_to(compaction_context::Entity)
                .from(compaction_operation::Column::Owner)
                .to(compaction_context::Column::Owner)
                .into(),
        )
        .expr(Expr::col((
            compaction_operation::Entity,
            compaction_operation::Column::Id,
        )))
        .filter(
            Expr::col((
                compaction_operation::Entity,
                compaction_operation::Column::Id,
            ))
            .eq(Expr::Value(operation.into()))
            .and(
                Expr::col((
                    compaction_operation::Entity,
                    compaction_operation::Column::ExecutionTurn,
                ))
                .is_not_null(),
            )
            .and(
                Expr::exists(
                    Query::select()
                        .expr(Expr::val(1_i64))
                        .from_as(compaction_execution_stop::Entity, "stop")
                        .and_where(
                            Expr::col(("stop", compaction_execution_stop::Column::Owner))
                                .eq(Expr::col((
                                    compaction_context::Entity,
                                    compaction_context::Column::Owner,
                                )))
                                .and(
                                    Expr::col(("stop", compaction_execution_stop::Column::TurnId))
                                        .eq(Expr::col((
                                            compaction_operation::Entity,
                                            compaction_operation::Column::ExecutionTurn,
                                        ))),
                                ),
                        )
                        .to_owned(),
                )
                .or(Expr::exists(
                    Query::select()
                        .expr(Expr::val(1_i64))
                        .from_as(turn::Entity, "t")
                        .and_where(
                            Expr::col(("t", turn::Column::Id))
                                .eq(Expr::col((
                                    compaction_operation::Entity,
                                    compaction_operation::Column::ExecutionTurn,
                                )))
                                .and(
                                    Expr::col(("t", turn::Column::Status))
                                        .is_in(["interrupted", "cancelled"])
                                        .not(),
                                ),
                        )
                        .to_owned(),
                )
                .not()),
            ),
        )
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_some())
}

pub(crate) async fn compaction_bind_execution_turn<C: ConnectionTrait>(
    db: &C,
    operation: &str,
    turn: &str,
) -> Result<bool> {
    let changed = compaction_operation::Entity::update_many()
        .col_expr(
            compaction_operation::Column::ExecutionTurn,
            Expr::Value(turn.into()),
        )
        .filter(
            Expr::col(compaction_operation::Column::Id)
                .eq(Expr::Value(operation.into()))
                .and(Expr::col(compaction_operation::Column::ExecutionTurn).is_null())
                .and(Expr::col(compaction_operation::Column::Status).eq(Expr::val("running")))
                .and(Expr::exists(
                    Query::select()
                        .expr(Expr::val(1_i64))
                        .from_as(compaction_context::Entity, "c")
                        .join_as(
                            JoinType::InnerJoin,
                            turn::Entity,
                            "t",
                            Expr::col(("t", turn::Column::ThreadId))
                                .eq(Expr::col(("c", compaction_context::Column::ThreadId))),
                        )
                        .and_where(
                            Expr::col(("c", compaction_context::Column::Owner))
                                .eq(Expr::col(("compaction_operation", "owner")))
                                .and(
                                    Expr::col(("t", turn::Column::Id)).eq(Expr::Value(turn.into())),
                                )
                                .and(
                                    Expr::exists(
                                        Query::select()
                                            .expr(Expr::val(1_i64))
                                            .from_as(compaction_execution_stop::Entity, "stop")
                                            .and_where(
                                                Expr::col((
                                                    "stop",
                                                    compaction_execution_stop::Column::Owner,
                                                ))
                                                .eq(Expr::col((
                                                    "c",
                                                    compaction_context::Column::Owner,
                                                )))
                                                .and(
                                                    Expr::col((
                                                        "stop",
                                                        compaction_execution_stop::Column::TurnId,
                                                    ))
                                                    .eq(Expr::col(("t", turn::Column::Id))),
                                                ),
                                            )
                                            .to_owned(),
                                    )
                                    .not(),
                                ),
                        )
                        .to_owned(),
                )),
        )
        .exec(db)
        .await?;
    ensure!(
        compaction_operation::Entity::find()
            .select_only()
            .expr(Expr::val(1_i64))
            .filter(
                Expr::col(compaction_operation::Column::Id)
                    .eq(Expr::Value(operation.into()))
                    .and(
                        Expr::col(compaction_operation::Column::ExecutionTurn)
                            .eq(Expr::Value(turn.into()))
                    )
            )
            .into_tuple::<i64>()
            .one(db)
            .await?
            .is_some(),
        "execution turn binding conflicts with operation scope"
    );
    Ok(changed.rows_affected > 0)
}

pub(crate) async fn compaction_prepare_runner<C: ConnectionTrait>(
    db: &C,
    operation: &str,
    budget: &ModelBudget,
    source_count: u64,
    reference_count: u64,
) -> Result<bool> {
    ensure!(
        source_count > 0,
        "compaction needs a selected source or previous checkpoint"
    );
    let descriptor = serde_json::to_string(budget)?;
    ensure!(
        descriptor.len() <= SOURCE_PAGE_BYTES,
        "runner descriptor exceeds quantum"
    );
    let source_count = i64::try_from(source_count)?;
    let reference_count = i64::try_from(reference_count)?;
    let changed = db
        .execute(
            &Query::insert()
                .into_table(compaction_runner_plan::Entity)
                .columns([
                    compaction_runner_plan::Column::OperationId,
                    compaction_runner_plan::Column::SourceCount,
                    compaction_runner_plan::Column::ReferenceCount,
                    compaction_runner_plan::Column::Descriptor,
                ])
                .select_from(
                    compaction_operation::Entity::find()
                        .select_only()
                        .expr(Expr::col(compaction_operation::Column::Id))
                        .expr(Expr::Value(source_count.into()))
                        .expr(Expr::Value(reference_count.into()))
                        .expr(exact_json(descriptor.clone()))
                        .filter(
                            Expr::col(compaction_operation::Column::Id)
                                .eq(Expr::Value(operation.into()))
                                .and(
                                    Expr::col(compaction_operation::Column::Status)
                                        .eq(Expr::val("running")),
                                ),
                        )
                        .into_query(),
                )?
                .on_conflict(
                    OnConflict::columns(["operation_id"])
                        .do_nothing()
                        .to_owned(),
                )
                .to_owned(),
        )
        .await?;
    let exact = compaction_runner_plan::Entity::find()
        .select_only()
        .expr(Expr::col(compaction_runner_plan::Column::OperationId))
        .filter(
            Expr::col(compaction_runner_plan::Column::OperationId)
                .eq(Expr::Value(operation.into()))
                .and(
                    Expr::col(compaction_runner_plan::Column::SourceCount)
                        .eq(Expr::Value(source_count.into())),
                )
                .and(
                    Expr::col(compaction_runner_plan::Column::ReferenceCount)
                        .eq(Expr::Value(reference_count.into())),
                )
                .and(
                    Expr::col(compaction_runner_plan::Column::Descriptor)
                        .cast_as("text")
                        .eq(Expr::Value(descriptor.into())),
                ),
        )
        .into_tuple::<String>()
        .one(db)
        .await?;
    ensure!(exact.is_some(), "runner plan idempotency conflict");
    Ok(changed.rows_affected() > 0)
}
pub(crate) async fn compaction_runner_plan<C: ConnectionTrait>(
    db: &C,
    operation: &str,
) -> Result<Option<RunnerPlanRecord>> {
    use pioneer_entity::compaction_runner_plan as plan;
    use sea_orm::EntityTrait;
    let Some(row) = plan::Entity::find_by_id(operation).one(db).await? else {
        return Ok(None);
    };
    Ok(Some(RunnerPlanRecord {
        source_count: u64::try_from(row.source_count)?,
        reference_count: u64::try_from(row.reference_count)?,
        budget: serde_json::from_value(row.descriptor)?,
        ready: row.ready != 0,
    }))
}
pub(crate) async fn compaction_append_manifest(
    store: &CrudStore,
    operation: &str,
    entries: &[ManifestEntry],
) -> Result<()> {
    ensure!(
        entries.len() <= SOURCE_PAGE_ROWS as usize,
        "manifest batch exceeds row quantum"
    );
    let mut bytes = 0;
    let prepared = entries
        .iter()
        .map(|entry| -> Result<_> {
            bytes += entry.thread_id.len()
                + entry.source.scope.len()
                + entry.source.id.len()
                + entry.source.version.len();
            let ordinal = i64::try_from(entry.ordinal)?;
            let unit = i64::try_from(entry.unit)?;
            let reference_only = i64::from(entry.reference_only);
            let model = compaction_manifest::ActiveModel {
                operation_id: sea_orm::Set(operation.to_owned()),
                ordinal: sea_orm::Set(ordinal),
                unit_ordinal: sea_orm::Set(unit),
                reference_only: sea_orm::Set(reference_only),
                source_thread: sea_orm::Set(entry.thread_id.clone()),
                source_scope: sea_orm::Set(entry.source.scope.clone()),
                source_id: sea_orm::Set(entry.source.id.clone()),
                source_version: sea_orm::Set(entry.source.version.clone()),
            };
            let exact = compaction_manifest::Entity::find()
                .filter(compaction_manifest::Column::OperationId.eq(operation))
                .filter(compaction_manifest::Column::Ordinal.eq(ordinal))
                .select_only()
                .column(compaction_manifest::Column::Ordinal)
                .filter(compaction_manifest::Column::UnitOrdinal.eq(unit))
                .filter(compaction_manifest::Column::ReferenceOnly.eq(reference_only))
                .filter(compaction_manifest::Column::SourceThread.eq(entry.thread_id.clone()))
                .filter(compaction_manifest::Column::SourceScope.eq(entry.source.scope.clone()))
                .filter(compaction_manifest::Column::SourceId.eq(entry.source.id.clone()))
                .filter(
                    compaction_manifest::Column::SourceVersion.eq(entry.source.version.clone()),
                );
            Ok((model, exact))
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        bytes <= SOURCE_PAGE_BYTES,
        "manifest batch exceeds byte quantum"
    );
    // Prepare only metadata statements outside writer capacity. The insert
    // rechecks live ownership and never overwrites an existing revision;
    // edits racing admission therefore remain detectable at final CAS.
    let seeds = entries
        .iter()
        .map(|entry| -> Result<_> {
            let (prefix, turn) = entry
                .source
                .scope
                .split_once(':')
                .ok_or_else(|| anyhow::anyhow!("invalid manifest source scope"))?;
            let kind = match prefix {
                "input" => super::compaction::CanonicalSource::Input,
                "event" => super::compaction::CanonicalSource::Event,
                "context" => super::compaction::CanonicalSource::ProviderContext,
                "item" => super::compaction::CanonicalSource::ToolItem,
                "checkpoint" | "task-basis" => return Ok(None),
                _ => anyhow::bail!("unsupported manifest source"),
            };
            let revision = entry
                .source
                .version
                .strip_prefix(&format!("{}:", kind.version_prefix()))
                .ok_or_else(|| anyhow::anyhow!("manifest source needs a revision"))?
                .parse::<u64>()?;
            ensure!(revision > 0, "invalid source revision");
            Ok(Some((
                kind,
                turn.to_owned(),
                entry.source.id.clone(),
                entry.thread_id.clone(),
            )))
        })
        .collect::<Result<Vec<_>>>()?;
    store
        .run_serialized_write(|| async {
            let txn = store.connection.begin().await?;
            let open = compaction_runner_plan::Entity::find()
                .select_only()
                .join(
                    JoinType::InnerJoin,
                    compaction_runner_plan::Entity::belongs_to(compaction_operation::Entity)
                        .from(compaction_runner_plan::Column::OperationId)
                        .to(compaction_operation::Column::Id)
                        .into(),
                )
                .expr(Expr::col((
                    compaction_runner_plan::Entity,
                    compaction_runner_plan::Column::OperationId,
                )))
                .filter(
                    Expr::col((
                        compaction_runner_plan::Entity,
                        compaction_runner_plan::Column::OperationId,
                    ))
                    .eq(Expr::Value(operation.into()))
                    .and(
                        Expr::col((
                            compaction_runner_plan::Entity,
                            compaction_runner_plan::Column::Ready,
                        ))
                        .eq(Expr::val(0_i64)),
                    )
                    .and(
                        Expr::col((
                            compaction_operation::Entity,
                            compaction_operation::Column::Status,
                        ))
                        .eq(Expr::val("running")),
                    ),
                )
                .into_tuple::<String>()
                .one(&txn)
                .await?;
            ensure!(open.is_some(), "manifest is immutable or operation stopped");
            for (kind, turn_id, source_id, thread_id) in seeds.iter().flatten() {
                match kind {
                    super::compaction::CanonicalSource::Input => {
                        if manifest_source_exists::<_, pioneer_entity::turn_input::Entity>(
                            &txn,
                            operation,
                            source_id,
                            turn_id,
                            thread_id,
                            (
                                pioneer_entity::turn_input::Column::Id,
                                pioneer_entity::turn_input::Column::TurnId,
                            ),
                        )
                        .await?
                        {
                            pioneer_entity::compaction_input_revision::Entity::insert(
                                pioneer_entity::compaction_input_revision::ActiveModel {
                                    source_id: sea_orm::Set(source_id.clone()),
                                    turn_id: sea_orm::Set(turn_id.clone()),
                                    revision: sea_orm::Set(1),
                                    present: sea_orm::Set(1),
                                    ..Default::default()
                                },
                            )
                            .on_conflict(
                                OnConflict::columns([
                                    pioneer_entity::compaction_input_revision::Column::SourceId,
                                ])
                                .do_nothing()
                                .to_owned(),
                            )
                            .exec_without_returning(&txn)
                            .await?;
                        }
                    }
                    super::compaction::CanonicalSource::Event => {
                        if manifest_source_exists::<_, pioneer_entity::turn_event::Entity>(
                            &txn,
                            operation,
                            source_id,
                            turn_id,
                            thread_id,
                            (
                                pioneer_entity::turn_event::Column::Id,
                                pioneer_entity::turn_event::Column::TurnId,
                            ),
                        )
                        .await?
                        {
                            pioneer_entity::compaction_event_revision::Entity::insert(
                                pioneer_entity::compaction_event_revision::ActiveModel {
                                    source_id: sea_orm::Set(source_id.clone()),
                                    turn_id: sea_orm::Set(turn_id.clone()),
                                    revision: sea_orm::Set(1),
                                    present: sea_orm::Set(1),
                                    ..Default::default()
                                },
                            )
                            .on_conflict(
                                OnConflict::columns([
                                    pioneer_entity::compaction_event_revision::Column::SourceId,
                                ])
                                .do_nothing()
                                .to_owned(),
                            )
                            .exec_without_returning(&txn)
                            .await?;
                        }
                    }
                    super::compaction::CanonicalSource::ProviderContext => {
                        if manifest_source_exists::<_, pioneer_entity::turn_llm_context::Entity>(
                            &txn,
                            operation,
                            source_id,
                            turn_id,
                            thread_id,
                            (
                                pioneer_entity::turn_llm_context::Column::Id,
                                pioneer_entity::turn_llm_context::Column::TurnId,
                            ),
                        )
                        .await?
                        {
                            pioneer_entity::compaction_source_revision::Entity::insert(
                                pioneer_entity::compaction_source_revision::ActiveModel {
                                    source_id: sea_orm::Set(source_id.clone()),
                                    turn_id: sea_orm::Set(turn_id.clone()),
                                    revision: sea_orm::Set(1),
                                    present: sea_orm::Set(1),
                                    ..Default::default()
                                },
                            )
                            .on_conflict(
                                OnConflict::columns([
                                    pioneer_entity::compaction_source_revision::Column::SourceId,
                                ])
                                .do_nothing()
                                .to_owned(),
                            )
                            .exec_without_returning(&txn)
                            .await?;
                        }
                    }
                    super::compaction::CanonicalSource::ToolItem => {
                        if manifest_source_exists::<_, pioneer_entity::turn_item::Entity>(
                            &txn,
                            operation,
                            source_id,
                            turn_id,
                            thread_id,
                            (
                                pioneer_entity::turn_item::Column::Id,
                                pioneer_entity::turn_item::Column::TurnId,
                            ),
                        )
                        .await?
                        {
                            pioneer_entity::compaction_item_revision::Entity::insert(
                                pioneer_entity::compaction_item_revision::ActiveModel {
                                    source_id: sea_orm::Set(source_id.clone()),
                                    turn_id: sea_orm::Set(turn_id.clone()),
                                    revision: sea_orm::Set(1),
                                    present: sea_orm::Set(1),
                                    ..Default::default()
                                },
                            )
                            .on_conflict(
                                OnConflict::columns([
                                    pioneer_entity::compaction_item_revision::Column::SourceId,
                                ])
                                .do_nothing()
                                .to_owned(),
                            )
                            .exec_without_returning(&txn)
                            .await?;
                        }
                    }
                }
            }
            for (insert, exact) in &prepared {
                compaction_manifest::Entity::insert(insert.clone())
                    .on_conflict(
                        OnConflict::columns([
                            compaction_manifest::Column::OperationId,
                            compaction_manifest::Column::Ordinal,
                        ])
                        .do_nothing()
                        .to_owned(),
                    )
                    .exec_without_returning(&txn)
                    .await?;
                ensure!(
                    exact.clone().into_tuple::<i64>().one(&txn).await?.is_some(),
                    "manifest idempotency conflict"
                );
            }
            txn.commit().await?;
            Ok(())
        })
        .await
}
pub(crate) async fn compaction_activate_runner(
    store: &CrudStore,
    operation: &str,
    initial: &RunnerState,
) -> Result<bool> {
    ensure!(
        initial.generation == 0
            && initial.attempts == 0
            && initial.retries == 0
            && initial.portion_attempts == 0
            && initial.overflow_rejections == 0
            && initial.overflow_input_ceiling.is_none()
            && initial.corrections == 0,
        "invalid initial runner state"
    );
    let encoded = serde_json::to_string(initial)?;
    let deadline = i64::try_from(initial.deadline_ms)?;
    store
        .run_serialized_write(|| async {
            let txn = store.connection.begin().await?;
            let valid = compaction_runner_plan::Entity::find()
                .select_only()
                .join(
                    JoinType::InnerJoin,
                    compaction_runner_plan::Entity::belongs_to(compaction_operation::Entity)
                        .from(compaction_runner_plan::Column::OperationId)
                        .to(compaction_operation::Column::Id)
                        .into(),
                )
                .expr(Expr::col((
                    compaction_runner_plan::Entity,
                    compaction_runner_plan::Column::OperationId,
                )))
                .filter(
                    Expr::col((
                        compaction_runner_plan::Entity,
                        compaction_runner_plan::Column::OperationId,
                    ))
                    .eq(Expr::Value(operation.into()))
                    .and(
                        Expr::col((
                            compaction_operation::Entity,
                            compaction_operation::Column::Status,
                        ))
                        .eq(Expr::val("running")),
                    )
                    .and(
                        Expr::col((
                            compaction_operation::Entity,
                            compaction_operation::Column::DeadlineMs,
                        ))
                        .eq(Expr::Value(deadline.into())),
                    )
                    .and(
                        Expr::SubQuery(
                            None,
                            Box::new(
                                Query::select()
                                    .expr(Expr::expr(
                                        Func::cust(Alias::new("count")).args([Expr::col(Asterisk)]),
                                    ))
                                    .from_as(compaction_manifest::Entity, "m")
                                    .and_where(
                                        Expr::col(("m", compaction_manifest::Column::OperationId))
                                            .eq(Expr::col((
                                                compaction_runner_plan::Entity,
                                                compaction_runner_plan::Column::OperationId,
                                            )))
                                            .and(
                                                Expr::col((
                                                    "m",
                                                    compaction_manifest::Column::ReferenceOnly,
                                                ))
                                                .eq(Expr::val(0_i64)),
                                            ),
                                    )
                                    .to_owned()
                                    .into(),
                            ),
                        )
                        .eq(Expr::col((
                            compaction_runner_plan::Entity,
                            compaction_runner_plan::Column::SourceCount,
                        ))),
                    )
                    .and(
                        Expr::SubQuery(
                            None,
                            Box::new(
                                Query::select()
                                    .expr(Expr::expr(
                                        Func::cust(Alias::new("count")).args([Expr::col(Asterisk)]),
                                    ))
                                    .from_as(compaction_manifest::Entity, "m")
                                    .and_where(
                                        Expr::col(("m", compaction_manifest::Column::OperationId))
                                            .eq(Expr::col((
                                                compaction_runner_plan::Entity,
                                                compaction_runner_plan::Column::OperationId,
                                            )))
                                            .and(
                                                Expr::col((
                                                    "m",
                                                    compaction_manifest::Column::ReferenceOnly,
                                                ))
                                                .eq(Expr::val(1_i64)),
                                            ),
                                    )
                                    .to_owned()
                                    .into(),
                            ),
                        )
                        .eq(Expr::col((
                            compaction_runner_plan::Entity,
                            compaction_runner_plan::Column::ReferenceCount,
                        ))),
                    )
                    .and(
                        Expr::exists(
                            Query::select()
                                .expr(Expr::val(1_i64))
                                .from_as(compaction_manifest::Entity, "m")
                                .and_where(
                                    Expr::col(("m", compaction_manifest::Column::OperationId))
                                        .eq(Expr::col((
                                            compaction_runner_plan::Entity,
                                            compaction_runner_plan::Column::OperationId,
                                        )))
                                        .and(
                                            Expr::col(("m", compaction_manifest::Column::Ordinal))
                                                .lt(Expr::val(0_i64))
                                                .or(Expr::col((
                                                    "m",
                                                    compaction_manifest::Column::Ordinal,
                                                ))
                                                .gte(
                                                    Expr::col((
                                                        compaction_runner_plan::Entity,
                                                        compaction_runner_plan::Column::SourceCount,
                                                    ))
                                                    .add(Expr::col((compaction_runner_plan::Entity, compaction_runner_plan::Column::ReferenceCount))),
                                                )),
                                        ),
                                )
                                .to_owned(),
                        )
                        .not(),
                    ),
                )
                .into_tuple::<String>()
                .one(&txn)
                .await?;
            ensure!(
                valid.is_some(),
                "manifest is incomplete or admission expired"
            );
            let inserted = insert_initial(&txn, operation, encoded.clone()).await?;
            let ready = compaction_runner_plan::Entity::update_many()
                .col_expr(compaction_runner_plan::Column::Ready, Expr::val(1_i64))
                .filter(
                    Expr::col(compaction_runner_plan::Column::OperationId)
                        .eq(Expr::Value(operation.into())),
                )
                .filter(compaction_runner_plan::Column::Ready.ne(1_i64))
                .exec(&txn)
                .await?;
            txn.commit().await?;
            Ok(inserted || ready.rows_affected > 0)
        })
        .await
}
pub(crate) async fn compaction_runner_state<C: ConnectionTrait>(
    db: &C,
    operation: &str,
) -> Result<Option<RunnerState>> {
    observation(db, operation)
        .await?
        .map(|row| row.decode())
        .transpose()
}
/// Resume only saved progress on a later admission of the identical plan.
/// Preparation reads operation/state and its bounded manifest page; the short
/// transaction rechecks snapshot, generation, checkpoint, head and Stop fences.
/// This publishes no history. Source freshness can race this preliminary check:
/// the existing runner revalidates before reading another raw portion. A
/// completed candidate resumes without revisiting its historical sources.
pub(crate) async fn compaction_resume_deadline(
    store: &CrudStore,
    operation: &str,
    execution_turn: &str,
    deadline_ms: u64,
) -> Result<bool> {
    resume_failed_operation(store, operation, execution_turn, deadline_ms, false).await
}

pub(crate) async fn compaction_resume_invalid_completion(
    store: &CrudStore,
    operation: &str,
    execution_turn: &str,
    deadline_ms: u64,
) -> Result<bool> {
    resume_failed_operation(store, operation, execution_turn, deadline_ms, true).await
}

async fn resume_failed_operation(
    store: &CrudStore,
    operation: &str,
    execution_turn: &str,
    deadline_ms: u64,
    invalid_completion: bool,
) -> Result<bool> {
    let Some(record) =
        super::compaction::compaction_operation(&store.connection, operation).await?
    else {
        return Ok(false);
    };
    if record.status != "failed"
        || (!invalid_completion && record.outcome.as_deref() != Some("deadline"))
    {
        return Ok(false);
    }
    // Expiry is diagnosed before any stale-source fallback, including Ready.
    let bound = super::compaction_source_projection::compaction_bound_source_projection(
        &store.connection,
        operation,
    )
    .await
    .map_err(|error| {
        if invalid_completion {
            error.context("compaction resume snapshot is unavailable")
        } else {
            error
        }
    })?;
    let Some(state) = compaction_runner_state(&store.connection, operation).await? else {
        if invalid_completion {
            anyhow::bail!("compaction resume phase is unsafe");
        }
        return Ok(false);
    };
    if invalid_completion {
        if !matches!(
            state.phase,
            RunnerPhase::Failed {
                kind: FailureKind::InvalidCompletion
            }
        ) {
            return Ok(false);
        }
        ensure!(
            state.previous_checkpoint.is_some(),
            "compaction resume checkpoint is unavailable"
        );
        ensure!(
            state.can_resume_invalid_completion(),
            "compaction resume phase is unsafe"
        );
        ensure!(
            compaction_runner_plan(&store.connection, operation)
                .await?
                .is_some_and(|p| p.ready),
            "compaction resume plan is unavailable"
        );
        ensure!(
            record
                .execution_turn
                .as_deref()
                .is_some_and(|old| old != execution_turn),
            "compaction resume requires a new user execution"
        );
        let checkpoint = state.previous_checkpoint.as_deref().unwrap();
        let saved = compaction_checkpoint::Entity::find_by_id(checkpoint)
            .filter(compaction_checkpoint::Column::Status.is_in(["candidate", "retained"]))
            .filter(compaction_checkpoint::Column::Summary.ne(""))
            .select_only()
            .column(compaction_checkpoint::Column::OperationId)
            .column(compaction_checkpoint::Column::Owner)
            .into_tuple::<(String, String)>()
            .one(&store.connection)
            .await?;
        ensure!(
            saved.is_some_and(|(id, owner)| id == operation && owner == record.owner),
            "compaction resume checkpoint is unavailable"
        );
    } else if !state.can_resume_deadline() || deadline_ms <= state.deadline_ms {
        return Ok(false);
    }
    let legacy_final = state.resume_phase.is_none()
        && compaction_manifest_page(
            &store.connection,
            operation,
            false,
            state.cursor.unit,
            state.cursor.source,
        )
        .await?
        .is_empty();
    let next = if invalid_completion {
        ensure!(!legacy_final, "compaction resume phase is unsafe");
        state.resume_invalid_completion(deadline_ms)?
    } else {
        state.resume_deadline(deadline_ms, legacy_final)?
    };
    if invalid_completion
        && matches!(
            next.phase,
            RunnerPhase::Ready {
                purpose: pioneer_compaction::runner::AttemptPurpose::Portion,
            }
        )
    {
        ensure!(
            !compaction_manifest_page(
                &store.connection,
                operation,
                false,
                state.cursor.unit,
                state.cursor.source
            )
            .await?
            .is_empty(),
            "compaction resume plan is unavailable"
        );
    }
    if (matches!(
        next.phase,
        RunnerPhase::Ready {
            purpose: pioneer_compaction::runner::AttemptPurpose::Portion
        } | RunnerPhase::Backoff {
            purpose: pioneer_compaction::runner::AttemptPurpose::Portion,
            ..
        }
    ) || invalid_completion)
        && !compaction_manifest_sources_current(&store.connection, operation).await?
    {
        if invalid_completion {
            anyhow::bail!("compaction resume sources changed");
        }
        return Ok(false);
    }
    let mut snapshot: pioneer_compaction::OperationSnapshot =
        serde_json::from_str(&record.snapshot)?;
    // Legacy raw-assertion admission retained its complete original plan;
    // frozen admission deliberately stored empty compact/coverage vectors.
    // Absence of a binding alone never proves the old raw mode.
    let legacy_raw = !snapshot.plan.compact.is_empty() && !snapshot.plan.coverage.is_empty();
    if invalid_completion {
        ensure!(
            legacy_raw || bound.is_some(),
            "compaction resume snapshot is unavailable"
        );
    }
    snapshot.admission.deadline_ms = deadline_ms;
    let snapshot = serde_json::to_string(&snapshot)?;
    let encoded = serde_json::to_string(&next)?;
    ensure!(
        encoded.len() <= SOURCE_PAGE_BYTES,
        "runner state exceeds quantum"
    );
    store.run_serialized_write(|| async {
        let tx = store.connection.begin().await?;
        // Decode the bound header result in this same writer transition. An
        // expired saved Commit is an explicit error, not an ordinary stale CAS.
        super::compaction_source_projection::compaction_bound_source_projection(&tx, operation)
            .await.map_err(|error| if invalid_completion {
                error.context("compaction resume snapshot is unavailable")
            } else { error })?;

        // Patch only the new request deadline for InvalidCompletion. Keeping
        // the saved JSON shape also preserves legacy defaults/unknown fields
        // under the admission trigger; Deadline retains its existing rewrite.
        let changed = tx.execute_raw(Statement::from_sql_and_values(
            sea_orm::DbBackend::Postgres,
            r#"UPDATE compaction_operation SET status='running',outcome=NULL,
                    deadline_ms=$2,
                    snapshot=CASE WHEN $10 THEN jsonb_set(snapshot::jsonb, '{admission,deadline_ms}', to_jsonb($2::bigint))::text ELSE $3 END,
                    execution_turn=$6
               WHERE id=$1 AND status='failed' AND outcome IS NOT DISTINCT FROM $9 AND snapshot=$4
               AND (NOT $10 OR (execution_turn IS NOT DISTINCT FROM $11 AND execution_turn<>$6
                   AND EXISTS (SELECT 1 FROM turn old WHERE old.id=compaction_operation.execution_turn AND old.status IN ('failed','completed','blocked'))
                   AND EXISTS (SELECT 1 FROM turn new WHERE new.id=$6 AND new.status='in_progress' AND new.origin='user')))
               AND (EXISTS (SELECT 1 FROM compaction_operation_projection p
                   JOIN compaction_frozen_history h ON h.id=p.manifest_id
                   WHERE p.operation_id=$1 AND h.expired=0 AND h.ready=1
                     AND h.message_count>=0 AND h.import_count>=0
                     AND h.next_ordinal=h.message_count AND h.next_import=h.import_count
                     AND h.identity_sha256=p.identity_sha256 AND h.imports_sha256=p.imports_sha256
                     AND h.import_count=p.import_count)
                 OR ($8 AND NOT EXISTS (SELECT 1 FROM compaction_operation_projection WHERE operation_id=$1)))
               AND EXISTS (SELECT 1 FROM compaction_runner_state s WHERE s.operation_id=$1 AND s.generation=$5)
               AND (NOT $10 OR EXISTS (SELECT 1 FROM compaction_runner_plan p WHERE p.operation_id=$1 AND p.ready=1))
               AND EXISTS (SELECT 1 FROM compaction_checkpoint p WHERE p.id=$7
                   AND p.operation_id=$1 AND p.owner=compaction_operation.owner AND p.status IN ('candidate','retained')
                   AND (NOT $10 OR length(p.summary)>0))
               AND EXISTS (SELECT 1 FROM compaction_context c JOIN turn t ON t.thread_id=c.thread_id
                   WHERE c.owner=compaction_operation.owner AND c.head IS NOT DISTINCT FROM compaction_operation.expected_head
                   AND t.id=$6 AND t.message_deleted_at IS NULL AND t.status NOT IN ('interrupted','cancelled'))
               AND NOT EXISTS (SELECT 1 FROM compaction_execution_stop x
                   WHERE x.owner=compaction_operation.owner AND x.turn_id IN (compaction_operation.execution_turn,$6))
               AND NOT EXISTS (SELECT 1 FROM turn t WHERE t.id=compaction_operation.execution_turn
                   AND (t.message_deleted_at IS NOT NULL OR t.status IN ('interrupted','cancelled')))"#,
            [operation.into(), i64::try_from(deadline_ms)?.into(), snapshot.clone().into(),
             record.snapshot.clone().into(), i64::try_from(state.generation)?.into(),
             execution_turn.into(), state.previous_checkpoint.clone().into(), legacy_raw.into(),
             record.outcome.clone().into(), invalid_completion.into(), record.execution_turn.clone().into()],
        )).await?.rows_affected();
        if changed == 0 { tx.rollback().await?; return Ok(false); }
        update_runner_state(i64::try_from(next.generation)?, encoded.clone())
            .filter(compaction_runner_state::Column::OperationId.eq(operation))
            .exec(&tx).await?;
        tx.commit().await?;
        Ok(true)
    }).await
}

/// A late cleanup from the previous request must not terminate a resumed request.
/// One bounded conditional write inherits the caller's serialized writer scope.
pub(crate) async fn compaction_finish_execution<C: ConnectionTrait>(
    db: &C,
    operation: &str,
    execution_turn: Option<&str>,
    deadline_ms: u64,
    status: &str,
    outcome: &str,
) -> Result<bool> {
    ensure!(
        matches!(
            (status, outcome),
            ("failed", "deadline") | ("cancelled", "cancelled")
        ),
        "invalid reconciliation reason"
    );
    let changed = db
        .execute_raw(Statement::from_sql_and_values(
            sea_orm::DbBackend::Postgres,
            "UPDATE compaction_operation SET status=$4,outcome=$5 WHERE id=$1 AND status='running'
         AND deadline_ms=$2 AND ($3::text IS NULL OR execution_turn=$3)",
            [
                operation.into(),
                i64::try_from(deadline_ms)?.into(),
                execution_turn.into(),
                status.into(),
                outcome.into(),
            ],
        ))
        .await?;
    Ok(changed.rows_affected() > 0)
}

/// Shared preparation for foreground reconciliation and pending repair. Decode
/// and encode happen outside the writer; the caller fences every source value.
pub(super) fn reconciled_terminal_runner(
    state: &RunnerState,
    status: &str,
    outcome: Option<&str>,
) -> Result<RunnerState> {
    if status == "running"
        || status == "completed"
        || matches!(
            state.phase,
            RunnerPhase::Failed { .. } | RunnerPhase::Applied { .. }
        )
    {
        return Ok(state.clone());
    }
    let kind = if status == "cancelled" {
        FailureKind::Cancelled
    } else if outcome == Some("deadline") {
        FailureKind::Deadline
    } else {
        FailureKind::Permanent
    };
    state.terminate(kind)
}

/// Complete the state record after a control-plane terminal fence. No
/// attempt can advance past that fence. Preparation reads one bounded row;
/// the write revalidates its generation and durable terminal classification.
/// The internal bool reports the CAS outcome, independently of the state DTO.
pub(crate) async fn compaction_reconcile_runner_state(
    store: &CrudStore,
    operation: &str,
) -> Result<(Option<RunnerState>, bool)> {
    use sea_orm::QuerySelect;
    let row = compaction_runner_state::Entity::find_by_id(operation)
        .inner_join(compaction_operation::Entity)
        .select_only()
        .column_as(
            Expr::col((
                compaction_runner_state::Entity,
                compaction_runner_state::Column::State,
            ))
            .cast_as("text"),
            "state",
        )
        .column_as(
            Expr::col((
                compaction_operation::Entity,
                compaction_operation::Column::Status,
            )),
            "status",
        )
        .column_as(
            Expr::col((
                compaction_operation::Entity,
                compaction_operation::Column::Outcome,
            )),
            "outcome",
        )
        .into_tuple::<(String, String, Option<String>)>()
        .one(&store.connection)
        .await?;
    let Some((state, status, outcome)) = row else {
        return Ok((None, false));
    };
    let state: RunnerState = serde_json::from_str(&state)?;
    let terminal = reconciled_terminal_runner(&state, &status, outcome.as_deref())?;
    if terminal == state {
        return Ok((Some(state), false));
    }
    let encoded = serde_json::to_string(&terminal)?;
    ensure!(
        encoded.len() <= SOURCE_PAGE_BYTES,
        "runner state exceeds quantum"
    );
    let changed =
        update_runner_state(i64::try_from(terminal.generation)?, encoded)
            .filter(
                Expr::col(compaction_runner_state::Column::OperationId)
                    .eq(Expr::Value(operation.into()))
                    .and(
                        Expr::col(compaction_runner_state::Column::Generation)
                            .eq(Expr::Value(i64::try_from(state.generation)?.into())),
                    )
                    .and(Expr::exists(
                        Query::select()
                            .expr(Expr::val(1_i64))
                            .from_as(compaction_operation::Entity, "o")
                            .and_where(
                                Expr::col(("o", compaction_operation::Column::Id))
                                    .eq(Expr::Value(operation.into()))
                                    .and(
                                        Expr::col(("o", compaction_operation::Column::Status))
                                            .eq(Expr::Value(status.into())),
                                    )
                                    .and(null_safe_eq(
                                        Expr::col(("o", compaction_operation::Column::Outcome)),
                                        Expr::Value(outcome.into()),
                                    ))
                                    .and(
                                        Expr::col(("o", compaction_operation::Column::Status))
                                            .is_in(["cancelled", "failed", "stale"]),
                                    ),
                            )
                            .to_owned(),
                    )),
            )
            .exec(&store.connection)
            .await?;
    if changed.rows_affected == 1 {
        return Ok((Some(terminal), true));
    }
    // A concurrent reconciler may have won the same idempotent transition.
    Ok((store.compaction_runner_state(operation).await?, false))
}

pub(crate) async fn compaction_manifest_page<C: ConnectionTrait>(
    db: &C,
    operation: &str,
    reference_only: bool,
    unit: u64,
    source_offset: u32,
) -> Result<Vec<ManifestEntry>> {
    use sea_orm::{ColumnTrait, QueryOrder, QuerySelect};
    #[derive(sea_orm::FromQueryResult)]
    struct ManifestSourceRow {
        ordinal: i64,
        unit_ordinal: i64,
        reference_only: i64,
        source_thread: String,
        source_scope: String,
        source_id: String,
        source_version: String,
    }
    let rows = compaction_manifest::Entity::find()
        .select_only()
        .columns([
            compaction_manifest::Column::Ordinal,
            compaction_manifest::Column::UnitOrdinal,
            compaction_manifest::Column::ReferenceOnly,
            compaction_manifest::Column::SourceThread,
            compaction_manifest::Column::SourceScope,
            compaction_manifest::Column::SourceId,
            compaction_manifest::Column::SourceVersion,
        ])
        .filter(compaction_manifest::Column::OperationId.eq(operation))
        .filter(compaction_manifest::Column::ReferenceOnly.eq(reference_only as i64))
        .filter(compaction_manifest::Column::UnitOrdinal.gte(i64::try_from(unit)?))
        .order_by_asc(compaction_manifest::Column::UnitOrdinal)
        .order_by_asc(compaction_manifest::Column::Ordinal)
        .limit(SOURCE_PAGE_ROWS)
        .offset(u64::from(source_offset))
        .into_model::<ManifestSourceRow>()
        .all(db)
        .await?;

    rows.into_iter()
        .map(|row| {
            Ok(ManifestEntry {
                ordinal: u64::try_from(row.ordinal)?,
                unit: u64::try_from(row.unit_ordinal)?,
                reference_only: row.reference_only != 0,
                thread_id: row.source_thread,
                source: SourceRef {
                    scope: row.source_scope,
                    id: row.source_id,
                    version: row.source_version,
                },
            })
        })
        .collect()
}
/// Preparation uses immutable values only. Generation, counters, ownership,
/// deadline and running status are revalidated atomically with candidate save.
pub(crate) async fn compaction_runner_transition(
    store: &CrudStore,
    operation: &str,
    expected: u64,
    next: &RunnerState,
    candidate: Option<&Checkpoint>,
) -> Result<bool> {
    ensure!(
        next.generation
            == expected
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("generation overflow"))?,
        "invalid state generation"
    );
    ensure!(
        !matches!(next.phase, RunnerPhase::Applied { .. })
            && next.retries <= 2
            && next.overflow_rejections <= 4
            // Total claims include rejected overflows. Preserve the independent
            // ordinary allowance, rejecting inconsistent counters before subtracting.
            && next.portion_attempts.checked_sub(next.overflow_rejections)
                .is_some_and(|ordinary| ordinary <= 3)
            && next.corrections <= 1,
        "invalid state transition"
    );
    let state = serde_json::to_string(next)?;
    ensure!(
        state.len() <= SOURCE_PAGE_BYTES,
        "runner state exceeds quantum"
    );
    let mut candidate_model = None;
    let mut coverage_models = Vec::new();
    let mut identity_query = None;
    if let Some(cp) = candidate {
        ensure!(
            cp.operation_id == operation
                && cp.coverage.len() <= CHECKPOINT_SOURCE_LIMIT
                && cp.format_version == 1,
            "invalid candidate scope or quantum"
        );
        ensure!(
            matches!(&next.phase,RunnerPhase::Candidate{checkpoint,..} if checkpoint == &cp.id),
            "candidate/state identity mismatch"
        );
        // Token cap bounds the answer; accommodate even long whitespace tokens.
        ensure!(
            cp.summary.len() <= 13_107 * 128 && !cp.summary.trim().is_empty(),
            "invalid candidate text"
        );
        let identity = checkpoint_identity(cp)?;
        candidate_model = Some(compaction_checkpoint::ActiveModel {
            proof_version: sea_orm::Set(0),
            id: sea_orm::Set(cp.id.clone()),
            operation_id: sea_orm::Set(operation.to_owned()),
            owner: sea_orm::Set(cp.owner.clone()),
            previous: sea_orm::Set(cp.previous.clone()),
            portion: sea_orm::Set(i64::try_from(next.attempts)?),
            summary: sea_orm::Set(cp.summary.clone()),
            selection: sea_orm::Set(serde_json::to_value(&cp.selection)?),
            projection_version: sea_orm::Set(i64::try_from(cp.projection_version)?),
            format_version: sea_orm::Set(1),
            identity_sha256: sea_orm::Set(identity.clone()),
            status: sea_orm::Set("candidate".to_owned()),
        });
        identity_query = Some(
            compaction_checkpoint::Entity::find()
                .select_only()
                .join(
                    JoinType::InnerJoin,
                    compaction_checkpoint::Entity::belongs_to(compaction_operation::Entity)
                        .from(compaction_checkpoint::Column::OperationId)
                        .to(compaction_operation::Column::Id)
                        .into(),
                )
                .expr(Expr::col((
                    compaction_checkpoint::Entity,
                    compaction_checkpoint::Column::Id,
                )))
                .filter(
                    Expr::col((
                        compaction_checkpoint::Entity,
                        compaction_checkpoint::Column::Id,
                    ))
                    .eq(Expr::Value(cp.id.clone().into()))
                    .and(
                        Expr::col((
                            compaction_checkpoint::Entity,
                            compaction_checkpoint::Column::IdentitySha256,
                        ))
                        .eq(Expr::Value(identity.into())),
                    )
                    .and(
                        Expr::col((
                            compaction_checkpoint::Entity,
                            compaction_checkpoint::Column::OperationId,
                        ))
                        .eq(Expr::Value(operation.into())),
                    )
                    .and(
                        Expr::col((
                            compaction_checkpoint::Entity,
                            compaction_checkpoint::Column::Owner,
                        ))
                        .eq(Expr::col((
                            compaction_operation::Entity,
                            compaction_operation::Column::Owner,
                        ))),
                    )
                    .and(
                        Expr::col((
                            compaction_checkpoint::Entity,
                            compaction_checkpoint::Column::ProjectionVersion,
                        ))
                        .eq(json_text(
                            Expr::col((
                                compaction_operation::Entity,
                                compaction_operation::Column::Snapshot,
                            )),
                            &["projection_version"],
                        )
                        .cast_as("bigint")),
                    )
                    .and(
                        json_text(
                            Expr::col((
                                compaction_checkpoint::Entity,
                                compaction_checkpoint::Column::Selection,
                            )),
                            &["transport"],
                        )
                        .eq(json_text(
                            Expr::col((
                                compaction_operation::Entity,
                                compaction_operation::Column::Snapshot,
                            )),
                            &["admission", "selection", "transport"],
                        )),
                    )
                    .and(
                        json_text(
                            Expr::col((
                                compaction_checkpoint::Entity,
                                compaction_checkpoint::Column::Selection,
                            )),
                            &["instance"],
                        )
                        .eq(json_text(
                            Expr::col((
                                compaction_operation::Entity,
                                compaction_operation::Column::Snapshot,
                            )),
                            &["admission", "selection", "instance"],
                        )),
                    )
                    .and(
                        json_text(
                            Expr::col((
                                compaction_checkpoint::Entity,
                                compaction_checkpoint::Column::Selection,
                            )),
                            &["model"],
                        )
                        .eq(json_text(
                            Expr::col((
                                compaction_operation::Entity,
                                compaction_operation::Column::Snapshot,
                            )),
                            &["admission", "selection", "model"],
                        )),
                    )
                    .and(null_safe_eq(
                        json_text(
                            Expr::col((
                                compaction_checkpoint::Entity,
                                compaction_checkpoint::Column::Selection,
                            )),
                            &["effort"],
                        ),
                        json_text(
                            Expr::col((
                                compaction_operation::Entity,
                                compaction_operation::Column::Snapshot,
                            )),
                            &["admission", "selection", "effort"],
                        ),
                    )),
                ),
        );
        coverage_models = cp
            .coverage
            .iter()
            .map(|source| compaction_coverage::ActiveModel {
                checkpoint_id: sea_orm::Set(cp.id.clone()),
                source_scope: sea_orm::Set(source.scope.clone()),
                source_id: sea_orm::Set(source.id.clone()),
                source_version: sea_orm::Set(source.version.clone()),
            })
            .collect::<Vec<_>>();
    }
    let observation_model = next
        .observation
        .as_ref()
        .map(|observation| -> Result<_> {
            ensure!(
                observation.number == next.attempts,
                "attempt observation identity mismatch"
            );
            Ok(compaction_attempt_observation::ActiveModel {
                operation_id: sea_orm::Set(operation.to_owned()),
                attempt: sea_orm::Set(i64::try_from(observation.number)?),
                observation: sea_orm::Set(serde_json::to_value(observation)?),
            })
        })
        .transpose()?;
    let basis_query =
        candidate.map(|cp| {
            compaction_runner_state::Entity::find()
                .select_only()
                .expr(Expr::col(compaction_runner_state::Column::OperationId))
                .filter(
                    Expr::col(compaction_runner_state::Column::OperationId)
                        .eq(Expr::Value(operation.into()))
                        .and(Expr::col(compaction_runner_state::Column::Generation).eq(
                            Expr::Value(i64::try_from(expected).unwrap_or(i64::MAX).into()),
                        ))
                        .and(null_safe_eq(
                            json_text(
                                Expr::col(compaction_runner_state::Column::State),
                                &["previous_checkpoint"],
                            ),
                            Expr::Value(cp.previous.clone().into()),
                        )),
                )
        });
    let (status, outcome) = match &next.phase {
        RunnerPhase::Failed {
            kind: FailureKind::Cancelled,
        } => ("cancelled", Some("cancelled")),
        RunnerPhase::Failed { .. } => ("failed", Some("runner_failed")),
        _ => ("running", None),
    };
    store
        .run_serialized_write(|| async {
            let txn = store.connection.begin().await?;
            if let Some(query) = &basis_query {
                if query
                    .clone()
                    .into_tuple::<String>()
                    .one(&txn)
                    .await?
                    .is_none()
                {
                    txn.rollback().await?;
                    return Ok(false);
                }
            }
            let updated = update_runner_state(i64::try_from(next.generation)?, state.clone())
                .filter(
                    Expr::col(compaction_runner_state::Column::OperationId)
                        .eq(Expr::Value(operation.into()))
                        .and(
                            Expr::col(compaction_runner_state::Column::Generation)
                                .eq(Expr::Value(i64::try_from(expected)?.into())),
                        )
                        .and(Expr::exists(
                            Query::select()
                                .expr(Expr::val(1_i64))
                                .from_as(compaction_operation::Entity, "o")
                                .join_as(
                                    JoinType::InnerJoin,
                                    compaction_runner_plan::Entity,
                                    "p",
                                    Expr::col(("p", compaction_runner_plan::Column::OperationId))
                                        .eq(Expr::col(("o", compaction_operation::Column::Id))),
                                )
                                .and_where(
                                    Expr::col(("o", compaction_operation::Column::Id))
                                        .eq(Expr::Value(operation.into()))
                                        .and(
                                            Expr::col(("o", compaction_operation::Column::Status))
                                                .eq(Expr::val("running")),
                                        )
                                        .and(
                                            Expr::col(("p", compaction_runner_plan::Column::Ready))
                                                .eq(Expr::val(1_i64)),
                                        )
                                        .and(
                                            Expr::col((
                                                "o",
                                                compaction_operation::Column::DeadlineMs,
                                            ))
                                            .eq(
                                                Expr::Value(
                                                    i64::try_from(next.deadline_ms)?.into(),
                                                ),
                                            ),
                                        )
                                        .and(
                                            Expr::col((
                                                "o",
                                                compaction_operation::Column::Attempts,
                                            ))
                                            .lte(Expr::Value(i64::try_from(next.attempts)?.into())),
                                        )
                                        .and(
                                            Expr::col((
                                                "o",
                                                compaction_operation::Column::Attempts,
                                            ))
                                            .add(Expr::val(1_i64))
                                            .gte(Expr::Value(i64::try_from(next.attempts)?.into())),
                                        )
                                        .and(
                                            Expr::col((
                                                "o",
                                                compaction_operation::Column::TransientRetries,
                                            ))
                                            .lte(Expr::Value(i64::from(next.retries).into())),
                                        )
                                        .and(
                                            Expr::col((
                                                "o",
                                                compaction_operation::Column::Correction,
                                            ))
                                            .lte(Expr::Value(i64::from(next.corrections).into())),
                                        ),
                                )
                                .to_owned(),
                        )),
                )
                .exec(&txn)
                .await?;
            if updated.rows_affected != 1 {
                txn.rollback().await?;
                return Ok(false);
            }
            if let Some(model) = &observation_model {
                compaction_attempt_observation::Entity::insert(model.clone())
                    .on_conflict(
                        OnConflict::columns([
                            compaction_attempt_observation::Column::OperationId,
                            compaction_attempt_observation::Column::Attempt,
                        ])
                        .update_column(compaction_attempt_observation::Column::Observation)
                        .to_owned(),
                    )
                    .exec_without_returning(&txn)
                    .await?;
            }
            if let Some(model) = &candidate_model {
                compaction_checkpoint::Entity::insert(model.clone())
                    .on_conflict(
                        OnConflict::columns([
                            compaction_checkpoint::Column::OperationId,
                            compaction_checkpoint::Column::Portion,
                        ])
                        .do_nothing()
                        .to_owned(),
                    )
                    .exec_without_returning(&txn)
                    .await?;
            }
            for model in &coverage_models {
                compaction_coverage::Entity::insert(model.clone())
                    .on_conflict(OnConflict::new().do_nothing().to_owned())
                    .exec_without_returning(&txn)
                    .await?;
            }
            if let Some(cp) = candidate {
                let portion = i64::try_from(next.attempts)?;
                let next_portion = portion
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("candidate portion overflow"))?;
                compaction_operation::Entity::update_many()
                    .col_expr(
                        compaction_operation::Column::NextPortion,
                        Expr::val(next_portion),
                    )
                    .filter(compaction_operation::Column::Id.eq(&cp.operation_id))
                    .filter(compaction_operation::Column::NextPortion.lt(next_portion))
                    .exec(&txn)
                    .await?;
            }

            if let Some(query) = &identity_query {
                ensure!(
                    query
                        .clone()
                        .into_tuple::<String>()
                        .one(&txn)
                        .await?
                        .is_some(),
                    "candidate identity conflict"
                );
            }
            compaction_operation::Entity::update_many()
                .col_expr(
                    compaction_operation::Column::Status,
                    Expr::Value(status.into()),
                )
                .col_expr(
                    compaction_operation::Column::Outcome,
                    Expr::Value(outcome.map(str::to_owned).into()),
                )
                .col_expr(
                    compaction_operation::Column::Attempts,
                    Expr::Value(i64::try_from(next.attempts)?.into()),
                )
                .col_expr(
                    compaction_operation::Column::TransientRetries,
                    Expr::Value(i64::from(next.retries).into()),
                )
                .col_expr(
                    compaction_operation::Column::Correction,
                    Expr::Value(i64::from(next.corrections).into()),
                )
                .filter(
                    Expr::col(compaction_operation::Column::Id).eq(Expr::Value(operation.into())),
                )
                .exec(&txn)
                .await?;
            txn.commit().await?;
            Ok(true)
        })
        .await
}

/// Prepare only the exact predecessor chain in bounded one-row quanta.
/// `retained` rows are invisible in live_sources while their operation is
/// running. The final operation/head CAS makes the whole prepared chain
/// visible atomically. Generation checks prevent a stale preparer from
/// publishing; cancellation/restart may safely leave invisible markers.
pub(crate) async fn prepare_checkpoint_ancestry<C: ConnectionTrait>(
    db: &C,
    operation: &str,
    checkpoint: &str,
    generation: u64,
) -> Result<()> {
    let mut cursor = Some(checkpoint.to_owned());
    let mut visited = std::collections::BTreeSet::new();
    while let Some(id) = cursor.take() {
        ensure!(visited.insert(id.clone()), "cyclic candidate ancestry");
        let (source_operation, previous) = compaction_checkpoint::Entity::find_by_id(id.clone())
            .select_only()
            .column(compaction_checkpoint::Column::OperationId)
            .column(compaction_checkpoint::Column::Previous)
            .into_tuple::<(String, Option<String>)>()
            .one(db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("candidate ancestry is missing"))?;
        if source_operation != operation {
            break;
        }
        cursor = previous;
        compaction_checkpoint::Entity::update_many()
            .col_expr(compaction_checkpoint::Column::Status, Expr::val("retained"))
            .filter(
                Expr::col(compaction_checkpoint::Column::Id)
                    .eq(Expr::Value(id.into()))
                    .and(
                        Expr::col(compaction_checkpoint::Column::OperationId)
                            .eq(Expr::Value(operation.into())),
                    )
                    .and(
                        Expr::col(compaction_checkpoint::Column::Status).eq(Expr::val("candidate")),
                    )
                    .and(Expr::exists(
                        Query::select()
                            .expr(Expr::val(1_i64))
                            .from_as(compaction_operation::Entity, "o")
                            .join_as(
                                JoinType::InnerJoin,
                                compaction_runner_state::Entity,
                                "s",
                                Expr::col(("s", compaction_runner_state::Column::OperationId))
                                    .eq(Expr::col(("o", compaction_operation::Column::Id))),
                            )
                            .and_where(
                                Expr::col(("o", compaction_operation::Column::Id))
                                    .eq(Expr::Value(operation.into()))
                                    .and(
                                        Expr::col(("o", compaction_operation::Column::Status))
                                            .eq(Expr::val("running")),
                                    )
                                    .and(
                                        Expr::col((
                                            "s",
                                            compaction_runner_state::Column::Generation,
                                        ))
                                        .eq(Expr::Value(i64::try_from(generation)?.into())),
                                    )
                                    .and(
                                        json_text(
                                            Expr::col((
                                                "s",
                                                compaction_runner_state::Column::State,
                                            )),
                                            &["phase", "Commit", "checkpoint"],
                                        )
                                        .eq(Expr::Value(checkpoint.into())),
                                    ),
                            )
                            .to_owned(),
                    )),
            )
            .exec(db)
            .await?;
    }
    Ok(())
}

/// Request-local validation of the saved candidate and its immutable coverage.
/// Live source revisions deliberately do not participate after summarization.
#[derive(Debug)]
struct PreparedRunnerPublication {
    operation: String,
    checkpoint: String,
    generation: u64,
    expected_head: Option<String>,
    identity_current: bool,
    coverage_exact: bool,
}

async fn prepare_runner_publication(
    store: &CrudStore,
    operation: &str,
    checkpoint: &str,
    generation: u64,
    expected_head: Option<&str>,
) -> Result<PreparedRunnerPublication> {
    let snapshot = store.connection.begin_read().await?;
    // Validate the saved candidate, not the current canonical history. Writer
    // publication rechecks this operation identity and generation atomically.
    let identity = snapshot
        .query_one_raw(Statement::from_sql_and_values(
            sea_orm::DbBackend::Postgres,
            r#"
SELECT o.owner AS operation_owner,o.expected_head,
 c.workspace_id,p.operation_id AS candidate_operation,p.owner AS candidate_owner,
 s.generation AS runner_generation,
 s.state #>> '{phase,Commit,checkpoint}' AS runner_checkpoint
FROM compaction_operation o
LEFT JOIN compaction_context c ON c.owner=o.owner
LEFT JOIN compaction_checkpoint p ON p.id=$2
LEFT JOIN compaction_runner_state s ON s.operation_id=o.id
WHERE o.id=$1
"#,
            [operation.into(), checkpoint.into()],
        ))
        .await?;
    let expected_generation = i64::try_from(generation)?;
    let identity_current = if let Some(identity) = identity {
        let operation_owner: String = identity.try_get("", "operation_owner")?;
        let stored_head: Option<String> = identity.try_get("", "expected_head")?;
        let workspace: Option<String> = identity.try_get("", "workspace_id")?;
        let candidate_operation: Option<String> = identity.try_get("", "candidate_operation")?;
        let candidate_owner: Option<String> = identity.try_get("", "candidate_owner")?;
        let runner_generation: Option<i64> = identity.try_get("", "runner_generation")?;
        let runner_checkpoint: Option<String> = identity.try_get("", "runner_checkpoint")?;
        workspace.is_some()
            && candidate_operation.as_deref() == Some(operation)
            && candidate_owner.as_deref() == Some(operation_owner.as_str())
            && stored_head.as_deref() == expected_head
            && runner_generation == Some(expected_generation)
            && runner_checkpoint.as_deref() == Some(checkpoint)
    } else {
        false
    };

    let coverage_exact = if identity_current {
        #[cfg(any(test, feature = "test-support"))]
        pause_publication_test_hook(
            store.connection.runtime_identity(),
            operation,
            PublicationTestPause::ReaderPreflight,
        )
        .await;
        compaction_runner_coverage_exact(&snapshot, operation).await?
    } else {
        false
    };
    // Explicitly end the read snapshot before queuing for the writer. This
    // also releases the maintenance-read permit carried by the scoped store.
    snapshot.commit().await?;
    // Stale candidates do not publish. Validate the graph only after exact
    // saved coverage validation, with the read snapshot released.
    if identity_current && coverage_exact {
        super::checkpoint_proofs::prepare_graph(store, checkpoint).await?;
    }
    Ok(PreparedRunnerPublication {
        operation: operation.to_owned(),
        checkpoint: checkpoint.to_owned(),
        generation,
        expected_head: expected_head.map(str::to_owned),
        identity_current,
        coverage_exact,
    })
}

async fn compaction_runner_coverage_exact<C: ConnectionTrait>(
    db: &C,
    operation: &str,
) -> Result<bool> {
    #[cfg(test)]
    record_publication_heavy_check(operation, false);
    // Exact coverage: every selected record has a fully read version, and no
    // reference-only or unselected source has acquired coverage.
    let missing = compaction_manifest::Entity::find()
        .select_only()
        .expr(Expr::col((
            compaction_manifest::Entity,
            compaction_manifest::Column::Ordinal,
        )))
        .filter(
            Expr::col((
                compaction_manifest::Entity,
                compaction_manifest::Column::OperationId,
            ))
            .eq(Expr::Value(operation.into()))
            .and(
                Expr::col((
                    compaction_manifest::Entity,
                    compaction_manifest::Column::ReferenceOnly,
                ))
                .eq(Expr::val(0_i64)),
            )
            .and(
                Expr::exists(
                    Query::select()
                        .expr(Expr::val(1_i64))
                        .from_as(compaction_coverage::Entity, "c")
                        .join_as(
                            JoinType::InnerJoin,
                            compaction_checkpoint::Entity,
                            "p",
                            Expr::col(("p", compaction_checkpoint::Column::Id))
                                .eq(Expr::col(("c", compaction_coverage::Column::CheckpointId))),
                        )
                        .and_where(
                            Expr::col(("p", compaction_checkpoint::Column::OperationId))
                                .eq(Expr::col((
                                    compaction_manifest::Entity,
                                    compaction_manifest::Column::OperationId,
                                )))
                                .and(
                                    Expr::col(("c", compaction_coverage::Column::SourceScope)).eq(
                                        Expr::col((
                                            compaction_manifest::Entity,
                                            compaction_manifest::Column::SourceScope,
                                        )),
                                    ),
                                )
                                .and(Expr::col(("c", compaction_coverage::Column::SourceId)).eq(
                                    Expr::col((
                                        compaction_manifest::Entity,
                                        compaction_manifest::Column::SourceId,
                                    )),
                                ))
                                .and(
                                    Expr::col(("c", compaction_coverage::Column::SourceVersion))
                                        .eq(Expr::col((
                                            compaction_manifest::Entity,
                                            compaction_manifest::Column::SourceVersion,
                                        ))),
                                ),
                        )
                        .to_owned(),
                )
                .not(),
            ),
        )
        .limit(1)
        .into_tuple::<i64>()
        .one(db)
        .await?;
    let extra = compaction_coverage::Entity::find()
        .select_only()
        .join(
            JoinType::InnerJoin,
            compaction_coverage::Entity::belongs_to(compaction_checkpoint::Entity)
                .from(compaction_coverage::Column::CheckpointId)
                .to(compaction_checkpoint::Column::Id)
                .into(),
        )
        .expr(Expr::col((
            compaction_coverage::Entity,
            compaction_coverage::Column::SourceId,
        )))
        .filter(
            Expr::col((
                compaction_checkpoint::Entity,
                compaction_checkpoint::Column::OperationId,
            ))
            .eq(Expr::Value(operation.into()))
            .and(
                Expr::exists(
                    Query::select()
                        .expr(Expr::val(1_i64))
                        .from_as(compaction_manifest::Entity, "m")
                        .and_where(
                            Expr::col(("m", compaction_manifest::Column::OperationId))
                                .eq(Expr::col((
                                    compaction_checkpoint::Entity,
                                    compaction_checkpoint::Column::OperationId,
                                )))
                                .and(
                                    Expr::col(("m", compaction_manifest::Column::ReferenceOnly))
                                        .eq(Expr::val(0_i64)),
                                )
                                .and(
                                    Expr::col((
                                        compaction_coverage::Entity,
                                        compaction_coverage::Column::SourceScope,
                                    ))
                                    .eq(Expr::col(("m", compaction_manifest::Column::SourceScope))),
                                )
                                .and(
                                    Expr::col((
                                        compaction_coverage::Entity,
                                        compaction_coverage::Column::SourceId,
                                    ))
                                    .eq(Expr::col(("m", compaction_manifest::Column::SourceId))),
                                )
                                .and(
                                    Expr::col((
                                        compaction_coverage::Entity,
                                        compaction_coverage::Column::SourceVersion,
                                    ))
                                    .eq(Expr::col((
                                        "m",
                                        compaction_manifest::Column::SourceVersion,
                                    ))),
                                ),
                        )
                        .to_owned(),
                )
                .not(),
            ),
        )
        .limit(1)
        .into_tuple::<String>()
        .one(db)
        .await?;
    Ok(missing.is_none() && extra.is_none())
}

/// Validate saved coverage outside writer capacity, then atomically publish.
/// A finished summary describes the versions actually consumed; subsequent
/// canonical edits/deletes and unrelated work cannot invalidate that result.
/// Candidate/manifest metadata is owned by this runner generation. Operation
/// identity, generation, cancellation and the target head can change while
/// preparing; the writer rechecks those exact control fields before commit.
pub(crate) async fn compaction_apply_runner(
    store: &CrudStore,
    operation: &str,
    state: &RunnerState,
    expected_head: Option<&str>,
) -> Result<super::compaction::CommitOutcome> {
    let RunnerPhase::Commit { checkpoint } = &state.phase else {
        anyhow::bail!("runner has no commit candidate")
    };
    let applied = state.applied(checkpoint)?;
    let encoded = serde_json::to_string(&applied)?;
    store
        .prepare_checkpoint_ancestry(operation, checkpoint, state.generation)
        .await?;
    let prepared = store
        .run_scoped_database_quantum(|| {
            prepare_runner_publication(
                store,
                operation,
                checkpoint,
                state.generation,
                expected_head,
            )
        })
        .await?;
    #[cfg(any(test, feature = "test-support"))]
    pause_publication_test_hook(
        store.connection.runtime_identity(),
        operation,
        PublicationTestPause::BeforeWriter,
    )
    .await;
    ensure!(
        prepared.operation.as_str() == operation
            && prepared.checkpoint.as_str() == checkpoint.as_str()
            && prepared.generation == state.generation
            && prepared.expected_head.as_deref() == expected_head,
        "runner publication preflight identity mismatch"
    );
    store
        .run_serialized_write(|| async {
            let txn = store.connection.begin().await?;
            #[cfg(test)]
            let _writer_test_guard = PublicationWriterTestGuard::enter(operation);
            let row = compaction_operation::Entity::find()
                .select_only()
                .join(
                    JoinType::InnerJoin,
                    compaction_operation::Entity::belongs_to(compaction_checkpoint::Entity)
                        .from(compaction_operation::Column::Id)
                        .to(compaction_checkpoint::Column::OperationId)
                        .on_condition(|_, _| {
                            sea_orm::Condition::all().add(
                                Expr::col((
                                    compaction_checkpoint::Entity,
                                    compaction_checkpoint::Column::Owner,
                                ))
                                .eq(Expr::col((
                                    compaction_operation::Entity,
                                    compaction_operation::Column::Owner,
                                ))),
                            )
                        })
                        .into(),
                )
                .expr(Expr::col((
                    compaction_operation::Entity,
                    compaction_operation::Column::Status,
                )))
                .filter(
                    Expr::col((
                        compaction_operation::Entity,
                        compaction_operation::Column::Id,
                    ))
                    .eq(Expr::Value(operation.into()))
                    .and(
                        Expr::col((
                            compaction_checkpoint::Entity,
                            compaction_checkpoint::Column::Id,
                        ))
                        .eq(Expr::Value(checkpoint.clone().into())),
                    )
                    .and(null_safe_eq(
                        Expr::col((
                            compaction_operation::Entity,
                            compaction_operation::Column::ExpectedHead,
                        )),
                        Expr::Value(expected_head.map(str::to_owned).into()),
                    )),
                )
                .into_tuple::<String>()
                .one(&txn)
                .await?;
            let Some(row) = row else {
                txn.rollback().await?;
                return Ok(super::compaction::CommitOutcome::Stale);
            };
            let status = row;
            if status == "completed" {
                let exact = compaction_checkpoint::Entity::find()
                    .select_only()
                    .expr(Expr::col(compaction_checkpoint::Column::Id))
                    .filter(
                        Expr::col(compaction_checkpoint::Column::Id)
                            .eq(Expr::Value(checkpoint.clone().into()))
                            .and(
                                Expr::col(compaction_checkpoint::Column::Status)
                                    .eq(Expr::val("applied")),
                            ),
                    )
                    .into_tuple::<String>()
                    .one(&txn)
                    .await?;
                txn.rollback().await?;
                return Ok(if exact.is_some() {
                    super::compaction::CommitOutcome::AlreadyApplied
                } else {
                    super::compaction::CommitOutcome::Stale
                });
            }
            if status != "running" {
                txn.rollback().await?;
                return Ok(super::compaction::CommitOutcome::Cancelled);
            }
            let interrupted = compaction_operation::Entity::find()
                .select_only()
                .join(
                    JoinType::InnerJoin,
                    compaction_operation::Entity::belongs_to(compaction_context::Entity)
                        .from(compaction_operation::Column::Owner)
                        .to(compaction_context::Column::Owner)
                        .into(),
                )
                .expr(Expr::col((
                    compaction_operation::Entity,
                    compaction_operation::Column::Id,
                )))
                .filter(
                    Expr::col((
                        compaction_operation::Entity,
                        compaction_operation::Column::Id,
                    ))
                    .eq(Expr::Value(operation.into()))
                    .and(
                        Expr::col((
                            compaction_operation::Entity,
                            compaction_operation::Column::ExecutionTurn,
                        ))
                        .is_not_null(),
                    )
                    .and(
                        Expr::exists(
                            Query::select()
                                .expr(Expr::val(1_i64))
                                .from_as(compaction_execution_stop::Entity, "stop")
                                .and_where(
                                    Expr::col(("stop", compaction_execution_stop::Column::Owner))
                                        .eq(Expr::col((
                                            compaction_context::Entity,
                                            compaction_context::Column::Owner,
                                        )))
                                        .and(
                                            Expr::col((
                                                "stop",
                                                compaction_execution_stop::Column::TurnId,
                                            ))
                                            .eq(
                                                Expr::col((
                                                    compaction_operation::Entity,
                                                    compaction_operation::Column::ExecutionTurn,
                                                )),
                                            ),
                                        ),
                                )
                                .to_owned(),
                        )
                        .or(Expr::exists(
                            Query::select()
                                .expr(Expr::val(1_i64))
                                .from_as(turn::Entity, "t")
                                .and_where(
                                    Expr::col(("t", turn::Column::Id))
                                        .eq(Expr::col((
                                            compaction_operation::Entity,
                                            compaction_operation::Column::ExecutionTurn,
                                        )))
                                        .and(
                                            Expr::col(("t", turn::Column::Status))
                                                .is_in(["interrupted", "cancelled"])
                                                .not(),
                                        ),
                                )
                                .to_owned(),
                        )
                        .not()),
                    ),
                )
                .into_tuple::<String>()
                .one(&txn)
                .await?;
            if interrupted.is_some() {
                txn.rollback().await?;
                return Ok(super::compaction::CommitOutcome::Cancelled);
            }

            let generation = compaction_runner_state::Entity::find()
                .select_only()
                .expr(Expr::col(compaction_runner_state::Column::OperationId))
                .filter(
                    Expr::col(compaction_runner_state::Column::OperationId)
                        .eq(Expr::Value(operation.into()))
                        .and(
                            Expr::col(compaction_runner_state::Column::Generation)
                                .eq(Expr::Value(i64::try_from(state.generation)?.into())),
                        )
                        .and(
                            json_text(
                                Expr::col(compaction_runner_state::Column::State),
                                &["phase", "Commit", "checkpoint"],
                            )
                            .eq(Expr::Value(checkpoint.clone().into())),
                        ),
                )
                .into_tuple::<String>()
                .one(&txn)
                .await?;
            if generation.is_none() {
                txn.rollback().await?;
                return Ok(super::compaction::CommitOutcome::Stale);
            }
            if !prepared.identity_current || !prepared.coverage_exact {
                txn.rollback().await?;
                return Ok(super::compaction::CommitOutcome::Stale);
            }
            let proof_ready = compaction_checkpoint::Entity::find_by_id(checkpoint)
                .select_only()
                .column(compaction_checkpoint::Column::Id)
                .filter(compaction_checkpoint::Column::OperationId.eq(operation))
                .filter(compaction_checkpoint::Column::ProofVersion.eq(1))
                .into_tuple::<String>()
                .one(&txn)
                .await?;
            if proof_ready.is_none() {
                txn.rollback().await?;
                return Ok(super::compaction::CommitOutcome::Stale);
            }

            let changed = compaction_context::Entity::update_many()
                .col_expr(
                    compaction_context::Column::Head,
                    Expr::Value(checkpoint.clone().into()),
                )
                .filter(
                    Expr::col(compaction_context::Column::Owner)
                        .eq(Expr::SubQuery(
                            None,
                            Box::new(
                                Query::select()
                                    .expr(Expr::col(compaction_operation::Column::Owner))
                                    .from(compaction_operation::Entity)
                                    .and_where(
                                        Expr::col(compaction_operation::Column::Id)
                                            .eq(Expr::Value(operation.into())),
                                    )
                                    .to_owned()
                                    .into(),
                            ),
                        ))
                        .and(null_safe_eq(
                            Expr::col(compaction_context::Column::Head),
                            Expr::Value(expected_head.map(str::to_owned).into()),
                        ))
                        .and(
                            Expr::col(compaction_context::Column::FormatVersion)
                                .eq(Expr::val(1_i64)),
                        ),
                )
                .exec(&txn)
                .await?;
            if changed.rows_affected != 1 {
                txn.rollback().await?;
                return Ok(super::compaction::CommitOutcome::Stale);
            }
            compaction_checkpoint::Entity::update_many()
                .col_expr(compaction_checkpoint::Column::Status, Expr::val("applied"))
                .filter(
                    Expr::col(compaction_checkpoint::Column::Id)
                        .eq(Expr::Value(checkpoint.clone().into())),
                )
                .exec(&txn)
                .await?;
            compaction_operation::Entity::update_many()
                .col_expr(compaction_operation::Column::Status, Expr::val("completed"))
                .col_expr(compaction_operation::Column::Outcome, Expr::val("applied"))
                .filter(
                    Expr::col(compaction_operation::Column::Id).eq(Expr::Value(operation.into())),
                )
                .exec(&txn)
                .await?;
            update_runner_state(i64::try_from(applied.generation)?, encoded.clone())
                .filter(
                    Expr::col(compaction_runner_state::Column::OperationId)
                        .eq(Expr::Value(operation.into()))
                        .and(
                            Expr::col(compaction_runner_state::Column::Generation)
                                .eq(Expr::Value(i64::try_from(state.generation)?.into())),
                        ),
                )
                .exec(&txn)
                .await?;
            txn.commit().await?;
            Ok(super::compaction::CommitOutcome::Applied)
        })
        .await
}

use sea_orm::QuerySelect;

async fn manifest_source_exists<C: ConnectionTrait, E: EntityTrait>(
    db: &C,
    operation: &str,
    source: &str,
    source_turn: &str,
    source_thread: &str,
    columns: (E::Column, E::Column),
) -> Result<bool> {
    let (id, turn_id) = columns;
    Ok(E::find()
        .select_only()
        .column(id)
        .join(
            JoinType::InnerJoin,
            E::belongs_to(turn::Entity)
                .from(turn_id)
                .to(turn::Column::Id)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            turn::Entity::belongs_to(thread::Entity)
                .from(turn::Column::ThreadId)
                .to(thread::Column::Id)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            thread::Entity::belongs_to(compaction_context::Entity)
                .from(thread::Column::WorkspaceId)
                .to(compaction_context::Column::WorkspaceId)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            compaction_context::Entity::belongs_to(compaction_operation::Entity)
                .from(compaction_context::Column::Owner)
                .to(compaction_operation::Column::Owner)
                .into(),
        )
        .filter(compaction_operation::Column::Id.eq(operation))
        .filter(id.eq(source))
        .filter(turn_id.eq(source_turn))
        .filter(thread::Column::Id.eq(source_thread))
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_some())
}

use sea_orm::{QueryTrait, Statement};

#[cfg(test)]
use pioneer_crud_compaction::manifest::source_validation_statement as compaction_manifest_sources_current_statement;

/// Admission/read-time predicate. Validate raw inputs before consuming them;
/// publication of a completed summary never rechecks canonical source state.
pub(crate) async fn compaction_manifest_sources_current<C: ConnectionTrait>(
    db: &C,
    operation: &str,
) -> Result<bool> {
    #[cfg(test)]
    record_publication_heavy_check(operation, true);
    pioneer_crud_compaction::manifest::sources_current(db, operation).await
}

#[cfg(test)]
#[path = "compaction_manifest_source_lookup_tests.rs"]
mod manifest_source_lookup_tests;
