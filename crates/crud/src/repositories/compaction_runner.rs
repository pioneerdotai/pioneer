use super::compaction::{
    CHECKPOINT_SOURCE_LIMIT, SOURCE_PAGE_BYTES, SOURCE_PAGE_ROWS, checkpoint_identity,
    sqlite_specific_sql,
};
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_compaction::runner::{FailureKind, RunnerPhase, RunnerState};
use pioneer_compaction::{Checkpoint, ModelBudget, SourceRef};
use pioneer_entity::{
    compaction_attempt_observation, compaction_checkpoint, compaction_context, compaction_coverage,
    compaction_execution_stop, compaction_manifest, compaction_operation, compaction_runner_plan,
    compaction_runner_state, thread, turn,
};
use sea_orm::sea_query::{
    Alias, Asterisk, BinOper, Expr, ExprTrait, Func, JoinType, OnConflict, Query,
};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use sea_orm::{ConnectionTrait, TransactionTrait};

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PublicationTestPause {
    ReaderPreflight,
    BeforeWriter,
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
static PUBLICATION_WRITER_FENCE_CHECKS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::BTreeMap<String, usize>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct PublicationTestMetrics {
    coverage_checks: usize,
    manifest_checks: usize,
    heavy_checks_while_writer: usize,
    writer_entries: usize,
    writer_depth: usize,
}

#[cfg(test)]
static PUBLICATION_TEST_METRICS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::BTreeMap<String, PublicationTestMetrics>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
fn reset_publication_test_metrics(operation: &str) {
    PUBLICATION_TEST_METRICS
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(operation.to_owned(), PublicationTestMetrics::default());
}

#[cfg(test)]
fn publication_test_metrics(operation: &str) -> PublicationTestMetrics {
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
struct PublicationWriterTestGuard(String);

#[cfg(test)]
impl PublicationWriterTestGuard {
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

#[cfg(test)]
fn publication_writer_fence_checks(operation: &str) -> usize {
    *PUBLICATION_WRITER_FENCE_CHECKS
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(operation)
        .unwrap_or(&0)
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
async fn pause_publication_test_hook(
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
                .binary(BinOper::Is, Expr::val(Option::<String>::None))
                .not(),
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
) -> Result<()> {
    compaction_operation::Entity::update_many()
        .col_expr(
            compaction_operation::Column::ExecutionTurn,
            Expr::Value(turn.into()),
        )
        .filter(
            Expr::col(compaction_operation::Column::Id)
                .eq(Expr::Value(operation.into()))
                .and(
                    Expr::col(compaction_operation::Column::ExecutionTurn)
                        .binary(BinOper::Is, Expr::val(Option::<String>::None)),
                )
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
    Ok(())
}

pub(crate) async fn compaction_prepare_runner<C: ConnectionTrait>(
    db: &C,
    operation: &str,
    budget: &ModelBudget,
    source_count: u64,
    reference_count: u64,
) -> Result<()> {
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
    db.execute(
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
                    .expr(Expr::Value(descriptor.clone().into()))
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
                        .eq(Expr::Value(descriptor.into())),
                ),
        )
        .into_tuple::<String>()
        .one(db)
        .await?;
    ensure!(exact.is_some(), "runner plan idempotency conflict");
    Ok(())
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
        budget: serde_json::from_str(&row.descriptor)?,
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
            let exact = compaction_manifest::Entity::find_by_id((operation.to_owned(), ordinal))
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
) -> Result<()> {
    ensure!(
        initial.generation == 0
            && initial.attempts == 0
            && initial.retries == 0
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
            compaction_runner_state::Entity::insert(compaction_runner_state::ActiveModel {
                operation_id: sea_orm::Set((operation).to_owned()),
                generation: sea_orm::Set(0_i64),
                state: sea_orm::Set((encoded.clone()).to_owned()),
            })
            .on_conflict(
                OnConflict::columns([compaction_runner_state::Column::OperationId])
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(&txn)
            .await?;
            compaction_runner_plan::Entity::update_many()
                .col_expr(compaction_runner_plan::Column::Ready, Expr::val(1_i64))
                .filter(
                    Expr::col(compaction_runner_plan::Column::OperationId)
                        .eq(Expr::Value(operation.into())),
                )
                .exec(&txn)
                .await?;
            txn.commit().await?;
            Ok(())
        })
        .await
}
pub(crate) async fn compaction_runner_state<C: ConnectionTrait>(
    db: &C,
    operation: &str,
) -> Result<Option<RunnerState>> {
    use pioneer_entity::compaction_runner_state as state;
    use sea_orm::EntityTrait;
    let row = state::Entity::find_by_id(operation).one(db).await?;
    row.map(|row| Ok(serde_json::from_str(&row.state)?))
        .transpose()
}
/// Resume only saved progress on a later admission of the identical plan.
/// Preparation reads operation/state and its bounded manifest page; the short
/// transaction rechecks snapshot, generation, checkpoint, head and Stop fences.
/// This publishes no history. Source freshness can race this preliminary check:
/// the existing runner revalidates before any next portion and at publication.
pub(crate) async fn compaction_resume_deadline(
    store: &CrudStore,
    operation: &str,
    execution_turn: &str,
    deadline_ms: u64,
) -> Result<bool> {
    let Some(record) =
        super::compaction::compaction_operation(&store.connection, operation).await?
    else {
        return Ok(false);
    };
    if record.status != "failed" || record.outcome.as_deref() != Some("deadline") {
        return Ok(false);
    }
    let Some(state) = compaction_runner_state(&store.connection, operation).await? else {
        return Ok(false);
    };
    if !state.can_resume_deadline() || deadline_ms <= state.deadline_ms {
        return Ok(false);
    }
    if !compaction_manifest_sources_current(&store.connection, operation).await? {
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
    let next = state.resume_deadline(deadline_ms, legacy_final)?;
    let mut snapshot: pioneer_compaction::OperationSnapshot =
        serde_json::from_str(&record.snapshot)?;
    snapshot.admission.deadline_ms = deadline_ms;
    let snapshot = serde_json::to_string(&snapshot)?;
    let encoded = serde_json::to_string(&next)?;
    ensure!(
        encoded.len() <= SOURCE_PAGE_BYTES,
        "runner state exceeds quantum"
    );
    store.run_serialized_write(|| async {
        let tx = store.connection.begin().await?;
        let changed = tx.execute_raw(Statement::from_sql_and_values(
            sea_orm::DbBackend::Sqlite,
            r#"UPDATE compaction_operation SET status='running',outcome=NULL,
                    deadline_ms=?2,snapshot=?3,execution_turn=?6
               WHERE id=?1 AND status='failed' AND outcome='deadline' AND snapshot=?4
               AND EXISTS (SELECT 1 FROM compaction_runner_state s WHERE s.operation_id=?1 AND s.generation=?5)
               AND EXISTS (SELECT 1 FROM compaction_checkpoint p WHERE p.id=?7
                   AND p.operation_id=?1 AND p.owner=compaction_operation.owner AND p.status IN ('candidate','retained'))
               AND EXISTS (SELECT 1 FROM compaction_context c JOIN turn t ON t.thread_id=c.thread_id
                   WHERE c.owner=compaction_operation.owner AND c.head IS compaction_operation.expected_head
                   AND t.id=?6 AND t.message_deleted_at IS NULL AND t.status NOT IN ('interrupted','cancelled'))
               AND NOT EXISTS (SELECT 1 FROM compaction_execution_stop x
                   WHERE x.owner=compaction_operation.owner AND x.turn_id IN (compaction_operation.execution_turn,?6))
               AND NOT EXISTS (SELECT 1 FROM turn t WHERE t.id=compaction_operation.execution_turn
                   AND (t.message_deleted_at IS NOT NULL OR t.status IN ('interrupted','cancelled')))"#,
            [operation.into(), i64::try_from(deadline_ms)?.into(), snapshot.clone().into(),
             record.snapshot.clone().into(), i64::try_from(state.generation)?.into(),
             execution_turn.into(), state.previous_checkpoint.clone().into()],
        )).await?.rows_affected();
        if changed == 0 { tx.rollback().await?; return Ok(false); }
        compaction_runner_state::Entity::update_many()
            .col_expr(compaction_runner_state::Column::Generation, Expr::val(i64::try_from(next.generation)?))
            .col_expr(compaction_runner_state::Column::State, Expr::val(encoded.clone()))
            .filter(compaction_runner_state::Column::OperationId.eq(operation))
            .exec(&tx).await?;
        tx.commit().await?;
        Ok(true)
    }).await
}

/// Complete the state record after a control-plane terminal fence. No
/// attempt can advance past that fence. Preparation reads one bounded row;
/// the write revalidates its generation and durable terminal classification.
pub(crate) async fn compaction_reconcile_runner_state(
    store: &CrudStore,
    operation: &str,
) -> Result<Option<RunnerState>> {
    use sea_orm::QuerySelect;
    let row = compaction_runner_state::Entity::find_by_id(operation)
        .inner_join(compaction_operation::Entity)
        .select_only()
        .column(compaction_runner_state::Column::State)
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
        return Ok(None);
    };
    let state: RunnerState = serde_json::from_str(&state)?;
    if status == "running"
        || status == "completed"
        || matches!(
            state.phase,
            RunnerPhase::Failed { .. } | RunnerPhase::Applied { .. }
        )
    {
        return Ok(Some(state));
    }
    let kind = if status == "cancelled" {
        FailureKind::Cancelled
    } else if outcome.as_deref() == Some("deadline") {
        FailureKind::Deadline
    } else {
        FailureKind::Permanent
    };
    let terminal = state.terminate(kind)?;
    let encoded = serde_json::to_string(&terminal)?;
    ensure!(
        encoded.len() <= SOURCE_PAGE_BYTES,
        "runner state exceeds quantum"
    );
    let changed =
        compaction_runner_state::Entity::update_many()
            .col_expr(
                compaction_runner_state::Column::Generation,
                Expr::Value(i64::try_from(terminal.generation)?.into()),
            )
            .col_expr(
                compaction_runner_state::Column::State,
                Expr::Value(encoded.into()),
            )
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
                                    .and(
                                        Expr::col(("o", compaction_operation::Column::Outcome))
                                            .binary(BinOper::Is, Expr::Value(outcome.into())),
                                    )
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
        return Ok(Some(terminal));
    }
    // A concurrent reconciler may have won the same idempotent transition.
    store.compaction_runner_state(operation).await
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
            id: sea_orm::Set(cp.id.clone()),
            operation_id: sea_orm::Set(operation.to_owned()),
            owner: sea_orm::Set(cp.owner.clone()),
            previous: sea_orm::Set(cp.previous.clone()),
            portion: sea_orm::Set(i64::try_from(next.attempts)?),
            summary: sea_orm::Set(cp.summary.clone()),
            selection: sea_orm::Set(serde_json::to_string(&cp.selection)?),
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
                        .eq(Expr::expr(
                            Func::cust(Alias::new("json_extract")).args([
                                Expr::col((
                                    compaction_operation::Entity,
                                    compaction_operation::Column::Snapshot,
                                )),
                                Expr::val("$.projection_version"),
                            ]),
                        )),
                    )
                    .and(
                        Expr::expr(Func::cust(Alias::new("json_extract")).args([
                            Expr::col((
                                compaction_checkpoint::Entity,
                                compaction_checkpoint::Column::Selection,
                            )),
                            Expr::val("$.transport"),
                        ]))
                        .eq(Expr::expr(
                            Func::cust(Alias::new("json_extract")).args([
                                Expr::col((
                                    compaction_operation::Entity,
                                    compaction_operation::Column::Snapshot,
                                )),
                                Expr::val("$.admission.selection.transport"),
                            ]),
                        )),
                    )
                    .and(
                        Expr::expr(Func::cust(Alias::new("json_extract")).args([
                            Expr::col((
                                compaction_checkpoint::Entity,
                                compaction_checkpoint::Column::Selection,
                            )),
                            Expr::val("$.instance"),
                        ]))
                        .eq(Expr::expr(
                            Func::cust(Alias::new("json_extract")).args([
                                Expr::col((
                                    compaction_operation::Entity,
                                    compaction_operation::Column::Snapshot,
                                )),
                                Expr::val("$.admission.selection.instance"),
                            ]),
                        )),
                    )
                    .and(
                        Expr::expr(Func::cust(Alias::new("json_extract")).args([
                            Expr::col((
                                compaction_checkpoint::Entity,
                                compaction_checkpoint::Column::Selection,
                            )),
                            Expr::val("$.model"),
                        ]))
                        .eq(Expr::expr(
                            Func::cust(Alias::new("json_extract")).args([
                                Expr::col((
                                    compaction_operation::Entity,
                                    compaction_operation::Column::Snapshot,
                                )),
                                Expr::val("$.admission.selection.model"),
                            ]),
                        )),
                    )
                    .and(
                        Expr::expr(Func::cust(Alias::new("json_extract")).args([
                            Expr::col((
                                compaction_checkpoint::Entity,
                                compaction_checkpoint::Column::Selection,
                            )),
                            Expr::val("$.effort"),
                        ]))
                        .binary(
                            BinOper::Is,
                            Expr::expr(Func::cust(Alias::new("json_extract")).args([
                                Expr::col((
                                    compaction_operation::Entity,
                                    compaction_operation::Column::Snapshot,
                                )),
                                Expr::val("$.admission.selection.effort"),
                            ])),
                        ),
                    ),
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
                observation: sea_orm::Set(serde_json::to_string(observation)?),
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
                        .and(
                            Expr::expr(Func::cust(Alias::new("json_extract")).args([
                                Expr::col(compaction_runner_state::Column::State),
                                Expr::val("$.previous_checkpoint"),
                            ]))
                            .binary(BinOper::Is, Expr::Value(cp.previous.clone().into())),
                        ),
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
            let updated = compaction_runner_state::Entity::update_many()
                .col_expr(
                    compaction_runner_state::Column::Generation,
                    Expr::Value(i64::try_from(next.generation)?.into()),
                )
                .col_expr(
                    compaction_runner_state::Column::State,
                    Expr::Value(state.clone().into()),
                )
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
                        Expr::col(compaction_checkpoint::Column::Status)
                            .is_in(["candidate", "retained"]),
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
                                        Expr::expr(Func::cust(Alias::new("json_extract")).args([
                                            Expr::col((
                                                "s",
                                                compaction_runner_state::Column::State,
                                            )),
                                            Expr::val("$.phase.Commit.checkpoint"),
                                        ]))
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

/// A request-local proof created only by the reader preflight below. Keeping
/// the type private prevents callers from substituting an unchecked boolean.
/// The durable database nonce also prevents moving a proof between stores.
#[derive(Debug)]
struct PreparedRunnerPublication {
    database_id: String,
    operation: String,
    checkpoint: String,
    generation: u64,
    expected_head: Option<String>,
    workspace: Option<String>,
    structural_generation: i64,
    source_mutation_generation: Option<i64>,
    source_insert_generation: Option<i64>,
    identity_current: bool,
    sources_current: bool,
}

async fn prepare_runner_publication(
    store: &CrudStore,
    operation: &str,
    checkpoint: &str,
    generation: u64,
    expected_head: Option<&str>,
) -> Result<PreparedRunnerPublication> {
    let snapshot = store.connection.begin_read().await?;
    let database_fence = snapshot
        .query_one_raw(Statement::from_string(
            sea_orm::DbBackend::Sqlite,
            "SELECT database_id,structural_generation \
             FROM compaction_publication_fence WHERE singleton=1"
                .to_owned(),
        ))
        .await?
        .ok_or_else(|| anyhow::anyhow!("compaction publication database fence is missing"))?;
    let database_id: String = database_fence.try_get("", "database_id")?;
    let structural_generation: i64 = database_fence.try_get("", "structural_generation")?;

    // Existence and ownership are independent from the later negative stale
    // scans: an empty scan is not evidence that the operation or candidate
    // exists. Read all identity fields in this same snapshot as the fence.
    let identity = snapshot
        .query_one_raw(Statement::from_sql_and_values(
            sea_orm::DbBackend::Sqlite,
            r#"
SELECT o.owner AS operation_owner,o.status,o.expected_head,
 c.workspace_id,p.operation_id AS candidate_operation,p.owner AS candidate_owner,
 s.generation AS runner_generation,
 json_extract(s.state,'$.phase.Commit.checkpoint') AS runner_checkpoint
FROM compaction_operation o
LEFT JOIN compaction_context c ON c.owner=o.owner
LEFT JOIN compaction_checkpoint p ON p.id=?2
LEFT JOIN compaction_runner_state s ON s.operation_id=o.id
WHERE o.id=?1
"#,
            [operation.into(), checkpoint.into()],
        ))
        .await?;
    let expected_generation = i64::try_from(generation)?;
    let (workspace, identity_current) = if let Some(identity) = identity {
        let operation_owner: String = identity.try_get("", "operation_owner")?;
        let _status: String = identity.try_get("", "status")?;
        let stored_head: Option<String> = identity.try_get("", "expected_head")?;
        let workspace: Option<String> = identity.try_get("", "workspace_id")?;
        let candidate_operation: Option<String> = identity.try_get("", "candidate_operation")?;
        let candidate_owner: Option<String> = identity.try_get("", "candidate_owner")?;
        let runner_generation: Option<i64> = identity.try_get("", "runner_generation")?;
        let runner_checkpoint: Option<String> = identity.try_get("", "runner_checkpoint")?;
        let current = workspace.is_some()
            && candidate_operation.as_deref() == Some(operation)
            && candidate_owner.as_deref() == Some(operation_owner.as_str())
            && stored_head.as_deref() == expected_head
            && runner_generation == Some(expected_generation)
            && runner_checkpoint.as_deref() == Some(checkpoint);
        (workspace, current)
    } else {
        (None, false)
    };

    let (source_mutation_generation, source_insert_generation) =
        if let Some(workspace) = workspace.as_deref() {
            let source_fence = snapshot
                .query_one_raw(Statement::from_sql_and_values(
                    sea_orm::DbBackend::Sqlite,
                    "SELECT mutation_generation,insert_generation \
                     FROM compaction_publication_source_fence WHERE workspace_id=?",
                    [workspace.into()],
                ))
                .await?
                .ok_or_else(|| anyhow::anyhow!("compaction publication source fence is missing"))?;
            (
                Some(source_fence.try_get("", "mutation_generation")?),
                Some(source_fence.try_get("", "insert_generation")?),
            )
        } else {
            (None, None)
        };

    let sources_current = if identity_current {
        #[cfg(any(test, feature = "test-support"))]
        pause_publication_test_hook(
            store.connection.runtime_identity(),
            operation,
            PublicationTestPause::ReaderPreflight,
        )
        .await;
        let coverage_exact = compaction_runner_coverage_exact(&snapshot, operation).await?;
        let manifest_current = compaction_manifest_sources_current(&snapshot, operation).await?;
        coverage_exact && manifest_current
    } else {
        false
    };
    // Explicitly end the read snapshot before queuing for the writer. This
    // also releases the maintenance-read permit carried by the scoped store.
    snapshot.commit().await?;
    Ok(PreparedRunnerPublication {
        database_id,
        operation: operation.to_owned(),
        checkpoint: checkpoint.to_owned(),
        generation,
        expected_head: expected_head.map(str::to_owned),
        workspace,
        structural_generation,
        source_mutation_generation,
        source_insert_generation,
        identity_current,
        sources_current,
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

/// Two-phase publication: the graph/manifest predicates run in one reader
/// snapshot, then the writer compares constant-size generations and performs
/// the existing atomic domain transition.
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
                    .and(
                        Expr::col((
                            compaction_operation::Entity,
                            compaction_operation::Column::ExpectedHead,
                        ))
                        .binary(
                            BinOper::Is,
                            Expr::Value(expected_head.map(str::to_owned).into()),
                        ),
                    ),
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
                        .binary(BinOper::Is, Expr::val(Option::<String>::None))
                        .not(),
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
                            Expr::expr(Func::cust(Alias::new("json_extract")).args([
                                Expr::col(compaction_runner_state::Column::State),
                                Expr::val("$.phase.Commit.checkpoint"),
                            ]))
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
            if !prepared.identity_current {
                txn.rollback().await?;
                return Ok(if prepared.workspace.is_some() {
                    // The constant-size writer guards now match even though
                    // reader identity did not. Rebuild the proof from the new
                    // snapshot instead of treating that race as domain stale.
                    super::compaction::CommitOutcome::RetryValidation
                } else {
                    super::compaction::CommitOutcome::Stale
                });
            }
            let (Some(workspace), Some(source_mutation_generation), Some(source_insert_generation)) = (
                prepared.workspace.as_deref(),
                prepared.source_mutation_generation,
                prepared.source_insert_generation,
            ) else {
                txn.rollback().await?;
                return Ok(super::compaction::CommitOutcome::Stale);
            };
            // Constant-size fence lookup. Insert generations matter for a
            // negative result (an insertion can repair it), while an append of
            // a distinct canonical ID cannot invalidate an already-positive
            // exact-ID proof.
            #[cfg(test)]
            {
                let mut checks = PUBLICATION_WRITER_FENCE_CHECKS
                    .get_or_init(|| {
                        std::sync::Mutex::new(std::collections::BTreeMap::new())
                    })
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *checks.entry(operation.to_owned()).or_default() += 1;
            }
            let fence_matches = txn
                .query_one_raw(Statement::from_sql_and_values(
                    sea_orm::DbBackend::Sqlite,
                    r#"
SELECT f.singleton
FROM compaction_publication_fence f
JOIN compaction_publication_source_fence s ON s.workspace_id=?2
WHERE f.singleton=1 AND f.database_id=?1 AND f.structural_generation=?3
 AND s.mutation_generation=?4 AND (?5=1 OR s.insert_generation=?6)
"#,
                    [
                        prepared.database_id.clone().into(),
                        workspace.into(),
                        prepared.structural_generation.into(),
                        source_mutation_generation.into(),
                        (if prepared.sources_current { 1_i64 } else { 0_i64 }).into(),
                        source_insert_generation.into(),
                    ],
                ))
                .await?
                .is_some();
            if !fence_matches {
                txn.rollback().await?;
                return Ok(super::compaction::CommitOutcome::RetryValidation);
            }
            if !prepared.sources_current {
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
                        .and(Expr::col(compaction_context::Column::Head).binary(
                            BinOper::Is,
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
            compaction_runner_state::Entity::update_many()
                .col_expr(
                    compaction_runner_state::Column::Generation,
                    Expr::Value(i64::try_from(applied.generation)?.into()),
                )
                .col_expr(
                    compaction_runner_state::Column::State,
                    Expr::Value(encoded.clone().into()),
                )
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

const COMPACTION_MANIFEST_SOURCES_CURRENT_SQL: &str = r#"
WITH RECURSIVE
current_operation AS (
 SELECT o.id,o.owner,o.expected_head,o.snapshot,c.workspace_id,c.thread_id
 FROM compaction_operation o JOIN compaction_context c ON c.owner=o.owner
 WHERE o.id=?1
),
current_sources(source_scope,source_id,source_version,thread_id,workspace_id) AS (
 SELECT 'context:'||context_revision.turn_id,context_revision.source_id,
  'revision:'||context_revision.revision,context_turn.thread_id,context_thread.workspace_id
 FROM compaction_manifest need JOIN current_operation o ON o.id=need.operation_id
 CROSS JOIN compaction_source_revision context_revision ON context_revision.source_id=need.source_id
 JOIN turn_llm_context context_source ON context_source.id=context_revision.source_id
  AND context_source.turn_id=context_revision.turn_id
 JOIN turn context_turn ON context_turn.id=context_revision.turn_id
 JOIN thread context_thread ON context_thread.id=context_turn.thread_id
 WHERE context_revision.present=1 AND 'context:'||context_revision.turn_id=need.source_scope
  AND 'revision:'||context_revision.revision=need.source_version
  AND context_thread.workspace_id=o.workspace_id
 UNION ALL
 SELECT 'item:'||item_revision.turn_id,item_revision.source_id,
  'item-revision:'||item_revision.revision,item_turn.thread_id,item_thread.workspace_id
 FROM compaction_manifest need JOIN current_operation o ON o.id=need.operation_id
 CROSS JOIN compaction_item_revision item_revision ON item_revision.source_id=need.source_id
 JOIN turn_item item_source ON item_source.id=item_revision.source_id
  AND item_source.turn_id=item_revision.turn_id
 JOIN turn item_turn ON item_turn.id=item_revision.turn_id
 JOIN thread item_thread ON item_thread.id=item_turn.thread_id
 WHERE item_revision.present=1 AND 'item:'||item_revision.turn_id=need.source_scope
  AND 'item-revision:'||item_revision.revision=need.source_version
  AND item_thread.workspace_id=o.workspace_id
 UNION ALL
 SELECT 'event:'||event_revision.turn_id,event_revision.source_id,
  'event-revision:'||event_revision.revision,event_turn.thread_id,event_thread.workspace_id
 FROM compaction_manifest need JOIN current_operation o ON o.id=need.operation_id
 CROSS JOIN compaction_event_revision event_revision ON event_revision.source_id=need.source_id
 JOIN turn_event event_source ON event_source.id=event_revision.source_id
  AND event_source.turn_id=event_revision.turn_id
 JOIN turn event_turn ON event_turn.id=event_revision.turn_id
 JOIN thread event_thread ON event_thread.id=event_turn.thread_id
 WHERE event_revision.present=1 AND 'event:'||event_revision.turn_id=need.source_scope
  AND 'event-revision:'||event_revision.revision=need.source_version
  AND event_thread.workspace_id=o.workspace_id
 UNION ALL
 SELECT 'input:'||input_revision.turn_id,input_revision.source_id,
  'input-revision:'||input_revision.revision,input_turn.thread_id,input_thread.workspace_id
 FROM compaction_manifest need JOIN current_operation o ON o.id=need.operation_id
 CROSS JOIN compaction_input_revision input_revision ON input_revision.source_id=need.source_id
 JOIN turn_input input_source ON input_source.id=input_revision.source_id
  AND input_source.turn_id=input_revision.turn_id
 JOIN turn input_turn ON input_turn.id=input_revision.turn_id
 JOIN thread input_thread ON input_thread.id=input_turn.thread_id
 WHERE input_revision.present=1 AND 'input:'||input_revision.turn_id=need.source_scope
  AND 'input-revision:'||input_revision.revision=need.source_version
  AND input_thread.workspace_id=o.workspace_id
 UNION ALL
 SELECT 'checkpoint:'||checkpoint_source.owner,checkpoint_source.id,
  checkpoint_source.identity_sha256,checkpoint_context.thread_id,checkpoint_context.workspace_id
 FROM compaction_manifest need JOIN current_operation o ON o.id=need.operation_id
 JOIN compaction_checkpoint checkpoint_source ON checkpoint_source.id=need.source_id
 JOIN compaction_context checkpoint_context ON checkpoint_context.owner=checkpoint_source.owner
 WHERE 'checkpoint:'||checkpoint_source.owner=need.source_scope
  AND checkpoint_source.identity_sha256=need.source_version
  AND checkpoint_source.format_version=1
  AND checkpoint_context.workspace_id=o.workspace_id
  AND (checkpoint_source.status='applied' OR (checkpoint_source.status='retained' AND EXISTS(
   SELECT 1 FROM compaction_operation committed
   WHERE committed.id=checkpoint_source.operation_id AND committed.status='completed'
  )))
 UNION ALL
 SELECT 'task-basis:'||basis_source.run_id,basis_source.run_id,
  'task-basis-revision:'||COALESCE(basis_revision.revision,1),
  basis_source.conversation_thread_id,basis_source.workspace_id
 FROM compaction_manifest need JOIN current_operation o ON o.id=need.operation_id
 JOIN task_run_conversation_snapshot basis_source ON basis_source.run_id=need.source_id
 JOIN thread basis_thread ON basis_thread.id=basis_source.conversation_thread_id
  AND basis_thread.workspace_id=basis_source.workspace_id
 LEFT JOIN compaction_task_basis_revision basis_revision ON basis_revision.run_id=basis_source.run_id
 WHERE substr(ltrim(basis_source.history_json),1,1)='['
  AND 'task-basis:'||basis_source.run_id=need.source_scope
  AND 'task-basis-revision:'||COALESCE(basis_revision.revision,1)=need.source_version
  AND basis_source.workspace_id=o.workspace_id
),
accepted_imports AS MATERIALIZED (
 SELECT i.source_scope, i.source_id, i.source_version, i.source_thread
 FROM current_operation o
 JOIN compaction_operation_projection p ON p.operation_id=o.id
 JOIN compaction_frozen_history h ON h.id=p.manifest_id
  AND h.workspace_id=o.workspace_id AND h.ready=1
  AND h.identity_sha256=p.identity_sha256 AND h.imports_sha256=p.imports_sha256
  AND h.import_count=p.import_count AND h.next_import=p.import_count
 JOIN compaction_frozen_import i ON i.manifest_id=h.id
  AND i.manifest_id=(SELECT manifest_id FROM compaction_operation_projection WHERE operation_id=?1)
),
accepted_basis AS (
 SELECT json_extract(f.reference_json,'$.source_thread') AS source_thread,
  json_extract(j.value,'$.scope') AS source_scope,
  json_extract(j.value,'$.id') AS source_id,
  json_extract(j.value,'$.version') AS source_version
 FROM current_operation o
 JOIN compaction_operation_projection p ON p.operation_id=o.id
 JOIN compaction_frozen_history h ON h.id=p.manifest_id
  AND h.workspace_id=o.workspace_id AND h.ready=1
  AND h.identity_sha256=p.identity_sha256 AND h.imports_sha256=p.imports_sha256
  AND h.import_count=p.import_count AND h.next_import=p.import_count
  AND h.next_ordinal=h.message_count
 JOIN compaction_frozen_message f ON f.manifest_id=h.id
  AND f.manifest_id=(SELECT manifest_id FROM compaction_operation_projection WHERE operation_id=?1)
 JOIN json_each(f.reference_json,'$.sources') j
 WHERE json_extract(f.reference_json,'$.inherited')=1
 LIMIT 65537
),
basis_within_bound AS (
 SELECT COUNT(*)<65537 AS valid FROM accepted_basis
),
accepted_basis_roots AS (
 SELECT source_scope AS root_scope, source_id AS root_id, source_version AS root_version,
  source_thread AS root_thread
 FROM accepted_basis, basis_within_bound
 WHERE valid AND source_scope LIKE 'checkpoint:%'
),
basis_coverage(root_scope,root_id,root_version,root_thread,source_scope,source_id,source_version,source_thread) AS (
 SELECT root_scope,root_id,root_version,root_thread,root_scope,root_id,root_version,root_thread
 FROM accepted_basis_roots
 UNION
 SELECT g.root_scope,g.root_id,g.root_version,g.root_thread,
  v.source_scope,v.source_id,v.source_version,m.source_thread
 FROM basis_coverage g
 JOIN compaction_checkpoint node ON node.id=g.source_id
  AND 'checkpoint:'||node.owner=g.source_scope AND node.identity_sha256=g.source_version
 JOIN compaction_coverage v ON v.checkpoint_id=node.id
 JOIN compaction_manifest m ON m.operation_id=node.operation_id
  AND m.source_scope=v.source_scope AND m.source_id=v.source_id
  AND m.source_version=v.source_version AND m.reference_only=0
 WHERE g.source_scope LIKE 'checkpoint:%'
 UNION
 SELECT g.root_scope,g.root_id,g.root_version,g.root_thread,
  'checkpoint:'||previous.owner,previous.id,previous.identity_sha256,previous_context.thread_id
 FROM basis_coverage g
 JOIN compaction_checkpoint node ON node.id=g.source_id
  AND 'checkpoint:'||node.owner=g.source_scope AND node.identity_sha256=g.source_version
 JOIN compaction_checkpoint previous ON previous.id=node.previous
  AND previous.owner=node.owner AND previous.format_version=1
 JOIN compaction_context previous_context ON previous_context.owner=previous.owner
  AND previous_context.thread_id=g.source_thread
 WHERE g.source_scope LIKE 'checkpoint:%'
 LIMIT 65537
),
complete_basis_roots AS (
 SELECT r.root_scope,r.root_id,r.root_version,r.root_thread
 FROM accepted_basis_roots r,current_operation o
 WHERE (SELECT COUNT(*) FROM basis_coverage)<65537
  AND EXISTS (
   SELECT 1 FROM compaction_checkpoint root JOIN compaction_context context ON context.owner=root.owner
   WHERE root.id=r.root_id AND 'checkpoint:'||root.owner=r.root_scope
    AND root.identity_sha256=r.root_version AND root.format_version=1
    AND context.thread_id=r.root_thread AND context.workspace_id=o.workspace_id
    AND (root.status='applied' OR (root.status='retained' AND EXISTS (
     SELECT 1 FROM compaction_operation committed
     WHERE committed.id=root.operation_id AND committed.status='completed'
    )))
  )
  AND EXISTS (SELECT 1 FROM basis_coverage g
   WHERE g.root_scope=r.root_scope AND g.root_id=r.root_id
    AND g.root_version=r.root_version AND g.root_thread=r.root_thread
    AND g.source_scope NOT LIKE 'checkpoint:%')
  AND NOT EXISTS (
   SELECT 1 FROM basis_coverage g
   WHERE g.root_scope=r.root_scope AND g.root_id=r.root_id
    AND g.root_version=r.root_version AND g.root_thread=r.root_thread
    AND g.source_scope LIKE 'checkpoint:%' AND NOT EXISTS (
     SELECT 1 FROM compaction_checkpoint c JOIN compaction_context context ON context.owner=c.owner
     WHERE c.id=g.source_id AND 'checkpoint:'||c.owner=g.source_scope
      AND c.identity_sha256=g.source_version AND c.format_version=1
      AND context.thread_id=g.source_thread AND context.workspace_id=o.workspace_id
    )
  )
  AND NOT EXISTS (
   SELECT 1 FROM basis_coverage g
   JOIN compaction_checkpoint node ON node.id=g.source_id
    AND 'checkpoint:'||node.owner=g.source_scope AND node.identity_sha256=g.source_version
   WHERE g.root_scope=r.root_scope AND g.root_id=r.root_id
    AND g.root_version=r.root_version AND g.root_thread=r.root_thread
    AND g.source_scope LIKE 'checkpoint:%' AND (
     EXISTS (
      SELECT 1 FROM compaction_coverage v
      WHERE v.checkpoint_id=node.id AND (
       SELECT COUNT(DISTINCT ownership.source_thread)
       FROM compaction_manifest ownership
       WHERE ownership.operation_id=node.operation_id AND ownership.reference_only=0
        AND ownership.source_scope=v.source_scope AND ownership.source_id=v.source_id
        AND ownership.source_version=v.source_version
      )<>1
     )
     OR (node.previous IS NOT NULL AND NOT EXISTS (
      SELECT 1 FROM compaction_checkpoint previous
      JOIN compaction_context previous_context ON previous_context.owner=previous.owner
      WHERE previous.id=node.previous AND previous.owner=node.owner
       AND previous.format_version=1 AND previous_context.workspace_id=o.workspace_id
       AND previous_context.thread_id=g.source_thread
     ))
    )
  )
),
accepted_basis_atoms AS (
 SELECT source_scope, source_id, source_version, source_thread
 FROM accepted_basis, basis_within_bound
 WHERE valid AND source_scope NOT LIKE 'checkpoint:%'
 UNION
 SELECT g.source_scope,g.source_id,g.source_version,g.source_thread
 FROM basis_coverage g
 JOIN complete_basis_roots r ON r.root_scope=g.root_scope AND r.root_id=g.root_id
  AND r.root_version=g.root_version AND r.root_thread=g.root_thread
 WHERE g.source_scope NOT LIKE 'checkpoint:%'
),
checkpoint_roots AS (
 SELECT m.ordinal,m.source_scope,m.source_id,m.source_version,m.source_thread,
  m.reference_only=0 AND m.source_thread<>o.thread_id
   AND NOT EXISTS (SELECT 1 FROM accepted_imports i
    WHERE i.source_scope=m.source_scope AND i.source_id=m.source_id
     AND i.source_version=m.source_version AND i.source_thread=m.source_thread) AS requires_grant
 FROM compaction_manifest m JOIN current_operation o ON o.id=m.operation_id
 WHERE m.source_scope LIKE 'checkpoint:%'
),
coverage(root,root_scope,root_id,root_version,root_thread,source_scope,source_id,source_version,source_thread,terminal_grant) AS (
 SELECT ordinal,source_scope,source_id,source_version,source_thread,
  source_scope,source_id,source_version,source_thread,0 FROM checkpoint_roots
 WHERE requires_grant
 UNION
 SELECT g.root,g.root_scope,g.root_id,g.root_version,g.root_thread,
  v.source_scope,v.source_id,v.source_version,m.source_thread,
  CASE WHEN v.source_scope LIKE 'checkpoint:%' AND (
   EXISTS (SELECT 1 FROM accepted_imports i
    WHERE i.source_scope=v.source_scope AND i.source_id=v.source_id
     AND i.source_version=v.source_version AND i.source_thread=m.source_thread)
   OR (json_extract(o.snapshot,'$.plan.coverage_domain')='working_context'
    AND EXISTS (SELECT 1 FROM accepted_basis b, basis_within_bound bound
     WHERE bound.valid AND b.source_scope=v.source_scope AND b.source_id=v.source_id
      AND b.source_version=v.source_version AND b.source_thread=m.source_thread))
  ) THEN 1 ELSE 0 END
 FROM coverage g
 CROSS JOIN current_operation o
 JOIN compaction_checkpoint node ON node.id=g.source_id
  AND 'checkpoint:'||node.owner=g.source_scope AND node.identity_sha256=g.source_version
 JOIN compaction_coverage v ON v.checkpoint_id=node.id
 JOIN compaction_manifest m ON m.operation_id=node.operation_id
  AND m.source_scope=v.source_scope AND m.source_id=v.source_id
  AND m.source_version=v.source_version AND m.reference_only=0
 WHERE g.source_scope LIKE 'checkpoint:%' AND g.terminal_grant=0
 UNION
 SELECT g.root,g.root_scope,g.root_id,g.root_version,g.root_thread,
  'checkpoint:'||previous.owner,previous.id,previous.identity_sha256,previous_context.thread_id,
  CASE WHEN (
   EXISTS (SELECT 1 FROM accepted_imports i
    WHERE i.source_scope='checkpoint:'||previous.owner AND i.source_id=previous.id
     AND i.source_version=previous.identity_sha256
     AND i.source_thread=previous_context.thread_id)
   OR (json_extract(o.snapshot,'$.plan.coverage_domain')='working_context'
    AND EXISTS (SELECT 1 FROM accepted_basis b, basis_within_bound bound
     WHERE bound.valid AND b.source_scope='checkpoint:'||previous.owner
      AND b.source_id=previous.id AND b.source_version=previous.identity_sha256
      AND b.source_thread=previous_context.thread_id))
  ) THEN 1 ELSE 0 END
 FROM coverage g
 CROSS JOIN current_operation o
 JOIN compaction_checkpoint node ON node.id=g.source_id
  AND 'checkpoint:'||node.owner=g.source_scope AND node.identity_sha256=g.source_version
 JOIN compaction_checkpoint previous ON previous.id=node.previous
  AND previous.owner=node.owner AND previous.format_version=1
 JOIN compaction_context previous_context ON previous_context.owner=previous.owner
  AND previous_context.thread_id=g.source_thread
 WHERE g.source_scope LIKE 'checkpoint:%' AND g.terminal_grant=0
 LIMIT 65537
),
complete_boundary_roots AS (
 SELECT r.ordinal FROM checkpoint_roots r,current_operation o
 WHERE (SELECT COUNT(*) FROM coverage)<65537
  AND EXISTS (SELECT 1 FROM coverage g WHERE g.root=r.ordinal
   AND (g.source_scope NOT LIKE 'checkpoint:%' OR g.terminal_grant=1))
  AND NOT EXISTS (
   SELECT 1 FROM coverage g WHERE g.root=r.ordinal
    AND g.source_scope LIKE 'checkpoint:%' AND NOT EXISTS (
     SELECT 1 FROM compaction_checkpoint c JOIN compaction_context context ON context.owner=c.owner
     WHERE c.id=g.source_id AND 'checkpoint:'||c.owner=g.source_scope
      AND c.identity_sha256=g.source_version AND c.format_version=1
      AND context.thread_id=g.source_thread AND context.workspace_id=o.workspace_id
      AND (g.terminal_grant=0 OR c.status='applied' OR (c.status='retained' AND EXISTS (
       SELECT 1 FROM compaction_operation committed
       WHERE committed.id=c.operation_id AND committed.status='completed'
      )))
    )
  )
  AND NOT EXISTS (
   SELECT 1 FROM coverage g
   JOIN compaction_checkpoint node ON node.id=g.source_id
    AND 'checkpoint:'||node.owner=g.source_scope AND node.identity_sha256=g.source_version
   WHERE g.root=r.ordinal AND g.source_scope LIKE 'checkpoint:%'
    AND g.terminal_grant=0 AND (
    EXISTS (
     SELECT 1 FROM compaction_coverage v
     WHERE v.checkpoint_id=node.id AND (
      SELECT COUNT(DISTINCT ownership.source_thread)
      FROM compaction_manifest ownership
      WHERE ownership.operation_id=node.operation_id AND ownership.reference_only=0
       AND ownership.source_scope=v.source_scope AND ownership.source_id=v.source_id
       AND ownership.source_version=v.source_version
     )<>1
    )
    OR (node.previous IS NOT NULL AND NOT EXISTS (
     SELECT 1 FROM compaction_checkpoint previous
     JOIN compaction_context previous_context ON previous_context.owner=previous.owner
     WHERE previous.id=node.previous AND previous.owner=node.owner
      AND previous.format_version=1 AND previous_context.workspace_id=o.workspace_id
      AND previous_context.thread_id=g.source_thread
    ))
   )
  )
),
valid_roots AS (
 SELECT r.ordinal FROM checkpoint_roots r, current_operation o
 WHERE r.requires_grant
  AND (
   EXISTS (SELECT 1 FROM accepted_imports i
    WHERE i.source_scope=r.source_scope AND i.source_id=r.source_id
     AND i.source_version=r.source_version AND i.source_thread=r.source_thread)
   OR (json_extract(o.snapshot,'$.plan.coverage_domain')='working_context'
    AND EXISTS (SELECT 1 FROM accepted_basis b, basis_within_bound bound
     WHERE bound.valid AND b.source_scope=r.source_scope AND b.source_id=r.source_id
      AND b.source_version=r.source_version AND b.source_thread=r.source_thread))
   OR (EXISTS (SELECT 1 FROM complete_boundary_roots c WHERE c.ordinal=r.ordinal)
    AND NOT EXISTS (
     SELECT 1 FROM coverage g WHERE g.root=r.ordinal
      AND g.source_scope NOT LIKE 'checkpoint:%' AND NOT (
       EXISTS (SELECT 1 FROM accepted_imports i
        WHERE i.source_scope=g.source_scope AND i.source_id=g.source_id
         AND i.source_version=g.source_version AND i.source_thread=g.source_thread)
       OR (json_extract(o.snapshot,'$.plan.coverage_domain')='working_context' AND (
        g.source_thread=o.thread_id
        OR EXISTS (SELECT 1 FROM accepted_basis_atoms b
         WHERE b.source_scope=g.source_scope AND b.source_id=g.source_id
          AND b.source_version=g.source_version AND b.source_thread=g.source_thread)
       ))
      )
    )
   )
  )
)
SELECT m.ordinal FROM compaction_manifest m JOIN current_operation o ON o.id=m.operation_id
WHERE
 EXISTS (SELECT 1 FROM json_each(o.snapshot,'$.source_epochs') wanted
  WHERE NOT EXISTS (SELECT 1 FROM thread t WHERE t.id=wanted.key AND t.workspace_id=o.workspace_id))
 OR NOT EXISTS (SELECT 1 FROM thread t WHERE t.id=o.thread_id AND t.workspace_id=o.workspace_id)
 OR (o.expected_head IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM compaction_checkpoint previous
  WHERE previous.id=o.expected_head AND previous.owner=o.owner AND previous.format_version=1
   AND (previous.status='applied' OR (previous.status='retained' AND EXISTS (
    SELECT 1 FROM compaction_operation committed
    WHERE committed.id=previous.operation_id AND committed.status='completed'
   )))
 ))
 OR NOT EXISTS (
  SELECT 1 FROM current_sources s
  WHERE s.source_scope=m.source_scope AND s.source_id=m.source_id AND s.source_version=m.source_version
   AND s.thread_id=m.source_thread AND s.workspace_id=o.workspace_id
   AND (m.reference_only=1 OR s.thread_id=o.thread_id
    OR EXISTS (SELECT 1 FROM accepted_imports i
     WHERE i.source_scope=s.source_scope AND i.source_id=s.source_id
      AND i.source_version=s.source_version AND i.source_thread=s.thread_id)
    OR (json_extract(o.snapshot,'$.plan.coverage_domain')='working_context'
     AND EXISTS (SELECT 1 FROM json_each(o.snapshot,'$.source_epochs') e WHERE e.key=s.thread_id)
     AND EXISTS (SELECT 1 FROM accepted_basis b, basis_within_bound bound
      WHERE bound.valid AND b.source_scope=s.source_scope AND b.source_id=s.source_id
       AND b.source_version=s.source_version AND b.source_thread=s.thread_id))
    OR EXISTS (SELECT 1 FROM valid_roots r WHERE r.ordinal=m.ordinal))
 )
LIMIT 1
"#;

fn compaction_manifest_sources_current_statement(operation: &str) -> Statement {
    sqlite_specific_sql(COMPACTION_MANIFEST_SOURCES_CURRENT_SQL, [operation.into()])
}

/// Shared admission/final-preflight predicate. Admission avoids provider work
/// for an invalid grant; final publication runs it in a fenced reader snapshot.
pub(crate) async fn compaction_manifest_sources_current<C: ConnectionTrait>(
    db: &C,
    operation: &str,
) -> Result<bool> {
    #[cfg(test)]
    record_publication_heavy_check(operation, true);
    // Direct raw manifest entries remain exact-current. Direct checkpoint
    // entries validate the published object only; bounded historical coverage
    // is used solely to prove compatibility with the bound frozen basis and
    // never joins canonical payload/revision rows. OWN imports still require
    // their immutable delivery proofs. Publication generations fence this
    // reader proof across the short writer CAS.
    let stale = db
        .query_one_raw(compaction_manifest_sources_current_statement(operation))
        .await?;
    Ok(stale.is_none())
}

#[cfg(test)]
#[path = "compaction_manifest_source_lookup_tests.rs"]
mod manifest_source_lookup_tests;
