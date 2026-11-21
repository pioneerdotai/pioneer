//! Fixed read shapes for the existing frozen maintenance owner.
//! Cache SQL/parameter slots, never connections, snapshots or eligibility.
use anyhow::Result;
use pioneer_entity::{
    compaction_checkpoint as checkpoint, compaction_coverage as coverage,
    compaction_frozen_history as history, compaction_operation as operation,
    compaction_operation_projection as projection, compaction_task_output as task_output, task,
    task_run_conversation_snapshot as task_input, thread_cli_runtime_binding as thread_binding,
    turn_cli_runtime_binding as turn_binding, turn_runtime_snapshot as runtime,
};
use sea_orm::sea_query::{
    Alias, BinOper, Expr, ExprTrait, JoinType, Query, SelectStatement, SimpleExpr,
};
use sea_orm::{
    Condition, ConnectionTrait, DbBackend, EntityTrait, FromQueryResult, QueryFilter, QueryOrder,
    QuerySelect, QueryTrait, Statement, Value,
};
use std::{collections::VecDeque, sync::OnceLock};

// Private build-time markers identify bound slots without parsing SQL or
// relying on a dialect's placeholder numbering. Every execution replaces them.
// Discovery selects only bounded row metadata. Oversized checkpoint keys
// use the separate full-key seek path.
pub const DISCARD_PAGE_KEY_BYTES: i64 = 64 * 1024;
const META_ROWS: u64 = 64;

const INPUTS: [&str; 2] = ["__frozen_read_arg_0", "__frozen_read_arg_1"];

struct PreparedRead {
    statement: Statement,
    slots: Vec<(usize, usize)>,
    input_count: usize,
}

impl PreparedRead {
    fn new(mut query: SelectStatement, input_count: usize, limit: u64) -> Self {
        query.limit(limit);
        let statement = DbBackend::Postgres.build(&query);
        let slots: Vec<_> = statement
            .values
            .iter()
            .flat_map(|values| values.0.iter().enumerate())
            .filter_map(|(slot, value)| match value {
                Value::String(Some(value)) => INPUTS[..input_count]
                    .iter()
                    .position(|input| value.as_str() == *input)
                    .map(|input| (slot, input)),
                _ => None,
            })
            .collect();
        assert!((0..input_count).all(|input| slots.iter().any(|&(_, index)| index == input)));
        Self {
            statement,
            slots,
            input_count,
        }
    }

    fn bind(&self, inputs: &[&str]) -> Statement {
        assert_eq!(inputs.len(), self.input_count);
        let mut statement = self.statement.clone();
        let values = &mut statement.values.as_mut().expect("read inputs are bound").0;
        for &(slot, input) in &self.slots {
            values[slot] = inputs[input].into();
        }
        statement
    }
}

fn input(index: usize) -> SimpleExpr {
    Expr::val(INPUTS[index])
}

struct FrozenReads {
    discard_page: PreparedRead,
    discard_preflight: PreparedRead,
    expiry_page: PreparedRead,
    directly_held: PreparedRead,
}

impl FrozenReads {
    fn get() -> &'static Self {
        static POSTGRES: OnceLock<FrozenReads> = OnceLock::new();
        POSTGRES.get_or_init(Self::new)
    }

    fn new() -> Self {
        let bounded_key = Expr::cust(format!("octet_length(id) <= {DISCARD_PAGE_KEY_BYTES}"));
        let key: SimpleExpr = Expr::case(bounded_key.clone(), Expr::col(checkpoint::Column::Id))
            .finally(Expr::Constant(Option::<String>::None.into()))
            .into();
        let candidate: SimpleExpr = Expr::case(
            Condition::all()
                .add(bounded_key)
                .add(discard_candidate_filter()),
            Expr::Constant(true.into()),
        )
        .finally(Expr::Constant(false.into()))
        .into();
        let discard_page = checkpoint::Entity::find()
            .select_only()
            .column_as(key, "id")
            .column_as(candidate, "candidate")
            // Eligibility stays in SELECT, never in the physical page filter.
            .filter(Expr::col((checkpoint::Entity, checkpoint::Column::Id)).gt(input(0)))
            .filter(Expr::col((checkpoint::Entity, checkpoint::Column::Id)).lte(input(1)))
            .order_by_asc(checkpoint::Column::Id)
            .into_query();

        let id = input(0);
        let previous = checkpoint::Entity::find()
            .select_only()
            .column(checkpoint::Column::Id)
            .filter(Expr::col((checkpoint::Entity, checkpoint::Column::Previous)).eq(id.clone()))
            .into_query();
        let incoming = coverage::Entity::find()
            .select_only()
            .column(coverage::Column::CheckpointId)
            .filter(Expr::col((coverage::Entity, coverage::Column::SourceId)).eq(id.clone()))
            .filter(
                Expr::col((coverage::Entity, coverage::Column::SourceScope))
                    .binary(BinOper::Like, Expr::Constant("checkpoint:%".into())),
            )
            .into_query();
        let discard_preflight = checkpoint::Entity::find()
            .select_only()
            .column(checkpoint::Column::ProofVersion)
            .filter(Expr::col((checkpoint::Entity, checkpoint::Column::Id)).eq(id))
            .filter(discard_candidate_filter())
            .filter(Expr::exists(previous).not())
            .filter(Expr::exists(incoming).not())
            .into_query();

        let held: SimpleExpr = Expr::case(
            direct_root_filter(Expr::col((history::Entity, history::Column::Id))),
            Expr::Constant(true.into()),
        )
        .finally(Expr::Constant(false.into()))
        .into();
        let expiry_page = history::Entity::find()
            .column_as(held, "held")
            .filter(
                Expr::col((history::Entity, history::Column::Expired)).eq(Expr::Constant(0.into())),
            )
            .filter(
                Expr::col((history::Entity, history::Column::Ready)).eq(Expr::Constant(1.into())),
            )
            .filter(Expr::col((history::Entity, history::Column::Id)).gt(input(0)))
            .filter(Expr::col((history::Entity, history::Column::Id)).lte(input(1)))
            .order_by_asc(history::Column::Id)
            .into_query();
        Self {
            discard_page: PreparedRead::new(discard_page, 2, META_ROWS),
            discard_preflight: PreparedRead::new(discard_preflight, 1, 1),
            expiry_page: PreparedRead::new(expiry_page, 2, 16),
            directly_held: PreparedRead::new(direct_roots(), 1, 1),
        }
    }
}

fn direct_roots() -> SelectStatement {
    Query::select()
        .expr_as(Expr::Constant(1.into()), Alias::new("held"))
        .cond_where(direct_root_filter(input(0)))
        .to_owned()
}

// The same necessary admission predicate for individual retries and bounded
// header pages. Keep it out of the page's WHERE so held rows cannot make the
// database search beyond the physical frontier to fill a candidate page.
fn direct_root_filter(id: SimpleExpr) -> Condition {
    let accepted = Query::select()
        .expr(Expr::Constant(1.into()))
        .from(task_input::Entity)
        .join(
            JoinType::LeftJoin,
            task::Entity,
            Expr::col((task::Entity, task::Column::Id))
                .eq(Expr::col((task_input::Entity, task_input::Column::TaskId))),
        )
        .and_where(
            Expr::col((task_input::Entity, task_input::Column::FrozenManifestId)).eq(id.clone()),
        )
        .cond_where(
            Condition::any()
                .add(Expr::col((task::Entity, task::Column::Id)).is_null())
                .add(Expr::col((task::Entity, task::Column::Status)).is_not_in(
                    ["completed", "failed", "cancelled"].map(|s| Expr::Constant(s.into())),
                )),
        )
        .to_owned();
    let runtime = Query::select()
        .expr(Expr::Constant(1.into()))
        .from(runtime::Entity)
        .and_where(Expr::col((runtime::Entity, runtime::Column::FrozenManifestId)).eq(id.clone()))
        .to_owned();
    let thread = Query::select()
        .expr(Expr::Constant(1.into()))
        .from(thread_binding::Entity)
        .and_where(
            Expr::col((
                thread_binding::Entity,
                thread_binding::Column::FrozenManifestId,
            ))
            .eq(id.clone()),
        )
        .to_owned();
    let turn = Query::select()
        .expr(Expr::Constant(1.into()))
        .from(turn_binding::Entity)
        .and_where(
            Expr::col((turn_binding::Entity, turn_binding::Column::FrozenManifestId))
                .eq(id.clone()),
        )
        .to_owned();
    let output = Query::select()
        .expr(Expr::Constant(1.into()))
        .from(task_output::Entity)
        .and_where(Expr::col((task_output::Entity, task_output::Column::ManifestId)).eq(id))
        .to_owned();
    Condition::any()
        .add(Expr::exists(accepted))
        .add(Expr::exists(runtime))
        .add(Expr::exists(thread))
        .add(Expr::exists(turn))
        .add(Expr::exists(output))
}

#[derive(FromQueryResult)]
pub struct ExpiryHeader {
    #[sea_orm(nested)]
    pub header: history::Model,
    pub held: bool,
}

#[derive(FromQueryResult)]
struct DiscardPageRow {
    id: Option<String>,
    candidate: bool,
}

#[derive(FromQueryResult)]
struct DiscardProof {
    proof_version: i64,
}

#[derive(FromQueryResult)]
struct RootHold {
    held: i32,
}

// SeaORM 2's Select stores values in its AST and has no rebinding API. Build
// through Entity/SeaQuery once, then bind the cached Statement and decode via
// Model/FromQueryResult. Each call still uses the scoped runtime and SQLx's
// connection-local prepared cache; no database data is cached here.
pub async fn discard_page(
    db: &impl ConnectionTrait,
    after: &str,
    upper: &str,
) -> Result<VecDeque<(Option<String>, bool)>> {
    let reads = FrozenReads::get();
    Ok(
        DiscardPageRow::find_by_statement(reads.discard_page.bind(&[after, upper]))
            .all(db)
            .await?
            .into_iter()
            .map(|row| (row.id, row.candidate))
            .collect(),
    )
}

pub async fn discard_preflight(db: &impl ConnectionTrait, id: &str) -> Result<bool> {
    let reads = FrozenReads::get();
    Ok(
        DiscardProof::find_by_statement(reads.discard_preflight.bind(&[id]))
            .one(db)
            .await?
            .map(|row| row.proof_version)
            .is_some(),
    )
}

pub async fn expiry_page(
    db: &impl ConnectionTrait,
    after: &str,
    upper: &str,
) -> Result<VecDeque<ExpiryHeader>> {
    let reads = FrozenReads::get();
    Ok(
        ExpiryHeader::find_by_statement(reads.expiry_page.bind(&[after, upper]))
            .all(db)
            .await?
            .into(),
    )
}

pub async fn directly_held(db: &impl ConnectionTrait, id: &str) -> Result<bool> {
    let reads = FrozenReads::get();
    Ok(RootHold::find_by_statement(reads.directly_held.bind(&[id]))
        .one(db)
        .await?
        .map(|row| row.held)
        .is_some())
}

// Necessary metadata-only conditions, shared by page admission and fresh
// preflight. Every subquery is correlated to the physical checkpoint's exact
// keys; no failed-operation or staging-payload inventory is searched per row.
fn discard_candidate_filter() -> Condition {
    use pioneer_entity::{
        compaction_checkpoint_event_input as event_input, compaction_checkpoint_import as import,
        compaction_checkpoint_replay_alias as replay_alias,
    };
    // Correlated exact-key binding lookups: operation/projection/history PKs
    // cannot fan out into a global terminal-operation scan per candidate.
    let binding =
        operation::Entity::find()
            .select_only()
            .column(operation::Column::Id)
            .filter(
                Expr::col((operation::Entity, operation::Column::Id)).eq(Expr::col((
                    checkpoint::Entity,
                    checkpoint::Column::OperationId,
                ))),
            )
            .filter(
                Expr::col((operation::Entity, operation::Column::Owner))
                    .eq(Expr::col((checkpoint::Entity, checkpoint::Column::Owner))),
            )
            .filter(
                Expr::col((operation::Entity, operation::Column::Status)).is_in(
                    ["failed", "cancelled", "stale"].map(|status| Expr::Constant(status.into())),
                ),
            )
            .filter(Expr::exists(
                projection::Entity::find()
                    .select_only()
                    .column(projection::Column::OperationId)
                    .filter(
                        Expr::col((projection::Entity, projection::Column::OperationId))
                            .eq(Expr::col((operation::Entity, operation::Column::Id))),
                    )
                    .filter(Expr::exists(
                        history::Entity::find()
                            .select_only()
                            .column(history::Column::Id)
                            .filter(Expr::col((history::Entity, history::Column::Id)).eq(
                                Expr::col((projection::Entity, projection::Column::ManifestId)),
                            ))
                            .filter(
                                Expr::col((history::Entity, history::Column::Expired))
                                    .eq(Expr::Constant(1.into())),
                            )
                            .into_query(),
                    ))
                    .into_query(),
            ))
            .into_query();
    let candidate_id = Expr::col((checkpoint::Entity, checkpoint::Column::Id));
    let imports = import::Entity::find()
        .select_only()
        .column(import::Column::CheckpointId)
        .filter(Expr::col((import::Entity, import::Column::CheckpointId)).eq(candidate_id.clone()))
        .into_query();
    let aliases = replay_alias::Entity::find()
        .select_only()
        .column(replay_alias::Column::CheckpointId)
        .filter(
            Expr::col((replay_alias::Entity, replay_alias::Column::CheckpointId))
                .eq(candidate_id.clone()),
        )
        .into_query();
    let events = event_input::Entity::find()
        .select_only()
        .column(event_input::Column::CheckpointId)
        .filter(
            Expr::col((event_input::Entity, event_input::Column::CheckpointId)).eq(candidate_id),
        )
        .into_query();
    Condition::all()
        .add(
            Expr::col((checkpoint::Entity, checkpoint::Column::Status))
                .ne(Expr::Constant("applied".into())),
        )
        .add(Expr::exists(binding))
        .add(
            Condition::any()
                .add(
                    Expr::col((checkpoint::Entity, checkpoint::Column::ProofVersion))
                        .eq(Expr::Constant(1.into())),
                )
                .add(Expr::exists(imports))
                .add(Expr::exists(aliases))
                .add(Expr::exists(events)),
        )
}

#[cfg(test)]
mod tests;
