use anyhow::{Result, ensure};
use pioneer_compaction::runner::RunnerState;
use pioneer_entity::compaction_runner_state as state;
use sea_orm::sea_query::{Expr, ExprTrait, OnConflict, Query};
use sea_orm::{ConnectionTrait, EntityTrait, QuerySelect, UpdateMany};

/// The PostgreSQL JSON type stores the exact serialization used by lifecycle
/// observations. Binding a Serde value through SQLx would reserialize it.
pub fn exact_json(serialized: String) -> Expr {
    Expr::val(serialized).cast_as("text").cast_as("json")
}

#[derive(Clone, Debug, PartialEq, Eq, sea_orm::FromQueryResult)]
pub struct RunnerStateRecord {
    pub generation: i64,
    pub state: String,
}

impl RunnerStateRecord {
    pub fn decode(&self) -> Result<RunnerState> {
        let state: RunnerState = serde_json::from_str(&self.state)?;
        ensure!(
            i64::try_from(state.generation)? == self.generation,
            "runner generation mismatch"
        );
        Ok(state)
    }
}

pub async fn observation<C: ConnectionTrait>(
    db: &C,
    operation_id: &str,
) -> Result<Option<RunnerStateRecord>> {
    Ok(state::Entity::find_by_id(operation_id)
        .select_only()
        .column(state::Column::Generation)
        .column_as(Expr::col(state::Column::State).cast_as("text"), "state")
        .into_model()
        .one(db)
        .await?)
}

pub async fn insert_initial<C: ConnectionTrait>(
    db: &C,
    operation_id: &str,
    serialized: String,
) -> Result<bool> {
    let inserted = db
        .execute(
            &Query::insert()
                .into_table(state::Entity)
                .columns([
                    state::Column::OperationId,
                    state::Column::Generation,
                    state::Column::State,
                ])
                .values_panic([
                    Expr::val(operation_id),
                    Expr::val(0_i64),
                    exact_json(serialized),
                ])
                .on_conflict(
                    OnConflict::column(state::Column::OperationId)
                        .do_nothing()
                        .to_owned(),
                )
                .to_owned(),
        )
        .await?;
    Ok(inserted.rows_affected() == 1)
}

pub fn update_runner_state(generation: i64, serialized: String) -> UpdateMany<state::Entity> {
    state::Entity::update_many()
        .col_expr(state::Column::Generation, Expr::val(generation))
        .col_expr(state::Column::State, exact_json(serialized))
}
