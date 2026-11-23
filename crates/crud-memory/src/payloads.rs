use anyhow::{Context, Result};
use pioneer_entity::{agent_memory as owner, agent_memory_payloads as payload};
use sea_orm::sea_query::{
    Alias, Expr, ExprTrait, Func, JoinType, OnConflict, Query, SelectStatement,
    extension::postgres::{PgBinOper, PgFunc},
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

/// The payload is storage owned by MemoryService's existing backend protocol.
/// It is deliberately separate from control rows: put precedes policy/control
/// publication, and uncommitted payloads must never appear in product search.
pub async fn put_payload<C: ConnectionTrait>(
    db: &C,
    memory_id: &str,
    scope_kind: &str,
    scope_key_hash: &str,
    content: &str,
) -> Result<i64> {
    let changed = payload::Entity::insert(payload::ActiveModel {
        memory_id: Set(memory_id.to_owned()),
        scope_kind: Set(scope_kind.to_owned()),
        scope_key_hash: Set(scope_key_hash.to_owned()),
        content: Set(content.to_owned()),
        revision: Set(1),
    })
    .on_conflict(
        OnConflict::column(payload::Column::MemoryId)
            .update_column(payload::Column::Content)
            .value(
                payload::Column::Revision,
                Expr::col((payload::Entity, payload::Column::Revision)).add(1),
            )
            .action_and_where(
                Expr::col((payload::Entity, payload::Column::ScopeKind))
                    .eq(scope_kind)
                    .and(
                        Expr::col((payload::Entity, payload::Column::ScopeKeyHash))
                            .eq(scope_key_hash),
                    ),
            )
            .to_owned(),
    )
    .exec_without_returning(db)
    .await?;
    anyhow::ensure!(changed == 1, "memory payload ownership mismatch");
    payload::Entity::find_by_id(memory_id)
        .select_only()
        .column(payload::Column::Revision)
        .into_tuple::<i64>()
        .one(db)
        .await?
        .context("written memory payload missing")
}

pub async fn get_payload<C: ConnectionTrait>(
    db: &C,
    memory_id: &str,
    scope_kind: &str,
    scope_key_hash: &str,
) -> Result<Option<pioneer_entity::agent_memory_payloads::Model>> {
    Ok(payload::Entity::find_by_id(memory_id)
        .filter(payload::Column::ScopeKind.eq(scope_kind))
        .filter(payload::Column::ScopeKeyHash.eq(scope_key_hash))
        .one(db)
        .await?)
}

pub async fn delete_payload<C: ConnectionTrait>(
    db: &C,
    memory_id: &str,
    scope_kind: &str,
    scope_key_hash: &str,
) -> Result<bool> {
    Ok(payload::Entity::delete_many()
        .filter(payload::Column::MemoryId.eq(memory_id))
        .filter(payload::Column::ScopeKind.eq(scope_kind))
        .filter(payload::Column::ScopeKeyHash.eq(scope_key_hash))
        .exec(db)
        .await?
        .rows_affected
        != 0)
}

/// Match the search index's configuration and combine ordinary text terms
/// with OR. The caller supplies its scoped, sensitivity-checked owners query.
pub async fn search<C: ConnectionTrait>(
    db: &C,
    text: &str,
    eligible: SelectStatement,
    limit: u32,
) -> Result<Vec<(String, String)>> {
    anyhow::ensure!(text.len() <= 64 * 1024, "memory query exceeds byte bound");
    let config = Expr::cust("'simple'::regconfig");
    let lexemes = Query::select()
        .expr_as(
            Func::cust(Alias::new("tsvector_to_array"))
                .arg(Func::cust(Alias::new("to_tsvector")).args([config.clone(), Expr::val(text)])),
            "lexemes",
        )
        .to_owned();
    let lexemes: Vec<String> = db
        .query_one(&lexemes)
        .await?
        .context("memory query lexer returned no row")?
        .try_get("", "lexemes")?;
    anyhow::ensure!(lexemes.len() <= 512, "memory query exceeds term bound");
    if lexemes.is_empty() {
        return Ok(Vec::new());
    }

    let query = lexemes
        .iter()
        .map(|lexeme| format!("'{}'", lexeme.replace('\\', "\\\\").replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(" | ");
    let matching = Expr::val(query).cast_as("tsquery");
    let body = Expr::col((payload::Entity, payload::Column::Content));
    // Keep this expression identical to the migration's GIN index key.
    let vector =
        Expr::from(Func::cust(Alias::new("to_tsvector")).args([config.clone(), body.clone()]));
    let rank = Expr::from(PgFunc::ts_rank_cd(vector.clone(), matching.clone()));
    let snippet = Func::cust(Alias::new("ts_headline")).args([
        config,
        body,
        matching.clone(),
        Expr::val(r#"MinWords=1, MaxWords=8, StartSel="", StopSel="""#),
    ]);

    Ok(payload::Entity::find()
        .select_only()
        .column(payload::Column::MemoryId)
        .expr_as(
            Func::cust(Alias::new("left")).args([snippet.into(), Expr::val(240)]),
            "snippet",
        )
        .join(
            JoinType::InnerJoin,
            payload::Entity::belongs_to(owner::Entity)
                .from(payload::Column::MemoryId)
                .to(owner::Column::Id)
                .into(),
        )
        .filter(payload::Column::MemoryId.in_subquery(eligible))
        .filter(
            Expr::col((payload::Entity, payload::Column::ScopeKind))
                .eq(Expr::col((owner::Entity, owner::Column::ScopeKind))),
        )
        .filter(
            Expr::col((payload::Entity, payload::Column::ScopeKeyHash))
                .eq(Expr::col((owner::Entity, owner::Column::ScopeKeyHash))),
        )
        .filter(vector.binary(PgBinOper::Matches, matching))
        .order_by_desc(rank)
        .order_by_asc(payload::Column::MemoryId)
        .limit(u64::from(std::cmp::min(limit, 1000)))
        .into_tuple::<(String, String)>()
        .all(db)
        .await?)
}
