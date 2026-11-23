//! Canonical identity links participate in the caller's publication transaction.
use anyhow::{Context, Result, ensure};
use pioneer_entity::{agent_memory as record, agent_memory_identity as identity};
use sea_orm::sea_query::{Expr, JoinType, OnConflict};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, QuerySelect, Set,
};

pub struct IdentityKey<'a> {
    pub scope_kind: &'a str,
    pub scope_hash: &'a str,
    pub namespace: &'a str,
    pub key: &'a str,
}

impl IdentityKey<'_> {
    fn select(&self) -> sea_orm::Select<identity::Entity> {
        identity::Entity::find()
            .filter(identity::Column::ScopeKind.eq(self.scope_kind))
            .filter(identity::Column::ScopeKeyHash.eq(self.scope_hash))
            .filter(identity::Column::Namespace.eq(self.namespace))
            .filter(identity::Column::CanonicalKey.eq(self.key))
    }
}

pub async fn lookup<C: ConnectionTrait>(db: &C, key: &IdentityKey<'_>) -> Result<Option<String>> {
    Ok(key
        .select()
        .select_only()
        .column(identity::Column::MemoryId)
        .join(
            JoinType::InnerJoin,
            identity::Entity::belongs_to(record::Entity)
                .from(identity::Column::MemoryId)
                .to(record::Column::Id)
                .into(),
        )
        .filter(record::Column::Status.eq("active"))
        .into_tuple::<String>()
        .one(db)
        .await?)
}

pub async fn bind(tx: &DatabaseTransaction, key: &IdentityKey<'_>, memory_id: &str) -> Result<()> {
    identity::Entity::insert(identity::ActiveModel {
        scope_kind: Set(key.scope_kind.to_owned()),
        scope_key_hash: Set(key.scope_hash.to_owned()),
        namespace: Set(key.namespace.to_owned()),
        canonical_key: Set(key.key.to_owned()),
        memory_id: Set(memory_id.to_owned()),
    })
    .on_conflict(
        OnConflict::columns([
            identity::Column::ScopeKind,
            identity::Column::ScopeKeyHash,
            identity::Column::Namespace,
            identity::Column::CanonicalKey,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(tx)
    .await?;

    // The unique constraint orders first publication. Lock an existing link
    // before deciding whether its old owner is still active.
    let existing = key
        .select()
        .select_only()
        .column(identity::Column::MemoryId)
        .lock_exclusive()
        .into_tuple::<String>()
        .one(tx)
        .await?
        .context("canonical memory identity disappeared")?;
    if existing == memory_id {
        return Ok(());
    }
    let active = record::Entity::find_by_id(&existing)
        .filter(record::Column::Status.eq("active"))
        .select_only()
        .column(record::Column::Id)
        .into_tuple::<String>()
        .one(tx)
        .await?;
    ensure!(
        active.is_none(),
        "canonical memory identity already belongs to another active record"
    );

    identity::Entity::update_many()
        .col_expr(identity::Column::MemoryId, Expr::val(memory_id))
        .filter(identity::Column::ScopeKind.eq(key.scope_kind))
        .filter(identity::Column::ScopeKeyHash.eq(key.scope_hash))
        .filter(identity::Column::Namespace.eq(key.namespace))
        .filter(identity::Column::CanonicalKey.eq(key.key))
        .exec(tx)
        .await?;
    Ok(())
}

pub async fn transfer<C: ConnectionTrait>(db: &C, old_id: &str, new_id: &str) -> Result<()> {
    identity::Entity::update_many()
        .col_expr(identity::Column::MemoryId, Expr::val(new_id))
        .filter(identity::Column::MemoryId.eq(old_id))
        .exec(db)
        .await?;
    Ok(())
}
