//! Bounded staging and readback of immutable checkpoint evidence.
use anyhow::{Result, ensure};
use pioneer_entity::{
    compaction_checkpoint as checkpoint, compaction_checkpoint_event_input as event_input,
    compaction_checkpoint_import as import, compaction_checkpoint_replay_alias as replay_alias,
};
use sea_orm::sea_query::{Alias as SqlAlias, Expr, ExprTrait, OnConflict, Order, Query};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, FromQueryResult, QueryFilter, QueryOrder,
    QuerySelect, QueryTrait, TransactionSession, TransactionTrait,
};

use crate::SOURCE_PAGE_BYTES;
use crate::proof_graph::{
    HistoricalEventInputEvidenceRow as Event, HistoricalReplayAliasRow as Alias, Metadata,
};

// A proof compares two reference records, each individually <=256 KiB, plus
// fixed provenance fields. This is metadata, never a transcript payload.
pub const FROZEN_IMPORT_PAGE_BYTES: usize = 1024 * 1024;

fn unsealed_checkpoint(id: &str) -> Expr {
    Expr::exists(
        checkpoint::Entity::find()
            .select_only()
            .column(checkpoint::Column::Id)
            .filter(checkpoint::Column::Id.eq(id))
            .filter(checkpoint::Column::ProofVersion.eq(0))
            .into_query(),
    )
}

// Byte length must be checked by the payload SELECT, before JSON is returned.
// Use the byte length of each stored UTF-8 field.
fn import_payload_size() -> Expr {
    Expr::cust(
        "octet_length(proof_json::text)::bigint+octet_length(target_source_thread)::bigint+octet_length(context_thread)::bigint+COALESCE(octet_length(target_checkpoint_json::text),0)",
    )
}

pub async fn stage_import(
    db: &(impl ConnectionTrait + TransactionTrait),
    proof: &import::Model,
) -> Result<()> {
    let select = Query::select()
        .exprs([
            Expr::val(proof.checkpoint_id.clone()),
            Expr::val(proof.import_ordinal),
            Expr::val(proof.message_ordinal),
            Expr::val(proof.target_source_thread.clone()),
            Expr::val(proof.context_thread.clone()),
            Expr::val(proof.target_checkpoint_json.clone()),
            Expr::val(proof.proof_json.clone()),
            Expr::val(proof.bytes),
        ])
        .and_where(unsealed_checkpoint(&proof.checkpoint_id))
        .to_owned();
    let insert = Query::insert()
        .into_table(import::Entity)
        .columns([
            import::Column::CheckpointId,
            import::Column::ImportOrdinal,
            import::Column::MessageOrdinal,
            import::Column::TargetSourceThread,
            import::Column::ContextThread,
            import::Column::TargetCheckpointJson,
            import::Column::ProofJson,
            import::Column::Bytes,
        ])
        .select_from(select)?
        .on_conflict(OnConflict::new().do_nothing().to_owned())
        .to_owned();
    let tx = db.begin().await?;
    tx.execute(&insert).await?;
    let exact = import::Entity::find()
        .filter(import::Column::CheckpointId.eq(&proof.checkpoint_id))
        .filter(import::Column::ImportOrdinal.eq(proof.import_ordinal))
        .filter(import::Column::Bytes.between(0, FROZEN_IMPORT_PAGE_BYTES as i64))
        .filter(import_payload_size().lte(FROZEN_IMPORT_PAGE_BYTES as i64))
        .one(&tx)
        .await?;
    ensure!(
        exact.as_ref() == Some(proof),
        "selected import staging conflict"
    );
    tx.commit().await?;
    Ok(())
}

// The logical key contains a nullable component. Generic find_by_id / delete_by_id
// would emit = NULL, so all alias readback/targeted staging access uses this filter.
fn alias_identity(id: &str, alias: &Alias) -> Condition {
    let condition = Condition::all()
        .add(replay_alias::Column::CheckpointId.eq(id))
        .add(replay_alias::Column::CoveredThread.eq(&alias.covered_thread))
        .add(replay_alias::Column::CoveredScope.eq(&alias.covered_scope))
        .add(replay_alias::Column::CoveredId.eq(&alias.covered_id))
        .add(replay_alias::Column::CoveredVersion.eq(&alias.covered_version))
        .add(replay_alias::Column::ReplayThread.eq(&alias.replay_thread))
        .add(replay_alias::Column::ReplayScope.eq(&alias.replay_scope))
        .add(replay_alias::Column::ReplayId.eq(&alias.replay_id))
        .add(replay_alias::Column::ReplayVersion.eq(&alias.replay_version));
    match &alias.tool_item_id {
        Some(tool) => condition.add(replay_alias::Column::ToolItemId.eq(tool)),
        None => condition.add(replay_alias::Column::ToolItemId.is_null()),
    }
}

pub async fn stage_alias(
    db: &(impl ConnectionTrait + TransactionTrait),
    id: &str,
    alias: &Alias,
) -> Result<()> {
    ensure!(
        serde_json::to_vec(alias)?.len() <= SOURCE_PAGE_BYTES,
        "oversized selected alias"
    );
    let select = Query::select()
        .exprs([
            Expr::val(id),
            Expr::val(alias.covered_thread.clone()),
            Expr::val(alias.covered_scope.clone()),
            Expr::val(alias.covered_id.clone()),
            Expr::val(alias.covered_version.clone()),
            Expr::val(alias.replay_thread.clone()),
            Expr::val(alias.replay_scope.clone()),
            Expr::val(alias.replay_id.clone()),
            Expr::val(alias.replay_version.clone()),
            Expr::val(alias.tool_item_id.clone()),
        ])
        .and_where(unsealed_checkpoint(id))
        .to_owned();
    let insert = Query::insert()
        .into_table(replay_alias::Entity)
        .columns([
            replay_alias::Column::CheckpointId,
            replay_alias::Column::CoveredThread,
            replay_alias::Column::CoveredScope,
            replay_alias::Column::CoveredId,
            replay_alias::Column::CoveredVersion,
            replay_alias::Column::ReplayThread,
            replay_alias::Column::ReplayScope,
            replay_alias::Column::ReplayId,
            replay_alias::Column::ReplayVersion,
            replay_alias::Column::ToolItemId,
        ])
        .select_from(select)?
        .on_conflict(OnConflict::new().do_nothing().to_owned())
        .to_owned();
    let tx = db.begin().await?;
    tx.execute(&insert).await?;
    let found = replay_alias::Entity::find()
        .select_only()
        .column(replay_alias::Column::CheckpointId)
        .filter(alias_identity(id, alias))
        .into_tuple::<String>()
        .one(&tx)
        .await?;
    ensure!(found.is_some(), "alias staging conflict");
    tx.commit().await?;
    Ok(())
}

pub async fn stage_event(
    db: &(impl ConnectionTrait + TransactionTrait),
    id: &str,
    event: &Event,
) -> Result<()> {
    ensure!(
        serde_json::to_vec(event)?.len() <= SOURCE_PAGE_BYTES,
        "oversized selected event"
    );
    let select = Query::select()
        .exprs([
            Expr::val(id),
            Expr::val(event.source_thread.clone()),
            Expr::val(event.source_scope.clone()),
            Expr::val(event.source_id.clone()),
            Expr::val(event.source_version.clone()),
            Expr::val(event.role.clone()),
        ])
        .and_where(unsealed_checkpoint(id))
        .to_owned();
    let insert = Query::insert()
        .into_table(event_input::Entity)
        .columns([
            event_input::Column::CheckpointId,
            event_input::Column::SourceThread,
            event_input::Column::SourceScope,
            event_input::Column::SourceId,
            event_input::Column::SourceVersion,
            event_input::Column::Role,
        ])
        .select_from(select)?
        .on_conflict(OnConflict::new().do_nothing().to_owned())
        .to_owned();
    let tx = db.begin().await?;
    tx.execute(&insert).await?;
    let found = event_input::Entity::find()
        .select_only()
        .column(event_input::Column::CheckpointId)
        .filter(event_input::Column::CheckpointId.eq(id))
        .filter(event_input::Column::SourceThread.eq(&event.source_thread))
        .filter(event_input::Column::SourceScope.eq(&event.source_scope))
        .filter(event_input::Column::SourceId.eq(&event.source_id))
        .filter(event_input::Column::SourceVersion.eq(&event.source_version))
        .filter(event_input::Column::Role.eq(&event.role))
        .into_tuple::<String>()
        .one(&tx)
        .await?;
    ensure!(found.is_some(), "event role staging conflict");
    tx.commit().await?;
    Ok(())
}

/// Selected ordinals need not be contiguous; these are original import ordinals.
pub async fn read_import(
    db: &impl ConnectionTrait,
    id: &str,
    after: i64,
) -> Result<Option<import::Model>> {
    let size = import::Entity::find()
        .select_only()
        .columns([import::Column::ImportOrdinal, import::Column::Bytes])
        .filter(import::Column::CheckpointId.eq(id))
        .filter(import::Column::ImportOrdinal.gt(after))
        .order_by_asc(import::Column::ImportOrdinal)
        .into_tuple::<(i64, i64)>()
        .one(db)
        .await?;
    let Some((ordinal, bytes)) = size else {
        return Ok(None);
    };
    ensure!(
        (0..=FROZEN_IMPORT_PAGE_BYTES as i64).contains(&bytes),
        "invalid permanent import size"
    );
    let row = import::Entity::find()
        .filter(import::Column::CheckpointId.eq(id))
        .filter(import::Column::ImportOrdinal.eq(ordinal))
        .filter(import::Column::Bytes.eq(bytes))
        .filter(import_payload_size().eq(bytes))
        .one(db)
        .await?;
    ensure!(
        row.is_some(),
        "permanent import readback missing or inconsistent"
    );
    Ok(row)
}

#[derive(Default)]
pub struct MetadataReadback {
    alias_after: i64,
    event_after: i64,
    events: bool,
    pub done: bool,
    pub metadata: Metadata,
}
impl MetadataReadback {
    /// One physical row per step, with a sizes-first lookup.
    pub async fn step(&mut self, db: &impl ConnectionTrait, id: &str) -> Result<()> {
        let (table, after, columns) = if self.events {
            (
                "compaction_checkpoint_event_input",
                self.event_after,
                "source_thread,source_scope,source_id,source_version,role",
            )
        } else {
            (
                "compaction_checkpoint_replay_alias",
                self.alias_after,
                "covered_thread,covered_scope,covered_id,covered_version,replay_thread,replay_scope,replay_id,replay_version,tool_item_id",
            )
        };
        let sum = columns
            .split(',')
            .map(|c| format!("COALESCE(octet_length({c}::text),0)::bigint"))
            .collect::<Vec<_>>()
            .join("+");
        let size = db
            .query_one(
                &Query::select()
                    .from(SqlAlias::new(table))
                    .column(SqlAlias::new("insertion_ordinal"))
                    .expr_as(Expr::cust(sum.clone()), SqlAlias::new("bytes"))
                    .and_where(Expr::col(SqlAlias::new("checkpoint_id")).eq(id))
                    .and_where(Expr::col(SqlAlias::new("insertion_ordinal")).gt(after))
                    .order_by(SqlAlias::new("insertion_ordinal"), Order::Asc)
                    .limit(1)
                    .to_owned(),
            )
            .await?;
        let Some(size) = size else {
            if self.events {
                self.done = true;
            } else {
                self.events = true;
            }
            return Ok(());
        };
        let insertion_ordinal: i64 = size.try_get("", "insertion_ordinal")?;
        let bytes: i64 = size.try_get("", "bytes")?;
        ensure!(
            (0..=SOURCE_PAGE_BYTES as i64).contains(&bytes),
            "oversized permanent metadata"
        );
        let row = db
            .query_one(
                &Query::select()
                    .from(SqlAlias::new(table))
                    .column(SqlAlias::new("checkpoint_id"))
                    .column(SqlAlias::new("insertion_ordinal"))
                    .columns(columns.split(',').map(SqlAlias::new))
                    .and_where(Expr::col(SqlAlias::new("checkpoint_id")).eq(id))
                    .and_where(Expr::col(SqlAlias::new("insertion_ordinal")).eq(insertion_ordinal))
                    .and_where(Expr::cust(sum.clone()).eq(bytes))
                    .limit(1)
                    .to_owned(),
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("permanent metadata readback disappeared"))?;
        if self.events {
            let saved =
                pioneer_entity::compaction_checkpoint_event_input::Model::from_query_result(
                    &row, "",
                )?;
            self.event_after = insertion_ordinal;
            self.metadata.events.insert(Event {
                source_thread: saved.source_thread,
                source_scope: saved.source_scope,
                source_id: saved.source_id,
                source_version: saved.source_version,
                role: saved.role,
            });
        } else {
            let saved =
                pioneer_entity::compaction_checkpoint_replay_alias::Model::from_query_result(
                    &row, "",
                )?;
            self.alias_after = insertion_ordinal;
            self.metadata.aliases.insert(Alias {
                covered_thread: saved.covered_thread,
                covered_scope: saved.covered_scope,
                covered_id: saved.covered_id,
                covered_version: saved.covered_version,
                replay_thread: saved.replay_thread,
                replay_scope: saved.replay_scope,
                replay_id: saved.replay_id,
                replay_version: saved.replay_version,
                tool_item_id: saved.tool_item_id,
            });
        }
        ensure!(
            self.metadata.aliases.len() <= pioneer_compaction::REPLAY_ALIAS_LIMIT
                && self.metadata.events.len() <= pioneer_compaction::REPLAY_ALIAS_LIMIT,
            "permanent metadata exceeds bound"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[pioneer_postgres_test::postgres_test]
    async fn nullable_alias_entities_preserve_distinct_keys_exact_retry_and_guarded_staging() {
        let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
        let db = fixture.connection();
        db.execute_unprepared(
            "INSERT INTO workspace(id,name) VALUES('ws','fixture');
            INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
                VALUES('thread','ws','','agent','m','p','active');
            INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version)
                VALUES('owner','ws','thread',1)",
        )
        .await
        .unwrap();
        db.execute_unprepared("INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms,attempts,transient_retries,correction,next_portion) VALUES('alias-op','owner','alias-op','running','{}',1000,0,0,0,0)").await.unwrap();
        db.execute_unprepared("INSERT INTO compaction_checkpoint(id,operation_id,owner,portion,summary,identity_sha256,selection,projection_version,format_version,status) VALUES('alias-cp','alias-op','owner',0,'s','identity','{}',1,1,'candidate')").await.unwrap();
        let alias = Alias {
            covered_thread: "thread".into(),
            covered_scope: "event:turn".into(),
            covered_id: "source".into(),
            covered_version: "v1".into(),
            replay_thread: "thread".into(),
            replay_scope: "item:turn".into(),
            replay_id: "item".into(),
            replay_version: "v1".into(),
            tool_item_id: None,
        };
        let empty = Alias {
            tool_item_id: Some(String::new()),
            ..alias.clone()
        };
        for value in [&alias, &empty, &alias, &empty] {
            stage_alias(db, "alias-cp", value).await.unwrap();
        }
        let null = replay_alias::Entity::find()
            .filter(alias_identity("alias-cp", &alias))
            .one(db)
            .await
            .unwrap()
            .unwrap();
        let blank = replay_alias::Entity::find()
            .filter(alias_identity("alias-cp", &empty))
            .one(db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(null.tool_item_id, None);
        assert_eq!(blank.tool_item_id, Some(String::new()));
        let json = serde_json::to_value(&null).unwrap();
        assert_eq!(
            json.as_object().unwrap().len(),
            11,
            "the row has ten proof columns and the generated insertion ordinal"
        );
        assert!(serde_json::Value::is_null(&json["tool_item_id"]));
        assert!(
            json["insertion_ordinal"]
                .as_i64()
                .is_some_and(|value| value > 0)
        );
        // Evidence updates are forbidden even in staging. Replacement must use
        // exact unsealed deletion; the NULL key must never touch the empty key.
        assert!(
            replay_alias::Entity::update_many()
                .col_expr(replay_alias::Column::CoveredVersion, Expr::val("other"))
                .filter(alias_identity("alias-cp", &alias))
                .filter(unsealed_checkpoint("alias-cp"))
                .exec(db)
                .await
                .is_err()
        );
        assert_eq!(
            replay_alias::Entity::delete_many()
                .filter(alias_identity("alias-cp", &alias))
                .filter(unsealed_checkpoint("alias-cp"))
                .exec(db)
                .await
                .unwrap()
                .rows_affected,
            1
        );
        assert!(
            replay_alias::Entity::find()
                .filter(alias_identity("alias-cp", &empty))
                .one(db)
                .await
                .unwrap()
                .is_some()
        );
        stage_alias(db, "alias-cp", &alias).await.unwrap();
        // This marker fixture only observes the immutable/delete guard; it
        // does not claim these synthetic rows pass the production extractor.
        db.execute_unprepared("UPDATE compaction_operation SET next_portion=1 WHERE id='alias-op'")
            .await
            .unwrap();
        db.execute_unprepared(
            "UPDATE compaction_checkpoint SET proof_version=1 WHERE id='alias-cp'",
        )
        .await
        .unwrap();
        stage_alias(db, "alias-cp", &alias).await.unwrap();
        assert_eq!(
            replay_alias::Entity::delete_many()
                .filter(alias_identity("alias-cp", &alias))
                .filter(unsealed_checkpoint("alias-cp"))
                .exec(db)
                .await
                .unwrap()
                .rows_affected,
            0
        );
        assert!(
            replay_alias::Entity::delete_many()
                .filter(alias_identity("alias-cp", &alias))
                .exec(db)
                .await
                .is_err()
        );
        assert!(
            stage_alias(db, "missing-checkpoint", &alias).await.is_err(),
            "parent predicate does not rely on FK rejection"
        );
    }
}
