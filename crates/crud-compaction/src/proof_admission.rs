//! Bounded exact reads of checkpoint admission and the associated runner state.
use anyhow::{Result, ensure};
use pioneer_compaction::OperationSnapshot;
use pioneer_entity::{
    compaction_checkpoint as checkpoint, compaction_operation as operation,
    compaction_runner_plan as plan, compaction_runner_state as runner,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::sea_query::{Alias as SqlAlias, Expr, ExprTrait, JoinType, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, QueryFilter, QuerySelect, Statement,
    TransactionTrait,
};

use crate::SOURCE_PAGE_BYTES;
use crate::proof_graph::CheckpointEdgesRow;
use crate::proof_publication::{ProofControl, checkpoint_row, read_proof_control};
use crate::sweep_work::retry_conflict;

pub async fn validate_operation_state<C: ConnectionTrait>(
    db: &C,
    row: &CheckpointEdgesRow,
    control: &ProofControl,
) -> Result<()> {
    use pioneer_compaction::runner::RunnerPhase;
    if let Some(generation) = control.generation {
        let saved = runner::Entity::find_by_id(&row.operation_id)
            .select_only()
            .expr_as(
                Expr::col(runner::Column::State).cast_as(SqlAlias::new("text")),
                "state",
            )
            .filter(runner::Column::Generation.eq(generation))
            .filter(
                Expr::cust("octet_length(state::text)::bigint")
                    .lte(i64::try_from(SOURCE_PAGE_BYTES)?),
            )
            .into_tuple::<String>()
            .one(db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("proof runner state missing or oversized"))?;
        let state: pioneer_compaction::runner::RunnerState = serde_json::from_str(&saved)?;
        ensure!(
            i64::try_from(state.generation)? == generation,
            "proof runner generation mismatch"
        );
        let consistent = match control.status.as_str() {
            "running" => !matches!(
                state.phase,
                RunnerPhase::Applied { .. } | RunnerPhase::Failed { .. }
            ),
            "completed" => matches!(state.phase, RunnerPhase::Applied { .. }),
            "failed" | "cancelled" | "stale" => matches!(state.phase, RunnerPhase::Failed { .. }),
            _ => false,
        };
        ensure!(consistent, "proof operation/runner state contradiction");
    } else {
        ensure!(
            row.manifest_id.is_none(),
            "bound checkpoint runner state missing"
        );
    }
    if row.manifest_id.is_some() {
        ensure!(
            plan::Entity::find_by_id(&row.operation_id)
                .select_only()
                .column(plan::Column::OperationId)
                .filter(plan::Column::Ready.eq(1_i64))
                .into_tuple::<String>()
                .one(db)
                .await?
                .is_some(),
            "checkpoint bound plan is not ready"
        );
    }
    Ok(())
}

/// Admission payloads have a domain bound, not a whole-JSON page limit. Only
/// one fragment is returned per statement/worker step; decode happens after
/// reader capacity is released. Admission writers insert/reuse saved
/// snapshots. Deadline resume preserves the plan and advances control generation
/// atomically; fragment/seal fences reject that transition. Closed portions guard
/// every other snapshot change during legacy proof preparation.
pub struct RawAdmissionRead {
    id: String,
    owner: String,
    fingerprint: String,
    control: ProofControl,
    size: i64,
    bytes: Vec<u8>,
    done: bool,
}
impl RawAdmissionRead {
    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn is_complete(&self) -> bool {
        self.done
    }

    pub async fn new<C: ConnectionTrait>(db: &C, id: &str) -> Result<Self> {
        let row = db
            .query_one(
                &Query::select()
                    .from(operation::Entity)
                    .columns([operation::Column::Owner, operation::Column::Fingerprint])
                    .expr_as(
                        Expr::cust("octet_length(snapshot::text)::bigint"),
                        SqlAlias::new("size"),
                    )
                    .and_where(operation::Column::Id.eq(id))
                    .limit(1)
                    .to_owned(),
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("original raw admission missing"))?;
        let size: i64 = row.try_get("", "size")?;
        ensure!(size > 0, "empty original raw admission");
        Ok(Self {
            id: id.into(),
            owner: row.try_get("", "owner")?,
            fingerprint: row.try_get("", "fingerprint")?,
            control: read_proof_control(db, id).await?,
            size,
            bytes: Vec::new(),
            done: false,
        })
    }
    pub async fn step<C: ConnectionTrait>(&mut self, db: &C) -> Result<()> {
        if self.done {
            return Ok(());
        }
        ensure!(
            read_proof_control(db, &self.id).await? == self.control,
            "raw admission control changed between fragments"
        );
        let row = db.query_one_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT substr(convert_to(o.snapshot::text, 'UTF8'), $4::bigint::int, $5::bigint::int) AS fragment
             FROM compaction_operation o
             LEFT JOIN compaction_runner_state r ON r.operation_id=o.id
             WHERE o.id=$1 AND o.owner=$2 AND o.fingerprint=$3
             AND octet_length(o.snapshot::text)=$6 AND o.status=$7
             AND r.generation IS NOT DISTINCT FROM $8 LIMIT 1",
            [self.id.clone().into(), self.owner.clone().into(), self.fingerprint.clone().into(),
             (i64::try_from(self.bytes.len())?+1).into(), i64::try_from(SOURCE_PAGE_BYTES)?.into(),
             self.size.into(), self.control.status.clone().into(), self.control.generation.into()],
        )).await?.ok_or_else(|| anyhow::anyhow!("raw admission identity changed between fragments"))?;
        let fragment: Vec<u8> = row.try_get("", "fragment")?;
        if i64::try_from(self.bytes.len())? == self.size {
            ensure!(fragment.is_empty(), "raw admission extra fragment");
            self.done = true;
        } else {
            ensure!(
                !fragment.is_empty() && fragment.len() <= SOURCE_PAGE_BYTES,
                "raw admission fragment missing or oversized"
            );
            self.bytes.extend(fragment);
            ensure!(
                i64::try_from(self.bytes.len())? <= self.size,
                "raw admission exceeded exact size"
            );
        }
        Ok(())
    }
    pub fn finish(self) -> Result<OperationSnapshot> {
        ensure!(
            self.done && i64::try_from(self.bytes.len())? == self.size,
            "raw admission incomplete"
        );
        let snapshot: OperationSnapshot = serde_json::from_slice(&self.bytes)?;
        ensure!(
            snapshot.id == self.id && snapshot.owner == self.owner,
            "raw admission identity mismatch"
        );
        Ok(snapshot)
    }
}

/// Close the existing portion boundary before preparing a legacy candidate.
/// New candidate writers already close it in their atomic coverage write set.
/// This is an immutable portion boundary, not a revision counter: it never
/// advances on edits, and introduces no new schema/state/publication fence.
pub async fn freeze_checkpoint(db: &SqliteDatabase, id: &str) -> Result<()> {
    let row = checkpoint_row(db, id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("checkpoint to freeze missing"))?;
    if row.coverage_closed {
        return Ok(());
    }
    retry_conflict(|| async {
        let tx = db.begin().await?;
        let query = Query::select()
            .from(checkpoint::Entity)
            .columns(
                [checkpoint::Column::Portion, checkpoint::Column::OperationId]
                    .map(|column| (checkpoint::Entity, column)),
            )
            .join(
                JoinType::InnerJoin,
                operation::Entity,
                Expr::col((operation::Entity, operation::Column::Id))
                    .eq(Expr::col((
                        checkpoint::Entity,
                        checkpoint::Column::OperationId,
                    )))
                    .and(
                        Expr::col((operation::Entity, operation::Column::Owner))
                            .eq(Expr::col((checkpoint::Entity, checkpoint::Column::Owner))),
                    ),
            )
            .and_where(Expr::col((checkpoint::Entity, checkpoint::Column::Id)).eq(id))
            .limit(1)
            .to_owned();
        let row = tx
            .query_one(&query)
            .await?
            .ok_or_else(|| anyhow::anyhow!("checkpoint to freeze missing"))?;
        let portion: i64 = row.try_get("", "portion")?;
        ensure!(
            portion >= 0 && portion < i64::MAX,
            "invalid checkpoint portion boundary"
        );
        let id: String = row.try_get("", "operation_id")?;
        operation::Entity::update_many()
            .col_expr(operation::Column::NextPortion, Expr::val(portion + 1))
            .filter(operation::Column::Id.eq(id))
            .filter(operation::Column::NextPortion.lt(portion + 1))
            .exec(&tx)
            .await?;
        tx.commit().await?;
        Ok(())
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_compaction::SourceRef;

    #[pioneer_postgres_test::postgres_test]
    async fn raw_admission_fragments_reject_control_change_and_restart_without_whole_snapshot_cap()
    {
        use pioneer_compaction::{
            CompactionMode, CompactionPlan, CompactionSettings, CoverageDomain, ModelSelection,
            Transport,
        };
        let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
        let db = fixture.connection();
        db.execute_unprepared(
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',TRUE,FALSE);
             INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);
             INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES('owner','ws','thread',1);",
        ).await.unwrap();
        let snapshot = OperationSnapshot {
            id: "raw".into(),
            owner: "owner".into(),
            expected_checkpoint: None,
            projection_version: 0,
            source_epochs: Default::default(),
            admission: CompactionSettings::default()
                .admit(
                    &ModelSelection {
                        service_mode: None,
                        context_authority: None,
                        context_window: None,
                        transport: Transport::Api,
                        instance: "fixture".into(),
                        model: "model".into(),
                        effort: None,
                    },
                    None,
                    0,
                )
                .unwrap(),
            plan: CompactionPlan {
                mode: CompactionMode::Normal,
                coverage_domain: CoverageDomain::WorkingContext,
                compact: (0..256).collect(),
                retain: vec![],
                fingerprint: "raw".into(),
                coverage: (0..256)
                    .map(|i| SourceRef {
                        scope: "event:turn".into(),
                        id: format!("{i:04}-{}", "x".repeat(2048)),
                        version: "event-revision:1".into(),
                    })
                    .collect(),
            },
        };
        let json = serde_json::to_string(&snapshot).unwrap();
        assert!(json.len() > 2 * SOURCE_PAGE_BYTES);
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms,next_portion) VALUES('raw','owner','raw','failed',$1,1,1)", [json.clone().into()])).await.unwrap();
        db.execute_unprepared("INSERT INTO compaction_runner_state(operation_id,generation,state) VALUES('raw',7,'{}')").await.unwrap();
        // Open legacy admission: fragment identity rejects a changed length or
        // fingerprint even when scalar status/generation did not change.
        for (id, mutation) in [
            ("raw-length", "snapshot=snapshot||' '"),
            ("raw-fingerprint", "fingerprint='other'"),
        ] {
            let mut open = snapshot.clone();
            open.id = id.into();
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES($1,'owner',$1,'running',$2,1)",
                [id.into(),serde_json::to_string(&open).unwrap().into()])).await.unwrap();
            let mut pages = RawAdmissionRead::new(db, id).await.unwrap();
            pages.step(db).await.unwrap();
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                format!("UPDATE compaction_operation SET {mutation} WHERE id=$1"),
                [id.into()],
            ))
            .await
            .unwrap();
            let error = pages.step(db).await.unwrap_err();
            assert!(format!("{error:#}").contains("identity changed between fragments"));
        }
        let mut interrupted = RawAdmissionRead::new(db, "raw").await.unwrap();
        interrupted.step(db).await.unwrap();
        assert_eq!(interrupted.bytes.len(), SOURCE_PAGE_BYTES);
        assert!(!interrupted.done);
        // Even same-length JSON replacement is rejected for closed portions.
        assert!(
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE compaction_operation SET snapshot=$1 WHERE id='raw'",
                [json.replacen("0000-", "zzzz-", 1).into()]
            ))
            .await
            .is_err()
        );
        db.execute_unprepared(
            "UPDATE compaction_runner_state SET generation=8 WHERE operation_id='raw'",
        )
        .await
        .unwrap();
        assert!(interrupted.step(db).await.is_err());
        drop(interrupted); // cancellation releases only private partial bytes
        let mut restart = RawAdmissionRead::new(db, "raw").await.unwrap();
        let mut steps = 0;
        while !restart.done {
            let before = restart.bytes.len();
            restart.step(db).await.unwrap();
            assert!(restart.bytes.len() - before <= SOURCE_PAGE_BYTES);
            steps += 1;
        }
        assert!(steps >= 4);
        assert_eq!(
            serde_json::to_string(&restart.finish().unwrap()).unwrap(),
            json
        );
    }
}
