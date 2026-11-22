//! Optimistic and transactional guards for resuming a saved compaction.

use anyhow::{Result, ensure};
use pioneer_compaction::runner::{FailureKind, RunnerPhase};
use pioneer_entity::{compaction_checkpoint, compaction_frozen_history, compaction_runner_state};
use pioneer_postgres_pools::PostgresDatabase;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, Statement,
    TransactionTrait,
};

use crate::SOURCE_PAGE_BYTES;
use crate::manifest::sources_current;
use crate::operations::compaction_operation;
use crate::runner_repository::{
    compaction_manifest_page, compaction_runner_plan, compaction_runner_state,
};
use crate::runner_state::update_runner_state;
use crate::source_projection::compaction_bound_source_projection;
use crate::sweep_work::retry_conflict;

/// Resume only saved progress on a later admission of the identical plan.
/// Preparation reads operation/state and its bounded manifest page; the short
/// transaction rechecks snapshot, generation, checkpoint, head and Stop fences.
/// This publishes no history. Source freshness can race this preliminary check:
/// the existing runner revalidates before reading another raw portion. A
/// completed candidate resumes without revisiting its historical sources.
pub async fn compaction_resume_deadline(
    db: &PostgresDatabase,
    operation: &str,
    execution_turn: &str,
    deadline_ms: u64,
) -> Result<bool> {
    resume_failed_operation(db, operation, execution_turn, deadline_ms, false).await
}

pub async fn compaction_resume_invalid_completion(
    db: &PostgresDatabase,
    operation: &str,
    execution_turn: &str,
    deadline_ms: u64,
) -> Result<bool> {
    resume_failed_operation(db, operation, execution_turn, deadline_ms, true).await
}

async fn resume_failed_operation(
    db: &PostgresDatabase,
    operation: &str,
    execution_turn: &str,
    deadline_ms: u64,
    invalid_completion: bool,
) -> Result<bool> {
    let Some(record) = compaction_operation(db, operation).await? else {
        return Ok(false);
    };
    if record.status != "failed"
        || (!invalid_completion && record.outcome.as_deref() != Some("deadline"))
    {
        return Ok(false);
    }
    // Expiry is diagnosed before any stale-source fallback, including Ready.
    let bound = compaction_bound_source_projection(db, operation)
        .await
        .map_err(|error| {
            if invalid_completion {
                error.context("compaction resume snapshot is unavailable")
            } else {
                error
            }
        })?;
    let Some(state) = compaction_runner_state(db, operation).await? else {
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
            compaction_runner_plan(db, operation)
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
            .one(db)
            .await?;
        ensure!(
            saved.is_some_and(|(id, owner)| id == operation && owner == record.owner),
            "compaction resume checkpoint is unavailable"
        );
    } else if !state.can_resume_deadline() || deadline_ms <= state.deadline_ms {
        return Ok(false);
    }
    let legacy_final = state.resume_phase.is_none()
        && compaction_manifest_page(db, operation, false, state.cursor.unit, state.cursor.source)
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
            !compaction_manifest_page(db, operation, false, state.cursor.unit, state.cursor.source)
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
        && !sources_current(db, operation).await?
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
    retry_conflict(|| async {
        let tx = db.begin().await?;
        // Decode the bound header result in this same writer transition. An
        // expired saved Commit is an explicit error, not an ordinary stale CAS.
        let verify_projection = async {
            if let Some(bound) = compaction_bound_source_projection(&tx, operation).await? {
                // Order the durable resume against expiry of the retained input.
                // The final operation CAS runs after this lock and its recheck.
                let current = compaction_frozen_history::Entity::find_by_id(&bound.manifest_id)
                    .filter(compaction_frozen_history::Column::Expired.eq(0_i64))
                    .select_only()
                    .column(compaction_frozen_history::Column::Id)
                    .lock_shared()
                    .into_tuple::<String>()
                    .one(&tx).await?;
                ensure!(
                    current.is_some(),
                    "compaction_origin_expired: сохранённый input освобождён; resume недоступен"
                );
            }
            Ok::<_, anyhow::Error>(())
        };
        verify_projection.await.map_err(|error| if invalid_completion {
            error.context("compaction resume snapshot is unavailable")
        } else { error })?;

        // Patch only the new request deadline for InvalidCompletion. Keeping
        // the saved JSON shape also preserves legacy defaults/unknown fields
        // under the admission trigger; Deadline retains its existing rewrite.
        // Keep the JSON deadline edit and the simultaneous operation, runner,
        // head and execution fences in PostgreSQL's one UPDATE statement.
        // The Entity update lacks a typed PostgreSQL jsonb_set expression;
        // the bound parameters keep the original and replacement JSON distinct.
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
