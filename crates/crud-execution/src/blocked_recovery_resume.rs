//! Admission for an explicit resume of blocked execution recovery.
use crate::{recovery_job, turn_runtime_snapshot};
use anyhow::Result;
use pioneer_crud_access::turn_execution::turn_status_from_db;
use pioneer_crud_conversations::{turn_item::turn_item_type_from_db, turn_reads};
use pioneer_entity::{recovery_job as job_entity, turn};
use pioneer_protocol::{TurnItemType, TurnStatus};
use sea_orm::DatabaseTransaction;

use recovery_job::RecoveryJobRecord;

#[derive(Debug, Clone)]
pub enum BlockedTurnRecoveryResumeOutcome {
    Resumed(RecoveryJobRecord),
    NotFound,
    MissingRuntimeSnapshot { recovery_job_id: String },
}

pub enum BlockedResumeAdmission {
    Ready {
        turn: turn::Model,
        job: job_entity::Model,
    },
    Unchanged {
        outcome: BlockedTurnRecoveryResumeOutcome,
        rollback_context: &'static str,
    },
}

pub async fn admit_blocked_recovery_resume(
    tx: &DatabaseTransaction,
    thread_id: &str,
    turn_id: &str,
    recovery_job_id: Option<&str>,
) -> Result<BlockedResumeAdmission> {
    let Some(turn) = turn_reads::find_turn_by_thread_and_id(tx, thread_id, turn_id).await? else {
        return Ok(BlockedResumeAdmission::Unchanged {
            outcome: BlockedTurnRecoveryResumeOutcome::NotFound,
            rollback_context: "failed to rollback missing blocked turn resume transaction",
        });
    };
    if turn_status_from_db(turn.status.as_str()) != Some(TurnStatus::Blocked) {
        return Ok(BlockedResumeAdmission::Unchanged {
            outcome: BlockedTurnRecoveryResumeOutcome::NotFound,
            rollback_context: "failed to rollback non-blocked turn resume transaction",
        });
    }

    let Some(job) = recovery_job::find_blocked_job_by_turn(tx, turn_id, recovery_job_id).await?
    else {
        return Ok(BlockedResumeAdmission::Unchanged {
            outcome: BlockedTurnRecoveryResumeOutcome::NotFound,
            rollback_context: "failed to rollback blocked turn resume without recovery job",
        });
    };

    let item_type =
        turn_item_type_from_db(job.item_type.as_str()).unwrap_or(TurnItemType::DynamicToolCall);
    if item_type.is_tool_item()
        && turn_runtime_snapshot::find_turn_runtime_snapshot(tx, turn_id)
            .await?
            .is_none()
    {
        return Ok(BlockedResumeAdmission::Unchanged {
            outcome: BlockedTurnRecoveryResumeOutcome::MissingRuntimeSnapshot {
                recovery_job_id: job.id,
            },
            rollback_context: "failed to rollback blocked tool recovery without snapshot",
        });
    }

    Ok(BlockedResumeAdmission::Ready { turn, job })
}

#[cfg(test)]
#[path = "blocked_recovery_resume_tests.rs"]
mod tests;
