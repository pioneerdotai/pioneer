//! Prepare a recovery failure and its exact publication fence.
use crate::native_terminal_effect_outbox;
use crate::recovery_job;
use crate::recovery_terminalization_outbox::{
    self, ClaimedRecoveryTerminalizationRecord, permanent_recovery_terminalization_error,
};
use crate::recovery_values::recovery_job_status_from_db;
use crate::turn_item_terminal::{
    TurnItemTerminalState, terminalize_turn_item_payload, tool_call_status,
};
use anyhow::{Context, Result};
use pioneer_crud_access::turn_execution::{
    turn_kind_from_db, turn_origin_from_db, turn_status_from_db,
};
use pioneer_crud_conversations::{
    collaboration,
    events::{TurnEventPayload, TurnWorkOwner},
    protocol_projection::{parse_turn_permission_profile, parse_turn_prompt_manifest},
    thread_metadata as thread,
    turn_item::turn_item_type_from_db,
    turn_payload, turn_reads as turn,
};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_protocol::{RecoveryJobStatus, ToolCallStatus, Turn, TurnItem, TurnStatus};
use sea_orm::{ConnectionTrait, EntityTrait, entity::prelude::DateTimeWithTimeZone};
use std::future::Future;

#[derive(Debug, Clone, PartialEq)]
pub struct AppliedRecoveryTerminalization {
    pub recovery_job_id: String,
    pub thread_id: String,
    pub final_item: Option<pioneer_protocol::ItemCompletedNotification>,
    /// Present only when this application authored the terminal Turn event.
    /// Historical already-terminal rows need no prompt/security decoding and
    /// must not be re-published to the in-memory projection.
    pub newly_terminal_turn: Option<Turn>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RecoveryTerminalizationApplyOutcome {
    Applied(AppliedRecoveryTerminalization),
    SupersededByTerminalStatus(TurnStatus),
    Quarantined { reason_code: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryTerminalCleanupPlan {
    pub runtime_generation: u64,
    pub runtime_contract: String,
}

pub struct PreparedRecoveryTerminalizationCommit<E> {
    pub outbox_updated_at: DateTimeWithTimeZone,
    pub job_updated_at: DateTimeWithTimeZone,
    pub turn_updated_at: DateTimeWithTimeZone,
    pub thread_id: String,
    pub workspace_id: String,
    pub revalidate_item: bool,
    pub item_updated_at: Option<DateTimeWithTimeZone>,
    pub item_event: Option<E>,
    pub terminal_event: Option<E>,
    pub terminal_effects: native_terminal_effect_outbox::PreparedNativeTerminalEffectPreparation,
    pub applied: AppliedRecoveryTerminalization,
}

pub enum PreparedRecoveryTerminalizationOutcome<E> {
    Commit(PreparedRecoveryTerminalizationCommit<E>),
    Superseded {
        outbox_updated_at: DateTimeWithTimeZone,
        job_updated_at: DateTimeWithTimeZone,
        turn_updated_at: DateTimeWithTimeZone,
        current_status: TurnStatus,
    },
}

pub async fn prepare_claimed_recovery_terminalization_commit<E, F, Fut>(
    connection: &PostgresDatabase,
    record: &ClaimedRecoveryTerminalizationRecord,
    resume: Option<&pioneer_protocol::TurnBlockedResumeMetadata>,
    cleanup_plan: &RecoveryTerminalCleanupPlan,
    event_created_at: DateTimeWithTimeZone,
    mut prepare_event: F,
) -> Result<PreparedRecoveryTerminalizationOutcome<E>>
where
    F: FnMut(TurnEventPayload, DateTimeWithTimeZone) -> Fut,
    Fut: Future<Output = Result<E>>,
{
    let outbox = pioneer_entity::recovery_terminalization_outbox::Entity::find_by_id(
        record.recovery_job_id.clone(),
    )
    .one(connection)
    .await?
    .context("recovery terminalization outbox row disappeared")?;
    if outbox.status != recovery_terminalization_outbox::STATUS_DELIVERING
        || outbox.claim_token.as_deref() != Some(record.claim_token.as_str())
    {
        anyhow::bail!("recovery terminalization claim is no longer owned");
    }
    let job = recovery_job::find_job_by_id(connection, record.recovery_job_id.as_str())
        .await?
        .ok_or_else(|| {
            permanent_recovery_terminalization_error(
                "missing_recovery_job",
                "recovery terminalization job disappeared",
            )
        })?;
    let job_status = recovery_job_status_from_db(job.status.as_str()).ok_or_else(|| {
        permanent_recovery_terminalization_error(
            "unknown_recovery_status",
            "recovery terminalization job has an unknown status",
        )
    })?;
    let job_item_type = turn_item_type_from_db(job.item_type.as_str()).ok_or_else(|| {
        permanent_recovery_terminalization_error(
            "unknown_item_type",
            "recovery terminalization job has an unknown item type",
        )
    })?;
    let job_attempt_number = u32::try_from(job.run_count.max(1)).map_err(|_| {
        permanent_recovery_terminalization_error(
            "invalid_attempt_number",
            "recovery terminalization job has an invalid attempt number",
        )
    })?;
    let job_error_message = job
        .last_error
        .clone()
        .or_else(|| job.reason.clone())
        .unwrap_or_else(|| "recovery reached a terminal outcome".to_owned());
    if job.turn_id != record.turn_id
        || job.item_id != record.item_id
        || job_item_type != record.item_type
        || job_status != record.recovery_status
        || job_attempt_number != record.attempt_number
        || job_error_message != record.error_message
    {
        return Err(permanent_recovery_terminalization_error(
            "job_identity_mismatch",
            "recovery terminalization is stale: job identity/status changed",
        ));
    }
    if !matches!(
        job_status,
        RecoveryJobStatus::Blocked | RecoveryJobStatus::Failed | RecoveryJobStatus::Exhausted
    ) {
        return Err(permanent_recovery_terminalization_error(
            "non_terminal_recovery_job",
            "recovery terminalization job is no longer terminalizable",
        ));
    }
    let turn_model = turn::find_turn_by_id(connection, record.turn_id.as_str())
        .await?
        .ok_or_else(|| {
            permanent_recovery_terminalization_error(
                "missing_turn",
                "recovery terminalization Turn is missing",
            )
        })?;
    let thread_model = thread::find_thread_by_id(connection, turn_model.thread_id.as_str())
        .await?
        .ok_or_else(|| {
            permanent_recovery_terminalization_error(
                "missing_thread",
                "recovery terminalization Thread is missing",
            )
        })?;
    let current_status = turn_status_from_db(turn_model.status.as_str()).ok_or_else(|| {
        permanent_recovery_terminalization_error(
            "unknown_turn_status",
            "recovery terminalization Turn has an unknown status",
        )
    })?;
    let desired_status = if job_status == RecoveryJobStatus::Blocked {
        TurnStatus::Blocked
    } else {
        TurnStatus::Failed
    };
    if current_status != TurnStatus::InProgress && current_status != desired_status {
        return Ok(PreparedRecoveryTerminalizationOutcome::Superseded {
            outbox_updated_at: outbox.updated_at,
            job_updated_at: job.updated_at,
            turn_updated_at: turn_model.updated_at,
            current_status,
        });
    }
    let blocked_diagnostic = job
        .diagnostic
        .as_deref()
        .and_then(|value| serde_json::from_str::<pioneer_protocol::RecoveryDiagnostic>(value).ok())
        .filter(|value| {
            value.stop_reason == Some(pioneer_protocol::RecoveryStopReason::NoProgress)
        });
    let terminal_error = if job_status == RecoveryJobStatus::Blocked {
        if current_status == TurnStatus::Blocked {
            turn_model
                .error
                .clone()
                .unwrap_or_else(|| record.error_message.clone())
        } else {
            let resume = resume.ok_or_else(|| {
                permanent_recovery_terminalization_error(
                    "missing_blocked_resume_metadata",
                    "blocked recovery terminalization is missing durable resume metadata",
                )
            })?;
            format!(
                "{} (recovery job {})",
                blocked_diagnostic
                    .as_ref()
                    .map(|value| value.public_message())
                    .unwrap_or_else(|| resume.human_message.clone()),
                record.recovery_job_id
            )
        }
    } else {
        job.diagnostic
            .as_deref()
            .and_then(|value| {
                serde_json::from_str::<pioneer_protocol::RecoveryDiagnostic>(value).ok()
            })
            .filter(|value| value.stop_reason.is_some())
            .unwrap_or_default()
            .public_message()
    };
    let already_terminal = current_status == desired_status;
    let cleanup_reason = terminal_error.chars().take(4_096).collect::<String>();
    let terminal_effects = native_terminal_effect_outbox::prepare_input(
        pioneer_protocol::NativeTerminalEffectPreparation {
            batch_id: format!(
                "{}:terminal-effects:recovery:{}",
                record.turn_id, cleanup_plan.runtime_generation
            ),
            workspace_id: thread_model.workspace_id.clone(),
            thread_id: turn_model.thread_id.clone(),
            turn_id: record.turn_id.clone(),
            runtime_generation: cleanup_plan.runtime_generation,
            effects: vec![pioneer_protocol::NativeTerminalEffectSpec {
                effect_id: format!("{}:terminal-effect:attached-task-cleanup", record.turn_id),
                effect_kind: pioneer_protocol::NativeTerminalEffectKind::AttachedTaskCleanup,
                gate: pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                payload: pioneer_protocol::NativeTerminalEffectPayload::AttachedTaskCleanup {
                    reason: cleanup_reason,
                    runtime_contract: cleanup_plan.runtime_contract.clone(),
                },
                max_attempts: 5,
            }],
        },
    )
    .map_err(|error| {
        permanent_recovery_terminalization_error(
            "invalid_terminal_cleanup_plan",
            format!("failed to prepare recovery terminal cleanup: {error:#}"),
        )
    })?;
    let terminal_turn = if already_terminal {
        // An older Gateway may have committed the terminal Turn before
        // consuming this outbox row. No new terminal event is needed, so
        // do not decode historical prompt/security schemas merely to
        // manufacture an unused return value.
        None
    } else {
        let prompt_manifest = parse_turn_prompt_manifest(&turn_model).map_err(|error| {
            permanent_recovery_terminalization_error(
                "invalid_prompt_manifest",
                format!("failed to decode terminal Turn prompt manifest: {error:#}"),
            )
        })?;
        let permission_profile = parse_turn_permission_profile(&turn_model).map_err(|error| {
            permanent_recovery_terminalization_error(
                "invalid_permission_profile",
                format!("failed to decode terminal Turn permission profile: {error:#}"),
            )
        })?;
        let collaboration =
            collaboration::collaboration_from_model(&turn_model).map_err(|error| {
                permanent_recovery_terminalization_error(
                    "invalid_collaboration_metadata",
                    format!("failed to decode terminal Turn collaboration metadata: {error:#}"),
                )
            })?;
        Some(Turn {
            work_owner: TurnWorkOwner::from_db_str(&turn_model.work_owner)
                .map_err(anyhow::Error::msg)?,
            id: turn_model.id.clone(),
            status: desired_status,
            turn_kind: turn_kind_from_db(turn_model.turn_kind.as_str()).unwrap_or_default(),
            origin: turn_origin_from_db(turn_model.origin.as_str()).unwrap_or_default(),
            mode: collaboration.mode.effective_mode,
            author: collaboration.author,
            reply_to_turn_id: collaboration.reply_to_turn_id,
            mentions: collaboration.mentions,
            message_revision: collaboration.message_revision,
            message_deleted: collaboration.message_deleted,
            error: Some(terminal_error.clone()),
            prompt_manifest,
            permission_profile,
        })
    };
    let item_model = if desired_status == TurnStatus::Failed {
        turn::find_turn_item(connection, record.turn_id.as_str(), record.item_id.as_str()).await?
    } else {
        None
    };
    let mut final_item = None;
    let item_event = if let Some(item_model) = item_model.as_ref() {
        let mut item: TurnItem = turn_payload::decode_item(item_model).map_err(|error| {
            permanent_recovery_terminalization_error(
                "invalid_turn_item_payload",
                format!("failed to decode recovery terminalization item: {error}"),
            )
        })?;
        if tool_call_status(&item) == Some(ToolCallStatus::InProgress) {
            terminalize_turn_item_payload(
                &mut item,
                TurnItemTerminalState::Failed {
                    reason: Some(terminal_error.clone()),
                },
            );
            let notification = pioneer_protocol::ItemCompletedNotification {
                workspace_id: thread_model.workspace_id.clone(),
                thread_id: turn_model.thread_id.clone(),
                turn_id: record.turn_id.clone(),
                item,
            };
            let prepared = prepare_event(
                TurnEventPayload::ItemCompleted(notification.clone()),
                event_created_at,
            )
            .await?;
            final_item = Some(notification);
            Some(prepared)
        } else {
            None
        }
    } else {
        None
    };
    let terminal_event = if already_terminal {
        None
    } else {
        let event = if desired_status == TurnStatus::Blocked {
            TurnEventPayload::TurnBlocked(pioneer_protocol::TurnBlockedNotification {
                workspace_id: thread_model.workspace_id.clone(),
                thread_id: turn_model.thread_id.clone(),
                turn: terminal_turn
                    .clone()
                    .expect("non-terminal recovery must construct a terminal Turn"),
                resume: resume.cloned().map(|mut resume| {
                    if let Some(diagnostic) = &blocked_diagnostic {
                        resume.human_message = diagnostic.public_message();
                    }
                    resume
                }),
            })
        } else {
            TurnEventPayload::TurnFailed(pioneer_protocol::TurnFailedNotification {
                workspace_id: thread_model.workspace_id.clone(),
                thread_id: turn_model.thread_id.clone(),
                turn: terminal_turn
                    .clone()
                    .expect("non-terminal recovery must construct a terminal Turn"),
            })
        };
        Some(prepare_event(event, event_created_at).await?)
    };
    Ok(PreparedRecoveryTerminalizationOutcome::Commit(
        PreparedRecoveryTerminalizationCommit {
            outbox_updated_at: outbox.updated_at,
            job_updated_at: job.updated_at,
            turn_updated_at: turn_model.updated_at,
            thread_id: thread_model.id.clone(),
            workspace_id: thread_model.workspace_id,
            revalidate_item: desired_status == TurnStatus::Failed,
            item_updated_at: item_model.map(|item| item.updated_at),
            item_event,
            terminal_event,
            terminal_effects,
            applied: AppliedRecoveryTerminalization {
                recovery_job_id: record.recovery_job_id.clone(),
                thread_id: turn_model.thread_id,
                final_item,
                newly_terminal_turn: terminal_turn,
            },
        },
    ))
}
