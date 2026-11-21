//! Private thread membership workflows with the caller-selected access scope.
use crate::identity::actor_ref_from_db;
use crate::membership::{
    NewThreadMembership, PrivateThreadParticipantMutation, authorize_private_participant_scope,
    delete_thread_membership, find_thread_membership, insert_thread_membership,
    list_thread_memberships_for_thread, private_thread_participant_target_is_eligible,
    validated_participant_ids_from_rows,
};
use anyhow::{Context, Result};
use pioneer_protocol::{GatewayId, PersistedActorRef, PrincipalId};
use pioneer_sqlite::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, SqliteDatabase,
    is_retryable_database_conflict, retry_with_backoff,
};
use sea_orm::{DatabaseTransaction, TransactionTrait, entity::prelude::DateTimeWithTimeZone};
use std::{future::Future, time::Duration};

pub trait CompletionOrder: Sync {
    fn current<'a>(
        &'a self,
        tx: &'a DatabaseTransaction,
    ) -> impl Future<Output = Result<i64>> + Send + 'a;
}

fn current_time() -> DateTimeWithTimeZone {
    let now = chrono::Utc::now();
    chrono::DateTime::from_timestamp(now.timestamp(), 0)
        .unwrap_or(now)
        .fixed_offset()
}

pub struct ParticipantRepository<'a> {
    connection: &'a SqliteDatabase,
}

impl<'a> ParticipantRepository<'a> {
    pub fn new(connection: &'a SqliteDatabase) -> Self {
        Self { connection }
    }

    async fn run_serialized_write<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        retry_with_backoff(
            operation,
            is_retryable_database_conflict,
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }
    pub async fn list_private_thread_participant_ids(
        &self,
        gateway_id: &GatewayId,
        workspace_id: &str,
        thread_id: &str,
        acting_member_principal_id: Option<&PrincipalId>,
    ) -> Result<Option<Vec<PrincipalId>>> {
        let transaction = self
            .connection
            .begin_read()
            .await
            .context("failed to begin participant list snapshot")?;
        if authorize_private_participant_scope(
            &transaction,
            workspace_id,
            thread_id,
            acting_member_principal_id,
        )
        .await?
        .is_none()
        {
            transaction
                .rollback()
                .await
                .context("failed to roll back inaccessible participant list")?;
            return Ok(None);
        }
        let rows = list_thread_memberships_for_thread(&transaction, thread_id).await?;
        let participant_ids =
            validated_participant_ids_from_rows(&transaction, gateway_id, workspace_id, rows)
                .await?;
        transaction
            .commit()
            .await
            .context("failed to finish participant list snapshot")?;
        Ok(Some(participant_ids))
    }

    pub async fn add_private_thread_participant(
        &self,
        gateway_id: &GatewayId,
        workspace_id: &str,
        thread_id: &str,
        acting_member_principal_id: Option<&PrincipalId>,
        target_principal_id: &PrincipalId,
        actor: PersistedActorRef,
        completion_order: &impl CompletionOrder,
    ) -> Result<Option<PrivateThreadParticipantMutation>> {
        let gateway_id = gateway_id.clone();
        let workspace_id = workspace_id.to_owned();
        let thread_id = thread_id.to_owned();
        let acting_member_principal_id = acting_member_principal_id.cloned();
        let target_principal_id = target_principal_id.clone();
        self.run_serialized_write(|| {
            let gateway_id = gateway_id.clone();
            let workspace_id = workspace_id.clone();
            let thread_id = thread_id.clone();
            let acting_member_principal_id = acting_member_principal_id.clone();
            let target_principal_id = target_principal_id.clone();
            let actor = actor.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin participant add transaction")?;
                if authorize_private_participant_scope(
                    &transaction,
                    workspace_id.as_str(),
                    thread_id.as_str(),
                    acting_member_principal_id.as_ref(),
                )
                .await?
                .is_none()
                {
                    transaction
                        .rollback()
                        .await
                        .context("failed to roll back inaccessible participant add")?;
                    return Ok(None);
                }
                if !private_thread_participant_target_is_eligible(
                    &transaction,
                    &gateway_id,
                    workspace_id.as_str(),
                    &target_principal_id,
                    false,
                )
                .await?
                {
                    transaction
                        .rollback()
                        .await
                        .context("failed to roll back unavailable participant add")?;
                    return Ok(Some(PrivateThreadParticipantMutation::TargetUnavailable));
                }
                let existed =
                    find_thread_membership(&transaction, thread_id.as_str(), &target_principal_id)
                        .await?
                        .is_some();
                let completion_sequence = completion_order.current(&transaction).await?;
                insert_thread_membership(
                    &transaction,
                    &NewThreadMembership {
                        gateway_id: gateway_id.clone(),
                        principal_id: target_principal_id,
                        workspace_id: workspace_id.clone(),
                        thread_id: thread_id.clone(),
                        added_by: actor,
                        now: current_time(),
                    },
                    completion_sequence,
                )
                .await?;
                let rows =
                    list_thread_memberships_for_thread(&transaction, thread_id.as_str()).await?;
                let participant_ids = validated_participant_ids_from_rows(
                    &transaction,
                    &gateway_id,
                    workspace_id.as_str(),
                    rows,
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit participant add transaction")?;
                Ok(Some(PrivateThreadParticipantMutation::Applied {
                    changed: !existed,
                    participant_ids,
                }))
            }
        })
        .await
    }

    pub async fn remove_private_thread_participant(
        &self,
        gateway_id: &GatewayId,
        workspace_id: &str,
        thread_id: &str,
        acting_member_principal_id: Option<&PrincipalId>,
        target_principal_id: &PrincipalId,
    ) -> Result<Option<PrivateThreadParticipantMutation>> {
        let gateway_id = gateway_id.clone();
        let workspace_id = workspace_id.to_owned();
        let thread_id = thread_id.to_owned();
        let acting_member_principal_id = acting_member_principal_id.cloned();
        let target_principal_id = target_principal_id.clone();
        self.run_serialized_write(|| {
            let gateway_id = gateway_id.clone();
            let workspace_id = workspace_id.clone();
            let thread_id = thread_id.clone();
            let acting_member_principal_id = acting_member_principal_id.clone();
            let target_principal_id = target_principal_id.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin participant remove transaction")?;
                let Some(thread) = authorize_private_participant_scope(
                    &transaction,
                    workspace_id.as_str(),
                    thread_id.as_str(),
                    acting_member_principal_id.as_ref(),
                )
                .await?
                else {
                    transaction
                        .rollback()
                        .await
                        .context("failed to roll back inaccessible participant remove")?;
                    return Ok(None);
                };
                let creator = actor_ref_from_db(
                    thread.created_by_actor_kind.as_deref(),
                    thread.created_by_actor_id.as_deref(),
                )?;
                if creator.as_ref()
                    == Some(&PersistedActorRef::Principal(target_principal_id.clone()))
                {
                    transaction
                        .rollback()
                        .await
                        .context("failed to preserve creator participant membership")?;
                    return Ok(Some(PrivateThreadParticipantMutation::MandatoryCreator));
                }
                if !private_thread_participant_target_is_eligible(
                    &transaction,
                    &gateway_id,
                    workspace_id.as_str(),
                    &target_principal_id,
                    true,
                )
                .await?
                {
                    transaction
                        .rollback()
                        .await
                        .context("failed to roll back unavailable participant remove")?;
                    return Ok(Some(PrivateThreadParticipantMutation::TargetUnavailable));
                }
                let changed = delete_thread_membership(
                    &transaction,
                    &gateway_id,
                    workspace_id.as_str(),
                    thread_id.as_str(),
                    &target_principal_id,
                )
                .await?;
                let rows =
                    list_thread_memberships_for_thread(&transaction, thread_id.as_str()).await?;
                let participant_ids = validated_participant_ids_from_rows(
                    &transaction,
                    &gateway_id,
                    workspace_id.as_str(),
                    rows,
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit participant remove transaction")?;
                Ok(Some(PrivateThreadParticipantMutation::Applied {
                    changed,
                    participant_ids,
                }))
            }
        })
        .await
    }
}
