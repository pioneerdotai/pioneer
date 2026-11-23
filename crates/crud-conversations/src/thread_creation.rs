//! Admission of a first Turn into a new user Thread.
use crate::{
    events::{TurnEventPayload, TurnStartedEventPayload},
    values::{thread_origin_kind_from_db, thread_sidebar_visibility_from_db},
};
use anyhow::{Result, bail};
use pioneer_crud_access::{
    identity::actor_ref_from_db,
    membership::{
        PersistedThreadAccessClass, find_thread_membership, persisted_thread_access_class_from_db,
    },
};
use pioneer_protocol::{GatewayId, PersistedActorRef, PrincipalId, ThreadVisibility, TurnStatus};
use sea_orm::DatabaseTransaction;

#[derive(Clone)]
pub struct NewUserThreadCreation {
    pub creator: PersistedActorRef,
    pub access_class: PersistedThreadAccessClass,
    pub member: Option<(GatewayId, PrincipalId)>,
}

/// Hold the creation lock through the caller's publication and membership commit.
pub async fn lock_and_validate_initial_turn<'a>(
    transaction: &DatabaseTransaction,
    events: &'a [TurnEventPayload],
    creation: &NewUserThreadCreation,
) -> Result<Option<&'a TurnStartedEventPayload>> {
    let Some((TurnEventPayload::TurnStarted(started), tail)) = events.split_first() else {
        bail!(
            "new user thread materialization requires turn/started, optional immediate turn/completed, and one or more permission audits"
        );
    };
    let audit_events = match tail.split_first() {
        Some((TurnEventPayload::TurnCompleted(completed), remaining))
            if completed.thread_id == started.thread.id
                && completed.turn.id == started.turn.id
                && completed.workspace_id == started.thread.workspace_id
                && completed.turn.status == TurnStatus::Completed =>
        {
            remaining
        }
        Some((TurnEventPayload::TurnCompleted(_), _)) => {
            bail!("new user thread immediate completion differs from its first Turn start");
        }
        _ => tail,
    };
    let Some(TurnEventPayload::TurnPermissionAudit(audit)) = audit_events.first() else {
        bail!(
            "new user thread materialization requires turn/started, optional immediate turn/completed, and one or more permission audits"
        );
    };
    if audit_events
        .iter()
        .any(|event| !matches!(event, TurnEventPayload::TurnPermissionAudit(_)))
    {
        bail!("new user thread materialization contains a non-audit trailing event");
    }
    if started.actor.as_ref() != Some(&creation.creator) {
        bail!("new user thread creator differs from first-turn actor");
    }
    let PersistedActorRef::Principal(creator_principal_id) = &creation.creator else {
        bail!("new user thread creator must be an authenticated principal");
    };
    if audit.workspace_id != started.thread.workspace_id
        || audit.thread_id != started.thread.id
        || audit.turn_id != started.turn.id
    {
        bail!("first-turn permission audit scope differs from the new user thread");
    }
    if audit_events.iter().any(|event| {
        let TurnEventPayload::TurnPermissionAudit(audit) = event else {
            return true;
        };
        audit.workspace_id != started.thread.workspace_id
            || audit.thread_id != started.thread.id
            || audit.turn_id != started.turn.id
    }) {
        bail!("first-turn permission audit scope differs from the new user thread");
    }
    if creation.access_class == PersistedThreadAccessClass::Internal {
        bail!("new user thread cannot use internal access class");
    }
    let expected_visibility = match creation.access_class {
        PersistedThreadAccessClass::Private => ThreadVisibility::Private,
        PersistedThreadAccessClass::Workspace => ThreadVisibility::Workspace,
        PersistedThreadAccessClass::Internal => unreachable!("validated above"),
    };
    if started.thread.visibility != Some(expected_visibility) {
        bail!("new user thread visibility differs from authorized access class");
    }
    if let Some((_, principal_id)) = creation.member.as_ref() {
        if creator_principal_id != principal_id {
            bail!("Member first-turn materialization requires its creator scope");
        }
    }

    crate::thread_state::lock_thread_creation(transaction, started.thread.id.as_str()).await?;
    // A competing first Message can reuse the committed draft only after
    // verifying its immutable creator and access facts.
    if let Some(existing) =
        crate::thread_metadata::find_thread_by_id(transaction, started.thread.id.as_str()).await?
    {
        let existing_creator = actor_ref_from_db(
            existing.created_by_actor_kind.as_deref(),
            existing.created_by_actor_id.as_deref(),
        )?;
        let existing_access_class =
            persisted_thread_access_class_from_db(existing.access_class.as_str())?;
        let existing_origin = thread_origin_kind_from_db(existing.origin_kind.as_str());
        let existing_sidebar_visibility =
            thread_sidebar_visibility_from_db(existing.sidebar_visibility.as_str());
        if existing.workspace_id != started.thread.workspace_id
            || existing_creator.as_ref() != Some(&creation.creator)
            || existing_access_class != creation.access_class
            || existing_origin != Some(started.thread.origin_kind)
            || existing_sidebar_visibility != Some(started.thread.sidebar_visibility)
        {
            bail!("runtime draft collides with a different durable user thread");
        }
        if creation.access_class == PersistedThreadAccessClass::Private
            && let Some((_, principal_id)) = creation.member.as_ref()
            && find_thread_membership(transaction, started.thread.id.as_str(), principal_id)
                .await?
                .is_none()
        {
            bail!("durable Member runtime draft is missing its creator membership");
        }
        return Ok(None);
    }
    Ok(Some(started))
}
