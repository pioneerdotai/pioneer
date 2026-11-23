use anyhow::Result;
use pioneer_crud_access::identity::GatewayPrincipalRecord;
use pioneer_crud_access::membership::{NewThreadMembership, NewWorkspaceMembership};
use pioneer_entity::{thread_membership, workspace_membership};
use pioneer_protocol::{GatewayId, PrincipalId};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ConnectionTrait, DatabaseTransaction};

#[allow(clippy::too_many_arguments)]
pub async fn create_superuser<C: ConnectionTrait>(
    db: &C,
    id: &PrincipalId,
    gateway_id: &GatewayId,
    display_name: &str,
    nickname: &str,
    nickname_key: &str,
    now: DateTimeWithTimeZone,
) -> Result<GatewayPrincipalRecord> {
    let sequence = pioneer_crud_tasks::completion_inbox::current_completion_sequence(db).await?;
    pioneer_crud_access::identity::create_superuser(
        db,
        id,
        gateway_id,
        display_name,
        nickname,
        nickname_key,
        now,
        sequence,
    )
    .await
}

pub async fn insert_workspace_membership(
    db: &DatabaseTransaction,
    input: &NewWorkspaceMembership,
) -> Result<workspace_membership::Model> {
    let sequence = pioneer_crud_tasks::completion_inbox::current_completion_sequence(db).await?;
    pioneer_crud_access::membership::insert_workspace_membership(db, input, sequence).await
}

pub async fn insert_thread_membership(
    db: &DatabaseTransaction,
    input: &NewThreadMembership,
) -> Result<thread_membership::Model> {
    let sequence = pioneer_crud_tasks::completion_inbox::current_completion_sequence(db).await?;
    pioneer_crud_access::membership::insert_thread_membership(db, input, sequence).await
}

#[cfg(test)]
#[path = "access/membership_tests.rs"]
mod tests;
