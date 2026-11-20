pub use pioneer_crud_access::identity::*;

use anyhow::Result;
use pioneer_protocol::{GatewayId, PrincipalId};
use sea_orm::{ConnectionTrait, entity::prelude::DateTimeWithTimeZone};

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
    let sequence = super::task_completion_inbox::current_completion_sequence(db).await?;
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
