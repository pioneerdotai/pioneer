pub use pioneer_crud_conversations::turn_event_delivery::*;

#[cfg(test)]
pub async fn claim_due(
    db: &impl sea_orm::ConnectionTrait,
    consumer: &str,
    now: sea_orm::entity::prelude::DateTimeWithTimeZone,
    claim_expires_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    limit: u64,
) -> anyhow::Result<Vec<ClaimedTurnEventDelivery>> {
    let candidates = discover(db, consumer, limit)
        .await?
        .into_iter()
        .filter(|(_, eligible_at)| *eligible_at <= now)
        .map(|(id, _)| id)
        .collect();
    claim_prepared(
        db,
        consumer,
        now,
        claim_expires_at,
        &prepare_claims(candidates),
    )
    .await
}
