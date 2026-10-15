use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("recovery_job"))
                    .add_column(boolean("resolution_pending").not_null().default(false))
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("recovery_terminalization_outbox"))
                    .add_column(string("quarantine_reason_code").string_len(64).null())
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("recovery_terminalization_outbox"))
                    .add_column(timestamp_with_time_zone("quarantined_at").null())
                    .to_owned(),
            )
            .await?;

        // Older Gateways could create a new recovery as soon as the previous
        // job became terminal, even while its terminalization outbox remained
        // unresolved. Select one authoritative outstanding episode per Turn
        // before installing the partial unique index. Active/pending work wins
        // over a terminal audit row; within a class, the newest durable intent
        // supersedes the older episode.
        let db = manager.get_connection();
        db.execute_unprepared(
            "CREATE TEMP TABLE recovery_episode_canonical AS
             SELECT turn_id, id AS recovery_job_id
             FROM (
                 SELECT r.turn_id,
                        r.id,
                        ROW_NUMBER() OVER (
                            PARTITION BY r.turn_id
                            ORDER BY CASE r.status
                                WHEN 'active' THEN 0
                                WHEN 'pending' THEN 1
                                ELSE 2
                            END,
                            r.created_at DESC,
                            r.id DESC
                        ) AS episode_rank
                 FROM recovery_job r
                 LEFT JOIN recovery_terminalization_outbox o
                   ON o.recovery_job_id = r.id
                 WHERE r.status IN ('pending', 'active')
                    OR (
                        r.status IN ('failed', 'exhausted', 'blocked')
                        AND (
                            o.recovery_job_id IS NULL
                            OR o.status IN ('pending', 'delivering', 'failed')
                        )
                    )
             ) ranked
             WHERE episode_rank = 1",
        )
        .await?;
        db.execute_unprepared(
            "UPDATE recovery_terminalization_outbox
             SET status = 'cancelled',
                 last_error = 'superseded duplicate recovery episode during invariant migration',
                 claim_token = NULL,
                 claim_expires_at = NULL,
                 updated_at = CURRENT_TIMESTAMP
             WHERE status IN ('pending', 'delivering', 'failed')
               AND recovery_job_id NOT IN (
                   SELECT recovery_job_id FROM recovery_episode_canonical
               )",
        )
        .await?;
        db.execute_unprepared(
            "UPDATE recovery_job
             SET status = 'cancelled',
                 last_error = 'superseded duplicate recovery episode during invariant migration',
                 claim_token = NULL,
                 claimed_at = NULL,
                 claim_expires_at = NULL,
                 active_attempt_id = NULL,
                 active_attempt_started_at = NULL,
                 updated_at = CURRENT_TIMESTAMP
             WHERE status IN ('pending', 'active')
               AND id NOT IN (
                   SELECT recovery_job_id FROM recovery_episode_canonical
               )",
        )
        .await?;
        db.execute_unprepared(
            "UPDATE recovery_job
             SET resolution_pending = 1
             WHERE id IN (
                 SELECT recovery_job_id FROM recovery_episode_canonical
             )",
        )
        .await?;
        db.execute_unprepared("DROP TABLE recovery_episode_canonical")
            .await?;
        db.execute_unprepared(
            "CREATE UNIQUE INDEX uq_recovery_job_unresolved_turn
             ON recovery_job(turn_id)
             WHERE resolution_pending = 1",
        )
        .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        db.execute_unprepared("DROP INDEX IF EXISTS uq_recovery_job_unresolved_turn")
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("recovery_terminalization_outbox"))
                    .drop_column(Alias::new("quarantined_at"))
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("recovery_terminalization_outbox"))
                    .drop_column(Alias::new("quarantine_reason_code"))
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("recovery_job"))
                    .drop_column(Alias::new("resolution_pending"))
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}
