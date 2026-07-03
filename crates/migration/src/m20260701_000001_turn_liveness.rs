use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table("turn_liveness")
                    .if_not_exists()
                    .col(string("turn_id").string_len(21).primary_key())
                    .col(string("thread_id").string_len(21))
                    .col(big_integer("last_activity_sequence"))
                    .col(string("last_activity_kind").string_len(64))
                    .col(string("last_activity_item_id").string_len(128).null())
                    .col(string("last_activity_item_type").string_len(64).null())
                    .col(timestamp_with_time_zone("last_activity_at"))
                    .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                    .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_liveness_thread")
                    .table("turn_liveness")
                    .col("thread_id")
                    .to_owned(),
            )
            .await?;

        backfill_turn_liveness_from_events(manager).await?;
        backfill_turn_liveness_from_item_heartbeats(manager).await?;

        for column in [
            big_integer("started_event_sequence").null(),
            string("recovery_suppressed_reason").string_len(128).null(),
            timestamp_with_time_zone("recovery_suppressed_at").null(),
            text("recovery_suppression_context_json").null(),
        ] {
            manager
                .alter_table(
                    Table::alter()
                        .table("turn_item_attempt")
                        .add_column_if_not_exists(column)
                        .to_owned(),
                )
                .await?;
        }

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_turn_item_attempt_recovery_suppressed")
                    .table("turn_item_attempt")
                    .col("recovery_suppressed_reason")
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .if_exists()
                    .name("idx_turn_item_attempt_recovery_suppressed")
                    .table("turn_item_attempt")
                    .to_owned(),
            )
            .await?;

        for column in [
            "recovery_suppression_context_json",
            "recovery_suppressed_at",
            "recovery_suppressed_reason",
            "started_event_sequence",
        ] {
            manager
                .alter_table(
                    Table::alter()
                        .table("turn_item_attempt")
                        .drop_column(Alias::new(column))
                        .to_owned(),
                )
                .await?;
        }

        manager
            .drop_index(
                Index::drop()
                    .if_exists()
                    .name("idx_turn_liveness_thread")
                    .table("turn_liveness")
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(Table::drop().if_exists().table("turn_liveness").to_owned())
            .await
    }
}

async fn backfill_turn_liveness_from_events(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            "INSERT INTO turn_liveness ( \
                 turn_id, \
                 thread_id, \
                 last_activity_sequence, \
                 last_activity_kind, \
                 last_activity_item_id, \
                 last_activity_item_type, \
                 last_activity_at, \
                 created_at, \
                 updated_at \
             ) \
             SELECT \
                 e.turn_id, \
                 e.thread_id, \
                 e.sequence, \
                 e.event_type, \
                 NULL, \
                 NULL, \
                 e.created_at, \
                 e.created_at, \
                 e.created_at \
             FROM turn_event e \
             WHERE e.event_type IN ( \
                 'turn/started', \
                 'item/started', \
                 'item/completed', \
                 'item/updated', \
                 'turn/execution_window/started', \
                 'turn/execution_window/checkpointed', \
                 'turn/execution_window/continued' \
             ) \
               AND NOT EXISTS ( \
                 SELECT 1 \
                 FROM turn_event later \
                 WHERE later.turn_id = e.turn_id \
                   AND later.event_type IN ( \
                     'turn/started', \
                     'item/started', \
                     'item/completed', \
                     'item/updated', \
                     'turn/execution_window/started', \
                     'turn/execution_window/checkpointed', \
                     'turn/execution_window/continued' \
                   ) \
                   AND ( \
                     later.created_at > e.created_at \
                     OR (later.created_at = e.created_at AND later.sequence > e.sequence) \
                   ) \
             ) \
               AND NOT EXISTS ( \
                 SELECT 1 \
                 FROM turn_liveness existing \
                 WHERE existing.turn_id = e.turn_id \
             )",
        )
        .await?;

    Ok(())
}

async fn backfill_turn_liveness_from_item_heartbeats(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            "INSERT INTO turn_liveness ( \
                 turn_id, \
                 thread_id, \
                 last_activity_sequence, \
                 last_activity_kind, \
                 last_activity_item_id, \
                 last_activity_item_type, \
                 last_activity_at, \
                 created_at, \
                 updated_at \
             ) \
             SELECT \
                 ti.turn_id, \
                 t.thread_id, \
                 COALESCE(( \
                     SELECT MAX(e.sequence) \
                     FROM turn_event e \
                     WHERE e.turn_id = ti.turn_id \
                 ), 0), \
                 'item/heartbeat', \
                 ti.item_id, \
                 ti.item_type, \
                 ti.last_heartbeat_at, \
                 ti.last_heartbeat_at, \
                 ti.last_heartbeat_at \
             FROM turn_item ti \
             JOIN turn t ON t.id = ti.turn_id \
             JOIN ( \
                 SELECT turn_id, MAX(last_heartbeat_at) AS last_heartbeat_at \
                 FROM turn_item \
                 WHERE last_heartbeat_at IS NOT NULL \
                 GROUP BY turn_id \
             ) latest \
                 ON latest.turn_id = ti.turn_id \
                AND latest.last_heartbeat_at = ti.last_heartbeat_at \
             WHERE ti.last_heartbeat_at IS NOT NULL \
               AND ti.item_id = ( \
                 SELECT tie.item_id \
                 FROM turn_item tie \
                 WHERE tie.turn_id = ti.turn_id \
                   AND tie.last_heartbeat_at IS NOT NULL \
                 ORDER BY tie.last_heartbeat_at DESC, tie.updated_at DESC, tie.item_id DESC \
                 LIMIT 1 \
               ) \
               AND NOT EXISTS ( \
                 SELECT 1 \
                 FROM turn_liveness existing \
                 WHERE existing.turn_id = ti.turn_id \
             )",
        )
        .await?;

    manager
        .get_connection()
        .execute_unprepared(
            "UPDATE turn_liveness \
             SET \
                 last_activity_sequence = COALESCE(( \
                     SELECT MAX(e.sequence) \
                     FROM turn_event e \
                     WHERE e.turn_id = turn_liveness.turn_id \
                 ), 0), \
                 last_activity_kind = 'item/heartbeat', \
                 last_activity_item_id = ( \
                     SELECT ti.item_id \
                     FROM turn_item ti \
                     WHERE ti.turn_id = turn_liveness.turn_id \
                       AND ti.last_heartbeat_at IS NOT NULL \
                     ORDER BY ti.last_heartbeat_at DESC, ti.updated_at DESC, ti.item_id DESC \
                     LIMIT 1 \
                 ), \
                 last_activity_item_type = ( \
                     SELECT ti.item_type \
                     FROM turn_item ti \
                     WHERE ti.turn_id = turn_liveness.turn_id \
                       AND ti.last_heartbeat_at IS NOT NULL \
                     ORDER BY ti.last_heartbeat_at DESC, ti.updated_at DESC, ti.item_id DESC \
                     LIMIT 1 \
                 ), \
                 last_activity_at = ( \
                     SELECT ti.last_heartbeat_at \
                     FROM turn_item ti \
                     WHERE ti.turn_id = turn_liveness.turn_id \
                       AND ti.last_heartbeat_at IS NOT NULL \
                     ORDER BY ti.last_heartbeat_at DESC, ti.updated_at DESC, ti.item_id DESC \
                     LIMIT 1 \
                 ), \
                 updated_at = ( \
                     SELECT ti.last_heartbeat_at \
                     FROM turn_item ti \
                     WHERE ti.turn_id = turn_liveness.turn_id \
                       AND ti.last_heartbeat_at IS NOT NULL \
                     ORDER BY ti.last_heartbeat_at DESC, ti.updated_at DESC, ti.item_id DESC \
                     LIMIT 1 \
                 ) \
             WHERE EXISTS ( \
                 SELECT 1 \
                 FROM turn_item ti \
                 WHERE ti.turn_id = turn_liveness.turn_id \
                   AND ti.last_heartbeat_at IS NOT NULL \
                   AND ti.last_heartbeat_at > turn_liveness.last_activity_at \
             )",
        )
        .await?;

    Ok(())
}
