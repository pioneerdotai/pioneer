use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        create_task_run_thread_binding(manager).await?;
        create_task_run_turn(manager).await?;
        create_task_result_candidate(manager).await?;
        create_task_result_review_event(manager).await?;
        add_thread_lineage_graph_columns(manager).await?;
        backfill_thread_lineage_graph_fields(manager).await?;
        backfill_primary_executor_bindings_from_lineage(manager).await?;
        backfill_primary_executor_bindings_from_execution_without_lineage(manager).await?;
        backfill_turns_from_lineage(manager).await?;
        backfill_turns_from_execution_without_lineage(manager).await?;
        backfill_accepted_candidates(manager).await?;
        backfill_runtime_auto_review_events(manager).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        delete_runtime_auto_review_events(manager).await?;
        delete_backfilled_accepted_candidates(manager).await?;
        delete_backfilled_task_run_turns(manager).await?;
        delete_backfilled_primary_executor_bindings(manager).await?;
        clear_backfilled_thread_lineage_graph_fields(manager).await?;
        drop_thread_lineage_graph_columns(manager).await?;
        drop_task_result_review_event(manager).await?;
        drop_task_result_candidate(manager).await?;
        drop_task_run_turn(manager).await?;
        drop_task_run_thread_binding(manager).await?;
        Ok(())
    }
}

async fn add_thread_lineage_graph_columns(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    if !manager.has_column("thread_lineage", "origin_kind").await? {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("thread_lineage"))
                    .add_column(string("origin_kind").string_len(32).null())
                    .to_owned(),
            )
            .await?;
    }

    if !manager
        .has_column("thread_lineage", "created_by_thread_id")
        .await?
    {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("thread_lineage"))
                    .add_column(string("created_by_thread_id").string_len(21).null())
                    .to_owned(),
            )
            .await?;
    }

    if !manager
        .has_column("thread_lineage", "created_by_turn_id")
        .await?
    {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("thread_lineage"))
                    .add_column(string("created_by_turn_id").string_len(21).null())
                    .to_owned(),
            )
            .await?;
    }

    create_index(
        manager,
        "idx_thread_lineage_origin",
        "thread_lineage",
        ["origin_kind"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_thread_lineage_created_by_thread",
        "thread_lineage",
        ["created_by_thread_id"],
        false,
    )
    .await?;

    Ok(())
}

async fn drop_thread_lineage_graph_columns(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    drop_index(
        manager,
        "idx_thread_lineage_created_by_thread",
        "thread_lineage",
    )
    .await?;
    drop_index(manager, "idx_thread_lineage_origin", "thread_lineage").await?;

    if manager
        .has_column("thread_lineage", "created_by_turn_id")
        .await?
    {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("thread_lineage"))
                    .drop_column(Alias::new("created_by_turn_id"))
                    .to_owned(),
            )
            .await?;
    }

    if manager
        .has_column("thread_lineage", "created_by_thread_id")
        .await?
    {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("thread_lineage"))
                    .drop_column(Alias::new("created_by_thread_id"))
                    .to_owned(),
            )
            .await?;
    }

    if manager.has_column("thread_lineage", "origin_kind").await? {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("thread_lineage"))
                    .drop_column(Alias::new("origin_kind"))
                    .to_owned(),
            )
            .await?;
    }

    Ok(())
}

async fn backfill_thread_lineage_graph_fields(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            "UPDATE thread_lineage \
             SET origin_kind = COALESCE(origin_kind, 'task_run'), \
                 created_by_thread_id = COALESCE(created_by_thread_id, parent_thread_id), \
                 created_by_turn_id = COALESCE(created_by_turn_id, parent_turn_id)",
        )
        .await?;
    Ok(())
}

async fn clear_backfilled_thread_lineage_graph_fields(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            "UPDATE thread_lineage \
             SET origin_kind = NULL, \
                 created_by_thread_id = NULL, \
                 created_by_turn_id = NULL \
             WHERE origin_kind = 'task_run' \
               AND created_by_thread_id = parent_thread_id",
        )
        .await?;
    Ok(())
}

async fn create_task_run_thread_binding(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("task_run_thread_binding")
                .if_not_exists()
                .col(string("id").string_len(96).primary_key())
                .col(string("task_id").string_len(21))
                .col(string("run_id").string_len(21))
                .col(string_null("execution_id").string_len(96))
                .col(string("thread_id").string_len(21))
                .col(string("binding_kind").string_len(32))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;

    create_index(
        manager,
        "idx_task_run_thread_binding_task",
        "task_run_thread_binding",
        ["task_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_task_run_thread_binding_run",
        "task_run_thread_binding",
        ["run_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_task_run_thread_binding_run_kind",
        "task_run_thread_binding",
        ["run_id", "binding_kind"],
        false,
    )
    .await?;
    create_index(
        manager,
        "uidx_task_run_thread_binding_thread",
        "task_run_thread_binding",
        ["thread_id"],
        true,
    )
    .await?;
    create_partial_index(
        manager,
        "uidx_task_run_thread_binding_primary_executor",
        "task_run_thread_binding",
        "run_id",
        "binding_kind = 'primary_executor'",
    )
    .await?;

    Ok(())
}

async fn create_task_run_turn(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("task_run_turn")
                .if_not_exists()
                .col(string("id").string_len(96).primary_key())
                .col(string("task_id").string_len(21))
                .col(string("run_id").string_len(21))
                .col(string_null("execution_id").string_len(96))
                .col(string("thread_id").string_len(21))
                .col(string("turn_id").string_len(21))
                .col(string("kind").string_len(32))
                .col(integer("round"))
                .col(integer("sequence"))
                .col(string("status").string_len(32))
                .col(string_null("reviews_candidate_id").string_len(96))
                .col(string_null("requested_by_candidate_id").string_len(96))
                .col(string_null("requested_by_review_event_id").string_len(96))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone_null("started_at"))
                .col(timestamp_with_time_zone_null("completed_at"))
                .to_owned(),
        )
        .await?;

    create_index(
        manager,
        "idx_task_run_turn_task",
        "task_run_turn",
        ["task_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_task_run_turn_run",
        "task_run_turn",
        ["run_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_task_run_turn_thread",
        "task_run_turn",
        ["thread_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_task_run_turn_reviews_candidate",
        "task_run_turn",
        ["reviews_candidate_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_task_run_turn_requested_candidate",
        "task_run_turn",
        ["requested_by_candidate_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_task_run_turn_requested_review",
        "task_run_turn",
        ["requested_by_review_event_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "uidx_task_run_turn_turn",
        "task_run_turn",
        ["turn_id"],
        true,
    )
    .await?;
    create_index(
        manager,
        "uidx_task_run_turn_run_sequence",
        "task_run_turn",
        ["run_id", "sequence"],
        true,
    )
    .await?;
    create_partial_index(
        manager,
        "uidx_task_run_turn_candidate_round",
        "task_run_turn",
        "run_id, round",
        "kind IN ('initial', 'revision', 'recovery')",
    )
    .await?;

    Ok(())
}

async fn create_task_result_candidate(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("task_result_candidate")
                .if_not_exists()
                .col(string("id").string_len(96).primary_key())
                .col(string("task_id").string_len(21))
                .col(string("run_id").string_len(21))
                .col(string("task_run_turn_id").string_len(96))
                .col(string("thread_id").string_len(21))
                .col(string("turn_id").string_len(21))
                .col(integer("round"))
                .col(string("status").string_len(32))
                .col(text_null("result_json"))
                .col(text_null("extraction_error_json"))
                .col(text_null("summary"))
                .col(text_null("diagnostics_json"))
                .col(string_null("final_review_event_id").string_len(96))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone("updated_at").default(Expr::current_timestamp()))
                .col(timestamp_with_time_zone_null("resolved_at"))
                .to_owned(),
        )
        .await?;

    create_index(
        manager,
        "idx_task_result_candidate_task",
        "task_result_candidate",
        ["task_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_task_result_candidate_run",
        "task_result_candidate",
        ["run_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_task_result_candidate_turn",
        "task_result_candidate",
        ["task_run_turn_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "uidx_task_result_candidate_turn",
        "task_result_candidate",
        ["task_run_turn_id"],
        true,
    )
    .await?;
    create_index(
        manager,
        "uidx_task_result_candidate_run_round",
        "task_result_candidate",
        ["run_id", "round"],
        true,
    )
    .await?;
    create_partial_index(
        manager,
        "uidx_task_result_candidate_active_per_run",
        "task_result_candidate",
        "run_id",
        "status IN ('pending_review', 'extraction_failed')",
    )
    .await?;
    create_partial_index(
        manager,
        "uidx_task_result_candidate_accepted_per_run",
        "task_result_candidate",
        "run_id",
        "status = 'accepted'",
    )
    .await?;

    Ok(())
}

async fn create_task_result_review_event(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table("task_result_review_event")
                .if_not_exists()
                .col(string("id").string_len(96).primary_key())
                .col(string("candidate_id").string_len(96))
                .col(string("task_id").string_len(21))
                .col(string("run_id").string_len(21))
                .col(string("task_run_turn_id").string_len(96))
                .col(string("reviewer_kind").string_len(32))
                .col(string_null("reviewer_thread_id").string_len(21))
                .col(string_null("reviewer_turn_id").string_len(21))
                .col(string_null("reviewer_user_id").string_len(128))
                .col(string_null("reviewer_agent_spec_id").string_len(96))
                .col(string("event_kind").string_len(32))
                .col(string("decision").string_len(32))
                .col(text_null("feedback_text"))
                .col(text_null("feedback_json"))
                .col(ColumnDef::new(Alias::new("confidence")).double().null())
                .col(string_null("supersedes_review_event_id").string_len(96))
                .col(string_null("next_task_run_turn_id").string_len(96))
                .col(timestamp_with_time_zone("created_at").default(Expr::current_timestamp()))
                .to_owned(),
        )
        .await?;

    create_index(
        manager,
        "idx_task_result_review_event_candidate",
        "task_result_review_event",
        ["candidate_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_task_result_review_event_run",
        "task_result_review_event",
        ["run_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_task_result_review_event_reviewer_thread",
        "task_result_review_event",
        ["reviewer_thread_id", "reviewer_turn_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_task_result_review_event_reviewer_user",
        "task_result_review_event",
        ["reviewer_user_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_task_result_review_event_reviewer_agent",
        "task_result_review_event",
        ["reviewer_agent_spec_id"],
        false,
    )
    .await?;
    create_index(
        manager,
        "idx_task_result_review_event_next_turn",
        "task_result_review_event",
        ["next_task_run_turn_id"],
        false,
    )
    .await?;

    Ok(())
}

async fn drop_task_result_review_event(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    drop_index(
        manager,
        "idx_task_result_review_event_next_turn",
        "task_result_review_event",
    )
    .await?;
    drop_index(
        manager,
        "idx_task_result_review_event_reviewer_agent",
        "task_result_review_event",
    )
    .await?;
    drop_index(
        manager,
        "idx_task_result_review_event_reviewer_user",
        "task_result_review_event",
    )
    .await?;
    drop_index(
        manager,
        "idx_task_result_review_event_reviewer_thread",
        "task_result_review_event",
    )
    .await?;
    drop_index(
        manager,
        "idx_task_result_review_event_run",
        "task_result_review_event",
    )
    .await?;
    drop_index(
        manager,
        "idx_task_result_review_event_candidate",
        "task_result_review_event",
    )
    .await?;
    manager
        .drop_table(
            Table::drop()
                .table("task_result_review_event")
                .if_exists()
                .to_owned(),
        )
        .await
}

async fn drop_task_result_candidate(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    drop_index(
        manager,
        "uidx_task_result_candidate_accepted_per_run",
        "task_result_candidate",
    )
    .await?;
    drop_index(
        manager,
        "uidx_task_result_candidate_active_per_run",
        "task_result_candidate",
    )
    .await?;
    drop_index(
        manager,
        "uidx_task_result_candidate_run_round",
        "task_result_candidate",
    )
    .await?;
    drop_index(
        manager,
        "uidx_task_result_candidate_turn",
        "task_result_candidate",
    )
    .await?;
    drop_index(
        manager,
        "idx_task_result_candidate_turn",
        "task_result_candidate",
    )
    .await?;
    drop_index(
        manager,
        "idx_task_result_candidate_run",
        "task_result_candidate",
    )
    .await?;
    drop_index(
        manager,
        "idx_task_result_candidate_task",
        "task_result_candidate",
    )
    .await?;
    manager
        .drop_table(
            Table::drop()
                .table("task_result_candidate")
                .if_exists()
                .to_owned(),
        )
        .await
}

async fn drop_task_run_turn(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    drop_index(
        manager,
        "uidx_task_run_turn_candidate_round",
        "task_run_turn",
    )
    .await?;
    drop_index(manager, "uidx_task_run_turn_run_sequence", "task_run_turn").await?;
    drop_index(manager, "uidx_task_run_turn_turn", "task_run_turn").await?;
    drop_index(
        manager,
        "idx_task_run_turn_requested_review",
        "task_run_turn",
    )
    .await?;
    drop_index(
        manager,
        "idx_task_run_turn_requested_candidate",
        "task_run_turn",
    )
    .await?;
    drop_index(
        manager,
        "idx_task_run_turn_reviews_candidate",
        "task_run_turn",
    )
    .await?;
    drop_index(manager, "idx_task_run_turn_thread", "task_run_turn").await?;
    drop_index(manager, "idx_task_run_turn_run", "task_run_turn").await?;
    drop_index(manager, "idx_task_run_turn_task", "task_run_turn").await?;
    manager
        .drop_table(Table::drop().table("task_run_turn").if_exists().to_owned())
        .await
}

async fn drop_task_run_thread_binding(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    drop_index(
        manager,
        "uidx_task_run_thread_binding_primary_executor",
        "task_run_thread_binding",
    )
    .await?;
    drop_index(
        manager,
        "uidx_task_run_thread_binding_thread",
        "task_run_thread_binding",
    )
    .await?;
    drop_index(
        manager,
        "idx_task_run_thread_binding_run_kind",
        "task_run_thread_binding",
    )
    .await?;
    drop_index(
        manager,
        "idx_task_run_thread_binding_run",
        "task_run_thread_binding",
    )
    .await?;
    drop_index(
        manager,
        "idx_task_run_thread_binding_task",
        "task_run_thread_binding",
    )
    .await?;
    manager
        .drop_table(
            Table::drop()
                .table("task_run_thread_binding")
                .if_exists()
                .to_owned(),
        )
        .await
}

async fn backfill_primary_executor_bindings_from_lineage(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            "INSERT INTO task_run_thread_binding ( \
                 id, task_id, run_id, execution_id, thread_id, binding_kind, created_at \
             ) \
             SELECT \
                 'trb_primary_' || tl.task_run_id, \
                 tl.task_id, \
                 tl.task_run_id, \
                 tre.id, \
                 tl.child_thread_id, \
                 'primary_executor', \
                 tl.created_at \
             FROM thread_lineage tl \
             LEFT JOIN task_run_execution tre ON tre.task_run_id = tl.task_run_id \
             WHERE NOT EXISTS ( \
                 SELECT 1 \
                 FROM task_run_thread_binding existing \
                 WHERE existing.run_id = tl.task_run_id \
                   AND existing.binding_kind = 'primary_executor' \
             ) \
               AND NOT EXISTS ( \
                 SELECT 1 \
                 FROM task_run_thread_binding existing_thread \
                 WHERE existing_thread.thread_id = tl.child_thread_id \
             )",
        )
        .await?;
    Ok(())
}

async fn backfill_primary_executor_bindings_from_execution_without_lineage(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            "INSERT INTO task_run_thread_binding ( \
                 id, task_id, run_id, execution_id, thread_id, binding_kind, created_at \
             ) \
             SELECT \
                 'trb_primary_' || tre.task_run_id, \
                 tre.task_id, \
                 tre.task_run_id, \
                 tre.id, \
                 tre.child_thread_id, \
                 'primary_executor', \
                 tre.created_at \
             FROM task_run_execution tre \
             WHERE tre.executor_kind = 'agent' \
               AND tre.child_thread_id IS NOT NULL \
               AND tre.child_turn_id IS NOT NULL \
               AND NOT EXISTS ( \
                 SELECT 1 FROM thread_lineage tl WHERE tl.task_run_id = tre.task_run_id \
             ) \
               AND NOT EXISTS ( \
                 SELECT 1 \
                 FROM task_run_thread_binding existing \
                 WHERE existing.run_id = tre.task_run_id \
                   AND existing.binding_kind = 'primary_executor' \
             ) \
               AND NOT EXISTS ( \
                 SELECT 1 \
                 FROM task_run_thread_binding existing_thread \
                 WHERE existing_thread.thread_id = tre.child_thread_id \
             )",
        )
        .await?;
    Ok(())
}

async fn delete_backfilled_primary_executor_bindings(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            "DELETE FROM task_run_thread_binding \
             WHERE binding_kind = 'primary_executor' \
               AND id LIKE 'trb_primary_%'",
        )
        .await?;
    Ok(())
}

async fn backfill_turns_from_lineage(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            "INSERT INTO task_run_turn ( \
                 id, task_id, run_id, execution_id, thread_id, turn_id, kind, round, sequence, \
                 status, reviews_candidate_id, requested_by_candidate_id, \
                 requested_by_review_event_id, created_at, started_at, completed_at \
             ) \
             SELECT \
                 'trt_' || tl.child_turn_id, \
                 tl.task_id, \
                 tl.task_run_id, \
                 tre.id, \
                 tl.child_thread_id, \
                 tl.child_turn_id, \
                 'initial', \
                 0, \
                 0, \
                 CASE \
                     WHEN t.status = 'completed' \
                      AND r.status = 'succeeded' \
                      AND r.result_json IS NOT NULL THEN 'candidate_created' \
                     WHEN t.status = 'failed' THEN 'failed' \
                     WHEN t.status = 'interrupted' THEN 'interrupted' \
                     WHEN t.status = 'cancelled' THEN 'cancelled' \
                     WHEN r.status = 'cancelled' THEN 'cancelled' \
                     WHEN t.status = 'completed' OR r.status IN ('failed', 'timed_out') THEN 'failed' \
                     ELSE 'in_progress' \
                 END, \
                 NULL, \
                 NULL, \
                 NULL, \
                 t.created_at, \
                 t.created_at, \
                 CASE \
                     WHEN t.status IN ('completed', 'failed', 'interrupted', 'cancelled') \
                       OR r.status IN ('failed', 'timed_out', 'cancelled') THEN t.updated_at \
                     ELSE NULL \
                 END \
             FROM thread_lineage tl \
             JOIN turn t ON t.id = tl.child_turn_id \
             LEFT JOIN task_run r ON r.id = tl.task_run_id \
             LEFT JOIN task_run_execution tre ON tre.task_run_id = tl.task_run_id \
             WHERE NOT EXISTS ( \
                 SELECT 1 FROM task_run_turn existing WHERE existing.turn_id = tl.child_turn_id \
             ) \
               AND NOT EXISTS ( \
                 SELECT 1 \
                 FROM task_run_turn existing_sequence \
                 WHERE existing_sequence.run_id = tl.task_run_id \
                   AND existing_sequence.sequence = 0 \
             )",
        )
        .await?;
    Ok(())
}

async fn backfill_turns_from_execution_without_lineage(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            "INSERT INTO task_run_turn ( \
                 id, task_id, run_id, execution_id, thread_id, turn_id, kind, round, sequence, \
                 status, reviews_candidate_id, requested_by_candidate_id, \
                 requested_by_review_event_id, created_at, started_at, completed_at \
             ) \
             SELECT \
                 'trt_' || tre.child_turn_id, \
                 tre.task_id, \
                 tre.task_run_id, \
                 tre.id, \
                 tre.child_thread_id, \
                 tre.child_turn_id, \
                 'initial', \
                 0, \
                 0, \
                 CASE \
                     WHEN t.status = 'completed' \
                      AND r.status = 'succeeded' \
                      AND r.result_json IS NOT NULL THEN 'candidate_created' \
                     WHEN t.status = 'failed' THEN 'failed' \
                     WHEN t.status = 'interrupted' THEN 'interrupted' \
                     WHEN t.status = 'cancelled' THEN 'cancelled' \
                     WHEN r.status = 'cancelled' THEN 'cancelled' \
                     WHEN t.status = 'completed' OR r.status IN ('failed', 'timed_out') THEN 'failed' \
                     ELSE 'in_progress' \
                 END, \
                 NULL, \
                 NULL, \
                 NULL, \
                 t.created_at, \
                 t.created_at, \
                 CASE \
                     WHEN t.status IN ('completed', 'failed', 'interrupted', 'cancelled') \
                       OR r.status IN ('failed', 'timed_out', 'cancelled') THEN t.updated_at \
                     ELSE NULL \
                 END \
             FROM task_run_execution tre \
             JOIN turn t ON t.id = tre.child_turn_id \
             LEFT JOIN task_run r ON r.id = tre.task_run_id \
             WHERE tre.child_thread_id IS NOT NULL \
               AND tre.child_turn_id IS NOT NULL \
               AND NOT EXISTS ( \
                 SELECT 1 FROM thread_lineage tl WHERE tl.task_run_id = tre.task_run_id \
             ) \
               AND NOT EXISTS ( \
                 SELECT 1 FROM task_run_turn existing WHERE existing.turn_id = tre.child_turn_id \
             ) \
               AND NOT EXISTS ( \
                 SELECT 1 \
                 FROM task_run_turn existing_sequence \
                 WHERE existing_sequence.run_id = tre.task_run_id \
                   AND existing_sequence.sequence = 0 \
             )",
        )
        .await?;
    Ok(())
}

async fn delete_backfilled_task_run_turns(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            "DELETE FROM task_run_turn \
             WHERE id LIKE 'trt_%' \
               AND kind = 'initial' \
               AND round = 0 \
               AND sequence = 0",
        )
        .await?;
    Ok(())
}

async fn backfill_accepted_candidates(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            "INSERT INTO task_result_candidate ( \
                 id, task_id, run_id, task_run_turn_id, thread_id, turn_id, round, status, \
                 result_json, extraction_error_json, summary, diagnostics_json, \
                 final_review_event_id, created_at, updated_at, resolved_at \
             ) \
             SELECT \
                 'trc_' || r.id, \
                 r.task_id, \
                 r.id, \
                 trt.id, \
                 trt.thread_id, \
                 trt.turn_id, \
                 trt.round, \
                 'accepted', \
                 r.result_json, \
                 NULL, \
                 CASE \
                     WHEN json_valid(r.result_json) THEN json_extract(r.result_json, '$.summary') \
                     ELSE NULL \
                 END, \
                 '[]', \
                 'trre_auto_' || r.id, \
                 COALESCE(r.completed_at, trt.completed_at, r.updated_at), \
                 COALESCE(r.completed_at, trt.completed_at, r.updated_at), \
                 COALESCE(r.completed_at, trt.completed_at, r.updated_at) \
             FROM task_run r \
             JOIN task_run_turn trt ON trt.run_id = r.id \
             WHERE r.status = 'succeeded' \
               AND r.result_json IS NOT NULL \
               AND trt.kind IN ('initial', 'revision', 'recovery') \
               AND NOT EXISTS ( \
                 SELECT 1 \
                 FROM task_result_candidate existing \
                 WHERE existing.run_id = r.id \
                   AND existing.status = 'accepted' \
             )",
        )
        .await?;
    Ok(())
}

async fn backfill_runtime_auto_review_events(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            "INSERT INTO task_result_review_event ( \
                 id, candidate_id, task_id, run_id, task_run_turn_id, reviewer_kind, \
                 reviewer_thread_id, reviewer_turn_id, reviewer_user_id, reviewer_agent_spec_id, \
                 event_kind, decision, feedback_text, feedback_json, confidence, \
                 supersedes_review_event_id, next_task_run_turn_id, created_at \
             ) \
             SELECT \
                 c.final_review_event_id, \
                 c.id, \
                 c.task_id, \
                 c.run_id, \
                 c.task_run_turn_id, \
                 'runtime_auto', \
                 NULL, \
                 NULL, \
                 NULL, \
                 NULL, \
                 'system_auto', \
                 'accept', \
                 NULL, \
                 NULL, \
                 NULL, \
                 NULL, \
                 NULL, \
                 COALESCE(c.resolved_at, c.updated_at, c.created_at) \
             FROM task_result_candidate c \
             WHERE c.status = 'accepted' \
               AND c.final_review_event_id IS NOT NULL \
               AND c.final_review_event_id LIKE 'trre_auto_%' \
               AND NOT EXISTS ( \
                 SELECT 1 \
                 FROM task_result_review_event existing \
                 WHERE existing.id = c.final_review_event_id \
             )",
        )
        .await?;
    Ok(())
}

async fn delete_runtime_auto_review_events(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            "DELETE FROM task_result_review_event \
             WHERE id LIKE 'trre_auto_%' \
               AND event_kind = 'system_auto' \
               AND reviewer_kind = 'runtime_auto'",
        )
        .await?;
    Ok(())
}

async fn delete_backfilled_accepted_candidates(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            "DELETE FROM task_result_candidate \
             WHERE id LIKE 'trc_%' \
               AND status = 'accepted'",
        )
        .await?;
    Ok(())
}

async fn create_index<const N: usize>(
    manager: &SchemaManager<'_>,
    name: &str,
    table: &str,
    columns: [&str; N],
    unique: bool,
) -> Result<(), DbErr> {
    let mut index = Index::create();
    index.if_not_exists().name(name).table(Alias::new(table));
    for column in columns {
        index.col(Alias::new(column));
    }
    if unique {
        index.unique();
    }
    manager.create_index(index.to_owned()).await
}

async fn create_partial_index(
    manager: &SchemaManager<'_>,
    name: &str,
    table: &str,
    columns: &str,
    predicate: &str,
) -> Result<(), DbErr> {
    let sql = format!(
        "CREATE UNIQUE INDEX IF NOT EXISTS {name} ON {table} ({columns}) WHERE {predicate}"
    );
    manager
        .get_connection()
        .execute_unprepared(sql.as_str())
        .await?;
    Ok(())
}

async fn drop_index(manager: &SchemaManager<'_>, name: &str, table: &str) -> Result<(), DbErr> {
    manager
        .drop_index(
            Index::drop()
                .if_exists()
                .name(name)
                .table(Alias::new(table))
                .to_owned(),
        )
        .await
}
