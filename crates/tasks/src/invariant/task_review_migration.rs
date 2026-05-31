use anyhow::Result;
use sea_orm::ConnectionTrait;

use super::{
    TaskRuntimeInvariantReport, TaskRuntimeInvariantViolation, TaskRuntimeInvariantViolationKind,
    get_optional_string, get_string, query_all, table_exists,
};

pub(super) async fn detect_migration_violations<C: ConnectionTrait>(
    db: &C,
    report: &mut TaskRuntimeInvariantReport,
) -> Result<()> {
    if !target_tables_exist(db).await? {
        return Ok(());
    }

    detect_missing_primary_executor_bindings(db, report).await?;
    detect_primary_executor_binding_violations(db, report).await?;
    detect_task_run_turn_reference_violations(db, report).await?;
    detect_accepted_candidate_violations(db, report).await?;
    Ok(())
}

async fn target_tables_exist<C: ConnectionTrait>(db: &C) -> Result<bool> {
    for table in [
        "task_run_thread_binding",
        "task_run_turn",
        "task_result_candidate",
        "task_result_review_event",
    ] {
        if !table_exists(db, table).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn detect_missing_primary_executor_bindings<C: ConnectionTrait>(
    db: &C,
    report: &mut TaskRuntimeInvariantReport,
) -> Result<()> {
    let rows = query_all(
        db,
        r#"
        select
            tl.task_id as task_id,
            tl.task_run_id as run_id,
            tl.child_thread_id as child_thread_id
        from thread_lineage tl
        left join task_run_thread_binding b
          on b.run_id = tl.task_run_id
         and b.binding_kind = 'primary_executor'
        where b.id is null
        order by tl.created_at asc
        "#,
    )
    .await?;

    for row in rows {
        let task_id = get_string(&row, "task_id")?;
        let run_id = get_string(&row, "run_id")?;
        let child_thread_id = get_string(&row, "child_thread_id")?;
        report.push(TaskRuntimeInvariantViolation::new(
            TaskRuntimeInvariantViolationKind::MissingPrimaryExecutorBinding {
                task_id,
                run_id,
                child_thread_id,
            },
            "thread_lineage task run row has no primary_executor task_run_thread_binding",
        ));
    }

    if !table_exists(db, "task_run_execution").await? {
        return Ok(());
    }

    let execution_only_rows = query_all(
        db,
        r#"
        select
            tre.task_id as task_id,
            tre.task_run_id as run_id,
            tre.child_thread_id as child_thread_id
        from task_run_execution tre
        left join task_run_thread_binding b
          on b.run_id = tre.task_run_id
         and b.binding_kind = 'primary_executor'
        where tre.executor_kind = 'agent'
          and tre.child_thread_id is not null
          and tre.child_turn_id is not null
          and b.id is null
          and not exists (
            select 1 from thread_lineage tl where tl.task_run_id = tre.task_run_id
          )
        order by tre.created_at asc
        "#,
    )
    .await?;

    for row in execution_only_rows {
        let task_id = get_string(&row, "task_id")?;
        let run_id = get_string(&row, "run_id")?;
        let child_thread_id = get_string(&row, "child_thread_id")?;
        report.push(TaskRuntimeInvariantViolation::new(
            TaskRuntimeInvariantViolationKind::MissingPrimaryExecutorBinding {
                task_id,
                run_id,
                child_thread_id,
            },
            "task_run_execution agent row has no primary_executor task_run_thread_binding",
        ));
    }
    Ok(())
}

async fn detect_primary_executor_binding_violations<C: ConnectionTrait>(
    db: &C,
    report: &mut TaskRuntimeInvariantReport,
) -> Result<()> {
    let missing_lineage = query_all(
        db,
        r#"
        select
            b.id as binding_id,
            b.task_id as task_id,
            b.run_id as run_id,
            b.thread_id as thread_id
        from task_run_thread_binding b
        left join thread_lineage tl on tl.child_thread_id = b.thread_id
        where tl.child_thread_id is null
        order by b.created_at asc
        "#,
    )
    .await?;

    for row in missing_lineage {
        let task_id = get_string(&row, "task_id")?;
        let run_id = get_string(&row, "run_id")?;
        let binding_id = get_string(&row, "binding_id")?;
        let thread_id = get_string(&row, "thread_id")?;
        report.push(TaskRuntimeInvariantViolation::new(
            TaskRuntimeInvariantViolationKind::PrimaryExecutorBindingMissingLineage {
                task_id,
                run_id,
                binding_id,
                thread_id,
            },
            "task_run_thread_binding.thread_id has no thread_lineage graph row",
        ));
    }

    let missing_execution = query_all(
        db,
        r#"
        select
            id as binding_id,
            task_id as task_id,
            run_id as run_id,
            thread_id as thread_id
        from task_run_thread_binding
        where binding_kind = 'primary_executor'
          and execution_id is null
        order by created_at asc
        "#,
    )
    .await?;

    for row in missing_execution {
        let task_id = get_string(&row, "task_id")?;
        let run_id = get_string(&row, "run_id")?;
        let binding_id = get_string(&row, "binding_id")?;
        let thread_id = get_string(&row, "thread_id")?;
        report.push(TaskRuntimeInvariantViolation::new(
            TaskRuntimeInvariantViolationKind::PrimaryExecutorBindingMissingExecution {
                task_id,
                run_id,
                binding_id,
                thread_id,
            },
            "primary_executor task_run_thread_binding has no execution_id",
        ));
    }

    let duplicate_primary = query_all(
        db,
        r#"
        select
            min(task_id) as task_id,
            run_id as run_id,
            group_concat(id, ',') as binding_ids
        from task_run_thread_binding
        where binding_kind = 'primary_executor'
        group by run_id
        having count(*) > 1
        order by run_id asc
        "#,
    )
    .await?;

    for row in duplicate_primary {
        let task_id = get_string(&row, "task_id")?;
        let run_id = get_string(&row, "run_id")?;
        let binding_ids = get_string(&row, "binding_ids")?
            .split(',')
            .map(str::to_owned)
            .collect::<Vec<_>>();
        report.push(TaskRuntimeInvariantViolation::new(
            TaskRuntimeInvariantViolationKind::MultiplePrimaryExecutorBindingsForRun {
                task_id,
                run_id,
                binding_ids,
            },
            "one task run has multiple primary_executor task_run_thread_binding rows",
        ));
    }
    Ok(())
}

async fn detect_task_run_turn_reference_violations<C: ConnectionTrait>(
    db: &C,
    report: &mut TaskRuntimeInvariantReport,
) -> Result<()> {
    let missing_lineage_turn = query_all(
        db,
        r#"
        select
            tl.task_id as task_id,
            tl.task_run_id as run_id,
            tl.child_thread_id as child_thread_id,
            tl.child_turn_id as child_turn_id
        from thread_lineage tl
        left join turn t on t.id = tl.child_turn_id
        where t.id is null
        order by tl.created_at asc
        "#,
    )
    .await?;

    for row in missing_lineage_turn {
        let task_id = get_string(&row, "task_id")?;
        let run_id = get_string(&row, "run_id")?;
        let child_thread_id = get_string(&row, "child_thread_id")?;
        let child_turn_id = get_string(&row, "child_turn_id")?;
        report.push(TaskRuntimeInvariantViolation::new(
            TaskRuntimeInvariantViolationKind::LineageChildTurnMissingTurn {
                task_id,
                run_id,
                child_thread_id,
                child_turn_id,
            },
            "thread_lineage.child_turn_id does not reference an existing turn row",
        ));
    }

    let lineage_missing_task_run_turn = query_all(
        db,
        r#"
        select
            tl.task_id as task_id,
            tl.task_run_id as run_id,
            tl.child_thread_id as child_thread_id,
            tl.child_turn_id as child_turn_id
        from thread_lineage tl
        join turn t on t.id = tl.child_turn_id
        left join task_run_turn trt
          on trt.run_id = tl.task_run_id
         and trt.turn_id = tl.child_turn_id
        where trt.id is null
        order by tl.created_at asc
        "#,
    )
    .await?;

    for row in lineage_missing_task_run_turn {
        let task_id = get_string(&row, "task_id")?;
        let run_id = get_string(&row, "run_id")?;
        let child_thread_id = get_string(&row, "child_thread_id")?;
        let child_turn_id = get_string(&row, "child_turn_id")?;
        report.push(TaskRuntimeInvariantViolation::new(
            TaskRuntimeInvariantViolationKind::LineageMissingTaskRunTurn {
                task_id,
                run_id,
                child_thread_id,
                child_turn_id,
            },
            "thread_lineage child turn has no task_run_turn target row",
        ));
    }

    if table_exists(db, "task_run_execution").await? {
        let missing_execution_turn = query_all(
            db,
            r#"
            select
                tre.id as execution_id,
                tre.task_id as task_id,
                tre.task_run_id as run_id,
                tre.child_thread_id as child_thread_id,
                tre.child_turn_id as child_turn_id
            from task_run_execution tre
            left join turn t on t.id = tre.child_turn_id
            where tre.executor_kind = 'agent'
              and tre.child_turn_id is not null
              and t.id is null
              and not exists (
                select 1 from thread_lineage tl where tl.task_run_id = tre.task_run_id
              )
            order by tre.created_at asc
            "#,
        )
        .await?;

        for row in missing_execution_turn {
            let task_id = get_string(&row, "task_id")?;
            let run_id = get_string(&row, "run_id")?;
            let execution_id = get_string(&row, "execution_id")?;
            let child_thread_id = get_optional_string(&row, "child_thread_id")?;
            let child_turn_id = get_string(&row, "child_turn_id")?;
            report.push(TaskRuntimeInvariantViolation::new(
                TaskRuntimeInvariantViolationKind::ExecutionChildTurnMissingTurn {
                    task_id,
                    run_id,
                    execution_id,
                    child_thread_id,
                    child_turn_id,
                },
                "task_run_execution.child_turn_id does not reference an existing turn row",
            ));
        }

        let execution_missing_task_run_turn = query_all(
            db,
            r#"
            select
                tre.id as execution_id,
                tre.task_id as task_id,
                tre.task_run_id as run_id,
                tre.child_thread_id as child_thread_id,
                tre.child_turn_id as child_turn_id
            from task_run_execution tre
            join turn t on t.id = tre.child_turn_id
            left join task_run_turn trt
              on trt.run_id = tre.task_run_id
             and trt.turn_id = tre.child_turn_id
            where tre.executor_kind = 'agent'
              and tre.child_turn_id is not null
              and trt.id is null
              and not exists (
                select 1 from thread_lineage tl where tl.task_run_id = tre.task_run_id
              )
            order by tre.created_at asc
            "#,
        )
        .await?;

        for row in execution_missing_task_run_turn {
            let task_id = get_string(&row, "task_id")?;
            let run_id = get_string(&row, "run_id")?;
            let execution_id = get_string(&row, "execution_id")?;
            let child_thread_id = get_optional_string(&row, "child_thread_id")?;
            let child_turn_id = get_string(&row, "child_turn_id")?;
            report.push(TaskRuntimeInvariantViolation::new(
                TaskRuntimeInvariantViolationKind::ExecutionMissingTaskRunTurn {
                    task_id,
                    run_id,
                    execution_id,
                    child_thread_id,
                    child_turn_id,
                },
                "task_run_execution child turn has no task_run_turn target row",
            ));
        }
    }

    let rows = query_all(
        db,
        r#"
        select
            trt.id as task_run_turn_id,
            trt.task_id as task_id,
            trt.run_id as run_id,
            trt.turn_id as turn_id
        from task_run_turn trt
        left join turn t on t.id = trt.turn_id
        where t.id is null
        order by trt.created_at asc
        "#,
    )
    .await?;

    for row in rows {
        let task_id = get_string(&row, "task_id")?;
        let run_id = get_string(&row, "run_id")?;
        let task_run_turn_id = get_string(&row, "task_run_turn_id")?;
        let turn_id = get_string(&row, "turn_id")?;
        report.push(TaskRuntimeInvariantViolation::new(
            TaskRuntimeInvariantViolationKind::TaskRunTurnMissingTurn {
                task_id,
                run_id,
                task_run_turn_id,
                turn_id,
            },
            "task_run_turn.turn_id does not reference an existing turn row",
        ));
    }
    Ok(())
}

async fn detect_accepted_candidate_violations<C: ConnectionTrait>(
    db: &C,
    report: &mut TaskRuntimeInvariantReport,
) -> Result<()> {
    let missing_turn = query_all(
        db,
        r#"
        select
            c.id as candidate_id,
            c.task_id as task_id,
            c.run_id as run_id,
            c.task_run_turn_id as task_run_turn_id
        from task_result_candidate c
        left join task_run_turn trt on trt.id = c.task_run_turn_id
        where c.status = 'accepted'
          and trt.id is null
        order by c.created_at asc
        "#,
    )
    .await?;

    for row in missing_turn {
        let task_id = get_string(&row, "task_id")?;
        let run_id = get_string(&row, "run_id")?;
        let candidate_id = get_string(&row, "candidate_id")?;
        let task_run_turn_id = get_string(&row, "task_run_turn_id")?;
        report.push(TaskRuntimeInvariantViolation::new(
            TaskRuntimeInvariantViolationKind::AcceptedCandidateMissingTurn {
                task_id,
                run_id,
                candidate_id,
                task_run_turn_id,
            },
            "accepted task_result_candidate does not belong to an existing task_run_turn",
        ));
    }

    let missing_accepted_candidate = query_all(
        db,
        r#"
        select
            r.task_id as task_id,
            r.id as run_id
        from task_run r
        join task_run_turn trt on trt.run_id = r.id
        left join task_result_candidate c
          on c.run_id = r.id
         and c.status = 'accepted'
        where r.status = 'succeeded'
          and r.result_json is not null
          and c.id is null
        group by r.task_id, r.id
        order by r.id asc
        "#,
    )
    .await?;

    for row in missing_accepted_candidate {
        let task_id = get_string(&row, "task_id")?;
        let run_id = get_string(&row, "run_id")?;
        report.push(TaskRuntimeInvariantViolation::new(
            TaskRuntimeInvariantViolationKind::SucceededRunMissingAcceptedCandidate {
                task_id,
                run_id,
            },
            "succeeded task_run with result_json has no accepted task_result_candidate",
        ));
    }

    let accepted_missing_result = query_all(
        db,
        r#"
        select
            id as candidate_id,
            task_id as task_id,
            run_id as run_id
        from task_result_candidate
        where status = 'accepted'
          and result_json is null
        order by created_at asc
        "#,
    )
    .await?;

    for row in accepted_missing_result {
        let task_id = get_string(&row, "task_id")?;
        let run_id = get_string(&row, "run_id")?;
        let candidate_id = get_string(&row, "candidate_id")?;
        report.push(TaskRuntimeInvariantViolation::new(
            TaskRuntimeInvariantViolationKind::AcceptedCandidateMissingResult {
                task_id,
                run_id,
                candidate_id,
            },
            "accepted task_result_candidate has no result_json",
        ));
    }

    let missing_review = query_all(
        db,
        r#"
        select
            c.id as candidate_id,
            c.task_id as task_id,
            c.run_id as run_id,
            c.final_review_event_id as final_review_event_id
        from task_result_candidate c
        left join task_result_review_event re
          on re.id = c.final_review_event_id
         and re.candidate_id = c.id
        where c.status = 'accepted'
          and (c.final_review_event_id is null or re.id is null)
        order by c.created_at asc
        "#,
    )
    .await?;

    for row in missing_review {
        let task_id = get_string(&row, "task_id")?;
        let run_id = get_string(&row, "run_id")?;
        let candidate_id = get_string(&row, "candidate_id")?;
        let final_review_event_id = get_optional_string(&row, "final_review_event_id")?;
        report.push(TaskRuntimeInvariantViolation::new(
            TaskRuntimeInvariantViolationKind::AcceptedCandidateMissingFinalReviewEvent {
                task_id,
                run_id,
                candidate_id,
                final_review_event_id,
            },
            "accepted task_result_candidate has no matching final review event",
        ));
    }

    let multiple_accepted = query_all(
        db,
        r#"
        select
            task_id as task_id,
            run_id as run_id,
            group_concat(id, ',') as candidate_ids
        from task_result_candidate
        where status = 'accepted'
        group by task_id, run_id
        having count(*) > 1
        order by run_id asc
        "#,
    )
    .await?;

    for row in multiple_accepted {
        let task_id = get_string(&row, "task_id")?;
        let run_id = get_string(&row, "run_id")?;
        let candidate_ids = get_string(&row, "candidate_ids")?
            .split(',')
            .map(str::to_owned)
            .collect::<Vec<_>>();
        report.push(TaskRuntimeInvariantViolation::new(
            TaskRuntimeInvariantViolationKind::MultipleAcceptedCandidatesForRun {
                task_id,
                run_id,
                candidate_ids,
            },
            "one task run has multiple accepted task_result_candidate rows",
        ));
    }

    Ok(())
}
