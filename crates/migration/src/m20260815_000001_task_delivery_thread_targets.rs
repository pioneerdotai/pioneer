use sea_orm_migration::sea_orm::QueryResult;
use sea_orm_migration::{prelude::*, schema::string};
use serde_json::{Map, Value};

const TASK: &str = "task";
const TASK_DELIVERY: &str = "task_delivery";
const TASK_EXECUTION_ADMISSION: &str = "task_execution_admission";
const THREAD_TARGET: &str = "thread_target";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new(TASK_DELIVERY))
                    .add_column(string(THREAD_TARGET).string_len(32).null().check((
                        "ck_task_delivery_thread_target_value",
                        Expr::cust(
                            "thread_target IS NULL OR thread_target IN (\
                                 'origin_thread', 'current_thread', \
                                 'collaboration_root', 'exact_thread')",
                        ),
                    )))
                    .to_owned(),
            )
            .await?;

        migrate_task_policies_up(manager).await?;
        migrate_delivery_rows_up(manager).await?;
        validate_delivery_rows(manager).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        migrate_task_policies_down(manager).await?;
        migrate_delivery_rows_down(manager).await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new(TASK_DELIVERY))
                    .drop_column(Alias::new(THREAD_TARGET))
                    .to_owned(),
            )
            .await
    }

    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }
}

async fn migrate_task_policies_up(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for row in task_policy_rows(manager).await? {
        let task_id = row.try_get::<String>("", "id")?;
        let owner_kind = row.try_get::<String>("", "owner_kind")?;
        let owner_id = row.try_get::<Option<String>>("", "owner_id")?;
        let created_by_thread_id = row.try_get::<Option<String>>("", "created_by_thread_id")?;
        let admission_root_thread_id =
            row.try_get::<Option<String>>("", "admission_root_thread_id")?;
        let policy_json = row.try_get::<String>("", "delivery_policy_json")?;
        let mut policy = parse_policy(task_id.as_str(), policy_json.as_str())?;
        let mode = policy_string(&policy, "mode", task_id.as_str())?;

        match mode.as_str() {
            "owner_thread" => {
                let thread_id = admission_root_thread_id
                    .as_deref()
                    .or_else(|| {
                        (owner_kind == "thread")
                            .then_some(owner_id.as_deref())
                            .flatten()
                    })
                    .or(created_by_thread_id.as_deref())
                    .ok_or_else(|| {
                        DbErr::Custom(format!(
                            "Task `{task_id}` owner_thread delivery has no resolvable origin thread"
                        ))
                    })?
                    .to_owned();
                policy.insert("mode".to_owned(), Value::String("thread".to_owned()));
                policy.insert(
                    "threadTarget".to_owned(),
                    Value::String("origin_thread".to_owned()),
                );
                policy.insert("threadId".to_owned(), Value::String(thread_id));
            }
            "thread" => {
                let target = non_empty_policy_string(&policy, "threadTarget")
                    .unwrap_or("exact_thread")
                    .to_owned();
                validate_thread_target(target.as_str(), task_id.as_str())?;
                let thread_id = non_empty_policy_string(&policy, "threadId")
                    .ok_or_else(|| {
                        DbErr::Custom(format!(
                            "Task `{task_id}` thread delivery has no concrete threadId"
                        ))
                    })?
                    .to_owned();
                policy.insert("threadTarget".to_owned(), Value::String(target));
                policy.insert("threadId".to_owned(), Value::String(thread_id));
            }
            "none" | "user_notification" | "webhook" => {
                policy.remove("threadTarget");
                policy.remove("threadId");
            }
            other => {
                return Err(DbErr::Custom(format!(
                    "Task `{task_id}` has unknown delivery mode `{other}`"
                )));
            }
        }

        update_task_policy(manager, task_id.as_str(), policy).await?;
    }
    Ok(())
}

async fn migrate_task_policies_down(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    for row in task_policy_rows(manager).await? {
        let task_id = row.try_get::<String>("", "id")?;
        let policy_json = row.try_get::<String>("", "delivery_policy_json")?;
        let mut policy = parse_policy(task_id.as_str(), policy_json.as_str())?;
        if policy_string(&policy, "mode", task_id.as_str())? == "thread" {
            match non_empty_policy_string(&policy, "threadTarget") {
                Some("origin_thread") => {
                    policy.insert("mode".to_owned(), Value::String("owner_thread".to_owned()));
                    policy.remove("threadId");
                }
                Some(target) => validate_thread_target(target, task_id.as_str())?,
                None => {}
            }
            policy.remove("threadTarget");
        }
        update_task_policy(manager, task_id.as_str(), policy).await?;
    }
    Ok(())
}

async fn task_policy_rows(manager: &SchemaManager<'_>) -> Result<Vec<QueryResult>, DbErr> {
    let query = Query::select()
        .column((Alias::new(TASK), Alias::new("id")))
        .column((Alias::new(TASK), Alias::new("owner_kind")))
        .column((Alias::new(TASK), Alias::new("owner_id")))
        .column((Alias::new(TASK), Alias::new("created_by_thread_id")))
        .column((Alias::new(TASK), Alias::new("delivery_policy_json")))
        .expr_as(
            Expr::col((
                Alias::new(TASK_EXECUTION_ADMISSION),
                Alias::new("root_thread_id"),
            )),
            Alias::new("admission_root_thread_id"),
        )
        .from(Alias::new(TASK))
        .left_join(
            Alias::new(TASK_EXECUTION_ADMISSION),
            Expr::col((Alias::new(TASK_EXECUTION_ADMISSION), Alias::new("task_id")))
                .equals((Alias::new(TASK), Alias::new("id"))),
        )
        .and_where(Expr::col((Alias::new(TASK), Alias::new("delivery_policy_json"))).is_not_null())
        .to_owned();
    manager.get_connection().query_all(&query).await
}

fn parse_policy(task_id: &str, policy_json: &str) -> Result<Map<String, Value>, DbErr> {
    serde_json::from_str::<Value>(policy_json)
        .map_err(|error| {
            DbErr::Custom(format!(
                "Task `{task_id}` delivery policy is invalid: {error}"
            ))
        })?
        .as_object()
        .cloned()
        .ok_or_else(|| DbErr::Custom(format!("Task `{task_id}` delivery policy is not an object")))
}

fn policy_string(policy: &Map<String, Value>, field: &str, task_id: &str) -> Result<String, DbErr> {
    non_empty_policy_string(policy, field)
        .map(str::to_owned)
        .ok_or_else(|| DbErr::Custom(format!("Task `{task_id}` delivery policy has no `{field}`")))
}

fn non_empty_policy_string<'a>(policy: &'a Map<String, Value>, field: &str) -> Option<&'a str> {
    policy
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn validate_thread_target(target: &str, task_id: &str) -> Result<(), DbErr> {
    if matches!(
        target,
        "origin_thread" | "current_thread" | "collaboration_root" | "exact_thread"
    ) {
        Ok(())
    } else {
        Err(DbErr::Custom(format!(
            "Task `{task_id}` has unknown threadTarget `{target}`"
        )))
    }
}

async fn update_task_policy(
    manager: &SchemaManager<'_>,
    task_id: &str,
    policy: Map<String, Value>,
) -> Result<(), DbErr> {
    let policy_json = serde_json::to_string(&Value::Object(policy)).map_err(|error| {
        DbErr::Custom(format!("failed to encode Task `{task_id}` policy: {error}"))
    })?;
    manager
        .execute(
            Query::update()
                .table(Alias::new(TASK))
                .value(Alias::new("delivery_policy_json"), policy_json)
                .and_where(Expr::col(Alias::new("id")).eq(task_id))
                .to_owned(),
        )
        .await?;
    Ok(())
}

async fn migrate_delivery_rows_up(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            "UPDATE task_delivery \
             SET thread_target = CASE mode \
                     WHEN 'owner_thread' THEN 'origin_thread' \
                     WHEN 'thread' THEN 'exact_thread' \
                 END, \
                 mode = 'thread', \
                 delivery_key = task_id || ':' || run_id || ':thread:' || \
                     CASE mode \
                         WHEN 'owner_thread' THEN 'origin_thread' \
                         ELSE 'exact_thread' \
                     END || ':' || target_thread_id \
             WHERE mode IN ('owner_thread', 'thread')",
        )
        .await?;
    Ok(())
}

async fn migrate_delivery_rows_down(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(
            "UPDATE task_delivery \
             SET mode = CASE thread_target \
                     WHEN 'origin_thread' THEN 'owner_thread' \
                     ELSE 'thread' \
                 END, \
                 delivery_key = task_id || ':' || run_id || ':' || \
                     CASE thread_target \
                         WHEN 'origin_thread' THEN 'owner_thread' \
                         ELSE 'thread' \
                     END || ':' || target_thread_id \
             WHERE mode = 'thread'",
        )
        .await?;
    Ok(())
}

async fn validate_delivery_rows(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let query = Query::select()
        .expr_as(Func::count(Expr::col(Asterisk)), Alias::new("count"))
        .from(Alias::new(TASK_DELIVERY))
        .and_where(Expr::cust(
            "(mode = 'thread' AND (\
                thread_target NOT IN ('origin_thread', 'current_thread', \
                    'collaboration_root', 'exact_thread') \
                OR thread_target IS NULL \
                OR target_thread_id IS NULL \
                OR trim(target_thread_id) = ''\
             )) OR (mode != 'thread' AND thread_target IS NOT NULL)",
        ))
        .to_owned();
    let count = manager
        .get_connection()
        .query_one(&query)
        .await?
        .map(|row| row.try_get::<i64>("", "count"))
        .transpose()?
        .unwrap_or(0);
    if count == 0 {
        Ok(())
    } else {
        Err(DbErr::Custom(format!(
            "task_delivery contains {count} row(s) outside the canonical thread target contract"
        )))
    }
}
