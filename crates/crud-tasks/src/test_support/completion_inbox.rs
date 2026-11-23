use sea_orm::{ConnectionTrait, DbBackend, Statement};

pub async fn seed(db: &impl ConnectionTrait) {
    db.execute_unprepared("UPDATE task_inbox_activation SET started_at=1792108800 WHERE id=1; UPDATE workspace SET is_current=FALSE WHERE is_current")
        .await
        .unwrap();
    // ACL membership tables enforce 21-character alphanumeric IDs. Keep the
    // fixture relationships valid without relaxing the migrated schema.
    for sql in [
        "INSERT INTO gateway_identity(id,singleton_key,identity_bootstrap_version,created_at,updated_at) VALUES('G00000000000000000001',1,1,'2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00')",
        "INSERT INTO gateway_principal(id,gateway_id,kind,role_key,status,display_name,nickname,nickname_key,created_at,updated_at) VALUES('P0000000000000000000A','G00000000000000000001','user','member','active','A','a','a','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00'),('P0000000000000000000B','G00000000000000000001','user','member','active','B','b','b','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00')",
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('W0000000000000000000I','Inbox',TRUE,TRUE),('W0000000000000000000O','Other',TRUE,FALSE)",
        "INSERT INTO thread(id,workspace_id,name,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('T0000000000000000000P','W0000000000000000000I','Public thread','','agent','m','p','active','user','workspace','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00'),('T0000000000000000000V','W0000000000000000000I','Private thread','','agent','m','p','active','user','private','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00'),('T0000000000000000000I','W0000000000000000000I',NULL,'','agent','m','p','active','task_run','internal','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00')",
        "INSERT INTO workspace_membership(principal_id,workspace_id,granted_by_actor_kind,created_at,updated_at) VALUES('P0000000000000000000A','W0000000000000000000I','system','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00'),('P0000000000000000000B','W0000000000000000000I','system','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00')",
        "INSERT INTO thread_membership(principal_id,thread_id,added_by_actor_kind,created_at,updated_at) VALUES('P0000000000000000000A','T0000000000000000000V','system','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00'),('P0000000000000000000B','T0000000000000000000V','system','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00')",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
}

pub async fn task(
    db: &impl ConnectionTrait,
    id: &str,
    thread: Option<&str>,
    root: Option<&str>,
    executor: &str,
    mode: &str,
    attachment: &str,
) {
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,root_task_id,executor_kind,status,title,goal,priority,delivery_policy_json,lifecycle_policy_json,revision,created_at,updated_at) VALUES($1,'W0000000000000000000I','workspace','W0000000000000000000I',$2,$3,$4,'completed','Task title','Goal',0,$5::text::json,$6::text::jsonb,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [id.into(),thread.into(),root.into(),executor.into(),serde_json::json!({"mode":mode,"includeResult":true,"format":"summary"}).to_string().into(),serde_json::json!({"attachment":attachment,"onParentCancel":"cancel","onParentFailure":"cancel","completion":"complete_on_terminal_run"}).to_string().into()])).await.unwrap();
}
pub async fn run(db: &impl ConnectionTrait, id: &str, task: &str, status: &str, number: i64) {
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind,completed_at,result_json,created_at,updated_at) VALUES($1,$2,$3,1,$4,$5,(SELECT executor_kind FROM task WHERE id=$2),'2026-10-16 10:00:00 +00:00',$6,CURRENT_TIMESTAMP,'2099-01-01 00:00:00 +00:00')",
        [id.into(),task.into(),id.into(),number.into(),status.into(),serde_json::json!({"summary":"summary","data":{"html":"not for the list"},"artifacts":[]}).to_string().into()])).await.unwrap();
}
