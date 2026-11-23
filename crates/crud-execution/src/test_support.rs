use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};

pub const EXECUTION_ID: &str = "E00000000000000000001";

pub async fn seed_agent_execution(db: &impl ConnectionTrait) {
    db.execute_unprepared(
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('outbox','Outbox',TRUE,FALSE);
         INSERT INTO agent_identity(id,workspace_id,source_kind,source_id,source_fingerprint)
         VALUES('I00000000000000000001','outbox','ephemeral','fixture','fixture');",
    )
    .await
    .unwrap();
    insert_agent_execution(db, EXECUTION_ID).await;
}

pub async fn insert_agent_execution(db: &impl ConnectionTrait, id: &str) {
    db.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Postgres,
        "INSERT INTO agent_execution(id,workspace_id,agent_identity_id,identity_source_revision,identity_source_fingerprint,home_root_thread_id,work_graph_root_execution_id,requested_identity_selection_json,requested_profile_selection_json,authorization_context_fingerprint)
         VALUES($1,'outbox','I00000000000000000001',1,'fixture','fixture-thread',$1,'{}','{}','fixture')",
        [id.into()],
    )).await.unwrap();
}
