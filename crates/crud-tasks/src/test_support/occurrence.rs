use pioneer_entity::task_run;
use sea_orm::{ConnectionTrait, EntityTrait, Set};

pub async fn seed_occurrence_pair(db: &impl ConnectionTrait) {
    let now = chrono::Utc::now().fixed_offset();
    db.execute_unprepared(
        "INSERT INTO workspace(id,name) VALUES ('occurrence-ws','Occurrence fixtures');
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
         VALUES ('parent-thread','occurrence-ws','','agent','model','provider','active');
         INSERT INTO task(id,workspace_id,owner_kind,owner_id,executor_kind,status,title,goal)
         VALUES ('occurrence-task','occurrence-ws','thread','parent-thread','system','running','Task','test');",
    ).await.unwrap();
    task_run::Entity::insert(task_run::ActiveModel {
        id: Set("occurrence-run".into()),
        task_id: Set("occurrence-task".into()),
        run_group_id: Set("occurrence-run".into()),
        attempt_number: Set(1),
        run_number: Set(1),
        status: Set("running".into()),
        executor_kind: Set("system".into()),
        created_at: Set(now),
        updated_at: Set(now),
        ..Default::default()
    })
    .exec_without_returning(db)
    .await
    .unwrap();
}
