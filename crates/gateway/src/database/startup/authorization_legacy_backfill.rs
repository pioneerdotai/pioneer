use anyhow::{Context, Result};
use pioneer_crud::{
    CrudStore, load_gateway_singleton, scan_authorization_persistence_invariants_cooperative,
};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};
use tracing::{info, warn};

const CLASSIFY_LEGACY_THREADS_SQL: &str = "\
    UPDATE thread \
       SET access_class = 'internal' \
     WHERE id IN ( \
        SELECT id FROM thread \
         WHERE access_class <> 'internal' \
           AND (origin_kind IN ('task_run', 'system') \
                OR sidebar_visibility = 'hidden' \
                OR EXISTS ( \
                    SELECT 1 \
                      FROM thread_lineage \
                     WHERE thread_lineage.child_thread_id = thread.id \
                )) \
         ORDER BY created_at, id \
         LIMIT 64 \
     )";

pub(super) async fn run(crud_store: &CrudStore) -> Result<()> {
    let database = crud_store.database_connection();
    let mut updated_threads = 0_u64;
    loop {
        let Some(updated) = crud_store
            .try_run_low_priority_write(|| backfill_once(&database))
            .await?
        else {
            super::maintenance_checkpoint().await?;
            continue;
        };
        updated_threads = updated_threads.saturating_add(updated);
        if updated == 0 {
            break;
        }
        super::maintenance_checkpoint().await?;
    }
    if updated_threads > 0 {
        info!(
            updated_threads,
            "legacy thread access-class background backfill completed"
        );
    }

    let gateway = match load_gateway_singleton(&database).await {
        Ok(Some(gateway)) => gateway,
        Ok(None) => {
            warn!("authorization background audit skipped because Gateway identity is missing");
            return Ok(());
        }
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "authorization background audit could not load Gateway identity"
            );
            return Err(error.into());
        }
    };
    match scan_authorization_persistence_invariants_cooperative(
        &database,
        &gateway.id,
        128,
        super::maintenance_checkpoint,
    )
    .await
    {
        Ok(report) if report.is_valid() => {
            if report.ineligible_active_learned_versions > 0 {
                info!(
                    ineligible_active_learned_versions = report.ineligible_active_learned_versions,
                    "active learned versions remain Superuser-only after authorization background audit"
                );
            }
        }
        Ok(report) => warn!(
            violations = %report.safe_diagnostic(),
            "Gateway authorization background audit found persistence invariant violations"
        ),
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "Gateway authorization background audit failed"
            );
            return Err(error);
        }
    }
    Ok(())
}

async fn backfill_once(database: &DatabaseConnection) -> Result<u64> {
    let result = database
        .execute_raw(Statement::from_string(
            DatabaseBackend::Sqlite,
            CLASSIFY_LEGACY_THREADS_SQL.to_owned(),
        ))
        .await
        .context("failed to classify legacy internal threads")?;
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use migration::{Migrator, MigratorTrait};
    use sea_orm::{ConnectionTrait, Database};

    use super::*;

    #[tokio::test]
    async fn classifies_legacy_internal_threads_idempotently() {
        let database = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&database, None).await.unwrap();
        database
            .execute_unprepared(
                "INSERT INTO workspace(id,name,is_active,is_current) \
                 VALUES('W00000000000000000001','Workspace',1,1); \
                 INSERT INTO thread(\
                    id,workspace_id,name,preview,mode,model,model_provider,status,origin_kind,\
                    sidebar_visibility,created_at,updated_at) \
                 VALUES('T00000000000000000001','W00000000000000000001','Visible','',\
                    'chat','test','test','active','user','visible',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),\
                       ('T00000000000000000002','W00000000000000000001','Task','',\
                    'agent','test','test','active','task_run','visible',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),\
                       ('T00000000000000000003','W00000000000000000001','System','',\
                    'agent','test','test','active','system','visible',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),\
                       ('T00000000000000000004','W00000000000000000001','Hidden','',\
                    'chat','test','test','active','user','hidden',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),\
                       ('T00000000000000000005','W00000000000000000001','Child','',\
                    'chat','test','test','active','user','visible',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); \
                 INSERT INTO thread_lineage(\
                    child_thread_id,parent_thread_id,root_thread_id,depth,created_at,\
                    origin_kind,created_by_thread_id,created_by_turn_id) \
                 VALUES('T00000000000000000005','T00000000000000000001',\
                    'T00000000000000000001',1,CURRENT_TIMESTAMP,'task_run',\
                    'T00000000000000000001',NULL)",
            )
            .await
            .unwrap();

        assert_eq!(backfill_once(&database).await.unwrap(), 4);
        assert_eq!(backfill_once(&database).await.unwrap(), 0);

        let rows = database
            .query_all_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT id, access_class FROM thread ORDER BY id".to_owned(),
            ))
            .await
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.try_get::<String>("", "access_class").unwrap())
                .collect::<Vec<_>>(),
            ["private", "internal", "internal", "internal", "internal"]
        );
    }
}
