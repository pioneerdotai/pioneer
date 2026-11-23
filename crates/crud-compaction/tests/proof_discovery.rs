use anyhow::{Context, Result};
use pioneer_crud_compaction::proof_discovery::{proof_candidate_page, proof_candidate_upper_bound};
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, DbBackend, Statement};
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn sparse_unsealed_pages_seek_past_seals_and_keep_parameterized_order() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            "INSERT INTO workspace(id,name) VALUES('ws','fixture');
             INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
               VALUES('thread','ws','','agent','m','p','active');
             INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version)
               VALUES('owner','ws','thread',1);
             INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms,next_portion)
               VALUES('raw','owner','raw','failed','{}',1000,0);
             INSERT INTO compaction_checkpoint
               (id,operation_id,owner,portion,summary,identity_sha256,selection,projection_version,format_version,status,proof_version)
               SELECT 'cp-'||lpad(i::text,4,'0'),'raw','owner',i,'summary','identity','{}',1,1,'candidate',
                      CASE WHEN i % 8 = 7 OR i = 64 THEN 0 ELSE 1 END
               FROM generate_series(0,529) i;
             ANALYZE compaction_checkpoint;",
        )
        .await?;
        let unsealed: Vec<_> = (0..530)
            .filter(|i| i % 8 == 7 || *i == 64)
            .map(|i| format!("cp-{i:04}"))
            .collect();
        let mut reader = fixture.connect_read_only().await?;
        let statements = Arc::new(Mutex::new(Vec::<Statement>::new()));
        let recorded = statements.clone();
        reader.set_metric_callback(move |info| {
            recorded.lock().unwrap().push(info.statement.clone());
        });

        let upper = proof_candidate_upper_bound(&reader).await?.context("upper checkpoint")?;
        let first = proof_candidate_page(&reader, None, "", &upper, 64)
            .await?
            .context("first page")?;
        assert_eq!(first.last_id, unsealed[63]);
        assert_eq!(first.eligible, unsealed[..64]);
        let second = proof_candidate_page(&reader, None, &first.last_id, &upper, 64)
            .await?
            .context("second page")?;
        assert_eq!(second.last_id, *unsealed.last().unwrap());
        assert_eq!(second.eligible, unsealed[64..]);

        let mut statement = statements.lock().unwrap().last().cloned().context("seek query")?;
        statement.sql = format!("EXPLAIN (COSTS OFF) {}", statement.sql);
        assert_eq!(statement.db_backend, DbBackend::Postgres);
        let details = reader
            .query_all_raw(statement)
            .await?
            .iter()
            .map(|row| row.try_get::<String>("", "QUERY PLAN"))
            .collect::<Result<Vec<_>, _>>()?;
        assert!(
            details.iter().any(|line| line.contains("Index Cond") && line.contains("id >")),
            "{details:#?}"
        );
        assert!(
            details.iter().any(|line| {
                (line.contains("Index Cond") || line.contains("Filter"))
                    && line.contains("proof_version = 0")
            }),
            "{details:#?}"
        );
        assert!(details.iter().all(|line| !line.contains("Seq Scan")), "{details:#?}");
        assert!(
            proof_candidate_page(&reader, None, &second.last_id, &upper, 64).await?.is_none()
        );
        reader.close().await?;
        Ok(())
    })
    .await
}
