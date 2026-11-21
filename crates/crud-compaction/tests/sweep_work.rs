use pioneer_crud_compaction::sweep_work as frozen_sweep_work;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ConnectionTrait, DbBackend, Statement};

async fn seed_scope(db: &impl ConnectionTrait) {
    db.execute_unprepared(
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',TRUE,FALSE)",
    )
    .await
    .unwrap();
    db.execute_unprepared("INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
}

async fn header(db: &SqliteDatabase, id: &str, count: i64, next: i64, ready: i64) {
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,imports_sha256,message_count,import_count,next_ordinal,next_import,ready) VALUES($1,'ws','thread','fixture','fixture',$2,$2,$3,$3,$4)",
        [id.into(),count.into(),next.into(),ready.into()])).await.unwrap();
}

async fn query(db: &SqliteDatabase, text: &str) -> i64 {
    db.query_one_raw(Statement::from_string(DbBackend::Postgres, text))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

async fn exec(db: &SqliteDatabase, text: &str) {
    db.execute_unprepared(text).await.unwrap();
}

async fn work(db: &SqliteDatabase, id: &str, kind: i64) -> frozen_sweep_work::Work {
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT revision FROM compaction_frozen_sweep_work WHERE manifest_id=$1 AND kind=$2",
            [id.into(), kind.into()],
        ))
        .await
        .unwrap()
        .unwrap();
    frozen_sweep_work::Work {
        manifest: id.into(),
        kind,
        revision: row.try_get("", "revision").unwrap(),
    }
}

async fn drain_metadata(db: &SqliteDatabase) {
    for _ in 0..5000 {
        if !frozen_sweep_work::fanout_step(db, None).await.unwrap() {
            return;
        }
    }
    panic!("bounded fixture fanout must finish");
}

#[pioneer_postgres_test::postgres_test]
async fn empty_physical_queue_does_not_hide_interrupted_fanout() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connection();
    seed_scope(writer).await;
    let reader = fixture.connect_read_only().await.unwrap();
    let db = SqliteDatabase::new(reader, writer.clone()).maintenance();
    assert!(matches!(
        frozen_sweep_work::discover(&db, &[], 0, &mut Default::default(), None, false)
            .await
            .unwrap(),
        frozen_sweep_work::Admission::Idle
    ));
    header(&db, "consumer", 2, 2, 0).await;
    for source in ["a", "b"] {
        header(&db, source, 1, 1, 0).await;
    }
    for source in ["a", "b"] {
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO compaction_frozen_span(manifest_id,kind,start,\"end\",source_manifest) VALUES('consumer',0,$1,$2,$3)",
            [i64::from(source=="b").into(),(i64::from(source=="b")+1).into(),source.into()])).await.unwrap();
    }
    let before = work(&db, "b", 0).await.revision;
    assert!(frozen_sweep_work::fanout_step(&db, None).await.unwrap());
    // A failed cursor commit rolls back its enqueue as well.
    exec(&db,"CREATE FUNCTION fixture_fanout_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fanout cursor fault'; END; $$; CREATE TRIGGER fixture_fanout_fault BEFORE UPDATE ON compaction_frozen_sweep_consumer FOR EACH ROW WHEN (NEW.after_source='b') EXECUTE FUNCTION fixture_fanout_fault()").await;
    let mut failed = false;
    for _ in 0..8 {
        let turn = query(
            &db,
            "SELECT revision AS n FROM compaction_frozen_sweep_fanout WHERE id=1",
        )
        .await;
        if frozen_sweep_work::fanout_step(&db, None).await.is_err() {
            assert_eq!(
                query(
                    &db,
                    "SELECT revision AS n FROM compaction_frozen_sweep_fanout WHERE id=1"
                )
                .await,
                turn,
                "failed source delivery rolls back service cursor too"
            );
            failed = true;
            break;
        }
    }
    assert!(failed);
    assert_eq!(work(&db, "b", 0).await.revision, before);
    exec(
        &db,
        "DROP TRIGGER fixture_fanout_fault ON compaction_frozen_sweep_consumer",
    )
    .await;
    drain_metadata(&db).await;
    assert!(work(&db, "b", 0).await.revision > before);
}
