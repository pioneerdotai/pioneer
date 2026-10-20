// Included inside frozen_storage_lifetime::tests to reuse its guarded fixtures.
mod durable_sweep {
    use super::*;
    include!("frozen_sweep_correction_tests.rs");

    async fn query(store: &CrudStore, text: &str) -> i64 {
        store
            .connection
            .query_one_raw(Statement::from_string(DbBackend::Sqlite, text))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "n")
            .unwrap()
    }
    async fn exec(store: &CrudStore, text: &str) {
        store.connection.execute_unprepared(text).await.unwrap();
    }
    async fn fixture_database(before_work: bool) -> sea_orm::DatabaseConnection {
        let mut options = sea_orm::ConnectOptions::new("sqlite::memory:");
        options.max_connections(1).min_connections(1);
        let db = Database::connect(options).await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys=OFF")
            .await
            .unwrap();
        let prefix = Migrator::migrations()
            .iter()
            .position(|m| m.name() == "m20261018_000001_frozen_sweep_work")
            .unwrap();
        Migrator::up(&db, before_work.then_some(prefix as u32))
            .await
            .unwrap();
        db.execute_unprepared(
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',1,1)",
        )
        .await
        .unwrap();
        db.execute_unprepared("INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
        db
    }
    async fn blank(before_work: bool) -> CrudStore {
        CrudStore::new(fixture_database(before_work).await).with_maintenance_access()
    }
    async fn header(store: &CrudStore, id: &str, count: i64, next: i64, ready: i64) {
        store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,imports_sha256,message_count,import_count,next_ordinal,next_import,ready) VALUES(?1,'ws','thread','fixture','fixture',?2,?2,?3,?3,?4)",
            [id.into(),count.into(),next.into(),ready.into()])).await.unwrap();
    }
    async fn header_kind(
        store: &CrudStore,
        id: &str,
        kind: i64,
        count: i64,
        next: i64,
        ready: i64,
    ) {
        let messages = if kind == 0 { count } else { 0 };
        let imports = if kind == 1 { count } else { 0 };
        let message_next = if kind == 0 { next } else { 0 };
        let import_next = if kind == 1 { next } else { 0 };
        store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,imports_sha256,message_count,import_count,next_ordinal,next_import,ready) VALUES(?1,'ws','thread','fixture','fixture',?2,?3,?4,?5,?6)",
            [id.into(),messages.into(),imports.into(),message_next.into(),import_next.into(),ready.into()])).await.unwrap();
    }
    async fn row(store: &CrudStore, id: &str, kind: i64, ordinal: i64) {
        let text = if kind == 0 {
            "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES(?1,?2,'{}',2)"
        } else {
            "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES(?1,?2,0,'event','e-'||?2,'v','thread','{}',2)"
        };
        store
            .connection
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                text,
                [id.into(), ordinal.into()],
            ))
            .await
            .unwrap();
    }
    async fn drain_metadata(store: &CrudStore) {
        for _ in 0..5000 {
            if !frozen_sweep_work::bootstrap_step(store).await.unwrap() {
                break;
            }
        }
        for _ in 0..5000 {
            if !frozen_sweep_work::fanout_step(store).await.unwrap() {
                return;
            }
        }
        panic!("bounded fixture fanout must finish");
    }
    async fn drain(store: &CrudStore, progress: &mut FrozenStorageLifetimeProgress) {
        for _ in 0..10000 {
            match progress.sweep_quantum(store).await {
                Ok(false) => return,
                Ok(true) => {}
                Err(error) if error.downcast_ref::<sea_orm::DbErr>().is_none() => {}
                Err(error) => panic!("unexpected DB error: {error}"),
            }
        }
        panic!("sweep must reach idle, including durable continuations");
    }
    async fn work(store: &CrudStore, id: &str, kind: i64) -> frozen_sweep_work::Work {
        let row = store
            .connection
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT revision FROM compaction_frozen_sweep_work WHERE manifest_id=?1 AND kind=?2",
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
    async fn pending(store: &CrudStore, id: &str, kind: i64) -> i64 {
        store
            .connection
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT pending AS n FROM compaction_frozen_sweep_work WHERE manifest_id=?1 AND kind=?2",
                [id.into(), kind.into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "n")
            .unwrap()
    }

    #[tokio::test]
    async fn bootstrap_groups_both_kinds_unusual_ids_and_atomic_cursor_restart() {
        let db = fixture_database(true).await;
        let store = CrudStore::new(db.clone()).with_maintenance_access();
        let ids = [
            "".to_owned(),
            "a\0'雪".to_owned(),
            format!("z{}", "x".repeat(4096)),
        ];
        // Missing physical headers can have scope inferred from a span endpoint.
        // Keep the old scope prerequisite without hiding these physical groups.
        header(&store, "scope-anchor", 3, 3, 0).await;
        for (start, id) in ids.iter().enumerate() {
            for kind in [0_i64, 1] {
                store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                    "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES('scope-anchor',?1,?2,?3,?4)",
                    [kind.into(),(start as i64).into(),(start as i64+1).into(),id.clone().into()])).await.unwrap();
            }
        }
        for id in &ids {
            for kind in [0, 1] {
                for ordinal in [0, 1, 7] {
                    row(&store, id, kind, ordinal).await;
                }
            }
        }
        Migrator::up(&db, None).await.unwrap();
        assert_eq!(
            query(
                &store,
                "SELECT count(*) AS n FROM compaction_frozen_sweep_work"
            )
            .await,
            0
        );
        assert!(frozen_sweep_work::bootstrap_step(&store).await.unwrap()); // capture upper
        exec(&store,"CREATE TRIGGER fixture_enqueue_fault BEFORE INSERT ON compaction_frozen_sweep_work BEGIN SELECT RAISE(ABORT,'enqueue fault'); END").await;
        assert!(frozen_sweep_work::bootstrap_step(&store).await.is_err());
        assert_eq!(
            query(
                &store,
                "SELECT count(*) AS n FROM compaction_frozen_sweep_bootstrap WHERE after_manifest IS NOT NULL"
            )
            .await,
            0
        );
        exec(&store, "DROP TRIGGER fixture_enqueue_fault").await;
        assert!(frozen_sweep_work::bootstrap_step(&store).await.unwrap());
        assert_eq!(
            query(
                &store,
                "SELECT count(*) AS n FROM compaction_frozen_sweep_work"
            )
            .await,
            1
        );
        // No RAM cursor is needed to resume, including between the two kinds.
        while query(
            &store,
            "SELECT complete AS n FROM compaction_frozen_sweep_bootstrap WHERE kind=0",
        )
        .await
            == 0
        {
            assert!(frozen_sweep_work::bootstrap_step(&store).await.unwrap());
        }
        assert_eq!(
            query(
                &store,
                "SELECT started AS n FROM compaction_frozen_sweep_bootstrap WHERE kind=1"
            )
            .await,
            0
        );
        drain_metadata(&store).await;
        assert_eq!(
            query(
                &store,
                "SELECT count(*) AS n FROM compaction_frozen_sweep_work"
            )
            .await,
            6
        );
        assert!(!frozen_sweep_work::bootstrap_step(&store).await.unwrap());
        let mut restarted = FrozenStorageLifetimeProgress::default();
        drain(&store, &mut restarted).await;
        assert_eq!(query(&store,"SELECT count(*) AS n FROM compaction_frozen_sweep_work WHERE pending=1 AND retry_at>unixepoch()").await,6);
        assert_eq!(
            query(
                &store,
                "SELECT count(*) AS n FROM compaction_frozen_sweep_bootstrap WHERE complete=1"
            )
            .await,
            2
        );
    }

    #[tokio::test]
    async fn inserts_on_both_sides_of_bootstrap_and_after_eof_survive() {
        let db = fixture_database(true).await;
        let store = CrudStore::new(db.clone()).with_maintenance_access();
        for id in ["a", "m", "n", "z", "zz", "after-eof"] {
            header(&store, id, 0, 0, 0).await;
        }
        row(&store, "m", 0, 0).await;
        row(&store, "z", 0, 0).await;
        Migrator::up(&db, None).await.unwrap();
        assert!(frozen_sweep_work::bootstrap_step(&store).await.unwrap());
        assert!(frozen_sweep_work::bootstrap_step(&store).await.unwrap());
        for (id, kind) in [("a", 0), ("n", 0), ("zz", 0), ("a", 1)] {
            row(&store, id, kind, 0).await;
        }
        row(&store, "m", 0, 1).await;
        drain_metadata(&store).await;
        for (id, kind) in [("a", 0), ("m", 0), ("n", 0), ("z", 0), ("zz", 0), ("a", 1)] {
            assert_eq!(pending(&store, id, kind).await, 1);
        }
        row(&store, "after-eof", 1, 0).await;
        assert!(!frozen_sweep_work::bootstrap_step(&store).await.unwrap());
        assert_eq!(pending(&store, "after-eof", 1).await, 1);
        assert_eq!(
            query(
                &store,
                "SELECT count(*) AS n FROM compaction_frozen_sweep_work WHERE manifest_id='m'"
            )
            .await,
            1
        );
    }

    #[tokio::test]
    async fn clean_held_restart_and_revisit_do_not_prepare_again() {
        let store = blank(false).await;
        header(&store, "held", 1, 1, 0).await;
        for kind in [0, 1] {
            row(&store, "held", kind, 0).await;
        }
        let mut progress = FrozenStorageLifetimeProgress::default();
        drain(&store, &mut progress).await;
        assert_eq!(pending(&store, "held", 0).await, 0);
        assert_eq!(pending(&store, "held", 1).await, 0);
        let revision = work(&store, "held", 0).await.revision;
        exec(
            &store,
            "UPDATE compaction_frozen_history SET identity_sha256=identity_sha256 WHERE id='held'",
        )
        .await;
        exec(&store,"UPDATE compaction_frozen_message_data SET reference_json=reference_json,bytes=bytes WHERE manifest_id='held'").await;
        assert_eq!(
            work(&store, "held", 0).await.revision,
            revision,
            "no-op domain writes do not dirty a clean key"
        );
        // Any renewed layout/span preparation would now fail. Durable quiet
        // discovery accesses neither; the real worker observer covers routes.
        exec(
            &store,
            "ALTER TABLE compaction_frozen_layout RENAME TO unavailable_layout",
        )
        .await;
        exec(
            &store,
            "ALTER TABLE compaction_frozen_span RENAME TO unavailable_span",
        )
        .await;
        let mut restart = FrozenStorageLifetimeProgress::default();
        restart.revisit_completed_cleanup(Instant::now(), Duration::from_secs(60));
        restart.revisit_completed_cleanup(
            Instant::now() + Duration::from_secs(61),
            Duration::from_secs(60),
        );
        for _ in 0..3 {
            assert!(!restart.sweep_quantum(&store).await.unwrap());
        }
        assert!(restart.sweep.is_none() && restart.sweep_pending.is_empty());
        assert_eq!(work(&store, "held", 0).await.revision, revision);
    }

    #[tokio::test]
    async fn consumer_fanout_tracks_all_sources_old_new_kind_and_coalesces() {
        let store = blank(false).await;
        for id in ["consumer", "a", "b", "c"] {
            header(&store, id, 3, 3, 0).await;
        }
        for kind in [0, 1] {
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES('consumer',?1,0,0)",[kind.into()])).await.unwrap();
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES('consumer',?1,0,1,'a'),('consumer',?1,1,2,'b')",[kind.into()])).await.unwrap();
        }
        drain_metadata(&store).await;
        let a = work(&store, "a", 0).await.revision;
        let b = work(&store, "b", 0).await.revision;
        // No reverse traversal under the header writer: fanout is durable.
        exec(
            &store,
            "UPDATE compaction_frozen_history SET identity_sha256='changed' WHERE id='consumer'",
        )
        .await;
        assert_eq!(work(&store, "a", 0).await.revision, a);
        assert!(
            query(
                &store,
                "SELECT count(*) AS n FROM compaction_frozen_sweep_consumer WHERE pending=1"
            )
            .await
                > 0
        );
        drain_metadata(&store).await;
        assert!(work(&store, "a", 0).await.revision > a);
        assert!(work(&store, "b", 0).await.revision > b);
        let old = work(&store, "b", 0).await.revision;
        let new = work(&store, "c", 1).await.revision;
        // Inactive incomplete layouts permit repair/retarget; ready published
        // fixtures would correctly reject this transition.
        exec(&store,"UPDATE compaction_frozen_span SET source_manifest='c',kind=1,start=2,end=3 WHERE manifest_id='consumer' AND kind=0 AND start=1").await;
        assert!(work(&store, "b", 0).await.revision > old);
        assert!(work(&store, "c", 1).await.revision > new);
        exec(&store,"DELETE FROM compaction_frozen_span WHERE manifest_id='consumer' AND kind=1 AND start=2").await;
        drain_metadata(&store).await;
        assert_eq!(
            query(
                &store,
                "SELECT count(*) AS n FROM compaction_frozen_sweep_work WHERE manifest_id='a' AND kind=0"
            )
            .await,
            1
        );
        // A second consumer shares the same key, without an event journal.
        header(&store, "consumer2", 1, 1, 0).await;
        exec(&store,"INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES('consumer2',0,0,1,'a')").await;
        drain_metadata(&store).await;
        assert_eq!(
            query(
                &store,
                "SELECT count(*) AS n FROM compaction_frozen_sweep_work WHERE manifest_id='a' AND kind=0"
            )
            .await,
            1
        );
    }

    #[tokio::test]
    async fn candidate_without_spans_tracks_old_new_failure_and_removal_both_kinds() {
        let store = blank(false).await;
        for id in ["a", "b"] {
            header(&store, id, 0, 0, 1).await;
        }
        header(&store, "capture", 0, 0, 0).await;
        drain_metadata(&store).await;
        for kind in [0, 1] {
            let a = work(&store, "a", kind).await.revision;
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO compaction_frozen_layout(manifest_id,kind,candidate) VALUES('capture',?1,'a')",[kind.into()])).await.unwrap();
            assert!(work(&store, "a", kind).await.revision > a);
            let a = work(&store, "a", kind).await.revision;
            let b = work(&store, "b", kind).await.revision;
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "UPDATE compaction_frozen_layout SET candidate='b' WHERE manifest_id='capture' AND kind=?1",[kind.into()])).await.unwrap();
            assert!(work(&store, "a", kind).await.revision > a);
            assert!(work(&store, "b", kind).await.revision > b);
            let b = work(&store, "b", kind).await.revision;
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "UPDATE compaction_frozen_layout SET failed=1 WHERE manifest_id='capture' AND kind=?1",[kind.into()])).await.unwrap();
            assert!(work(&store, "b", kind).await.revision > b);
            let b = work(&store, "b", kind).await.revision;
            store
                .connection
                .execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "DELETE FROM compaction_frozen_layout WHERE manifest_id='capture' AND kind=?1",
                    [kind.into()],
                ))
                .await
                .unwrap();
            assert!(work(&store, "b", kind).await.revision > b);
        }
        assert_eq!(
            query(&store, "SELECT count(*) AS n FROM compaction_frozen_span").await,
            0
        );
    }

    #[tokio::test]
    async fn redirected_append_tracks_actual_physical_destination_and_rollback() {
        for kind in [0, 1] {
            let store = blank(false).await;
            header_kind(&store, "backing", kind, 1, 1, 1).await;
            header_kind(&store, "capture", kind, 2, 1, 0).await;
            row(&store, "backing", kind, 0).await;
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES('capture',?1,0,0)",[kind.into()])).await.unwrap();
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES('capture',?1,0,1,'backing')",[kind.into()])).await.unwrap();
            exec(
                &store,
                "UPDATE compaction_frozen_layout SET active=1 WHERE manifest_id='capture'",
            )
            .await;
            drain_metadata(&store).await;
            let old = work(&store, "backing", kind).await.revision;
            for commit in [false, true] {
                let tx = store.connection.begin().await.unwrap();
                let source = super::super::super::compaction_frozen_storage::append_source(
                    &tx, "capture", kind, 1,
                )
                .await
                .unwrap();
                assert_eq!(source, "backing");
                let text = if kind == 0 {
                    "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES(?1,1,'{}',2)"
                } else {
                    "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES(?1,1,0,'event','tail','v','thread','{}',2)"
                };
                tx.execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    text,
                    [source.into()],
                ))
                .await
                .unwrap();
                let field = if kind == 0 {
                    "next_ordinal"
                } else {
                    "next_import"
                };
                tx.execute_unprepared(&format!(
                    "UPDATE compaction_frozen_history SET {field}=2 WHERE id='capture'"
                ))
                .await
                .unwrap();
                if commit {
                    tx.commit().await.unwrap();
                } else {
                    tx.rollback().await.unwrap();
                    assert_eq!(work(&store, "backing", kind).await.revision, old);
                }
            }
            assert!(work(&store, "backing", kind).await.revision > old);
            let (table, _) = physical_table(kind);
            assert_eq!(
                query(
                    &store,
                    &format!("SELECT count(*) AS n FROM {table} WHERE manifest_id='backing'")
                )
                .await,
                2
            );
        }
    }

    #[tokio::test]
    async fn stale_preparation_and_ack_aba_overflow_and_no_duplicate_active_key() {
        let store = blank(false).await;
        header(&store, "key", 1, 1, 0).await;
        row(&store, "key", 0, 0).await;
        drain_metadata(&store).await;
        let old = work(&store, "key", 0).await;
        let mut sweep = SweepPreparation::new("key".into(), 0, 0);
        sweep.work = Some(old.clone());
        while !sweep.prepare(&store).await.unwrap() {}
        exec(
            &store,
            "DELETE FROM compaction_frozen_history WHERE id='key'",
        )
        .await;
        header(&store, "key", 1, 1, 0).await;
        drain_metadata(&store).await;
        old.finish(&store, Some(sweep.completion_guard()))
            .await
            .unwrap();
        assert_eq!(pending(&store, "key", 0).await, 1);
        assert!(work(&store, "key", 0).await.revision > old.revision);
        assert!(
            store
                .connection
                .execute_unprepared(
                    "DELETE FROM compaction_frozen_sweep_work WHERE manifest_id='key'"
                )
                .await
                .is_err()
        );
        assert!(store.connection.execute_unprepared("UPDATE compaction_frozen_sweep_consumer SET consumer_row=consumer_row+100 WHERE manifest_id='key'").await.is_err(),"durable service identity cannot be reassigned");
        exec(&store,"UPDATE compaction_frozen_sweep_work SET revision=9223372036854775807 WHERE manifest_id='key' AND kind=0").await;
        // Overflow must abort the entire domain mutation, never reset/reuse.
        assert!(
            store
                .connection
                .execute_unprepared(
                    "UPDATE compaction_frozen_history SET identity_sha256='overflow' WHERE id='key'"
                )
                .await
                .is_err()
        );
        assert_eq!(query(&store,"SELECT count(*) AS n FROM compaction_frozen_history WHERE identity_sha256='overflow'").await,0);
        let mut progress = FrozenStorageLifetimeProgress::default();
        for _ in 0..20 {
            progress.sweep_quantum(&store).await.unwrap();
            let keys = progress
                .sweep
                .iter()
                .chain(progress.sweep_pending.iter())
                .map(|s| (s.manifest.clone(), s.kind))
                .collect::<Vec<_>>();
            assert_eq!(keys.len(), keys.iter().collect::<BTreeSet<_>>().len());
            assert!(keys.len() <= ACTIVE_CONTAINERS);
        }
    }

    #[tokio::test]
    async fn final_guard_refusal_is_retry_and_mutation_before_ack_stays_pending() {
        let store = batch_fixture(0, 4).await;
        drain_metadata(&store).await;
        let selected = work(&store, "batch-physical", 0).await;
        let mut sweep = prepare_batch(&store, 0, 3).await;
        sweep.work = Some(selected.clone());
        batch_consumer(&store, "late", 0, 1).await;
        assert_eq!(
            sweep
                .step(&store, &mut SweepAccounting::default())
                .await
                .unwrap(),
            SweepStep::Retry
        );
        selected.finish(&store, None).await.unwrap();
        assert_eq!(pending(&store, "batch-physical", 0).await, 1);
        assert_eq!(batch_ordinals(&store, 0).await, vec![0, 1, 2, 3]);
        drain_metadata(&store).await;
        let selected = work(&store, "batch-physical", 0).await;
        let mut sweep = prepare_batch(&store, 0, 3).await;
        sweep.work = Some(selected.clone());
        while sweep
            .step(&store, &mut SweepAccounting::default())
            .await
            .unwrap()
            == SweepStep::Continue
        {}
        row(&store, "batch-physical", 0, 4).await; // between terminal step and ack
        selected
            .finish(&store, Some(sweep.completion_guard()))
            .await
            .unwrap();
        assert_eq!(pending(&store, "batch-physical", 0).await, 1);
        assert!(batch_ordinals(&store, 0).await.contains(&4));
    }

    #[tokio::test]
    async fn poison_retry_does_not_block_other_keys_or_kind_and_repair_releases_work() {
        let store = blank(false).await;
        // This is a per-key post-bootstrap refusal fixture; initial legacy
        // scope admission is exercised through quantum in correction tests.
        while frozen_sweep_work::bootstrap_step(&store).await.unwrap() {}
        row(&store, "a-orphan", 0, 0).await;
        header(&store, "z-free", 0, 0, 0).await;
        for kind in [0, 1] {
            row(&store, "z-free", kind, 0).await;
        }
        let mut progress = FrozenStorageLifetimeProgress::default();
        // Data refusals are normally swallowed by quantum; this focused driver
        // explicitly keeps driving other queued keys after the refusal.
        for _ in 0..1000 {
            if matches!(progress.sweep_quantum(&store).await, Ok(false)) {
                break;
            }
        }
        assert_eq!(query(&store,"SELECT count(*) AS n FROM compaction_frozen_message_data WHERE manifest_id='z-free'").await,0);
        assert_eq!(
            query(
                &store,
                "SELECT count(*) AS n FROM compaction_frozen_import_data WHERE manifest_id='z-free'"
            )
            .await,
            0
        );
        assert_eq!(query(&store,"SELECT count(*) AS n FROM compaction_frozen_sweep_work WHERE manifest_id='a-orphan' AND pending=1 AND retry_at>unixepoch()").await,1);
        assert!(!progress.sweep_quantum(&store).await.unwrap());
        header(&store, "a-orphan", 0, 0, 0).await;
        drain(&store, &mut FrozenStorageLifetimeProgress::default()).await;
        assert_eq!(
            query(
                &store,
                "SELECT count(*) AS n FROM compaction_frozen_message_data"
            )
            .await,
            0
        );
        assert_eq!(pending(&store, "a-orphan", 0).await, 0);
    }

    #[tokio::test]
    async fn unreadable_physical_bound_is_deferred_without_hiding_later_work() {
        let store = blank(false).await;
        header_kind(&store, "a-poison", 0, 0, 0, 0).await;
        header_kind(&store, "z-free", 0, 0, 0, 0).await;
        row(&store, "a-poison", 0, 0).await;
        row(&store, "z-free", 0, 0).await;
        // Supported schema admits legacy SQLite affinity poison. Do not disable
        // constraints/triggers to seed it, or impose a new ID restriction.
        exec(&store,"INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES('a-poison','unreadable-bound','{}',2)").await;
        drain(&store, &mut FrozenStorageLifetimeProgress::default()).await;
        assert_eq!(pending(&store, "a-poison", 0).await, 1);
        assert_eq!(pending(&store, "z-free", 0).await, 0);
        assert_eq!(query(&store,"SELECT count(*) AS n FROM compaction_frozen_message_data WHERE manifest_id='z-free'").await,0);
        exec(&store,"DELETE FROM compaction_frozen_message_data WHERE manifest_id='a-poison' AND ordinal='unreadable-bound'").await;
        // Deletion does not introduce work; this refused generation already
        // retains its retry. Simulate its next admission without sleeping.
        exec(
            &store,
            "UPDATE compaction_frozen_sweep_work SET retry_at=0 WHERE manifest_id='a-poison'",
        )
        .await;
        drain(&store, &mut FrozenStorageLifetimeProgress::default()).await;
        assert_eq!(pending(&store, "a-poison", 0).await, 0);
    }

    #[tokio::test]
    async fn consumer_expiry_releases_previously_clean_shared_backings_both_kinds() {
        let store = blank(false).await;
        header(&store, "backing", 1, 0, 0).await;
        header(&store, "consumer", 1, 1, 1).await;
        for kind in [0, 1] {
            row(&store, "backing", kind, 0).await;
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES('consumer',?1,0,0)",[kind.into()])).await.unwrap();
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES('consumer',?1,0,1,'backing')",[kind.into()])).await.unwrap();
        }
        exec(
            &store,
            "UPDATE compaction_frozen_layout SET active=1 WHERE manifest_id='consumer'",
        )
        .await;
        drain(&store, &mut FrozenStorageLifetimeProgress::default()).await;
        for kind in [0, 1] {
            assert_eq!(pending(&store, "backing", kind).await, 0);
        }
        exec(
            &store,
            "UPDATE compaction_frozen_history SET expired=1 WHERE id='consumer'",
        )
        .await;
        let mut restart = FrozenStorageLifetimeProgress::default();
        drain(&store, &mut restart).await;
        for kind in [0, 1] {
            let (table, _) = physical_table(kind);
            assert_eq!(
                query(
                    &store,
                    &format!("SELECT count(*) AS n FROM {table} WHERE manifest_id='backing'")
                )
                .await,
                0
            );
            assert_eq!(pending(&store, "backing", kind).await, 0);
        }
        let revision = work(&store, "backing", 0).await.revision;
        for _ in 0..4 {
            assert!(!restart.sweep_quantum(&store).await.unwrap());
        }
        assert_eq!(
            work(&store, "backing", 0).await.revision,
            revision,
            "delete and ack do not self-requeue"
        );
    }

    #[tokio::test]
    async fn unknown_scope_refusal_is_delayed_and_repair_can_retry_without_domain_invalidation() {
        let store = blank(false).await;
        header(&store, "free", 0, 0, 0).await;
        row(&store, "free", 0, 0).await;
        exec(&store,"INSERT INTO compaction_context(owner,workspace_id,thread_id,format_version) VALUES('owner','ws','thread',1)").await;
        exec(&store,"INSERT INTO compaction_operation(id,owner,fingerprint,status,snapshot,deadline_ms) VALUES('unknown','owner','unknown','failed','{}',1)").await;
        exec(&store,"INSERT INTO compaction_runner_plan(operation_id,source_count,reference_count,descriptor,ready) VALUES('unknown',0,0,'{}',0)").await;
        drain(&store, &mut FrozenStorageLifetimeProgress::default()).await;
        assert_eq!(
            query(
                &store,
                "SELECT count(*) AS n FROM compaction_frozen_message_data"
            )
            .await,
            1
        );
        assert_eq!(query(&store,"SELECT count(*) AS n FROM compaction_frozen_sweep_work WHERE manifest_id='free' AND kind=0 AND pending=1 AND retry_at>unixepoch()").await,1);
        let revision = work(&store, "free", 0).await.revision;
        exec(
            &store,
            "DELETE FROM compaction_runner_plan WHERE operation_id='unknown'",
        )
        .await;
        assert_eq!(work(&store, "free", 0).await.revision, revision);
        assert!(
            !FrozenStorageLifetimeProgress::default()
                .sweep_quantum(&store)
                .await
                .unwrap()
        );
        // Advance eligibility only, emulating the worker's next idle admission.
        // Geometry, readiness, and all production safety triggers remain intact.
        exec(
            &store,
            "UPDATE compaction_frozen_sweep_work SET retry_at=0 WHERE manifest_id='free'",
        )
        .await;
        drain(&store, &mut FrozenStorageLifetimeProgress::default()).await;
        assert_eq!(
            query(
                &store,
                "SELECT count(*) AS n FROM compaction_frozen_message_data"
            )
            .await,
            0
        );
        assert_eq!(pending(&store, "free", 0).await, 0);
    }

    #[tokio::test]
    async fn generation_stays_pending_across_batches_and_new_consumer_preserves_remainder() {
        let store = batch_fixture(0, 601).await;
        header_kind(&store, "prefix", 0, 209, 209, 1).await;
        // Real rows remain available for the prefix before the first delete.
        exec(&store,"INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) SELECT 'prefix',ordinal,reference_json,bytes FROM compaction_frozen_message_data WHERE manifest_id='batch-physical' AND ordinal<209").await;
        drain_metadata(&store).await;
        let selected = work(&store, "batch-physical", 0).await;
        let mut sweep = prepare_batch(&store, 0, 600).await;
        sweep.work = Some(selected.clone());
        let mut accounting = SweepAccounting::default();
        assert_eq!(
            sweep.step(&store, &mut accounting).await.unwrap(),
            SweepStep::Continue
        );
        assert_eq!(accounting.rows[0], 209);
        assert_eq!(pending(&store, "batch-physical", 0).await, 1);
        header_kind(&store, "late-consumer", 0, 601, 601, 1).await;
        exec(&store,"INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES('late-consumer',0,0,0)").await;
        exec(&store,"INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES('late-consumer',0,0,209,'prefix'),('late-consumer',0,209,601,'batch-physical')").await;
        exec(
            &store,
            "UPDATE compaction_frozen_layout SET active=1 WHERE manifest_id='late-consumer'",
        )
        .await;
        assert_eq!(
            sweep.step(&store, &mut accounting).await.unwrap(),
            SweepStep::Retry
        );
        selected.finish(&store, None).await.unwrap();
        assert_eq!(accounting.rows[0], 209);
        assert_eq!(
            batch_ordinals(&store, 0).await,
            (209..601).collect::<Vec<_>>()
        );
        drain(&store, &mut FrozenStorageLifetimeProgress::default()).await;
        assert_eq!(
            batch_ordinals(&store, 0).await,
            (209..601).collect::<Vec<_>>()
        );
        assert_eq!(pending(&store, "batch-physical", 0).await, 0);
    }

    #[tokio::test]
    async fn durable_boundaries_route_maintenance_cancel_writer_admission_and_rollback_ack() {
        use pioneer_sqlite::{
            SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteWriteClass,
            SqliteWriteEvent, SqliteWriteExecutor, SqliteWriteObserver,
        };
        use std::sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        };
        #[derive(Default)]
        struct Observer {
            writes: Mutex<Vec<SqliteWriteEvent>>,
            reads: Mutex<Vec<SqliteReadEvent>>,
            armed: AtomicBool,
            queued: tokio::sync::Notify,
        }
        impl SqliteWriteObserver for Observer {
            fn observe(&self, event: SqliteWriteEvent) {
                self.writes.lock().unwrap().push(event);
                if matches!(event, SqliteWriteEvent::Enqueued { .. })
                    && self.armed.load(Ordering::SeqCst)
                {
                    self.queued.notify_one();
                }
            }
        }
        impl SqliteReadObserver for Observer {
            fn observe(&self, event: SqliteReadEvent) {
                self.reads.lock().unwrap().push(event);
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("durable-boundaries.sqlite");
        let mut options =
            sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
        options.max_connections(1).min_connections(1);
        let writer = Database::connect(options).await.unwrap();
        writer
            .execute_unprepared("PRAGMA foreign_keys=OFF")
            .await
            .unwrap();
        Migrator::up(&writer, None).await.unwrap();
        writer
            .execute_unprepared("PRAGMA journal_mode=WAL")
            .await
            .unwrap();
        writer
            .execute_unprepared(
                "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',1,1)",
            )
            .await
            .unwrap();
        writer.execute_unprepared("INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
        let mut options =
            sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
        options
            .max_connections(1)
            .map_sqlx_sqlite_opts(|o| o.pragma("query_only", "ON"));
        let reader = Database::connect(options).await.unwrap();
        let observer = Arc::new(Observer::default());
        let database = SqliteDatabase::from_executor_with_read_observer(
            reader,
            SqliteWriteExecutor::with_observer(writer, observer.clone()),
            observer.clone(),
        );
        let store = CrudStore::new(database).with_maintenance_access();
        let held = store.connection.begin().await.unwrap();
        observer.armed.store(true, Ordering::SeqCst);
        {
            let future = frozen_sweep_work::bootstrap_step(&store);
            tokio::pin!(future);
            tokio::select! {
                result=&mut future=>panic!("writer should be queued: {result:?}"),
                _=tokio::time::timeout(Duration::from_secs(5),observer.queued.notified())=>{},
            }
            // Dropping the future cancels only its queued writer acquisition.
        }
        observer.armed.store(false, Ordering::SeqCst);
        assert!(observer.writes.lock().unwrap().iter().any(|event|matches!(event,SqliteWriteEvent::Cancelled {class:SqliteWriteClass::Maintenance,queue,..} if queue.maintenance==0)));
        held.rollback().await.unwrap();
        assert_eq!(
            query(
                &store,
                "SELECT sum(started) AS n FROM compaction_frozen_sweep_bootstrap"
            )
            .await,
            0
        );
        // A physical scope can become unknown after reader discovery but
        // before queued writer admission. The writer must recheck the oracle.
        header(&store, "scope-race", 0, 0, 0).await;
        row(&store, "scope-race", 0, 0).await;
        assert!(frozen_sweep_work::bootstrap_step(&store).await.unwrap());
        let held = store.connection.begin().await.unwrap();
        observer.armed.store(true, Ordering::SeqCst);
        {
            let future = frozen_sweep_work::bootstrap_step(&store);
            tokio::pin!(future);
            tokio::select! {
                result=&mut future=>panic!("scope commit should be queued: {result:?}"),
                signal=tokio::time::timeout(Duration::from_secs(5),observer.queued.notified())=>signal.unwrap(),
            }
            observer.armed.store(false, Ordering::SeqCst);
            held.execute_unprepared(
                "UPDATE compaction_frozen_history SET workspace_id='' WHERE id='scope-race'",
            )
            .await
            .unwrap();
            held.commit().await.unwrap();
            let error = future.await.unwrap_err();
            assert!(error.to_string().contains("scope unknown"));
        }
        assert_eq!(query(&store,"SELECT count(*) AS n FROM compaction_frozen_sweep_bootstrap WHERE kind=0 AND started=1 AND after_manifest IS NULL AND complete=0").await,1);
        exec(
            &store,
            "UPDATE compaction_frozen_history SET workspace_id='ws' WHERE id='scope-race'",
        )
        .await;
        header(&store, "held", 1, 1, 0).await;
        row(&store, "held", 0, 0).await;
        drain_metadata(&store).await;
        exec(&store,"CREATE TRIGGER fixture_ack_fault BEFORE UPDATE ON compaction_frozen_sweep_work WHEN NEW.pending=0 BEGIN SELECT RAISE(ABORT,'ack rollback'); END").await;
        let mut progress = FrozenStorageLifetimeProgress::default();
        let mut failed = false;
        for _ in 0..1000 {
            if progress.sweep_quantum(&store).await.is_err() {
                failed = true;
                break;
            }
        }
        assert!(failed);
        assert_eq!(pending(&store, "held", 0).await, 1);
        exec(&store, "DROP TRIGGER fixture_ack_fault").await;
        drain(&store, &mut FrozenStorageLifetimeProgress::default()).await;
        assert_eq!(pending(&store, "held", 0).await, 0);
        assert!(store.connection.reader_query_only_enabled().await.unwrap());
        assert!(
            observer
                .reads
                .lock()
                .unwrap()
                .iter()
                .all(|event| match event {
                    SqliteReadEvent::OperationFinished { class, .. } =>
                        *class == SqliteReadClass::Maintenance,
                    _ => true,
                })
        );
        assert!(
            observer
                .writes
                .lock()
                .unwrap()
                .iter()
                .all(|event| match event {
                    SqliteWriteEvent::Acquired { class, .. } =>
                        *class == SqliteWriteClass::Maintenance,
                    _ => true,
                })
        );
    }

    #[tokio::test]
    async fn empty_physical_queue_does_not_hide_bootstrap_or_interrupted_fanout() {
        let store = blank(false).await;
        assert!(matches!(
            frozen_sweep_work::discover(&store, &[], 0, &mut Default::default())
                .await
                .unwrap(),
            frozen_sweep_work::Admission::Idle
        ));
        assert!(
            FrozenStorageLifetimeProgress::default()
                .sweep_quantum(&store)
                .await
                .unwrap()
        );
        header(&store, "consumer", 2, 2, 0).await;
        for source in ["a", "b"] {
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES('consumer',0,?1,?2,?3)",
                [i64::from(source=="b").into(),(i64::from(source=="b")+1).into(),source.into()])).await.unwrap();
        }
        while frozen_sweep_work::bootstrap_step(&store).await.unwrap() {}
        let before = work(&store, "b", 0).await.revision;
        assert!(frozen_sweep_work::fanout_step(&store).await.unwrap());
        // A failed cursor commit rolls back its enqueue as well.
        exec(&store,"CREATE TRIGGER fixture_fanout_fault BEFORE UPDATE ON compaction_frozen_sweep_consumer WHEN NEW.after_source='b' BEGIN SELECT RAISE(ABORT,'fanout cursor fault'); END").await;
        let mut failed = false;
        for _ in 0..8 {
            let turn = query(
                &store,
                "SELECT revision AS n FROM compaction_frozen_sweep_fanout WHERE id=1",
            )
            .await;
            if frozen_sweep_work::fanout_step(&store).await.is_err() {
                assert_eq!(
                    query(
                        &store,
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
        assert_eq!(work(&store, "b", 0).await.revision, before);
        exec(&store, "DROP TRIGGER fixture_fanout_fault").await;
        drain_metadata(&store).await;
        assert!(work(&store, "b", 0).await.revision > before);
    }
}
