// Included inside frozen_storage_lifetime::tests to reuse its guarded fixtures.
mod durable_sweep {
    use super::*;
    include!("frozen_sweep_correction_tests.rs");

    async fn query(store: &CrudStore, text: &str) -> i64 {
        store
            .connection
            .query_one_raw(Statement::from_string(DbBackend::Postgres, text))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "n")
            .unwrap()
    }
    async fn exec(store: &CrudStore, text: &str) {
        store.connection.execute_unprepared(text).await.unwrap();
    }
    async fn seed_scope(db: &impl ConnectionTrait) {
        db.execute_unprepared(
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',TRUE,FALSE)",
        ).await.unwrap();
        db.execute_unprepared("INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    }
    async fn blank() -> CrudStore {
        let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
        let writer = fixture.connection();
        seed_scope(writer).await;
        let reader = fixture.connect_read_only().await.unwrap();
        CrudStore::new(pioneer_sqlite::SqliteDatabase::new(reader, writer.clone()))
            .with_maintenance_access()
    }
    async fn header(store: &CrudStore, id: &str, count: i64, next: i64, ready: i64) {
        store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,imports_sha256,message_count,import_count,next_ordinal,next_import,ready) VALUES($1,'ws','thread','fixture','fixture',$2,$2,$3,$3,$4)",
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
        store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,imports_sha256,message_count,import_count,next_ordinal,next_import,ready) VALUES($1,'ws','thread','fixture','fixture',$2,$3,$4,$5,$6)",
            [id.into(),messages.into(),imports.into(),message_next.into(),import_next.into(),ready.into()])).await.unwrap();
    }
    async fn row(store: &CrudStore, id: &str, kind: i64, ordinal: i64) {
        let text = if kind == 0 {
            "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES($1,$2,'{}',2)"
        } else {
            "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES($1,$2,0,'event','e-'||$2,'v','thread','{}',2)"
        };
        store
            .connection
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                text,
                [id.into(), ordinal.into()],
            ))
            .await
            .unwrap();
    }
    async fn drain_metadata(store: &CrudStore) {
        for _ in 0..5000 {
            if !frozen_sweep_work::fanout_step(store, None).await.unwrap() {
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
    async fn pending(store: &CrudStore, id: &str, kind: i64) -> i64 {
        store
            .connection
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT pending AS n FROM compaction_frozen_sweep_work WHERE manifest_id=$1 AND kind=$2",
                [id.into(), kind.into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "n")
            .unwrap()
    }

    #[pioneer_postgres_test::postgres_test]
    async fn clean_held_restart_and_revisit_do_not_prepare_again() {
        let store = blank().await;
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

    #[pioneer_postgres_test::postgres_test]
    async fn consumer_fanout_tracks_all_sources_old_new_kind_and_coalesces() {
        let store = blank().await;
        for id in ["consumer", "a", "b", "c"] {
            header(&store, id, 3, 3, 0).await;
        }
        for kind in [0, 1] {
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES('consumer',$1,0,0)",[kind.into()])).await.unwrap();
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "INSERT INTO compaction_frozen_span(manifest_id,kind,start,\"end\",source_manifest) VALUES('consumer',$1,0,1,'a'),('consumer',$1,1,2,'b')",[kind.into()])).await.unwrap();
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
        exec(&store,"UPDATE compaction_frozen_span SET source_manifest='c',kind=1,start=2,\"end\"=3 WHERE manifest_id='consumer' AND kind=0 AND start=1").await;
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
        exec(&store,"INSERT INTO compaction_frozen_span(manifest_id,kind,start,\"end\",source_manifest) VALUES('consumer2',0,0,1,'a')").await;
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

    #[pioneer_postgres_test::postgres_test]
    async fn candidate_without_spans_tracks_old_new_failure_and_removal_both_kinds() {
        let store = blank().await;
        for id in ["a", "b"] {
            header(&store, id, 0, 0, 1).await;
        }
        header(&store, "capture", 0, 0, 0).await;
        drain_metadata(&store).await;
        for kind in [0, 1] {
            let a = work(&store, "a", kind).await.revision;
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "INSERT INTO compaction_frozen_layout(manifest_id,kind,candidate) VALUES('capture',$1,'a')",[kind.into()])).await.unwrap();
            assert!(work(&store, "a", kind).await.revision > a);
            let a = work(&store, "a", kind).await.revision;
            let b = work(&store, "b", kind).await.revision;
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "UPDATE compaction_frozen_layout SET candidate='b' WHERE manifest_id='capture' AND kind=$1",[kind.into()])).await.unwrap();
            assert!(work(&store, "a", kind).await.revision > a);
            assert!(work(&store, "b", kind).await.revision > b);
            let b = work(&store, "b", kind).await.revision;
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "UPDATE compaction_frozen_layout SET failed=1 WHERE manifest_id='capture' AND kind=$1",[kind.into()])).await.unwrap();
            assert!(work(&store, "b", kind).await.revision > b);
            let b = work(&store, "b", kind).await.revision;
            store
                .connection
                .execute_raw(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "DELETE FROM compaction_frozen_layout WHERE manifest_id='capture' AND kind=$1",
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

    #[pioneer_postgres_test::postgres_test]
    async fn redirected_append_tracks_actual_physical_destination_and_rollback() {
        for kind in [0, 1] {
            let store = blank().await;
            header_kind(&store, "backing", kind, 1, 1, 1).await;
            header_kind(&store, "capture", kind, 2, 1, 0).await;
            row(&store, "backing", kind, 0).await;
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES('capture',$1,0,0)",[kind.into()])).await.unwrap();
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "INSERT INTO compaction_frozen_span(manifest_id,kind,start,\"end\",source_manifest) VALUES('capture',$1,0,1,'backing')",[kind.into()])).await.unwrap();
            exec(
                &store,
                "UPDATE compaction_frozen_layout AS layout SET active=1,verified_span_revision=(SELECT c.revision FROM compaction_frozen_sweep_consumer c WHERE c.manifest_id=layout.manifest_id AND c.kind=layout.kind) WHERE layout.manifest_id='capture'",
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
                    "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES($1,1,'{}',2)"
                } else {
                    "INSERT INTO compaction_frozen_import_data(manifest_id,ordinal,message_ordinal,source_scope,source_id,source_version,source_thread,proof_json,bytes) VALUES($1,1,0,'event','tail','v','thread','{}',2)"
                };
                tx.execute_raw(Statement::from_sql_and_values(
                    DbBackend::Postgres,
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

    #[pioneer_postgres_test::postgres_test]
    async fn stale_preparation_and_ack_aba_overflow_and_no_duplicate_active_key() {
        let store = blank().await;
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
        old.finish(
            &store.connection,
            frozen_sweep_work::Completion::Complete(sweep.completion_guard()),
        )
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

    #[pioneer_postgres_test::postgres_test]
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
            SweepStep::Blocked
        );
        selected
            .finish(&store.connection, frozen_sweep_work::Completion::Blocked)
            .await
            .unwrap();
        assert_eq!(pending(&store, "batch-physical", 0).await, 1);
        assert!(
            frozen_sweep_work::next_retry_deadline(&store)
                .await
                .unwrap()
                .is_none()
        );
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
            .finish(
                &store.connection,
                frozen_sweep_work::Completion::Complete(sweep.completion_guard()),
            )
            .await
            .unwrap();
        assert_eq!(pending(&store, "batch-physical", 0).await, 1);
        assert!(batch_ordinals(&store, 0).await.contains(&4));
    }

    #[pioneer_postgres_test::postgres_test]
    async fn addressed_cleanup_and_due_retry_leave_other_containers_pending() {
        let store = blank().await;
        let mut progress = FrozenStorageLifetimeProgress::default();
        let mut complete = false;
        for _ in 0..128 {
            if !progress.quantum(&store).await.unwrap() {
                complete = true;
                break;
            }
        }
        assert!(complete, "empty startup recovery must reach checked EOF");
        for id in ["current", "queued", "deadline", "future"] {
            header(&store, id, 0, 0, 0).await;
            row(&store, id, 0, 0).await;
        }
        drain_metadata(&store).await;
        for id in ["deadline", "future"] {
            work(&store, id, 0)
                .await
                .finish(&store.connection, frozen_sweep_work::Completion::Retry)
                .await
                .unwrap();
        }
        // Advance one existing retry without sleeping or altering its revision.
        exec(&store, "UPDATE compaction_frozen_sweep_work SET retry_at=1 WHERE manifest_id='deadline' AND kind=0").await;
        progress
            .restart_for_changes(&store, Some(vec!["m:current".into()]))
            .await
            .unwrap();
        drain(&store, &mut progress).await;
        assert_eq!(query(&store, "SELECT count(*) AS n FROM compaction_frozen_message_data WHERE manifest_id='current'").await, 0);
        assert_eq!(query(&store, "SELECT count(*) AS n FROM compaction_frozen_message_data WHERE manifest_id IN ('queued','deadline','future')").await, 3);

        // A timer wake owns due retries, not unrelated dependency-blocked work.
        progress
            .restart_for_changes(&store, Some(Vec::new()))
            .await
            .unwrap();
        drain(&store, &mut progress).await;
        assert_eq!(query(&store, "SELECT count(*) AS n FROM compaction_frozen_message_data WHERE manifest_id='deadline'").await, 0);
        assert_eq!(pending(&store, "deadline", 0).await, 0);
        assert_eq!(query(&store, "SELECT count(*) AS n FROM compaction_frozen_message_data WHERE manifest_id IN ('queued','future')").await, 2);
        assert_eq!(pending(&store, "queued", 0).await, 1);
        assert_eq!(pending(&store, "future", 0).await, 1);
        assert!(
            progress
                .next_retry_deadline(&store)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[pioneer_postgres_test::postgres_test]
    async fn foreign_keys_and_types_reject_invalid_backings_without_blocking_cleanup() {
        let store = blank().await;
        header_kind(&store, "z-free", 0, 0, 0, 0).await;
        row(&store, "z-free", 0, 0).await;
        for sql in [
            "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES('a-orphan',0,'{}',2)",
            "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES('z-free','unreadable-bound','{}',2)",
        ] {
            assert!(store.connection.execute_unprepared(sql).await.is_err());
        }
        drain(&store, &mut FrozenStorageLifetimeProgress::default()).await;
        assert_eq!(pending(&store, "z-free", 0).await, 0);
        assert_eq!(query(&store,"SELECT count(*) AS n FROM compaction_frozen_message_data").await, 0);
    }

    #[pioneer_postgres_test::postgres_test]
    async fn consumer_expiry_releases_previously_clean_shared_backings_both_kinds() {
        let store = blank().await;
        header(&store, "backing", 1, 0, 0).await;
        header(&store, "consumer", 1, 1, 1).await;
        for kind in [0, 1] {
            row(&store, "backing", kind, 0).await;
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES('consumer',$1,0,0)",[kind.into()])).await.unwrap();
            store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "INSERT INTO compaction_frozen_span(manifest_id,kind,start,\"end\",source_manifest) VALUES('consumer',$1,0,1,'backing')",[kind.into()])).await.unwrap();
        }
        exec(
            &store,
            "UPDATE compaction_frozen_layout AS layout SET active=1,verified_span_revision=(SELECT c.revision FROM compaction_frozen_sweep_consumer c WHERE c.manifest_id=layout.manifest_id AND c.kind=layout.kind) WHERE layout.manifest_id='consumer'",
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

    #[pioneer_postgres_test::postgres_test]
    async fn unknown_scope_refusal_parks_and_repair_rechecks_pending_generation() {
        let store = blank().await;
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
        assert_eq!(query(&store,"SELECT count(*) AS n FROM compaction_frozen_sweep_work WHERE manifest_id='free' AND kind=0 AND pending=1 AND retry_at=0").await,1);
        assert!(
            frozen_sweep_work::next_retry_deadline(&store)
                .await
                .unwrap()
                .is_none()
        );
        let revision = work(&store, "free", 0).await.revision;
        exec(
            &store,
            "DELETE FROM compaction_runner_plan WHERE operation_id='unknown'",
        )
        .await;
        assert_eq!(work(&store, "free", 0).await.revision, revision);
        // Emulate the new discovery pass admitted by repair invalidation.
        // Expected protection is not an error-owned timed retry; no synthetic
        // retry_at update or idle timer is needed to make this generation due.
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

    #[pioneer_postgres_test::postgres_test]
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
        exec(&store,"INSERT INTO compaction_frozen_span(manifest_id,kind,start,\"end\",source_manifest) VALUES('late-consumer',0,0,209,'prefix'),('late-consumer',0,209,601,'batch-physical')").await;
        exec(
            &store,
            "UPDATE compaction_frozen_layout AS layout SET active=1,verified_span_revision=(SELECT c.revision FROM compaction_frozen_sweep_consumer c WHERE c.manifest_id=layout.manifest_id AND c.kind=layout.kind) WHERE layout.manifest_id='late-consumer'",
        )
        .await;
        assert_eq!(
            sweep.step(&store, &mut accounting).await.unwrap(),
            SweepStep::Blocked
        );
        selected
            .finish(&store.connection, frozen_sweep_work::Completion::Blocked)
            .await
            .unwrap();
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

    #[pioneer_postgres_test::postgres_test]
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
        let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
        let writer = fixture.connect_with_limit(1).await.unwrap();
        seed_scope(&writer).await;
        let reader = fixture.connect_read_only().await.unwrap();
        let observer = Arc::new(Observer::default());
        let database = SqliteDatabase::from_executor_with_read_observer(
            reader,
            SqliteWriteExecutor::with_observer(writer, observer.clone()),
            observer.clone(),
        );
        let store = CrudStore::new(database).with_maintenance_access();
        header(&store, "held", 1, 1, 0).await;
        row(&store, "held", 0, 0).await;
        drain_metadata(&store).await;
        let selected = work(&store, "held", 0).await;
        let held = store.connection.begin().await.unwrap();
        observer.armed.store(true, Ordering::SeqCst);
        {
            let finish = selected.finish(&store.connection, frozen_sweep_work::Completion::Retry);
            tokio::pin!(finish);
            tokio::select! {
                result = &mut finish => panic!("writer should be queued: {result:?}"),
                signal = tokio::time::timeout(Duration::from_secs(5), observer.queued.notified()) => signal.unwrap(),
            }
        }
        observer.armed.store(false, Ordering::SeqCst);
        assert!(observer.writes.lock().unwrap().iter().any(|event| matches!(
            event, SqliteWriteEvent::Cancelled { class: SqliteWriteClass::Maintenance, queue, .. }
                if queue.maintenance == 0
        )));
        held.rollback().await.unwrap();
        assert_eq!(pending(&store, "held", 0).await, 1);
        exec(&store,"CREATE FUNCTION fixture_ack_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'ack rollback'; END; $$; CREATE TRIGGER fixture_ack_fault BEFORE UPDATE ON compaction_frozen_sweep_work FOR EACH ROW WHEN (NEW.pending=0) EXECUTE FUNCTION fixture_ack_fault()").await;
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
        exec(&store, "DROP TRIGGER fixture_ack_fault ON compaction_frozen_sweep_work").await;
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

}
