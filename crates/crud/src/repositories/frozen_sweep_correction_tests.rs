// Included in durable_sweep: same guarded fixtures and actual sweep driver.

#[tokio::test]
async fn unrelated_continuous_fanout_allows_physical_admission_delete_and_clean_skip() {
    for kind in [0, 1] {
        let store = blank(false).await;
        header_kind(&store, "a-hot", kind, 0, 0, 0).await;
        header_kind(&store, "b-free", kind, 0, 0, 0).await;
        header_kind(&store, "b-held", kind, 1, 1, 0).await;
        row(&store, "b-held", kind, 0).await;
        let mut progress = FrozenStorageLifetimeProgress::default();
        drain(&store, &mut progress).await; // establish baseline only, before A churn
        assert_eq!(pending(&store, "b-held", kind).await, 0);
        let held_revision = work(&store, "b-held", kind).await.revision;
        let held_visits = progress.accounting.held;
        row(&store, "b-free", kind, 0).await;
        let mut completed = false;
        for step in 0..160 {
            store
                .connection
                .execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "UPDATE compaction_frozen_history SET identity_sha256=?1 WHERE id='a-hot'",
                    [format!("churn-{step}").into()],
                ))
                .await
                .unwrap();
            // A is pending at every entry, including B's preparation/delete/ack.
            assert_eq!(query(&store,"SELECT count(*) AS n FROM frozen_sweep_consumer WHERE manifest_id='a-hot' AND pending=1").await,2);
            assert!(progress.quantum(&store).await.unwrap());
            assert!(
                progress.sweep_pending.len() + usize::from(progress.sweep.is_some())
                    <= ACTIVE_CONTAINERS
            );
            if pending(&store, "b-free", kind).await == 0 {
                completed = true;
            }
        }
        assert!(
            completed,
            "B must finish within bounded quanta while A never quiets"
        );
        let (table, _) = physical_table(kind);
        assert_eq!(
            query(
                &store,
                &format!("SELECT count(*) AS n FROM {table} WHERE manifest_id='b-free'")
            )
            .await,
            0
        );
        assert_eq!(
            query(
                &store,
                &format!("SELECT count(*) AS n FROM {table} WHERE manifest_id='b-held'")
            )
            .await,
            1
        );
        assert_eq!(pending(&store, "b-held", kind).await, 0);
        assert_eq!(work(&store, "b-held", kind).await.revision, held_revision);
        assert_eq!(
            progress.accounting.held, held_visits,
            "unrelated events must not repeat checked held geometry"
        );
        assert_eq!(progress.accounting.rows[kind as usize], 1);
    }
}

#[tokio::test]
async fn resetting_low_consumer_cannot_starve_later_sources_or_other_kind() {
    let store = blank(false).await;
    header(&store, "a-hot", 0, 0, 0).await;
    header(&store, "z-consumer", 1, 1, 1).await;
    header(&store, "z-backing", 0, 0, 0).await;
    for kind in [0, 1] {
        row(&store, "z-backing", kind, 0).await;
        store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES('z-consumer',?1,0,0)",[kind.into()])).await.unwrap();
        store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES('z-consumer',?1,0,1,'z-backing')",[kind.into()])).await.unwrap();
    }
    exec(
        &store,
        "UPDATE compaction_frozen_layout SET active=1 WHERE manifest_id='z-consumer'",
    )
    .await;
    let mut progress = FrozenStorageLifetimeProgress::default();
    drain(&store, &mut progress).await;
    let old = [
        work(&store, "z-backing", 0).await.revision,
        work(&store, "z-backing", 1).await.revision,
    ];
    exec(
        &store,
        "UPDATE compaction_frozen_history SET expired=1 WHERE id='z-consumer'",
    )
    .await;
    for step in 0..160 {
        store
            .connection
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE compaction_frozen_history SET identity_sha256=?1 WHERE id='a-hot'",
                [format!("hot-{step}").into()],
            ))
            .await
            .unwrap();
        assert!(progress.quantum(&store).await.unwrap());
        // Drop only RAM state mid-interleaving; durable service/source cursors
        // must still give later consumers both target delivery and EOF turns.
        if step == 31 {
            progress = FrozenStorageLifetimeProgress::default();
        }
    }
    assert_eq!(query(&store,"SELECT count(*) AS n FROM frozen_sweep_consumer WHERE manifest_id='z-consumer' AND pending=1").await,0);
    for kind in [0, 1] {
        assert!(work(&store, "z-backing", kind).await.revision > old[kind as usize]);
    }
    // Any relevant transient refusal remains addressed. Advance its eligibility
    // after propagation, still without stopping unrelated A or draining metadata.
    exec(
        &store,
        "UPDATE frozen_sweep_work SET retry_at=0 WHERE manifest_id='z-backing'",
    )
    .await;
    for step in 0..160 {
        store
            .connection
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE compaction_frozen_history SET identity_sha256=?1 WHERE id='a-hot'",
                [format!("after-{step}").into()],
            ))
            .await
            .unwrap();
        assert!(progress.quantum(&store).await.unwrap());
    }
    for kind in [0, 1] {
        let (table, _) = physical_table(kind);
        assert_eq!(
            query(
                &store,
                &format!("SELECT count(*) AS n FROM {table} WHERE manifest_id='z-backing'")
            )
            .await,
            0
        );
        assert_eq!(pending(&store, "z-backing", kind).await, 0);
    }
}

#[tokio::test]
async fn relevant_pending_geometry_blocks_stale_delete_and_ack_before_target_revision_changes() {
    let store = batch_fixture(0, 4).await;
    header_kind(&store, "mutable-consumer", 0, 1, 1, 0).await;
    exec(&store,"INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES('mutable-consumer',0,0,0)").await;
    exec(&store,"INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES('mutable-consumer',0,0,1,'batch-physical')").await;
    exec(
        &store,
        "UPDATE compaction_frozen_layout SET active=1 WHERE manifest_id='mutable-consumer'",
    )
    .await;
    drain_metadata(&store).await;
    let selected = work(&store, "batch-physical", 0).await;
    let mut sweep = prepare_batch(&store, 0, 3).await;
    sweep.work = Some(selected.clone());
    // A permitted incomplete identity changes and returns to the original
    // geometry. Exact snapshot equality alone would miss this intermediate work.
    exec(&store,"UPDATE compaction_frozen_history SET identity_sha256='changed' WHERE id='mutable-consumer'").await;
    exec(&store,"UPDATE compaction_frozen_history SET identity_sha256='fixture' WHERE id='mutable-consumer'").await;
    assert_eq!(
        work(&store, "batch-physical", 0).await.revision,
        selected.revision
    );
    let mut accounting = SweepAccounting::default();
    assert_eq!(
        sweep.step(&store, &mut accounting).await.unwrap(),
        SweepStep::Retry
    );
    selected
        .finish(&store, Some(sweep.completion_guard()))
        .await
        .unwrap();
    assert_eq!(pending(&store, "batch-physical", 0).await, 1);
    assert_eq!(accounting.rows, [0, 0]);
    assert_eq!(batch_ordinals(&store, 0).await, vec![0, 1, 2, 3]);
    // The actual new delivery increments target revision and removes its retry.
    drain_metadata(&store).await;
    assert!(work(&store, "batch-physical", 0).await.revision > selected.revision);
    drain(&store, &mut FrozenStorageLifetimeProgress::default()).await;
    assert_eq!(batch_ordinals(&store, 0).await, vec![0]);
    assert_eq!(pending(&store, "batch-physical", 0).await, 0);
}

async fn assert_initial_scope_refusal(
    store: &CrudStore,
    progress: &mut FrozenStorageLifetimeProgress,
) {
    for _ in 0..1000 {
        match progress.quantum(store).await {
            Err(error) => {
                assert!(
                    error
                        .to_string()
                        .contains("legacy frozen dependency scope unknown"),
                    "{error}"
                );
                assert!(!progress.scope.done);
                assert!(
                    progress.expiry.is_none()
                        && progress.sweep.is_none()
                        && progress.discard.is_none()
                );
                assert_eq!(progress.accounting.expired, 0);
                assert_eq!(
                    query(
                        store,
                        "SELECT expired AS n FROM compaction_frozen_history WHERE id='eligible'"
                    )
                    .await,
                    0
                );
                return;
            }
            Ok(more) => assert!(more, "unknown legacy scope cannot become global EOF"),
        }
    }
    panic!("legacy orphan must hold initial admission");
}

async fn drive_lifetime_to_idle(store: &CrudStore, progress: &mut FrozenStorageLifetimeProgress) {
    for _ in 0..4000 {
        if !progress.quantum(store).await.unwrap() {
            return;
        }
    }
    panic!("finite fixture must reach idle");
}

#[tokio::test]
async fn initial_physical_scope_refusal_survives_restart_and_repairs_each_kind_before_cleanup() {
    let db = fixture_database(true).await;
    let store = CrudStore::new(db.clone()).with_maintenance_access();
    header(&store, "a-known", 0, 0, 0).await;
    header(&store, "eligible", 0, 0, 1).await;
    for kind in [0, 1] {
        row(&store, "a-known", kind, 0).await;
    }
    row(&store, "m-orphan-message", 0, 0).await;
    row(&store, "m-orphan-import", 1, 0).await;
    Migrator::up(&db, None).await.unwrap();
    let mut progress = FrozenStorageLifetimeProgress::default();
    assert_initial_scope_refusal(&store, &mut progress).await;
    assert_eq!(query(&store,"SELECT count(*) AS n FROM frozen_sweep_bootstrap WHERE kind=0 AND after_manifest='a-known' AND complete=0").await,1);
    // Already checked scope must not be re-inventoried after restart.
    exec(&store,"CREATE TRIGGER fixture_checked_cursor BEFORE UPDATE ON frozen_sweep_bootstrap WHEN NEW.kind=0 AND NEW.after_manifest='a-known' BEGIN SELECT RAISE(ABORT,'checked group repeated'); END").await;
    let mut restart = FrozenStorageLifetimeProgress::default();
    assert_initial_scope_refusal(&store, &mut restart).await;
    header(&store, "m-orphan-message", 0, 0, 0).await;
    assert_initial_scope_refusal(&store, &mut restart).await;
    assert_eq!(
        query(
            &store,
            "SELECT complete AS n FROM frozen_sweep_bootstrap WHERE kind=0"
        )
        .await,
        1
    );
    assert_eq!(
        query(
            &store,
            "SELECT complete AS n FROM frozen_sweep_bootstrap WHERE kind=1"
        )
        .await,
        0
    );
    exec(&store, "DROP TRIGGER fixture_checked_cursor").await;
    header(&store, "m-orphan-import", 0, 0, 0).await;
    let mut repaired = FrozenStorageLifetimeProgress::default();
    drive_lifetime_to_idle(&store, &mut repaired).await;
    assert_eq!(
        query(
            &store,
            "SELECT expired AS n FROM compaction_frozen_history WHERE id='eligible'"
        )
        .await,
        1
    );
    assert_eq!(
        query(
            &store,
            "SELECT count(*) AS n FROM frozen_sweep_bootstrap WHERE complete=1"
        )
        .await,
        2
    );
    assert_eq!(
        query(
            &store,
            "SELECT count(*) AS n FROM compaction_frozen_message_data"
        )
        .await,
        0
    );
    assert_eq!(
        query(
            &store,
            "SELECT count(*) AS n FROM compaction_frozen_import_data"
        )
        .await,
        0
    );
    exec(&store,"CREATE TRIGGER fixture_no_bootstrap_restart BEFORE UPDATE ON frozen_sweep_bootstrap BEGIN SELECT RAISE(ABORT,'bootstrap restarted'); END").await;
    drive_lifetime_to_idle(&store, &mut FrozenStorageLifetimeProgress::default()).await;
}

#[tokio::test]
async fn initial_physical_scope_uses_both_candidate_and_span_endpoint_directions() {
    for endpoint in [
        "own-candidate",
        "reverse-candidate",
        "own-span",
        "reverse-span",
    ] {
        let db = fixture_database(true).await;
        let store = CrudStore::new(db.clone()).with_maintenance_access();
        header(&store, "anchor", 1, 1, 1).await;
        header(&store, "orphan", 1, 1, 1).await;
        header(&store, "eligible", 0, 0, 1).await;
        for kind in [0, 1] {
            row(&store, "anchor", kind, 0).await;
            row(&store, "orphan", kind, 0).await;
            let text = match endpoint {
                "own-candidate" => {
                    "INSERT INTO compaction_frozen_layout(manifest_id,kind,candidate) VALUES('orphan',?1,'anchor')"
                }
                "reverse-candidate" => {
                    "INSERT INTO compaction_frozen_layout(manifest_id,kind,candidate) VALUES('anchor',?1,'orphan')"
                }
                "own-span" => {
                    "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES('orphan',?1,0,1,'anchor')"
                }
                "reverse-span" => {
                    "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES('anchor',?1,0,1,'orphan')"
                }
                _ => unreachable!(),
            };
            store
                .connection
                .execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    text,
                    [kind.into()],
                ))
                .await
                .unwrap();
        }
        // Guarded dependencies were valid at creation. FK OFF legacy removal
        // leaves a missing physical parent with inferable scope, not unknown.
        exec(
            &store,
            "DELETE FROM compaction_frozen_history WHERE id='orphan'",
        )
        .await;
        Migrator::up(&db, None).await.unwrap();
        let mut progress = FrozenStorageLifetimeProgress::default();
        drive_lifetime_to_idle(&store, &mut progress).await;
        assert!(progress.scope.done, "{endpoint}");
        assert_eq!(
            query(
                &store,
                "SELECT count(*) AS n FROM frozen_sweep_bootstrap WHERE complete=1"
            )
            .await,
            2
        );
        assert_eq!(
            query(
                &store,
                "SELECT expired AS n FROM compaction_frozen_history WHERE id='eligible'"
            )
            .await,
            1
        );
        // Scope inference is not authority to delete missing-parent data.
        for kind in [0, 1] {
            let (table, _) = physical_table(kind);
            assert_eq!(
                query(
                    &store,
                    &format!("SELECT count(*) AS n FROM {table} WHERE manifest_id='orphan'")
                )
                .await,
                1
            );
        }
    }
}

#[tokio::test]
async fn physical_round_serves_behind_cursor_despite_continuous_arrivals_and_redirty() {
    for kind in [0, 1] {
        let store = blank(false).await;
        for id in ["a-behind", "m-cursor", "z-end"] {
            header_kind(&store, id, kind, 0, 0, 0).await;
        }
        let mut progress = FrozenStorageLifetimeProgress::default();
        drain(&store, &mut progress).await; // baseline only, before the interleaving
        row(&store, "m-cursor", kind, 0).await;
        let mut at_cursor = false;
        for _ in 0..40 {
            assert!(progress.sweep_quantum(&store).await.unwrap());
            if progress
                .sweep
                .iter()
                .chain(progress.sweep_pending.iter())
                .any(|s| s.manifest == "m-cursor" && s.kind == kind)
            {
                at_cursor = true;
                break;
            }
        }
        assert!(at_cursor, "position the cursor by actual admission");
        let frontier = progress.sweep_round[kind as usize].until_row;
        row(&store, "a-behind", kind, 0).await;
        // The import case is an already-due addressed retry. Fresh retry_at=0
        // arrivals must not indefinitely overtake it either.
        if kind == 1 {
            exec(
                &store,
                "UPDATE frozen_sweep_work SET retry_at=1 WHERE manifest_id='a-behind' AND kind=1",
            )
            .await;
        }
        let mut prepared_b = false;
        let mut completed_b = false;
        let mut completed_arrival = false;
        let mut completed_inside = false;
        for step in 0..320 {
            // Alternate IDs above the old cursor and inside its old TEXT range.
            // Keep arrivals going until both B and an actual new key finish.
            let id = if step % 2 == 0 {
                format!("n{step:04}")
            } else {
                format!("a-inside-{step:04}")
            };
            header_kind(&store, &id, kind, 0, 0, 0).await;
            row(&store, &id, kind, 0).await;
            if step % 4 == 0 {
                // Repeated invalidation of an existing service ID cannot move
                // it forward within the round. Never rewrite the header/proof.
                row(&store, "m-cursor", kind, step + 1).await;
            }
            assert!(progress.sweep_quantum(&store).await.unwrap());
            let active = progress
                .sweep
                .iter()
                .chain(progress.sweep_pending.iter())
                .collect::<Vec<_>>();
            assert!(active.len() <= ACTIVE_CONTAINERS);
            for (index, a) in active.iter().enumerate() {
                assert!(
                    !active[index + 1..]
                        .iter()
                        .any(|b| a.manifest == b.manifest && a.kind == b.kind)
                );
            }
            prepared_b |= active
                .iter()
                .any(|s| s.manifest == "a-behind" && s.kind == kind && s.prepared.is_some());
            if !completed_b && pending(&store, "a-behind", kind).await == 0 {
                assert!(
                    step < 192,
                    "B waits only for finite preexisting work, not the arrival stream"
                );
                completed_b = true;
            }
            completed_arrival |= pending(&store, "n0000", kind).await == 0;
            if step > 0 {
                completed_inside |= pending(&store, "a-inside-0001", kind).await == 0;
            }
            if completed_b && completed_arrival && completed_inside {
                break;
            }
        }
        assert!(prepared_b && completed_b && completed_arrival && completed_inside);
        let (table, _) = physical_table(kind);
        for id in ["a-behind", "n0000", "a-inside-0001"] {
            assert_eq!(
                query(
                    &store,
                    &format!("SELECT count(*) AS n FROM {table} WHERE manifest_id='{id}'")
                )
                .await,
                0
            );
        }
        assert!(
            query(
                &store,
                "SELECT work_row AS n FROM frozen_sweep_work WHERE manifest_id='n0000' AND kind=0"
            )
            .await
                > frontier
        );
        assert!(progress.accounting.rows[kind as usize] >= 3);
        // Service identity survives invalidation and cannot be moved behind a
        // frontier. Explicit backwards allocation fails atomically as well.
        assert!(store.connection.execute_unprepared("UPDATE frozen_sweep_work SET work_row=work_row+100000 WHERE manifest_id='a-behind'").await.is_err());
        assert!(store.connection.execute_unprepared("INSERT INTO frozen_sweep_work(work_row,manifest_id,kind) VALUES(-1,'backwards',0)").await.is_err());
        exec(&store,"INSERT INTO frozen_sweep_work(work_row,manifest_id,kind) VALUES(9223372036854775807,'service-exhausted',0)").await;
        assert!(store.connection.execute_unprepared("INSERT INTO frozen_sweep_work(work_row,manifest_id,kind) VALUES(9223372036854775806,'behind-frontier',0)").await.is_err());
        // Random fallback IDs at exhaustion must abort the physical mutation,
        // rather than registering its new key behind an existing frontier.
        assert!(store.connection.execute_unprepared("INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES('allocation-refused',0,'{}',2)").await.is_err());
        assert_eq!(query(&store,"SELECT count(*) AS n FROM compaction_frozen_message_data WHERE manifest_id='allocation-refused'").await,0);
        assert_eq!(
            query(
                &store,
                "SELECT count(*) AS n FROM frozen_sweep_work WHERE manifest_id='allocation-refused'"
            )
            .await,
            0
        );
    }
}

#[tokio::test]
async fn physical_round_pages_future_retries_without_false_eof_or_idle_loop() {
    for kind in [0, 1] {
        let store = blank(false).await;
        for index in 0..40 {
            header_kind(&store, &format!("a-future-{index:02}"), kind, 0, 0, 0).await;
        }
        header_kind(&store, "z-due", kind, 0, 0, 0).await;
        let mut progress = FrozenStorageLifetimeProgress::default();
        drain(&store, &mut progress).await;
        for index in 0..40 {
            let id = format!("a-future-{index:02}");
            row(&store, &id, kind, 0).await;
            work(&store, &id, kind)
                .await
                .finish(&store, None)
                .await
                .unwrap();
        }
        // Pending deferred work alone is idle, with no artificial continuation.
        for _ in 0..3 {
            assert!(!progress.sweep_quantum(&store).await.unwrap());
        }
        row(&store, "z-due", kind, 0).await;
        store
            .connection
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE frozen_sweep_work SET retry_at=1 WHERE manifest_id='z-due' AND kind=?1",
                [kind.into()],
            ))
            .await
            .unwrap();
        progress = FrozenStorageLifetimeProgress::default();
        let mut completed = false;
        for _ in 0..100 {
            // The finite service round must continue past multiple metadata
            // pages containing only deferred keys. No metadata drain here.
            assert!(progress.sweep_quantum(&store).await.unwrap());
            if pending(&store, "z-due", kind).await == 0 {
                completed = true;
                break;
            }
        }
        assert!(completed);
        let (table, _) = physical_table(kind);
        assert_eq!(
            query(
                &store,
                &format!("SELECT count(*) AS n FROM {table} WHERE manifest_id='z-due'")
            )
            .await,
            0
        );
        assert_eq!(
            query(&store, &format!("SELECT count(*) AS n FROM {table}")).await,
            40
        );
        assert!(!progress.sweep_quantum(&store).await.unwrap());
        let revision = work(&store, "a-future-00", kind).await.revision;
        drop(progress);
        let mut restarted = FrozenStorageLifetimeProgress::default();
        assert!(!restarted.sweep_quantum(&store).await.unwrap());
        assert_eq!(work(&store, "a-future-00", kind).await.revision, revision);
    }
}

#[tokio::test]
async fn losing_physical_round_keeps_generation_and_relevant_prefanout_fence() {
    for kind in [0, 1] {
        let store = batch_fixture(kind, 4).await;
        header_kind(&store, "mutable-consumer", kind, 1, 1, 0).await;
        store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES('mutable-consumer',?1,0,0)",[kind.into()])).await.unwrap();
        store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES('mutable-consumer',?1,0,1,'batch-physical')",[kind.into()])).await.unwrap();
        exec(
            &store,
            "UPDATE compaction_frozen_layout SET active=1 WHERE manifest_id='mutable-consumer'",
        )
        .await;
        drain_metadata(&store).await; // baseline only; no draining after mutation
        let selected = work(&store, "batch-physical", kind).await;
        let mut progress = FrozenStorageLifetimeProgress::default();
        let mut prepared = false;
        for _ in 0..160 {
            assert!(progress.sweep_quantum(&store).await.unwrap());
            if progress
                .sweep
                .iter()
                .chain(progress.sweep_pending.iter())
                .any(|s| s.manifest == "batch-physical" && s.prepared.is_some())
            {
                prepared = true;
                break;
            }
        }
        assert!(prepared);
        exec(&store,"UPDATE compaction_frozen_history SET identity_sha256='changed' WHERE id='mutable-consumer'").await;
        exec(&store,"UPDATE compaction_frozen_history SET identity_sha256='fixture' WHERE id='mutable-consumer'").await;
        assert_eq!(
            work(&store, "batch-physical", kind).await.revision,
            selected.revision
        );
        // Resume the existing physical step before giving fanout a turn. The
        // captured target revision still matches, but the relevant fence fails.
        // Capture the actual checked geometry guard; even that proof cannot
        // acknowledge work while relevant propagation remains outstanding.
        let guard = progress
            .sweep
            .iter()
            .chain(progress.sweep_pending.iter())
            .find(|s| s.manifest == "batch-physical" && s.prepared.is_some())
            .unwrap()
            .completion_guard();
        progress.sweep_discover = false;
        assert!(progress.sweep_quantum(&store).await.unwrap());
        assert_eq!(batch_ordinals(&store, kind).await, vec![0, 1, 2, 3]);
        assert_eq!(pending(&store, "batch-physical", kind).await, 1);
        assert_eq!(progress.accounting.rows, [0, 0]);
        drop(progress); // lose advisory frontier and all active preparations
        selected.finish(&store, Some(guard)).await.unwrap();
        assert_eq!(pending(&store, "batch-physical", kind).await, 1);
        let mut restarted = FrozenStorageLifetimeProgress::default();
        let mut idle = false;
        for _ in 0..320 {
            let more = restarted.sweep_quantum(&store).await.unwrap();
            let keys = restarted
                .sweep
                .iter()
                .chain(restarted.sweep_pending.iter())
                .map(|s| (&s.manifest, s.kind))
                .collect::<Vec<_>>();
            assert!(keys.len() <= ACTIVE_CONTAINERS);
            assert_eq!(keys.len(), keys.iter().collect::<BTreeSet<_>>().len());
            if !more {
                idle = true;
                break;
            }
        }
        assert!(idle);
        assert!(work(&store, "batch-physical", kind).await.revision > selected.revision);
        assert_eq!(batch_ordinals(&store, kind).await, vec![0]);
        assert_eq!(pending(&store, "batch-physical", kind).await, 0);
        assert_eq!(restarted.accounting.rows[kind as usize], 3);
    }
}
