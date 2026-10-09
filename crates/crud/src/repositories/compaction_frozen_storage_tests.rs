//! Structural span-copy fixtures isolate metadata budgets, not import authority
//! or verified finish. Ready legacy headers/spans are seeded explicitly; no
//! synthetic receipt is submitted to the full capture verifier.
use super::*;
use migration::{Migrator, MigratorTrait};
use pioneer_compaction::frozen::FrozenHistoryRef;
use sea_orm::Database;

async fn fixture(
    kind: i64,
    sources: &[String],
    suffix: i64,
) -> (CrudStore, crate::FrozenUseGuard, crate::FrozenUseGuard) {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    let store = CrudStore::new(db).with_maintenance_access();
    let db = store.database_connection();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',1,1)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    let count = sources.len() as i64;
    let base = format!("base-{}", "б".repeat(2048));
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready) VALUES (?,'ws','thread',?,?,?,? ,?,?,1)",
        [base.clone().into(),"a".repeat(64).into(),count.into(),count.into(),(if kind==1 {count} else {0}).into(),"b".repeat(64).into(),(if kind==1 {count} else {0}).into()])).await.unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES (?,?,1,0)",
        [base.clone().into(), kind.into()],
    ))
    .await
    .unwrap();
    for (ordinal, id) in sources.iter().enumerate() {
        // Zero-count backing may have a foreign physical tail. This fixture
        // copies pointers only; it intentionally never reads that tail's body.
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,import_count,imports_sha256,next_import,ready) VALUES (?,'ws','thread',?,0,0,0,?,0,1)",
            [id.clone().into(),super::super::compaction_frozen_import::EMPTY_FROZEN_IMPORT_SHA256.into(),super::super::compaction_frozen_import::EMPTY_FROZEN_IMPORT_SHA256.into()])).await.unwrap();
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES (?,?,?,?,?)",
            [base.clone().into(),kind.into(),(ordinal as i64).into(),(ordinal as i64+1).into(),id.clone().into()])).await.unwrap();
    }
    let candidate = store
        .compaction_acquire_frozen_use(
            "ws",
            &FrozenHistoryRef {
                format: 1,
                manifest_id: base.clone(),
                messages: count as u64,
                identity_sha256: "a".repeat(64),
            },
            Some("thread"),
        )
        .await
        .unwrap();
    let target = format!("target-{}", "т".repeat(2048));
    let descriptor = FrozenHistoryRef {
        format: 1,
        manifest_id: target.clone(),
        messages: (count + suffix) as u64,
        identity_sha256: "a".repeat(64),
    };
    let guard = store
        .compaction_begin_frozen_history_with_imports(
            "ws",
            "thread",
            &descriptor,
            if kind == 1 {
                (count + suffix) as u64
            } else {
                0
            },
            super::super::compaction_frozen_import::EMPTY_FROZEN_IMPORT_SHA256,
        )
        .await
        .unwrap();
    if suffix > 0 {
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            if kind == 0 {
                "UPDATE compaction_frozen_history SET next_ordinal=? WHERE id=?"
            } else {
                "UPDATE compaction_frozen_history SET next_import=? WHERE id=?"
            },
            [(count + suffix).into(), target.clone().into()],
        ))
        .await
        .unwrap();
    }
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending,candidate) VALUES (?,?,0,1,?)",
        [target.into(),kind.into(),base.into()])).await.unwrap();
    (store, guard, candidate)
}

#[tokio::test]
async fn long_utf8_span_ids_copy_only_a_byte_bounded_contiguous_prefix_and_retry_atomically() {
    for kind in [0, 1] {
        let sources = (0..130)
            .map(|i| format!("physical-{i}-{}", "с".repeat(4096)))
            .collect::<Vec<_>>();
        let (store, guard, candidate) = fixture(kind, &sources, 2).await;
        let h = history::Entity::find_by_id(&guard.header().id)
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        let mut state = layout::Entity::find_by_id((h.id.clone(), kind))
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        let old = state.clone();
        let mut bytes = h.id.len() * 2 + 24; // Final own suffix reserved in this batch budget.
        let mut expected = 0_i64;
        for source in &sources {
            let size = span_copy_bytes(&candidate.header().id, &h.id, source.len() as i64).unwrap();
            if bytes + size > BYTES as usize {
                break;
            }
            bytes += size;
            expected += 1;
        }
        assert!(expected > 0 && expected < 128);
        assert_eq!(
            publish(&store, &h, &state, 130, &guard, Some(&candidate))
                .await
                .unwrap(),
            CopyOutcome::Progress
        );
        state = layout::Entity::find_by_id((h.id.clone(), kind))
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(state.copy_next, expected);
        assert_eq!(state.copy_to, Some(130));
        assert_eq!(state.active, 0);
        assert_eq!(
            publish(&store, &h, &old, 130, &guard, Some(&candidate))
                .await
                .unwrap(),
            CopyOutcome::Progress
        );
        // Span writes precede cursor CAS. A failure in CAS rolls the entire
        // second quantum back while retaining the committed first prefix.
        store.connection.execute_unprepared("CREATE TRIGGER abort_span_cursor BEFORE UPDATE OF copy_next ON compaction_frozen_layout BEGIN SELECT RAISE(ABORT,'span cursor rollback'); END").await.unwrap();
        assert!(
            publish(&store, &h, &state, 130, &guard, Some(&candidate))
                .await
                .is_err()
        );
        assert_eq!(
            layout::Entity::find_by_id((h.id.clone(), kind))
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap(),
            state
        );
        let spans = span::Entity::find()
            .filter(span::Column::ManifestId.eq(&h.id))
            .filter(span::Column::Kind.eq(kind))
            .order_by_asc(span::Column::Start)
            .all(&store.connection)
            .await
            .unwrap();
        assert_eq!(spans.len() as i64, expected);
        store
            .connection
            .execute_unprepared("DROP TRIGGER abort_span_cursor")
            .await
            .unwrap();
        // A retry may never overwrite a pre-existing different physical pointer.
        store.connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO compaction_frozen_span(manifest_id,kind,start,end,source_manifest) VALUES (?,?,?,?,?)",
            [h.id.clone().into(),kind.into(),expected.into(),(expected+1).into(),sources[0].clone().into()])).await.unwrap();
        let error = publish(&store, &h, &state, 130, &guard, Some(&candidate))
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "copied span retry changed physical pointer"
        );
        assert_eq!(
            layout::Entity::find_by_id((h.id.clone(), kind))
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap(),
            state
        );
        store
            .connection
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "DELETE FROM compaction_frozen_span WHERE manifest_id=? AND kind=? AND start=?",
                [h.id.clone().into(), kind.into(), expected.into()],
            ))
            .await
            .unwrap();
        loop {
            let outcome = publish(&store, &h, &state, 130, &guard, Some(&candidate))
                .await
                .unwrap();
            state = layout::Entity::find_by_id((h.id.clone(), kind))
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap();
            if outcome == CopyOutcome::Complete {
                break;
            }
            assert_eq!(outcome, CopyOutcome::Progress);
        }
        assert_eq!(state.copy_next, 130);
        assert_eq!(state.active, 1);
        let spans = span::Entity::find()
            .filter(span::Column::ManifestId.eq(&h.id))
            .filter(span::Column::Kind.eq(kind))
            .order_by_asc(span::Column::Start)
            .all(&store.connection)
            .await
            .unwrap();
        assert_eq!(spans.len(), 131);
        for (i, source) in sources.iter().enumerate() {
            assert_eq!(spans[i].source_manifest, *source);
            assert_eq!(spans[i].start, i as i64);
            assert_eq!(spans[i].end, i as i64 + 1);
        }
        assert_eq!(spans[130].source_manifest, h.id);
        assert_eq!((spans[130].start, spans[130].end), (130, 132));
        assert_eq!(
            publish(&store, &h, &state, 130, &guard, Some(&candidate))
                .await
                .unwrap(),
            CopyOutcome::Complete
        );
        candidate.close().await.unwrap();
        guard.close().await.unwrap();
    }
}

#[tokio::test]
async fn oversized_legal_span_is_retained_without_eof_truncation_or_cursor_advance() {
    for kind in [0, 1] {
        let sources = vec![format!("oversized-{}", "я".repeat(140_000))];
        let (store, guard, candidate) = fixture(kind, &sources, 0).await;
        let h = history::Entity::find_by_id(&guard.header().id)
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        let state = layout::Entity::find_by_id((h.id.clone(), kind))
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            publish(&store, &h, &state, 1, &guard, Some(&candidate))
                .await
                .unwrap(),
            CopyOutcome::Retained
        );
        let retained = layout::Entity::find_by_id((h.id.clone(), kind))
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retained.copy_next, 0);
        assert_eq!(retained.copy_to, None);
        assert_eq!(retained.active, 0);
        assert_eq!(retained.candidate, state.candidate);
        assert_eq!(retained.failed, 1);
        assert_eq!(retained.pending, 0);
        assert!(
            span::Entity::find()
                .filter(span::Column::ManifestId.eq(&h.id))
                .all(&store.connection)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            publish(&store, &h, &retained, 1, &guard, Some(&candidate))
                .await
                .unwrap(),
            CopyOutcome::Retained
        );
        candidate.close().await.unwrap();
        guard.close().await.unwrap();
    }
}
