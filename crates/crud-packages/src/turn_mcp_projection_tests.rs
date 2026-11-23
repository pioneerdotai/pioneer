use super::*;
use crate::package_store::PackageStore;
use pioneer_postgres_pools::PostgresDatabase;

async fn projection_fixture(
    fixture: &pioneer_postgres_test::TestDatabase,
    workspace: &str,
    thread: &str,
    turn: &str,
) -> (PostgresDatabase, PackageStore) {
    use sea_orm::{DbBackend, Statement};
    let writer = fixture.connection();
    let txn = writer.begin().await.unwrap();
    for (sql, values) in [
        (
            "INSERT INTO workspace(id,name) VALUES ($1,'Projection test')",
            vec![workspace.into()],
        ),
        (
            "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status) VALUES ($1,$2,'','agent','model','provider','active')",
            vec![thread.into(), workspace.into()],
        ),
        (
            "INSERT INTO turn(id,thread_id,status) VALUES ($1,$2,'in_progress')",
            vec![turn.into(), thread.into()],
        ),
    ] {
        txn.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            sql,
            values,
        ))
        .await
        .unwrap();
    }
    txn.commit().await.unwrap();
    let db = PostgresDatabase::new(fixture.connect_read_only().await.unwrap(), writer.clone());
    (db.clone(), PackageStore::new(db))
}

fn test_turn_mcp_projection(
    turn_id: &str,
    workspace_id: &str,
    manifest_hash: &str,
    tool_count: i32,
) -> TurnMcpProjectionRecord {
    TurnMcpProjectionRecord {
        turn_id: turn_id.to_owned(),
        workspace_id: workspace_id.to_owned(),
        projection_version: 1,
        manifest_hash: manifest_hash.to_owned(),
        resolution_status: "resolved".to_owned(),
        tool_count,
        created_at_unix: 1_700_000_100,
    }
}

fn test_turn_mcp_binding(index: usize) -> TurnMcpBindingRecord {
    let canonical_callable_name = format!("mcp_test_tool_{index}");
    TurnMcpBindingRecord {
        server_installation_id: format!("server_mcp_{index:02}"),
        server_name: "test".to_owned(),
        raw_tool_name: format!("tool_{index}"),
        callable_name: canonical_callable_name.clone(),
        canonical_callable_name: canonical_callable_name.clone(),
        provider_callable_name: format!("mcp__pioneer__{canonical_callable_name}"),
        catalog_version: "catalog-v1".to_owned(),
        fingerprint: format!("installation-fingerprint-{index}"),
        canonical_schema_fingerprint: format!("canonical-schema-{index}"),
        provider_schema_fingerprint: format!("provider-schema-{index}"),
        annotations_json: r#"{"readOnlyHint":true}"#.to_owned(),
        annotations_digest: format!("annotations-{index}"),
        effective_timeout_ms: 20_000,
        runtime_generation: 7,
        projection_activation_generation: 3,
        selection_reason: "explicit_composer_capability".to_owned(),
        capability_id: Some(format!("mcp:test:tool_{index}")),
    }
}

#[tokio::test]
async fn turn_mcp_projection_atomic_projection_replace_handles_zero_one_many_and_invalid_inputs() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (db, store) = projection_fixture(
            fixture,
            "workspace_projection_replace",
            "thread_projection_replace",
            "turn_projection_replace",
        )
        .await;

        let invalid_count = TurnMcpProjectionReplacement {
            projection: test_turn_mcp_projection(
                "turn_projection_replace",
                "workspace_projection_replace",
                "manifest-invalid-count",
                2,
            ),
            bindings: vec![test_turn_mcp_binding(1)],
        };
        assert!(matches!(
            store
                .replace_turn_mcp_projection(&invalid_count)
                .await
                .expect_err("declared count mismatch must fail"),
            TurnMcpProjectionPersistenceError::InvalidToolCount {
                declared: 2,
                actual: 1
            }
        ));

        let wrong_workspace = TurnMcpProjectionReplacement {
            projection: test_turn_mcp_projection(
                "turn_projection_replace",
                "workspace_other",
                "manifest-wrong-workspace",
                0,
            ),
            bindings: Vec::new(),
        };
        assert!(matches!(
            store
                .replace_turn_mcp_projection(&wrong_workspace)
                .await
                .expect_err("workspace mismatch must fail"),
            TurnMcpProjectionPersistenceError::WorkspaceMismatch { .. }
        ));

        let one = TurnMcpProjectionReplacement {
            projection: test_turn_mcp_projection(
                "turn_projection_replace",
                "workspace_projection_replace",
                "manifest-one",
                1,
            ),
            bindings: vec![test_turn_mcp_binding(1)],
        };
        let one_outcome = store
            .replace_turn_mcp_projection(&one)
            .await
            .expect("one-tool projection should persist");
        assert_eq!(one_outcome.manifest_hash, "manifest-one");
        assert_eq!(
            store
                .get_turn_mcp_projection("turn_projection_replace")
                .await
                .expect("projection header query should succeed"),
            Some(one.projection.clone())
        );
        assert_eq!(
            store
                .list_turn_mcp_bindings("turn_projection_replace")
                .await
                .expect("one-tool bindings should load"),
            one.bindings
        );

        store
            .replace_turn_mcp_projection(&one)
            .await
            .expect("identical projection replacement should be safe");
        assert_eq!(
            store
                .list_turn_mcp_bindings("turn_projection_replace")
                .await
                .expect("repeated bindings should load"),
            one.bindings
        );

        let many = TurnMcpProjectionReplacement {
            projection: test_turn_mcp_projection(
                "turn_projection_replace",
                "workspace_projection_replace",
                "manifest-many",
                3,
            ),
            bindings: vec![
                test_turn_mcp_binding(3),
                test_turn_mcp_binding(1),
                test_turn_mcp_binding(2),
            ],
        };
        store
            .replace_turn_mcp_projection(&many)
            .await
            .expect("many-tool projection should replace prior state");
        let persisted_many = store
            .list_turn_mcp_bindings("turn_projection_replace")
            .await
            .expect("many-tool bindings should load");
        assert_eq!(
            persisted_many
                .iter()
                .map(|binding| binding.canonical_callable_name.as_str())
                .collect::<Vec<_>>(),
            vec!["mcp_test_tool_1", "mcp_test_tool_2", "mcp_test_tool_3"]
        );
        assert_eq!(persisted_many[0].annotations_digest, "annotations-1");
        assert_eq!(persisted_many[2].runtime_generation, 7);

        let zero = TurnMcpProjectionReplacement {
            projection: test_turn_mcp_projection(
                "turn_projection_replace",
                "workspace_projection_replace",
                "manifest-zero",
                0,
            ),
            bindings: Vec::new(),
        };
        store
            .replace_turn_mcp_projection(&zero)
            .await
            .expect("zero-tool projection should persist and clear bindings");
        assert_eq!(
            store
                .get_turn_mcp_projection("turn_projection_replace")
                .await
                .expect("zero projection header query should succeed"),
            Some(zero.projection)
        );
        assert!(
            store
                .list_turn_mcp_bindings("turn_projection_replace")
                .await
                .expect("zero projection binding query should succeed")
                .is_empty()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn atomic_projection_replace_rolls_back_header_delete_and_every_insert_fault() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (db, store) = projection_fixture(
            fixture,
            "workspace_projection_fault",
            "thread_projection_fault",
            "turn_projection_fault",
        )
        .await;
        let committed = TurnMcpProjectionReplacement {
            projection: test_turn_mcp_projection(
                "turn_projection_fault",
                "workspace_projection_fault",
                "manifest-committed",
                1,
            ),
            bindings: vec![test_turn_mcp_binding(9)],
        };
        store
            .replace_turn_mcp_projection(&committed)
            .await
            .expect("baseline projection should persist");

        let attempted = TurnMcpProjectionReplacement {
            projection: test_turn_mcp_projection(
                "turn_projection_fault",
                "workspace_projection_fault",
                "manifest-attempted",
                3,
            ),
            bindings: vec![
                test_turn_mcp_binding(1),
                test_turn_mcp_binding(2),
                test_turn_mcp_binding(3),
            ],
        };
        for fault in [
            AtomicProjectionReplaceFault::AfterHeader,
            AtomicProjectionReplaceFault::AfterDelete,
            AtomicProjectionReplaceFault::AfterInsert(1),
            AtomicProjectionReplaceFault::AfterInsert(2),
            AtomicProjectionReplaceFault::AfterInsert(3),
        ] {
            replace_turn_mcp_projection_with_fault(&db, &attempted, fault)
                .await
                .expect_err("injected transaction fault must fail");
            assert_eq!(
                store
                    .get_turn_mcp_projection("turn_projection_fault")
                    .await
                    .expect("committed header query should succeed"),
                Some(committed.projection.clone()),
                "header changed after {fault:?}"
            );
            assert_eq!(
                store
                    .list_turn_mcp_bindings("turn_projection_fault")
                    .await
                    .expect("committed bindings query should succeed"),
                committed.bindings,
                "bindings changed after {fault:?}"
            );
        }
        Ok(())
    })
    .await
    .unwrap();
}
