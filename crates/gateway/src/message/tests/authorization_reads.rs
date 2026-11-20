use super::*;
use sea_orm::{DatabaseBackend, Statement};

#[pioneer_postgres_test::postgres_test]
async fn projected_binding_keeps_internal_capsule_and_corrupt_lineage_checks() {
    use crate::authorization::ExecutionAuthorizationContext;
    let harness = setup_progress_delta_harness(
        "authlineage",
        TurnItem::AgentMessage {
            id: "auth-lineage-item".into(),
            text: String::new(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        },
        false,
    )
    .await;
    let store = harness.crud_store.as_ref();
    let db = store.database_connection();
    let child = harness.thread_id.as_str();
    let turn = harness.turn_id.as_str();
    let mut root = store.get_thread_model(child).await.unwrap().unwrap();
    root.id = generate_test_request_id("thread", "authroot");
    root.turns.clear();
    store
        .upsert_thread_model(
            &root,
            PersistedActorRef::Principal(authenticated_test_superuser().principal_id.clone()),
        )
        .await
        .unwrap();
    pioneer_entity::thread::Entity::update_many()
        .col_expr(
            pioneer_entity::thread::Column::AccessClass,
            Expr::value("internal"),
        )
        .filter(pioneer_entity::thread::Column::Id.eq(child))
        .exec(&db)
        .await
        .unwrap();
    let error = ExecutionAuthorizationContext::load_for_turn(store, turn)
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("root points at an internal child"));
    let context = ExecutionAuthorizationContext::for_test(
        authenticated_test_superuser().as_ref(),
        &harness.workspace_id,
        &root.id,
        &default_test_permission_profile(),
        None,
    );
    store
        .set_turn_execution_authorization_context(turn, &context.to_persisted_json().unwrap())
        .await
        .unwrap();
    let error = ExecutionAuthorizationContext::load_for_turn(store, turn)
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("child Turn has no durable lineage"));
    pioneer_entity::thread_lineage::Entity::insert(pioneer_entity::thread_lineage::ActiveModel {
        insertion_ordinal: Default::default(),
        publication_xid: Default::default(),
        child_thread_id: Set(child.into()),
        parent_thread_id: Set(root.id.clone()),
        root_thread_id: Set(root.id.clone()),
        depth: Set(1),
        created_at: Set(chrono::Utc::now().fixed_offset()),
        origin_kind: Set(Some("task_run".into())),
        created_by_thread_id: Set(Some(root.id.clone())),
        created_by_turn_id: Set(Some(turn.into())),
    })
    .exec(&db)
    .await
    .unwrap();
    // Principal-authored internal continuations validate capsule isolation
    // without impersonating a System-derived Task turn.
    ExecutionAuthorizationContext::load_for_turn(store, turn)
        .await
        .unwrap();
    pioneer_entity::thread_lineage::Entity::update_many()
        .col_expr(
            pioneer_entity::thread_lineage::Column::RootThreadId,
            Expr::value(child),
        )
        .filter(pioneer_entity::thread_lineage::Column::ChildThreadId.eq(child))
        .exec(&db)
        .await
        .unwrap();
    let error = ExecutionAuthorizationContext::load_for_turn(store, turn)
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("lineage differs from its authority envelope"));
    pioneer_entity::thread_lineage::Entity::update_many()
        .col_expr(
            pioneer_entity::thread_lineage::Column::RootThreadId,
            Expr::value(root.id.clone()),
        )
        .filter(pioneer_entity::thread_lineage::Column::ChildThreadId.eq(child))
        .exec(&db)
        .await
        .unwrap();
    pioneer_entity::turn::Entity::update_many()
        .col_expr(
            pioneer_entity::turn::Column::InitiatedByActorKind,
            Expr::value("system"),
        )
        .col_expr(
            pioneer_entity::turn::Column::InitiatedByActorId,
            Expr::value(None::<String>),
        )
        .filter(pioneer_entity::turn::Column::Id.eq(turn))
        .exec(&db)
        .await
        .unwrap();
    let error = ExecutionAuthorizationContext::load_for_turn(store, turn)
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("lineage is self-referential"));
    // A different creating Turn outside the declared parent is still denied.
    pioneer_entity::thread_lineage::Entity::update_many()
        .col_expr(
            pioneer_entity::thread_lineage::Column::CreatedByTurnId,
            Expr::value("missing-parent-turn"),
        )
        .filter(pioneer_entity::thread_lineage::Column::ChildThreadId.eq(child))
        .exec(&db)
        .await
        .unwrap();
    let error = ExecutionAuthorizationContext::load_for_turn(store, turn)
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("creating Turn is missing or outside its parent"));
}

#[pioneer_postgres_test::postgres_test]
async fn durable_commit_rereads_envelope_without_an_acl_generation_change() {
    let harness = setup_progress_delta_harness(
        "authprojection",
        TurnItem::AgentMessage {
            id: "auth-projection-item".into(),
            text: String::new(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        },
        false,
    )
    .await;
    let processor = &harness.processor;
    let store = &harness.crud_store;
    let turn_id = harness.turn_id.as_str();
    let generation = processor.current_authorization_revision().await.unwrap();
    let original = store
        .get_turn_execution_authorization_context(turn_id)
        .await
        .unwrap()
        .unwrap();
    drop(processor.guard_execution_commit(turn_id).await.unwrap());

    // A warm lease must see a missing envelope and valid JSON with an invalid
    // envelope shape. PostgreSQL rejects malformed JSON before it can persist.
    assert!(
        store
            .set_turn_execution_authorization_context(turn_id, "malformed")
            .await
            .is_err()
    );
    assert_eq!(
        processor.current_authorization_revision().await.unwrap(),
        generation
    );
    for encoded in [None, Some("{}")] {
        if let Some(encoded) = encoded {
            assert!(
                store
                    .set_turn_execution_authorization_context(turn_id, encoded)
                    .await
                    .unwrap()
            );
        } else {
            pioneer_entity::turn::Entity::update_many()
                .col_expr(
                    pioneer_entity::turn::Column::ExecutionAuthorizationContextJson,
                    Expr::value(None::<serde_json::Value>),
                )
                .filter(pioneer_entity::turn::Column::Id.eq(turn_id))
                .exec(&store.database_connection())
                .await
                .unwrap();
        }
        let error = processor.guard_execution_commit(turn_id).await.unwrap_err();
        let expected = if encoded.is_none() {
            "no durable authority envelope"
        } else {
            "invalid authority envelope"
        };
        assert!(format!("{error:#}").contains(expected));
        assert_eq!(
            processor.current_authorization_revision().await.unwrap(),
            generation
        );
        store
            .set_turn_execution_authorization_context(turn_id, &original)
            .await
            .unwrap();
        drop(processor.guard_execution_commit(turn_id).await.unwrap());
    }

    let wrong_workspace = crate::authorization::ExecutionAuthorizationContext::for_test(
        authenticated_test_superuser().as_ref(),
        "another-workspace",
        &harness.thread_id,
        &default_test_permission_profile(),
        None,
    )
    .to_persisted_json()
    .unwrap();
    store
        .set_turn_execution_authorization_context(turn_id, &wrong_workspace)
        .await
        .unwrap();
    assert!(
        format!(
            "{:#}",
            processor.guard_execution_commit(turn_id).await.unwrap_err()
        )
        .contains("workspace differs from its authority envelope")
    );
    assert_eq!(
        processor.current_authorization_revision().await.unwrap(),
        generation
    );
    store
        .set_turn_execution_authorization_context(turn_id, &original)
        .await
        .unwrap();

    // All persisted initiator variants still reach their specific binding guard.
    for (kind, id, expected) in [
        (
            "principal",
            Some("P0000000000000000000B"),
            "initiator differs from its principal grant",
        ),
        (
            "system",
            None,
            "System execution at a capsule root has no typed Task derivation",
        ),
        (
            "agent_execution",
            Some("E0000000000000000000B"),
            "agent execution actor cannot author a capsule-root Turn",
        ),
    ] {
        store
            .database_connection()
            .execute_raw(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "UPDATE \"turn\" SET initiated_by_actor_kind=$1, initiated_by_actor_id=$2 WHERE id=$3",
                vec![kind.into(), id.into(), turn_id.into()],
            ))
            .await
            .unwrap();
        assert!(
            format!(
                "{:#}",
                processor.guard_execution_commit(turn_id).await.unwrap_err()
            )
            .contains(expected)
        );
    }
    store.database_connection().execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "UPDATE \"turn\" SET initiated_by_actor_kind='principal', initiated_by_actor_id=$1 WHERE id=$2",
        vec![authenticated_test_superuser().principal_id.as_str().into(), turn_id.into()],
    )).await.unwrap();

    // Binding reads must ignore corrupt presentation snapshots yet continue
    // to reject mutable terminal status through the real lease guard.
    store.database_connection().execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "UPDATE \"turn\" SET prompt_manifest_json=$1, permission_profile_snapshot_json=$1, mentions_json=$1 WHERE id=$2",
        [r#""broken""#.into(), turn_id.into()],
    )).await.unwrap();
    drop(processor.guard_execution_commit(turn_id).await.unwrap());
    for status in [TurnStatus::Completed, TurnStatus::Interrupted] {
        store
            .update_turn_status(
                &harness.thread_id,
                turn_id,
                status,
                None,
                chrono::Utc::now().timestamp(),
            )
            .await
            .unwrap();
        let error = processor.guard_execution_commit(turn_id).await.unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<crate::authorization::ExecutionTurnNotInProgress>()
                .unwrap()
                .status(),
            status
        );
    }
}
