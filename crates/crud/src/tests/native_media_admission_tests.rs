use super::*;
use pioneer_protocol::ArtifactStatus;
use sea_orm::ActiveModelTrait;

#[tokio::test]
async fn exact_native_input_binding_is_atomic_idempotent_and_revalidates_scope() {
    let store = test_store_with_workspace("ws_media_acceptance").await;
    let timestamp = 1_700_000_000;
    let thread = sample_thread("ws_media_acceptance", "thread_media_acceptance", timestamp);
    let seed = sample_turn("turn_media_seed");
    store
        .materialize_turn_start(
            &thread,
            SandboxMode::FullAccess,
            &seed,
            &[],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    let ingested = store
        .ingest_artifact_metadata(
            NewArtifactBlobRecord {
                workspace_id: thread.workspace_id.clone(),
                sha256: "a".repeat(64),
                size_bytes: 8,
                mime_type: Some("image/png".into()),
                storage_backend: "fixture".into(),
                storage_key: "immutable-original".into(),
                metadata: BTreeMap::new(),
            },
            IngestArtifactMetadataRecord {
                workspace_id: thread.workspace_id.clone(),
                primary_thread_id: Some(thread.id.clone()),
                display_name: "original.png".into(),
                kind: ArtifactKind::Image,
                mime_type: Some("image/png".into()),
                created_by_kind: ArtifactCreatedByKind::User,
                created_by_actor_id: None,
                metadata: BTreeMap::new(),
            },
            None,
            BTreeMap::new(),
        )
        .await
        .unwrap();
    for (index, input, expected_ok) in [
        (
            0,
            UserInput::Artifact {
                artifact_id: ingested.artifact.id.clone(),
                version_id: Some("missing-version".into()),
            },
            false,
        ),
        (
            1,
            UserInput::LocalImage {
                path: "/private/SECRET/mutable.png".into(),
            },
            false,
        ),
        (
            2,
            UserInput::Artifact {
                artifact_id: ingested.artifact.id.clone(),
                version_id: Some(ingested.version.id.clone()),
            },
            true,
        ),
        (
            3,
            UserInput::Artifact {
                artifact_id: ingested.artifact.id.clone(),
                version_id: Some(ingested.version.id.clone()),
            },
            false,
        ),
    ] {
        if index == 3 {
            // Ingress already observed Ready; the commit must revalidate a
            // deletion that raced that preparation, including the binding.
            store
                .update_artifact_status(
                    &thread.workspace_id,
                    &ingested.artifact.id,
                    ArtifactStatus::Deleted,
                    Some(unix_to_datetime(timestamp + 1)),
                )
                .await
                .unwrap();
        }
        let mut turn = sample_turn(&format!("turn_media_acceptance_{index}"));
        turn.mode = ThreadMode::Agent;
        let audit = pioneer_protocol::TurnPermissionAuditEvent {
            workspace_id: thread.workspace_id.clone(),
            thread_id: thread.id.clone(),
            turn_id: turn.id.clone(),
            event_kind: TurnPermissionAuditEventKind::ProfileSelected,
            profile_mode: turn.permission_profile.mode,
            profile_source: turn.permission_profile.source,
            security_snapshot_id: None,
            security_snapshot_version: None,
            security_reason_code: None,
            security_capability: None,
            item_id: None,
            tool_call_id: None,
            tool_name: None,
            action_kind: None,
            request_key: None,
            decision: None,
            reason: None,
            cached: false,
        };
        let result = store
            .materialize_authorized_turn_start_with_reasoning_effort_and_permission_audit(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[input],
                None,
                TurnWorkOwner::Turn,
                PersistedActorRef::System,
                audit,
                r#"{"kind":"test_explicit_authority"}"#,
                None,
                Some(crate::NewTurnAdmission {
                    turn_id: turn.id.clone(),
                    thread_id: thread.id.clone(),
                    workspace_id: thread.workspace_id.clone(),
                    request_digest: "b".repeat(64),
                    policy_generation: None,
                    role_key: None,
                    policy_fingerprint: None,
                    execution_lease: None,
                }),
                Some(crate::NewTurnExecution {
                    turn_id: turn.id.clone(),
                    thread_id: thread.id.clone(),
                    workspace_id: thread.workspace_id.clone(),
                    executor_kind: crate::TurnExecutorKind::NativeAgent,
                    executor_key: Some("fixture".into()),
                    status: crate::TurnExecutionStatus::Starting,
                    owner_id: "owner".into(),
                    lease_until: unix_to_datetime(timestamp + 45),
                    created_at: unix_to_datetime(timestamp),
                }),
                &TurnExecutionSecuritySnapshot::unrestricted_full_access(
                    "/fixture",
                    timestamp * 1000,
                ),
                Vec::new(),
                None,
                None,
            )
            .await;
        assert_eq!(result.is_ok(), expected_ok, "scenario{index}: {result:?}");
        assert_eq!(
            store
                .get_turn(&thread.id, &turn.id)
                .await
                .unwrap()
                .is_some(),
            expected_ok
        );
        assert_eq!(
            store.get_turn_admission(&turn.id).await.unwrap().is_some(),
            expected_ok
        );
        if !expected_ok {
            assert_eq!(
                pioneer_entity::artifact_binding::Entity::find()
                    .filter(pioneer_entity::artifact_binding::Column::TurnId.eq(&turn.id))
                    .count(&store.connection)
                    .await
                    .unwrap(),
                0
            );
        }
        if expected_ok {
            let prepared = crate::repositories::artifact::prepare_native_input_binding(
                &thread.workspace_id,
                &thread.id,
                &turn.id,
                &ingested.artifact.id,
                &ingested.version.id,
                0,
                unix_to_datetime(timestamp),
            );
            crate::repositories::artifact::apply_native_input_binding(&store.connection, prepared)
                .await
                .unwrap();
            let pins = pioneer_entity::artifact_binding::Entity::find()
                .filter(
                    pioneer_entity::artifact_binding::Column::ArtifactId.eq(&ingested.artifact.id),
                )
                .filter(pioneer_entity::artifact_binding::Column::TurnId.eq(&turn.id))
                .all(&store.connection)
                .await
                .unwrap();
            assert_eq!(pins.len(), 1);
            assert_eq!(
                pins[0].artifact_version_id.as_deref(),
                Some(ingested.version.id.as_str())
            );
            let original_binding_id = pins[0].id.clone();
            let canonical_item = format!("user_{}", turn.id);
            let target = ArtifactBindingTargetRecord {
                thread_id: Some(thread.id.clone()),
                turn_id: Some(turn.id.clone()),
                message_id: Some(canonical_item.clone()),
                turn_item_id: Some(canonical_item.clone()),
                tool_call_id: None,
                task_id: None,
                task_run_id: None,
                binding_kind: ArtifactBindingKind::UserInput,
                direction: ArtifactBindingDirection::Input,
                role: Some(ArtifactRole::User),
                item_index: Some(0),
            };
            for replay in 0..2 {
                let access = if replay == 0 {
                    store.clone()
                } else {
                    store.with_maintenance_access()
                };
                let published = access
                    .bind_artifact(
                        &thread.workspace_id,
                        &ingested.artifact.id,
                        Some(&ingested.version.id),
                        target.clone(),
                        BTreeMap::new(),
                    )
                    .await
                    .unwrap();
                assert_eq!(published.binding_id, original_binding_id);
                assert_eq!(
                    published.message_id.as_deref(),
                    Some(canonical_item.as_str())
                );
                assert_eq!(
                    published.turn_item_id.as_deref(),
                    Some(canonical_item.as_str())
                );
            }
            assert_eq!(
                pioneer_entity::artifact_binding::Entity::find()
                    .filter(pioneer_entity::artifact_binding::Column::TurnId.eq(&turn.id))
                    .count(&store.connection)
                    .await
                    .unwrap(),
                1
            );
            // Older publication produced this exact canonical-plus-placeholder
            // ambiguity. Refuse it atomically rather than delete an ownership pin.
            let mut duplicate = sea_orm::IntoActiveModel::into_active_model(pins[0].clone());
            duplicate.id = Set(pioneer_protocol::generate_id(21));
            duplicate.insert(&store.connection).await.unwrap();
            assert!(
                store
                    .bind_artifact(
                        &thread.workspace_id,
                        &ingested.artifact.id,
                        Some(&ingested.version.id),
                        target,
                        BTreeMap::new()
                    )
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("accepted input binding is ambiguous")
            );
            assert_eq!(
                pioneer_entity::artifact_binding::Entity::find()
                    .filter(pioneer_entity::artifact_binding::Column::TurnId.eq(&turn.id))
                    .count(&store.connection)
                    .await
                    .unwrap(),
                2
            );
        }
    }
    store
        .update_artifact_status(
            &thread.workspace_id,
            &ingested.artifact.id,
            ArtifactStatus::Ready,
            None,
        )
        .await
        .unwrap();
    assert!(
        store
            .native_input_direct_scope_matches(
                &thread.workspace_id,
                &ingested.artifact.id,
                &thread.id
            )
            .await
            .unwrap()
    );
    // A corrupt/foreign workspace binding cannot authorize the direct fallback.
    store.connection.execute_unprepared("INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws_foreign_media','Foreign',1,0)").await.unwrap();
    store.connection.execute_raw(sea_orm::Statement::from_sql_and_values(sea_orm::DatabaseBackend::Sqlite,
        "INSERT INTO artifact_binding(id,artifact_id,artifact_version_id,workspace_id,thread_id,binding_kind,direction,metadata_json,created_at) VALUES('foreign-media-pin',?,?,'ws_foreign_media',?,'user_input','input','{}',CURRENT_TIMESTAMP)",
        [ingested.artifact.id.clone().into(), ingested.version.id.clone().into(), thread.id.clone().into()])).await.unwrap();
    assert!(
        !store
            .native_input_direct_scope_matches(
                &thread.workspace_id,
                &ingested.artifact.id,
                &thread.id
            )
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn task_child_media_admission_uses_exact_atomic_response_root_before_lineage_link() {
    use crate::{
        AgentTurnResponseInput, NewTurnAdmission, NewTurnExecution, TurnExecutionStatus,
        TurnExecutorKind,
    };
    let workspace = "ws_task_native_media";
    let store = test_store_with_workspace(workspace).await;
    let timestamp = 1_700_000_000;
    let parent = sample_thread(workspace, "task_media_parent", timestamp);
    let seed = sample_turn("task_media_seed");
    store
        .materialize_turn_start(
            &parent,
            SandboxMode::FullAccess,
            &seed,
            &[],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    let other_root = sample_thread(workspace, "foreign-root", timestamp);
    store
        .materialize_turn_start(
            &other_root,
            SandboxMode::FullAccess,
            &sample_turn("other_task_media_seed"),
            &[],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    ensure_test_agent_identity(&store, workspace, timestamp).await;
    let mut task = sample_task(timestamp);
    task.id = "task_native_media".into();
    task.workspace_id = workspace.into();
    task.owner_id = Some(parent.id.clone());
    task.created_by_thread_id = Some(parent.id.clone());
    task.created_by_turn_id = Some(seed.id.clone());
    let mut run = sample_task_run(timestamp);
    run.id = "run_native_media".into();
    run.task_id = task.id.clone();
    run.trigger_id = None;
    run.run_group_id = run.id.clone();
    store
        .append_task_events(
            vec![
                TaskEventPayload::TaskCreated { task: task.clone() },
                TaskEventPayload::RunCreated {
                    run: run.clone(),
                    agent_spec: None,
                },
            ],
            timestamp,
        )
        .await
        .unwrap();
    let execution = store
        .reserve_execution_for_run(&run.id, TaskExecutorKind::Agent, timestamp)
        .await
        .unwrap();
    attach_test_agent_execution_contract(
        &store,
        workspace,
        &task.id,
        &run.id,
        &execution.id,
        &parent.id,
        timestamp,
    )
    .await;
    let actor = crate::load_agent_execution(&store.connection, &execution.id)
        .await
        .unwrap()
        .unwrap();
    let ingested = store
        .ingest_artifact_metadata(
            NewArtifactBlobRecord {
                workspace_id: workspace.into(),
                sha256: "c".repeat(64),
                size_bytes: 5,
                mime_type: Some("text/plain".into()),
                storage_backend: "fixture".into(),
                storage_key: "task-media-original".into(),
                metadata: BTreeMap::new(),
            },
            IngestArtifactMetadataRecord {
                workspace_id: workspace.into(),
                primary_thread_id: Some(parent.id.clone()),
                display_name: "original.txt".into(),
                kind: ArtifactKind::File,
                mime_type: Some("text/plain".into()),
                created_by_kind: ArtifactCreatedByKind::User,
                created_by_actor_id: None,
                metadata: BTreeMap::new(),
            },
            None,
            BTreeMap::new(),
        )
        .await
        .unwrap();
    for (index, expected_ok) in [(0, true), (1, false)] {
        if !expected_ok {
            store
                .connection
                .execute_raw(Statement::from_sql_and_values(
                    DatabaseBackend::Sqlite,
                    "UPDATE agent_execution SET home_root_thread_id='foreign-root' WHERE id=?",
                    [execution.id.clone().into()],
                ))
                .await
                .unwrap();
        }
        let mut child = sample_thread(workspace, &format!("task_media_child_{index}"), timestamp);
        child.origin_kind = ThreadOriginKind::TaskRun;
        let mut turn = sample_turn(&format!("task_media_turn_{index}"));
        turn.mode = ThreadMode::Agent;
        let audit = pioneer_protocol::TurnPermissionAuditEvent {
            workspace_id: workspace.into(),
            thread_id: child.id.clone(),
            turn_id: turn.id.clone(),
            event_kind: TurnPermissionAuditEventKind::ProfileSelected,
            profile_mode: turn.permission_profile.mode,
            profile_source: turn.permission_profile.source,
            security_snapshot_id: None,
            security_snapshot_version: None,
            security_reason_code: None,
            security_capability: None,
            item_id: None,
            tool_call_id: None,
            tool_name: None,
            action_kind: None,
            request_key: None,
            decision: None,
            reason: None,
            cached: false,
        };
        let result = store
            .materialize_authorized_turn_start_with_reasoning_effort_and_permission_audit(
                &child,
                SandboxMode::FullAccess,
                &turn,
                &[UserInput::Artifact {
                    artifact_id: ingested.artifact.id.clone(),
                    version_id: Some(ingested.version.id.clone()),
                }],
                None,
                TurnWorkOwner::Turn,
                PersistedActorRef::System,
                audit,
                r#"{"kind":"fixture_exact_task_authority"}"#,
                None,
                Some(NewTurnAdmission {
                    turn_id: turn.id.clone(),
                    thread_id: child.id.clone(),
                    workspace_id: workspace.into(),
                    request_digest: "d".repeat(64),
                    policy_generation: None,
                    role_key: None,
                    policy_fingerprint: None,
                    execution_lease: None,
                }),
                Some(NewTurnExecution {
                    turn_id: turn.id.clone(),
                    thread_id: child.id.clone(),
                    workspace_id: workspace.into(),
                    executor_kind: TurnExecutorKind::NativeAgent,
                    executor_key: None,
                    status: TurnExecutionStatus::Starting,
                    owner_id: "task-media-owner".into(),
                    lease_until: unix_to_datetime(timestamp + 45),
                    created_at: unix_to_datetime(timestamp),
                }),
                &TurnExecutionSecuritySnapshot::unrestricted_full_access(
                    "/fixture",
                    timestamp * 1000,
                ),
                Vec::new(),
                None,
                Some(AgentTurnResponseInput {
                    turn_id: turn.id.clone(),
                    execution_id: execution.id.clone(),
                    presentation_snapshot_id: actor.presentation_snapshot_id.clone().unwrap(),
                    now: unix_to_datetime(timestamp),
                }),
            )
            .await;
        assert_eq!(result.is_ok(), expected_ok, "scenario {index}: {result:?}");
        assert!(
            store
                .get_task_thread_lineage(&child.id)
                .await
                .unwrap()
                .is_none(),
            "the native atomic admission precedes the existing Task lineage link"
        );
        assert_eq!(
            pioneer_entity::artifact_binding::Entity::find()
                .filter(pioneer_entity::artifact_binding::Column::TurnId.eq(&turn.id))
                .count(&store.connection)
                .await
                .unwrap(),
            u64::from(expected_ok)
        );
        if !expected_ok {
            assert!(store.get_turn(&child.id, &turn.id).await.unwrap().is_none());
            assert!(
                crate::load_agent_turn_response(&store.connection, &turn.id)
                    .await
                    .unwrap()
                    .is_none(),
                "response/root proof also rolls back with the failed binding"
            );
        }
    }
}
