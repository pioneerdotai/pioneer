use super::*;
use pioneer_protocol::ArtifactStatus;

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
