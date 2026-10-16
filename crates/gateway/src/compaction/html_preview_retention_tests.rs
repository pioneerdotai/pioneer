use super::*;
use pioneer_artifacts::{
    ArtifactBindingTarget, ArtifactService, IngestArtifactBytesRequest, LocalArtifactBlobStore,
};
use pioneer_provider::{
    CanonicalProviderRoundEnvelope, ChatMessage, MessageContentPart, ProviderCallIdentity,
    ProviderToolCall,
};
use serde_json::json;

// Reuse the existing compaction fixture and MessageProcessor test configs.
// No provider is invoked by authorized history preparation.
fn import_processor(f: &Fixture, service: ArtifactService) -> crate::message::MessageProcessor {
    let mut processor = crate::message::MessageProcessor::new(
        Arc::new(crate::thread::ThreadManager::new(
            "fixture-model",
            "fixture",
        )),
        Arc::new(pioneer_provider::ProviderRegistry::with_provider(
            "fixture",
            f.provider.clone(),
        )),
        Arc::new(crate::session::SessionManager::new()),
        Arc::new(crate::workspace::WorkspaceManager::new(
            f.store.database_connection(),
        )),
        Arc::new(f.store.clone()),
        Arc::new(crate::secrets::GatewaySecrets::new(Arc::new(
            pioneer_keystore::MemorySecretStore::new(),
        ))),
        crate::message::test_summary_config(),
        crate::message::test_tool_loop_config(),
    );
    processor.artifact_service = Arc::new(service);
    processor
}

async fn capture_delivered_output(
    store: &CrudStore,
) -> pioneer_crud::compaction::TaskOutputSnapshot {
    let db = store.database_connection();
    for sql in [
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('parent','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES ('thread','parent','parent',1,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('parent-command','parent','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('parent-delivery','parent','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('preview-task','ws','thread','parent','parent','parent-command','agent','running','Delivered output','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('preview-run','preview-task','preview-run',1,1,'succeeded','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('preview-task-turn','preview-task','preview-run','thread','turn','initial',0,1,'candidate_created',CURRENT_TIMESTAMP)",
        "INSERT INTO task_result_candidate(id,task_id,run_id,task_run_turn_id,thread_id,turn_id,round,status,created_at,updated_at) VALUES ('preview-candidate','preview-task','preview-run','preview-task-turn','thread','turn',0,'accepted',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,thread_target,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id,delivered_at) VALUES ('preview-delivery','ws','preview-task','preview-run','preview-delivery','thread','origin_thread','parent','delivered',1,1,'parent-delivery',CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    let task_turn = store
        .get_task_run_turn("preview-task-turn")
        .await
        .unwrap()
        .unwrap();
    let output = super::super::frozen::capture_task_output(store, "ws", &task_turn)
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO compaction_delivery_output(delivery_id,candidate_id,task_run_turn_id) VALUES ('preview-delivery','preview-candidate','preview-task-turn')").await.unwrap();
    store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "parent".into(),
                turn_id: "parent-delivery".into(),
                item: pioneer_protocol::TurnItem::AgentMessage {
                    id: pioneer_protocol::task_delivery_result_item_id("preview-delivery"),
                    text: "delivered result".into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap();
    output
}

// Persist the prepared context through the existing Task snapshot/root path,
// then prepare the child's next request using that actual accepted input.
async fn retain_as_task_basis(
    f: &Fixture,
    id: &str,
    descriptor: &pioneer_compaction::frozen::FrozenHistoryRef,
) {
    let db = f.store.database_connection();
    for sql in [
        format!(
            "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('{id}','ws','','agent','fixture','fixture','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"
        ),
        format!(
            "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES ('{id}','parent','parent',1,CURRENT_TIMESTAMP)"
        ),
        format!(
            "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn-{id}','{id}','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"
        ),
        format!(
            "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task-{id}','ws','thread','parent','parent','parent-command','agent','running','Consumer','fixture')"
        ),
        format!(
            "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run-{id}','task-{id}','run-{id}',1,1,'running','agent')"
        ),
        format!(
            "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('rt-{id}','task-{id}','run-{id}','{id}','turn-{id}','initial',0,1,'completed',CURRENT_TIMESTAMP)"
        ),
    ] {
        db.execute_unprepared(&sql).await.unwrap();
    }
    f.store
        .insert_task_run_conversation_snapshot_if_absent(
            pioneer_crud::NewTaskRunConversationSnapshot {
                run_id: format!("run-{id}"),
                task_id: format!("task-{id}"),
                workspace_id: "ws".into(),
                conversation_thread_id: "parent".into(),
                source_turn_id: Some("parent-command".into()),
                history_json: serde_json::to_string(descriptor).unwrap(),
                created_at: chrono::Utc::now().fixed_offset(),
            },
        )
        .await
        .unwrap();
}

async fn drain_preview_worker(store: &CrudStore, service: &ArtifactService) {
    let mut progress = crate::database::maintenance::html_previews::Progress::default();
    for _ in 0..3 {
        let mut idle = false;
        for _ in 0..256 {
            if !crate::database::maintenance::html_previews::quantum(store, service, &mut progress)
                .await
                .unwrap()
            {
                idle = true;
                break;
            }
        }
        assert!(idle);
    }
}

#[tokio::test]
async fn released_preview_history_keeps_pairing_for_next_native_cli_task_and_frozen_replay() {
    let f = fixture("answer", vec![Reply::Success], true, false).await;
    // The shared fixture starts a raw compaction operation. Publish its exact
    // checkpoint before adding preview history, so it does not remain an
    // unresolved legacy owner that correctly blocks frozen input retirement.
    assert!(matches!(
        f.runner.run(CancellationToken::new()).await.unwrap(),
        CompactionExit::Applied(_)
    ));
    assert_eq!(f.provider.calls.lock().unwrap().len(), 1);
    let root = tempfile::tempdir().unwrap();
    let service = ArtifactService::new_with_policies(
        Arc::new(f.store.clone()),
        Arc::new(LocalArtifactBlobStore::new(root.path())),
        Default::default(),
        pioneer_artifacts::ArtifactGcPolicy {
            grace_secs: 0,
            ..Default::default()
        },
    );
    let mut permanent = Vec::new();
    for publication in [false, true] {
        let bytes = if publication {
            b"<!doctype html><p>published</p>".to_vec()
        } else {
            b"user attachment".to_vec()
        };
        let request = IngestArtifactBytesRequest {
            workspace_id: "ws".into(),
            primary_thread_id: Some("thread".into()),
            bytes: bytes.clone(),
            display_name: if publication {
                "published.html".into()
            } else {
                "upload.txt".into()
            },
            kind: pioneer_protocol::ArtifactKind::File,
            mime_type: Some(if publication {
                "text/html".into()
            } else {
                "text/plain".into()
            }),
            created_by_kind: if publication {
                pioneer_protocol::ArtifactCreatedByKind::Tool
            } else {
                pioneer_protocol::ArtifactCreatedByKind::User
            },
            created_by_actor_id: publication.then(|| "html_render".into()),
            binding: publication.then(|| ArtifactBindingTarget {
                thread_id: Some("thread".into()),
                turn_id: Some("turn".into()),
                message_id: None,
                turn_item_id: None,
                tool_call_id: Some("render-document".into()),
                task_id: None,
                task_run_id: None,
                binding_kind: pioneer_protocol::ArtifactBindingKind::ToolOutput,
                direction: pioneer_protocol::ArtifactBindingDirection::Output,
                role: Some(pioneer_protocol::ArtifactRole::Tool),
                item_index: None,
            }),
            metadata: if publication {
                std::collections::BTreeMap::from([(
                    "htmlTool".into(),
                    json!({"version":1,"publication":true,"turnId":"turn","callId":"render-document","index":"document"}),
                )])
            } else {
                Default::default()
            },
        };
        permanent.push((service.ingest_bytes(request).await.unwrap(), bytes));
    }
    // The upload is an initial input in the completed output history, not just
    // an unrelated file that happens to survive storage cleanup. Revision zero
    // keeps it current; a positive Message revision represents an input edit.
    f.store
        .replace_turn_input_projection_for_test(
            "turn",
            &[pioneer_protocol::UserInput::Artifact {
                artifact_id: permanent[0].0.artifact.artifact_id.clone(),
                version_id: permanent[0].0.artifact.version_id.clone(),
            }],
            0,
            chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap();
    let mut adoption_request = None;
    let mut parts = Vec::new();
    let mut previews = Vec::new();
    let mut images = Vec::new();
    let mut preview_paths = Vec::new();
    for width in [360u32, 768, 1024] {
        let mut png = std::io::Cursor::new(Vec::new());
        image::DynamicImage::new_rgba8(width, 1)
            .write_to(&mut png, image::ImageFormat::Png)
            .unwrap();
        let request = IngestArtifactBytesRequest {
            workspace_id: "ws".into(),
            primary_thread_id: Some("thread".into()),
            bytes: png.into_inner(),
            display_name: "internal".into(),
            kind: pioneer_protocol::ArtifactKind::Screenshot,
            mime_type: Some("image/png".into()),
            created_by_kind: pioneer_protocol::ArtifactCreatedByKind::Tool,
            created_by_actor_id: Some("html_preview".into()),
            binding: Some(ArtifactBindingTarget {
                thread_id: Some("thread".into()),
                turn_id: Some("turn".into()),
                message_id: None,
                turn_item_id: None,
                tool_call_id: Some("tool-item".into()),
                task_id: None,
                task_run_id: None,
                binding_kind: pioneer_protocol::ArtifactBindingKind::ToolOutput,
                direction: pioneer_protocol::ArtifactBindingDirection::Output,
                role: Some(pioneer_protocol::ArtifactRole::Tool),
                item_index: None,
            }),
            metadata: std::collections::BTreeMap::from([(
                "htmlTool".into(),
                json!({"version":1,"publication":false,"turnId":"turn","callId":"tool-item","index":width.to_string(),"details":{"width":width}}),
            )]),
        };
        adoption_request = Some(request.clone());
        let summary = service.ingest_bytes(request).await.unwrap();
        let resolved = service
            .resolve_provider_attachment(
                "ws",
                &summary.artifact.artifact_id,
                summary.artifact.version_id.as_deref(),
            )
            .await
            .unwrap();
        let blob = f
            .store
            .get_artifact_version_blob("ws", &summary.artifact.artifact_id, None)
            .await
            .unwrap();
        preview_paths.push(
            LocalArtifactBlobStore::new(root.path())
                .blob_path("ws", &blob.storage_key)
                .unwrap(),
        );
        previews.push(json!({"width":width,"artifact":summary.artifact}));
        images.push(json!({"width":width,"attachment":resolved.attachment}));
        parts.push(MessageContentPart::image(resolved.attachment));
    }
    let round = CanonicalProviderRoundEnvelope {
        version: 1,
        round_id: "round".into(),
        termination: ProviderTermination::ToolCalls,
        message: ChatMessage::assistant_tool_calls(
            None::<String>,
            vec![ProviderToolCall {
                id: "provider-call".into(),
                name: "html_preview".into(),
                arguments: "{\"html\":\"<p>fixture</p>\"}".into(),
            }],
        ),
        calls: vec![ProviderCallIdentity {
            provider_call_id: "provider-call".into(),
            turn_item_id: "tool-item".into(),
            ordinal: 0,
        }],
    };
    let mut message = ChatMessage::tool_result(
        "provider-call",
        "html_preview",
        json!({"previews":previews,"llm_context":{"images":images}}).to_string(),
    );
    message.content_parts = parts;
    for (item, source, payload) in [
        (
            "round",
            "assistant_round",
            serde_json::to_string(&round).unwrap(),
        ),
        (
            "tool-item",
            "tool_result_v2",
            json!({"kind":"json","truncated":false,"value":message}).to_string(),
        ),
    ] {
        f.store
            .append_turn_llm_context(
                pioneer_crud::NewTurnLlmContextEntry {
                    turn_id: "turn".into(),
                    item_id: Some(item.into()),
                    attempt_id: None,
                    sequence: 0,
                    source: source.into(),
                    tool_name: (source == "tool_result_v2").then(|| "html_preview".into()),
                    payload,
                    output_policy_snapshot: "{}".into(),
                    created_at: chrono::Utc::now().fixed_offset(),
                    expires_at: None,
                },
                item,
            )
            .await
            .unwrap();
    }
    f.store
        .database_connection()
        .execute_unprepared("UPDATE turn SET status='completed' WHERE id='turn'")
        .await
        .unwrap();
    super::super::history::prepare_history(&f.store, "ws", "thread")
        .await
        .unwrap();
    let fence = f.store.compaction_history_read_fence().await.unwrap();
    let before = super::super::history::load_line_history(&f.store, "ws", "thread", None, &fence)
        .await
        .unwrap();
    assert_eq!(
        before
            .iter()
            .flat_map(|m| &m.content_parts)
            .filter(|p| matches!(p, MessageContentPart::Image { .. }))
            .count(),
        3
    );
    // The real completion capture records the permanent compaction_task_output
    // root while the Task is running. This also represents a persisted pre-upgrade
    // snapshot: the preview index and verification receipts do not yet exist.
    let output = capture_delivered_output(&f.store).await;
    let task_turn = f
        .store
        .get_task_run_turn("preview-task-turn")
        .await
        .unwrap()
        .unwrap();
    let db = f.store.database_connection();
    let receipt = f
        .store
        .compaction_delivery_output("ws", "preview-delivery")
        .await
        .unwrap()
        .unwrap();
    let root_hold = f
        .store
        .compaction_acquire_frozen_history("ws", &output.history)
        .await
        .unwrap();
    db.execute_unprepared("UPDATE task SET status='completed' WHERE id='preview-task'")
        .await
        .unwrap();
    crate::session::test_support::ensure_test_superuser_execution_authority(&f.store).await;
    let processor = import_processor(&f, service.clone());
    let principal = crate::session::test_support::authenticated_test_superuser();
    let live = processor
        .capture_authorized_task_basis_prepared(
            &f.store, &principal, "ws", "parent", None, None, None,
        )
        .await
        .unwrap();
    retain_as_task_basis(&f, "active-preview-import", &live.descriptor).await;
    assert_eq!(
        live.messages
            .iter()
            .filter(|m| m.name.as_deref() == Some("html_preview"))
            .flat_map(|m| &m.content_parts)
            .filter(|p| matches!(p, MessageContentPart::Image { .. }))
            .count(),
        3
    );
    let live_manifest = live.descriptor.manifest_id.clone();
    drop(live);
    // Build the existing dependency index without preparing witnesses yet.
    // This lets the same fixture exercise the owner fence at both boundaries.
    let mut indexing = pioneer_artifacts::HtmlPreviewMaintenanceProgress::default();
    let mut indexed = false;
    for _ in 0..256 {
        if !service
            .html_preview_maintenance_quantum(&mut indexing)
            .await
            .unwrap()
        {
            indexed = true;
            break;
        }
    }
    assert!(indexed);
    let mut cursor = (String::new(), 0);
    let mut accepted_witness = None;
    for _ in 0..256 {
        let (next, witness) = f
            .store
            .html_preview_archive_witness_page(&cursor)
            .await
            .unwrap();
        if let Some(witness) = witness.filter(|w| w.manifest == live_manifest) {
            accepted_witness = Some(witness);
            break;
        }
        let Some(next) = next else {
            break;
        };
        cursor = next;
    }
    let witness = accepted_witness.expect("accepted child preview has a pending witness");
    assert_eq!(witness.owner_thread, "parent");
    assert_eq!(witness.reference.source_thread, "thread");
    assert_eq!(witness.reference.context_thread.as_deref(), Some("parent"));
    let mut wrong_context = witness.clone();
    wrong_context.reference.context_thread = Some("outside-context".into());
    assert!(
        super::super::frozen::prepare_html_preview_archive_witness(&f.store, &wrong_context)
            .await
            .is_err()
    );
    let mut wrong_owner = witness.clone();
    wrong_owner.owner_thread = "outside-context".into();
    assert!(
        super::super::frozen::prepare_html_preview_archive_witness(&f.store, &wrong_owner)
            .await
            .is_err()
    );
    // A changed owner cannot publish a receipt even if preparation was skipped.
    f.store
        .record_html_preview_archive_witness(&wrong_owner, &"a".repeat(64))
        .await
        .unwrap();
    let stored = db.query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "SELECT archive_wire_sha256 FROM html_preview_frozen_result WHERE manifest_id=?1 AND ordinal=?2",
        [witness.manifest.clone().into(),witness.ordinal.into()])).await.unwrap().unwrap();
    assert!(
        stored
            .try_get::<Option<String>>("", "archive_wire_sha256")
            .unwrap()
            .is_none()
    );
    super::super::frozen::prepare_html_preview_archive_witness(&f.store, &witness)
        .await
        .unwrap();
    assert!(preview_paths.iter().all(|p| p.exists()));
    let mut progress = crate::database::maintenance::html_previews::Progress::default();
    // Complete several bounded passes: index, verification and blob discovery
    // may reach EOF at different points. The exact read hold still protects bytes.
    for _ in 0..3 {
        let mut idle = false;
        for _ in 0..128 {
            if !crate::database::maintenance::html_previews::quantum(
                &f.store,
                &service,
                &mut progress,
            )
            .await
            .unwrap()
            {
                idle = true;
                break;
            }
        }
        assert!(idle);
    }
    assert!(preview_paths.iter().all(|p| p.exists()));
    drop(root_hold);
    drain_preview_worker(&f.store, &service).await;
    assert!(
        preview_paths.iter().all(|p| p.exists()),
        "actual accepted import root preserves active preview media"
    );
    db.execute_unprepared(
        "UPDATE task SET status='completed' WHERE id='task-active-preview-import'",
    )
    .await
    .unwrap();
    // Existing lifetime maintenance releases the completed accepted input root;
    // no fixture expires the permanent Task output or edits its immutable body.
    let mut lifetime = pioneer_crud::FrozenStorageLifetimeProgress::default();
    let mut idle = false;
    for _ in 0..4096 {
        if !lifetime.quantum(&f.store).await.unwrap() {
            idle = true;
            break;
        }
    }
    assert!(idle);
    for (manifest, expired) in [
        (&live_manifest, 1_i64),
        (&output.history.manifest_id, 0_i64),
    ] {
        let header = db
            .query_one_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT expired FROM compaction_frozen_history WHERE id=?",
                [manifest.clone().into()],
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            header.try_get::<i64>("", "expired").unwrap(),
            expired,
            "completed accepted input expires while permanent Task output stays rooted"
        );
    }
    // The same output can also be accepted input of a continuing Task. A
    // permanent output root and this live accepted input have distinct policy.
    db.execute_unprepared("INSERT INTO task(id,workspace_id,owner_kind,owner_id,executor_kind,status,title,goal) VALUES ('preview-consumer','ws','thread','thread','agent','running','Consumer','fixture')").await.unwrap();
    db.execute_unprepared("INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('preview-consumer-run','preview-consumer','preview-consumer-run',1,1,'running','agent')").await.unwrap();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,history_json,frozen_manifest_id,created_at) VALUES('preview-consumer-run','preview-consumer','ws','thread',?1,?2,CURRENT_TIMESTAMP)",[serde_json::to_string(&output.history).unwrap().into(),output.history.manifest_id.clone().into()])).await.unwrap();
    for _ in 0..2 {
        let mut idle = false;
        for _ in 0..128 {
            if !crate::database::maintenance::html_previews::quantum(
                &f.store,
                &service,
                &mut progress,
            )
            .await
            .unwrap()
            {
                idle = true;
                break;
            }
        }
        assert!(idle);
    }
    assert!(preview_paths.iter().all(|p| p.exists()));
    db.execute_unprepared("UPDATE task_run SET status='succeeded' WHERE id='preview-consumer-run'")
        .await
        .unwrap();
    db.execute_unprepared("UPDATE task SET status='completed' WHERE id='preview-consumer'")
        .await
        .unwrap();
    // A restarted worker reuses persisted receipts and retires only after all
    // temporary owners have released; it never expires/deletes the Task root.
    let mut progress = crate::database::maintenance::html_previews::Progress::default();
    for _ in 0..3 {
        let mut idle = false;
        for _ in 0..128 {
            if !crate::database::maintenance::html_previews::quantum(
                &f.store,
                &service,
                &mut progress,
            )
            .await
            .unwrap()
            {
                idle = true;
                break;
            }
        }
        assert!(idle);
    }
    assert!(preview_paths.iter().all(|p| !p.exists()));
    for (summary, bytes) in &permanent {
        assert_eq!(
            &service
                .read_bounded_artifact(
                    pioneer_artifacts::ArtifactBoundedReadRequest {
                        workspace_id: "ws".into(),
                        artifact_id: summary.artifact.artifact_id.clone(),
                        version_id: summary.artifact.version_id.clone(),
                        projection_kind: None,
                        offset: None,
                        max_bytes: Some(1024)
                    },
                    1024
                )
                .await
                .unwrap()
                .bytes,
            bytes
        );
    }
    assert_eq!(
        f.store
            .compaction_task_output("ws", &task_turn.id)
            .await
            .unwrap()
            .unwrap(),
        output
    );
    assert_eq!(
        f.store
            .compaction_delivery_output("ws", "preview-delivery")
            .await
            .unwrap()
            .unwrap(),
        receipt
    );
    assert!(
        f.store
            .compaction_delivery_output("other-workspace", "preview-delivery")
            .await
            .unwrap()
            .is_none()
    );
    let accepted = processor
        .capture_authorized_task_basis_prepared(
            &f.store, &principal, "ws", "parent", None, None, None,
        )
        .await
        .unwrap();
    let imports = f
        .store
        .compaction_frozen_import_page("ws", "parent", &accepted.descriptor.manifest_id, 0)
        .await
        .unwrap();
    let archived = imports
        .iter()
        .find(|p| p.preview_archive.is_some())
        .expect("actual context-source HTML grant has an archival body");
    assert_ne!(archived.source, *archived.render_source());
    assert!(archived.source.scope.starts_with("context:"));
    assert_eq!(archived.output_manifest, output.history.manifest_id);
    let restored = crate::turn_runtime_snapshot::restore_history_json(
        &f.store,
        "ws",
        &accepted.accepted_scopes,
        &serde_json::to_string(&accepted.descriptor).unwrap(),
    )
    .await
    .unwrap();
    assert!(
        restored
            .iter()
            .any(|m| m.name.as_deref() == Some("html_preview") && m.content_parts.is_empty())
    );
    let repeated = processor
        .capture_authorized_task_basis_prepared(
            &f.store, &principal, "ws", "parent", None, None, None,
        )
        .await
        .unwrap();
    assert_eq!(accepted.descriptor, repeated.descriptor);
    assert_eq!(
        f.store
            .compaction_delivery_output("ws", "preview-delivery")
            .await
            .unwrap()
            .unwrap(),
        receipt
    );
    assert!(
        processor
            .capture_authorized_task_basis_prepared(
                &f.store,
                &principal,
                "other-workspace",
                "parent",
                None,
                None,
                None
            )
            .await
            .is_err()
    );
    assert!(
        super::super::frozen::validate_frozen_history_current(
            &f.store,
            "ws",
            "parent",
            &serde_json::to_string(&accepted.descriptor).unwrap(),
        )
        .await
        .unwrap(),
        "the diagnostic and ordinary attachment are exact-current before forwarding"
    );
    retain_as_task_basis(&f, "forward-archive", &accepted.descriptor).await;
    let forwarded = processor
        .capture_authorized_task_basis_prepared(
            &f.store,
            &principal,
            "ws",
            "forward-archive",
            Some("turn-forward-archive"),
            Some("turn-forward-archive"),
            None,
        )
        .await
        .unwrap();
    let forwarded_imports = f
        .store
        .compaction_frozen_import_page(
            "ws",
            "forward-archive",
            &forwarded.descriptor.manifest_id,
            0,
        )
        .await
        .unwrap();
    let grant = forwarded_imports
        .iter()
        .find(|p| p.preview_archive.is_some())
        .unwrap();
    assert_eq!(grant.source, archived.source);
    assert_eq!(grant.delivery_id, archived.delivery_id);
    assert_eq!(grant.preview_archive, archived.preview_archive);
    let forwarded_replay = crate::turn_runtime_snapshot::restore_history_json(
        &f.store,
        "ws",
        &forwarded.accepted_scopes,
        &serde_json::to_string(&forwarded.descriptor).unwrap(),
    )
    .await
    .unwrap();
    assert!(
        forwarded_replay
            .iter()
            .any(|m| m.name.as_deref() == Some("html_preview") && m.content_parts.is_empty())
    );
    // Seal the same accepted grant into an actual published checkpoint, then
    // exercise the production checkpoint import/forwarding consumer as well.
    let references = f
        .store
        .compaction_frozen_history_page(
            "ws",
            "forward-archive",
            &forwarded.descriptor.manifest_id,
            0,
        )
        .await
        .unwrap();
    let mut units = std::collections::BTreeMap::new();
    let mut manifest = Vec::new();
    for reference in references
        .iter()
        .filter(|r| !r.inherited && r.complete && !r.protected_input)
    {
        let next = units.len() as u64;
        let unit = *units.entry(reference.unit_id.clone()).or_insert(next);
        for source in &reference.sources {
            manifest.push(ManifestEntry {
                ordinal: manifest.len() as u64,
                unit,
                reference_only: false,
                thread_id: reference.source_thread.clone(),
                source: source.clone(),
            });
        }
    }
    let epoch = f
        .store
        .compaction_projection_version("ws", "forward-archive")
        .await
        .unwrap();
    let mut operation = f.runner.snapshot.clone();
    operation.id = "archival-import-checkpoint-operation".into();
    operation.owner = super::super::native::native_owner("ws", "forward-archive");
    operation.expected_checkpoint = None;
    operation.projection_version = epoch;
    operation.source_epochs = forwarded.source_epochs.clone();
    operation.plan.coverage_domain = pioneer_compaction::CoverageDomain::OwnContribution;
    operation.plan.compact = (0..units.len()).collect();
    operation.plan.retain.clear();
    operation.plan.coverage = manifest.iter().map(|m| m.source.clone()).collect();
    operation.plan.fingerprint = "archival-import-checkpoint-plan".into();
    let mut admitted = operation.clone();
    admitted.plan.compact.clear();
    admitted.plan.coverage.clear();
    let initial = prepare_task_input_checkpoint_runner(
        &f,
        "forward-archive",
        Some("turn-forward-archive"),
        &admitted,
        &forwarded.descriptor,
        &ModelBudget::new(None, None, None),
        manifest.len() as u64,
        0,
        &manifest,
        1000,
    )
    .await;
    let attempt = initial.claim(1).unwrap();
    assert!(
        f.store
            .compaction_runner_transition(&operation.id, initial.generation, &attempt, None)
            .await
            .unwrap()
    );
    let checkpoint = Checkpoint {
        id: "archival-import-checkpoint".into(),
        operation_id: operation.id.clone(),
        owner: operation.owner.clone(),
        previous: None,
        coverage: operation.plan.coverage.clone(),
        summary: HEADINGS
            .iter()
            .map(|h| format!("{h}\nArchived preview diagnostics.\n"))
            .collect(),
        selection: operation.admission.selection.clone(),
        projection_version: epoch,
        format_version: pioneer_compaction::FORMAT_VERSION,
    };
    let candidate = attempt
        .candidate(
            1,
            checkpoint.id.clone(),
            SourceCursor {
                unit: units.len() as u64,
                ..Default::default()
            },
            true,
            2,
        )
        .unwrap();
    assert!(
        f.store
            .compaction_runner_transition(
                &operation.id,
                attempt.generation,
                &candidate,
                Some(&checkpoint)
            )
            .await
            .unwrap()
    );
    let ready = candidate.candidate_checked(true).unwrap();
    assert!(
        f.store
            .compaction_runner_transition(&operation.id, candidate.generation, &ready, None)
            .await
            .unwrap()
    );
    assert_eq!(
        f.store
            .compaction_apply_runner(&operation.id, &ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    let checkpoint_basis = processor
        .capture_authorized_task_basis_prepared(
            &f.store,
            &principal,
            "ws",
            "forward-archive",
            Some("turn-forward-archive"),
            Some("turn-forward-archive"),
            None,
        )
        .await
        .unwrap();
    let checkpoint_imports = f
        .store
        .compaction_frozen_import_page(
            "ws",
            "forward-archive",
            &checkpoint_basis.descriptor.manifest_id,
            0,
        )
        .await
        .unwrap();
    assert!(
        checkpoint_imports
            .iter()
            .any(|p| p.preview_archive == archived.preview_archive
                && p.source == archived.source
                && p.delivery_id == archived.delivery_id)
    );
    let checkpoint_replay = crate::turn_runtime_snapshot::restore_history_json(
        &f.store,
        "ws",
        &checkpoint_basis.accepted_scopes,
        &serde_json::to_string(&checkpoint_basis.descriptor).unwrap(),
    )
    .await
    .unwrap();
    assert!(
        checkpoint_replay
            .iter()
            .any(|m| m.content.contains("Archived preview diagnostics."))
    );
    assert!(
        checkpoint_replay
            .iter()
            .flat_map(|m| &m.content_parts)
            .all(|p| !matches!(p, MessageContentPart::Image { .. }))
    );
    drain_preview_worker(&f.store, &service).await;
    let index = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT complete,unknown FROM html_preview_frozen_index WHERE manifest_id=?",
            [checkpoint_basis.descriptor.manifest_id.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(index.try_get::<i64>("", "complete").unwrap(), 1);
    assert_eq!(index.try_get::<i64>("", "unknown").unwrap(), 0);
    assert_eq!(
        f.store
            .compaction_delivery_output("ws", "preview-delivery")
            .await
            .unwrap()
            .unwrap(),
        receipt
    );
    let allowed = std::collections::BTreeSet::from(["thread".to_owned()]);
    let replay = super::super::frozen::restore(&f.store, "ws", &allowed, &receipt.output.history)
        .await
        .unwrap();
    assert!(
        replay
            .iter()
            .any(|m| m.name.as_deref() == Some("html_preview")
                && m.content_parts.is_empty()
                && m.tool_call_id.as_deref() == Some("provider-call"))
    );
    assert!(
        super::super::frozen::restore(
            &f.store,
            "ws",
            &std::collections::BTreeSet::new(),
            &output.history
        )
        .await
        .is_err()
    );
    let execution = super::super::frozen::restore_accepted_history_for_execution(
        &f.store,
        "ws",
        None,
        "after-preview-retirement",
        &allowed,
        &serde_json::to_string(&output.history).unwrap(),
    )
    .await
    .unwrap();
    assert!(
        execution
            .messages
            .iter()
            .any(|m| m.name.as_deref() == Some("html_preview")
                && m.content_parts.is_empty()
                && m.content.contains("previewRetention"))
    );
    // A new accepted context can freeze the compact diagnostic at its actual
    // current revision, without changing the old output manifest identity.
    let recaptured = super::super::frozen::capture(&f.store, "ws", "thread", &allowed, &replay)
        .await
        .unwrap();
    assert_eq!(
        super::super::frozen::restore(&f.store, "ws", &allowed, &recaptured)
            .await
            .unwrap(),
        replay
    );
    super::super::history::prepare_history(&f.store, "ws", "thread")
        .await
        .unwrap();
    let fence = f.store.compaction_history_read_fence().await.unwrap();
    let after = super::super::history::load_line_history(&f.store, "ws", "thread", None, &fence)
        .await
        .unwrap();
    let result = after
        .iter()
        .find(|m| m.name.as_deref() == Some("html_preview"))
        .unwrap();
    assert_eq!(result.tool_call_id.as_deref(), Some("provider-call"));
    assert!(result.content_parts.is_empty());
    assert!(result.content.contains("previewRetention"));
    assert_eq!(after.iter().filter(|m| m.tool_calls.is_some()).count(), 1);
    assert_eq!(
        super::super::history::load_task_output_history(&f.store, "ws", "thread", "turn", &fence)
            .await
            .unwrap(),
        after
    );
    let allowed = std::collections::BTreeSet::from(["thread".to_owned()]);
    let frozen = super::super::frozen::capture(&f.store, "ws", "thread", &allowed, &after)
        .await
        .unwrap();
    assert_eq!(
        super::super::frozen::restore(&f.store, "ws", &allowed, &frozen)
            .await
            .unwrap(),
        after
    );
    assert!(
        super::super::frozen::capture(&f.store, "ws", "thread", &allowed, &before)
            .await
            .is_err(),
        "stale image history must not be accepted after release"
    );
    // Current Message inputs are a supported adoption even without a
    // UserMessage item or the old async attachment-binding step.
    let mut request = adoption_request.unwrap();
    request.metadata.get_mut("htmlTool").unwrap()["callId"] = json!("message-adoption");
    request.binding.as_mut().unwrap().tool_call_id = Some("message-adoption".into());
    let current = service.ingest_bytes(request.clone()).await.unwrap();
    let input = pioneer_protocol::UserInput::Artifact {
        artifact_id: current.artifact.artifact_id.clone(),
        version_id: current.artifact.version_id.clone(),
    };
    f.store.database_connection().execute_unprepared("CREATE TRIGGER fail_preview_adoption BEFORE INSERT ON artifact_binding WHEN NEW.binding_kind='user_input' BEGIN SELECT RAISE(ABORT,'fixture adoption rollback'); END").await.unwrap();
    assert!(
        f.store
            .replace_turn_input_projection_for_test(
                "turn",
                &[input.clone()],
                1,
                chrono::Utc::now().timestamp()
            )
            .await
            .is_err()
    );
    assert!(
        !service
            .get_artifact("ws", &current.artifact.artifact_id, None)
            .await
            .unwrap()
            .bindings
            .iter()
            .any(|b| b.binding_kind == pioneer_protocol::ArtifactBindingKind::UserInput)
    );
    f.store
        .database_connection()
        .execute_unprepared("DROP TRIGGER fail_preview_adoption")
        .await
        .unwrap();
    f.store
        .replace_turn_input_projection_for_test("turn", &[input], 1, chrono::Utc::now().timestamp())
        .await
        .unwrap();
    assert!(
        service
            .get_artifact("ws", &current.artifact.artifact_id, None)
            .await
            .unwrap()
            .bindings
            .iter()
            .any(|b| b.binding_kind == pioneer_protocol::ArtifactBindingKind::UserInput)
    );
    // Simulate a pre-migration input that predates the synchronous adoption
    // hook. Its bounded backfill runs before any preview may be retired.
    request.metadata.get_mut("htmlTool").unwrap()["callId"] = json!("legacy-message-adoption");
    request.binding.as_mut().unwrap().tool_call_id = Some("legacy-message-adoption".into());
    let legacy = service.ingest_bytes(request).await.unwrap();
    let payload = serde_json::to_string(&pioneer_protocol::UserInput::Artifact {
        artifact_id: legacy.artifact.artifact_id.clone(),
        version_id: legacy.artifact.version_id.clone(),
    })
    .unwrap();
    f.store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"INSERT INTO turn_input(id,turn_id,input_index,input_type,payload,created_at) VALUES('legacy-preview-input','turn',1,'artifact',?1,CURRENT_TIMESTAMP)",[payload.into()])).await.unwrap();
    f.store
        .database_connection()
        .execute_unprepared(
            "UPDATE html_preview_index_backfill SET inputs_complete=0 WHERE singleton=1",
        )
        .await
        .unwrap();
    let mut progress = pioneer_artifacts::HtmlPreviewMaintenanceProgress::default();
    for _ in 0..64 {
        service
            .html_preview_maintenance_quantum(&mut progress)
            .await
            .unwrap();
    }
    assert!(
        service
            .get_artifact("ws", &legacy.artifact.artifact_id, None)
            .await
            .unwrap()
            .bindings
            .iter()
            .any(|b| b.binding_kind == pioneer_protocol::ArtifactBindingKind::UserInput)
    );
    // A later edit cannot be accepted using the release receipt as a general
    // revision exemption. Both fresh preparation and accepted replay reject it.
    let row = f
        .store
        .list_turn_llm_context("turn")
        .await
        .unwrap()
        .into_iter()
        .find(|c| c.tool_name.as_deref() == Some("html_preview"))
        .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE turn_llm_context SET payload=payload||' ' WHERE id=?1",
        [row.id.into()],
    ))
    .await
    .unwrap();
    assert!(
        processor
            .capture_authorized_task_basis_prepared(
                &f.store, &principal, "ws", "parent", None, None, None
            )
            .await
            .is_err()
    );
    assert!(
        crate::turn_runtime_snapshot::restore_history_json(
            &f.store,
            "ws",
            &accepted.accepted_scopes,
            &serde_json::to_string(&accepted.descriptor).unwrap()
        )
        .await
        .is_err()
    );
    assert_eq!(
        f.provider.calls.lock().unwrap().len(),
        1,
        "authorized preparation and replay never invoke the provider"
    );
}

#[tokio::test]
async fn known_rooted_import_without_preview_does_not_pin_an_independent_preview() {
    let f = fixture("ordinary delivered answer", vec![], true, false).await;
    let root = tempfile::tempdir().unwrap();
    let service = ArtifactService::new_with_policies(
        Arc::new(f.store.clone()),
        Arc::new(LocalArtifactBlobStore::new(root.path())),
        Default::default(),
        pioneer_artifacts::ArtifactGcPolicy {
            grace_secs: 0,
            ..Default::default()
        },
    );
    let db = f.store.database_connection();
    db.execute_unprepared("UPDATE turn SET status='completed' WHERE id='turn'")
        .await
        .unwrap();
    capture_delivered_output(&f.store).await;
    db.execute_unprepared("UPDATE task SET status='completed' WHERE id='preview-task'")
        .await
        .unwrap();
    crate::session::test_support::ensure_test_superuser_execution_authority(&f.store).await;
    let processor = import_processor(&f, service.clone());
    let principal = crate::session::test_support::authenticated_test_superuser();
    let accepted = processor
        .capture_authorized_task_basis_prepared(
            &f.store, &principal, "ws", "parent", None, None, None,
        )
        .await
        .unwrap();
    let imports = f
        .store
        .compaction_frozen_import_page("ws", "parent", &accepted.descriptor.manifest_id, 0)
        .await
        .unwrap();
    assert!(!imports.is_empty());
    assert!(
        accepted
            .messages
            .iter()
            .all(|m| m.name.as_deref() != Some("html_preview"))
    );
    retain_as_task_basis(&f, "ordinary-import-owner", &accepted.descriptor).await;
    let history = serde_json::to_string(&accepted.descriptor).unwrap();
    let scopes = accepted.accepted_scopes.clone();
    let manifest = accepted.descriptor.manifest_id.clone();
    drop(accepted); // The persisted running Task root, not a local hold, survives.

    insert_projection_event(&f, "independent-preview").await;
    let summary=service.ingest_bytes(IngestArtifactBytesRequest {
        workspace_id:"ws".into(),primary_thread_id:Some("independent-preview".into()),
        bytes:b"<!doctype html><p>temporary independent source</p>".to_vec(),display_name:"internal".into(),
        kind:pioneer_protocol::ArtifactKind::File,mime_type:Some("text/html".into()),
        created_by_kind:pioneer_protocol::ArtifactCreatedByKind::Tool,created_by_actor_id:Some("html_preview".into()),
        binding:Some(ArtifactBindingTarget {thread_id:Some("independent-preview".into()),turn_id:Some("independent-preview-turn".into()),message_id:None,turn_item_id:None,tool_call_id:Some("independent-call".into()),task_id:None,task_run_id:None,binding_kind:pioneer_protocol::ArtifactBindingKind::ToolOutput,direction:pioneer_protocol::ArtifactBindingDirection::Output,role:Some(pioneer_protocol::ArtifactRole::Tool),item_index:None}),
        metadata:std::collections::BTreeMap::from([("htmlTool".into(),json!({"version":1,"publication":false,"turnId":"independent-preview-turn","callId":"independent-call","index":"input"}))]),
    }).await.unwrap();
    let blob = f
        .store
        .get_artifact_version_blob("ws", &summary.artifact.artifact_id, None)
        .await
        .unwrap();
    let path = LocalArtifactBlobStore::new(root.path())
        .blob_path("ws", &blob.storage_key)
        .unwrap();
    let message = ChatMessage::tool_result(
        "independent-provider-call",
        "html_preview",
        json!({"source":summary.artifact}).to_string(),
    );
    let round = CanonicalProviderRoundEnvelope {
        version: 1,
        round_id: "independent-round".into(),
        termination: ProviderTermination::ToolCalls,
        message: ChatMessage::assistant_tool_calls(
            None::<String>,
            vec![ProviderToolCall {
                id: "independent-provider-call".into(),
                name: "html_preview".into(),
                arguments: "{\"html\":\"<p>fixture</p>\"}".into(),
            }],
        ),
        calls: vec![ProviderCallIdentity {
            provider_call_id: "independent-provider-call".into(),
            turn_item_id: "independent-call".into(),
            ordinal: 0,
        }],
    };
    for (item, source, payload) in [
        (
            "independent-round",
            "assistant_round",
            serde_json::to_string(&round).unwrap(),
        ),
        (
            "independent-call",
            "tool_result_v2",
            json!({"kind":"json","truncated":false,"value":message}).to_string(),
        ),
    ] {
        f.store
            .append_turn_llm_context(
                pioneer_crud::NewTurnLlmContextEntry {
                    turn_id: "independent-preview-turn".into(),
                    item_id: Some(item.into()),
                    attempt_id: None,
                    sequence: 0,
                    source: source.into(),
                    tool_name: (source == "tool_result_v2").then(|| "html_preview".into()),
                    payload,
                    output_policy_snapshot: "{}".into(),
                    created_at: chrono::Utc::now().fixed_offset(),
                    expires_at: None,
                },
                item,
            )
            .await
            .unwrap();
    }
    // Exercise upgrade from the previous implementation's blanket-unknown
    // derived marker as well as a fresh bounded import traversal after restart.
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO html_preview_frozen_index(manifest_id,workspace_id,identity_sha256,message_count,ready,expired,unknown) SELECT id,workspace_id,identity_sha256,message_count,ready,expired,1 FROM compaction_frozen_history WHERE id=? ON CONFLICT(manifest_id) DO UPDATE SET unknown=1,complete=0",
        [manifest.clone().into()])).await.unwrap();
    drain_preview_worker(&f.store, &service).await;
    assert!(
        !path.exists(),
        "known ordinary imported context must not pin unrelated preview bytes"
    );
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT complete,unknown FROM html_preview_frozen_index WHERE manifest_id=?",
            [manifest.into()],
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "complete").unwrap(), 1);
    assert_eq!(row.try_get::<i64>("", "unknown").unwrap(), 0);
    let restored =
        crate::turn_runtime_snapshot::restore_history_json(&f.store, "ws", &scopes, &history)
            .await
            .unwrap();
    assert!(
        restored
            .iter()
            .any(|m| m.content.contains("ordinary delivered answer"))
    );
    let next = processor
        .capture_authorized_task_basis_prepared(
            &f.store,
            &principal,
            "ws",
            "ordinary-import-owner",
            Some("turn-ordinary-import-owner"),
            Some("turn-ordinary-import-owner"),
            None,
        )
        .await
        .unwrap();
    assert!(
        next.messages
            .iter()
            .any(|m| m.content.contains("ordinary delivered answer"))
    );
    assert!(f.provider.calls.lock().unwrap().is_empty());
}
