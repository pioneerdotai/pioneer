// Included in the existing service fixture module. Execution requires review.

use sea_orm::{DbBackend, Statement};

async fn preview_fixture() -> TestHarness {
    let harness = setup().await;
    let db = harness.store.database_connection();
    db.execute_unprepared("INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('preview-thread','ws_a','','agent','fixture','fixture','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO turn(id,thread_id,status,created_at,updated_at) VALUES('preview-turn','preview-thread','in_progress',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    harness
}

fn preview_request(index: &str, bytes: Vec<u8>) -> IngestArtifactBytesRequest {
    let mut request = ingest_request("ws_a", "preview-thread", "preview-turn", &bytes, "internal");
    request.created_by_kind = ArtifactCreatedByKind::Tool;
    request.created_by_actor_id = Some("html_preview".into());
    request.kind = if index == "input" {
        ArtifactKind::File
    } else {
        ArtifactKind::Screenshot
    };
    request.metadata.insert("htmlTool".into(),json!({"version":1,"publication":false,"turnId":"preview-turn","callId":"canonical-call","index":index,"details":{"width":index.parse::<u32>().ok()}}));
    let binding = request.binding.as_mut().unwrap();
    binding.message_id = None;
    binding.tool_call_id = Some("canonical-call".into());
    binding.binding_kind = ArtifactBindingKind::ToolOutput;
    binding.role = Some(pioneer_protocol::ArtifactRole::Tool);
    request
}

async fn preview_maintenance(service: &ArtifactService) {
    let mut progress = crate::HtmlPreviewMaintenanceProgress::default();
    // A retirement can occur after this pass's blob EOF. The next pass must
    // discover that newly durable orphan without depending on cursor ordering.
    for _ in 0..2 {
        let mut idle = false;
        for _ in 0..128 {
            if !service
                .html_preview_maintenance_quantum(&mut progress)
                .await
                .unwrap()
            {
                idle = true;
                break;
            }
        }
        assert!(idle, "bounded fixture did not reach EOF");
    }
}

#[tokio::test]
async fn html_preview_lists_filter_before_identity_pagination_and_preserve_publications() {
    let harness = preview_fixture().await;
    let mut visible = std::collections::BTreeSet::new();
    let mut all_ids = Vec::new();
    for index in 0..6 {
        let request = if index % 2 == 0 {
            preview_request(
                if index == 0 {
                    "input"
                } else if index == 2 {
                    "360"
                } else {
                    "768"
                },
                vec![index],
            )
        } else {
            let mut r = ingest_request(
                "ws_a",
                "preview-thread",
                "preview-turn",
                &[index],
                "visible",
            );
            if index == 1 || index == 3 {
                r.created_by_kind = ArtifactCreatedByKind::Tool;
                r.created_by_actor_id = Some("html_render".into());
                r.metadata.insert("htmlTool".into(),json!({"version":1,"publication":true,"turnId":"preview-turn","callId":format!("render-{index}")}));
            } else {
                r.kind = ArtifactKind::Screenshot;
                r.created_by_actor_id = Some("ordinary-screenshot-tool".into());
            }
            r
        };
        let summary = harness.service.ingest_bytes(request).await.unwrap();
        all_ids.push(summary.artifact.artifact_id.clone());
        if index % 2 != 0 {
            visible.insert(summary.artifact.artifact_id);
        }
    }
    // A real upload remains visible too.
    let mut upload = ingest_request(
        "ws_a",
        "preview-thread",
        "preview-turn",
        b"upload",
        "upload",
    );
    upload.created_by_kind = ArtifactCreatedByKind::User;
    let upload = harness
        .service
        .ingest_bytes(upload)
        .await
        .unwrap()
        .artifact
        .artifact_id;
    all_ids.push(upload.clone());
    visible.insert(upload);
    // Duplicate bindings must consume one identity, never an extra page slot.
    let id = visible.first().unwrap();
    harness
        .service
        .bind_artifact(BindArtifactRequest {
            workspace_id: "ws_a".into(),
            artifact_id: id.clone(),
            version_id: None,
            target: target("preview-thread", "preview-turn"),
            metadata: Default::default(),
        })
        .await
        .unwrap();
    for thread_scope in [false, true] {
        let mut cursor = None;
        let mut actual = std::collections::BTreeSet::new();
        loop {
            let page = harness
                .service
                .list_artifacts(
                    "ws_a",
                    ArtifactListFilter {
                        limit: Some(1),
                        cursor,
                        thread_id: thread_scope.then(|| "preview-thread".into()),
                        authorized_artifact_ids: Some(all_ids.clone()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(
                page.items.len(),
                1,
                "no service-only or duplicate-binding empty pages"
            );
            actual.insert(page.items[0].artifact.artifact_id.clone());
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(actual, visible);
    }
    let adopted = harness
        .service
        .ingest_bytes(preview_request("1024", b"adopted".to_vec()))
        .await
        .unwrap();
    let mut target = target("preview-thread", "preview-turn");
    target.binding_kind = ArtifactBindingKind::ManualAttach;
    harness
        .service
        .bind_artifact(BindArtifactRequest {
            workspace_id: "ws_a".into(),
            artifact_id: adopted.artifact.artifact_id.clone(),
            version_id: adopted.artifact.version_id.clone(),
            target,
            metadata: Default::default(),
        })
        .await
        .unwrap();
    harness
        .store
        .database_connection()
        .execute_unprepared("UPDATE turn SET status='completed' WHERE id='preview-turn'")
        .await
        .unwrap();
    preview_maintenance(&harness.service).await;
    assert!(
        harness
            .store
            .get_artifact_version_blob(
                "ws_a",
                &adopted.artifact.artifact_id,
                adopted.artifact.version_id.as_deref()
            )
            .await
            .is_ok()
    );
    assert!(
        harness
            .service
            .list_artifacts("ws_a", ArtifactListFilter::default())
            .await
            .unwrap()
            .items
            .iter()
            .any(|s| s.artifact.artifact_id == adopted.artifact.artifact_id)
    );
}

#[tokio::test]
async fn html_preview_active_delivery_frozen_context_and_legacy_copies_release_after_owners() {
    use pioneer_provider::{ChatMessage, MessageContentPart};
    let mut harness = preview_fixture().await;
    harness.service.gc_policy.grace_secs = 0;
    let source = harness
        .service
        .ingest_bytes(preview_request("input", b"<html>source</html>".to_vec()))
        .await
        .unwrap();
    let mut summaries = Vec::new();
    let mut parts = Vec::new();
    let mut images = Vec::new();
    let mut old_paths = Vec::new();
    for width in [360, 768, 1024] {
        let summary = harness
            .service
            .ingest_bytes(preview_request(
                &width.to_string(),
                test_png_bytes(width, 2),
            ))
            .await
            .unwrap();
        let original = harness
            .service
            .resolve_provider_attachment(
                "ws_a",
                &summary.artifact.artifact_id,
                summary.artifact.version_id.as_deref(),
            )
            .await
            .unwrap();
        let blob = harness
            .store
            .get_artifact_version_blob(
                "ws_a",
                &summary.artifact.artifact_id,
                summary.artifact.version_id.as_deref(),
            )
            .await
            .unwrap();
        assert!(
            matches!(&original.attachment.source,pioneer_provider::AttachmentDataSource::Path{path} if path.contains("/blobs/"))
        );
        // The pre-fix provider resolver created this independent disk copy.
        let legacy = harness
            .service
            .blob_store
            .materialize_readable_copy("ws_a", &blob.storage_key, "legacy.png")
            .await
            .unwrap();
        old_paths.push(legacy.clone());
        let mut attachment = original.attachment;
        attachment.source = pioneer_provider::AttachmentDataSource::Path {
            path: legacy.to_string_lossy().into_owned(),
        };
        images.push(json!({"width":width,"attachment":attachment}));
        parts.push(MessageContentPart::image(attachment));
        summaries.push(summary);
    }
    let mut message = ChatMessage::tool_result(
        "provider-call",
        "html_preview",
        json!({"source":source.artifact,"previews":[],"llm_context":{"images":images}}).to_string(),
    );
    message.content_parts = parts;
    let entry = pioneer_crud::NewTurnLlmContextEntry {
        turn_id: "preview-turn".into(),
        item_id: Some("canonical-call".into()),
        attempt_id: None,
        sequence: 1,
        source: "tool_result_v2".into(),
        tool_name: Some("html_preview".into()),
        payload: json!({"kind":"json","truncated":false,"value":message}).to_string(),
        output_policy_snapshot: "{}".into(),
        created_at: chrono::Utc::now().fixed_offset(),
        expires_at: None,
    };
    harness
        .store
        .append_turn_llm_context(entry.clone(), "preview-result")
        .await
        .unwrap();
    preview_maintenance(&harness.service).await;
    assert!(old_paths.iter().all(|p| p.exists()));
    harness
        .store
        .database_connection()
        .execute_unprepared("UPDATE turn SET status='completed' WHERE id='preview-turn'")
        .await
        .unwrap();
    // A real accepted immutable source holds the screenshots after completion.
    let descriptor=serde_json::from_value(json!({"format":1,"manifest_id":"preview-held","messages":1,"identity_sha256":"a".repeat(64)})).unwrap();
    harness
        .store
        .compaction_begin_frozen_history("ws_a", "preview-thread", &descriptor)
        .await
        .unwrap();
    let source_ref = harness
        .store
        .compaction_context_reference_for_item(
            "ws_a",
            "preview-thread",
            "preview-turn",
            "canonical-call",
            "tool_result_v2",
        )
        .await
        .unwrap()
        .unwrap();
    let references:Vec<_>=serde_json::from_value(json!([{"source_thread":"preview-thread","logical_turn_id":"preview-turn","unit_id":"preview-unit","sources":[source_ref],"inherited":false,"complete":true,"protected_input":false,"wire_sha256":"b".repeat(64),"replay_source":null,"tool_call_id":"provider-call","tool_name":"html_preview"}])).unwrap();
    harness
        .store
        .compaction_append_frozen_history("ws_a", "preview-thread", "preview-held", 0, &references)
        .await
        .unwrap();
    let held = harness
        .store
        .compaction_finish_frozen_history_held("ws_a", "preview-thread", &descriptor)
        .await
        .unwrap();
    preview_maintenance(&harness.service).await;
    assert!(old_paths.iter().all(|p| p.exists()));
    // Existing frozen lifetime owns expiry; this isolated fixture emulates it
    // after dropping the exact read hold. No user/dev database is involved.
    drop(held);
    harness
        .store
        .database_connection()
        .execute_unprepared(
            "UPDATE compaction_frozen_history SET expired=1 WHERE id='preview-held'",
        )
        .await
        .unwrap();
    let delivery = harness.service.html_preview_activity().await;
    preview_maintenance(&harness.service).await;
    assert!(
        old_paths.iter().all(|p| p.exists()),
        "unfinished delivery retains media"
    );
    drop(delivery);
    // An abort in the middle of version retirement rolls the canonical archive
    // and every logical owner back together; no file deletion is yet eligible.
    harness.store.database_connection().execute_unprepared("CREATE TRIGGER fail_preview_release BEFORE DELETE ON artifact_version WHEN EXISTS(SELECT 1 FROM artifact a WHERE a.id=OLD.artifact_id AND a.created_by_actor_id='html_preview' AND json_extract(a.metadata_json,'$.htmlTool.index')='768') BEGIN SELECT RAISE(ABORT,'fixture release rollback'); END").await.unwrap();
    let mut rollback_progress = crate::HtmlPreviewMaintenanceProgress::default();
    let mut rolled_back = false;
    for _ in 0..64 {
        if harness
            .service
            .html_preview_maintenance_quantum(&mut rollback_progress)
            .await
            .is_err()
        {
            rolled_back = true;
            break;
        }
    }
    assert!(rolled_back);
    assert!(old_paths.iter().all(|p| p.exists()));
    let retained = harness
        .store
        .list_turn_llm_context("preview-turn")
        .await
        .unwrap();
    let retained: serde_json::Value = serde_json::from_str(&retained[0].payload).unwrap();
    assert_eq!(
        retained["value"]["content_parts"].as_array().unwrap().len(),
        3
    );
    harness
        .store
        .database_connection()
        .execute_unprepared("DROP TRIGGER fail_preview_release")
        .await
        .unwrap();
    preview_maintenance(&harness.service).await;
    assert!(old_paths.iter().all(|p| !p.exists()));
    for summary in summaries {
        assert!(
            harness
                .store
                .get_artifact_version_blob(
                    "ws_a",
                    &summary.artifact.artifact_id,
                    summary.artifact.version_id.as_deref()
                )
                .await
                .is_err()
        );
    }
    let context = harness
        .store
        .list_turn_llm_context("preview-turn")
        .await
        .unwrap();
    let archived: serde_json::Value = serde_json::from_str(&context[0].payload).unwrap();
    let restored: ChatMessage = serde_json::from_value(archived["value"].clone()).unwrap();
    assert_eq!(restored.tool_call_id.as_deref(), Some("provider-call"));
    assert_eq!(restored.name.as_deref(), Some("html_preview"));
    assert!(restored.content_parts.is_empty());
    assert!(!restored.content.contains("llm_context"));
    assert!(
        harness
            .store
            .append_turn_llm_context(entry, "preview-result")
            .await
            .unwrap()
            .already_committed,
        "late replay uses the same archived canonical result"
    );
    // Restart uses durable orphan/index metadata and remains idempotent.
    preview_maintenance(&ArtifactService::new(
        harness.store.clone(),
        harness.service.blob_store.clone(),
    ))
    .await;
}

struct PreviewDeletionGate {
    local: LocalArtifactBlobStore,
    fail_once: AtomicBool,
    pause_once: AtomicBool,
    entered: Arc<tokio::sync::Notify>,
    late_once: AtomicBool,
    late_resume: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    resume: tokio::sync::Notify,
}
impl PreviewDeletionGate {
    async fn late_unlink(
        &self,
        w: &str,
        k: &str,
        guard: Option<crate::ArtifactBlobMutationGuard>,
    ) -> ArtifactResult<()> {
        let path = self.local.blob_path(w, k)?;
        let entered = self.entered.clone();
        let resume = self.late_resume.clone();
        // Once dispatched, this actual filesystem operation survives caller cancellation.
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            entered.notify_one();
            let (lock, condition) = &*resume;
            let mut ready = lock.lock().unwrap();
            while !*ready {
                ready = condition.wait(ready).unwrap();
            }
            drop(ready);
            std::fs::remove_file(path).map_err(|source| ArtifactError::Io {
                message: "fixture late unlink".into(),
                source,
            })
        })
        .await
        .unwrap()
    }
}
#[async_trait::async_trait]
impl ArtifactBlobStore for PreviewDeletionGate {
    fn local_artifact_root(&self) -> Option<std::path::PathBuf> {
        self.local.local_artifact_root()
    }
    fn local_blob_path(&self, w: &str, k: &str) -> ArtifactResult<Option<std::path::PathBuf>> {
        self.local.local_blob_path(w, k)
    }
    async fn put_bytes(&self, w: &str, i: ArtifactBlobInput) -> ArtifactResult<StoredArtifactBlob> {
        self.local.put_bytes(w, i).await
    }
    async fn open_read(&self, w: &str, k: &str) -> ArtifactResult<ArtifactReadHandle> {
        self.local.open_read(w, k).await
    }
    async fn materialize_readable_copy(
        &self,
        w: &str,
        k: &str,
        n: &str,
    ) -> ArtifactResult<std::path::PathBuf> {
        self.local.materialize_readable_copy(w, k, n).await
    }
    async fn delete_unreferenced(&self, w: &str, k: &str) -> ArtifactResult<()> {
        if self.late_once.swap(false, Ordering::SeqCst) {
            return self.late_unlink(w, k, None).await;
        }
        if self.fail_once.swap(false, Ordering::SeqCst) {
            return Err(ArtifactError::Io {
                message: "fixture deletion failure".into(),
                source: ErrorKind::PermissionDenied.into(),
            });
        }
        if self.pause_once.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.resume.notified().await;
        }
        self.local.delete_unreferenced(w, k).await
    }
    async fn delete_unreferenced_guarded(
        &self,
        w: &str,
        k: &str,
        guard: crate::ArtifactBlobMutationGuard,
    ) -> ArtifactResult<()> {
        if self.late_once.swap(false, Ordering::SeqCst) {
            return self.late_unlink(w, k, Some(guard)).await;
        }
        if self.fail_once.swap(false, Ordering::SeqCst) {
            return Err(ArtifactError::Io {
                message: "fixture deletion failure".into(),
                source: ErrorKind::PermissionDenied.into(),
            });
        }
        if self.pause_once.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.resume.notified().await;
        }
        self.local.delete_unreferenced_guarded(w, k, guard).await
    }
}

#[tokio::test]
async fn html_preview_shared_blob_concurrent_ingestion_and_failed_deletion_restart() {
    let mut harness = preview_fixture().await;
    let storage = Arc::new(PreviewDeletionGate {
        local: LocalArtifactBlobStore::new(harness._temp.path()),
        fail_once: AtomicBool::new(false),
        pause_once: AtomicBool::new(false),
        entered: Default::default(),
        late_once: AtomicBool::new(false),
        late_resume: Default::default(),
        resume: Default::default(),
    });
    harness.service.blob_store = storage.clone();
    harness.service.gc_policy.grace_secs = 0;
    let bytes = b"shared preview and publication";
    let preview = harness
        .service
        .ingest_bytes(preview_request("input", bytes.to_vec()))
        .await
        .unwrap();
    let mut request = ingest_request("ws_a", "preview-thread", "preview-turn", bytes, "published");
    request.created_by_kind = ArtifactCreatedByKind::Tool;
    request.created_by_actor_id = Some("html_render".into());
    request.metadata.insert(
        "htmlTool".into(),
        json!({"version":1,"publication":true,"turnId":"preview-turn","callId":"render"}),
    );
    let published = harness.service.ingest_bytes(request).await.unwrap();
    let blob = harness
        .store
        .get_artifact_version_blob("ws_a", &published.artifact.artifact_id, None)
        .await
        .unwrap();
    harness
        .store
        .database_connection()
        .execute_unprepared("UPDATE turn SET status='completed' WHERE id='preview-turn'")
        .await
        .unwrap();
    preview_maintenance(&harness.service).await;
    assert!(
        harness
            .store
            .get_artifact_version_blob("ws_a", &preview.artifact.artifact_id, None)
            .await
            .is_err()
    );
    assert_eq!(
        harness
            .service
            .read_bounded_artifact(
                ArtifactBoundedReadRequest {
                    workspace_id: "ws_a".into(),
                    artifact_id: published.artifact.artifact_id.clone(),
                    version_id: published.artifact.version_id.clone(),
                    projection_kind: None,
                    offset: None,
                    max_bytes: Some(100)
                },
                100
            )
            .await
            .unwrap()
            .bytes,
        bytes
    );
    assert!(
        storage
            .local
            .blob_path("ws_a", &blob.storage_key)
            .unwrap()
            .exists()
    );

    // A second source is orphaned by the production release path. Deletion
    // failure keeps its blob row, so a fresh maintenance owner can retry it.
    let orphan = harness
        .service
        .ingest_bytes({
            let mut r = preview_request("input", b"race bytes".to_vec());
            r.metadata.get_mut("htmlTool").unwrap()["callId"] = json!("race-call");
            r.binding.as_mut().unwrap().tool_call_id = Some("race-call".into());
            r
        })
        .await
        .unwrap();
    let orphan_blob = harness
        .store
        .get_artifact_version_blob("ws_a", &orphan.artifact.artifact_id, None)
        .await
        .unwrap();
    storage.fail_once.store(true, Ordering::SeqCst);
    let mut progress = crate::HtmlPreviewMaintenanceProgress::default();
    let mut failed = false;
    for _ in 0..64 {
        if harness
            .service
            .html_preview_maintenance_quantum(&mut progress)
            .await
            .is_err()
        {
            failed = true;
            break;
        }
    }
    assert!(failed);
    assert!(
        storage
            .local
            .blob_path("ws_a", &orphan_blob.storage_key)
            .unwrap()
            .exists()
    );
    storage.pause_once.store(true, Ordering::SeqCst);
    let restarted = harness.service.clone();
    let cleanup = tokio::spawn(async move { preview_maintenance(&restarted).await });
    storage.entered.notified().await;
    let ingestion_service = harness.service.clone();
    let registration = tokio::spawn(async move {
        ingestion_service
            .ingest_bytes(ingest_request(
                "ws_a",
                "preview-thread",
                "preview-turn",
                b"race bytes",
                "new user owner",
            ))
            .await
            .unwrap()
    });
    // The actual local storage guard excludes registration until unlink and
    // metadata-row retirement finish; put then recreates the immutable blob.
    tokio::task::yield_now().await;
    assert!(!registration.is_finished());
    storage.resume.notify_one();
    cleanup.await.unwrap();
    let registered = registration.await.unwrap();
    let content = harness
        .service
        .read_bounded_artifact(
            ArtifactBoundedReadRequest {
                workspace_id: "ws_a".into(),
                artifact_id: registered.artifact.artifact_id,
                version_id: registered.artifact.version_id,
                projection_kind: None,
                offset: None,
                max_bytes: Some(100),
            },
            100,
        )
        .await
        .unwrap();
    assert_eq!(content.bytes, b"race bytes");
    preview_maintenance(&harness.service).await;

    // Failed ordinary registration follows a different production cleanup path
    // from GC. Cancel its caller after unlink has actually been dispatched.
    harness.store.database_connection().execute_unprepared("CREATE TRIGGER fail_cancel_registration BEFORE INSERT ON artifact BEGIN SELECT RAISE(ABORT,'fixture registration failure'); END").await.unwrap();
    storage.late_once.store(true, Ordering::SeqCst);
    let failed_service = harness.service.clone();
    let failed = tokio::spawn(async move {
        failed_service
            .ingest_bytes(ingest_request(
                "ws_a",
                "preview-thread",
                "preview-turn",
                b"cancelled cleanup bytes",
                "failed registration",
            ))
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        storage.entered.notified(),
    )
    .await
    .unwrap();
    failed.abort();
    assert!(failed.await.unwrap_err().is_cancelled());
    harness
        .store
        .database_connection()
        .execute_unprepared("DROP TRIGGER fail_cancel_registration")
        .await
        .unwrap();
    let next_service = harness.service.clone();
    let mut next = tokio::spawn(async move {
        next_service
            .ingest_bytes(ingest_request(
                "ws_a",
                "preview-thread",
                "preview-turn",
                b"cancelled cleanup bytes",
                "new owner after cancellation",
            ))
            .await
            .unwrap()
    });
    let pending = tokio::time::timeout(std::time::Duration::from_millis(100), &mut next).await;
    let was_blocked = pending.is_err();
    // Always release the fixture's outstanding I/O, even on a regression.
    {
        let (lock, condition) = &*storage.late_resume;
        *lock.lock().unwrap() = true;
        condition.notify_one();
    }
    let next = match pending {
        Ok(result) => result.unwrap(),
        Err(_) => next.await.unwrap(),
    };
    assert!(
        was_blocked,
        "late unlink must retain the guard through actual completion"
    );
    assert_eq!(
        harness
            .service
            .read_bounded_artifact(
                ArtifactBoundedReadRequest {
                    workspace_id: "ws_a".into(),
                    artifact_id: next.artifact.artifact_id,
                    version_id: next.artifact.version_id,
                    projection_kind: None,
                    offset: None,
                    max_bytes: Some(100)
                },
                100
            )
            .await
            .unwrap()
            .bytes,
        b"cancelled cleanup bytes"
    );

    // A cancelled first-party HTML registration leaves a durable staged orphan.
    // The same-age ordinary orphan is outside automatic preview cleanup policy.
    let ordinary = storage
        .local
        .put_bytes(
            "ws_a",
            ArtifactBlobInput::Bytes(b"ordinary orphan".to_vec()),
        )
        .await
        .unwrap();
    harness
        .store
        .find_or_create_artifact_blob(NewArtifactBlobRecord {
            workspace_id: "ws_a".into(),
            sha256: ordinary.sha256.clone(),
            size_bytes: ordinary.size_bytes,
            mime_type: None,
            storage_backend: ordinary.storage_backend.clone(),
            storage_key: ordinary.storage_key.clone(),
            metadata: Default::default(),
        })
        .await
        .unwrap();
    let cancelled = tokio_util::sync::CancellationToken::new();
    cancelled.cancel();
    assert!(
        harness
            .service
            .ingest_tool_bytes(
                preview_request("input", b"staged HTML orphan".to_vec()),
                "staged-preview".into(),
                0,
                cancelled
            )
            .await
            .is_err()
    );
    let staged = stored_blob_for(b"staged HTML orphan");
    let staged_path = storage
        .local
        .blob_path("ws_a", &staged.storage_key)
        .unwrap();
    assert!(staged_path.exists());
    harness.store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"UPDATE artifact_blob SET created_at='2000-01-01 00:00:00+00:00' WHERE storage_key IN (?1,?2)",[ordinary.storage_key.clone().into(),staged.storage_key.clone().into()])).await.unwrap();
    preview_maintenance(&harness.service).await;
    assert!(!staged_path.exists());
    assert!(
        storage
            .local
            .blob_path("ws_a", &ordinary.storage_key)
            .unwrap()
            .exists()
    );
    assert!(
        storage
            .local
            .blob_path("ws_a", &blob.storage_key)
            .unwrap()
            .exists(),
        "permanent shared owner remains readable"
    );
}

#[tokio::test]
async fn html_preview_quiescent_pass_reaches_idle_and_discovers_later_preview() {
    use sha2::{Digest, Sha256};
    let mut harness = preview_fixture().await;
    harness.service.gc_policy.grace_secs = 0;
    // One indexed frozen header and 17 referenced blobs deliberately give
    // the independent traversals different EOF phases at page size sixteen.
    let descriptor=serde_json::from_value(json!({"format":1,"manifest_id":"idle-header","messages":0,"identity_sha256":hex::encode(Sha256::digest([]))})).unwrap();
    harness
        .store
        .compaction_begin_frozen_history("ws_a", "preview-thread", &descriptor)
        .await
        .unwrap();
    let held = harness
        .store
        .compaction_finish_frozen_history_held("ws_a", "preview-thread", &descriptor)
        .await
        .unwrap();
    drop(held);
    for index in 0..17u8 {
        harness
            .service
            .ingest_bytes(ingest_request(
                "ws_a",
                "preview-thread",
                "preview-turn",
                &[index],
                "ordinary referenced blob",
            ))
            .await
            .unwrap();
    }
    let mut progress = crate::HtmlPreviewMaintenanceProgress::default();
    let mut idle = false;
    for _ in 0..64 {
        if !harness
            .service
            .html_preview_maintenance_quantum(&mut progress)
            .await
            .unwrap()
        {
            idle = true;
            break;
        }
    }
    assert!(idle, "EOF is remembered until the common pass ends");
    let preview = harness
        .service
        .ingest_bytes(preview_request("input", b"arrived after idle".to_vec()))
        .await
        .unwrap();
    let blob = harness
        .store
        .get_artifact_version_blob("ws_a", &preview.artifact.artifact_id, None)
        .await
        .unwrap();
    let path = LocalArtifactBlobStore::new(harness._temp.path())
        .blob_path("ws_a", &blob.storage_key)
        .unwrap();
    assert!(path.exists());
    harness
        .store
        .database_connection()
        .execute_unprepared("UPDATE turn SET status='completed' WHERE id='preview-turn'")
        .await
        .unwrap();
    // Reuse the same worker state after idle. Later data is found on the next
    // complete pass, and its newly orphaned bytes on the following one.
    for _ in 0..2 {
        let mut idle = false;
        for _ in 0..64 {
            if !harness
                .service
                .html_preview_maintenance_quantum(&mut progress)
                .await
                .unwrap()
            {
                idle = true;
                break;
            }
        }
        assert!(idle);
    }
    assert!(!path.exists());
}
