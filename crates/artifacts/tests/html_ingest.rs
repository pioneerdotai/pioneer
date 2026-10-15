//! Ownership, retry and quota regression coverage using a physically read-only
//! reader and the production serialized writer. Execution is review-gated.
use migration::Migrator;
use pioneer_artifacts::{
    ArtifactBindingTarget, ArtifactBoundedReadRequest, ArtifactQuotaPolicy, ArtifactService,
    IngestArtifactBytesRequest, LocalArtifactBlobStore,
};
use pioneer_crud::CrudStore;
use pioneer_protocol::{
    ArtifactBindingDirection, ArtifactBindingKind, ArtifactCreatedByKind, ArtifactKind,
    ArtifactRole,
};
use pioneer_sqlite::{SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor};
use sea_orm::{ConnectOptions, ConnectionTrait, Database};
use std::{collections::BTreeMap, sync::Arc};
use tokio_util::sync::CancellationToken;

async fn fixture() -> (tempfile::TempDir, ArtifactService, Arc<CrudStore>) {
    fixture_with_projection(true).await
}

async fn fixture_with_projection(
    work_projection: bool,
) -> (tempfile::TempDir, ArtifactService, Arc<CrudStore>) {
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("fixture.sqlite");
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", file.display()));
    options.max_connections(1).min_connections(1);
    let connection = Database::connect(options).await.unwrap();
    let writer = SqliteWriteExecutor::new(connection.clone());
    writer
        .run_migrations::<Migrator>(SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    let seed = SqliteDatabase::from_executor(connection, writer.clone()).maintenance();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','HTML fixture',1,1)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('thread','ws','','agent','model','provider','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('other','ws','','agent','model','provider','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn','thread','in_progress','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('foreign','other','in_progress','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn_work_projection(turn_id,workspace_id,thread_id,presentation,state) VALUES ('turn','ws','thread','expanded_live','running')",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('finished','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ] {
        if !work_projection && sql.starts_with("INSERT INTO turn_work_projection") {
            continue;
        }
        seed.execute_unprepared(sql).await.unwrap();
    }
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=ro", file.display()));
    options.max_connections(1).min_connections(1);
    let reader = Database::connect(options).await.unwrap();
    reader
        .execute_unprepared("PRAGMA query_only=ON")
        .await
        .unwrap();
    let database = SqliteDatabase::from_executor(reader, writer);
    assert!(database.reader_query_only_enabled().await.unwrap());
    let store = Arc::new(CrudStore::new(database));
    let service = ArtifactService::new_with_quota(
        store.clone(),
        Arc::new(LocalArtifactBlobStore::new(temp.path().join("blobs"))),
        ArtifactQuotaPolicy {
            max_file_bytes: 1024,
            max_workspace_bytes: 1024,
            max_files_per_workspace: 1,
            warn_at_percent: 80,
        },
    );
    (temp, service, store)
}

fn request(call: &str, turn: &str) -> IngestArtifactBytesRequest {
    IngestArtifactBytesRequest {
        workspace_id: "ws".into(),
        primary_thread_id: Some("thread".into()),
        bytes: b"<!doctype html><p>one</p>".to_vec(),
        display_name: "Page".into(),
        kind: ArtifactKind::File,
        mime_type: Some("text/html".into()),
        created_by_kind: ArtifactCreatedByKind::Tool,
        created_by_actor_id: Some("html_render".into()),
        metadata: BTreeMap::from([(
            "htmlTool".into(),
            serde_json::json!({"version":1,"publication":true,"turnId":turn,"callId":call,"index":"document","title":"Page"}),
        )]),
        binding: Some(ArtifactBindingTarget {
            thread_id: Some("thread".into()),
            turn_id: Some(turn.into()),
            message_id: None,
            turn_item_id: None,
            tool_call_id: Some(call.into()),
            task_id: None,
            task_run_id: None,
            binding_kind: ArtifactBindingKind::ToolOutput,
            direction: ArtifactBindingDirection::Output,
            role: Some(ArtifactRole::Tool),
            item_index: None,
        }),
    }
}

#[tokio::test]
async fn html_ingest_recovers_exact_result_at_full_quota_and_preserves_its_blob_on_conflict() {
    let (_temp, service, _store) = fixture().await;
    let (a, b) = tokio::join!(
        service.ingest_tool_bytes(
            request("call", "turn"),
            "identity".into(),
            1,
            CancellationToken::new()
        ),
        service.ingest_tool_bytes(
            request("call", "turn"),
            "identity".into(),
            1,
            CancellationToken::new()
        )
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(a.artifact, b.artifact);
    assert_eq!(a.bindings.len(), 1);
    assert_eq!(a.artifact.mime_type.as_deref(), Some("text/html"));
    assert_eq!(a.bindings[0].version_id, a.artifact.version_id);
    let recovered = service
        .ingest_tool_bytes(
            request("call", "turn"),
            "identity".into(),
            1,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(recovered.artifact, a.artifact);
    assert!(
        service
            .ingest_tool_bytes(
                request("another", "turn"),
                "another-identity".into(),
                1,
                CancellationToken::new()
            )
            .await
            .is_err()
    );
    let mut conflict = request("call", "turn");
    conflict.bytes = b"<p>changed</p>".to_vec();
    assert!(
        service
            .ingest_tool_bytes(conflict, "identity".into(), 1, CancellationToken::new())
            .await
            .is_err()
    );
    let read = service
        .read_bounded_artifact(
            ArtifactBoundedReadRequest {
                workspace_id: "ws".into(),
                artifact_id: a.artifact.artifact_id,
                version_id: a.artifact.version_id,
                projection_kind: None,
                offset: None,
                max_bytes: Some(1024),
            },
            1024,
        )
        .await
        .unwrap();
    assert_eq!(read.bytes, request("call", "turn").bytes);
    assert!(!read.truncated);
}

#[tokio::test]
async fn html_ingest_rechecks_owner_and_serializes_competing_quota_reservations() {
    let (_temp, service, _store) = fixture().await;
    assert!(
        service
            .ingest_tool_bytes(
                request("stale", "turn"),
                "stale".into(),
                2,
                CancellationToken::new()
            )
            .await
            .is_err()
    );
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(
        service
            .ingest_tool_bytes(
                request("cancelled", "turn"),
                "cancelled".into(),
                1,
                cancelled
            )
            .await
            .is_err()
    );
    for turn in ["foreign", "finished", "missing"] {
        assert!(
            service
                .ingest_tool_bytes(
                    request("call", turn),
                    format!("identity-{turn}"),
                    1,
                    CancellationToken::new()
                )
                .await
                .is_err()
        );
    }
    let (a, b) = tokio::join!(
        service.ingest_tool_bytes(
            request("a", "turn"),
            "a".into(),
            1,
            CancellationToken::new()
        ),
        service.ingest_tool_bytes(
            request("b", "turn"),
            "b".into(),
            1,
            CancellationToken::new()
        )
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
}

#[tokio::test]
async fn html_publication_status_advances_replay_revision_and_bindings_do_not_consume_identity_page()
 {
    let (_temp, service, store) = fixture().await;
    let before = store
        .get_turn_work_projection("turn")
        .await
        .unwrap()
        .unwrap();
    let summary = service
        .ingest_tool_bytes(
            request("call", "turn"),
            "identity".into(),
            1,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let published = store
        .get_turn_work_projection("turn")
        .await
        .unwrap()
        .unwrap();
    assert!(published.updated_at > before.updated_at);
    assert_eq!(
        published.source_high_watermark,
        before.source_high_watermark
    );
    service
        .bind_artifact(pioneer_artifacts::BindArtifactRequest {
            workspace_id: "ws".into(),
            artifact_id: summary.artifact.artifact_id.clone(),
            version_id: summary.artifact.version_id.clone(),
            target: request("call", "turn").binding.unwrap(),
            metadata: Default::default(),
        })
        .await
        .unwrap();
    let page = service
        .list_thread_artifacts(
            "ws",
            "thread",
            pioneer_artifacts::ArtifactListFilter {
                turn_id: Some("turn".into()),
                created_by_actor_id: Some("html_render".into()),
                limit: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert!(page.next_cursor.is_none());
    let mut foreign = request("call", "foreign").binding.unwrap();
    foreign.thread_id = Some("other".into());
    service
        .bind_artifact(pioneer_artifacts::BindArtifactRequest {
            workspace_id: "ws".into(),
            artifact_id: summary.artifact.artifact_id.clone(),
            version_id: summary.artifact.version_id.clone(),
            target: foreign,
            metadata: Default::default(),
        })
        .await
        .unwrap();
    let foreign_page = service
        .list_thread_artifacts(
            "ws",
            "other",
            pioneer_artifacts::ArtifactListFilter {
                turn_id: Some("foreign".into()),
                created_by_actor_id: Some("html_render".into()),
                limit: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(foreign_page.items.is_empty());
    assert!(foreign_page.next_cursor.is_none());
    service
        .delete_artifact("ws", &summary.artifact.artifact_id)
        .await
        .unwrap();
    let deleted = store
        .get_turn_work_projection("turn")
        .await
        .unwrap()
        .unwrap();
    assert!(deleted.updated_at > published.updated_at);
    let summary = service
        .get_artifact("ws", &summary.artifact.artifact_id, None)
        .await
        .unwrap();
    assert_eq!(
        pioneer_protocol::HtmlPublication::from_summary(&summary, "thread", "turn")
            .unwrap()
            .artifact
            .status,
        pioneer_protocol::ArtifactStatus::Deleted
    );
    service
        .restore_artifact("ws", &summary.artifact.artifact_id)
        .await
        .unwrap();
    let restored = store
        .get_turn_work_projection("turn")
        .await
        .unwrap()
        .unwrap();
    assert!(restored.updated_at > deleted.updated_at);
}

#[tokio::test]
async fn html_publication_survives_completion_without_cli_tool_echo_and_rebuilds_missing_work() {
    let (_temp, service, store) = fixture_with_projection(false).await;
    assert!(
        store
            .get_turn_work_projection("turn")
            .await
            .unwrap()
            .is_none()
    );
    let summary = service
        .ingest_tool_bytes(
            request("call", "turn"),
            "identity".into(),
            1,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let work = store
        .get_turn_work_projection("turn")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(work.work_count, 0);
    let now = chrono::Utc::now().timestamp();
    let published_revision = work.updated_at;
    store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                item: pioneer_protocol::TurnItem::AgentMessage {
                    id: "final".into(),
                    text: "Published".into(),
                    phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
                    markdown: None,
                    markdown_version: None,
                },
            },
            now,
        )
        .await
        .unwrap();
    let (_, mut turn) = store.get_turn("thread", "turn").await.unwrap().unwrap();
    turn.status = pioneer_protocol::TurnStatus::Completed;
    store
        .materialize_turn_completed(
            pioneer_protocol::TurnCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn,
            },
            now,
        )
        .await
        .unwrap();
    let work = store
        .get_turn_work_projection("turn")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(work.work_count, 0);
    assert_eq!(work.state, "completed");
    assert!(work.updated_at >= published_revision);
    let page = store
        .list_thread_timeline_projection_page(
            "thread",
            None,
            pioneer_crud::ProjectionPageAnchor::Start,
            10,
        )
        .await
        .unwrap();
    assert!(
        page.iter()
            .any(|block| block.block_kind == pioneer_crud::BLOCK_KIND_TURN_WORK)
    );
    assert!(pioneer_protocol::HtmlPublication::from_summary(&summary, "thread", "turn").is_some());
}
