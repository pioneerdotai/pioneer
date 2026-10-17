use super::*;
use migration::{Migrator, MigratorTrait};
use pioneer_artifacts::{ArtifactService, LocalArtifactBlobStore};
use pioneer_protocol::{
    PersistedActorRef, TurnFilesystemAccess, TurnFilesystemSandboxEntry, TurnPermissionMode,
    TurnPermissionProfileSource,
};
use pioneer_provider::{
    AttachmentDataSource, InputContentType, MessageContentPart, ProviderRegistry,
};
use sea_orm::{ConnectionTrait, Database};
use std::sync::Arc;

#[tokio::test]
async fn accepted_local_media_uses_exact_bytes_after_mutation_deletion_and_snapshot_restore() {
    let database = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&database, None).await.unwrap();
    database
        .execute_unprepared(
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','Media',1,1)",
        )
        .await
        .unwrap();
    database.execute_unprepared("INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status) VALUES ('thread','ws','','agent','model','fixture','idle')").await.unwrap();
    let store = Arc::new(CrudStore::new(database));
    let root = tempfile::tempdir().unwrap();
    let artifacts = ArtifactService::new(
        store.clone(),
        Arc::new(LocalArtifactBlobStore::new(root.path().join("blobs"))),
    );
    let providers = ProviderRegistry::new(|_| String::new());
    let profile = TurnPermissionProfileSnapshot::from_mode(
        TurnPermissionMode::AutoAcceptEdits,
        TurnPermissionProfileSource::Composer,
    );
    let security = TurnExecutionSecuritySnapshot::workspace_write(
        profile.clone(),
        root.path().to_string_lossy(),
        vec![TurnFilesystemSandboxEntry::workspace_root(
            TurnFilesystemAccess::Write,
            root.path().to_string_lossy(),
        )],
        1,
    );
    let mut input = Vec::new();
    let mut paths = Vec::new();
    for kind in [
        InputContentType::Image,
        InputContentType::File,
        InputContentType::Audio,
        InputContentType::Video,
    ] {
        let path = root.path().join(format!("original-{kind:?}.bin"));
        tokio::fs::write(&path, b"original accepted bytes")
            .await
            .unwrap();
        let path_text = path.to_string_lossy().to_string();
        input.push(match kind {
            InputContentType::Image => UserInput::LocalImage { path: path_text },
            InputContentType::Audio => UserInput::LocalAudio { path: path_text },
            InputContentType::Video => UserInput::LocalVideo { path: path_text },
            _ => UserInput::LocalFile { path: path_text },
        });
        paths.push(path);
    }
    let accepted = capture_native_media_inputs(
        &artifacts,
        &providers,
        "ws",
        "thread",
        "fixture",
        None,
        &input,
        &security,
        &PersistedActorRef::System,
    )
    .await
    .unwrap();
    assert!(accepted.iter().all(|value| matches!(value, UserInput::Artifact { version_id: Some(version), .. } if !version.is_empty())));
    let live = resolve_owned_artifact_inputs(&artifacts, "ws", &accepted)
        .await
        .unwrap();
    for path in paths {
        tokio::fs::write(&path, b"replacement bytes").await.unwrap();
        tokio::fs::remove_file(&path).await.unwrap();
    }
    let cold = resolve_owned_artifact_inputs(&artifacts, "ws", &accepted)
        .await
        .unwrap();
    assert_eq!(live, cold);
    assert_eq!(
        cold.iter()
            .map(|value| value.content_type)
            .collect::<Vec<_>>(),
        [
            InputContentType::Image,
            InputContentType::File,
            InputContentType::Audio,
            InputContentType::Video
        ]
    );
    assert!(cold.iter().all(|value| value.attachment.source
        == pioneer_provider::attachments::owned_bytes_source(b"original accepted bytes")));
    // Persisted operational paths are ignored when exact artifact identity exists.
    let mut message = ChatMessage::user_parts(
        cold.iter()
            .map(|value| {
                let mut attachment = value.attachment.clone();
                attachment.source = AttachmentDataSource::Path {
                    path: "/deleted/SECRET/old-copy".to_owned(),
                };
                match value.content_type {
                    InputContentType::Image => MessageContentPart::image(attachment),
                    InputContentType::Audio => MessageContentPart::audio(attachment),
                    InputContentType::Video => MessageContentPart::video(attachment),
                    _ => MessageContentPart::file(attachment),
                }
            })
            .collect(),
    );
    let restored_history = materialize_owned_history(&artifacts, "ws", vec![message.clone()])
        .await
        .unwrap();
    assert!(
        restored_history[0]
            .content_parts
            .iter()
            .all(|part| match part {
                MessageContentPart::Image { image: attachment }
                | MessageContentPart::File { file: attachment }
                | MessageContentPart::Audio { audio: attachment }
                | MessageContentPart::Video { video: attachment } =>
                    matches!(attachment.source, AttachmentDataSource::Bytes { .. }),
                _ => false,
            })
    );
    if let MessageContentPart::Image { image } = &mut message.content_parts[0] {
        image.sha256 = Some("incorrect".into());
    }
    assert!(
        materialize_owned_history(&artifacts, "ws", vec![message])
            .await
            .is_err()
    );
    let snapshot = new_turn_runtime_snapshot(
        "thread",
        "ws",
        "turn",
        ThreadMode::Agent,
        &AgentTurnHookRuntimeContext::default(),
        "model",
        "fixture",
        None,
        &HashMap::new(),
        &accepted,
        &[],
        &cold,
        &HashMap::new(),
        &restored_history,
        &[],
        &pioneer_skills::SkillCatalogSnapshot {
            version: 0,
            generated_at_unix: 0,
            skills: Vec::new(),
        },
    )
    .unwrap();
    let durable: Vec<ResolvedArtifactInput> =
        serde_json::from_str(&snapshot.resolved_artifacts_json).unwrap();
    assert!(durable.iter().all(|value| matches!(
        value.attachment.source,
        AttachmentDataSource::Reference { .. }
    )));
    assert!(!snapshot.resolved_artifacts_json.contains("base64_data"));
    assert!(!snapshot.input_json.contains("original-"));
    // A legacy locator is not implicitly reopened into a new semantic Turn.
    assert!(
        resolve_owned_artifact_inputs(&artifacts, "ws", &input)
            .await
            .is_err()
    );
}

#[test]
fn accepted_name_is_semantic_basename_and_never_includes_url_credentials_or_query() {
    assert_eq!(
        accepted_media_name("/private/input/report.pdf", true, InputContentType::File),
        "report.pdf"
    );
    assert_eq!(
        accepted_media_name(
            "https://user:SECRET@example.org/doc/report.pdf?token=SECRET",
            false,
            InputContentType::File
        ),
        "report.pdf"
    );
}
