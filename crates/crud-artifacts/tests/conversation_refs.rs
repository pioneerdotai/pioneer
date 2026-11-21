use pioneer_crud_artifacts::{
    artifact::{
        ArtifactBindingTargetRecord, ArtifactRepository, ConversationArtifactRefLimits,
        IngestArtifactMetadataRecord, NewArtifactBlobRecord,
    },
    ingest::{ArtifactIngestStore, PreparedArtifactIngest},
    mutations::bind_artifact,
};
use pioneer_protocol::{
    ArtifactBindingDirection, ArtifactBindingKind, ArtifactCreatedByKind, ArtifactKind,
    ArtifactRole,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::ConnectionTrait;
use std::collections::BTreeMap;

fn test_artifact_binding(
    binding_kind: ArtifactBindingKind,
    message_id: Option<&str>,
) -> ArtifactBindingTargetRecord {
    ArtifactBindingTargetRecord {
        thread_id: Some("thread_artifact_refs".to_owned()),
        turn_id: Some("turn_artifact_refs".to_owned()),
        message_id: message_id.map(ToOwned::to_owned),
        turn_item_id: Some("item_artifact_refs".to_owned()),
        tool_call_id: None,
        task_id: None,
        task_run_id: None,
        binding_kind,
        direction: ArtifactBindingDirection::Input,
        role: Some(ArtifactRole::User),
        item_index: Some(0),
    }
}

#[tokio::test]
async fn conversation_artifact_refs_exclude_draft_upload_bindings() {
    pioneer_postgres_test::with_database(async |fixture| {
        let writer = fixture.connection();
        writer
            .execute_unprepared(
                "UPDATE workspace SET is_current = false WHERE is_current;
                 INSERT INTO workspace (id, name, is_active, is_current)
                 VALUES ('ws_artifact_refs', 'Test Workspace', true, true)",
            )
            .await?;
        let db = SqliteDatabase::new(fixture.connect_read_only().await?, writer.clone());
        let wake = tokio::sync::Notify::new();
        let store = ArtifactIngestStore::new(&db, &wake);
        let prepared = PreparedArtifactIngest::new(
            NewArtifactBlobRecord {
                workspace_id: "ws_artifact_refs".to_owned(),
                sha256: "sha256_artifact_refs".to_owned(),
                size_bytes: 128,
                mime_type: Some("image/png".to_owned()),
                storage_backend: "memory".to_owned(),
                storage_key: "blob_artifact_refs".to_owned(),
                metadata: BTreeMap::new(),
            },
            IngestArtifactMetadataRecord {
                workspace_id: "ws_artifact_refs".to_owned(),
                primary_thread_id: Some("thread_artifact_refs".to_owned()),
                display_name: "image.png".to_owned(),
                kind: ArtifactKind::Image,
                mime_type: Some("image/png".to_owned()),
                created_by_kind: ArtifactCreatedByKind::User,
                created_by_actor_id: Some("user_1".to_owned()),
                metadata: BTreeMap::new(),
            },
            Some(test_artifact_binding(
                ArtifactBindingKind::DraftUpload,
                None,
            )),
            BTreeMap::new(),
            None,
            None,
        )?;
        let ingested = store
            .commit_without_publication(prepared, None)
            .await
            .expect("artifact ingest should succeed");

        bind_artifact(
            &db,
            "ws_artifact_refs",
            &ingested.artifact.id,
            Some(&ingested.version.id),
            test_artifact_binding(ArtifactBindingKind::UserInput, Some("user_message_1")),
            BTreeMap::new(),
        )
        .await
        .expect("user input binding should succeed");

        let refs = ArtifactRepository::new()
            .list_conversation_artifact_refs(
                &db,
                "ws_artifact_refs",
                "thread_artifact_refs",
                &["turn_artifact_refs".to_owned()],
                ConversationArtifactRefLimits::default(),
            )
            .await
            .expect("conversation refs should load");
        let turn_refs = refs.get("turn_artifact_refs").expect("turn refs");

        assert_eq!(turn_refs.user.len(), 1);
        assert_eq!(
            turn_refs.user[0].binding_kind,
            ArtifactBindingKind::UserInput
        );
        assert_eq!(
            turn_refs.user[0].message_id.as_deref(),
            Some("user_message_1")
        );
        assert!(turn_refs.assistant.is_empty());
        Ok(())
    })
    .await
    .unwrap();
}
