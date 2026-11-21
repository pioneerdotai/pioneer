use pioneer_crud_access::authorization_scope::{
    list_abandoned_runtime_draft_artifact_ids, list_artifact_ids_for_authorized_thread_roots,
    resolve_artifact_authorization_scope, resolve_runtime_draft_artifact_authorization_scope,
};
use pioneer_crud_artifacts::{
    artifact::{ArtifactBindingTargetRecord, IngestArtifactMetadataRecord, NewArtifactBlobRecord},
    ingest::PreparedArtifactIngest,
};
use pioneer_protocol::{
    ArtifactBindingDirection, ArtifactBindingKind, ArtifactCreatedByKind, ArtifactKind,
    ArtifactRole, PrincipalId,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, TransactionTrait, sea_query::Expr,
};
use std::collections::BTreeMap;

#[tokio::test]
async fn draft_upload_authorization_uses_thread_while_planned_turn_is_not_materialized() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            "INSERT INTO workspace (id, name) VALUES ('ws_draft_artifact_scope', 'Draft uploads');
             INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
             VALUES ('thread_draft_artifact_scope', 'ws_draft_artifact_scope', '', 'agent', 'm', 'p', 'active');
             INSERT INTO turn (id, thread_id, status)
             VALUES ('turn_existing_draft_artifact_scope', 'thread_draft_artifact_scope', 'in_progress');",
        ).await?;
        let draft_binding = ArtifactBindingTargetRecord {
            thread_id: Some("thread_draft_artifact_scope".to_owned()),
            turn_id: Some("turn_planned_draft_artifact_scope".to_owned()),
            message_id: None,
            turn_item_id: None,
            tool_call_id: None,
            task_id: None,
            task_run_id: None,
            binding_kind: ArtifactBindingKind::DraftUpload,
            direction: ArtifactBindingDirection::Input,
            role: Some(ArtifactRole::User),
            item_index: None,
        };
        let prepared = PreparedArtifactIngest::new(
                NewArtifactBlobRecord {
                    workspace_id: "ws_draft_artifact_scope".to_owned(),
                    sha256: "sha256_draft_artifact_scope".to_owned(),
                    size_bytes: 128,
                    mime_type: Some("image/png".to_owned()),
                    storage_backend: "memory".to_owned(),
                    storage_key: "blob_draft_artifact_scope".to_owned(),
                    metadata: BTreeMap::new(),
                },
                IngestArtifactMetadataRecord {
                    workspace_id: "ws_draft_artifact_scope".to_owned(),
                    primary_thread_id: Some("thread_draft_artifact_scope".to_owned()),
                    display_name: "draft.png".to_owned(),
                    kind: ArtifactKind::Image,
                    mime_type: Some("image/png".to_owned()),
                    created_by_kind: ArtifactCreatedByKind::User,
                    created_by_actor_id: Some("user_1".to_owned()),
                    metadata: BTreeMap::new(),
                },
                Some(draft_binding),
                BTreeMap::new(), None, None,
            )?;
        let transaction = db.begin().await?;
        let ingested = prepared.apply(&transaction).await?;
        transaction.commit().await?;

        let scope = resolve_artifact_authorization_scope(
            db,
            ingested.artifact.id.as_str(),
            Some("ws_draft_artifact_scope"),
            Some("thread_draft_artifact_scope"),
        )
        .await
        .expect("draft artifact scope should resolve")
        .expect("planned turn gap must use the bound thread scope");
        assert_eq!(
            scope.thread_id.as_deref(),
            Some("thread_draft_artifact_scope")
        );

        let authorized_roots = [vec!["thread_draft_artifact_scope".to_owned()]];
        assert_eq!(
            list_artifact_ids_for_authorized_thread_roots(db, "ws_draft_artifact_scope", &authorized_roots).await?,
            [ingested.artifact.id.clone()]
        );
        pioneer_entity::artifact_binding::Entity::update_many()
            .col_expr(
                pioneer_entity::artifact_binding::Column::BindingKind,
                Expr::value("manual_attach"),
            )
            .filter(
                pioneer_entity::artifact_binding::Column::ArtifactId
                    .eq(ingested.artifact.id.clone()),
            )
            .exec(db)
            .await
            .expect("binding kind update should succeed");
        assert!(
            resolve_artifact_authorization_scope(
                db,
                ingested.artifact.id.as_str(),
                Some("ws_draft_artifact_scope"),
                Some("thread_draft_artifact_scope"),
            )
            .await
            .expect("non-draft artifact scope should resolve deterministically")
            .is_none(),
            "only draft uploads may reference an unmaterialized planned turn"
        );
        assert!(
            list_artifact_ids_for_authorized_thread_roots(db, "ws_draft_artifact_scope", &authorized_roots).await?.is_empty()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn runtime_draft_artifact_scope_requires_exact_owner_and_exclusive_draft_binding() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            "INSERT INTO workspace (id, name) VALUES ('ws_runtime_draft_scope', 'Runtime drafts')",
        )
        .await?;
        let owner = PrincipalId::new("P0000000000000000000A").expect("principal id");
        let thread_id = "thread_runtime_draft_scope";
        let prepared = PreparedArtifactIngest::new(
            NewArtifactBlobRecord {
                workspace_id: "ws_runtime_draft_scope".to_owned(),
                sha256: "sha256_runtime_draft_scope".to_owned(),
                size_bytes: 128,
                mime_type: Some("text/plain".to_owned()),
                storage_backend: "memory".to_owned(),
                storage_key: "blob_runtime_draft_scope".to_owned(),
                metadata: BTreeMap::new(),
            },
            IngestArtifactMetadataRecord {
                workspace_id: "ws_runtime_draft_scope".to_owned(),
                primary_thread_id: Some(thread_id.to_owned()),
                display_name: "draft.txt".to_owned(),
                kind: ArtifactKind::Text,
                mime_type: Some("text/plain".to_owned()),
                created_by_kind: ArtifactCreatedByKind::User,
                created_by_actor_id: Some(owner.to_string()),
                metadata: BTreeMap::new(),
            },
            Some(ArtifactBindingTargetRecord {
                thread_id: Some(thread_id.to_owned()),
                turn_id: Some("turn_planned_runtime_draft_scope".to_owned()),
                message_id: None,
                turn_item_id: None,
                tool_call_id: None,
                task_id: None,
                task_run_id: None,
                binding_kind: ArtifactBindingKind::DraftUpload,
                direction: ArtifactBindingDirection::Input,
                role: Some(ArtifactRole::User),
                item_index: None,
            }),
            BTreeMap::new(),
            None,
            None,
        )?;
        let transaction = db.begin().await?;
        let ingested = prepared.apply(&transaction).await?;
        transaction.commit().await?;

        assert!(
            resolve_runtime_draft_artifact_authorization_scope(
                db,
                ingested.artifact.id.as_str(),
                "ws_runtime_draft_scope",
                thread_id,
                &owner,
            )
            .await
            .expect("runtime draft artifact scope should resolve")
            .is_some()
        );
        let foreign = PrincipalId::new("P0000000000000000000B").expect("foreign principal id");
        assert!(
            resolve_runtime_draft_artifact_authorization_scope(
                db,
                ingested.artifact.id.as_str(),
                "ws_runtime_draft_scope",
                thread_id,
                &foreign,
            )
            .await
            .expect("foreign runtime draft artifact scope should resolve deterministically")
            .is_none(),
            "a different principal must not inherit the draft upload"
        );
        assert_eq!(
            list_abandoned_runtime_draft_artifact_ids(db, "ws_runtime_draft_scope", thread_id,)
                .await
                .expect("abandoned runtime draft artifacts should list"),
            vec![ingested.artifact.id.clone()]
        );

        pioneer_entity::artifact_binding::Entity::update_many()
            .col_expr(
                pioneer_entity::artifact_binding::Column::BindingKind,
                Expr::value("manual_attach"),
            )
            .filter(
                pioneer_entity::artifact_binding::Column::ArtifactId
                    .eq(ingested.artifact.id.clone()),
            )
            .exec(db)
            .await
            .expect("binding kind update should succeed");
        assert!(
            resolve_runtime_draft_artifact_authorization_scope(
                db,
                ingested.artifact.id.as_str(),
                "ws_runtime_draft_scope",
                thread_id,
                &owner,
            )
            .await
            .expect("promoted artifact scope should resolve deterministically")
            .is_none(),
            "only an exclusive draft_upload binding is valid before materialization"
        );
        assert!(
            list_abandoned_runtime_draft_artifact_ids(db, "ws_runtime_draft_scope", thread_id,)
                .await
                .expect("promoted artifact cleanup scope should resolve")
                .is_empty(),
            "cleanup must not delete an artifact that has left draft-only scope"
        );
        Ok(())
    })
    .await
    .unwrap();
}
