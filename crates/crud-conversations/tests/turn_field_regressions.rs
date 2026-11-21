use pioneer_crud_conversations::{
    thread_mutations::upsert_thread_with_creator, turn_reads::get_turn_for_thread,
    turn_writes::write_turn_prompt_manifest,
};
use pioneer_protocol::*;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ConnectionTrait, DatabaseBackend, EntityTrait, Set, Statement};

fn unix_to_datetime(seconds: i64) -> chrono::DateTime<chrono::FixedOffset> {
    chrono::DateTime::from_timestamp(seconds, 0)
        .unwrap()
        .fixed_offset()
}

async fn connection_with_workspace(
    fixture: &pioneer_postgres_test::TestDatabase,
    workspace: &str,
) -> SqliteDatabase {
    let writer = fixture.connection();
    writer
        .execute_unprepared("UPDATE workspace SET is_current = false WHERE is_current")
        .await
        .unwrap();
    writer
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "INSERT INTO workspace (id,name,is_active,is_current) VALUES ($1,'Fixture',true,true)",
            [workspace.into()],
        ))
        .await
        .unwrap();
    SqliteDatabase::new(fixture.connect_read_only().await.unwrap(), writer.clone())
}

#[tokio::test]
async fn old_turn_without_permission_profile_reads_as_defaulted_full_access() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = connection_with_workspace(fixture, "ws_old_permission_profile").await;
        let timestamp = 1_700_000_000;
        let thread = Thread {
            context_window: None,
            service_mode: None,
            context_authority: None,
            workspace_id: "ws_old_permission_profile".to_owned(),
            id: "thr_old_permission_profile".to_owned(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        let at = unix_to_datetime(timestamp);
        upsert_thread_with_creator(&store, &thread, &PersistedActorRef::System, at, at)
            .await
            .expect("thread should persist");

        let now = unix_to_datetime(timestamp);
        pioneer_entity::turn::Entity::insert(pioneer_entity::turn::ActiveModel {
            id: Set("turn_old_permission_profile".to_owned()),
            thread_id: Set(thread.id.clone()),
            status: Set("in_progress".to_owned()),
            turn_kind: Set("conversation".to_owned()),
            origin: Set("user".to_owned()),
            error: Set(None),
            prompt_manifest_json: Set("{}".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        })
        .exec(&store)
        .await
        .expect("old-style turn insert should succeed");

        let (_, fetched_turn) =
            get_turn_for_thread(&store, thread.id.as_str(), "turn_old_permission_profile")
                .await
                .expect("must read old turn")
                .expect("turn should exist");
        let permission_profile = &fetched_turn.permission_profile;
        assert_eq!(permission_profile.mode, TurnPermissionMode::FullAccess);
        assert_eq!(
            permission_profile.source,
            TurnPermissionProfileSource::Defaulted
        );
        assert_eq!(
            permission_profile.effective_policy,
            ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow)
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn unknown_permission_profile_columns_read_as_defaulted_full_access() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = connection_with_workspace(fixture, "ws_unknown_permission_profile").await;
        let timestamp = 1_700_000_000;
        let thread = Thread {
            context_window: None,
            service_mode: None,
            context_authority: None,
            workspace_id: "ws_unknown_permission_profile".to_owned(),
            id: "thr_unknown_permission_profile".to_owned(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        let at = unix_to_datetime(timestamp);
        upsert_thread_with_creator(&store, &thread, &PersistedActorRef::System, at, at)
            .await
            .expect("thread should persist");

        let now = unix_to_datetime(timestamp);
        pioneer_entity::turn::Entity::insert(pioneer_entity::turn::ActiveModel {
            id: Set("turn_unknown_permission_profile".to_owned()),
            thread_id: Set(thread.id.clone()),
            status: Set("in_progress".to_owned()),
            turn_kind: Set("conversation".to_owned()),
            origin: Set("user".to_owned()),
            error: Set(None),
            prompt_manifest_json: Set("{}".to_owned()),
            permission_profile_mode: Set(Some("future_mode".to_owned())),
            permission_profile_source: Set(Some("future_source".to_owned())),
            permission_profile_snapshot_json: Set(Some("{}".to_owned())),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        })
        .exec(&store)
        .await
        .expect("future-style turn insert should succeed");

        let (_, fetched_turn) = get_turn_for_thread(
            &store,
            thread.id.as_str(),
            "turn_unknown_permission_profile",
        )
        .await
        .expect("must read turn with unknown permission columns")
        .expect("turn should exist");
        let permission_profile = &fetched_turn.permission_profile;
        assert_eq!(permission_profile.mode, TurnPermissionMode::FullAccess);
        assert_eq!(
            permission_profile.source,
            TurnPermissionProfileSource::Defaulted
        );
        assert_eq!(
            permission_profile.effective_policy,
            ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow)
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn update_turn_prompt_manifest_roundtrips_via_prompt_manifest_json() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = connection_with_workspace(fixture, "ws_000000000000000001").await;
        let timestamp = 1_700_000_000;
        let thread = Thread {
            context_window: None,
            service_mode: None,
            context_authority: None,
            workspace_id: "ws_000000000000000001".to_owned(),
            id: "thr_000000000000000002".to_owned(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            work_owner: Default::default(),
            id: "turn_000000000000000002".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };

        let at = unix_to_datetime(timestamp);
        let creator = PersistedActorRef::System;
        upsert_thread_with_creator(&store, &thread, &creator, at, at).await?;
        let projected = pioneer_crud_conversations::turn_writes::prepare_projected_turn_upsert(
            &store,
            &turn.id,
            &thread.id,
            &turn,
            None,
            None,
            Some(turn.work_owner),
            Some(&creator),
            true,
            at,
            at,
        )
        .await?;
        pioneer_crud_conversations::turn_writes::apply_prepared_turn_upsert(&store, projected)
            .await?;

        let manifest = PromptManifest {
            compiler_version: "0.1.0-test".to_owned(),
            profile: PromptManifestProfile::AssistantFull,
            section_ids: vec![
                "identity_base".to_owned(),
                "assistant_safety".to_owned(),
                "soul_core".to_owned(),
            ],
            fingerprint_stable: "stable".to_owned(),
            fingerprint_dynamic: "dynamic".to_owned(),
            fingerprint_full: "full".to_owned(),
            diagnostics: vec![PromptManifestDiagnostic {
                code: PromptManifestDiagnosticCode::MissingFile,
                message: "bootstrap file `SOUL.md` is missing".to_owned(),
                file: Some("/tmp/SOUL.md".to_owned()),
                section_id: None,
                hook_source: Some(PromptManifestHookSource {
                    hook_id: "test.crud_hook".to_owned(),
                    subscription_id: "test.crud_subscription".to_owned(),
                    phase: PromptManifestHookPhase::TurnPrePromptCompile,
                    contribution_id: None,
                    contribution_hash: Some("sha256:cruddiagnostic".to_owned()),
                }),
            }],
            hook_sources: vec![PromptManifestHookSourceEntry {
                source: PromptManifestHookSource {
                    hook_id: "test.crud_hook".to_owned(),
                    subscription_id: "test.crud_subscription".to_owned(),
                    phase: PromptManifestHookPhase::TurnPrePromptCompile,
                    contribution_id: None,
                    contribution_hash: Some("sha256:crudsource".to_owned()),
                },
                section_id: Some("identity_base".to_owned()),
                contribution_kind: PromptManifestHookContributionKind::PromptSection,
                priority: Some(10),
                source_count: Some(1),
                truncation: PromptManifestHookTruncation::None,
            }],
        };

        let updated = write_turn_prompt_manifest(
            &store,
            thread.id.as_str(),
            turn.id.as_str(),
            &manifest,
            timestamp,
        )
        .await
        .expect("update should succeed");
        assert!(updated, "turn row must be updated");

        let (_workspace_id, roundtrip_turn) =
            get_turn_for_thread(&store, thread.id.as_str(), turn.id.as_str())
                .await
                .expect("turn/get should succeed")
                .expect("turn should exist");
        assert_eq!(roundtrip_turn.prompt_manifest, Some(manifest));
        Ok(())
    })
    .await
    .unwrap();
}
