use super::{insert_thread_membership, insert_workspace_membership};
use pioneer_crud_access::membership::*;
use pioneer_crud_access::test_support::membership_fixture;
use pioneer_entity::{gateway_principal, thread, turn_event, workspace};
use pioneer_protocol::{
    AUTH_DOMAIN_ID_LEN, GatewayId, PRINCIPAL_ID_LEN, PersistedActorRef, PrincipalId, PrincipalKind,
    PrincipalStatus, RoleKey, SandboxMode, Thread, ThreadMode, ThreadOriginKind,
    ThreadSidebarVisibility, ThreadStatus, ThreadVisibility, Turn, TurnPermissionAuditEvent,
    TurnPermissionAuditEventKind, TurnStatus, generate_id,
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseTransaction,
    EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set, Statement, TransactionTrait,
};
use std::collections::BTreeSet;

#[tokio::test]
async fn member_thread_creation_is_private_and_rolls_back_without_parent_membership() {
    pioneer_postgres_test::with_database(async |test_database| {
        let fixture = membership_fixture(test_database.connection()).await;
        let store = crate::CrudStore::new(fixture.database.clone());
        let thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
        let timestamp = chrono::Utc::now().timestamp();
        let new_thread = Thread {
            context_window: None,
            service_mode: None,
            context_authority: None,
            id: thread_id.clone(),
            workspace_id: fixture.red_workspace_id.clone(),
            name: Some("Atomic private thread".to_owned()),
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Chat,
            model: "test".to_owned(),
            model_provider: "test".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        let creator = PersistedActorRef::Principal(fixture.member_id.clone());

        store
            .create_member_private_thread(
                &new_thread,
                &fixture.gateway_id,
                &fixture.member_id,
                creator.clone(),
            )
            .await
            .expect_err("missing workspace membership must abort the whole transaction");
        assert!(
            thread::Entity::find_by_id(thread_id.clone())
                .one(&fixture.database)
                .await
                .expect("query rolled-back thread")
                .is_none(),
            "failed creator membership must leave no partial thread"
        );

        let transaction = fixture
            .database
            .begin()
            .await
            .expect("begin membership grant");
        insert_workspace_membership(
            &transaction,
            &NewWorkspaceMembership {
                gateway_id: fixture.gateway_id.clone(),
                principal_id: fixture.member_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                granted_by: PersistedActorRef::Principal(fixture.superuser_id.clone()),
                now: chrono::Utc::now().fixed_offset(),
            },
        )
        .await
        .expect("grant parent workspace");
        transaction.commit().await.expect("commit membership grant");

        store
            .create_member_private_thread(
                &new_thread,
                &fixture.gateway_id,
                &fixture.member_id,
                creator,
            )
            .await
            .expect("authorized member thread transaction");
        let persisted = thread::Entity::find_by_id(thread_id.clone())
            .one(&fixture.database)
            .await
            .expect("query committed thread")
            .expect("thread committed");
        assert_eq!(persisted.access_class, "private");
        assert!(
            find_thread_membership(&fixture.database, &thread_id, &fixture.member_id)
                .await
                .expect("query creator membership")
                .is_some(),
            "private creator membership must commit with the thread"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn member_first_turn_creates_thread_membership_and_events_in_one_transaction() {
    pioneer_postgres_test::with_database(async |test_database| {
        let fixture = membership_fixture(test_database.connection()).await;
        let store = crate::CrudStore::new(fixture.database.clone());
        let timestamp = chrono::Utc::now().timestamp();
        let thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
        let turn_id = generate_id(AUTH_DOMAIN_ID_LEN);
        let thread = Thread {
            context_window: None,
            service_mode: None,
            context_authority: None,
            id: thread_id.clone(),
            workspace_id: fixture.red_workspace_id.clone(),
            name: None,
            preview: "first message".to_owned(),
            preview_author: None,
            mode: ThreadMode::Chat,
            model: "test".to_owned(),
            model_provider: "test".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: Some(ThreadVisibility::Private),
            turns: Vec::new(),
        };
        let permission_profile = pioneer_protocol::default_turn_permission_profile_snapshot();
        let turn = Turn {
            work_owner: Default::default(),
            id: turn_id.clone(),
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
            permission_profile: permission_profile.clone(),
        };
        let audit = TurnPermissionAuditEvent {
            workspace_id: thread.workspace_id.clone(),
            thread_id: thread.id.clone(),
            turn_id: turn.id.clone(),
            event_kind: TurnPermissionAuditEventKind::ProfileSelected,
            profile_mode: permission_profile.mode,
            profile_source: permission_profile.source,
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
        let actor = PersistedActorRef::Principal(fixture.member_id.clone());

        store
            .materialize_new_member_turn_start_with_reasoning_effort_and_permission_audit(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                None,
                actor.clone(),
                audit.clone(),
                &fixture.gateway_id,
                &fixture.member_id,
                crate::PersistedThreadAccessClass::Private,
            )
            .await
            .expect_err("missing workspace membership must roll back the initial turn");
        assert!(
            thread::Entity::find_by_id(thread_id.clone())
                .one(&fixture.database)
                .await
                .expect("query rolled-back initial thread")
                .is_none()
        );
        assert!(
            crate::find_thread_membership(&fixture.database, &thread_id, &fixture.member_id)
                .await
                .expect("query rolled-back creator membership")
                .is_none()
        );
        assert!(
            turn_event::Entity::find()
                .filter(turn_event::Column::TurnId.eq(turn_id.clone()))
                .all(&fixture.database)
                .await
                .expect("query rolled-back turn events")
                .is_empty()
        );

        let transaction = fixture
            .database
            .begin()
            .await
            .expect("begin workspace membership grant");
        insert_workspace_membership(
            &transaction,
            &NewWorkspaceMembership {
                gateway_id: fixture.gateway_id.clone(),
                principal_id: fixture.member_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                granted_by: PersistedActorRef::Principal(fixture.superuser_id.clone()),
                now: chrono::Utc::now().fixed_offset(),
            },
        )
        .await
        .expect("grant creator workspace membership");
        transaction
            .commit()
            .await
            .expect("commit workspace membership grant");

        store
            .materialize_new_member_turn_start_with_reasoning_effort_and_permission_audit(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                None,
                actor.clone(),
                audit.clone(),
                &fixture.gateway_id,
                &fixture.member_id,
                crate::PersistedThreadAccessClass::Private,
            )
            .await
            .expect("first turn should atomically materialize the private thread");

        let persisted = thread::Entity::find_by_id(thread_id.clone())
            .one(&fixture.database)
            .await
            .expect("query materialized thread")
            .expect("materialized thread should exist");
        assert_eq!(persisted.access_class, "private");
        assert!(
            crate::find_thread_membership(&fixture.database, &thread_id, &fixture.member_id)
                .await
                .expect("query committed creator membership")
                .is_some()
        );
        assert!(
            store
                .get_turn(&thread_id, &turn_id)
                .await
                .expect("query committed first turn")
                .is_some()
        );
        assert_eq!(
            turn_event::Entity::find()
                .filter(turn_event::Column::TurnId.eq(turn_id))
                .all(&fixture.database)
                .await
                .expect("query committed first-turn events")
                .len(),
            2
        );

        let workspace_thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
        let workspace_turn_id = generate_id(AUTH_DOMAIN_ID_LEN);
        let workspace_thread = Thread {
            context_window: None,
            service_mode: None,
            context_authority: None,
            id: workspace_thread_id.clone(),
            visibility: Some(ThreadVisibility::Workspace),
            ..thread.clone()
        };
        let workspace_turn = Turn {
            id: workspace_turn_id.clone(),
            ..turn
        };
        let workspace_audit = TurnPermissionAuditEvent {
            workspace_id: workspace_thread.workspace_id.clone(),
            thread_id: workspace_thread_id.clone(),
            turn_id: workspace_turn_id.clone(),
            ..audit
        };

        store
            .materialize_new_member_turn_start_with_reasoning_effort_and_permission_audit(
                &workspace_thread,
                SandboxMode::FullAccess,
                &workspace_turn,
                &[],
                None,
                actor,
                workspace_audit,
                &fixture.gateway_id,
                &fixture.member_id,
                crate::PersistedThreadAccessClass::Workspace,
            )
            .await
            .expect("first turn should atomically materialize the workspace-visible thread");

        let persisted = thread::Entity::find_by_id(workspace_thread_id.clone())
            .one(&fixture.database)
            .await
            .expect("query materialized workspace thread")
            .expect("materialized workspace thread should exist");
        assert_eq!(persisted.access_class, "workspace");
        assert!(
            crate::find_thread_membership(
                &fixture.database,
                &workspace_thread_id,
                &fixture.member_id,
            )
            .await
            .expect("query workspace-thread creator membership")
            .is_none(),
            "workspace-visible threads must not persist explicit membership"
        );
        assert!(
            store
                .get_turn(&workspace_thread_id, &workspace_turn_id)
                .await
                .expect("query committed workspace-thread first turn")
                .is_some()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn public_thread_creation_cannot_select_internal_access() {
    pioneer_postgres_test::with_database(async |test_database| {
        let fixture = membership_fixture(test_database.connection()).await;
        let store = crate::CrudStore::new(fixture.database.clone());
        let thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
        let timestamp = chrono::Utc::now().timestamp();
        let new_thread = Thread {
            context_window: None,
            service_mode: None,
            context_authority: None,
            id: thread_id.clone(),
            workspace_id: fixture.red_workspace_id,
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Chat,
            model: "test".to_owned(),
            model_provider: "test".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };

        store
            .create_superuser_thread(
                &new_thread,
                PersistedActorRef::Principal(fixture.superuser_id),
                PersistedThreadAccessClass::Internal,
            )
            .await
            .expect_err("user thread path must reject internal access");
        assert!(
            thread::Entity::find_by_id(thread_id)
                .one(&fixture.database)
                .await
                .expect("query rejected thread")
                .is_none()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn creator_management_preserves_memberships_across_visibility_and_archive_roundtrip() {
    pioneer_postgres_test::with_database(async |test_database| {
        let fixture = membership_fixture(test_database.connection()).await;
        let store = crate::CrudStore::new(fixture.database.clone());
        let participant_id =
            PrincipalId::new(generate_id(PRINCIPAL_ID_LEN)).expect("participant id");
        let now = chrono::Utc::now().fixed_offset();
        gateway_principal::ActiveModel {
            id: Set(participant_id.to_string()),
            gateway_id: Set(fixture.gateway_id.to_string()),
            kind: Set("user".to_owned()),
            role_key: Set(Some("member".to_owned())),
            status: Set("active".to_owned()),
            display_name: Set("Participant".to_owned()),
            nickname: Set("participant".to_owned()),
            nickname_key: Set("participant".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            removed_at: Set(None),
            authorization_guard: Set(1),
            ..Default::default()
        }
        .insert(&fixture.database)
        .await
        .expect("create participant");
        let transaction = fixture.database.begin().await.expect("begin grants");
        for principal_id in [&fixture.member_id, &participant_id] {
            insert_workspace_membership(
                &transaction,
                &NewWorkspaceMembership {
                    gateway_id: fixture.gateway_id.clone(),
                    principal_id: (*principal_id).clone(),
                    workspace_id: fixture.red_workspace_id.clone(),
                    granted_by: PersistedActorRef::Principal(fixture.superuser_id.clone()),
                    now,
                },
            )
            .await
            .expect("grant workspace");
        }
        transaction.commit().await.expect("commit grants");

        let thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
        let timestamp = now.timestamp();
        let new_thread = Thread {
            context_window: None,
            service_mode: None,
            context_authority: None,
            id: thread_id.clone(),
            workspace_id: fixture.red_workspace_id.clone(),
            name: Some("Managed".to_owned()),
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Chat,
            model: "test".to_owned(),
            model_provider: "test".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        store
            .create_member_private_thread(
                &new_thread,
                &fixture.gateway_id,
                &fixture.member_id,
                PersistedActorRef::Principal(fixture.member_id.clone()),
            )
            .await
            .expect("create creator-owned private thread");
        let transaction = fixture
            .database
            .begin()
            .await
            .expect("begin participant grant");
        insert_thread_membership(
            &transaction,
            &NewThreadMembership {
                gateway_id: fixture.gateway_id.clone(),
                principal_id: participant_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                thread_id: thread_id.clone(),
                added_by: PersistedActorRef::Principal(fixture.member_id.clone()),
                now,
            },
        )
        .await
        .expect("add explicit participant");
        transaction
            .commit()
            .await
            .expect("commit participant grant");

        assert_eq!(
            store
                .update_user_thread_management(
                    fixture.red_workspace_id.as_str(),
                    thread_id.as_str(),
                    Some(&participant_id),
                    Some("Not allowed"),
                    None,
                    None,
                )
                .await
                .expect("non-creator update decision"),
            None,
            "participant membership must not transfer creator management rights"
        );
        assert_eq!(
            store
                .update_user_thread_management(
                    fixture.red_workspace_id.as_str(),
                    thread_id.as_str(),
                    Some(&fixture.member_id),
                    Some("Renamed"),
                    Some(PersistedThreadAccessClass::Workspace),
                    Some(true),
                )
                .await
                .expect("creator publish and archive"),
            Some(crate::ThreadManagementChange {
                changed: true,
                access_changed: true
            })
        );
        // Supplying the current visibility alongside another changed field is
        // a data update, not an ACL mutation. Comparison stays in the transaction.
        for (name, changed) in [("Renamed again", true), ("Renamed again", false)] {
            assert_eq!(
                store
                    .update_user_thread_management(
                        fixture.red_workspace_id.as_str(),
                        thread_id.as_str(),
                        Some(&fixture.member_id),
                        Some(name),
                        Some(PersistedThreadAccessClass::Workspace),
                        Some(true),
                    )
                    .await
                    .unwrap(),
                Some(crate::ThreadManagementChange {
                    changed,
                    access_changed: false
                }),
            );
        }
        let published = thread::Entity::find_by_id(thread_id.clone())
            .one(&fixture.database)
            .await
            .expect("query published thread")
            .expect("published thread exists");
        assert_eq!(published.access_class, "workspace");
        assert_eq!(published.status, "closed");
        assert_eq!(published.name.as_deref(), Some("Renamed again"));

        assert_eq!(
            store
                .update_user_thread_management(
                    fixture.red_workspace_id.as_str(),
                    thread_id.as_str(),
                    None,
                    None,
                    Some(PersistedThreadAccessClass::Private),
                    Some(false),
                )
                .await
                .expect("Superuser private restore"),
            Some(crate::ThreadManagementChange {
                changed: true,
                access_changed: true
            })
        );
        let restored = thread::Entity::find_by_id(thread_id.clone())
            .one(&fixture.database)
            .await
            .expect("query restored thread")
            .expect("restored thread exists");
        assert_eq!(restored.access_class, "private");
        assert_eq!(restored.status, "idle");
        for principal_id in [&fixture.member_id, &participant_id] {
            assert!(
                find_thread_membership(&fixture.database, &thread_id, principal_id)
                    .await
                    .expect("query preserved membership")
                    .is_some(),
                "visibility transition must preserve every explicit membership"
            );
        }
        Ok(())
    })
    .await
    .unwrap();
}
