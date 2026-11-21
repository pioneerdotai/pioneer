pub use pioneer_crud_access::membership::*;

use anyhow::Result;
use pioneer_entity::{thread_membership, workspace_membership};
use sea_orm::DatabaseTransaction;

pub async fn insert_workspace_membership(
    db: &DatabaseTransaction,
    input: &NewWorkspaceMembership,
) -> Result<workspace_membership::Model> {
    let sequence = super::task_completion_inbox::current_completion_sequence(db).await?;
    pioneer_crud_access::membership::insert_workspace_membership(db, input, sequence).await
}

pub async fn insert_thread_membership(
    db: &DatabaseTransaction,
    input: &NewThreadMembership,
) -> Result<thread_membership::Model> {
    let sequence = super::task_completion_inbox::current_completion_sequence(db).await?;
    pioneer_crud_access::membership::insert_thread_membership(db, input, sequence).await
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use pioneer_crud_access::test_support::membership_fixture;
    use pioneer_entity::{gateway_principal, thread, turn_event, workspace};
    use pioneer_protocol::{
        AUTH_DOMAIN_ID_LEN, GatewayId, PRINCIPAL_ID_LEN, PersistedActorRef, PrincipalId,
        SandboxMode, Thread, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus,
        ThreadVisibility, Turn, TurnPermissionAuditEvent, TurnPermissionAuditEventKind, TurnStatus,
        generate_id,
    };
    use pioneer_protocol::{PrincipalKind, PrincipalStatus, RoleKey};
    use sea_orm::{ActiveModelTrait, DatabaseBackend, Set, Statement, TransactionTrait};
    use sea_orm::{
        ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
    };

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

    #[tokio::test]
    async fn participant_service_is_idempotent_scoped_and_preserves_creator_membership() {
        pioneer_postgres_test::with_database(async |test_database| {
            let fixture = membership_fixture(test_database.connection()).await;
            let store = crate::CrudStore::new(fixture.database.clone());
            let participant_id = PrincipalId::new("B00000000000000000002").expect("participant id");
            let now = chrono::Utc::now().fixed_offset();
            gateway_principal::ActiveModel {
                id: Set(participant_id.to_string()),
                gateway_id: Set(fixture.gateway_id.to_string()),
                kind: Set("user".to_owned()),
                role_key: Set(Some("member".to_owned())),
                status: Set("active".to_owned()),
                display_name: Set("Participant service".to_owned()),
                nickname: Set("participant_service".to_owned()),
                nickname_key: Set("participant_service".to_owned()),
                created_at: Set(now),
                updated_at: Set(now),
                removed_at: Set(None),
                authorization_guard: Set(1),
                ..Default::default()
            }
            .insert(&fixture.database)
            .await
            .expect("create participant");
            let transaction = fixture
                .database
                .begin()
                .await
                .expect("begin workspace grants");
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
                .expect("grant participant workspace");
            }
            transaction.commit().await.expect("commit workspace grants");

            let thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
            let new_thread = Thread {
                context_window: None,
                service_mode: None,
                context_authority: None,
                id: thread_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                name: None,
                preview: String::new(),
                preview_author: None,
                mode: ThreadMode::Chat,
                model: "test".to_owned(),
                model_provider: "test".to_owned(),
                reasoning_effort: None,
                created_at: now.timestamp(),
                updated_at: now.timestamp(),
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
                .expect("create participant test thread");

            let first = store
                .add_private_thread_participant(
                    &fixture.gateway_id,
                    fixture.red_workspace_id.as_str(),
                    thread_id.as_str(),
                    Some(&fixture.member_id),
                    &participant_id,
                    PersistedActorRef::Principal(fixture.member_id.clone()),
                )
                .await
                .expect("first participant add")
                .expect("authorized participant add");
            let PrivateThreadParticipantMutation::Applied {
                changed,
                participant_ids: _,
            } = first
            else {
                panic!("eligible participant add must be applied");
            };
            assert!(changed);
            let duplicate = store
                .add_private_thread_participant(
                    &fixture.gateway_id,
                    fixture.red_workspace_id.as_str(),
                    thread_id.as_str(),
                    Some(&fixture.member_id),
                    &participant_id,
                    PersistedActorRef::Principal(fixture.member_id.clone()),
                )
                .await
                .expect("duplicate participant add")
                .expect("authorized duplicate add");
            let PrivateThreadParticipantMutation::Applied {
                changed,
                participant_ids,
            } = duplicate
            else {
                panic!("eligible duplicate participant add must be applied idempotently");
            };
            assert!(!changed);
            assert_eq!(participant_ids.len(), 2);

            let mut participant: gateway_principal::ActiveModel =
                gateway_principal::Entity::find_by_id(participant_id.to_string())
                    .one(&fixture.database)
                    .await
                    .expect("query participant")
                    .expect("participant exists")
                    .into();
            participant.status = Set("suspended".to_owned());
            participant
                .update(&fixture.database)
                .await
                .expect("suspend existing participant");
            let suspended_participants = store
                .list_private_thread_participant_ids(
                    &fixture.gateway_id,
                    fixture.red_workspace_id.as_str(),
                    thread_id.as_str(),
                    Some(&fixture.member_id),
                )
                .await
                .expect("suspended participant list")
                .expect("authorized suspended participant list");
            let mut expected_suspended_participants =
                vec![fixture.member_id.clone(), participant_id.clone()];
            expected_suspended_participants.sort();
            assert_eq!(
                suspended_participants, expected_suspended_participants,
                "suspend preserves current membership rows and directory visibility"
            );

            let member_creator_removal = store
                .remove_private_thread_participant(
                    &fixture.gateway_id,
                    fixture.red_workspace_id.as_str(),
                    thread_id.as_str(),
                    Some(&fixture.member_id),
                    &fixture.member_id,
                )
                .await
                .expect("Member creator removal must be classified")
                .expect("Member creator manages this private thread");
            assert_eq!(
                member_creator_removal,
                PrivateThreadParticipantMutation::MandatoryCreator
            );
            assert!(
                find_thread_membership(&fixture.database, &thread_id, &fixture.member_id)
                    .await
                    .expect("query creator membership")
                    .is_some()
            );
            let superuser_creator_removal = store
                .remove_private_thread_participant(
                    &fixture.gateway_id,
                    fixture.red_workspace_id.as_str(),
                    thread_id.as_str(),
                    None,
                    &fixture.member_id,
                )
                .await
                .expect("Superuser creator removal must be classified")
                .expect("Superuser can manage this private thread");
            assert_eq!(
                superuser_creator_removal,
                PrivateThreadParticipantMutation::MandatoryCreator,
                "normal Superuser participant mutation must also preserve the creator membership"
            );
            assert!(
                find_thread_membership(&fixture.database, &thread_id, &fixture.member_id)
                    .await
                    .expect("query creator membership after Superuser attempt")
                    .is_some()
            );

            let removed = store
                .remove_private_thread_participant(
                    &fixture.gateway_id,
                    fixture.red_workspace_id.as_str(),
                    thread_id.as_str(),
                    Some(&fixture.member_id),
                    &participant_id,
                )
                .await
                .expect("suspended participant remove")
                .expect("authorized participant remove");
            let PrivateThreadParticipantMutation::Applied {
                changed,
                participant_ids,
            } = removed
            else {
                panic!("persisted suspended participant must remain removable");
            };
            assert!(changed);
            assert_eq!(participant_ids, vec![fixture.member_id.clone()]);

            let mut participant: gateway_principal::ActiveModel =
                gateway_principal::Entity::find_by_id(participant_id.to_string())
                    .one(&fixture.database)
                    .await
                    .expect("query participant")
                    .expect("participant exists")
                    .into();
            participant.status = Set("suspended".to_owned());
            participant
                .update(&fixture.database)
                .await
                .expect("suspend participant");
            let suspended_add = store
                .add_private_thread_participant(
                    &fixture.gateway_id,
                    fixture.red_workspace_id.as_str(),
                    thread_id.as_str(),
                    Some(&fixture.member_id),
                    &participant_id,
                    PersistedActorRef::Principal(fixture.member_id.clone()),
                )
                .await
                .expect("suspended target rejection must be classified")
                .expect("creator remains authorized");
            assert_eq!(
                suspended_add,
                PrivateThreadParticipantMutation::TargetUnavailable
            );
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn member_thread_collection_filters_private_workspace_and_internal_rows_in_sql() {
        pioneer_postgres_test::with_database(async |test_database| {
            let mut fixture = membership_fixture(test_database.connection()).await;
            let member_b_id =
                PrincipalId::new(generate_id(PRINCIPAL_ID_LEN)).expect("second member id");
            let now = chrono::Utc::now().fixed_offset();
            gateway_principal::ActiveModel {
                id: Set(member_b_id.to_string()),
                gateway_id: Set(fixture.gateway_id.to_string()),
                kind: Set("user".to_owned()),
                role_key: Set(Some("member".to_owned())),
                status: Set("active".to_owned()),
                display_name: Set("Member B".to_owned()),
                nickname: Set("member_b_matrix".to_owned()),
                nickname_key: Set("member_b_matrix".to_owned()),
                created_at: Set(now),
                updated_at: Set(now),
                removed_at: Set(None),
                authorization_guard: Set(1),
                ..Default::default()
            }
            .insert(&fixture.database)
            .await
            .expect("create second Member");

            let transaction = fixture.database.begin().await.expect("begin grants");
            for (principal_id, workspace_id) in [
                (&fixture.member_id, &fixture.red_workspace_id),
                (&fixture.member_id, &fixture.blue_workspace_id),
                (&member_b_id, &fixture.red_workspace_id),
            ] {
                insert_workspace_membership(
                    &transaction,
                    &NewWorkspaceMembership {
                        gateway_id: fixture.gateway_id.clone(),
                        principal_id: (*principal_id).clone(),
                        workspace_id: (*workspace_id).clone(),
                        granted_by: PersistedActorRef::Principal(fixture.superuser_id.clone()),
                        now,
                    },
                )
                .await
                .expect("grant overlapping workspace matrix");
            }
            insert_thread_membership(
                &transaction,
                &NewThreadMembership {
                    gateway_id: fixture.gateway_id.clone(),
                    principal_id: fixture.member_id.clone(),
                    workspace_id: fixture.red_workspace_id.clone(),
                    thread_id: fixture.private_thread_id.clone(),
                    added_by: PersistedActorRef::Principal(fixture.superuser_id.clone()),
                    now,
                },
            )
            .await
            .expect("grant private thread");
            transaction.commit().await.expect("commit grants");

            let workspace_thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
            let internal_thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
            let peer_private_thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
            let blue_workspace_thread_id = generate_id(AUTH_DOMAIN_ID_LEN);
            for (thread_id, workspace_id, access_class, origin_kind, creator_id) in [
                (
                    workspace_thread_id.as_str(),
                    fixture.red_workspace_id.as_str(),
                    "workspace",
                    "user",
                    fixture.superuser_id.to_string(),
                ),
                (
                    internal_thread_id.as_str(),
                    fixture.red_workspace_id.as_str(),
                    "internal",
                    "task_run",
                    fixture.superuser_id.to_string(),
                ),
                (
                    peer_private_thread_id.as_str(),
                    fixture.red_workspace_id.as_str(),
                    "private",
                    "user",
                    member_b_id.to_string(),
                ),
                (
                    blue_workspace_thread_id.as_str(),
                    fixture.blue_workspace_id.as_str(),
                    "workspace",
                    "user",
                    fixture.member_id.to_string(),
                ),
            ] {
                thread::ActiveModel {
                    workspace_unread_started_at: sea_orm::NotSet,
                    id: Set(thread_id.to_owned()),
                    workspace_id: Set(workspace_id.to_owned()),
                    name: Set(None),
                    preview: Set(String::new()),
                    preview_author_json: Set(None),
                    mode: Set("chat".to_owned()),
                    model: Set("test".to_owned()),
                    model_provider: Set("test".to_owned()),
                    status: Set("idle".to_owned()),
                    origin_kind: Set(origin_kind.to_owned()),
                    // Deliberately visible to exercise access_class fail-closed filtering.
                    sidebar_visibility: Set("visible".to_owned()),
                    access_class: Set(access_class.to_owned()),
                    agent_nickname: Set(None),
                    agent_role: Set(None),
                    summary: Set(None),
                    summary_turn_count: Set(None),
                    created_at: Set(now),
                    updated_at: Set(now),
                    created_by_actor_id: Set(Some(creator_id)),
                    created_by_actor_kind: Set(Some("principal".to_owned())),
                    ..Default::default()
                }
                .insert(&fixture.database)
                .await
                .expect("insert collection fixture thread");
            }

            let transaction = fixture
                .database
                .begin()
                .await
                .expect("begin peer private grant");
            insert_thread_membership(
                &transaction,
                &NewThreadMembership {
                    gateway_id: fixture.gateway_id.clone(),
                    principal_id: member_b_id.clone(),
                    workspace_id: fixture.red_workspace_id.clone(),
                    thread_id: peer_private_thread_id.clone(),
                    added_by: PersistedActorRef::Principal(member_b_id.clone()),
                    now,
                },
            )
            .await
            .expect("grant peer private thread");
            transaction
                .commit()
                .await
                .expect("commit peer private grant");

            let member_a_red = list_accessible_threads_for_principal(
                &fixture.database,
                &fixture.member_id,
                fixture.red_workspace_id.as_str(),
                100,
            )
            .await
            .expect("list Member A red threads")
            .into_iter()
            .map(|thread| thread.id)
            .collect::<BTreeSet<_>>();
            assert_eq!(
                member_a_red,
                BTreeSet::from([
                    fixture.private_thread_id.clone(),
                    workspace_thread_id.clone()
                ])
            );
            assert!(!member_a_red.contains(&internal_thread_id));
            assert!(!member_a_red.contains(&peer_private_thread_id));
            assert_eq!(
                find_accessible_thread_for_principal(
                    &fixture.database,
                    &fixture.member_id,
                    fixture.red_workspace_id.as_str(),
                    workspace_thread_id.as_str(),
                )
                .await
                .expect("resolve accessible workspace thread")
                .map(|thread| thread.id),
                Some(workspace_thread_id.clone())
            );
            assert!(
                find_accessible_thread_for_principal(
                    &fixture.database,
                    &fixture.member_id,
                    fixture.red_workspace_id.as_str(),
                    peer_private_thread_id.as_str(),
                )
                .await
                .expect("hide peer private thread")
                .is_none()
            );
            assert!(
                find_accessible_thread_for_principal(
                    &fixture.database,
                    &fixture.member_id,
                    fixture.blue_workspace_id.as_str(),
                    workspace_thread_id.as_str(),
                )
                .await
                .expect("reject exact parent mismatch")
                .is_none()
            );

            let member_b_red = list_accessible_threads_for_principal(
                &fixture.database,
                &member_b_id,
                fixture.red_workspace_id.as_str(),
                100,
            )
            .await
            .expect("list Member B red threads")
            .into_iter()
            .map(|thread| thread.id)
            .collect::<BTreeSet<_>>();
            assert_eq!(
                member_b_red,
                BTreeSet::from([peer_private_thread_id.clone(), workspace_thread_id.clone()])
            );
            assert!(!member_b_red.contains(&fixture.private_thread_id));
            assert!(!member_b_red.contains(&internal_thread_id));

            let member_a_blue = list_accessible_threads_for_principal(
                &fixture.database,
                &fixture.member_id,
                fixture.blue_workspace_id.as_str(),
                100,
            )
            .await
            .expect("list Member A blue threads")
            .into_iter()
            .map(|thread| thread.id)
            .collect::<BTreeSet<_>>();
            assert_eq!(
                member_a_blue,
                BTreeSet::from([blue_workspace_thread_id.clone()])
            );

            let member_b_blue = list_accessible_threads_for_principal(
                &fixture.database,
                &member_b_id,
                fixture.blue_workspace_id.as_str(),
                100,
            )
            .await
            .expect("list ungranted Member B blue threads")
            .into_iter()
            .map(|thread| thread.id)
            .collect::<BTreeSet<_>>();
            assert!(member_b_blue.is_empty());

            // Exercise the CrudStore attachment path with the same ACL matrix.
            // Turns on excluded rows must never enter the selected-ID batches.
            for id in [
                &fixture.private_thread_id,
                &workspace_thread_id,
                &internal_thread_id,
                &peer_private_thread_id,
                &blue_workspace_thread_id,
            ] {
                fixture
                    .database
                    .execute_raw(Statement::from_sql_and_values(
                        DatabaseBackend::Postgres,
                        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) \
                     VALUES ($1,$2,'in_progress','task_run','scheduled_task',$3,$4)",
                        vec![
                            generate_id(AUTH_DOMAIN_ID_LEN).into(),
                            id.clone().into(),
                            now.into(),
                            now.into(),
                        ],
                    ))
                    .await
                    .unwrap();
            }
            let statements = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Statement>::new()));
            fixture.database.set_metric_callback({
                let statements = statements.clone();
                move |info| statements.lock().unwrap().push(info.statement.clone())
            });
            let store = crate::CrudStore::new(fixture.database.clone());
            for (principal, workspace, expected) in [
                (&fixture.member_id, &fixture.red_workspace_id, &member_a_red),
                (&member_b_id, &fixture.red_workspace_id, &member_b_red),
                (
                    &fixture.member_id,
                    &fixture.blue_workspace_id,
                    &member_a_blue,
                ),
                (&member_b_id, &fixture.blue_workspace_id, &member_b_blue),
            ] {
                statements.lock().unwrap().clear();
                let listed = store
                    .list_accessible_threads_for_principal(principal, workspace, 100)
                    .await
                    .unwrap();
                assert_eq!(
                    listed
                        .iter()
                        .map(|thread| thread.id.clone())
                        .collect::<BTreeSet<_>>(),
                    *expected
                );
                crate::tests::assert_thread_turn_snapshot_statements(
                    &statements.lock().unwrap(),
                    &listed,
                );
                for thread in listed {
                    assert_eq!(thread.turns.len(), 1);
                    assert_eq!(
                        thread.turns[0].turn_kind,
                        pioneer_protocol::TurnKind::TaskRun
                    );
                    assert!(thread.reasoning_effort.is_none());
                }
            }

            // 229 public rows plus the two original authorized rows = 231. Equal
            // updated_at values also exercise the membership selector's ID ASC tie.
            for index in 0..229 {
                let id = format!("B{index:020}");
                fixture.database.execute_raw(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,access_class,created_at,updated_at) \
                     VALUES ($1,$2,'','chat','test','test','idle','workspace',$3,$4)",
                    vec![id.clone().into(), fixture.red_workspace_id.clone().into(), now.into(),
                         (now + chrono::Duration::seconds(1)).into()],
                )).await.unwrap();
                fixture
                    .database
                    .execute_raw(Statement::from_sql_and_values(
                        DatabaseBackend::Postgres,
                        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) \
                     VALUES ($1,$2,'in_progress','task_run','scheduled_task',$3,$4)",
                        vec![
                            format!("C{index:020}").into(),
                            id.into(),
                            now.into(),
                            now.into(),
                        ],
                    ))
                    .await
                    .unwrap();
            }
            for (limit, turn_queries) in [(0, 0), (128, 2), (129, 4), (231, 4)] {
                let expected = list_accessible_threads_for_principal(
                    &fixture.database,
                    &fixture.member_id,
                    &fixture.red_workspace_id,
                    limit,
                )
                .await
                .unwrap()
                .into_iter()
                .map(|model| model.id)
                .collect::<Vec<_>>();
                statements.lock().unwrap().clear();
                let listed = store
                    .list_accessible_threads_for_principal(
                        &fixture.member_id,
                        &fixture.red_workspace_id,
                        limit,
                    )
                    .await
                    .unwrap();
                assert_eq!(listed.len(), limit as usize);
                assert_eq!(
                    listed
                        .iter()
                        .map(|thread| thread.id.clone())
                        .collect::<Vec<_>>(),
                    expected
                );
                let recorded = statements.lock().unwrap().clone();
                assert_eq!(recorded.len(), 1 + turn_queries);
                crate::tests::assert_thread_turn_snapshot_statements(&recorded, &listed);
                assert!(
                    listed
                        .iter()
                        .all(|thread| thread.workspace_id == fixture.red_workspace_id)
                );
                assert!(listed.iter().all(
                    |thread| thread.id != peer_private_thread_id && thread.id != internal_thread_id
                ));
                if limit == 231 {
                    assert!(
                        listed
                            .iter()
                            .any(|thread| thread.id == fixture.private_thread_id)
                    );
                    assert!(listed.iter().any(|thread| thread.id == workspace_thread_id));
                }
            }
            Ok(())
        }).await.unwrap();
    }
}
