use pioneer_crud_access as domain;
use pioneer_crud_access::membership::{
    NewThreadMembership, NewWorkspaceMembership, PrivateThreadParticipantMutation,
    find_thread_membership, insert_thread_membership, insert_workspace_membership,
};
use pioneer_crud_access::thread_participants::{CompletionOrder, ParticipantRepository};
use pioneer_entity::{gateway_principal, task_inbox_activation};
use pioneer_protocol::{PersistedActorRef, PrincipalId};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ActiveModelTrait, DatabaseTransaction, EntityTrait, Set, TransactionTrait};

#[path = "support/membership.rs"]
mod fixture;
use fixture::membership_fixture;

struct FixtureCompletionOrder;

impl CompletionOrder for FixtureCompletionOrder {
    async fn current<'a>(&'a self, transaction: &'a DatabaseTransaction) -> anyhow::Result<i64> {
        Ok(task_inbox_activation::Entity::find_by_id(1_i64)
            .one(transaction)
            .await?
            .expect("fixture inbox activation")
            .completion_sequence)
    }
}

#[tokio::test]
async fn participant_service_is_idempotent_scoped_and_preserves_creator_membership() {
    pioneer_postgres_test::with_database(async |test_database| {
        let fixture = membership_fixture(test_database.connection()).await;
        let reader = test_database.connect_read_only().await?;
        let db = SqliteDatabase::new(reader, fixture.database.clone());
        let store = ParticipantRepository::new(&db);
        let grant_order = FixtureCompletionOrder;
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
                grant_order.current(&transaction).await?,
            )
            .await
            .expect("grant participant workspace");
        }
        transaction.commit().await.expect("commit workspace grants");

        let thread_id = fixture.private_thread_id.clone();
        let tx = fixture.database.begin().await?;
        let mut private: pioneer_entity::thread::ActiveModel =
            pioneer_entity::thread::Entity::find_by_id(&thread_id)
                .one(&tx)
                .await?
                .expect("fixture private thread")
                .into();
        private.created_by_actor_id = Set(Some(fixture.member_id.to_string()));
        private.update(&tx).await?;
        insert_thread_membership(
            &tx,
            &NewThreadMembership {
                gateway_id: fixture.gateway_id.clone(),
                principal_id: fixture.member_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                thread_id: thread_id.clone(),
                added_by: PersistedActorRef::Principal(fixture.member_id.clone()),
                now,
            },
            grant_order.current(&tx).await?,
        )
        .await?;
        tx.commit().await?;

        let first = store
            .add_private_thread_participant(
                &fixture.gateway_id,
                fixture.red_workspace_id.as_str(),
                thread_id.as_str(),
                Some(&fixture.member_id),
                &participant_id,
                PersistedActorRef::Principal(fixture.member_id.clone()),
                &grant_order,
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
                &grant_order,
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
                &grant_order,
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
