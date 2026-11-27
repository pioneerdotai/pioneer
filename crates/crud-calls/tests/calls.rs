use pioneer_calls::Call;
use pioneer_crud_access as domain;
use pioneer_crud_access::membership::{
    NewThreadMembership, NewWorkspaceMembership, insert_thread_membership,
    insert_workspace_membership,
};
use pioneer_crud_calls::{CallRepository, Scope};
use pioneer_entity::{task_inbox_activation, thread};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_protocol::{PersistedActorRef, ThreadCallEndReason};
use sea_orm::{ActiveModelTrait, EntityTrait, Set, TransactionTrait};

#[path = "../../crud-access/tests/support/membership.rs"]
mod fixtures;

#[tokio::test]
async fn database_owns_active_slot_and_scopes_roster_and_history() {
    pioneer_postgres_test::with_database(async |fixture| {
        let people = fixtures::membership_fixture(fixture.connection()).await;
        let alice = people.superuser_id.to_string();
        let bob = people.member_id.to_string();
        let gateway = people.gateway_id.to_string();
        let thread_id = people.private_thread_id;
        let workspace_id = people.red_workspace_id;
        let scope = Scope {
            gateway: &gateway,
            workspace: &workspace_id,
            thread: &thread_id,
        };
        let writer =
            PostgresDatabase::new(fixture.connect_read_only().await?, people.database.clone());
        let concurrent =
            PostgresDatabase::new(fixture.connect_read_only().await?, fixture.connect().await?);
        let a = CallRepository(&writer);
        let b = CallRepository(&concurrent);

        assert_eq!(a.classify(&scope).await?, vec![alice.clone()]);
        assert!(!a.contains(&scope, &bob).await?);
        let tx = people.database.begin().await?;
        let sequence = task_inbox_activation::Entity::find_by_id(1_i64)
            .one(&tx)
            .await?
            .expect("inbox activation")
            .completion_sequence;
        let operator = PersistedActorRef::Principal(people.superuser_id.clone());
        let now = chrono::Utc::now().fixed_offset();
        insert_workspace_membership(
            &tx,
            &NewWorkspaceMembership {
                gateway_id: people.gateway_id.clone(),
                principal_id: people.member_id.clone(),
                workspace_id: workspace_id.clone(),
                granted_by: operator.clone(),
                now,
            },
            sequence,
        )
        .await?;
        insert_thread_membership(
            &tx,
            &NewThreadMembership {
                gateway_id: people.gateway_id.clone(),
                principal_id: people.member_id,
                workspace_id: workspace_id.clone(),
                thread_id: thread_id.clone(),
                added_by: operator,
                now,
            },
            sequence,
        )
        .await?;
        tx.commit().await?;
        assert!(a.contains(&scope, &bob).await?);
        assert_eq!(a.classify(&scope).await?.len(), 2);

        let first = Call::new(
            gateway.clone(),
            workspace_id.clone(),
            thread_id.clone(),
            "alpha".into(),
            alice.clone(),
            Some(bob.clone()),
            now.timestamp(),
        );
        let second = Call::new(
            gateway.clone(),
            workspace_id.clone(),
            thread_id.clone(),
            "beta".into(),
            bob.clone(),
            Some(alice.clone()),
            now.timestamp(),
        );
        let (saved_first, saved_second) = tokio::join!(
            a.save(&first, None, Some(&alice)),
            b.save(&second, None, Some(&bob)),
        );
        assert_ne!(saved_first?, saved_second?);
        assert_eq!(
            a.active_for_principal(&alice).await?.len() + a.active_for_principal(&bob).await?.len(),
            1
        );

        let mut call = a.active(&thread_id).await?.expect("race winner");
        let revision = call.snapshot.revision;
        assert!(call.expire_ring(now.timestamp() + pioneer_calls::RING_SECONDS));
        assert!(a.save(&call, Some(revision), None).await?);
        assert!(!b.save(&call, Some(revision), None).await?);
        assert!(a.active(&thread_id).await?.is_none());
        let (history, next) = a.history(&scope, None, &bob).await?;
        assert_eq!(history.len(), 1);
        assert_eq!(
            history[0].snapshot.end_reason,
            Some(ThreadCallEndReason::Missed)
        );
        assert!(next.is_none());
        assert!(a.history(&scope, None, "outsider").await?.0.is_empty());

        let replacement = Call::new(
            gateway.clone(),
            workspace_id.clone(),
            thread_id.clone(),
            "replacement".into(),
            alice.clone(),
            Some(bob.clone()),
            now.timestamp(),
        );
        assert!(a.save(&replacement, None, Some(&alice)).await?);
        assert_eq!(
            a.active(&thread_id).await?.unwrap().snapshot.call_id,
            "replacement"
        );
        assert_eq!(
            a.get(&call.snapshot.call_id)
                .await?
                .unwrap()
                .snapshot
                .end_reason,
            Some(ThreadCallEndReason::Missed),
        );

        let mut record: thread::ActiveModel = thread::Entity::find_by_id(&thread_id)
            .one(&people.database)
            .await?
            .expect("test thread")
            .into();
        record.access_class = Set("workspace".into());
        record.update(&people.database).await?;
        assert!(a.contains(&scope, &bob).await?);
        let other_scope = Scope {
            workspace: &people.blue_workspace_id,
            ..scope
        };
        assert!(!a.contains(&other_scope, &bob).await?);
        Ok(())
    })
    .await
    .unwrap();
}
