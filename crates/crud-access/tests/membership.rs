use anyhow::Result;
use pioneer_crud_access as domain;
use pioneer_crud_access::membership::*;
#[path = "support/membership.rs"]
mod support;
use pioneer_entity::{gateway_principal, thread_membership, workspace, workspace_membership};
use pioneer_protocol::{
    PRINCIPAL_ID_LEN, PersistedActorRef, PrincipalId, PrincipalKind, generate_id,
};
use sea_orm::{
    ActiveModelTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, Set, Statement,
    TransactionTrait,
};
use std::collections::BTreeSet;
use std::time::Duration;
use support::membership_fixture;

async fn insert_workspace_membership(
    db: &DatabaseTransaction,
    input: &NewWorkspaceMembership,
) -> Result<workspace_membership::Model> {
    pioneer_crud_access::membership::insert_workspace_membership(db, input, 0).await
}

async fn insert_thread_membership(
    db: &DatabaseTransaction,
    input: &NewThreadMembership,
) -> Result<thread_membership::Model> {
    pioneer_crud_access::membership::insert_thread_membership(db, input, 0).await
}

#[tokio::test]
async fn workspace_removal_and_private_grant_preserve_parent_membership_in_both_orders()
-> Result<()> {
    for remove_first in [true, false] {
        pioneer_postgres_test::with_database(async |database| {
            let fixture = membership_fixture(database.connection()).await;
            let now = chrono::Utc::now().fixed_offset();
            let parent = NewWorkspaceMembership {
                gateway_id: fixture.gateway_id.clone(),
                principal_id: fixture.member_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                granted_by: PersistedActorRef::System,
                now,
            };
            let child = NewThreadMembership {
                gateway_id: fixture.gateway_id,
                principal_id: fixture.member_id,
                workspace_id: fixture.red_workspace_id,
                thread_id: fixture.private_thread_id,
                added_by: PersistedActorRef::System,
                now,
            };
            let seed = fixture.database.begin().await?;
            insert_workspace_membership(&seed, &parent).await?;
            seed.commit().await?;

            let first = fixture.database.begin().await?;
            let independent = database.connect().await?;
            let second = independent.begin().await?;
            let pid: i32 = second
                .query_one_raw(Statement::from_string(
                    sea_orm::DbBackend::Postgres,
                    "SELECT pg_backend_pid() AS pid",
                ))
                .await?
                .expect("PostgreSQL PID")
                .try_get("", "pid")?;
            let second_child = child.clone();
            if remove_first {
                let deleted = delete_workspace_membership(
                    &first,
                    &child.gateway_id,
                    &child.principal_id,
                    &child.workspace_id,
                )
                .await?;
                assert!(deleted.changed);
                let other =
                    tokio::spawn(
                        async move { insert_thread_membership(&second, &second_child).await },
                    );
                wait_for_lock(&fixture.database, pid).await?;
                first.commit().await?;
                let error = other
                    .await?
                    .expect_err("a deleted parent cannot grant thread access");
                assert!(error.to_string().contains("without workspace"));
            } else {
                insert_thread_membership(&first, &child).await?;
                let other = tokio::spawn(async move {
                    let deleted = delete_workspace_membership(
                        &second,
                        &second_child.gateway_id,
                        &second_child.principal_id,
                        &second_child.workspace_id,
                    )
                    .await?;
                    second.commit().await?;
                    Ok::<_, anyhow::Error>(deleted)
                });
                wait_for_lock(&fixture.database, pid).await?;
                first.commit().await?;
                assert_eq!(
                    other.await??.removed_private_thread_ids,
                    [child.thread_id.clone()]
                );
            }
            assert!(
                find_workspace_membership(
                    &fixture.database,
                    &child.principal_id,
                    &child.workspace_id
                )
                .await?
                .is_none()
            );
            assert!(
                find_thread_membership(&fixture.database, &child.thread_id, &child.principal_id)
                    .await?
                    .is_none()
            );
            independent.close().await?;
            Ok(())
        })
        .await?;
    }
    Ok(())
}

async fn wait_for_lock(database: &impl ConnectionTrait, pid: i32) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let blocked: bool = database
                .query_one_raw(Statement::from_sql_and_values(
                    sea_orm::DbBackend::Postgres,
                    "SELECT cardinality(pg_blocking_pids($1)) > 0 AS waiting",
                    [pid.into()],
                ))
                .await?
                .expect("PostgreSQL lock state")
                .try_get("", "waiting")?;
            if blocked {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await?
}

#[tokio::test]
async fn composite_membership_inserts_are_idempotent_and_exact() {
    pioneer_postgres_test::with_database(async |test_database| {
        let fixture = membership_fixture(test_database.connection()).await;
        let transaction = fixture.database.begin().await.expect("begin");
        let now = chrono::Utc::now().fixed_offset();
        let workspace_grant = NewWorkspaceMembership {
            gateway_id: fixture.gateway_id.clone(),
            principal_id: fixture.member_id.clone(),
            workspace_id: fixture.red_workspace_id.clone(),
            granted_by: PersistedActorRef::Principal(fixture.superuser_id.clone()),
            now,
        };
        let first = insert_workspace_membership(&transaction, &workspace_grant)
            .await
            .expect("first workspace grant");
        let duplicate = insert_workspace_membership(&transaction, &workspace_grant)
            .await
            .expect("idempotent workspace grant");
        assert_eq!(first, duplicate);

        let thread_grant = NewThreadMembership {
            gateway_id: fixture.gateway_id.clone(),
            principal_id: fixture.member_id.clone(),
            workspace_id: fixture.red_workspace_id.clone(),
            thread_id: fixture.private_thread_id.clone(),
            added_by: PersistedActorRef::Principal(fixture.superuser_id.clone()),
            now,
        };
        let first = insert_thread_membership(&transaction, &thread_grant)
            .await
            .expect("first thread grant");
        let duplicate = insert_thread_membership(&transaction, &thread_grant)
            .await
            .expect("idempotent thread grant");
        assert_eq!(first, duplicate);
        assert_eq!(
            list_workspace_memberships_for_principal(&transaction, &fixture.member_id)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            list_thread_memberships_for_principal(&transaction, &fixture.member_id)
                .await
                .unwrap()
                .len(),
            1
        );
        transaction.commit().await.expect("commit");
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn active_workspace_queries_apply_membership_in_sql() {
    pioneer_postgres_test::with_database(async |test_database| {
        let fixture = membership_fixture(test_database.connection()).await;
        let transaction = fixture.database.begin().await.expect("begin");
        insert_workspace_membership(
            &transaction,
            &NewWorkspaceMembership {
                gateway_id: fixture.gateway_id.clone(),
                principal_id: fixture.member_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                granted_by: PersistedActorRef::System,
                now: chrono::Utc::now().fixed_offset(),
            },
        )
        .await
        .expect("grant red workspace");
        transaction.commit().await.expect("commit");

        let visible = list_active_workspaces_for_principal(&fixture.database, &fixture.member_id)
            .await
            .expect("list exact workspace grants");
        assert_eq!(
            visible
                .iter()
                .map(|workspace| workspace.id.as_str())
                .collect::<Vec<_>>(),
            vec![fixture.red_workspace_id.as_str()]
        );
        assert!(
            find_active_workspace_for_principal(
                &fixture.database,
                &fixture.member_id,
                fixture.red_workspace_id.as_str(),
            )
            .await
            .expect("resolve granted workspace")
            .is_some()
        );
        assert!(
            find_active_workspace_for_principal(
                &fixture.database,
                &fixture.member_id,
                fixture.blue_workspace_id.as_str(),
            )
            .await
            .expect("resolve ungranted workspace")
            .is_none()
        );

        let mut red: workspace::ActiveModel =
            workspace::Entity::find_by_id(fixture.red_workspace_id.clone())
                .one(&fixture.database)
                .await
                .expect("query red workspace")
                .expect("red workspace exists")
                .into();
        red.is_active = Set(false);
        red.update(&fixture.database)
            .await
            .expect("deactivate red workspace");
        assert!(
            list_active_workspaces_for_principal(&fixture.database, &fixture.member_id)
                .await
                .expect("list after deactivation")
                .is_empty(),
            "inactive memberships must be filtered by the authoritative SQL query"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn complete_thread_acl_query_accepts_the_unbounded_caller_sentinel() {
    pioneer_postgres_test::with_database(async |test_database| {
        let fixture = membership_fixture(test_database.connection()).await;
        let transaction = fixture.database.begin().await.expect("begin");
        let now = chrono::Utc::now().fixed_offset();
        insert_workspace_membership(
            &transaction,
            &NewWorkspaceMembership {
                gateway_id: fixture.gateway_id.clone(),
                principal_id: fixture.member_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                granted_by: PersistedActorRef::System,
                now,
            },
        )
        .await
        .expect("grant workspace");
        insert_thread_membership(
            &transaction,
            &NewThreadMembership {
                gateway_id: fixture.gateway_id.clone(),
                principal_id: fixture.member_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                thread_id: fixture.private_thread_id.clone(),
                added_by: PersistedActorRef::System,
                now,
            },
        )
        .await
        .expect("grant private thread");
        transaction.commit().await.expect("commit grants");

        let threads = list_accessible_threads_for_principal(
            &fixture.database,
            &fixture.member_id,
            fixture.red_workspace_id.as_str(),
            u64::MAX,
        )
        .await
        .expect("unbounded authorized thread list must remain valid SQLite");
        assert_eq!(
            threads
                .into_iter()
                .map(|thread| thread.id)
                .collect::<Vec<_>>(),
            vec![fixture.private_thread_id]
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn thread_membership_rejects_cross_workspace_and_missing_parent_grant() {
    pioneer_postgres_test::with_database(async |test_database| {
        let fixture = membership_fixture(test_database.connection()).await;
        let transaction = fixture.database.begin().await.expect("begin");
        let now = chrono::Utc::now().fixed_offset();
        let input = NewThreadMembership {
            gateway_id: fixture.gateway_id.clone(),
            principal_id: fixture.member_id.clone(),
            workspace_id: fixture.blue_workspace_id.clone(),
            thread_id: fixture.private_thread_id.clone(),
            added_by: PersistedActorRef::System,
            now,
        };
        assert!(
            insert_thread_membership(&transaction, &input)
                .await
                .is_err()
        );

        let input = NewThreadMembership {
            workspace_id: fixture.red_workspace_id.clone(),
            ..input
        };
        assert!(
            insert_thread_membership(&transaction, &input)
                .await
                .is_err()
        );
        transaction.rollback().await.expect("rollback");
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn directory_acl_precedes_pagination_and_hides_unrelated_profiles() {
    pioneer_postgres_test::with_database(async |test_database| {
        let fixture = membership_fixture(test_database.connection()).await;
        let now = chrono::Utc::now().fixed_offset();
        let shared_id = PrincipalId::new(generate_id(PRINCIPAL_ID_LEN)).expect("shared id");
        let unrelated_id = PrincipalId::new(generate_id(PRINCIPAL_ID_LEN)).expect("unrelated id");
        let removed_id = PrincipalId::new(generate_id(PRINCIPAL_ID_LEN)).expect("removed id");
        for (id, nickname) in [
            (&shared_id, "zzz_shared"),
            (&unrelated_id, "aaa_unrelated"),
            (&removed_id, "removed_author"),
        ] {
            gateway_principal::ActiveModel {
                id: Set(id.to_string()),
                gateway_id: Set(fixture.gateway_id.to_string()),
                kind: Set("user".to_owned()),
                role_key: Set(Some(if id == &shared_id {
                    "synthetic_executor".to_owned()
                } else {
                    "member".to_owned()
                })),
                status: Set("active".to_owned()),
                display_name: Set(nickname.to_owned()),
                nickname: Set(nickname.to_owned()),
                nickname_key: Set(nickname.to_owned()),
                created_at: Set(now),
                updated_at: Set(now),
                removed_at: Set(None),
                authorization_guard: Set(1),
                ..Default::default()
            }
            .insert(&fixture.database)
            .await
            .expect("insert directory principal");
        }

        let transaction = fixture
            .database
            .begin()
            .await
            .expect("begin directory grants");
        for (principal_id, workspace_id) in [
            (&fixture.member_id, &fixture.red_workspace_id),
            (&shared_id, &fixture.red_workspace_id),
            (&removed_id, &fixture.red_workspace_id),
            (&unrelated_id, &fixture.blue_workspace_id),
        ] {
            insert_workspace_membership(
                &transaction,
                &NewWorkspaceMembership {
                    gateway_id: fixture.gateway_id.clone(),
                    principal_id: (*principal_id).clone(),
                    workspace_id: (*workspace_id).clone(),
                    granted_by: PersistedActorRef::System,
                    now,
                },
            )
            .await
            .expect("insert directory membership");
        }
        transaction.commit().await.expect("commit directory grants");

        for (principal_id, status, removed_at) in [
            (&shared_id, "suspended", None),
            (&removed_id, "removed", Some(now)),
        ] {
            let mut principal: gateway_principal::ActiveModel =
                gateway_principal::Entity::find_by_id(principal_id.to_string())
                    .one(&fixture.database)
                    .await
                    .expect("query directory principal")
                    .expect("directory principal exists")
                    .into();
            principal.status = Set(status.to_owned());
            principal.removed_at = Set(removed_at);
            principal
                .update(&fixture.database)
                .await
                .expect("update directory principal status");
        }

        let page = list_member_directory_page(
            &fixture.database,
            &fixture.gateway_id,
            &fixture.member_id,
            PrincipalKind::User,
            None,
            1,
        )
        .await
        .expect("list one authorized directory row");
        assert_eq!(page.principals.len(), 1);
        assert_ne!(
            page.principals[0].id, unrelated_id,
            "an unrelated row sorted first must not consume the authorized page"
        );

        let shared_profile = find_shared_workspace_principal_for_principal(
            &fixture.database,
            &fixture.gateway_id,
            &fixture.member_id,
            &shared_id,
        )
        .await
        .expect("resolve suspended shared profile")
        .expect("synthetic registered role remains directory-visible");
        assert_eq!(
            shared_profile.role_key.as_deref(),
            Some("synthetic_executor")
        );
        assert!(
            find_shared_workspace_principal_for_principal(
                &fixture.database,
                &fixture.gateway_id,
                &fixture.member_id,
                &unrelated_id,
            )
            .await
            .expect("hide unrelated profile")
            .is_none()
        );
        assert!(
            find_shared_workspace_principal_for_principal(
                &fixture.database,
                &fixture.gateway_id,
                &fixture.member_id,
                &removed_id,
            )
            .await
            .expect("removed profile is not an interactive directory entry")
            .is_none()
        );

        let superuser_directory = list_member_directory_page(
            &fixture.database,
            &fixture.gateway_id,
            &fixture.superuser_id,
            PrincipalKind::Superuser,
            None,
            100,
        )
        .await
        .expect("list complete Superuser directory")
        .principals;
        let superuser_ids = superuser_directory
            .into_iter()
            .map(|principal| principal.id)
            .collect::<BTreeSet<_>>();
        assert!(superuser_ids.contains(&fixture.superuser_id));
        assert!(superuser_ids.contains(&shared_id));
        assert!(superuser_ids.contains(&unrelated_id));
        assert!(
            superuser_ids.contains(&removed_id),
            "removed actor identity remains available to historical Superuser inspection"
        );

        let mut red_workspace: workspace::ActiveModel =
            workspace::Entity::find_by_id(fixture.red_workspace_id.clone())
                .one(&fixture.database)
                .await
                .expect("query shared workspace")
                .expect("shared workspace exists")
                .into();
        red_workspace.is_active = Set(false);
        red_workspace
            .update(&fixture.database)
            .await
            .expect("deactivate shared workspace");
        assert!(
            find_shared_workspace_principal_for_principal(
                &fixture.database,
                &fixture.gateway_id,
                &fixture.member_id,
                &shared_id,
            )
            .await
            .expect("inactive workspace must not establish directory scope")
            .is_none()
        );
        let member_ids = list_member_directory_page(
            &fixture.database,
            &fixture.gateway_id,
            &fixture.member_id,
            PrincipalKind::User,
            None,
            100,
        )
        .await
        .expect("list directory after workspace deactivation")
        .principals
        .into_iter()
        .map(|principal| principal.id)
        .collect::<BTreeSet<_>>();
        assert!(!member_ids.contains(&shared_id));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn deleting_workspace_membership_removes_dependent_thread_grants() {
    pioneer_postgres_test::with_database(async |test_database| {
        let fixture = membership_fixture(test_database.connection()).await;
        let transaction = fixture.database.begin().await.expect("begin");
        let now = chrono::Utc::now().fixed_offset();
        insert_workspace_membership(
            &transaction,
            &NewWorkspaceMembership {
                gateway_id: fixture.gateway_id.clone(),
                principal_id: fixture.member_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                granted_by: PersistedActorRef::System,
                now,
            },
        )
        .await
        .unwrap();
        insert_thread_membership(
            &transaction,
            &NewThreadMembership {
                gateway_id: fixture.gateway_id.clone(),
                principal_id: fixture.member_id.clone(),
                workspace_id: fixture.red_workspace_id.clone(),
                thread_id: fixture.private_thread_id.clone(),
                added_by: PersistedActorRef::System,
                now,
            },
        )
        .await
        .unwrap();
        let deletion = delete_workspace_membership(
            &transaction,
            &fixture.gateway_id,
            &fixture.member_id,
            fixture.red_workspace_id.as_str(),
        )
        .await
        .unwrap();
        assert!(deletion.changed);
        assert_eq!(
            deletion.removed_private_thread_ids,
            vec![fixture.private_thread_id.clone()]
        );
        assert!(
            find_thread_membership(
                &transaction,
                fixture.private_thread_id.as_str(),
                &fixture.member_id
            )
            .await
            .unwrap()
            .is_none()
        );
        transaction.commit().await.expect("commit");
        Ok(())
    })
    .await
    .unwrap();
}

#[test]
fn persisted_thread_access_class_conversion_is_strict() {
    for value in [
        PersistedThreadAccessClass::Private,
        PersistedThreadAccessClass::Workspace,
        PersistedThreadAccessClass::Internal,
    ] {
        assert_eq!(
            persisted_thread_access_class_from_db(persisted_thread_access_class_to_db(value))
                .unwrap(),
            value
        );
    }
    assert!(persisted_thread_access_class_from_db("public").is_err());
}
