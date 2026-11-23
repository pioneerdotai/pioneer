use super::*;
use crate::test_support::{EXECUTION_ID, seed_agent_execution};
use pioneer_postgres_test::{postgres_test, scoped_database};
use sea_orm::{DbBackend, Statement, TransactionTrait};
use std::time::Duration;

#[postgres_test]
async fn idempotent_insert_observes_the_committed_owner_and_releases_a_rolled_back_claim() {
    let fixture = scoped_database().await.unwrap();
    let db = fixture.connection();
    seed_agent_execution(db).await;
    let competing_pool = fixture.connect_with_limit(1).await.unwrap();

    for committed in [true, false] {
        let key = format!("race-{committed}");
        let input = AgentActionInput {
            id: format!("first-{key}"),
            execution_id: EXECUTION_ID.to_owned(),
            action_kind: "send_message".to_owned(),
            idempotency_key: key.clone(),
            request_fingerprint: "first-fingerprint".to_owned(),
            now: chrono::Utc::now().fixed_offset(),
        };
        let competing = AgentActionInput {
            id: format!("second-{key}"),
            request_fingerprint: "second-fingerprint".to_owned(),
            ..input.clone()
        };
        let first = db.begin().await.unwrap();
        let original = insert_agent_action_idempotent(&first, &input)
            .await
            .unwrap();
        assert!(load_agent_action(db, &original.id).await.unwrap().is_none());

        let second = competing_pool.begin().await.unwrap();
        let pid: i32 = second
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT pg_backend_pid() AS pid",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "pid")
            .unwrap();
        let competing_request = competing.clone();
        let contender = tokio::spawn(async move {
            let result = insert_agent_action_idempotent(&second, &competing_request).await;
            second.commit().await.unwrap();
            result
        });
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let waiting: bool = db
                    .query_one_raw(Statement::from_sql_and_values(
                        DbBackend::Postgres,
                        "SELECT cardinality(pg_blocking_pids($1)) > 0 AS waiting",
                        [pid.into()],
                    ))
                    .await
                    .unwrap()
                    .unwrap()
                    .try_get("", "waiting")
                    .unwrap();
                if waiting {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("contender must wait for the unique key lock");

        if committed {
            first.commit().await.unwrap();
            let error = contender.await.unwrap().unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("different immutable action facts")
            );
            assert_eq!(
                insert_agent_action_idempotent(db, &input).await.unwrap(),
                original
            );
            assert!(
                load_agent_action(db, &competing.id)
                    .await
                    .unwrap()
                    .is_none()
            );
        } else {
            first.rollback().await.unwrap();
            let winner = contender.await.unwrap().unwrap();
            assert_eq!(winner.id, competing.id);
            assert!(load_agent_action(db, &input.id).await.unwrap().is_none());
            assert_eq!(
                insert_agent_action_idempotent(db, &competing)
                    .await
                    .unwrap(),
                winner
            );
        }
    }
}

#[test]
fn route_receipt_projects_only_safe_action_provenance() {
    let route = safe_route_provenance_from_receipt(
        r#"{"routeId":"D00000000000000000001","routeGeneration":4,"sourcePolicyGeneration":7,"destinationPolicyGeneration":9,"action":"send_message"}"#,
        "send_message",
    )
    .expect("valid exact route receipt");
    assert_eq!(
        route,
        pioneer_protocol::SafeRouteProvenance::delegated_action(
            pioneer_protocol::AgentRouteAction::SendMessage,
        )
    );
    let encoded = serde_json::to_string(&route).expect("safe projection");
    assert!(!encoded.contains("D00000000000000000001"));
    assert!(!encoded.contains("generation"));
}

#[test]
fn route_receipt_rejects_action_rebinding_or_invalid_generations() {
    let rebound = safe_route_provenance_from_receipt(
        r#"{"routeId":"D00000000000000000001","routeGeneration":4,"sourcePolicyGeneration":7,"destinationPolicyGeneration":9,"action":"start_agent"}"#,
        "send_message",
    );
    assert!(rebound.is_err());
    let invalid_generation = safe_route_provenance_from_receipt(
        r#"{"routeId":"D00000000000000000001","routeGeneration":0,"sourcePolicyGeneration":7,"destinationPolicyGeneration":9,"action":"send_message"}"#,
        "send_message",
    );
    assert!(invalid_generation.is_err());
}
