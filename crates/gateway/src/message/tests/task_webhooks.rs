//! HTTP boundary regression using the ordinary Gateway/TaskService fixtures.
//! This suite is intentionally not executed before external review.
use super::*;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use pioneer_protocol::TaskGetParams;
use std::collections::BTreeSet;
use tower::ServiceExt as _;

#[tokio::test]
async fn webhook_http_admits_distinct_durable_runs_and_create_get_recover_the_same_url() {
    let (processor, sessions, _, _notifications, _, workspace_id) =
        setup_workspace_message_processor().await;
    let processor = Arc::new(processor);
    processor.set_gateway_listen_addr("[::1]:17878".parse().unwrap());
    let mut params = test_task_create_params(
        &workspace_id,
        "thr_webhook_http_parent",
        "turn_webhook_http_parent",
        "Interpret each webhook request",
        3,
    );
    params.trigger.spec = TaskTriggerSpec::External {
        source: "webhook".to_owned(),
        event_type: None,
        filter: None,
    };
    params.lifecycle_policy = None;
    params.delivery_policy = None;
    params.concurrency_policy = Some(pioneer_protocol::TaskConcurrencyPolicy {
        key: None,
        max_parallel_runs: 1,
        on_conflict: pioneer_protocol::TaskConcurrencyConflictPolicy::Queue,
    });
    params
        .agent_spec
        .as_mut()
        .unwrap()
        .prompt
        .output_instructions = Some("Return a concise summary.".to_owned());
    let created = create_task_for_test(&processor, params).await.unwrap();
    assert!(created.run.is_none());
    assert_eq!(created.trigger.id.len(), 21);
    assert!(
        created
            .trigger
            .id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric())
    );
    let mut create_output = serde_json::to_value(&created).unwrap();
    crate::task_tools::enrich_task_webhook_output(
        &processor,
        std::slice::from_ref(&created.trigger),
        &mut create_output,
    )
    .await
    .unwrap();
    let typed_create: pioneer_protocol::TaskCreateResponse =
        serde_json::from_value(create_output.clone()).unwrap();
    assert_eq!(
        typed_create.webhook_scope,
        Some(pioneer_protocol::TaskWebhookScope::Loopback)
    );
    let url = url::Url::parse(create_output["webhookUrl"].as_str().unwrap()).unwrap();
    assert_eq!(create_output["webhookScope"], "loopback");
    assert_eq!(url.host_str(), Some("[::1]"));
    assert_eq!(url.port(), Some(17878));
    let fetched = processor
        .task_runtime
        .service()
        .get_task(TaskGetParams {
            task_id: created.task.id.clone(),
        })
        .await
        .unwrap();
    let mut get_output =
        serde_json::to_value(crate::task_projection::project_task_get(&fetched, false)).unwrap();
    crate::task_tools::enrich_task_webhook_output(&processor, &fetched.triggers, &mut get_output)
        .await
        .unwrap();
    assert_eq!(get_output["webhookUrl"], create_output["webhookUrl"]);
    let typed_get: pioneer_protocol::PublicTaskGetResponse =
        serde_json::from_value(get_output).unwrap();
    assert_eq!(typed_get.webhook_url, typed_create.webhook_url);
    assert_eq!(typed_get.webhook_scope, typed_create.webhook_scope);
    assert_eq!(fetched.triggers[0].id, created.trigger.id);

    let config: pioneer_config::AppConfig =
        toml::from_str(include_str!("../../../../../config/default.toml")).unwrap();
    let identity = Arc::new(
        crate::identity::bootstrap_identity(&processor.crud_store.database_connection())
            .await
            .unwrap()
            .snapshot,
    );
    let signing_key = crate::secrets::AuthKeyMaterial::from_test_bytes(vec![8; 64]);
    let credential_key = crate::secrets::AuthKeyMaterial::from_test_bytes(vec![9; 64]);
    let auth = crate::auth::AuthAdmissionService::new(&config, &signing_key, &identity).unwrap();
    let auth_service = Arc::new(
        crate::auth::GatewayAuthService::new(
            processor.crud_store.database_connection(),
            config.gateway.auth.clone(),
            identity,
            &signing_key,
            &credential_key,
        )
        .unwrap(),
    );
    let state = crate::transport::GatewayHttpState::new(
        config,
        auth,
        auth_service,
        processor.clone(),
        sessions,
    )
    .unwrap();
    state.set_readiness(pioneer_protocol::GatewayReadinessStatus::Operational);
    let app = crate::transport::gateway_router(state);
    let request_path = url.path();
    let post = |payload: &'static str| {
        Request::post(request_path)
            .header("content-type", "text/plain; charset=utf-8")
            .body(Body::from(payload))
            .unwrap()
    };
    let (first, second) = tokio::join!(
        app.clone().oneshot(post("первый запрос")),
        app.clone().oneshot(post("second request"))
    );
    let mut run_ids = Vec::new();
    for response in [first.unwrap(), second.unwrap()] {
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(response.headers()["referrer-policy"], "no-referrer");
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1024).await.unwrap()).unwrap();
        run_ids.push(body["runId"].as_str().unwrap().to_owned());
    }
    assert_ne!(run_ids[0], run_ids[1]);
    let fetched = processor
        .task_runtime
        .service()
        .get_task(TaskGetParams {
            task_id: created.task.id.clone(),
        })
        .await
        .unwrap();
    assert_eq!(fetched.runs.len(), 2);
    let inputs = fetched
        .runs
        .iter()
        .map(|run| run.trigger_input.as_ref().unwrap().body.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(inputs, BTreeSet::from(["первый запрос", "second request"]));
    assert!(fetched.runs.iter().all(|run| {
        run.trigger_input.as_ref().unwrap().content_type.as_deref()
            == Some("text/plain; charset=utf-8")
    }));
    assert_eq!(
        fetched
            .runs
            .iter()
            .map(|run| run.run_number)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([1, 2])
    );
    let wrong_path = format!("/webhooks/tasks/{}/{}", created.trigger.id, "0".repeat(64));
    let wrong = app
        .clone()
        .oneshot(
            Request::post(wrong_path)
                .body(Body::from("private rejected body"))
                .unwrap(),
        )
        .await
        .unwrap();
    let absent_path = format!("/webhooks/tasks/{}/{}", "A".repeat(21), "0".repeat(64));
    let absent = app
        .clone()
        .oneshot(Request::post(absent_path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(wrong.status(), StatusCode::NOT_FOUND);
    assert_eq!(absent.status(), wrong.status());
    let oversized = app
        .clone()
        .oneshot(
            Request::post(request_path)
                .body(Body::from(vec![b'x'; 1024 * 1024 + 1]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let fetched = processor
        .task_runtime
        .service()
        .get_task(TaskGetParams {
            task_id: created.task.id.clone(),
        })
        .await
        .unwrap();
    assert_eq!(fetched.runs.len(), 2);
    let accepted_run_ids = fetched
        .runs
        .iter()
        .map(|run| run.id.clone())
        .collect::<Vec<_>>();

    let (body_polled_tx, mut body_polled_rx) = tokio::sync::oneshot::channel();
    let mut body_polled_tx = Some(body_polled_tx);
    let pending = Body::from_stream(futures_util::stream::poll_fn(move |_| {
        if let Some(sender) = body_polled_tx.take() {
            let _ = sender.send(());
        }
        std::task::Poll::<Option<Result<Vec<u8>, std::io::Error>>>::Pending
    }));
    tokio::time::pause();
    let response = app.oneshot(Request::post(request_path).body(pending).unwrap());
    tokio::pin!(response);
    let polling_started_at = std::time::Instant::now();
    loop {
        // Keep the runtime runnable while the real handler authenticates through
        // SQLite. Paused time must not auto-advance to a DB acquisition timeout.
        assert!(
            polling_started_at.elapsed() < std::time::Duration::from_secs(5),
            "the handler never started reading the request body"
        );
        tokio::select! {
            result = &mut response => panic!("request finished before body deadline: {result:?}"),
            polled = &mut body_polled_rx => { polled.unwrap(); break; },
            _ = tokio::task::yield_now() => {},
        }
    }
    tokio::time::advance(std::time::Duration::from_secs(10)).await;
    // This guard only prevents a hang if the production timeout is removed.
    // Its expiration fails the test; it cannot manufacture the checked HTTP 408.
    let response = tokio::time::timeout(std::time::Duration::from_secs(1), &mut response)
        .await
        .expect("production body deadline did not return an HTTP response")
        .unwrap();
    tokio::time::resume();
    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
    let after_timeout = processor
        .task_runtime
        .service()
        .get_task(TaskGetParams {
            task_id: created.task.id,
        })
        .await
        .unwrap();
    assert_eq!(
        after_timeout
            .runs
            .iter()
            .map(|run| run.id.clone())
            .collect::<Vec<_>>(),
        accepted_run_ids
    );
}
