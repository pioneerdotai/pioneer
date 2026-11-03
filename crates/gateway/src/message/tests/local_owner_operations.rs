//! Owner transport and local authority, using the real file runtime/services.
use super::*;
use crate::local_operations::{Action, DeviceOperation};
use pioneer_protocol::{AuthDeviceCreateResponse, REQUEST_ID_LEN, RequestId, generate_id};

#[tokio::test]
async fn local_device_online_admission_and_lost_reply_replay_use_the_owned_runtime() {
    let directory = tempfile::tempdir().unwrap();
    let mut config: pioneer_config::AppConfig =
        toml::from_str(include_str!("../../../../../config/default.toml")).unwrap();
    config.gateway.listen_addr = "127.0.0.1:0".to_owned();
    let database = crate::database::initialize(directory.path(), &config)
        .await
        .unwrap();
    let crud = Arc::new(CrudStore::new(database.clone()));
    let workspace_manager = Arc::new(WorkspaceManager::new(database.clone()));
    let workspace = workspace_manager.ensure_default_workspace().await.unwrap();
    let sessions = Arc::new(SessionManager::new());
    let secrets = Arc::new(GatewaySecrets::open(directory.path()).unwrap());
    let identity = Arc::new(
        crate::identity::bootstrap_identity(&database)
            .await
            .unwrap()
            .snapshot,
    );
    crate::auth::ensure_auth_readiness(&database, &identity)
        .await
        .unwrap();
    let signing = secrets
        .load_or_create_access_jwt_signing_key(config.gateway.auth.secret_size_bytes)
        .unwrap();
    let credential = secrets
        .load_or_create_auth_credential_hmac_key(config.gateway.auth.secret_size_bytes)
        .unwrap();
    let auth = crate::auth::AuthAdmissionService::new(&config, &signing, &identity).unwrap();
    let auth_service = Arc::new(
        crate::auth::GatewayAuthService::new(
            database.clone(),
            config.gateway.auth.clone(),
            identity,
            &signing,
            &credential,
        )
        .unwrap(),
    );
    let processor = Arc::new(
        MessageProcessor::new(
            Arc::new(ThreadManager::new("o4-mini", "openai")),
            test_provider(),
            sessions.clone(),
            workspace_manager,
            crud.clone(),
            secrets,
            test_summary_config(),
            test_tool_loop_config(),
        )
        .with_runtime_home_for_tests(directory.path().to_path_buf()),
    );
    let server =
        crate::transport::spawn_server(config.clone(), auth, auth_service, processor, sessions)
            .await
            .unwrap();
    server.set_readiness(pioneer_protocol::GatewayReadinessStatus::Operational);
    // Busy ownership does not prevent the supported online local command.
    assert!(
        crate::database::initialize_existing_for_operations(directory.path(), &config)
            .await
            .is_err()
    );
    let operation = DeviceOperation::open(&config, directory.path()).unwrap();
    let operation_id = operation.id.clone();
    let first: AuthDeviceCreateResponse = crate::local_operations::try_online(
        &config,
        directory.path(),
        operation.id.clone(),
        Action::CreateDevice,
    )
    .await
    .unwrap()
    .unwrap();
    drop(operation); // response not acknowledged by CLI; persisted operation remains
    let replay = crate::prepare_device_creation(&config, directory.path(), false)
        .await
        .unwrap();
    assert_eq!(&first, replay.response());
    let output =
        replay.deliver(|_| Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe).into()));
    assert!(output.is_err()); // actual delivery boundary refuses acknowledgement
    let output_replay = crate::prepare_device_creation(&config, directory.path(), false)
        .await
        .unwrap();
    assert_eq!(output_replay.response(), &first);
    drop(output_replay);
    let row = pioneer_crud::load_session(&database, &first.session_id)
        .await
        .unwrap()
        .unwrap();
    assert!(row.created_by_session_id.is_none());
    assert_eq!(row.status, "pending");
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let response = client.post(format!("http://{}/local/owner-operation", server.local_addr())).header("pioneer-local-mac", "00").json(&serde_json::json!({"database":"wrong", "id":RequestId::new(generate_id(REQUEST_ID_LEN)).unwrap(), "action":{"operation":"CreateDevice"}})).send().await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(
        pioneer_crud::load_session(&database, &first.session_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "pending"
    );
    // Real inspections remain available while the writer lock is held. Apply
    // operations must reach these running owner services, since acquiring a
    // second offline runtime above already failed.
    let status = crate::operations::secrets_status(&config, directory.path())
        .await
        .unwrap();
    assert!(status.mcp_orphans.available);
    let usage = crate::operations::artifact_storage_usage(&config, directory.path(), &workspace.id)
        .await
        .unwrap();
    assert_eq!(usage.bytes, 0);
    let plan = crate::operations::artifact_gc_dry_run(
        &config,
        directory.path(),
        &workspace.id,
        chrono::Utc::now().timestamp_millis(),
    )
    .await
    .unwrap();
    assert!(plan.orphan_blobs.is_empty());
    let gc = crate::operations::artifact_gc_execute(
        &config,
        directory.path(),
        &workspace.id,
        chrono::Utc::now().timestamp_millis(),
        true,
    )
    .await
    .unwrap();
    assert_eq!(gc.deleted_blobs, 0);
    let gc = crate::operations::secrets_garbage_collection(&config, directory.path(), false)
        .await
        .unwrap();
    assert!(!gc.dry_run);
    assert_eq!(gc.deleted_refs, 0);

    // A VALID capability does not grant browser/proxy ingress. Reuse the exact
    // real request identity/result; rejection must happen before business replay.
    use hmac::Mac as _;
    use sha2::Digest as _;
    let body = serde_json::to_vec(&crate::local_operations::Request {
        database: hex::encode(sha2::Sha256::digest(
            std::fs::canonicalize(
                crate::database::gateway_database_path(directory.path(), &config).unwrap(),
            )
            .unwrap()
            .to_string_lossy()
            .as_bytes(),
        )),
        id: operation_id,
        issued_at_unix: crate::helpers::unix_timestamp_secs().unwrap(),
        action: Action::CreateDevice,
    })
    .unwrap();
    let mut signer =
        <hmac::Hmac<sha2::Sha256> as hmac::KeyInit>::new_from_slice(credential.as_bytes()).unwrap();
    signer.update(b"pioneer.local-owner.request.v1\0");
    signer.update(&body);
    let signature = hex::encode(signer.finalize().into_bytes());
    for (header, value) in [
        ("origin", "http://localhost"),
        ("forwarded", "for=127.0.0.1"),
        ("x-forwarded-for", "127.0.0.1"),
    ] {
        let rejected = client
            .post(format!(
                "http://{}/local/owner-operation",
                server.local_addr()
            ))
            .header("pioneer-local-mac", &signature)
            .header(header, value)
            .body(body.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(rejected.status(), reqwest::StatusCode::UNAUTHORIZED);
    }
    // Cancellation after a real owner response but before delivery keeps the
    // same durable operation; it is not an acknowledgement of CLI output.
    let (sent, received) = tokio::sync::oneshot::channel();
    let cancel_config = config.clone();
    let home = directory.path().to_path_buf();
    let caller = tokio::spawn(async move {
        let pending = crate::prepare_device_creation(&cancel_config, &home, false)
            .await
            .unwrap();
        sent.send(pending.response().clone()).unwrap();
        std::future::pending::<()>().await;
        pending.confirm().unwrap()
    });
    assert_eq!(received.await.unwrap(), first);
    caller.abort();
    let _ = caller.await;
    let still_pending = crate::prepare_device_creation(&config, directory.path(), false)
        .await
        .unwrap();
    assert_eq!(still_pending.response(), &first);
    drop(still_pending);
    server.shutdown().await.unwrap();
    drop(crud);
    database.close().await.unwrap();
    let recovered = crate::create_device(&config, directory.path())
        .await
        .unwrap();
    assert_eq!(recovered, first);
    let offline = crate::create_device(&config, directory.path())
        .await
        .unwrap();
    assert_ne!(
        offline.device_id, first.device_id,
        "confirmed ordinary creation starts another operation"
    );
    let another = crate::create_device(&config, directory.path())
        .await
        .unwrap();
    assert_ne!(
        another.device_id, offline.device_id,
        "two ordinary successful calls create two devices"
    );
    // An unconfirmed expired replay is not resurrected. Explicit new intent can
    // abandon it; confirming that result restores normal ordinary creation.
    let expired = crate::prepare_device_creation(&config, directory.path(), false)
        .await
        .unwrap();
    let expired_response = expired.response().clone();
    drop(expired);
    let reopened = crate::database::initialize_existing_for_operations(directory.path(), &config)
        .await
        .unwrap()
        .unwrap();
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
    pioneer_entity::auth_session::Entity::update_many()
        .col_expr(
            pioneer_entity::auth_session::Column::Status,
            sea_orm::sea_query::Expr::value("expired"),
        )
        .filter(pioneer_entity::auth_session::Column::Id.eq(expired_response.session_id.as_str()))
        .exec(&reopened)
        .await
        .unwrap();
    reopened.close().await.unwrap();
    assert!(
        crate::prepare_device_creation(&config, directory.path(), false)
            .await
            .is_err()
    );
    let replacement = crate::create_device_new_operation(&config, directory.path())
        .await
        .unwrap();
    assert_ne!(replacement.device_id, expired_response.device_id);
    let next = crate::create_device(&config, directory.path())
        .await
        .unwrap();
    assert_ne!(next.device_id, replacement.device_id);
    // A real server published A's authenticated endpoint above and then shut
    // down. Its retained rendezvous must not block a new selected existing B.
    let mut other_config = config.clone();
    other_config.gateway.database.file_name = "other-selected.db".into();
    let other_db = crate::database::initialize(directory.path(), &other_config)
        .await
        .unwrap();
    other_db.close().await.unwrap();
    let other = crate::create_device(&other_config, directory.path())
        .await
        .unwrap();
    assert_ne!(other.device_id, next.device_id);
    let other_db =
        crate::database::initialize_existing_for_operations(directory.path(), &other_config)
            .await
            .unwrap()
            .unwrap();
    assert!(
        pioneer_crud::load_session(&other_db, &other.session_id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        pioneer_crud::load_session(&other_db, &next.session_id)
            .await
            .unwrap()
            .is_none()
    );
    other_db.close().await.unwrap();
}

#[tokio::test]
async fn confirmed_device_creation_switches_existing_database_but_unknown_result_remains_bound() {
    let directory = tempfile::tempdir().unwrap();
    let mut first: pioneer_config::AppConfig =
        toml::from_str(include_str!("../../../../../config/default.toml")).unwrap();
    first.gateway.database.file_name = "receipt-a.db".into();
    let mut second = first.clone();
    second.gateway.database.file_name = "receipt-b.db".into();
    for config in [&first, &second] {
        let db = crate::database::initialize(directory.path(), config)
            .await
            .unwrap();
        db.close().await.unwrap();
    }
    let a = crate::prepare_device_creation(&first, directory.path(), false)
        .await
        .unwrap();
    let a_response = a.response().clone();
    a.deliver(|_| Ok(())).unwrap();
    let b = crate::prepare_device_creation(&second, directory.path(), false)
        .await
        .unwrap();
    let b_response = b.response().clone();
    assert_ne!(b_response.device_id, a_response.device_id);
    assert_ne!(b_response.session_id, a_response.session_id);
    drop(b); // result is durable but not delivered; preserve replay identity
    for start_new in [false, true] {
        assert!(
            crate::prepare_device_creation(&first, directory.path(), start_new)
                .await
                .is_err(),
            "unconfirmed B must not be silently abandoned on A"
        );
    }
    let replay = crate::prepare_device_creation(&second, directory.path(), false)
        .await
        .unwrap();
    assert_eq!(replay.response(), &b_response);
    replay.deliver(|_| Ok(())).unwrap();
    for (config, own, foreign) in [
        (&first, &a_response, &b_response),
        (&second, &b_response, &a_response),
    ] {
        let db = crate::database::initialize_existing_for_operations(directory.path(), config)
            .await
            .unwrap()
            .unwrap();
        let session = pioneer_crud::load_session(&db, &own.session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            session.status, "pending",
            "new identity must have useful durable creation"
        );
        assert!(
            pioneer_crud::load_session(&db, &foreign.session_id)
                .await
                .unwrap()
                .is_none()
        );
        db.close().await.unwrap();
    }
    // Confirmation of B also allows an ordinary new operation back on A.
    let back = crate::create_device(&first, directory.path())
        .await
        .unwrap();
    assert_ne!(back.device_id, a_response.device_id);
    assert_ne!(back.device_id, b_response.device_id);
}
