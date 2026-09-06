//! Opt-in contract test against the installed Codex app-server. Uses an isolated
//! home and synthetic history; never calls a model or reads account credentials.
use anyhow::{Context, Result};
use pioneer_cli_agent_runtime::codex::{
    CodexAccountProbeConfig, CodexAppServerClient, CodexGenerationOverlayIdentity,
    CodexJsonlRpcClient, CodexThreadStartParams, cleanup_codex_generation_overlay,
    codex_generation_app_server_process_config, recover_codex_stale_rollout_path,
};
use pioneer_cli_agent_runtime::process::{SensitiveEnvironment, spawn_cli_agent_process};
use serde_json::json;
use std::time::Duration;
use tokio::io::BufReader;

#[tokio::test]
#[ignore = "requires PIONEER_TEST_CODEX executable and sqlite3; no model requests"]
async fn paginated_resume_survives_removed_generation_with_real_codex() -> Result<()> {
    let root = tempfile::tempdir()?;
    let shared = root.path().join("shared");
    let managed = root.path().join("managed");
    let budget = Duration::from_secs(30);
    let config = CodexAccountProbeConfig {
        executable: std::env::var("PIONEER_TEST_CODEX")?,
        home_path: shared.to_string_lossy().into_owned(),
        shadow_home_path: None,
        cwd: Some(root.path().to_path_buf()),
        home_dir: Some(root.path().to_path_buf()),
        env: SensitiveEnvironment::default(),
        initialize_timeout: budget,
        request_timeout: budget,
        shutdown_grace: Duration::from_secs(2),
        stderr_ring_lines: 32,
    };
    let identity = |generation| {
        CodexGenerationOverlayIdentity::new("workspace", "codex", "thread", "boot", generation)
    };
    let (spawn, first) =
        codex_generation_app_server_process_config(&config, &managed, identity(1)?)?;
    let thread_id = "01900000-0000-7000-8000-000000000001";
    let relative =
        "2026/09/05/rollout-2026-09-05T00-00-00-01900000-0000-7000-8000-000000000001.jsonl";
    let selected = first.effective_home_path.join("sessions").join(relative);
    std::fs::create_dir_all(selected.parent().unwrap())?;
    let history = [
        json!({"type":"session_meta","payload":{
            "id":thread_id,"timestamp":"2026-09-05T00:00:00Z",
            "cwd":root.path(),"originator":"pioneer-test","cli_version":"0.0.0",
            "source":"cli","model_provider":"openai","history_mode":"paginated"
        }}),
        json!({"type":"response_item","payload":{
            "type":"message","role":"user","content":[{"type":"input_text","text":"continuity sentinel"}]
        }}),
    ];
    let mut serialized = String::new();
    for (ordinal, mut line) in history.into_iter().enumerate() {
        line["timestamp"] = json!("2026-09-05T00:00:00Z");
        line["ordinal"] = json!(ordinal);
        serialized.push_str(&serde_json::to_string(&line)?);
        serialized.push('\n');
    }
    std::fs::write(&selected, serialized)?;
    let params = || CodexThreadStartParams {
        cwd: root.path().to_string_lossy().into_owned(),
        approval_policy: "never".to_owned(),
        ephemeral: false,
        sandbox: Some("read-only".to_owned()),
        permissions: None,
        model: Some("gpt-5.6-sol".to_owned()),
        service_tier: None,
    };
    let mut process = spawn_cli_agent_process(&spawn)?;
    let (reader, writer) = process.take_stdio()?;
    let client =
        CodexAppServerClient::new(CodexJsonlRpcClient::new(BufReader::new(reader), writer));
    client.initialize(budget).await?;
    client
        .thread_resume_at_path(thread_id, params(), Some(selected.clone()), budget)
        .await
        .context("seed paginated thread through the original generation")?;
    let initial = client.thread_read_metadata(thread_id, budget).await?;
    let durable = std::fs::canonicalize(shared.join("sessions").join(relative))?;
    assert_eq!(initial.rollout_path.as_deref(), Some(durable.as_path()));
    client.rpc().shutdown().await?;
    process.terminate_with_grace(config.shutdown_grace).await?;
    // Seed the old writer's persisted lexical CODEX_HOME path in this disposable
    // Codex index. Resume of an imported fixture already canonicalizes its path,
    // whereas the production thread/start writer persisted a generation alias.
    // No Pioneer database or account home is involved in this fixture setup.
    let seed = std::process::Command::new("sqlite3")
        .arg(shared.join("state_5.sqlite"))
        .arg(format!(
            "UPDATE threads SET rollout_path='{}' WHERE id='{thread_id}' AND history_mode='paginated'; SELECT changes();",
            selected.to_string_lossy().replace('\'', "''"),
        ))
        .output()?;
    assert!(
        seed.status.success(),
        "{}",
        String::from_utf8_lossy(&seed.stderr)
    );
    assert_eq!(String::from_utf8(seed.stdout)?.trim(), "1");
    cleanup_codex_generation_overlay(&first)?;
    assert!(!selected.exists());

    let (spawn, second) =
        codex_generation_app_server_process_config(&config, &managed, identity(2)?)?;
    let mut process = spawn_cli_agent_process(&spawn)?;
    let (reader, writer) = process.take_stdio()?;
    let client =
        CodexAppServerClient::new(CodexJsonlRpcClient::new(BufReader::new(reader), writer));
    client.initialize(budget).await?;
    let persisted = client.thread_read_metadata(thread_id, budget).await?;
    assert_eq!(persisted.rollout_path.as_deref(), Some(selected.as_path()));
    let error = client
        .thread_resume_at_path(thread_id, params(), Some(durable.clone()), budget)
        .await
        .expect_err("unrepaired paginated index must reproduce the production failure");
    assert!(error.to_string().contains("stale path"), "{error}");

    let resolved = recover_codex_stale_rollout_path(&second, &selected)?;
    assert_eq!(resolved, Some(durable.clone()));
    let resumed = client
        .thread_resume_at_path(thread_id, params(), resolved, budget)
        .await
        .context("resume paginated thread after restoring its selected alias")?;
    assert_eq!(resumed.native_thread_id, thread_id);
    assert_eq!(
        client
            .thread_read_metadata(thread_id, budget)
            .await?
            .rollout_path,
        Some(durable.clone())
    );
    client.rpc().shutdown().await?;
    process.terminate_with_grace(config.shutdown_grace).await?;
    cleanup_codex_generation_overlay(&second)?;
    let (spawn, third) =
        codex_generation_app_server_process_config(&config, &managed, identity(3)?)?;
    let mut process = spawn_cli_agent_process(&spawn)?;
    let (reader, writer) = process.take_stdio()?;
    let client =
        CodexAppServerClient::new(CodexJsonlRpcClient::new(BufReader::new(reader), writer));
    client.initialize(budget).await?;
    assert_eq!(
        client
            .thread_read_metadata(thread_id, budget)
            .await?
            .rollout_path,
        Some(durable.clone()),
        "the resumed writer must persist the durable path across restart"
    );
    let resolved = recover_codex_stale_rollout_path(&third, &durable)?;
    assert_eq!(
        client
            .thread_resume_at_path(thread_id, params(), resolved, budget)
            .await?
            .native_thread_id,
        thread_id
    );
    assert!(
        !second.effective_home_path.exists(),
        "a stable resume needs no retired alias repair"
    );
    assert!(std::fs::read_to_string(&durable)?.contains("continuity sentinel"));
    client.rpc().shutdown().await?;
    process.terminate_with_grace(config.shutdown_grace).await?;
    cleanup_codex_generation_overlay(&third)?;
    Ok(())
}
