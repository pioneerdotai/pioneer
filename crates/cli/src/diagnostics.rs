//! Local diagnostics commands never initialize a sampler in this CLI process.
use anyhow::{Context, Result, bail, ensure};
use pioneer_observability::diagnostics::{self, CaptureMode, CaptureRequest};
use std::{path::PathBuf, process::Stdio, time::Duration};

pub(crate) fn run(mut args: impl Iterator<Item = String>) -> Result<()> {
    let action = args.next().unwrap_or_else(|| "list".into());
    if action == "launch-memory" {
        return launch_memory(args);
    }
    if action == "validate-symbols" || action == "validate-capture-symbols" {
        let binary = PathBuf::from(args.next().context("binary path required")?);
        let manifest = PathBuf::from(args.next().context("symbol manifest required")?);
        ensure!(args.next().is_none(), "unexpected argument");
        let module = if action == "validate-capture-symbols" {
            use std::io::Read as _;
            let mut bytes = Vec::new();
            std::fs::File::open(&binary)?
                .take(8 * 1024 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            ensure!(bytes.len() <= 8 * 1024 * 1024, "capture manifest overflow");
            let capture: diagnostics::CaptureManifest = serde_json::from_slice(&bytes)?;
            ensure!(capture.schema_version == 1, "unsupported capture schema");
            capture
                .module
                .context("capture did not record a module identity")?
        } else {
            pioneer_observability::symbols::module_identity(&binary)?
        };
        pioneer_observability::symbols::validate_symbols(&module, &manifest)?;
        println!("{}", serde_json::to_string_pretty(&module)?);
        return Ok(());
    }
    let mut target = None;
    let mut seconds = 10;
    let mut frequency_hz = 99;
    let mut child_generation = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--target" => {
                target = Some(
                    args.next()
                        .context("--target requires gateway, desktop, or run ID")?,
                )
            }
            "--seconds" => seconds = args.next().context("--seconds requires number")?.parse()?,
            "--frequency" => {
                ensure!(
                    action != "sql-cpu",
                    "--frequency is native-only and does not apply to sql-cpu"
                );
                frequency_hz = args
                    .next()
                    .context("--frequency requires number")?
                    .parse()?
            }
            "--child" => {
                child_generation = Some(
                    args.next()
                        .context("--child requires registered generation")?
                        .parse()?,
                )
            }
            _ => bail!("unknown diagnostics argument: {arg}"),
        }
    }
    let config = pioneer_config::AppConfig::load()?;
    let home = config.runtime_home_dir()?;
    let scan = diagnostics::targets(&home)?;
    if scan.skipped_descriptors > 0 {
        eprintln!(
            "local diagnostics: {} unreadable/malformed/unsafe descriptors skipped; files preserved",
            scan.skipped_descriptors
        );
    }
    let targets = scan.targets;
    if action == "list" {
        let safe:Vec<_>=targets.iter().map(|t|serde_json::json!({"service":t.service,"run":t.run,"start_identity":t.start_identity})).collect();
        println!("{}", serde_json::to_string_pretty(&safe)?);
        return Ok(());
    }
    let selector = target.context("--target gateway|desktop|RUN_ID is required")?;
    let matches: Vec<_> = targets
        .iter()
        .filter(|t| t.service == selector || t.run.run_id == selector)
        .collect();
    ensure!(
        matches.len() == 1,
        "target must select exactly one live run (found {}); use diagnostics list and --target RUN_ID",
        matches.len()
    );
    let target = matches[0];
    let result = match action.as_str() {
        "snapshot" => serde_json::to_value(diagnostics::inspect_target(target)?)?,
        "bundle" => diagnostics::bundle_target(target)?,
        "cancel" => diagnostics::cancel_target(target)?,
        mode => {
            let mode = capture_mode(mode)?;
            serde_json::to_value(diagnostics::capture_target(
                target,
                CaptureRequest {
                    mode,
                    seconds,
                    frequency_hz,
                    child_generation,
                },
            )?)?
        }
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    if let Some(sql_cpu) = result.get("sql_cpu") {
        eprintln!(
            "local SQL CPU: {}; observed-group aggregation: {}; coverage and independent capture window are in the manifest and sql-cpu.json",
            sql_cpu
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown"),
            match sql_cpu.get("status").and_then(|v| v.as_str()) {
                Some("recorded") => match sql_cpu
                    .pointer("/aggregation/complete")
                    .and_then(|v| v.as_bool())
                {
                    Some(true) => "complete",
                    Some(false) => "INCOMPLETE (inspect aggregation reason and group_overflow)",
                    None => "unknown (legacy schema)",
                },
                _ => "unavailable (table not retained)",
            }
        );
    }
    ensure!(
        result.get("status").and_then(|v| v.as_str()) != Some("failed"),
        "capture_failed; inspect the emitted manifest reasons"
    );
    Ok(())
}
fn capture_mode(action: &str) -> Result<CaptureMode> {
    Ok(match action {
        "cpu" => CaptureMode::Cpu,
        "sql-cpu" => CaptureMode::SqlCpu,
        "allocations" => CaptureMode::Allocations,
        "allocation-history" => CaptureMode::AllocationHistory,
        "live-allocations" => CaptureMode::LiveAllocations,
        "vm" => CaptureMode::Vm,
        _ => bail!(
            "expected list|snapshot|cpu|sql-cpu|allocations|allocation-history|live-allocations|vm|bundle|launch-memory|validate-symbols"
        ),
    })
}

fn launch_memory(mut args: impl Iterator<Item = String>) -> Result<()> {
    ensure!(
        cfg!(target_os = "macos"),
        "memory preparation requires macOS libmalloc stack logging"
    );
    let mut seconds = None;
    let mut log_budget_mib = None;
    loop {
        match args.next().as_deref() {
            Some("--seconds") => {
                ensure!(seconds.is_none(), "duplicate --seconds");
                seconds = Some(args.next().context("duration required")?.parse::<u64>()?);
            }
            Some("--log-budget-mib") => {
                ensure!(log_budget_mib.is_none(), "duplicate --log-budget-mib");
                let value = args
                    .next()
                    .context("--log-budget-mib requires 1..4096")?
                    .parse::<u64>()
                    .context("--log-budget-mib requires 1..4096")?;
                ensure!(
                    (1..=4096).contains(&value),
                    "--log-budget-mib must be 1..4096"
                );
                log_budget_mib = Some(value);
            }
            Some("--") => break,
            _ => bail!(
                "usage: diagnostics launch-memory --seconds 1..300 [--log-budget-mib 1..4096] -- /absolute/executable [args]"
            ),
        }
    }
    let seconds = seconds.context("--seconds is required")?;
    let log_budget_mib = log_budget_mib.unwrap_or(1024);
    ensure!(
        (1..=300).contains(&seconds),
        "launch duration must be 1..300 seconds"
    );
    let executable = PathBuf::from(args.next().context("executable required")?);
    ensure!(
        executable.is_absolute() && executable.is_file(),
        "preparation executable must be an existing absolute path"
    );
    let config = pioneer_config::AppConfig::load()?;
    let home = config.runtime_home_dir()?;
    let settings = home.join(&config.gateway.settings_file_name);
    let consent = || -> Result<bool> {
        use std::io::Read as _;
        let mut bytes = Vec::new();
        match std::fs::File::open(&settings) {
            Ok(file) => {
                file.take(256 * 1024 + 1).read_to_end(&mut bytes)?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(config.gateway.telemetry.enabled);
            }
            Err(e) => return Err(e.into()),
        }
        ensure!(
            bytes.len() <= 256 * 1024,
            "settings file exceeds preparation-read bound"
        );
        let value: toml::Value = toml::from_str(std::str::from_utf8(&bytes)?)?;
        Ok(value
            .get("general")
            .and_then(|v| v.get("telemetry_enabled"))
            .and_then(toml::Value::as_bool)
            .unwrap_or(config.gateway.telemetry.enabled))
    };
    ensure!(
        consent()?,
        "consent_disabled: native memory preparation refused"
    );
    let logs = diagnostics::PreparedMemoryLogs::new(&home, log_budget_mib)?;
    // Both env vars are child-only. OS stack logs use the owned private budget.
    struct OwnedPreparation(std::process::Child);
    impl Drop for OwnedPreparation {
        fn drop(&mut self) {
            if self.0.try_wait().ok().flatten().is_none() {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
    }
    let mut child = OwnedPreparation(
        std::process::Command::new(executable)
            .args(args.collect::<Vec<_>>())
            .env_remove("MallocStackLogging")
            .env("MallocStackLoggingNoCompact", "1")
            .env("MallocStackLoggingDirectory", logs.directory())
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?,
    );
    let pid = child.0.id();
    let start = pioneer_observability::resources::process_start_identity(pid)?;
    println!(
        "{}",
        serde_json::json!({"launched_pid":pid,"preparation":"full_history_since_launch","lifetime_seconds":seconds,"log_budget_mib":log_budget_mib,"scope":"owned launch only; logs private and ephemeral; consent/volume checked each second; budget is not a guarantee of full-duration coverage; use diagnostics list for its live run"})
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let _runtime_observer = pioneer_observability::runtime::register(
        runtime.handle(),
        pioneer_observability::runtime::RuntimeRole::CliHelper,
    );
    let result=_runtime_observer.block_on(&runtime, async {
        let started=std::time::Instant::now();let mut connected_epoch=None;
        loop {
            if let Some(status)=child.0.try_wait()? {
                validate_prepared_exit(status, || {ensure!(consent()?,"consent_revoked_during_preparation");logs.check_volume()})?;
                return Ok(());
            }
            ensure!(consent()?,"consent_revoked_during_preparation");logs.check_volume()?;
            let target=diagnostics::targets(&home)?.targets.into_iter().find(|d|d.run.pid==pid && d.start_identity==start);
            if let Some(target)=target {
                let inspected=diagnostics::inspect_target(&target);
                let snapshot=match reconcile_prepared_inspection(inspected, ||child.0.try_wait(), || {
                    ensure!(consent()?,"consent_revoked_during_preparation");logs.check_volume()
                }).await? {Some(snapshot)=>snapshot,None=>{
                    eprintln!("prepared_child_exit_success; terminal_diagnostics=unavailable; final_consent_epoch=not_observed");
                    return Ok(());
                }};
                connected_epoch=Some(prepared_epoch(connected_epoch,snapshot.consent_generation)?);
            } else if started.elapsed()>=Duration::from_secs(15) {
                // Discovery can lose the descriptor in the same terminal race.
                // Alive/unavailable remains an error; only observed exit reconciles.
                let missing: Result<()> = Err(std::io::Error::new(std::io::ErrorKind::NotConnected,"prepared_target_diagnostics_unavailable").into());
                reconcile_prepared_inspection(missing, ||child.0.try_wait(), || {
                    ensure!(consent()?,"consent_revoked_during_preparation");logs.check_volume()
                }).await?;
                eprintln!("prepared_child_exit_success; terminal_diagnostics=unavailable; final_consent_epoch=not_observed");
                return Ok(());
            }
            if started.elapsed()>=Duration::from_secs(seconds){return Ok(());}
            tokio::select!{_ = tokio::time::sleep(Duration::from_secs(1))=>{}, signal=tokio::signal::ctrl_c()=>{signal?;return Ok(());}}
        }
    });
    drop(child); // kill/reap only this launch before deleting native stack logs
    drop(logs);
    result
}

fn prepared_epoch(connected: Option<u64>, observed: u64) -> Result<u64> {
    ensure!(
        connected.is_none_or(|epoch| epoch == observed),
        "consent_epoch_changed_during_preparation"
    );
    Ok(observed)
}

// A diagnostic endpoint can close during normal owned-child shutdown, after
// the loop's try_wait and before/during inspect. Only a transport teardown
// error plus an actually observed successful exit may reconcile that race.
fn validate_prepared_exit(
    status: std::process::ExitStatus,
    validate: impl FnOnce() -> Result<()>,
) -> Result<()> {
    validate()?;
    ensure!(
        status.success(),
        "prepared application exited unsuccessfully"
    );
    Ok(())
}
async fn reconcile_prepared_inspection<T>(
    inspected: Result<T>,
    mut observe_exit: impl FnMut() -> std::io::Result<Option<std::process::ExitStatus>>,
    validate: impl Fn() -> Result<()>,
) -> Result<Option<T>> {
    let error = match inspected {
        Ok(snapshot) => return Ok(Some(snapshot)),
        Err(error) => error,
    };
    let teardown = error.downcast_ref::<std::io::Error>().is_some_and(|e| {
        matches!(
            e.kind(),
            std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::NotConnected
                | std::io::ErrorKind::UnexpectedEof
        )
    });
    if !teardown {
        return Err(error);
    } // consent/epoch/protocol/storage errors stay errors
    validate()?;
    if let Some(status) = observe_exit()? {
        validate_prepared_exit(status, validate)?;
        return Ok(None);
    }
    // A single bounded grace lets the kernel publish terminal status after
    // endpoint teardown. No retry of inspect, no polling loop or extra reap.
    tokio::time::sleep(Duration::from_millis(100)).await;
    validate()?;
    if let Some(status) = observe_exit()? {
        validate_prepared_exit(status, validate)?;
        return Ok(None);
    }
    Err(error) // still alive/unavailable is never treated as successful exit
}

#[cfg(all(test, unix))]
mod preparation_tests {
    use super::*;
    use std::{cell::Cell, os::unix::process::ExitStatusExt, process::ExitStatus};

    // Exercise inspect_target -> the actual bounded request/read/decode path.
    // The endpoint validates a complete request before returning bytes or EOF;
    // it never launches an application or an external native tool.
    fn endpoint_response(bytes: Vec<u8>) -> Result<diagnostics::TargetSnapshot> {
        use std::{
            io::{Read, Write},
            net::TcpListener,
            sync::{
                Arc,
                atomic::{AtomicBool, Ordering},
            },
            thread::JoinHandle,
            time::Instant,
        };
        struct Endpoint {
            stop: Arc<AtomicBool>,
            worker: Option<JoinHandle<Result<()>>>,
        }
        impl Drop for Endpoint {
            fn drop(&mut self) {
                self.stop.store(true, Ordering::Release);
                if let Some(worker) = self.worker.take() {
                    let _ = worker.join(); // all accept/read/write loops have a total deadline
                }
            }
        }
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
        listener.set_nonblocking(true)?;
        let descriptor = diagnostics::TargetDescriptor {
            schema_version: 1,
            service: "gateway-fixture".into(),
            run: pioneer_observability::identity::run_identity().clone(),
            start_identity: "fixture-only".into(),
            endpoint: listener.local_addr()?,
            token: "fixture-auth-token".into(),
        };
        let expected = descriptor.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let worker = std::thread::spawn(move || -> Result<()> {
            let deadline = Instant::now() + Duration::from_secs(3);
            let remaining = || -> Result<Duration> {
                ensure!(!stopping.load(Ordering::Acquire), "fixture stopped");
                Ok(deadline
                    .checked_duration_since(Instant::now())
                    .filter(|d| !d.is_zero())
                    .context("fixture IO deadline")?
                    .min(Duration::from_millis(50)))
            };
            let mut stream = loop {
                remaining()?;
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1))
                    }
                    Err(error) => return Err(error.into()),
                }
            };
            let retry = |error: &std::io::Error| {
                matches!(
                    error.kind(),
                    std::io::ErrorKind::Interrupted
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::WouldBlock
                )
            };
            let mut request = Vec::new();
            loop {
                stream.set_read_timeout(Some(remaining()?))?;
                let mut byte = [0];
                match stream.read(&mut byte) {
                    Ok(0) => bail!("fixture request truncated"),
                    Ok(_) if byte[0] == b'\n' => break,
                    Ok(_) => {
                        ensure!(request.len() < 4096, "fixture request overflow");
                        request.push(byte[0]);
                    }
                    Err(error) if retry(&error) => continue,
                    Err(error) => return Err(error.into()),
                }
            }
            let request: serde_json::Value = serde_json::from_slice(&request)?;
            ensure!(request["token"] == expected.token, "fixture authentication");
            ensure!(
                request["run_id"] == expected.run.run_id,
                "fixture run identity"
            );
            ensure!(request["epoch"].is_null(), "snapshot discovery epoch");
            ensure!(
                request["command"]["command"] == "snapshot",
                "fixture command"
            );
            let mut offset = 0;
            while offset < bytes.len() {
                stream.set_write_timeout(Some(remaining()?))?;
                match stream.write(&bytes[offset..bytes.len().min(offset + 64 * 1024)]) {
                    Ok(0) => bail!("fixture write zero"),
                    Ok(n) => offset += n,
                    Err(error) if retry(&error) => continue,
                    Err(error) => return Err(error.into()),
                }
            }
            Ok(()) // close the accepted stream, including zero-byte response
        });
        let mut endpoint = Endpoint {
            stop,
            worker: Some(worker),
        };
        let result = diagnostics::inspect_target(&descriptor);
        endpoint
            .worker
            .take()
            .unwrap()
            .join()
            .expect("endpoint worker panicked")?;
        result
    }

    #[tokio::test]
    async fn actual_empty_response_is_teardown_only_for_observed_successful_owned_exit() {
        for raw in [Some(0), Some(7 << 8), None] {
            let inspected = endpoint_response(Vec::new());
            assert_eq!(
                inspected
                    .as_ref()
                    .unwrap_err()
                    .downcast_ref::<std::io::Error>()
                    .unwrap()
                    .kind(),
                std::io::ErrorKind::UnexpectedEof
            );
            let probes = Cell::new(0);
            let checks = Cell::new(0);
            let result = reconcile_prepared_inspection(
                inspected,
                || {
                    let n = probes.get();
                    probes.set(n + 1);
                    // The first check still sees a live child; terminal evidence
                    // becomes available only on the existing final check.
                    Ok(if n == 0 {
                        None
                    } else {
                        raw.map(ExitStatus::from_raw)
                    })
                },
                || {
                    checks.set(checks.get() + 1);
                    Ok(())
                },
            )
            .await;
            assert_eq!(probes.get(), 2);
            assert!(checks.get() >= 2);
            if raw == Some(0) {
                assert!(matches!(result, Ok(None)));
            } else {
                assert!(result.is_err());
            }
        }
        for reason in [
            "consent_revoked_during_preparation",
            "storage_fixture_failure",
        ] {
            let result = reconcile_prepared_inspection(
                endpoint_response(Vec::new()),
                || Ok(Some(ExitStatus::from_raw(0))),
                || bail!(reason),
            )
            .await;
            assert_eq!(result.unwrap_err().to_string(), reason);
        }
        let result = reconcile_prepared_inspection(
            endpoint_response(Vec::new()),
            || Err(std::io::Error::other("status unavailable")),
            || Ok(()),
        )
        .await;
        assert!(result.is_err(), "unknown child status cannot reconcile EOF");
    }

    #[tokio::test]
    async fn actual_nonempty_decode_status_and_epoch_failures_cannot_hide_behind_exit_zero() {
        for bytes in [
            b"{".to_vec(), // serde EOF, but NOT an empty transport response
            b"not JSON".to_vec(),
            br#"{"status":"unavailable","value":{"reason":"authorization_denied"}}"#.to_vec(),
            br#"{"status":"unavailable","value":{"reason":"consent_epoch_changed"}}"#.to_vec(),
            br#"{"status":"ok","value":{}}"#.to_vec(), // invalid snapshot schema
            vec![b' '; 8 * 1024 * 1024 + 1],           // the existing client byte bound
        ] {
            let truncated = bytes == b"{";
            let oversized = bytes.len() == 8 * 1024 * 1024 + 1;
            let inspected = endpoint_response(bytes);
            let error = inspected.as_ref().unwrap_err();
            assert!(
                error.downcast_ref::<std::io::Error>().is_none(),
                "not transport teardown: {error}"
            );
            if truncated {
                assert!(error.downcast_ref::<serde_json::Error>().unwrap().is_eof());
            }
            if oversized {
                assert_eq!(error.to_string(), "response too large");
            }
            assert!(
                reconcile_prepared_inspection(
                    inspected,
                    || panic!("decode/status failures cannot reconcile a successful child"),
                    || panic!("non-teardown cannot enter terminal validation")
                )
                .await
                .is_err()
            );
        }
        let target = diagnostics::TargetSnapshot {
            schema_version: 1,
            service: "gateway-fixture".into(),
            run: pioneer_observability::identity::run_identity().clone(),
            consent_generation: 4,
            resources: None,
            stack_logging: "not_prepared".into(),
            module: None,
            module_status: "not_recorded".into(),
            live: None,
            runtimes: None,
            export_health: serde_json::from_value(serde_json::json!({
                "source":"fixture", "observed":false, "unavailable_signals":0,
                "failed_exports":0, "metrics_last_success_age_ms":null,
                "traces_last_success_age_ms":null, "backend_completeness":"unknown"
            }))
            .unwrap(),
        };
        let response =
            serde_json::to_vec(&serde_json::json!({"status":"ok","value":target})).unwrap();
        let observed = reconcile_prepared_inspection(
            endpoint_response(response),
            || panic!("healthy snapshot does not probe terminal status"),
            || Ok(()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(observed.consent_generation, 4);
        assert_eq!(observed.run.run_id, target.run.run_id);
        assert!(
            reconcile_prepared_inspection(
                prepared_epoch(Some(2), observed.consent_generation),
                || panic!("remote epoch mismatch cannot reconcile exit zero"),
                || Ok(())
            )
            .await
            .is_err()
        );
    }

    fn reset() -> Result<u64> {
        Err(std::io::Error::from(std::io::ErrorKind::ConnectionReset).into())
    }
    #[tokio::test]
    async fn inspect_teardown_reconciles_only_observed_successful_owned_exit() {
        for raw in [0, 7 << 8] {
            let calls = Cell::new(0);
            let result = reconcile_prepared_inspection(
                reset(),
                || {
                    let n = calls.get();
                    calls.set(n + 1);
                    Ok(if n == 0 {
                        None
                    } else {
                        Some(ExitStatus::from_raw(raw))
                    })
                },
                || Ok(()),
            )
            .await;
            assert_eq!(calls.get(), 2);
            if raw == 0 {
                assert_eq!(result.unwrap(), None);
            } else {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("exited unsuccessfully")
                );
            }
        }
        let calls = Cell::new(0);
        let error = reconcile_prepared_inspection(
            reset(),
            || {
                calls.set(calls.get() + 1);
                Ok(None)
            },
            || Ok(()),
        )
        .await
        .unwrap_err();
        assert_eq!(calls.get(), 2);
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::ConnectionReset
        );
    }
    #[tokio::test]
    async fn successful_exit_does_not_hide_consent_epoch_or_storage_failure() {
        for reason in [
            "consent_revoked_during_preparation",
            "storage_fixture_failure",
        ] {
            let checks = Cell::new(0);
            let result = reconcile_prepared_inspection(
                reset(),
                || Ok(Some(ExitStatus::from_raw(0))),
                || {
                    checks.set(checks.get() + 1);
                    if checks.get() == 2 {
                        bail!(reason);
                    }
                    Ok(())
                },
            )
            .await;
            assert_eq!(result.unwrap_err().to_string(), reason);
        }
        let calls = Cell::new(0);
        let result = reconcile_prepared_inspection::<u64>(
            prepared_epoch(Some(2), 4),
            || {
                calls.set(calls.get() + 1);
                Ok(Some(ExitStatus::from_raw(0)))
            },
            || Ok(()),
        )
        .await;
        assert_eq!(calls.get(), 0);
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("consent_epoch_changed")
        );
        let disappeared: Result<u64> =
            Err(std::io::Error::from(std::io::ErrorKind::NotConnected).into());
        assert_eq!(
            reconcile_prepared_inspection(
                disappeared,
                || Ok(Some(ExitStatus::from_raw(0))),
                || Ok(())
            )
            .await
            .unwrap(),
            None
        );
        let denied: Result<u64> =
            Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied).into());
        assert!(
            reconcile_prepared_inspection(
                denied,
                || panic!("permission errors cannot reconcile exit"),
                || Ok(())
            )
            .await
            .is_err()
        );
        assert_eq!(prepared_epoch(None, 2).unwrap(), 2);
        assert_eq!(prepared_epoch(Some(2), 2).unwrap(), 2);
        assert_eq!(
            reconcile_prepared_inspection(
                Ok(42),
                || panic!("no terminal probe on healthy snapshot"),
                || Ok(())
            )
            .await
            .unwrap(),
            Some(42)
        );
    }
}

#[cfg(test)]
mod capture_dispatch_tests {
    use super::*;
    #[test]
    fn sql_cpu_action_has_distinct_wire_mode_and_rejects_native_frequency() {
        assert_eq!(capture_mode("sql-cpu").unwrap(), CaptureMode::SqlCpu);
        assert_eq!(
            serde_json::to_value(capture_mode("sql-cpu").unwrap()).unwrap(),
            "sql_cpu"
        );
        let error = run(["sql-cpu", "--frequency", "99"]
            .into_iter()
            .map(str::to_owned))
        .unwrap_err();
        assert!(error.to_string().contains("--frequency is native-only"));
        for (action, mode) in [
            ("cpu", CaptureMode::Cpu),
            ("allocations", CaptureMode::Allocations),
            ("allocation-history", CaptureMode::AllocationHistory),
            ("live-allocations", CaptureMode::LiveAllocations),
            ("vm", CaptureMode::Vm),
        ] {
            assert_eq!(capture_mode(action).unwrap(), mode);
        }
    }
}
