use super::*;

// A bounded writer makes every report take multiple writes and rejects any
// attempt to keep writing (or flush) after a write failure.
#[derive(Default)]
struct ReportWriter {
    bytes: Vec<u8>,
    write_calls: usize,
    flush_calls: usize,
    fail_after: Option<usize>,
    write_error: Option<io::ErrorKind>,
    flush_error: Option<io::ErrorKind>,
    write_failed: bool,
}

impl Write for ReportWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        assert!(
            !self.write_failed,
            "write attempted after report output failed"
        );
        self.write_calls += 1;
        if self.fail_after == Some(self.bytes.len()) {
            self.write_failed = true;
            return Err(io::Error::new(
                self.write_error.unwrap_or(io::ErrorKind::BrokenPipe),
                "controlled write failure",
            ));
        }
        let remaining = self
            .fail_after
            .map(|limit| limit - self.bytes.len())
            .unwrap_or(usize::MAX);
        let count = bytes.len().min(7).min(remaining);
        self.bytes.extend_from_slice(&bytes[..count]);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        assert!(
            !self.write_failed,
            "flush attempted after report write failed"
        );
        self.flush_calls += 1;
        assert_eq!(self.flush_calls, 1, "flush must not be retried");
        match self.flush_error {
            Some(kind) => {
                self.write_failed = true;
                Err(io::Error::new(kind, "controlled flush failure"))
            }
            None => Ok(()),
        }
    }
}

fn report(command: installer::InstallCommand) -> installer::InstallReport {
    let (command, phase) = match command {
        installer::InstallCommand::Install => ("install", "installed"),
        installer::InstallCommand::Update => ("update", "updated"),
    };
    installer::InstallReport {
        phase,
        command,
        installed_version: "1.2.3".into(),
        installed_binary: "/tmp/pioneer/bin/pioneer".into(),
        install_root: "/tmp/pioneer".into(),
        service_active: true,
        gateway_reachable: true,
        was_active: false,
        started: true,
        command_link_created: true,
        path_updated: false,
        rollback_performed: false,
        error_code: None,
        warnings: vec![
            installer::InstallWarning {
                code: "first".into(),
                message: "first warning".into(),
            },
            installer::InstallWarning {
                code: "second".into(),
                message: "second warning".into(),
            },
        ],
        stage_timings: vec![installer::InstallStageTiming {
            stage: "complete".into(),
            started_after_ms: 10,
            duration_ms: 20,
            outcome: "ok".into(),
        }],
    }
}

fn parsed_command(name: &str) -> installer::InstallCommand {
    let (options, json_output) =
        parse_install_command_options(name, ["--json".to_owned()].into_iter())
            .expect("install command options");
    assert!(json_output);
    let expected = if name == "install" {
        installer::InstallCommand::Install
    } else {
        installer::InstallCommand::Update
    };
    assert_eq!(options.command, expected);
    options.command
}

#[derive(Debug)]
struct InstallFailure;

impl fmt::Display for InstallFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("original install failure")
    }
}

impl std::error::Error for InstallFailure {}

fn failure() -> anyhow::Error {
    anyhow::Error::new(InstallFailure).context("installer operation context")
}

#[test]
fn install_reports_broken_pipe_preserves_operation_result_and_stops_output() {
    for name in ["install", "update", "self-update"] {
        let command = parsed_command(name);
        for json_output in [false, true] {
            for fail_after in [Some(0), Some(5), None] {
                let mut writer = ReportWriter {
                    fail_after,
                    flush_error: fail_after.is_none().then_some(io::ErrorKind::BrokenPipe),
                    ..Default::default()
                };
                finish_install_result(command, Ok(report(command)), json_output, &mut writer)
                    .expect("closed consumer must not turn installation into failure");
                assert_eq!(writer.flush_calls, usize::from(fail_after.is_none()));
                if let Some(limit) = fail_after {
                    assert_eq!(writer.bytes.len(), limit);
                    assert_eq!(writer.write_calls, if limit == 0 { 1 } else { 2 });
                }

                let mut writer = ReportWriter {
                    fail_after,
                    flush_error: fail_after.is_none().then_some(io::ErrorKind::BrokenPipe),
                    ..Default::default()
                };
                let error =
                    finish_install_result(command, Err(failure()), json_output, &mut writer)
                        .expect_err("failed installation must remain failed");
                assert!(error.downcast_ref::<InstallFailure>().is_some());
                assert!(error.downcast_ref::<InstallFailureReportError>().is_none());
                assert_eq!(
                    format!("{error:#}"),
                    "installer operation context: original install failure"
                );
                assert_eq!(error.root_cause().to_string(), "original install failure");
                if !json_output {
                    assert_eq!(writer.write_calls, 0);
                    assert_eq!(writer.flush_calls, 0);
                } else {
                    assert_eq!(writer.flush_calls, usize::from(fail_after.is_none()));
                    if let Some(limit) = fail_after {
                        assert_eq!(writer.bytes.len(), limit);
                        assert_eq!(writer.write_calls, if limit == 0 { 1 } else { 2 });
                    }
                }
            }
        }
    }
}

#[test]
fn install_reports_other_output_errors_are_returned_or_secondary_to_install_failure() {
    for name in ["install", "update", "self-update"] {
        let command = parsed_command(name);
        for on_flush in [false, true] {
            for json_output in [false, true] {
                let make_writer = || ReportWriter {
                    fail_after: (!on_flush).then_some(5),
                    write_error: Some(io::ErrorKind::PermissionDenied),
                    flush_error: on_flush.then_some(io::ErrorKind::PermissionDenied),
                    ..Default::default()
                };
                let mut writer = make_writer();
                let error =
                    finish_install_result(command, Ok(report(command)), json_output, &mut writer)
                        .expect_err("non-BrokenPipe output errors must be returned");
                assert_eq!(
                    error
                        .downcast_ref::<io::Error>()
                        .expect("typed I/O error")
                        .kind(),
                    io::ErrorKind::PermissionDenied
                );
                assert_eq!(writer.flush_calls, usize::from(on_flush));

                let mut writer = make_writer();
                let error =
                    finish_install_result(command, Err(failure()), json_output, &mut writer)
                        .expect_err("installation failed");
                assert!(error.downcast_ref::<InstallFailure>().is_some());
                assert_eq!(error.root_cause().to_string(), "original install failure");
                if json_output {
                    let secondary = error
                        .downcast_ref::<InstallFailureReportError>()
                        .expect("secondary report error");
                    assert_eq!(
                        secondary
                            .0
                            .downcast_ref::<io::Error>()
                            .expect("typed secondary I/O error")
                            .kind(),
                        io::ErrorKind::PermissionDenied
                    );
                    let text = format!("{error:#}");
                    assert!(text.contains("controlled"));
                    assert!(text.contains("installer operation context: original install failure"));
                    assert_eq!(writer.flush_calls, usize::from(on_flush));
                } else {
                    assert!(error.downcast_ref::<InstallFailureReportError>().is_none());
                    assert_eq!(writer.write_calls, 0);
                }
            }
        }
    }
}

#[test]
fn install_reports_success_json_keeps_schema_pretty_format_and_newline_with_short_writes() {
    let command = installer::InstallCommand::Install;
    let mut writer = ReportWriter::default();
    finish_install_result(command, Ok(report(command)), true, &mut writer).expect("report written");
    let expected = json!({
        "phase": "installed", "command": "install", "installed_version": "1.2.3",
        "installed_binary": "/tmp/pioneer/bin/pioneer", "install_root": "/tmp/pioneer",
        "service_active": true, "gateway_reachable": true, "was_active": false,
        "started": true, "command_link_created": true, "path_updated": false,
        "rollback_performed": false, "error_code": null,
        "warnings": [{"code": "first", "message": "first warning"}, {"code": "second", "message": "second warning"}],
        "stage_timings": [{"stage": "complete", "started_after_ms": 10, "duration_ms": 20, "outcome": "ok"}]
    });
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&writer.bytes).expect("JSON"),
        expected
    );
    // Preserve the struct's existing serialization order as well as pretty printing.
    let expected_bytes = format!(
        "{}\n",
        serde_json::to_string_pretty(&report(command)).expect("serialize report")
    );
    assert_eq!(writer.bytes, expected_bytes.as_bytes());
    assert!(writer.write_calls > 1);
    assert_eq!(writer.flush_calls, 1);
}

#[test]
fn install_reports_failure_json_keeps_schema_pretty_format_and_newline_with_short_writes() {
    for name in ["install", "update", "self-update"] {
        let command = parsed_command(name);
        let mut writer = ReportWriter::default();
        let original =
            anyhow::anyhow!("health check failed; rolled back").context("installation failed");
        let error = finish_install_result(command, Err(original), true, &mut writer)
            .expect_err("installation failed");
        assert_eq!(
            format!("{error:#}"),
            "installation failed: health check failed; rolled back"
        );
        let expected = json!({
            "phase": "failed", "command": if name == "install" { "install" } else { "update" },
            "installed_version": env!("CARGO_PKG_VERSION"), "service_active": false,
            "gateway_reachable": false, "command_link_created": false, "path_updated": false,
            "rollback_performed": true, "error_code": "health_check_failed",
            "warnings": [], "stage_timings": [],
            "error": "installation failed: health check failed; rolled back"
        });
        let expected_bytes = format!(
            "{}\n",
            serde_json::to_string_pretty(&expected).expect("serialize expected JSON")
        );
        assert_eq!(writer.bytes, expected_bytes.as_bytes());
        assert!(writer.write_calls > 1);
        assert_eq!(writer.flush_calls, 1);
    }
}

#[test]
fn install_reports_text_keeps_line_order_and_warnings_with_short_writes() {
    for name in ["install", "update", "self-update"] {
        let command = parsed_command(name);
        for with_warnings in [false, true] {
            let mut report = report(command);
            if !with_warnings {
                report.warnings.clear();
            }
            let expected = format!(
                "Phase: {}\nCommand: {}\nInstall root: /tmp/pioneer\nInstalled binary: /tmp/pioneer/bin/pioneer\nInstalled version: 1.2.3\nService active before install: false\nService started after install: true\nCommand link created: true\nPATH updated: false\nService active now: true\nGateway reachable now: true\n{}",
                report.phase,
                report.command,
                if with_warnings {
                    "Warnings:\n- [first] first warning\n- [second] second warning\n"
                } else {
                    ""
                },
            );
            let mut writer = ReportWriter::default();
            finish_install_result(command, Ok(report), false, &mut writer)
                .expect("text report written");
            assert_eq!(writer.bytes, expected.as_bytes());
            assert!(writer.write_calls > 1);
            assert_eq!(writer.flush_calls, 1);
        }
    }
}
