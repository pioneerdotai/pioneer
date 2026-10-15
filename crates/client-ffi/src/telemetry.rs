use pioneer_observability::{
    MobileStartupOutcome, MobileStartupReport, MobileStartupStage, MobileStartupStageTiming,
    OtlpTelemetryConfig, TelemetryTarget,
};
use serde::{Deserialize, Serialize};
use std::time::{Duration, UNIX_EPOCH};

const MAX_STARTUP_DURATION_MS: f64 = 10.0 * 60.0 * 1_000.0;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientMobileStartupRecordRequest {
    enabled: bool,
    metrics_endpoint: String,
    traces_endpoint: String,
    export_interval_ms: u64,
    export_timeout_ms: u64,
    deployment_environment: String,
    #[serde(default)]
    service_version: Option<String>,
    started_at_unix_ms: u64,
    duration_ms: f64,
    outcome: String,
    stages: Vec<ClientMobileStartupStageTiming>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientMobileStartupStageTiming {
    name: String,
    start_offset_ms: f64,
    duration_ms: f64,
    #[serde(default)]
    failed: bool,
    #[serde(default)]
    cancelled: bool,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub(crate) struct ClientMobileStartupRecordResult {
    pub recorded: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
}

pub(crate) fn record_mobile_startup(
    input_json: &str,
) -> Result<ClientMobileStartupRecordResult, String> {
    let value: serde_json::Value =
        serde_json::from_str(input_json).map_err(|_| "invalid telemetry report".to_owned())?;
    if value.get("kind").and_then(|v| v.as_str()) == Some("turn_startup") {
        #[derive(Deserialize)]
        struct Report {
            key: String,
            duration_ms: Option<f64>,
            text: Option<bool>,
            loss: Option<String>,
            receive: Option<bool>,
            bridge_ms: Option<f64>,
        }
        let report: Report =
            serde_json::from_value(value).map_err(|_| "invalid turn startup report".to_owned())?;
        if report.key.len() > 512 {
            return Err("invalid startup key".to_owned());
        }
        let turn_id = pioneer_observability::turn_startup::canonical_key(&report.key);
        let lost = report.loss.as_deref().is_some_and(|reason| {
            pioneer_observability::turn_startup::mobile_lost(&report.key, reason)
        });
        let bridge = report
            .bridge_ms
            .is_some_and(|ms| pioneer_observability::turn_startup::mobile_bridge(&report.key, ms));
        let recorded = lost
            || bridge
            || report.duration_ms.is_some_and(|ms| {
                if report.receive.unwrap_or(false) {
                    pioneer_observability::turn_startup::mobile_received(
                        &report.key,
                        ms,
                        report.text.unwrap_or(false),
                    )
                } else {
                    pioneer_observability::turn_startup::mobile_presented(
                        &report.key,
                        ms,
                        report.text.unwrap_or(false),
                    )
                }
            });
        return Ok(ClientMobileStartupRecordResult { recorded, turn_id });
    }
    let request = serde_json::from_str::<ClientMobileStartupRecordRequest>(input_json)
        .map_err(|error| format!("invalid mobile startup report: {error}"))?;
    if !request.enabled {
        pioneer_observability::set_telemetry_enabled(false);
        return Ok(ClientMobileStartupRecordResult {
            recorded: false,
            turn_id: None,
        });
    }

    let duration = duration_from_millis(request.duration_ms, "duration_ms")?;
    let outcome = MobileStartupOutcome::parse(request.outcome.as_str())
        .ok_or_else(|| "invalid mobile startup outcome".to_owned())?;
    if request.stages.len() > 32 {
        return Err("mobile startup report has too many stages".to_owned());
    }
    let stages = request
        .stages
        .into_iter()
        .map(|stage| {
            let parsed = MobileStartupStage::parse(stage.name.as_str())
                .ok_or_else(|| format!("invalid mobile startup stage `{}`", stage.name))?;
            let start_offset = duration_from_millis(stage.start_offset_ms, "start_offset_ms")?;
            let stage_duration = duration_from_millis(stage.duration_ms, "stage.duration_ms")?;
            if start_offset > duration
                || stage_duration > duration
                || start_offset.saturating_add(stage_duration) > duration
            {
                return Err("mobile startup stage is outside the startup timeline".to_owned());
            }
            validate_stage_outcome(stage.failed, stage.cancelled)?;
            Ok(MobileStartupStageTiming {
                stage: parsed,
                start_offset,
                duration: stage_duration,
                failed: stage.failed,
                cancelled: stage.cancelled,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    // Validate the complete FFI payload before initializing the process-wide
    // telemetry pipeline. A malformed first report must not permanently claim
    // the OnceLock with an unusable configuration and no recorded event.
    pioneer_observability::set_telemetry_enabled(true);
    pioneer_observability::init_otlp_observability_for(
        TelemetryTarget::Mobile,
        OtlpTelemetryConfig {
            metrics_endpoint: request.metrics_endpoint,
            traces_endpoint: request.traces_endpoint,
            export_interval: Duration::from_millis(request.export_interval_ms),
            export_timeout: Duration::from_millis(request.export_timeout_ms),
            deployment_environment: Some(request.deployment_environment),
            service_version: request.service_version,
        },
    )
    .map_err(|error| format!("failed to initialize mobile observability: {error:#}"))?;

    pioneer_observability::record_mobile_startup(MobileStartupReport {
        started_at: UNIX_EPOCH + Duration::from_millis(request.started_at_unix_ms),
        duration,
        outcome,
        stages,
    });
    // The mobile app may move to the background before the periodic metrics
    // interval elapses. Flush this single lifecycle sample immediately, but
    // never block the JavaScript/UI thread on telemetry network I/O.
    pioneer_observability::schedule_observability_flush();
    Ok(ClientMobileStartupRecordResult {
        recorded: true,
        turn_id: None,
    })
}

fn validate_stage_outcome(failed: bool, cancelled: bool) -> Result<(), String> {
    if failed && cancelled {
        return Err("mobile startup stage cannot be both failed and cancelled".to_owned());
    }
    Ok(())
}

fn duration_from_millis(value: f64, field: &str) -> Result<Duration, String> {
    if !value.is_finite() || !(0.0..=MAX_STARTUP_DURATION_MS).contains(&value) {
        return Err(format!("{field} is outside the supported startup range"));
    }
    Ok(Duration::from_secs_f64(value / 1_000.0))
}

#[cfg(test)]
mod tests {
    use super::{duration_from_millis, validate_stage_outcome};

    #[test]
    fn turn_reports_are_bounded_and_do_not_require_a_mobile_startup_payload() {
        for extra in [
            "\"receive\":true",
            "\"loss\":\"background\"",
            "\"text\":true",
        ] {
            let json = format!(
                "{{\"kind\":\"turn_startup\",\"key\":\"missing-turn\",\"duration_ms\":1,{extra}}}"
            );
            let result = super::record_mobile_startup(&json).unwrap();
            assert!(!result.recorded);
            assert!(result.turn_id.is_none());
        }
        let json = serde_json::json!({"kind":"turn_startup","key":"x".repeat(513)}).to_string();
        assert!(super::record_mobile_startup(&json).is_err());
    }

    #[test]
    fn startup_durations_are_finite_and_bounded() {
        assert!(duration_from_millis(250.0, "duration").is_ok());
        assert!(duration_from_millis(f64::NAN, "duration").is_err());
        assert!(duration_from_millis(700_000.0, "duration").is_err());
    }

    #[test]
    fn mobile_stage_outcomes_are_unambiguous() {
        assert!(validate_stage_outcome(false, false).is_ok());
        assert!(validate_stage_outcome(true, false).is_ok());
        assert!(validate_stage_outcome(false, true).is_ok());
        assert!(validate_stage_outcome(true, true).is_err());
    }
}
