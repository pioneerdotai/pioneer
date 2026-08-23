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
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub(crate) struct ClientMobileStartupRecordResult {
    pub recorded: bool,
}

pub(crate) fn record_mobile_startup(
    input_json: &str,
) -> Result<ClientMobileStartupRecordResult, String> {
    let request = serde_json::from_str::<ClientMobileStartupRecordRequest>(input_json)
        .map_err(|error| format!("invalid mobile startup report: {error}"))?;
    pioneer_observability::set_telemetry_enabled(request.enabled);
    if !request.enabled {
        return Ok(ClientMobileStartupRecordResult { recorded: false });
    }

    pioneer_observability::init_otlp_observability_for(
        TelemetryTarget::Mobile,
        OtlpTelemetryConfig {
            metrics_endpoint: request.metrics_endpoint,
            traces_endpoint: request.traces_endpoint,
            export_interval: Duration::from_millis(request.export_interval_ms),
            export_timeout: Duration::from_millis(request.export_timeout_ms),
            deployment_environment: Some(request.deployment_environment),
        },
    )
    .map_err(|error| format!("failed to initialize mobile observability: {error:#}"))?;

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
            Ok(MobileStartupStageTiming {
                stage: parsed,
                start_offset,
                duration: stage_duration,
                failed: stage.failed,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    pioneer_observability::record_mobile_startup(MobileStartupReport {
        started_at: UNIX_EPOCH + Duration::from_millis(request.started_at_unix_ms),
        duration,
        outcome,
        stages,
    });
    Ok(ClientMobileStartupRecordResult { recorded: true })
}

fn duration_from_millis(value: f64, field: &str) -> Result<Duration, String> {
    if !value.is_finite() || !(0.0..=MAX_STARTUP_DURATION_MS).contains(&value) {
        return Err(format!("{field} is outside the supported startup range"));
    }
    Ok(Duration::from_secs_f64(value / 1_000.0))
}

#[cfg(test)]
mod tests {
    use super::duration_from_millis;

    #[test]
    fn startup_durations_are_finite_and_bounded() {
        assert!(duration_from_millis(250.0, "duration").is_ok());
        assert!(duration_from_millis(f64::NAN, "duration").is_err());
        assert!(duration_from_millis(700_000.0, "duration").is_err());
    }
}
