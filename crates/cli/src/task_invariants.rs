use anyhow::{Context, Result, bail};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::usage_error;

#[derive(Debug, Clone, PartialEq, Eq)]
struct TaskInvariantCommand {
    json_output: bool,
    stale_turn_after_seconds: Option<i64>,
}

pub(crate) fn run(args: impl Iterator<Item = String>) -> Result<()> {
    let command = parse(args)?;
    let observed_at_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_secs() as i64;
    let (config, runtime_home) = crate::service::load_config_and_runtime_home()?;
    let report =
        crate::service::block_on_gateway_operation(pioneer_gateway::task_runtime_invariants(
            &config,
            &runtime_home,
            observed_at_unix,
            command.stale_turn_after_seconds,
        ))?;

    if command.json_output {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print!("{report}");
    }

    if !report.is_empty() {
        bail!(
            "task runtime invariant check found {} violation(s)",
            report.violation_count()
        );
    }

    Ok(())
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<TaskInvariantCommand> {
    let mut json_output = false;
    let mut stale_turn_after_seconds = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--json" => {
                json_output = true;
            }
            "--stale-turn-after-seconds" => {
                let value = args.next().ok_or_else(|| {
                    usage_error("`--stale-turn-after-seconds` requires an integer value")
                })?;
                let parsed = value.parse::<i64>().map_err(|_| {
                    usage_error(format!(
                        "invalid `--stale-turn-after-seconds` value `{value}`"
                    ))
                })?;
                if parsed < 0 {
                    return Err(usage_error(
                        "`--stale-turn-after-seconds` must be non-negative",
                    ));
                }
                stale_turn_after_seconds = Some(parsed);
            }
            "--help" | "-h" => {
                return Err(usage_error(
                    "Usage: pioneer task-invariants [--json] [--stale-turn-after-seconds <seconds>]",
                ));
            }
            other => {
                return Err(usage_error(format!(
                    "unexpected argument for task-invariants: {other}"
                )));
            }
        }
    }

    Ok(TaskInvariantCommand {
        json_output,
        stale_turn_after_seconds,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_args(args: &[&str]) -> Result<TaskInvariantCommand> {
        parse(args.iter().map(|value| (*value).to_owned()))
    }

    #[test]
    fn no_arguments_scans_the_configured_gateway() {
        let command = parse_args(&[]).expect("parse");
        assert!(!command.json_output);
        assert_eq!(command.stale_turn_after_seconds, None);
    }

    #[test]
    fn parses_json_and_stale_threshold() {
        let command = parse_args(&["--json", "--stale-turn-after-seconds", "15"]).expect("parse");
        assert!(command.json_output);
        assert_eq!(command.stale_turn_after_seconds, Some(15));
    }

    #[test]
    fn rejects_negative_stale_threshold() {
        let error = parse_args(&["--stale-turn-after-seconds", "-1"])
            .expect_err("negative threshold should fail");
        assert!(format!("{error:#}").contains("must be non-negative"));
    }
}
