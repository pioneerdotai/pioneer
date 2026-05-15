use anyhow::{Context, Result, bail};
use pioneer_tasks::TaskRuntimeInvariantScanner;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
struct TaskInvariantCommand {
    db_path: PathBuf,
    json_output: bool,
    stale_turn_after_seconds: Option<i64>,
}

pub(crate) fn run(args: impl Iterator<Item = String>) -> Result<()> {
    let command = parse(args)?;
    let observed_at_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_secs() as i64;
    let scanner = match command.stale_turn_after_seconds {
        Some(value) => TaskRuntimeInvariantScanner::new().with_stale_turn_after_seconds(value),
        None => TaskRuntimeInvariantScanner::new(),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to create task invariant scanner runtime")?;
    let report = runtime.block_on(scanner.scan_sqlite_path(&command.db_path, observed_at_unix))?;

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
    let mut db_path = None;
    let mut json_output = false;
    let mut stale_turn_after_seconds = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => {
                let value = args
                    .next()
                    .context("`task-invariants --db` requires a SQLite database path")?;
                db_path = Some(PathBuf::from(value));
            }
            "--json" => {
                json_output = true;
            }
            "--stale-turn-after-seconds" => {
                let value = args
                    .next()
                    .context("`--stale-turn-after-seconds` requires an integer value")?;
                let parsed = value.parse::<i64>().with_context(|| {
                    format!("invalid `--stale-turn-after-seconds` value `{value}`")
                })?;
                if parsed < 0 {
                    bail!("`--stale-turn-after-seconds` must be non-negative");
                }
                stale_turn_after_seconds = Some(parsed);
            }
            "--help" | "-h" => bail!(
                "Usage: pioneer task-invariants --db <path> [--json] [--stale-turn-after-seconds <seconds>]"
            ),
            other => bail!("unexpected argument for task-invariants: {other}"),
        }
    }

    let db_path = db_path.context("missing required `task-invariants --db <path>`")?;
    Ok(TaskInvariantCommand {
        db_path,
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
    fn parses_required_db_path() {
        let command = parse_args(&["--db", "/tmp/gateway.db"]).expect("parse");

        assert_eq!(command.db_path, PathBuf::from("/tmp/gateway.db"));
        assert!(!command.json_output);
        assert_eq!(command.stale_turn_after_seconds, None);
    }

    #[test]
    fn parses_json_and_stale_threshold() {
        let command = parse_args(&[
            "--json",
            "--db",
            "/tmp/gateway.db",
            "--stale-turn-after-seconds",
            "15",
        ])
        .expect("parse");

        assert_eq!(command.db_path, PathBuf::from("/tmp/gateway.db"));
        assert!(command.json_output);
        assert_eq!(command.stale_turn_after_seconds, Some(15));
    }

    #[test]
    fn requires_db_path() {
        let error = parse_args(&["--json"]).expect_err("missing db should fail");

        assert!(format!("{error:#}").contains("missing required `task-invariants --db <path>`"));
    }

    #[test]
    fn rejects_negative_stale_threshold() {
        let error = parse_args(&[
            "--db",
            "/tmp/gateway.db",
            "--stale-turn-after-seconds",
            "-1",
        ])
        .expect_err("negative threshold should fail");

        assert!(format!("{error:#}").contains("must be non-negative"));
    }
}
