pub mod apply;
pub mod cleanup;
pub mod plan;
pub mod platform;
pub mod process;
pub mod result_state;

use std::path::PathBuf;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

const EXIT_SUCCESS: i32 = 0;
const EXIT_FAILURE: i32 = 1;
const EXIT_USAGE: i32 = 64;

pub fn run_cli_from_env() -> i32 {
    run_cli(std::env::args().skip(1))
}

pub fn run_cli(args: impl IntoIterator<Item = String>) -> i32 {
    match parse_cli(args) {
        Ok(Command::Version) => {
            println!("{VERSION}");
            EXIT_SUCCESS
        }
        Ok(Command::Apply { plan_path }) => match apply::apply_plan(plan_path.as_path()) {
            Ok(()) => EXIT_SUCCESS,
            Err(error) => {
                eprintln!("desktop update failed: {error:#}");
                EXIT_FAILURE
            }
        },
        Err(message) => {
            eprintln!("{message}");
            eprintln!("{}", usage());
            EXIT_USAGE
        }
    }
}

enum Command {
    Version,
    Apply { plan_path: PathBuf },
}

fn parse_cli(args: impl IntoIterator<Item = String>) -> Result<Command, String> {
    let args = args.into_iter().collect::<Vec<_>>();
    match args.as_slice() {
        [flag] if flag == "--version" || flag == "-V" => Ok(Command::Version),
        [command, plan_flag, plan_path] if command == "apply" && plan_flag == "--plan" => {
            if plan_path.trim().is_empty() {
                Err("missing plan path".to_owned())
            } else {
                Ok(Command::Apply {
                    plan_path: PathBuf::from(plan_path),
                })
            }
        }
        [] => Err("missing command".to_owned()),
        [command, ..] => Err(format!("unknown or invalid command `{command}`")),
    }
}

fn usage() -> &'static str {
    "usage: pioneer-app-updater --version | pioneer-app-updater apply --plan <path>"
}

#[cfg(test)]
mod tests {
    use super::{Command, parse_cli};
    use std::path::PathBuf;

    #[test]
    fn parses_version_command() {
        assert!(matches!(
            parse_cli(["--version".to_owned()]),
            Ok(Command::Version)
        ));
    }

    #[test]
    fn parses_apply_plan_command() {
        let command = parse_cli([
            "apply".to_owned(),
            "--plan".to_owned(),
            "/tmp/plan.json".to_owned(),
        ])
        .unwrap();

        assert!(matches!(
            command,
            Command::Apply { plan_path } if plan_path == PathBuf::from("/tmp/plan.json")
        ));
    }

    #[test]
    fn rejects_unknown_commands() {
        assert!(parse_cli(["install".to_owned()]).is_err());
    }
}
