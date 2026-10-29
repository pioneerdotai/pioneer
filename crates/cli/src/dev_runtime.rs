//! Only source CU starts compose a runtime. Release/package starts keep the
//! installed layout and never invoke Python or replace their Gateway executable.
use anyhow::{Context, Result};
use pioneer_config::AppConfig;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Deserialize)]
pub(crate) struct PreparedRuntime {
    pub binary: PathBuf,
    pub root: PathBuf,
}

pub(crate) fn prepare(config: &AppConfig, source: &Path) -> Result<PreparedRuntime> {
    config.validate_development_context()?;
    let root = config.runtime_home_dir()?.join("gateway-runtime");
    pioneer_config::ensure_development_path(&root)?;
    std::fs::create_dir_all(&root).context("cannot create dev runtime directory")?;
    // Embed the helper and pinned declaration from this build, rather than
    // depending on a retained checkout or a separately installed release CLI.
    let helper = tempfile::Builder::new()
        .prefix(".helper-")
        .tempdir_in(&root)?;
    let script = helper.path().join("cua-driver.py");
    std::fs::write(
        &script,
        include_str!("../../../scripts/packaging/cua-driver.py"),
    )?;
    std::fs::write(
        helper.path().join("cua-release.json"),
        include_str!("../../../scripts/packaging/cua-release.json"),
    )?;
    #[cfg(windows)]
    let interpreters = [("py", Some("-3")), ("python3", None), ("python", None)];
    #[cfg(not(windows))]
    let interpreters = [("python3", None)];
    let mut output = None;
    for (program, version) in interpreters {
        let mut command = Command::new(program);
        command.args(version);
        match command
            .arg(&script)
            .arg("dev")
            .arg("--target")
            .arg(env!("PIONEER_CUA_TARGET"))
            .arg("--binary")
            .arg(source)
            .arg("--root")
            .arg(&root)
            .output()
        {
            Ok(result) => {
                output = Some(result);
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error).context("cannot run dev CU preparation"),
        }
    }
    let output = output.context("dev computer-use preparation requires Python 3.9+ on PATH")?;
    anyhow::ensure!(
        output.status.success(),
        "dev computer-use preparation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let line = output
        .stdout
        .split(|byte| *byte == b'\n')
        .rev()
        .find(|line| !line.is_empty())
        .context("dev runtime preparation returned no result")?;
    let prepared: PreparedRuntime = serde_json::from_slice(line)?;
    anyhow::ensure!(
        prepared.root.starts_with(&root) && prepared.binary.starts_with(&prepared.root),
        "prepared runtime escaped the dev directory"
    );
    pioneer_config::ensure_development_path(&prepared.binary)?;
    Ok(prepared)
}
