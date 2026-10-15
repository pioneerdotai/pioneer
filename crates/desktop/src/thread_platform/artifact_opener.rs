use anyhow::{Context as _, Result};
use std::{path::Path, process::Command};

#[cfg(target_os = "macos")]
pub(crate) fn spawn_reveal_file(path: &Path) -> Result<()> {
    spawn_command(
        Command::new("open").arg("-R").arg(path),
        "reveal artifact file",
    )
}

#[cfg(all(unix, not(target_os = "macos")))]
pub(crate) fn spawn_reveal_file(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    spawn_command(Command::new("xdg-open").arg(parent), "reveal artifact file")
}

#[cfg(windows)]
pub(crate) fn spawn_reveal_file(path: &Path) -> Result<()> {
    spawn_command(
        Command::new("explorer").arg("/select,").arg(path),
        "reveal artifact file",
    )
}

fn spawn_command(command: &mut Command, action: &str) -> Result<()> {
    command
        .spawn()
        .with_context(|| format!("failed to {action}"))?;
    Ok(())
}
