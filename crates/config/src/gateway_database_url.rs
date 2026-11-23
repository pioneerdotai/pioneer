use std::env::VarError;
use std::fs;
use std::io::{ErrorKind, Write};
use std::path::Path;

use anyhow::{Context, Result};

use crate::{AppConfig, RuntimeEnvironment, workspace_root};

const DATABASE_URL: &str = "DATABASE_URL";
const DEVELOPMENT_SERVICE_OVERRIDE: &str = "gateway-service-database-url";

pub fn resolve(config: &AppConfig, runtime_home: &Path) -> Result<Option<String>> {
    let environment = RuntimeEnvironment::current();
    // An isolated runtime home, such as a database test fixture, owns its own
    // configuration and must never default to the developer's database.
    let workspace_dotenv = if environment == RuntimeEnvironment::Development
        && config.runtime_home_dir()? == runtime_home
    {
        Some(workspace_root().join(".env"))
    } else {
        None
    };
    resolve_from_sources(
        environment,
        process_url()?,
        workspace_dotenv.as_deref(),
        runtime_home,
    )
}

/// Preserve an inline DATABASE_URL across service-manager launches, which do
/// not inherit the CLI environment. A new start replaces the previous choice.
pub fn save_development_service_override(runtime_home: &Path) -> Result<()> {
    let Some(url) = process_url()? else {
        return clear_development_service_override(runtime_home);
    };
    write_development_service_override(runtime_home, &url)
}

pub fn clear_development_service_override(runtime_home: &Path) -> Result<()> {
    match fs::remove_file(runtime_home.join(DEVELOPMENT_SERVICE_OVERRIDE)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).context("clear development Gateway service DATABASE_URL"),
    }
}

fn write_development_service_override(runtime_home: &Path, url: &str) -> Result<()> {
    (|| -> std::io::Result<()> {
        fs::create_dir_all(runtime_home)?;
        let mut file = tempfile::NamedTempFile::new_in(runtime_home)?;
        file.write_all(url.as_bytes())?;
        file.persist(runtime_home.join(DEVELOPMENT_SERVICE_OVERRIDE))
            .map_err(|error| error.error)?;
        Ok(())
    })()
    .context("save development Gateway service DATABASE_URL")
}

fn process_url() -> Result<Option<String>> {
    match std::env::var(DATABASE_URL) {
        Ok(url) => Ok(Some(url)),
        Err(VarError::NotPresent) => Ok(None),
        Err(VarError::NotUnicode(_)) => anyhow::bail!("DATABASE_URL must be UTF-8"),
    }
}

fn resolve_from_sources(
    environment: RuntimeEnvironment,
    process_url: Option<String>,
    workspace_dotenv: Option<&Path>,
    runtime_home: &Path,
) -> Result<Option<String>> {
    if process_url.is_some() {
        return Ok(process_url);
    }
    if environment == RuntimeEnvironment::Development {
        match fs::read_to_string(runtime_home.join(DEVELOPMENT_SERVICE_OVERRIDE)) {
            Ok(url) => return Ok(Some(url)),
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).context("read development Gateway service DATABASE_URL");
            }
        }
        if let Some(path) = workspace_dotenv
            && let Some(url) = dotenv_url(path, "workspace .env")?
        {
            return Ok(Some(url));
        }
    }
    dotenv_url(&runtime_home.join(".env"), "Gateway runtime .env")
}

fn dotenv_url(path: &Path, source: &str) -> Result<Option<String>> {
    let entries = match dotenvy::from_path_iter(path) {
        Ok(entries) => entries,
        Err(error) if error.not_found() => return Ok(None),
        Err(_) => anyhow::bail!("open {source}"),
    };
    for entry in entries {
        // dotenvy syntax errors include the original line and can expose credentials.
        let (key, value) = entry.map_err(|_| anyhow::anyhow!("parse {source}"))?;
        if key == DATABASE_URL {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn development_database_url_follows_service_and_dotenv_precedence() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let runtime = tempfile::tempdir()?;
        let workspace_dotenv = workspace.path().join(".env");
        let runtime_dotenv = runtime.path().join(".env");
        let inline_url = "postgresql://app@localhost/inline";
        let saved_url = "postgresql://app@localhost/saved";
        let workspace_url = "postgresql://app@localhost/workspace";
        let runtime_url = "postgresql://app@localhost/runtime";
        fs::write(
            &workspace_dotenv,
            format!("DATABASE_URL='{workspace_url}'\n"),
        )?;
        fs::write(&runtime_dotenv, format!("DATABASE_URL={runtime_url}\n"))?;

        let resolve = |process_url| {
            resolve_from_sources(
                RuntimeEnvironment::Development,
                process_url,
                Some(&workspace_dotenv),
                runtime.path(),
            )
        };
        assert_eq!(resolve(None)?.as_deref(), Some(workspace_url));

        write_development_service_override(runtime.path(), saved_url)?;
        assert_eq!(resolve(None)?.as_deref(), Some(saved_url));
        assert_eq!(
            resolve(Some(inline_url.into()))?.as_deref(),
            Some(inline_url)
        );

        write_development_service_override(runtime.path(), inline_url)?;
        assert_eq!(resolve(None)?.as_deref(), Some(inline_url));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions =
                fs::metadata(runtime.path().join(DEVELOPMENT_SERVICE_OVERRIDE))?.permissions();
            assert_eq!(permissions.mode() & 0o777, 0o600);
        }

        clear_development_service_override(runtime.path())?;
        assert_eq!(resolve(None)?.as_deref(), Some(workspace_url));
        fs::write(&workspace_dotenv, "OTHER_SETTING=unused\n")?;
        assert_eq!(resolve(None)?.as_deref(), Some(runtime_url));
        fs::remove_file(runtime_dotenv)?;
        assert_eq!(resolve(None)?, None);
        Ok(())
    }

    #[test]
    fn production_database_url_ignores_development_sources() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let runtime = tempfile::tempdir()?;
        let workspace_dotenv = workspace.path().join(".env");
        let production_url = "postgresql://app@localhost/production";
        fs::write(
            &workspace_dotenv,
            "DATABASE_URL=postgresql://app@localhost/dev\n",
        )?;
        write_development_service_override(runtime.path(), "postgresql://app@localhost/saved")?;
        fs::write(
            runtime.path().join(".env"),
            format!("DATABASE_URL={production_url}\n"),
        )?;

        let resolved = resolve_from_sources(
            RuntimeEnvironment::Production,
            None,
            Some(&workspace_dotenv),
            runtime.path(),
        )?;
        assert_eq!(resolved.as_deref(), Some(production_url));
        Ok(())
    }
}
