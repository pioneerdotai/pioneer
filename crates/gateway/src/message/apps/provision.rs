//! SDK configuration for this Gateway's private local Microsandbox backend.
use anyhow::{Context, Result, bail};
use microsandbox::{LocalBackend, MicrosandboxError};
use std::{path::Path, sync::Arc};

use super::architecture;

pub(super) fn backend(root: &Path) -> Result<Arc<LocalBackend>> {
    architecture()?;
    let home = root.join("msb");
    // Unix-domain sockets have a pathname budget. The SDK uses named pipes on
    // Windows, so applying this limit there rejects ordinary AppData paths.
    #[cfg(unix)]
    anyhow::ensure!(
        home.as_os_str().as_encoded_bytes().len() + 52 < 104,
        "apps.runtime_home_too_long"
    );
    let backend = LocalBackend::builder()
        .home(&home)
        .config_path(home.join("gateway.json"))
        .build_lazy()?;
    anyhow::ensure!(
        backend.config().home() == home,
        "apps.runtime_home_redirected"
    );
    for directory in [
        backend.sandboxes_dir(),
        backend.volumes_dir(),
        backend.snapshots_dir(),
        backend.cache_dir(),
        backend.logs_dir(),
        backend.secrets_dir(),
    ] {
        anyhow::ensure!(directory.starts_with(&home), "apps.runtime_home_redirected");
    }
    // Refuse ambient runtime overrides instead of using a user's engine or
    // mutating process-global SDK configuration shared with other consumers.
    for path in [
        &backend.config().paths.msb,
        &backend.config().paths.libkrunfw,
        &backend.config().paths.agentd,
    ]
    .into_iter()
    .flatten()
    {
        anyhow::ensure!(path.starts_with(&home), "apps.runtime_path_redirected");
    }
    Ok(Arc::new(backend))
}
pub(super) async fn prepare(
    backend: &LocalBackend,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<()> {
    // Setup has not created an environment. The SDK owns atomic component
    // installation; no custom downloader or runtime subprocess is introduced.
    tokio::select! {
        biased;
        _ = cancel.cancelled() => bail!("apps.cancelled"),
        result = tokio::time::timeout(std::time::Duration::from_secs(180), microsandbox::setup::ensure_runtime(backend.config(), microsandbox::setup::InstallOptions {
            version: "0.7.15".into(), ..Default::default()
        })) => match result {
            Ok(result) => { result?; Ok(()) },
            Err(_) => Err(MicrosandboxError::Io(std::io::Error::from(std::io::ErrorKind::TimedOut)).into()),
        }
    }
}
pub(super) fn retryable(error: &anyhow::Error) -> bool {
    match error.downcast_ref::<MicrosandboxError>() {
        Some(MicrosandboxError::Http(error)) => {
            error.is_timeout()
                || error.is_connect()
                || error.is_body()
                || error
                    .status()
                    .is_some_and(|s| s.is_server_error() || s.as_u16() == 429)
        }
        Some(MicrosandboxError::Io(error)) => matches!(
            error.kind(),
            std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
        ),
        _ => false,
    }
}
pub(super) fn image(environment: pioneer_apps::runtime::Environment) -> Result<String> {
    let value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../packaging/app-runtimes/runtime-catalog.candidate.json"
    ))?;
    value["runtimes"][environment.name()]["image"]
        .as_str()
        .filter(|v| v.starts_with("ghcr.io/pioneerdotai/runtime-") && v.contains("@sha256:"))
        .map(str::to_owned)
        .context("apps.invalid_runtime_catalog")
}
pub(super) fn owned_name(name: &str) -> bool {
    name.strip_prefix("pioneer-app-").is_some_and(|suffix| {
        suffix.len() == 64
            && suffix
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}
pub(super) const CLEANUP_BUDGET: std::time::Duration = std::time::Duration::from_secs(20);

// The SDK timeout only bounds stopped-state observation. Keep one Gateway
// deadline around lookup, transition-lock acquisition, termination and removal.
pub(super) async fn cleanup_before<T>(
    deadline: tokio::time::Instant,
    operation: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    anyhow::ensure!(
        tokio::time::Instant::now() < deadline,
        "apps.runtime_cleanup_not_confirmed"
    );
    tokio::time::timeout_at(deadline, operation)
        .await
        .context("apps.runtime_cleanup_not_confirmed")?
}

pub(super) async fn remove(
    backend: Arc<LocalBackend>,
    name: &str,
    deadline: tokio::time::Instant,
) -> Result<()> {
    anyhow::ensure!(owned_name(name), "apps.runtime_identity_mismatch");
    let scoped: Arc<dyn microsandbox::Backend> = backend;
    cleanup_before(
        deadline,
        microsandbox::with_backend(scoped, async {
            let handle = match microsandbox::Sandbox::get(name).await {
                Ok(handle) => handle,
                Err(MicrosandboxError::SandboxNotFound(_)) => return Ok(()),
                Err(error) => return Err(error.into()),
            };
            handle
                .destroy_with(microsandbox::sandbox::DestroyOptions {
                    force: true,
                    timeout: CLEANUP_BUDGET,
                })
                .await?;
            Ok(())
        }),
    )
    .await
}

pub(super) async fn recover(backend: Arc<LocalBackend>) -> Result<()> {
    // Startup cancellation does not abandon resources recovered from the last
    // Gateway process. Its worker joins this bounded-per-operation SDK cleanup.
    let cancel = tokio_util::sync::CancellationToken::new();
    let scoped: Arc<dyn microsandbox::Backend> = backend.clone();
    microsandbox::with_backend(scoped, async {
        let mut cursor = None;
        loop {
            let page = cleanup_before(tokio::time::Instant::now() + CLEANUP_BUDGET, async {
                Ok(microsandbox::Sandbox::list_with(|query| {
                    let query = query.limit(64);
                    match cursor.as_ref() {
                        Some(cursor) => query.cursor(cursor),
                        None => query,
                    }
                })
                .await?)
            })
            .await?;
            cursor = page.next_cursor;
            for handle in page.sandboxes {
                if !owned_name(handle.name()) {
                    continue;
                }
                prepare(&backend, &cancel).await?;
                cleanup_before(tokio::time::Instant::now() + CLEANUP_BUDGET, async {
                    handle
                        .destroy_with(microsandbox::sandbox::DestroyOptions {
                            force: true,
                            timeout: CLEANUP_BUDGET,
                        })
                        .await?;
                    Ok(())
                })
                .await?;
            }
            if cursor.is_none() {
                break;
            }
        }
        Ok(())
    })
    .await
}

#[cfg(test)]
mod sdk_boundary_tests {
    use super::*;
    #[test]
    fn recovery_only_accepts_gateway_app_execution_names() {
        assert!(owned_name(&format!("pioneer-app-{}", "a".repeat(64))));
        for name in [
            "pioneer-app-test".to_owned(),
            format!("other-{}", "a".repeat(64)),
            format!("pioneer-app-{}", "A".repeat(64)),
            format!("pioneer-app-{}", "a".repeat(63)),
        ] {
            assert!(!owned_name(&name));
        }
    }
    #[test]
    #[cfg(unix)]
    fn private_sdk_home_rejects_socket_paths_before_opening_the_backend() {
        let root = std::path::PathBuf::from(format!("/{}", "x".repeat(128)));
        let error = backend(&root)
            .err()
            .expect("long SDK home must be rejected");
        if architecture().is_ok() {
            assert_eq!(error.to_string(), "apps.runtime_home_too_long");
        } else {
            assert_eq!(error.to_string(), "apps.unsupported_host_platform");
        }
    }
}

#[cfg(all(test, windows))]
mod windows_sdk_boundary_tests {
    use super::*;

    #[test]
    fn windows_apps_accept_a_private_home_longer_than_a_unix_socket_path() {
        #[cfg(target_arch = "x86_64")]
        assert_eq!(architecture().unwrap(), "linux/amd64");
        #[cfg(target_arch = "aarch64")]
        assert_eq!(architecture().unwrap(), "linux/arm64");
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("windows-app-data-path-".repeat(6));
        // Only resolves lazy SDK configuration: no installation, VM, SDK
        // database, or host hypervisor access is needed by this regression.
        assert!(
            backend(&home).is_ok(),
            "Windows named pipes have no Unix socket pathname budget"
        );
    }
}
