//! App lifecycle, execution authority and UI admission over the neutral package coordinator.
mod authority;
pub(super) mod dev;
#[cfg(not(all(target_os = "macos", target_arch = "x86_64")))]
pub(super) mod provision;
#[cfg(not(all(target_os = "macos", target_arch = "x86_64")))]
pub(crate) mod runtime;
#[cfg(all(target_os = "macos", target_arch = "x86_64"))]
#[path = "apps/unsupported.rs"]
pub(crate) mod runtime;
mod ui;
use super::packages::plugin_error;
use super::*;
use pioneer_protocol::constants::methods;
use pioneer_protocol::*;
use serde_json::json;

pub(in crate::message) fn architecture() -> anyhow::Result<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") | ("linux", "aarch64") | ("windows", "aarch64") => Ok("linux/arm64"),
        ("linux", "x86_64") | ("windows", "x86_64") => Ok("linux/amd64"),
        _ => anyhow::bail!(APPS_UNSUPPORTED_HOST_PLATFORM),
    }
}

impl MessageProcessor {
    pub(in crate::message) async fn apps_request(
        &self,
        context: &RequestContext,
        request: JsonRpcRequest,
    ) {
        if matches!(
            request.method.as_str(),
            methods::APPS_DEV_ADD | methods::APPS_DEV_LIST | methods::APPS_DEV_REMOVE
        ) {
            self.dev_apps_request(context, request).await;
            return;
        }
        if architecture().is_err()
            && !matches!(
                request.method.as_str(),
                methods::APPS_LIST
                    | methods::APPS_DETAILS
                    | methods::APPS_OPEN_VIEW
                    | methods::APPS_RUNTIME_INFO
                    | methods::APPS_RENEW_VIEW
                    | methods::APPS_CLOSE_VIEW
                    | methods::APPS_REMOVE
            )
        {
            let mut error = plugin_error(&request.id, APPS_UNSUPPORTED_HOST_PLATFORM);
            error.error.data = Some(json!({"code": APPS_UNSUPPORTED_HOST_PLATFORM}));
            self.send_error(context.connection_id(), error).await;
            return;
        }
        if matches!(
            request.method.as_str(),
            methods::APPS_RENEW_VIEW | methods::APPS_CLOSE_VIEW
        ) {
            self.app_view_lease(context, request).await;
            return;
        }
        if request.method == methods::APPS_RUNTIME_INFO {
            self.app_runtime_info(context, request).await;
            return;
        }
        if request.method == methods::APPS_OPEN_VIEW {
            self.app_open_view(context, request).await;
            return;
        }
        let id = request.id.clone();
        let params = request.params.unwrap_or_else(|| json!({}));
        let decode = || -> anyhow::Result<(&'static str, serde_json::Value)> {
            macro_rules! typed {
                ($method:expr, $ty:ty) => {
                    (
                        $method,
                        serde_json::to_value(serde_json::from_value::<$ty>(params.clone())?)?,
                    )
                };
            }
            let result = match request.method.as_str() {
                methods::APPS_LIST => typed!(methods::PLUGINS_LIST, AppsListParams),
                methods::APPS_DETAILS => typed!(methods::PLUGINS_DETAILS, AppsDetailsParams),
                methods::APPS_PREVIEW => typed!(methods::PLUGINS_PREVIEW, AppsSourceParams),
                methods::APPS_INSTALL => typed!(methods::PLUGINS_INSTALL, AppsInstallParams),
                methods::APPS_SET_ENABLED => {
                    typed!(methods::PLUGINS_SET_ENABLED, AppsSetEnabledParams)
                }
                methods::APPS_UPDATE => typed!(methods::PLUGINS_UPDATE, AppsUpdateParams),
                methods::APPS_REMOVE => typed!(methods::PLUGINS_REMOVE, AppsRemoveParams),
                methods::APPS_RETRY => typed!(methods::PLUGINS_RETRY, AppsRetryParams),
                methods::APPS_CONTINUE => typed!(methods::PLUGINS_CONTINUE, AppsContinueParams),
                _ => anyhow::bail!("apps.invalid_request"),
            };
            Ok(result)
        };
        match decode() {
            Ok((method, mut params)) => {
                // Only already-validated App DTOs reach this adapter. Unknown fields,
                // including client-injected children/Plugin identities, were rejected.
                fn identity(value: &mut serde_json::Value) {
                    if let Some(map) = value.as_object_mut() {
                        if let Some(id) = map.remove("app_id") {
                            map.insert("plugin_id".into(), id);
                        }
                    }
                }
                identity(&mut params);
                if let Some(target) = params.get_mut("target") {
                    identity(target);
                }
                self.packages_request(
                    context,
                    JsonRpcRequest {
                        jsonrpc: request.jsonrpc,
                        id,
                        method: method.into(),
                        params: Some(params),
                    },
                    PackageKind::App,
                )
                .await;
            }
            Err(_) => {
                self.send_error(
                    context.connection_id(),
                    plugin_error(&id, "apps.invalid_request"),
                )
                .await
            }
        }
    }
}
pub(super) fn app_response(
    method: &str,
    value: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(match method {
        methods::PLUGINS_LIST => {
            let value: PluginsListResponse = serde_json::from_value(value)?;
            json!(AppsListResponse {
                apps: value
                    .plugins
                    .into_iter()
                    .map(AppItem::from_package)
                    .collect()
            })
        }
        methods::PLUGINS_UPDATE
        | methods::PLUGINS_REMOVE
        | methods::PLUGINS_RETRY
        | methods::PLUGINS_CONTINUE => {
            let value: PluginsMutationResponse = serde_json::from_value(value)?;
            json!(AppsMutationResponse {
                app: value.plugin.map(AppItem::from_package),
                removed: value.removed
            })
        }
        methods::PLUGINS_PREVIEW => value,
        _ => json!(AppItem::from_package(serde_json::from_value(value)?)),
    })
}

impl MessageProcessor {
    /// Resolve the declaring App's current backend. The existing MCP actor owns
    /// connection lifecycle; this adapter only supplies runtime routing/authority.
    pub(crate) async fn resolve_pioneer_mcp_target(
        &self,
        installation_id: &str,
        installation: &pioneer_mcp::McpServerInstallation,
        is_invocation: bool,
    ) -> anyhow::Result<pioneer_mcp::McpHttpTarget> {
        anyhow::ensure!(installation.is_pioneer(), "apps.invalid_mcp_target");
        // Metadata/session reconciliation is maintenance; tool routing blocks
        // its invocation owner. Reuse the existing database-scoped processor.
        let processor = self.scoped_with_database_class(if is_invocation {
            pioneer_sqlite::SqliteWriteClass::Interactive
        } else {
            pioneer_sqlite::SqliteWriteClass::Maintenance
        });
        let app_id = installation.source_ref["app_id"]
            .as_str()
            .context("apps.mcp_owner_missing")?;
        let owner = processor
            .crud_store
            .find_mcp_plugin_owner(installation_id)
            .await?
            .context("apps.mcp_owner_missing")?;
        anyhow::ensure!(
            owner.package_id == app_id
                && owner.member_key == installation.source_ref["member_key"].as_str().unwrap_or(""),
            "apps.mcp_owner_mismatch"
        );
        anyhow::ensure!(
            processor
                .crud_store
                .plugin_child_runtime_available(installation_id, &installation.scope_key)
                .await?,
            "apps.mcp_unavailable"
        );
        let parent = processor
            .crud_store
            .find_package_installation(app_id)
            .await?
            .context("apps.not_found")?;
        anyhow::ensure!(parent.kind == PackageKind::App, "apps.mcp_owner_not_app");
        let pioneer_mcp::McpTransportConfig::StreamableHttp { url, .. } = &installation.transport
        else {
            anyhow::bail!("apps.invalid_mcp_transport")
        };
        // The common parser owns URL validation. Only its path is used here;
        // the destination authority always comes from the App runtime owner.
        let url = url::Url::parse(url)?;
        anyhow::ensure!(url.path().starts_with('/'), "apps.invalid_mcp_path");
        let (address, credential, cancellation) = if dev::is_dev(&parent) {
            let upstream = processor.dev_upstream(&parent).await?;
            (upstream.address, upstream.credential, upstream.cancellation)
        } else {
            let upstream = self
                .app_runtime
                .upstream(app_id)
                .await
                .context("apps.not_ready")?;
            anyhow::ensure!(
                self.app_runtime.status(app_id).await.state == "ready"
                    && !upstream.2.is_cancelled(),
                "apps.not_ready"
            );
            upstream
        };
        let mut value = axum::http::HeaderValue::from_str(credential.expose_secret())?;
        value.set_sensitive(true);
        let headers = std::collections::HashMap::from([(
            axum::http::HeaderName::from_static("pioneer-app-execution-credential"),
            value,
        )]);
        Ok(pioneer_mcp::McpHttpTarget {
            uri: format!("http://{address}{}", url.path()).into(),
            headers,
            cancellation: Some(cancellation),
        })
    }
}
