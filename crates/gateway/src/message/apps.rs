//! App lifecycle, execution authority and UI admission over the neutral package coordinator.
mod authority;
pub(super) mod provision;
pub(crate) mod runtime;
mod ui;
use super::packages::plugin_error;
use super::*;
use pioneer_protocol::constants::methods;
use pioneer_protocol::*;
use serde_json::json;

impl MessageProcessor {
    pub(in crate::message) async fn apps_request(
        &self,
        context: &RequestContext,
        request: JsonRpcRequest,
    ) {
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
