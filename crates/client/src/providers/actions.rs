//! Provider action orchestration.

use super::{catalog, list::ProviderListState};
use pioneer_protocol::{
    CLIRuntimeLoginStartParams, CLIRuntimeLoginStartType, ProviderDeleteApiKeyParams,
    ProviderSetApiKeyParams,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ProviderApiKeyActionUnavailable {
    GatewayNotConnected,
    WorkspaceNotSelected,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ProviderApiKeyActionRequest<TParams> {
    pub connection_id: u64,
    pub canonical_provider_id: String,
    pub params: TParams,
}

pub type ProviderSetApiKeyActionRequest = ProviderApiKeyActionRequest<ProviderSetApiKeyParams>;
pub type ProviderDeleteApiKeyActionRequest =
    ProviderApiKeyActionRequest<ProviderDeleteApiKeyParams>;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CLIRuntimeLoginStartActionRequest {
    pub connection_id: u64,
    pub runtime_id: String,
    pub params: CLIRuntimeLoginStartParams,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum ProviderSetApiKeyPlan {
    Send(ProviderSetApiKeyActionRequest),
    Unavailable(ProviderApiKeyActionUnavailable),
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum ProviderDeleteApiKeyPlan {
    Send(ProviderDeleteApiKeyActionRequest),
    Unavailable(ProviderApiKeyActionUnavailable),
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum CLIRuntimeLoginStartPlan {
    Send(CLIRuntimeLoginStartActionRequest),
    Unavailable(ProviderApiKeyActionUnavailable),
}

pub fn plan_provider_set_api_key(
    gateway_connected: bool,
    connection_id: Option<u64>,
    workspace_id: Option<String>,
    provider_id: String,
    api_key: String,
) -> ProviderSetApiKeyPlan {
    let Some(connection_id) = available_connection_id(gateway_connected, connection_id) else {
        return ProviderSetApiKeyPlan::Unavailable(
            ProviderApiKeyActionUnavailable::GatewayNotConnected,
        );
    };
    let Some(workspace_id) = workspace_id else {
        return ProviderSetApiKeyPlan::Unavailable(
            ProviderApiKeyActionUnavailable::WorkspaceNotSelected,
        );
    };
    let canonical_provider_id = catalog::canonical_provider_id(provider_id.as_str());

    ProviderSetApiKeyPlan::Send(ProviderSetApiKeyActionRequest {
        connection_id,
        canonical_provider_id,
        params: ProviderSetApiKeyParams {
            workspace_id,
            provider: provider_id,
            api_key,
        },
    })
}

pub fn plan_provider_delete_api_key(
    gateway_connected: bool,
    connection_id: Option<u64>,
    workspace_id: Option<String>,
    provider_id: String,
) -> ProviderDeleteApiKeyPlan {
    let Some(connection_id) = available_connection_id(gateway_connected, connection_id) else {
        return ProviderDeleteApiKeyPlan::Unavailable(
            ProviderApiKeyActionUnavailable::GatewayNotConnected,
        );
    };
    let Some(workspace_id) = workspace_id else {
        return ProviderDeleteApiKeyPlan::Unavailable(
            ProviderApiKeyActionUnavailable::WorkspaceNotSelected,
        );
    };
    let canonical_provider_id = catalog::canonical_provider_id(provider_id.as_str());

    ProviderDeleteApiKeyPlan::Send(ProviderDeleteApiKeyActionRequest {
        connection_id,
        canonical_provider_id,
        params: ProviderDeleteApiKeyParams {
            workspace_id,
            provider: provider_id,
        },
    })
}

pub fn plan_cli_runtime_login_start(
    gateway_connected: bool,
    connection_id: Option<u64>,
    workspace_id: Option<String>,
    runtime_id: String,
    login_type: CLIRuntimeLoginStartType,
) -> CLIRuntimeLoginStartPlan {
    let Some(connection_id) = available_connection_id(gateway_connected, connection_id) else {
        return CLIRuntimeLoginStartPlan::Unavailable(
            ProviderApiKeyActionUnavailable::GatewayNotConnected,
        );
    };
    let Some(workspace_id) = workspace_id else {
        return CLIRuntimeLoginStartPlan::Unavailable(
            ProviderApiKeyActionUnavailable::WorkspaceNotSelected,
        );
    };

    CLIRuntimeLoginStartPlan::Send(CLIRuntimeLoginStartActionRequest {
        connection_id,
        runtime_id: runtime_id.clone(),
        params: CLIRuntimeLoginStartParams {
            workspace_id,
            runtime_id,
            login_type,
        },
    })
}

pub fn provider_api_key_action_matches_connection(
    action_connection_id: u64,
    current_connection_id: Option<u64>,
) -> bool {
    current_connection_id == Some(action_connection_id)
}

pub fn mark_provider_api_key_action_started(providers: &mut ProviderListState) {
    providers.set_error(None);
}

pub fn apply_provider_set_api_key_success(
    providers: &mut ProviderListState,
    canonical_provider_id: String,
) {
    providers.insert_configured(canonical_provider_id);
}

pub fn apply_provider_delete_api_key_success(
    providers: &mut ProviderListState,
    canonical_provider_id: &str,
) {
    providers.remove_configured(canonical_provider_id);
}

pub fn apply_provider_api_key_failure(providers: &mut ProviderListState, error: String) {
    providers.set_error(Some(error));
}

fn available_connection_id(gateway_connected: bool, connection_id: Option<u64>) -> Option<u64> {
    gateway_connected.then_some(connection_id).flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_api_key_plan_builds_canonical_provider_and_params() {
        let plan = plan_provider_set_api_key(
            true,
            Some(42),
            Some("workspace".to_owned()),
            "AWS_Bedrock".to_owned(),
            "secret".to_owned(),
        );

        let ProviderSetApiKeyPlan::Send(request) = plan else {
            panic!("expected send plan");
        };
        assert_eq!(request.connection_id, 42);
        assert_eq!(request.canonical_provider_id, "bedrock");
        assert_eq!(request.params.workspace_id, "workspace");
        assert_eq!(request.params.provider, "AWS_Bedrock");
        assert_eq!(request.params.api_key, "secret");
    }

    #[test]
    fn delete_api_key_plan_reports_unavailable_reasons() {
        assert!(matches!(
            plan_provider_delete_api_key(
                true,
                None,
                Some("workspace".to_owned()),
                "openai".to_owned()
            ),
            ProviderDeleteApiKeyPlan::Unavailable(
                ProviderApiKeyActionUnavailable::GatewayNotConnected
            )
        ));
        assert!(matches!(
            plan_provider_delete_api_key(true, Some(7), None, "openai".to_owned()),
            ProviderDeleteApiKeyPlan::Unavailable(
                ProviderApiKeyActionUnavailable::WorkspaceNotSelected
            )
        ));
    }

    #[test]
    fn cli_runtime_login_start_plan_builds_runtime_params() {
        let plan = plan_cli_runtime_login_start(
            true,
            Some(42),
            Some("workspace".to_owned()),
            "codex".to_owned(),
            CLIRuntimeLoginStartType::ChatgptDeviceCode,
        );

        let CLIRuntimeLoginStartPlan::Send(request) = plan else {
            panic!("expected send plan");
        };
        assert_eq!(request.connection_id, 42);
        assert_eq!(request.runtime_id, "codex");
        assert_eq!(request.params.workspace_id, "workspace");
        assert_eq!(request.params.runtime_id, "codex");
        assert_eq!(
            request.params.login_type,
            CLIRuntimeLoginStartType::ChatgptDeviceCode
        );
    }

    #[test]
    fn cli_runtime_login_start_plan_reports_unavailable_reasons() {
        assert!(matches!(
            plan_cli_runtime_login_start(
                false,
                None,
                Some("workspace".to_owned()),
                "codex".to_owned(),
                CLIRuntimeLoginStartType::ChatgptDeviceCode
            ),
            CLIRuntimeLoginStartPlan::Unavailable(
                ProviderApiKeyActionUnavailable::GatewayNotConnected
            )
        ));
        assert!(matches!(
            plan_cli_runtime_login_start(
                true,
                Some(7),
                None,
                "codex".to_owned(),
                CLIRuntimeLoginStartType::ChatgptDeviceCode
            ),
            CLIRuntimeLoginStartPlan::Unavailable(
                ProviderApiKeyActionUnavailable::WorkspaceNotSelected
            )
        ));
    }

    #[test]
    fn api_key_result_helpers_update_provider_state() {
        let mut providers = ProviderListState::default();

        mark_provider_api_key_action_started(&mut providers);
        apply_provider_set_api_key_success(&mut providers, "openai".to_owned());
        assert!(providers.is_configured("openai"));
        assert!(providers.error().is_none());

        apply_provider_api_key_failure(&mut providers, "failed".to_owned());
        assert_eq!(providers.error(), Some("failed"));

        apply_provider_delete_api_key_success(&mut providers, "openai");
        assert!(!providers.is_configured("openai"));
        assert!(providers.error().is_none());
    }

    #[test]
    fn api_key_action_connection_guard_detects_stale_results() {
        assert!(provider_api_key_action_matches_connection(9, Some(9)));
        assert!(!provider_api_key_action_matches_connection(9, Some(10)));
        assert!(!provider_api_key_action_matches_connection(9, None));
    }
}
