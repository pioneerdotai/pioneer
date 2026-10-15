use pioneer_client::gateway::{
    migration::{GatewayRegistryLoad, GatewayRegistryLoadError, load_registry_json},
    types::GatewayRegistry,
};
use serde::{Deserialize, Serialize};
pub const CLIENT_NOT_INITIALIZED_CODE: &str = "client_not_initialized";
pub const GATEWAY_DISCONNECTED_CODE: &str = "gateway_disconnected";
#[cfg(test)]
pub const VOICE_RECONFIGURATION_BUSY_CODE: &str = "voice_reconfiguration_busy";
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoadGatewayRegistryRequest {
    pub document: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum LoadGatewayRegistryResult {
    Current { registry: GatewayRegistry },
    Migrated { registry: GatewayRegistry },
    ReconfigurationRequired { endpoint_ids: Vec<String> },
}

pub fn load_gateway_registry_request(
    request: LoadGatewayRegistryRequest,
) -> Result<LoadGatewayRegistryResult, GatewayRegistryLoadError> {
    Ok(match load_registry_json(request.document.as_str())? {
        GatewayRegistryLoad::Current(registry) => LoadGatewayRegistryResult::Current { registry },
        GatewayRegistryLoad::Migrated(registry) => LoadGatewayRegistryResult::Migrated { registry },
        GatewayRegistryLoad::ReconfigurationRequired { endpoint_ids } => {
            LoadGatewayRegistryResult::ReconfigurationRequired { endpoint_ids }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_migration_result_is_typed_and_secret_free() {
        let result = load_gateway_registry_request(LoadGatewayRegistryRequest {
            document: r#"{
                "version":2,
                "active_gateway_id":"custom",
                "remotes":[{"id":"custom","name":"Custom","address":"wss://relay.example/socket","kind":"remote"}]
            }"#
            .to_owned(),
        })
        .unwrap();
        assert_eq!(
            result,
            LoadGatewayRegistryResult::ReconfigurationRequired {
                endpoint_ids: vec!["custom".to_owned()]
            }
        );
        let encoded = serde_json::to_string(&result).unwrap();
        assert!(!encoded.contains("wss://"));
        assert!(!encoded.contains("credential"));
    }
}
