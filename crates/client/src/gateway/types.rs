//! Gateway endpoint and registry types.

use pioneer_protocol::GatewayId;
use serde::{Deserialize, Serialize};

use super::endpoint::GatewayBaseUrl;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GatewayEndpointKind {
    Local,
    Remote,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayEndpoint {
    pub id: String,
    pub name: String,
    pub gateway_base_url: GatewayBaseUrl,
    pub kind: GatewayEndpointKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_gateway_id: Option<GatewayId>,
    #[serde(default)]
    pub workspace_id: Option<String>,
    pub service_name: Option<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayRegistry {
    #[serde(deserialize_with = "deserialize_registry_v3")]
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub installation_id: Option<String>,
    pub active_gateway_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local: Option<GatewayEndpoint>,
    #[serde(default)]
    pub remotes: Vec<GatewayEndpoint>,
}

fn deserialize_registry_v3<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let version = u32::deserialize(deserializer)?;
    if version == super::registry::CURRENT_GATEWAY_REGISTRY_VERSION {
        Ok(version)
    } else {
        Err(serde::de::Error::custom("unsupported Gateway registry version"))
    }
}
