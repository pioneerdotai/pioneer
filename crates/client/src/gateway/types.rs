//! Gateway endpoint and registry types.

use pioneer_protocol::GatewayId;
use serde::{Deserialize, Serialize};

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
    pub address: String,
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
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub installation_id: Option<String>,
    pub active_gateway_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local: Option<GatewayEndpoint>,
    #[serde(default)]
    pub remotes: Vec<GatewayEndpoint>,
}
