use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GatewayEndpointKind {
    Local,
    Remote,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GatewayEndpoint {
    pub id: String,
    pub name: String,
    pub address: String,
    pub kind: GatewayEndpointKind,
    #[serde(default)]
    pub auth_token: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
    pub service_name: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GatewayRegistry {
    pub version: u32,
    pub active_gateway_id: Option<String>,
    pub local: GatewayEndpoint,
    #[serde(default)]
    pub remotes: Vec<GatewayEndpoint>,
}
