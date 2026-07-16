//! Dependency-safe primitives for the private CLI MCP byte bridge.
//!
//! This crate intentionally owns transport contracts only. It must not depend
//! on Gateway business types, MCP runtimes, persistence, or tool policy.

pub mod artifacts;
pub mod bootstrap;
pub mod framing;
pub mod helper;
pub mod platform;

pub use artifacts::{
    PrivateArtifactError, PrivateBootstrapArtifact, PrivateSessionDirectory,
    create_private_session_directory,
};
pub use bootstrap::{
    AttachRequest, BootstrapDecodeError, BootstrapDocument, BootstrapEncodeError, BootstrapNonce,
    BridgeEndpoint, BridgeEndpointKind, BridgeGeneration, BridgeSessionId, MAX_BOOTSTRAP_BYTES,
    NONCE_BYTES,
};
pub use framing::{
    BRIDGE_FRAME_MAGIC, BRIDGE_FRAME_VERSION, BridgeFrame, BridgeFrameError, BridgeFrameType,
    FRAME_HEADER_BYTES, MAX_FRAME_PAYLOAD_BYTES, decode_frame, decode_frame_with_limit,
    encode_frame, read_frame, read_frame_with_limit, write_frame,
};
pub use platform::{
    BridgeFrameTransport, PeerIdentity, PlatformConnection, PlatformListener,
    PrivateEndpointConfig, PrivateIpcError, bind_private_endpoint, connect_private_endpoint,
    private_endpoint_descriptor,
};

#[cfg(test)]
mod tests {
    #[test]
    fn dependency_boundary_excludes_gateway_business_crates() {
        let manifest = include_str!("../Cargo.toml");
        for forbidden in [
            "pioneer-gateway",
            "pioneer-tools",
            "pioneer-crud",
            "pioneer-mcp",
            "rmcp",
        ] {
            assert!(
                !manifest.contains(forbidden),
                "bridge manifest must not depend on {forbidden}"
            );
        }
    }
}
