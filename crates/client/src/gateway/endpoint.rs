//! Canonical Gateway endpoint contract re-exported from `pioneer-protocol`.

pub use pioneer_protocol::{
    GatewayBaseUrl, GatewayBaseUrlError, GatewayTransportSecurity, PIONEER_PROTOCOL_VERSION,
    PIONEER_PROTOCOL_VERSION_HEADER,
};

pub(crate) use pioneer_protocol::canonical_storage_path;
