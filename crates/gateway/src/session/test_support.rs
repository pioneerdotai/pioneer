use std::sync::Arc;

use pioneer_protocol::{GatewayId, PrincipalId, PrincipalKind};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use super::{ConnectionId, SessionManager};
use crate::auth::{AuthenticatedPrincipal, CredentialKind};

pub(crate) const TEST_GATEWAY_ID: &str = "G00000000000000000001";
pub(crate) const TEST_SUPERUSER_PRINCIPAL_ID: &str = "P00000000000000000001";

pub(crate) fn authenticated_test_superuser() -> Arc<AuthenticatedPrincipal> {
    Arc::new(AuthenticatedPrincipal {
        gateway_id: GatewayId::new(TEST_GATEWAY_ID).expect("valid deterministic Gateway id"),
        principal_id: PrincipalId::new(TEST_SUPERUSER_PRINCIPAL_ID)
            .expect("valid deterministic Principal id"),
        kind: PrincipalKind::Superuser,
        role_key: None,
        credential_kind: CredentialKind::LegacySuperuserJwt,
    })
}

pub(crate) async fn register_authenticated_test_connection(
    manager: &SessionManager,
    sender: mpsc::Sender<Message>,
) -> ConnectionId {
    manager
        .register_connection(sender, authenticated_test_superuser())
        .await
}
