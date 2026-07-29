use std::sync::Arc;

use pioneer_protocol::{PersistedActorRef, PrincipalKind, RequestId};

use crate::auth::AuthenticatedSessionPrincipal;
use crate::session::ConnectionId;

const UNKNOWN_RPC_METHOD: &str = "rpc/unknown";
const MAX_CANONICAL_RPC_METHOD_LEN: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AuthorizationDecision {
    AllowSuperuser,
    DenyUnsupportedPrincipal,
}

impl AuthorizationDecision {
    pub(crate) const fn is_allowed(self) -> bool {
        matches!(self, Self::AllowSuperuser)
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::AllowSuperuser => "allow_superuser",
            Self::DenyUnsupportedPrincipal => "deny_unsupported_principal",
        }
    }
}

pub(crate) fn authorize_principal(
    principal: &AuthenticatedSessionPrincipal,
) -> AuthorizationDecision {
    match principal.kind {
        PrincipalKind::Superuser => AuthorizationDecision::AllowSuperuser,
        _ => AuthorizationDecision::DenyUnsupportedPrincipal,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ConnectionContext {
    connection_id: ConnectionId,
    principal: Arc<AuthenticatedSessionPrincipal>,
}

impl ConnectionContext {
    pub(crate) fn new(
        connection_id: ConnectionId,
        principal: Arc<AuthenticatedSessionPrincipal>,
    ) -> Self {
        Self {
            connection_id,
            principal,
        }
    }

    pub(crate) const fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    pub(crate) fn principal(&self) -> &AuthenticatedSessionPrincipal {
        self.principal.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn principal_arc(&self) -> &Arc<AuthenticatedSessionPrincipal> {
        &self.principal
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalMethod(Arc<str>);

impl CanonicalMethod {
    pub(crate) fn rpc(method: &str) -> Self {
        if is_canonical_rpc_method(method) {
            return Self(Arc::from(method));
        }

        Self(Arc::from(UNKNOWN_RPC_METHOD))
    }

    pub(crate) fn binary(method: &'static str) -> Self {
        Self(Arc::from(method))
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RequestContext {
    connection: ConnectionContext,
    request_id: Option<RequestId>,
    method: CanonicalMethod,
}

impl RequestContext {
    pub(crate) fn new(
        connection: &ConnectionContext,
        request_id: Option<RequestId>,
        method: CanonicalMethod,
    ) -> Self {
        Self {
            connection: connection.clone(),
            request_id,
            method,
        }
    }

    pub(crate) const fn connection_id(&self) -> ConnectionId {
        self.connection.connection_id()
    }

    pub(crate) fn connection(&self) -> &ConnectionContext {
        &self.connection
    }

    pub(crate) fn principal(&self) -> &AuthenticatedSessionPrincipal {
        self.connection.principal()
    }

    #[cfg(test)]
    pub(crate) fn principal_arc(&self) -> &Arc<AuthenticatedSessionPrincipal> {
        self.connection.principal_arc()
    }

    pub(crate) fn request_id(&self) -> Option<&RequestId> {
        self.request_id.as_ref()
    }

    pub(crate) fn canonical_method(&self) -> &CanonicalMethod {
        &self.method
    }

    pub(crate) fn persisted_actor(&self) -> PersistedActorRef {
        PersistedActorRef::Principal(self.principal().principal_id.clone())
    }

    pub(crate) fn authorization_decision(&self) -> AuthorizationDecision {
        authorize_principal(self.principal())
    }

    pub(crate) fn request_span(&self) -> tracing::Span {
        request_span(
            self.connection(),
            self.request_id(),
            self.canonical_method(),
            self.authorization_decision(),
        )
    }
}

pub(crate) fn request_span(
    connection: &ConnectionContext,
    request_id: Option<&RequestId>,
    canonical_method: &CanonicalMethod,
    authorization_decision: AuthorizationDecision,
) -> tracing::Span {
    let principal = connection.principal();
    tracing::debug_span!(
        "gateway_request",
        gateway_id = %principal.gateway_id,
        principal_id = %principal.principal_id,
        principal_kind = ?principal.kind,
        device_id = %principal.device_id,
        auth_session_id = %principal.session_id,
        connection_id = connection.connection_id(),
        canonical_method = canonical_method.as_str(),
        request_id = request_id.map(RequestId::as_str).unwrap_or("unavailable"),
        authorization_decision = authorization_decision.as_str(),
    )
}

fn is_canonical_rpc_method(method: &str) -> bool {
    !method.is_empty()
        && method.len() <= MAX_CANONICAL_RPC_METHOD_LEN
        && method.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'/' | b'_' | b'-' | b'.')
        })
}

#[cfg(test)]
mod tests {
    use super::{
        AuthorizationDecision, CanonicalMethod, ConnectionContext, RequestContext,
        authorize_principal,
    };
    use crate::auth::AuthenticatedSessionPrincipal;
    use pioneer_protocol::{
        AuthSessionId, DeviceId, GatewayId, PersistedActorRef, PrincipalId, PrincipalKind,
        RequestId,
    };
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct TraceBuffer(Arc<Mutex<Vec<u8>>>);

    struct TraceWriter(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for TraceWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("trace buffer lock")
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for TraceBuffer {
        type Writer = TraceWriter;

        fn make_writer(&'writer self) -> Self::Writer {
            TraceWriter(self.0.clone())
        }
    }

    fn principal(id_byte: char) -> Arc<AuthenticatedSessionPrincipal> {
        Arc::new(AuthenticatedSessionPrincipal {
            gateway_id: GatewayId::new("G".repeat(21)).expect("gateway id"),
            principal_id: PrincipalId::new(id_byte.to_string().repeat(21)).expect("principal id"),
            kind: PrincipalKind::Superuser,
            role_key: None,
            device_id: DeviceId::new("D".repeat(21)).expect("device id"),
            session_id: AuthSessionId::new("S".repeat(21)).expect("session id"),
            access_jti: "J".repeat(21),
            access_expires_at_unix: u64::MAX,
        })
    }

    #[test]
    fn request_context_preserves_server_connection_and_principal() {
        let principal = principal('P');
        let connection = ConnectionContext::new(42, principal.clone());
        let request_id = RequestId::new("R".repeat(21)).expect("request id");

        let context = RequestContext::new(
            &connection,
            Some(request_id.clone()),
            CanonicalMethod::rpc("thread/start"),
        );

        assert_eq!(context.connection_id(), 42);
        assert!(Arc::ptr_eq(context.principal_arc(), &principal));
        assert_eq!(context.principal().principal_id, principal.principal_id);
        assert_eq!(context.request_id(), Some(&request_id));
        assert_eq!(context.canonical_method().as_str(), "thread/start");
        assert_eq!(
            context.persisted_actor(),
            PersistedActorRef::Principal(principal.principal_id.clone())
        );
    }

    #[test]
    fn client_shaped_identity_fields_cannot_replace_context_values() {
        let connection_principal = principal('P');
        let attacker_principal = principal('A');
        let connection = ConnectionContext::new(7, connection_principal.clone());
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "R".repeat(21),
            "method": "workspace/list",
            "params": {
                "connection_id": 999,
                "principal_id": attacker_principal.principal_id.to_string(),
                "actor": "system",
                "method": "gateway/settings/update"
            }
        });
        let request: pioneer_protocol::JsonRpcRequest =
            serde_json::from_value(payload).expect("valid request");

        let context = RequestContext::new(
            &connection,
            Some(request.id),
            CanonicalMethod::rpc(request.method.as_str()),
        );

        assert_eq!(context.connection_id(), 7);
        assert!(Arc::ptr_eq(context.principal_arc(), &connection_principal));
        assert_eq!(context.canonical_method().as_str(), "workspace/list");
    }

    #[test]
    fn unsafe_or_oversized_rpc_methods_receive_fixed_unknown_tag() {
        assert_eq!(
            CanonicalMethod::rpc("workspace/list\nforged=true").as_str(),
            "rpc/unknown"
        );
        assert_eq!(
            CanonicalMethod::rpc("x".repeat(129).as_str()).as_str(),
            "rpc/unknown"
        );
    }

    #[test]
    fn authorization_allows_only_superuser_and_fails_user_closed() {
        let superuser = principal('P');
        assert_eq!(
            authorize_principal(superuser.as_ref()),
            AuthorizationDecision::AllowSuperuser
        );

        let mut user = (*principal('U')).clone();
        user.kind = PrincipalKind::User;
        assert_eq!(
            authorize_principal(&user),
            AuthorizationDecision::DenyUnsupportedPrincipal
        );
        assert!(!AuthorizationDecision::DenyUnsupportedPrincipal.is_allowed());
        assert!(AuthorizationDecision::AllowSuperuser.is_allowed());
    }

    #[test]
    fn request_span_has_bounded_identity_fields_and_never_logs_payload_or_credentials() {
        let principal = principal('P');
        let connection = ConnectionContext::new(42, principal);
        let request_id = RequestId::new("R".repeat(21)).expect("request id");
        let context = RequestContext::new(
            &connection,
            Some(request_id),
            CanonicalMethod::rpc("workspace/list"),
        );
        let malicious_method = CanonicalMethod::rpc(
            "workspace/list Authorization: Bearer raw-jwt signing_secret=raw-signing \
             display_name=Private nickname=secret prompt=private file_payload=bytes",
        );
        let buffer = TraceBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .with_writer(buffer.clone())
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            {
                let span = context.request_span();
                let _guard = span.enter();
                tracing::debug!("request dispatch test");
            }
            {
                let span = super::request_span(
                    &connection,
                    None,
                    &malicious_method,
                    AuthorizationDecision::AllowSuperuser,
                );
                let _guard = span.enter();
                tracing::warn!("request rejection test");
            }
        });
        let bytes = buffer.0.lock().expect("trace buffer lock").clone();
        let output = String::from_utf8(bytes).expect("trace output should be UTF-8");

        for required in [
            "gateway_id=G",
            "principal_id=P",
            "principal_kind=Superuser",
            "device_id=D",
            "auth_session_id=S",
            "connection_id=42",
            "canonical_method=\"workspace/list\"",
            "request_id=\"RRRRRRRRRRRRRRRRRRRRR\"",
            "authorization_decision=\"allow_superuser\"",
            "canonical_method=\"rpc/unknown\"",
        ] {
            assert!(
                output.contains(required),
                "request span omitted `{required}`: {output}"
            );
        }
        for forbidden in [
            "raw-jwt",
            "raw-signing",
            "Private",
            "nickname=secret",
            "prompt=private",
            "file_payload=bytes",
            "Authorization:",
            "Bearer",
        ] {
            assert!(
                !output.contains(forbidden),
                "request span leaked forbidden value `{forbidden}`: {output}"
            );
        }
    }
}
