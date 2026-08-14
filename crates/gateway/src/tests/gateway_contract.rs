//! Deterministic, process-free Gateway contract fixtures.
//!
//! This module intentionally contains no production implementation. The
//! matrices and fixtures exercise the real Router, transports and storage
//! services from deterministic unit tests.

use std::{
    io::{Error, ErrorKind, SeekFrom},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use axum::{
    Router,
    body::Body,
    http::{Request, Response, StatusCode},
};
use tokio::io::{AsyncRead, AsyncSeek, DuplexStream, ReadBuf, duplex};
use tower::ServiceExt;

pub(crate) const PROTOCOL_VERSION_HEADER: &str = "Pioneer-Protocol-Version";
pub(crate) const PROTOCOL_VERSION_V1: &str = "1";
// A 256-bit URL-safe secret has 43 unpadded base64url characters. Keep the
// redaction fixture shape-identical to a real grant without using a real one.
pub(crate) const TEST_VIEW_GRANT: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
pub(crate) const TEST_AUTHORIZATION: &str =
    "Bearer test_access_header.test_access_payload.test_access_signature";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TestMethod {
    Get,
    Head,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RouteOwner {
    WebSocket,
    ArtifactContent,
    ArtifactProjection,
    ViewGrant,
    MemberAvatar,
    AgentAvatar,
    Health,
    Readiness,
    ReservedWebhooks,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RouteAuthentication {
    AccessOrRestricted,
    BearerAccess,
    ServerBoundViewGrant,
    None,
    Unimplemented,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RouteVersion {
    ExactHeaderV1,
    ServerBoundV1,
    Unversioned,
    None,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RouteContract {
    pub methods: &'static [TestMethod],
    pub path: &'static str,
    pub owner: RouteOwner,
    pub authentication: RouteAuthentication,
    pub version: RouteVersion,
    pub registered: bool,
}

const GET: &[TestMethod] = &[TestMethod::Get];
const GET_HEAD: &[TestMethod] = &[TestMethod::Get, TestMethod::Head];

pub(crate) const ROUTE_CONTRACTS: &[RouteContract] = &[
    RouteContract {
        methods: GET,
        path: "/",
        owner: RouteOwner::WebSocket,
        authentication: RouteAuthentication::AccessOrRestricted,
        version: RouteVersion::ExactHeaderV1,
        registered: true,
    },
    RouteContract {
        methods: GET_HEAD,
        path: "/storage/workspaces/{workspace_id}/artifacts/{artifact_id}/versions/{version_id}/content",
        owner: RouteOwner::ArtifactContent,
        authentication: RouteAuthentication::BearerAccess,
        version: RouteVersion::ExactHeaderV1,
        registered: true,
    },
    RouteContract {
        methods: GET_HEAD,
        path: "/storage/workspaces/{workspace_id}/artifacts/{artifact_id}/versions/{version_id}/projections/{projection_kind}",
        owner: RouteOwner::ArtifactProjection,
        authentication: RouteAuthentication::BearerAccess,
        version: RouteVersion::ExactHeaderV1,
        registered: true,
    },
    RouteContract {
        methods: GET_HEAD,
        path: "/storage/views/{opaque_grant}",
        owner: RouteOwner::ViewGrant,
        authentication: RouteAuthentication::ServerBoundViewGrant,
        version: RouteVersion::ServerBoundV1,
        registered: true,
    },
    RouteContract {
        methods: GET_HEAD,
        path: "/storage/members/{principal_id}/avatar/{avatar_revision}",
        owner: RouteOwner::MemberAvatar,
        authentication: RouteAuthentication::BearerAccess,
        version: RouteVersion::ExactHeaderV1,
        registered: true,
    },
    RouteContract {
        methods: GET_HEAD,
        path: "/storage/system/agent/avatar/{avatar_revision}",
        owner: RouteOwner::AgentAvatar,
        authentication: RouteAuthentication::BearerAccess,
        version: RouteVersion::ExactHeaderV1,
        registered: true,
    },
    RouteContract {
        methods: GET,
        path: "/health",
        owner: RouteOwner::Health,
        authentication: RouteAuthentication::None,
        version: RouteVersion::Unversioned,
        registered: true,
    },
    RouteContract {
        methods: GET,
        path: "/ready",
        owner: RouteOwner::Readiness,
        authentication: RouteAuthentication::None,
        version: RouteVersion::Unversioned,
        registered: true,
    },
    RouteContract {
        methods: GET,
        path: "/webhooks/{future_path}",
        owner: RouteOwner::ReservedWebhooks,
        authentication: RouteAuthentication::Unimplemented,
        version: RouteVersion::None,
        registered: false,
    },
];

pub(crate) const REJECTED_WS_PATHS: &[&str] =
    &["/ws", "/socket", "/api/v1/ws", "/arbitrary-custom-path"];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProtocolHeaderCase {
    ExactV1,
    Missing,
    Duplicate,
    Malformed,
    Unsupported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CredentialCase {
    Access,
    Refresh,
    DeviceActivation,
    Invitation,
    Missing,
    Duplicate,
    Malformed,
    InvalidOrExpired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WsAdmissionOutcome {
    NormalUpgrade,
    RestrictedUpgrade,
    Reject(StatusCode),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WsAdmissionCase {
    pub name: &'static str,
    pub path: &'static str,
    pub protocol: ProtocolHeaderCase,
    pub credential: CredentialCase,
    pub standard_upgrade_headers_valid: bool,
    pub outcome: WsAdmissionOutcome,
}

pub(crate) const WS_ADMISSION_CASES: &[WsAdmissionCase] = &[
    ws_case(
        "access",
        ProtocolHeaderCase::ExactV1,
        CredentialCase::Access,
        true,
        WsAdmissionOutcome::NormalUpgrade,
    ),
    ws_case(
        "refresh",
        ProtocolHeaderCase::ExactV1,
        CredentialCase::Refresh,
        true,
        WsAdmissionOutcome::RestrictedUpgrade,
    ),
    ws_case(
        "activation",
        ProtocolHeaderCase::ExactV1,
        CredentialCase::DeviceActivation,
        true,
        WsAdmissionOutcome::RestrictedUpgrade,
    ),
    ws_case(
        "invitation",
        ProtocolHeaderCase::ExactV1,
        CredentialCase::Invitation,
        true,
        WsAdmissionOutcome::RestrictedUpgrade,
    ),
    ws_case(
        "missing_version",
        ProtocolHeaderCase::Missing,
        CredentialCase::Access,
        true,
        WsAdmissionOutcome::Reject(StatusCode::BAD_REQUEST),
    ),
    ws_case(
        "duplicate_version",
        ProtocolHeaderCase::Duplicate,
        CredentialCase::Access,
        true,
        WsAdmissionOutcome::Reject(StatusCode::BAD_REQUEST),
    ),
    ws_case(
        "malformed_version",
        ProtocolHeaderCase::Malformed,
        CredentialCase::Access,
        true,
        WsAdmissionOutcome::Reject(StatusCode::BAD_REQUEST),
    ),
    ws_case(
        "unsupported_version",
        ProtocolHeaderCase::Unsupported,
        CredentialCase::Access,
        true,
        WsAdmissionOutcome::Reject(StatusCode::BAD_REQUEST),
    ),
    ws_case(
        "missing_credential",
        ProtocolHeaderCase::ExactV1,
        CredentialCase::Missing,
        true,
        WsAdmissionOutcome::Reject(StatusCode::UNAUTHORIZED),
    ),
    ws_case(
        "duplicate_credential",
        ProtocolHeaderCase::ExactV1,
        CredentialCase::Duplicate,
        true,
        WsAdmissionOutcome::Reject(StatusCode::UNAUTHORIZED),
    ),
    ws_case(
        "malformed_credential",
        ProtocolHeaderCase::ExactV1,
        CredentialCase::Malformed,
        true,
        WsAdmissionOutcome::Reject(StatusCode::UNAUTHORIZED),
    ),
    ws_case(
        "invalid_access",
        ProtocolHeaderCase::ExactV1,
        CredentialCase::InvalidOrExpired,
        true,
        WsAdmissionOutcome::Reject(StatusCode::UNAUTHORIZED),
    ),
    ws_case(
        "invalid_ws_headers",
        ProtocolHeaderCase::ExactV1,
        CredentialCase::Access,
        false,
        WsAdmissionOutcome::Reject(StatusCode::BAD_REQUEST),
    ),
    WsAdmissionCase {
        name: "legacy_path",
        path: "/ws",
        protocol: ProtocolHeaderCase::ExactV1,
        credential: CredentialCase::Access,
        standard_upgrade_headers_valid: true,
        outcome: WsAdmissionOutcome::Reject(StatusCode::NOT_FOUND),
    },
];

const fn ws_case(
    name: &'static str,
    protocol: ProtocolHeaderCase,
    credential: CredentialCase,
    standard_upgrade_headers_valid: bool,
    outcome: WsAdmissionOutcome,
) -> WsAdmissionCase {
    WsAdmissionCase {
        name,
        path: "/",
        protocol,
        credential,
        standard_upgrade_headers_valid,
        outcome,
    }
}

pub(crate) const CLOSE_ACCESS_EXPIRED: u16 = 4401;
pub(crate) const CLOSE_RESTRICTED_DONE: u16 = 4403;
pub(crate) const CLOSE_RESTRICTED_INVALID: u16 = 4400;
pub(crate) const CLOSE_RESTRICTED_TIMEOUT: u16 = 4408;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HttpFailure {
    UnknownPath,
    OrdinaryRoot,
    InvalidProtocolHeader,
    InvalidWebSocketHeaders,
    InvalidAccess,
    HiddenOrMissingResource,
    DisclosedForbidden,
    ExpiredOrRevokedGrant,
    MalformedRange,
    UnsatisfiableRange,
    MultipleRanges,
    StorageInvariant,
    PerScopeCapacity,
    GlobalCapacity,
    TemporaryBackend,
}

pub(crate) const fn status_for_failure(failure: HttpFailure) -> StatusCode {
    match failure {
        HttpFailure::UnknownPath | HttpFailure::ExpiredOrRevokedGrant => StatusCode::NOT_FOUND,
        HttpFailure::OrdinaryRoot
        | HttpFailure::InvalidProtocolHeader
        | HttpFailure::InvalidWebSocketHeaders
        | HttpFailure::MalformedRange
        | HttpFailure::MultipleRanges => StatusCode::BAD_REQUEST,
        HttpFailure::InvalidAccess => StatusCode::UNAUTHORIZED,
        HttpFailure::HiddenOrMissingResource => StatusCode::NOT_FOUND,
        HttpFailure::DisclosedForbidden => StatusCode::FORBIDDEN,
        HttpFailure::UnsatisfiableRange => StatusCode::RANGE_NOT_SATISFIABLE,
        HttpFailure::StorageInvariant => StatusCode::INTERNAL_SERVER_ERROR,
        HttpFailure::TemporaryBackend => StatusCode::SERVICE_UNAVAILABLE,
        HttpFailure::PerScopeCapacity => StatusCode::TOO_MANY_REQUESTS,
        HttpFailure::GlobalCapacity => StatusCode::SERVICE_UNAVAILABLE,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ContentDispositionPolicy {
    Inline,
    Attachment,
}

pub(crate) fn disposition_for_mime(mime: &str) -> ContentDispositionPolicy {
    match mime.as_bytes() {
        b"image/png" | b"image/jpeg" | b"image/webp" | b"image/gif" | b"application/pdf"
        | b"text/plain" | b"audio/mpeg" | b"audio/ogg" | b"video/mp4" | b"video/webm" => {
            ContentDispositionPolicy::Inline
        }
        _ => ContentDispositionPolicy::Attachment,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CachePolicy {
    BearerExactVersion,
    Projection,
    ViewGrant,
    AvatarRevision,
}

pub(crate) const fn cache_control(policy: CachePolicy) -> &'static str {
    match policy {
        CachePolicy::BearerExactVersion | CachePolicy::Projection => "private, no-cache",
        CachePolicy::ViewGrant => "private, no-store",
        CachePolicy::AvatarRevision => "private, max-age=31536000, immutable",
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EntityTagSource {
    ArtifactSha256,
    ProjectionRevision,
    ViewGrantArtifactSha256,
    AvatarRevision,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EntityTagContract {
    pub source: EntityTagSource,
    pub strong: bool,
    pub supports_if_none_match: bool,
    pub supports_if_range: bool,
}

pub(crate) const ENTITY_TAG_CONTRACTS: &[EntityTagContract] = &[
    EntityTagContract {
        source: EntityTagSource::ArtifactSha256,
        strong: true,
        supports_if_none_match: true,
        supports_if_range: true,
    },
    EntityTagContract {
        source: EntityTagSource::ProjectionRevision,
        strong: true,
        supports_if_none_match: true,
        supports_if_range: true,
    },
    EntityTagContract {
        source: EntityTagSource::ViewGrantArtifactSha256,
        strong: true,
        supports_if_none_match: true,
        supports_if_range: true,
    },
    EntityTagContract {
        source: EntityTagSource::AvatarRevision,
        strong: true,
        supports_if_none_match: true,
        supports_if_range: false,
    },
];

pub(crate) fn strong_etag(value: &str) -> String {
    assert!(
        !value.is_empty()
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "ETag fixture must be a bounded safe revision"
    );
    format!("\"{value}\"")
}

pub(crate) const CONTENT_SECURITY_HEADERS: &[(&str, &str)] = &[
    ("x-content-type-options", "nosniff"),
    ("content-security-policy", "sandbox; default-src 'none'"),
];
pub(crate) const VIEW_SECURITY_HEADERS: &[(&str, &str)] = &[
    ("x-content-type-options", "nosniff"),
    ("referrer-policy", "no-referrer"),
    ("content-security-policy", "sandbox; default-src 'none'"),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GatewayContractTestLimits {
    pub ws_message_bytes: usize,
    pub ws_frame_bytes: usize,
    pub request_header_bytes: usize,
    pub artifact_streams_global: usize,
    pub artifact_streams_per_session: usize,
    pub artifact_streams_per_grant: usize,
    pub open_handles: usize,
    pub max_single_range_bytes: u64,
    pub header_auth_open_timeout: Duration,
    pub body_idle_timeout: Duration,
    pub grant_ttl: Duration,
    pub restricted_request_bytes: usize,
    pub restricted_response_bytes: usize,
}

impl GatewayContractTestLimits {
    pub(crate) const fn deterministic() -> Self {
        Self {
            ws_message_bytes: 64 * 1024,
            ws_frame_bytes: 32 * 1024,
            request_header_bytes: 8 * 1024,
            artifact_streams_global: 4,
            artifact_streams_per_session: 2,
            artifact_streams_per_grant: 1,
            open_handles: 4,
            max_single_range_bytes: 256 * 1024,
            header_auth_open_timeout: Duration::from_secs(2),
            body_idle_timeout: Duration::from_secs(3),
            grant_ttl: Duration::from_secs(3 * 60),
            restricted_request_bytes: 64 * 1024,
            restricted_response_bytes: 256 * 1024,
        }
    }
}

#[derive(Debug)]
pub(crate) struct DeterministicClock {
    now_unix: std::sync::atomic::AtomicU64,
}

impl DeterministicClock {
    pub(crate) const fn new(now_unix: u64) -> Self {
        Self {
            now_unix: std::sync::atomic::AtomicU64::new(now_unix),
        }
    }

    pub(crate) fn now_unix(&self) -> u64 {
        self.now_unix.load(Ordering::SeqCst)
    }

    pub(crate) fn advance(&self, seconds: u64) -> u64 {
        self.now_unix.fetch_add(seconds, Ordering::SeqCst) + seconds
    }
}

pub(crate) const TEST_GATEWAY_ID: &str = "G00000000000000000001";
pub(crate) const TEST_PRINCIPAL_ID: &str = "P00000000000000000001";
pub(crate) const TEST_SESSION_ID: &str = "S00000000000000000001";
pub(crate) const TEST_REVOKED_SESSION_ID: &str = "S00000000000000000002";
pub(crate) const TEST_WORKSPACE_ID: &str = "W00000000000000000001";
pub(crate) const TEST_ARTIFACT_ID: &str = "A00000000000000000001";
pub(crate) const TEST_HIDDEN_ARTIFACT_ID: &str = "A00000000000000000002";
pub(crate) const TEST_VERSION_ID: &str = "V00000000000000000001";
pub(crate) const TEST_ARTIFACT_SHA256: &str =
    "6161616161616161616161616161616161616161616161616161616161616161";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FixtureSessionState {
    Active,
    Revoked,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SessionFixture {
    pub session_id: &'static str,
    pub principal_id: &'static str,
    pub state: FixtureSessionState,
}

pub(crate) fn active_session_fixture() -> SessionFixture {
    SessionFixture {
        session_id: TEST_SESSION_ID,
        principal_id: TEST_PRINCIPAL_ID,
        state: FixtureSessionState::Active,
    }
}

pub(crate) fn revoked_session_fixture() -> SessionFixture {
    SessionFixture {
        session_id: TEST_REVOKED_SESSION_ID,
        principal_id: TEST_PRINCIPAL_ID,
        state: FixtureSessionState::Revoked,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ArtifactFixture {
    pub workspace_id: &'static str,
    pub artifact_id: &'static str,
    pub version_id: &'static str,
    pub sha256: &'static str,
    pub size_bytes: u64,
    pub mime_type: &'static str,
    pub visible: bool,
}

pub(crate) fn authorized_artifact_fixture() -> ArtifactFixture {
    ArtifactFixture {
        workspace_id: TEST_WORKSPACE_ID,
        artifact_id: TEST_ARTIFACT_ID,
        version_id: TEST_VERSION_ID,
        sha256: TEST_ARTIFACT_SHA256,
        size_bytes: 4096,
        mime_type: "application/pdf",
        visible: true,
    }
}

pub(crate) fn hidden_artifact_fixture() -> ArtifactFixture {
    ArtifactFixture {
        artifact_id: TEST_HIDDEN_ARTIFACT_ID,
        visible: false,
        ..authorized_artifact_fixture()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectionFixture {
    pub artifact: ArtifactFixture,
    pub kind: &'static str,
    pub revision: &'static str,
}

pub(crate) fn projection_fixture() -> ProjectionFixture {
    ProjectionFixture {
        artifact: authorized_artifact_fixture(),
        kind: "thumbnail",
        revision: "projection-revision-test",
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AvatarFixture {
    pub principal_id: &'static str,
    pub revision: &'static str,
    pub media_type: &'static str,
    pub bytes: &'static [u8],
    pub visible: bool,
}

pub(crate) fn avatar_fixture() -> AvatarFixture {
    AvatarFixture {
        principal_id: TEST_PRINCIPAL_ID,
        revision: "avatar-revision-test",
        media_type: "image/png",
        bytes: b"test-avatar-bytes",
        visible: true,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ViewGrantFixture {
    pub raw_grant: &'static str,
    pub session_id: &'static str,
    pub artifact: ArtifactFixture,
    pub protocol_version: u16,
    pub issued_at_unix: u64,
    pub expires_at_unix: u64,
}

pub(crate) fn view_grant_fixture(now_unix: u64) -> ViewGrantFixture {
    ViewGrantFixture {
        raw_grant: TEST_VIEW_GRANT,
        session_id: TEST_SESSION_ID,
        artifact: authorized_artifact_fixture(),
        protocol_version: 1,
        issued_at_unix: now_unix,
        expires_at_unix: now_unix + 180,
    }
}

#[derive(Clone)]
pub(crate) struct InProcessRouterHarness {
    router: Router,
}

impl InProcessRouterHarness {
    pub(crate) fn new(router: Router) -> Self {
        Self { router }
    }

    pub(crate) async fn request(&self, request: Request<Body>) -> Response<Body> {
        self.router
            .clone()
            .oneshot(request)
            .await
            .expect("in-process Router request")
    }
}

pub(crate) fn fake_ws_duplex(capacity: usize) -> (DuplexStream, DuplexStream) {
    assert!(capacity > 0, "duplex capacity must be bounded and non-zero");
    duplex(capacity)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ForwardedRequestFixture {
    pub host: &'static str,
    pub request_prefix: Vec<u8>,
}

impl ForwardedRequestFixture {
    pub(crate) fn deterministic() -> Self {
        Self {
            host: "gateway.test.invalid",
            request_prefix: b"GET /storage/views/[redacted] HTTP/1.1\r\nHost: gateway.test.invalid\r\nPioneer-Protocol-Version: 1\r\n\r\n".to_vec(),
        }
    }

    pub(crate) fn forward_without_listener(&self) -> Vec<u8> {
        self.request_prefix.clone()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FakeBlobMode {
    Bytes,
    RepeatedByte,
}

pub(crate) struct FakeBlobHandle {
    bytes: Vec<u8>,
    repeated_byte: u8,
    logical_len: u64,
    position: u64,
    max_read: usize,
    fail_at: Option<u64>,
    cancelled: Arc<AtomicBool>,
    mode: FakeBlobMode,
}

impl FakeBlobHandle {
    pub(crate) fn from_bytes(bytes: Vec<u8>) -> Self {
        let logical_len = bytes.len() as u64;
        Self {
            bytes,
            repeated_byte: 0,
            logical_len,
            position: 0,
            max_read: usize::MAX,
            fail_at: None,
            cancelled: Arc::new(AtomicBool::new(false)),
            mode: FakeBlobMode::Bytes,
        }
    }

    pub(crate) fn large_logical(logical_len: u64, repeated_byte: u8) -> Self {
        Self {
            bytes: Vec::new(),
            repeated_byte,
            logical_len,
            position: 0,
            max_read: usize::MAX,
            fail_at: None,
            cancelled: Arc::new(AtomicBool::new(false)),
            mode: FakeBlobMode::RepeatedByte,
        }
    }

    pub(crate) fn with_short_reads(mut self, max_read: usize) -> Self {
        assert!(max_read > 0, "short-read limit must be non-zero");
        self.max_read = max_read;
        self
    }

    pub(crate) fn with_failure_at(mut self, offset: u64) -> Self {
        self.fail_at = Some(offset);
        self
    }

    pub(crate) fn cancellation_handle(&self) -> Arc<AtomicBool> {
        self.cancelled.clone()
    }

    pub(crate) fn logical_len(&self) -> u64 {
        self.logical_len
    }
}

impl AsyncRead for FakeBlobHandle {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.cancelled.load(Ordering::SeqCst) {
            return Poll::Ready(Err(Error::new(
                ErrorKind::Interrupted,
                "fake blob cancelled",
            )));
        }
        if self.fail_at.is_some_and(|offset| self.position >= offset) {
            return Poll::Ready(Err(Error::other("injected fake blob read failure")));
        }
        let remaining = self.logical_len.saturating_sub(self.position);
        if remaining == 0 || buffer.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let before_failure = self
            .fail_at
            .map(|offset| offset.saturating_sub(self.position))
            .unwrap_or(u64::MAX);
        let read_len = remaining
            .min(before_failure)
            .min(buffer.remaining() as u64)
            .min(self.max_read as u64) as usize;
        if read_len == 0 {
            return Poll::Ready(Err(Error::other("injected fake blob read failure")));
        }
        match self.mode {
            FakeBlobMode::Bytes => {
                let start = self.position as usize;
                buffer.put_slice(&self.bytes[start..start + read_len]);
            }
            FakeBlobMode::RepeatedByte => {
                for _ in 0..read_len {
                    buffer.put_slice(&[self.repeated_byte]);
                }
            }
        }
        self.position += read_len as u64;
        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for FakeBlobHandle {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        let next = match position {
            SeekFrom::Start(offset) => i128::from(offset),
            SeekFrom::End(offset) => i128::from(self.logical_len) + i128::from(offset),
            SeekFrom::Current(offset) => i128::from(self.position) + i128::from(offset),
        };
        if next < 0 || next > i128::from(self.logical_len) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "fake blob seek out of bounds",
            ));
        }
        self.position = next as u64;
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}

pub(crate) fn redact_sensitive_snapshot(input: &str) -> String {
    input
        .replace(TEST_AUTHORIZATION, "[redacted-authorization]")
        .replace(TEST_VIEW_GRANT, "[redacted-view-grant]")
}

pub(crate) fn assert_snapshot_redacted(rendered: &str) {
    assert!(!rendered.contains(TEST_AUTHORIZATION));
    assert!(!rendered.contains(TEST_VIEW_GRANT));
    assert!(!rendered.contains("test_access_payload"));
}

#[cfg(test)]
mod tests {
    use axum::{Router, routing::get};
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    use super::*;

    #[test]
    fn matrices_cover_canonical_and_reserved_namespaces() {
        assert_eq!(
            PROTOCOL_VERSION_HEADER,
            pioneer_protocol::PIONEER_PROTOCOL_VERSION_HEADER
        );
        assert_eq!(
            PROTOCOL_VERSION_V1,
            pioneer_protocol::PIONEER_PROTOCOL_VERSION
        );
        assert_eq!(ROUTE_CONTRACTS.len(), 9);
        assert!(
            ROUTE_CONTRACTS
                .iter()
                .all(|route| { !route.methods.is_empty() && route.path.starts_with('/') })
        );
        assert!(ROUTE_CONTRACTS.iter().any(|route| {
            route.owner == RouteOwner::ViewGrant
                && route.version == RouteVersion::ServerBoundV1
                && route.authentication == RouteAuthentication::ServerBoundViewGrant
        }));
        assert!(
            ROUTE_CONTRACTS
                .iter()
                .any(|route| { route.owner == RouteOwner::ReservedWebhooks && !route.registered })
        );
        assert!(REJECTED_WS_PATHS.contains(&"/api/v1/ws"));

        assert_eq!(WS_ADMISSION_CASES.len(), 14);
        for protocol in [
            ProtocolHeaderCase::ExactV1,
            ProtocolHeaderCase::Missing,
            ProtocolHeaderCase::Duplicate,
            ProtocolHeaderCase::Malformed,
            ProtocolHeaderCase::Unsupported,
        ] {
            assert!(
                WS_ADMISSION_CASES
                    .iter()
                    .any(|case| case.protocol == protocol)
            );
        }
        for credential in [
            CredentialCase::Access,
            CredentialCase::Refresh,
            CredentialCase::DeviceActivation,
            CredentialCase::Invitation,
            CredentialCase::Missing,
            CredentialCase::Duplicate,
            CredentialCase::Malformed,
            CredentialCase::InvalidOrExpired,
        ] {
            assert!(
                WS_ADMISSION_CASES
                    .iter()
                    .any(|case| case.credential == credential)
            );
        }
        assert!(
            WS_ADMISSION_CASES
                .iter()
                .all(|case| { !case.name.is_empty() && case.path.starts_with('/') })
        );
        assert!(WS_ADMISSION_CASES.iter().any(|case| {
            case.standard_upgrade_headers_valid && case.outcome == WsAdmissionOutcome::NormalUpgrade
        }));
        assert!(WS_ADMISSION_CASES.iter().any(|case| {
            case.standard_upgrade_headers_valid
                && case.outcome == WsAdmissionOutcome::RestrictedUpgrade
        }));
        for status in [
            StatusCode::BAD_REQUEST,
            StatusCode::UNAUTHORIZED,
            StatusCode::NOT_FOUND,
        ] {
            assert!(
                WS_ADMISSION_CASES
                    .iter()
                    .any(|case| { case.outcome == WsAdmissionOutcome::Reject(status) })
            );
        }
        assert_eq!(
            (
                CLOSE_ACCESS_EXPIRED,
                CLOSE_RESTRICTED_DONE,
                CLOSE_RESTRICTED_INVALID,
                CLOSE_RESTRICTED_TIMEOUT,
            ),
            (4401, 4403, 4400, 4408),
        );
    }

    #[test]
    fn failure_content_and_cache_matrices_are_closed() {
        for (failure, expected) in [
            (HttpFailure::UnknownPath, StatusCode::NOT_FOUND),
            (HttpFailure::OrdinaryRoot, StatusCode::BAD_REQUEST),
            (HttpFailure::InvalidProtocolHeader, StatusCode::BAD_REQUEST),
            (
                HttpFailure::InvalidWebSocketHeaders,
                StatusCode::BAD_REQUEST,
            ),
            (HttpFailure::InvalidAccess, StatusCode::UNAUTHORIZED),
            (HttpFailure::HiddenOrMissingResource, StatusCode::NOT_FOUND),
            (HttpFailure::DisclosedForbidden, StatusCode::FORBIDDEN),
            (HttpFailure::ExpiredOrRevokedGrant, StatusCode::NOT_FOUND),
            (HttpFailure::MalformedRange, StatusCode::BAD_REQUEST),
            (
                HttpFailure::UnsatisfiableRange,
                StatusCode::RANGE_NOT_SATISFIABLE,
            ),
            (HttpFailure::MultipleRanges, StatusCode::BAD_REQUEST),
            (
                HttpFailure::StorageInvariant,
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            (HttpFailure::PerScopeCapacity, StatusCode::TOO_MANY_REQUESTS),
            (HttpFailure::GlobalCapacity, StatusCode::SERVICE_UNAVAILABLE),
            (
                HttpFailure::TemporaryBackend,
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        ] {
            assert_eq!(status_for_failure(failure), expected);
        }
        assert_eq!(
            disposition_for_mime("text/html"),
            ContentDispositionPolicy::Attachment
        );
        for (policy, expected) in [
            (CachePolicy::BearerExactVersion, "private, no-cache"),
            (CachePolicy::Projection, "private, no-cache"),
            (CachePolicy::ViewGrant, "private, no-store"),
            (
                CachePolicy::AvatarRevision,
                "private, max-age=31536000, immutable",
            ),
        ] {
            assert_eq!(cache_control(policy), expected);
        }
        assert!(ENTITY_TAG_CONTRACTS.iter().all(|contract| contract.strong));
        assert_eq!(strong_etag("revision-test"), "\"revision-test\"");
        assert_eq!(
            CONTENT_SECURITY_HEADERS[0],
            ("x-content-type-options", "nosniff")
        );
        assert!(VIEW_SECURITY_HEADERS.contains(&("referrer-policy", "no-referrer")));
    }

    #[test]
    fn deterministic_fixtures_are_bounded_and_secret_safe() {
        let limits = GatewayContractTestLimits::deterministic();
        assert_eq!(limits.grant_ttl, Duration::from_secs(180));
        assert!(limits.artifact_streams_per_session <= limits.artifact_streams_global);
        let clock = DeterministicClock::new(1_800_000_000);
        assert_eq!(clock.advance(5), 1_800_000_005);
        let grant = view_grant_fixture(clock.now_unix());
        assert_eq!(grant.expires_at_unix - grant.issued_at_unix, 180);
        assert_eq!(TEST_GATEWAY_ID, "G00000000000000000001");
        assert_eq!(active_session_fixture().state, FixtureSessionState::Active);
        assert_eq!(
            revoked_session_fixture().state,
            FixtureSessionState::Revoked
        );
        assert_eq!(active_session_fixture().principal_id, TEST_PRINCIPAL_ID);
        assert_eq!(
            revoked_session_fixture().session_id,
            TEST_REVOKED_SESSION_ID
        );
        assert!(!hidden_artifact_fixture().visible);
        assert_eq!(
            hidden_artifact_fixture().artifact_id,
            TEST_HIDDEN_ARTIFACT_ID
        );
        assert_eq!(projection_fixture().kind, "thumbnail");
        assert_eq!(avatar_fixture().principal_id, TEST_PRINCIPAL_ID);
        assert_eq!(avatar_fixture().revision, "avatar-revision-test");
        let redacted = redact_sensitive_snapshot(&format!(
            "Authorization: {TEST_AUTHORIZATION}; /storage/views/{TEST_VIEW_GRANT}"
        ));
        assert_snapshot_redacted(&redacted);
    }

    #[tokio::test]
    async fn router_and_transport_seams_do_not_bind_a_listener() {
        let router = Router::new().route("/health", get(|| async { "ok" }));
        let response = InProcessRouterHarness::new(router)
            .request(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let (_client, _server) = fake_ws_duplex(1024);
        let relay = ForwardedRequestFixture::deterministic();
        assert_eq!(relay.forward_without_listener(), relay.request_prefix);
    }

    #[tokio::test]
    async fn fake_blob_supports_seek_short_read_failure_cancel_and_large_length() {
        let mut blob = FakeBlobHandle::from_bytes(b"abcdef".to_vec()).with_short_reads(2);
        let mut bytes = [0_u8; 4];
        assert_eq!(blob.read(&mut bytes).await.expect("short read"), 2);
        blob.seek(SeekFrom::Start(4)).await.expect("seek");
        assert_eq!(blob.read(&mut bytes).await.expect("tail read"), 2);

        let mut failed = FakeBlobHandle::from_bytes(b"abcdef".to_vec()).with_failure_at(1);
        assert_eq!(
            failed.read(&mut bytes).await.expect("read before failure"),
            1
        );
        assert!(failed.read(&mut bytes).await.is_err());

        let mut cancelled = FakeBlobHandle::from_bytes(b"abcdef".to_vec());
        cancelled
            .cancellation_handle()
            .store(true, Ordering::SeqCst);
        assert!(cancelled.read(&mut bytes).await.is_err());

        let large = FakeBlobHandle::large_logical(8 * 1024 * 1024 * 1024, 0x61);
        assert_eq!(large.logical_len(), 8 * 1024 * 1024 * 1024);
    }
}
