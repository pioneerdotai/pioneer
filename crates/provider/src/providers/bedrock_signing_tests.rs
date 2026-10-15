//! Offline conformance cases: the independent AWS SDK oracle runs ONLY when
//! these tests are explicitly executed. No AWS client, credential discovery,
//! network transport, model request or scratch-generated signature is involved.
use super::{BedrockProvider, SERVICE, canonical_request, sign_request};
use aws_credential_types::Credentials;
use aws_sigv4::{
    http_request::{
        PayloadChecksumKind, PercentEncodingMode, SessionTokenMode, SignableBody, SignableRequest,
        SignatureLocation, SigningSettings, UriPathNormalizationMode, sign,
    },
    sign::v4,
};
use serde::Deserialize;
use url::Url;

#[derive(Deserialize)]
struct Fixture {
    access_key: String,
    secret_key: String,
    service: String,
    timestamp: String,
    amz_date: String,
    body: String,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    model: String,
    region: String,
    session_token: Option<String>,
    transmitted_url: String,
    transmitted_path: String,
    canonical_uri: String,
    canonical_request: String,
}

fn sdk_authorization(fixture: &Fixture, case: &Case, encoding: PercentEncodingMode) -> String {
    // The oracle uses only fixture input: no Pioneer signing or encoding code.
    // Versions, source revision, settings and scenario provenance are recorded
    // in tests/fixtures/bedrock_signing.README.md and Cargo.lock.
    let identity = Credentials::new(
        &fixture.access_key,
        &fixture.secret_key,
        case.session_token.clone(),
        None,
        "non-secret-test-fixture",
    )
    .into();
    let time = chrono::DateTime::parse_from_rfc3339(&fixture.timestamp)
        .unwrap()
        .into();
    let mut settings = SigningSettings::default();
    settings.percent_encoding_mode = encoding;
    settings.payload_checksum_kind = PayloadChecksumKind::NoHeader;
    settings.signature_location = SignatureLocation::Headers;
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Enabled;
    settings.session_token_mode = SessionTokenMode::Include;
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region(&case.region)
        .name(&fixture.service)
        .time(time)
        .settings(settings)
        .build()
        .unwrap()
        .into();
    let request = SignableRequest::new(
        "POST",
        &case.transmitted_url,
        [("content-type", "application/json")].into_iter(),
        SignableBody::Bytes(fixture.body.as_bytes()),
    )
    .unwrap();
    let (instructions, _) = sign(request, &params).unwrap().into_parts();
    instructions
        .headers()
        .find(|(name, _)| *name == "authorization")
        .expect("AWS SDK Authorization header")
        .1
        .to_owned()
}

#[test]
fn bedrock_sigv4_matches_independent_aws_sdk() {
    let fixture: Fixture =
        serde_json::from_str(include_str!("../../tests/fixtures/bedrock_signing.json")).unwrap();
    assert_eq!(fixture.service, "bedrock");
    assert_eq!(SERVICE, fixture.service);
    for case in &fixture.cases {
        let provider = match &case.session_token {
            Some(token) => BedrockProvider::with_session_token(
                &fixture.access_key,
                &fixture.secret_key,
                &case.region,
                token,
            ),
            None => BedrockProvider::new(&fixture.access_key, &fixture.secret_key, &case.region),
        };
        let transmitted = provider.converse_url(&case.model);
        assert_eq!(transmitted, case.transmitted_url);
        let url: Url = transmitted.parse().unwrap();
        assert_eq!(url.path(), case.transmitted_path);
        assert_ne!(case.transmitted_path, case.canonical_uri);

        let (actual_canonical, _) = canonical_request(
            "POST",
            &url,
            fixture.body.as_bytes(),
            case.session_token.as_deref(),
            &fixture.amz_date,
        );
        // Literal canonical fixture describes the wire/signing distinction;
        // it is not produced by Pioneer or a copy of its encoder/signer.
        assert_eq!(actual_canonical, case.canonical_request);
        assert_eq!(
            actual_canonical.lines().nth(1),
            Some(case.canonical_uri.as_str())
        );
        assert_eq!(
            url.path(),
            case.transmitted_path,
            "signing must not mutate the wire URL"
        );

        let actual = sign_request(
            "POST",
            &url,
            fixture.body.as_bytes(),
            &fixture.access_key,
            &fixture.secret_key,
            case.session_token.as_deref(),
            &provider.region,
            SERVICE,
            &fixture.amz_date,
        );
        let expected = sdk_authorization(&fixture, case, PercentEncodingMode::Double);
        // Exact Authorization equality checks signature, scope, session token
        // inclusion and header ordering with an independently implemented SDK.
        assert_eq!(
            actual,
            expected,
            "{} / session={}",
            case.model,
            case.session_token.is_some()
        );
        // Demonstrate that these cases detect a Single-encoding regression.
        let single = sdk_authorization(&fixture, case, PercentEncodingMode::Single);
        assert_ne!(expected, single);
    }
}
