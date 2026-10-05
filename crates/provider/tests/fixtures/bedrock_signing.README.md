# Bedrock SigV4 independent oracle (unexecuted)

These are authored offline request scenarios, **not previously executed AWS acceptance goldens**. No Authorization signature was computed while preparing this fixture. Exact expected Authorization is produced only inside the future test by the independent official SDK signer.

Oracle: `aws-sigv4 = 1.6.0`, `aws-credential-types = 1.3.0`, test-only dependencies pinned exactly, with transitive versions in Cargo.lock. `aws-sigv4` crate `.cargo_vcs_info.json` identifies smithy-rs commit `dd4d62780ee72db9adae16134e5dac47a9b6393e`.

Sources:

- [SDK signing API and example](https://docs.rs/aws-sigv4/1.6.0/aws_sigv4/http_request/index.html).
- [SDK canonical request implementation](https://github.com/smithy-lang/smithy-rs/blob/dd4d62780ee72db9adae16134e5dac47a9b6393e/sdk/aws-sigv4/src/http_request/canonical_request.rs).
- [SDK percent encoding settings](https://docs.rs/aws-sigv4/1.6.0/aws_sigv4/http_request/enum.PercentEncodingMode.html).
- [AWS canonical request specification](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html).
- [Bedrock Converse URI](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_Converse.html).

Settings: POST, service `bedrock`, signing region from the scenario, fixed timestamp `2026-10-01T12:00:00Z`, body bytes `{}`, `content-type:application/json`. Double URI encoding, normalization enabled, headers signature, no added payload checksum header, session token included in canonical headers if present. SDK supplies date/host/session headers itself. Credentials are the published AWS documentation example, never credential discovery or an AWS client.

The literal canonical forms describe the spec's ordering and empty query, fixed headers and body digest. Wire paths are manually specified once-escaped strings; signing paths separately use `%253A` and `%252F`. No expected value uses Pioneer `sign_request`, its copy, or its encoding helper. Exact SDK Authorization comparison independently covers the full canonical digest and signature; a second SDK call in Single mode must differ and demonstrates sensitivity to the reviewed URI change. Both oracle calls remain unexecuted until tests are authorized. Canonical extraction in production merely exposes its existing construction for exact inspection; its algorithm stays Double.

Cases: `model:0` in commercial `us-east-1` and ARN containing colons/slash in CN `cn-north-1`, each with and without a session token. URL equality, transmitted path, canonical request/URI and exact Authorization are separate assertions. The fixture body and model IDs are signing inputs, not a claim that AWS accepts this inference request or grants the account access.
