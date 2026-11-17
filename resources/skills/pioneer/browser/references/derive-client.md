# Derive an API client from an observed session

<!-- Modified for Pioneer. -->

Use this workflow when the user wants a standalone client/CLI for observed site
flows. Prefer an available service connector or supported documented API when
it already covers the task. Recording a browser session helps discover actual
request shapes; it does not authorize new API effects or broader data access.

All browser commands below carry the task's unique `$browser_session`. Use
[authentication](authentication.md), [trust boundaries](trust-boundaries.md)
and [tools and runtime](tools-and-runtime.md) for credentials and artifacts.

## Record the relevant flows

Map the requested functions first: for example search, fetch detail and paginate.
Use an authorized login before starting capture so the login exchange itself
does not unnecessarily enter the HAR. Auth headers and cookies can still appear
on every later request.

Choose an absolute private `$private_dir` outside the generated source and
shareable report bundle:

```bash
umask 077
mkdir -p "$private_dir"
agent-browser --session "$browser_session" network har start
# Drive only the requested, authorized flows using fresh snapshots/refs.
agent-browser --session "$browser_session" network har stop "$private_dir/site.har"
```

Where safe and useful, exercise a flow with different inputs, such as two search
queries or two detail pages. Comparing them distinguishes parameters from
fixed route components. Do not replay a consequential action merely to gather
another sample. Record navigation, pagination, sort/filter behavior and success
conditions that the client actually needs.

Text response bodies are embedded by default; `--content all` also includes
binary bodies and `--content none` omits body capture. Bodies may be capped
(upstream documents a 2 MB per-body cap) or unavailable. Check whether captured
content is complete before deriving a schema. Binary content may be base64.

While a session is open, `network requests` and `network request <id>` can help
target a request. Their output can also include secrets. Capture it privately
and sanitize it before any model-visible inspection; do not print raw request
details as a shortcut. HAR persists across browser navigation and close, which
makes it more useful for multi-page work.

## Redact before analysis

The raw HAR is a credential-bearing artifact. Before displaying or loading its
contents into model context, create a local redacted projection with an audited
script appropriate to this site's data. The projection should:

- Remove/redact `Authorization`, `Cookie`, `Set-Cookie`, API keys, CSRF headers
  and other credential/custom header values in requests and responses.
- Redact tokens, passwords, OTP/OAuth codes, signed URL parameters, URL userinfo
  and sensitive identifiers in query strings, fragments or path segments.
- Parse JSON, form, multipart and response bodies as applicable; redact sensitive
  fields recursively, including embedded/nested payloads. Prefer type/shape
  summaries or a small allowlist of needed non-sensitive fields.
- Omit bodies that cannot be sanitized reliably; a regex for field names alone
  does not prove arbitrary HTML, text, blobs or dynamic-key maps are safe.

Do not `cat` the HAR, dump full headers or show a sanitizer's raw failure input.
Keep the original private for local runtime auth handling when needed. Use
`$redacted_har` below only after producing and checking that projection.

## Identify actual endpoints

Inventory the sanitized recording by method, status, URL and media type:

```bash
jq -r '.log.entries[]
  | select((.response.content.mimeType // "") | test("json"))
  | "\(.request.method) \(.response.status) \(.request.url)"' "$redacted_har"
```

Correlate each candidate endpoint with the UI action that produced it. Separate
first-party application calls from static assets, telemetry and infrastructure
noise, such as `/collect`, `/track`, `/beacon` or unrelated analytics domains.
Do not exclude an endpoint solely by its name: verify its role from the observed
flow. Keep an endpoint-to-function map, sample status, pagination parameters
and evidence entry ids.

## Extract shapes and auth requirements

For a relevant endpoint, inspect sanitized request/response shapes. This example
prints header names and replaces JSON scalar values with their types:

```bash
jq '
  def shape:
    if type == "object" then with_entries(.value |= shape)
    elif type == "array" then {items: ([.[] | shape] | unique)}
    else type end;
  def jsonshape: try (fromjson | shape) catch "non-JSON or omitted";
  .log.entries[]
  | select(.request.url | test("api/search"))
  | {method: .request.method,
     requestHeaderNames: [.request.headers[]?.name],
     requestShape: ((.request.postData.text // "") | jsonshape),
     responseShape: ((.response.content.text // "") | jsonshape)}
' "$redacted_har"
```

Adjust the endpoint filter from the sanitized inventory. This type summary is
not a substitute for prior redaction: object keys and metadata can themselves
contain sensitive values.

Determine required auth by header/cookie names and local processing, without
showing values. A read endpoint may require session cookies, a CSRF token or
specific `user-agent`, `referer` or `x-requested-with` headers. Test minimal
headers on permitted read calls or fixtures; never remove auth controls from
the service to make a replay succeed.

If exporting cookies is needed and authorized, direct the output straight to
a private file, not the transcript:

```bash
umask 077
agent-browser --session "$browser_session" cookies get --json > "$private_dir/cookies.json"
```

The client must load that file privately, restrict cookies/headers to the
intended target domains and avoid forwarding credentials on unrelated redirects.
Do not include private auth files in generated source, fixtures or deliverables.

## Generate the client

- Expose one function per requested observed flow, such as `search(query)` or
  `getItem(id)`, with types based on actual sanitized response shapes.
- Make observed pagination, sort and filter parameters explicit options.
- Load cookies/tokens from a protected file, environment or existing credential
  provider at runtime. Never hardcode values or log authenticated requests.
- Preserve the headers and CSRF/signing behavior actually required. Keep a flow
  browser-driven when reproducing a per-form token or signed step is unreliable.
- Handle status codes, retries, timeouts and rate limits with bounded behavior.
  Do not retry a possibly completed write without an idempotency strategy or
  an independent result check.
- Provide an actionable expired-session error that describes the supported
  reauthentication path without displaying credentials.

Treat undocumented APIs as changeable. Keep a private or sanitized recording
and endpoint map according to the user's retention needs so the client can be
updated; do not publish the raw HAR for reproducibility.

## Verify within authorized effects

Compare generated types/parsers with captured samples or sanitized fixtures.
Exercise permitted read calls against the live API when appropriate, comparing
their shape and useful results with the browser observation. Test mutation
serialization, response handling and errors with fixtures/dry runs when live
effects are not authorized. State which functions have not been run live; a
working client implementation does not require unrequested production writes.

| Symptom | Possible cause | Next step |
| --- | --- | --- |
| 401/403 | Missing or expired session | Use the supported login and private auth refresh path |
| 403/419 on a write | Per-session/per-form CSRF | Implement the observed token flow or keep the step in the browser |
| Replay later fails | Expiring/signed parameters | Re-record safely or retain browser handling for that step |
| Response shape differs | Version, experiment, role or region | Compare actual conditions; model optional variants supported by evidence |

Respect actual service limits and the user's scope. Report the supported flows,
validation performed and limitations. Deliver source and redacted documentation
through the available file/artifact mechanism; keep HAR and credentials private.
