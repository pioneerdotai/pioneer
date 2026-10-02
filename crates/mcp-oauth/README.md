# Native MCP OAuth

`pioneer-mcp-oauth` owns authorization independently of Gateway and desktop. It
uses the pinned rmcp 3.5.0 authorization manager for discovery, PKCE S256,
resource binding, registration, issuer-aware code exchange and refresh.
`pioneer-mcp` supplies the lower-layer `McpOAuthProvider` interface and the
Streamable HTTP adapter; it does not depend on this crate.

The service owns cancellation, tracked background tasks, one shared manager per
installation, bounded monotonic and wall-clock flow deadlines, retry/backoff and exchange rate limits.
Gateway supplies persistence, an authenticated event sink and the installation
identity. Its adapter checks current installation configuration and management
rights before delivering an authorization URL only to the initiating connection.

## First connection and shell boundary

Desktop prepares a loopback listener before sending the normal install request.
The installation is saved before its asynchronous MCP connection starts. Public
servers connect without authorization effects. A genuine OAuth challenge starts
one flow while the initiating connection and the volatile install intent remain
current. Transient connection/discovery/registration failures can recover within
that operation. The install RPC does not wait for consent.

A shell-neutral notification drives a deduplicated browser effect. Desktop owns
`http://127.0.0.1:37643/oauth/mcp/callback` on the browser's device and relays parsed
callback fields over the authenticated Gateway connection. This also works with
a remote Gateway: its localhost never needs to be reachable from the browser.
The Gateway-side service verifies client ownership, flow TTL, state and issuer,
then quickly accepts a single-use callback and releases the RPC queue. Its owned
exchange task lets rmcp verify PKCE/state and exchange the code once; acceptance
is not durable sign-in success. A terminal event acknowledges authorization only
after storage succeeds and the operation wins its atomic terminal decision. Cancel
or timeout winning that decision restores the prior grant under the same lease.
A short observable projection keeps Details and Cancel admission independent of
network preparation/exchange. Gateway admits the runtime effect against the current durable installation
UUID/OAuth identity/generation under a lifecycle gate shared with installation
mutations, then updates its catalog and capability projections.

Cancellation, timeout, disconnect, loss of the initiating connection, uninstall
and configuration replacement fence the flow and release the shell listener.
Desktop uses the existing hardened webbrowser launcher on bounded, tracked Client
browser workers without holding UI projection/listener mutexes or blocking the
shared event dispatcher and MCP action queue. Browser launch failure
keeps a manual retry action using the existing live listener. Atomic launch
admission rejects retired operations; an OS launch already admitted cannot be
recalled. Listener failure never offers an
unusable URL. A busy callback port is reported; the redirect URI is never silently
changed to one that disagrees with the persisted registration.

## Durable authorization

The existing keystore stores one atomic JSON account record per installation in
`pioneer.gateway.mcp_oauth`, plus a non-secret promotion fence in that namespace.
Disconnect, identity replacement and orphan GC account for both keys. The account
record contains the full SDK credentials,
granted scopes, absolute `token_received_at`, resource, issuer and complete
client configuration. DCR request/response metadata (including provider extension
fields) is retained alongside the registration. Restoring or saving credentials
does not reset token age. The SDK's `initialize_from_store` is deliberately not
used to restore client configuration because it loses secret/redirect details.

The current production store is a permissions-restricted `keystore.db` with
`encryption_opts: None`; this is neither an OS Keychain nor encrypted storage.
No general secret-storage migration or new primary database table is introduced.
Gateway's existing repositories and Maintenance scope handle main-database
installation reads and orphan GC; database capacity is released before OAuth,
keystore I/O, waits and backoff.

Identity includes the installation ID, full resource URL, configured OAuth
identity/issuer/scopes, referenced client-secret material and the presence of an
explicit Authorization header. Discovery independently verifies the persisted
issuer before tokens are reused, including each reconnect with a shared manager. Session
manager identity and volatile event generations reject late challenges and queued
browser effects from replaced or completed sessions. OAuth RPCs also carry the
installation UUID, enter the installation lifecycle gate and reread configuration
before acting. Callback/Cancel never restore a binding; Disconnect stops only
the exact admitted row. A reused server name cannot receive an old callback. Separate installations never share credentials.
OAuth access/refresh rotation is outside configuration and effective-secret
fingerprints, so refresh does not restart MCP.

One shared rmcp manager coordinates transport and background expiry checks.
The worker uses rmcp's access-token API for normal expiry, including its
30-second buffer. After a request refresh fails, recovery invokes the same
manager's SDK refresh API rather than accepting a still-cached rejected token;
there is no second refresh mechanism. CredentialRefreshGuard
combines a local lock with a permissions-restricted fs4 file lock per installation.
The SDK reloads current credentials after locking. Pending consent exposes its
previous grant to existing readers until terminal success; independent readers
wait for the refresh/file lease. Consent token save retains a durable pending candidate and the complete previous
record in the same atomic secret record. Until terminal success and a separate
owned atomic commit, credential readers receive only the prior grant. Failed
rollback cannot promote a candidate after adapter replacement or restart. Final
promotion retains the previous record and installs a confirmed durable fence
before mutation. Errors are checked by exact readback; an unconfirmed promotion
remains fenced, and a confirmed promotion acknowledges success only after fence
cleanup is confirmed. Unknown fence deletion remains owned reconciliation until
readback resolves or retirement/shutdown interrupts it; it never reports Failed
for a potentially unfenced, confirmed grant. Bind recovers failed consent under
the per-installation and refresh/file leases or reports storage failure. Ordinary
SDK store clones own independent refresh reentrancy, and reads recheck visibility
after I/O. Actual blocking writes retain
the refresh/file lease even if their async caller is cancelled. Independently
created persistence adapters share an IO owner and completion registry; GC drains
started mutations before enumeration, and replacement/deletion rereads identity
under the same per-installation file lease before cleanup. Shutdown joins owned
operation tasks (including newly enqueued cleanup) and drains the IO registry. Newly rotated
refresh tokens are saved immediately; omitted refresh tokens retain the previous
one through rmcp.

Restart, reconnect, sleep/network recovery and normal refresh never create a
browser intent. Repeating an install with a saved registration also never creates
a new implicit consent intent; reauthorization is an explicit user action. Transient provider/network/storage errors preserve credentials
and use bounded exponential backoff with jitter. A definitive rejected refresh
token becomes AuthRequired until explicit sign-in. Authorization attempts are
bounded; SDK POST exchanges are capped at eight per connection per minute.

## Configuration

Ordinary Streamable HTTP installs discover OAuth automatically. Providers without
DCR can use a registered client. For example:

```json
{
  "mcpServers": {
    "protected": {
      "url": "https://mcp.example.com/mcp",
      "oauth": {
        "client_id": "registered-pioneer-client",
        "client_secret": "provider-issued-secret",
        "token_endpoint_auth_method": "client_secret_post",
        "issuer": "https://identity.example.com",
        "scopes": ["tools.read", "offline_access"]
      }
    }
  }
}
```

The supported client authentication methods are `none`, `client_secret_basic`
and `client_secret_post`. The assigned DCR method takes precedence. For a
pre-registered confidential client, configure its assigned method explicitly;
without one the compatibility default is Basic. Unsupported or inconsistent
methods fail with a safe diagnostic. Because rmcp 3.5.0 has no per-code-client
AuthType setter, a per-manager SDK HTTP adapter applies POST authentication only
at the exact discovered token endpoint, leaving the AS metadata and SDK grant/
redirect/resource/PKCE behavior intact.

The optional client secret is materialized as an existing secret reference;
installation metadata and UI never contain it. OAuth plus an explicit
Authorization header is rejected. Existing stdio, secret-backed HTTP headers and
arbitrary mcp-remote commands retain their behavior. No automatic import from
`~/.mcp-auth`, legacy SSE implementation or invented CIMD metadata URL is added.

## Runtime and diagnostics

Typed errors distinguish authorization required, rejected refresh, transient
refresh, credential-store failure, insufficient scope and ordinary Forbidden.
Only a real insufficient_scope challenge offers additional consent. rmcp's 401
recovery retries once after silent refresh; Pioneer does not replay ambiguous
failed tool calls. Auth loss during initialize/catalog/tools/stream/refresh
invalidates runtime availability; successful consent reconnects and refreshes
projections. Normal token refresh produces no runtime-restart event. A temporary failure
followed by successful silent recovery emits a separate recovery transition.
Transport errors carry a typed generation/revision of the notified OAuth failure.
Recovery compares this cause at execution, so late delivery of failure A does not
invalidate recovery A, while a new failure B rejects the old recovery. Untagged
errors use the conservative actor counter. One coalesced recovery mailbox belongs
to each runtime actor. The 30-second ack deadline releases unclaimed admission
but retains pending delivery; the actor reacquires interruptible lifecycle
admission and rereads durable UUID/configuration and event currency before acting.
Stopping/replacing the actor clears its mailbox; no healthy session restart or
failed tool replay is needed.
Gateway restores only the OAuth-related degradation on a live session, preserves
unrelated reasons and generation, and refreshes management projections without
replaying the failed call. Failed/denied/timed-out sign-ins also offer clearing the
saved registration before starting fresh consent.

OAuth network operations use the SDK HTTP policy independently of MCP custom
headers. Token requests retain the SDK redirect policy. Raw SDK error bodies,
code/token debug logging and authenticated MCP error payloads are suppressed or
replaced with safe diagnostics. DTO debug representations redact callback
code/state and authorization URLs. Tokens, client secrets and PKCE verifier are
absent from UI projections and internal Codex/Claude MCP bridge configuration.

## Practical limits

Refresh cannot run while the computer or Gateway is off. Revoked/expired refresh
tokens require consent again. A provider rotation followed by a local persistence
failure can leave an unrecoverable old refresh token. DCR registration itself is
an external side effect; a failure before saving registration can leave an orphan
client registration at the provider. The fixed callback port must be available
and pre-registered clients must allow its URI. Native browser/listener integration
is currently the desktop shell; mobile/FFI consumers keep shell-neutral contracts
and do not acquire credentials or start their own OAuth flows.

**The corrected code is not accepted; no correction regressions have been run.**
Non-test compilation/static checks and deferred exact commands are recorded in
[VALIDATION.md](VALIDATION.md). Tests use local fake OAuth/MCP servers and do not
launch a real browser or require an external account.

The OAuth registry map is held only for lookup/publication/removal. Owned per-ID
bind gates serialize retirement, old-actor/refresh completion and keystore cleanup;
other installations' Details/Callback/Cancel do not wait for that I/O. Publication
checks shutdown again under the short registry lock. Failed-consent cleanup can
restore the prior record in place without retiring its healthy live manager.

## Shutdown and uncertain consent outcome

External bind/cleanup/provider-connect callers participate in lifecycle admission.
Shutdown closes admission, cancels the root, waits those callers and owned tasks,
then drains actual blocking mutations before clearing entries. No registry lock
spans read/write/network waits; per-installation bind independence is preserved.
A second shutdown waits the same shutdown owner.

After consent wins its single terminal decision, `Resolving` distinguishes durable
promotion from a still-cancelable code exchange. Desktop releases the callback
relay and offers Clear sign-in; it does not offer Cancel or another consent in
this phase. Original deadline or initiator loss retires reconciliation without
retroactively cancelling consent or reporting a false failure/success. The last
management projection remains Resolving until clear/rebind/restart can reconcile.
Clear and replacement interrupt the owner before waiting for its lease. Confirmed
promotion plus confirmed fence removal can become Authorized; an unreadable
outcome cannot. Permanent storage failure can prevent cleanup; real blocking IO
must still complete before its retained lease and shutdown ownership are released.

Resolving projection and the consent winner share one synchronous linearization
under operation admission. Resolution retirement cannot suppress the projection
or its current-state notification. Replacing/suspending/clearing a binding sends
an addressed, URL-free `Retired` presentation reset for the old flow. It does not
cancel consent retroactively. Client removes only that presentation and rejects
late signals from it; equal-identity live updates keep the current flow. Current
management details do not reuse OAuth state from a different bound configuration.

A failed or uncertain Clear sign-in retires the old consent but retains a separate
`CleanupRequired` management projection for the current installation. Desktop
offers repeat Clear directly; this cancelled holder cannot restore a MCP manager
or start consent. The error presentation has its own safe UUID, so retirement of
the old relay does not hide cleanup. Clear re-establishes the promotion fence before
account deletion; success requires removal of both account and fence. Actual blocking
IO retains its leases, and admitted callers finish before shutdown's final drain.
Permanent storage failure can prevent cleanup; no successful deletion is claimed
from an Err or unknown result. W1 regressions are written/compiled only, NOT RUN.

After confirmed management Clear, a fresh credential-free admission remains
`AuthRequired` for this running service. Workspace reconciliation cannot start an
anonymous connection while the user is signed out; explicit Sign in starts a new
consent operation. This volatile signed-out admission is not a persisted account.
Management details expose only cleanup availability, never credentials: Desktop
hides Clear when no registration remains and retains it for uncertain cleanup.

Workspace synchronization accepts an equal-configuration CleanupRequired holder
without restoring an OAuth client; disable retains its repeat-Clear projection.
Client transport and new consent still fail closed until cleanup succeeds. Current
management cleanup replaces completed consent copy such as Denied, while current
live flows and session retirement fences reject late cleanup/Retired effects.
Desktop uses the shared management-state selector and preserves the existing
localized cleanup guidance/actions. X1–X3 regressions are NOT RUN.

### Desktop callback port

Desktop binds `127.0.0.1` and uses `/oauth/mcp/callback`, including when the
Gateway is remote. The default/production port is 37643. To run a development
Desktop alongside production, add this to the development override:

```toml
[desktop.mcp_oauth]
callback_port = 37644
```

Keep the existing development `home_directory`, Gateway `listen_addr`, and
service overrides so development does not share production storage or Gateway.
Configuration sources, from lowest to highest priority, are embedded
`config/default.toml`, workspace `config/local.toml` (debug builds only), the
system config directory's `pioneer-dev/config.toml` (debug) or
`pioneer/config.toml` (production), and the file selected by `PIONEER_CONFIG`.
Production does not include workspace `config/local.toml` automatically. A file
explicitly selected through `PIONEER_CONFIG` remains the highest-priority source,
even when it is that same `config/local.toml`. The port must be an integer
from 1 through 65535, excluding port 80 (URL normalization removes the explicit
HTTP default port required by the existing loopback redirect contract). Invalid configuration and occupied ports fail preparation;
Desktop does not fall back to another port or open a browser without a listener.
Retry sign-in after freeing an occupied port.

Restart Desktop after changing the port: each shell keeps one immutable setting
for binding, redirect URI, and callback parsing. Existing OAuth registrations
retain their original redirect URI. A mismatch requires **Clear sign-in**, then
fresh sign-in; changing the setting does not rewrite registration or delete an
account. Pre-registered clients must have the matching redirect URI registered
with their provider. Refresh and normal startup do not create a listener or open
a browser.
