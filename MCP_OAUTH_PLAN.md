# Native MCP OAuth

Implementation is in progress on `feature/mcp-oauth`. This note records the agreed scope for the draft PR. It describes planned behavior, not a finished feature.

OAuth-protected MCP servers currently need an external bridge such as `mcp-remote`. Pioneer will handle authorization directly through rmcp for Streamable HTTP servers.

When someone adds a server that needs OAuth, Pioneer will prepare the callback listener and open the browser automatically. After sign-in, it will save the credentials, connect the server, and load its tools. Routine restarts and token refreshes should happen in the background. A revoked or expired refresh token can still require another sign-in.

The new `pioneer-mcp-oauth` crate will own authorization flows, credential recovery, background refresh, refresh coordination, and cleanup. `pioneer-mcp` will keep the MCP transport and sessions. Gateway will provide dependency wiring, permission checks, RPC routing, and event forwarding. The existing keystore will store credentials and client registration.

The callback must work with both local and remote Gateways. For a remote Gateway, Desktop will receive the browser callback locally and relay it through an authorized RPC. The Gateway process will exchange the code and store the tokens through the OAuth service.

Verification will use focused tests in the affected crates, with fake OAuth and MCP servers. Planned coverage includes automatic sign-in, restart recovery, token rotation, concurrent refresh, transient failures, scope changes, callback validation, and cleanup. Full workspace and Gateway test suites are outside this work's test plan.

Existing stdio, secret-backed HTTP headers, and `mcp-remote` configurations will remain supported.
