# Codex MCP deterministic fixtures

These local fixtures describe the stable adapter scenarios exercised by the
unignored Codex MCP integration target. The target validates exact projections,
restart and continuation identity, native-event binding, permission lifecycle,
empty projections, and concurrent A/B isolation without starting Codex or
requiring account credentials.

The same integration target also runs the production same-binary
`__cli-mcp-stdio` helper against two concurrent private bridge generations. It
proves the pre-list barrier, exact disjoint lists, call/cancellation flow, grant
revocation, artifact cleanup, and secret-surface isolation without starting
Codex.
