# CLI tool embedding causality repair

Base: `26d7a9ea1`; branch: `fix/cli-tool-embedding-causality`.

## Evidence and cause

Read-only inspection of installed 0.72.0 found nine search calls whose
canonical tool observations use `item:<turn>:<call>`, with explicit null
ToolObservation. None has a `tool:<turn>:<call>:1` observation. The MCP shared
runtime preserves the provider call ID for thread readers; ToolOrchestrator
uses that absent first-attempt identity as the causal scope. JournalProvider
preserves it across the blocking embedding bridge. Its Call::start requires
an existing parent before dispatch and returns `usage causal parent unavailable`.
The search handler discards that underlying cause. This is a source/record
reconstruction; no live provider retry was executed.

## Change and invariants

The existing usage repository prefers the exact scoped parent. For an absent
first tool attempt it can resolve the corresponding persisted canonical item,
only with matching workspace/turn, tool-invocation kind and explicit null
observation. It stores the resolved identity consistently in journal and
projection. No missing-parent bypass or unjournaled provider call is introduced.
Lookup and bounded JSON validation happen outside writer capacity. The existing
transaction rechecks parent existence and ownership/scope and commits usage
and projection together. Existing source disclosure and search ranking are unchanged.

## Regression and validation

Extended the existing Gateway nested-workspace journal regression for measured
and CLI parents, missing/cross-turn/other-attempt rejection, live usage visibility,
completed journal/projection parent identity and workspace isolation. Production
file hashes remained unchanged throughout the test-only phase.

`cargo fmt --all --check` and `git diff --check` passed. Tests, builds, cargo
check, migrations, codegen and CI were not run. Live instance files, the production database and
running processes were not modified. Changes remain uncommitted in this worktree.

The empty search projection and pending refill are separate existing upgrade
work. This patch fixes the causal-parent rejection before the query API call;
it does not claim that production has received or executed the fix.


## Adjacent provider paths reviewed

- Native API tools install the durable tool receiver. ToolRuntime awaits the
  CallStarted commit ACK before handler dispatch; chat forwards the actual
  observation/attempt into the canonical event. Its `tool:` parent therefore
  exists before nested embedding preparation. The CLI shared runtime has no
  such durable tool receiver and its external item projector uses null observation.
- Background index/refill provider calls normally have no tool causal scope;
  the installed instance has a successful post-upgrade embedding probe on that
  path. This does not establish completion of the queued corpus indexing.
- Memory extraction scopes to the verified turn. Native compaction publishes
  preparation durably before using its item parent; background history uses the
  existing parent or verified turn. Reviewed title/summary and self-improvement
  journal callers as well. The repair is in the common journal repository, not
  a search-only bypass.
- Read-only production inspection found no dangling committed non-turn parents
  among journal provider observations. This cannot prove absence of calls rejected
  before a journal was written. Other recorded failures exist: OpenRouter rate
  limiting and memory-extraction output-length termination; these reached provider
  dispatch and are distinct from the missing-parent failure.
- No additional production changes, provider calls or test execution were made
  during this adjacent-path review. Native production embedding success after
  deployment is not claimed; the regression covers exact and CLI parent cases.
