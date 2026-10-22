# threads_search / workspace recall implementation map

Baseline: adb446a542e4c0c24e2d011fe73b184c9af1fb29.
No tests, builds, applications or schema generators may run before external acceptance.

| Requirement | Existing owner | Change | Static/authoring evidence |
| --- | --- | --- | --- |
| Shared indices/config/query preprocessing | ThreadEpisodicRecallService, memory backend | Scoped segment retrieval before prompt caps, prepared query vector | Real search + workspace recall call sites; provider fixtures |
| Lexical/hybrid continuation | Native search/ask | Stateless scan/select of indexed thread IDs; lexical quanta plus fixed native hybrid candidates per segment | Multi-page, later-segment and source-filter regression fixtures |
| Current authority | ExecutionAuthorizationContext, AuthorizationResolver | Durable initiating principal; current root/action proof; per-thread read proof and execution ceiling | Missing principal, private, two-user and revocation fixtures |
| Default recall | memory hooks planner and host full-input path | Workspace operation including current thread, source-specific provenance | Hook provider/plan and prompt tests |
| Hidden tool | tools domain, turn MCP native catalog | threads_search only in threads, ThreadRead, immutable CLI selection | Visibility and transport schema tests |
| History anchors | canonical episodic item provenance | workspace/thread/turn/item + source context, never native frame ID | Canonical source fixtures |

## Continuation algorithm chosen before editing

Discover indexed thread IDs in bounded keyset batches, without directory title,
recency or sidebar preselection. For each currently authorized thread, traverse
all workspace capsules in segment order and all native lexical continuation
quanta. The first quantum also retrieves the fixed server-configured native
hybrid candidate set, when compatible vector projections are ready. Query
embedding is prepared once per invocation with embed_query_checked and reused.
A thread's score is the maximum validated source score over its quanta; keep
only a bounded number of source references per thread. Native fusion is retained.

An authenticated cursor carries traversal position, a bounded limit+1 selection
of references/scores and the previous page's (score, threadId) boundary. Until
all indexed threads/segments have been visited, return work_budget and a cursor,
without publishing a provisional page. On completion, the selection is the next
page in global score/thread-ID order. A further page restarts bounded discovery
strictly below the relevance boundary. This costs another corpus traversal per
page, but reaches threads beyond any fixed global top-k without a persistent
search session or loading full histories/embeddings into gateway memory.

Bind the cursor to query, limit, operation, workspace, initiating principal,
execution envelope, configuration, actual prepared query vector and capsule generations. Recheck access and
hydrate references on every call; reject changed snapshots. A process restart
invalidates the process-keyed cursor. Corpus modifications invalidate continuation.

Hybrid coverage remains approximate, because native vector retrieval is bounded
and HNSW is approximate. Approximation is distinct from missing/unready segments,
provider degradation and work-budget continuation. Native lexical cursor is never
presented as a cursor of native hybrid rank. Per-thread fixed native candidates
are supplemented by continued lexical retrieval, and final card order is decided
only after the complete scan. URI-scoped vector candidates must be retrieved past
the initial global top-k; the dependency patch is reproducible in vendor.
A saturated native hybrid window explicitly marks incomplete coverage, including
when lexical traversal has ended; it is never represented as complete semantic EOF.
