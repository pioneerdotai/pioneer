# Portable turn payload stage 1: integrated static audit

Base: `0691788deb0ee6ef3bf8b7b0bc5e5655b7178801`.
Branch: `feat/turn-payload-portable-stage1`.
This records source review, not test execution or external acceptance.
The initial codegen blocker was resolved by the user's explicit permission to
build the normal Migrator. Application builds and tests remain prohibited.

## Admission and schema

`m20261115_000001_portable_turn_payload` adds revision, identity and reference
columns to the physical plain/backing table. Existing identities default to
empty; only AFTER INSERT assigns a fresh random identity. Ordinary UPDATE
never admits an old incarnation. DELETE/reinsert gets a fresh identity even
with the same ID or an old timestamp. No clock, ID ordering or startup cursor
proves admission. Migration and its marker share the normal transaction.

Discovery frontier/page predicates, fragment and publication eligibility all
exclude empty identities. Legacy compression independently requires empty
identity at discovery, dictionary sampling and final CAS. The two workers
cannot publish against the same admitted row. No completion marker is reset;
there is no historical conversion, lazy conversion, new worker or queue.

Published logical replacements clear/retire old references; ownership changes
advance revision. Publication updates bytes/reference atomically, including
writable views. Compaction revision/epoch/publication and incremental repair
triggers distinguish physical representation changes from logical writes.
Legacy dictionaries, backing tables, views and SQL decode remain available.

Each new portable pending index scans metadata once, storing only admitted
pending keys. On an existing Zstd backing table a legacy pending partial index
also scans metadata once, storing only historical raw keys. When transparent
storage is installed later, its existing installer creates that index there.
These are O(N) metadata installation costs; none decompresses/hashes/copies or
rewrites historical payload bytes. The legacy index prevents repeated EOF
scans through an ever-growing new raw queue. No new table is introduced.

## Coverage map

Paths are relative to `crates/`. Metadata-only consumers retain their queries.

| Path / owner | Previous assumption | Integrated handling / proof to review |
| --- | --- | --- |
| `crud/src/repositories/turn_event.rs` prepared append, replay and idempotency | Full JSON in Model.payload; semantic preflight fenced only by ID | Reader hydration before semantic comparison; writer checks ID/identity/revision or exact inline bytes; sequence/key allocation unchanged. |
| `crud/src/lib.rs` atomic/custom publishers; `projector.rs` | Dependent events read projected canonical rows inside the transaction | Envelopes capture logical item state before writer admission. If a published source is needed, `PreparedInlineItem` restores verified bytes/reference atomically under identity/revision CAS before existing synchronous projection. A source already replaced by an earlier batch event is raw and needs no restoration; later events observe that actual state. New codec/hash work stays outside DB resources. |
| `crud/src/repositories/turn_payload.rs` | No turn-family logical loader | Shared typed loaders capture holds, hydrate after SELECT release and retry source capture races through scoped CRUD. Explicit reference selects portable; no reference retains legacy/pending TEXT. Fixed trace labels distinguish legacy/pending/portable; integrity failures propagate. |
| `crud/src/repositories/turn.rs` item get/list, exact timeline/agent messages | Full JSON rows, including typed projections | Hydrate full models and exact typed projections; metadata-only transactional reads have an explicit name. Preserve ordering, ownership and final-message selection. |
| `crud/src/repositories/turn.rs::post_turn_tool_summaries` | JSON1 extraction/filter/LIMIT over logical TEXT | Hydrate one candidate body at a time, parse outside capacity, apply LIMIT after the same filters. Preserve first duplicate label and NUL/160-character substring behavior. |
| `crud/src/repositories/canonical_turn_event.rs` | Sync canonical decoder receives full JSON | Hydration before boundary/direct/history decode; preserve actor, owner, source and event-type validation and event order. |
| `crud/src/repositories/compaction.rs` | Physical source length/body/substring are logical | Logical length from explicit reference metadata; whole reads and bounded PPAY character ranges use existing verified decoder. Raw fallback captures bytes/reference in one snapshot and verifies length/chars/hash before slicing outside capacity. Legacy fragment and reference share one snapshot too. Discovery still supports unregistered legacy revisions; exact revision fences remain. Input/provider-context families retain their storage. |
| `compaction.rs` tool access / shell preference | JSON1 sees full item body | Hydrate exact owned item, preserve each query's individual admission and whitelist; malformed legacy JSON still fails json_valid for shell preference. No codec errors are swallowed by that optional JSON predicate. |
| `compaction_lifecycle.rs` | Payload string comparison and JSON terminal predicate | Hydrate preparation outside capacity; revalidate identity/revision with domain metadata, independent of physical publication. |
| `crud/src/lib.rs` invariant scan / incremental repair | Direct item Model.payload; writer payload equality | Hydrate after bounded read release; write CAS uses captured identity/revision/status. Historical repair remains legacy. Existing completion markers and repair algorithm version are unchanged. |
| `thread_episodic.rs`, `thread_episodic_source_store.rs` | Source payload equality in writer; direct canonical source models | Hydrate search rows. Prepare expected SHA outside writer; compare immutable portable metadata or inline bytes inside existing authority/status transaction. No compression/decode/hash added there. |
| `self_improvement_history.rs`, `turn_execution_window.rs` | Direct event/item decode | Hydrate logical bodies before existing decoding; portable integrity errors do not become a successful legacy skip. Counts/identity-only queries need no body. |
| `cli_mcp_call.rs` | Accepted completion bytes used as replay fence | Hydrate preflight, then compare ID/identity/revision in writer; retain current attempt and owner guards. |
| `crud/src/lib.rs::get_blocked_turn_recovery_diagnostic` | JSON1 join extracts job ID from event TEXT | Decode before second relational query; revalidate exact event incarnation/revision, latest transition, Turn, job and outbox guards. No unrelated latest-job substitution. |
| `turn_item_attempt.rs` | Active canonical items are raw | Item admission excludes running canonical/attempt state. Actual attempt producer is ItemStarted, which replaces inline bytes atomically before creating a running attempt. Timeout/terminal CAS and verification remain in existing transactions. |
| `terminal_cleanup.rs` event/payload mutations | Legacy maintenance wakes only | Add portable WorkChange to the existing mutation boundaries, including blocked Turns; retain unknown settlement, quiet no-ops, bounded terminal probes and existing coordinator lifecycle. |
| `gateway/src/database/zstd_column.rs` | All eligible raw turn rows belong to legacy compressor | Shared eligibility excludes admitted identities at all uses, including final source CAS. Decode functions/configs/dictionaries stay intact. |
| `gateway/.../startup/turn_permission_profile_backfill.rs` | JSON1 over historical event payload | CASE excludes admitted INSERTs before evaluating legacy JSON; final update also excludes new incarnations. Existing markers, version and historical repair remain. No portable history backfill. |
| `migration` and `sqlite/src/zstd/transparent.rs` | Per-column writable-view UPDATE triggers; new columns can be NULL in a view INSERT | Upgrade rebuilds the logical view retaining domain triggers and old decode. Later normal Zstd installation supplies portable defaults and one atomic backing UPDATE. No logical payload rewrite on unrelated legacy metadata UPDATE. |
| `observability/src/portable_payload.rs`, `sqlite/src/database.rs` | Five portable families/source pairs | Two fixed labels/native source pairs; existing phases/outcomes/errors reused. No payloads, SQL, keys, IDs, paths or dictionary bytes in added observations. |

Also reviewed: `compaction_preparation`, `compaction_history`, frozen imports,
compaction runner/task output, delivery, finalization, task completion/child
cleanup, membership, agent timeline targets, Gateway history/tool callers and
startup projection replay. Their remaining direct Entity access is either
metadata-only, test-only, or the existing legacy ownership repair (misowned
legacy rows cannot pass new portable ownership/admission). No new codec SQL
function or general storage refactor was introduced.

## Intermediate and final static findings

1. Generic discovery would backfill old null references: closed by durable
   INSERT identity and repeated admission guards, not a cursor.
2. Writable-view rows_affected is zero: closed by verifying the unique reserved
   reference in the publication transaction. Per-column view triggers could
   split publication: replaced only for the two portable turn targets.
3. A view has no physical rowid: native source reads discover physical table
   metadata, then alias its guarded Entity SELECT to the logical owner name.
   The guard and bounded native range share one reader snapshot.
4. Physical placeholder/inline restoration could invalidate compaction and
   dirty repair: changed only their payload-change predicates, retaining all
   other trigger bodies and domain metadata predicates.
5. JSON1 conversion risk: preserved first duplicate-key behavior, filtered
   LIMIT and shell fallback; fixed an intermediate over-broad shared query
   that accidentally added access-only restrictions to shell preference.
6. A recovery projection may read an already portable item in writer: prepare
   bytes beforehand and CAS representation restoration; no new portable decode
   inside a transaction. Existing domain projection implementation is retained.
7. One corrupt portable delivery could suppress its batch: keep per-event
   errors and settlement, allowing healthy claimed events to progress.
8. Legacy EOF scans could grow with admitted raw rows: partial legacy pending
   index on physical storage. Install cost is documented above.
9. Explicit ActiveModel/Model fixtures need new generated fields: updated only
   consumer defaults; never hand-edited generated models.
10. New missing-header negative control initially had an independent damaged
    item: remove that fault and verify healthy replay before corrupting header.
    Restart fixture now asserts real staging and drops old store/writer handles.
11. Final range review found a raw-fallback branch could skip hash validation
    and its second SELECT could see publication instead of the captured source.
    Capture raw/legacy bytes with reference in one snapshot, verify raw through
    the existing decoder outside capacity, retain the compressed header's hold.
    Added a healthy fragment control and corrupt raw-digest rejection.
12. Terminal snapshot producer need not have canonical events or stream state.
    A mandatory stream JOIN would strand those new items. Shared admission now
    rejects events beyond the durable watermark (missing means -1), admitting
    a streamless owner only with no events. The real snapshot producer now
    precedes completion/replay in the primary new-path regression.
13. Static test ownership review found an expected tiny item moved before its
    later serialization; retain a clone in that assertion. No compiler/test
    success is inferred from this source correction.
14. Fresh main added package-installation migration at the same list tail.
    Local integration retains it before the stage-one migration. Auto-merged
    finalization/window-count changes remain metadata-only and preserve the
    new hydration boundaries. Normal Migrator/codegen repeated on an isolated
    database from the combined schema; no generated turn model diff remained.

No application compilation or tests establish these conclusions. The final
review must examine all source, generated fields and test fixtures together.

## Written regression allocation

Extended `crud/tests/compaction_payload_compression.rs` with:

- `portable_turn_payload_real_producer_restart_retry_and_corruption`;
- `portable_turn_upgrade_keeps_plain_and_zstd_incarnations_legacy`;
- `portable_turn_stale_preparation_cannot_publish_into_reinserted_id`;
- `logical_tool_summary_keeps_json1_first_label_and_filtered_limit`.

Together these cover snapshot and canonical producers and projection/replay, staged restart,
legacy plain/Zstd upgrade, no admission on read/UPDATE/restart, arbitrary old
IDs/timestamps, stale preparation/reinsert, exact compaction read, published
UPDATE, raw fallback (including fragment digest verification), retirement,
rollback, deletion and integrity errors.

Legacy Zstd suites now seed history before cutover and explicitly check that
later admitted rows remain untouched. Existing worker wake/pacing/idle tests
account for seven families. Explicit generated-model consumers use defaults.
Shared codec corruption/dictionary, reservation, read-hold cleanup, owner/root
CAS, raw threshold, resumed-turn cancellation, fairness and worker cancellation
suites remain the primary proof for unchanged common mechanisms; no duplicate
scenario cross product was added. Test-only audit batch preserved production
bytes; production companions were implemented separately under the user's task.

All tests remain UNRUN. Baseline failure and passing proofs are unavailable.
