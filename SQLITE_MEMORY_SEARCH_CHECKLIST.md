# SQLite memory/search implementation checklist

Base: `d1e0a10d0ee715ad51f0fe9f63cab8789ad35727`.
Branch: `refactor/sqlite-memory-search`.
Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/sqlite-memory-search`.
Delivery: uncommitted implementation for external review; no push, PR or CI.

## Existing owners and direct replacement

- [x] MemoryService, policy, quality, ownership, sensitivity, lifecycle,
  quarantine and recall hooks remain; SQLite replaces its existing backend.
- [x] Episodic ingestion, index executor, durable jobs/retries, WorkSignal,
  settings/refill supervisor, deadlines and reconciliation remain.
- [x] Thread Search retains current-source validation, authorization, signed
  cursors, grouping, current titles, anchors and oversized-card continuation.
- [x] LocalEmbeddingRuntime retains model registry, installed file paths,
  providers, preparation, budgets and document/query instructions.
- [x] No second service, queue, worker, migration runner or canonical history.

## Storage and engine boundary

- [x] Common contracts use domain eligibility, ordinary vectors and branch ranks.
  Services own provider calls, orchestration, access policy, rank policy,
  source disclosure and user pagination.
- [x] SQLite adapter/repositories own storage, FTS syntax/tokenization, extension
  SQL and eligibility pushdown through CrudStore/SqliteDatabase. No own pool,
  connection or independently committed publication transaction.
- [x] Existing embedding_artifacts.vector_bytes stores deduplicated float32
  vectors; real item occurrences link to artifacts independently of source ACL.
  Scalar cosine handles non-unit vectors; no vec0, quantization or file index.
- [x] Items.search_text is a derived projection, not canonical history.
  Agent_memory_payloads stores complete new payloads, not just previews.
  External-content FTS5 uses transactional incremental insert/update/delete
  triggers; no rebuild on each insertion or entire corpus transferred to Rust.
- [x] Actual SQLite unicode61 tokenizer produces escaped literal OR terms.
  Quotes, punctuation, AND/OR/NOT have no user syntax; empty/punctuation-only
  lexical queries produce no matches. Parser budget: 64 KiB and 512 terms.
- [x] Common equal-weight RRF policy, k=60, calibrated 0..1 score: both first
  ranks = 1; one first rank = 0.5. Defaults/cutoffs adapted; no BM25/cosine mix.
  SQLite pushes equivalent ordering down to return only bounded page rows.
- [x] Provider unavailable: lexical branch remains; failures mark incomplete
  retrieval and retain failure-class diagnostics. Explicit semantic mode fails.
- [x] Static sqlite-vec/literal-function registration occurs before all physical
  reader/writer connections, including pool replacements. Reader read-only and
  query_only protections and serialized writer routing remain intact.
- [x] Future pgvector needs an adapter, repositories and schema; no future
  backend, stub, registry, SQL framework or new model/queue system was added.

## Publication, reset and recovery

- [x] Provider work/vector encoding precedes writer admission. Existing atomic
  completion rechecks actual claim, source payload, deletion/exclusions and
  durable target identity before publishing text/artifact link + completed job.
- [x] Target reset uses existing bounded maintenance and durable source cursor;
  replaces job identities to fence late callbacks. Historical duplicate claims
  are drained in bounded write sets, preserving user deletion/exclusion outcomes.
- [x] Existing retry budgets, unknown commit handling, cancellation settlement,
  restart recovery and postcommit wakeups retained. No new idle polling owner.
- [x] Finite legacy agent payload retirement uses 64-row quanta and durable
  marker; new SQLite payloads rechecked at commit. File cleanup follows durable
  FilesPending before Complete, is restart-safe and checks cancellation.
- [x] File cleanup only visits known dedicated old roots, regular .mv2 files,
  bounded DFS depth and quanta; never database URIs or symlinks. Historical
  custom paths/deeper directories may remain inert, deliberately untraversed.
- [x] Canonical history, tasks, directory/affinity metadata and audit history
  retained. No production database, real capsule, model or process was changed.

## Removed and schema alignment

- [x] Removed memvid-core from manifests/lock and removed exclusive dependencies.
  sqlite-vec 0.1.6 has verified registry checksum
  `4ec77b84fb8dd5f0f8def127226db83b5d1152c5bf367f09af03998b76ba554a`.
- [x] Removed Memvid backend, file open/search fanout, capsule repositories,
  frame/segment/storage references, rotation/capacity/file repair machinery,
  storage-only settings and engine-specific protocol diagnostics/scores.
- [x] Historical DDL unchanged. New standalone migration installs projections,
  FTS and reset seed; no canonical backfill in schema migration. Native SQLite
  DROP COLUMN does rewrite affected metadata tables; installation not measured.
- [x] Manual Entity exception: agent_memory, agent_memory_repair_job,
  thread_episodic_items, thread_episodic_index_jobs; new agent_memory_payloads;
  deleted agent_memory_capsule/thread_episodic_capsules and aligned lib/prelude.
  Normal migration-backed Entity regeneration is deferred until after review.
- [x] Historical checkpoint/lock names and DB-only audit reason decoding remain
  intentionally; these are recovery/history compatibility, not a legacy backend.

## Regressions written/adapted, never executed

- SQLite agent backend/service integration suites: full payload put/get/search/
  delete, ownership, publication/supersession, failures, repair, sensitivity and
  quarantine before candidate limit; existing MemoryService policy tests retained.
- Gateway episodic owner: real ingestion/claim/publication, rollback of text/
  vector/job, separate physical reader, query_only and scheduling observations;
  cosine versus L2 ordering, equal-rank fusion, literal Unicode/operators/empty
  text, deterministic bounded pages including 256+1 boundary, provider failure.
- Existing source/model/retry/restart/cancellation/poison/unknown-commit tests
  adapted to SQL observations; explicit target-change publication rejection.
- Thread Search: ACL before top-k, exclusions/deletion, fresh disclosure and
  changed source, grouping/anchors, signed pagination and oversized Unicode cards;
  existing callers exercise the SQLite backend without fabricated file receipts.
- CRUD/schema: fresh install, upgrade, rollback/reapply, canonical preservation,
  bounded resumable reset, protected new payloads, artifact validation/conflicts,
  historical audit decoding and duplicate legacy job callbacks across reset.
- Startup owner: cancellation/restart/file cleanup and EOF without re-cleanup;
  local embeddings: CLS/normalization/shape and existing preparation/batch tests.
- Retired only obsolete engine/file/wrapper contracts; no assertions disabled to
  make the new implementation pass. Test-audit phases kept production frozen.
  One phase-4 insertion script crossed its intended region: integrity check
  caught it, that action was restored from entry snapshot, then reapplied only
  inside the test module. Final protected hashes had zero differences.

## Static self-audit and validation

- [x] Intermediate and final audit: existing system rewritten, no parallel path;
  neutral product contracts; SQL confined to SQLite repositories/adapter;
  same source/attempt/model fences and disclosure boundaries.
- [x] Reviewed changed constructors/callers, migration/Entity/CRUD exports,
  protocol/schema consumers, local model loading, reset and cancellation paths.
- [x] Removed runtime library/import/capability identifiers searched across repo;
  remaining old labels are historical migrations/reset/audit decode or unrelated
  CLI execution segments and authorization capsules.
- [x] cargo fmt --all and cargo fmt --all --check; git diff --check.
- [x] 1,430 JSON schemas parsed; TOML manifests/lock parsed. All 1,655 locked
  packages' dependency edges resolve uniquely; no Memvid package remains.

These are source/static checks, not compilation or runtime proof. Tests,
benchmarks, builds, cargo check, clippy, nextest, application binaries, migrations,
codegen, CI and production cleanup were NOT run. Model parity, runtime behavior
and throughput require execution after external review; no performance claim.
No historical payload import, dual read/write or fallback exists by design.

## Повторная сверка по запросу пользователя

См. SQLITE_MEMORY_SEARCH_AUDIT.md: все 15 разделов и тестовая матрица.
Исправлены оставшиеся removed Entity Column-ссылки в agent-memory upsert
и workspace-wide duplicate discovery под writer. Discovery теперь на reader,
публикация/удаление снова проверяют durable состояние в короткой транзакции.
Предыдущая формулировка полной готовности была преждевременной; аудит исходников
не выдаётся за доказательство компиляции и runtime без исполнения.

## Внешнее ревью: четыре исправления

- [x] Однозначный std::cmp::min в agent FTS search; аналогичные места проверены.
- [x] Только compact artifact IDs и scalar scores материализуются; lazy CASE
  защищает distance, occurrences/RRF/page order сохраняются.
- [x] Directory count обновляется атомарными item-state delta triggers, metadata
  upsert его не затирает. COUNT только при создании/восстановлении directory.
- [x] Bounded page сначала, FTS snippets вокруг lexical match затем; обе памяти.
- [x] Написаны late Unicode snippet и directory lifecycle regressions; reset
  assertion согласован с точным derived count. Тесты не исполнялись.
- [x] cargo fmt --all --check и git diff --check; source/model/ACL fences сохранены.

Редкий initial directory COUNT и native DDL rewrite directory-column не измерены.
Результат требует внешнего ревью, без разрешения на тесты/сборку/commit/push.

## Следующее ревью: lifecycle payload

- [x] Reindex проверяет полный body/revision и lifecycle атомарно; preview не
  восстанавливает оригинал, missing payload не даёт repair_status=ok.
- [x] SQLite prepare_put не пишет; payload/FTS/control/supersession/events
  публикуются одним guarded CrudStore commit, в том числе при in-place update.
- [x] Hash-only repair требует kind/hash, проверяет payload/owner с DELETE;
  чужие и поздно опубликованные владельцы защищены. Повтор идемпотентен.
- [x] Расширены существующие SQLite service regressions: long body restore,
  missing original, cancellation/restart без orphan, реальное scope cleanup.
- [x] Frozen-production test-audit; тесты не запущены. Formatter/static diff
  checks выполнены после выхода из тестовой фазы. Schema/Entity не менялись.

## Cleanup-аудит: memory сохраняется

- [x] Current refill path — memory/locks/workspace-<hash>.lock; workspace fs4
  ownership сохранён, отдельное legacy/global filename удалено. Current inode
  не удаляется. DB WriterOwnership и process leases не менялись.
- [x] Fixed memory/capsules полностью удаляется конечным startup owner вместе
  с service files/empty directories; symlinks не обходятся, held .lock откладывает
  cleanup. memory, memory/locks, models/history/DB сохраняются.
- [x] Existing checkpoint complete v2 отделяет новую обязанность от old complete
  v1; depth budget failure не маскируется completion, EOF не polling.
- [x] Сверены 2 таблицы/17 indexes/16 полей/rename и актуальные Entity; DDL и
  Entity этой итерацией не менялись. Schema regressions покрывают полный состав.
- [x] Дополнены существующие startup/refill/schema owners; test-audit integrity
  без production differences. Тесты не запускались.
- [x] cargo fmt --all --check и git diff --check; реальные instance/processes
  не менялись. Подробнее — последняя секция SQLITE_MEMORY_SEARCH_AUDIT.md.
