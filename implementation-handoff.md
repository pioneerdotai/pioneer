# Gateway execution observability — handoff для внешнего ревью

Дата: 2026-11-09. Ветка `feat/gateway-execution-observability`.
Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/gateway-execution-observability`.
Base и текущий HEAD: `16f100cf66fa2614fbb4112d4b27cf8d792012e0` (проверенный main).
Origin: `https://github.com/pioneerdotai/pioneer.git`. Все изменения незакоммичены.

Publication update (2026-11-09): the user accepted this result and authorized a
commit, a normal push, and a GitHub pull request. The review and uncommitted-state
notes below describe the earlier handoff. Publication does not claim compilation
or runtime verification.

## Статус и граница приёмки

Review 1 не принял реализацию. В этом раунде исправлены только R1–R3 из
[review-1.md](/Users/alexander/Code/pioneer/pioneer-proposal/gateway-observability-research-2026-11-09/review-1.md).
Накопленный diff A–E и прежние regressions сохранены. Ни это исправление, ни
самоаудит не являются внешней приёмкой или разрешением исполнения.

A–E реализованы в backend diff: producers → durable repositories → зарегистрированные,
авторизованные RPC. Подготовка реализации и два прохода собственного статического
ревью завершены. Компиляция, генерация и runtime-проверки **не выполнялись**;
это review candidate, внешняя приёмка ещё не получена. Внешних блокировок доступа нет.
Ниже unknown/partial обозначают ограничения фактического evidence и bounded detail,
а не обещание полной внутренней видимости CLI или точного provider invoice.

Исходная рабочая карта: A — identity/ownership/revisions; B — captured historical
contract/usage/decimal resolution; C — lifecycle/transport/tools/waits; D — canonical
и adapter manifests; E — scoped queries/own/inclusive summaries/reconciliation.
Исследование research.md, references-comparison.md, contracts-and-acceptance.md,
production-evidence.json, catalog-evidence.json использовано как навигация и
ограниченное evidence, не universal fixtures. Прочитаны существующие owners,
AGENTS и соответствующие пути Pi/OpenCode; их backend/timers не переносились.

## A. Контракт фактов и ownership

- Authoritative lifecycle: существующие turn/event, turn_execution/window,
  agent_execution, task/run/turn mappings. Writer повторно проверяет workspace,
  thread, turn, непосредственного owner и runtime causal parent; FK OFF учитывается.
- Native: logical request = reasoning/round item; operation = `provider:{turn}:{item}`;
  physical_attempt_id создаётся UsageContext до dispatch. Auxiliary: одно operation
  `provider:journal:{id}`, отдельный physical ID; tools — invocation/item и attempt.
  Повторный внешний вызов создаёт новый attempt; redelivery/SSE refine прежний.
- Связи root_thread/task/task_run/task_execution/execution/window сохраняются из
  проверенных DB relations либо durable parent. Поздние milestones наследуют
  контекст своей попытки, не нового окна. Недоказанная связь остаётся null.
- Runtime task-local parent явно переносится в spawned native tool task,
  orchestrator, compaction, memory extraction. Auxiliary wrapper удерживает
  проверяемый inherited parent также для существующего sync embeddings bridge.
- Source revision и immutable view revision раздельны: reconciliation не занимает
  следующий номер producer. CAS проверяет predecessor sequence и live JSON.
  Заявленное started не доказывает отправку; crashed старый process — unconfirmed.
- Additive: unique latest physical charge leaves; inclusive — обход разрешённых
  causal descendants с visited set. Не складываются inclusive узлы, revisions,
  reported+estimated, reasoning+inclusive output или cache+inclusive input.
  CLI statistics сохраняют фактический scope, но без доказанного leaf baseline
  являются external_usage_snapshot, nonadditive.

## B. Исторический тариф, количества и деньги

Перед внешним dispatch Native durable lane и auxiliary journal подтверждают
initial usage/identity и retained applicable tariff. UsageContext удерживает
Arc catalog/native model evidence до конца stream; одноразовая reservation
проверяет provider/authority/model/API/version/route. Изменённый профиль или
неподтверждённый initial commit останавливает send. Само наблюдение не вызывает retry.

`execution_tariff` хранит содержимое evidence один раз на workspace/content hash
(SHA256), а не только hash. Цена, условия, raw pricing и source provenance доступны
после удаления подробного transcript/source. Attempt capture time хранится отдельно
от source capture/effective time; неизвестный effective_from не придумывается.
Снапшот источника/нормализатора/резолвера/калькулятора, requested/reported/resolved
billing model и фактическая API/route/connection applicability сохраняются раздельно.
Новый каталог не изменяет старые записи. Auto Router разрешает response model по
тому же удержанному catalog; отсутствие применимого entry/mapping остаётся unknown.
Весь публичный catalog не архивируется как full payload каждого запроса.

Поддержаны structured `tiers[].tier={type:context,size}`, `inputTokensAbove`,
snake/camel cache aliases, corroborating legacy context_over_200k; conflicts не
выбираются произвольно. Порог строго `billable inclusive input > N`, UI window
в выборе цены не участвует. OpenRouter retained raw overrides применяются по
подтверждённому контракту в порядке массива, с наследованием отдельных meters.
Vercel raw input/output/cache USD/token масштабируются явно; документированный
`varies_by_provider=false` не трактуется как неизвестное условие, true остаётся conditional. Источники уже существующие;
embedding billing_models отделены от chat picker. Новых probes/источников нет.

Public model/API/endpoint guards сохранены. Private/custom/subscription deployment,
неподтверждённый reported alias, неизвестная route/region/service/modality/retention
не получают стандартную цену. Actual serialized request даёт allowlisted условия;
настройки выполнения не изменяются. Неподдержанный 1h write contract исключает
именно write meter и оставляет известные компоненты, если остальные условия это
позволяют. Дополнительные service units/raw rates видны с unresolved reasons;
unknown rate-changing condition запрещает притвориться, что base — known subtotal.
Неизвестные тарифы embeddings/сервисов не подставляются по похожему имени.

Семантика Pioneer: inclusive input = uncached + read + write; reasoning — subset
inclusive output. Retention split суммируется, только когда обе части известны;
несовпадение split/aggregate, невозможные subsets, отрицательные/дробные counters
дают inconsistent. Gemini candidates/thoughts нормализуются до сложения; отсутствующая
категория не обнуляется. Raw allowlist и API interpretation сохраняются. Cumulative
SSE обновляет одну journal запись; повторный snapshot не начисляется заново.
OpenAI embeddings input-only/cache-not-applicable — узкий документированный случай.

Локальная checked i128 arithmetic: rates — 1e12 units на USD/миллион tokens;
amounts — 1e18 units/USD. Decimal strings authoritative; промежуточного округления
до центов или f64 sum нет. Unsupported precision/range — явный unknown. Legacy numeric
amount сохранён как compatibility projection, reported amount также имеет lexical
представление; подflow через f64 не превращает tiny amount в zero.
Cost lines содержат meter, unit, quantity, scaled rate, decimal amount, currency,
basis; tariff reference принадлежит enclosing attempt. Known subtotal не full total.
Reported zero приоритетен; reported cost и catalog estimate сохраняются отдельно.
Credits группируются по provider basis, unknown currency не агрегируется; fees из
reported total второй раз не добавляются. BYOK upstream details — отдельное evidence,
не выдуманная конверсия/второе начисление.

Reconciliation RPC читает только retained исторический contract, добавляет новую
immutable resolution, проверяет expected sequence и live usage CAS; неизменный
результат — no-op. Source producer revisions и исходное evidence сохраняются.
При следующем изменении интерпретатора нужно явно повысить version; сейчас version 1.
Current-price what-if не изменяет historical totals и отдельным endpoint не добавлен.

Ожидание из fixtures, **не запуск**:

| Fixture | Ожидание |
| --- | --- |
| N=272000, input rate 2 USD/M, output=0 | 0.544 USD, base tier |
| N=272001, higher rate 4 USD/M | 1.088004 USD, structured tier 272000 |
| input130/read100/write20, output20/reasoning10; rates2/0.1/unknown/10 | known subtotal0.00023 USD, partial write |
| old rate2 → новый catalog9 | old0.00022, next0.00099; retained recalculation old0.00022 |
| reported0 + estimate10 | selected reported0, не10 и не сумма |
| parent0.03 + child0.02 | own0.03, inclusive0.05 unique leaves |

## Producers → persistence → query / coverage matrix

| Реальный путь base | Producer и durable storage | Coverage / граница |
| --- | --- | --- |
| Native streaming/non-streaming round, text and tools | ProviderAttemptObservation → acknowledged provider_usage item + compact phases; mutable native journal refinements | Identity/tariff до send, partial/error/late numeric usage, canonical+adapter manifest; first chunk только где наблюдаем |
| OpenAI Responses/Chat, generic Chat-compatible, Anthropic, Bedrock, Gemini и остальные existing Provider trait adapters | общий UsageContext + HTTP helper + Native/Journal owner | API-specific usage, существующая normalization; unknown тариф/API guard не выключается |
| Title API | существующий JournalProvider chat | initial/terminal usage, tariff, phases, canonical/adapter |
| Summary/compaction API, foreground/background | existing summarizer wrapper + context preparation/runtime parent | тот же financial contract, preparation и causal relation; compaction policy не меняется |
| Post-turn memory extraction | существующий memory wrapper, Maintenance, проверенный turn parent | каждый реальный API call и retry отдельный charge identity |
| Self-improvement | existing learner JournalProvider, проверенный self_improvement_run | workspace-owned API view; run ownership, numeric/manifest |
| LLM внутри existing tool/service | orchestrator tool parent → existing JournalProvider | direct call — собственный leaf; arguments/result footprint не новый charge |
| Remote embeddings: OpenAI/OpenRouter/Vercel, indexing/recall/readiness/refill и существующий dimension probe | production resolver с usage store → JournalProvider::embed → journal/facts | больше нет embed delegate bypass; input usage/request ID/model/ошибки vector validation удерживаются; тариф может быть unknown |
| Local embeddings/local voice transcription | локальные существующие исполнители, без external billable adapter | не объявлены внешней платной модельной операцией; новый paid service не создан |
| Web search/fetch/http/download | existing tool lifecycle/storage/observations | local handler duration/sizes; DuckDuckGo/public web path без сообщаемого billing evidence; direct external charge unobserved, не guessed zero |
| Image/audio/media/model server tools | existing model adapter usage/raw allowlist и tool lifecycle | input media/opaque отдельно; известные extra units видны; без применимого meter contract unknown; standalone paid generation/transcription adapter в base не найден |
| MCP/plugin/external services | существующий tool owner/attempt | timing, output stages; внешний внутренний billing без contract unknown. Base не содержит plugin-origin binding; origin явно unobserved, не угадывается из имени |
| Main/auxiliary CLI | существующий импорт statistics и service Call envelope | unchanged external runtime; observed counters/cost scope retained, nonadditive snapshots; internal prompts/retries/compaction/tool phases не выводятся |

Gateway physical attempt означает наблюдаемый adapter invocation; скрытые provider/
upstream transport hops без присланной identity не выдаются за отдельные измеренные calls.
Поведение CLI и соседних features не интегрировалось ради полноты этого inventory.

## C. Timeline и время

Source boundaries: preparation start/terminal; serialized render complete/dispatch
intent; send start, подтверждённый response headers; transport body EOF;
first normalized chunk; first semantic text/reasoning/tool declaration/arguments;
normalized stream EOF; numeric settlement ACK. Body completion и settlement tools
разделены. Формирование tool arguments не называется handler execution. Non-stream
first-token время отсутствует; TTFT не называется GPU prefill/reasoning time.

ToolObservation несёт process run, local sequence, source wall/monotonic points,
до48 typed/lock/approval/handler/output phases и omitted counter. Existing canonical
start подтверждается до handler; phase detail собирается у того же lifecycle owner.
Retry/backoff/permission/window события проецируются content-free; если исходник
не содержит времени, остаётся event order/untimed, а due_at не становится start.
Старый running process — unconfirmed, finish/updated_at не подставляется.
Monotonic clocks разных process не вычитаются; start process хранится отдельно.
Рабочие clock_ms → ns сохраняют реальную 1ms resolution, не обещают наносекундную точность.

Summary различает summed measured work, wall envelope с gaps, union busy и parent
exclusive (минус union пересекающихся same-process children). Clock/containment/
missing-parent anomalies открыты. Span/union bound4096; overflow явно ухудшает
coverage. Outer wall без authoritative interval — null; live owner projections
сохраняют реальные start/complete fields, не дублируют lifecycle. User idle/thread
lifetime не называется inference time. Дети могут перекрываться и выходить за parent.

## D. Context composition / tool output sizes

1. Application canonical: ordered messages, compiled system sections (base,
   instructions/AGENTS, skills, memory/recall/thread context), observable reasoning,
   opaque replay, arguments, results, compaction source refs, tool definitions.
2. Adapter serialized JSON boundary: actual prepared body fingerprint/bytes,
   ordered root components и rendered definitions hash после API transforms.
   Это JSON adapter representation; скрытые server prefixes/network framing не измерены.

Сохраняются role/category/ordinal, bytes/characters, text estimate + version,
source IDs/versions/fingerprints (source-file version иногда только section aggregate,
что помечено), inclusion/replaced/opaque/compaction provenance и limits/reserves.
Резерв не billed input. Только section с included_in_provider_instructions=true
прибавляется к message footprint. TurnContext и недоставленный повторно bundle
inventory — nonadditive reference: фактическая доставка уже измерена в messages.
Разбиение текста/вычитание эвристических совпадений не используется.
Media/base64/opaque не получают exact token estimate.
Server input usage сопоставляется с оценками только через labelled residual;
это не точная token partition. Safe existing locators не дают права читать источник:
ACL/availability перепроверяются существующим source reader. Reconstruction availability
не обещается после cleanup; тексты не архивируются этими manifests.

Ordered canonical hash и rendered hash отдельны от legacy replay fingerprint.
Disclosure reasons берутся из runtime initial/restore/request_tools либо cause_unobserved;
hash change не объявляет причину cache miss. Builtin/MCP/skill binding проверяется,
plugin origin в данной base неизвестен. Provider cache counters не распределяются
на sources пропорционально bytes.

Четыре размера доступны раздельно: captured handler text/prepared source candidate;
реально committed shell fields или serialized turn_llm_context source; actual tool
model projection; request inclusion occurrence в manifest. Retained source metadata
коммитится в той же транзакции, что source, в том числе при HTML release projection;
source cleanup не удаляет measurement/fingerprint. Fields/stdout/aggregate могут
дублировать представление — retained_item_fields_bytes именно сумма stored fields.
Batch/nested children не считаются model-visible output автоматически.

## E. API и права

Зарегистрированы protocol constants/schema source, dispatch, central authorization,
independent read admission:

- `thread/execution/observability`: ThreadRead, scope thread + optional turn/task/
  execution/operation/parent/provider/model/purpose/half-open source-wall period.
- `workspace/execution/observability`: WorkspaceRead, только thread_id=NULL owners.
- Views: summary, paged timeline, contexts, конкретный attempt с evidence и32 children,
  bounded content-free export. Comparison делается по таким же scope/frontier/version
  exports; full prompts и synthetic quality score не выдаются.
- `thread/execution/reconcile` ThreadManage и workspace equivalent WorkspaceManage;
  исторический retained-only recalculation в Maintenance, expectedSequence CAS.

Read page1..100, stable sequence after/through; latest immutable revision определяется
на captured frontier. Фильтрованный page может быть пустым с next cursor: reader
продвигается по scanned rows. Live cumulative usage имеет явный revision/fingerprint,
не обещан frozen usage snapshot. Owner/descendant edges — live DB projection, также
помечены. Detail child continuation — parentOperationId + through/after. Нет долгой
reader transaction и нет выгрузки всех payloads треда клиенту.

Inclusive traversal заново авторизует каждого descendant, следует runtime lineage/
task ancestry/execution links, ограничен1024 threads/64 task ancestors. Cycles,
unknown ownership, unplaced period operations и limits ухудшают coverage явно.
Unknown currencies не складываются; amounts grouped by currency AND basis,
credits также provider. Summary amount groups≤256; overflow остаётся в leaf detail
и счётчике incomplete summary. Cache ratio — суммы known read/corresponding input,
не среднее процентов и не денежная экономия. Usage completeness counts и unobserved
direct tool metering показаны отдельно от completeness известных provider charges;
local duration/footprint не превращаются в расходы. Model breakdown имеет явный
requested-model basis, resolved billing identity доступен в attempt detail.
Allocation method=none/version1,
all_cost_unallocated=true: estimated footprint не выдаётся за provider bill и не
создаёт дополнительные начисления.

## Storage, bounds, installation и recovery

Две новые таблицы, не второй журнал payloads и не sidecar:

- execution_tariff: workspace/hash PK, immutable evidence≤1MiB.
- execution_observation: sequence PK, unique workspace/operation/revision, small
  typed query columns + facts JSON≤64KiB; initial/terminal/derived revisions.
  Indexes workspace frontier; workspace/thread/sequence; parent; task; execution;
  owner; turn/sequence для атомарного cleanup без полного ledger scan.

Migration устанавливает пустые таблицы/indexes и fixed cleanup triggers, не сканирует
старые prompts и не делает financial backfill. Trigger DDL — документированный
SeaQuery gap; ordinary CRUD использует Entity/typed projections/CrudStore.
Thread/turn/workspace delete атомарно удаляет соответствующие facts/journals при
foreign_keys=OFF; owner checks не заменены relations. Tariff snapshots остаются
workspace-owned до удаления workspace, даже если больше не referenced; здесь нет
необоснованного отдельного GC/7-day policy. Источники/full text живут по прежней policy.

Native/aux use один serialized writer, scoped readers и существующий ACK/WorkChange
lifecycle. Numeric snapshots mutate существующий journal, immutable start/terminal
и reconciliation дают read revisions; SSE delta не отдельный financial row.
Обычный successful text stream даёт constant milestones, дополнительно до2*64
boundaries declared tools/arguments. Item lifecycle проецируется из существующих
canonical events. Нет row на каждый text/token delta.

Request manifests≤12KiB,≤64 entries; adapter diagnostic parse≤1MiB, body может быть
больше с size/hash-only coverage. Tool detail≤48 boundaries. Pre-send channel4 с durable ACK остаётся у текущего
owner; post-send mailbox4 принадлежит UsageContext, а HTTP response/body переносит
weak binding (dispatch + physical attempt + loss counter), без sender старого
open scope. Notify будит действующий owner при drain; новых workers нет. Настоящий
body EOF переносится из spawned decoder, protocol terminal не создаёт EOF. Переполнение,
ошибка/cancellation записи и потеря последнего owner учитываются как detail loss;
terminal usage содержит observed/persisted/lost/pending body coverage. Drop не пишет DB. Journal≤32KiB; diagnostic entries
уступают numeric facts, omission counters сохраняются. Финансовое evidence не
sampling/drop; initial persistence failure не допускает send. После send diagnostic
failure не заменяет provider outcome retryable ошибкой; settlement failure оставляет
initial/unconfirmed evidence и явную controlled warning/coverage. Поздняя numerical
refinement проверяет исходный prepared attempt и не открывает execution заново.

Hashing/serialization/preparation выполняются до writer admission, потом короткая
owner/CAS revalidation + commit. Никаких provider/MCP/FS/notification awaits при DB
capacity. Reader fetch bounded; JSON/агрегация после release. Нового scheduler,
writer, OTLP exporter/consent или empty polling нет.

Historical backfill — существующий Gateway startup data-migration owner,
Maintenance quanta32 rows, bytes≤2MiB/native source и32KiB/aux source, durable
projection-meta cursor, poison/unavailable counts. EOF завершает finite pass;
новые producers пишут напрямую. Нет blocking full scan startup gate/paid backfill.
API сообщает отдельно main/aux global completion без cross-scope counts.

## Незапущенные регрессии и самоаудит

Все ниже только написаны/статически прочитаны; fail-before/pass-after не установлен.
Test-audit использован для authoring gate; прямые инструкции задачи имеют приоритет
над test-only freeze, запретом production formatting и требованиями запуска skill/AGENTS.

| Группа | Основное место доказательства |
| --- | --- |
| Historical snapshot/retry/reopen | provider usage old/new retained recalculation; CRUD file-backed close/reopen с настоящими reader/query_only/writer settings, immutable frontier/dedup; existing auxiliary retry/late stream fixtures |
| Source shapes/boundary/TTL/service/modality/route | provider usage tests: 272000/N+1, aliases/conflicts, OpenRouter override order, private/API guards, partial subtotal, unsupported conditions |
| Usage/partial/errors/zero | existing API-specific cumulative snapshot tests + new invalid counters, started-only, reported zero, tiny reported precision; embedding vector-error preserves usage |
| Lifecycle/causality | transport before-send ACK fault/post-send diagnostic loss; CRUD native physical/source validation/redelivery; Gateway nested journal parent, auxiliary pre-drain identity/late usage |
| Aggregation | Gateway real RPC own/inclusive unique leaves, reported zero/credits/USD/bases/unknowns/partial, all-unallocated conservation, period filtering |
| Timeline | CRUD same-process/restart/clock anomaly; Gateway overlap union/exclusive/running/untimed fixtures; source phase intervals/statical wait/backoff caller audit |
| Manifests | order-only tools hash, bounded media/opaque/private payload omission; transport final JSON versus canonical; embedding ordered occurrences; atomic retained-source sizes/cleanup |
| DB/API | real Migrator fresh/up/down/reapply fixtures, FK OFF cleanup/owner conflict, source/view CAS, journal projection rollback/unknown reply replay, central RPC ACL/frontier concurrent append; existing scoped-route/cancellation tests cover shared append path |
| Retention/overload | source cleanup preserves tariff/numeric facts, separately committed retained size, manifest omission bounds, post-send diagnostic failure counters without changing successful provider result |

Самоаудит по этапам и повторно по всему tracked/untracked diff: constructors и
exhaustive enum matches, source versions/protocol registration, exact units,
public/private pricing applicability, producer branches, ownership/CAS/publish order,
source deletion/FK OFF, reader/writer resource lifetime и idle owner contract.
Исправлены выявленные дефекты: authority wrapper reservation; price raw tier aliases;
no known subtotal для started-only; source/view revision collision; backward wall
clock cumulative updates; native journal двойной charge; nested owner/query isolation;
embedding delegate bypass и paid usage после validation error; tool parent propagation;
actual retained-source measurement; provider-phase filters/inherited window; period
unknowns; main+aux backfill coverage; tiny reported cost underflow.

Единственная правка старой migration fixture: task_run_occurrence tracker получает
новую orthogonal schema напрямую registered migration в test setup, без нового
receipt, чтобы не сдвигать проверяемую историческую rollback boundary. Production
assertions/AGENTS не ослаблены. Новых test-only production seams нет.

## Совместимость и manual generated artifacts

#288 `437f51725e913711681ecda448504e5f113c9e81`, #289
`95f1f902cd3e205d9cd4a974b9d6ec8552b6a3ea`, tool-output-pages-and-retention,
native-context-tiers, responses-tool-deltas не merge/cherry-pick/copy.
Disclosure/order, shell projections, replay fingerprints, retention/paging,
context-tier/compaction selection не менялись ради аналитики. Metadata observation
serde-skipped у compiled prompt. Tool source measurement прикреплён к existing
source transaction и не меняет содержание/model delivery. Совместимость проверена
статически по boundaries; merge/compile совместимость ещё не установлена.

Вынужденная ручная синхронизация (не результат запуска генератора):
`crates/entity/src/execution_observation.rs`, `execution_tariff.rs`, `lib.rs`,
`prelude.rs`. Причина — новый schema contract требует typed ORM до CRUD, а запуск
Migrator/SeaORM прямо запрещён. Dense existing format, composite/auto PK, nullable
fields, unique index attributes и exports согласованы вручную. Schema не объявляет
FK relations; ownership/cleanup явные. После отдельного разрешения обязательна
настоящая Entity generation из normal Migrator DB с repository options и сравнение.
Public protocol types/serialization/schema registration — исходники, а не fake
сгенерированные artifacts. API/schema/FFI generation не запускалась; клиентский UI/
FFI plumbing не добавлен. Catalog generator source изменён, сам generator не запускался.

## Проверки, предположения и запреты

Разрешённые команды: read-only git status/branch/rev-parse/cat-file/remote/worktree;
`git worktree add -b feat/gateway-execution-observability .worktrees/gateway-execution-observability 16f100cf66fa2614fbb4112d4b27cf8d792012e0`
выполнен при создании (exit0); статические rg/sed/cat и editing-only Python.
`cargo fmt --all` выполнялся, завершённые проходы exit0. Финальные результаты
check-only команд фиксируются отдельным блоком ниже после их фактического завершения.

Не доказаны компиляция/features/platform compatibility, реальные миграции/codegen,
reader/writer cancellation/concurrency, production latency/write amplification,
дисковый объём на большой базе, реальное поведение upstream и invoice equality.
Новые regressions предназначены именно для последующей отдельной внешней приёмки.
Bounds/query/retention свойства здесь — статические design guarantees, не измеренный
benchmark. Legacy source unavailable, unsupported rate shape/условие, old process
unfinished и неприсланный usage остаются явными пробелами API.

Основной checkout повторно проверен: main/исходный SHA, status чистый. Pioneer-app,
чужие worktrees и production DB не изменялись. Статусы других worktrees могут
меняться их владельцами; их diff не переносился и prune/clean/reset/stash не делались.
Production DB вообще не открывалась. Коммитов/push/PR/remote CI нет. Тесты (включая
новые file-backed fixtures), сборки/check/clippy/metadata, project binaries,
smoke/model requests, migrations, generators и DB probes не запускались.
После handoff автоматического перехода к исполнению нет; внешняя приёмка не получена.

## Полный список изменённых и новых файлов

- `crates/agent/src/agent_loop.rs`
- `crates/agent/src/chat/mod.rs`
- `crates/agent/src/chat/provider.rs`
- `crates/agent/src/chat/tooling.rs`
- `crates/agent/src/compaction/controller.rs`
- `crates/agent/src/compaction/request.rs`
- `crates/agent/src/manager_tests.rs`
- `crates/crud/src/lib.rs`
- `crates/crud/src/repositories/execution_observability.rs` — new
- `crates/crud/src/repositories/mod.rs`
- `crates/crud/src/repositories/provider_usage.rs`
- `crates/crud/src/repositories/turn_event.rs`
- `crates/crud/src/tests/execution_observability.rs` — new
- `crates/crud/src/tests/task_run_occurrence.rs`
- `crates/entity/src/execution_observation.rs` — new
- `crates/entity/src/execution_tariff.rs` — new
- `crates/entity/src/lib.rs`
- `crates/entity/src/prelude.rs`
- `crates/gateway/src/authorization/registry.rs`
- `crates/gateway/src/compaction/background_history.rs`
- `crates/gateway/src/compaction/service.rs`
- `crates/gateway/src/compaction/tests.rs`
- `crates/gateway/src/database/startup/execution_observability.rs` — new
- `crates/gateway/src/database/startup/mod.rs`
- `crates/gateway/src/database/startup/thread_episodic_workspace_capsule_refill.rs`
- `crates/gateway/src/memory_tools.rs`
- `crates/gateway/src/message/agent_runtime.rs`
- `crates/gateway/src/message/dispatch.rs`
- `crates/gateway/src/message/execution_observability.rs` — new
- `crates/gateway/src/message/mod.rs`
- `crates/gateway/src/message/summary.rs`
- `crates/gateway/src/message/tests.rs`
- `crates/gateway/src/message/tests/execution_observability.rs` — new
- `crates/gateway/src/thread_episodic.rs`
- `crates/gateway/src/transport/server.rs`
- `crates/gateway/src/usage_journal.rs`
- `crates/migration/src/lib.rs`
- `crates/migration/src/m20261109_000001_execution_observability.rs` — new
- `crates/protocol/src/agent_event.rs`
- `crates/protocol/src/constants.rs`
- `crates/protocol/src/execution_observability.rs` — new
- `crates/protocol/src/lib.rs`
- `crates/protocol/src/schema.rs`
- `crates/protocol/src/turn.rs`
- `crates/provider/Cargo.toml`
- `crates/provider/src/catalog.rs`
- `crates/provider/src/catalog/generator.rs`
- `crates/provider/src/catalog/runtime.rs`
- `crates/provider/src/catalog/sources.rs`
- `crates/provider/src/http.rs`
- `crates/provider/src/providers/anthropic.rs`
- `crates/provider/src/providers/bedrock.rs`
- `crates/provider/src/providers/chat_completions_api_compatible.rs`
- `crates/provider/src/providers/embedding.rs`
- `crates/provider/src/providers/gemini.rs`
- `crates/provider/src/providers/openai.rs`
- `crates/provider/src/providers/openrouter.rs`
- `crates/provider/src/providers/responses_api_compatible/tests.rs`
- `crates/provider/src/providers/vercel_ai_gateway.rs`
- `crates/provider/src/registry.rs`
- `crates/provider/src/traits.rs`
- `crates/provider/src/types.rs`
- `crates/provider/src/usage.rs`
- `crates/provider/src/usage/manifest.rs` — new
- `crates/provider/src/usage/tariff.rs` — new
- `crates/provider/src/usage/tests.rs`
- `crates/provider/src/usage/transport.rs` — new
- `crates/tools/src/events.rs`
- `crates/tools/src/observation.rs`
- `crates/tools/src/orchestrator.rs`
- `crates/tools/src/runtime.rs`
- `implementation-handoff.md` — new

## Результаты проверок исходного A–E diff перед review 1

- `cargo fmt --all` — последний проход завершился exit0.
- `git diff --check && cargo fmt --all --check` — финальная последовательность
  завершилась exit0, stdout/stderr пустые. Это formatting/whitespace, не compile check.
- Для каждого из13 untracked файлов: `git diff --no-index --check /dev/null <file>` —
  нет whitespace diagnostics, exit1 означает new-file diff; статический wrapper exit0.
- Статическая сверка git tracked/untracked inventory с разделом handoff — совпали
  все72 файла. Постороннего formatter diff/AGENTS/lockfile/client/UI diff нет.
- `git status --short`, `git branch --show-current`, `git rev-parse HEAD` в main —
  чистый checkout, main, `16f100cf66fa2614fbb4112d4b27cf8d792012e0`.
  Собственный worktree имеет тот же HEAD, все72 файла оставлены незакоммиченными.
- Последние исправления самоаудита дополнительно включают Vercel fixed/conditional
  flag и cache rates, malformed raw quantity completeness, сумму несовместимых
  amount groups с явным overflow, ordered embeddings manifest и file-backed reopen
  regression. Все соответствующие тесты остаются **незапущенными**.

Ни одна из этих статических проверок не разрешает тесты, codegen, builds или runtime.
Внешняя приёмка отсутствует; после отчёта исполнение не продолжается автоматически.

## Исправления review 1: R1–R3

### R1 — additive request occurrence, nonadditive bundle reference

Production owners: `agent/chat/mod.rs::compiled_prompt_payload_from_delivery_plan`
→ `provider/usage/manifest.rs::request_manifest` → Gateway `context_breakdown`.
Прочитаны native_instruction_policy, turn_context_with_inventory_baseline,
prepare_context_metadata и prepare_turn_input_projection: TurnContext уже является
частью user content. Agent/manifest теперь явно отмечают footprint inclusion;
Gateway прибавляет только подтверждённые provider-instruction sections, а остальные
оставляет nonadditive reference с отдельным count/basis. Для ранее сохранённых
manifests используется уже существующий included_in_provider_instructions; неизвестный
flag не доказывает inclusion. Model-facing content, delivery policy, inventory,
compaction, estimator и replay/tool fingerprints не изменены.

Расширенная existing contexts RPC regression содержит 12 bytes provider instructions,
23 bytes actual user occurrence с уже включённым 3-byte TurnContext и 16-byte
недоставленным inventory reference. Независимое ожидание: 35 additive bytes,
9 estimated text tokens, при input 20 diagnostic residual 11; две nonadditive references.
Проверяются отсутствие secret content в response и прежний ACL negative control.
Тест не запускался. Промежуточный самоаудит сверил producer flags, cloning metadata,
старые manifests и суммирование без пересечения представлений.

### R2 — body-polling binding и request-owned lifetime

Production owners: UsageContext сохраняет BodyObservations через open→drain;
HTTP send после pre-send ACK помещает DispatchObservation в response extensions;
read_response_bytes_bounded/bounded_response_stream фиксируют **реальный** EOF у
владельца body polling, в том числе в существующем spawned decoder. Запись использует
captured dispatch/physical attempt identity, а не task-local producer task.
Post-send callback синхронно кладёт только content-free milestone в bounded mailbox4;
он не ждёт ACK от закрытого receiver, не держит живым request owner и не выполняет DB.
Открытый/следующий scoped consumer читает ту же очередь, просыпаясь через Notify.
Admission channel4 с durable pre-send ACK не смешан с post-send diagnostic mailbox.

Ошибка post-send persistence не меняет provider result. Overflow, cancellation
in-flight handler, queued facts при drop последнего owner и поздний producer после
исчезновения owner увеличивают loss counter. Persisted accounting coverage различает
actual EOF observed/persisted/lost/pending и unobserved. При cancellation/crash без
terminal ACK initial durable attempt остаётся unconfirmed; фоновой DB записи из Drop
нет. Early protocol terminal, consumer EOF и settlement не являются HTTP EOF; SSE
runtime, stopping policy и число provider calls не изменены. Body может законно не
дойти до EOF — тогда boundary остаётся unobserved.

Незапущенные regressions:

- Existing transport admission/diagnostic-failure fixture сохранена; новый реальный
  HTTP bounded-body fixture переносит polling в tokio::spawn и закрывает open scope
  до drain. Проверяются identity, normal drain, diagnostic failure с сохранением 7,
  early terminal/cancellation без EOF, EOF после исчезновения owner как explicit loss.
- Gateway owner fixture использует настоящие JournalProvider/OpenAiProvider,
  spawned Responses decoder и local chunked response. Server gate отдаёт настоящий
  body EOF после возврата stream_chat; отсутствие protocol terminal остаётся provider
  failure. Usage journal и авторизованный RPC detail должны видеть тот же body milestone.
  Это написанный локальный fixture, не выполненный HTTP/model/API probe.

Промежуточный самоаудит: перечитаны HTTP helper callers и spawn paths Responses,
Anthropic, OpenAI Chat, OpenRouter, Gemini, Native scope и auxiliary unfold/drain.
Binding не содержит sender или DB closure; locks снимаются перед handler await,
предельные очереди и cancellation guards проверены статически. Compile/Send/runtime
свойства ещё требуют отдельно разрешённого исполнения.

### R3 — retained interpretation для конкретного source evidence

Production owners: legacy auxiliary backfill → immutable execution_observation;
Gateway historical_reconcile и observation_facts; существующий exact JSON/sequence CAS.
Backfill сохраняет SHA256 точного исходного journal JSON (bounded32KiB) вне DB capacity.
Read overlay применяется только к доказанно новой versioned revision. Unversioned
journal не затирает derived accounting; совпавший source fingerprint сохраняет
interpretation, изменившийся получает explicit reconciliation-required/order-unconfirmed
coverage. Новый hash сам по себе не устанавливает время/новизну. Explicit reconciliation
может выбрать точный current unversioned JSON под существующим CAS; сохраняет fingerprint
и interpretation в той же immutable revision. Для старых facts без fingerprint нет
догадки по wall clock; при явном пересчёте binding устанавливается по exact live JSON.

Повторная reconciliation без изменения source/calculator — no-op; при изменении
source/интерпретации новая immutable revision не становится новым charge. Journal и
оригинальные facts/evidence не переписываются. Более новый versioned cumulative snapshot
продолжает уточнять usage/view по прежнему source revision contract. Нового revision
engine, таблиц, migrations или current-catalog lookup нет. Reference-only legacy
applicability остаётся unresolved.

Новая регрессия на existing Gateway/CRUD harness проходит legacy journal → normal
backfill → reconcile RPC → повторные Attempt/Summary reads → same-source redelivery
→ no-op reconciliation → versioned producer refinement/redelivery. Два независимых
contract cases: applicable retained tariff даёт 0.00004 USD, reference-only сохраняет
unconfirmed_historical_applicability и unknown; позднее input 20 даёт 0.00006 USD при
том же числе 2 unique leaves. Чтение старого immutable frontier отдельно проверяет
live overlay доказанного нового versioned snapshot. Проверяется неизменность
исходных facts/tariff/journal до настоящего producer update. Тест не запускался.
Промежуточный самоаудит проверил оба missing-revision branches, no-op source binding,
source/view revisions, exact live JSON CAS, conservative applicability и общую read
функцию для Attempt/Summary/Timeline/Export.

### Финальный самоаудит раунда

Повторно проверены накопленный diff A–E и новые modules по schema/Entity/exports,
CRUD ownership/cleanup/CAS, producer lifecycle/error paths, source sizes, tariff units,
RPC registration/ACL/pagination, tool/auxiliary/CLI boundaries и prepared fixtures.
Исправления ограничены R1–R3 и синхронными regression/constructor/документальными
правками. Schema/Entity/API-generated mirrors этого раунда не менялись; прежнее
manual Entity exception и необходимость настоящей генерации остаются в силе.
Полный inventory по-прежнему содержит 72 файла; новых имён файлов в этом раунде нет.
Непосредственные правки R1–R3 находятся в десяти файлах:

- `crates/agent/src/chat/mod.rs`
- `crates/provider/src/usage/manifest.rs`
- `crates/provider/src/usage/transport.rs`
- `crates/provider/src/http.rs`
- `crates/provider/src/usage.rs`
- `crates/provider/src/usage/tests.rs`
- `crates/crud/src/repositories/execution_observability.rs`
- `crates/gateway/src/message/execution_observability.rs`
- `crates/gateway/src/message/tests/execution_observability.rs`
- `implementation-handoff.md`

Остальной A–E diff сохранён для повторного полного внешнего ревью. Промежуточный
и финальный самоаудит не подтвердили compile/runtime/concurrency/performance;
эти предположения остаются перечисленными выше. Model-facing input и число платных
вызовов не менялись ради исправлений R1–R3.

### Реальные результаты разрешённых проверок раунда R1–R3

- `cargo fmt --all` — завершённые проходы exit 0; последний выполнен после
  дополнения regression чтением старого frontier.
- `cargo fmt --all --check` — exit 0, stdout/stderr пустые.
- `git diff --check` — exit 0, stdout/stderr пустые.
- `git diff --cached --exit-code` — exit 0, index пуст.
- Для каждого untracked файла выполнен `git diff --no-index --check /dev/null <file>`:
  все 13 файлов без whitespace diagnostics; exit 1 означает наличие new-file diff.
  Статический Python wrapper только вызывает эти read-only Git проверки и сверяет
  имена с handoff; итог exit 0, inventory совпал: 72 файла.
- `git branch --show-current`, `git rev-parse HEAD`, `git worktree list`,
  `git remote get-url origin`, `git cat-file -t 16f100cf66fa2614fbb4112d4b27cf8d792012e0`
  подтвердили собственные branch/worktree, исходный HEAD/base, origin и commit.
- В основном checkout повторные `git status --short`, `git branch --show-current`,
  `git rev-parse HEAD` дали пустой status, main, исходный SHA. Чужие worktrees,
  pioneer-app и production DB не менялись; production DB не открывалась.

Это только статические formatting/whitespace/inventory проверки. Незапущенные
регрессии не объявляются прошедшими. Тесты, сборки/check/clippy/metadata/run,
генераторы/миграции, project binaries, HTTP/model/DB probes и remote CI не запускались.
Коммитов/push/PR нет; весь diff оставлен незакоммиченным в собственном worktree.
Внешняя приёмка пока отсутствует. После этого handoff автоматического перехода
к исполнению нет.
