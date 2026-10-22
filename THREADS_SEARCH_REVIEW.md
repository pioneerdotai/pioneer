# Передача threads_search и workspace episodic recall на внешнее ревью

Дата: 2026-10-22. Статус: реализация и самостоятельное статическое ревью завершены;
внешняя приёмка и разрешение на исполнение проверок ещё не получены.

- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-search-recall`.
- Ветка: `feat/threads-search-recall`.
- Исходный commit: `adb446a542e4c0c24e2d011fe73b184c9af1fb29` (локальная main).
- Финальный commit: см. `git rev-parse HEAD` этой ветки; этот отчёт входит в него.
- Основной checkout и соседние worktree не изменялись. Merge/push/deploy не выполнялись.

## Контракт инструмента

Скрытый `threads_search` открывается через домен `threads`, требует `ThreadRead`,
не включён в core/task domain. Native и выбранный фиксированный CLI-каталог используют
один handler. Операция не меняет unread/read cursor, участников, title или placement,
не создаёт Task/Turn и не генерирует модельный ответ.

Первая страница:

```json
{"query":"где обсуждали настройку провайдеров и выбор модели","limit":20,"cursor":null}
```

Следующий вызов, включая продолжение незаконченного вычисления страницы:

```json
{"query":"где обсуждали настройку провайдеров и выбор модели","limit":20,"cursor":"<nextCursor из предыдущего ответа>"}
```

`query` обязателен, непустой, до 4096 символов; `limit` — 1..20; `cursor`
отсутствует/null либо непрозрачная строка до 65536 символов. Другие аргументы
отклоняются, в том числе workspaceId/userId/mode/provider/model/vectors/weights.

Ответ:

```json
{
  "threads": [],
  "nextCursor": "<продолжение>",
  "status": "work_budget",
  "approximate": false,
  "incomplete": true
}
```

В карточке: `threadId`, актуальный разрешённый `title`, `createdAt`, `updatedAt`,
`relevance`, до трёх `fragments`. Каждый fragment содержит `workspaceId`, `threadId`,
`turnId`, `itemId`, `sourceKind`, `text`, `relevance`. Название ограничено 256 символами,
текст фрагмента — максимум 1200. Используется существующая structured output policy;
JSON не обрезается. Название не берётся из native frame title.

Состояния:

- `more`: сформирована страница, следующий cursor начинает следующую страницу.
- `exhausted`: обход доступной выдачи завершён; `nextCursor=null`. При `approximate=true`
  это исчерпание карточек выбранного native retrieval, а не обещание exact semantic recall.
- `work_budget`: итоговая страница ещё вычисляется; cursor продолжает frontier,
  временная выдача не публикуется.
- `index_unavailable`: индекс/refill ещё не готов либо backend временно недоступен;
  `incomplete=true`. Если frontier уже установлен и его snapshot пригоден, cursor
  позволяет повторить неудавшийся quantum.
- `partial`: карточки доступны, но покрытие неполное — например, lexical degradation
  при недоступной vector-конфигурации, pending/failed projections, недоступные segments
  или заполненное ограниченное semantic окно (также при source validation,
  отфильтровавшей все hybrid candidates).

`nextCursor` отражает наличие продолжения независимо от `status`; `incomplete`
и `approximate` — разные признаки. При неполном покрытии `nextCursor=null` не является
доказательством полного поиска. Изменившийся индекс, конфигурация, authority, query,
limit, повреждённый/чужой/истёкший cursor требуют нового поиска.

## Lexical и hybrid continuation

Алгоритм до редактирования записан в
`crates/gateway/src/thread_episodic/SEARCH_REVIEW.md`.

Сервер keyset-порциями обнаруживает треды по indexed source metadata, проверяет
текущую авторизацию и проходит все workspace capsules/lexical continuation quanta.
Directory title/recency/sidebar не отсекают корпус. При выключенных vectors query
embedding не вызывается. При совместимом готовом vector projection query один раз
подготавливается через существующий `embed_query_checked` и используется всеми segments
этого вызова. Provider, model, dimension и normalization принадлежат существующему resolver.

На первом quantum каждого thread/segment вызывается native hybrid ask с
`context_only=true`; его fusion/reranking сохраняются. Последующие quanta продолжают
lexical retrieval. Numeric lexical cursor не объявляется hybrid cursor. Оценка треда —
максимальная оценка проверенного источника по всем фиксированным quanta; порядок
`relevance DESC, threadId ASC` не зависит от размера возвращаемой страницы.

Для очередной страницы cursor удерживает только `limit+1` лучших selections,
до трёх canonical references на тред, текущий frontier и предыдущую границу рейтинга.
После полного прохода это следующие карточки в общем порядке. Следующая страница
повторяет проход строго после последней `(score, threadId)` границы. Поэтому треды
за первой 32/128 выборкой и последующими segments достижимы; один тред не повторяется.
Каждый вызов обрабатывает до 32 retrieval/discovery quanta; рабочий cutoff имеет
продолжение, а не маскируется под EOF. Recall дополнительно сохраняет существующие
candidate/segment/prompt budgets и внешние hook deadlines, явно помечая неполноту.

Cursor защищён process-local HMAC, ограничен 15 минутами и связан с операцией,
workspace, initiating principal, durable execution envelope/turn, policy generation,
query/limit, embedding identity/configuration и фактическим query-вектором,
native capsule generations и source
projection metadata. Snapshot проверяется до/после retrieval. После рестарта cursor
недействителен. Frontier, пересёкший запрещённые треды, сериализуется как отдельный
HMAC-токен; запрещённые IDs не попадают даже в декодируемый payload cursor.
Накопленные references заново авторизуются и hydrate на каждом вызове и перед ответом.

## Изменение зависимости

Зафиксированная `memvid-core 3.1.8`, commit
`fe0b3d9ac883c667587c262f7b372bf06eb605d5`, воспроизводимо включена в
`vendor/memvid-core` через Cargo patch, без обновления версий зависимостей.
Причины: нужен committed generation getter; native lexical top document window
не доказывает EOF; lexical/vector URI filtering после глобального top-k теряет
scoped results. Lexical scope теперь входит в query по уже существующему URI index.

`vendor/memvid-core/PIONEER.patch` содержит весь patch к исходному Cargo manifest
и пяти исходным файлам. Добавлены generation getter, продолжение при полном
lexical document window (с учётом известного конечного frame filter), стабильный
BM25 порядок scoped lexical pages, URI prefix в native Tantivy query, progressive
scoped vector overfetch, растущий HNSW search width и исключение inactive frames до
финального vector cutoff. Native RRF/semantic reranking не переписаны. HNSW остаётся
приближённым. Пустой quantum без cursor progress при полном candidate window —
явная ошибка индекса, а не ложный EOF. Оригинальные исходники в Cargo cache не менялись.
Vendor исключён из членов workspace; upstream examples/bench targets не копировались.

## Общая цепочка и реальные потребители

`thread_episodic/search.rs` выделяет подготовку authority/config/readiness/query,
`retrieve_authorized_quantum`, `scan_shared`, grouping/continuation и повторную
hydration. `thread_episodic.rs::validate_search_source` общий для свежего retrieval,
cursor hydration и прежнего адресного current-thread service.

Реальные consumers:

1. `message/threads_search.rs` → `ThreadEpisodicRecallService::search_threads` →
   `scan_shared` → memory backend `retrieve_segment`.
2. `ThreadEpisodicMemoryRecallProvider` и host-owned `ThreadContextRecallProvider` →
   `WorkspaceEpisodicRecallService` / `recall_authorized_workspace` → тот же
   `scan_shared` / `retrieve_authorized_quantum`.

Provider `recall_current_thread` сохраняет адресный scope через `current_thread_only`.
Related/workspace provider операции используют ту же общую область. Старые внутренние
bounded service APIs и их regressions сохранены для совместимости; штатные новые
model-facing paths не используют directory preselection.

Без второй системы переиспользованы ingestion, index executor, workspace capsules,
message embedding artifacts, refill/configuration readiness, provider resolver,
query preprocessing, file locks/read-only opens, domain repositories, native ask,
ranking, canonical reconstruction и recall-event persistence. Не добавлялись таблицы,
миграции, постоянные search sessions, новый provider/HTTP client/credentials или UI.

## Default recall и доступ

`message/mod.rs` подключает `with_workspace_service`. Deterministic planner выбирает
один workspace query при доступной capability. Full-input host-owned path использует
тот же workspace retrieval. Current/Workspace provenance разделены в provider output
и prompt contributions, с реальными workspace/thread/turn/item references.

Ранний host-owned hook помечает выполненную попытку во внутренних diagnostics;
поздний active hook не повторяет current/related/workspace поиск, сохраняя task/durable
работу. Это действует и для пустого результата. No-use/policy/config gates и budgets
сохранены. Существующие active/deterministic conditions не заменены безусловной
загрузкой всего workspace в prompt. Projection dedup использует действующие source
и projection group identities; равный текст независимых тредов не схлопывается.

Principal извлекается из проверенного `ExecutionAuthorizationContext::load_for_turn`,
включая durable child/task lineage, а не из последнего автора/identity/model arguments.
`AgentTurnHookRuntimeContext` несёт server-owned initiating principal/capabilities;
проверены actual native admission/dispatch, task initial/revision/reviewer/restore,
CLI post-turn и recovery snapshot consumers. DTO related/workspace recall теперь
передают настоящий turnId. Отсутствующий hint допустим только когда initiating principal
восстановлен из валидного durable envelope; отсутствие самого authority закрывает доступ.

Root action/manifest revalidates; текущий principal загружается заново при проверках.
Для `ScopedCollaboration` действует дополнительный предел корневого execution capsule,
для `Absolute` — доступный текущему инициатору workspace. Выход за эти ограничения
не разрешается широкими правами другого collaborator. Private и sidebar-hidden треды
проверяются обычным resolver; internal — `authorize_internal_thread_via_root`, с
текущим правом на root и проверкой parent execution grant. Root/parent IDs отдельно
не раскрываются. Membership/role/status/revocation проверяются на каждом вызове.

Source validation требует active projection, разрешённый source context/visibility,
отсутствие exclusion, живой committed canonical source и совпадающий source hash,
а также соответствие index metadata канонической identity/hash. Missing/deleted/stale
source не восстанавливается из старого текста индекса. Названия/snippets/references
и ошибки не раскрывают запрещённых кандидатов.

## Контракт anchor для будущего history reader

Fragment `workspaceId/threadId/turnId/itemId` указывает на реальную canonical source
occurrence; `sourceKind` различает `user_visible_thread_item`,
`user_visible_task_summary` и `thread_compaction_summary`. Это не native frame ID и
не index_item_id. History reader может открыть указанное сообщение либо разрешённую
summary-проекцию и продолжить историю старше/новее anchor. Он обязан заново проверить
доступ, существование и source kind. Child hit сохраняет собственные IDs. Сам reader
здесь не реализован.

## Самоаудит и тесты

При промежуточном и полном ревью исправлены: потерянные runtime fields при actual
native dispatch; отсутствие workspace provider/capabilities; duplicate host/active
search; неверная CurrentThread маркировка; текстовая межтредовая дедупликация;
missing-canonical fallback; отсутствие обязательного indexed source hash; поздний
snippet, обрезавшийся до начала сообщения; source mutations без capsule update;
утечка denied frontier ID через cursor; internal-root authorization; sticky partial
после успешного повтора backend; сохранение candidate/segment budgets у recall;
явная неполнота заполненного semantic top-k; проверка дрейфа query-вектора;
отклонение пустого recall query до retrieval и неполнота при отсутствии capsules.
После разделения Current/Workspace contributions восстановлен общий host prompt
budget, включая provenance headers и artifact references; удалён тест старого
workspace-visible фильтра, больше не вызываемого production-кодом.
Исправлено lexical scope post-filtering до глобального TopDocs cutoff; добавлен
сценарий редких совпадений одного треда по обе стороны большой чужой выборки.

Добавлены/расширены тесты (только написаны):

- `thread_episodic/search_tests.rs`: multiple pages/late candidate quantum/later capsule,
  grouping, independent equal text, private/sidebar-hidden, два Member одного workspace,
  internal root ACL, отзыв доступа и foreign cursor, cursor query/generation binding,
  missing authority, deleted/excluded/stale/missing canonical sources, prompt budget,
  настоящий workspace provider с восстановлением инициатора и Current/Workspace labels,
  недоступный backend/work budget и конфиденциальность cursor frontier.
  Отдельный shared gateway сценарий проверяет vector readiness, ноль embedding
  calls при lexical, один query call на несколько тредов, vector drift и degradation.
- `memory/thread_episodic.rs`: реальный native lexical traversal за первым candidate
  window; scoped semantic-only источник за global top-k; hybrid adapter/query-only
  provider; отсутствие query call при lexical; reuse подготовленного query.
- `thread_episodic_embedding.rs`: query-path reuse без document embedding.
- memory hook tests: один default workspace query, full-input, no-use, provenance,
  independent equal text, отсутствие второго поиска после host attempt.
- agent preflight: host-owned full-input composition для current/workspace scope.
- gateway host hook: разные provenance sections через штатный prompt context
  aggregator сохраняют один общий лимит и корректные workspace source refs.
- tools/native catalog: строгие arguments/bounds, default hidden, threads-domain reveal,
  host permission block, ThreadRead и отсутствие Task operation classification.
- Существующие fixtures скорректированы для канонических source records/hash вместо
  ожидания индексного fallback текста.

## Выполненная проверка и ограничения

Выполнялись только чтение/поиск (`rg`, `sed`, `cat`), обычные Git-команды, редактирование
файлов, форматирование, `git diff --check`, статический разбор TOML стандартной библиотекой
Python и сравнение vendored файлов SHA-256 с закреплённым исходником. Полный собственный
diff просмотрен, включая новые файлы; неизменённые vendored файлы сверены по hash,
все реальные изменения движка просмотрены отдельным diff/patch.

Команды форматирования:

```text
cargo fmt -p pioneer-gateway -p pioneer-crud -p pioneer-memory -p pioneer-tools -p pioneer-agent -- --config skip_children=true
cargo fmt -p pioneer-gateway -p pioneer-memory -p pioneer-crud -p pioneer-tools -p pioneer-agent -- --check --config skip_children=true
rustfmt --edition 2024 --config skip_children=true <затронутые новые/изменённые Rust-файлы>
git diff --check
```

**Ни тесты, ни cargo check/build/test/nextest/clippy/run, ни иные сборки, приложение,
Gateway, CLI-агенты для проверки или schema generators не запускались. Компиляция и
успешное выполнение тестов не заявляются.**

Ограничения для оценки внешним reviewer:

1. Stateless scan/select намеренно дороже глобального top-k: повторяет corpus retrieval
   для каждой итоговой страницы и читает projection metadata/capsule generations для
   snapshot-проверки на каждом вызове. БД читается короткими bounded batches без удержания
   capacity во время файлов/embedding/ranking. Gateway не загружает весь transcript или
   embeddings, но latency на большом корпусе требует измерения после разрешения проверок.
2. Semantic coverage приближённое и ограничено native candidates; `approximate` не
   означает operational failure, а `partial/incomplete` не означает полный EOF.
3. Readiness/backend degradation может инвалидировать cursor, чтобы не менять порядок
   и смысл уже начатого обхода. При transient native retrieval failure frontier
   сохраняется для повтора; при недоступности fingerprint/readiness потребуется новый поиск.
4. Старые recovery snapshots могут не содержать preflight capability projection.
   Initiating authority восстанавливается заново; actual recall provider и host path
   используют текущие checks/configuration. Отсутствующая preflight projection закрывается
   безопасно; general recovery не получает новые capability grants из старого JSON.
5. Контракты tool задаются непосредственно штатной tool schema; генерация схем не
   потребовалась и не запускалась. Непроверенную компиляцию должен оценить внешний reviewer
   до отдельного разрешения любых исполняемых проверок.

Для распределения реализации использованы переданные пользователем skills
[pioneer-system-subagents](/Users/alexander/.codex/skills/pioneer-system-subagents/SKILL.md)
и [pioneer-system-tasks](/Users/alexander/.codex/skills/pioneer-system-tasks/SKILL.md).
Прикреплённые исполнители завершены; их изменения проверены и интегрированы родителем.
Самостоятельное ревью не является внешней приёмкой.
