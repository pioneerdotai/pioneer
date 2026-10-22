# Передача threads_search и workspace episodic recall на внешнее ревью

Дата: 2026-10-22. Статус: шесть замечаний внешнего аудита к commit
65a23239a48da56118eda3f8358354378610235b исправлены; выполнено повторное самостоятельное
статическое ревью. После подтверждения этих исправлений устранён дополнительный P2
к commit 7783a4ecedf6871265bcfdd37c83c4e321d37cc0: глобальная timeline вытесняла
scoped candidates. Внешняя приёмка этой правки и разрешение на исполнение проверок
ещё не получены.

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
lexical retrieval. Raw BM25 не сравнивается с semantic score: native final order
кодируется отдельным монотонным ключом выше 1 (с сохранением различий query relevance
между URI scopes); continuation BM25 отображается в [0,0.5). Более поздняя lexical
копия не может вытеснить native-ranked источник. Исходная semantic/lexical relevance
отдельно сохраняется для configured cutoff; дополнительный role boost не меняет
native hybrid order. Native semantic/RRF ties имеют frame-ID tie-breaker.
Numeric lexical cursor не объявляется hybrid cursor. Оценка треда —
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
и семи исходным файлам. Добавлены generation getter, продолжение при полном
lexical document window (с учётом известного конечного frame filter), стабильный
BM25 порядок scoped lexical pages, URI prefix в native Tantivy query, progressive
scoped vector overfetch, растущий HNSW search width и исключение inactive frames до
финального vector cutoff. Native ask сообщает saturation фактического vector window
до fusion (24..64), независимо от final output limit. Native RRF/semantic reranking
сохранены; добавлены только стабильные tie-breakers и diagnostic. HNSW остаётся
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
Workspace observation определяется отдельным `ThreadObserveWorkspace`, а не
`RuntimePrincipalPolicy::Absolute`. Новое admission выдаёт этот grant инициатору
после проверки права; он не входит в unconditional `EXECUTION_RUNTIME_ACTIONS`.
Member остаётся `ScopedCollaboration`, но новое разрешённое execution может искать
по всем ACL-доступным roots workspace. Старые/root-only Turn или Agent manifests
без observation grant сохраняют свой ceiling. Широкие права другого collaborator
не добавляют observation authority. Private и sidebar-hidden треды
проверяются обычным resolver; internal — `authorize_internal_thread_via_root`, с
текущим правом на root и проверкой parent execution grant. Root/parent IDs отдельно
не раскрываются. Для internal source вне текущего execution thread нужен Agent
`ChildObserve`, как у history reader. Membership/role/status/revocation проверяются
на каждом вызове. Общий service (включая recall) читает responding AgentExecution
и его сохранённый grant, проверяет fingerprint/identity/root и пересекает allowed
существующего envelope с текущей Agent policy. Отсутствующий Agent grant закрывает
model-facing recall безопасно. Native/CLI materialization и dispatch дополнительно
пересекают это с фактическим BoundAgentActionAdapter; reviewer/messenger не расширены.

Source validation требует active projection, разрешённый source context/visibility,
отсутствие exclusion, живой committed canonical source, отсутствие логического
`turn.message_deleted_at` и совпадающий source hash,
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
  недоступный backend/work budget и конфиденциальность cursor frontier.
  Отдельный shared gateway сценарий проверяет vector readiness, ноль embedding
  calls при lexical, один query call на несколько тредов, vector drift и degradation.
- `message/tests/threads_search_authority.rs`: production admission и реальный Agent
  binding, Search/provider Recall с Current/Workspace labels, старый root-only ceiling,
  reviewer/messenger, native/CLI materialization, обычные и summary anchors.
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


## Повторное ревью после внешнего аудита

Проверенный ранее commit: `65a23239a48da56118eda3f8358354378610235b`.
Исправления оформлены следующим commit этой же ветки (точный HEAD указан в передаче).

| Замечание | Исправление и основное доказательство (тесты только написаны) |
| --- | --- |
| 1. Несуществующий enum variant | В snapshot используется CRUD `ThreadEpisodicItemStatus::Failed`; protocol enum не менялся. Call sites сверены статически. |
| 2. User/Turn permissions обходят Agent ceiling | Общий `SearchAgentAuthority::load` восстанавливает persisted responding AgentExecution/grant через repositories; проверяет JSON fingerprint, identity/root/workspace и применяет `constrain_to_persisted_actions` текущей Agent policy. Tool materialization/dispatch используют `thread_search_authority` и реальный binding. Recall использует эту же persisted границу. |
| 3. Logical Turn deletion | `validate_search_source` проверяет canonical Turn guards до reconstruction. Регрессия вызывает штатный `delete_turn_message`, доказывает сохранность physical turn_item и Active projection, затем исключение из Search/Recall и rejection реально выданного cursor с удалённой canonical boundary. Fallback отсутствует. |
| 4. Member искусственно ограничен root | Отдельный `ThreadObserveWorkspace` совместим с reader. Новое user admission проверяет действие; новые Agent роли thread_agent/observer допускают observation; старые manifests не расширяются. Member остаётся ScopedCollaboration. |
| 5. BM25 отменяет hybrid order | Native final order/RRF/promotion сохраняется монотонным adapter ordering key; raw query relevance отдельно от ordering key. Lexical continuation имеет нижнюю шкалу и не перезаписывает native source при max-score dedup. Нативные ties стабилизированы frame ID. |
| 6. Saturation сравнивается с output limit | Native `AskResponse.vector_candidates_saturated` вычисляется на фактическом vector candidate window до fusion; adapter передаёт его как incomplete. Регрессия max_candidates=128, >64 semantic candidates и небольшое lexical подмножество. |

Новая основная boundary fixture: `message/tests/threads_search_authority.rs`.
Она использует реальный `ExecutionAdmissionService::admit_context`, canonical Agent
execution graph commit и persisted binding factory; runtime/model не запускается.
Покрыты Member roots A/B и недоступный root другого пользователя; positive Search
и provider Recall с Current/Workspace provenance; old/root-only execution; настоящий
reviewer без ThreadRead; messenger без ChildObserve; native/CLI materialization и
реальный search dispatch callback. Проверяются канонические anchors обычных items,
Task summary item и internal child. Старый положительный provider fixture на вручную
расширенной authority заменён этим более сильным production-path доказательством.
Прочие низкоуровневые pagination fixtures сохраняют узкую тестовую authority.

Native regression сравнивает порядок с реальным `Memvid::ask`, отдельно доказывает,
что он отличается от raw lexical order, проходит последующие lexical quanta и
max-score source dedup, затем повторяет retrieval на том же snapshot. Это проверяет
порядок, а не только наличие semantic hit. Source thresholds применяются до сохранения
refs по исходной relevance, а не по adapter rank key.

Статически сверены реальный native provider materialization, предварительный CLI
catalog и его финальная фильтрация перед замораживанием, общий dispatch handler,
provider/host-owned recall, immutable child derivation, source validation и cursor
rehydration. Код соседнего reader читался только для совместимости, не менялся и не
включён в эту ветку. Source refs пригодны для `{threadId,anchor:{turnId,itemId},limit:20}`;
reader должен повторять свою авторизацию. Индексные frame IDs не подменяют anchors.

При финальном самоаудите также исправлено ошибочное предположение нового Agent
resolver о `AgentExecution.parent_thread_id`: у Task это создавшая задачу беседа,
а не текущий child thread. Используется точный canonical Turn response mapping;
существующие `verify_turn_scope`/durable lineage и workspace/root/grant checks
сохранены. При отсутствии response mapping или grant model-facing поиск закрыт.
Сверены native/direct-child и Task initial/reviewer/recovery call sites; их
исполнение, как и остальные тесты, не проверялось.

Фактически выполнялись только чтение/поиск, правки, обычные Git diff/status,
`rustfmt --edition 2024 --config skip_children=true`,
`cargo fmt -p pioneer-gateway -p pioneer-memory -- --check --config skip_children=true`,
`git diff --check`, статическое сравнение vendor и обновление PIONEER.patch по diff
с закреплённым исходником. Запрет тестов, cargo check и любых сборок сохранён.
Ни компиляция, ни прохождение регрессий не подтверждены.

Оставшиеся ограничения: стоимость stateless scan/select и snapshots не измерялась;
HNSW/vector coverage остаётся approximate, насыщение окна явно incomplete; adapter
ordering key не является вероятностью релевантности. Native fusion не заменён новым
движком. Старые executions без persisted Agent observation grant не получают broad
recall автоматически. Следующий этап — повторное внешнее ревью до разрешения запуска
каких-либо тестов или сборок.

## Дополнительный P2: scoped timeline candidate selection

Начальный HEAD этого исправления: `7783a4ecedf6871265bcfdd37c83c4e321d37cc0`,
ветка/worktree сохранены; начальный статус был чистым. Изменены только scoped
retrieval/coverage, регрессия и эта документация с воспроизводимым vendor patch.
Permissions, Member observation grants, logical deletion/source validation,
hybrid ordering, query embedding и проверка saturation не ослаблены.

В `vendor/memvid-core/src/memvid/ask.rs` обе глобальные timeline-подстановки
разрешены только при `scope=None`. Для scoped аналитического запроса сохранены
существующие query expansion/candidate budgets, lexical/vector candidate lists,
native RRF и semantic reranking. Аналитическая эвристика больше не выключает
scoped vector retrieval. Глобальные timeline entries не поступают в ограниченную
fusion, поэтому последующая adapter scope-фильтрация не должна восстанавливать
уже вытесненные ими источники. Поведение unscoped timeline сохранено.

Остаётся штатный native candidate cutoff при fusion/diversification. Даже при
ненасыщенном vector window он может удалить semantic-only источник, который не
вернётся через lexical continuation. Добавлен узкий native diagnostic
`AskResponse.vector_candidates_omitted` (`serde(default)`): bounded set полученных
vector frame IDs сравнивается с конечными native hits. Adapter передаёт
`saturated || omitted` как `incomplete`; алгоритмы fusion/reranking не заменены.
HNSW остаётся `approximate` независимо от этих operational coverage flags.

Одна табличная регрессия на настоящем native/adapter boundary:
`scoped_hybrid_keeps_semantic_candidates_outside_foreign_timeline_windows`
в `crates/memory/src/thread_episodic.rs`. Общий временный индекс содержит первые
100 глобальных timeline entries другого scope, затем 31 scoped semantic-only
источник и 32 lexical-only источника без embeddings. Native индекс заполняется
одним commit с явными timestamps; никаких моков retrieval или новых test seams.

- Обычный lexical miss при top_k=32 сохраняет все 31 semantic источника.
- `compare zzzxylophone over time` также сохраняет все 31 источника за пределами
  глобального аналитического timeline window.
- Native hits до adapter filtering принадлежат нужному scope; первые два случая
  имеют `approximate=true`, `incomplete=false`, lexical `next_cursor=None`.
- Контроль со scoped lexical matches создаёт реальный fusion cutoff при
  ненасыщенном vector window: потеря semantic candidates отражается как
  `vector_candidates_omitted=true` и adapter `incomplete=true`.
- Query-only provider и prepared embedding переиспользуются на native/adapter
  вызовах; document embedding в этой fixture запрещён самим provider.

Регрессия только написана. Сравнение исполнения до/после, компиляция и успешное
прохождение не заявляются. Статически прежний обычный путь смешивает глобальные
timeline и scoped vectors перед top-32 fusion, аналитический путь пропускает vectors;
именно эти две ветки проверяет новая fixture.

Самоаудит охватил оба call site `build_timeline_fallback_response`, scoped поисковые
variants/correction candidates, native fusion/reranking и конечную scope-фильтрацию,
lexical continuation, coverage flags и общий `retrieve_authorized_quantum` /
`scan_shared` для `search_threads` и `recall_authorized_workspace`. Просмотрены новый
diff и совокупный diff от `adb446a542e4c0c24e2d011fe73b184c9af1fb29`, включая исходные
контракты/authority/recall wiring; файлы предыдущих security-исправлений не менялись.

Фактически выполненные проверки этой правки: чтение/поиск (`rg`, `sed`, `cat`),
`git status`, `git rev-parse`, `git branch`, `git diff`, `git diff --check`,
`rustfmt --edition 2024 --config skip_children=true` затронутых Rust-файлов,
`cargo fmt -p pioneer-memory -- --check --config skip_children=true` и статическое
сравнение vendor SHA-256/diff стандартной библиотекой Python. `PIONEER.patch`
обновлён как точный diff к закреплённому upstream; зависимости не обновлялись,
общий Cargo cache не менялся. Новые attached tasks не создавались.

Тесты, cargo check/build/test/clippy/run/nextest, иные сборки, generators,
приложение, Gateway и CLI-агенты для проверки не запускались. Следующий этап —
повторное внешнее ревью; commit и чистота worktree указаны в передаче.
