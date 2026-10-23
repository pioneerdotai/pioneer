# Threads search / episodic recall v2: передача на внешнее ревью

Статус: замечания внешнего аудита исправлены, код передаётся на повторное внешнее ревью. Тесты и компиляция не проверены; окончательная внешняя приёмка не получена.

- База: `a61a51409603de887d92630e44c6c4f54932109b`, актуальный локальный `main` на момент создания worktree.
- Первая реализация: `54e6bc267bf36239f75167ab11aa8fc33d2cf1d0`; проверенный внешним аудитом HEAD с отчётом: `f6d211d846102c3165d4e9623fc26fa3858f7d16`.
- Итоговый commit кода после исправлений: `6e002986372487adec6fd1fbd1717a961f16107c`. Следующий commit меняет только этот отчёт. Итоговый HEAD передачи указан в финальном сообщении и доступен через `git rev-parse HEAD` в worktree.
- Ветка: `feat/threads-search-recall-v2`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-search-recall-v2`.
- PR, push, merge и deploy не выполнялись. Реализация PR #162 не использовалась.

## Карта требований и файлов

Карта была составлена до изменения кода: каналы и ранжирование; каноническая валидация; общая авторизация; сессии; подключение существующих hooks; native/CLI и протокол; регрессии.

| Область | Файлы и результат |
|---|---|
| Нейтральный контракт | `crates/memory/src/episodic_search.rs`: scope, явный work budget, готовый query embedding с identity, упорядоченные ссылки на источники, состояние каналов, RRF |
| Memvid adapter | `crates/memory/src/thread_episodic.rs`: direct `search` / `search_vec`, metadata-only кандидаты, проверка identity, bounded partitions; существующий публичный API зависимости |
| Общий поиск | `crates/gateway/src/thread_episodic/search.rs`: фабрика текущего адаптера, подготовка embedding, валидация канонических источников, causal projection groups, детерминированное ранжирование, ограничения |
| Канонические источники и recall events | `crates/gateway/src/thread_episodic.rs`: переиспользованный source resolver, request-scoped восстановление, отсутствие fallback на индекс, consumer budgets / запись events |
| Авторизация и dispatch | `crates/gateway/src/message/thread_read_tools.rs`: существующий ThreadReadAuthority / thread_source_readable, immutable Agent ceiling до/после materialization, спецификация и общий handler |
| Сессии и страницы | `crates/gateway/src/message/thread_search.rs`, поле в `message/mod.rs`: RAM snapshot ссылок, повторная валидация, существующий confidential cursor codec |
| Recall | `thread_episodic_hooks.rs`, `memory_tools.rs`, `message/mod.rs`: один production provider для current hook и active recall, workspace search без каталожного shortlist |
| Production preflight | `crates/agent/src/{lib.rs,agent_loop.rs,chat/mod.rs}`, `crates/memory/src/hooks/recall.rs`: согласованный runtime snapshot provider, разрешённые capabilities до формирования запроса существующего планировщика |
| Memory pipeline | `crates/memory/src/hooks/{providers.rs,active_recall.rs,handlers/active_recall.rs}`: реальный turnId в cross-thread requests, reuse внутри выполнения, дедупликация, общий episodic prompt budget |
| Контракты | `crates/protocol/src/{thread_read.rs,lib.rs,schema.rs,turn.rs}`, `schemas/threads_search_{params,response}.json`: типизированный контракт, schema registration, output policy |
| Каталог | `crates/tools/src/domain.rs`, `crates/gateway/src/authorization/catalog.rs`: только threads, скрытость через существующий domain selection; NativeToolFamily автоматически использует общий каталог читателей и общий dispatch |

## Граница общего поиска

Общий цикл работает со scope и предикатом серверной read authority, references/version, канальными позициями, каноническим текстом/provenance и итоговым RRF score. Recall-типы score breakdown, prompt budgets и recall events находятся у потребителя. Пара независимых аргументов lexical/semantic provider позволяет подключить другой semantic источник без реализации lexical через него.

Пути, capsules/segments и frame IDs остаются внутри адаптера. В общем ранжировании нет `memvid_score`, внутренних cursors/generation Memvid, ask-политики, BM25/vector arithmetic, старого adaptive cutoff либо старых relevancy thresholds. RRF использует k=60. Повторения одного causal projection group дают не более одного вклада на канал; представитель и равенства разрешаются через реальные канонические IDs. Независимые совпадающие тексты остаются отдельными источниками.

Индексирование, dependency source/version, формат хранения, embedding identity, reconciliation и durable MemoryBackend не заменены. Chroma не реализован.

Production tool и оба production hook-пути вызывают `MessageProcessor::search_thread_sources` → `ThreadEpisodicRecallService::search_sources`. Существующие legacy episodic facades/evaluations оставлены; старый WorkspaceEpisodicRecallService с shortlist не подключён к production memory hooks.

## Точный контракт инструмента

Начальный запрос:

```json
{"query":"где обсуждали настройку провайдеров","limit":20}
```

Продолжение:

```json
{"query":"где обсуждали настройку провайдеров","limit":20,"cursor":"opaque-nextCursor"}
```

`query` — непустой после trim текст до 4096 UTF-8 bytes. `limit` — 1..20 фрагментов, а не число тредов. `cursor` optional/null. Неизвестные поля отклоняются; scope/principal/backend/vector/mode агент не задаёт.

Форма ответа (IDs в примере иллюстративные; production возвращает только подтверждённые canonical IDs):

```json
{
  "threads":[{
    "threadId":"T00000000000000000001",
    "title":"Настройка провайдеров",
    "fragments":[{
      "snippet":"Обсуждали настройку провайдера…",
      "source":{"threadId":"T00000000000000000001","anchor":{"turnId":"V00000000000000000001","itemId":"I00000000000000000001"}},
      "provenance":{"actorRole":"user","sourceContext":"user_visible_thread_item"}
    }]
  }],
  "nextCursor":"opaque-nextCursor",
  "eof":false,
  "limitations":{"selectionLimited":false,"coverageLimited":true,"semanticApproximate":true,"semanticDegraded":false}
}
```

`title` может быть null; anchor без itemId использует подтверждённый turnId. `source` передаётся в threads_history_read вместе с limit. Reader не переделан. Фрагмент до 360 chars, title до 256 chars. Страница собирается до 24 KiB / 3000 tokens для массива cards; общий handler сохраняет предел 32 KiB / 4096 tokens, output policy — structured 32 KiB.

Флаги независимы:

- `nextCursor` / `eof`: остаток сформированной snapshot-выборки.
- `selectionLimited`: обрезание итоговой выборки / фрагментов на тред / слишком большого результата.
- `coverageLimited`: бюджет кандидатов/сегментов/валидации либо недоступность источника/сегмента.
- `semanticApproximate`: semantic retrieval имеет приближённый характер.
- `semanticDegraded`: настроенный semantic недоступен/несовместим; используется доступный lexical.

Пустая страница и eof не доказывают отсутствие совпадений в корпусе.

## Сессии, лимиты и ресурсы

- До 16 сегментов, по 128 кандидатов на канал/сегмент.
- До 1024 посещений кандидатов при канонической валидации, распределённых между каналами/partitions; неиспользованный semantic budget не превращается в неограниченный lexical scan.
- До 128 итоговых источников; до трёх фрагментов одного треда в сохранённой выдаче, порядок тредов — первое появление в ранжировании.
- До четырёх общих поисков одновременно; admission через try-acquire без собственной очереди. Один Arc-owned permit остаётся и у синхронного embedding worker, и внутри каждого уже запущенного lexical/vector blocking retrieval до фактического завершения, даже после отмены async waiter. Ожидание file lock отменяется без запуска новой работы. Очередь поиска не добавлена.
- До 64 RAM-сессий, до 4 MiB учитываемых ссылок/служебных данных суммарно; вытеснение старейших.
- TTL 300 секунд от создания, без продления. Cleanup ленивый на insert/get; resident память остаётся ограниченной и при простое.
- Сессии разделяются клонами MessageProcessor через Arc. После рестарта Gateway состояние отсутствует.
- Cursor защищён существующим ThreadReadCursorCodec; содержит только случайный session ID и позицию. Scope включает principal/workspace/query/limit, execution thread/turn, root и observation flags. Текст/списки результатов/открытые внутренние IDs в cursor не помещаются.
- Новые сообщения не добавляются; продолжение не вызывает retrieval/embedding. Исчезнувшие, изменившиеся и недоступные источники пропускаются с продвижением позиции.
- Expired/evicted/missing session: `search session expired or evicted; restart search without cursor`.

Никаких БД-сессий, Redis, tasks или фонового достраивания выдачи. Чтения идут через существующие CrudStore/repositories и inherited Interactive scope; metadata capsules запрашивается ограниченной порцией 17. DB capacity не удерживается на embedding, backend I/O, fusion или сериализации. Отмена распространяется через awaited futures и существующие caller deadlines.

## Авторизация и актуальность

ThreadRead revalidation определяет инициирующего principal из серверного turn/execution envelope. Hook actor/principal hint не расширяет доступ. Отдельно проверяются ThreadObserveWorkspace, immutable grants и текущая политика/ACL. Agent ceiling читается из runtime binding либо того же persisted Agent grant, которым пользуется materialization; новый grant не создаётся. Child/internal правила остаются в общем thread reader helper.

Каждый кандидат проверяется по фактическому workspace/thread, episodic row/status/deleted_at, source hash/version, visibility/context, occurrence exclusions, каноническому committed item, роли/виду источника и logical deletion turn. Права повторно проверяются после retrieval и на каждой странице. Текст идёт только из канонического источника; ошибка чтения не разрешает вернуть старый индексный текст. Неавторизованные IDs/names/snippets/counts не выводятся.

## Recall и бюджеты

Production provider обслуживает текущий hook и existing current/related/workspace provider requests. Cross-thread requests получили настоящий turnId, чтобы восстановить доверенный execution context. Workspace область включает текущий и остальные разрешённые треды; private не отбрасывается по visibility. CurrentThread/WorkspaceThread provenance определяется по реальному источнику. Related-запрос сохраняет RelatedThread provenance и исключает текущий тред до применения consumer budget.

Gateway устанавливает один episodic provider и соответствующий HookRuntime в AgentManager атомарно через `set_memory_hook_runtime`. Provider входит в `NativeTurnRuntimeSnapshot` и передаётся через существующий turn flow в оба вызова `run_agent_turn_preflight_stage`. Тот вызывает `prepare_active_recall_preflight`: capabilities берутся у provider до построения available episodic modes и запроса существующего preflight orchestrator. Current доступен только при действительной ThreadRead authority и включённом recall; related/workspace требуют также текущий ThreadObserveWorkspace с immutable ceiling. Full-input берётся из прежней конфигурации проекции embeddings, без безусловных true. После preflight active hook заново разрешает capabilities, нормализует план и вызывает тот же общий retrieval с повторной ACL-проверкой.

Recall применяет существующие policy/capabilities/planning, собственные top-k / hit / prompt caps и artifact enrichment после общего retrieval. Эти caps не обрезают количество результатов tools. Recall events сохраняются потребителем fail-open: query hash, проверенные кандидаты до prompt cap, возвращённые hits и latency; legacy Memvid score/adaptive поля не заполняются для нейтрального поиска.

В одном active plan одинаковые requests переиспользуются локально. Между current hook и active recall используется уже внесённый неусечённый prompt context с query fingerprint; для завершённого пустого поиска — ограниченный 64-символьный opaque receipt в существующих phase diagnostics. Только эта отметка имеет safe_for_user=true; редактирование остальных сообщений и metadata сохраняется. При coverage/selection limits или semantic degradation provider возвращает типизированный SearchIncomplete, и пустой поиск не кешируется. Approximate semantic сам по себе остаётся успешным выполнением. Ошибки ACL/чтения и изменения при повторной проверке сохраняют coverageLimited. Failed/policy-skipped/budget-truncated пустые вызовы также не становятся negative cache. Глобальный кеш пользовательских запросов не добавлен. Ссылки из усечённой агрегатором current-записи не исключают новые результаты: полный source_refs такой записи не доказывает показ источников. Пересекающиеся source projections из неусечённого контекста удаляются; существующий current контекст вычитается из episodic prompt budget, а четыре provenance-группы не получают по независимому полному budget.

## Написанные регрессии — не запускались

- `memory/episodic_search.rs`: RRF, детерминированный tie, отсутствие вклада реплик.
- `memory/thread_episodic.rs`: реальные существующие Memvid lexical/vector API, metadata hydration, несовместимая identity и размерность, без ask.
- `gateway/message/thread_read_tools/search_tests.rs`: lexical; hybrid с одним query embedding; lexical fallback при сбое vector/provider; tool и recall через общий retrieval; workspace после дополнительных каталоговых тредов; private/Member/Admin; ограниченные grants; отсутствие достоверного execution; запрет подмены hook principal; страницы без retrieval/embedding; query/principal/workspace/tampering; ACL revocation; удалённые/excluded/hidden/stale/missing источники; отсутствие раскрытия в ответе; causal projections и независимые равные тексты; реальные history anchors; eof отдельно от coverage.
- `gateway/message/thread_search.rs`: expiry/eviction, scope binding, session bounds.
- Existing CLI provider regression дополнена threads_search invocation через фиксированный каталог и общий dispatch, с Agent execution fixtures; runtime не запускался.
- Existing tools visibility/domain/permission regressions обновлены для hidden threads-only регистрации.
- `memory/hooks/tests/episodic_recall.rs`: одинаковые requests, reuse уже внесённого current поиска, единый context budget, реальное усечение агрегатором без исключения непоказанного источника. Проверка пустого reuse перенесена в gateway-интеграцию с настоящим HookRuntime; искусственная отметка в тесте удалена.
- Новые исправляющие регрессии перечислены по замечаниям ниже; ни одна не запускалась.
- Legacy episodic fixtures теперь материализуют canonical items; synthetic compaction без canonical source проверяет подавление, а не fallback.

## Самоаудит и оставшиеся ограничения

Исправлены при чтении: неправильное место создания provider; shared ownership session store при Clone процессора; iterator API hook context; зависимость early hooks от отсутствующего runtime binding; ошибочный Maintenance scope canonical reads; fallback на индекс; канальная starvation в бюджете; вклад повторных causal projections; marker пустого recall после budget failure; независимые полные budgets provenance-групп. Нейтральный результат отделён от legacy score breakdown, а budgets/events вынесены к recall consumer.

Сохранены ограниченный coverage и approximate semantic: direct vector API не фильтрует произвольный ACL до top-k. Post-filter не гарантирует полноту; новые глобальные проходы не добавлены. Источники без подтверждаемого canonical representation пропускаются. Внешний threads_search отклоняет query сверх 4096 UTF-8 байт; JSON Schema maxLength отдельно ограничивает символы и не проверяет байты. Внутренний current hook передаёт полный input_text в тот же retrieval, без нового численного потолка или повторного прохода; query-specific embedding и fingerprint относятся к этому же полному запросу. Hook failure остаётся fail-open. Cleanup сессий не использует таймер. Схемы обновлены вручную: совпадение с генератором не подтверждено.

Cargo manifests/lockfiles и исходники внешних зависимостей не редактировались; vendor/patch/fork/source replacement не добавлены. Common ranking не зависит от Memvid internals; pagination не требует backend generation/cursor. Безопасность основывается на Pioneer, а не на индексе. Ограничения покрытия не скрываются за eof.

Все записи выполнялись только в новом worktree (кроме обычной регистрации новой ветки/worktree в общей Git metadata). Основной checkout и соседние worktree этим исполнением не изменялись. При старте main имел четыре чужих dirty paths; за время работы он независимо продвинулся до `afe4e51b`, dirty state исчез, появились дополнительные worktree. Эти внешние изменения не присваиваются данной реализации и не переносились в неё.

## Исправления внешнего аудита

| Замечание | Исправление и регрессия (не запускалась) |
|---|---|
| 1 — P1, fixture/signature | В `canonical_source_for` удалено отсутствующее поле notification.timestamp; время передано аргументом i64 в materialize_item_completed. Сверены объявления Turn, ItemCompletedNotification, materialize_turn_start/completed, TaskTurnItem, embedding identity/provider, search request и все source_candidates callers. Исправлен также ошибочный DB literal visibility='hidden' на существующее internal_hidden: сценарий должен проверять подавление скрытого источника, а не ошибку декодирования строки. Используются существующие canonical-source и history-anchor регрессии. |
| 2 — P2, production planning | Provider и runtime устанавливаются одной generation; snapshot доставляет provider в production preflight. Никаких hardcoded workspace/related/full-input true. `agent/manager_tests::production_preflight_uses_installed_episodic_capabilities_before_executing_provider_plan` проходит настоящий native turn flow, записывает структурированный запрос preflight-provider и проверяет allowed/denied выполнение его workspace-плана. Gateway `production_preflight_capabilities_reach_planning_and_runtime_recall_without_expanding_grants` использует тот же prepare helper с реальным серверным provider, передаёт ответ recording planner в установленный runtime и проверяет вызов общего candidate backend, отсутствие расширения grants, отзыв права между planning/execution и missing execution. Отдельные проверки покрывают границы snapshot/orchestrator и gateway ACL/retrieval; прямой вызов provider не выдаётся за проверку всего production-планирования. |
| 3 — P2, empty receipt | Безопасен только bounded fingerprint receipt; настоящий HookRuntime сохраняет его, остальные diagnostics редактируются. SearchIncomplete — типизированное состояние неполного выполнения, оно не распознаётся по message. Изменены protocol enum и три опубликованных schema вручную. Gateway `runtime_reuses_only_successfully_completed_empty_search_across_phases` проходит current hook → runtime redaction → aggregation с phase diagnostics → post-preflight active hook; проверяет completed empty, budget-limited coverage и unavailable channel. Существующая hybrid-регрессия дополнена approximate-success и semantic failure через тот же runtime. Ошибки read authority/canonical revalidation больше не теряют признак неполноты. |
| 4 — P2, dedup after truncation | Active recall исключает прежние source_refs только у неусечённых current entries. Общий агрегатор не менялся; реально внесённые символы по-прежнему вычитаются из episodic budget. `aggregator_truncation_does_not_exclude_sources_that_were_not_shown` сокращает запись настоящим aggregate_contributions, сохраняет refs и проверяет доступность непоказанного источника последующему recall при общем бюджете 600 chars. |
| 5 — P2, long input | Внутренняя search_sources проверяет непустоту; byte limit остаётся у внешнего tool handler. Полный query, fingerprint и query-specific embedding сохранены; нового потолка или второго прохода нет. `current_hook_searches_long_input_while_tool_enforces_documented_utf8_limit` вызывает установленный current hook через runtime и проверяет фактический backend call с длинным исходным текстом. |
| 6 — P2, cancellation | Gateway adapter передаёт Arc-owned admission permit в source_candidates; run_search_blocking переносит clone внутрь той closure, которую запускает существующий capsule blocking helper. `cancelled_search_retains_admission_until_blocking_retrieval_finishes` управляет start/release worker, отменяет async waiter, проверяет занятый слот до release и возможность следующего поиска в другом capsule после окончания worker. Existing real-Memvid direct-channel regression обновлена для явного lease. Не добавлены фоновая достройка и неограниченные очереди. |
| 7 — P3, Unicode contract | Одинаковое пояснение 4096 UTF-8 bytes в tool query schema, protocol doc и вручную обновлённой опубликованной schema; maxLength не объявляется байтовой проверкой. В существующей регрессии длины приняты 2048 кириллических букв (4096 bytes), отклонены 2049 и 3000 при числе символов менее 4096. Численные лимиты не менялись. |

При статическом самоаудите исправлений дополнительно обнаружены и исправлены: первоначальная вставка установки provider вне memory-runtime branch; потеря локального deterministic summary, необходимого orchestrator; проверка capabilities тестом по всему prompt вместо структурированного planner input; скрытый enum literal в fixture; потеря ошибок ACL и повторной валидации. После исправления повторно прочитаны соответствующие callers и тестовые цепочки. Форматирование не является проверкой типов.

Совокупный diff от `a61a51409603de887d92630e44c6c4f54932109b` перечитан по production-коду, callers, тестам, схемам, каталогам visibility/permissions/native/CLI и отчёту. Граница адаптера, session TTL/eviction, real anchors, отсутствие индексного fallback и отдельный recall budget сохранены. Cargo manifests/lockfiles, зависимости, cache/vendor, Tasks/Subagents и ограничения остальных читателей не изменены. В этом исправляющем проходе основной checkout начался на `afe4e51b` с чистым status; все записи выполнены только в существующем isolated worktree. Соседние worktree не редактировались.

Оставшиеся ограничения: bounded/approximate coverage, ленивый cleanup сессий и необходимость нового поиска после expiry/eviction/restart остаются частью контракта. Написанные тесты, типы и runtime-поведение требуют внешней проверки с отдельно разрешённым запуском. Схемы редактировались вручную; совпадение с генератором не подтверждено.

## Команды проверки

Ниже перечислены точные уникальные команды проверки; повторные идентичные вызовы объединены. Чтение/поиск исходников выполнялись через cat/sed/rg/wc, редактирование — обычными файловыми операциями; проект ими не исполнялся.

```sh
git rev-parse HEAD
git branch --show-current
git status --short
git status --porcelain
git log -2 --oneline
git worktree list
git diff --stat
git diff --numstat
git diff --summary
git diff --check
git diff --cached --check
git diff --check a61a51409603de887d92630e44c6c4f54932109b HEAD
git ls-files --others --exclude-standard
cargo fmt --package pioneer-memory --package pioneer-gateway --package pioneer-protocol --package pioneer-tools
cargo fmt --package pioneer-memory --package pioneer-gateway --package pioneer-protocol --package pioneer-tools -- --check
python3 -m json.tool schemas/threads_search_params.json > /dev/null
python3 -m json.tool schemas/threads_search_response.json > /dev/null
git diff --name-only a61a51409603de887d92630e44c6c4f54932109b -- Cargo.toml Cargo.lock ':(glob)**/Cargo.toml' ':(glob)**/Cargo.lock' ':(glob)**/vendor/**' resources/skills/pioneer
git -C /Users/alexander/Code/pioneer/pioneer status --short
```

В исправляющем проходе дополнительно выполнены следующие команды проверки (повторные идентичные вызовы объединены):

```sh
cargo fmt --package pioneer-agent --package pioneer-memory --package pioneer-gateway --package pioneer-protocol --package pioneer-tools
cargo fmt --package pioneer-agent --package pioneer-memory --package pioneer-gateway --package pioneer-protocol --package pioneer-tools -- --check
git diff --check
git diff --cached --check
git diff --check a61a51409603de887d92630e44c6c4f54932109b
git diff --check a61a51409603de887d92630e44c6c4f54932109b HEAD
git diff --stat
git diff --numstat
git diff --name-only a61a51409603de887d92630e44c6c4f54932109b
python3 -m json.tool schemas/threads_search_params.json > /dev/null
python3 -m json.tool schemas/threads_search_response.json > /dev/null
python3 -m json.tool schemas/thread_episodic_recall_diagnostic.json > /dev/null
python3 -m json.tool schemas/thread_episodic_recall_diagnostic_code.json > /dev/null
python3 -m json.tool schemas/thread_episodic_recall_output.json > /dev/null
git diff --name-only a61a51409603de887d92630e44c6c4f54932109b -- Cargo.toml Cargo.lock ':(glob)**/Cargo.toml' ':(glob)**/Cargo.lock' ':(glob)**/vendor/**' resources/skills/pioneer
git rev-parse HEAD
git branch --show-current
git status --short
git worktree list
git -C /Users/alexander/Code/pioneer/pioneer status --short
```

`cargo fmt --check`, `git diff --check` и JSON syntax checks завершились с exit 0. Это только форматирование и статические проверки текста/JSON; тестирование, компиляция и эквивалентность схем генератору ими не подтверждаются.

Также перечитан полный diff от базы, production callers, новые файлы, тесты, схемы, membership/visibility/permissions/native/CLI и этот отчёт. Local commits созданы с `-c core.hooksPath=/dev/null -c commit.gpgsign=false`.

Тесты, cargo check, компиляция, сборки, приложение, CLI-агенты и генераторы не запускались. Компилируемость и прохождение тестов не подтверждены. Реализация передаётся на внешнее ревью; окончательная приёмка не получена.
