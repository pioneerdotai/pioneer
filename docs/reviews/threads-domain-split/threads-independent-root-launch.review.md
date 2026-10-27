# Threads independent root launch — самоаудит и исправления ревью части 2/4

## Ревизии и рабочая область

- Ветка: `fix/threads-independent-root-launch`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-independent-root-launch`.
- Начальный HEAD первого самоаудита: `13b22123b19026b1ab4701e72c884f3efe4a15b3`.
- Фактический начальный HEAD исправлений внешнего ревью: `e5dbb24661f1dc19ef906e978431cd8913ef2783`, совпал с указанным проверенным HEAD. Ветка верная, дерево чистое, чужих изменений не было.
- База собственного интервала: `0361ef587bff0b33f2c38edf87d254e3c1769547`.
- Родительская ветка будущего PR: `fix/threads-tool-access`.
- Начальное дерево чистое, staged/unstaged/untracked изменений не было. Обновлённая база от ревьюера не поступала.
- Итоговый commit — коммит с исправлениями трёх подтверждённых дефектов и этим отчётом; его точный SHA передаётся отдельно при сдаче, чтобы отчёт не ссылался сам на себя. Предыдущее исправление pinned launch сохранено.
- Кандидат `9d86bc3eb65cc58f0e67bf3022a545dfec0b14b6` и его самоотчёт не использовались как эталон. Интеграция с ним, main и другими ветками не выполнялась.

Прочитан применимый корневой `AGENTS.md` именно этого worktree. Вложенных `AGENTS.md` для затронутых Gateway/CRUD файлов здесь нет. Указания о запуске проверок в AGENTS и test-audit подчинены прямому запрету пользователя. Подключённые Pioneer skills прочитаны; задачи, сообщения, thread turns и subagents через инструменты не создавались.

## Scope и файлы

Прочитан весь собственный diff относительно базы, включая подготовленный prompt, и непосредственно связанные callers/callees:

- `crates/gateway/src/authorization/execution.rs` — independent provenance и текущая admission initiating principal.
- `crates/gateway/src/authorization/agent_{action_service,runtime_adapter,execution}.rs` — серверное размещение StartAgent до depth gates, подготовка и материализация.
- `crates/gateway/src/message/agent_action_tools.rs` — canonical StartAgent, граф, persisted dispatch, восстановление binding, replay.
- `crates/gateway/src/message/turn_handlers.rs` — API preparation, общий CLI dispatcher, typed failure completion.
- `crates/gateway/src/thread/mod.rs` — локальная rehydration точных model/provider/reasoning из durable launch для API preparation.
- `crates/gateway/src/task_tools/mod.rs`, `crates/gateway/src/turn_mcp/native_tools.rs` — typed CLI catalog preparation и публичный строковый интерфейс.
- `crates/gateway/src/message/agent_runtime.rs`, `crates/gateway/src/message/compaction_background.rs` — общая классификация и test barriers.
- `crates/crud/src/lib.rs`, `crates/crud/src/repositories/agent_domain.rs`, `crates/crud/src/repositories/turn_execution.rs` — атомарная запись и activation CAS.
- `crates/gateway/src/message/tests/agent_action_outbox.rs`, `crates/gateway/src/message/tests/thread_launch_choices.rs` — typed-proof регрессия и общий fixture запуска.
- `crates/gateway/src/message/tests/{cli_task_tools,task_start_failure}.rs` — реальный CLI first dispatch и обновление constructors/fixtures.

Дополнительно прослежены существующие `message/mod.rs`, `authorization/lease.rs`, `authorization/agent_{execution,runtime_adapter}.rs`, `thread` rehydration, `tasks/start_failure.rs`, CRUD terminal acceptance и `repositories/native_cancellation_context.rs`. Проверены реальные определения shared signatures, а не предполагаемые определения следующих частей. Manifest Gateway/CRUD и CI прочитаны для определения границ проверок; они не изменены.

Первый самоаудит изменил rehydration и две существующие регрессии. Исправления после `e5dbb246…` ограничены тремя указанными ниже цепочками и необходимыми callers/tests. Stop, foreground conflicts, final dispatch confirmation, exhausted reconciliation, фоновые политики, зависимости, лимиты и конфиги не менялись.

## Найденное и выполненные исправления

Найден и исправлен дефект общей зависимости своей цепочки: `rehydrate_committed_agent_turn` клонировал текущую модель/провайдера thread, а `prepare_committed_agent_api_turn` использовал эти поля для reasoning, snapshot и API runtime request. Если другой StartAgent изменил настройки того же thread между canonical admission первого Turn и dispatch, первый Turn получал чужой выбор модели при сохранении своего execution/profile/authority. Это воспроизводимый по коду сценарий уже существующей регрессии: `start-visible` фиксирует `gpt-5.4`, затем `explicit-api` меняет тот же thread на `gpt-5.4-catalog-other` до обработки outbox.

Правка в `e5dbb246…` берёт model/provider/reasoning для локального `TurnStartMaterialization.thread` из точных params durable dispatch. Отсутствующий model/provider отвергается вместо fallback к mutable composer. Текущее состояние thread и его rollback snapshot не меняются. Исправлено ошибочное утверждение первого отчёта: production callers два — outbox dispatcher и API Task recovery в `task_agent_executor.rs`. Второй caller проверен: он строит `TurnStartParams` с `Some(effective_model.model)`, `Some(effective_model.model_provider)` и `turn_settings.reasoning` из pinned Task launch, затем передаёт сохранённый input в rehydration. Требуемые поля присутствуют, recovery использует тот же exact model; этот путь не изменён. Правка общей зависимости должна переноситься в следующие части вместе с принятыми исправлениями.

В первом самоаудите расширены две существующие регрессии, обе сохранены:

1. `inherited_configuration_survives_visible_thread_creation_message_and_start_replay` теперь проверяет rehydration первого committed Turn после явной смены модели в том же thread: в runtime payload остаются `gpt-5.4`/`openai`/`high`, mutable composer остаётся на второй модели. Используется настоящий результат tool admission и сохранённый input, без готового helper-result. Дополнительно закрыт пробел структуры: проверяются реальные persisted response/execution/grant/receipt/Turn после production tool handler, новый root с generation 1, отсутствие execution/task parent, исходный AgentExecution как автор и owner действия, точные source action/execution/turn bindings. Authority загружается штатным проверяющим путём до появления runtime snapshot; проверяется сохранение initiating principal и отсутствие расширения action ceiling. Replay сохраняет grant. В этом же сценарии home-capsule thread запускается через выданный opaque target: сохраняются parent execution, исходный work graph и generation родителя + 1.
2. `agent_start_proof_preserves_typed_storage_faults_without_retrying_semantic_denials` покрывал прямой anyhow context, но не вложенный `TaskStartFailure`, который используется CLI preparation. Существующая матрица расширена этим transport/error boundary и внешним `PreparationUnavailable` context. Проверяется `StorageUnavailable` и сохранение `DbErr` в source chain. Отрицательные случаи malformed JSON, семантического отказа, RecordNotFound/Type и недоверенного текста сохранены.

Первый самоаудит не добавлял сквозных тестов и пропустил три дефекта, подтверждённых внешним ревью. В текущем раунде добавлены именно запрошенные регрессии ниже. Готовые результаты helper вместо production dispatch и новые production test seams не добавлялись. Test-audit authoring gate применён к покрытию; явное поручение пользователя исправить production-код имеет приоритет над test-only ограничением skill.

Регрессии не запускались. Статически ожидаемый detectable regression: до исправления rehydration возвращает вторую модель вместо первой; возврат routed root в родительский граф ломает новые lineage assertions; удаление обхода вложенного error source ломает wrapped-storage случай. Baseline fail/pass экспериментально не подтверждён.

## Три подтверждённых дефекта внешнего ревью

1. **P1: повторная admission committed CLI Turn.** Outbox больше не вызывает `rollback_turn_start`. `prepare_committed_agent_cli_runtime_turn` передаёт rehydrated outcome через `DurableAgent.admitted_outcome`. Общий `turn_start_cli_runtime` считает его already materialized, исключает текущий Turn из previous-delivered history существующим creation-order lookup, забирает готовый outcome и пропускает создание Turn и `persist_admitted_turn_start`. Отсутствующий outcome у DurableAgent отклоняется до подготовки, без fallback к новому Turn. Все constructors и patterns проверены. Task с outcome и Task без outcome сохраняют прежнее поведение; обычный fresh CLI Turn по-прежнему создаётся и сохраняется общим dispatcher.
2. **P2: устаревшее время activation claim.** У `CrudStore::claim_unactivated_agent_turn` удалён внешний аргумент времени. Каждая попытка получает writer через `begin`, проверяет canonical terminal acceptance и читает `utc_now()` непосредственно перед UPDATE fence. Storage retry заново проходит эту последовательность. Сама транзакционная граница и predicates workspace/thread/owner/generation/lease/status/started_at сохранены; продления production lease и повышения priority нет.
3. **P2: depth source-графа применялся к самостоятельному root.** До `adapter.prepare_start_agent` handler получает серверный факт visible destination без child lineage в нужном workspace. Adapter пересекает его с точным routed target в другую capsule (`destination_thread_id == destination_capsule_id`, не source capsule). Этот внутренний признак не является полем tool input. Обычные source action, route/disclosure и generation проверки выполняются по-прежнему. Service подготавливает independent root с depth 0, собственными graph/branch; materialization получает тот же признак и глубину. Canonical commit повторно сверяет destination shape, отвергает изменение размещения и сохраняет graph, а не сбрасывает depth после ранних gates. Настоящие children, включая routed internal targets, сохраняют source.depth + 1 и max_depth. Resource root теперь проверяется на точное совпадение и для independent root.

Написано покрытие (не запускалось):

- `threads_first_cli_outbox_dispatch_uses_committed_turn_once`: обычный CLI parent запускается существующим fixture; настоящий `threads_turn_start` через native invoker создаёт новый delegated target и сохранённый outbox. До dispatch у target отсутствуют snapshot, binding и attempt. Outbox проходит общий dispatcher, recording runtime должен получить ровно один дополнительный native turn/start с исходной командой/model/reasoning. Проверяются неизменность admission, input author, grant, security/context, отсутствие второй in-memory identity, наличие настоящего attempt и отсутствие второго native start при replay. Fixture trigger запрещает повторный INSERT admission. Готовый prepared helper-result не подставляется.
- `agent_activation_claim_rechecks_lease_after_waiting_for_writer`: существующий queued-future polling синхронизирует ожидание занятого writer при ещё живом lease. Фикстура намеренно держит writer до границы истечения; claim после освобождения должен вернуть false, сохранив starting/generation/started_at. Затем реально renewed lease принимается один раз, повторный claim отклоняется. Задержка существует только в тексте этой adversarial test fixture; тест не выполнялся.
- `source_at_max_depth_can_start_only_an_independent_root`: полная согласованная ancestry строится реальными admissions до текущего default max_depth. Предыдущие owners завершаются через canonical Turn completion и существующую финализацию ресурсов, чтобы отказ не объяснялся concurrency capacity. Source на max_depth запускает independent root с собственным scope, generation 1, без parent execution и с исходным автором/provenance. Новый home-capsule child того же source получает точный public depth-limit outcome; action/receipt/outbox отсутствуют, число canonical turns/executions не меняется.

Также исправлен permission-matrix fixture `start_threads_cli_fixture_with_policy`: он теперь сначала сохраняет Turn/admission/security/context и передаёт реальный admitted outcome; точные model/provider задаются до этой admission. Failure-only constructor DurableAgent в `task_start_failure.rs` получил `admitted_outcome: None` и по-прежнему проверяет только completion reporter. Отдельная CLI storage/replay matrix части 4 сюда не переносилась.

## Самоаудит цепочки

### Контракт и авторизация

- Создание user-visible routed root отделяет lifecycle: root execution ID равен его work-graph root, depth определяется как 0 до обеих предварительных проверок, branch относится к новому root. Home-capsule child сохраняет execution parent и прежний resource scope.
- Input author остаётся исходным execution; responding execution получает собственные pinned identity/profile/presentation и generation. Наследование выбранной identity не подменяет execution ID.
- `revalidate_new_root_launch` разрешает новую работу по текущему initiating principal и его permission cap; полученный proof не записывается поверх inherited immutable context. Выводимый child context сохраняет исходную provenance и action ceiling. В commit остаются source authority, explicit route и generation fences.

### Canonical acceptance и сохранение

- `materialize_agent_turn_start_with_graph_and_action` связывает исходного actor с grant source/action, response с точным execution и Turn. Общий writer атомарно записывает Turn/admission/security/context/ownership, graph/grant/response и canonical action/receipt/outbox. Ошибка в любом из этих шагов откатывает write set.
- Для независимого root проверка ceiling на CRUD стороне получает исходный execution из immutable grant, не создавая parent lifecycle edge. Для настоящего child сохраняется проверка общего work graph.
- Runtime authority validation проверяет canonical source action/allowed receipt/outbox и grant, но не требует текущей liveness автора. Существующая ledger compaction сохраняет `spawned_execution_id`, необходимые ownership facts и launch fingerprint, поэтому provenance не зависит от сохранения полного dispatch blob.

### Preparation, runtime/control, результат и replay

- Первый API dispatch допускает `observe_turn == None`/`ThreadNotFound` и отсутствие snapshot. Исправленная rehydration формирует payload из pinned launch независимо от mutable composer. Preparation регистрирует lease, runtime thread/listener, принимает историю и сохраняет snapshot до activation CAS.
- CAS сериализован с canonical terminal acceptance; проверяет точные workspace/thread/owner/generation, актуальный после writer admission lease, queued/starting и отсутствие started_at. Перевод в running исключает второй успешный claim даже в окне до runtime ACK.
- Если snapshot уже есть после неоднозначной активации, native dispatch возвращает `OutcomeUnknown`; отсутствие snapshot само по себе также не обходит CAS. Поздняя owner generation передаёт активацию протоколу recovery.
- CLI ветка передаёт committed outcome в `prepare_committed_agent_cli_runtime_turn` → общий `turn_start_cli_runtime` → activation, без rollback и повторной canonical admission. Durable CLI attempt остаётся runtime receipt; native snapshot не служит CLI receipt.
- Typed CLI materialization сохраняет исходную инфраструктурную ошибку; доказанный deny остаётся отдельным результатом. `DurableAgent` completion переносит `TaskStartFailure` со source chain. Классификация storage не использует текст сообщения как доказательство отказа доступа.
- DB scheduling classes и transaction boundaries не изменены. Подготовка истории, provider/CLI работа и test barriers находятся вне activation writer transaction.

## Зависимости и пределы передачи

- Эта ветка по-прежнему содержит только часть 2 на указанной базе части 1; исправления принятой части 1 самостоятельно не переносились.
- Здесь намеренно сохранены имя `dispatch_agent_action_outbox_row`, прежние terminal shortcut и подтверждение dispatch. Новые shared failure types/CAS/barriers — определения для последующих слоёв, а не реализация их бизнес-логики.
- Часть 3 должна принести окончательную foreground admission, её конфликты, ранний Stop и native first-dispatch+Stop/foreground регрессии.
- Часть 4 должна принести wrapper окончательных отказов, exhausted reconciliation, terminal cleanup/replay, outcome confirmation и CLI storage/replay matrix. Текущие early returns по snapshot/attempt/terminal не выдаются этим отчётом за окончательное доказательство runtime outcome.
- Продолжение Stop/reconciliation и изменения `native_cancellation_context` сюда не переносились. Ревьюеру необходимо переносить вместе pinned launch, новый `DurableAgent.admitted_outcome` и caller/fixture updates, сигнатуру activation claim без времени, а также серверное размещение во всех трёх слоях authorization. Перенос выполняет родительский ревьюер.

Это передача своей части на внешнее ревью. Самоаудит не означает внешнюю приёмку или общую приёмку четырёх частей.

## Реально выполненные команды и ограничения

Все Git и formatting операции выполнялись в указанном worktree. Выполнены:

- `pwd`, `git status --short --branch`, `git status --porcelain=v1`, `git rev-parse HEAD`, `git rev-parse --show-toplevel`, `git log --oneline -5` — начальная ревизия и рабочая область.
- `rg --files` для поиска AGENTS и затронутых тестов; `cat`, `sed -n`, `rg -n`, `wc -l` для чтения инструкций, skills, diff owners/callers, manifest и CI. Поиск AGENTS в родительских каталогах был только чтением.
- `git diff --stat 0361ef587bff0b33f2c38edf87d254e3c1769547..HEAD`, `git diff --name-only 0361ef587bff0b33f2c38edf87d254e3c1769547..HEAD`, целевые `git diff 0361ef58..HEAD -- <files>` и `git diff 0361ef58 -- <files>` — собственный интервал.
- `git diff --name-only 13b22123`, `git diff --numstat`, целевые `git diff -- <test files>` и `git diff -- crates/gateway/src/thread/mod.rs` — контроль правок самоаудита.
- В раунде исправлений выполнены `git rev-parse HEAD`, `git branch --show-current`, `git status --short --branch`, `git status --porcelain=v1`; прочитаны `AGENTS.md` и prompt части. Выполнены целевые `git diff -- <files>`, `git diff --stat`, `git diff --numstat`, `rg -n` по всем DurableAgent/shared signatures и обоим callers rehydration, чтение owners/fixtures через `sed -n`. Сравнение с начальным `e5dbb246…` и базой части выполнено обычным Git; кандидат и другие worktree не использовались.
- `cargo fmt --all --check` до правок — успешно; `cargo fmt --all` после правок — успешно. Итоговые `cargo fmt --all --check`, `git diff --check`, `git diff --check 0361ef58` — успешно.
- `git add` только изменённых цепочек/callers/tests и отчёта; `git diff --cached --check`; `git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit` — фиксация с отключёнными hooks. Итоговые HEAD/ветка/чистота проверены обычными Git-командами и переданы отдельно.

Тесты, cargo check, компиляция, сборки, приложение, Gateway, тестовые CLI-агенты и генераторы не запускались. Миграции, EXPLAIN, benchmarks и выполнение кода проекта также не выполнялись. Компилируемость и прохождение тестов не подтверждены. Formatting не заменяет эти проверки. Для запуска после приёмки требуется отдельное разрешение пользователя.

Merge, push, deploy и создание PR не выполнялись. Основной checkout, соседние worktree, службы, базы и установленные skills не изменялись.
