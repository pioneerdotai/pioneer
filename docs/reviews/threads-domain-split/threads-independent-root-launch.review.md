# Threads independent root launch — статический самоаудит части 2/4

## Ревизии и рабочая область

- Ветка: `fix/threads-independent-root-launch`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-independent-root-launch`.
- Фактический начальный HEAD: `13b22123b19026b1ab4701e72c884f3efe4a15b3`.
- База собственного интервала: `0361ef587bff0b33f2c38edf87d254e3c1769547`.
- Родительская ветка будущего PR: `fix/threads-tool-access`.
- Начальное дерево чистое, staged/unstaged/untracked изменений не было. Обновлённая база от ревьюера не поступала.
- Итоговый commit — коммит, содержащий этот отчёт, исправление rehydration и расширения двух регрессий; его точный SHA передаётся отдельно при сдаче, чтобы отчёт не ссылался сам на себя.
- Кандидат `9d86bc3eb65cc58f0e67bf3022a545dfec0b14b6` и его самоотчёт не использовались как эталон. Интеграция с ним, main и другими ветками не выполнялась.

Прочитан применимый корневой `AGENTS.md` именно этого worktree. Вложенных `AGENTS.md` для затронутых Gateway/CRUD файлов здесь нет. Указания о запуске проверок в AGENTS и test-audit подчинены прямому запрету пользователя. Подключённые Pioneer skills прочитаны; задачи, сообщения, thread turns и subagents через инструменты не создавались.

## Scope и файлы

Прочитан весь собственный diff относительно базы, включая подготовленный prompt, и непосредственно связанные callers/callees:

- `crates/gateway/src/authorization/execution.rs` — independent provenance и текущая admission initiating principal.
- `crates/gateway/src/message/agent_action_tools.rs` — canonical StartAgent, граф, persisted dispatch, восстановление binding, replay.
- `crates/gateway/src/message/turn_handlers.rs` — API preparation, общий CLI dispatcher, typed failure completion.
- `crates/gateway/src/thread/mod.rs` — локальная rehydration точных model/provider/reasoning из durable launch для API preparation.
- `crates/gateway/src/task_tools/mod.rs`, `crates/gateway/src/turn_mcp/native_tools.rs` — typed CLI catalog preparation и публичный строковый интерфейс.
- `crates/gateway/src/message/agent_runtime.rs`, `crates/gateway/src/message/compaction_background.rs` — общая классификация и test barriers.
- `crates/crud/src/lib.rs`, `crates/crud/src/repositories/agent_domain.rs`, `crates/crud/src/repositories/turn_execution.rs` — атомарная запись и activation CAS.
- `crates/gateway/src/message/tests/agent_action_outbox.rs`, `crates/gateway/src/message/tests/thread_launch_choices.rs` — typed-proof регрессия и общий fixture запуска.

Дополнительно прослежены существующие `message/mod.rs`, `authorization/lease.rs`, `authorization/agent_{execution,runtime_adapter}.rs`, `thread` rehydration, `tasks/start_failure.rs`, CRUD terminal acceptance и `repositories/native_cancellation_context.rs`. Проверены реальные определения shared signatures, а не предполагаемые определения следующих частей. Manifest Gateway/CRUD и CI прочитаны для определения границ проверок; они не изменены.

Изменения этого самоаудита после начального HEAD ограничены одной необходимой production-правкой в `thread/mod.rs`, двумя test-only файлами и этим отчётом. Stop, фоновые политики, зависимости, лимиты и конфиги не менялись.

## Найденное и выполненные исправления

Найден и исправлен дефект общей зависимости своей цепочки: `rehydrate_committed_agent_turn` клонировал текущую модель/провайдера thread, а `prepare_committed_agent_api_turn` использовал эти поля для reasoning, snapshot и API runtime request. Если другой StartAgent изменил настройки того же thread между canonical admission первого Turn и dispatch, первый Turn получал чужой выбор модели при сохранении своего execution/profile/authority. Это воспроизводимый по коду сценарий уже существующей регрессии: `start-visible` фиксирует `gpt-5.4`, затем `explicit-api` меняет тот же thread на `gpt-5.4-catalog-other` до обработки outbox.

Минимальная правка берёт model/provider/reasoning для локального `TurnStartMaterialization.thread` из точных params durable dispatch. Отсутствующий model/provider отвергается вместо fallback к mutable composer. Текущее состояние thread и его rollback snapshot не меняются. Единственный production caller rehydration — dispatcher этой цепочки; API и CLI далее получают согласованный materialization. Транзакции, политики и generation fences не менялись. Эту правку общей зависимости необходимо перенести в следующие части вместе с принятыми изменениями данной ветки.

Расширены две существующие регрессии:

1. `inherited_configuration_survives_visible_thread_creation_message_and_start_replay` теперь проверяет rehydration первого committed Turn после явной смены модели в том же thread: в runtime payload остаются `gpt-5.4`/`openai`/`high`, mutable composer остаётся на второй модели. Используется настоящий результат tool admission и сохранённый input, без готового helper-result. Дополнительно закрыт пробел структуры: проверяются реальные persisted response/execution/grant/receipt/Turn после production tool handler, новый root с generation 1, отсутствие execution/task parent, исходный AgentExecution как автор и owner действия, точные source action/execution/turn bindings. Authority загружается штатным проверяющим путём до появления runtime snapshot; проверяется сохранение initiating principal и отсутствие расширения action ceiling. Replay сохраняет grant. В этом же сценарии home-capsule thread запускается через выданный opaque target: сохраняются parent execution, исходный work graph и generation родителя + 1.
2. `agent_start_proof_preserves_typed_storage_faults_without_retrying_semantic_denials` покрывал прямой anyhow context, но не вложенный `TaskStartFailure`, который используется CLI preparation. Существующая матрица расширена этим transport/error boundary и внешним `PreparationUnavailable` context. Проверяется `StorageUnavailable` и сохранение `DbErr` в source chain. Отрицательные случаи malformed JSON, семантического отказа, RecordNotFound/Type и недоверенного текста сохранены.

Новые отдельные сквозные тесты, runtime doubles с готовым результатом dispatch и дополнительные production seams не добавлены. Проверки базовой структуры используют уже существующий fixture и настоящие tool handlers/CRUD; они не заявляют подтверждённый запуск модели. Test-audit authoring gate применён к тестовым изменениям. Production-исправление выполнялось в рамках явного поручения пользователя о минимальной правке общей зависимости, имеющего приоритет над test-only ограничением skill. `git diff --name-only 13b22123` и просмотр новых hunks ограничивают production scope методом rehydration.

Регрессии не запускались. Статически ожидаемый detectable regression: до исправления rehydration возвращает вторую модель вместо первой; возврат routed root в родительский граф ломает новые lineage assertions; удаление обхода вложенного error source ломает wrapped-storage случай. Baseline fail/pass экспериментально не подтверждён.

## Самоаудит цепочки

### Контракт и авторизация

- Создание user-visible routed root отделяет lifecycle: root execution ID равен его work-graph root, depth сбрасывается до 0, branch относится к новому root. Home-capsule child сохраняет execution parent и прежний resource scope.
- Input author остаётся исходным execution; responding execution получает собственные pinned identity/profile/presentation и generation. Наследование выбранной identity не подменяет execution ID.
- `revalidate_new_root_launch` разрешает новую работу по текущему initiating principal и его permission cap; полученный proof не записывается поверх inherited immutable context. Выводимый child context сохраняет исходную provenance и action ceiling. В commit остаются source authority, explicit route и generation fences.

### Canonical acceptance и сохранение

- `materialize_agent_turn_start_with_graph_and_action` связывает исходного actor с grant source/action, response с точным execution и Turn. Общий writer атомарно записывает Turn/admission/security/context/ownership, graph/grant/response и canonical action/receipt/outbox. Ошибка в любом из этих шагов откатывает write set.
- Для независимого root проверка ceiling на CRUD стороне получает исходный execution из immutable grant, не создавая parent lifecycle edge. Для настоящего child сохраняется проверка общего work graph.
- Runtime authority validation проверяет canonical source action/allowed receipt/outbox и grant, но не требует текущей liveness автора. Существующая ledger compaction сохраняет `spawned_execution_id`, необходимые ownership facts и launch fingerprint, поэтому provenance не зависит от сохранения полного dispatch blob.

### Preparation, runtime/control, результат и replay

- Первый API dispatch допускает `observe_turn == None`/`ThreadNotFound` и отсутствие snapshot. Исправленная rehydration формирует payload из pinned launch независимо от mutable composer. Preparation регистрирует lease, runtime thread/listener, принимает историю и сохраняет snapshot до activation CAS.
- CAS сериализован с canonical terminal acceptance; проверяет точные workspace/thread/owner/generation, lease, queued/starting и отсутствие started_at. Перевод в running исключает второй успешный claim даже в окне до runtime ACK.
- Если snapshot уже есть после неоднозначной активации, native dispatch возвращает `OutcomeUnknown`; отсутствие snapshot само по себе также не обходит CAS. Поздняя owner generation передаёт активацию протоколу recovery.
- CLI ветка использует durable CLI attempt и `prepare_committed_agent_cli_runtime_turn` → общий `turn_start_cli_runtime` → activation. Native snapshot не служит CLI receipt.
- Typed CLI materialization сохраняет исходную инфраструктурную ошибку; доказанный deny остаётся отдельным результатом. `DurableAgent` completion переносит `TaskStartFailure` со source chain. Классификация storage не использует текст сообщения как доказательство отказа доступа.
- DB scheduling classes и transaction boundaries не изменены. Подготовка истории, provider/CLI работа и test barriers находятся вне activation writer transaction.

## Зависимости и пределы передачи

- Эта ветка по-прежнему содержит только часть 2 на указанной базе части 1; исправления принятой части 1 самостоятельно не переносились.
- Здесь намеренно сохранены имя `dispatch_agent_action_outbox_row`, прежние terminal shortcut и подтверждение dispatch. Новые shared failure types/CAS/barriers — определения для последующих слоёв, а не реализация их бизнес-логики.
- Часть 3 должна принести окончательную foreground admission, её конфликты, ранний Stop и native first-dispatch+Stop/foreground регрессии.
- Часть 4 должна принести wrapper окончательных отказов, exhausted reconciliation, terminal cleanup/replay, outcome confirmation и CLI storage/replay matrix. Текущие early returns по snapshot/attempt/terminal не выдаются этим отчётом за окончательное доказательство runtime outcome.
- Продолжение Stop/reconciliation и изменения `native_cancellation_context` сюда не переносились. Единственное дополнительное исправление shared production-зависимости для переноса — pinned launch в `thread/mod.rs`; перенос выполняет родительский ревьюер.

Это передача своей части на внешнее ревью. Самоаудит не означает внешнюю приёмку или общую приёмку четырёх частей.

## Реально выполненные команды и ограничения

Все Git и formatting операции выполнялись в указанном worktree. Выполнены:

- `pwd`, `git status --short --branch`, `git status --porcelain=v1`, `git rev-parse HEAD`, `git rev-parse --show-toplevel`, `git log --oneline -5` — начальная ревизия и рабочая область.
- `rg --files` для поиска AGENTS и затронутых тестов; `cat`, `sed -n`, `rg -n`, `wc -l` для чтения инструкций, skills, diff owners/callers, manifest и CI. Поиск AGENTS в родительских каталогах был только чтением.
- `git diff --stat 0361ef587bff0b33f2c38edf87d254e3c1769547..HEAD`, `git diff --name-only 0361ef587bff0b33f2c38edf87d254e3c1769547..HEAD`, целевые `git diff 0361ef58..HEAD -- <files>` и `git diff 0361ef58 -- <files>` — собственный интервал.
- `git diff --name-only 13b22123`, `git diff --numstat`, целевые `git diff -- <test files>` и `git diff -- crates/gateway/src/thread/mod.rs` — контроль правок самоаудита.
- `cargo fmt --all --check` до правок — успешно; `cargo fmt --all` после правок — успешно. Итоговые `cargo fmt --all --check`, `git diff --check`, `git diff --check 0361ef58` — успешно.
- `git add` только `thread/mod.rs`, двух тестовых файлов и отчёта; `git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit` — фиксация с отключёнными hooks. Итоговые HEAD/ветка/чистота проверены обычными Git-командами и переданы отдельно.

Тесты, cargo check, компиляция, сборки, приложение, Gateway, тестовые CLI-агенты и генераторы не запускались. Миграции, EXPLAIN, benchmarks и выполнение кода проекта также не выполнялись. Компилируемость и прохождение тестов не подтверждены. Formatting не заменяет эти проверки. Для запуска после приёмки требуется отдельное разрешение пользователя.

Merge, push, deploy и создание PR не выполнялись. Основной checkout, соседние worktree, службы, базы и установленные skills не изменялись.
