# CLI MCP call finalization — передача на внешнее ревью

## База и изоляция

- Ветка: `fix/cli-mcp-call-finalization`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/cli-mcp-call-finalization`.
- Исходная локальная база: `7f226499d3fa8beac5dd15b3d241040e7a99016e`.
- Итоговая база: **`dc90743137af9d220a3594c8fe6190bdbde7d999`** — фактический удалённый `main`, проверенный через `git ls-remote origin refs/heads/main`. Собственная ветка перебазирована на этот commit; повторная проверка удалённого main дала тот же SHA.
- Между базами в main вошла оптимизация task-child discovery (#216). Она является частью объявленной базы, а не заимствованием из соседнего worktree. Собственное исправление её не меняет.
- Основной checkout и соседние worktree не редактировались. Имена ветки/worktree были свободны. Не было merge в main, push, deploy или публикации PR. Все commits и rebase выполнялись с `core.hooksPath=/dev/null`.

### Повторная передача после внешнего ревью, 29 октября 2026

Работа продолжена в той же ветке/worktree с проверенного чистого HEAD **`8ef7f2ef285fc2adf9103c66c28c1fb6bba6fc05`**. База остаётся **`dc90743137af9d220a3594c8fe6190bdbde7d999`**, как указано в повторном задании; новый main не интегрировался. Прочитаны корневой `AGENTS.md` и `crates/crud/AGENTS.md`, проверены применимые scoped instructions и `git worktree list`. Изменены только собственные файлы этой ветки. Ни одна регрессия не запускалась.

Исправлены три замечания:

1. **Canonical acceptance и projection.** Проверка superseded observation теперь читает существующий canonical item index (`compaction_event_revision` → точный `turn_event` для turn/item и типа ItemCompleted), который обновляется в транзакции принятия. Отсутствие terminal read model больше не открывает путь новому start/update. При обработке уже принятого события применяется строгий `completion.sequence < observation.sequence`; исходный start перед completion по-прежнему создаёт свою попытку. Идемпотентная повторная подача уже принятого observation сохраняет прежнюю обработку. Replay пропускает только позднюю managed MCP проекцию, её deadlines и semantic timeline, но сохраняет projection receipt и продвижение watermark. Проверка также присутствует в обычном projector; остальные lifecycle events не подавляются.
2. **Отказ до исполнения.** После проверки frozen binding и managed session validate сохраняет invocation-local контекст exact native claim и execution owner. Ошибка последующей validation проходит через тот же `materialize_cli_mcp_call_completed`, без вызова execution. Metadata `executionAdmission=rejected`, `success=false` и исходный reason code отличают известный отказ от неизвестного результата операции. Для проверенного PermissionDenied это failed/permission_denied с `incomplete=false`; delivery остаётся unconfirmed. Отказ до проверки session/identity не получает права завершать item. Авторизация, schema fingerprints, governor и writer fencing не ослаблены. Порядок validation для native API и legacy callers сохранён.
3. **Ответ отдельно от progress/доставки.** Полный существующий replay-ответ записывается `leader.complete` до дополнительного await завершающего progress. Повтор logical call читает его из прежнего ledger и не исполняет операцию снова. Facade permit удерживается до конца progress с прежним timeout. Если future потерян раньше появления ответа, Drop по-прежнему сохраняет response_unconfirmed; после сохранения известного ответа Drop его не заменяет. Ранняя запись ответа не является delivery ACK.

Промежуточный и итоговый статический аудит охватил acceptance helpers, projector/replay и watermark, invoker validation/execution callers, обе границы Drop, точные native correlation/grants и generation, terminal cleanup, Task delivery consumer, output policies/redaction. При аудите дополнительно исправлены время жизни facade permit и test caller конструктора completion в Task delivery gate. Весь diff перечитан относительно объявленной базы, а не только относительно review HEAD. Авторизация и действия операции не повторяются ради восстановления.

Регрессии используют прежний интеграционный harness с реальным facade/coordinator/correlator/Gateway invoker/CrudStore. Совмещённый сценарий регистрирует unsaved native start, принимает predecessor без проекции, сохраняет deferred MCP completion, подаёт late start/update и восстанавливает существующим replay: один terminal result, без running attempt и повторного execution. Его второй порядок отдельно останавливает replay после ранее принятого start и проверяет исходную running attempt перед завершением. Добавлена проверка Gateway validation refusal с нулём execution, поздним native completion и terminal turn; stale generation остаётся отрицательным контролем. Исправлена проверка Drop во время final progress после canonical commit: replay возвращает исходные content/isError/duration, один execution/event и unconfirmed delivery. Отдельная новая проверка Drop до результата требует response_unconfirmed и Unknown при закрытии turn, без успеха и cancellation ACK. Существующие проверки чужой native identity, generation во время исполнения, настоящей отмены/ошибки, поздней отмены и Task observation сохранены. **Все тесты написаны, но не запускались.**

## Подтверждённая причина

Исходные факты предоставлены в задании: turn `7NnfvFK1hNkopujr9ywc1`, native item `exec-22d09e63-3a0d-429b-8585-f87d9f770dfc`; 28 октября 2026, 18:22:59–18:23:03 UTC. Остановка exec-скрипта произошла раньше успешного аудита Gateway, отдельного native ItemCompleted нет, позднее terminal-Turn cleanup записал `turn_terminal_before_item_completed` как execution_failed.

По исходникам это два независимых пути:

1. Facade выполняет точно связанный MCP-вызов через invoker, сохраняет ответ в ограниченном invocation ledger и возвращает его транспорту. McpService отдельно пишет аудит.
2. Native ItemCompleted обычно проецируется в canonical item event. При его отсутствии `prepare_terminal_running_attempts` закрывает running attempt, не имея подтверждённого результата операции.

Дополнительные найденные причины рассогласования: facade мог отбросить future invoker по cancellation token, заменить уже полученный результат поздней отменой, а Drop лидера записывал cancellation без подтверждения исполнения.

Аудит `allowed/is_error=false` не использован ни как ответ, ни как delivery receipt. Исторические БД, живые Gateway и Codex не читались и не запускались; исторический item не исправлялся задним числом по одному аудиту.

## Точка согласования и источник истины

`GatewayTurnMcpInvoker::invoke`, после существующей validation/execution либо проверенного отказа до execution, перед возвратом результата facade, сохраняет подтверждённый исход через обычный `ItemCompletedNotification` и существующие canonical append/project/delivery механизмы CrudStore.

Участвуют только CLI-вызовы с native identity от адаптера и проверенной привязкой `provider_call_id == native_item_id`. Native API и прежние внутренние callers без native claim сохраняют существующий путь. Новая identity — аргумент проверки canonical commit, не новое хранилище вызовов.

Источник факта — типизированный результат invoker: полноценный `CanonicalMcpToolResult` либо его ошибка. После принятия источником истины служит canonical turn event, а не TTL/эвиктируемый ledger и не аудит. Facade хранит существующий replay-ответ, но не является единственным владельцем факта исполнения.

Нормальный случай принимает event и проецирует terminal item/attempt в одном writer transaction. Если перед ним уже принят, но ещё не спроецирован terminal event, результат вместе с существующей projection claim сохраняется канонически. Его применит действующий replay после предшественника. Новый poller, очередь, Task, таблица, idempotencyKey или повторный запуск операции не добавлены. Остальные atomic callers сохраняют прежнее поведение; возможность отложенной проекции включена только здесь.

Подготовка payload, policy projection, сериализация и копирование prepared event происходят до writer transaction. Подготовка MCP completion не выводит новый payload из старого состояния item. Binding, generation, execution owner и текущая попытка перепроверяются при commit. Порядок causal events проверяется существующим watermark.

## Исполнение, отмена и доставка

| Подтверждённый источник | Сохранённый смысл |
| --- | --- |
| Полный MCP result, `is_error=false` | Успех операции, delivery остаётся unconfirmed |
| MCP error result или типизированная ошибка исполнения | Ошибка с соответствующим существующим ToolOutcome |
| Определённый отказ validation после проверки managed identity | Отказ до исполнения (`executionAdmission=rejected`), исходный code, без запуска операции |
| `Cancelled` от invoker | Отмена с её причиной; это не утверждение об отсутствии частичных эффектов |
| Timeout / неизвестный исход | Неполный исход, без автоматического retry |
| Drop future до появления результата | `response_unconfirmed` в facade replay; это не cancellation ACK |
| Drop во время final progress после сохранения ответа | Сохранённый исходный replay-ответ; первая доставка остаётся unconfirmed |
| Нет подтверждения при закрытии turn | `success=None`, `Unknown`, `incomplete=true`, `cli_mcp_result_unconfirmed`; metadata больше не говорит inProgress |
| Истёк/очищен cache либо нет native completion | Ни успех, ни отмена не выводятся из отсутствия данных |

Существующий контракт имеет общий terminal `ToolCallStatus::Failed`; различие настоящей ошибки, cancellation и неизвестности сохраняется в `ToolOutcome.error_class`, `incomplete`, причине и nullable success. Подтверждённая cancellation использует также существующий CRUD lifecycle/attempt status `cancelled`. Новый статус протокола не вводился.

Поздняя cancellation больше не подменяет полученный результат. Token по-прежнему передаётся существующему invoker/runtime, admission/governor и MCP cancellation сохраняются. Исполняемая операция не отделяется в новую фоновую задачу. Существующие deadlines и bounded shutdown drain не изменены.

Если закрытие turn уже оставило диагностический interrupted attempt, подтверждённый результат может завершить именно эту попытку: по точному active_attempt_id, исходному started_event_sequence и специальной причине cleanup. Переход и canonical projection атомарны. Другие terminal attempts не переоткрываются, восстановление не исполняет операцию повторно.

## Identity, повторы и callers

Перед canonical acceptance перепроверяются workspace/thread/turn, runtime, session generation, manifest и projection activation generation, native thread/turn/item, актуальная durable native attempt, execution owner и owner generation. Существующие grants, lease revalidation, frozen bindings и проверки schema fingerprints не ослаблены. Completion сохраняет schema fingerprints и selection/capability metadata из проверенного binding. Не используется сопоставление по имени, arguments, времени или FIFO; sequence здесь является causal fence текущей попытки, а не способом поиска вызова.

Native ItemCompleted больше не создаёт второе завершение управляемого MCP item и не заменяет operation outcome состоянием ожидания клиента. Late native start/update отсекается после canonical acceptance terminal результата, включая случай отложенной проекции. Уже принятые observations проверяются по canonical sequence. Обычный event idempotency и projection replay сохранены. Приватные callers изменённого append helper проверены; существующий CRUD-тест адаптирован к явно пропущенному observation.

Обнаруженный важный caller — Task observation gate: ранее он использовал completed MCP item как свидетельство доставки. Новый execution-only item помечен `responseDelivery=unconfirmed` и исключён из этого свидетельства. Отдельный успешный native task-result completion сохраняет прежний смысл через **существующий** `task.terminal.observed`, с детерминированным id. Это отдельный observation receipt, не второе завершение вызова. Используются прежние нормализация Task payload, семантические outcome keys и continuation receipts; правила review/accept, Tasks/Subagents protocol, запуск turn и авторизация не изменены. Claude payload, нормализованный до raw-redaction, учитывается через прежний taskResult.

Output policies, redaction и хранение проходят существующий `mcp_tool_item`/permanent-storage validation. Не добавлены отсечения ответа, лимиты текста/корпуса, TTL или raw diagnostics. `safe_read_error`, Threads error vocabulary, результаты Threads mutations, pagination, поиск, индексация, UI, зависимости и skills не менялись.

## Написанные регрессии — НЕ ЗАПУСКАЛИСЬ

Основной владелец проверки стыка — `crates/gateway/src/mcp_service/tests/cli_mcp_call_finalization_tests.rs`. Он использует настоящий facade, coordinator/grants, Codex exact correlator, GatewayTurnMcpInvoker, frozen projection/authorization и canonical CrudStore. В обычных сценариях подменено только validated execution; проверка отказа дополнительно использует существующий RuntimeView seam для определённого PermissionDenied. Отдельный progress transport позволяет оборвать ожидание после реального canonical commit. CLI-процесс не создаётся.

Написаны десять тестов:

1. Успешный MCP без native ItemCompleted, закрытие turn, новый CrudStore и существующий replay; один terminal event, без повторного исполнения и без raw-result canary в истории.
2. Drop ожидания после commit во время final progress: ledger возвращает сохранённый исходный ответ, canonical успех остаётся, delivery не выдумана.
3. Общая матрица настоящего MCP error result, typed execution error, подтверждённой отмены, поздней cancellation и unknown: результат до/после terminal turn, сохранение причины и одного исхода.
4. Unsaved native start, принятый terminal predecessor, deferred completion и late start/update: восстановление штатным replay сохраняет terminal результат. Второй порядок проверяет start, принятый до completion.
5. Turn закрывается первым: честный unknown до подтверждения и корректное завершение исходной interrupted attempt после него.
6. Одинаковые tool/arguments у A/B: A не завершает B; поздний native start не воскрешает A, native completion не дублирует результат.
7. Смена generation во время исполнения не завершает item чужой generation.
8. Чужой native thread не заимствует зарегистрированный вызов; отсутствие подтверждения остаётся unknown.
9. Подтверждённый validation refusal сохраняется до native completion/turn close без execution; stale generation не получает права завершить item.
10. Drop до получения результата оставляет response_unconfirmed и unknown, без выдуманного успеха или cancellation ACK.

Адаптирован projector-тест native MCP start/completion. Расширен существующий `cli_terminal_outcomes_require_successful_identified_durable_delivery`: реальный Task result, сохранённый как execution-only canonical item, не даёт observation; отдельная native delivery по-прежнему необходима. Fixture дополнен существующим host manifest identity. Это отдельная проверка caller/delivery boundary, а не дублирование выбора статуса.

Pre-fix/post-fix выполнение, компилируемость fixtures и прохождение тестов **не подтверждены**. Статически основной сценарий до исправления оставляет item inProgress до превращения в execution_failed; новые assertions требуют canonical terminal result до native completion.

## Результат самостоятельного статического ревью

Проведены промежуточные проходы по identity/source, canonical commit, cancellation/delivery, generation/повторам/гонкам и output policies/callers. Затем перечитан весь собственный diff, включая тестовый harness и адаптированные callers.

Найденные и устранённые проблемы: поздняя отмена поверх результата; ложный cancelled при Drop; возрождение поздним start; execution_failed вместо unknown; потеря результата за принятым predecessor; восстановление auto-interrupted attempt; влияние execution-only записи на Task delivery gate; Claude taskResult после redaction; изменение возвращаемого типа приватного append helper в существующем тесте.

Финальные допустимые проверки: `cargo fmt --all --check` и `git diff --check`. Никакая проверка типов/исполнения не подменялась статическим ревью. Тесты, cargo check, компиляция, сборки, приложение, Gateway, CLI-агенты, генераторы, миграции, benchmarks и живое воспроизведение **не запускались**. Это передача на внешнее ревью, не окончательная приёмка. Для запуска тестов/сборок требуется отдельное разрешение пользователя после внешней приёмки.

## Выполненные команды первой передачи

Команды ниже выполнены из своего worktree, кроме первоначальной подготовки, явно указанной отдельно. Чтение исходников делалось `cat`, `sed -n` и `rg` по перечисленной в задании цепочке, canonical event store/projector, policies, Task observation callers, manifests и CI; файлы менялись через `apply_patch`. Скрипты и код проекта не исполнялись.

Подготовка в `/Users/alexander/Code/pioneer/pioneer`:

```sh
git status --short
git worktree list
git rev-parse main
git branch --list fix/cli-mcp-call-finalization
git worktree add -b fix/cli-mcp-call-finalization /Users/alexander/Code/pioneer/pioneer/.worktrees/cli-mcp-call-finalization main
```

Прочитаны применимые `AGENTS.md` в корне и `crates/crud/AGENTS.md`, проверено отсутствие дополнительных scoped AGENTS для изменяемых путей. Для authoring gate прочитаны `test-audit/SKILL.md` и его `references/pioneer.md`, `references/audit.md`; переданные Pioneer skills прочитаны локально, их Task/Threads операции не вызывались.

Git и выравнивание базы:

```sh
git ls-remote origin refs/heads/main
git -c core.hooksPath=/dev/null fetch --no-tags origin dc90743137af9d220a3594c8fe6190bdbde7d999
git add crates/crud/src/cli_mcp_call.rs crates/crud/src/lib.rs crates/crud/src/projector.rs crates/crud/src/repositories/turn_item_attempt.rs crates/gateway/src/cli_runtime/mcp/facade.rs crates/gateway/src/cli_runtime/projector.rs crates/gateway/src/mcp_service.rs crates/gateway/src/turn_mcp/invoker.rs crates/gateway/src/mcp_service/tests/cli_mcp_call_finalization_tests.rs
git -c core.hooksPath=/dev/null commit -m 'fix: reconcile managed CLI MCP outcomes with canonical items'
git diff --stat 7f226499d3fa8beac5dd15b3d241040e7a99016e dc90743137af9d220a3594c8fe6190bdbde7d999
git log --oneline 7f226499d3fa8beac5dd15b3d241040e7a99016e..dc90743137af9d220a3594c8fe6190bdbde7d999 -12
git -c core.hooksPath=/dev/null rebase dc90743137af9d220a3594c8fe6190bdbde7d999
git restore --source=dc90743137af9d220a3594c8fe6190bdbde7d999 --worktree crates/crud/src/projector.rs
```

Последний restore убрал только собственный промежуточный вариант projector перед окончательным решением; соседние реализации не читались и не копировались.

Статическое ревью и форматирование (повторялись при изменениях):

```sh
git diff --stat
git diff --numstat
git diff --check
git status --short
git diff --name-only dc90743137af9d220a3594c8fe6190bdbde7d999
git diff dc90743137af9d220a3594c8fe6190bdbde7d999 -- crates/crud/src/lib.rs crates/crud/src/projector.rs crates/crud/src/repositories/turn_item_attempt.rs
git diff dc90743137af9d220a3594c8fe6190bdbde7d999 -- crates/gateway/src/cli_runtime/mcp/facade.rs crates/gateway/src/cli_runtime/projector.rs crates/gateway/src/mcp_service.rs crates/gateway/src/turn_mcp/invoker.rs
git diff dc90743137af9d220a3594c8fe6190bdbde7d999 -- crates/gateway/src/task_tools/mod.rs crates/gateway/src/message/tests/cli_task_tools.rs
cat crates/crud/src/cli_mcp_call.rs
cat crates/gateway/src/mcp_service/tests/cli_mcp_call_finalization_tests.rs
cargo fmt --all --check
cargo fmt --all
```

Первый fmt --check показал только неотформатированные собственные изменения; далее использован разрешённый cargo fmt. Итоговые команды передачи:

```sh
cargo fmt --all --check
git diff --check
git ls-remote origin refs/heads/main
git add --all
git diff --cached --check
git diff --cached --stat
git diff --cached dc90743137af9d220a3594c8fe6190bdbde7d999 -- crates/crud/src/lib.rs crates/crud/src/cli_mcp_call.rs crates/crud/src/projector.rs crates/crud/src/repositories/turn_item_attempt.rs crates/gateway/src/cli_runtime/mcp/facade.rs crates/gateway/src/cli_runtime/projector.rs crates/gateway/src/mcp_service.rs crates/gateway/src/turn_mcp/invoker.rs crates/gateway/src/task_tools/mod.rs crates/gateway/src/message/tests/cli_task_tools.rs
git diff --cached dc90743137af9d220a3594c8fe6190bdbde7d999 -- crates/crud/src/projector.rs crates/crud/src/repositories/turn_item_attempt.rs crates/gateway/src/cli_runtime/mcp/facade.rs
git diff --cached dc90743137af9d220a3594c8fe6190bdbde7d999 -- crates/gateway/src/mcp_service/tests/cli_mcp_call_finalization_tests.rs
git -c core.hooksPath=/dev/null commit --amend --no-edit
git diff --check dc90743137af9d220a3594c8fe6190bdbde7d999..HEAD
git status --porcelain=v1
git log -1 --format='%H%n%P%n%s'
git worktree list
```

После последнего commit ожидается чистое дерево своей ветки; фактический SHA и состояние дерева сообщаются в передаче вместе с этим отчётом.

## Команды повторной передачи

Рабочий каталог всех перечисленных ниже команд:
`/Users/alexander/Code/pioneer/pioneer/.worktrees/cli-mcp-call-finalization`.
Чтение через `cat`, `sed -n`, `rg` и просмотр diff выполнялись по изменяемым файлам, их указанным в задании callers, existing event index/Entity, protocol metadata и AGENTS. Правки выполнены через `apply_patch`, без запуска скриптов/кода проекта. В дополнение к уже прочитанному test-audit проверены локальные authoring references; требования пользователя о production fixes и запрете тестовых запусков имеют приоритет.

Подготовка и контроль изоляции:

```sh
cat AGENTS.md crates/crud/AGENTS.md
rg --files -g AGENTS.md crates/gateway crates/crud docs
git rev-parse HEAD
git branch --show-current
git status --short
git worktree list
git merge-base HEAD dc90743137af9d220a3594c8fe6190bdbde7d999
```

Статический аудит (повторялся после исправлений; дополнительные адресные чтения делались `sed -n` и `rg`):

```sh
git diff --stat
git diff --shortstat
git diff --name-only
git diff --numstat dc90743137af9d220a3594c8fe6190bdbde7d999
git diff --stat dc90743137af9d220a3594c8fe6190bdbde7d999
git diff -- crates/crud/src
git diff -- crates/gateway/src/turn_mcp/invoker.rs crates/gateway/src/cli_runtime/mcp/facade.rs
git diff -- crates/gateway/src/message/tests/cli_task_tools.rs
git diff dc90743137af9d220a3594c8fe6190bdbde7d999 -- crates/crud/src/cli_mcp_call.rs crates/crud/src/lib.rs
git diff dc90743137af9d220a3594c8fe6190bdbde7d999 -- crates/crud/src/projector.rs crates/crud/src/repositories/turn_item_attempt.rs crates/crud/src/repositories/turn_event.rs
git diff dc90743137af9d220a3594c8fe6190bdbde7d999 -- crates/gateway/src/cli_runtime/mcp/facade.rs crates/gateway/src/cli_runtime/projector.rs crates/gateway/src/turn_mcp/invoker.rs
git diff dc90743137af9d220a3594c8fe6190bdbde7d999 -- crates/gateway/src/task_tools/mod.rs crates/gateway/src/message/tests/cli_task_tools.rs crates/gateway/src/mcp_service.rs
rg -n 'confirmed_mcp_call_completed\(' crates
ps -axo pid,ppid,etime,comm | rg 'cargo-fmt|rustfmt|cargo$'
```

`ps` был только чтением списка процессов при длительном разрешённом форматировании; процессы других задач не менялись. Поиск одного предполагаемого пути `crates/protocol/src/tool_metadata.rs` вернул отсутствие файла; определение найдено в `crates/protocol/src/turn.rs`.

Разрешённые проверки и фиксация повторной передачи:

```sh
cargo fmt --all
cargo fmt --all --check
git diff --check
git diff --check dc90743137af9d220a3594c8fe6190bdbde7d999
git diff -- crates/crud/src/cli_mcp_call.rs crates/crud/src/lib.rs crates/crud/src/projector.rs crates/crud/src/repositories/turn_event.rs crates/gateway/src/cli_runtime/mcp/facade.rs crates/gateway/src/cli_runtime/projector.rs crates/gateway/src/turn_mcp/invoker.rs
git diff -- crates/gateway/src/mcp_service/tests/cli_mcp_call_finalization_tests.rs crates/gateway/src/message/tests/cli_task_tools.rs docs/reviews/cli-mcp-call-finalization.md
git add crates/crud/src/cli_mcp_call.rs crates/crud/src/lib.rs crates/crud/src/projector.rs crates/crud/src/repositories/turn_event.rs crates/gateway/src/cli_runtime/mcp/facade.rs crates/gateway/src/cli_runtime/projector.rs crates/gateway/src/turn_mcp/invoker.rs crates/gateway/src/mcp_service/tests/cli_mcp_call_finalization_tests.rs crates/gateway/src/message/tests/cli_task_tools.rs docs/reviews/cli-mcp-call-finalization.md
git diff --cached --check
git diff --cached --stat
git -c core.hooksPath=/dev/null commit -m 'fix: address CLI MCP finalization review gaps'
git status --porcelain=v1
git log -1 --format='%H%n%P%n%s'
```

Результаты: форматирование и whitespace checks пройдены; фактическое чистое состояние дерева и SHA нового commit передаются в ответе. Тесты (включая mock), `cargo check`, компиляция, сборки, приложение, Gateway, CLI-агенты и генераторы **не запускались**. Компилируемость и прохождение тестов **не подтверждены**. Это повторная передача на внешнее ревью, не окончательная приёмка.

## Исправление P1: путь к константе события

Исходный HEAD проверен: `07142d3f73027fc000e1d4e56fec43a4fd16fccc`; дерево было чистым. Работа выполнена в прежней ветке/worktree после чтения применимых AGENTS.md и проверки `git worktree list`.

В `crates/crud/src/repositories/turn_event.rs` исправлена единственная ссылка на `pioneer_protocol::constants::events::ITEM_COMPLETED`. Статически сверены объявление `constants::events`, экспорт `constants` и остальные event-constant paths в файлах исходного исправления. Новый re-export/API не добавлен; три предыдущих исправления и все регрессии сохранены без изменений. Новые тесты не добавлялись.

Точные проверки этой передачи:

- `git rev-parse HEAD`, `git branch --show-current`, `git status --porcelain=v1`, `git worktree list` — проверка исходной ветки, HEAD и изоляции.
- `rg --files -g AGENTS.md crates/crud docs`, чтение корневого и CRUD `AGENTS.md`.
- `rg -n 'pioneer_protocol::(constants::)?events::|constants::events|pub mod constants|pub mod events|ITEM_COMPLETED' crates/crud/src/repositories/turn_event.rs crates/crud/src/events.rs crates/protocol/src/lib.rs crates/protocol/src/constants.rs`.
- `rg -n 'pioneer_protocol::events::|constants::events' crates/crud/src/cli_mcp_call.rs crates/crud/src/lib.rs crates/crud/src/projector.rs crates/crud/src/repositories/turn_item_attempt.rs crates/gateway/src/cli_runtime/mcp/facade.rs crates/gateway/src/cli_runtime/projector.rs crates/gateway/src/mcp_service.rs crates/gateway/src/mcp_service/tests/cli_mcp_call_finalization_tests.rs crates/gateway/src/message/tests/cli_task_tools.rs crates/gateway/src/task_tools/mod.rs crates/gateway/src/turn_mcp/invoker.rs`.
- `cargo fmt -p pioneer-crud --check` — первый проход потребовал только переноса исправленного выражения; перенос внесён, повторная проверка пройдена.
- `git diff --check`, `git diff --cached --check` — whitespace.
- `git diff -- crates/crud/src/repositories/turn_event.rs docs/reviews/cli-mcp-call-finalization.md` — полное статическое чтение нового diff: только ссылка и этот отчёт.
- После commit: `git status --porcelain=v1`, `git log -1 --format='%H%n%P%n%s'`.

Commit создаётся с `git -c core.hooksPath=/dev/null commit -m 'fix: use existing protocol event constant path'`. Merge, push и deploy не выполнялись. Тесты, mock-сценарии, `cargo check`, компиляция, сборки, приложение, Gateway, CLI-агенты, миграции и генераторы **не запускались**; компилируемость и прохождение тестов **не подтверждены**. Передача на внешнее ревью, не окончательная приёмка.
