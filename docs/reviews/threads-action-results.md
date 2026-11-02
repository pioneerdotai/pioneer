# Threads action results — передача на внешнее ревью

## Повторная передача после PR №227 — 2026-11-01

Этот раздел описывает актуальный кандидат. Нижние разделы сохранены как исторический отчёт первоначальной реализации; прежняя база и прежние успешные проверки в них не относятся к текущему main.

- Исходный HEAD: `b784bfcfbe38da1214fcec69cc88987f58bc0f5a`; прежняя база: `b9a932f576282c0e75f501be3eaf39abf0ecadf5`.
- Перед rebase исходный HEAD сохранён отдельной веткой `backup/threads-action-results-before-main-20261101`.
- После `git -c core.hooksPath=/dev/null fetch origin main` фактические main и origin/main совпали: `3f862da3f494f3e6048e211f426c24cd81d2aeac`.
- PR №227 присутствует в main: merge commit `5e0eac89dee86c0fa27ce99e94d13b068d1e967c`, заголовок `fix(threads): share thread turn launch path (#227)`. Проверено ancestry через `git merge-base --is-ancestor`.
- Новый HEAD реализации после переноса: `7b6baf116aa61d0fc470a4806b404e868eee5c16`, непосредственный parent — указанный актуальный main. Финальный HEAD включает следующий commit обновления этого отчёта и передаётся в итоговом ответе; точная ссылка: `git rev-parse fix/threads-action-results`.
- Ветка и worktree прежние: `fix/threads-action-results`, `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-action-results`.
- Rebase выполнен как `git -c core.hooksPath=/dev/null rebase --onto 3f862da3f494f3e6048e211f426c24cd81d2aeac b9a932f576282c0e75f501be3eaf39abf0ecadf5 fix/threads-action-results`: перенесён ровно один собственный commit. Старый общий запуск повторно не переносился.

### Конфликт и адаптации

Единственный текстовый конфликт — `crates/gateway/src/message/tests/cli_task_tools.rs`, в `cli_threads_actions_use_facade_shared_handlers_routes_and_replay_for_all_providers`. Main добавил проверки сохранности исходного canonical completion и отсутствия второго ItemCompleted при replay. Сохранены обе эти проверки и все прежние assertions threadResult; удалены только conflict markers. `git range-diff b9a932f5..backup/threads-action-results-before-main-20261101 main..7b6baf116` показывает только изменение контекста этого блока. Production patch, типы, схемы и bundled docs перенесены без изменения содержания.

Актуальные исправления main сохранены: prepared composer Task payload, work-owner binding, восстановление detached admission и bounded async futures; в native fixtures — ProviderMetadata вместо удалённого static reasoning registry, чтение TaskRun card из history вместо latest-turn snapshot и штатный expire_review_admission. Никаких assertions не удалено и не ослаблено. Других необходимых адаптаций не выявлено. Новые тесты при этой интеграции не добавлялись: существующие расширенные регрессии из списка ниже покрывают затронутые риски и остались написанными, но незапущенными.

### Полный статический самоаудит на новой базе

1. Перечитаны весь собственный diff относительно `3f862da3f` и затронутые callers, отдельно от изменений main. Контракт неизменен: create — workspaceId/threadId; send — workspaceId/threadId/turnId; Collaborative — родительские threadId/turnId и реальный taskId; foreground — threadId/turnId/executionId фактического исполнения. Будущий child не угадывается, модель не ожидается.
2. Сверены существующие `commit_agent_thread_creation_with_action_once`, `materialize_completed_agent_message_turn_with_action`, `materialize_agent_turn_start_with_graph_and_action` и TaskService `commit_task_creation`: threadResult входит в action/receipt JSON перед тем же атомарным commit эффекта. Новые границы транзакций не вводились.
3. Проверены первоначальные ответы, обычный ledger replay и replay под StartAgent commit lock. Detached ответ после общего запуска и после ошибки ответа TaskService читает уже сохранённый receipt. Fault-regression сохраняет точные ссылки до потери вызывающей future и сверяет их после recovery. Compaction сохраняет исходный threadResult в action и receipt, не меняя hash fencing и outbox lifecycle.
4. Проверены `saved_thread_action_result` и optional serde: старые receipts без поля возвращают отсутствие ссылок, без реконструкции или выдумывания IDs. Denied не получает threadResult. Авторизация, workspace/thread scope, source fences, opaque targetOptions и ограничения исполнения не менялись: ID не выдаёт право доступа.
5. Native и admitted CLI продолжают пользоваться общим handler и JSON result. MCP facade переносит structuredContent целиком. Сохранены изменения main для canonical CLI outcome/replay, отмены и recovery. Перечитаны все изменённые тестовые блоки и актуальные fixtures/API, включая continuation в том же TaskRun child.
6. Сверены optional threadResult, его экспорт и schema registry, оба описания AgentThreadActionResult, схемы opaque option, bundled Threads skill и references. Новые правила main для списка тредов и запрета inter-agent conversation writes сохранены; Tasks/Subagents policy blocks не менялись. Установленные skills не редактировались.

Подтверждённых дефектов интеграции при статическом просмотре не обнаружено. Это не подтверждение компилируемости или прохождения регрессий.

### Фактически выполненные проверки

- `cargo fmt --all --check` — exit 1: единственное замечание к порядку imports в `crates/observability/src/lib.rs:828`. Через `git show main:crates/observability/src/lib.rs` и пустой diff этого файла относительно main подтверждено, что это состояние базы. Посторонний файл не исправлялся.
- `cargo fmt -p pioneer-protocol -p pioneer-gateway -p pioneer-crud -p pioneer-tasks --check` — exit 0 для всех затронутых Rust packages. Форматирование с записью в этом проходе не запускалось.
- `git diff --check main` и `git diff --check main HEAD` — exit 0. Проверены range-diff и состав собственного patch; после обновления отчёта whitespace-проверка повторена.
- Прочитаны применимые root/CRUD AGENTS.md в исходной и новой базе. Требования запуска тестов уступают прямому запрету пользователя.
- Все изменяющие Git-операции (fetch, backup branch, rebase, add, rebase continue, commit) выполнены с `core.hooksPath=/dev/null`; rebase continue — с `GIT_EDITOR=true`. Основной checkout и соседние worktree не изменялись; исходное дерево было чистым.

Тесты, cargo check/build/test/nextest/clippy/run, компиляция, приложение, Gateway, CLI-агенты, миграции, генераторы и воспроизведение не запускались. Успешность тестов и компиляции не подтверждена. Merge в main, push, deploy и публикация PR не выполнялись. Результат передаётся на повторное внешнее ревью, не на окончательную приёмку.

## Исторический отчёт первоначального кандидата

Подготовлено для внешнего ревью, не для окончательной приёмки. Код проекта не выполнялся; компилируемость и прохождение тестов не подтверждены.

## База и изоляция

- Фактический local `main` при начале и итоговой проверке: `7f226499d3fa8beac5dd15b3d241040e7a99016e`, основной checkout чистый.
- Общий запуск ещё не включён в этот main. Рабочая база этой задачи: `b9a932f576282c0e75f501be3eaf39abf0ecadf5`, ветка зависимости `fix/threads-shared-launch`.
- Зависимость была прочитана по её commit и отчёту (теперь доступен через `git show b9a932f5:docs/reviews/threads-shared-launch.md`; файл удалён из актуального main). Её наличие тогда не означало внешнюю приёмку.
- Собственная ветка: `fix/threads-action-results`.
- Собственный worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-action-results`.
- Имена были свободны. Незакоммиченные изменения соседних worktree не переносились. Основной checkout и соседние worktree не изменялись.
- Собственный patch: `git diff b9a932f576282c0e75f501be3eaf39abf0ecadf5 HEAD`. Изменения общего запуска между main и базой в этот объём не входят.

## Ответ

Добавлено optional `AgentToolSafeResult.threadResult: AgentThreadActionResult`. Старые status, outcome, receiptId, outboxId, publicMessage и семантика refreshed targetOptions сохранены.

| Операция | Содержимое threadResult | Источник и подтверждение |
| --- | --- | --- |
| thread_create | workspaceId, threadId | Реальный Thread, переданный в commit_agent_thread_creation_with_action. Thread, route/lineage и action/receipt/outbox сохраняются одной существующей транзакцией. |
| thread_message_send | workspaceId, threadId, turnId | Фактические thread/turn из подготовленного completed message outcome, сохранённые materialize_completed_agent_message_turn_with_action вместе с canonical events и action. |
| threads_turn_start, foreground | workspaceId, threadId, turnId, executionId | Проверенное назначение, admitted turn и execution graph, сохранённые materialize_agent_turn_start_with_graph_and_action. Это turn исполнения в том же разговоре, включая TaskRun child. |
| threads_turn_start, Collaborative | workspaceId, threadId, turnId, taskId | Общий start_thread_turn/start_detached_thread_turn задаёт точный родительский admission; TaskService добавляет фактический taskId перед существующим атомарным commit_task_creation вместе с action/receipt/outbox. |

`threadId` всегда обозначает созданный или целевой разговор. `turnId` принадлежит именно этому threadId. В Collaborative это родительский admission, а не turn исполнения в child. `executionId` выдаётся только для foreground graph, `taskId` — для detached Task. Нового способа создания Task/child/turn нет.

Ссылки фиксируются в момент commit. Исполняемый child у detached Task создаётся позже существующим Task runtime: его ID не угадывается, модель не ожидается. Агент может сразу открыть родительскую историю по `threadId` и `anchor:{turnId}` и затем следовать её штатной child reference. Более поздние сведения при replay не подмешиваются.

ID — ссылки для навигации, не полномочия. Все прежние проверки identity/authority, workspace/thread scope, opaque option, source fence и admission сохраняются. targetOptionId по-прежнему обязателен для send/start; возвращённый threadId его не заменяет. Accepted/queued подтверждают admission, а не завершение модели.

## Commit и replay

- `record_thread_action_result` дополняет существующий response JSON; остальные receipt metadata сохраняются. Одинаковое значение записывается в action_response_json и receipt_response_json до эффекта операции в её существующей транзакции. Новых таблиц, транзакционных границ и механизмов идемпотентности нет.
- Обычный replay и повтор, попавший под существующий StartAgent commit lock, читают threadResult из сохранённого receipt. Поиск по названию, workspace listing, индекс и «последний turn» не используются.
- Detached handler после общего запуска, включая восстановление после потерянного ответа TaskService, читает уже сохранённый receipt. taskId не берётся из локального предположения или нового create_task.
- После commit ошибка последующей публикации/проекции не стирает ссылки: повтор той же logical call использует durable результат, а не повторный эффект.
- Compaction action/receipt сохраняет точный threadResult вместе с прежними hash markers. Без этого штатная очистка больших response payloads теряла бы ранее выданные ссылки. Outbox lifecycle и правила его compaction не менялись.
- Исторические receipt без threadResult остаются совместимыми: отсутствующее поле возвращается отсутствующим. Невосстановимые старые ссылки не изобретаются и старые записи не переписываются.
- Denied/failed не получают спекулятивных ссылок. Результат с новыми ID возвращается только после подтверждённого commit.

## Проекции и документация

Native и CLI используют те же AgentActionToolHandler и FunctionToolOutput JSON. Прочитана существующая проекция MCP structuredContent; отдельного CLI результата не добавлено. Optional serde-поле сохраняет чтение старых ответов.

Вручную обновлены `agent_tool_safe_result.json`, добавлена `agent_thread_action_result.json` и запись в protocol schema registry. Описание opaque target исправлено в типе и трёх остальных схемах этого типа: IDs навигации теперь возвращаются, но не заменяют option.

Bundled `resources/skills/pioneer/threads` изменён только в результатах create/send/start и соответствующих workflows/troubleshooting. Раздел списка тредов не изменён. Установленные skills, Tasks/Subagents skills и policy blocks не редактировались.

## Написанные регрессии — не запускались

Расширены существующие содержательные тесты и mock-исполнители:

- `inherited_configuration_survives_visible_thread_creation_message_and_start_replay`: native create сравнивает ссылку с сохранённым Thread; send — с фактическим message turn и штатным history read по возвращённому anchor; create/send replay сохраняют ссылки. Concurrent Collaborative start и replay возвращают одну Task и родительский admission, отличающийся от реального child.
- `inherited_reasoning_replay_uses_committed_choice_when_catalog_changes`: foreground result с точным сохранённым responding execution и turn; replay сохраняет ссылку при изменённом/недоступном каталоге.
- `detached_threads_action_recovers_after_commit_without_recreating_or_dispatching_parent`: вызывающая future теряется на существующем barrier после Task/action commit; receipt уже содержит точные ссылки; после recovery и реального mock child replay возвращает те же значения без второй Task/dispatch.
- `cli_threads_actions_use_facade_shared_handlers_routes_and_replay_for_all_providers`: настоящие admitted invocation через существующий MCP facade для Codex/Claude/OpenCode; create/send/start сохраняют threadResult на reconnect replay, create указывает на реально сохранённый тред, send открывается через штатный history anchor, foreground execution сверяется с durable turn response. Существующий подсчёт исключает повторное создание.
- `cli_threads_collaborative_admission_uses_one_detached_task_and_normal_stop`: returned admission/Task отличаются от исполняемого child; continuation из TaskRun возвращает фактический foreground turn в том же child, без новой Task. Replay сохраняет ссылки.
- `cli_threads_permission_matrix_preserves_restricted_names_denies_and_missing_grants`: отказ restricted binding не выдаёт threadResult.
- `terminal_action_ledger_compaction_preserves_window_and_exact_replay_hashes`: foreground и detached ссылки сохраняются при реальной compaction ledger, большие поля удаляются, старые replay hashes остаются валидны.
- `thread_action_references_are_additive_and_preserve_wire_casing`: отдельная protocol compatibility-регрессия для старого ответа без поля и camelCase нового nested результата.

Это описание написанного покрытия, не подтверждение его прохождения. Baseline/fixed тесты не исполнялись.

## Статический самоаудит

Промежуточный проход и полное итоговое ревью собственного diff выполнены последовательно:

1. Контракт и схемы: optional совместимость, casing, required workspace/thread, ссылки отдельно от opaque options; проверены все AgentToolSafeResult constructors и schema registry.
2. Операции/commit: сверены источники ID и существующие атомарные границы create, send, foreground, shared detached Task. Результат не выдаётся до успешного сохранения.
3. Replay: проверены обе replay-ветки, потеря ответа общего запуска, immutable action/receipt и compaction. Найденная потеря response при compaction исправлена сохранением threadResult.
4. Native/CLI: просмотрены общий handler, MCP structuredContent и admitted facade fixtures; bundled Threads docs согласованы с выбранным контрактом.
5. Тесты/callers: перечитаны все изменённые тестовые блоки и окружающие fixtures. Во время промежуточного просмотра исправлены неверное размещение нового assertion block и затенение переменной каталога в тесте. После этого цепочки перечитаны повторно.

Финальное ревью охватывает весь собственный diff, включая новый schema-файл и этот отчёт. Других найденных незакрытых проблем в статическом просмотре нет. Это не доказательство типовой корректности или поведения при исполнении.

## Выполненные команды и передача

Точные shell invocations первоначальной реализации с cwd, включая чтение/поиск и команды с отсутствовавшими путями, находятся в [историческом журнале команд](threads-action-results.commands.jsonl). Это журнал исходного кандидата b784bfcf, не нового интеграционного прохода. Журнал извлечён только из tool calls того transcript; вывод команд, чужие сессии и содержимое инструкций туда не копируются. Вспомогательная обработка журнала читала текст, код проекта не загружала и не выполняла. Файлы изменялись через apply_patch; схемы редактировались вручную, генераторы не вызывались.

Проверки: `cargo fmt --all`, затем `cargo fmt --all --check` — exit 0; `git diff --check` — exit 0. После оформления отчёта whitespace-проверка повторена относительно рабочей базы. Прочитаны применимые root/CRUD AGENTS.md; их требования запуска тестов уступают прямому запрету пользователя.

Завершающие команды после снимка журнала приведены явно (cwd — собственный worktree):

```sh
git diff --check b9a932f576282c0e75f501be3eaf39abf0ecadf5
git add crates resources/skills/pioneer/threads schemas docs/reviews/threads-action-results.md docs/reviews/threads-action-results.commands.jsonl
git -c core.hooksPath=/dev/null commit -m 'fix: return durable thread action references'
git status --porcelain=v1
git rev-parse HEAD
git log -1 --format='%H%n%s%n%P'
git diff --check b9a932f576282c0e75f501be3eaf39abf0ecadf5 HEAD
git -C /Users/alexander/Code/pioneer/pioneer status --porcelain=v1
```

Commit создан с отключёнными hooks. Merge в main, push, deploy и публикация PR не выполнялись. Тесты, cargo check, компиляция, сборки, приложение, Gateway, CLI-агенты, генераторы, миграции и сценарии воспроизведения не запускались. Компилируемость и прохождение тестов не подтверждены. Проверки с выполнением кода требуют отдельного разрешения после внешней приёмки.
