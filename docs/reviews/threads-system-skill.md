# Threads system skill: передача на внешнее ревью

## Ревизии и изоляция

- Исходный commit/main: `80787a5aea854bab51ebd0b90292336ef16ace94`.
- Ветка: `feat/threads-system-skill`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-system-skill`.
- Рабочая база после подключения зависимостей: `51205596d9fc23c490089db4f3b14cb305b0e763`.
- Итоговый commit реализации: `e70f8cf77d49b3065175334697bba51f27fe0d66`.
- Этот отчёт добавляется отдельным следующим коммитом передачи. Его идентификатор — HEAD указанной ветки после передачи; код зафиксирован предыдущим commit реализации. Это позволяет указать точный проверенный commit кода без самоссылки отчёта на собственный Git hash.

При начале работы основной checkout был чистым. Указанные ветка и worktree отсутствовали; они созданы от фактического main. Основной checkout и соседние worktree не редактировались. Исходные dependency branches сохранены. Merge в main, push и deploy не выполнялись. При создании worktree, merge и commit использован `-c core.hooksPath=/dev/null`; commit hooks не запускались.

## Подключённые зависимости и собственный diff

В main уже было чтение истории, но отсутствовали готовые search и identity/models изменения. В изолированной ветке выполнены:

1. Fast-forward на `feat/threads-search-recall`, `f911bb566007a15016bbdf886c7f0792eb3cc7a2`.
2. Merge `feat/threads-identity-models`, `b02a483cea37136a303da6aae91d6aeb20b7ba1a`.
3. Необходимое разрешение двух конфликтов: CLI-фильтрация в `message/turn_handlers.rs` сохраняет search authority check и identity/models capabilities; snapshot домена Threads в `promt/tests/compile_snapshots.rs` содержит и search, и identity/models.

Dependency diff — `80787a5a..51205596`: 271 файл, 404676 добавленных и 703 удалённых строки. Основной объём относится к готовому search backend и его vendor-коду; изменения Cargo и schemas в этом диапазоне пришли из зависимостей. Эти реализации не переписывались. Их компиляция и тесты в этой работе не проверялись.

**Собственный diff задачи — `51205596..e70f8cf77d49b3065175334697bba51f27fe0d66`.** В нём нет изменений Cargo, зависимостей, сгенерированных schemas, UI, search/history/model business logic или Tasks/Subagents skills/references. Общий diff от main нельзя использовать как меру собственного изменения системного skill.

## Карта требований

| Требование | Основные файлы | Результат |
| --- | --- | --- |
| Отдельный системный skill | `resources/skills/pioneer/threads/SKILL.md`, три файла `references/`, `resources/skills/bundled-system-skills.toml` | `system:pioneer/threads`, стабильный SkillId `ThR7m2K9vQ4cX8pL6nB3s`; Required, catalog-hidden, model-invocable; bundling/hash/materialization через существующий manifest |
| CLI export и lifecycle | `crates/gateway/src/cli_runtime/skills.rs`, `message/skills/catalog.rs`, `message/turn_handlers.rs` | Ограниченный alias `pioneer-system-threads`; общий install plan, turn binding, frontmatter rename, пакет с references, fingerprint/receipts, ownership checks, update/restore и session identity |
| CLI tools/capabilities | `turn_mcp/native_tools.rs`, `cli_runtime/context.rs`, `message/turn_handlers.rs`, `crates/promt/src/cli_runtime_context.rs` | Независимая классификация Threads по domain membership; разрешённое подмножество фиксированного каталога; отдельный список операций и инструкции |
| Отдельный prompt block | `crates/promt/src/{content,bundle,section,instruction_delivery}.rs`, `compile/compiler.rs`, `crates/agent/src/chat/mod.rs` | Независимые `ThreadsPolicy`, manifest id `threads_policy`, `include_threads_policy`, native/CLI loading; Pioneer trust и штатная delivery-компиляция |
| Согласование native availability | `crates/gateway/src/message/artifact_tools.rs`, `crates/agent/src/chat/mod.rs` | Readers материализуются при существующей read authority; блок учитывает доступные скрытые capabilities и путь загрузки skill, до открытия домена |

Skill описывает все десять фактических имён из `THREADS_DOMAIN_TOOL_NAMES`: `threads_tools_result_read`, `threads_list`, `threads_history_read`, `threads_children_list`, `threads_search`, `threads_start_options`, `threads_identity_odels_list`, `thread_message_send`, `thread_create`, `threads_turn_start`. Написание `odels` сохранено намеренно.

## Какие общие механизмы дополнены

- В существующий whitelist системных CLI exports добавлена только пара `system:pioneer/threads` → `pioneer-system-threads`. Произвольные locked system skills не получили разрешения экспорта. Ранее поддержанные exports не менялись.
- Автовыбор Threads имеет собственное условие по фактически спроецированной операции, отличной от общего `threads_start_options`. После финальной фильтрации `reconcile_implicit_threads_skill` удаляет только implicit Threads binding/install plan, если таких операций больше нет. Явно выбранный skill остаётся документацией и не выдаёт инструментов.
- CLI capability context перечисляет Threads отдельно от Task operations. Общий options callable перечисляется в Threads context лишь вместе с реальной Threads operation. Используются существующие provider projections: Codex/Claude и OpenCode сохраняют свои callable names и frozen schemas.
- Финальная фильтрация readers проверяет текущую `thread_read_authority`, не обходя последующие проверки каждой страницы и каждого источника. Для каталога только с reader tools не требуется материализация Task tooling. Для non-reader tools сохранён существующий общий materialization path.
- Runtime snapshot теперь сохраняется и для CLI turn с Threads без `task_create`: создание треда и наследование настроек требуют фактов текущего исполнения; recovery должен получить тот же snapshot. Добавлено отдельное условие обновления MCP lease для Threads при восстановлении Codex/Claude; прежние Task/OpenCode условия сохранены.
- В `PromptCompileInput` добавлен отдельный флаг; обновлены все callers, первоначальная и повторные компиляции агента. Путь без native tool calling передаёт false. CLI вычисляет флаг из итогового capability context. Recovery читает сохранённые инструкции, install receipts, bindings и runtime snapshot общими механизмами.
- `threads_policy` зарезервирован как встроенный manifest id: динамическая секция не может подменить этот policy block.

Канонические определения и схемы инструментов, provider callable conversions, schema fingerprints, frozen bindings, общий MCP invoker, lease validation, domain handlers, Agent action ledger/outbox и replay переиспользованы. Отдельной CLI business logic, нового idempotency protocol, transport family или grant не добавлено. В частности, search и options остаются в прежнем транспортном `NativeToolFamily::Tasks`, хотя относятся к домену Threads; классификация prompt/skills не выводится из транспортного family. `threads_start_options` по-прежнему имеет одну definition и один binding.

## Граница Tasks/Subagents

Содержимое `resources/skills/pioneer/tasks/`, `resources/skills/pioneer/subagents/` и их references не изменено относительно рабочей базы и main. Тексты native/CLI policy blocks Tasks/Subagents не менялись. Их `include_task_orchestration_policy`, условия выбора/удаления implicit skills и Task operations classifier сохранены.

Новый блок не запускает Task wait/review/heartbeat protocol. Thread create/send/start продолжают использовать Agent action results и replay, без Task error envelope. Изменения существующих Task-тестов ограничены ожидаемым отдельным Threads attachment, расширением общей lifecycle-матрицы и доступом к test-only helper для новой Threads suite.

## Матрица доступности и permissions

| Сценарий | Native | Codex / Claude / OpenCode |
| --- | --- | --- |
| Разрешены readers/search | Скрытые инструменты; загрузка точного `skill:<id>` из Internal Skill References через `read_skill`, затем `request_tools` с `domains:["threads"]` | Разрешённые canonical tools входят в frozen projection; implicit export skill и отдельный Threads block/context |
| Разрешены create/send/start/model options | Имеющиеся Agent capabilities, user/execution grants и opaque options; runtime permission checks сохраняются | Имеющиеся explicit tool selection, user/execution action grants и Agent capabilities; финальная материализация и текущий MCP lease; opaque routes проверяет общий handler |
| Только `threads_start_options` | Сам по себе не включает Threads block | Сам по себе не выбирает implicit Threads skill, не включает Threads block или Tasks/Subagents policy |
| Explicit deny или ограниченный allow-list | Не является основанием включать новый block по запрещённому capability; execution permission evaluator остаётся обязательным | Threads фильтруется до frozen binding; skill не возвращает исключённые операции |
| Нет read authority | Readers не материализуются без read authority | Финальная проекция исключает readers без read authority |
| Выбранный источник удалён или недоступен | Каждый вызов, переход и страница проверяют актуальный источник | Те же source checks в общих handlers; наличие reader в каталоге не обещает доступ к каждому источнику |
| Нет Threads capabilities | Нового блока нет; hidden tools не становятся постоянно видимыми ради prompt | Нет implicit Threads attachment, блока и списка Threads operations |
| Восстановление исполнения | Новая materialization и те же flags во всех повторных компиляциях | Сохранённые instructions/receipts/bindings, runtime snapshot и текущая lease; recovery не выдаёт новые capabilities |

Наличие skill, известного thread ID, broad user rights или options не расширяет immutable execution grants. У CLI по-прежнему нет runtime `request_tools`; каталог фиксирован. Доступность полного набора из десяти имён не обещается каждому исполнению.

## Написанные и расширенные тесты — не запускались

| Owner | Покрытие |
| --- | --- |
| `gateway/src/system_skills.rs` | Отдельный стабильный id и manifest entry; materialization пакета и references; hidden internal reference; runtime asset root/read_skill index; прежний отказ неразрешённых system exports |
| `gateway/src/cli_runtime/skills.rs` | `final_catalog_reconciles_only_implicit_threads_delivery`; options-only/empty/search/read и explicit/implicit selection; существующие install/restore/package-reference и session-reuse матрицы дополнены Threads, native и shadow homes; update после copy-before-receipt; foreign destinations/receipts/local edits и защита alias |
| `gateway/src/turn_mcp/native_tools.rs` | Общая all-provider projection/budget/schema-матрица: все Threads names по одному, единичный options binding, search сохраняет транспортную идентичность, Threads отделён от Task operations |
| `gateway/src/message/tests/cli_threads_tools.rs` | `cli_threads_facade_preserves_permissions_routes_launch_and_mutation_replay_for_all_providers`: реальный общий facade (`initialize`, `tools/list`, active grant/turn, `tools/call`), frozen bindings, read/create/send/start, identity/executor/model/reasoning в dispatch, текущие grants, отсутствие Tasks, отказ запрещённой операции и thread ID вместо opaque route; новый facade на каждой повторной доставке проверяет durable replay; один эффект create/send/start; restore инструкций и runtime snapshot |
| `gateway/src/message/tests.rs`, `tests/cli_task_tools.rs`, `tests/thread_launch_choices.rs` | Общий bundled fixture включает Threads; ожидаемые отдельные skill attachments и persisted binding events; coexistence с пользовательским skill `threads`; переиспользование test-only native setup/activation helpers |
| `promt/src/compile/compiler.rs` | Независимая матрица Tasks/Threads × native/CLI, delivery/trust/manifest sections, сохранение block при повторной компиляции; прежние sections Tasks/Subagents включаются только своим флагом |
| `promt/src/cli_runtime_context.rs` | Раздельные capabilities для трёх CLI; пустой/options-only каталог не активирует policy; Threads-only не включает Task protocol и не требует native loading |
| `agent/src/chat/mod.rs` | Allowed hidden capabilities включают guidance до `request_tools`; deny/restricted allow-list и отсутствие пути открытия не активируют его; options-only не активирует Threads |
| `promt/src/section.rs`, `promt/tests/compile_snapshots.rs` | Reserved manifest id защищён от dynamic collision; все прежние compile fixtures явно оставляют Threads flag выключенным |

Новая facade suite использует существующие recording CLI fixtures. Нативные setup-вызовы только готовят разрешённый child execution; проверяемые CLI mutations проходят facade → invoker → lease → shared runtime/handler → ledger/outbox. Тесты не подменены сравнением схем или прямым вызовом mutation handler. Существующее бизнес-покрытие поиска, пагинации истории и model discovery не переписывалось.

## Результаты самоаудита

1. **Skill/manifest/references.** Сверены protocol types и canonical tool schemas. Уточнены минимальные бюджеты tool-result reader (1), различие byte offsets истории и Unicode offsets reader, отдельные log chunks, anchor/reverse continuation и реальные safe results без выдуманного threadId/title/idempotencyKey. Все JSON-примеры сверены статически с типами; placeholder IDs/модели/effort явно требуют подстановки возвращённых значений.
2. **CLI lifecycle.** Проверены whitelist, frontmatter rewrite, copying references, receipts, foreign ownership, restore, native/shadow roots и application-order/session fingerprints. Threads selection и removal отделены от Tasks/Subagents.
3. **Projection/permissions/dispatch.** Проверены оба транспортных family, единичный options binding, ThreadRead и Agent capability filters, explicit allow/deny, frozen schema identity, текущая lease, общий dispatch и стабильный call id. Новая mutation не требует JSON idempotencyKey: транспорт уже заполняет внутреннюю идентичность первой доставки и replay.
4. **Prompt/recovery.** Обнаружены и исправлены отдельные пробелы Threads-only runtime snapshot и lease renewal. Проверены initial/retry/no-tool/continuation callers, reserved section id, trust, CLI instruction persistence/restore. Native guidance использует доступные скрытые capabilities, а не ждёт уже открытого домена.
5. **Тесты/финальный diff.** Перечитан собственный diff и новые файлы, проверены callers изменённых контрактов. Исправлены fixture conversion для provider fingerprint, тип invoker для facade, initialization/tools-list readiness и provider string-call identity для durable replay. Отдельно исключён вызов Task tooling для каталога только с readers. После исправлений повторно просмотрены затронутые цепочки. Незавершённых известных замечаний статического ревью нет; это не подтверждение компиляции.

## Фактически выполненные проверки

Только форматирование, whitespace и статический просмотр:

- `cargo fmt --all -- --check`: первоначально обнаружил форматирование изменений; повторные вызовы после форматирования завершились успешно.
- `cargo fmt --package pioneer-agent --package pioneer-gateway --package pioneer-promt`: форматирование затронутых пакетов после правок.
- `cargo fmt --package pioneer-agent --package pioneer-gateway --package pioneer-promt -- --check`: успешно на итоговом коде.
- `git diff --check`: успешно по ходу работы и на итоговых изменениях.
- `git diff --cached --check`: успешно для подготовленного собственного diff.
- `git diff --check 80787a5a..51205596`: успешно для подключённых зависимостей.
- `git diff --check 51205596..HEAD`: успешно для собственного commit реализации.
- `git diff 51205596 -- resources/skills/pioneer/tasks resources/skills/pioneer/subagents`: пустой diff; дополнительно просмотрен diff `content.rs`, в котором только новые Threads constants.
- Read-only `git status`, `git worktree list`, `git log`, `git rev-parse`, просмотр `git diff` и поиск/чтение схем, типов и callers. После commit передачи повторно проверяется чистота своего worktree и основного checkout.

**Тесты, cargo check, компиляция, сборки, приложение, Gateway, CLI-агенты и генераторы не запускались. Компиляция и прохождение тестов не подтверждены.** Python использовался только для редактирования текстовых файлов; код проекта и schema generators не исполнялись. Разрешение на любые исполняющие проверки этим самоаудитом не предоставляется.

## Ограничения и внешняя приёмка

- Требуются внешнее ревью и отдельное разрешение пользователя перед запуском любых тестов/компиляции. Включая dependency diff, эта ветка не имеет подтверждённого build/test результата от данной работы.
- Набор операций зависит от текущих существующих grants, explicit selections, source lifecycle и backend capabilities. Новых прав/маршрутов/поисковых режимов нет.
- Search остаётся зависимым от существующего индекса и серверной retrieval policy; incomplete/approximate/index_unavailable не трактуются как отсутствие совпадений.
- Фактические provider процессы и реальная работа native/shadow homes здесь не проверялись запуском: написанное покрытие использует recording fixtures и остаётся ненапущенным.
- Установленные skills в пользовательских CLI-каталогах вручную не менялись. Выполнена только подготовка репозиторного bundled package и штатного подключения.
