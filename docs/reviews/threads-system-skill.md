# Threads system skill: передача на внешнее ревью

## База и изоляция

- Исходный main: `43b32ae8cfe69ff1a54f2035a59f23eb213eabdf` (Merge pull request #174, Threads search).
- `git fetch origin main` при подготовке подтвердил совпадение локального main и origin/main. Фактическая рабочая база — этот же commit; исторические реализации не переносились.
- Ветка: `feat/threads-system-skill`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-system-skill`.
- Имя ветки и путь были свободны. Основной checkout и соседние worktree не редактировались. Подзадачи и CLI-агенты не запускались.
- Прочитан корневой AGENTS.md; более узких AGENTS.md в изменяемых каталогах нет. Правила запуска проверок из задания имеют приоритет над обычной рекомендацией AGENTS.md запускать тесты.

## Карта трёх дополнений

| Требование | Файлы и результат |
| --- | --- |
| Отдельный bundled skill | `resources/skills/bundled-system-skills.toml`; `resources/skills/pioneer/threads/SKILL.md`; `references/tool-schemas.md`, `references/workflows.md`, `references/troubleshooting.md`. Отдельный стабильный SkillId `Th7kE4qP9mNs2vX6aR8cZ`, скрытый required skill `system:pioneer/threads`. |
| CLI skill/capabilities | `crates/gateway/src/cli_runtime/skills.rs`, `context.rs`; `message/skills/catalog.rs`, `message/turn_handlers.rs`; `turn_mcp/native_tools.rs`; `crates/promt/src/cli_runtime_context.rs`. Whitelist alias `pioneer-system-threads`, отдельная автоматическая выборка и окончательное удаление implicit attachment при отсутствии Threads operations, отдельный список операций. |
| Native/CLI policy block | `crates/promt/src/content.rs`, `bundle.rs`, `section.rs`, `compile/compiler.rs`, `instruction_delivery.rs`; `crates/agent/src/chat/mod.rs`; `crates/gateway/src/message/artifact_tools.rs`. Независимые `include_threads_policy`, `ThreadsPolicy` / `threads_policy`, доверенная доставка и проверка исполнимости загрузки skill. |

Тестовые изменения находятся в `crates/agent/src/chat/skills.rs`, тестовых участках `gateway/src/cli_runtime/skills.rs`, `gateway/src/system_skills.rs`, `gateway/src/message/tests.rs`, `gateway/src/message/tests/cli_task_tools.rs`, `promt/src/cli_runtime_context.rs`, `promt/src/compile/compiler.rs`, `promt/tests/compile_snapshots.rs` и тестовых callers в `agent/src/chat/mod.rs`. Этот отчёт — дополнительный файл передачи.

## Проверенные зависимости и контракты

Источники истины: `crates/tools/src/domain.rs`; `crates/protocol/src/agent_tools.rs`, `agent_launch.rs`, `turn.rs`, `thread_read.rs`, `thread_search.rs`, `thread_tool_result.rs`; общие handlers в `gateway/src/message/thread_read_tools.rs`, `thread_history_read.rs`, `thread_search_tools.rs`, `thread_tool_results.rs`, `agent_action_tools.rs`.

В main присутствуют все десять канонических инструментов:

- `threads_tools_result_read`, `threads_list`, `threads_history_read`, `threads_children_list`, `threads_search`;
- `threads_start_options`, `threads_identity_models_list`;
- `thread_message_send`, `thread_create`, `threads_turn_start`.

Определения, схемы, transport families и mutation handlers не заменены. `threads_start_options` остаётся единственной регистрацией в `NativeToolFamily::Tasks`; принадлежность к домену Threads не создаёт вторую регистрацию или binding. Новый предикат Threads operations проверяет каноническое определение и исключает общий options tool как самостоятельное основание подключения skill/policy. Прежняя опечатка имени identity/models и alias для неё не добавлялись.

Skill различает создание, сообщение и запуск; использует `optionId`, `targetOptionId`, массив `AgentAuthoredInput` и реальные `launch`-контракты. Создание не обещает result.threadId/title/sidebar payload. Для видимого треда описана серверная семантика delegated root. Наследование launch не требует identity/models lookup, но сохраняет обязательность targetOptionId. Явные provider/model/identity/reasoning берутся из каталога; `useDefaultReasoning` и legacy null описаны отдельно.

Пагинация поиска, истории, крупных записей и сохранённых результатов не заменена preview. Поиск сохраняет существующие страницы до 100 фрагментов и 2 MiB, продолжение выдачи и точную сборку pageFragment. Это не ограничение корпуса. Документация различает строковые cursors и snake_case result reader со структурированным `next`; неполное покрытие индекса не приравнивается к отсутствию истории. Новых лимитов, усечений, режимов поиска, embeddings API или механизмов идемпотентности нет.

## Общие механизмы и матрица доступности

| Путь | Инструменты и фильтрация | Skill и инструкции |
| --- | --- | --- |
| Native, домен ещё скрыт | Учитываются зарегистрированные, исполнимые capabilities через индекс router и доступный request_tools. Скрытые инструменты не переводятся в постоянную видимость. | Active/available system skill, read_skill и точная доставленная Internal Skill Reference обязательны. |
| Native, инструменты уже открыты / recovery | Проверяются текущая видимость либо доступность открытия домена, allowed/denied tools и наличие read_skill. Readers регистрируются только после успешной проверки существующей read authority. Action schemas и общая Task options регистрация сохраняют собственные действующие capability gates. | Тот же отдельный флаг передаётся во все пути повторной компиляции; в запросах без инструментов блок выключен. |
| Codex CLI | Канонические definitions проходят существующую проекцию, schema preflight, frozen binding и MCP transport. | Экспортированный alias и отдельный Threads operations list; штатная квалификация `mcp__pioneer__…`. |
| Claude CLI | Тот же фиксированный разрешённый каталог, штатные схемы, native item correlation и managed skill integration. | Тот же alias и отдельные инструкции; штатная квалификация `mcp__pioneer__…`, без wildcard-разрешений. |
| OpenCode CLI | Тот же общий каталог и dispatch, существующие ограничения проекции и native correlation. | Тот же alias; штатная квалификация `pioneer_…`. |
| Только threads_start_options | Единственный общий binding сохраняется. Самостоятельной Threads operation не считается. | Не включает Threads skill/policy и не становится новым основанием для Tasks/Subagents. |
| Операция исключена окончательной проекцией | Не добавляется обратно из skill или при recovery. Явные запреты и ограниченный allowed set исключают соответствующие Threads schemas, включая общий threads_start_options; его каноническая регистрация не меняется. | Implicit Threads attachment/install plan удаляется отдельно от Tasks/Subagents; policy требует одновременно final operations и фактический exported skill. Explicit выбор skill сохраняет обычную семантику, но не даёт операций. |

Использованы существующие bundling/catalog/hash/materialization, разрешение skills, install plans, turn bindings, schema fingerprints, provider callable allocation, session identity, managed receipts и native/shadow homes. OUT_DIR не редактировался.

Dispatch и защита результата остаются в существующих `turn_mcp/projection.rs`, `invoker.rs`, `mcp_service.rs` и `message/agent_action_tools.rs`: frozen identity → MCP invocation → текущая execution lease → initiating-user/workspace/thread grants → общая permission evaluation → shared handler → Agent action ledger/outbox и replay. Эти файлы бизнес-логики dispatch не требовали изменений. Нет переноса Task heartbeat, wait/review protocol или Task error envelope на Threads.

## Условия native policy и доставка

`native_threads_policy_available` проверяет одновременно:

1. Наличие Threads operation, отличной от общего options tool, с разрешённым именем и доступной регистрацией. Она может оставаться скрытой, если доступен request_tools.
2. Active/available skill с source kind system, owner pioneer, slug threads и catalog-hide.
3. Реальную видимость read_skill в окончательном плане и разрешение этого имени.
4. Наличие соответствующей записи в read_skill index и точной строки `- pioneer/threads: skill:…` (с каноническими Markdown backticks) в доставляемом Internal Skill References.

Проверяется фактически построенный skills prompt после возможного fallback rebuild. Блок не синтезирует ссылку для агента. Компилятор доставляет уже построенный SkillsRuntimePrompt без дополнительного усечения. Новый policy section использует существующие trust/delivery conventions. Первоначальная компиляция, retries, дальнейшие rounds и восстановленное execution window используют тот же контракт; запросы без инструментов не требуют загрузить недоступный skill.

## Export/install/update/restore/legacy

Whitelist дополнен только `system:pioneer/threads → pioneer-system-threads`; произвольные required system skills по-прежнему не экспортируются. Canonical frontmatter name остаётся `threads`; только временный export получает alias. Весь пакет с references копируется и хешируется общим installer.

Alias защищён при планировании, непосредственной установке и восстановлении. Установщик сохраняет прежние проверки receipt ownership, локальных изменений, symlinks и файлов вне hash. Наличие чужих файлов не разрешает автоматическое присвоение каталога.

Threads alias добавлен к protected destinations legacy conversion. Дополнительно проверяется уже сопоставленная foreign claim: установка другого explicit skill не может сначала записать сконвертированный receipt, присвоивший Threads alias чужому skill. Проверка выполняется до записи receipt и пользовательских файлов. Успешные managed update, восстановление после копирования до commit receipt, restore и conversion используют прежние общие механизмы. Session identity получает выбранный экспорт и его hash без отдельной логики Threads в менеджерах провайдеров.

## Tasks/Subagents

Пакеты `resources/skills/pioneer/tasks/` и `resources/skills/pioneer/subagents/`, включая references, побайтно не изменены. Native и CLI константы SUBAGENTS_POLICY_PROMPT / TASKS_POLICY_PROMPT и их CLI варианты побайтно совпадают с базой. Старый include_task_orchestration_policy и условия выбора/удаления implicit Tasks/Subagents сохранены.

Новые поля в общих структурах и дополнительные аргументы внесены последовательно во все callers, включая тестовые. Threads operations не добавлены в Task operations. Тестовые списки ожидаемых attachments обновлены на третий самостоятельный пакет; существующие Tasks/Subagents assertions сохранены.

## Написанные и расширенные тесты — НЕ ЗАПУСКАЛИСЬ

- `system_skills`: расширены materialization/catalog/read_skill/reference assertions и точная таблица стабильных manifest identities.
- `cli_required_task_skills_are_supported_without_enabling_other_required_skills`: Threads включён в ограниченный whitelist, отказ для memory сохранён.
- `automatic_orchestration_exports_preserve_foreign_names_and_restore_exact_bindings`: все три пакета, references, alias, current install, restore, session reuse и успешная legacy conversion для поддерживаемых провайдеров.
- `claude_explicit_and_automatic_skill_projection_restores_exactly`: обновлён самостоятельный третий attachment с сохранением application order.
- `orchestration_export_update_recovers_after_copy_without_receipt_commit`: расширен на Threads, включая восстановление после копирования без обновлённого receipt.
- `orchestration_exports_refuse_foreign_destinations_receipts_and_local_edits` и `orchestration_exports_never_replace_or_follow_foreign_symlinks`: матрицы расширены на Threads, сохранение файлов и receipt проверяется до изменений.
- `threads_reserved_alias_rejects_user_plans_and_mapped_legacy_claims_before_writes`: plan/install/restore и сопоставленная foreign legacy claim.
- `final_threads_catalog_reconciles_implicit_attachments_without_changing_task_selection`: отсутствие операций, только общий options, реальная read operation и explicit skill selection.
- `native_threads_policy_requires_readable_hidden_skill_and_delivered_exact_reference`: настоящий bundled пакет, скрытая capability до request_tools, отрицательные варианты skill/reader/reference/index/discovery/permissions и повторная компиляция с continuation.
- `cli_threads_policy_requires_operations_and_exported_skill_independently_of_tasks`: независимый policy, обязательность alias и final operations, три CLI runtime kinds, повторная компиляция.
- `cli_threads_actions_use_facade_shared_handlers_routes_and_replay_for_all_providers`: реальный MCP façade с production correlators и invoker для Codex/Claude/OpenCode, read/create/send/start, отсутствие второго options binding, provider names/schema identities, явный deny, повторная доставка mutations через новый façade без JSON idempotencyKey, проверка единственности созданного треда/сообщения и сохранённых launch identity/model/reasoning в outbox.
- Существующие CLI admission/capability/recovery fixtures и compile snapshot callers обновлены для нового attachment/аргумента без запуска snapshots или генераторов.

## Самоаудит

1. Skill/manifest: сверены все десять canonical names, payloads, result casing, варианты создания, launch inheritance, reasoning и три независимых вида продолжения. Поиск описан как поиск доступного корпуса с пагинацией результатов.
2. Lifecycle: проверены пути планирования, install/update/restore, fingerprint/ownership и legacy conversion. Найдена и закрыта mapped foreign alias claim до записи receipt. Legacy fixture приведён к реальному qualified skill label.
3. Projection/dispatch: сохранены transport families, единственный shared options binding, canonical definitions и существующая replay-цепочка. Добавлено исключение явно запрещённых Threads readers и общего options tool из CLI-каталога (исполнение уже подчинялось этим запретам); native регистрация readers учитывает реальную read authority. Дополнительная зависимость mutation schemas от read authority исключена при повторной сверке handlers; их действующие gates сохранены.
4. Prompt: проверены отдельный section/manifest id, trust, delivered reference и все места компиляции. Исправлен пропущенный аргумент тестового caller; статически сверена арность всех native compilation callers.
5. Итоговый diff: повторно просмотрены production изменения, тестовые фикстуры, пакеты и связанные неизменённые callers. Отдельно сверена побайтная неизменность Tasks/Subagents skills и четырёх policy constants. Проверены отсутствие изменений dependencies/lockfiles/backend, новых search limits и новых mutation API.

Самоаудит является статическим ревью, а не подтверждением исполнения тестов или компиляции.

## Фактически выполненные проверки и ограничения

Выполнены чтение/поиск исходников, просмотр Git diff и worktree state, статический анализ callers/контрактов, `cargo fmt --all`, `cargo fmt --all --check`, `git diff --check`. Не запускались project scripts, hooks, schema generators, IDE checks или внешние провайдеры. Commit выполняется с `core.hooksPath=/dev/null`.

Тесты, cargo check, компиляция, сборки, приложение, Gateway, CLI-агенты и генераторы не запускались. Компиляция и прохождение тестов не подтверждены. Форматирование не заменяет эти проверки. Старые восстановленные execution сохраняют свой frozen catalog и attachments; новый skill не добавляет им инструменты задним числом. Доступность моделей, approximate retrieval и ограничения фактически сохранённых результатов следуют существующим контрактам main.

Merge, push, release и deploy не выполнялись. Это передача на внешнее ревью; окончательная приёмка и разрешение выполнять код проекта остаются отдельным шагом пользователя.
