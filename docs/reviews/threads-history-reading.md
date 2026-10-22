# Передача инструментов чтения тредов на внешнее ревью

Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-history-reading`.
Ветка: `feat/threads-history-reading`.
Исходный commit локальной main: `adb446a542e4c0c24e2d011fe73b184c9af1fb29`.
Текущий commit реализации указан в финальном сообщении; полный diff:
`git diff adb446a542e4c0c24e2d011fe73b184c9af1fb29..HEAD`.
Основной checkout не изменялся. Merge, push и deploy не выполнялись.

## Реализация и контракты

Все три инструмента зарегистрированы в `THREADS_DOMAIN_TOOL_NAMES`, исключаются
из начальной native visibility и открываются доменом `threads`. В task domain и
core tools не добавлены. Native/API используют общие handlers; CLI получает
семейство `pioneer-thread-tools-v1` / `pioneer_threads` через существующую
проекцию фиксированного каталога turn. Вызов CLI проходит проверку persisted
binding, поколения и канонической схемы; каталог активного CLI-turn не расширяется.

Входные JSON-объекты строгие: неизвестные поля отвергаются. `limit` обязателен,
целое число 1..100. `cursor` — отсутствующий/null для первой страницы, иначе
непрозрачная строка `nextCursor` предыдущего ответа. Модель не задаёт пользователя
или workspace.

| Инструмент | Вход | Выход |
| --- | --- | --- |
| `threads_list` | `{limit, cursor?}` | `{threads: ThreadMetadata[], nextCursor: string|null, eof: boolean}` |
| `threads_history_read` | `{threadId: string, limit, cursor?}` | `{threadId: string, events: HistoryRecord[], nextCursor: string|null, eof: boolean}` |
| `threads_children_list` | `{threadId: string, recursive: boolean, limit, cursor?}` | `{threads: ChildMetadata[], nextCursor: string|null, eof: boolean}` |

Штатная runtime-оболочка может добавлять `truncated`, `tool_outcome` и прочие
существующие поля tool protocol. Новый history tool не обрезает записи молча.

`ThreadMetadata`:

- `threadId`, `title: string|null`, `titleTruncated: boolean`;
- `preview` — до 160 Unicode-символов; title до 256 с явным флагом сокращения;
- `createdAt`, `updatedAt` — Unix seconds;
- `visibility` — сохранённое `visible`/`hidden` sidebar visibility;
- `status` — сохранённое состояние; `archived` соответствует `status == closed`;
- `parentThreadId`, `rootThreadId` — только если сохранена lineage и отдельно
  разрешено раскрытие соответствующего источника.

Список включает hidden/internal и Closed, не использует UI/sidebar filters,
не гидратирует turns. Сервер сначала выбирает только ограниченную страницу ID,
авторизует источники и затем читает компактные метаданные.

`ChildMetadata` содержит `threadId`, `title`, `titleTruncated`, `depth`, `status`,
`visibility` и допустимый `parentThreadId`. Глубина относительна запрошенному
узлу. `recursive=false` читает прямые edges, `true` делает DFS по реальным
parent/child edges. Соседние ветви и предки не включаются.

`HistoryRecord` — сохранённая запись одного из типов:

- `kind: turn`: `eventId`, `turnId`, `createdAt`, `content` с текущим сохранённым
  status, author, origin, messageRevision/messageDeleted и error;
- `kind: input`: тот же envelope, `content: {input: UserInput, author}`;
- `kind: item`: envelope с `itemId`; разрешённый канонический TurnItem и автор,
  либо компактная проекция tool item с аргументами и ссылками на результаты;
- `kind: childReference`: `eventId`, `createdAt`, допустимый `sourceTurnId`,
  `childReference: {threadId, parentThreadId, title, status, originKind}`.

`createdAt` истории — сохранённый timestamp текстового DB-представления,
используемый каноническим порядком. `eventId` стабилен, состоит из kind и
сохранённого row ID. User input использует immutable author turn; ответы,
commentary и tool records используют существующую agent-author projection.
Скрытые внутренние system events отсекаются существующим классификатором.
Task item отдаёт состояние без сырых внутренних receipts; child references
выдаются отдельными авторизованными записями lineage.

Большая запись превращается в envelope с `fragment`:
`{encoding:"json", version:sha256, offset, nextOffset, eof, text}`.
Offsets — UTF-8 byte offsets в сериализованном JSON исходной записи. Нужно
склеить `text` по offsets одной версии до `fragment.eof=true`, затем разобрать
JSON. Верхний `eof` относится к истории, а не к отдельному фрагменту.
Фрагменты child reference дополнительно несут минимальную структурированную
ссылку на ребёнка. Записи чтения другого источника и их фрагменты сохраняют
`sourceThreadReference`, чтобы проверять права и при повторном чтении saved result. Аргументы и сообщения сохраняются полностью через продолжение.
Если источник известного observation tool уже недоступен, его аргументы и
результаты не раскрываются: остаётся `sourceUnavailable:true` без source ID.

## Порядок, курсоры и размер

Список: `thread.id ASC`, exclusive keyset. Новая вставка не сдвигает старые
позиции, как при offset. Список не является глобальным snapshot workspace:
новые ID до пройденной границы видны в новом обходе.

История: `(created_at, kind, id) DESC`, от новых к старым внутри каждой страницы
и между страницами. Первая страница атомарным metadata SELECT фиксирует insertion
watermarks четырёх канонических источников: turn, turn_input, turn_item и lineage.
Поэтому новые записи, включая вставки с тем же timestamp и меньшим ID, не
попадают внутрь уже начатого обхода. Перечитываются версии сохранённых inputs/items
и message revisions замороженного набора. Изменение/удаление делает cursor stale;
большой фрагмент дополнительно защищён digest исходной разрешённой записи.
Ни транзакция, ни read snapshot между вызовами не удерживаются.

Дети: DFS, дети каждого узла сортируются по уникальному child ID ASC. Сервер
восстанавливает только путь предков текущего узла; курсор хранит текущий узел,
exclusive child position и hash пути. Память O(depth), размер cursor не растёт
с деревом. Reparenting текущего пути даёт stale error; циклы пропускаются при
спуске/обнаруживаются при восстановлении. Нет общего лимита 128 и fixed depth.

Все курсоры зашифрованы и аутентифицированы ChaCha20-Poly1305 с operation/user/
workspace/resource/recursive/limit в associated data. Это необходимо, поскольку
позиция фильтрации может содержать недоступный candidate ID. Добавлена прямая
зависимость gateway на уже существующий в Cargo.lock `ring 0.17.14`; новые
миграции, индексы и постоянные таблицы не добавлялись.

Вызов списка просматривает максимум 200 candidate ID, истории — 100 metadata
records, детей — 200 шагов. Пустая страница с `eof=false` законна: нужно продолжить.
Страницы дополнительно ограничены сериализованными байтами и token budget,
с резервом для cursor framing; наружный handler проверяет 32768 bytes/4096 tokens.
`eof=true` и `nextCursor=null` явно завершают доступный сохранённый набор.
Ошибки invalid/scope-mismatched cursor, stale cursor и unavailable/access-denied
различаются. Сырые SQL errors/IDs наружу не передаются.

## Авторизация и tool outputs

Каждый вызов перепроверяет execution lease, неизменяемый grant manifest,
provider authority, текущую Agent role и её сохранённый action ceiling.
Пользователь берётся из server-owned initiating principal execution envelope
и текущей записи principal. Сотрудник, предоставляющий continuation authority
общему execution lease, не подменяет инициирующего пользователя.

Каждый source проходит `AuthorizationResolver::authorize_thread` и при
необходимости `authorize_internal_thread_via_root`. Internal source требует
реальной lineage, авторизованного корня и существующего creator execution
context. Скрытость sidebar не является ACL. Для чужих внутренних детей
пересекается также Agent `ChildObserve`; собственный execution thread остаётся
допустимым источником при существующем полномочии чтения.

Для чтения других доступных бесед введён отдельный
`thread_observe_workspace`: он допускается при admission инициатора, а не
добавляется всем shared-capsule grants. Текущие пользовательские ACL, role,
Agent ceiling и immutable execution grant должны разрешать наблюдение.
Старые или ограниченные grants не расширяются правами текущего пользователя.
Этот action не создаёт write routes и не даёт MessageCreate/AgentTurnStart/TaskCreate.

Tool-result references: точные `workspace_id`, `thread_id`, `turn_id`, `item_id`.
Та же ссылка с `output_log:true` открывает сохранённый лог существующим
`threads_tools_result_read`. Новый reader outputs не создавался.
Оба инструмента используют один source authorization helper.
Удалённый turn не отдаёт прежние results/logs. Когда отдельный полный result
не сохранялся, existing reader может постранично вернуть сохранённый typed item
с `kind:saved_item, fullResultSaved:false`, без recovery internals.

Повторное чтение сохранённых результатов новых observation tools проверяет
все их источники; cached result-reader chains проходят проверку исходных точных
references с защитой циклов. Если хотя бы один источник больше недоступен,
сохранённый результат скрывается целиком; можно повторить живой list/history call.
Так старый byte cursor или кэш инструмента не заменяет текущую авторизацию.

Новые read paths не вызывают `thread/read`, mark_read, subscribe, setters,
Task creation или модели. Canonical turn/input/item используются независимо
от context compaction; checkpoint summary не заменяет сохранённую переписку.

## Пример последовательности

Здесь значения ID и cursor — placeholders, вызовы не выполнялись.

```json
{"tool":"threads_list","arguments":{"limit":20}}
{"tool":"threads_history_read","arguments":{"threadId":"PARENT_ID","limit":20}}
{"tool":"threads_history_read","arguments":{"threadId":"CHILD_ID_FROM_REFERENCE","limit":20}}
{"tool":"threads_history_read","arguments":{"threadId":"PARENT_ID","limit":20,"cursor":"PARENT_NEXT_CURSOR"}}
```

По необходимости:

```json
{"tool":"threads_children_list","arguments":{"threadId":"PARENT_ID","recursive":true,"limit":20}}
{"tool":"threads_tools_result_read","arguments":{"workspace_id":"CONTEXT_WORKSPACE","thread_id":"PARENT_ID","turn_id":"TURN_ID","item_id":"ITEM_ID","output_log":true}}
```

## Самоаудит, тесты и выполненные проверки

Исправлены в ходе самостоятельного ревью:

- подмена initiator полномочиями lease collaborator;
- обход ограниченного ChildObserve и отсутствие отдельного observation grant;
- раскрытие candidate IDs через открытый cursor;
- timestamp-only snapshot, пропускающий новые записи с равным временем;
- ошибочное авторство агентских ответов;
- byte-only budget и отсутствие фрагментации аргументов/сообщений;
- неработающие ссылки на legacy/native output и чтение deleted tool sources;
- повторное раскрытие закрытых источников через сохранённые observation results;
- устаревшее ожидание числа CLI bindings после добавления семейства Threads;
- подавление ошибок авторизации ребёнка как будто это недоступная запись: теперь
  ошибка прерывает страницу, не продвигая выданный агенту cursor.

Написаны и статически просмотрены, но НЕ ЗАПУСКАЛИСЬ:

- `crud/src/tests/thread_read_pages.rs`: hidden/Closed, workspace isolation,
  equal timestamps, keyset, 151 ребёнок, intermediate-parent isolation;
- `crud/src/tests/saved_history_read.rs`: tie order, same-time append вне snapshot,
  independent thread, edit invalidation, tombstone без старого текста;
- `gateway/src/message/thread_read_tools/tests.rs`: два пользователя, отсутствие
  закрытых names/previews/references, cursor scope, revoke между страницами,
  известный ID, restricted capsule, internal child через реальный root context,
  ChildObserve, несколько уровней/142 потомка/cycle/sibling exclusion,
  независимость parent cursor, Unicode fragments/edit/delete, отсутствие
  записи read cursor, legacy results/logs, cached observation ACL;
- crypto cursor и exact tool arguments/result references;
- расширены существующие visibility/domain/permission и native catalog tests;
- расширен существующий CLI contract test для Codex/Claude/OpenCode вызовами
  list/history через frozen binding;
- расширен существующий checkpoint/cleanup сценарий чтением original final и
  commentary новым history helper после реального compaction apply.

Действительно выполнялись: чтение/rg, обычные Git status/worktree/branch/diff/add/
commit операции, редактирование и `cargo fmt` для pioneer-gateway, pioneer-crud,
pioneer-protocol, pioneer-tools. `git diff --check` и проверка staged diff выполнены.
Весь diff от исходного commit, включая новые файлы, прочитан самостоятельно.

ТЕСТЫ, cargo check, cargo build, cargo clippy, cargo run, nextest и другие
сборки/проверочные запуски приложения, серверов и CLI НЕ ВЫПОЛНЯЛИСЬ.
Нельзя утверждать, что реализация компилируется или тесты проходят.
Генераторы схем не запускались; tool schemas поддержаны вручную, protocol inputs
добавлены обычными serde/JsonSchema типами.

Для implementation delegation использованы переданные skills
`/Users/alexander/.codex/skills/pioneer-system-subagents/SKILL.md` и
`/Users/alexander/.codex/skills/pioneer-system-tasks/SKILL.md`.
Storage/catalog результаты просмотрены и приняты. History candidate был
просмотрен, запрос revision вернул task_operation_failed; дочерняя задача
подтверждённо отменена, дальнейшие исправления сделаны родителем самостоятельно.
Test subagent завершён, его test code просмотрен родителем. Незавершённых
дочерних задач реализации не оставлено. Независимое внешнее ревью ещё не проведено.

## Файлы и фокус внешнего ревью

Основные production изменения:
`tools/{domain,visibility,permissions}`, `protocol/thread_read.rs`,
`crud/repositories/{thread,thread_lineage,thread_history_read}.rs`, CRUD wrappers,
`gateway/message/{thread_read_tools,thread_read_cursor,thread_history_read,thread_tool_results}.rs`,
`gateway/authorization/{domain,execution,lease,role_registry,service,catalog}.rs`,
`gateway/{turn_mcp/native_tools,mcp_service,message/turn_handlers}.rs`.

Известные границы: не восстанавливаются никогда не сохранённые данные;
legacy execution без нового observation grant остаётся в своей капсуле;
новые CLI callables появляются при построении нового fixed catalog;
сохранённый составной observation result отказывается раскрываться целиком при
потере прав хотя бы на один его источник. В обходе детей память и восстановление
пути зависят от глубины, не от размера всего дерева. Проверка history revisions
читает scoped metadata aggregates замороженного треда, не загружая все payloads.

Внешнему reviewer особенно проверить совместимость immutable grants со всеми
admission/recovery путями, SQL watermark/revision semantics, safe TurnItem
projection и source authorization outputs. После полной внешней приёмки нужно
отдельное разрешение пользователя на compilation/tests; само это handoff его
не предоставляет.
