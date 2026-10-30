# Threads list: передача на внешнее ревью

Дата: 2026-10-29. Это подготовка к внешнему ревью, не окончательная приёмка.

## База и изоляция

- Начальная база: `7f226499d3fa8beac5dd15b3d241040e7a99016e`. После первоначального `git fetch origin main` локальный `main` и `origin/main` совпадали с ней.
- Итоговая база: `dc90743137af9d220a3594c8fe6190bdbde7d999`. Main продвинулся во время работы (принятый PR #216, task-child discovery); своя ветка перебазирована на этот main с отключёнными hooks. Экспериментальные Threads-ветки не интегрировались.
- Ветка: `fix/threads-list-pagination`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-list-pagination`.
- Имена были свободны. Основной checkout изначально чист; соседние worktree и экспериментальные Threads-ветки не изменялись и не интегрировались.
- Прочитаны корневой `AGENTS.md`, CRUD, migration и Entity rules. Новых Entity нет: миграция добавляет только индекс, генерация не требуется и не выполнялась.

## Изменение поведения

Существующий `threads_list` использует `created_at DESC, id DESC`. Свежесть определяется созданием, а не `updated_at`. CRUD возвращает typed projection `ThreadReadPosition { created_at, id }` через существующий Entity/ORM-путь и scoped connection. `created_at` сохраняется как полный исходный SQLite timestamp text: публичный `createdAt` в секундах не используется; парсинг/переформатирование не меняет точность и строковую границу сравнения.

Продолжение выбирает `created_at < position.created_at OR (created_at = position.created_at AND id < position.id)`, с дополнительной индексируемой верхней границей `created_at <= position.created_at`. Ключ продвигается через недоступных кандидатов. После удаления завершённого кандидата lookup этой записи не нужен. Новые записи перед позицией видны при новом чтении первой страницы; существующая цепочка не возвращается назад.

Полный workspace revision удалён вместе с обоими вызовами проверки. Нет замороженного каталога, серверной сессии, новой таблицы или фонового обхода. Сохраняются страницы 1–100 карточек, quantum 200 кандидатов и прежний бюджет ответа. Пустые нетерминальные страницы сохраняют nextCursor; EOF достижим через продолжения, без общего лимита тредов, кандидатов или страниц.

Новая миграция `m20261029_000001_thread_read_creation_order` добавляет только `idx_thread_workspace_created_id(workspace_id, created_at, id)`. Обратный проход по индексу соответствует убывающему порядку внутри workspace. Установка требует одного построения индекса по существующим тредам; down удаляет только этот индекс. Старые миграции не переписаны. План запроса и стоимость установки не измерялись, EXPLAIN не выполнялся.

## Курсор, доступ и фрагменты

Новый формат отделён authenticated operation scope `list-created-desc-v1`; list scope также включает конкретный `current_turn_id`. Сохраняются AEAD-конфиденциальность и аутентичность, привязки к principal, workspace, root/current thread, разрешённому scope, операции и limit. Общий codec не изменён. Старые ID-ascending/revision курсоры, подмена и несовпадение scope отклоняются с безопасным предложением начать чтение заново. Молчаливого перехода на первую страницу нет.

Текущая авторизация проверяется при каждом запросе, для каждого раскрываемого кандидата и повторно перед публикацией результата handler. Доступные внутренние и архивные треды включаются прежними правилами; недоступные пропускаются. Авторизация не выводится из наличия курсора.

Если карточка фрагментирована, cursor содержит её ключ и прежний `MetadataFragment` с thread ID, digest и byte offset. Следующий запрос читает именно эту карточку, независимо от новых записей вокруг позиции, проверяет доступ и digest текущего сериализованного содержимого. Изменение карточки возвращает stale-source ошибку, исчезновение/отказ доступа — объединённую unavailable/denied ошибку. Завершение фрагмента сохраняет позицию; последующее продолжение переходит к следующим кандидатам (возможна дополнительная пустая страница до EOF). Отпечаток других тредов не вычисляется.

Самостоятельные scope, порядок и правила snapshot/revision для history и children не изменены. Create/send/start, search, lifecycle CLI, ledger, cancellation и terminal reconciliation не менялись.

## Публичные ошибки native и CLI

Native handler сохраняет прежний безопасный словарь. Узкая ветка в `mcp_service.rs` преобразует ошибки исполнения только трёх read tools native Threads в MCP `result` с `isError:true`, одинаковым безопасным `error` в text/structuredContent и `pioneer.reasonCode`:

| Ситуация | Публичная причина | reasonCode |
| --- | --- | --- |
| Старый, подменённый, несовместимый cursor | `invalid or incompatible cursor; restart reading with the same resource and parameters` | `read_cursor_incompatible` |
| Изменилась продолжаемая карточка | `cursor is stale; restart reading from the first page` | `read_source_changed` |
| Неверные параметры чтения | `invalid read parameters` / `limit must be between 1 and 100` | `read_invalid_parameters` |
| Источник недоступен или доступ запрещён | `source unavailable or access denied` | `read_source_unavailable` |

CLI адаптер принимает только точные сообщения из этого словаря; остальные диагностики заменяются безопасным unavailable. Произвольный `ToolError.to_string()`, SQL, пути и сведения о запрещённых объектах не публикуются. Cancellation остаётся на прежнем пути. Ошибки JSON-RPC/admission/schema не переименовываются в бизнес-ошибки; facade production-код не менялся.

Первый исторический сбой не объявляется следствием revision: точный старый cursor известен, подробная причина того вызова утрачена. Исправлены подтверждённые дефекты реализации.

## Написанные и обновлённые регрессии — НЕ ЗАПУЩЕНЫ

- `crates/crud/src/tests/thread_read_pages.rs`: существующая observation regression теперь проверяет ID против хронологии, равные даты через границу страницы, различие в одну наносекунду, mutable activity, удаление последнего кандидата, вставку перед позицией, workspace isolation и размер страницы. Отдельная migration regression описывает fresh install, upgrade, состав индекса, rollback/reapply и сохранность тредов.
- `crates/gateway/src/message/thread_read_tools/tests.rs`: обновлено прежнее ожидание stale после постороннего редактирования; добавлен полный обход после 205 недоступных кандидатов с пустой нетерминальной страницей, tie/fractional boundaries, посторонней активностью, вставкой, удалением и отзывом доступа. Добавлены legacy/tamper/shape/scope отрицательные случаи, включая другой turn. Добавлена проверка точного источника фрагмента при вставке перед ним, постороннем редактировании, изменении, исчезновении и потере доступа. Существующая сборка большой карточки до полного JSON сохранена.
- `crates/gateway/src/message/tests/cli_task_tools.rs`: `cli_threads_read_errors_keep_safe_reasons_through_the_admitted_facade` использует существующие реальные admission/facade/correlation/Gateway-handler fixtures (без запуска внешнего процесса). Написаны проверки старого/неверного cursor, семантически неверных history params, unavailable history/children, изменения настоящего фрагмента списка, точных безопасных MCP payloads и отсутствия диагностик. Malformed JSON должен остаться JSON-RPC parse error. Новая error regression использует Codex facade; shared mapping одинаков для CLI-провайдеров.

Тесты не ищут строки исходников и не вычисляют ожидаемый список копией keyset-алгоритма. Отдельные CRUD и Gateway проверки защищают storage precision и disclosure/fragment/transport риски. Baseline failure, компилируемость и прохождение тестов не подтверждены.

## Статический самоаудит

Промежуточно проверены согласованность ORM order/predicate/index, точность и scope курсора, прогресс через пропуски, авторизация, фрагменты, native/CLI ошибки, документация и тесты. Полностью перечитан собственный diff и затронутые callers: CRUD method/export, list handler/metadata/codec, native-family materialization, Gateway mapping, CLI facade encoding и existing real facade fixture. Поиск callers не обнаружил оставшихся обращений к workspace listing revision или описаний старого ID-порядка в bundled Threads skill. Изменены только разделы bundled skill о списке, продолжении и ошибках; установленные skills не редактировались.

Вспомогательная MCP-регрессия была делегирована через `pioneer-system-subagents`, её полный результат и diff проверены и приняты родителем. Независимое статическое ревью нашло пропущенный `TransactionTrait` в новом migration-тесте и старое описание порядка в строке таблицы reference; оба замечания подтверждены чтением и исправлены. После исправлений родитель перечитал затронутый diff. Все ограничения на запуск распространялись на дочернюю работу. Других конкретных дефектов статическое ревью не выявило; исполнение, компиляция, фактический query plan и однородность исторических SQLite date representations не проверены. Предикат и позиция согласованы с буквальным SQL-порядком колонки `created_at`.

## Выполненные команды

Подготовка (основной checkout; только Git-состояние, чтение правил и создание отдельного worktree):

```sh
git status --short
git status --porcelain=v1
git worktree list
git branch --list main fix/threads-list-pagination
git rev-parse main
git remote -v
git log -1 --format='%H %s' main
git fetch origin main
git rev-parse main origin/main
git worktree add -b fix/threads-list-pagination /Users/alexander/Code/pioneer/pioneer/.worktrees/threads-list-pagination main
```

Чтение выполнялось через `pwd`, `cat`, `rg`/`rg --files`, `sed -n`, `head`, `tail`, `nl`, `git log` и `git diff` по перечисленным выше файлам, применимым AGENTS.md, Cargo manifests/CI и локальному исходному коду ORM/SQLite timestamp decoding. Эти команды не выполняли код проекта. Ключевые точные поиски/просмотры для статического аудита:

```sh
rg --files -g AGENTS.md -g '*CRUD*' -g '*migration*' -g '*MIGRATION*'
rg -n 'read_candidates|ThreadReadCandidate' crates/crud
rg -n 'thread_listing_revision|list_thread_read_candidates_page|immutable thread ID|Metadata changes invalidate|changed listing source|ordered by' crates resources/skills/pioneer/threads --glob '*.rs' --glob '*.md'
rg -n 'ThreadReadAuthority \{' crates/gateway/src
rg -n 'immutable.*thread.*ID|thread-ID order|thread_listing_revision|list-created-desc-v1|list_thread_read_candidates_page' crates/gateway crates/crud resources/skills/pioneer/threads
git diff -- crates/gateway/src/message/thread_read_tools.rs
git diff -- crates/gateway/src/message/thread_read_tools/tests.rs
git diff -- crates/gateway/src/message/tests/cli_task_tools.rs
git diff -- crates/crud/src/repositories/thread.rs crates/protocol/src/thread_read.rs crates/migration/src/lib.rs
cat crates/migration/src/m20261029_000001_thread_read_creation_order.rs
git diff --name-only
git diff --numstat
git diff --stat
git status --short
git rev-parse main origin/main HEAD
git branch --show-current
git log --oneline 7f226499d3fa8beac5dd15b3d241040e7a99016e..main
git diff --stat 7f226499d3fa8beac5dd15b3d241040e7a99016e..main
```

Проверки и форматирование (только task worktree):

```sh
cargo fmt --all --check
cargo fmt --all
cargo fmt --all --check
git diff --check
```

Первый fmt-check родителя и fmt-check дочернего автора завершились с кодом 1, показав форматирование нового diff. После `cargo fmt --all` повторный fmt-check и `git diff --check` завершились с кодом 0. Проверки повторяются после любых дальнейших изменений.

Дочерний автор дополнительно выполнил только чтение/поиск и следующие Git-команды:

```sh
git rev-parse HEAD
git hash-object crates/gateway/src/message/tests/cli_task_tools.rs
git branch --show-current
git diff --numstat -- crates/gateway/src/message/tests/cli_task_tools.rs
git diff --stat -- crates/gateway/src/message/tests/cli_task_tools.rs
git status --porcelain=v1
```

Фиксация и проверка передачи (hooks отключены, без push/PR/merge):

```sh
git add crates/crud/src/lib.rs crates/crud/src/repositories/thread.rs crates/crud/src/tests/thread_read_pages.rs crates/gateway/src/mcp_service.rs crates/gateway/src/message/thread_read_tools.rs crates/gateway/src/message/thread_read_tools/tests.rs crates/gateway/src/message/tests/cli_task_tools.rs crates/migration/src/lib.rs crates/migration/src/m20261029_000001_thread_read_creation_order.rs crates/protocol/src/thread_read.rs resources/skills/pioneer/threads/SKILL.md resources/skills/pioneer/threads/references/tool-schemas.md resources/skills/pioneer/threads/references/troubleshooting.md resources/skills/pioneer/threads/references/workflows.md docs/reviews/threads-list-pagination.md
git diff --cached --check
git -c core.hooksPath=/dev/null commit -m "Fix threads list creation-order pagination and safe read errors"
git -c core.hooksPath=/dev/null rebase dc90743137af9d220a3594c8fe6190bdbde7d999
git diff dc90743137af9d220a3594c8fe6190bdbde7d999..HEAD
git diff dc90743137af9d220a3594c8fe6190bdbde7d999..HEAD --check
git status --short
git log -1 --format='%H %s'
```

Тесты, cargo check, компиляция, сборки, приложение, Gateway, CLI-агенты, миграции и генераторы не запускались. Не выполнялись nextest, clippy, cargo run, EXPLAIN, benchmarks или сценарии воспроизведения. Из Cargo использовался только fmt. Компилируемость и прохождение тестов не подтверждены; разрешение на их запуск остаётся за внешней приёмкой пользователя.
