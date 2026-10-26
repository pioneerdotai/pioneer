# Threads tool access: статический самоаудит части 1

Первичный проход передан commit `0218ad63df5addef7da32ad923693bbe32f99359`.
Ниже сохранены его результаты; последующий раздел «Исправление P2» уточняет
пропущенный в нём дефект подготовки регрессии.

## Ревизии и состояние

- Ветка: `fix/threads-tool-access`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-tool-access`.
- Зафиксированная база: `bb85dcdf7080164c4dc6480d12410e5e38edade8`.
- Фактический начальный HEAD: `0361ef587bff0b33f2c38edf87d254e3c1769547`.
- На старте дерево было чистым. Обновлённая база от ревьюера не поступала.
- Итоговая ревизия — commit, содержащий этот отчёт; его полный hash передаётся
  отдельно в итоговом сообщении, чтобы избежать ссылки commit на самого себя.
- Интервал аудита: весь diff от указанной базы до итоговой ревизии, включая
  перенесённые изменения и правки этого прохода.
- Родитель будущего PR: исходная зафиксированная база `main`; текущий `main`
  не обновлялся. Кандидат `9d86bc3eb65cc58f0e67bf3022a545dfec0b14b6`
  не использовался как эталон корректности и не интегрировался.

## Scope и файлы

Production diff прочитан целиком:

- `crates/gateway/src/authorization/{domain,execution,lease,agent_runtime_adapter}.rs`;
- `crates/gateway/src/message/{agent_action_tools,task_handlers,turn_handlers}.rs`;
- `crates/gateway/src/task_tools/mod.rs`;
- `crates/gateway/src/turn_mcp/{native_tools,invoker}.rs`.

Прочитаны оба изменённых набора тестов:
`message/tests/cli_task_tools.rs`, `message/tests/thread_launch_choices.rs`,
а также перенесённый `threads-tool-access.prompt.md`. Дополнительно проверены
непосредственные определения/callers в `message/{mod,agent_runtime}.rs`,
authorization resolver/service/Agent action service, protocol Agent tool
catalog, permission/request_tools handlers и CRUD commit boundary.

В первичном проходе изменены только существующая регрессия в
`crates/gateway/src/message/tests/thread_launch_choices.rs` и этот отчёт.
Production-код остался побайтово равен начальному HEAD; его дополнительных
исправлений по результатам статического аудита не потребовалось.

## Найденные и исправленные проблемы покрытия

1. Member-регрессия проверяла создание при наличии сразу двух grants. Она не
   различала независимость private/workspace и ошибочное объединение этих прав.
   Теперь каждый вариант вызывается через существующий shared handler при
   наличии ровно одного immutable create-grant. `threads_start_options`
   должен вернуть только соответствующий вариант; ранее выданный option
   исключённого варианта должен завершиться `agent_action_not_allowed` без
   сохранённого outbox.
2. Положительная часть этой регрессии ограничивалась `status: accepted`.
   Теперь ответ сопоставляется с сохранёнными receipt/outbox, а ID из outbox —
   с существующим созданным тредом. ACK сам по себе не считается доказательством
   canonical acceptance.

Для моделируемых старых admissions восстанавливается также execution lease
через существующий `register_execution_lease`: его immutable action set должен
соответствовать persisted context. Перед проверкой реального отзыва workspace
membership возвращаются исходный context и lease, чтобы отказ не объяснялся
предыдущим искусственным ограничением grants. Изменение JSON здесь является
подготовкой изолированной тестовой модели, а не production-путём readmission.

Новых test-only production seams и новых сквозных lifecycle-тестов нет.
Эта проверка ловит утечку исключённого варианта options, принятие старого option
и ложный ACK без сохранённого результата. Доказательство выполнения до/после
изменений отсутствует: тесты запрещено запускать.

## Проверенные цепочки

| Цепочка | Статический результат |
| --- | --- |
| Обычный CLI catalog → frozen provider bindings | Удалено дополнительное требование ручного Threads allow-list. Существующий фильтр restricted-empty, explicit allow/deny остаётся перед проекцией grants. Далее catalog пересекается с Agent capabilities и фактическими shared handlers. |
| `thread_create` → private/workspace | `NativeToolFamily::actions` допускает любой из двух grants на общей транспортной границе; opaque option определяет конкретный audience. Handler повторно проверяет именно соответствующее действие. |
| Admission → текущие права | Create-grants добавляются опционально после обычного admission. Проверяются source `ThreadRead` и отдельная workspace authority. Отсутствие create-права само по себе не запрещает обычный turn. |
| Immutable grant → lease → current collaborator | Creation проецируется только для initiating principal. Другой collaborator, включая Superuser, не подставляется вместо него. Проверяются текущие source access и workspace create authority; immutable manifest не расширяется при revalidation. |
| Options → действие | Refresh пересекает envelope options с отдельной revalidation обоих create-actions. Перед новым commit shared handler снова получает authorization для конкретного audience. |
| Fresh Agent Task | Новый context сужает только create-grants по source `granted_actions`, до сериализации admission seed. Остальные Task grants и существующая политика сохраняются. |
| `task_resume` | Persisted admission возвращается с исходной строкой JSON до применения source ceiling. Ceiling применяется только при создании отсутствующего legacy admission. RPC caller передаёт `None`, agent tool caller — source action set. |
| Tasks/Subagents и native discovery | Изменения не затрагивают `request_tools`, domain discovery, policy blocks, Task visibility/control/review semantics и skills. `threads_start_options` остаётся единственным catalog entry семейства Tasks. |

Проверены обе стороны гонки permission change/commit: предварительные proofs
сужают options и действие, а существующий canonical CRUD commit в той же
транзакции проверяет точную policy generation, execution generation,
attempt generation, identity source и receipt/grant/route authority. Создание
треда и action/receipt/outbox атомарны; ошибка action commit откатывает тред.
Этот проход не переносил transaction boundaries и не менял scheduling class.

Replay просмотрен отдельно: handler сверяет execution, request fingerprint,
idempotency и наличие receipt/outbox; canonical commit имеет собственную
проверку уже принятого результата. Новые call IDs, routes, idempotency rules
или generation fences этим проходом не вводились.

## Callers и зависимости

- Все найденные callers `task_execution_readmission_seed` согласованы с новым
  аргументом: RPC resume и `TaskToolHandler::handle_resume`.
- Единственный внутренний caller
  `task_execution_admission_seed_for_existing_task` передаёт source ceiling.
- Определения `constrain_thread_creation_grants`,
  `restrict_thread_creation_options`, `NativeToolFamily::actions`,
  `register_execution_lease`, `mark_turn_blocked` и используемых CRUD helpers
  присутствуют в этом worktree. Проверка signatures выполнена чтением исходников,
  без type-checker.
- Shared fixtures сохранены для следующих частей. Их probe и member-вариант
  не требуют определения из будущих веток; исходные fixture callers сохранены.
- Independent-root lifecycle, dispatch/reconciliation и Stop здесь не
  реализованы. Их последующее ревью и интеграция остаются отдельными частями.
- Другие ветки/worktree не изменялись; merge, push, deploy и PR не выполнялись.

## Тесты

Расширен существующий
`member_creation_uses_both_workspace_variants_and_revocation_fences_the_shared_handler`.

Статически прочитаны перенесённые проверки:

- `cli_threads_permission_matrix_preserves_restricted_names_denies_and_missing_grants`;
- `task_resume_preserves_persisted_admission_but_constrains_a_new_legacy_seed`;
- `optional_creation_admission_uses_workspace_and_cannot_borrow_collaborator_rights`;
- изменения обычного CLI-каталога и provider facade/replay для Codex, Claude,
  OpenCode; положительный Task-child create; общие launch fixtures.

Применён `test-audit` для содержательности изменения теста. Инструкции
репозитория/skill о запуске тестов уступают прямому запрету пользователя.

## Реально выполненные команды

Все рабочие Git-команды и проверки выполнялись из указанного worktree.

```text
pwd
git status --short
git status --porcelain=v1
git branch --show-current
git rev-parse HEAD
git rev-parse --show-toplevel
git log --oneline -4
git diff --stat bb85dcdf7080164c4dc6480d12410e5e38edade8..HEAD
git diff --name-only bb85dcdf7080164c4dc6480d12410e5e38edade8..HEAD
git diff bb85dcdf..HEAD -- <перечисленные выше файлы, отдельными группами>
git diff bb85dcdf7080164c4dc6480d12410e5e38edade8 -- <production scope>
git diff HEAD -- crates/gateway/src/message/tests/thread_launch_choices.rs
git diff HEAD --name-only
git diff HEAD --numstat
git diff --numstat bb85dcdf..HEAD
git diff --exit-code HEAD -- . ':(exclude)crates/gateway/src/message/tests/thread_launch_choices.rs'
git diff --check
git diff bb85dcdf7080164c4dc6480d12410e5e38edade8 HEAD --check
cargo fmt --all --check
```

Для чтения использованы `cat`, `sed -n`, `rg -n`, `rg --files`, `head`, `tail`:
AGENTS.md в worktree и предках, явно загруженные skills, `test-audit` и его
references, Gateway Cargo.toml, CI и перечисленные исходники/callers.
Поиски двух предполагаемых файлов `authorization/policy.rs` и
`message/tool_visibility.rs` сообщили об отсутствии; реальные определения
найдены в service/resolver и tools visibility/request_tools. Это не результаты
проверки проекта. Правки выполнены через `apply_patch`.

Первый и повторный `cargo fmt --all --check` завершились успешно, как и
whitespace checks. Проверка diff относительно начального HEAD с исключением
изменённого test-only файла подтвердила отсутствие production-правок.

Команды финализации (результат commit и состояние дерева передаются отдельно):

```text
git add -- crates/gateway/src/message/tests/thread_launch_choices.rs docs/reviews/threads-domain-split/threads-tool-access.review.md
git diff --cached --check
git diff --cached --stat
git -c core.hooksPath=/dev/null commit -m "test(gateway): audit Threads tool access and creation grants"
git rev-parse HEAD
git status --short
git diff bb85dcdf7080164c4dc6480d12410e5e38edade8..HEAD --check
git show --stat --oneline HEAD
```

## Ограничения передачи

Это статическая передача части 1 на внешнее ревью, не приёмка кода.
Тесты, cargo check, компиляция, сборки, приложение, Gateway, тестовые CLI-агенты,
миграции, EXPLAIN, benchmarks и генераторы не запускались. Компилируемость и
прохождение тестов не подтверждены. Асинхронные гонки оценены по коду, без
экспериментального воспроизведения. Dev/prod-базы, службы, конфиги, установленные
skills и соседние worktree не менялись.

Внешняя приёмка всех четырёх частей и совместная интеграция ещё предстоят.
Даже после приёмки запуск проверок требует отдельного разрешения пользователя.

## Исправление P2: согласованный caller для Task readmission

Начальный HEAD повторного прохода:
`0218ad63df5addef7da32ad923693bbe32f99359`, ветка и worktree совпали с указанными
выше, дерево было чистым. База части остаётся
`bb85dcdf7080164c4dc6480d12410e5e38edade8`. Итоговый hash commit с этим
исправлением передаётся отдельно; обновления базы и интеграции не было.

Подтверждённый дефект находился в подготовке существующего теста
`task_resume_preserves_persisted_admission_but_constrains_a_new_legacy_seed`
в `crates/gateway/src/message/tests/cli_task_tools.rs`. Он удалял create-grants
из JSON уже запущенного sibling Task child. Setter меняет только Turn context,
а `authorization_fingerprint()` включает grant manifest. Execution graph
сохранял старый fingerprint, поэтому штатная загрузка child и последующая
MCP validation отклоняли context до dispatch/readmission. Первичный самоаудит
этого несоответствия не обнаружил. Дефект установлен статически, без запуска.

Исправлена только последовательность подготовки этого теста:

1. Проверяемый Task создаётся с исходным admission, его запись сохраняется
   для сравнения, Task переводится в Blocked прежним путём.
2. Оба create-grant удаляются из admission principal-authored корневого fixture.
   Существующий `task_create` через тот же MCP facade создаёт sibling уже после
   ограничения source ceiling.
3. Child sibling загружается через
   `load_turn_execution_authorization_context` до resume; добавлены проверки
   отсутствия обоих create-grants и совпадения capsule root.
4. `task_resume` вызывается через существующий `ThreadsFacade::call`. Сохранены
   проверки результата, выхода Task из Blocked и итогового admission:
   при `persisted=true` сравнивается вся исходная запись, включая неизменную
   строку `authorization_context_json`; при `persisted=false` новый legacy seed
   не содержит ни одного create-grant. После resume caller загружается повторно.

Статически прослежена цепочка:

- `start_task_cli_fixture` запускает principal-authored root через
  `process_request_for_connection`. В `verify_durable_turn_binding` такой root
  проверяется по principal/workspace/root lineage; к нему не применяется
  fingerprint-проверка ветки agent-authored Task child.
- `resolve_task_tool_action_scope` читает актуальный source manifest;
  `TaskToolAuthorizationScope::authorize_execution_intent` пересекает create-grants
  до сериализации нового seed. В `task_agent_executor` child context сначала
  выводится из Task admission, затем его fingerprint передаётся в
  `persist_task_agent_execution_graph`.
- `load_for_turn` проверяет child graph fingerprint и derivation от admission,
  затем проходит родительский root. Новый child context не переписывается.
  `GatewayTurnMcpInvoker::revalidate_execution_authorization` использует ту же
  штатную загрузку перед dispatch. Уникальный resume call проходит в
  `TaskToolHandler::handle_resume`, затем в `task_execution_readmission_seed`.
- Persisted branch helper возвращает исходный JSON до ограничения source
  ceiling; legacy branch применяет ceiling к новому context до сохранения.

Shared test helpers и их сигнатуры не менялись. Просмотрены их определения,
существующий соседний сценарий ограничения root admission в permission matrix,
обе ветви исправленного теста и весь diff повторного прохода. Production-код,
fingerprint guards, сохранённые fingerprints и Tasks/Subagents policies не
изменялись. Independent-root lifecycle, Stop, dispatch/recovery не затронуты.

Фактические проверки повторного прохода: чтение через `cat`, `sed -n`, `rg`;
`git branch --show-current`, `git rev-parse HEAD`, `git status --porcelain=v1`,
`git diff --check`, `git diff --exit-code HEAD` до правок и проверка diff с
исключением изменённого test-only файла после правки. Выполнен
`cargo fmt --all --check` без записи исходников; formatting и whitespace checks
завершились успешно. При финализации используются
`git diff --cached --check` и commit с `-c core.hooksPath=/dev/null`, затем
проверяются итоговый HEAD и чистота дерева; результаты передаются в сообщении.

Тесты, cargo check, компиляция, сборки, приложение, Gateway, тестовые CLI-агенты
и генераторы не запускались. Компилируемость и прохождение тестов не
подтверждены. Статическая прослеживаемость пути не является результатом
выполнения теста; проверки до/после запрещены пользователем. Для запуска после
приёмки требуется отдельное разрешение. Merge, push, deploy и изменения
соседних веток/worktree не выполнялись.
