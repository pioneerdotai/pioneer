# Threads domain execution: передача на повторное внешнее ревью

Актуальная передача — **пятый цикл, девять замечаний**, в конце этого отчёта.
Проверенный кандидат: `32d997153d211b78ea694170bc5c7de1311919f6`.
Commit реализации: `3243465c2ef8737f409aeb7583d3cf526e28a236`.
Предыдущие разделы сохраняют историю реализации и аудитов.

## База, изоляция и граница проверки

- Диагностический commit: `bb85dcdf7080164c4dc6480d12410e5e38edade8`.
- Фактическая база реализации: `bb85dcdf7080164c4dc6480d12410e5e38edade8` (`main`). Расхождений с диагностической базой не было.
- Проверенный кандидат: `669d6a6c2d63b7cd1931936a42c982e84530aa6c`.
- Commit исправлений семи замечаний: `c026919b913109ae82d256fd8dba6f848e1ab067` (`Fix Threads execution audit findings`). Следующий commit обновляет только этот отчёт; итоговый HEAD передаётся в финальном сообщении.
- Ветка: `fix/threads-domain-execution`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-domain-execution`.
- Основной checkout был чистым. Ветка и путь перед созданием были свободны; суффикс не понадобился. Соседние worktree не изменялись.
- Прочитан корневой `AGENTS.md`. В затронутых gateway/crud каталогах дополнительных `AGENTS.md` не обнаружено.
- Dev/prod-базы не открывались; конфиги, установленные skills, службы и пользовательские данные не изменялись. Диагностические факты из задания не воспроизводились запуском приложения.
- Tests, `cargo check`, компиляция, сборки, приложение, Gateway, внешние CLI-агенты и генераторы **не запускались**. Компиляция и прохождение тестов **не подтверждены**. Запрет пользователя имеет приоритет над рекомендациями AGENTS/skills запускать проверки.
- Это результат реализации и статического самоаудита, а не окончательная приёмка. Следующий этап — независимое внешнее ревью; запуск проверок требует отдельного разрешения.

## Карта, составленная до реализации

| № | Находка и первопричина | Инвариант | Место исправления | Основная регрессия |
| --- | --- | --- | --- | --- |
| 1 | CLI исключал actions/models отдельным требованием перечислять Threads в `allowed_tools` | Каталог определяется общими allow/deny, grants и binding | `message/turn_handlers.rs`, `turn_mcp/native_tools.rs`, `turn_mcp/invoker.rs` | Обычный CLI turn → MCP facade → общие handlers для Codex/Claude/OpenCode; отрицательная матрица ограничений |
| 2 | Обычный admission не закреплял create-действия | Только разрешённые инициатору варианты входят в новый immutable manifest; возможность необязательна | `authorization/execution.rs`, `authorization/lease.rs` | Реальный admission Member/ограниченной роли, обычный и Task-backed CLI |
| 3 | Create проверялся на существующем thread | Доступ к source capsule и право workspace creation доказываются отдельно; выбранное действие повторно проверяется | Те же authorization-файлы, `authorization/domain.rs`, adapter и общий create-handler | Оба вида создания, чужой workspace, неподходящий option, отзыв доступа, старый manifest |
| 4 | Root destination получал child execution и общий граф инициатора | Новый root имеет собственного владельца; автор входа остаётся исходным AgentExecution | `message/agent_action_tools.rs`, `authorization/execution.rs`, CRUD graph validation | Завершение/отмена source execution, отдельный Stop destination, настоящее авторство, наследование launch |
| 5 | Observe происходил до создания native actor | Только штатный ThreadNotFound допустим до первого старта; CLI опирается на CLI attempt | Dispatcher и существующая API/CLI preparation | Первый native/CLI dispatch, записывающие runtime/provider, replay потерянного ACK |
| 6 | Cancellation context отсутствует до активации | Доказанное accepted/unactivated состояние можно атомарно остановить; активному runtime контекст обязателен | `message/agent_runtime.rs`, CRUD canonical event pipeline, cancellation/turn-execution repositories | Очередь ресурсов, Starting, activation claim, обе стороны гонки, повторный Stop, отсутствие активного контекста |
| 7 | Исходный Err стирался; окончательный отказ оставлял работающие записи | Безопасная классификация, атомарный отказ до активации, освобождение ресурсов; неизвестный исход сначала сверяется | Outbox processor/repository и общий canonical terminal pipeline | Временный/окончательный/неизвестный отказ, безопасная причина, owner/generation fence |
| 8 | Foreground root использовал concurrent child admission | Один foreground start; сохранённый конфликт имеет терминальный исход, а не взаимное ожидание | StartAgent admission и dispatcher | Одновременные запросы; сохранённый foreground conflict; сохранность другого turn |

## Минимальные решения и изменённые файлы

### Каталог, admission и authorization

Убран только дополнительный Threads-only explicit-name gate. Общий фильтр имён сохранён: unrestricted пустой allowlist не ограничивает; restricted пустой запрещает; непустой ограничивает; deny имеет приоритет. Default allowed-tools не заполняется именами Threads. Общая CLI-проекция продолжает обслуживать три адаптера. `threads_start_options` остаётся в transport family Tasks одним общим binding. Нативное раскрытие через `request_tools` не менялось.

`NativeToolFamily::actions` перечисляет два существующих create-действия как альтернативы для предварительного допуска. Прочие инструменты сохраняют свой единственный action и transport family. Invoker допускает create только при успешной проверке одного из закреплённых вариантов; общий handler затем разрешает точный серверный audience/option и повторно проверяет соответствующее действие.

`ExecutionAdmissionService` факультативно доказывает private/workspace creation. Эти действия не добавлены в безусловный `EXECUTION_RUNTIME_ACTIONS`. Source thread проверяется своим ресурсным правилом; create — на workspace. Lease-проекция не позволяет другому collaborator одолжить инициатору более широкие create-права. Обычный разговор не требует create-действия. Старые manifests при загрузке не расширяются.

Adapter формирует creation options только для разрешённых envelope-вариантов и пересекает их с текущей execution authorization. Workspace-only возможность больше не зависит от наличия private-create. Оpaque IDs, существующее авторство, публичные payloads и написание `threads_identity_models_list` сохранены.

Файлы: `authorization/{domain,execution,lease,agent_runtime_adapter}.rs`, `turn_mcp/{native_tools,invoker}.rs`, `message/{turn_handlers,agent_action_tools}.rs`. `authorization/service.rs` перечитан: существующий workspace resource contract правильный, его расширять не потребовалось.

### Независимый root и происхождение

Routed destination, являющийся собственным collaboration root без lineage и отличающийся от source capsule, получает `parent_execution_id = None`, собственный `work_graph_root_execution_id`, generation 1 и собственный resource scope. Используется существующий `AgentExecutionGraphCommitInput` с root и response binding; нового Task или Subagent протокола нет. Internal HomeCapsule destinations сохраняют parent execution, общий граф и concurrent child admission.

Source execution/action сохраняются внутри существующего hashed grant JSON. Автор канонического входа — настоящий source AgentExecution. Graph, Turn, author, response, security, admission, action/receipt/outbox коммитятся существующей общей транзакцией. CRUD проверяет связь input actor с source action; recovery/dispatcher повторно сверяют immutable grant, source ledger/receipt/outbox и точный response. Подлинность не выводится из предположения об обязательном parent lifecycle.

Существующая проверка monotonic child-launch ceiling применяется и к source независимого root. Контекст destination выводится из parent ceiling, а свежий admission инициатора проверяется перед новым root launch и первым dispatch. Этот свежий proof не заменяет сохранённый контекст и не добавляет ему actions. Identity/provider/executor/model/reasoning проходят прежние launch resolution и security derivation.

Файлы: `message/agent_action_tools.rs`, `authorization/execution.rs`, `crud/src/lib.rs`, `crud/src/repositories/agent_domain.rs`. Прочитаны helpers в `authorization/{agent_execution,agent_runtime_adapter}.rs`, ordinary root admission, response restoration и cancellation graph traversal.

### Первый dispatch, replay и конфликт

Dispatcher различает отсутствие native actor (`ThreadNotFound`) и ошибки control plane. Другие ошибки observation не интерпретируются как отсутствие исполнения. CLI проверяет свой durable attempt и не требует native actor. Проверки committed author, identity/profile, workspace, response, resource permit и authorization остаются.

Перед native model request используется CAS существующего `turn_execution` из queued/starting в running с точным owner/generation и действующим lease. `started_at` заполняет обычный runtime ACK. Принятая canonical terminal event запрещает claim. Повторный dispatch с живым native owner или CLI attempt подтверждает только доставку, а не успешную работу модели. Preparation snapshot при первоначальном starting/queued owner generation 1 и started_at=NULL допускает повторную подготовку через CAS. Snapshot после claim без подтверждённого live owner считается неоднозначным исходом; слепого второго запроса модели нет. Recovery/resume с новым owner generation владеет собственным протоколом; исходный outbox не запускает его повторно.

Для самостоятельного root вызывается обычный `agent_turn_start_with_permission_profile`. Занятость возвращает отказ до сохранения второго запуска. При уже сохранённом routed-root конфликте dispatcher завершает конфликтующий pending start; другой foreground turn не изменяется. Обычный foreground guard применяется также к Current thread/Current collaboration, если destination — видимая пользовательская root-беседа без lineage. Настоящие internal child destinations сохраняют прежнее ожидание. Новый scheduler/очередь не введены.

Файлы: `message/agent_action_tools.rs`, `crud/src/lib.rs`, `crud/src/repositories/turn_execution.rs`. Неизменённые ordinary API/CLI prepare/dispatch, thread manager foreground gate и CLI transition mutex проверены статически.

### Stop и окончательный отказ

Добавлена узкая ветка accepted/unactivated terminal transition в существующем canonical event engine. Она требует durable execution scope, owner и generation, отсутствие `started_at`, native cancellation registration и CLI attempt. Running без `started_at` — только промежуточный activation claim; Gateway дополнительно исключает уже принадлежащий native actor turn. Отсутствия cancellation context самого по себе недостаточно.

Writer повторно сверяет прочитанные условия непосредственно при принятии события. Canonical terminal acceptance, terminal execution status и освобождение direct AgentExecution ресурсов входят в одну транзакцию. Native context registration и CLI initial-attempt registration проверяют принятую terminal event. Поэтому подтверждённый Stop не разрешает последующий model start. Активированный native runtime продолжает требовать immutable cancellation context; фиктивный контекст не создаётся.

Окончательные отказы StartAgent используют тот же guard и canonical Failed/Blocked events. Late cleanup дополнительно проверяет точное поколение и терминальное состояние перед освобождением ресурсов. Task-backed execution с `parent_task_id` оставляет завершение своего Task/run существующему lifecycle.

Файлы: `message/{agent_action_tools,agent_runtime}.rs`, `crud/src/lib.rs`, repositories `{native_cancellation_context,turn_execution,agent_domain}.rs`.

### Delivery diagnostics и retry

Сохраняются фиксированные классы: integrity, authority, unsupported runtime, preparation unavailable, control unavailable, storage unavailable, unknown outcome и foreground conflict. Сырые provider errors, пользовательские payloads и секреты в outbox diagnostics не записываются. Ошибки подготовки с неопределённой причиной сохраняют retry; типизированные storage errors сохраняют исходный путь повторов. Перед окончательным отказом сверяются response, автор, source ledger и текущее исполнение.

После подтверждённого локального terminal refusal outbox остаётся `failed` с фактическим числом attempts и `next_attempt_at = NULL`. Немедленная выборка и retention отличают эти конкретные StartAgent причины от старых retryable failures. Бюджет 8, lease, backoff и остальные лимиты не увеличены. Семантика остальных Agent actions и legacy immediate-failure rows сохранена.

## Неизбежные дополнительные контракты

Новых таблиц, колонок, миграций, зависимостей, инструментов и пользовательских настроек нет.

- Private `TurnEventProjectionContext.unactivated_terminal` не сериализуется и переносит ожидаемый execution record до writer revalidation. Он нужен для атомарной защиты Stop/refusal от activation и смены owner generation.
- CRUD methods claim/unactivated terminal/fenced terminal cleanup обслуживают реальные production callers, используют существующие состояния и serialized writer. Они нужны для at-most-one initial activation и защиты нового поколения от старого callback.
- `AgentActionOutboxFailure` — закрытый набор безопасных диагностических причин вместо произвольной строки. Он нужен для классификации и прекращения невозможной доставки без изменения attempt budget.
- Source execution/action в существующем grant JSON — доказательство происхождения независимого root без child-lifecycle связи. Это не новый публичный payload.
- Visibility общего terminal synchronization helper и существующего typed SQLite-lock classifier расширена только до `pub(super)` ради повторного использования outbox caller. Typed BUSY/LOCKED сохраняет storage retry и не превращается в окончательный semantic refusal.

Новые writer-переходы содержат только необходимые bounded DB reads/writes. Подготовка событий, payload parsing в dispatcher, provider/runtime/catalog I/O выполняются вне DB capacity. Repositories получают scoped database handle; прямые соединения и ручные permits не добавлены. Сохранены read-only readers, serialized writer, existing Interactive/Maintenance scopes.

## Написанные и обновлённые регрессии — НЕ ЗАПУСКАЛИСЬ

`message/tests/cli_task_tools.rs`:

- `cli_tasks_share_authorization_schemas_skills_and_durable_replay_for_all_providers`: четыре операции ожидаются по действительным правам обычного turn, без ручного allowlist.
- `cli_threads_actions_use_facade_shared_handlers_routes_and_replay_for_all_providers`: теперь первичный positive path — обычный `process_request_for_connection`; три адаптера, facade, create/send/start/models, один options binding, explicit model/reasoning до recording runtime, idempotent receipts. Отрицательные проверки search/options сохранены в ограниченных контекстах.
- `cli_explicit_model_and_reasoning_reach_native_launch_and_survive_mcp_replay`: дополнен реальным Task-backed child admission и create через facade.
- `cli_threads_permission_matrix_preserves_restricted_names_denies_and_missing_grants`: обе пустые allowlist семантики, непустой allowlist, deny, ограниченный binding, старый manifest без create grant. Негативные вызовы несут настоящую native call correlation.
- `ordinary_cli_delegated_root_dispatch_uses_cli_attempt_and_replays_without_native_actor`: первый CLI-root dispatch и lost-ACK replay для всех трёх адаптеров, без фиктивного native actor и без TaskRun.

`message/tests/thread_launch_choices.rs`:

- Общий fixture использует настоящий `ExecutionAdmissionService`, не выдаёт список Threads grants; добавлены recording probe и Member с реальными workspace/thread memberships.
- `inherited_configuration_survives_visible_thread_creation_message_and_start_replay`: перед вторым foreground start завершается первый; create/send по-прежнему не запускают модель.
- `delegated_root_first_dispatch_replay_and_stop_have_an_independent_owner`: Source execution завершён либо отменён до delivery; новый root доходит до provider один раз, сохраняет автора, модель/effort и переживает ACK replay; активный и повторный Stop.
- `delegated_root_foreground_admission_rejects_competing_start_without_persisting_it`: одновременные запросы, один durable start, ранний Stop, источник остаётся работающим, redispatch не создаёт runtime.
- `queued_internal_thread_stop_releases_only_its_child_resources_before_dispatch`: ожидание ресурса, прежний child graph, отдельная отмена и отсутствие model call.
- `definitive_start_refusal_has_safe_cause_terminal_turn_and_released_resources`: retirement identity до dispatch, safe cause, Failed turn/execution, освобождение permit, отсутствие повторных attempts/model calls.
- `persisted_foreground_conflict_refuses_pending_root_and_preserves_other_turn`: сохранённый конфликт через прежний concurrent admission; отказ pending root без изменения другого turn.
- `member_creation_uses_both_workspace_variants_and_revocation_fences_the_shared_handler`: оба create-варианта, неподходящий option, revoke и отсутствие неразрешённого action/outbox.

`authorization/lease.rs`:

- `optional_creation_admission_uses_workspace_and_cannot_borrow_collaborator_rights`: Member, чужой workspace, роль без create при разрешённом обычном разговоре, revoke и неизменность manifests.

`message/tests/agent_action_outbox.rs`:

- Обновлена проверка safe причины malformed payload.
- `start_outbox_refusal_stops_delivery_without_inflating_attempts_and_unknown_error_is_safe`: окончательный отказ, прежний retry для control/unknown, redaction произвольной ошибки и отсутствие повторного claim terminal row. Существующие ACK/panic/claim-fault регрессии сохранены.

`crud/src/native_cancellation_tests.rs`:

- `accepted_unactivated_stop_and_activation_share_a_durable_writer_fence`: queued/starting activation claim, повторный Stop, запрет регистрации контекста и повторного claim после Stop; гонка двух реальных writer contenders.
- `early_stop_never_bypasses_an_activated_owner_or_missing_immutable_context`: активный execution без обязательного контекста остаётся защищённым.
- `unactivated_refusal_and_stop_preserve_activation_terminal_results_and_recovery_ownership`: детерминированный activation-first порядок, сохранение принятого Completed, отказ старому owner после настоящего recovery claim.

Существующие активные native cancellation/recovery fixtures и настоящие Task/Subagent tests не заменены строковыми проверками. Новые provider probes используют Notify; внешние CLI-процессы не требуются. Ни baseline, ни исправленная версия тестов не запускались.

## Промежуточный и итоговый самоаудит

1. CLI projection: прослежены fresh и durable branches, общий binding/final filter и native handlers. Специальный список имён удалён; deny и restrictive allowlist сохранены.
2. Admission/workspace: проверены обычный native/CLI admission, Task admission/continuation, manifests и lease. Create факультативен; wrong resource заменён двумя независимыми proof; старые grants не расширяются.
3. Root/child: сверены author, source action, graph writer, response binding, restore, source launch ceiling, current initiating-user admission и root cancellation traversal. Generation независимого root не зависит от поколения source.
4. First dispatch/replay: проверены отсутствие actor, native control errors, CLI attempts, snapshot/unknown исход, owner/lease claim, потеря ACK и передача ownership recovery/resume.
5. Stop: сверены pre-activation proof, context registration, CLI attempt registration, accepted terminal guard, actor presence и атомарное освобождение direct ресурсов. Отдельно сохранён Task lifecycle cleanup.
6. Delivery/conflict: исправлены потеря Err, преждевременная повторная выборка terminal failed row и зависимость foreground conflict от настоящего routed root. Поздние terminal/refusal callbacks защищены writer generation fence.
7. Fixtures/integration: основной facade positive path переведён на обычный admission; отрицательные контексты выделены явно. Перечитаны изменённые тесты и необходимые неизменённые callers. Новых test-only production wrappers/генераторов нет.

Итоговый diff от базы перечитан вместе с graph persistence, canonical projection/replay, root cancellation, обычными API/CLI dispatch helpers, CLI transition guard и Task occurrence paths. Сверены затронутые struct initializers, enum matches и единственный caller изменённого API preparation result. Самоаудит выявил и исправил также повторный claim локально окончательного отказа, возможность одолжить create у collaborator, ошибочное применение root conflict к current-thread child, Task cleanup ownership и гонку позднего отказа с новым owner generation.

## Сохранённые записи и известные ограничения

- Frozen CLI-каталоги уже начатых turn не расширяются. Старый manifest без create не получает grant из текущих более широких прав пользователя. Новый обычный turn формирует исправленный каталог автоматически, без новой настройки.
- Старые child graphs не переписываются в независимые roots задним числом. Уже сохранённые routed-root конфликты получают отказ вместо взаимного ожидания. Остальные legacy/recovery состояния продолжают обрабатываться существующим recovery; живые dev-записи вручную не исправлялись.
- Наличие runtime snapshot без достоверного live owner не доказывает, был ли model call. Такой исход не вызывает слепого нового запуска; Blocked либо существующий recovery сохраняют неопределённость. При текущем control-plane сбое сохраняется retry в прежнем бюджете.
- Смена owner generation передаёт активацию существующему recovery/resume протоколу. Начальный outbox не активирует новое поколение.
- Не добавлена поддержка ACP dispatcher; существующие launch/backend ограничения сохранены.
- Accepted/queued и ACK означают прежние границы admission/delivery, а не завершение модели.
- Главная непроверенная область: типизация, выполнение и прохождение новых и существующих тестов. Только внешнее ревью и отдельно разрешённые проверки могут подтвердить её.

## Фактически выполненные команды

Все команды ниже запускались из worktree, кроме явно указанной подготовки основного checkout. Чтение/редактирование исходников выполнялось `rg`, `sed -n`, `cat`, `head`, `tail`, `ls`, `pwd` и `python3 - <<'PY'` heredoc-редактированием. Python не импортировал и не выполнял код проекта. Полные payloads инструментов чтения/редактирования находятся в истории выполнения; это не результаты тестов.

Подготовка (основной checkout):

```sh
pwd
rg --files -g AGENTS.md -g '!target' -g '!node_modules' /Users/alexander/Code/pioneer 2>/dev/null
git status --short
git rev-parse HEAD main
git worktree list
git branch --list 'fix/threads-domain-execution*'
ls -ld .worktrees/threads-domain-execution
cat AGENTS.md
git worktree add -b fix/threads-domain-execution .worktrees/threads-domain-execution main
```

Инспекция Git и допустимое форматирование (команды повторялись после правок):

```sh
git status --short
git branch --show-current
git rev-parse HEAD main
git worktree list
git config --get core.hooksPath
git diff --stat
git diff --numstat
git diff --check
cargo fmt --all
cargo fmt --package pioneer-crud -- crates/crud/src/native_cancellation_tests.rs
```

`core.hooksPath` не был установлен. Дополнительный путь в cargo fmt нужен для тестового файла, подключённого через include. Общий formatter не внёс изменений за пределами перечисленных файлов задачи.

Окончательные разрешённые проверки завершились с exit code 0:

```sh
cargo fmt --all -- --check
cargo fmt --package pioneer-crud -- --check crates/crud/src/native_cancellation_tests.rs
git diff --check
```

Повторно проверены `git status --short`, `git rev-parse main` и `git worktree list`: основной checkout чистый, main остаётся на базовом commit; изменён только выделенный worktree. Единственный новый файл — этот отчёт.

Фиксация выполняется с отключёнными hooks; staging ограничен 17 перечисленными Rust-файлами и этим отчётом. Перед commit применяется `git diff --cached --check`, после — `git status --short` и `git rev-parse HEAD`:

```sh
git -c core.hooksPath=/dev/null add <18 явных путей файлов задачи>
git diff --cached --check
git diff --cached --stat
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -m "Fix Threads domain admission and independent execution"
git status --short
git rev-parse HEAD
git log -1 --format='%H %s'
```

Идентификатор commit передаётся в финальном сообщении, поскольку включить собственный hash внутрь того же commit невозможно. Тесты, cargo check, компиляция, сборки, приложение, Gateway, CLI-агенты и генераторы не запускались; компиляция и прохождение тестов не подтверждены.


## Повторное ревью: карта семи замечаний (до коррекции)

База исправляющего diff и проверенный внешним аудитом кандидат — `669d6a6c2d63b7cd1931936a42c982e84530aa6c`; исходная база и изолированная ветка/worktree прежние. Предыдущие разделы описывают исходные восемь направлений; уточнения повторного ревью ниже имеют приоритет.

| № | Первопричина → инвариант | Место коррекции → регрессия |
| --- | --- | --- |
| 1 | Свежий Task admission восстанавливает create по principal → immutable source ceiling действует и при делегировании | execution/task_tools, MCP Task child missing-grants |
| 2 | Stop застревает между actor/context ACK и writer → один запрос проходит существующую фазу с owner/generation fence | native control + canonical cancellation, публичные phase-race tests |
| 3 | Собственный visible root использует child bypass → любой пользовательский root проходит foreground guard | commit/dispatch, Current thread/collaboration concurrency |
| 4 | Последняя delivery error теряет cleanup → exhausted StartAgent сохраняет durable reconciliation obligation | outbox repository/dispatcher, реальные storage/terminal commit faults |
| 5 | Preparation snapshot принят за возможный запуск → исходный starting/queued без CAS можно доставить | dispatch snapshot fence, lease/storage CAS regressions и unknown activation |
| 6 | Proof unavailable ошибочно становится denial → typed storage failure сохраняет retry без выдачи прав | lease/context dispatch, real read fault и semantic denial |
| 7 | Тип foreground conflict стёрт строкой → atomic admission возвращает agent_action_conflict | thread admission/tool boundary, точный публичный исход |


### Исправления после внешнего аудита

1. **Task не восстанавливает отсутствующие create-grants.** `ExecutionAuthorizationContext::constrain_thread_creation_grants` пересекает только два факультативных create-действия с immutable actions исходного исполнения. Ограничение применяется после свежего agent-origin TaskCreate admission и к agent-origin TaskResume readmission, включая legacy seed. Public user Task admission, envelopes, skills, policy blocks и Task/Subagent протоколы не переписаны. Файлы: `authorization/execution.rs`, `task_tools/mod.rs`. Расширен `cli_threads_permission_matrix_preserves_restricted_names_denies_and_missing_grants`: реальный MCP task_create успешен, Task admission и настоящий CLI child manifest не содержат create, у child нет creation options/binding и MCP create запрещён. Существующий положительный Task-backed сценарий сохранён.

2. **Один Stop проходит смену фазы.** Initial native run хранит процессное доказательство ожидания durable registration ACK. Gateway сопоставляет durable owner/generation/scope с конкретными actor generation/run_id. Существующий CancelTurn control отменяет и завершает именно этот process run, обеспечивает настоящий immutable context и только затем ACK. Generation-bound phase command сам не публикует unguarded Interrupted: исходный Gateway caller перечитывает durable owner/generation и фиксирует canonical result с writer guard. Если ранний writer проиграл регистрации, типизированный `UnactivatedTurnPhaseChanged` разрешает один переход к обычной cancellation с реальным context, при неизменном owner/generation. Storage errors и смена владельца не становятся поводом для произвольного retry. Активный runtime с отсутствующим обязательным context по-прежнему запрещён. Без новых таймаутов/sleep/cancellation framework. Файлы: `agent/{lib,agent_loop}.rs`, `message/agent_runtime.rs`, `crud/src/lib.rs`, `native_cancellation_context.rs`.

   Написаны `public_native_stop_finishes_actor_admission_before_context_registration` и `public_native_stop_switches_to_registered_context_when_early_writer_loses_phase` в `recovered_native_turn_stop.rs`. Это публичный turn/cancel, ordinary admission/preparation, настоящий actor, recording provider и deterministic barriers; проверяются canonical Interrupted, неизменный owner/generation, execution/resource cancelled, released permit, repeat Stop и отсутствие позднего provider call. Старые CRUD-тесты ручного fallback не используются как доказательство публичного перехода. Существующий `unactivated_refusal_and_stop_preserve_activation_terminal_results_and_recovery_ownership` дополнен проверкой writer generation guard после настоящего recovery claim с прежним owner ID: старый Stop не принимает terminal нового поколения.

3. **Собственный visible root тоже использует foreground admission.** Выбор зависит от destination (home root, visible, без lineage), а не только от отличия source/destination capsule. Busy guard остаётся атомарным в ThreadManager. Dispatcher завершает ранее сохранённый own-root конфликт, сохраняя другой turn. Internal child admission и настоящие Tasks/Subagents сохраняют concurrent семантику. Файлы: `agent_action_tools.rs`, `thread/mod.rs`. Существующие concurrency и persisted-conflict тесты расширены вариантами New delegated root, Current thread и Current collaboration; для собственного занятого root проверены точный публичный отказ и отсутствие сохранённого запуска.

4. **После восьмой попытки остаётся durable reconciliation.** Дополнительная bounded выборка exhausted StartAgent входит в прежний timed quantum; общий batch limit не увеличен, обычная timed predicate сохранена. Известные failed/8/NULL записи подбираются без ручной правки живой базы. Claim не увеличивает attempts, помечает существующий outbox receipt как reconciliation и использует прежний lease. Этот путь никогда не входит в launch dispatcher: он сверяет ledger/response/owner/generation/runtime и либо фиксирует canonical Failed/Blocked с освобождением ресурсов, либо подтверждает, что существующий runtime/recovery владеет исходом. Ошибка storage или terminal commit сохраняет обязанность следующего reconciliation через прежний lease interval. Одного last_error/attempts недостаточно для исключения из cleanup: exhausted discovery сверяет accepted terminal и терминальные execution/resource state, отсутствие held permit; compaction и поздний terminal cleanup также требуют durable terminal acceptance. Остальные Agent actions сохраняют прежнюю исчерпанность. Файлы: `agent_domain.rs`, `agent_action_tools.rs`, CRUD re-export.

5. **Preparation snapshot отделён от activation.** Snapshot при первоначальном owner generation 1, starting/queued и started_at=NULL разрешает штатную повторную preparation/CAS. Running после успешного claim, смена поколения и неопределённый ACK не разрешают повторный model call. Lease, accepted terminal и exact owner/generation остаются CAS fences. Файл: `agent_action_tools.rs`.

   Общая содержательная регрессия `prepared_agent_start_retries_before_claim_but_reconciles_after_claim_or_exhaustion` в `thread_launch_choices.rs` проходит настоящий StartAgent/prepare/dispatch с recording provider: lease CAS false; реальная storage ошибка CAS до commit; реальная ошибка persisted authority read после успешных ранних reads; storage failure и отказ canonical terminal append на последней delivery attempt; потеря процесса после успешного claim. Используются disposable fixture DB, реальные failed SQL операции и существующие deterministic barriers, без подстановки Rejected. После восстановления первые три случая доходят до одного provider call и выдерживают lost-ACK replay; exhausted/unknown случаи ожидают Blocked и released permit без model call и без attempts>8.

6. **Proof unavailable отличается от semantic denial.** На контекстных границах сохранены typed DB causes (Conn/Exec/Query/ConnectionAcquire, прежние pool/lock classifiers), поэтому SQLITE_IOERR и недоступная lease/context query сохраняют safe StorageUnavailable/retry. Некорректный JSON/envelope, missing immutable data и подтверждённый denial остаются семантическим отказом. Нет распознавания по тексту или выдачи дополнительных прав. Файлы: `agent_action_tools.rs`, `turn_handlers.rs`. Дополнительный тест `agent_start_proof_preserves_typed_storage_faults_without_retrying_semantic_denials` проверяет IOERR code 10 и semantic/serde/type варианты; он дополняет реальную read-fault regression выше. `definitive_start_refusal_has_safe_cause_terminal_turn_and_released_resources` расширен настоящей suspension initiating principal, ожидает AuthorityDenied и ноль model calls.

7. **Штатный публичный конфликт.** Атомарный foreground guard возвращает `ForegroundTurnConflict`; граница инструмента переводит только этот тип в существующий `agent_action_conflict`. Другие preparation errors сохраняют прежнюю классификацию; предварительная проверка занятости не заменяет guard. Файлы: `thread/mod.rs`, `agent_action_tools.rs`. Конкурентный тест требует точный публичный code и один durable launch; busy own-root не сохраняет rejected launch.

### Результат повторного статического самоаудита

- Сопоставлены все семь замечаний с реальными admission, ledger, canonical writer, control ACK и runtime callers. Участки Task grants и proof-errors проверены отдельно; результаты обеих подзадач и Stop-подзадачи перечитаны и интегрированы. Незавершённой attached-работы нет.
- Сверены новые CancelTurn initializers/matches, точный run fence до out-of-band cancellation и actor-side Stop fences, все новые generation-guard callers, private projection initializers, proof classifier и API preparation caller.
- Повторно просмотрены исправляющий diff от `669d6a6c` и совокупный diff от `bb85dcdf`, включая изменения тестов и неизменённые ordinary API/CLI preparation, native context registration, Task admission/child projection, root cancellation и canonical replay callers.
- Во время аудита исправлены дополнительные дефекты внутри затронутых цепочек: process ACK не равен context ACK; delayed control не должен терминализировать новое durable поколение; exhausted terminal-commit error не должен выглядеть как подтверждённый terminal receipt; минимальный batch limit=2 должен давать прогресс обычным due rows; тип ошибки storage должен переживать semantic context wrappers. В public Stop fixture явно выбран Agent mode, чтобы проверять реальный execution graph и permit.
- `exhausted_start_reconciliation_is_bounded_and_yields_to_an_ordinary_due_row` проверяет реальный claim/process quantum при limit=2, отсутствие девятой попытки и прогресс другой строки. Poison row без response не выдаётся за завершённый cleanup.
- Identity/model/reasoning, workspace creation options, реальное AgentExecution авторство, независимость delegated root, once-only replay и первоначальные CLI/grant ограничения сохранены. Новых таблиц, DB/public payload fields, зависимостей, пользовательских настроек, лимитов, scheduler или второй системы восстановления нет. Узкие process-only registration flags и run fence нужны для фактически существующей initial native фазы.

### Ограничения повторной передачи

Все регрессии только написаны. Компиляция, типизация, выполнение гонок и прохождение тестов не подтверждены. SQL reconciliation проверен статически; его выполнение и query plan не измерялись. При недоступном storage/control либо недоказуемом повреждённом ledger outcome остаётся unresolved и повторно согласуется в bounded outbox quantum; он не считается cleanup по одному error marker. Frozen CLI catalogs и старые immutable manifests не расширяются. Старые child graphs не преобразуются в independent roots задним числом. Живые dev/prod записи не исправлялись и не перезапускались.


### Фактически выполненные проверки повторной передачи

Разрешённые команды завершились с exit code 0:

```sh
cargo fmt --all
cargo fmt --package pioneer-crud -- crates/crud/src/native_cancellation_tests.rs
cargo fmt --all -- --check
cargo fmt --package pioneer-crud -- --check crates/crud/src/native_cancellation_tests.rs
git diff --check
git diff --cached --check
git diff 669d6a6c --check
git diff bb85dcdf --check
```

После последней production-правки повторены общий formatter и общий fmt check; включённый CRUD test-файл отдельно отформатирован и проверен. Сверены список изменённых файлов, initializers и enum matches; посторонних formatter-изменений нет. Read-only сверки: `git status --short`, `git rev-parse HEAD main`, `git worktree list`, `git diff --stat`, `git diff --numstat`, исправляющий `git diff` и совокупный `git diff bb85dcdf`. Основной checkout остаётся чистым, main — `bb85dcdf7080164c4dc6480d12410e5e38edade8`; соседние worktree не изменялись.

Точные команды фиксации (из выделенного worktree, hooks отключены):

```sh
git -c core.hooksPath=/dev/null add crates/agent/src/agent_loop.rs crates/agent/src/lib.rs crates/crud/src/lib.rs crates/crud/src/native_cancellation_tests.rs crates/crud/src/repositories/agent_domain.rs crates/crud/src/repositories/native_cancellation_context.rs crates/gateway/src/authorization/execution.rs crates/gateway/src/message/agent_action_tools.rs crates/gateway/src/message/agent_runtime.rs crates/gateway/src/message/tests/agent_action_outbox.rs crates/gateway/src/message/tests/cli_task_tools.rs crates/gateway/src/message/tests/recovered_native_turn_stop.rs crates/gateway/src/message/tests/thread_launch_choices.rs crates/gateway/src/message/turn_handlers.rs crates/gateway/src/task_tools/mod.rs crates/gateway/src/thread/mod.rs
git diff --cached --check
git diff --cached --stat
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -m "Fix Threads execution audit findings"
git rev-parse HEAD
git -c core.hooksPath=/dev/null add docs/reviews/threads-domain-execution.md
git diff --cached --check
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -m "Document Threads execution audit corrections"
git status --short
git rev-parse HEAD
git log -2 --format='%H %s'
git -c core.hooksPath=/dev/null add docs/reviews/threads-domain-execution.md
git diff --cached --check
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit --amend --no-edit
git status --short
git rev-parse HEAD
```

Тесты, cargo check, компиляция, сборки, приложение, Gateway, CLI-агенты и генераторы не запускались; компиляция и прохождение тестов не подтверждены. Merge, push и deploy не выполнялись. Передача предназначена для повторного внешнего ревью и не является окончательной приёмкой.


## Третий проход внешнего ревью: карта замечаний

Исходный HEAD этого прохода: `5485fa9c5bd327861fb5ebab9103ee40bc35bf88`; совокупная база `bb85dcdf7080164c4dc6480d12410e5e38edade8`. Ветка/worktree прежние, при начале чистые. Фактический main уже `4f212561708f5918f2a32f43849c0cc65e115857`; он не менялся этой работой, rebase/merge не выполнялись.

| № | Причина | Инвариант и узкое место исправления |
| --- | --- | --- |
| 1 | Native helper возвращает unit при неуспешном recovery/terminal commit | Committed outbox требует подтверждение runtime/recovery/canonical terminal после helper |
| 2 | Actor появляется после выбора early Stop | Тот же owner/generation повторно проходит existing initial actor control и real context |
| 3 | Process ACK ждёт durable storage и уничтожает actor по короткому timeout | Process ACK и registration receipt разделены внутри существующей CancelTurn команды; Gateway остаётся владельцем canonical Stop |
| 4 | Terminal replay освобождает ресурсы без синхронизации памяти | Existing terminal synchronization подтверждается в fresh/terminal replay ветках; false сохраняет reconciliation |
| 5 | Compaction удаляет dispatch.turn_id | Минимальные turn_id/execution_generation сохраняются без launch/input payload; terminal proof остаётся проверяемым |
| 6 | Shared CLI preparation блокирует committed turn при transient proof read | Только DurableAgent storage failure сохраняет typed cause в internal completion и outbox retry |
| 7 | Assertion расходится с существующим resource mapping | Turn Blocked, execution blocked, resource failed, permit released; mapping production не меняется |


### Минимальные решения третьего прохода

1. **Подтверждение native delivery.** Общий `dispatch_prepared_api_provider_turn_start` сохраняет прежний unit-контракт и поведение ordinary callers. Committed-outbox caller после него проверяет canonical terminal (с fenced cleanup и подтверждённой memory sync), unresolved recovery job либо существующий runtime. При отсутствии proof возвращает OutcomeUnknown в существующий reconciliation, не Delivered. Успешный activation CAS и подготовленный snapshot по-прежнему исключают слепой второй запуск. Файл: `message/agent_action_tools.rs`. В существующей recording-регрессии добавлен `actor_admission_storage`: после настоящего CAS штатный `AgentManager::remove_thread` создаёт реальный ThreadNotFound при actor admission; fixture SQL triggers отклоняют recovery_job и canonical terminal insert, но позволяют outbox bookkeeping. После восстановления хранения ожидаются Blocked/blocked/failed/released, ноль provider calls, attempts <= 8. Provider registry не подменяется внешним HTTP provider.

2. **Actor появился после выбора early Stop.** Общий узкий helper `cancellation_context_after_initial_phase` перечитывает exact durable owner/generation/scope и либо возвращает реальный context, либо передаёт Stop доказанному initial actor/run через существующий generation-bound control. Он используется и исходной фазой, и fallback после typed UnactivatedTurnPhaseChanged. После control receipt снова проверяется durable owner/generation; canonical writer сохраняет прежний generation guard. Файл: `message/agent_runtime.rs`. `exercise_initial_native_stop` расширен вариантом `ActorAppearsBeforeRegistration`; тест `public_native_stop_follows_actor_appearing_before_registration` удерживает early writer, принимает реальный actor, проверяет started_at, задерживает запись context и одним public turn/cancel подтверждает весь terminal cleanup без позднего provider call.

3. **Задержка регистрации не превращается в process ACK timeout.** Для generation-bound CancelTurn существующий короткий ACK подтверждает принятие control-команды и применённую out-of-band отмену. Отдельный oneshot receipt той же команды подтверждает durable регистрацию captured immutable context после завершения process run. Gateway остаётся awaited владельцем canonical Stop и не попадает в rollback по обычному двухсекундному process timeout при исправном actor. Не менялись timeout/backoff/budget; не добавлены detached workers или retry loops. Закрытый канал регистрации даёт ошибку, а не фиктивный успех. Обычный CancelTurn без phase guard использует прежнее поведение. Файлы: `agent/{lib,agent_loop}.rs`. `public_native_stop_retains_completion_after_the_process_ack_window` использует existing barriers и управляемые Tokio pause/advance (3 секунды при прежнем ACK window 2 секунды), затем подтверждает canonical Interrupted, owner/generation, resource cleanup, repeat Stop и отсутствие model calls. Сами тесты не выполнялись.

4. **Terminal replay подтверждает memory sync.** Fresh refusal, terminal finalizer replay и ранняя terminal-ветка dispatcher требуют положительный результат существующего synchronize helper. False/unavailable сохраняют обязанность обработки. Fenced cleanup по-прежнему проверяет exact owner/generation и принятый canonical terminal; глобальные Blocked/resume semantics не менялись. Exhausted selection сохраняет строку с назначенным next_attempt_at даже при уже принятом terminal/resource cleanup: иначе последняя ошибка memory sync снова исчезала бы из обработки. Файлы: `agent_action_tools.rs`, `agent_domain.rs`. Recording case `terminal_memory` теперь начинается с attempts=7, теряет процесс после CAS, принимает настоящий Blocked на восьмой попытке, затем получает реальную query error до memory sync. Следующий reconciliation должен синхронизировать загруженный Turn с canonical и освободить обычный foreground guard без девятой delivery attempt/model call.

5. **Compaction сохраняет terminal binding.** У StartAgent сохраняются только dispatch.turn_id и execution_generation; input, params и launch payload освобождаются. SQL discovery продолжает требовать accepted canonical terminal, terminal execution/resource и отсутствие held permit. Для уже compacted receipt без dispatch используется существующий immutable grant, связанный точным source_action_id: он восстанавливает только terminal Turn link. Finalizer для такого legacy link допускает лишь подтверждённый terminal cleanup/sync, никогда activation или refusal нового running поколения. Файлы: `agent_domain.rs`, `agent_action_tools.rs`. В реальном exhausted case выполняется compaction, проверяется минимальный retained binding, моделируется старый compacted payload без dispatch и потеря ACK; последующие quanta не выбирают завершённую строку и не увеличивают attempts. Unresolved exhausted receipt до terminal acceptance остаётся с полным payload и доступен для reconciliation. Существующий `terminal_action_ledger_compaction_preserves_window_and_exact_replay_hashes` дополнен настоящими binding fields и проверкой их минимального retention.

6. **Typed proof-unavailable доходит через shared CLI preparation.** Только у committed DurableAgent ошибки storage при register_execution_lease и повторном context read обходят преждевременный Blocked. Internal completion сохраняет typed TaskStartFailure cause существующего TurnStartFailure::internal_typed, вместо сериализации в public JSON; outbox получает прежний StorageUnavailable/retry. Semantic denial, ordinary user и Task/Subagent branches сохраняют прежнее поведение. Файл: `turn_handlers.rs`. `ordinary_cli_delegated_root_dispatch_uses_cli_attempt_and_replays_without_native_actor` расширен для Codex, Claude, OpenCode вариантами lease/context read faults на реальной shared preparation границе (disposable fixture table временно недоступна). Первая доставка ожидает InProgress, отсутствие terminal/attempt/provider start; после восстановления — один destination runtime start и lost-ACK replay без второго вызова. Внешние CLI-процессы тестам не требуются.

7. **Корректный resource contract в регрессии.** В `prepared_agent_start_retries_before_claim_but_reconciles_after_claim_or_exhaustion` сохранены содержательные проверки canonical Blocked и released permit; resource assertion исправлен на failed и добавлена отдельная проверка AgentExecution blocked. Production mapping не менялся. Файл: `thread_launch_choices.rs`.

### Статический самоаудит третьего прохода

Перечитаны весь исправляющий diff от `5485fa9c` и связанные unchanged ordinary native/CLI callers, native registration/ACK, canonical cancellation writer, typed error completion, terminal replay и compaction. Проверены все CancelTurn initializers/matches (registration_ack=None у обычных callers), closed-receipt error, initial-run/actor generation/run fences, обе стороны early writer гонки и обязательный real context у activated runtime. Подготовка, control ожидания и provider/runtime calls не происходят при удержании DB capacity.

Проверена композиция частичных операций: CAS без model admission; accepted Blocked без memory update; последняя storage error; exhausted reconciliation с уже готовым terminal proof, но ещё назначенной обработкой; compaction и потерянный ACK. Аудит дополнительно исправил exclusion последнего memory-sync retry из SQL selection и устаревший compaction fixture без Turn binding. Для admission fault выбран настоящий missing actor вместо удаления injected provider, чтобы регрессия не могла перейти к внешнему HTTP provider.

Не изменены authorization/grants, immutable source ceiling, lifecycle независимого root, identity/model/reasoning, Tasks/Subagents policies/skills/protocols, search/recall/Memvid, UI, зависимости, лимиты и пользовательские настройки. Новых таблиц, DB fields и отдельной системы cancellation/recovery/delivery нет. Internal registration receipt — продолжение уже существующей generation-bound CancelTurn, а не свидетельство успешного Stop само по себе.

Оставшиеся ограничения: типизация и выполнение регрессий не подтверждены; timing/query plans не измерялись. Неизвестная активация сохраняет запрет повторного model call. Повреждённый legacy ledger без доказуемой canonical связи остаётся unresolved в bounded reconciliation, не выдаётся за завершение. Runtime/recovery liveness вне описанных цепочек сохраняет существующие механизмы. Живые данные и службы не открывались и не изменялись.


### Команды и граница проверки третьего прохода

Прочитан применимый корневой `AGENTS.md`; в затронутых crates дополнительных инструкций не найдено. Из основного checkout выполнены только read-only Git сверки; он чистый и остался на `4f212561708f5918f2a32f43849c0cc65e115857`. Исправления находятся только в `fix/threads-domain-execution`, `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-domain-execution`. Formatter затронул только перечисленные файлы задачи. Незавершённой или непросмотренной подзадачи этого прохода нет.

Фактически выполненные проверки, все с exit code 0 (formatting повторялось после правок):

```sh
cat AGENTS.md
rg --files -g AGENTS.md crates docs
git status --short
git rev-parse HEAD
git rev-parse HEAD main
git branch --show-current
git worktree list
git diff --name-only
git diff --stat
git diff --numstat
cargo fmt --all
cargo fmt --all -- --check
git diff --check
git diff 5485fa9c --check
```

Исходники и связанные callers читались через `rg`, `sed -n` и Git diff; Python heredocs использовались только для редактирования текста, не исполняли код проекта. Для финальной фиксации выполнены следующие команды с отключёнными hooks:

```sh
git -c core.hooksPath=/dev/null add crates/agent/src/agent_loop.rs crates/agent/src/lib.rs crates/crud/src/repositories/agent_domain.rs crates/gateway/src/message/agent_action_tools.rs crates/gateway/src/message/agent_runtime.rs crates/gateway/src/message/tests/cli_task_tools.rs crates/gateway/src/message/tests/recovered_native_turn_stop.rs crates/gateway/src/message/tests/thread_launch_choices.rs crates/gateway/src/message/turn_handlers.rs docs/reviews/threads-domain-execution.md
git diff --cached --check
git diff --cached --stat
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -m "Fix Threads dispatch confirmation and cancellation replay"
git status --short
git rev-parse HEAD
git log -1 --format='%H %s'
```

Итоговый commit/HEAD передаётся в финальном сообщении. Тесты, cargo check, компиляция, сборки, приложение, Gateway, CLI-агенты и генераторы не запускались; компиляция и прохождение тестов не подтверждены. Merge, push и deploy не выполнялись. Это передача на повторное внешнее ревью, не окончательная приёмка.

## Четвёртый проход внешнего ревью: шесть замечаний

Проверенный кандидат и начало этого прохода: `7af503c7318d9078f2fccb271aabc9286e8c579d`. Совокупная база: `bb85dcdf7080164c4dc6480d12410e5e38edade8`. Commits исправлений: `48a4b775ea75ed4c35f55a9da0d2ba4c2761f10d` и `f4db95662fc3c0b00da14eb51980705878603161` (дополнительная ошибка регистрации найдена итоговым самоаудитом). Implementation HEAD: `f4db95662fc3c0b00da14eb51980705878603161`. Последующий commit меняет только этот отчёт, его HEAD передаётся при сдаче.

Ветка: `fix/threads-domain-execution`. Абсолютный worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-domain-execution`. При начале worktree был чистым, HEAD совпадал с кандидатом. Прочитан корневой `AGENTS.md`; вложенные инструкции desktop/client/client-ffi к изменяемым файлам не относятся. Проверены branch, status и worktree list. Основной checkout при проверке чистый, его HEAD — `0e0db2d51f124a617ea6f689e4d56d237b59d9b0`. Основной checkout, соседние worktree и main этой работой не изменялись; rebase/merge не выполнялись.

Этот раздел уточняет более ранние описания registration receipt, terminal replay и exhausted discovery в отчёте.

### Карта исправлений и регрессий

| № | Первопричина | Минимальное исправление и инвариант | Файлы и незапущенная регрессия |
| --- | --- | --- | --- |
| 1 | Registration receipt освобождал actor до canonical acceptance; дальнейшее чтение/commit Gateway могло отказать | Actor сохраняет существующую durable publication до guarded Interrupted, необходимого cleanup и memory sync. В событие передаётся captured durable owner/generation; actor generation/run проверяются существующей control-командой. Completion receipt отличается от process ACK и cached process ACK. После подтверждённого completion не требуются повторные обязательные proof reads | `agent/{agent_loop,lib}.rs`, `protocol/agent_event.rs`, `message/agent_runtime.rs`; `public_initial_stop_retains_completion_across_a_real_terminal_storage_failure` |
| 2 | Root Stop сначала вызывал обычный native CancelTurn у прямого потомка, теряя actor на ACK timeout | У native Threads-потомка сначала guarded Gateway cancellation, затем best-effort signalling. CLI сохраняет существующий cancellation helper с session transition/guard. Ошибка прямого Threads cleanup не выдаётся за успешный Stop корня. Настоящие Tasks сохраняют прежний путь | `message/agent_runtime.rs`; `public_root_stop_finishes_a_direct_threads_child_after_delayed_registration` |
| 3 | Task catalog materialization стирала typed DB cause через ToolError/String | Только committed DurableAgent использует внутреннюю typed materialization. Scope/lease/context и optional TaskCreate proof сохраняют инфраструктурную причину; semantic denial остаётся отказом. Ordinary CLI, MCP invocation и TaskToolProvider сохраняют прежние String/ToolError границы | `message/{turn_handlers,agent_action_tools}.rs`, `turn_mcp/native_tools.rs`, `task_tools/mod.rs`; вариант `catalog` существующей CLI delegated-root регрессии для Codex/Claude/OpenCode |
| 4 | Old terminal snapshot применялся после отдельного durable cleanup и мог затереть resumed generation | Canonical source и owner/generation повторно подтверждаются под существующим ThreadManager write lock. Generic Blocked resume удерживает этот же lock на durable resume и применении canonical InProgress; recovery/provider dispatch идёт после освобождения. Обычный terminal commit сохраняет прежний контракт | `thread/mod.rs`, `message/{agent_runtime,turn_handlers}.rs`; вариант `terminal_resume` recording-регрессии |
| 5 | Terminal replay требовал owner ID прежнего процесса | Terminal acknowledgement использует immutable source action/receipt/author, response binding, workspace, текущий owner/generation и canonical terminal cleanup. Проверка current process owner остаётся обязательной для активной работы и отказа до активации | `message/agent_action_tools.rs`; вариант `terminal_restart`, новый MessageProcessor с другим owner над той же fixture DB; отрицательный resumed N+1 case |
| 6 | Exhausted NULL/timed ветка не имела подходящего индексного диапазона; terminal proof мог постоянно фильтровать исторические строки | Добавлен узкий partial expression index новой миграцией. COALESCE(deadline,'') объединяет NULL и timed в один searchable range с прежней квотой. Legacy closed failed/8/NULL проходит bounded reconciliation и получает ACK только после полного terminal proof; подтверждённый результат выходит из due index через delivered | `crud/repositories/agent_domain.rs`, `message/agent_action_tools.rs`, `migration/{lib,m20261026_000001_agent_start_reconciliation_index}.rs`; реальный discovery/query-plan тест и сохранённый StartAgent → terminal → compaction → legacy replay |

### Уточнения решений и границ

**Stop.** `completion_ack` — усиленный receipt существующей CancelTurn-команды. Он отправляется после durable canonical completion; обычный process ACK и его timeout остались прежними. Actor использует тот же publisher/retry механизм, без нового worker, sleep, backoff или лимита. Captured runtime snapshot даёт настоящий immutable cancellation context; отсутствующий context активного runtime не заменяется фиктивным. На writer остаётся exact generation guard. Смена owner/generation — permanent отказ старому callback, не основание прервать новое исполнение.

Для generation-bound completion внутренний `AgentDurableEvent::TurnInterrupted` получил optional `cancellation_owner`. Это необходимая передача уже захваченного proof между существующими control и durable event механизмами. CLI и обычные producers передают None, его сериализованное представление при None не меняется. Сверены constructors и matches в agent manager tests, CLI projector, protocol tests, Gateway и runtime-events. Новых пользовательских payloads, инструментов или настроек нет.

Native actor сохраняет обязанность при typed storage failure terminal write/read/cleanup. Fenced terminal cleanup для обычных roots/direct Threads завершается до completion ACK; настоящий Task aggregate сохраняет своего владельца cleanup. В Gateway результат true означает именно подтверждённый completion receipt; cached process ACK возвращает false и требует обычный proof path. Отдельно прослежена ошибка дополнительных чтений после прежнего registration receipt: теперь принятый actor outcome не зависит от этих чтений и не вызывает откат provisional Interrupted.

Отдельно проверена повторная initial registration внутри уже переданного Stop: typed storage error сохраняет existing publication retry только при доказанном pending initial actor и Applied CancelTurn того же actor generation. Ошибки без этого control proof и semantic failures сохраняют прежний classifier; общий initial start не получает новую retry-политику. Actor после регистрации не освобождается до terminal acceptance.

**Отмена потомка.** Порядок изменён внутри native fallback после существующего CLI cancellation helper. Поэтому прямой native потомок не теряет actor до guarded canonical cancellation, а CLI session guard не обходится предварительным общим mark. Для parent_task_id=Some прежняя политика и best-effort signalling сохранены. Независимый delegated root по-прежнему имеет собственный work graph и не входит в targets создавшего его root.

**CLI proof.** Общая materialization не дублирует schemas/handlers. Выделен внутренний typed entry point и общая сборка уже допущенного Task bundle; обычный TaskToolProvider оставляет прежнее поведение. На DurableAgent boundary typed storage cause передаётся в existing internal completion/outbox retry до формирования строки Blocked. Ни текущие права principal, ни более поздняя materialization не расширяют frozen catalog, source ceiling или старый manifest. Semantic denial не переводится в retry по тексту ошибки.

**Memory/resume.** Важно совместное владение lock на обеих сторонах: одной дополнительной проверки generation перед отдельным commit недостаточно. `resume_blocked_turn_control` используется existing generic public resume branch; он выполняет только existing durable resume и canonical read под memory lock. Task-owned resume branch не менялся. После возврата запускается прежний recovery coordinator. DB capacity не удерживается во время получения memory lock, fanout или provider/control I/O. В callback под memory lock выполняются только непосредственные DB-операции, каждая освобождает capacity до применения данных в памяти. Проверены обе последовательности: resume применён раньше sync; sync владеет lock раньше resume. Обычный `commit_terminal_turn` по-прежнему возвращает false для незагруженного thread.

**Restart/reconciliation.** Новый процесс может подтвердить завершение, но не получает право активировать чужое текущее исполнение. Writer cleanup повторно проверяет canonical acceptance, terminal Turn/TurnExecution, scope, response execution и generation. Memory synchronization также должна подтвердиться. У активного foreign owner либо resumed N+1 старый outbox не завершает Turn и не освобождает новые ресурсы.

**Индекс и terminal exclusion.** Новый index ограничен pending/failed, attempts=8 и immutable payload kind StartAgent; missing/invalid kind остаётся candidate. Authoritative action kind дополнительно проверяется join, а полная авторизация/terminal proof остаётся в dispatcher/finalizer. Нормальная история других action types не входит в новый рабочий диапазон. Используется существующий инвариант соответствия immutable action/payload kind, проверяемый canonical action producer и compaction; JSON-discriminator не разрешает запуск и не заменяет grant/ledger proof.

Exhaustion/last_error/compaction сами по себе не являются terminal proof. Даже старый confirmed failed/8/NULL сначала проходит existing reconciliation и exact cleanup/sync. Только Finalized/подтверждённый runtime outcome даёт outbox ACK. Для подтверждённой exhausted terminal refusal теперь используется delivered: это подтверждение границы доставки, Turn остаётся Failed/Blocked/Interrupted. Accepted/queued/ACK не объявляются успехом модели. Unresolved строка сохраняет existing scheduled reconciliation, attempts остаются 8, повторного model start нет. Уже ACKed terminal строки исключены статусом из partial index; это также предотвращает постоянный просмотр closed exhausted history. Квоты immediate/timed, размер quantum 64, range 32, limit=2, таймауты и delivery budget не увеличены. Старые миграции не переписаны; новая миграция во время реализации не выполнялась.

### Написанные регрессии

- Initial Stop fixture использует ordinary API admission/preparation, реальный actor и public turn/cancel. После регистрации настоящий fixture trigger отклоняет canonical turn_event INSERT; после снятия trigger один Stop должен завершить canonical Interrupted, exact owner/generation, cancelled execution/resource и released permit, без provider call. Повторный Stop проверяется общим fixture.
- `public_initial_stop_retains_completion_across_registration_storage_failure` сохраняет реальный context, удерживает registration ACK, затем временно делает недоступной fixture-таблицу native_cancellation_context. Ошибка настоящей повторной регистрации должна оставить actor/completion owner; после восстановления хранения тот же публичный Stop завершает canonical Interrupted и общий cleanup, без второго Stop и provider calls.
- Тот же fixture создаёт прямого HomeCapsule Threads-потомка настоящими thread_create/threads_turn_start. Registration потомка удерживается barrier; проверяется started_at, затем public Stop корня, управляемое время за прежним ACK window и освобождение registration. Проверяются оба canonical Interrupted, cancelled execution/resource и освобождение конкретного captured permit потомка, ноль provider calls. Родитель остаётся законно admitted, ещё не активированным root; provider у него также не запускался.
- CLI delegated-root matrix расширена реальной ошибкой authorization read внутри Task catalog finalization после lease registration. Fixture table восстанавливается до caller branch, поэтому terminal persistence доступна и преждевременный Blocked обнаружим. Проверяются InProgress, storage-class retry, отсутствие нового CLI attempt, затем один runtime start и replay без второго.
- Recording StartAgent matrix расширена terminal_resume: настоящий Blocked acceptance и потеря memory-sync/ACK, остановка sync после чтения старого snapshot, existing durable resume/control restoration до N+1, освобождение sync. Проверяются canonical/loaded InProgress, foreground conflict, отсутствие лишнего provider call и permanent rejection старого generation-bound interruption. Для active N+1 новый процесс также не принимает старый terminal ACK.
- Вариант terminal_restart использует другой MessageProcessor/owner над той же DB, exhausted scheduled receipt и настоящий terminal cleanup. Проверяются окончательный ACK, исключение из следующих quanta, loaded canonical Blocked, прежние attempts и отсутствие model calls/resources.
- Query-plan тест вызывает реальный discovery SQL builder на штатной migrated fixture schema, с 2048 closed ledger rows и малым NULL/timed exhausted backlog. Проверяются searchable expression-index range, отсутствие history scan и последовательный прогресс при limit=2 без девятой попытки. Существующая unresolved/ordinary due progress регрессия сохранена. EXPLAIN содержится только в незапущенном тесте.
- В существующей terminal/compaction регрессии дополнительно воспроизводится legacy failed/8/NULL с compacted payload без dispatch, затем подтверждённый ACK и отсутствие повторного выбора. Turn Blocked → execution blocked → resource failed → permit released остаётся прежним production mapping.

### Статический самоаудит

По завершённым участкам и итоговому diff прослежены ordinary native/CLI callers, optional native delivery, control cache/ACK, generation-bound CancelTurn, initial registration, canonical writer, response/resource cleanup, fresh/terminal/exhausted outbox replay, compaction и generic resume. Исправляющий diff проверен относительно `7af503c7`, совокупные изменения сопоставлены с `bb85dcdf` по admission/grants/authorization/dispatch/Stop/replay и соответствующим fixtures. Runtime acceptance, recovery obligation и terminal proof по-прежнему различаются.

В ходе самоаудита исправлены дополнительные композиционные ошибки внутри этих шести пунктов: возврат ordinary commit_terminal_turn для unloaded state; сохранение String/ToolError поведения ordinary CLI/Tasks; общий lock для resume и terminal sync; различение cached process ACK и canonical completion; исключение повторных fallible proof reads после completion; guarded CLI helper до native fallback; bounded draining закрытых exhausted receipts. После этих исправлений соответствующие callers и обе стороны гонок перечитаны повторно.

Навигационные/CRUD callers изменённых contracts и enum initializers сверены поиском и чтением. Formatter не внёс посторонних файлов. `message/compaction_background.rs` изменён только cfg(test) wrapper существующего barrier для межмодульной регрессии. Зависимости, skills, policy blocks, Tasks/Subagents protocols, UI, поиск, recall, Memvid и page limits не изменены.

### Фактически выполненные проверки и ограничения

Разрешённые проверки выполнялись из указанного worktree:

```sh
git rev-parse HEAD
git branch --show-current
git status --short
git worktree list
git -C /Users/alexander/Code/pioneer/pioneer status --short
git -C /Users/alexander/Code/pioneer/pioneer rev-parse HEAD
git log -5 --oneline
git diff --stat
git diff --numstat
git diff --name-only
git diff 7af503c7318d9078f2fccb271aabc9286e8c579d -- crates/gateway/src/message/turn_handlers.rs crates/gateway/src/thread/mod.rs crates/gateway/src/turn_mcp/native_tools.rs
git diff bb85dcdf7080164c4dc6480d12410e5e38edade8 -- crates/gateway/src/authorization crates/gateway/src/turn_mcp/invoker.rs crates/crud/src/repositories/turn_execution.rs crates/crud/src/repositories/native_cancellation_context.rs
cargo fmt --all
cargo fmt --all -- --check
git diff --check
git diff --cached --check
git diff --cached --stat
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -m "Fix Threads Stop ownership and terminal reconciliation"
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -m "Retain initial Stop registration publication on storage failure"
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit --amend --no-edit
```

Первый fmt check показывал форматирование; после форматирования окончательный `cargo fmt --all -- --check` завершился с exit 0. Whitespace-проверки завершились без замечаний. Чтение выполнялось через `rg`, `rg --files`, `sed`, `cat`, `head`, `tail`, `ls`, `pwd`; Python использовался только для редактирования текста исходников, без импорта/выполнения проекта. Содержимое diff также читалось отдельно по изменённым файлам и частям через sed. После найденной ошибки регистрации повторены разрешённые fmt/whitespace checks для изменённых цепочек. Отчёт фиксируется отдельно командой `git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -m "Document fourth Threads execution review corrections"`, затем проверяются `git status --short` и `git rev-parse HEAD`.

Проверки плана, миграции, поведение барьеров, типизация Rust и результаты регрессий фактически не выполнялись. Успех baseline/post-fix тестов не заявляется. При недоступном authoritative proof обработка остаётся fail-closed/pending; raw provider errors и пользовательские payloads в новые диагностические классы не сохраняются. Постоянную недоступность хранения исправления не маскируют новыми retry budgets. Существующие frozen CLI catalogs и старые immutable grants не расширяются; принудительная передоставка живых dev-запусков не выполнялась.

Тесты, cargo check, компиляция, сборки, приложение, Gateway, тестовые CLI-агенты и генераторы не запускались; компиляция и прохождение тестов не подтверждены. Dev/prod-базы, пользовательские данные, службы и конфиги не изменялись. Merge, push и deploy не выполнялись. Это передача на повторное внешнее ревью, не окончательная приёмка.

## Пятый цикл внешнего ревью: девять замечаний

### База и изоляция

Проверенный HEAD этого цикла: `32d997153d211b78ea694170bc5c7de1311919f6`.
Совокупная база осталась `bb85dcdf7080164c4dc6480d12410e5e38edade8`.
Ветка — `fix/threads-domain-execution`, абсолютный worktree —
`/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-domain-execution`.
При продолжении HEAD и ветка совпали с заданием; незакоммиченный diff принадлежал
прерванной работе этого же цикла и был перечитан перед дальнейшими изменениями.
Основной checkout наблюдался чистым, его HEAD в worktree list — `a6d688a7`;
он и соседние worktree не изменялись. Корневой AGENTS.md перечитан; указания о
запуске проверок не применялись ввиду прямого запрета пользователя.

Commit реализации этого цикла: `3243465c2ef8737f409aeb7583d3cf526e28a236`
(`Fix Threads cancellation ownership and replay audit findings`). Последующий
commit обновляет только отчёт; итоговый HEAD передаётся отдельно.

### Карта замечаний, решений и покрытия

| № | Причина → минимальное исправление | Затронутая цепочка/файлы | Написанное или уточнённое покрытие |
| --- | --- | --- | --- |
| 1 | Guard терял типизированный DbErr::Query до регистрации и мог уничтожить initial Stop publisher. При гарантированном mailbox handoff сохраняется owner/generation в существующем control; Gateway сверяет actor/run/batch, workspace, durable owner/generation и повторно control. Только такой publisher сохраняет retry для typed storage failure, в том числе в guard и acceptance lookup. Registered flag больше не служит доказательством владения Stop. | `agent/src/lib.rs`; `gateway/src/message/agent_runtime.rs`: dispatch_cancel_turn_command → lease guard → NativeCancellationContextPrepared → initial registration. | Существующий `public_initial_stop_retains_completion_across_registration_storage_failure` теперь доходит до барьера на фактической ошибке guard при недоступной fixture-таблице. После восстановления тот же public Stop должен завершить canonical Interrupted/cleanup без модели. Общий fixture проверяет владельца, поколение, permit и повторный Stop. |
| 2 | Обычный CancelTurn ждал durable publication до process ACK. ACK возвращён сразу после принятия control cancellation; actor продолжает регистрацию/публикацию, separate completion receipt initial Stop остаётся после canonical acceptance и cleanup. | `agent/src/agent_loop.rs`; неизменённый реальный `TaskAgentExecutor::cancel_run` прослежен до ordinary AgentManager::cancel_turn. | `native_task_cancel_ack_keeps_the_actor_owning_delayed_durable_publication`: настоящий Agent Task, initial registration удерживается, один Task cancel, actor остаётся после управляемого продвижения времени за ACK window, затем Interrupted и ноль provider calls. |
| 3 | После graph fence active-only discovery забывал unfinished responses уже cancelled executions; выход на первом child error терял siblings. Бounded discovery включает точные InProgress responses cancelled direct executions, не настоящие Tasks. Gateway проходит остальных потомков и возвращает первую ошибку после обхода; повтор public Stop снова находит незавершённое. | `crud/src/repositories/agent_domain.rs::cancel_agent_work_graph`; `gateway/src/message/agent_runtime.rs::cancel_root_agent_work_graph_for_turn`. | `public_root_stop_rediscovers_unfinished_children_after_a_graph_fence`: root и два настоящих Threads-потомка. Existing materialization barrier выбирает первого реально обрабатываемого child после fence; trigger отклоняет его canonical INSERT. Проверяются completed sibling cancellation, сохранённый InProgress первого, восстановление хранения и второй public Stop, завершающий все turns/resources без модели. |
| 4 | Accepted-but-pending append считался permanent preparation rejection; idempotent повтор мог принять append за завершённую проекцию. Pending классифицируется отдельно как retry; после idempotent append проверяется canonical projection, и publisher продолжает существующий durable loop до проекции, exact cleanup и guarded memory sync. False sync также остаётся pending. | `gateway/src/message/agent_runtime.rs::commit_initial_native_cancellation` и typed durable event consumer. | `public_initial_stop_waits_for_accepted_cancellation_projection`: реальный unapplied predecessor с удержанным projection claim, один public Stop, accepted terminal при canonical InProgress, затем production projection replay и завершение того же Stop; provider calls — ноль. |
| 5 | Recovery fallback и recovery terminalization применяли старый Blocked через обычный memory commit. Оба caller используют existing guarded canonical synchronization с повторной проверкой snapshot/owner/generation под тем же memory lock, что resume. | `gateway/src/message/agent_runtime.rs`: prepare_native_turn_control fallback, process_due_recovery_terminalizations; существующие ThreadManager guarded sync/resume helpers. | `recovery_terminalization_cannot_apply_blocked_over_a_resumed_generation`: ordinary Threads admission и настоящая preparation/CAS, затем настоящий recovery worker Blocked. Sync задержан после чтения snapshot; existing resume создаёт N+1. После освобождения canonical/loaded остаются InProgress, foreground admission отклоняет конкурента, старое поколение provider не вызывает. Существующий terminal_resume с проверкой старого cancellation generation сохранён. |
| 6 | Seeded loaded thread законно не содержит history/target Turn, но sync требовал ровно один target. После canonical proof отсутствие target теперь подтверждает отсутствие работы в памяти; workspace mismatch, duplicate ID и конфликт реально загруженного Turn сохраняются как отказы. | `gateway/src/thread/mod.rs::commit_terminal_turn_if_current`; `gateway/src/message/agent_runtime.rs::synchronize_projected_native_terminal_turn`. | `terminal_restart` внутри StartAgent matrix теперь явно проверяет пустой loaded thread до/после terminal replay, окончательный delivered/8 ACK, отсутствие следующего due выбора, новых execution/resources/permits и model calls. Отдельный `terminal_memory` по-прежнему проверяет действительно загруженный Turn. |
| 7 | Task resume изменял JSON существующего immutable admission. Ceiling передаётся внутрь readmission и применяется только после fresh legacy admission; ранний persisted return переиспользует JSON без изменений. Public user caller передаёт None, agent caller — свой immutable action ceiling. | `gateway/src/message/task_handlers.rs`; `gateway/src/task_tools/mod.rs`. Все callers двух внутренних seed helpers сверены. | `task_resume_preserves_persisted_admission_but_constrains_a_new_legacy_seed`: настоящий blocked Agent Task с create-grants; другой живой Task execution той же capsule без этих grants вызывает task_resume через MCP facade. Persisted admission сравнивается целиком, caller остаётся без create-grants. Второй вариант моделирует отсутствие legacy admission и проверяет, что fresh admission не приобретает create-grants. |
| 8 | Immediate selector уже исключал terminal failures, partial index продолжал включать closed failed/1..7/NULL. Новая транзакционная миграция заменяет только immediate index точным predicate selector; down восстанавливает прежний predicate. | Новая `migration/src/m20261026_000002_agent_action_immediate_terminal_index.rs`, регистрация в `migration/src/lib.rs`; прежние миграции не менялись. | Existing plan/workload test расширен 2048 closed terminal-class failed/1..7/NULL, малым immediate/timed backlog и пустым backlog после claim. Реальные selector plans проверяются вместе с отрицательной eligibility проверкой: узкий index не должен планировать прежний широкий range. Exhausted NULL/timed, terminal exclusion, unresolved progress и limit=2 тесты сохранены. EXPLAIN не выполнялся. |
| 9 | Rust optional tuple cancellation_owner отсутствовал в checked-in schema. Schema исправлена вручную: optional/null либо tuple string/uint64 длины 2, с действующим описанием. Поле не добавлено в required. | `schemas/agent_durable_event.json`; wire regression в `protocol/src/agent_event.rs`. | Existing durable TurnInterrupted test проверяет omission для None, чтение старой формы без поля, сериализацию Some в двухэлементный array и round trip. Генератор не запускался. |

### Инварианты и статический самоаудит

По завершённым участкам перечитаны новые изменения, связанные callers и
сохраняемая реализация от совокупной базы. Отдельно прослежены ordinary
native/CLI admission, lease registration/catalog proof, committed preparation,
activation CAS, once-only dispatch, terminal/outbox replay и Stop.
Typed CLI proof chain предыдущего цикла не переписывалась и не переводилась в
String; прежние allow/deny, frozen bindings, immutable grants и resource gates
сохраняются. Независимый root и настоящий AgentExecution author не менялись.

Control handoff записывается только после mailbox reservation и до отправки
команды/OOB cancellation. Новое поле является private памятью существующего
TurnExecutionControl, связанной с его lifetime; таблица или альтернативный
cancellation protocol не создавались. Оно нужно, потому что флаг регистрации
описывает фазу, но не обязанность уже переданного Stop. Actor generation/run
и durable owner/generation сверяются отдельно. Изменение registered flag само
по себе не отнимает эту обязанность; после очистки control или смены владельца
старый proof не разрешает terminal commit. Typed infrastructure error не
разрешает работу при недоступной авторизации и не заменяет semantic denial.

Проверены обе стороны phase change, registration ACK, ошибки guard до записи
context, ошибки после context registration и до canonical acceptance,
accepted pending projection и idempotent повтор append. Registration receipt
не считается canonical cancellation. Completion receipt остаётся после
projection/cleanup/sync; ранний process ACK обычной команды не выдаётся за
канонический результат. При неизвестной предыдущей активации модель повторно
не запускается. Immutable context для активного native runtime обязателен.

Graph discovery остаётся ограниченным существующим max_total_nodes и точным
work_graph_root_execution_id; дополнительный повтор охватывает только direct
cancelled executions с unfinished response. Task aggregate cancellation и
семантика attached Subagents не изменены. Общая узкая коррекция process ACK
устраняет задержку TaskAgentExecutor cancellation без изменения его протокола,
политик или таймаутов. Права Task caller не расширяются persisted admission
другого исполнения.

Terminal cleanup и memory sync сохраняют разные подтверждения. Recovery
callers больше не применяют переданный старый Turn snapshot без повторной
canonical проверки. Shared resume lock закрывает промежуток между proof и
memory application; N+1 остаётся защищённым. Для unloaded target нет фиктивной
загрузки истории и нет снятия проверок настоящего memory conflict.

DB calls идут через существующие CrudStore/repositories и scoped handles.
Ни control ACK, ни ожидание барьера/публикации, ни provider/runtime I/O не
выполняются при удержании DB capacity. Graph переход и индексная миграция
атомарны; времена ожидания, восемь delivery attempts, диапазоны 32 и quantum
64 не изменены. Closed history исключается predicate индекса, а не увеличением
LIMIT и не изменением delivery/reconciliation policy.

В ходе финального прохода исправлены проблемы самих fixtures: корректные
поля AgentMessage predecessor, настройка настоящего Task cleanup bridge для
recovery worker, выбор первого фактически обрабатываемого graph child вместо
предположения о порядке ID, отдельное управляющее execution в Task resume
сценарии и проверка результата по durable Task status. Formatter не внёс
изменений вне перечисленных файлов. Skills, policy blocks, зависимости, UI,
поиск, recall, Memvid и пользовательские данные не менялись.

### Фактически выполненные команды и ограничения

Из указанного worktree выполнялись следующие Git/formatting команды
(некоторые чтения/форматирование повторялись после новых изменений):

```sh
pwd
git status --short
git status --short --untracked-files=all
git rev-parse HEAD
git branch --show-current
git worktree list
git -C /Users/alexander/Code/pioneer/pioneer status --short
git diff --stat
git diff --name-only
git diff --numstat bb85dcdf
git diff --check
cargo fmt --all
```

Дополнительно читались полные/частичные source diffs командами `git diff --
<перечисленные файлы>` и `git diff bb85dcdf -- <перечисленные файлы>`;
навигация выполнялась `rg`, `rg --files`, `cat`, `sed`, `head`, `tail`.
Python использовался только для текстовых правок исходников и schema, без
импорта или выполнения проекта. Один навигационный rg с shell glob завершился
ошибкой отсутствия совпадений; нужное определение затем прочитано по точному
пути. Это не проверка проекта.

Окончательные разрешённые проверки и фиксация результата указаны в конце
этого раздела после их выполнения. Никаких тестов, миграций, EXPLAIN,
benchmark, генератора или исполняемого кода проекта в этом цикле не запускалось.
Новая миграция и планы остаются непроверенными исполнением. Сценарии гонок,
реальные Task/Threads fixtures и wire tests написаны, но их результаты не
подтверждены. Постоянная недоступность хранения оставляет существующего
publisher/reconciliation pending; новая бесконечная система повторов не
добавлялась. Frozen CLI catalogs и старые immutable manifests не расширялись,
живые dev-запуски не возобновлялись вручную.

Тесты, cargo check, компиляция, сборки, приложение, Gateway, тестовые CLI-агенты
и генераторы не запускались; компилируемость и прохождение тестов не
подтверждены. Merge, push и deploy не выполнялись. Это передача на повторное
внешнее ревью, не окончательная приёмка.

### Результат разрешённых проверок пятого цикла

Окончательные `cargo fmt --all -- --check`, `git diff --check` и
`git diff --cached --check` завершились с exit 0. После последних изменений
повторное форматирование и fmt check также завершились с exit 0.
Перечень formatter diff сверялся через `git diff --name-only`,
`git diff --numstat`, `git diff --cached --stat`: посторонних файлов нет.
Новая индексная миграция включена в commit, но не выполнялась.

Фактически использованные команды фиксации/финальной проверки:

```sh
cargo fmt --all
cargo fmt --all -- --check
git diff --check
git diff --cached --check
git diff --cached --stat
git diff --name-only
git diff --numstat
git add crates/agent/src/agent_loop.rs crates/agent/src/lib.rs crates/crud/src/repositories/agent_domain.rs crates/crud/src/repositories/agent_domain/action_outbox_tests.rs crates/gateway/src/message/agent_runtime.rs crates/gateway/src/message/task_handlers.rs crates/gateway/src/message/tests/cli_task_tools.rs crates/gateway/src/message/tests/recovered_native_turn_stop.rs crates/gateway/src/message/tests/thread_launch_choices.rs crates/gateway/src/task_tools/mod.rs crates/gateway/src/thread/mod.rs crates/migration/src/lib.rs crates/migration/src/m20261026_000002_agent_action_immediate_terminal_index.rs crates/protocol/src/agent_event.rs schemas/agent_durable_event.json
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -m "Fix Threads cancellation ownership and replay audit findings"
git rev-parse HEAD
git status --short
git log -2 --oneline
```

Отчёт фиксируется отдельно с тем же отключением hooks командой
`git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -m "Document fifth Threads execution audit corrections"`;
перед ней выполняются `git diff --check`,
`git add docs/reviews/threads-domain-execution.md`, `git diff --cached --check`,
а после — `git status --short` и `git rev-parse HEAD`.

Форматирование подтверждает только форматирование, не типизацию или поведение.
Тесты, cargo check, компиляция, сборки, приложение, Gateway, тестовые CLI-агенты
и генераторы не запускались; компилируемость и прохождение тестов не
подтверждены. Это передача на повторное внешнее ревью, не окончательная приёмка.
