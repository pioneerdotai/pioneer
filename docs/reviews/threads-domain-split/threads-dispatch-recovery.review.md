# Threads dispatch/recovery: статический самоаудит части 4

Результат подготовлен к внешнему ревью. Это не приёмка части или совместной интеграции четырёх частей. Исторический `docs/reviews/threads-domain-execution.md` не использовался как доказательство корректности или внешней приёмки и не изменён.

## Ревизии и изоляция

- Ветка: `fix/threads-dispatch-recovery`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-dispatch-recovery`.
- Фактический начальный HEAD: `ab5be79064624dd6ffeb05e247e09ddc005bcca0`.
- Начальное дерево: чистое, включая отсутствие staged/unstaged/untracked изменений.
- База собственной части: `4ea81a4c67a9d24876e4333989dae1955b00b943`.
- Родитель для будущего PR: `fix/threads-turn-cancellation`.
- Итоговый commit исправлений: `e98bb3aeffb3ba9d353ea78052e237b78f211899`.
- Следующий commit добавляет только этот отчёт. Его окончательный SHA передаётся отдельно в handoff, поскольку файл не может содержать hash собственного commit.
- Новая база от родительского ревьюера в этом запуске не передавалась. Аудит выполнен относительно указанной базы, включая перенесённый код и текущие исправления.
- Исходный кандидат `9d86bc3eb65cc58f0e67bf3022a545dfec0b14b6` и совокупная база `bb85dcdf7080164c4dc6480d12410e5e38edade8` служат только контекстом разделения. Интеграция с ними или main не выполнялась.

Изменения и commits выполнены только в указанном worktree. Другие ветки/worktree, dev/prod-базы, службы, конфиги и установленные skills не изменялись. Merge, rebase, push, deploy и создание PR не выполнялись. Hooks отключены при обоих commits.

## Инструкции и область

Прочитаны корневой `AGENTS.md` worktree, `AGENTS.md` физического родительского checkout и указанные последним CRUD/migration rules. В самом worktree дополнительных CRUD/migration AGENTS нет. Для написания регрессий прочитаны `test-audit`, его Pioneer/audit references, manifests затронутых crates и CI. Прямое разрешение пользователя исправлять production и запрет выполнять проверки имеют приоритет над несовместимыми рекомендациями skill/AGENTS. Переданные Pioneer Tasks/Subagents/HTML/Threads skills прочитаны; внешние операции и делегирование не потребовались.

Проверен собственный продуктовый diff от `4ea81a4c67a9d24876e4333989dae1955b00b943`, в том числе:

- `crates/gateway/src/message/agent_action_tools.rs`: wrapper/inner dispatch, типизированный bookkeeping, rejected-start reconciliation и terminal replay.
- `crates/crud/src/repositories/agent_domain.rs`: discovery, claim/lease/attempt bookkeeping, failure classes, terminal ledger compaction; re-exports в `crates/crud/src/lib.rs`.
- `crates/crud/src/repositories/agent_domain/action_outbox_tests.rs`, Gateway `message/tests/{agent_action_outbox,thread_launch_choices,cli_task_tools}.rs`.
- Две миграции `m20261026_000001_agent_start_reconciliation_index.rs`, `m20261026_000002_agent_action_immediate_terminal_index.rs` и регистрация в Migrator.
- Handoff README/prompt и сохранённый исторический отчёт рассмотрены отдельно от продуктовых доказательств.

Новые коррекции затронули семь Rust-файлов: dispatcher, CRUD repository, CRUD outbox tests, Gateway outbox tests, Gateway launch tests и обе новые миграции. Этот review-файл — единственное новое дополнение к handoff. `agent_runtime.rs`, общие primitives, CLI preparation, Migrator, re-exports и исторические документы в данном проходе не менялись.

## Найденные проблемы и исправления

| Проблема в начальном HEAD | Исправление | Основное незапущенное покрытие |
| --- | --- | --- |
| Exhausted child reconciliation связывал response с родителем, но проверял source action через immutable grant только у независимого root. Перенаправленный payload мог завершить sibling того же родителя. | Один сохранённый grant проверяется для root и child: source action/execution, response execution/Turn, parent, identity, presentation snapshot и workspace. Существующие action/receipt/author/generation checks сохранены. Legacy compaction использует grant только для восстановления terminal-связи. | Новый `exhausted_child_start_cannot_reconcile_a_sibling_from_another_action`: два child через настоящий tool admission; перенаправление первого outbox к sibling не меняет terminal/resources/grant. Восстановление исходного payload подтверждает, что собственная связь остаётся reconcilable. |
| При accepted-but-unprojected terminal обычный dispatcher мог вернуть Delivered из отдельного acceptance shortcut; exhausted finalizer мог вернуть RuntimeOwned по прежнему runtime evidence. Cleanup и memory sync оставались неподтверждёнными. | Accepted terminal входит в общий terminal proof path до runtime/recovery shortcuts. Пока canonical Turn InProgress, finalizer сохраняет Unresolved. ACK после проекции проходит существующие fenced cleanup и guarded memory sync. | Новый `start_outbox_waits_for_accepted_terminal_projection_before_acknowledging_runtime`: реальный dispatch до recording provider; существующий append-only fault seam моделирует потерю projector. Обычная и exhausted ветви не ACK до production projection replay; затем проверяются ACK, memory, resource cleanup и отсутствие повторного model call. |
| Compaction выбирал старые failed/8/NULL StartAgent без terminal receipt, пропускал их после LIMIT и мог постоянно оставлять следующие closed tuples за quantum. | Unresolved exhausted StartAgent исключены до LIMIT через коррелированный lookup canonical action по PK. Не-StartAgent exhaustion и подтверждённые terminal refusals сохраняют eligibility; последующие safety checks и write fences сохранены. | Расширен `terminal_action_ledger_compaction_preserves_window_and_exact_replay_hashes`: более старый unresolved StartAgent перед closed tuple, limit=1; compacted только closed tuple, unresolved payload сохранён. |
| Exhausted Rejected ACK преобразовывал даже `Ok(false)` от CAS в `true`, завышая delivered count. | Возвращается фактический результат ACK CAS. | Расширен `start_outbox_refusal_stops_delivery_without_inflating_attempts_and_unknown_error_is_safe`: другой ACK уже подтвердил claim; проигравший CAS не считается второй доставкой. |

Дополнительно две новые draft-миграции приведены к применимым migration rules: вместо raw DDL используются SchemaManager/SeaQuery builders и локальные Alias. Их имена, очередность, индексируемые поля, NULL/CASE/COALESCE semantics, partial predicates и прежний down-контракт сохранены. У immediate replacement сохранена транзакция. Указана стоимость однократного построения индексов на существующем ledger. Historical migrations не менялись.

Существующая `exhausted_discovery_uses_indexed_null_and_timed_ranges_with_a_large_closed_ledger` дополнена rollback двух новых миграций на заполненном ledger, проверкой восстановления широкого immediate index, отсутствия exhausted index, повторным upgrade/reapply и прежними production discovery/plan/progress assertions. Fresh install продолжает покрываться существующими fixtures. Миграции и планы не исполнялись.

## Зависимости и проверенные цепочки

Общие failure/storage classifiers и API/CLI activation primitives части 2 перечитаны без переписывания. Подтверждены существующие signatures `prepare_committed_agent_api_turn`, `prepare_committed_agent_cli_runtime_turn`, `activate_prepared_committed_agent_cli_runtime_turn` и их callers.

Из части 3 использованы существующие `materialize_unactivated_turn_refusal_owned`, `finalize_terminal_agent_turn_owned`, `synchronize_projected_native_terminal_turn` и `ThreadManager::commit_terminal_turn_if_current`. Их реализации и recovery/resume callers прочитаны; дубликаты и изменения общей зависимости не потребовались. Отдельная recovery terminalization/resume race из части 3 не дублировалась и не менялась.

| Цепочка | Результат статического прохода |
| --- | --- |
| Contract/input → authorization → сохранение | Сверены создание child/root grant, source action, receipt, input author, response binding и durable dispatch. Дополнительные проверки finalizer сверяют сохранённые факты, не выдают новые права. Initiating user, explicit allow/deny, immutable ceilings, scope и opaque routes не изменены. |
| Dispatch/retry → runtime → результат | Storage failures остаются типизированными и не становятся definitive denial. Invalid/denied/runtime-unavailable/conflict различаются с control/preparation/unknown outcomes. Подготовленный snapshot и успешный CAS без model admission не объявляются успехом. После API helper проверяется runtime/recovery/terminal boundary; CLI использует собственную preparation/attempt цепочку. |
| Accepted terminal → cleanup → memory → ACK | Отдельно прослежены acceptance и projection. Pending terminal сохраняет outbox obligation. Fenced writer повторно проверяет owner/generation и canonical terminal; guarded sync проверяет текущую проекцию под общим с resume memory lock. Rejected/Delivered подтверждают границу доставки, не успешный ответ модели. |
| Exhaustion/restart/replay | Claim exhausted StartAgent сохраняет attempts=8 и переводит только в reconciliation. Его dispatcher не запускает модель снова. Foreign live owner остаётся защищённым; новый процесс может подтвердить уже canonical terminal. Loaded thread без target Turn не требует фиктивной гидратации history. |
| Resume N+1 | Terminal replay/cleanup используют существующие generation fences. Прочитаны guarded recovery terminalization и fallback callers: старый Blocked snapshot не применяется напрямую. Сохранены existing terminal_resume и foreign-owner отрицательные сценарии. |
| Discovery/indexes | Immediate predicate согласован с terminal-class partial index; exhausted CASE/COALESCE predicate и ordering согласованы с expression index. NULL и timed candidates делят прежний timed quantum; UNION/dedup сохраняют limit=2, range=32 и общий limit=64. Exact lease revalidation и bounded poison deferral сохранены. |
| Compaction | Сохраняются action/owner/kind, spawned execution, Turn/execution generation, hash marker и launch request fingerprint. Input/launch/runtime payload не восстанавливаются ради replay. Неизвестное exhaustion не считается terminal receipt. Preparation и hash/JSON обработка остаются вне write transaction; существующий write CAS сохранён. |
| Scheduling/workload | Прочитаны resilience worker callers: discovery/claim/bookkeeping и compaction наследуют maintenance handle; narrow correctness calls используют существующий scoped store. DB priority, квоты, интервалы, lease duration и восемь delivery attempts не увеличены. Нет control/provider I/O при удержании DB capacity. |

Весь новый diff и непосредственно затронутые callers перечитаны после правок. Отдельно проверены существующие matrices: `agent_action_outbox` с poison/ACK/panic/fault cases; CRUD ranges/plans/workload/cancellation; StartAgent failure/retry/restart/resume; admitted CLI Codex/Claude/OpenCode с lease/context/catalog proof faults. Тестовые ожидания сопоставлены с production status mapping: Turn Blocked, AgentExecution blocked, resource failed, permit released.

Authoring gate для новых регрессий: основной owner — production outbox dispatch; sibling подмена и pending projection с живым runtime — разные lifecycle/authority gaps, не дублирующие Stop или recovery race. Append-only seam создаёт реальный durable pending event и не подменяет результат dispatcher/finalizer. Compaction и ACK CAS расширяют прежние owner tests. Static pre-fix trace показывает обнаруживаемый каждым тестом дефект; baseline/fixed запуск не выполнялся, прохождение не заявляется.

## Фактически выполненные команды

Все изменения выполнены из указанного worktree. Для чтения использовались `pwd`, `rg`, `rg --files`, `cat`, `sed -n`, `head`, `tail`, `git diff`, `git log`; правки — `apply_patch`. Вне worktree читались только инструкции/skills и установленный source SeaQuery 1.0.2 / SeaORM migration 2.0.4 для проверки доступных builder signatures; зависимости и skills не изменялись. Первые попытки прочитать отсутствующие локальные CRUD/migration AGENTS и один неверный source path в registry дали ошибки чтения; нужные инструкции и определения затем найдены и прочитаны.

Git-инспекция (часть команд повторялась):

```sh
git status --short
git branch --show-current
git rev-parse HEAD
git log -4 --oneline
git diff --stat 4ea81a4c67a9d24876e4333989dae1955b00b943..HEAD
git diff 4ea81a4c67a9d24876e4333989dae1955b00b943..HEAD -- <файлы собственной части>
git diff --name-only
git diff --numstat
git diff --stat
git diff -- <затронутые файлы>
```

Разрешённые formatting/whitespace проверки завершились без ошибок:

```sh
cargo fmt --all
cargo fmt --package pioneer-crud -- crates/crud/src/repositories/agent_domain/action_outbox_tests.rs
cargo fmt --all -- --check
cargo fmt --package pioneer-crud -- --check crates/crud/src/repositories/agent_domain/action_outbox_tests.rs
git diff --check
git diff 4ea81a4c67a9d24876e4333989dae1955b00b943 --check
git diff --cached --check
git diff --cached --stat
```

Staging ограничен перечисленными семью Rust-файлами; отчёт фиксируется отдельно. Команды commits:

```sh
git -c core.hooksPath=/dev/null add <семь явных путей Rust-файлов выше>
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -m "Fix Threads outbox terminal reconciliation and compaction"
git -c core.hooksPath=/dev/null add docs/reviews/threads-domain-split/threads-dispatch-recovery.review.md
git diff --cached --check
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -m "Document Threads dispatch recovery self-audit"
git status --short
git rev-parse HEAD
```

## Ограничения передачи

Тесты, cargo check, компиляция, сборки, приложение, Gateway, тестовые CLI-агенты и генераторы не запускались. Миграции, EXPLAIN и benchmarks также не запускались. Компилируемость и прохождение тестов не подтверждены. Formatting подтверждает только форматирование; builder-generated SQL и использование индексов требуют отдельно разрешённого исполнения.

Повреждённый ledger без точной grant/response/owner связи остаётся unresolved с bounded reconciliation, а не выдаётся за завершённый. Старые frozen manifests и неизвестная активация не получают новых прав или повторного model start. Для уже принятого terminal с недоступной проекцией/cleanup/sync сохраняется прежнее outbox obligation.

Следующий этап — внешнее ревью этой части. Перенос принятых исправлений и совместную интеграцию выполняет родительский ревьюер. Общая приёмка возможна после ревью и интеграции всех четырёх частей; даже приёмка не разрешает запуск проверок без отдельного согласия пользователя.
