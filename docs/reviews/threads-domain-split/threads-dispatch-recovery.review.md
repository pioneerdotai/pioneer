# Threads dispatch/recovery: статический самоаудит части 4

Результат подготовлен к внешнему ревью. Это не приёмка части или совместной интеграции четырёх частей. Исторический `docs/reviews/threads-domain-execution.md` не использовался как доказательство корректности или внешней приёмки и не изменён.

Актуальная передача после P2 о совместимости исторического child grant описана в последнем разделе. Предыдущие разделы сохраняют историю самоаудита и трёх исправлений внешнего аудита; уточнения ниже имеют приоритет.

## Ревизии и изоляция

- Ветка: `fix/threads-dispatch-recovery`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-dispatch-recovery`.
- Фактический начальный HEAD: `ab5be79064624dd6ffeb05e247e09ddc005bcca0`.
- Начальное дерево: чистое, включая отсутствие staged/unstaged/untracked изменений.
- База собственной части: `4ea81a4c67a9d24876e4333989dae1955b00b943`.
- Родитель для будущего PR: `fix/threads-turn-cancellation`.
- Commit исправлений первого самоаудита: `e98bb3aeffb3ba9d353ea78052e237b78f211899`.
- Commit первого отчёта, проверенный внешним аудитом: `2a7ebe79daf8351e938f22b44ffb682ad0630689`.
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

В первом самоаудите обе новые draft-миграции были переведены на SchemaManager/SeaQuery builders и локальные Alias. Внешний аудит подтвердил ошибку этого решения для expression index: SQLite builder SeaQuery 1.0.2 не поддерживает IndexColumn::Expr. В актуальном исправлении только его создание возвращено к статическому raw DDL с документированным исключением; вторая миграция сохранена. Имена, очередность, поля, NULL/CASE/COALESCE semantics, partial predicates, down-контракт и транзакция immediate replacement не изменены. Стоимость однократного построения индексов описана в коде. Historical migrations не менялись.

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

## Повторное внешнее ревью: три подтверждённых замечания

Начальный HEAD этого прохода — `2a7ebe79daf8351e938f22b44ffb682ad0630689`, дерево было чистым, ветка и абсолютный worktree совпали с заданными. База собственной части остаётся `4ea81a4c67a9d24876e4333989dae1955b00b943`; переносов соседних частей или обновления базы не было.

Commit исправлений: `92f9445cc1158d35920ab5878db742ad29d20b02` (`Fix Threads expression index and terminal source binding`). Следующий commit обновляет только этот отчёт; его SHA передаётся отдельно в финальном handoff.

### Исправления и регрессии

1. **SQLite expression index.** Прочитана полная цепочка установленной SeaQuery 1.0.2: `src/index/common.rs` преобразует FunctionCall и Expr в IndexColumn::Expr, `src/backend/sqlite/index.rs` использует default prepare_index_columns из `src/backend/index_builder.rs`, где Expr вызывает panic. Создание индекса в `m20261026_000001_agent_start_reconciliation_index.rs` использует статический raw DDL через существующее SchemaManager connection. Это конкретное отсутствие операции в builder, разрешённое migration rules; замена Func на Expr::cust не применялась. Сохранены IF NOT EXISTS, имя, COALESCE(next_attempt_at,''), created_at/id, status/attempts predicate и CASE/json_valid/json_extract. Down продолжает удалять тот же индекс через поддерживаемый builder; регистрация в Migrator и вторая миграция не менялись. Установку по-прежнему покрывают существующие fresh-install fixtures и exhausted rollback/reapply regression; теста на текст SQL не добавлено.

2. **Source action до ordinary terminal cleanup/ACK.** Из finalizer минимально выделена приватная `agent_start_grant_matches_source`. Оба callers проверяют сохранённый grant: source_action_id = row.action_id, source_execution_id = row.owner_execution_id, execution_id, Turn, parent и identity. Ordinary dispatcher также связывает row owner с input author до terminal/runtime shortcuts; у root прежняя независимая provenance/revalidation проверка сохранена. Ошибка чтения сохраняет typed DB cause, semantic mismatch возвращает InvalidStart. Wrapper повторно вызывает finalizer, где тот же mismatch даёт Unresolved до любых terminal writes, cleanup или memory sync. В exhausted ветви остаётся тот же guard без нового model start и без девятой попытки.

   Расширен существующий `exhausted_child_start_cannot_reconcile_a_sibling_from_another_action`: общий fixture реально допускает HomeCapsule children A/B через tools. При attempts=0 B завершается production pre-activation Stop до projected canonical Interrupted; затем payload A перенаправляется на B и идёт через production outbox processing. Проверяются отсутствие ACK A, attempts=1 и назначенный retry, сохранность собственного Turn/execution/resources A, а также canonical Turn, loaded memory, owner, execution, resources и grant B. Прежний exhausted случай с живым B и attempts=8 сохранён, дополнен проверками A; после восстановления исходного payload сохранён положительный reconciliation-контроль. Dispatcher result не подставляется, новые production test hooks не создавались.

3. **Историческая migration-fixture.** В `action_outbox_index_migration_and_marker_rollback_and_retry_are_atomic` сохранены исходный ограниченный upgrade, rollback индексов и marker, namespace-collision failure, повтор установки и проверки исходных строк. Только после них добавлен штатный `Migrator::up(&db, None)` перед заключительным `ids()`. Текущий discovery получает необходимый exhausted index. Production INDEXED BY, selectors и квоты не менялись; fallback со сканированием не добавлен.

Затронуты ровно четыре Rust-файла: expression migration, `message/agent_action_tools.rs`, `message/tests/thread_launch_choices.rs`, `repositories/agent_domain/action_outbox_tests.rs`; дополнительно обновлён этот отчёт. Вторая миграция, зависимости, shared primitives частей 2–3, grants и Tasks/Subagents policies не менялись.

### Статический самоаудит

- Перечитаны собственный продуктовый diff от базы `4ea81a4c…` и непосредственно связанные grant producer/loader, independent-root proof, ordinary API/CLI preparation/activation, bookkeeping, discovery/compaction и terminal/recovery callers. Проверены существующие Model/TurnExecutionRecord signatures и PartialEq для расширенных assertions, без предположений о будущих definitions.
- Прослежены обе ветви перенаправленного payload. Ordinary direct child теперь не доходит до terminal cleanup B и не подтверждает A; exhausted finalizer останавливается на том же immutable source mismatch. Helper не выдаёт права и не записывает данные. Подготовка/JSON parse и runtime/control остаются вне DB capacity.
- Exact grant proof предшествует terminal shortcut, но не заменяет response/presentation, parent, workspace, execution generation и owner fences. Terminal ACK по-прежнему требует projected canonical Turn, существующий fenced cleanup и guarded memory sync. Pending projection остаётся unresolved; Rejected/Delivered не означают успех модели.
- Сохранены restart и resumed N+1 ограничения: canonical terminal другого процесса может подтверждаться, активный foreign owner не активируется; old snapshot не применяется поверх resume. Shared writer и memory-lock primitives, а также recovery terminalization/resume race части 3 не изменялись.
- Для миграции проверены up/down/reapply, точное соответствие raw partial predicate и expression ordering текущему discovery, а также независимость second migration от unsupported expression columns. Историческая fixture сначала завершает свой marker/rollback/retry контракт, затем доходит до актуальной схемы. NULL/timed ranges, poison progress, attempts, quotas и bounded quantum сохранены.
- Authoring gate: новый ordinary вариант проверяет отдельный lifecycle gap, который прежний exhausted-only вариант не достигал. На исходном HEAD parent-only guard пропускал terminal B и ACK-ал A; это ожидаемая причина падения расширенной регрессии. Baseline и исправленная версия не запускались, прохождение не заявляется.

### Команды и ограничения этого прохода

До правок прочитаны применимые AGENTS.md, prompt и review. Для чтения использовались `pwd`, `rg`, `rg --files`, `cat`, `sed -n`, `head`, `tail`, `git diff` (включая полный продуктовый diff от базы и `git diff -w` изменённой регрессии). Прочитаны manifests, CI, test-audit references и три указанных source-файла установленной зависимости; код зависимости не исполнялся и не менялся. Один навигационный shell glob без совпадений и поиск отсутствующего `types.rs` были исправлены чтением существующих путей; это не проверки проекта.

Фактически выполнены допустимые formatting/whitespace проверки, все завершились с exit 0:

```sh
cargo fmt --all
cargo fmt --package pioneer-crud -- crates/crud/src/repositories/agent_domain/action_outbox_tests.rs
cargo fmt --all -- --check
cargo fmt --package pioneer-crud -- --check crates/crud/src/repositories/agent_domain/action_outbox_tests.rs
git diff --check
git diff 4ea81a4c67a9d24876e4333989dae1955b00b943 --check
git diff --cached --check
```

Выполнены `git branch --show-current`, `git rev-parse HEAD`, `git status --short --untracked-files=all`, `git diff --stat`, `git diff --name-only`, `git diff --numstat`, `git diff --cached --stat`. Product staging содержал четыре явных пути выше. Фиксация исправлений и отчёта выполняется с отключёнными hooks:

```sh
git -c core.hooksPath=/dev/null add <четыре явных Rust-пути выше>
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -m "Fix Threads expression index and terminal source binding"
git -c core.hooksPath=/dev/null add docs/reviews/threads-domain-split/threads-dispatch-recovery.review.md
git diff --cached --check
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -m "Document Threads external audit corrections"
git status --short --untracked-files=all
git rev-parse HEAD
```

Основной checkout, соседние worktree, dev/prod-базы, службы, конфиги и установленные skills не изменялись. Merge, push, deploy, создание PR и интеграция частей 1–3 не выполнялись.

Тесты, cargo check, компиляция, сборки, приложение, Gateway, тестовые CLI-агенты, миграции, EXPLAIN, benchmarks и генераторы не запускались; компилируемость и прохождение тестов не подтверждены. Это передача на повторное внешнее ревью части 4 без окончательной приёмки. После полного ревью и приёмки для запуска проверок всё равно требуется отдельное разрешение пользователя.

## Повторное внешнее ревью: P2, исторический child grant

Начальный HEAD этого прохода — `ca8847597f62fadb839d01cd7b8e24bd4d49362d`; фактические ветка `fix/threads-dispatch-recovery` и worktree `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-dispatch-recovery` совпали с заданными. Начальное дерево было чистым, включая untracked. База собственной части остаётся `4ea81a4c67a9d24876e4333989dae1955b00b943`; обновления базы и интеграции частей не было.

Commit исправления и регрессии: `8bdd43bde7b8045b2bffc0b1371b1fb0b4d394a4` (`Support historical child grants in Threads dispatch recovery`). Следующий commit содержит только обновление этого отчёта; его SHA передаётся отдельно, поскольку отчёт не может ссылаться на собственный commit.

### Причина и минимальное исправление

Через `git show bb85dcdf7080164c4dc6480d12410e5e38edade8:crates/gateway/src/message/agent_action_tools.rs` проверен реальный исторический producer: `child_grant_json` не содержал `source_action_id`, `source_execution_id`, `turn_id`, но input Turn уже получал `canonical_agent_id('T', "agent-start-turn\0" + action_id)`. Текущий producer сохраняет ту же формулу ID. Прочитаны `load_agent_execution_grant` и `upgrade_agent_execution_grant_model_to_current`: fingerprint проверяется в сохранённом представлении, upcast касается вложенного child-launch ceiling и не добавляет source-полей. Поэтому обязательная проверка всех трёх полей ошибочно блокировала корректный старый dispatch и terminal reconciliation.

Изменены только `crates/gateway/src/message/agent_action_tools.rs`, существующая регрессия в `crates/gateway/src/message/tests/thread_launch_choices.rs` и этот отчёт.

- Общий приватный `agent_start_grant_matches_source` сохраняет строгую проверку нового формата. Историческая ветвь разрешена только для `kind=agent_child`, когда все три source-поля отсутствуют. Наличие null, неверного типа, частичного набора или противоречащего значения не включает fallback. Независимые roots продолжают использовать строгие source-поля и прежнюю provenance-проверку.
- Для исторического child проверяются сохранённые grant parent/identity/execution, parent execution и JSON parent против row owner, точный action-derived input Turn ID, committed StartAgent action того же owner и allowed receipt с тем же agent actor. Оба callers дополнительно проверяют сохранённого автора Turn и точный response execution/presentation; прежние workspace, parent thread, identity/profile, owner/generation checks остаются на месте. Совпадение одного parent не является доказательством: Turn sibling не соответствует action-derived ID, а сочетание собственного Turn с чужим execution отсекается response binding.
- Обычный dispatch и finalizer используют один async helper до terminal cleanup/ACK. Дополнительные reads идут через существующие scoped CRUD handles и repositories; типизированные ошибки хранения сохраняются. Helper только читает факты и не удерживает DB capacity во время runtime/control работы.
- Для исторического compacted receipt без dispatch и без source-полей grant используется тот же canonical Turn ID. После общей проверки сохраняется существующее ограничение: этот путь подтверждает только projected canonical terminal через fenced cleanup и guarded memory sync, не активирует и не завершает живую или resumed generation. Payload пользователя не восстанавливается. Exhausted claim по-прежнему идёт только в reconciliation, без повторного model call и без увеличения attempts.

Immutable grants и fingerprints в production не переписываются. Права initiating user, explicit allow/deny, scope, routes, ceilings и общие primitives частей 2–3 не менялись. Три предыдущих исправления сохранены: raw expression-index DDL и историческая migration fixture в этом проходе не изменены; exact source-action guard остаётся перед ordinary terminal cleanup.

### Написанная, но не запущенная регрессия

Расширен существующий `exhausted_child_start_cannot_reconcile_a_sibling_from_another_action`, без нового теста или production seam. Реальные tools допускают двух HomeCapsule children; disposable fixture воспроизводит старый grant удалением ровно трёх полей и пересчётом корректного fingerprint. Production grant loader проверяет этот fingerprint обычным образом.

- Матрица теперь включает ordinary attempts=0 и exhausted attempts=8 для нового и исторического grant. Redirect A на B остаётся unresolved: A не ACK-ается, его Turn/execution/resources сохранены; canonical Turn, memory, owner, execution, resources и grant B не изменены. В ordinary случае B заранее завершается настоящим pre-activation Stop; exhausted случай сохраняет живого B. Model calls остаются равны нулю.
- После восстановления собственного payload исторический ordinary child проходит production outbox dispatch до recording provider ровно один раз. Настоящий Stop создаёт canonical terminal; потерянный ACK проигрывается через production outbox без второго model call. Exhausted child с корректной исторической связью проходит production отказ/terminalization и ACK без запуска модели.
- Production compaction удаляет bulky материал; fixture дополнительно убирает dispatch, как старый compactor. Exhausted replay такого receipt подтверждает canonical Interrupted/Blocked, очищает retry obligation, синхронизирует loaded memory и не вызывает модель повторно. Проверяется неизменность compacted payload и сырых grant models с fingerprints после обработки.
- Для нового grant правильный собственный payload по-прежнему отклоняется при противоречии каждого из трёх source-полей и при частично сохранённом наборе. Проверяются отсутствие ACK/model call и сохранность собственного Turn/resources и самого grant. Эти строки проходят настоящий dispatcher и его finalizer; готовый результат finalizer не подставляется.

### Статический самоаудит и выполненные команды

Перечитаны весь diff этого исправления, собственный продуктовый diff от базы `4ea81a4c…` и затронутые callers: исторический/текущий producer, grant loader/upcast, ordinary root/child proof, response/author checks, rejected-start finalizer, exhausted claim/bookkeeping, compaction, terminal writer и guarded memory sync. Проверены существующие signatures и типы моделей. Положительный старый путь прослежен до runtime admission либо canonical terminal; отрицательные пути останавливаются до cleanup/sync другого исполнения. Pending terminal projection по-прежнему не ACK-ается, foreign live owner не активируется, resumed N+1 защищён прежними writer и memory fences. Limits, deadlines, quotas, scheduling priority и число попыток не увеличены.

Статический authoring gate: на начальном HEAD валидный старый grant не проходит strict source check; ordinary dispatch не достиг бы provider, а terminal/exhausted replay остался бы Unresolved. Это ожидаемая причина падения положительных расширений до исправления. Baseline и исправленная версия не исполнялись; прохождение не заявляется.

Прочитаны применимые AGENTS.md, prompt и review. Для чтения использовались `pwd`, `rg`, `rg --files`, `cat`, `sed -n`, `git show`, `git diff` (включая diff от базы и полный diff текущей правки). Навигационные поиски без совпадений уточнялись чтением существующих путей. Выполнены `git branch --show-current`, `git rev-parse HEAD`, `git status --short --untracked-files=all`, `git diff --stat`, `git diff --numstat`, `git diff --name-only` и `git diff --cached --stat`.

Фактически выполнены только разрешённые formatting/whitespace проверки; завершились с exit 0:

```sh
cargo fmt --all
cargo fmt --all -- --check
git diff --check
git diff 4ea81a4c67a9d24876e4333989dae1955b00b943 --check
git diff --cached --check
```

Исправление зафиксировано с отключёнными hooks; staging содержал ровно два Rust-пути выше. Для отчёта используется отдельный commit с тем же отключением hooks:

```sh
git -c core.hooksPath=/dev/null add crates/gateway/src/message/agent_action_tools.rs crates/gateway/src/message/tests/thread_launch_choices.rs
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -m "Support historical child grants in Threads dispatch recovery"
git -c core.hooksPath=/dev/null add docs/reviews/threads-domain-split/threads-dispatch-recovery.review.md
git diff --cached --check
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -m "Document historical child grant compatibility review"
git status --short --untracked-files=all
git rev-parse HEAD
```

Основной checkout, соседние worktree/ветки, dev/prod-базы, службы, конфиги и установленные skills не менялись. Merge, push, deploy, PR и интеграция частей не выполнялись.

Тесты, cargo check, компиляция, сборки, приложение, Gateway, тестовые CLI-агенты, миграции, EXPLAIN, benchmarks и генераторы не запускались. Компилируемость и прохождение тестов не подтверждены. Это передача на повторное внешнее ревью части 4 без окончательной приёмки; запрет запуска проверок сохраняется до отдельного разрешения пользователя после внешней приёмки.
