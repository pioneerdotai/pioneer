# Threads turn cancellation: передача на внешнее ревью

## Ревизии и рабочая область

- Ветка: `fix/threads-turn-cancellation`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-turn-cancellation`.
- Фактический начальный commit: `4ea81a4c67a9d24876e4333989dae1955b00b943`
  (`Split Threads Stop, foreground control and terminal synchronization`).
- При входе дерево было чистым, staged/unstaged/untracked работы не было.
- База собственной части: `13b22123b19026b1ab4701e72c884f3efe4a15b3`.
- Интервал аудита: указанная база → начальный commit, затем весь собственный
  итоговый diff с исправлениями этой сессии.
- Итоговый commit содержит этот отчёт; его точный hash передаётся отдельно
  в финальном сообщении, чтобы отчёт не ссылался сам на себя.
- Родительская ветка будущего PR: `fix/threads-independent-root-launch`.
  Обновлённая база от ревьюера в этой сессии не поступала.
- Исходный кандидат `9d86bc3eb65cc58f0e67bf3022a545dfec0b14b6` и его
  самоотчёт не использовались как эталон корректности. Совокупная база
  разделения: `bb85dcdf7080164c4dc6480d12410e5e38edade8`.

Это статический самоаудит части 3/4, а не приёмка. Интеграцию принятых
исправлений в следующие части выполняет родительский ревьюер.

## Прочитанные инструкции и scope

Прочитан `AGENTS.md` указанного worktree, проверено наличие вложенных
инструкций. В этой ревизии `crates/crud/AGENTS.md` отсутствует; прочитан
доступный одноимённый файл основного checkout только как инструкция по CRUD.
Прочитаны предоставленные Pioneer skills и test-audit. Для тестовых правок
использованы authoring gates; явные инструкции пользователя разрешают
минимальные production-исправления и запрещают запуск проверок кода.
Tasks, Subagents, Threads control tools для управления реальной работой
продукта не вызывались, дочерние агенты не создавались.

Полностью просмотрен собственный diff и непосредственно связанные callers:

- `crates/agent/src/{lib,agent_loop,manager_tests}.rs`;
- `crates/crud/src/lib.rs`, `native_cancellation_tests.rs`,
  `repositories/{agent_domain,native_cancellation_context}.rs`;
- `crates/gateway/src/cli_runtime/projector.rs`;
- `crates/gateway/src/message/{agent_action_tools,agent_runtime,turn_handlers}.rs`;
- `crates/gateway/src/message/tests.rs`,
  `tests/{cli_task_tools,recovered_native_turn_stop,thread_launch_choices}.rs`;
- `crates/gateway/src/thread/mod.rs`;
- `crates/protocol/src/agent_event.rs`, `schemas/agent_durable_event.json`;
- сохранённый prompt этой части и настоящий отчёт.

Дополнительно прочитаны затронутые execution guard, recovery resume,
canonical append/projection, terminal delivery и cancellation context paths,
существующие fault/barrier fixtures, manifests и CI. Shared signatures
проверялись по определениям в этой ревизии, без предположений о будущей части 4.

## Найдено и исправлено

1. **Duplicate loaded Turn при generic resume.**
   `resume_blocked_turn_with_projection` выбирал первый Turn по ID. При двух
   loaded Turn с одинаковым ID он мог выполнить durable resume и обновить
   только один из них, оставив конфликтующий terminal в памяти. Теперь
   дубликаты отвергаются под существующим memory lock до polling durable
   transition. Отсутствующий loaded target по-прежнему допускается;
   workspace и конфликтующий terminal проверяются отдельно.

2. **Semantic denial мог маскироваться последующим storage failure.**
   Обработка ошибки initial registration и execution guard безусловно
   запрашивала handoff proof. Если исходный отказ был семантическим, ошибка
   дополнительного чтения owner могла заменить его retryable отказом.
   В регистрации handoff читается только для typed storage failure; в guard
   также сохранён отдельный случай replay принятого Interrupted. Остальные
   semantic denials сразу классифицируются по исходной причине. Новых
   полномочий, более широких generation proofs и retry budgets не добавлено.

3. **Пробел покрытия фактической registration write failure.**
   Существующий сценарий `RegistrationStorageUnavailable` ломает чтение в
   guard после регистрации. Добавлен отдельный вариант той же public Stop
   fixture с ошибкой настоящего INSERT immutable cancellation context.
   Он проверяет pending Stop, живого актора, отсутствие context/terminal и
   provider calls во время ошибки, затем завершение той же отмены и точное
   освобождение ресурсов после восстановления записи.

4. **Односторонняя recovery/resume регрессия.**
   Существующая интеграционная регрессия проверяла resume перед задержанным
   terminal memory sync. Теперь она также пропускает terminal sync первым,
   проверяет loaded Blocked и только затем делает resume. В обоих вариантах
   проверяются InProgress N+1 в storage/memory и foreground conflict нового
   старта. Новая сквозная fixture не создавалась.

Также убран неиспользуемый binding `expected_run_id` в обработчике CancelTurn;
сам run fence остаётся в предварительной проверке команды.

## Статическая проверка цепочек

| Цепочка | Проверенное свойство и граница подтверждения |
| --- | --- |
| Standalone root start | `user_root` использует общий foreground admission; typed conflict возвращается как `agent_action_conflict` до durable graph/action commit. Ошибка последующего commit имеет rollback memory admission. |
| Stop до actor admission | Unactivated writer атомарно сверяет owner/generation/scope, отсутствие registration/CLI attempt/terminal acceptance. Победа activation переводит существующий bounded fallback к реальному context. |
| Initial Stop handoff | Manager проверяет actor generation/run; handoff содержит точный owner/generation и сверяет workspace, turn и batch. Registration flag описывает фазу; после mailbox admission обязанность хранится у актора. |
| ACK | Ordinary CancelTurn посылает process ACK до ожидания shutdown/registration/publication. Generation-bound completion имеет отдельный receipt. Cached process ACK не превращается в canonical completion. |
| Storage failures | Typed failures в guard, context registration и terminal publication не становятся permanent refusal для текущего handed-off Stop. Semantic denial не получает handoff retry из-за дополнительного storage lookup. |
| Accepted projection pending | Initial Stop вызывает fenced CRUD напрямую; pending projection классифицируется retryable. Completion следует после projection, exact terminal resource cleanup и guarded memory sync. Наличие snapshot/registration не используется как completion proof. |
| Root graph Stop | Cancelled direct executions с unfinished InProgress response вновь попадают в targets. Ошибка прямого потомка сохраняется, siblings обрабатываются; повторный public Stop видит остаток. Настоящие Tasks используют aggregate cancellation/reconciliation. |
| Resource ownership | Exact finalizer сверяет owner/generation/scope, accepted terminal, terminal execution/Turn и response execution в writer transaction. Несовпадение поколения не освобождает новые ресурсы. |
| Recovery и resume | Shared memory lock охватывает durable source confirmation и terminal application либо durable resume и InProgress application; provider/recovery dispatch происходит после lock. Старый terminal N не применяется после resume N+1. |
| Loaded state | Unloaded thread/target не является memory conflict. Загруженные duplicate target IDs и конфликтующие terminal states отклоняются; resume теперь проверяет duplicates до durable transition. |
| Wire compatibility | `cancellation_owner: Option<(String, u64)>` имеет serde default/skip-none, native constructor передаёт owner только для fenced Stop, CLI constructor передаёт None, exhaustive patterns согласованы. JSON Schema содержит optional nullable tuple и не добавляет поле в required. |

Новые DB-транзакционные границы в этой сессии не вводились; existing writer
route, scheduling class, deadlines, quotas и Task protocol не менялись.

## Тесты: прочитаны и дополнены, не запускались

- Добавлен `resume_rejects_conflicting_loaded_turns_before_durable_transition`:
  одинаковые duplicates, duplicate с другим terminal и одиночный
  conflicting terminal; durable future не опрашивается, loaded state сохранён.
  Это отдельная проверка admission guard, без подмены storage результата.
- Добавлен
  `public_initial_stop_retains_completion_across_registration_write_failure`
  на существующем production public request → actor → CRUD пути.
- Расширен
  `recovery_terminalization_cannot_apply_blocked_over_a_resumed_generation`
  на оба порядка memory sync/resume.
- Прочитаны существующие `native_cancellation_tests` для unactivated writer
  race, activated missing-context refusal, terminal-first, changed owner и
  same-owner/new-generation rejection.
- Прочитаны `recovered_native_turn_stop`: restored recovery, competing
  terminal/provisional rollback, delayed registration/process ACK, настоящий
  Task, direct child, graph siblings/repeat и accepted projection pending.
- Прочитаны independent-root first dispatch/replay/Stop, foreground conflict,
  queued child resource release, fixture завершения предыдущего launch,
  native/CLI constructors и protocol legacy roundtrip.
- Прочитаны существующие typed storage/semantic classification regressions.
  Новая комбинация semantic denial + unavailable secondary handoff lookup
  проверена статически по short-circuit; отдельной выполненной fault-регрессии
  для неё нет. Pre-fix/post-fix результаты тестов не заявляются.

## Реально выполненные проверки и команды

Все команды редактирования и Git выполнены в указанном worktree.

- Начальное состояние: `pwd`, `git status --short`,
  `git branch --show-current`, `git rev-parse HEAD`, `git log -5 --oneline`.
- Аудит: `rg --files`/`rg -n`, `cat`, `sed -n`,
  `git diff --stat 13b22123b19026b1ab4701e72c884f3efe4a15b3..HEAD`,
  `git diff --name-only` и полные тематические `git diff` этого интервала;
  после правок также `git diff`, `git diff -w` и `git diff --word-diff=plain`.
- `python3` использовался только для статического редактирования текста
  существующей recovery fixture и чтения checked-in JSON Schema. Код проекта
  и генераторы этим не выполнялись.
- Первый `cargo fmt --all --check` завершился с кодом 1: требовалось
  форматирование новых тестовых hunks.
- Выполнены `cargo fmt -p pioneer-gateway -p pioneer-agent` и затем
  `cargo fmt -p pioneer-gateway` после последней production-правки.
- Итоговый `cargo fmt --all --check`: код 0.
- `git diff --check`: код 0 после исправлений; повторяется перед commit.
- Для commit используются `git -c core.hooksPath=/dev/null commit ...`;
  итоговые `git rev-parse HEAD` и `git status --porcelain=v1` передаются
  с результатом. Hooks отключены.

## Зависимости и ограничения передачи

Часть зависит от уже присутствующих admission/authority и independent-root
launch изменений базы `13b22123…`. Guarded synchronization primitives,
generic resume bridge и recovery terminalization consumer остаются здесь;
часть 4 должна использовать их, не копируя. Existing
`materialize_unactivated_turn_refusal_owned` сейчас имеет тестовые callers;
будущие recovery/outbox callers части 4 не считались уже подключёнными.
Restart/outbox matrix этой сессией не добавлялась.

Основной checkout, соседние worktree и другие ветки не изменялись. Merge,
push, deploy и создание PR не выполнялись. Dev/prod-базы, службы, конфиги,
skills и policy blocks не изменялись. SQL fault injection существует только
в тексте не запущенных тестов.

**Тесты, cargo check, компиляция, сборки, приложение, Gateway, тестовые
CLI-агенты и генераторы не запускались. Компилируемость и прохождение тестов
не подтверждены.** Schema regeneration также не выполнялась; проверена
только checked-in форма. Проверки runtime races и новых тестов остаются
неподтверждёнными до отдельного разрешённого запуска. Самоаудит и внешнее
ревью не дают такого разрешения. Общая приёмка возможна после ревью всех
четырёх частей и совместной интеграции.
