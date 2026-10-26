# Threads turn cancellation: повторная передача на внешнее ревью

## Ревизии и рабочая область

- Ветка: `fix/threads-turn-cancellation`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-turn-cancellation`.
- Фактический начальный commit текущей доработки:
  `a30fc0bba9922916cc93df5ddac38b0658167387` — совпал с проверенным HEAD.
- Начальный commit предыдущего самоаудита:
  `4ea81a4c67a9d24876e4333989dae1955b00b943`
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

Это повторный статический самоаудит части 3/4, а не приёмка. Интеграцию принятых
исправлений в следующие части выполняет родительский ревьюер.

## Доработка по трём подтверждённым замечаниям внешнего аудита

Предыдущий самоаудит не обнаружил три дефекта ниже. Его выводы о projection
и полном раннем completion были недостаточны; актуальные изменения и
ограничения этой передачи приведены здесь. Дерево на входе снова было чистым.

1. **Owned Stop после настоящего сбоя проектора.**
   Обе формы `TurnEventProjectionAfterAppendError` означают подтверждённый
   append, включая `pending=false` с сохранённой текстом DB-причиной.
   Обработка owned TurnInterrupted теперь использует существующий typed
   `turn_event_was_appended_before_error`, не текст ошибки и не только
   `is_pending()`. Актор сохраняет свою публикацию и completion receipt до
   projection replay, exact cleanup и guarded memory sync. Отказы до append
   сохраняют прежнюю классификацию; owner/generation mismatch остаётся
   permanent denial.

2. **Ранний Stop и повторный Interrupted.**
   Только для Interrupted общий post-append fallback больше не превращает
   незавершённую projection в `Ok(())`. Семантика ACK остальных событий
   сохранена. Уже принятое событие не откатывается. Из существующего owned
   completion выделен общий `finish_turn_cancellation`: он проверяет точный
   owner/generation и scope, projected Interrupted, существующий точный
   finalizer и guarded sync, затем повторно сверяет owner/generation.
   Ранняя ветка вызывает его после своего writer, а обычная interruption
   публикация также использует его при повторной доставке. Поэтому
   `current_turn.status == Interrupted` не пропускает ни projection, ни
   освобождение прежних ресурсов. Настоящие Tasks по-прежнему пропускают
   прямой graph finalizer и остаются у aggregate protocol.

   Ошибка проверки completion после успешного append сохраняет typed context
   `NativeTurnControlTransitionPending`; она не разрешает rollback принятого
   результата. Semantic owner/generation denial внутри context проверяется
   раньше retry-классификации. Public fallback теперь читает canonical Turn
   и проверяет сохранённый owner/generation и guarded sync. После неудачного
   mark он допускает только другой projected terminal outcome; provisional
   или ещё не завершённый Interrupted не подтверждает Stop.

3. **CLI initial attempt выиграл ранний writer.**
   После `UnactivatedTurnPhaseChanged` сначала повторно проверяются точные
   owner/generation/workspace/thread, затем различаются CLI attempt и native
   registration. CLI продолжение захватывает существующий session transition
   gate, перечитывает binding, проверяет неизменяемую session identity,
   attempt ID и durable owner/generation. Оно не требует native context.
   Если activation уже завершила эту отмену, повторного terminal append нет.
   Иначе небольшой внутренний CRUD adapter использует существующий canonical
   append/projection pipeline с writer guard по owner/generation/scope и
   binding/attempt. Штатные terminalization и cleanup выполняются под тем же
   gate; completion включает exact finalizer и guarded sync. Новых внешних
   tool/API операций, очередей, таймаутов и альтернативного CLI lifecycle нет.

Новый guard — дополнительная проверка в существующей append-транзакции,
а не новая транзакционная граница. Подготовленный snapshot используется
только под CLI gate; mutable owner/generation, scope, binding route и
attempt ID/status перепроверяются самим writer. Все DB-ресурсы освобождаются
до runtime cleanup, notification и gate ожидания. Scheduling class не менялся.

### Написанные регрессии этой доработки (не запускались)

- `public_initial_stop_retains_actor_after_a_real_projection_failure`:
  настоящая ошибка UPDATE `turn.status` после canonical append. Проверяется
  accepted terminal + сохранённый InProgress + незавершённый Stop + живой
  актор. Первая ошибка действительно проходит классификацию, после чего тест
  ждёт следующую попытку completion при всё ещё сломанной projection;
  остановка актора на permanent rejection не может пройти этот барьер.
  Затем существующий replay завершает тот же Stop без нового model call.
- `public_early_stop_and_repeat_wait_for_real_projection_without_an_actor`:
  та же ошибка UPDATE без dispatch актора; первый и повторный public Stop
  возвращают persistence error, пока DB Turn InProgress, хотя память содержит
  provisional Interrupted. После восстановления и projection replay
  подтверждается прежняя отмена, совпадают memory/DB/resources/permit.
- `public_root_stop_retries_a_direct_child_after_real_projection_failure`:
  расширена существующая graph/siblings fixture. Проекция одного ребёнка
  не завершается, другой ребёнок обрабатывается; повторный public Stop до
  replay также не успешен. После replay завершается оставшаяся отмена.
- `early_public_stop_and_cli_initial_attempt_share_the_exact_writer_fence`:
  существующий recording CLI harness и реальные публичные start/cancel,
  без подготовленного результата вместо writer. Порядок Stop-first
  запрещает настоящий initial attempt; порядок CLI-first продолжает тот же
  Stop через CLI cleanup без второго нажатия. Отдельные отрицательные
  варианты меняют owner+generation и только generation при прежнем owner.
  В них canonical Turn/новый owner не меняются этой отменой. В положительных
  вариантах проверяются Interrupted, terminal binding/attempt, память,
  resource state и released permit; native model calls отсутствуют.

Для детерминированного порядка переиспользован существующий test-only barrier
на границах initial binding и перехода раннего Stop. Нового production control
механизма нет. Все SQL triggers и fake/recording runtimes остаются только
текстом не запущенных тестов.

### Повторный самоаудит и реальные проверки

Просмотрен итоговый diff от `13b22123…`, включая ранее перенесённую часть и
текущие исправления; повторно прочитаны непосредственные callers и shared
signatures: actor ACK/publication, canonical append/error/replay,
unactivated writer, terminal finalizer, public turn/cancel, CLI initial
binding, transition gate, guarded terminalization и runtime effects.
Проверены оба порядка гонок, повторная отмена, отсутствие rollback после
append, отсутствие успешного completion из provisional memory, точные fences,
сохранение Task aggregate protocol и отделение process ACK от completion.
Другие части не переносились, recovery/outbox matrix части 4 не реализовывалась.

В этой доработке выполнены только чтение (`cat`, `rg`, `sed`), Git inspection
(`pwd`, `git rev-parse HEAD`, `git branch --show-current`, `git status --short`,
`git diff`, в том числе diff от `13b22123`), статические текстовые правки
через `apply_patch`/`python3`, несколько проходов
`cargo fmt -p pioneer-gateway -p pioneer-crud`, затем
`cargo fmt --all --check`, `git diff --check` и
`git diff --check 13b22123b19026b1ab4701e72c884f3efe4a15b3`.
Итоговые formatting/whitespace checks — код 0. Перед commit также проверяется
`git diff --cached --check`; commit выполняется с
`git -c core.hooksPath=/dev/null commit ...`.

Это статические результаты. Тесты, cargo check, компиляция, сборки,
приложение, Gateway, тестовые CLI-агенты, миграции, EXPLAIN, benchmarks и
генераторы не запускались. Компилируемость, фактическое прохождение новых
и существующих регрессий и pre-fix/post-fix результаты не подтверждены.
Требуется повторное внешнее ревью; разрешения на запуск оно не даёт.

## Материалы предыдущего самоаудита

Ниже сохранены scope, предшествующие исправления и проверки первой передачи;
выводы по трём новым замечаниям уточнены разделом выше.

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
