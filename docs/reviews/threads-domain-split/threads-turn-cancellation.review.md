# Threads turn cancellation: повторная передача на внешнее ревью

## Ревизии и рабочая область

- Ветка: `fix/threads-turn-cancellation`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-turn-cancellation`.
- Фактический начальный commit текущей доработки:
  `c3374ebeaab2a63fd8fec3f220ae206607950918` — совпал с проверенным HEAD.
- Начальный commit предыдущей доработки по трём замечаниям:
  `a30fc0bba9922916cc93df5ddac38b0658167387`.
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

## CI repair after the first complete Linux run

CI run https://github.com/pioneerdotai/pioneer/actions/runs/40387902314
checked head `b1be607e12ba3c00722ba5f9025e050f1651384d`. Windows, macOS
and Linux build succeeded; all four Linux test shards failed. The monitor
`yzK5AbQsY0y2kZwNKFvV2` paused future firings before this repair. Logs were
read from the GitHub jobs API, including completed jobs while the run was
still active. No local test or build was run.

The activation claim attempted to store running with no started_at, violating
ck_turn_execution_started_state. It now claims the existing starting receipt
with a timestamp. The timestamp rejects duplicate claims; actual runtime
registration still owns the transition to running. Cancellation recognizes
that claimed starting phase before registration. Its serialized writer still
requires the exact owner, generation and scope, no immutable native context,
no CLI attempt and no accepted competing terminal. A running execution without
its immutable context remains a refusal. The existing writer-fence regression
also checks that a claimed execution remains starting with its timestamp.
No schema, new state, queue, timeout or resource policy was introduced.

The failed fixtures also contained incompatible setup assumptions:

- The initial Stop matrix now persists its ordinary thread and restores its
  durable lifecycle before requesting authoritative admission.
- Cancellation and execution-window fixtures use the same resolved author and
  permission profile in memory and storage. Strict terminal equality stays in
  production; fixtures no longer depend on an actorless snapshot matching an
  author hydrated from the durable initiator.
- The restored Task manifest scenario subscribes its public client before
  Stop, so ordinary terminal cleanup retains the loaded Turn whose hydration
  the scenario asserts.
- The CLI child fixture waits for the existing startup transition gate to be
  released as well as for the binding, native turn and MCP metadata.
- Launch fixtures restore the active source Turn and use a valid thread ID
  for membership. The depth chain uses distinct hidden home-capsule threads,
  preserving the foreground gate on the visible source. The pinned-launch
  check updates its already durable thread through durable thread/start.
- The internal DurableAgent completion check expects the typed, unreported
  validation failure that the accepted base hands to its committed outbox.
  External public-error coverage remains in its existing owner test.
- The capability persistence-order assertion includes the four admitted
  Threads actions/model tools and checks their names before skill/native start.

Every failed scenario remains enabled. Tests were extended or repaired in
place; no assertions were suppressed, timeouts increased or checks skipped.
Static review followed claim to registration, preactivation cancellation,
projection and guarded completion, plus fixture callers and shared signatures.
Only cargo fmt and whitespace checks are used locally. The final formatting
and Git conflict results are confirmed during handoff. The new CI run must
confirm these repairs; this report does not assert GREEN.

## PR publication and base conflict resolution

The user accepted the result and authorized publication, same-branch CI repairs,
and conflict resolution. PR: https://github.com/pioneerdotai/pioneer/pull/199.
The unpublished final commit was renamed to the conventional English title
`fix(threads): preserve runtime cancellation through projection recovery`;
its resulting commit is `77f968a026f5d96305b17253d68b8f8338058ff9`.

The PR remains stacked on `fix/threads-independent-root-launch` (#198).
That remote base advanced to `36f4a3fa59244154dd14c5d4577755a404e41a5e`.
GitHub confirmed a content conflict in the shared launch fixture. The base was
merged into this branch only; its branch/worktree were not changed or pushed.
Both fixture guarantees are preserved: replay keeps immutable root provenance,
and a foreground launch finishes before the next launch. Composer settings now
change through the existing ThreadManager thread/start path before rehydrating
the still-runnable first Turn, preserving the base's pinned-launch regression.

The updated base also removed the caller-provided clock from
claim_unactivated_agent_turn. Three cancellation test callers now use its actual
signature. Their fixture creates a live lease against the clock used by the
writer, retaining the same 100-second lease duration; replacement-owner cases
derive their synthetic expired times from that lease. No assertion, generation
fence or production timeout was relaxed.

The current origin/main commit is
`6a446fac6d8ad60eb0ed8e1ffae5fcf3c59d4984`. Static git merge-tree inspection
found no conflict with it. Formatting and whitespace checks are the only local
checks used during publication; local builds, tests and project execution remain
prohibited. The friendly English PR body was edited with the humanizer skill
and contains no validation-execution section.

One durable CI monitor, `yzK5AbQsY0y2kZwNKFvV2`, posts results to the initiating
conversation. Its instructions preserve the PR URL, worktree, branch, restrictions,
15-minute schedule, pause-during-repair rule, current-head GREEN checks and
permanent stop condition. Future firings were paused during this conflict repair;
the verified next schedule is reported separately after publication.

## Доработка по двум P2: текущая передача

Предыдущая передача `c3374ebe…` не закрывала два случая ниже. Начальное
дерево было чистым; база части осталась `13b22123…`. Изменены только
`crates/agent/src/lib.rs`, cancellation-фрагменты
`crates/gateway/src/message/agent_runtime.rs`,
`crates/gateway/src/thread/mod.rs`, существующие fixtures
`message/tests.rs`, `message/tests/recovered_native_turn_stop.rs` и этот отчёт.
CRUD, canonical payload/schema, Task protocol и другие части не менялись.

### P2: runtime прямого потомка после ошибки projection

Причина: direct graph path прекращал обработку конкретного потомка после
неуспешного `mark_turn_interrupted`, до cancellation observation и runtime
control. После replay ответ уже Interrupted и больше не выбирается graph
query, поэтому повторный Stop не мог остановить оставшийся provider work.

Graph path теперь сохраняет исходный `DurableCommitRejection` в ошибке
общего Stop, продолжает штатный runtime cancel при retryable completion
failure и обрабатывает siblings. Permanent semantic refusal по-прежнему
не разрешает отменять заменивший источник. В первой и idempotent ветках
interruption подтверждённый append также передаётся существующему
`observe_accepted_native_cancellation` до возврата ошибки projection.
Observation перечитывает durable receipt для текущего owner и привязывает
его к захваченному actor generation и текущему run control. Обычная команда
CancelTurn теперь также несёт run ID уже захваченного control, используя
имеющийся run fence. Process ACK остаётся быстрым и не ждёт projection.

Ошибка materialization не превращается в успешный completion; exact cleanup,
projection и guarded sync остаются обязательными. Post-append тип проверяется
структурно, текст DB-ошибки не классифицируется. Owner/generation phase denial
не разрешает receipt observation. Реальные Tasks продолжают использовать
свой aggregate cancellation protocol.

Расширен `GraphFenceProjectionUnavailable` в существующем
`public_root_stop_retries_a_direct_child_after_real_projection_failure`:
оба прямых потомка dispatch-нуты настоящим action outbox и удерживаются
в recording provider. Первый public Stop получает реальную ошибку UPDATE
проектора после append; sibling завершается. До восстановления проекции
проверяются принятый terminal при DB InProgress первого ребёнка, ошибка Stop,
сброс обоих provider futures и cleanup активных actor controls. Provider
не освобождается успешным ответом. Повторный Stop до replay также ошибочен;
после штатного replay и повторного public Stop проверяются Interrupted,
точные owner/generation, memory/DB, cancelled resources, released permits
и отсутствие дополнительных model calls. Drop recorder существует только
в тестовой fixture и отличает отмену запроса от успешного завершения.

### P2: manifest восстановленного Task и identity повторной отмены

Причина: штатный thread snapshot намеренно опускает prompt_manifest, а
terminal upsert сохраняет прежний manifest в БД. Полное сравнение loaded
Interrupted(None) с projected Interrupted(Some) отклоняло подтверждённую
отмену. Начальный unguarded commit accepted payload при повторном Stop
затем конфликтовал с уже обогащённой памятью.

Только guarded terminal synchronization может заполнить отсутствующий
manifest у Interrupted. После заполнения одного поля весь Turn должен
точно совпадать с подтверждённой DB projection. Другой status/outcome,
другая причина/результат, конфликтующий уже заполненный manifest, duplicate
loaded Turns и scope mismatch остаются отказами. Unguarded terminal commit
сохраняет прежнее строгое сравнение. Owner/generation и повторная проверка
projection под общим с resume memory lock не менялись; terminal N не получает
права заменить InProgress N+1.

Начальная и idempotent ветки повторно materialize-ят исходную accepted
notification вместо записи её snapshot в память или создания события из
обогащённого Turn. Дополнительное чтение accepted notification закрывает
также гонку, когда другая отмена успела между первым чтением и turn_finish.
CRUD уже умеет переиспользовать accepted payload из receipt; его identity,
ID, payload и full-payload idempotency key не переписываются. Manifest в БД
не стирается. Completion достигается через прежний finish_turn_cancellation
и guarded sync; Task reconciliation остаётся у существующего consumer.

Добавлен вариант той же настоящей Task recovery fixture:
`recovered_task_public_stop_preserves_prompt_manifest_and_canonical_identity`.
Первый настоящий model call сохраняет непустой manifest, затем существующая
fixture делает execution lease stale, убирает прежний actor и запускает
штатный Task restart. Существующий recovery control barrier задерживает
продолжение до повторной компиляции промпта. Проверяются loaded InProgress
с None при сохранённом Some, первый и повторный публичные turn/cancel,
совпадение памяти с projected Interrupted(Some), сохранность manifest,
accepted payload с None, единственный неизменный terminal event после обоих
Stop и cleanup актора, TaskRun Cancelled и отсутствие recovery model calls.
Оба прежних recovery/preflight варианта используют тот же helper без Stop.

### Статический самоаудит и выполненные проверки текущей доработки

Перечитан весь собственный итоговый diff от `13b22123…` и непосредственные
callers. Проверены public Stop → graph fence/Task intent → canonical append →
projection error/replay → receipt observation → runtime CancelTurn → cleanup →
guarded sync/completion, включая повторный Stop. Отдельно прослежены snapshot
mapper, terminal upsert columns, get_turn с полным manifest, immutable accepted
payload, memory sync и оба порядка existing recovery/resume regression.
Shared signatures проверены по определениям текущей ревизии.

Три предыдущих исправления сохранены: owned initial Stop удерживает publication
при обеих формах typed post-append error; ранний и повторный Interrupted не
подтверждают provisional memory и ожидают projection/cleanup/sync; победивший
CLI initial attempt продолжает тот же Stop через существующий gate и binding
checks. Native/CLI constructors и optional cancellation_owner остаются
совместимыми. Existing отрицательные проверки competing terminal, duplicate
loaded Turns, смены owner/generation и InProgress N+1 не удалялись и не
ослаблялись. Нового control API, очереди, receipt system или lifecycle нет.
Другие ветки/worktree и части 1/2/4 не менялись и не интегрировались.

Выполнены только чтение (`cat`, `rg`, `sed`, включая AGENTS.md, prompt и review),
Git inspection (`pwd`, `git branch --show-current`, `git rev-parse HEAD`,
`git status --short`, `git diff`, тематические полные diff от базы и их
статические срезы), текстовые правки `apply_patch`/`python3`,
`cargo fmt -p pioneer-agent -p pioneer-gateway`, затем
`cargo fmt --all --check`, `git diff --check` и
`git diff --check 13b22123b19026b1ab4701e72c884f3efe4a15b3`.
Formatting/whitespace checks завершились с кодом 0. Перед commit выполнен
`git diff --cached --check`; commit — с `git -c core.hooksPath=/dev/null commit`.
Финальные `git rev-parse HEAD` и `git status --porcelain=v1` переданы отдельно;
итоговый commit включает этот отчёт и потому не указан в нём самом.

Тесты только дополнены и прочитаны. Тесты, cargo check, компиляция, сборки,
приложение, Gateway, тестовые CLI-агенты, миграции, EXPLAIN, benchmarks и
генераторы не запускались. Компилируемость и прохождение тестов, включая
новые race/fault сценарии, не подтверждены. Нужны повторное внешнее ревью
и отдельное разрешение пользователя на исполнение проверок после приёмки.
Это передача своей части, не окончательная приёмка.

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
