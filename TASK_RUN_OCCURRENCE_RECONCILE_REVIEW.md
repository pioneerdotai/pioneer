# Incremental TaskRun occurrence reconciliation — review handoff

Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/incremental-task-run-occurrence-reconcile`

Branch: `fix/incremental-task-run-occurrence-reconcile`

Base: `6022ad18c6f0aaf85d2c63f4153c8d7f3d1552fb`

Тесты не запускались. Приложение не запускалось. Пользовательская БД не открывалась,
миграции на ней не выполнялись. Это передача реализации на ревью, до отдельного
этапа принятия и запуска тестов.

## Protocol

1. Шесть SQL-триггеров на физических `task_run`/`turn` поддерживают только текущий
   предикат M: равные ID, `turn_kind='task_run'`, терминальный TaskRun и несовпадающий
   статус Turn. Маппинг: succeeded/completed; failed, timed_out/failed;
   blocked/blocked; cancelled/interrupted. SQL `<>` сохраняет NULL/unknown semantics.
2. Значимый INSERT/UPDATE создаёт pending или обновляет его generation и сбрасывает
   token. Existing attempt count и due time сохраняются. M=false и DELETE снимают
   pending. UPDATE ID обслуживает старый и новый ID. Null-safe `IS NOT` подавляет
   одинаковые значения; heartbeat и общий updated_at не запускают tracking.
3. Discovery читает только индексированный due-набор; один SELECT, максимум 64
   пары run_id/generation. Каждый выбранный кандидат расходует бюджет.
4. Claim генерирует токен до DB capacity, читает точную advisory pending-строку,
   вычисляет backoff вне writer и условно обновляет её в короткой Maintenance
   транзакции. Условие включает generation, прежние due/count/token и due<=now.
   `rows_affected==1` и успешный commit необходимы для подготовки ремонта.
5. Backoff уже durable до parsing/serialization. Claim не повторяется автоматически
   при неоднозначной ошибке commit. Повторная попытка возможна по новому due time;
   повторный захват одной generation обязательно меняет token.
6. Общая подготовка канонического Turn event выполняется на readers. Обычный
   Task-event fanout использует тот же commit. Background добавляет ожидаемый
   generation/token; сам ремонт имеет Maintenance reads / Critical writes.
7. Перед append/project transaction проверяет claim, TaskRun, полный Turn, полный
   Thread и наличие Workspace. Сохраняются существующие projection-order fences.
   Устаревшая работа возвращает StaleClaim, без изменения новой попытки.
8. Turn UPDATE удаляет pending триггером в этой же транзакции. Любая ошибка
   последующего проектора откатывает событие, Turn и удаление pending.
9. No-op housekeeping использует отдельную Maintenance транзакцию, условные
   generation/token и актуальный NOT M. Missing Thread/Workspace при M=true
   сохраняет pending. Нет unconditional DELETE после успешного commit.
10. Gateway продолжает остальные candidates при ошибке. Summary отдельно считает
    выбранные/claimed/changed/lost/stale/unresolved/no-op/claim errors/repair errors/
    storage errors/notification errors. Reporter учитывает partial failure при Ok
    summary; typed storage failure имеет приоритет перед poison error. Diagnostics
    не включают IDs, SQL, payload или raw errors. Уведомления идут после commit.

## Schema

Migration `m20261002_000001_task_run_occurrence_reconcile`, последняя в Migrator,
`use_transaction=Some(true)`: installation и completion marker атомарны.

`task_run_occurrence_reconcile_pending`:

| Column | Representation |
| --- | --- |
| run_id | TEXT, NOT NULL, PRIMARY KEY |
| generation | signed INTEGER, typeof=integer, >0 |
| next_attempt_at | signed INTEGER Unix seconds UTC |
| attempt_count | INTEGER, 0..16 |
| claim_token | nullable TEXT |

`task_run_occurrence_reconcile_sequence`: singleton INTEGER PRIMARY KEY CHECK=1;
generation INTEGER CHECK typeof=integer and >=0. Начальная строка (1,0).
Счётчик не сбрасывается при empty pending. Generation=i64::MAX отвергает следующую
выдачу через RAISE(ABORT), до сложения; missing/invalid singleton отвергает
значимый source DML. Generation не становится REAL и не повторяется после ABA.

Index `idx_task_run_occurrence_reconcile_due(next_attempt_at,generation,run_id)`.
Порядок соответствует range, ORDER BY и LIMIT; выбираются только run_id/generation,
поэтому индекс покрывает discovery без копирования attempt_count/token.
Новая запись получает реальные текущие Unix seconds через SQLite strftime;
нет постоянного sentinel priority=0. Индексы доменной истории не добавлены.

Triggers:

- task_run_occurrence_reconcile_task_run_insert
- task_run_occurrence_reconcile_task_run_update
- task_run_occurrence_reconcile_task_run_delete
- task_run_occurrence_reconcile_turn_insert
- task_run_occurrence_reconcile_turn_update
- task_run_occurrence_reconcile_turn_delete

Raw SQL только для SQLite trigger DDL и SQLite typeof checks. Runtime repository
использует SeaORM/SeaQuery. Query-result projection типизирована. Schema down сначала
снимает все триггеры, затем обе служебные таблицы; due index снимается с таблицей.

Gateway connection initialization выполняет Migrator через writer executor до
открытия reader. `lib.rs` вызывает bootstrap recovery/replay после завершения
initialize_database; resilience worker запускается ещё позднее. Порядок менять
не понадобилось. История при установке принимается согласованной; начальных JOIN
или INSERT из источников нет. Значимые поздние изменения старых записей отслеживаются.

## Preparation freshness

TaskRun: id, status, completed_at, exact error_json. Только эти поля используются
для идентичности, терминального mapping, времени события и error.message; unrelated
result/executor/heartbeat не десериализуются. updated_at не служит fence.

Turn: весь исходный `pioneer_entity::turn::Model`, включая identity/thread_id/status/
turn_kind/error, prompt JSON и compiler/profile/fingerprints, permission/security
snapshots, origin/reasoning, author/actor/collaboration fields, mentions/reply/revision/
deleted fields, timestamps и work_owner. Это покрывает декодированные данные события
и existing Turn upsert без повторного parsing внутри транзакции.

Thread: весь исходный `pioneer_entity::thread::Model`, в том числе id/workspace_id.
Workspace: существование строки по ID из актуального Thread; другие Workspace
поля не участвуют в подготовке. Вся Thread модель сравнивается консервативно.

Дополнительная подготовка остаётся у существующего проектора:

- Turn upsert повторно сравнивает expected_existing model.
- Running attempts: существующий лимит 64; membership/binding/status, active
  attempt и exact item payload проверяются до/при conditional updates.
- Native terminal effects: существующий лимит 2; exact effect ID set, thread,
  effect/gate kinds, hashes, exact payload_json, updated_at, prepared status и
  NULL terminal_committed_at ограждают activation. Новый exact payload_json
  predicate закрывает изменение JSON при прежних hash/timestamp.
- Accepted-result candidate gate пересчитывается в транзакции.
- Receipt successor/watermark, durable Turn owner, semantic projection и
  остальных необходимых atomic writes проверяет прежний append/project путь.

На зависимости дополнительных триггеров нет. Нерешённая пара остаётся pending
для следующей попытки после исправления Thread или projection blocker.

## Failure and restart behavior

Source rollback откатывает pending и generation. Projection rollback сохраняет
claim/backoff, зафиксированные отдельной предыдущей транзакцией. Crash, panic или
cancellation после claim не требуют error bookkeeping. После reopen остаются те же
generation/token/count/due; повторный захват после due выдаёт новый token.

Stale generation/token не применяет подготовленное событие. Full models и exact
Run fields защищают и изменения с одинаковыми timestamps. Event-driven ремонт
может опередить background: его trigger удаляет pending, old holder становится
stale. Late Turn INSERT обнаруживается, но absent Turn не создаётся. Resume старого
blocked Run снимает pending; последующее terminal DML выдаёт новое generation.

Уведомления не входят в транзакцию и не возвращают выполненный repair в pending.
Subscriber send helpers по прежнему best-effort; summary считает ошибки DB lookup
при подготовке fanout, а не все недоставленные сообщения отдельным подписчикам.

## Backoff and cost model

Policy constants: initial=5 seconds, cap=300 seconds, count cap=16. Последовательность
5,10,20,40,80,160,300,... выбрана как короткая первая отсрочка относительно двухсекундного
worker poll и конечный период проверки poison rows; это не измеренные значения.
Нет окончательного удаления poison rows.

- Empty set: один due SELECT за проход; нет обхода истории, новых persistent rows
  или discovery write. Singleton хранится постоянно.
- Normal terminal transition: точечные PK probes в триггерах; возможны sequence
  UPDATE и pending INSERT, затем pending DELETE при отдельной Turn projection.
  Кратковременное расхождение всё равно пишет pending/index/counter в WAL, даже
  если обычный fanout исправит Turn до первого background pass.
- Backlog: одна строка на несовпадающую пару плюс PK/due index; максимум 64 advisory
  кандидата за проход. Каждый успешный claim отдельно пишет token/count/due и due
  index в WAL. Каждая выбранная строка учитывается, включая failed/lost claims.
- Source edits pending: generation/token меняются, count/due остаются назначенными.
- Restart: обычное открытие и due query; без history replay этого механизма,
  bootstrap scan, backfill, cursor, high watermark, checkpoint или fallback.
- B-tree operations имеют стоимость поиска/изменения индекса; наличие LIMIT и
  индекса не является измерением производительности. Существующая стоимость
  event preparation/projector одного кандидата учитывается отдельно и не
  объявляется постоянной. Размер WAL и DB зависит также от checkpoint/vacuum
  политики SQLite, не изменяемой этой задачей.

## Regression code and deferred execution

Новый CRUD модуль `crates/crud/src/tests/task_run_occurrence.rs` покрывает predicate
matrix/unknown/NULL, same-status error, late insert/delete/ID/ABA, recovery DML,
resume, terminal commit без fanout, replay, generation/token races, equal timestamps,
source/projection rollback, missing Thread, abandonment, attempt saturation,
migration install/down/rollback marker/no backfill, overflow/missing singleton,
disk reopen, scheduling/cancellation/read-only/RETURNING, native effect payload fence.
EXPLAIN tests проверяют empty set, 4096 deferred rows и due backlog: covering due
index и отсутствие TEMP B-TREE. Эти assertions ещё не выполнялись.

Gateway `message/tests/task_run_occurrence_tracker.rs` покрывает poison progress,
64-candidate budget (including a claim error recognized by the existing transient retry classifier), partial-success reporting, low-cardinality diagnostics и
post-commit notification failure. Existing occurrence и reconciliation_workers
tests переведены на новый summary и pending discovery.

После отдельного принятия ревьюером предлагается выполнить:

```sh
cargo test -p pioneer-crud occurrence_tracker
cargo test -p pioneer-crud task_run_occurrence_terminalization
cargo test -p pioneer-gateway task_run_occurrence_tracker
cargo test -p pioneer-gateway reconciliation_workers
cargo test -p pioneer-crud -p pioneer-gateway -p pioneer-migration -p pioneer-sqlite
```

Не запускать эти команды до принятия реализации. Runtime behavior, actual restart,
EXPLAIN assertions и производительность сейчас не подтверждены выполнением.

## Performed validation

- Проверены применимые AGENTS.md, исходный HEAD/status, branches и worktrees.
  Task worktree создан от указанного актуального HEAD. Основной checkout остался
  чистым на `6022ad18c6f0aaf85d2c63f4153c8d7f3d1552fb`.
- `cargo fmt --all` и `cargo fmt --all -- --check` — успешно.
- `git diff --check` — успешно.
- `cargo check -p pioneer-crud -p pioneer-gateway -p pioneer-migration --lib` —
  успешно после исправления найденной компилятором неоднозначности ExprTrait/min.
- `cargo check -p pioneer-crud --tests` — успешно.
- `cargo check -p pioneer-crud -p pioneer-gateway -p pioneer-migration --tests` —
  успешно, в том числе после последнего Gateway regression test. Это компиляция
  тестового кода, без выполнения тестов. Первые проходы обнаружили ошибки типов/
  импортов и test-only Migrator scope; они исправлены.
- Статически проверены trigger coverage, guarded claims/cleanup, projection
  rollback, используемые поля подготовки, scheduler scopes и startup ordering.
  Поиск `list_mismatched_terminal_task_run_occurrence_ids` в `crates` не находит
  ни реализации, ни ссылок.

Тесты не запускались. Нет выполненного доказательства корректности runtime SQL,
гонок, cancellation/restart или EXPLAIN assertions. Производительность и объём
WAL не измерялись. Следующий этап — независимое ревью и затем разрешённый запуск
предложенных тестов; отсутствие известных незавершённых частей реализации не
заменяет этот этап проверки.

## Changed files

- `crates/migration/src/m20261002_000001_task_run_occurrence_reconcile.rs` — schema/triggers/up/down.
- `crates/migration/src/lib.rs` — registration in Migrator.
- `crates/entity/src/task_run_occurrence_reconcile_pending.rs` — pending model.
- `crates/entity/src/task_run_occurrence_reconcile_sequence.rs` — singleton model.
- `crates/entity/src/lib.rs` — entity modules.
- `crates/entity/src/prelude.rs` — entity exports.
- `crates/crud/src/repositories/task_run_occurrence_reconcile.rs` — due query, claim CAS, conditional cleanup.
- `crates/crud/src/repositories/native_terminal_effect_outbox.rs` — exact payload fence for shared atomic repair.
- `crates/crud/src/repositories/mod.rs` — repository registration.
- `crates/crud/src/task_run_occurrence.rs` — scoped protocol and common event repair.
- `crates/crud/src/lib.rs` — remove historical discovery, export protocol, update existing tests.
- `crates/crud/src/tests/task_run_occurrence.rs` — 23 focused regression tests.
- `crates/gateway/src/message/tasks.rs` — bounded pass/summary and post-commit fanout.
- `crates/gateway/src/message/mod.rs` — remove batch retry, report counts/partial success.
- `crates/gateway/src/message/reconciliation_diagnostics.rs` — bounded categories and partial failure.
- `crates/gateway/src/message/tests.rs` — test registration and existing assertions.
- `crates/gateway/src/message/tests/reconciliation_workers.rs` — summary-aware existing reporting tests.
- `crates/gateway/src/message/tests/task_run_occurrence_tracker.rs` — 4 Gateway regression tests.
- `TASK_RUN_OCCURRENCE_RECONCILE_REVIEW.md` — this handoff.
