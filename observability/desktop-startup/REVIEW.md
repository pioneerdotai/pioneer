# Передача на внешнее ревью

- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/desktop-startup-preconnect-tracing`
- Ветка: `feat/desktop-startup-preconnect-tracing`
- Исходный локальный main: `2d928d6f9c290c0feece92287c6c0289ec315237`
- Commit не создавался; HEAD остаётся исходным SHA. Полный результат подготовлен в index.
- Merge, push, deploy, запуск приложения и изменение Axiom не выполнялись.

## Результат и границы

Ранние platform операции записываются в исходный `desktop.startup`, до UI
observer. Измеряются конкретная очередь effect, config/home/open keystore,
retired credentials cleanup, environment и вложенные registry/load/save,
фактически выполненные installation/update/service/reachability/readiness
checks. Условный local prepare измеряется целиком от dispatch, включая
config/home/timings и условное создание activation внешней командой; внутренние
update, service start и health wait spans сохранены. Desktop импортирует все шесть
существующих session diagnostics:
credentials/load, refresh intent/persist, refresh/request, credentials/persist,
connect attempt и identity verify с исходными timestamps/durations.

Подробные границы, вложенность, исходы, ограничения и APL находятся в
[README.md](README.md). Это измерение, а не оптимизация. Причина наблюдавшейся
задержки пока не установлена: приложение для получения новых traces не запускалось.

## Изменённые файлы

| Файл | Назначение |
| --- | --- |
| `crates/desktop/src/main.rs` | Передача timeline в runtime до первого Initialize. |
| `crates/desktop/src/client_runtime/mod.rs` | Startup-aware установка worker; прежняя установка без trace сохранена. |
| `crates/desktop/src/client_runtime/platform_effect_router.rs` | Queue boundary, keystore prepare/open, cleanup, environment/local prepare dispatch; сигнатура существующего test dispatch обновлена. |
| `crates/desktop/src/client_runtime/startup.rs` | Импорт четырёх preparation diagnostics и cancelled outcome. Readiness predicates не менялись. |
| `crates/desktop/src/gateway/onboarding_platform.rs` | Узкие синхронные guards вокруг реально выполняемых операций, с сохранением short-circuit и ошибок. |
| `crates/desktop/src/gateway/registry.rs` | Необязательный trace до внутреннего save, без изменения registry I/O. |
| `crates/observability/src/startup.rs` | Фиксированные имена стадий, bounded capture, monotonic queue timing, idempotent импорт outcomes, целевые тесты. |
| `crates/client/src/core.rs` | Enqueue Instant в bounded pending map; после ревью — локальное свидетельство вокруг `AuthFailed` под прежним lock. |
| `crates/client/src/gateway/session_controller.rs` | Первый diagnostic cohort, cancellation, terminal provenance и закрытие capture; целевые тесты. |
| `crates/client/src/gateway/session_connection.rs` | Epoch/Weak flight identity, свидетельство собственных terminal-мутаций; регрессии initial flow и отмен. |
| `observability/desktop-startup/README.md` | Семантика измерений и APL. |
| `observability/desktop-startup/REVIEW.md` | Эта передача. |

Изменения вне desktop/observability нужны в трёх client файлах:

1. `core.rs`: время enqueue невозможно восстановить из времени пробуждения worker.
   Instant хранится только в pending entry, не в публичном/wire effect plan;
   getter проверяет operation и generation. Entry удаляется прежним механизмом.
   После повторного ревью этот файл также снимает диагностическое свидетельство
   до/после `AuthFailed` под существующим session lock. Без точки внутри reducer
   невозможно отличить его собственную инвалидизацию от конкурентной отмены;
   прежний lifecycle API делегирует тому же коду с отсутствующим свидетельством.
2. `session_controller.rs`: первоначальный first-per-stage map позволял заполнять
   разные стадии разными endpoint/attempt и объявлять отменённый возврат успешным.
   Исправлена только диагностическая принадлежность/исход, без изменения результата
   операции, lifecycle reduction, refresh, retry/backoff или readiness.
3. `session_connection.rs`: для проверки принадлежности требуется реальный flight,
   поскольку один epoch может пережить несколько flights. Weak не продлевает работу
   flight и не мешает существующему coalescing; удержание идентичности bounded.

Новых зависимостей, wire/schema/DB изменений, metric instruments и DB capacity
reservations нет. Guards не удерживают locks во время операций. Данные и raw
errors не добавлялись в telemetry.

## Саморевью

1. После восстановления пути: проверены main timeline, ранняя install/runtime,
   Initialize, native publication, worker, environment, session preparation,
   socket и поздняя установка observer. Исправлен план wiring: trace передаётся
   при install в main, а не только при открытии окна.
2. После основного изменения: проверены реальные queue boundaries и monotonic
   duration, вложенный registry/save, short-circuit local probes, повторный open
   после ошибки, best-effort cleanup, оригинальные session timings, opt-out gate.
   Добавлены cap в 128 reservations и idempotent session import. Исправлены
   cancelled outcome и смешение endpoint/operation/connection cohorts; capture
   закрывается после identity либо неудачного connect, чтобы поздний refresh/retry
   не заполнял первоначальные пропуски.
3. Финальный diff относительно исходного SHA: перечитаны все изменённые Rust
   файлы, тесты и документация. Дополнительно обнаружено повторное использование
   connection epoch между flights; добавлен Weak identity и регрессионный тест.
   Проверены late completion после finalization, отсутствие перенаправления guards
   в другой timeline, consent generation, bounded retention, отсутствие новых
   чувствительных атрибутов и изменений порядка/количества domain операций.

Ограничение сохранено явно: session diagnostics показывают первое наблюдение
каждой стадии начального cohort, а не каждый reconnect и не сумму refresh retry
requests/backoff. Pending diagnostics без duration не экспортируются как
завершённые; после лимита platform observations подробные spans могут отсутствовать.

## Выполненные проверки

- Форматирование только затронутых Rust файлов через `rustfmt --edition 2024`
  (при окончательных проходах — `--config skip_children=true`).
- `git diff --check` и просмотр полного diff относительно исходного SHA.
- `cargo check -p pioneer-desktop --tests` — успешно.
- `cargo check -p pioneer-client --tests --features test-support` — успешно.
- `cargo check -p pioneer-observability --tests` — успешно.

Первый комбинированный `cargo check -p pioneer-observability -p pioneer-client
-p pioneer-desktop --tests` без `test-support` остановился на существующем
`composer/workflow.rs`: fixture вызывает `ClientMutationAuthority::for_test`,
который gated feature `test-support`. Наличие вызова и feature gate подтверждено
в исходном SHA. Никакого соседнего исправления не сделано; проверка с нужным
feature компилируется. Desktop также выдаёт существующее future-incompatibility
предупреждение зависимости `block v0.1.6`.

Все перечисленные cargo commands выполняют только компиляцию. Тестовые бинарники
и приложение не запускались, hooks и CI не запускались.

## Доработка после внешнего ревью

1. У `PrepareLocalGateway` ранее измерялись только внутренние действия. Config/home/
   timings и создание activation после последнего credentials load оставались вне
   общего интервала. Добавлена `gateway_preconnect.local.prepare` вокруг фактического
   dispatch с тем же bounded recorder. Начало предшествует `execute`, конец следует
   за его результатом; error не меняет сам результат. Пропущенный effect не создаёт
   span. Внутренние интервалы входят в общий и не складываются с ним.
2. Общая проверка изменения identity ошибочно считала собственную terminal-
   инвалидизацию отменой. В native verification локальное свидетельство соединяет
   владельцев до/после retirement и `AuthFailed`. Обе пары снимаются в существующих
   lock boundaries без перемещения domain операций. При изменении владельца error
   сохраняется только при `Err` и точной непрерывной цепочке от исходного к текущему
   владельцу. Внешняя отмена
   до/между/после этих мутаций, смена flight и shutdown остаются cancelled.
   Это не приоритет любого `Err` над отменой: чужой/разорванный receipt не принимается.
   Свидетельство живёт только внутри одного вызова observer; нового lifecycle owner,
   retained event storage, wire-контракта или exported атрибутов нет.

Самоаудиты после каждого исправления проверили полную временную границу,
error/cancelled и неизменность количества, порядка и short-circuit вызовов.
Финально перечитан весь diff относительно `2d928d6f9c290c0feece92287c6c0289ec315237`,
включая первоначальные изменения и тесты. Повторно проверены cap в 128 reservations,
первый cohort из шести diagnostics, dedup импорта, late completion/finalization,
существующий consent generation gate и отсутствие чувствительных атрибутов.
Первоначальные staged changes сохранены; соседний composer worktree не использован.

Фактически выполненные проверки повторной доработки:

- Форматирование и `rustfmt --check` только затронутых Rust файлов с `skip_children=true` — успешно.
- `git diff --check`, проверка staged diff и полного diff относительно исходного SHA — успешно.
- `cargo check -p pioneer-client --tests --features test-support` — успешно.
- `cargo check -p pioneer-desktop --tests` — успешно.
- Финальная совместная компиляция client/observability с тестовым кодом:
  `cargo check -p pioneer-client -p pioneer-observability --tests --features pioneer-client/test-support` — успешно.

Запусков тестов, test runners, приложения, smoke-сценариев, hooks и CI не было.
Новый commit не создавался. Все первоначальные изменения и доработка подготовлены в index.

## Написанные тесты и будущий запуск

Первоначально добавлено 10 тестов: четыре observability (точная граница очереди с управляемыми
Instant, исходные timings/outcomes и dedup, bounded/late completion, early capture
и consent generation), один core (enqueue/republication/stale/retirement), четыре
session controller (results/dedup/owner, closure после connect/identity, cancellation,
replacement epoch), один session connection (разные flights с тем же epoch).
Sleep-based assertions не добавлялись. Существующий desktop platform storage test
обновлён только под дополнительный аргумент trace.

После ревью добавлены ещё четыре регрессии в `session_connection.rs`:

- `terminal_identity_failures_keep_error_unless_the_operation_was_externally_cancelled`:
  реальные storage failure и identity mismatch через существующие ports/fixtures,
  собственный epoch increment и terminal result; также внешняя отмена после terminal.
- `external_identity_cancellation_keeps_late_terminal_errors_cancelled`: смена operation,
  retirement, замена flight и shutdown внутри identity port с последующим terminal Err.
- `cancellation_between_terminal_mutations_invalidates_the_diagnostic_receipt`:
  детерминированная отмена между retirement и `AuthFailed`, без sleeps.
- `successful_initial_session_keeps_all_preparation_and_identity_diagnostics`:
  успешные refresh/persist/connect/identity через существующий fixture, шесть исходных
  diagnostics, их принадлежность и completed outcomes с измеренной duration.

Следующие команды — только для будущего запуска после полного принятия реализации
внешним ревьюером в исходном треде и его явного разрешения на тестирование:

```sh
cargo test -p pioneer-observability startup::tests
cargo test -p pioneer-client --features test-support core::tests::native_effect_enqueue_time
cargo test -p pioneer-client --features test-support gateway::session_controller::tests
cargo test -p pioneer-client --features test-support gateway::session_connection::tests::diagnostic_capture
cargo test -p pioneer-desktop client_runtime::
cargo test -p pioneer-client --features test-support gateway::session_connection::tests::terminal_identity_failures
cargo test -p pioneer-client --features test-support gateway::session_connection::tests::external_identity_cancellation
cargo test -p pioneer-client --features test-support gateway::session_connection::tests::cancellation_between_terminal_mutations
cargo test -p pioneer-client --features test-support gateway::session_connection::tests::successful_initial_session
```

Тесты не запускались. Реализация передана на повторное внешнее ревью.
