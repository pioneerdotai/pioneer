# Передача на внешнее ревью: mobile startup observability

Реализованы исправления измерений A–F. Физическое ускорение приложения не установлено.
Данные Axiom за 10–17 октября не устанавливают причину замедления или потери
экспорта; Android и причины разницы counts здесь не исследовались. Axiom не менялся.

## Парные исходники

Обе ветки: `fix/mobile-startup-observability`.

- Rust worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/mobile-startup-observability`
- Mobile worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/mobile-startup-observability-app`
- Rust base: `2d928d6f9c290c0feece92287c6c0289ec315237`
- Mobile base: `f21d279de0d6aff72f3103fade4c5bfd09f3416e`

Финальные commit SHA приведены в сообщении передачи. Изменения соседних desktop
веток не переносились. Основные checkout не редактировались; исходное изменение
`pioneer-app/bun.lock` сохранено. Merge, push, deploy не выполнялись.

## Границы и outcomes

1. Оболочка: прежний mount после fonts/Client/registry, два прежних RAF,
   затем существующий `hideAppSplash`. Этап `ui.shell_frame` честно описывает
   эти два кадра, `splash.hide` заканчивается после hide promise. Это ранние
   этапы внутри startup report; они больше не завершают operational startup.
   Ошибки fonts/initialize/hydrate используют прежний ранний failure/hide путь.
2. `mobile.startup`: от достоверного RN native start; при его отсутствии — от
   первого исполнения telemetry module, без заявления об измеренном native
   времени. Конец — effect после применения дерева текущего scope и готовности
   стартового экрана: выбранный gateway, действующая transport connection
   generation, bootstrap соответствующего presentation connection, активный
   workspace, завершившийся существующий refresh с применённой публикацией,
   отсутствие обязательного loading. Пустое дерево успешно.
   Outcomes: `ready`, `setup_required` (нет gateway), `authentication_required`
   (терминальная необходимость авторизации), `degraded` (подтверждённая ошибка
   initialization/fonts/hydration, storage, workspace или дерева), `cancelled`
   (смена scope, уход со стартового дерева либо предел наблюдения).
   Повторные refresh не создают startup. После финализации observer снимается.
3. `mobile.thread.open` / `pioneer.mobile.thread.open.duration`: от обработчика
   открытия существующего треда в корне/папке дерева или кнопки создания нового
   треда, непосредственно перед существующим router push/navigate. Route
   принимает уже начатую попытку. Если её нет, начало — layout effect при focus
   `/thread/[threadId]` или `/thread/new`, до их async open/create effects.
   Конец — committed, действительно focused экран, актуальный thread snapshot
   и активный composer/draft/revision, готовая authorization, отсутствие
   первоначального screen loader, действующие правила доступности composer.
   Message не ждёт модели, runtime или текста. Chat/Agent ждут необходимой
   provider/model selection и соответствующего CLI runtime/draft/workspace,
   используют существующие права и permission-mode predicates. Sending/locked
   composer остаётся ожиданием; закрытый/запрещённый composer — `degraded`.
   Outcomes: `ready`, `setup_required` (нет необходимой selection или gateway),
   `authentication_required`, `degraded` (подтверждённый open/create/screen/auth/
   runtime отказ), `cancelled` (blur/unmount, замена попытки, смена scope,
   отменённая необходимая операция, deadline). Поздние/повторные события
   проверяют token/scope; скрытый экран не пишет успех.

Основной поддержанный сценарий — дерево → existing/new при выбранном подключённом
workspace. Прямой thread deep link отменяет tree-startup, а thread-open начинается
в route layout, не задним числом. При cold deep link смена начального scope во
время подключения отменяет эту попытку; это не успешный замер tree startup.
Отдельная task-child route и предшествующая route навигации обработка task
notifications не объявляются измеренными от пользовательского действия.
Возврат на поддержанную route начинает новый focus attempt без повторной загрузки
ради telemetry. Новая универсальная маршрутизация не вводилась.

Каждое наблюдение ограничено 590 секундами, ниже прежнего FFI лимита 10 минут.
Deadline означает `cancelled`, не ошибку приложения. При задержке JS delivery
конец остаётся на cutoff; пересекающие его stages отменяются. Таймер не может
заблокировать последующее раскрытие splash. Максимум stages остаётся 32;
фиксированный набор JS stages меньше лимита. Diagnostics хранят только последнее
наблюдение каждой разрешённой стадии, без журнала retries.

`startup.boundary=tree_operational_v2` маркирует новые root traces и duration
metrics. Старые shell-ready и новые operational-ready startup durations
несопоставимы как доказательство ускорения. Thread-open имеет отдельные span и
histogram и не пишет app-startup/turn-startup метрики. Для durations используется
монотонное время; wall clock нужен только для привязки timestamp экспортера.

## Подтверждённые исправления

- A: shell reveal отделён от финализации; readiness учитывает реальные scope,
  bootstrap и завершённую публикацию существующего tree refresh.
- B: отсутствующие native/runtime timestamps не заменяются нулевым успешным
  runtime stage; без достоверных границ соответствующий stage отсутствует.
- C: недостоверный `thread_tree.response.apply` удалён из producer. Сохранён
  `thread_tree.load`, охватывающий существующий refresh вместе с publication drain;
  `thread_tree.request` также не выдаётся за чистое сетевое время.
- D: dispatch `syncComposerModelSelection` сохранён, но больше не измеряется как
  готовность composer. Реальная готовность наблюдается на thread screen отдельно.
- E: только FFI `mobile_startup_record` освобождён от `require_initialized`.
  JSON/outcomes/stages/durations/endpoints/consent validation сохранены.
  Один прежний Rust OTLP exporter и background flush обслуживают оба отчёта.
  JS показывает фиксированную безопасную диагностику отказа/`recorded=false`,
  не включает payload/raw error и не вызывает telemetry рекурсивно.
- F: session observer обходит актуальный snapshot при подписке; ключ наблюдения
  включает endpoint/start timestamp/duration/state. Исходные native timestamps
  и `cancelled` сохраняются. Подписка diagnostics снимается при финализации.

Ни один telemetry producer не запрашивает providers/models/runtime/composer и
не вызывает test send/turn. Fonts/useFonts, splash conditions и число RAF,
маршруты/layout, порядок существующих сетевых вызовов, keystore/auth/refresh,
tree/unread и отправка сообщений сохранены.

## Контракты и изменения вне telemetry

Добавлены необязательный JSON `kind: app_startup | thread_open` (default прежний
app startup), outcome `cancelled` и разрешённый stage `ui.shell_frame`.
Этот telemetry payload уже был ручным контрактом `src/client/native.ts` ↔
private Rust DTO; генерируемой JSON schema у него нет. Nitro/C ABI signatures
не изменились, поэтому схемы domain API и generated bindings не менялись.
Штатный `boundary-integrity.mjs source` обновил только source fingerprints;
`check-source` подтвердил пару. Это согласованность исходников, не бинарников.
Native prebuilt artifacts не пересобирались и не изменялись. Native compilation,
iOS/Android execution и совместимость новых JS payload со старыми prebuilts
не подтверждены: для них нужна последующая штатная сборка из парных исходников.

Необходимые изменения вне telemetry: root монтирует временный operational
observer; tree hook отмечает факт завершённого существующего refresh; route и
три action sites задают начало/владельца измерения; ThreadScreen передаёт текущие
UI predicates и composer publication; active-thread hook сообщает существующий
open failure, сохраняя callback dependencies через ref; session hook освобождает
telemetry observer. Rust lib exports/telemetry state нужны для отдельного
thread-open histogram в существующем exporter. Иных domain изменений нет.
В атрибутах экспорта нет thread/session IDs, URLs, paths, operation IDs,
сообщений или raw errors. Internal scope/token нужны только для ownership.

## Самоаудит и фактические проверки

1. До редактирования восстановлены shell → session → bootstrap → tree publication
   и route → thread/draft → authorization/composer/runtime. Подтверждены A–F.
2. После основного изменения исправлены stale scope, callback dependency,
   ранняя отписка и независимость splash от telemetry deadline.
3. Полный итоговый diff относительно обоих base SHA просмотрен, включая новые
   тесты и fingerprints. Исправлены порядок stage completion перед readiness,
   cancelled runtime/deadline classification, текущий composer draft/revision,
   ожидание transient busy/connection и защита скрытого экрана. Перепроверены
   bounded lifetime, consent и отсутствие новых запросов. Desktop/turn семантика
   не менялась; mobile-only ветки в shared observability проверены отдельно.

Выполнены:

- `cargo check --locked -p pioneer-client-ffi -p pioneer-observability` — успешно.
- `cargo check --tests --locked -p pioneer-client-ffi -p pioneer-observability` —
  успешно; компиляция test-кода без выполнения тестов.
- rustfmt затронутых Rust файлов (`skip_children=true`), Prettier затронутых TS,
  `git diff --check` обоих репозиториев — успешно.
- `tsc --noEmit --pretty false --preserveSymlinks` — не зелёный: 8 диагностик.
  Отдельная неизменённая временная копия исходного Mobile SHA даёт те же 8:
  четыре отсутствующего `@workspace-sh/react-native-source-editor` и четыре
  использования `visibleThreadId` до объявления. Новых диагностик нет после
  нормализации line offsets. Исходные ошибки вне задачи не исправлялись.
- Для TS использованы symlinks на доступные точные locked версии из локального
  Bun cache. Install/postinstall, обновление dependencies/lockfiles не выполнялись.
- Штатные `boundary-integrity.mjs source` и `check-source` с явно заданным
  `PIONEER_RUST_ROOT` парного Rust worktree — успешно.

Тесты/приложение/симулятор/smoke/CI/hooks не запускались. Native platform build
не выполнялся. Успешной runtime проверки и выигрыша скорости не заявляется.

## Тесты для выполнения только после принятия внешним ревьюером

Написаны/обновлены focused Jest fixtures с fake clocks и управляемыми состояниями:
независимость shell/startup и deadline, empty tree и scope, отсутствие composer
activation/дополнительного tree request, late response, setup/auth/error,
new/existing action timestamps, empty Message, обязательный runtime, отмена,
скрытый экран и повторное завершение, native timing unavailable, session snapshot
replay/различные попытки/cancelled. Rust regressions проверяют доступность telemetry
validation после initialize failure, сохранение guard domain API, default/new kind
и запрет startup stages в thread-open payload. Эти тесты скомпилированы там, где
указано выше, но не выполнялись.

Будущие команды (не запускались):

```sh
# В Rust worktree, после принятия ревьюером:
cargo test --locked -p pioneer-client-ffi initialization_failure_does_not_block_telemetry_validation
cargo test --locked -p pioneer-client-ffi telemetry::tests
cargo test --locked -p pioneer-observability startup::tests

# В Mobile worktree с установленными штатными зависимостями, после принятия:
node ./node_modules/jest/bin/jest.js --runInBand src/services/telemetry/mobile-startup.test.ts src/services/telemetry/mobile-startup-readiness.test.ts src/services/telemetry/thread-opening.test.ts src/services/gateway/session-diagnostics.test.ts src/hooks/use-thread-tree-startup.test.tsx src/hooks/use-gateway-session.test.tsx src/client/root-startup.test.tsx
```

## Затронутые файлы

Rust:

- `crates/client-ffi/src/lib.rs`
- `crates/client-ffi/src/telemetry.rs`
- `crates/observability/src/lib.rs`
- `crates/observability/src/startup.rs`
- `crates/observability/src/telemetry.rs`
- `docs/reviews/mobile-startup-observability.md`

Mobile:

- `src/hooks/use-thread-tree-startup.test.tsx`
- `src/services/gateway/session-diagnostics.test.ts`
- `src/services/telemetry/mobile-startup-tree.ts`
- `src/services/telemetry/mobile-startup.test.ts`
- `src/services/telemetry/thread-opening-readiness.ts`
- `src/services/telemetry/thread-opening-timeline.ts`
- `src/services/telemetry/thread-opening.test.ts`
- `src/services/telemetry/thread-opening.ts`
- `modules/pioneer-client-nitro/boundary-source.json`
- `src/client/native.ts`
- `src/components/overlays/menu/components/tabs/index.tsx`
- `src/hooks/use-active-thread.ts`
- `src/hooks/use-gateway-session.test.tsx`
- `src/hooks/use-gateway-session.ts`
- `src/hooks/use-thread-tree.ts`
- `src/routes/_layout.tsx`
- `src/routes/thread/[threadId].tsx`
- `src/routes/thread/new.tsx`
- `src/screens/home/components/thread-tree/index.tsx`
- `src/screens/thread/index.tsx`
- `src/screens/threads/folder/index.tsx`
- `src/services/gateway/session-coordinator.ts`
- `src/services/telemetry/mobile-startup-readiness.test.ts`
- `src/services/telemetry/mobile-startup-readiness.ts`
- `src/services/telemetry/mobile-startup.ts`

Тесты не запускались. Реализация передана на внешнее ревью
