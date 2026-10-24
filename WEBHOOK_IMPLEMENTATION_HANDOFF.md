# Task webhook implementation handoff

Status: the user accepted the implementation and authorized commits, GitHub pushes and pull requests. Local builds, tests, generators and project binaries remain prohibited; only Cargo formatting is allowed locally.

## Publication update

- Branch in both repositories: `feature/task-webhook-trigger`; the worktree paths below remain unchanged.
- Relay PR: https://github.com/pioneerdotai/relay/pull/2. Published relay revision: `62103cbd83e9533304e62cba6c14fb4871a08c43`.
- Relay includes the two upstream main commits through `b2cf57dc01cc04e0ea012fa316ad7b669ad6275a`; Git merged them without conflicts. The authenticated metadata read and the newer control-session lifecycle remain in place.
- Pioneer now pins this published relay revision in its workspace manifest and matching lockfile source. The absolute local override remains an optional historical review aid, not a published dependency requirement.
- One durable CI monitor will cover the Pioneer PR and its relay companion, with results delivered to the initiating conversation. It must pause future firings during repairs, preserve both feature branches, check conflicts with current main after each fix, and rearm for 15 minutes after each pending check. It must stop permanently after verified GREEN or PR closure.

The remaining sections preserve the review history, original base revisions and commands from earlier rounds. Their references to unpublished work and lack of acceptance describe those earlier stages.

## Изолированный комплект

| Репозиторий | Worktree | Ветка | Original base SHA |
|---|---|---|---|
| Pioneer | `/Users/alexander/Code/pioneer/pioneer/.worktrees/task-webhook-trigger` | `feature/task-webhook-trigger` | `afe4e51b5e69bbe6a2c90b148462f224dab54a00` |
| Relay | `/Users/alexander/Code/pioneer/pioneer-relay/.worktrees/task-webhook-trigger` | `feature/task-webhook-trigger` | `93e0ddccab7cb6c255b417c391ef649a4f7554a7` |

Обе реализации находятся в **незакоммиченных изменениях этих двух worktree** поверх указанных HEAD. Они образуют один согласованный комплект. Новые файлы также перечислены ниже; обычный `git diff` их не показывает, используйте дополнительно `git status --short` и читайте новые файлы. Коммиты, push, merge, PR и deploy не выполнялись. Исходные рабочие копии не переключались. В исходном relay ещё до работы был untracked `.worktrees/`; он не удалён и `.gitignore` не изменён. Чужие worktree/ветки не менялись, prune не выполнялся.

Pioneer по-прежнему указывает Git dependency `https://github.com/pioneerdotai/relay.git`, version `0.6.1`; его прежний lock pin `b2cf57dc01cc04e0ea012fa316ad7b669ad6275a` **не содержит нового API**. Для этого комплекта необходим локальный override ниже. В relay локальная версия manifest и соответствующая запись существующего lockfile статически выровнены с `0.6.0` до `0.6.1`. Это не публикация релиза. В Pioneer lockfile вручную добавлена только уже существующая `windows-sys 0.61.2` в зависимости gateway; разрешение Cargo не запускалось. Публикация relay и перевод Pioneer на опубликованную ревизию — отдельный этап после принятия.

## Карта пути и контракт

1. Model `task_create` принимает `trigger: {"kind":"external","source":"webhook"}` с обычным agent executor. `eventType` и `filter` для webhook отвергаются. Нужны сохранённые самодостаточные instructions/outputInstructions. Обычный task input сохраняется.
2. Gateway заранее получает устойчивую keystore-запись: для повторов model-tool используется SHA-256 стабильной идентичности вызова плюс существующая блокировка и action receipt. Ошибка task DB commit не ротирует уже подготовленный секрет. Нативный RPC следует существующей семантике создания без model-tool receipt. `task_get` только читает секрет, ничего не генерирует.
3. TaskService создаёт обычные task/trigger/actor/admission. `generate_id(21)` использует проектный alphanumeric NanoID. Триггер сохраняет id и opaque `webhook_secret_ref`, но не bearer secret и не host. Начальный run отсутствует; статус Waiting. По умолчанию только точный webhook spec (`source:"webhook"`, без eventType/filter) использует фоновые detached/keep-active/origin-thread delivery правила, явные lifecycle/delivery настройки имеют приоритет. Старые сохранённые политики не мигрируют.
4. `POST /webhooks/tasks/{trigger_id}/{secret}` — отдельный Axum route с прежним общим header bound 16 KiB и своей body policy. Другие маршруты продолжают запрещать body. Лимит payload **1 MiB**, суммарное чтение body **10 секунд**, включая chunked. Никакого JSON-конверта/обязательных полей. Content-Type сохраняется буквально как допустимое HTTP header string; дубли Content-Type отвергаются. UTF-8 хранится без trim/потерь, остальные байты — standard base64 с `encoding:"base64"`; отсутствующий Content-Type остаётся отсутствующим. Пустое тело допустимо.
5. Секрет — 32 случайных байта в hex (64 символа), отдельный keystore-домен `pioneer.gateway.task_webhook_secret`. HTTP проверяет его constant-time. Неверный секрет и отсутствующий endpoint возвращают одинаковый 404. Некорректная форма capability тоже 404. При верном секрете paused/cancelled/terminal/finish-requested дают 409; недоступный runtime/storage или исчерпанная конкурирующая подготовка — 503. Слишком большое body — 413, timeout — 408, неверное чтение/Content-Type — 400.
6. `TaskService::accept_webhook` читает ограниченный admission snapshot (task, нужный trigger, immutable actor, актуальный base agent spec, следующий run number и execution generation), без истории payload/spec/review. Затем подготавливает событие RunCreated, snapshot agent spec с новым id, TaskQueued, TriggerFired refresh и обычный occurrence. В короткой сериализованной writer transaction CrudStore повторно проверяет trigger status/ref/spec, task revision/status/finish, следующий run_number и следующий уникальный execution_generation, затем атомарно сохраняет events/projections/run/input/occurrence. При изменившемся снимке preparation повторяется вне writer, максимум 32 попытки. У каждого POST новый run/group id; одинаковые body не дедуплицируются.
7. Только после commit выполняются wake/fanout; HTTP отвечает **202 `{"runId":"..."}`**. Handler не запускает агент и не откладывает durable insert в detached spawn. Существующая сохранённая очередь runs, scheduler, actor/grant/resource admission, executor, retries, recovery, review и delivery остаются владельцами исполнения.
8. Prompt compiler добавляет `WEBHOOK INPUT DATA` с JSON-представлением принятого входа и явной границей внешних данных. Goal, инструкции, обычный input, контекст и разрешения компилируются прежним путём. Retry создаёт новый TaskRun, копируя triggerInput; recovery читает сохранённый run.
9. По завершении run keep-active webhook снова Waiting. Создание idle webhook возвращает `runId:null`, `waitable:false` даже при явно attached lifecycle; создатель не обязан ждать входящий POST. Повторный POST снова запускает обычную задачу. Pause/resume сохраняют capability, finish запрещает новый admission и следует существующим правилам draining/очистки queued work. Перепланирование существующего webhook сохраняет ref; превратить обычную задачу в новый webhook через update/reschedule нельзя без provisioning в task_create.
10. При авторизованных create/get Gateway дополняет ответ текущими `webhookUrl` и `webhookScope: public|lan|loopback`; поля отражены в TaskCreateResponse/PublicTaskGetResponse. Model-tool replay заново проверяет TaskRead и вычисляет URL, исторический host не кэшируется в mutation receipt. Общие списки/уведомления используют прежние безопасные проекции без capability и triggerInput. Операторские task/event представления сохраняют прежнюю модель доступа к приватному input. Debug triggerInput редактирован; HTTP route не трассирует path/body и ставит no-referrer.

## URL: relay → handshake → supervisor → settings → response

- Реестр relay передаёт **точный `RelayTunnelConfig.url`** после успешной аутентификации. Wire version повышен до **2**. Hello/Auth/Ack сохраняют фиксированный PacketLength; после Ack::Ok следует `u16` big-endian длина UTF-8 metadata, максимум 8192 байта, затем URL. Ноль означает None для обычного service server; TokenHash relay client требует URL. Все отправители/читатели и client events обновлены согласованно.
- Неизвестная версия, недопустимый URL, не-UTF-8, oversize/truncated metadata, отсутствие relay URL отвергаются до Connected. Public URL требует HTTPS и не допускает credentials/query/fragment/unspecified host/port 0. Домен и prefix не вычисляются из ключа или control address.
- `RatholeClientEvent::ControlChannelConnected.public_base_url` проходит в RemoteAccessSupervisor. Статус имеет URL только для текущего успешно аутентифицированного Connected. При reconnect/disconnect/failure/смене key или отменённом apply старый URL не считается текущим. Существующая supervisor ownership/generation схема сохранена.
- Gateway сериализует settings updates с runtime apply, берёт актуальный supervisor snapshot под его блокировкой и сохраняет `remote_access.public_base_url` существующим TOML persistence. Данные от старого watch notification не копируются как актуальные. Disable/rekey/server change очищают cache. Отложенный startup заново читает актуальные settings под тем же update lock.
- Выдача ссылки использует текущий authenticated Connected URL, иначе локальный адрес. Дисковый cache сам по себе не доказывает подключение и не выбирается base URL.
- Локальный адрес определяется при выдаче ссылки. Фактический bound SocketAddr (включая автоматически выделенный порт) записывается после `TcpListener::local_addr()`. Explicit bind сохраняется, loopback остаётся loopback; явные scoped/link-local IPv6 bind отклоняются описанной ниже ошибкой. Для wildcard пассивный getifaddrs (macOS/Linux/Unix) или GetAdaptersAddresses (Windows) выбирает активный unicast интерфейс того же семейства; private/ULA предпочтительны. Без подходящего адреса — loopback. 0.0.0.0/:: не выдаются как destination. Сканирования, сокетных LAN-проб, ping, Bonjour, запросов дополнительных прав нет.
- `GatewayBaseUrl::task_webhook_url` добавляет capability path с сохранением существующего canonical path prefix, порта и IPv6 authority. IPv6 link-local с zone identifier не выбирается из wildcard интерфейсов, поскольку переносимого URL zone формата в существующем GatewayBaseUrl нет. Только wildcard fallback может вернуть loopback. Явный `[fe80::1234%7]:17878`, любой IPv6 bind с ненулевым scope_id и link-local bind без зоны возвращают ошибку `GatewayBaseUrl cannot preserve the required zone identifier`: существующий URL-тип не поддерживает zone identifiers. Ни потеря scope, ни подмена явного listener на loopback не допускаются. При текущем authenticated public URL локальный fallback не нужен. Firewall/router/OS-настройки не менялись.

## Хранение и необходимые общие правки

Миграция `m20261023_000001_task_webhook` добавляет nullable `task_trigger.webhook_secret_ref TEXT` и `task_run.trigger_input_json TEXT`. Она транзакционная, проверяет наличие колонок при up/down, не переписывает существующие задачи. Вход run входит в обычный RunCreated/RunRetryScheduled event/projection контур. Keystore хранит bearer отдельно. Host не хранится в task. Схемы/entity/repository mapping обновлены статически.

Общие правки ограничены необходимыми инвариантами:

- `has_capacity_for_queued_run` считает активные слоты отдельно от Queued, учитывает порядок готовых run. CrudStore атомарно повторяет capacity/order check внутри Queued→Starting claim, включая конкурентных диспетчеров. Большой triggerInput декодируется до writer и присоединяется к возвращённому run после commit; внутри сравнивается его сохранённое представление.
- Terminal preparation допускает ограниченную переподготовку после доказанного нового webhook admission; защищённые snapshot/fence факты продолжают сравниваться, retry вычисляет run_number по свежему состоянию (W1 ниже).
- Cancellation готовила run set вне writer. Добавлена финальная writer-проверка, что webhook-задача не имеет пропущенного живого run; точный precommit conflict повторно подготавливает план максимум 8 раз. Общие и postcommit ошибки не повторяются как успешная операция. Уже отменённые предыдущими quanta runs не блокируют финальный commit.
- Test-only исторические projector fixtures получили две nullable колонки, чтобы сегодняшние Entity могли читать старый тестовый schema prefix без изменения migration markers. Два client test literals и другие существующие fixtures дополнены новыми optional полями; это не новая UI-функция.
- Никаких новых inbox/broker/worker/provider adapter/dedup/coalescing механизмов нет.

## Статическое саморевью

Промежуточные проверки охватили обе ветви create/get и replay, field constructors/exports, migration/entity/repository/event mapping, retry/new-run copy, recovery/read paths, public/operator projections, framing/version/PacketLength, settings/key races и Unix/Windows cfg/dependencies.

При проходах исправлены: уникальность occurrence generation; сохранение отдельного base agent spec; атомарный claim при нескольких Queued; отмена позднего webhook run; enum/constructor несоответствия в новых тестовых fixtures; чтение body вне claim writer; отсутствие URL/scope в typed response schemas; idle waitability при explicit attachment. Необязательные webhook prompt требования сужены по source, чтобы не менять валидацию старых generic External задач.

Перед первым внешним ревью был выполнен отдельный статический проход по diff обоих репозиториев и новым файлам. Последовавшее внешнее ревью выявило W1–W6; прежняя самооценка не являлась доказательством корректности. Исправления и повторный проход описаны отдельно ниже. Это **не доказательство компиляции и не внешняя приёмка**.

На первоначальном этапе фактически выполнено: `cargo fmt --all`, `cargo fmt --all -- --check` в обоих worktree; `git diff --check` в обоих worktree. В relay formatter предупреждает о существующем nightly-only imports_granularity, команды завершились успешно. Дополнительно статически прочитаны 53 изменённых JSON schema-файла: синтаксис корректен, внутренние `$defs` ссылки разрешаются. Посторонних изменений форматирования не оставлено.

## Исправления после первого внешнего ревью: W1–W6

Все изменения этого раунда остаются в прежнем Pioneer worktree/ветке поверх того же base SHA. Relay diff сохранён без новых правок; новая relay API по-прежнему доступна только в согласованной рабочей копии, не в старом remote pin. Миграция, keystore и формат сохранённого input не изменялись этим раундом.

### W1 — terminal commit при новом admission

Файлы: `crates/crud/src/task_terminal.rs`, `crates/crud/src/repositories/task_run.rs`, `crates/tasks/src/tests_webhook.rs`.

Writer распознаёт только конкретный конфликт роста webhook-очереди: появился обычный webhook run с большим номером и собственным input, а snapshot отличается лишь latest_run_number, разрешённым aggregate Queued/status, временем firing/update активного webhook trigger. Общий TaskTerminalConflict по-прежнему возвращается наружу. Узкий внутренний marker разрешает переподготовить write-set вне writer в существующем ограниченном цикле (не более 16 commit-попыток). До принятия нового write-set повторно сравниваются policy/actor, task revision, исходный run/spec/occurrence, locks и execution/attempt fence; cancellation/resume/изменения политики не превращаются в допустимый конфликт. Retry строится с актуальным latest_run_number и исходным input. Queue watermark читается до mutable task/trigger фактов: admission во время отдельных reader-запросов preparation тоже остаётся видимым как рост очереди при commit, а не как необъяснимый aggregate conflict. Финальная writer-проверка сохраняется при каждой попытке.

Подготовлены owner-регрессии `terminal_commit_reprepares_after_webhook_admission_without_losing_result_or_retry_number` (детерминированно prepare первого → admission второго → commit; success/result/delivery первого и оба body; отдельная retry-ветка получает run_number=3) и `queued_webhook_does_not_allow_terminal_commit_to_adopt_changed_policy` (конфликт политики остаётся отклонённым). Они НЕ запускались. Статически проверены rollback отклонённого write-set, точность marker, новая preparation вне writer, повторные fences и отсутствие перезаписи второго run.

### W2 — ограниченная подготовка POST

Файлы: `crates/crud/src/task_webhook.rs`, `crates/crud/src/lib.rs`, `crates/crud/src/repositories/task_agent_spec.rs`, `crates/tasks/src/service.rs`, `crates/tasks/src/tests_webhook.rs`, `crates/crud/src/tests/task_webhook.rs`.

Новый reader `get_task_webhook_admission_snapshot` использует существующие repositories/scoped SqliteDatabase: по одной task/trigger/actor строке, последний base spec (`run_id IS NULL`, created_at DESC, как у обычного update/executor), агрегат следующего run_number и одну актуальную occurrence generation. Он не читает прошлые runs/body, run-specific specs, binding/turn/candidate/review history. Общий task_get не менялся. Writer по-прежнему атомарно перепроверяет task revision/status/finish, trigger spec/ref/status и оба номера; optimistic retry перечитывает только этот ограниченный snapshot.

Подготовлена регрессия `webhook_admission_does_not_decode_historical_inputs_or_run_specific_specs`: история содержит отдельные payload/spec snapshots, затем старые payload/spec JSON намеренно делаются нечитаемыми fixture-записью. Полный get_task служит отрицательным контролем, а новый POST обязан сохранить run 9 с собственным body и текущими обновлёнными инструкциями. Существующая CRUD-регрессия DB routing теперь проводит подготовку через этот snapshot, сохраняя физические query_only/Interactive/Maintenance проверки. Тесты НЕ запускались. Статически проверены ограниченность выдачи SQL, актуальность base spec, наследование DB class, неизменность actor и финальная writer-перепроверка. При отдельном просмотре исправлений устранён случайный повторный re-export snapshot внутри test module.

### W3 — defaults только для webhook

Файлы: `crates/tasks/src/policy.rs`, `crates/tasks/src/lib.rs`, `crates/tasks/src/service.rs`, `crates/gateway/src/task_tools/mod.rs`, `crates/gateway/src/message/task_handlers.rs`, `crates/tasks/src/tests_webhook.rs`.

Полноспецификационные helpers применяют keep-active/origin-thread defaults только к точному webhook spec. Старые kind-only helpers и generic External сохраняют прежние complete-on-terminal/no-delivery defaults. Создание quota lease классифицирует ScheduledTask только для scheduled/interval/cron либо точного webhook; generic External остаётся QueuedTask. Gateway normalizes delivery с тем же полным spec, иначе он мог бы затереть правильный service default. Явные lifecycle/delivery берутся прежде defaults; существующие строки БД не мигрируют.

Одна owner-проверка `webhook_defaults_and_admission_class_do_not_change_generic_external_or_overrides` сравнивает webhook/generic External, сохранённые defaults, quota class/bucket и явные overrides. Она НЕ запускалась. Статически просмотрены все изменённые callers, kind-only attachment fallbacks и lease mapping; общий admission engine не менялся.

### W4 — явный scoped IPv6 bind

Файл: `crates/gateway/src/webhook_address.rs`.

Проверка SocketAddrV6 выполняется до извлечения IpAddr: ненулевой scope_id или link-local IP дают ясную ошибку неподдерживаемого zone identifier. GatewayBaseUrl не поддерживает zone URLs; новый парсер не добавлен, явный listener не подменяется loopback. Public relay URL продолжает иметь приоритет. Wildcard selection по-прежнему исключает link-local IPv6.

Подготовлена регрессия `explicit_scoped_ipv6_bind_reports_unsupported_zone_instead_of_losing_it` с fe80::1234, scope 7 и фактическим портом. Она НЕ запускалась. Статически проверены порядок преобразований SocketAddrV6 → IpAddr → GatewayBaseUrl, prefix/порт и отсутствие ложной LAN-ссылки.

### W5 — client JSON schemas

Файлы: `schemas/client/gateway_ws_event.json`, `schemas/client/task_accept_response.json`, `schemas/client/task_revise_response.json`, `schemas/client/task_cancel_response.json`.

Во вложенные полные TaskRun добавлены optional triggerInput и идентичные protocol schemas определения TaskRunTriggerInput/TaskRunInputEncoding; в полный TaskTrigger WS event добавлен optional webhookSecretRef. Безопасные публичные проекции не расширены. Выполнен только статический разбор JSON и сверка полей/required/defs/ref по всему каталогу schemas: 1422 документа, 29 полных TaskRun и 24 полных TaskTrigger, несоответствий и неразрешённых локальных $defs ссылок не найдено. Это не генерация и не runtime-тест. Генераторное сравнение остаётся отложенным до разрешения (команды ниже).

### W6 — настоящий model-facing tool contract

Файл: `crates/gateway/src/message/tests/thread_launch_choices.rs`; существующая HTTP/CRUD/body матрица сохранена.

Тест `webhook_task_tools_create_get_and_replay_require_current_task_read` использует существующую execution-bound authorization/launch fixture и реальные materialized task_create/task_get handlers. Он покрывает default и явно attached lifecycle, отсутствие начального run, waitable:false, полный IPv6 URL/scope и стабильные trigger/secret при повторных get и create с одной mutation identity. После смены listen port cached replay обязан сохранить capability path и обновить host. Затем все handlers с mutation cache удаляются и материализуются заново: replay восстанавливается из настоящего durable CLI action receipt (assert alreadyCommitted), а не из тестового enrich/helper или старого cache. Текущий fixture grant лишается TaskRead, сохраняя TaskCreate: настоящий get отклоняется, create replay не выдаёт URL/scope/secret и не создаёт дубликат. После восстановления grant get снова выдаёт тот же capability на актуальном порту.

Тест НЕ запускался. Статически прослежены загрузка текущего grant на каждом вызове, TaskRead-проверка replay, receipt marker/mutation key, weak ownership CLI cache и запрет disclosure; fixture не вызывает агентный provider или runtime run.

### Повторное статическое саморевью и ограничения

После групп правок просмотрены callers, SQL/transactions, snapshots/fences, enum/struct/schema references и семантика тестов. Затем повторно просмотрен актуальный комплект обоих worktree, новые файлы и отдельно изменения этого раунда относительно сохранённого исходного дерева. Исправления ограничены W1–W6; relay, remote pin, lockfile и миграция в этом раунде не менялись. Известных незакрытых замечаний W1–W6 после статического прохода не осталось; внешнее принятие всё ещё требуется.

Из Cargo в этом раунде выполнялись только cargo fmt (включая --check); отдельно выполнен git diff --check обоих worktree. Тесты, compilation, schema/binding generators и платформенные прогоны **по-прежнему НЕ выполнялись**. Результат форматирования/статического просмотра не подтверждает прохождение этих проверок. История первоначальных разрешённых действий выше сохранена отдельно.

## Исправления после второго внешнего ревью: S1 / C1

В этом раунде изменены только пять файлов Pioneer, перечисленных ниже, включая этот handoff. Предыдущая реализация и W1–W6 сохранены. Relay, manifests/lockfile, remote dependency pin, схемы, миграции, настройки и keystore в этом раунде не менялись. Worktree, ветки и base SHA остаются указанными выше.

### S1 — независимые terminal commits параллельных webhook runs

Файлы: `crates/crud/src/task_terminal.rs`, `crates/tasks/src/tests_webhook.rs`.

Terminal preparation для точного webhook spec (`External`, source=webhook, без eventType/filter) сохраняет trigger.updated_at, полученный при admission. Завершение run не является новым срабатыванием HTTP trigger. Событие RunTerminalStatusRefresh по-прежнему создаётся и атомарно применяется: существующий projector проверяет живые siblings и переводит aggregate в Waiting после последнего run. Пока sibling активен, trigger повторно проецируется с теми же фактами, поэтому подготовленный snapshot этого sibling остаётся действительным. Другие триггеры сохраняют прежнее обновление времени при terminal refresh.

Общая snapshot-проверка и validate_terminal_policy не ослаблены; не добавлены generic retry или исключение полей из сравнения. Task revision, policy/actor, собственный run/spec/occurrence, locks, pause/resume/cancel/finish и execution/attempt fences сохраняются. W1 marker/bounded reprepare/порядок чтения queue watermark и копирование input/актуальный номер retry не менялись. Сериализация остаётся вне writer; новых DB-запросов, соединений, transaction boundaries или действий под DB capacity нет.

Добавлена одна owner-регрессия `parallel_webhook_terminal_commits_preserve_both_results_inputs_and_deliveries` в существующей TaskService suite. Она принимает ровно два POST, допускает оба run при max_parallel_runs=2, подготавливает оба success outcome до первого commit, затем фиксирует их последовательно. Completion timestamps заданы явно как trigger.updated_at+10 и +20; совпадение в пределах одной секунды не скрывает дефект. Проверяются неизменность trigger и Running между commit, оба Succeeded/result/completed_at/input, две обычные origin-thread delivery с соответствующими result snapshots и итоговое Waiting. Тест НЕ запускался.

Статическое саморевью: прослежены подготовка terminal events → writer snapshot/fences → RunCompleted → TaskRescheduled → проверка live siblings → delivery/finalization. Сопоставлены ветки non-webhook/paused/finish и W1 success/retry; refresh-событие не имеет cursor idempotency key и сохраняет прежнюю атомарную дедупликацию terminal batch по run id. Изменение устраняет ненужную запись нового timestamp вместо принятия посторонних snapshot-конфликтов.

### C1 — HTTP timeout проверяется через production handler

Файлы: `crates/gateway/src/transport/http/webhooks.rs` (только tests), `crates/gateway/src/message/tests/task_webhooks.rs`.

Удалена timeout-часть helper-теста, самостоятельно создававшая tokio timeout вокруг read_body. Полезные точные byte-bound проверки helper сохранены (`chunked_body_limit_bounds_ingress`). В существующую HTTP fixture с настоящим merged Gateway router и валидной capability добавлен один запрос с незавершающимся Body stream. Paused clock включается перед поллингом handler; oneshot-сигнал из первого poll тела подтверждает завершение аутентификации и начало чтения до advance(10s). Пока handler ожидает SQLite, yield_now удерживает runtime runnable, чтобы paused clock не перескочил к DB timeout автоматически. Ограничитель реального ожидания начала чтения и отдельный guard ожидания ответа только завершают тест ошибкой при зависании.

Проверяемый ответ 408 может выдать только production handler; внешний guard не генерирует HTTP-ответ. Удаление production timeout приводит к ошибке guard, неверный HTTP mapping — к несовпадению статуса. После resume clock тест сравнивает полный список durable run IDs до/после запроса: новый run не должен появиться. Production HTTP-код, API и hooks не менялись; матрицы HTTP/DB/tools не дублировались. Тест НЕ запускался.

Статическое саморевью: прослежены реальный router/header policy → capability/keystore → первый poll body → production deadline → HTTP 408; при timeout вызов accept_webhook не достигается. Проверены lifetime pinned request/oneshot sender и возобновление часов перед последующим DB-чтением.

### Статический аудит и выполненные команды этого раунда

Повторно просмотрены актуальное дерево diff обоих worktree, новые файлы и отдельно изменения S1/C1 относительно сохранённого дерева перед этим раундом. Проверены сохранность W1–W6, ограниченность production-правки точным webhook spec, DB routing/атомарность/fences, retry/input/delivery и содержательность обеих регрессий. Сверены все 123 изменённых/новых файла комплекта (115 Pioneer, 8 Relay), включая статический разбор всех 56 JSON schemas и локальных ссылок. Relay и остальные файлы комплекта сохранены побайтно. Известных незакрытых S1/C1 после статического прохода не осталось; это не внешнее принятие и не доказательство runtime-корректности.

Разрешённые команды этого раунда: чтение/поиск/статические сравнения, Git status/rev-parse/diff, `cargo fmt --all` в Pioneer, `cargo fmt --all -- --check` и `git diff --check` в обоих worktree. Предыдущие результаты форматирования выше относятся к прошлым этапам. Проверки compilation, tests/nextest/doctests/benchmarks, generators/bindings, binary/приложений и платформенные прогоны по-прежнему **НЕ выполнялись**. Запрет сохраняется до явного внешнего принятия и разрешения.

Обе новые регрессии входят в существующий отложенный фильтр `webhook`. Для точечного запуска после явного разрешения из Pioneer (тот же TASK_RELAY_PATCH, что ниже):

```sh
cargo --config "$TASK_RELAY_PATCH" nextest run --locked -p pioneer-tasks --test-threads 2 --no-fail-fast --retries 0 parallel_webhook_terminal_commits
cargo --config "$TASK_RELAY_PATCH" nextest run --locked -p pioneer-gateway --test-threads 2 --no-fail-fast --retries 0 webhook_http_admits_distinct
```

Эти команды НЕ выполнялись. Локальный relay override и необходимый отдельный этап согласования lock source ниже остаются обязательными; relay не публиковался.

## Исправления после третьего внешнего ревью: S2 / S3

В этом раунде изменены четыре файла Pioneer: `crates/crud/src/task_terminal.rs`, `crates/crud/src/repositories/task_run.rs`, `crates/tasks/src/tests_webhook.rs` и этот handoff. Relay, schemas, миграции, settings/keystore, manifests/lockfiles и remote pin не менялись. Пути/ветки/base SHA и локальный relay override остаются прежними. Описания W1–W6 и S1/C1 выше фиксируют прошлые этапы; их новые сценарии не выполнялись повторно как runtime-проверки.

### Согласованное решение S2 / S3

Используется прежний terminal prepare → serialized writer snapshot recheck → commit. Внутренний marker расширен с WebhookAdmissionTerminalRace до WebhookProgressTerminalRace, а не до общего TaskTerminalConflict. Переподготовка write-set остаётся вне writer и ограничена прежними 16 попытками. После неё снова сравниваются защищённые факты относительно предыдущего snapshot, а перед применением событий — текущий атомарный writer snapshot.

Чистое сравнение допускает только точный активный webhook spec с сохранённым secret ref: рост queue watermark и ограниченные aggregate activity labels (Queued/Running/WaitingReview → Running/WaitingReview; Queued при доказанном росте очереди). При неизменном watermark trigger должен совпадать полностью, включая updated_at/last_fire_at. Остальные поля, task revision/политики/actor, собственный run/spec/input/occurrence, locks, stop/finish и execution/attempt fences продолжают сравниваться прежним способом. Новое поведение не распространяется на generic External и другие триггеры; произвольная смена task.status не принимается.

При конфликте writer дополнительно требует доказательства из существующего repository в том же snapshot:

- Рост очереди подтверждается ровно run на актуальном latest_run_number, с тем же task/trigger и сохранённым input. Обычный POST имеет admission occurrence с ключом webhook:<runId>. Retry должен ссылаться на failed sibling (не завершаемый run), сохранять его task/trigger, parent/run_group и точные input JSON bytes, увеличивать attempt на один и иметь admitted source occurrence. Снимается не просто фильтр retries: проверяется их происхождение. Исторические body не передаются в Rust и не декодируются.
- Изменение activity подтверждается последним durable событием, выставляющим activity label (TaskQueued / RunStarted / TaskRunEnteredReview / TaskRevisionRequested). Оно должно принадлежать другому начатому agent run этого task/trigger с сохранённым input и occurrence и выставлять именно Running либо WaitingReview. Завершаемый run исключён из свидетельства. Свидетельство сохраняется, если sibling успел terminalize после start/review; полный payload события не читается. Для Queued после нового POST/retry свидетельством служит сам проверенный рост очереди.

Оба SELECT возвращают только scalar existence (LIMIT 1); queue lookup ограничен точным номером, retry source ищется по PK, activity event выбирается через task/sequence index и метаданные типа/ссылок. Поля input сравниваются внутри SQLite лишь у этой пары строк. Reader/writer scheduling class наследуется от переданного scoped ConnectionTrait, новые соединения/транзакции/worker не вводятся. Сериализация событий/подготовка retry остаются вне writer, уведомления — после commit.

### Подготовленные owner-регрессии — НЕ запускались

Существующий `parallel_webhook_terminal_commits_preserve_both_results_inputs_and_deliveries` расширен без новой HTTP/tool матрицы: success/success сохраняет S1, failure+retry / success покрывает S2, два заранее подготовленных failures проверяют fresh retry numbers 3/4. Для каждого source проверяются исходный body, outcome, retry_of/parent/group/attempt, собственный input retry и обычная доставка только окончательного success. Timestamps по-прежнему явно отличаются на +10/+20. Финальный Waiting проверяется в success/success, а Queued с сохранёнными retry — в retry-вариантах.

Добавлен `webhook_terminal_does_not_adopt_retry_with_changed_source_input`: после настоящего sibling retry fixture намеренно портит input successor; неподтверждённый рост очереди должен остаться TaskTerminalConflict без terminal outcome/delivery другого run.

Добавлен `webhook_terminal_reconciles_started_sibling_but_rejects_unbacked_activity`: prepare A после принятия Queued B → штатные claim/execution/mark_started B → commit A. Проверяются результат/input/delivery A, Running/input B, неизменные trigger и run numbers. Дополнительный вариант завершает B до commit A и проверяет две delivery и итог Waiting: activity label не становится ошибочным свидетельством лишь потому, что sibling уже terminal. Отрицательный вариант меняет только aggregate label fixture-записью без старта B: наличие собственного Running A не является доказательством sibling activity, commit отклоняется. Существующая changed-policy отрицательная регрессия сохранена.

Review-ветка прослежена статически через настоящий record_pending_review_result_candidate → TaskRunEnteredReview → projector: B получает WaitingReview, имеет started_at, admission occurrence и событие TaskRunEnteredReview, A не меняется. Проверка не требует уже записанного occurrence.status B, поскольку штатный API обновляет его после event batch отдельным шагом. Искусственный review-переход в тестах не добавлялся; runtime-прохождение этого варианта не утверждается.

### Статическое саморевью / ограничения

Просмотрены POST/retry/start/review/terminal ordering, оба repository predicates, pure snapshot comparison, bounded reprepare и повторная writer-проверка. Сопоставлены W1 (новый POST и свежий retry number), S1 (два success), stop/policy/actor/revision/run/occurrence/attempt fences, idle Waiting и ordinary delivery. Затем просмотрены полное актуальное дерево обоих worktree/новые файлы и отдельно изменения этого раунда; сохранность остальных файлов сверена побайтно с исходным деревом раунда. Код не принят самостоятельно: внешнее принятие остаётся обязательным.

Фактически в этом раунде выполнены чтение/поиск, Git status/diff/rev-parse, статические сравнения/разбор JSON, cargo fmt --all в Pioneer, cargo fmt --all -- --check и git diff --check в обоих worktree. Старые записи проверок выше относятся к прошлым этапам. Тесты/nextest/doctests/benchmarks, compilation/check/build/run/clippy, schema/binding generators, binaries/apps/Gateway/relay, CI и platform runs **НЕ выполнялись**. Форматирование и саморевью не снимают запрет исполнения.

После внешнего принятия и отдельного разрешения (из Pioneer, с TASK_RELAY_PATCH и согласованием lock source как ниже) подготовлены точечные команды, которые сейчас НЕ выполнялись:

```sh
cargo --config "$TASK_RELAY_PATCH" nextest run --locked -p pioneer-tasks --test-threads 2 --no-fail-fast --retries 0 parallel_webhook_terminal_commits
cargo --config "$TASK_RELAY_PATCH" nextest run --locked -p pioneer-tasks --test-threads 2 --no-fail-fast --retries 0 webhook_terminal_
cargo --config "$TASK_RELAY_PATCH" nextest run --locked -p pioneer-tasks --test-threads 2 --no-fail-fast --retries 0 terminal_commit_reprepares_after_webhook
cargo --config "$TASK_RELAY_PATCH" nextest run --locked -p pioneer-tasks --test-threads 2 --no-fail-fast --retries 0 queued_webhook_does_not_allow
```

## Исправление после четвёртого внешнего ревью: остаточный S2

Изменены только `crates/crud/src/repositories/task_run.rs`, `crates/tasks/src/tests_webhook.rs` и этот handoff. Relay и остальные 120 файлов согласованного комплекта сохранены без правок этого раунда. Предыдущая запись S2 о наличии source occurrence описывает старое доказательство: после штатного retry admission run_id этой строки уже указывает на successor.

### Доказательство logical occurrence после retry admission

`is_webhook_queue_run` сохраняет точный latest watermark, failed source, task/trigger/executor, parent, group, attempt+1 и побайтовое равенство input JSON. Вместо обязательной привязки occurrence к failed source запрос проверяет существующую постоянную logical identity: occurrence_id указывает на исходный webhook run первой попытки того же task/trigger/group, occurrence_key равен `webhook:<originId>`, action idempotency key — `task:<taskId>:<originId>`. Origin проверяется по PK, без обхода истории/цепочки retries и без чтения его payload в Rust.

Владелец этой occurrence может быть только непосредственный failed source (до admission) либо именно проверяемый successor (после admission). retry_attempt должен соответствовать владельцу: 0 для исходного webhook, номер попытки для допущенного retry. Поиск ограничен двумя уникальными run_id, origin/source — PK lookup; результат scalar EXISTS. Статусы Starting/Running/review/terminal successor не меняют постоянные идентичности и не обнуляют доказательство. При появлении следующего retry проверяется уже новый актуальный watermark и его непосредственный source. EXISTS не удалён; произвольная соседняя occurrence не принимается.

Task revision, policy/actor, собственные run/spec/input/occurrence и locks, stop/finish/cancel/resume, execution/attempt fences, bounded reprepare вне writer и финальная атомарная writer-проверка не изменены. Новых SQL соединений, persistence, фоновых операций или тяжёлого разбора внутри DB capacity нет. Подготовка/уведомления сохраняют прежние границы.

### Подготовленная регрессия — НЕ запускалась

В `parallel_webhook_terminal_commits_preserve_both_results_inputs_and_deliveries` добавлен ровно один вариант: prepare failure A и success B → commit A → `previous.retry` и настоящий `claim_task_retry_for_dispatch_with_event` C в ready_at → commit ранее подготовленного B. Никакой выдуманной occurrence или non-retry admission нет. Проверяется, что A больше не владеет occurrence, C Starting владеет теми же occurrence_id/key/action/generation и attempt 2, имеет body A и правильные parent/retry_of/group; B Succeeded сохраняет свой body/result и получает ровно одну обычную thread delivery; номера 1/2/3. Вариант обнаруживает прежний отказ после реального rebind.

Прежние success/success, queued retry/success и два queued retry с номерами 3/4 остаются в той же owner-suite. Отрицательные input/policy/unbacked-status проверки, S3 start/review semantics, W1 admission race и S1/C1 не изменены. Новых HTTP/tool матриц нет. Команда после полного внешнего принятия и отдельного разрешения остаётся прежней: `cargo --config "$TASK_RELAY_PATCH" nextest run --locked -p pioneer-tasks --test-threads 2 --no-fail-fast --retries 0 parallel_webhook_terminal_commits` (с согласованием lock source, как ниже).

### Статическое саморевью и выполненные команды

Прослежен путь retry creation → previous.retry → атомарный occurrence rebind/Starting → Running/review/terminal → terminal commit соседа; проверены SQL predicates, первая и последующие retry-попытки, сохранность snapshot/fences и отсутствие unbounded history scan. Отдельно сопоставлены новые утверждения owner-теста с настоящим claim/projector и delivery paths. Полный актуальный diff/новые файлы обоих worktree и отдельный diff этого раунда просмотрены; сохранность остальных файлов сверена побайтно с начальным снимком раунда.

Фактически в этом раунде выполнены чтение/поиск, Git status/diff/rev-parse, статические файловые сравнения и разбор JSON, `cargo fmt --all` в Pioneer, `cargo fmt --all -- --check` и `git diff --check` в обоих worktree; все перечисленные проверки форматирования/whitespace завершились с кодом 0. Relay fmt сообщает о существующем nightly-only imports_granularity. Статически разобраны все 56 изменённых JSON-документов: внутренние ссылки разрешаются, optional triggerInput/webhookSecretRef и вложенные input definitions соответствуют protocol schemas; публичные безопасные типы не получили этих полей. Побайтное сравнение подтвердило 112 неизменённых файлов Pioneer и 8 Relay относительно снимка начала этого раунда. Только форматирование запускалось из Cargo. Результаты команд относятся к этому раунду; прошлые записи выше не объявляются новыми запусками. Компиляция, тесты/nextest/doctests/benchmarks, генераторы, бинарники, Gateway/relay/app, CI и платформенные прогоны **НЕ выполнялись**. Внешнее принятие не объявляется, запрет исполнения сохраняется.

## Написанные тесты — НЕ запускались

- `crates/gateway/src/message/tests/task_webhooks.rs`: реальный merged Router, отсутствие начального run, create/get typed URL, конкурентные POST, сохранённые точные inputs/Content-Type, run numbers, неверный секрет/отсутствующий endpoint, oversize.
- `crates/gateway/src/transport/http/webhooks.rs`: plain JSON без envelope, произвольный UTF-8/пустой/нетекстовый body, base64 и chunked byte bound; production deadline/408 покрывает merged router fixture в `message/tests/task_webhooks.rs`; `header_policy.rs`: изоляция старой body policy и header/referrer bounds.
- `crates/gateway/src/secrets.rs`: повторная provisioning после неуспешного task commit и повторное открытие keystore без ротации; `task_projection.rs`: privacy canaries body/ref.
- `crates/tasks/src/tests_webhook.rs`: независимые/конкурентные/одинаковые POST, unique run/generation, base input/spec сохранены, конкурентный FIFO claim при лимите 1, pause/resume/cancel/finish; runtime restart/retry input; реальный scheduler последовательно исполняет два ожидающих run при лимите 1, обычная доставка и возвращение Waiting; cancellation gate для поздних Queued и Starting runs.
- `crates/crud/src/tests/task_webhook.rs`: Interactive/Maintenance physical routing и query_only readers, event ordering, новое открытие физической БД с сохранёнными body/occurrence/ref, rollback events/run/task/fanout при ошибке occurrence insert, recheck pause/cancel/finish, отмена ожидающего writer без leaked queue entries, retry и повторный up migration с сохранением данных.
- `crates/promt/src/render/task_run.rs`: добавление только секции внешнего входа при сохранении остальных байтов обычного prompt/context/permissions.
- `protocol/gateway_endpoint`, `gateway/webhook_address`, settings и message tests: prefix/IPv6/port, loopback/family/interface selection, save/reload/rekey clearing и игнорирование stale persisted relay cache.
- relay `protocol::tests`, tunnel unit/lifecycle и `relay_token_routing`: framing и следующий control packet, неверные metadata/version, URL реестра по аутентифицированному ключу/prefix, смена key, disconnect/cancelled apply без stale URL.

## Проверки после полного внешнего принятия и явного разрешения

**Следующие команды НЕ выполнялись.** Сначала ревьюер принимает оба source diff, затем отдельно разрешает compilation/tests/lock refresh/generation. Команды запускаются в указанных worktree. Новый path source требует пересогласования Cargo.lock; нельзя обещать успех `--locked` с прежним remote lock pin.

```sh
cd /Users/alexander/Code/pioneer/pioneer/.worktrees/task-webhook-trigger
TASK_RELAY_PATCH='patch."https://github.com/pioneerdotai/relay.git".rathole.path="/Users/alexander/Code/pioneer/pioneer-relay/.worktrees/task-webhook-trigger"'

# Первый разрешённый check также согласует local path source в lockfile.
# Осознанно без --locked только для этого шага; diff lockfile затем проверить.
cargo --config "$TASK_RELAY_PATCH" check -p pioneer-protocol -p pioneer-crud -p pioneer-tasks -p pioneer-promt -p pioneer-tunnel -p pioneer-gateway

cargo --config "$TASK_RELAY_PATCH" nextest run --locked -p pioneer-crud -p pioneer-tasks -p pioneer-promt -p pioneer-protocol -p pioneer-gateway --test-threads 2 --no-fail-fast --retries 0 webhook
cargo --config "$TASK_RELAY_PATCH" nextest run --locked -p pioneer-gateway --test-threads 2 --no-fail-fast --retries 0 explicit_scoped_ipv6_bind
# W1/W2/W3/W6 owner tests выбираются предыдущим webhook-фильтром.
cargo --config "$TASK_RELAY_PATCH" nextest run --locked -p pioneer-tunnel -p pioneer-keystore --test-threads 2 --no-fail-fast --retries 0
cargo --config "$TASK_RELAY_PATCH" nextest run --locked -p pioneer-gateway --test-threads 2 --no-fail-fast --retries 0 collaborator_projection
# После целевых owner-проверок — требуемые общие/соседние регрессии, при разрешении:
cargo --config "$TASK_RELAY_PATCH" nextest run --locked -p pioneer-crud -p pioneer-tasks -p pioneer-promt -p pioneer-protocol -p pioneer-gateway --test-threads 2 --no-fail-fast --retries 0

cd /Users/alexander/Code/pioneer/pioneer-relay/.worktrees/task-webhook-trigger
cargo nextest run --locked -p rathole --test-threads 2 --no-fail-fast --retries 0 protocol::tests
cargo nextest run --locked -p rathole --test-threads 2 --no-fail-fast --retries 0
```

Отдельно нужны Windows/Linux/macOS compile+platform runs для пассивных системных API. При установленных target/SDK и разрешении, из Pioneer с тем же override:

```sh
cargo --config "$TASK_RELAY_PATCH" check -p pioneer-gateway --target x86_64-pc-windows-msvc
cargo --config "$TASK_RELAY_PATCH" check -p pioneer-gateway --target x86_64-unknown-linux-gnu
cargo --config "$TASK_RELAY_PATCH" check -p pioneer-gateway --target aarch64-apple-darwin
```

## Отложенная штатная генерация

Ручные изменения схем отражают новые контрактные поля. Генераторы не выполнялись, поскольку исполняют проектный код. После разрешения, из Pioneer:

```sh
cargo --config "$TASK_RELAY_PATCH" run --locked -p pioneer-protocol --bin schema -- schemas
cargo --config "$TASK_RELAY_PATCH" run --locked -p pioneer-client --features schema --bin schema -- schemas/client
# Если текущий потребитель требует FFI boundary schema:
cargo --config "$TASK_RELAY_PATCH" run --locked -p pioneer-client-ffi --features schema --bin schema -- schemas/client
```

Проверить generated diff и сохранить только контрактные изменения этого feature. Нового FFI API/нового UI не вводилось; platform binding generators не запускались. До review нельзя запускать export scripts как обход запрета.

## Ограничения проверки

Ни одного cargo check/build/run/test/nextest/clippy/update/metadata/generate-lockfile, rustc, rust-analyzer, проектного генератора, приложения, Gateway, relay или smoke binary не запускалось. CI не инициировался. Компиляция, новые и старые тесты, реальные HTTP/relay/platform прогоны и сравнение с результатом генераторов **не проверены**. Форматирование и статическое чтение не заменяют эти этапы. Новые tests сами ещё могут содержать дефекты, выявляемые компилятором/внешним ревью. Сетевые/firewall/router разрешения и production settings/data не менялись.

## Полный перечень файлов комплекта

Изменения сгруппированы выше по назначению. Здесь перечислены все tracked modified и новые файлы для воспроизводимого ревью, включая статические схемы и fixture additions.

### Pioneer

- `schemas/client/task_revise_response.json`

- `schemas/client/task_cancel_response.json`

- `schemas/client/task_accept_response.json`

- `crates/crud/src/task_webhook.rs`
- `crates/crud/src/tests/task_webhook.rs`
- `crates/gateway/src/message/tests/task_webhooks.rs`
- `crates/gateway/src/message/webhook_endpoint.rs`
- `crates/gateway/src/transport/http/webhooks.rs`
- `crates/gateway/src/webhook_address.rs`
- `crates/migration/src/m20261023_000001_task_webhook.rs`
- `crates/tasks/src/tests_webhook.rs`
- `schemas/task_run_input_encoding.json`
- `schemas/task_run_trigger_input.json`
- `schemas/task_webhook_scope.json`
- `Cargo.lock`
- `crates/client/src/gateway/settings_store.rs`
- `crates/client/src/runtime.rs`
- `crates/crud/src/lib.rs`
- `crates/crud/src/repositories/task_agent_spec.rs`
- `crates/crud/src/repositories/task_run.rs`
- `crates/crud/src/repositories/task_trigger.rs`
- `crates/crud/src/task_delivery_lifecycle.rs`
- `crates/crud/src/task_projector.rs`
- `crates/crud/src/task_terminal.rs`
- `crates/entity/src/task_run.rs`
- `crates/entity/src/task_trigger.rs`
- `crates/gateway/Cargo.toml`
- `crates/gateway/src/lib.rs`
- `crates/gateway/src/mcp_service.rs`
- `crates/gateway/src/message/mod.rs`
- `crates/gateway/src/message/task_handlers.rs`
- `crates/gateway/src/message/tests.rs`
- `crates/gateway/src/message/tests/task_capsule.rs`
- `crates/gateway/src/message/tests/thread_launch_choices.rs`
- `crates/gateway/src/secrets.rs`
- `crates/gateway/src/settings.rs`
- `crates/gateway/src/task_projection.rs`
- `crates/gateway/src/task_tools/mod.rs`
- `crates/gateway/src/transport/http/errors.rs`
- `crates/gateway/src/transport/http/header_policy.rs`
- `crates/gateway/src/transport/http/mod.rs`
- `crates/gateway/src/transport/http/router.rs`
- `crates/gateway/src/transport/mod.rs`
- `crates/gateway/src/transport/server.rs`
- `crates/keystore/src/id.rs`
- `crates/migration/src/lib.rs`
- `crates/promt/src/render/task_run.rs`
- `crates/protocol/src/gateway_endpoint.rs`
- `crates/protocol/src/lib.rs`
- `crates/protocol/src/schema.rs`
- `crates/protocol/src/settings.rs`
- `crates/protocol/src/task.rs`
- `crates/tasks/src/policy.rs`
- `crates/tasks/src/scheduler.rs`
- `crates/tasks/src/service.rs`
- `crates/tasks/src/tests.rs`
- `crates/tasks/src/trigger.rs`
- `crates/tunnel/src/lib.rs`
- `crates/tunnel/tests/relay_token_routing.rs`
- `crates/tunnel/tests/remote_access_lifecycle.rs`
- `resources/skills/pioneer/tasks/SKILL.md`
- `resources/skills/pioneer/tasks/references/delivery-and-scheduled.md`
- `resources/skills/pioneer/tasks/references/tool-schemas.md`
- `schemas/agent_durable_event.json`
- `schemas/client/cli_runtime_provider_settings_plan.json`
- `schemas/client/gateway_settings_get_response.json`
- `schemas/client/gateway_settings_state.json`
- `schemas/client/gateway_settings_store.json`
- `schemas/client/gateway_settings_update_plan.json`
- `schemas/client/gateway_settings_update_response.json`
- `schemas/client/gateway_ws_event.json`
- `schemas/client/settings_page_publication.json`
- `schemas/gateway_notification.json`
- `schemas/gateway_remote_access_settings.json`
- `schemas/gateway_remote_access_status_changed_notification.json`
- `schemas/gateway_remote_access_status_snapshot.json`
- `schemas/gateway_settings_get_response.json`
- `schemas/gateway_settings_snapshot.json`
- `schemas/gateway_settings_update_response.json`
- `schemas/public_task_events_response.json`
- `schemas/public_task_get_response.json`
- `schemas/public_task_tree_response.json`
- `schemas/task_accept_response.json`
- `schemas/task_agenda_item.json`
- `schemas/task_agenda_response.json`
- `schemas/task_cancel_response.json`
- `schemas/task_create_response.json`
- `schemas/task_event.json`
- `schemas/task_event_payload.json`
- `schemas/task_events_response.json`
- `schemas/task_get_response.json`
- `schemas/task_pause_response.json`
- `schemas/task_paused_notification.json`
- `schemas/task_queued_notification.json`
- `schemas/task_reschedule_response.json`
- `schemas/task_rescheduled_notification.json`
- `schemas/task_resume_response.json`
- `schemas/task_resumed_notification.json`
- `schemas/task_revise_response.json`
- `schemas/task_run.json`
- `schemas/task_run_completed_notification.json`
- `schemas/task_run_created_notification.json`
- `schemas/task_run_failed_notification.json`
- `schemas/task_run_started_notification.json`
- `schemas/task_scheduled_notification.json`
- `schemas/task_tree.json`
- `schemas/task_tree_response.json`
- `schemas/task_trigger.json`
- `schemas/task_update_response.json`
- `schemas/task_updated_notification.json`
- `schemas/task_wait_item.json`
- `schemas/task_wait_response.json`
- `schemas/task_wait_review_item.json`
- `WEBHOOK_IMPLEMENTATION_HANDOFF.md` — этот отчёт.

### Relay

- `Cargo.lock`
- `Cargo.toml`
- `src/client.rs`
- `src/config.rs`
- `src/lib.rs`
- `src/protocol.rs`
- `src/relay.rs`
- `src/server.rs`
