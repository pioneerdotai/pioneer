# Threads: общий запуск turn — передача на внешнее ревью

Работа подготовлена для внешнего ревью; это не окончательная приёмка. Проверки с выполнением кода проекта не разрешены и не выполнялись.

## База и размещение

- Исходный commit при начале работы: `4f086cbe54e88ceb515d7d4ab4c3cf89ad7fa794`. После обычного `fetch origin main` local main и origin/main совпадали с ним.
- Во время работы main обновился. Изменение перенесено обычным rebase на `7f226499d3fa8beac5dd15b3d241040e7a99016e` — актуальные local main и origin/main при подготовке передачи. Rebase прошёл без конфликтов. Вошедшее в main исправление доставки событий просмотрено отдельно; оно не является частью собственного patch.
- Повторное ревью начато с проверенного `b9a932f576282c0e75f501be3eaf39abf0ecadf5`, рабочее дерево было чистым. База реализации остаётся `7f226499d3fa8beac5dd15b3d241040e7a99016e`; повторные исправления сделаны поверх reviewed HEAD без rebase/merge. На этом этапе фактические local main и origin/main — `dc90743137af9d220a3594c8fe6190bdbde7d999`; они только прочитаны, не интегрировались.
- Ветка: `fix/threads-shared-launch`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-shared-launch`.
- Основной checkout и соседние worktree этой работой не изменялись. Старые экспериментальные ветки Threads не читались как эталон и не интегрировались.
- Коммиты создавались с `core.hooksPath=/dev/null`. Merge, push, deploy и PR не выполнялись.

## Причина и общий механизм

На исходной базе RPC `turn/start` выбирал `DetachedTask` для Collaborative, а `commit_start_agent` сразу создавал executable turn и direct dispatch outbox. Дополнительно `is_detached_admission` определял роль по Principal-автору и текущему типу треда, хотя в хранилище уже был immutable `work_owner`.

В `crates/gateway/src/message/turn_handlers.rs` оба входа теперь вызывают `start_thread_turn`. Это общий выбор по серверному `thread.origin_kind` и общий detached executor `start_detached_thread_turn`: нормализация входа, сохранение исходного сообщения, фиксация authority/security/launch, создание обычной Task через `TaskService`, передача выполнения существующему Task lifecycle. Реализации создания detached Task в handler инструмента нет.

RPC-адаптер `composer_detached_task_start` отвечает штатным RPC/voice response. Агентский адаптер `commit_start_agent` передаёт уже проверенные durable authority, исходного автора, ограниченный child grant, выбранную identity/profile и canonical action; затем подтверждает action обычным `AgentToolSafeResult`. Он возвращается до direct graph materialization и не создаёт параллельного foreground запуска для detached-пути.

Foreground продолжает использовать существующие native/CLI исполнители. Оба входа используют общий конструктор Turn в `ThreadManager::turn_start_for_actor_with_permission_profile_concurrency`; каноническая запись turn/start, audit, authority, owner и security сведена в `CrudStore::materialize_admitted_thread_turn`. Публичные CRUD-адаптеры сохраняют свои проверки runtime draft или точного agent graph/action. Native activation использует `dispatch_prepared_api_provider_turn_start`, CLI — существующий `turn_start_cli_runtime`; durable agent activation остаётся обязанностью существующего action outbox. Новых исполнителей и планировщиков нет.

## Поведение

| Серверный тип назначения | Пользовательский вход | `threads_turn_start` |
| --- | --- | --- |
| Collaborative | Сообщение в родителе → detached Task → TaskRun child → штатная карточка/результат | Тот же механизм; автор сообщения — исходный AgentExecution |
| Исполняемый TaskRun child | Foreground turn в этом же child | Foreground turn в этом же child, с существующим continuation context; без новой Task/child |
| DirectMessage | Существующий foreground путь | Существующий foreground путь |
| Legacy User | Существующий foreground путь | Существующий foreground путь |
| System | Существующий foreground путь | Существующий foreground путь |

Visibility, provider, автор и наличие opaque route не выбирают способ исполнения. Старые `user_root`/lineage проверки сохраняют своё назначение в foreground graph admission и не заменяют продуктовую политику.

## Авторство, допуск и независимость

Initiating principal остаётся в `ExecutionAuthorizationContext` и Task admission. Автор родительского сообщения и creator Task — исходный AgentExecution с его immutable presentation snapshot. Исполняющая identity/profile фиксируется отдельно в Task actor contract. Поля model, provider, reasoning и capabilities сохраняются в launch/`composer_work`; permission/security caps получаются из уже ограниченного контекста.

Durable agent entry не превращается в Fresh principal admission. Используется child envelope с исходным `allowed_actions`, fingerprint, policy generation и child launch ceiling. Сохраняются существующие проверки opaque options/routes, source fence, текущих прав principal, workspace/thread scope и канонического action commit. Перечень разрешённых действий дочерней работы не расширяется.

Новая самостоятельная Task принадлежит целевому треду: owner/created_by_thread — назначение, created_by_turn — admission в назначении. `parent_task_id` отсутствует; lifecycle — Detached, KeepRunning при завершении родителя. `creator_work_graph_root_execution_id` хранит provenance, а `work_graph_root_execution_id` не копируется от автора. Существующий Task executor выделяет собственный occurrence/resource root.

Завершение admission не должно финализировать его автора: `finalize_agent_execution_for_turn` учитывает `work_owner`. Проверка authority agent-authored admission и Task child опирается на сохранённый Task actor/admission contract, а не требует несуществующий direct responding execution родительского сообщения.

## Commit, повторы и восстановление

1. Как в прежнем пользовательском пути, родительское admission-сообщение сохраняется перед созданием Task. До Task/action commit инструмент не подтверждает принятую работу. Отказ создания отражается ошибкой/Blocked admission, модель в родителе не запускается. При сбое между сохранением сообщения и Task commit повтор использует точный сохранённый InProgress admission с теми же автором и input.
2. `TaskCreateContext.agent_action_commit` передаётся в существующий `TaskCreationCommitInput.agent_action`; Task, run, actor/occurrence/admission contracts и canonical action/receipt/outbox сохраняются одной транзакцией `commit_task_creation`.
3. TaskService добавляет фактические task/trigger/run IDs в внутренний `taskCreation` receipt и task ID в outbox. Это часть той же транзакции. При конкурирующей попытке с новым сгенерированным Task ID immutable replay validation отклоняет несовпавшие response/outbox; весь конкурирующий write set откатывается.
4. В одном runtime повторная доставка дополнительно использует уже существующий mutex execution binding и повторное чтение action/receipt/outbox. После потерянного ответа найденный action возвращает сохранённое подтверждение; `create_task` повторно не вызывается.
5. Для нового detached outbox сохранены `execution_mode`, task/thread/turn IDs и fingerprint исходного launch request. Поле прямого `spawned_execution_id` и direct `dispatch` не используются. Outbox проверяет связь с Task/creator/admission и завершает admission; реальное выполнение, retry, Stop, terminal result и delivery принадлежат Task runtime.
6. Если ответ TaskService теряется после commit, handler проверяет durable receipt перед ошибкой. Ошибка публикации не объявляется успешным выполнением модели: action подтверждает admission, Task status описывает результат работы.
7. После restart используется существующий ownership CAS. До принятия Task admission завершается Failed, без provider restart/Blocked job. Для уже принятой пользовательской Task recovery завершает admission штатным canonical terminal event и его durable delivery. Для агентской Task сохраняется обязанность action outbox. Отсутствие provider state не служит причиной пропустить admission. Никакого второго запуска модели из admission outbox нет.
8. Replay восстанавливает профиль и reasoning нового detached запуска из Task actor contract. Изменение/недоступность каталога и поздние настройки вызывающего агента не выбирают новую работу.

Старые action/outbox без detached marker проходят прежний direct путь независимо от нынешнего типа треда. Новая Task вокруг них не создаётся. История и receipts не удаляются и не переписываются. При штатной compaction новых outbox сохраняются immutable стратегия и Task binding вместе с исходным replay fingerprint; прежняя compaction direct outbox сохранена.

## Протокол и проекции

Существующий `TurnWorkOwner` перенесён из CRUD в protocol и переэкспортирован CRUD — второго enum/источника истины нет. В `Turn` добавлено совместимое `work_owner`: отсутствующее старое значение означает `turn`; default не сериализуется, сохраняя старые foreground payloads. Новые admissions передают `detached_task`.

Оба DB → Turn snapshot mapper, terminal reconstruction, canonical history и обычный public thread history передают сохранённого владельца. Обычный history reader получает owner для lifecycle-событий существующими snapshot batches без изменения лимитов выдачи. Исторические события гидратируются только в прочитанной копии; сохранённые canonical payloads остаются неизменными. Роль старого direct agent-authored turn не выводится из текущего origin_kind.

Client state machine и semantic timeline используют этот признак для live, snapshot/reconnect и terminal events. Admission не захватывает composer/foreground work и не создаёт ложный Worked-блок. Серверная публикация не повторяет уже сохранённый UserMessage lifecycle. Карточка и результат по-прежнему принадлежат штатному TaskRun occurrence.

Вручную обновлены 30 существующих JSON-schema проекций Turn, добавлен `turn_work_owner.json` и запись в schema registry. Статическое сравнение JSON подтвердило, что в старых схемах добавлены только это optional поле и его enum definition. Генераторы не запускались.

## Регрессии — написаны, не запускались

- `inherited_configuration_survives_visible_thread_creation_message_and_start_replay`: реальный native tool handler, Collaborative admission с AgentExecution-автором, одна Task при одновременной доставке, сохранённый launch/authority, replay при недоступном каталоге, отсутствие parent runtime/work, настоящий Task child и отдельный graph root, завершение и одна штатная result delivery/card.
- `detached_threads_action_recovers_after_commit_without_recreating_or_dispatching_parent`: существующий preparation barrier после Task/action commit, потеря вызывающей future, завершение source execution, replacement processor и обычный owner CAS/recovery/outbox, одна Task и один вызов модели в child.
- `cli_threads_collaborative_admission_uses_one_detached_task_and_normal_stop`: существующий MCP facade с admitted invocation, выбранные CLI model/reasoning в фактическом mock runtime, reconnect replay, обычный task/cancel RPC, отсутствие foreground work родителя; дополнительный Threads start из настоящего TaskRun child остаётся в том же child без Task.
- Существующая пользовательская Collaborative RPC-регрессия дополнена проверкой переданного owner и Principal-автора. Сохраняются содержательные пользовательские проверки concurrent Task execution, child Stop, voice TaskRun continuation и result delivery.
- Прямые foreground/replay/Stop/fault/catalog fixtures теперь явно используют DirectMessage вместо ошибочного предположения «visible root = foreground». Existing legacy direct dispatch/replay дополнительно сохраняет direct receipt при Collaborative типе назначения.
- CRUD-регрессия Task-owned parent дополнена snapshot/public history проверками, включая старый terminal payload без поля owner. Existing ledger compaction test расширен новым detached outbox binding.
- Client live/snapshot matrices проверяют agent-authored admission и старую direct работу, отсутствие ложного work блока, восстановление composer и повторную доставку событий.
- Protocol compatibility-регрессия проверяет старый agent-authored payload без owner и добавочный detached owner.

Запуски регрессий отложены до отдельного разрешения после внешнего ревью. Предложенные сценарии не являются подтверждением их прохождения.

## Самоаудит

Последовательно просмотрены вход → проверки → сохранение → выполнение → результат для общего запуска, Task/child ownership, authority/авторства, commit/replay и проекций. Найденные и исправленные проблемы: загрузчик authority ожидал direct response у admission; terminal finalizer мог финализировать автора; replay resolver искал только direct grant; orphan recovery могла заблокировать admission; public history теряла owner старых terminal payloads; compaction не знала нового Task binding. После исправлений просмотрены callers этих контрактов.

Первичный статический аудит после rebase оказался неполным: внешнее ревью нашло три конструктора с shorthand-полями, ошибочную admission fixture и пропущенные окна cancellation/recovery. В повторном проходе рассмотрен итоговый patch относительно базы реализации, включая ранее принятые изменения. Лексический просмотр Rust-текста с балансировкой delimiters, comments/strings и учётом shorthand проверил 163 полных конструктора Turn, четыре расширенных update literals и отдельно более короткие update/pattern случаи; полных literals без owner не найдено. Это вспомогательная статическая проверка, не Rust type checking. В 34 Rust-файлах полный diff содержит только заполнение default owner. Все 30 изменённых существующих схем повторно сравнены как JSON после удаления добавленного optional owner/enum: иных изменений нет. Новый standalone enum, bundled Threads docs, protocol/client projections и тестовые блоки перечитаны. В этой итерации wire-схемы и skills не менялись. Изменения Tasks/Subagents skills и policy blocks отсутствуют.

Кодовый аргумент отсутствия двойного запуска: общий detached executor создаёт только Task; agent adapter подтверждает action и возвращается до direct materialization; detached outbox выполняет только admission reconciliation. Foreground constructor/commit и существующие native/CLI activation helpers остаются общими нижними границами.

Это статический аудит. Компилируемость, типовая корректность и прохождение тестов не подтверждены.

## Выполненные команды и ограничения

Точные shell invocations с cwd, включая чтение/поиск и inline Python для статической обработки текста, сохранены в [журнале команд](threads-shared-launch.commands.jsonl). Это только команды собственного текущего выполнения, извлечённые из его локального transcript; вывод инструментов, чужие треды и содержимое иных сессий не включены. Журнал — снимок до завершающего amend; финальные Git-команды приведены ниже.

Существенные Git/format команды:

```sh
git -c core.hooksPath=/dev/null fetch origin main
git -c core.hooksPath=/dev/null worktree add -b fix/threads-shared-launch .worktrees/threads-shared-launch origin/main
git add -A
git -c core.hooksPath=/dev/null commit -m 'fix: share thread turn launch admission'
git -c core.hooksPath=/dev/null rebase 7f226499d3fa8beac5dd15b3d241040e7a99016e
cargo fmt --all
cargo fmt --all --check
git diff --check 7f226499d3fa8beac5dd15b3d241040e7a99016e
git -c core.hooksPath=/dev/null commit --amend --no-edit
```

Первый `cargo fmt --all --check` выявил форматирование текущих правок; затем применялся разрешённый `cargo fmt --all`. Финальные `cargo fmt --all --check` и `git diff --check 7f226499d3fa8beac5dd15b3d241040e7a99016e` завершились с exit 0 после последнего изменения Rust-кода.

Не запускались тесты, cargo check, компиляция, сборки, приложение, Gateway, CLI-агенты, миграции, генераторы, EXPLAIN и benchmarks. Production/dev базы, службы, конфиги и установленные skills не изменялись. Проверки с выполнением кода проекта после внешнего ревью требуют отдельного разрешения пользователя.

## Состав patch

Основные файлы: `message/turn_handlers.rs`, `message/agent_action_tools.rs`, `authorization/execution.rs`, `message/agent_runtime.rs`, `resilience/recovery.rs`, `thread/mod.rs`, CRUD event/history/admission helpers, `tasks/service.rs`, protocol Turn/schema, client conversation/semantic projection и перечисленные выше тесты. Остальные Rust-изменения — механическое заполнение нового поля. Полный список файлов доступен командой `git diff --name-only 7f226499d3fa8beac5dd15b3d241040e7a99016e HEAD`.

После снимка журнала выполнены завершающие Git-команды в указанном worktree:

```sh
git -c core.hooksPath=/dev/null commit --amend --no-edit
git status --porcelain=v1
git rev-parse HEAD
git log -1 --format='%H%n%s%n%P'
git diff --check 7f226499d3fa8beac5dd15b3d241040e7a99016e HEAD
git -C /Users/alexander/Code/pioneer/pioneer status --porcelain=v1
```

## Исправления пяти замечаний повторного ревью

| Замечание | Исправление и затронутые файлы |
| --- | --- |
| 1. Stop отменял source graph | `message/agent_runtime.rs::cancel_root_agent_work_graph_for_turn` возвращает отсутствие foreground graph для сохранённого DetachedTask owner до fallback на автора. `turn_cancel` продолжает обычную canonical отмену только самого admission. Foreground root и TaskRun child сохраняют owner Turn и прежнюю отмену. Detached Task продолжает работать по существующему KeepRunning policy; её собственный Stop остаётся в Task lifecycle. |
| 2. Пропущенные Turn constructors | В `crud/tests/self_improvement_canonical_history.rs`, `crud/tests/self_improvement_source_turn.rs`, `gateway/src/self_improvement/history.rs` добавлен default owner Turn. Это прежние исполняемые/исторические fixtures; текущий Collaborative тип не переклассифицирует их. Self-improvement logic не менялась. |
| 3. Recovery commit windows | `resilience/recovery.rs::enqueue_claimed_turn_execution_recovery` после обычного owner CAS читает точную workspace/thread/creator-turn связь Task. Нет принятой Task — Failed admission с честным сообщением; одна принятая пользовательская Task с совпавшими owner, pinned launch, actor contract и независимым graph — Completed admission через существующий canonical projector/live delivery. У агентской Task завершение остаётся у action outbox. В `turn_handlers.rs` postcommit ошибка TaskService распознаётся по durable acceptance для обоих входов. |
| 4. Terminal outbox reconciliation | `message/agent_action_tools.rs` проверяет canonical action/receipt/source, Task/creator, workspace, pinned launch и admission owner/author до подтверждения. Completed/Interrupted/Blocked/Failed согласуются через existing terminal synchronization и обычный delivered ACK, без изменения canonical исхода. Ошибки storage и гонки завершения/rehydration остаются retryable. Повреждённая привязка получает terminal failed через existing CRUD failure transition, без успешного ACK и без finalization исходного агента. |
| 5. Client fixture semantics | В `client/src/threads/registry.rs` helper composer_admission выставляет DetachedTask. running_turn и composer_occurrence остаются Turn. Единственный foreground-потребитель composer_admission, использующий его author snapshot, явно возвращает Turn. Все остальные потребители просмотрены: admission/live/snapshot assertions сохранены. |

Дополнительный необходимый fence находится в `CrudStore::commit_task_creation_once` (`crates/crud/src/lib.rs`), внутри существующей Task/action транзакции. Точная привязка готовится до writer; состояние saved DetachedTask admission и его turn_execution повторно читается внутри writer. Terminal admission либо Recovering owner не допускают новую Task. Поэтому:

- Если Stop или replacement CAS выиграли раньше Task commit, дальнейшая попытка принятия откатывается целиком, включая action/receipt/outbox. Admission не исполняет модель и не остаётся InProgress после recovery.
- Если Task commit выиграл раньше Stop/recovery, принятие и агентский receipt атомарны; recovery/outbox только завершают admission. Task не создаётся заново, её KeepRunning/Stop/result delivery остаются штатными.
- Проверка применяется к сохранённому DetachedTask owner, а не к наличию composer_work, видимости, автору или типу треда. Ordinary/legacy Task requests с composer payload и owner Turn сохраняют своё поведение. Grants и авторизация TaskService не ослаблены.

`complete_detached_thread_admission` теперь сначала читает сохранённый исход: уже terminal admission синхронизируется без перевода в Completed. При гонке завершения действующий canonical terminal fence сохраняет первый принятый исход. Временные ошибки после доказанной привязки получают существующий PreparationUnavailable, а не ложное решение о повреждении.

В `crud/src/repositories/agent_domain.rs` добавлен только внутренний fixed diagnostic `outbox_detached_binding_rejected` для failed delivery. Existing immediate/exhausted discovery и claim учитывают этот terminal outcome и не выбирают его бесконечно, включая attempts=8. Исходные predicates существующих partial indexes сохранены; отдельный дополнительный фильтр не требует новых индексов, таблиц, миграций или poller. Старые direct receipts/outbox сохраняют прежнюю exhausted reconciliation. Повреждённые записи сохраняются, не удаляются и не подтверждаются успешной доставкой. Планы запросов и производительность не проверялись: EXPLAIN/benchmarks запрещены.

### Новые регрессии — не запускались

В существующем `message/tests/thread_launch_choices.rs` добавлены три содержательные matrices с прежними handlers, barriers и replacement fixture:

- `detached_admission_early_stop_preserves_live_source_graph_on_both_sides_of_task_commit`: настоящий Threads invocation при живом source root и другом direct потомке; RPC turn/cancel до и после Task commit; исходный graph целиком неизменён, admission Interrupted, до commit Task/action не принимаются, после commit одна Task сохраняет KeepRunning; parent runtime/response/work и вызовы модели отсутствуют.
- `replaced_gateway_resolves_detached_admission_before_commit_and_after_user_task_acceptance`: потеря вызывающей future до Task/action commit для агента и пользователя, а также после пользовательского Task commit; истечение persisted lease, replacement processor и два recovery прохода; точный Failed/Completed outcome, terminal execution вместо зависшего Recovering, ноль/одна Task, отсутствие модели/work у admission. Для принятой пользовательской Task затем проверяется один обычный child model call через Task bridge и отсутствие повторного выполнения после следующего scheduler прохода.
- `terminal_detached_admission_outbox_preserves_outcome_and_acks_even_after_retry_budget`: сохранённые Interrupted/Blocked/Failed до обработки outbox; повтор с потерянным ACK и attempts=8; canonical outcome неизменён, одна Task, parent model не запускается, source не отменяется. На этой же границе проверяются временная недоступность receipt storage (retry сохраняется) и подмена turn binding реальным чужим source turn (terminal failed без delivered ACK, включая повтор после исчерпания бюджета).

Предыдущие native/CLI MCP facade, TaskRun continuation, grants/configuration, replay/compaction и live/snapshot регрессии сохранены. Client admission matrices снова используют корректные fixtures; assertions свободного composer, отсутствия Work-блока и исторического direct owner не ослаблены. Сценарии прослежены статически до barriers и Task/action/recovery границ; фактическое достижение этих границ не подтверждено запуском.

### Результат повторного самоаудита и ограничения

После каждого замечания просмотрены его callers и пересечения с остальными исправлениями. Повторно прослежены оба входа через общий start_thread_turn/start_detached_thread_turn, atomic Task/action acceptance, Task child runtime и штатную delivery. В адаптерах не появилось второй последовательности создания detached работы; общий selector по ThreadOriginKind сохранён. Source author, responding execution и work_owner различаются на cancellation, completion, recovery и snapshot/history границах. Pinned identity/model/reasoning и immutable caps остаются в прежних Task/authority contracts; эти контракты не расширялись.

Отдельно рассмотрены отказ до commit, ранний Stop, ownership takeover, сбой после commit, повтор outbox и уже terminal admission. Ни recovery, ни detached reconciliation не вызывают provider dispatch/create_task. Исторический direct replay по сохранённому outbox не использует новый selector. Native/CLI foreground activation и Tasks/Subagents protocol не изменялись. Дополнительные исправления ограничены перечисленными областями; новые public wire APIs, очереди, исполнители, лимиты, зависимости и DB priority отсутствуют.

Полный итоговый diff рассмотрен относительно `7f226499d3fa8beac5dd15b3d241040e7a99016e`, а incremental diff — относительно `b9a932f576282c0e75f501be3eaf39abf0ecadf5`. Production/dev данные, установленные приложения/службы/конфиги/skills, main и соседние worktree не изменялись. Статическое ревью не подтверждает компилируемость и не заменяет тестирование; возможные type/runtime ошибки должны проверяться после отдельного разрешения пользователя. Это передача на повторное внешнее ревью, не окончательная приёмка.

Команды этого этапа, включая чтение, Git и Python исключительно для статической обработки текста, добавлены в [точный журнал](threads-shared-launch.commands.jsonl) с phase `external-review-fixes`. Журнал является снимком; завершающие команды после него приведены ниже. Разрешённые formatting/whitespace проверки: cargo fmt --all, cargo fmt --all --check, git diff --check и git diff --check относительно базы реализации.

Не запускались тесты, cargo check, компиляция, сборки, приложение, Gateway, CLI-агенты, миграции, генераторы, EXPLAIN и benchmarks. Компилируемость и прохождение тестов не подтверждены.

Финальные `cargo fmt --all --check` и `git diff --check 7f226499d3fa8beac5dd15b3d241040e7a99016e` завершились с exit 0. Тесты остаются незапущенными.

Завершающие shell-команды после снимка журнала (cwd — этот worktree):

```sh
git diff --check 7f226499d3fa8beac5dd15b3d241040e7a99016e
git diff --numstat b9a932f576282c0e75f501be3eaf39abf0ecadf5
git add -A
git diff --cached --check
git -c core.hooksPath=/dev/null commit -m 'fix: reconcile detached thread admission lifecycle'
git status --porcelain=v1
git rev-parse HEAD
git log -1 --format='%H%n%s%n%P'
git diff --check 7f226499d3fa8beac5dd15b3d241040e7a99016e HEAD
git -C /Users/alexander/Code/pioneer/pioneer status --porcelain=v1
```
