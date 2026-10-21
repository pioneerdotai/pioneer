# Повторная передача task-launch-continuity на внешнее ревью

Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/task-launch-continuity`.
Ветка: `fix/task-launch-continuity`.
Исходный локальный main и HEAD: `3c0de49779e3fe3d4c091f58a231c2c2f9c48e5f`.
Pull, rebase, merge, push, PR, установка и развёртывание не выполнялись.
Основной checkout и соседние worktree не изменялись.

Компиляция и тесты не запускались.
Первый внешний просмотр выявил два замечания. Ниже описан итог после их
исправления; повторное внешнее ревью и принятие реализации ещё не состоялись.

## Решения по результатам исследования до правок

| Изменение | Следующий новый запуск | Уже созданное исполнение |
| --- | --- | --- |
| CLI binary, args, home, таймауты, имя, off/on, возврат настроек | Тот же активный источник в workspace; прежние runtime/backend/model и ограничения | Собственный снимок и текущая проверка источника |
| Другой исполнитель, source kind, workspace, runtime или provider | Отказ как выход за сохранённый выбор | Подмена исключена |
| Выключение, удаление, retirement | Нет запуска; существующий CLI preflight может оставить run в очереди до включения | Текущая проверка доступности обязательна |
| Добавленные модели, reasoning, действия, инструменты, MCP, skills | Старый допуск не расширяется | Старый снимок не расширяется |
| Сужение действующих прав | Сужение ограничений либо отказ, если необходимого доступа больше нет | Повторная проверка действующих прав |
| API-ключ, proxy, endpoint, connection environment | Новая авторизация сохранённого provider/model в прежнем workspace/capsule с сохранёнными пределами | Прежняя точная authority; незаметной замены нет |
| Изменение между подготовкой и commit/start | Устаревшая подготовка не позволяет исполнить отозванные права | Снимок не переписывается |

**CLI — доказанная причина.** `identity/catalog::from_effective_settings` включает
полную effective-конфигурацию в материал fingerprint. CRUD-каталог дополнительно
учитывает kind, display name, nickname и enabled, сохраняя постоянный ID источника
и повышая revision. Новый occurrence в `materialize_task_agent_action_binding_for_execution`
сравнивал текущую запись с давним снимком задачи. Полное равенство снимков также
отсекало тот же источник в `intersect_child_launch_ceiling` и при проверке
дочернего grant относительно родительского в `commit_agent_execution_graph`.

**API — отдельная доказанная причина.** `ProviderRegistry::authority_fingerprint`
хеширует workspace/provider, ключ, proxy, endpoint и connection environment.
Ключ не является CLI `source_revision` или native-agent `config_revision`.
`resolve_task_parent_execution_authorization_context` загружал старый durable
admission и перед каждым новым запуском требовал прежний fingerprint authority.
Допустимая замена ключа могла навсегда блокировать новые исполнения задачи.

**Корректные API-механизмы сохранены.** Загрузка durable authorization проверяет
principal/session provenance, workspace и корневую capsule. Lease revalidation
проверяет текущих активных участников, доступ к корню, сохранённую роль и её
provider/model, actions, permission cap, skills/MCP. Derivation проверяет backend,
выбранные ресурсы и предел разрешений; durable Turn admission проверяет актуальную
generation в транзакции. Точные provider authority/token fences существующих
исполнений сохранены. Эти пути не переписаны ради симметрии с CLI.

## Реализация и границы снимков

Постоянными остаются сохранённый identity/source, backend, provider/runtime,
model и верхние пределы полномочий. Новое исполнение разрешает текущую
identity/presentation того же источника и текущие настройки соединения.
Исторический snapshot доказывает принадлежность сохранённого выбора;
workspace/source kind и CLI runtime ID/kind проверяются отдельно.

Каталог дочерних вариантов строится из сохранённых профилей, поэтому текущая
модель composer и новые элементы каталога не меняют старые варианты. Общие
предикаты в protocol используются в проекции, наследовании и транзакционном
допуске: источник должен остаться тем же, а backend/provider/model и совместимые
identity/reasoning/permission sets не могут расшириться. Полное равенство внутри
одного неизменяемого grant не ослаблено.

Для ephemeral CLI-профиля также проверяется зарегистрированный CLI-источник из
сохранённого ceiling. Его снимок фиксируется в grant нового запуска и проверяется
в транзакции: виртуальная identity не позволяет обойти доступность или сменить
CLI-движок. Каталог исключает такой профиль без действующего разрешённого источника.

Новый execution graph атомарно фиксирует execution, presentation, profile,
grant, child ceiling, lineage, resource state и permit/queue. Договор задачи
не обновляется. Occurrence создаётся после graph commit и получает автора
из presentation конкретного исполнения. Рабочий Turn сохраняет прежнюю семантику
автора входа и точную response-привязку к исполнению. Reviewer получает собственный
graph; revision продолжает использовать выбранное исполнение.

При восстановлении identity/presentation/profile берутся из существующего
execution и grant, а текущий source fence проверяется отдельно. Повторный graph
commit использует прежний grant и историческую presentation, не перехватывает
старый nickname и не заменяет execution facts. Смена API authority после admission
не превращает прежний execution в новый, в том числе в промежутке до появления
рабочего Turn: несовпадение сохранённого execution fingerprint даёт отказ.

API authority для **нового** исполнения разрешается только после проверки
сохранённого допуска и текущего доступа. Копируется envelope с прежними пределами,
меняется только authority того же workspace/provider/model. Registry проверяет
локальную доступность провайдера, необходимую конфигурацию и retirement;
построение adapter валидирует конфигурацию без обращения к внешнему сервису.
Occurrence и child используют один выбранный fingerprint, который повторно
проверяется перед продолжением admission.

Изменение endpoint или аккаунта не объявляется технически эквивалентным старому
fingerprint. Выполняется новая авторизация в существующей модели прав
workspace/provider/model. Внешние права аккаунта и фактическая доступность модели
не выводятся из хеша или наличия ключа: их по-прежнему проверяет провайдер при
dispatch. Старые записи содержат непрозрачный fingerprint, а не сведения о
внешнем аккаунте; новая параллельная система прав не добавлена.

Сохранены TaskAgentToolPolicy, security/permission caps, MCP/skill selections,
маршруты, capsule, lineage, depth/delegation limits и current role checks.
Обнаруженные после создания API agent skills ограничены сохранённым allow-set.
Reviewer разрешается через ID владельца сохранённого nickname,
а его действия ограничены сохранённым и текущим envelope кандидата.

Точные revision/fingerprint проверки остались для исторической принадлежности,
целостности snapshot/grant, idempotency, текущего состояния source и короткого
промежутка подготовки/commit. `expected_current_identity_source_*` в действиях
агента сохранены. В graph commit текущая identity служит fence; presentation
может быть исторической только у уже существующего исполнения с точной привязкой.
Транзакция проверяет active/retired, native config enabled/revision и принадлежность
backend/source/workspace. Generation, ownership, attempt и resource permit проверки
не ослаблялись. Конфигурация, hashing и provider registry остаются вне
транзакции. API-гонки дополнительно отсекаются существующими authority/token
проверками перед фактическим обращением к сервису.

Ранее сохранённые задачи читаются существующими readers/upcasters. Используются
их постоянные ID, профиль, модель и ограничения. Формат, таблицы, миграции,
массовая замена fingerprint и переписывание истории не понадобились. Cancelled
и исторические failed объекты не возобновляются, расписания не меняются.

## Саморевью и исправленные находки

- Исправлен старый identity в `InheritParent` в обход текущих вариантов.
- Убрана зависимость старых дочерних профилей от сегодняшнего composer/default model.
- Автор occurrence перенесён с договора задачи на фактически сохранённое исполнение.
- Replay использует сохранённый grant; историческая presentation не требует старой текущей revision и не перехватывает nickname.
- Native enabled/retired и workspace/source/backend проверяются при атомарном graph commit.
- Identity в grant точно сверяется с authoritative presentation, включая CLI kind в role label; старую метку нельзя совместить с новой ревизией другого движка.
- API occurrence/child не выбирают authority независимо друг от друга.
- Revision/recovery загружают authority того же execution; новая authority не подменяет старую.
- Обнаруженный после создания задачи agent skill overlay не расширяет допуск.
- Reviewer после переименования разрешается по сохранённой принадлежности nickname; текущая reviewer-role не расширяет предел действий кандидата.
- Проверены вызовы изменённых сигнатур и экспорт общих предикатов из protocol.

## Доработки по двум замечаниям внешнего ревью

1. **Occurrence, lineage и security.** Graph по-прежнему сохраняется раньше
   occurrence, чтобы его автор происходил из уже допущенного execution/presentation.
   После создания или загрузки occurrence `start_new_child_turn` заново выводит
   полный security snapshot рабочего Turn из сохранённого security этого родителя
   штатным CLI/API resolver. Он повторно применяет security/permission caps,
   sandbox, filesystem, network/process и tool policy, включая native runtime
   roots. Это не исправление одной строки `parent_turn_id`. Lineage вычисляется
   через тот же окончательный parent. Permission envelope сверяется с подготовленным
   для graph authorization: несогласованный snapshot не сохраняется. Occurrence
   получает максимальный Task cap, выбранное дочернее разрешение может его сузить.
   Для immediate attached отдельный occurrence не создаётся, исходный родитель
   и выведенный из него security сохраняются. Существующие рабочие Turn продолжают
   восстанавливаться из своих сохранённых снимков.

2. **CLI kind при recovery/replay.** Gateway `current_agent_identity_source_fence`
   загружает историческую authoritative presentation по execution, проверяет её
   принадлежность и точные revision/fingerprint исполнения, затем сравнивает
   сохранённый role label с текущим kind того же runtime. В атомарном graph commit
   для существующего зарегистрированного CLI execution kind дополнительно
   сверяется с текущей authoritative presentation каталога. Исторические
   identity/presentation/profile/grant не меняются. Технические fingerprint и
   revision могут различаться; текущий source fence по-прежнему должен точно
   совпадать с каталогом в транзакции. Смена kind во время подготовки либо нарушает
   этот fence, либо отклоняется сравнением kind. Это закрывает конкретный разрыв
   проверки источника; отсутствие прежней проверки само по себе не доказывает
   dispatch другим движком, поскольку существуют дополнительные fingerprint-барьеры.
   API authority и ephemeral CLI проверки этой доработкой не изменены.

Промежуточное саморевью проследило CLI/API и immediate attached материализацию,
сохранение security вместе с Turn, lineage и авторство occurrence. Проверены
recovery, revision/reviewer и прямые agent-action вызовы общего source fence.
Дополнительная транзакционная проверка ограничена существующим CLI graph,
чтобы не менять первичное создание. Финальный просмотр включает весь diff от
исходного SHA, оба новых тестовых файла и этот отчёт. Новых миграций, форматов,
повторов пользовательских операций и изменений соседних исправлений нет.

Расширены три существующих сценария с управляемыми зависимостями:
`detached_composer_work_runs_natively_in_codex_and_claude_and_delivers`,
`scheduled_task_agent_run_routes_occurrence_to_exact_delivery_thread` и
`review_disabled_immediate_task_agent_run_creates_child_thread_and_wait_returns_result`.
Они проходят настоящую материализацию Task child до сохранённых security и lineage.
Общая проверка сопоставляет весь snapshot с выводом из фактического родителя,
проверяет permission/tool cap и точного автора occurrence из сохранённого execution.
Исходное ожидание `security.parent_cap.parent_turn_id == run.id` сохранено.

Добавлены `existing_cli_task_recovery_refuses_changed_engine_with_a_current_catalog`
на Gateway recovery/source-fence пути и
`existing_cli_graph_replays_technical_revision_but_refuses_changed_engine`
на настоящем CRUD graph commit. Последний сначала допускает replay после off/on
и технического изменения при Codex, затем отказывает после Codex → Claude с
актуальным fence. Он сравнивает исторические execution, presentation, grant,
resource state/scope, permits и queue до и после операции. Положительный Gateway
replay после технического изменения и переименования также сохранён.
Все эти проверки написаны, но не запускались.

## Файлы

Изменены:

- `crates/crud/src/repositories/agent_domain.rs`
- `crates/gateway/src/authorization/agent_execution.rs`
- `crates/gateway/src/authorization/agent_runtime_adapter.rs`
- `crates/gateway/src/authorization/execution.rs`
- `crates/gateway/src/message/agent_action_tools.rs`
- `crates/gateway/src/message/task_agent_executor.rs`
- `crates/gateway/src/message/tests.rs`
- `crates/protocol/src/agent_launch.rs`
- `crates/protocol/src/lib.rs`
- `crates/provider/src/registry.rs`
- `crates/tasks/src/start_failure.rs`

Новые:

- `crates/crud/src/repositories/agent_domain_launch_continuity_tests.rs`
- `crates/gateway/src/message/tests/task_launch_continuity.rs`
- `TASK_LAUNCH_CONTINUITY_REVIEW.md`

## Регрессии, написанные без запуска

Gateway: сохранённая CLI-задача после off/on, изменения binary, возврата прежних
настроек с новой revision и переименования; неизменность модели и договора;
graph admission и replay с прежним grant/author; обновлённые дочерние варианты
и reviewer после переименования; API key rotation через текущую авторизацию
и настоящий graph commit; согласованная authority occurrence/child; отказ
при потере capsule access и native disable; запрет перепривязки существующего
API graph к следующему ключу; отказ при гонке конфигурации.

CRUD: новый child с новой revision в старом parent ceiling; replay без второго
execution/permit; атомарный rollback при расширении model/reasoning/skills/MCP/
permission cap, смене workspace/source; приём суженного reasoning/tool cap;
retirement, stale preparation, новый посторонний исполнитель и подмена метки CLI-движка при смене kind.
Отдельная регрессия проводит ephemeral CLI child через отказ со старым снимком
зарегистрированного источника и успешный допуск с его текущим снимком.

Дополнены проверки наследуемого child grant, catalog narrowing без потери той же
identity, запрета provider/backend/model/capability расширений, чтения V1 ceiling
без переписывания authenticated snapshot, удаления API-ключа и retired provider.
Это исходники регрессий, а не свидетельство успешного выполнения.

## Проверки и дальнейший этап

Выполнялись только чтение/поиск исходников, Git-операции собственного worktree,
просмотр полного diff, `git diff --check`, `cargo fmt --all` и
`cargo fmt --all -- --check`. Сборка, тестовые бинарники, CLI/diagnostic probes,
Pioneer/Gateway и реальные API не запускались. Пользовательские настройки,
секреты и рабочая БД не открывались и не изменялись.

На момент проверки у обеих соседних веток (`cli-provider-probe-isolation` и
`opencode-startup-contention`) пересечение только в
`crates/gateway/src/message/tests.rs`: регистрации тестовых модулей.
Их реализации не переносились и не объединялись с этой работой.

Предлагаемые команды **только после полного внешнего ревью и принятия реализации**,
из указанного worktree:

```sh
cargo test -p pioneer-gateway message::tests::task_launch_continuity
cargo test -p pioneer-gateway message::tests::detached_composer_work_runs_natively_in_codex_and_claude_and_delivers -- --exact
cargo test -p pioneer-gateway message::tests::scheduled_task_agent_run_routes_occurrence_to_exact_delivery_thread -- --exact
cargo test -p pioneer-gateway message::tests::review_disabled_immediate_task_agent_run_creates_child_thread_and_wait_returns_result -- --exact
cargo test -p pioneer-crud repositories::agent_domain::launch_continuity_tests
cargo test -p pioneer-provider registry::tests::new_durable_authority_rotates_key_but_refuses_removed_credentials_and_retired_provider
cargo test -p pioneer-gateway authorization::agent_execution::tests
cargo test -p pioneer-gateway message::agent_action_tools::tests
cargo test -p pioneer-gateway authorization::execution::tests
cargo test -p pioneer-protocol agent_launch::tests
cargo test -p pioneer-tasks start_failure::
```

Статический просмотр и форматирование не подтверждают type checking, linking,
успех тестов или поведение работающего приложения. Повторное внешнее ревью
и принятие ещё не состоялись; собственное ревью их не заменяет. После передачи фоновые проверки
не запускаются.

## Исправления по Linux CI 21 октября 2026

Run 39613532359 (head 3811fd49) завершился с 17 ошибками Gateway; Windows и macOS прошли. Логи подтвердили три причины:

- Исторический non-composer CLI admission хранит `cli_runtime:…` как provider grant. Для него сохраняется точная проверка fingerprint без построения API-адаптера. Текущие runtime/kind, model, source и graph fences по-прежнему проверяются отдельными существующими слоями. Добавлена регрессия на сохранённом admission.
- Composer admission теперь захватывает разрешённые Agent skills до сохранения задачи через существующую composite authorization, включая scoped-проверку для Member. Родительский message-only turn не получает runtime pins. Фильтр по сохранённым skill IDs при запуске задачи сохранён, поэтому последующие изменения каталога не расширяют её allowance. Вертикальная регрессия дополнена проверкой сохранённого admission.
- Фикстура отзыва доступа атомарно переводит изолированного инициатора в suspended member: статус suspended запрещён для superuser ограничением БД. Проверка отказа revalidation сохранена.

Проверены вызывающие пути и весь добавленный diff. `cargo fmt --all` и `git diff --check` выполнены. Локальная компиляция и тесты не запускались. Новые результаты ожидаются от GitHub CI.

## Стабилизация mixed-checkpoint регрессии

Run 39638523268 на head 5b8a49ba: Linux shard 1 упал в
`composer_reuses_late_mixed_checkpoint_before_attached_child_policy`; остальные
пять matrix jobs прошли. При LastNTurns без summary ожидался старый принятый
turn, а выбран был uncovered tail с полным tool round.

Проверка `order_history_by_creation` и `select_task_history` подтвердила:
порядок определяется датами исходных сообщений, а фикстура задавала статические
даты только turn, оставляя источникам CURRENT_TIMESTAMP. Совпадение секунд
сохраняло порядок композиции, переход секунды менял выбранный последний turn.
Фикстура теперь задаёт явную хронологию исходным input/event/context rows
до создания frozen snapshots. Для raw и projected basis ожидается один
uncovered tail; дополнительно проверяется сохранение точных источников,
единственность и порядок tool call/result. Production-код не изменён.

Выполнены статический просмотр, форматирование и `git diff --check`.
Локальные сборки, cargo check и тесты не запускались.
