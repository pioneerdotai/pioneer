Отчёт для статического ревью PIONEER-M / PIONEER-K

Ветка: `fix/native-cancellation-terminal-obligations`.
Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/native-cancellation-terminal-obligations`.
База и HEAD: `5a721be5b14c2c7b7a41331869ca82e42d7751b0`.

Доработка выполнена в существующем worktree с сохранением предыдущих незакоммиченных изменений. Прочитан применимый корневой AGENTS.md; дополнительных AGENTS.md под затронутыми crates нет. Основной checkout и соседние worktree этой работой не изменялись. Из main и других задач изменения не переносились. Commit, push, merge, rebase, reset и публикация не выполнялись. Sentry issues, настройки и комментарии не изменялись.

При первоначальном создании worktree локальный main совпадал с указанной базой. В предыдущей фазе было обнаружено внешнее продвижение main до `7201f95a8c4b99ba0392c818b5ca8adc7ace207a`; чтение тогдашнего diff не выявило изменений cancellation/terminal outbox/ownership/recovery. Пересечение в observability касалось отдельного MCP breadcrumb правила. В этой доработке main не импортировался; совместимость с его последующими изменениями не проверялась.

Тесты, сборка, cargo check, приложение и CI не запускались. Все изменения оставлены незакоммиченными.

Полный накопленный diff, включая новые файлы и этот отчёт: `/tmp/pioneer-native-cancellation-terminal-obligations.patch`.

Весь список изменённых и новых файлов (29):

- `Cargo.lock`
- `NATIVE_CANCELLATION_REVIEW.md` — новый файл
- `crates/agent/Cargo.toml`
- `crates/agent/src/agent_loop.rs`
- `crates/agent/src/lib.rs`
- `crates/agent/src/manager_tests.rs`
- `crates/agent/src/post_turn.rs`
- `crates/crud/src/lib.rs`
- `crates/crud/src/native_cancellation_tests.rs` — новый файл
- `crates/crud/src/repositories/compaction_lifecycle.rs`
- `crates/crud/src/repositories/mod.rs`
- `crates/crud/src/repositories/native_cancellation_context.rs` — новый файл
- `crates/crud/src/repositories/native_terminal_effect_outbox.rs`
- `crates/crud/src/repositories/turn_event_projection_stream_state.rs`
- `crates/entity/src/lib.rs`
- `crates/entity/src/native_cancellation_context.rs` — новый файл
- `crates/entity/src/native_terminal_effect_outbox.rs`
- `crates/entity/src/prelude.rs`
- `crates/entity/src/turn_event_projection_stream_state.rs`
- `crates/gateway/src/authorization/lease.rs`
- `crates/gateway/src/message/agent_runtime.rs`
- `crates/gateway/src/message/mod.rs`
- `crates/gateway/src/message/native_preparation_failure_tests.rs` — новый файл
- `crates/gateway/src/message/tests.rs`
- `crates/migration/src/lib.rs`
- `crates/migration/src/m20261001_000001_native_cancellation_context.rs` — новый файл
- `crates/observability/src/lib.rs`
- `crates/protocol/src/agent_event.rs`
- `crates/runtime-events/src/hub.rs`

Порядок отмены и исходный контракт

До исправления Gateway сначала фиксировал Interrupted, затем сигнализировал CancelTurn. Actor останавливал работу и пытался записать обычную NativeTerminalEffectsPrepared, затем TurnInterrupted. Guard отклонял подготовку уже Interrupted turn. Publisher возвращал bool после ERROR; macro создавал второй ERROR с exhausted и останавливал actor. Снятие внешнего guard не устраняло запрет обычного outbox prepare после terminal commit.

Теперь до начала provider/tool actor получает durable ACK регистрации исходного cancellation description. Start-control ACK сохраняет прежний смысл допуска команды и не заменяет этот durable ACK. Gateway загружает сохранённое описание без ожидания mailbox/provider, готовит отменный план до writer, затем фиксирует canonical cancellation и её обязательства в одной append-транзакции. После принятия Gateway получает owned receipt и привязывает observation к точному actor generation/run. Cooperative token останавливает работу вне mailbox. Последующий CancelTurn завершает ожидание задачи и локальную очистку без повторной обычной preparation или terminal публикации. Actor может принимать следующий turn, если нет независимой причины остановки.

Immutable-контекст и восстановление

Источник — исходный AgentTurnRequest и NativeTurnRuntimeSnapshot начатого turn. Сохраняются workspace/thread/turn, исходные runtime generation и batch/run identity, effective Interrupted policy через условный набор эффектов, исходный bounded HookPhaseRequest с gate/limits, durable handler/subscription snapshot, effect identity/max_attempts и cleanup runtime contract. Значение Interrupted hook по умолчанию остаётся отключённым. Нет полной истории разговора, executable trait objects или параллельного общего runtime. Разделение durable description и runtime adapters сохранено.

Interrupted handler snapshot кешируется на поколение runtime и разделяется через Arc. Он снимается только при включённой Interrupted policy и наличии hook runtime; при смене поколения кеш сбрасывается. Условность dispatch и прежние пределы payload сохраняются. Регистрация и controller не исполняют hooks, extraction, provider или cleanup; это делают существующие workers.

Новая Entity `native_cancellation_context` подключена в lib и prelude. Таблица содержит одну строку на turn_id: turn_id (PK), thread_id, workspace_id, execution_owner_id, context_json, context_sha256, nullable accepted_event_id и created_at (timestamp_with_time_zone / DateTimeWithTimeZone). Единственный FK — turn_id → turn.id с Cascade. accepted_event_id — nullable логический ID canonical cancellation, без FK на turn_event view. Дополнительных индексов на новой таблице нет. JSON/digest после регистрации не обновляются; обновляется только receipt.

Сериализация, проверка bound и SHA-256 выполняются до writer. Load через Entity получает ограниченный owned payload (SQL length(blob) ≤516 KiB), освобождает ресурс запроса, затем проверяет digest и разбирает JSON. Scope описания также проверяется. Fence statement строится до writer и связывает компактный digest, scope, current execution owner и in_progress/непринятую cancellation; JSON под writer не сравнивается и повторно не разбирается. Digest защищает неизменность описания, а не является отдельной авторизацией.

Первоначальная регистрация разрешена для in_progress turn в исходном scope и при действующем execution owner. Повтор и recovery подтверждают наличие первой строки, не заменяя JSON/digest текущей конфигурацией. При смене runtime adapters или законном Blocked resume исходный контекст сохраняется. При отсутствии обязательного контекста — явный permanent cancellation_context_missing, без реконструкции из текущих настроек. Нет исторического backfill или отдельного background worker. Legacy/CLI terminalization без native execution/runtime snapshot/живого native owner сохраняет существующий путь.

Между Start-control ACK и durable registration есть граница допуска: controller может безопасно отказать из-за missing context, а не принять отмену без обязательств. Provider ещё не начат. Прямой CancelTurn завершает регистрацию/подтверждение исходного контекста и публикует TurnInterrupted через типизированный Gateway commit. Отдельной ordinary NativeTerminalEffectsPrepared для Interrupted в этой ветке больше нет: Gateway атомарно готовит сохранённый отменный план. Cancellation token сам по себе не является доказательством durable Interrupted.

Окончательная схема terminal marker

Entity и существующая таблица turn_event_projection_stream_state расширены тремя nullable полями: accepted_terminal_event_id, accepted_terminal_event_type и accepted_terminal_sequence (i64/big_integer). Это компактная отметка canonical acceptance текущего execution cycle, без payload. Она закрывает окно между append и projection. Interrupted представлен canonical TurnFailed с Interrupted status; его семантику подтверждает cancellation receipt.

Полностью удалён idx_turn_event_terminal_fence. Нет нового индекса на turn_event или _turn_event_zstd, запроса has_terminal_event, поиска event_type по истории, произвольного исторического LIMIT, сканирования/backfill журнала. Existing PK turn_id достаточен для marker и контекста. Новая миграция использует SchemaManager и SeaQuery builders для таблицы, nullable колонок, допустимого FK и защиты down. Обычные context операции используют Entity/SeaORM; joins/fences строятся SeaQuery, без нового hand-written production SQL.

Down сначала отказывает при любой context строке либо любом ненулевом marker поле, в том числе при marker без context или частично повреждённой отметке. Только пустое durable состояние допускает удаление. Проверки down не читают event journal. Миграция не запускалась; fixtures описывают plain и zstd пути, включая безопасный пустой down/up.

Доработка двух замечаний: cancellation IDs и race fallback

Перед первоначальной durable регистрацией actor выбирает отдельные детерминированные IDs: `<turn_id>:cancellation-effect:post-turn` и `<turn_id>:cancellation-effect:attached-task-cleanup`. Это преобразование применяется только к cancellation description, а обычный terminal_effect_preparation сохраняет прежние terminal-effect IDs и контракты других исходов. Controller берёт идентификаторы из сохранённого исходного контекста; другие причины, повтор и recovery не генерируют новый набор. Existing context, JSON и digest не перезаписываются. Контексты, сохранённые до этой правки со старыми IDs, сохраняются как есть; несовместимость с ранее activated строкой остаётся явным отказом, без скрытой реконструкции или миграции идентичности.

Уже активированный Blocked-plan и cancellation-plan теперь имеют разные PK outbox строк. В существующих preparation/supersede/activation queries ранее активированные строки исключены через terminal_committed_at/status; лимит по-прежнему равен двум эффектам на preparation и activation, плюс существующий sentinel row для проверки превышения. Quantum не увеличен, новых runtime DB запросов в этой доработке нет. Для этого в текущей новой миграции удаляется прежний unique index `uidx_native_terminal_effect_turn_kind` (turn_id, effect_kind): он запрещал соседство двух результатов одного вида несмотря на разные effect IDs. Unique-key annotations удалены из outbox Entity. PK effect_id и уже существующие неуникальные turn/batch, due и completed indexes сохраняются; новых индексов не создаётся. Existing validator сохраняет запрет повторного kind/ID в одном bounded batch. Down после guards восстанавливает прежний unique index; несовместимые данные вызывают ошибку и rollback, не потерю контекста. Plain/zstd migration fixtures проверяют наличие старого индекса до up, отсутствие после up, сохранность turn/batch index и восстановление при разрешённом пустом down. Эта миграция не исполнялась. Один turn может сохранять две исторические Blocked строки и две новые отменные строки; их присутствие не расширяет текущую волну activation. Payload/identity/gate/claims/checkpoint/attempts/terminal timestamps старой строки отменой не меняются. Собственный worker по-прежнему делает штатные claim/checkpoint/completion/compaction переходы этой строки.

В race fallback `commit_turn_interrupted_with_recovery_disposition` после ошибки turn_finish и локального Interrupted materialization разбирается через match: Ok подтверждает принятие и выполняет прежнюю очистку; Err возвращает classify_native_preparation_error именно ошибки materialization. Локальный Interrupted не становится durable ACK; типизированный BUSY/LOCKED остаётся retryable storage_temporarily_unavailable. Другой локальный терминальный исход сохраняет permanent interruption_transition_rejected, а реальный durable completion конфликт — permanent native_preparation_rejected.

Новые race fixtures имеют только cfg(test) barrier перед turn_finish и одноразовый отказ на границе materialization до DB capacity. BUSY для fault injection получен из настоящего SeaORM/SQLx driver error на изолированной БД, сохранён после rollback; под writer нет ожиданий/notifications. Тест проходит саму функцию отмены: initial InProgress read → конкурентный локальный finish → ошибочный turn_finish → fallback materialization → retryable отказ без receipt/ACK → реальный успешный повтор. Отдельная ветка создаёт настоящий конкурирующий canonical Completed и проверяет permanent отказ. Это не только вызов классификатора и не заглушка успешного publisher ACK; временный отказ инжектируется на границе, а успешное сохранение выполняется производственным CRUD путём.

Атомарная граница, races и replay

1. До writer: bounded context load/digest/parse, Interrupted hook input, bounded reason, проверка immutable плана, payload limits, сериализация/hashing outbox payload, compact fence statement и canonical payload preparation.
2. В `append_claimed_turn_event_projection_once`: действующие owner/guard проверки; для новой cancellation — digest/scope/owner/status revalidation, PK lookup compact marker, обычный bounded outbox prepare. Далее canonical append, context receipt, projection receipt, terminal marker CAS и optional deliveries фиксируются одной транзакцией. Marker CAS требует всех трёх null полей и правильного thread. Конфликт/ошибка откатывает весь write set.
3. `append_and_project_turn_event_in_transaction` также записывает marker только для newly inserted terminal canonical event, после существующего projection receipt, в той же транзакции. Поэтому конкурирующий Completed/Failed/Blocked canonical append запрещает cancellation даже до projection. Accepted cancellation receipt ограждает competing terminal append в обоих путях и старые owned execution записи. Execution guard не ослаблен.
4. Отдельная существующая projection-транзакция меняет Turn/execution и активирует уже durable outbox obligations атомарно. Append и projection не объединены механически. Canonical event остаётся источником принятого payload/identity, receipt и marker означают acceptance, outbox содержит сохранённый план. Crash после append до projection не требует модели, текущей конфигурации или старого actor: существующий replay/recovery доводит projection/activation. Deferred projection, event-appended-before-error и ACK semantics сохранены.
5. Exact canonical replay не пишет/не очищает marker, не создаёт событие заново и не переактивирует эффекты. Повтор cancellation после потерянного ACK использует первый canonical payload и причину. Pending projection не отменяет факт принятия. Устаревший controller read перепроверяется writer; конкурентный новый результат не становится фиктивным успехом.
6. Старый неподтверждённый success-plan может быть заменён отменным планом до acceptance по существующей outbox политике; после receipt обычная preparation и recovery supplemental не могут подменить план. Уже activated immutable obligations сохраняют прежние строгие правила совместимого replay. Произвольного добавления новых эффектов после terminal commit нет.

Законный Blocked resume

В `resume_blocked_turn_recovery` и `resume_task_owned_turn` helper `clear_confirmed_blocked_for_resume` вызывается в существующей writer-транзакции до изменения Turn status. Используется уже прочитанная и revalidated модель Blocked того же thread/turn; дополнительного Turn reread нет. Accepted cancellation receipt запрещает очистку. Ненулевая отметка должна полностью обозначать turn/blocked с положительной sequence, достигнутой projection watermark. CAS очищает только совпадающую ID/type/sequence отметку. Затем существующие Turn/recovery/execution ownership и task FSM/CAS переходы должны успешно завершиться; любой конфликт откатывает также очистку.

Legacy отсутствие marker допускается при сохранении прежних durable Blocked проверок. Interrupted/Failed/Completed marker не очищается при обычном recovery, смене owner, actor restart, повторной регистрации, восстановлении stream health или watermark backfill. Immutable cancellation context при resume не меняется. Исторический TurnBlocked после законного resume не является вечным fence нового execution cycle. Прежние activated outbox payload не переписываются при resume.

Результаты actor и диагностика

- Committed: подтверждён durable commit.
- CooperativeCancellation: token отменил ожидание; сам по себе это не ACK и не подтверждение Interrupted. Без owned durable observation применяется прежнее безопасное завершение.
- SupersededByDurableInterruption: типизированное подтверждение принятой owned cancellation; старая execution запись не объявляется committed. Actor останавливает/дожидается задачи, записывает локальную Interrupted observation, очищает active control/request/recovery и last snapshot/request.
- PermanentRejection {safe code, attempt}: без бессмысленных повторов, один самостоятельный ERROR publisher с фиксированным event_kind и безопасными полями. Необходимая остановка actor — DEBUG, без второго ERROR и exhausted.

Штатность не выводится из текста execution_fenced. Unknown/domain/ownership/context conflicts остаются permanent и заметными. Новые ERROR поля не содержат payload, raw chain или пользовательские данные. Existing transient timeout/backoff и разреженная outage диагностика сохранены; несвязанные сообщения и уровни не менялись. Public sentry_tracing_layer даёт тестам настоящий mapper со scoped subscriber, без глобального subscriber и живого Sentry.

classify_native_preparation_error дополнительно распознаёт типизированную цепочку anyhow (включая Context) → SeaORM DbErr Conn/Exec/Query → RuntimeErr::SqlxError → SQLx DatabaseError.code. Primary BUSY=5/LOCKED=6 определяются по low byte, включая extended codes. Сохраняется существующая обработка других временных ошибок доступа. Глобальная CRUD retry policy не расширена; исчерпавший ограниченные CRUD retries BUSY/LOCKED возвращается retryable в существующий publisher/controller контракт.

Два receipt lookup в commit_durable_agent_event теперь различают Ok(true), Ok(false), Err. Ok(true) подтверждает owned cancellation, Ok(false) оставляет исходную классификацию отказа, Err классифицирует фактическую ошибку чтения и не подменяет её старым guard/domain rejection. Ошибка чтения никогда не доказывает принятую отмену.

DB операции и фиксированные пределы

- Initial registration: owner lookup ≤1, context PK scope existence ≤1, Turn PK/status/scope ≤1, Thread PK/workspace ≤1, context insert ≤1 в одной writer-транзакции. Repeat/recovery: owner lookup ≤1 + context PK scope existence ≤1, без JSON reread или overwrite. Candidate clone делается до begin; serialization/digest до writer.
- Controller: context load ≤1 bounded row; receipt replay дополнительно ≤1 receipt и canonical event PK lookup. Missing context checks bounded execution/runtime snapshot/actor ownership, без истории. Context ≤516 KiB; effects ≤2 в одной preparation/activation; CRUD payload ≤256 KiB каждый, hook payload ≤255 KiB, cleanup reason ≤4096 символов.
- Новая cancellation внутри append: accepted receipt PK fence, exact replay lookup, compact joined digest/scope/owner/status fence, marker PK lookup, существующие bounded проверки/записи outbox ≤2 эффектов (включая supersede omitted effects), receipt CAS ≤1. После существующего projection receipt — marker CAS ≤1. Повторного ensure_healthy для marker нет: строку уже обеспечивает insert_claimed.
- Каждый новый Completed/Failed/Blocked terminal append в обоих производственных путях добавляет marker CAS ≤1; canonical replay и nonterminal events marker не обновляют.
- Законный resume добавляет accepted receipt PK lookup ≤1, stream PK lookup ≤1 и marker CAS clear ≤1. Turn/recovery/task/execution проверки остаются в той же транзакции. Legacy null marker обходится без clear UPDATE.
- Owned execution/competing terminal append и outbox preparation используют bounded receipt fences; full lease validation дополнительно читает accepted receipt. Операции идут через SqliteDatabase/CrudStore/repositories, наследуют operation scopes, scheduling classes и physical reader/writer routes. Приоритеты не повышены. Нет hooks/network/channel sends/joins/sleeps/backoff под DB capacity. Общие scheduling/outbox/recovery/auth механизмы не переписаны.

Это ненулевая DB/CPU/byte-copy стоимость; writer latency и throughput не измерялись.

Написанные регрессии — не запускались

Сохранены прежние manager, publisher, Gateway и 11 CRUD cancellation регрессий: исходный cleanup/context и lost ACK replay; невозможность удалить исходные обязательства; rollback append; restart deferred projection и fence старой preparation; конкуренция Completed/Failed/Blocked; замена неподтверждённого success-plan; пустой план; operation scopes; physical routes/event order/cancelled reservation; fail-closed recovery без истории; owner/immutable-plan revalidation.

Добавлены CRUD проверки:

- native_cancellation_digest_is_checked_outside_writer_and_fence_has_no_json — Entity timestamp/digest, tamper, отсутствие JSON в fence.
- native_cancellation_competing_terminal_append_before_projection_is_fenced — canonical acceptance до projection отвергает отмену.
- native_cancellation_marker_and_receipt_fence_both_append_paths_and_replay — оба production append пути, receipt/canonical identity replay.
- native_cancellation_marker_failure_rolls_back_entire_append_boundary — rollback marker/canonical/outbox/receipt.
- native_cancellation_blocked_lawful_resume_keeps_original_context_and_allows_interruption — настоящая preparation двух Blocked effects → canonical Blocked/activation → claims и hook checkpoint → отказ отмены без resume → lawful resume → Interrupted с двумя отдельными отменными effects. Полные старые in-flight rows неизменны после cancellation, повторов и реконструкции CrudStore. Worker completion старых claims и новый replay сохраняют штатное итоговое состояние; количество строк равно четырём, попытки новых и старых эффектов не сбрасываются. Competing Completed остаётся запрещён.
- native_cancellation_owner_change_and_health_restore_cannot_clear_fence — смена owner и stream health не очищают cancellation fence.
- native_cancellation_migration_plain_and_zstd_use_entity_schema_without_event_index — Entity/new migration для plain и zstd схемы, nullable marker/digest/timestamp, отсутствие event-index.
- native_cancellation_migration_down_preserves_context_and_terminal_markers; native_cancellation_migration_down_rejects_marker_without_context — защита durable данных.
- native_cancellation_task_resume_clears_only_confirmed_blocked_and_rolls_back_conflict — task-owned atomic resume и rollback всех агрегатов при conflicting marker.

Gateway typed failure tests в native_preparation_failure_tests.rs: настоящие SQLite BUSY, LOCKED_SHAREDCACHE и BUSY_SNAPSHOT под SeaORM/anyhow Context; остальные extended BUSY/LOCKED primary families; constraint и совпадающий текст не дают retries; actual receipt read failure возвращает storage_temporarily_unavailable вместо прежнего domain rejection, Ok(true)/Ok(false) различаются.

Сквозной `native_gateway_cancellation_persists_and_processes_hook_cleanup_without_repeat_preparation_or_error`: занятый controlled provider, непустые реальные Interrupted hook и cleanup obligations, canonical отмена и marker/receipt с исходными payload, обработка существующими workers с durable hook store и idempotent cleanup adapter. Управляемые Notify/Semaphore удерживают оба эффекта для проверки durable rows и затем разрешают завершение. Повтор отмены/worker poll не создаёт повторной обработки. Проверяется отсутствие ordinary NativeTerminalEffectsPrepared actor и повторных M/K ERROR через настоящий mapper/local Sentry transport. Scoped subscriber действует на изолированном current-thread runtime; глобальный subscriber не меняется. Сохранены пустой Gateway scenario и manager next-turn cleanup тест.

Дополнительно написаны Gateway `native_direct_cancellation_after_activated_blocked_resume_preserves_old_worker_rows` (реальный непустой Blocked prepare/append/activation, оба существующих workers, lawful resume, восстановление actor, прямой CancelTurn через listener/typed commit, отсутствие ordinary Interrupted preparation, здоровый actor, новые effects, неизменность всех полей старых completed rows при повторе/reconstructed store) и `native_cancellation_race_fallback_preserves_materialization_error_and_requires_durable_ack` (управляемая гонка, typed BUSY materialization failure, no receipt/ACK, успешный повтор, реальные permanent completion конфликты).

Manager `cancellation_context_freezes_interrupted_policy_request_handlers_and_cleanup_contract` проверяет обе policy ветки, исходные request/handler snapshot/contract и смену конфигурации. Publisher тесты различают cooperative cancellation, supersession и permanent rejection, проверяют bounded retry/timeout semantics и один ERROR с safe fields без canary payload. Existing shutdown/direct CancelTurn и terminal replay проверки сохранены. Тесты не используют живые сервисы; readiness новых сценариев определяется futures/barriers, а не sleeps.

Статические проверки

Прочитан накопленный tracked diff и новые исходники; проверены callers CancelTurn (Gateway, прямые manager callers, recovery и shutdown), обе append/projection/activation границы, replay, Blocked resume, entity literals и outbox ограничения. Выполнены rustfmt --check --edition 2024 --config skip_children=true для всех 26 изменённых/новых Rust-файлов, git diff --check, whitespace-проверка untracked файлов и Python tomllib разбор Cargo.lock/Cargo.toml. Эти проверки не подтверждают type checking, компиляцию, прохождение миграций или выполнение регрессий.

Будущий запуск только после отдельного разрешения, из этого worktree:

```sh
cargo test -p pioneer-crud native_cancellation
cargo test -p pioneer-agent durable_gateway_cancellation
cargo test -p pioneer-agent cancellation_context_freezes
cargo test -p pioneer-agent agent_loop::tests
cargo test -p pioneer-gateway native_gateway_cancellation
cargo test -p pioneer-gateway native_direct_cancellation_after_activated_blocked_resume
cargo test -p pioneer-gateway native_cancellation_race_fallback
cargo test -p pioneer-gateway native_preparation_failure_tests
cargo test -p pioneer-gateway turn_cancel_interrupts_running_turn_and_is_idempotent
cargo test -p pioneer-agent
cargo test -p pioneer-crud
cargo test -p pioneer-gateway
cargo test -p pioneer-runtime-events -p pioneer-protocol -p pioneer-migration -p pioneer-observability
```

Ограничения и непроверенные аспекты

Компиляция, новые миграции на обеих схемах, все fixtures и runtime regression outcomes пока не проверены исполнением. После разрешения нужен запуск новых и существующих replay/recovery/shutdown тестов; возможные type/fixture ошибки нельзя исключить статическим чтением. Down преднамеренно запрещает потерю durable данных. Legacy отсутствие обязательного исходного context остаётся fail-closed при rolling upgrade. Resume и отмена не переписывают ранее активированные immutable outbox obligations. Новая Blocked→Interrupted регрессия действительно готовит и активирует непустой Blocked-план до resume, включая in-flight claims/checkpoint; сквозной Gateway вариант также обрабатывает старые и новые эффекты настоящими workers. Стоимость DB операций и byte copies не измерялась. Forward migration удаляет один прежний unique outbox index без переписывания строк; down восстанавливает его после защитных проверок, что может потребовать работы по существующим outbox данным. Runtime lookup сохраняет прежний nonunique turn/batch index. Миграционный writer hold time не измерен.

Предоставленная историческая диагностика доказывает отказ подготовки и остановку actor, но не состав его эффектов. Здесь не утверждается потеря конкретного исторического hook/cleanup, подтверждённая компиляция/исполнение новых тестов или исчезновение всех production-событий M/K.
