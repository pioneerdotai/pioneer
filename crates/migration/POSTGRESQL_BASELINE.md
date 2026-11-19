# Начальная схема Pioneer 1.0.0 для PostgreSQL

Ветка `postgresql-migration`, исходный HEAD `ebbbee8fc5df359e3e6d39741acbe4decbe296fd`. Версия 1.0.0 начинает с пустой базы и собственного журнала SeaORM. Исполняемая линия 0.x остаётся доступна по исходному SHA.

## Источник схемы

Штатный SQLite-мигратор исходного HEAD отработал на двух новых временных файлах. Опись снята в режиме чтения через `sqlite_schema`, `table_list`, `table_xinfo`, `foreign_key_list`, `index_list`, `index_xinfo` и выборку непустых таблиц. DDL обоих файлов совпал.

Применились **все 120 регистраций** исходного `crates/migration/src/lib.rs`, от `m20260313_125253_create_workspace_table` до `m20261115_000001_portable_turn_payload`. Имена и порядок в журнале вывода совпали с регистрациями. Множество имён совпало с `seaql_migrations`. Пять регистраций вложенных модулей inbox использовали их явные имена `m20261016_000001_task_completion_inbox`, `m20261017_000001_task_inbox_activation`, `m20261018_000001_task_cancellation_actor`, `m20261018_000002_task_inbox_query_indexes` и `m20261018_000003_task_inbox_access_start`.

В исходной описи 233 обычные таблицы с учётом `sqlite_sequence` и журнала SeaORM, 2 виртуальные таблицы, 8 теневых таблиц FTS5, 549 явных индексов, 232 автоиндекса, 3 представления и 236 триггеров. Колонки сопоставлены с `crates/entity/src`, включая модели двух frozen-представлений. Вызовы сверены с CRUD, поиском и путями запуска Gateway. В свежей SQLite-базе `turn_event` и `turn_item` физически являются таблицами. Gateway может позднее включить для них прозрачное сжатие. Источники этого слоя находятся в `crates/gateway/src/database/startup/zstd_payload_compression.rs`, `crates/gateway/src/database/zstd_column.rs` и `crates/sqlite/src/zstd/transparent.rs`. Временная таблица из `prepare_expected_patch_snapshot_references` в `crates/crud/src/repositories/patch_history.rs` создаётся для одного прогона reconciliation и остаётся частью операции.

## Порядок и ведомость соответствия

У таблиц, прикладных индексов, представлений и перенесённых триггеров сохранены имена из итогового SQLite-каталога. В колонке «Шаг» указан суффикс файла `src/m20261119_*.rs`. Число в скобках показывает количество прикладных таблиц шага.

| Шаг | Исходные объекты и потребители | Решение PostgreSQL |
| --- | --- | --- |
| `000001_identity` (15) | `workspace*`, `gateway_identity`, `gateway_principal`, устройства, сессии и refresh, приглашения, аватары, аудит и поколения политики. CRUD `identity.rs`, `auth_session.rs`, `membership.rs`, `policy_generation.rs`. | Singleton, проверки пар полей, уникальность активного текущего workspace, FK и частичные ключи. Хеши в `bytea`, ID ревизии аватара сверяется через `encode(..., 'hex')`. Начальный workspace из Gateway bootstrap и строка поколения политики. |
| `000002_packages` (17) | `package_*`, `plugin_*`, `app_installation`, `skill_*`, `mcp_*`, документы AGENTS. CRUD установки пакетов, навыков и MCP. | Конечная модель package/base и plugin/app extension, проверки JSON и области установки. [Триггеры](src/sql/m20261119_000010_database_rules/packages.sql) сохраняют ограничения владения, запрет прямого удаления extension, очистку при удалении родителя, отвязку ссылок и максимум 256 компонентов. Блокировки строк workspace/package упорядочивают проверки slug и лимита при `READ COMMITTED`. |
| `000003_payloads` (18) | `portable_payload*`, артефакты и загрузка, `tool_output_chunk`, `patch_*`, `applied_patch_*`, `turn_diff_state`. CRUD `portable_payload.rs`, `artifact_binding.rs`, хранилища в `crates/tools/src/apply_patch`. | `bytea` для блоков, хешей и снимков. Проверки размера и смещений. [Триггеры](src/sql/m20261119_000010_database_rules/payloads.sql) присваивают 32-символьный hex ID, увеличивают ревизии, сбрасывают ссылку при логической замене и помечают старый payload retired в той же транзакции. |
| `000004_conversations` (38) | `thread*`, `turn*`, включая attempt/session bindings, события, входы, элементы, projection states, unread, membership. CRUD `turn.rs`, `turn_event.rs`, `thread_history_read.rs`. | Стабильные PK и FK, в том числе порядок `(turn_id, sequence)`, уникальность idempotency key и ограничения actor pair. `turn_item` и `turn_event` хранят логический TEXT и portable-ссылки. Триггеры содержат проверки и изменение read-model в той же транзакции. |
| `000005_execution` (38) | `agent_*` без памяти, `cli_runtime_*`, `execution_*`, `native_*`, `recovery_*`, `read_model_repair_*` и usage. CRUD `agent_domain.rs`, `native_event_cleanup.rs`, `native_terminal_effect_outbox.rs`. | Частичная уникальность активного и незавершённого recovery, очереди и индексы lease/deadline. [Триггеры](src/sql/m20261119_000010_database_rules/execution.sql) ставят native cleanup, пробуждают ожидающую работу и удаляют телеметрию владельца. Строки координаторов и счётчиков установлены миграцией. |
| `000006_tasks` (38) | `task*`, уведомления, `hook_*`, occurrences, reviews, delivery, inbox и durable pending. CRUD `task_event_fanout.rs`, `task_completion_inbox.rs`, `task_child_work.rs`, `task_delivery_recovery.rs`. | Сохранены последовательности событий, idempotency и inbox activation. [Триггеры](src/sql/m20261119_000010_database_rules/tasks.sql) обновляют поколение и durable pending атомарно с источником, соблюдают условия очистки по курсору и сброс retry при изменении доставки. Начальные строки курсоров и bootstrap сохранены. |
| `000007_history_storage` (42) | `compaction_*`, `html_preview_*`; CRUD `compaction.rs`, `checkpoint_proofs.rs`, `frozen_storage_lifetime.rs`, `frozen_sweep_work.rs`, `html_preview_retention.rs`. | [Представления](src/sql/m20261119_000010_database_rules/views.sql) `compaction_live_sources`, `compaction_frozen_message`, `compaction_frozen_import` сохраняют фильтры видимости, readiness, bounds и span-проекцию. [Триггеры](src/sql/m20261119_000010_database_rules/history.sql) сохраняют версии источников, запечатанные proofs, защиту frozen roots, удержание preview и постановку lifecycle/sweep. Сброс terminal generation в BEFORE UPDATE присваивает `NEW`. SQLite `MAX + 1` для capture order и service rows сериализуется транзакционными advisory locks, чтобы курсор видел назначенный порядок после коммита. |
| `000008_memory` (18) | `agent_memory*`, `thread_episodic*`, `self_improvement*`. CRUD `agent_memory.rs`, `thread_episodic.rs`, `thread_episodic_search.rs`. | GIN на `to_tsvector('simple', …)` для обеих текстовых проекций. [Триггеры](src/sql/m20261119_000010_database_rules/memory.sql) обновляют `indexed_item_count` и сериализуют инициализацию каталога с изменением eligibility по thread. `vector` устанавливается до колонки `embedding`. Размерность совпадает с `dimension`, нулевой вектор отклоняется. |
| `000009_relations` | 129 декларативных связей итоговой SQLite-схемы. | FK устанавливаются после создания доменных таблиц и ключей. Сохранены составы колонок и действия `ON DELETE` — 77 CASCADE, 41 RESTRICT, 6 NO ACTION и 5 SET NULL. В PostgreSQL они действуют при записи и удалении. |
| `000010_database_rules` | Все три прикладных view и оставшиеся row-trigger правила из перечисленных групп. | SeaQuery 1.0.2 не строит view, функции PL/pgSQL и триггеры, поэтому используется DDL в `src/sql/m20261119_000010_database_rules`. В `down` сначала удаляются правила и представления, затем FK и таблицы. |

### Типы, ordering и объекты хранения

- SQLite `varchar(n)` фактически принимает строки произвольной длины. Обычные текстовые колонки PostgreSQL наследуют collation базы. Побайтовые исключения приведены ниже. Явные проверки длины и алфавитов сохранены. Для auth и аватаров байтовые лимиты продолжают действовать на `bytea`. Имена отдельных CHECK PostgreSQL назначает при создании.
- `timestamp_with_timezone_text` переходит в `timestamptz`, как требует Rust `DateTimeWithTimeZone`. Сравнения идут по моменту времени. Временные значения по умолчанию берутся на начало SQL-оператора с точностью PostgreSQL. Unix seconds и milliseconds остаются `bigint`. Например, `idx_task_run_inbox_epoch` использует UTC и целые секунды. Nullable-deadline action outbox использует `-infinity` внутри индекса для прежнего положения пустого срока.
- Ширина целых `i32` и `i64`, а также представление `bool` взяты из моделей. Числовые флаги 0/1, опубликованные как `i64` или `i32`, сохранены. Проверка 0/1 у Boolean `native_event_cleanup_bootstrap.complete` заменена самим типом и `NOT NULL`. Проверки SQLite `typeof` покрыты типами. JSON документов хранится в TEXT для текущего Rust-контракта. Ограничения и выражения проверки используют PostgreSQL JSON, в том числе `IS JSON` и адресные пути.
- `sqlite_sequence` заменена PostgreSQL identity для фактически автоназначаемых PK. Для прикладной привязки к порядку есть `insertion_ordinal` в `turn`, `turn_input`, `turn_item`, `thread_lineage`, `compaction_item_revision`, `compaction_input_revision`, `compaction_frozen_span`, `compaction_frozen_layout`, `compaction_checkpoint_event_input`, `compaction_checkpoint_replay_alias` и `task_run_conversation_snapshot`. Порядок канонических событий остаётся по доменному `sequence`. PostgreSQL identity выдаёт номер до коммита. Runtime предстоит привязать многотранзакционные high-water scans к согласованному снимку или сериализованному watermark.
- `thread_episodic_fts`, `agent_memory_fts`, их 8 теневых таблиц и 6 синхронизирующих триггеров заменены двумя GIN-индексами на источниках. Новые индексы обновляются вместе со строкой. Условия доступа и ранжирование потребуют нового SQL, включая токенизацию, BM25, сниппеты и tie-breaks.
- `thread_episodic_embedding_artifacts.vector_bytes` заменена на `embedding vector` без фиксированного typmod. `crates/gateway/src/thread_episodic_embedding.rs` поддерживает разные provider/model, в том числе 384, 1536, 3072 и custom 4096. CRUD использует точное cosine distance после фильтра provider/model/dimension/normalized. PostgreSQL-эквивалент — `<=>` в пределах того же набора кандидатов. ANN потребовал бы отдельного решения по точности и размерностям. Сохранены идентичность артефакта и B-tree индексы. Верхняя граница типа pgvector — 16 000 координат. Тип отклоняет нечисловые и бесконечные компоненты.
- Пять `_stable_skill_id_legacy_*` и два `compaction_publication_*fence` исключены. Первые хранили строки для перехода 0.x, вторые остались после удаления триггеров в `m20261008_000009`. Поиск использований подтвердил отсутствие производственных потребителей у удалённых модулей `stable_skill_id` и `portable_payload_target`. Промежуточные переименования, дедупликации и одноразовые UPDATE вошли в конечное определение таблиц и ключей.
- `_turn_event_zstd`, `_turn_item_zstd`, `_payload_dict`, `_zstd_configs`, `_zstd_dicts`, SQLite write-through views и extension-хуки заменены обычными таблицами с управляемым PostgreSQL хранением TOAST и существующим portable payload. Правила ревизий, read-model и lifecycle привязаны к логическим изменениям.
- В схеме 20 стартовых строк. Это default workspace, начальные поколения и координаторы authorization/agent/read-model/native cleanup, task occurrence/fanout/inbox/child, compaction lifecycle/frozen sweep и HTML preview. Исключены строка retired publication fence и маркер `memory_sqlite_payload_reset_v1` для 0.x. Уникальные gateway/superuser IDs создаёт `crates/gateway/src/identity/bootstrap.rs`. Повторные операции восстановления прерванных jobs, replay, refills, reconciliation и integrity gates остаются в `crates/gateway/src/bootstrap.rs` и `crates/gateway/src/database/startup/mod.rs`.

## Фактические прогоны и сверка

Обычный вызов Cargo из корня остановился при разрешении зависимостей workspace. В офлайн-повторе точная причина — отсутствующий checkout `cua-driver-sdk` для `pioneer-gateway` (`trycua/cua`, `cua-driver-rs-v0.38.0#85e2b131`). Для SQLite использована временная копия трёх крейтов из исходного HEAD с исходным `main.rs`; для PostgreSQL — временный workspace с одним member и ссылкой на рабочий `crates/migration/src`. В обоих случаях зависимости взяты из исходного lockfile после исключения остальных members. Собирался target `pioneer-migration` и его библиотечные зависимости.

Старый бинарник отдельно запущен на новом SQLite-файле:

```sh
/tmp/pioneer-pg-baseline.RSdWXF/sqlite-migrator \
  -u 'sqlite:///tmp/pioneer-pg-baseline.RSdWXF/baseline-confirmed.db?mode=rwc' up
```

PostgreSQL 18.7 запущен из `/Users/alexander/Code/pioneer/pioneer-pg/classic/pgvector-test/vendor/runtime` с отдельным `PGDATA=/tmp/pioneer-pg-baseline.RSdWXF/pgdata` и loopback-портом 55483. В целевой базе перед установкой было 0 объектов в `public` и 0 записей о расширении `vector`.

Команда выполнена дважды с тем же адресом и без изменений схемы между прогонами:

```sh
CARGO_NET_OFFLINE=true CARGO_TARGET_DIR=/tmp/pioneer-pg-baseline.RSdWXF/target \
  cargo run --locked --manifest-path /tmp/pioneer-pg-baseline.RSdWXF/new/Cargo.toml \
  -p pioneer-migration -- -u 'postgresql://pioneer_review@127.0.0.1:55483/pioneer_v1_review' up
```

Первый запуск применил все 10 миграций, второй сообщил `No pending migrations`. Оба завершились с кодом 0. На отдельной копии `pioneer_v1_down` бинарник с явно указанным URL выполнил `reset`. Все 10 шагов откатились в обратном порядке. Таблицы, представления, последовательности и журнал SeaORM удалены. Расширение `vector` осталось как разделяемая инфраструктура.

Сверка в режиме чтения использовала `pg_class`, `pg_attribute`, `pg_attrdef`, `pg_constraint`, `pg_index`, `pg_trigger`, `pg_proc` и `pg_extension` и прочитала журнал и стартовые записи. Сопоставлены имена, типы, NULL, defaults, составы ключей, направления индексов, признаки уникальности и частичные предикаты. Проверены определения трёх view, два GIN и характерные expression/partial индексы, включая `uidx_workspace_single_active_current`, `checkpoint_replay_alias_identity`, `native_event_cleanup_candidate`, inbox epoch и exhausted outbox.

| Объект | Результат |
| --- | ---: |
| Таблицы, включая `seaql_migrations` | 225 |
| Колонки таблиц, включая журнал | 2464 |
| Объявленные прикладные defaults | 507 |
| Прикладные PK / FK / UNIQUE constraints / CHECK | 223 / 129 / 33 / 252 |
| Явные индексы без PK и UNIQUE-constraint индексов | 544 |
| Частичные / уникальные среди явных индексов | 48 / 102 |
| Представления / identity sequences | 3 / 15 |
| Прикладные триггеры | 232 |
| Начальные строки | 20 |
| Расширение | vector 0.8.7 |

Из 232 PostgreSQL-триггеров 230 соответствуют SQLite, ещё два выделяют стабильные номера frozen sweep work/consumer. `compaction_checkpoint_replay_alias` сохраняет исходный составной expression unique index в качестве ключа дедупликации.

## Исправления после ревью

- [history.sql](src/sql/m20261119_000010_database_rules/history.sql) — заменены все **13** скалярных `max` в `ON CONFLICT DO UPDATE` для `compaction_lifecycle_pending`. `due_at` берёт `GREATEST` от нового `eligible_at` и сохранённого `retry_not_before`. В 13 ветках вставки `due_at` берёт `GREATEST(0, eligible_at)`, поскольку начальный `retry_not_before` равен нулю. Обе ветки соблюдают CHECK даже при отрицательном `deadline_ms`. Статический просмотр SQL начальной цепочки нашёл 13 оставшихся вызовов `min`/`max`, у всех ровно один аргумент.
- [packages.sql](src/sql/m20261119_000010_database_rules/packages.sql) — `package_component_insert` и `package_component_update` используют общую функцию `package_component_validate`. Проверка области установки и захват `FOR SHARE` происходят в одном запросе к строке установки. Блокировка действует до конца транзакции и конфликтует с блокировкой, которую берёт `UPDATE` области, включая изменения неключевых полей. CHECK разрешает максимум одну ссылку на установку на строку компонента. Валидатор выбирает одну ветку, поэтому за один вызов захватывает максимум одну строку установки. При вставке существующий `package_component_bound` выполняется раньше валидатора, в порядке пакет → установка.
- [drop_rules.sql](src/sql/m20261119_000010_database_rules/drop_rules.sql) — при откате общая функция удаляется после обоих использующих её триггеров. Существующие FK проверены по каталогу. Они ссылаются на ID, сохраняют `ON DELETE SET NULL` для навыка и MCP и `ON DELETE RESTRICT` для пакета. Выразить правила области через них потребовалось бы с дублированием полей области и отдельным представлением признака `pack_id IS NULL`. Схема колонок и ограничений сохранена.

| Установка | Первой блокировку получила привязка | Первым блокировку получило изменение области |
| --- | --- | --- |
| Навык | `FOR SHARE OF s` удерживает строку `skill_installation`. Изменение `scope_key` или `pack_id` ждёт коммита. Затем `package_skill_scope` выполняет свой запрос на свежем снимке `READ COMMITTED`, видит привязку и отклоняет область другого workspace или ненулевой `pack_id`. | Запрос привязки ждёт на `FOR SHARE OF s`. PostgreSQL повторно проверяет предикаты по актуальной версии строки. Несовпавший `scope_key` или ненулевой `pack_id` дают пустой результат, и валидатор отклоняет привязку. |
| MCP | `FOR SHARE OF m` удерживает строку `mcp_server_installation`. Изменение `scope_key` или `scope_kind` ждёт коммита. Затем `package_mcp_scope` на свежем снимке видит привязку и отклоняет несовместимый workspace или `scope_kind`. | Запрос привязки ждёт на `FOR SHARE OF m`. После коммита PostgreSQL повторно проверяет `scope_key` и `scope_kind = 'workspace'` на актуальной версии. Пустой результат приводит к отказу валидатора. |

### Повторный прогон после исправлений

Для окончательной версии использовался отдельный кластер PostgreSQL 18.7 с каталогом `/tmp/pioneer-pg-baseline.RSdWXF/pgdata-revision`. В новой базе `pioneer_v1_revision_final` до запуска было 0 объектов в `public`. Сервер показывал доступность pgvector 0.8.7. Штатный мигратор собран с тем же временным узким workspace.

Оба раза выполнена одна команда:

```sh
CARGO_NET_OFFLINE=true CARGO_TARGET_DIR=/tmp/pioneer-pg-baseline.RSdWXF/target \
  cargo run --locked --manifest-path /tmp/pioneer-pg-baseline.RSdWXF/new/Cargo.toml \
  -p pioneer-migration -- -u 'postgresql://pioneer_review@127.0.0.1:55484/pioneer_v1_revision_final' up
```

Первый запуск применил все 10 шагов, второй сообщил `No pending migrations`. Через `pg_get_functiondef` сверены 12 тел функций с исправленным SQL, включая 9 функций с 13 upsert-путями компактизации. `pg_get_triggerdef` подтвердил привязку двух событий компонента к общему валидатору и события UPDATE полей области на обеих установках. `pg_get_constraintdef` подтвердил формулу `due_at = GREATEST(eligible_at, retry_not_before)`, CHECK вида компонента и все три FK. В журнале 10 записей, установлено расширение `vector` 0.8.7.

Конкурентная корректность разобрана статически для `READ COMMITTED`. Выполнение конкурентного сценария остаётся за отдельным разрешением. Сверка ограничена запуском мигратора и чтением каталога.

## Закрытие состава доказательств контрольной точки

Правка затрагивает [history.sql](src/sql/m20261119_000010_database_rules/history.sql) и этот отчёт. Имена и привязки триггеров, FK и порядок отката остались прежними.

| Таблица | Исправленные триггеры | Правило UPDATE |
| --- | --- | --- |
| `compaction_checkpoint_replay_alias` | `compaction_checkpoint_replay_alias_sealed_insert`, `compaction_checkpoint_replay_alias_sealed_delete` | `compaction_checkpoint_replay_alias_immutable_update` запрещает UPDATE |
| `compaction_checkpoint_event_input` | `compaction_checkpoint_event_input_sealed_insert`, `compaction_checkpoint_event_input_sealed_delete` | `compaction_checkpoint_event_input_immutable_update` запрещает UPDATE |
| `compaction_checkpoint_import` | `compaction_checkpoint_import_sealed_insert`, `compaction_checkpoint_import_sealed_delete` | `compaction_checkpoint_import_immutable_update` запрещает UPDATE |
| `compaction_coverage` | `checkpoint_coverage_sealed_insert`, `checkpoint_coverage_sealed_delete`, `checkpoint_coverage_sealed_update` | UPDATE берёт блокировки старой и новой контрольных точек по возрастанию ID и проверяет обе |

### Статический разбор при READ COMMITTED

`seal` в `crates/crud/src/repositories/checkpoint_proofs.rs` завершает публикацию через `UPDATE compaction_checkpoint SET proof_version = 1 WHERE id = … AND proof_version = 0` в транзакции. INSERT доказательства блокирует родителя по `NEW.checkpoint_id`, DELETE по `OLD.checkpoint_id`. Запрос выбирает строку по ID с `FOR SHARE` и проверяет возвращённый `proof_version`. Для coverage блокировка взята внутри материализованного запроса по ID, а предикат закрытия применяется к его результату. Сохранены проверка закрытой порции и исключение для повторной вставки существующего ключа coverage. Такой повтор не добавляет строку, а UPDATE остаётся под проверкой закрытия.

- Изменение доказательства первым получило `FOR SHARE` на открытой точке. Обновление `proof_version` ждёт окончания транзакции, поскольку блокировка UPDATE неключевого поля конфликтует с `FOR SHARE`.
- Закрытие первым получило блокировку строки. Запрос доказательства ждёт и при коммите закрытия получает актуальную строку с `proof_version = 1`. Попытка изменить состав отклоняется. При откате закрытия запрос получает открытую версию и может продолжить с учётом остальных предикатов.
- `checkpoint_sealed_delete` защищает родителя с `proof_version = 1` или закрытой порцией. Для удаляемой открытой точки `compaction_coverage` использует `ON DELETE CASCADE`. Когда его дочерний DELETE выполняется после удаления родителя тем же оператором, блокирующая выборка пуста и триггер пропускает удаление. У replay alias, event input и import действует `NO ACTION`: зависимые строки удаляются явно. Отсутствующий родитель не создаёт нового запрета в DELETE, а целостность INSERT по-прежнему обеспечивает FK.

### Проверка установки и каталога

Новый изолированный PostgreSQL 18.7, каталог данных `/tmp/pioneer-pg-baseline.RSdWXF/pgdata-proof-seal`. Перед первым запуском в схеме `public` было 0 объектов, сервер показывал доступность pgvector 0.8.7. Дважды выполнена одна команда штатного мигратора из временного узкого workspace:

```sh
CARGO_NET_OFFLINE=true CARGO_TARGET_DIR=/tmp/pioneer-pg-baseline.RSdWXF/target \
  cargo run --locked --manifest-path /tmp/pioneer-pg-baseline.RSdWXF/new/Cargo.toml \
  -p pioneer-migration -- -u 'postgresql://pioneer_review@127.0.0.1:55485/pioneer_v1_proof_seal' up
```

Строки результата первого запуска:

```text
Migration 'm20261119_000001_identity' has been applied
Migration 'm20261119_000002_packages' has been applied
Migration 'm20261119_000003_payloads' has been applied
Migration 'm20261119_000004_conversations' has been applied
Migration 'm20261119_000005_execution' has been applied
Migration 'm20261119_000006_tasks' has been applied
Migration 'm20261119_000007_history_storage' has been applied
Migration 'm20261119_000008_memory' has been applied
Migration 'm20261119_000009_relations' has been applied
Migration 'm20261119_000010_database_rules' has been applied
```

Строки результата второго запуска:

```text
Applying all pending migrations
No pending migrations
```

В режиме чтения сопоставлены тела функций по `pg_get_functiondef` и привязки по `pg_get_triggerdef`. Совпали девять изменённых правил, три запрета UPDATE и запрет DELETE закрытого родителя. `pg_get_constraintdef` подтвердил один `ON DELETE CASCADE` и три `NO ACTION`. В журнале SeaORM 10 записей, установлено расширение `vector` 0.8.7. Оба порядка транзакций и поведение каскада разобраны по коду. Конкурентные SQL-сценарии, тесты и приложение не запускались.

## Сериализация coverage с продвижением порции

Изменены три функции `checkpoint_coverage_sealed_insert`, `checkpoint_coverage_sealed_update` и `checkpoint_coverage_sealed_delete` в [history.sql](src/sql/m20261119_000010_database_rules/history.sql) и этот отчёт. Состав триггеров, FK и порядок отката сохранены.

### Статический разбор

В [compaction_runner.rs](../crud/src/repositories/compaction_runner.rs#L1740) runner в одной транзакции обновляет поколение состояния, записывает observation и кандидата, добавляет coverage с `ON CONFLICT DO NOTHING`, затем продвигает `next_portion` через UPDATE одной строки операции и обновляет статус. Триггеры состояния сбрасывают terminal generation и обновляют очередь lifecycle. Триггер вставки кандидата проверяет границу обычным SELECT, а FK защищают ключ операции. Сам UPDATE `next_portion` запускает `operation_portion_monotonic`, который проверяет монотонность без чтения или блокировки дочерних строк. Сброс terminal generation и обновление lifecycle для операции относятся к отдельному UPDATE статуса. В [checkpoint_proofs.rs](../crud/src/repositories/checkpoint_proofs.rs#L3426) `freeze_checkpoint` также продвигает границу без блокирующего чтения checkpoint. Прежний `FOR SHARE` на checkpoint закрывал гонку с `proof_version`, а FK-блокировка ключа операции совместима с UPDATE неключевого `next_portion`.

Теперь триггеры coverage сначала получают checkpoint по ID с `FOR SHARE`. При переносе выбирают обе строки в порядке ID. Подзапрос ARRAY собирает все их `operation_id` до захвата блокировок операций. Затем операции выбираются по ID с `FOR SHARE` в порядке ID. Предикат `portion < next_portion` использует возвращённое блокирующей выборкой значение. Runner захватывает блокировки в том же порядке на шаге записи coverage и затем повышает блокировку единственной операции при UPDATE. Сам UPDATE границы не пытается захватить checkpoint или coverage в обратном порядке.

- Продвижение первым блокирует операцию. Запись coverage ждёт. При коммите блокирующая выборка возвращает новый `next_portion`, и триггер отклоняет изменение закрытой порции. При откате выборка получает прежнюю границу. Открытая порция допускает запись при соблюдении проверки `proof_version`.
- Допустимая запись coverage первой получает `FOR SHARE` на операции. Продвижение ждёт коммита или отката записи. После коммита изменение предшествует закрытию, после отката граница может продвинуться без этого изменения.
- Повторный INSERT проверяет точный ключ coverage отдельным оператором после ожидания блокировок. На свежем снимке видно и совпадающую строку, которую успела зафиксировать продвигающая транзакция. Такой повтор сохраняет прежнюю семантику `ON CONFLICT DO NOTHING`. UPDATE проверяет обе стороны переноса. При каскадном удалении отсутствующий checkpoint даёт пустую выборку и пропускает очистку. Если виден checkpoint, а его операция уже удалена, `LEFT JOIN` сохраняет проверку `proof_version`. Конкурентный каскад начинает с блокировки родителя. При цикле ожиданий PostgreSQL может прервать одну транзакцию, поэтому для удаления сохраняется необходимость повторять транзакцию после deadlock.

Соседние правила с таким же окном гонки по границе: `checkpoint_closed_portion_insert`, `checkpoint_sealed_identity`, `checkpoint_sealed_delete` и `manifest_sealed_ownership_insert`, `manifest_sealed_ownership_update`, `manifest_sealed_ownership_delete`. Они читают границу без конфликтующей блокировки операции. Блокировка самой строки checkpoint при UPDATE или DELETE не защищает от пути `freeze_checkpoint`. Исправления этих шести правил описаны в следующем разделе.

### Проверка PostgreSQL

Новая изолированная база `pioneer_v1_coverage` в кластере `/tmp/pioneer-pg-baseline.RSdWXF/pgdata-coverage`, PostgreSQL 18.7. Перед первым запуском в `public` было 0 объектов и сервер показывал доступность pgvector 0.8.7. Дважды выполнена одна команда:

```sh
CARGO_NET_OFFLINE=true CARGO_TARGET_DIR=/tmp/pioneer-pg-baseline.RSdWXF/target \
  cargo run --locked --manifest-path /tmp/pioneer-pg-baseline.RSdWXF/new/Cargo.toml \
  -p pioneer-migration -- -u 'postgresql://pioneer_review@127.0.0.1:55486/pioneer_v1_coverage' up
```

Первый запуск применил все 10 шагов по журналу, включая `Migration 'm20261119_000010_database_rules' has been applied`. Второй вывел `Applying all pending migrations` и `No pending migrations`. `pg_get_functiondef` и `pg_get_triggerdef` подтвердили точное совпадение трёх тел и привязок. Каталог подтвердил оба FK каскада `compaction_operation → compaction_checkpoint → compaction_coverage`, 10 записей журнала и установленный `vector` 0.8.7. Логи находятся в `/tmp/pioneer-pg-baseline.RSdWXF/pg-coverage-up-{1,2}.log` и `pg-coverage-catalog.log` в том же каталоге. Порядок блокировок, ожидание и откат разобраны статически. Конкурентные SQL-сценарии, тесты и приложение не запускались.

## Границы checkpoint и владение manifest

Изменены [history.sql](src/sql/m20261119_000010_database_rules/history.sql) и этот отчёт. Имена и привязки триггеров и порядок отката сохранены.

| Триггеры | Блокировки и проверка при READ COMMITTED |
| --- | --- |
| `checkpoint_closed_portion_insert` | Если кандидат с тем же ключом operation/portion уже виден, берёт `FOR SHARE` на checkpoint до блокировки операции. Затем читает `next_portion` через `FOR SHARE` на операции. Для закрытой порции проверяет точный повтор отдельным запросом после ожидания. |
| `checkpoint_sealed_identity`, `checkpoint_sealed_delete` | UPDATE или DELETE уже держит строку checkpoint. Затем триггер читает границу под `FOR SHARE` на операции. UPDATE блокирует старую и новую операции по ID. Исходный предикат защищает старую порцию, а при смене пары операция/порция UPDATE дополнительно проверяет целевую границу. |
| `manifest_sealed_ownership_{insert,update,delete}` | Берут `FOR NO KEY UPDATE` на операции, при переносе — на обеих в порядке ID. Затем отдельным оператором перечитывают coverage, `proof_version`, `next_portion` и, для INSERT, возможность точного повтора. Блокировка берётся даже при пустом исходном наборе coverage. Manifest не берёт блокировки checkpoint. |
| `checkpoint_published_proof_reset` | На UPDATE `proof_version` берёт `FOR SHARE` на операции после захвата строки checkpoint. Это включает переход 0 → 1. Поля операции при совмещённом UPDATE упорядочены тем же образом. |
| Три `compaction_checkpoint_*_sealed_insert` и `checkpoint_coverage_sealed_{insert,update}` | Проверяют, что нужный checkpoint получен блокирующей выборкой. Coverage требует также заблокированную операцию нового checkpoint. INSERT доказательств использует `NEW.checkpoint_id`. |

### Что следует из исходников

1. **Запись до закрытия.** Вставка или удаление доказательства держит `FOR SHARE` на checkpoint, и UPDATE `proof_version` ждёт. Coverage берёт checkpoint, затем операцию, и продвижение `next_portion` ждёт освобождения операции. Manifest удерживает операцию в режиме `FOR NO KEY UPDATE`. Этот режим конфликтует и с продвижением, и с `FOR SHARE`, который берут писатели checkpoint, coverage и `proof_version`. Runner в `crates/crud/src/repositories/compaction_runner.rs` пишет кандидата и coverage до UPDATE границы. Для видимого повторного кандидата порядок checkpoint → operation сохраняется уже в INSERT кандидата. Для нового кандидата runner уже держит операцию при вставке собственной строки checkpoint, видимой ему в последующем INSERT coverage. Само продвижение запускает проверку монотонности без захвата checkpoint.
2. **Запись после ожидания закрытия.** При коммите блокирующее чтение возвращает актуальную версию checkpoint или операции. Изменения доказательств проверяют `proof_version`, coverage и соседние правила — предусмотренные для них версию и порцию. Manifest читает все условия после получения блокировок новым оператором. В том числе видит coverage, созданную транзакцией продвижения. При откате блокирующее чтение и последующий запрос видят состояние без изменений отменённой транзакции. Исключения для точных повторов checkpoint, coverage и manifest сохранены. Возможные циклы при каскадах и нескольких транзакциях по-прежнему требуют обработки deadlock при портировании.
3. **Параллельное создание родителя.** INSERT доказательства отклоняется с `23503`, если выборка не нашла checkpoint. INSERT и UPDATE coverage отклоняются, если из заблокированных строк нельзя подтвердить новый checkpoint и его операцию. Если родитель вставлен ранее в той же транзакции, он виден и проходит обычные проверки. INSERT checkpoint и INSERT/UPDATE manifest также требуют найденную и заблокированную целевую операцию. Отдельный FK не может подтвердить, что родитель прошёл проверку в триггере. DELETE допускает отсутствие удалённого родителя. Каскад operation → checkpoint → coverage, а также operation → manifest продолжают пользоваться прежними FK и правилами удаления. Если checkpoint ещё виден, проверка его `proof_version` действует и при отсутствии строки операции.

### Что подтверждено на PostgreSQL

В новой изолированной базе `pioneer_v1_boundaries_final` до первого запуска в `public` было 0 объектов. PostgreSQL 18.7, pgvector 0.8.7. Оба раза выполнена одна команда штатного мигратора:

```sh
CARGO_NET_OFFLINE=true CARGO_TARGET_DIR=/tmp/pioneer-pg-baseline.RSdWXF/target \
  cargo run --locked --manifest-path /tmp/pioneer-pg-baseline.RSdWXF/new/Cargo.toml \
  -p pioneer-migration -- -u 'postgresql://pioneer_review@127.0.0.1:55487/pioneer_v1_boundaries_final' up
```

Первый запуск применил все 10 шагов. Второй вывел `Applying all pending migrations` и `No pending migrations`. Сверка через `pg_get_functiondef` и `pg_get_triggerdef` подтвердила все 12 изменённых функций и их привязки. Каталог подтвердил три каскадных FK, журнал из 10 шагов и установленный `vector` 0.8.7. Логи `pg-boundaries-final-up-{1,2}.log` и `pg-boundaries-final-catalog.log` находятся в указанном временном каталоге. Конкурентные сценарии, тесты и приложение не запускались.

## Проверка целевой порции при UPDATE checkpoint

`checkpoint_sealed_identity` в [history.sql](src/sql/m20261119_000010_database_rules/history.sql) проверяет `NEW.portion < next_portion` по уже заблокированной строке `NEW.operation_id`, если пара `(operation_id, portion)` изменилась. При нарушении возвращается `23514` с сообщением `candidate portion already closed`. Строки операций по-прежнему читаются через `FOR SHARE` в порядке ID. Проверки исходной порции, `OLD.proof_version = 1` и существования целевой операции сохранены.

Статический разбор при границе 5 даёт отказ для переноса открытого checkpoint с порции 7 на свободную 3. Свободная порция 5 проходит целевую проверку благодаря строгому сравнению. При переносе между операциями используется граница целевой операции, а проверка исходной стороны использует исходную границу.

На новой изолированной PostgreSQL 18.7 с доступным pgvector 0.8.7 до запуска было 0 объектов в `public`. Дважды выполнена команда:

```sh
CARGO_NET_OFFLINE=true CARGO_TARGET_DIR=/tmp/pioneer-pg-baseline.RSdWXF/target \
  cargo run --locked --manifest-path /tmp/pioneer-pg-baseline.RSdWXF/new/Cargo.toml \
  -p pioneer-migration -- -u 'postgresql://pioneer_review@127.0.0.1:55488/pioneer_v1_checkpoint_destination' up
```

Оба запуска завершились с кодом 0. Первый применил все 10 миграций, второй вывел `Applying all pending migrations` и `No pending migrations`. Тело функции из `pg_get_functiondef` совпало с исходником побайтно. Результаты переходов установлены статически. Прикладной SQL, тесты и приложение не запускались.

## Исправления по независимому ревью

### HTML-превью

[history.sql:3111](src/sql/m20261119_000010_database_rules/history.sql#L3111) — `html_preview_header_update` явно читает `html_preview_frozen_index.next_ordinal` и `html_preview_frozen_index.complete` в ветках THEN. При совпадении ID, хеша и числа сообщений используются сохранённые значения, в остальных случаях обе величины сбрасываются в ноль. Исходные ошибки сверены с `qualification.log`; исправленный оператор из установленного тела принят `PQprepare` с типизированными `NEW` и `OLD` как двумя параметрами. Оператор сохранён в `/tmp/pioneer-pg-baseline.RSdWXF/independent-fixes-preview-prepare.sql`. Выполнение upsert не проводилось.

### Удаление пакетов

[relations:1162](src/m20261119_000009_relations.rs#L1162) и [relations:1207](src/m20261119_000009_relations.rs#L1207) переводят четыре FK на `ON DELETE CASCADE`. PostgreSQL сам удаляет строки `plugin_keyword`, `plugin_default_prompt`, `plugin_installation` и `app_installation` после удаления родителя. Удалены `package_domain_cleanup` из [packages.sql:59](src/sql/m20261119_000010_database_rules/packages.sql#L59) и его откат из [drop_rules.sql](src/sql/m20261119_000010_database_rules/drop_rules.sql). Relations down снимает те же FK по именам.

Статически прослежен [delete_parent](../crud/src/repositories/plugins.rs#L519): проверка ревизии, состояния и отвязки установок, удаление компонентов и списков, DELETE родителя. При этом прямой DELETE подтипа с существующим родителем блокируют [plugin_extension_delete и app_extension_delete](src/sql/m20261119_000010_database_rules/packages.sql#L233). При каскаде родитель уже удалён, поэтому дочерний триггер пропускает DELETE. Оставшиеся компоненты блокируют DELETE родителя в [package_parent_restrict](src/sql/m20261119_000010_database_rules/packages.sql#L45) и защищены своим `ON DELETE RESTRICT`. Успешное удаление не зависит от алфавитного порядка триггеров. Каталог подтвердил четыре CASCADE, один RESTRICT для компонентов, наличие трёх защитных триггеров и удаление старого cleanup. После правки в схеме 231 прикладной триггер, у FK 81 CASCADE и 37 RESTRICT, остальные действия из исходной описи сохранены.

### Публикация checkpoint

Изменены [plan_sealed_readiness и plan_sealed_delete](src/sql/m20261119_000010_database_rules/history.sql#L2368), [checkpoint_published_proof_reset](src/sql/m20261119_000010_database_rules/history.sql#L2651), [projection_sealed_insert/update/delete](src/sql/m20261119_000010_database_rules/history.sql#L2914), [context_sealed_identity](src/sql/m20261119_000010_database_rules/history.sql#L3010). В плановую проверку также включён descriptor, а перенос плана по operation_id проверяет обе операции.

Статический разбор для READ COMMITTED:

- UPDATE checkpoint уже владеет строкой checkpoint. Для публикации 0 → 1 триггер берёт `FOR SHARE` на context, затем `FOR NO KEY UPDATE` на операции. При двух операциях они берутся по возрастанию ID. Это сохраняет относительный порядок checkpoint → operation, используемый coverage, и конфликтует с manifest. Под блокировкой операции seal читает зависимости обычным SELECT, без обратного захвата строк plan или projection.
- План и проекция берут `FOR SHARE` на операции до отдельного запроса `proof_version`. При переносе они блокируют обе операции по ID и требуют видимую целевую операцию. UPDATE context уже держит ту же строку context, которую читает seal, и проверяет старого и нового owner. Если seal первым получил конфликтующую блокировку и зафиксировался, писатель после ожидания видит `proof_version = 1` и применяет запрет. Точный повтор INSERT проекции проверяется на свежем снимке после ожидания и допускает прежний `ON CONFLICT DO NOTHING`. DELETE допускает отсутствующую операцию при каскаде. После отката seal проверка выполняется по оставшемуся зафиксированному состоянию.
- Если первым зафиксировался писатель, seal после ожидания перечитывает workspace, готовность плана и текущую привязку. Для checkpoint с binding снижение ready, отсутствие готового плана или несовпадение текущего binding с готовым frozen header приводят к `23514`. Проверяются хеши, количество импортов, readiness, expiry и границы header. При откате писателя seal читает прежние данные.
- **Оставшаяся работа в PostgreSQL-порте CRUD.** Триггер проверяет текущую согласованность. Он не может узнать точную привязку, с которой вызывающий код уже подготовил доказательство, или отличить удалённую привязку от допустимого raw-origin без привязки. Поэтому в [NodePreparation::seal, checkpoint_proofs.rs:1467](../crud/src/repositories/checkpoint_proofs.rs#L1467) PostgreSQL-путь должен взять блокировки checkpoint → context → operation до окончательных `control`, `checkpoint_row`, проверки ожидаемого binding/header и `plan.ready`. После ожидания нужно заново сравнить их с подготовленным `self.scan` и удерживать блокировки до UPDATE и коммита. Без этого согласованная подмена binding, исчезновение binding или изменение плана с сохранением ready могут пройти текущую проверку, хотя доказательство готовилось для прежнего состояния. Эта правка в CRUD выходит за границы миграций. Дополнительно нужно проверить конкуренцию с изменением самого frozen header и обработку deadlock при каскадах.

### Штатная проверка

PostgreSQL 18.7 из поставки с pgvector 0.8.7. Новый каталог `/tmp/pioneer-pg-baseline.RSdWXF/pgdata-independent-fixes`, до первого запуска 0 объектов в `public`. Команды из фактического прогона:

```sh
T=/tmp/pioneer-pg-baseline.RSdWXF
PGBIN=/Users/alexander/Code/pioneer/pioneer-pg/classic/vendor/runtime/bin
url=postgresql://pioneer_review@127.0.0.1:55489/pioneer_v1_independent_fixes
for step in up-1 up-2 reset reapply; do
  case "$step" in reset) action=reset;; *) action=up;; esac
  CARGO_NET_OFFLINE=true CARGO_TARGET_DIR="$T/target" cargo run --locked \
    --manifest-path "$T/new/Cargo.toml" -p pioneer-migration -- -u "$url" "$action" \
    >"$T/independent-fixes-$step.log" 2>&1
  case "$step" in
    up-2|reapply) "$PGBIN/pg_dump" --schema-only --restrict-key=PioneerSchemaReview \
      "$url" >"$T/independent-fixes-$step.sql";;
  esac
done
cmp "$T/independent-fixes-up-2.sql" "$T/independent-fixes-reapply.sql"
shasum -a 256 "$T/independent-fixes-up-2.sql" "$T/independent-fixes-reapply.sql"
```

- Первый up: 10 шагов, последняя строка `Migration 'm20261119_000010_database_rules' has been applied`.
- Повторный up: `Applying all pending migrations`, `No pending migrations`.
- Reset: 10 шагов откатились, последняя строка `Migration 'm20261119_000001_identity' has been rolled back`. В `public` осталось 0 объектов, `vector` сохранил версию 0.8.7.
- Повторная установка: 10 шагов, последняя строка `Migration 'm20261119_000010_database_rules' has been applied`. `cmp` завершился с кодом 0. SHA-256 обоих дампов `59f9b89d317df981a60ab776da42577df9adaeb47e8a6303e874122d54676f07`.
- `pg_get_functiondef` и `pg_get_triggerdef` подтвердили определения 11 затронутых или проверяемых правил, `pg_get_constraintdef` — пять FK пакетов. Журнал `independent-fixes-catalog.log` содержит определения FK и результат `PQprepare: html_preview_header_update upsert COMMAND_OK`.

Удаление пакетов и оба порядка закрытия разобраны по коду. Прикладной DML, конкурентные сценарии, тесты и приложение не запускались.

## Готовность плана для raw origin и frozen origin

В [checkpoint_proofs.rs:175](../crud/src/repositories/checkpoint_proofs.rs#L175) `OriginScan::new` выбирает frozen header при наличии `manifest_id`, иначе переходит в `Phase::Raw`. В [NodePreparation::seal:1507](../crud/src/repositories/checkpoint_proofs.rs#L1507) проверки точной привязки, header и `plan.ready = 1` находятся внутри `if let Some(header)`. В [compaction_runner.rs:495](../crud/src/repositories/compaction_runner.rs#L495) INSERT создаёт план без указания ready, с дефолтом 0, и использует `ON CONFLICT (operation_id) DO NOTHING`. После этого CRUD сверяет ключ, счётчики и descriptor. Отдельный [UPDATE готовности](../crud/src/repositories/compaction_runner.rs#L1022) устанавливает ready в 1.

В [checkpoint_published_proof_reset](src/sql/m20261119_000010_database_rules/history.sql#L2651) удалён самостоятельный запрет на seal при `ready <> 1`. Проверка готовности осталась внутри запроса привязки проекции. Если привязка есть, отсутствие плана даёт NULL из LEFT JOIN, и условие `IS NOT TRUE` отклоняет публикацию, как и `ready = 0`. Raw origin допускает seal независимо от наличия и готовности плана. Состав объектов и down сохранены.

Статический разбор при READ COMMITTED:

| Вид checkpoint | Сначала создание плана | Сначала публикация |
| --- | --- | --- |
| Raw origin | План с ready = 0 допускает seal. Без плана seal также допустим. | Последующий INSERT с ready = 0 допустим. Точный повтор пользуется уникальным ключом, `DO NOTHING` и проверкой CRUD. Прежние запреты UPDATE и DELETE плана после seal действуют. Добавление новой привязки после seal блокирует `projection_sealed_insert`. |
| Frozen origin с привязкой | Созданного плана с ready = 0 пока недостаточно. Если UPDATE готовности удерживает `FOR SHARE` на операции, seal ждёт с `FOR NO KEY UPDATE`, затем читает готовность отдельно на свежем снимке. После коммита готового плана seal может пройти остальные проверки. При откате UPDATE readiness в расчёт идёт оставшееся состояние. | Успешный seal уже требует существующего готового плана. Попытка публикации до создания плана отклоняется. После успешного seal UPDATE и DELETE не могут убрать готовый план или вывести его из готового состояния. Повторный INSERT с ready по умолчанию оставляет существующую строку через `DO NOTHING`. |

Обычный INSERT плана может идти параллельно с seal. Для frozen origin невидимый план считается отсутствующим на снимке проверки, а видимый неготовый план отклоняется. Для raw origin готовность плана не участвует в решении. Защита INSERT отдельным триггером здесь избыточна. Сверка с подготовленным `self.scan` остаётся задачей PostgreSQL-порта CRUD.

### Фактический прогон

Новая изолированная база `pioneer_v1_origin_plan`, PostgreSQL 18.7 и pgvector 0.8.7. До запуска в `public` было 0 объектов. Команды:

```sh
T=/tmp/pioneer-pg-baseline.RSdWXF
PGBIN=/Users/alexander/Code/pioneer/pioneer-pg/classic/vendor/runtime/bin
url=postgresql://pioneer_review@127.0.0.1:55490/pioneer_v1_origin_plan
for step in up-1 up-2 reset reapply; do
  case "$step" in reset) action=reset;; *) action=up;; esac
  CARGO_NET_OFFLINE=true CARGO_TARGET_DIR="$T/target" cargo run --locked \
    --manifest-path "$T/new/Cargo.toml" -p pioneer-migration -- -u "$url" "$action" \
    >"$T/origin-plan-$step.log" 2>&1
  case "$step" in
    up-2|reapply) "$PGBIN/pg_dump" --schema-only --restrict-key=PioneerSchemaReview \
      "$url" >"$T/origin-plan-$step.sql";;
  esac
done
cmp "$T/origin-plan-up-2.sql" "$T/origin-plan-reapply.sql"
```

Первый и последний up применили по 10 миграций, последняя строка в обоих — `Migration 'm20261119_000010_database_rules' has been applied`. Второй up вывел `Applying all pending migrations` и `No pending migrations`. Reset откатил 10 миграций, закончив на `Migration 'm20261119_000001_identity' has been rolled back`. После reset в `public` осталось 0 объектов, версия vector осталась 0.8.7. Дампы совпали побайтно, SHA-256 обоих `1476d3b3f0e7db595ce3ba2cd0bc424381bad680e5d55aee6c1007a46c32109f`.

Каталог подтвердил тела функций и привязки `checkpoint_published_proof_reset`, `plan_sealed_readiness`, `plan_sealed_delete` и `projection_sealed_insert`, а также отсутствие пользовательского INSERT-триггера на плане. Журналы `origin-plan-*.log` находятся в `/tmp/pioneer-pg-baseline.RSdWXF`. Порядки записи разобраны по исходникам. Приложение, тесты, прикладной DML и конкурентные сценарии не запускались.

## Номера строк frozen sweep

[history.sql:4454](src/sql/m20261119_000010_database_rules/history.sql#L4454) переносит проверку нового `work_row` в существующий BEFORE INSERT и сначала ищет пару `(manifest_id, kind)`. Удалено правило `compaction_frozen_sweep_work_row_insert` и его откат в [drop_rules.sql](src/sql/m20261119_000010_database_rules/drop_rules.sql). Запреты DELETE, смены пары и смены выданного номера сохранены.

В `m20261018_000001_frozen_sweep_work.rs` из исходного HEAD SQLite проверял максимум построчно только для фактического создания work-строки. [bootstrap_step](../crud/src/repositories/frozen_sweep_work.rs#L30) вставляет пару без номера с `ON CONFLICT DO NOTHING`, а [fanout_step](../crud/src/repositories/frozen_sweep_work.rs#L217) переоткрывает существующую work-строку через DO UPDATE, присваивая revision, pending и retry_at. Оба сохраняют номер при повторе. Fanout ограничивает обход consumer-строк снимком их максимального номера.

### Статический разбор

- **Пакет новых строк.** Каждый BEFORE INSERT видит ранее вставленные строки того же оператора. При исходной границе 7 две новые автоматически нумеруемые пары получают 8 и 9 в порядке обработки строк. Явный номер новой work-пары должен превышать видимую границу. Проверки по общему максимуму после вставки пакета больше нет.
- **Повтор существующей пары и смешанный пакет.** Для существующей пары без указанного номера BEFORE INSERT подставляет текущий номер и передаёт управление обычному разрешению конфликта. DO UPDATE и DO NOTHING не расходуют очередной номер. При последовательности «новая, существующая, новая» и границе 7 новые строки получают 8 и 9, ранее выданный номер существующей остаётся прежним. Попытка UPDATE поменять номер отклоняется. Ограничения PostgreSQL для повторного UPDATE одной и той же конфликтующей строки в одном операторе продолжают действовать.
- **Исчерпание.** Поиск существующей пары выполняется до расчёта следующего номера, поэтому при максимуме bigint допустимый UPSERT проходит в свою конфликтную ветку. Для новой автоматически нумеруемой пары проверка максимума происходит до сложения и возвращает `23514`. Явная вставка новой work-строки с номером позади или на границе также возвращает `23514`. Если новая строка в смешанном пакете не получает номер, весь оператор откатывается, включая уже выполненные ветки UPDATE.
- **Два конкурентных писателя.** Общий `pg_advisory_xact_lock(1885955182, 2)` удерживается до конца транзакции. В READ COMMITTED поиск пары и чтение максимума идут отдельными операторами после ожидания. После коммита первого второй видит его строку и номер, после отката видит прежнюю границу. Номер более поздней успешной выдачи не обгоняет незакоммиченную раннюю выдачу.

[Выдача consumer_row](src/sql/m20261119_000010_database_rules/history.sql#L4493) использует ту же последовательность для автоматических номеров. Исходная SQLite-схема запрещает менять выданный consumer_row, но отдельного запрета на явный номер позади максимума для consumer нет. Явный consumer_row сохраняет прежние проверки PK и положительного значения. Автоматическое создание новой consumer-пары при исчерпании завершается ошибкой, повтор существующей пары передаётся в конфликтную ветку.

### Проверка PostgreSQL

До первого запуска в новой изолированной базе `pioneer_v1_sweep_row` было 0 объектов в `public`, доступен pgvector 0.8.7. Команды штатного прогона:

```sh
T=/tmp/pioneer-pg-baseline.RSdWXF
PGBIN=/Users/alexander/Code/pioneer/pioneer-pg/classic/vendor/runtime/bin
url=postgresql://pioneer_review@127.0.0.1:55491/pioneer_v1_sweep_row
for step in up-1 up-2 reset reapply; do
  case "$step" in reset) action=reset;; *) action=up;; esac
  CARGO_NET_OFFLINE=true CARGO_TARGET_DIR="$T/target" cargo run --locked \
    --manifest-path "$T/new/Cargo.toml" -p pioneer-migration -- -u "$url" "$action" \
    >"$T/sweep-row-$step.log" 2>&1
  case "$step" in
    up-2|reapply) "$PGBIN/pg_dump" --schema-only --restrict-key=PioneerSchemaReview \
      "$url" >"$T/sweep-row-$step.sql";;
  esac
done
cmp "$T/sweep-row-up-2.sql" "$T/sweep-row-reapply.sql"
shasum -a 256 "$T/sweep-row-up-2.sql" "$T/sweep-row-reapply.sql"
```

Первый и последний up применили по 10 миграций и завершились строкой `Migration 'm20261119_000010_database_rules' has been applied`. Второй up вывел `Applying all pending migrations` и `No pending migrations`. Reset откатил 10 миграций, завершился строкой `Migration 'm20261119_000001_identity' has been rolled back`. После reset в `public` осталось 0 объектов, расширение vector сохранилось. Дампы совпали побайтно, SHA-256 обоих `8ff1700e17b60731ce1f40a2773e7b72cb621e2ccf8c6bf33bd5a363ebc3fad6`.

`pg_get_functiondef` и `pg_get_triggerdef` подтвердили восемь функций и триггеров выдачи, неизменяемости и удержания строк. Каталог подтвердил отсутствие прежнего AFTER INSERT. Журналы `sweep-row-*.log` находятся в указанном временном каталоге. Поведение вставок и конкуренция писателей разобраны статически. Приложение, тесты, прикладной DML и конкурентные сценарии не запускались.

## Collation текстовых колонок

`text()` и `text_null()` в восьми доменных миграциях берутся из SeaORM. Локальные обёртки из `src/lib.rs` удалены. Явная `C` осталась на 16 колонках. Существующие регулярные выражения для ASCII-алфавитов сохраняют `C` на самом предикате, включая actor guards в `src/sql/m20261119_000010_database_rules/execution.sql`. Это сохраняет интерпретацию диапазонов, не назначая collation колонке.

| Колонка с `COLLATE "C"` | Зависимость от порядка | Код |
| --- | --- | --- |
| `thread.id` | При равном updated_at SQL для обычного участника ограничивает выдачу по ID. После объединения с тредами из памяти Gateway применяет тот же порядок Rust Ord и обрезает выдачу. При лимите 500 расхождение меняет состав результата. | [CRUD](../crud/src/repositories/membership.rs#L297), [лимит Gateway](../gateway/src/message/thread_handlers.rs#L655), [слияние](../gateway/src/message/thread_handlers.rs#L1182) |
| `turn.id` | SQL ограничивает страницу по ID, Gateway проверяет продвижение через `last.id > after`. | [CRUD](../crud/src/repositories/compaction_history.rs#L2865), [Gateway](../gateway/src/compaction/history.rs#L2440) |
| `applied_patch_change_index.environment_id`, `applied_patch_change_index.turn_id` | Две ограниченные SQL-выборки по source/destination объединяются, сортируются и обрезаются в Rust. Несовпадение порядков отбрасывает нужные строки до объединения. | [CRUD](../crud/src/repositories/patch_history.rs#L1742), [слияние](../tools/src/apply_patch/history/db_store.rs#L1772) |
| `task_run.id` | SQL использует ID при равном времени завершения. Клиент проверяет продвижение составного курсора и сортирует уведомления по тому же ключу. | [CRUD](../crud/src/repositories/task_completion_inbox.rs#L751), [клиент](../client/src/tasks/notifications.rs#L1516) |
| `thread_timeline_block.sort_key`, `thread_read_cursor.last_read_sort_key` | SQL использует ключ блока как позицию страницы и сравнивает его с нижней границей чтения. Максимумы блока и курсора сравниваются с новым ключом в Rust; клиент проверяет monotonic read position и сортирует страницы. | [SQL](../crud/src/repositories/thread_timeline_projection.rs#L515), [MAX](../crud/src/repositories/unread_start.rs#L56), [клиент](../client/src/workspaces/directory.rs#L471) |
| `turn_work_item_projection.order_key` | SQL выбирает страницу до или после order_key, клиент сливает страницы по Rust Ord. | [CRUD](../crud/src/repositories/thread_timeline_projection.rs#L1131), [клиент](../client/src/timeline/semantic.rs#L4427) |
| `patch_commit_intent.turn_id`, `patch_commit_terminal.turn_id`, `applied_patch_record.turn_id` | SQL ограничивает две страницы статусов; Rust сливает их и обрезает по `(turn_id, ordinal)`. Затем Rust сливает страницы статусов со страницами записей по тому же ключу. | [статусы](../crud/src/repositories/patch_history.rs#L1172), [записи](../crud/src/repositories/patch_history.rs#L1421), [слияние](../tools/src/apply_patch/history/db_store.rs#L1574) |
| `turn_cli_runtime_attempt.turn_id` | SQL отдаёт ограниченную страницу по `(turn_id, attempt_index)`; при следующем обходе Rust пропускает группы `turn < last_turn`. При разных порядках группа может быть пропущена или прочитана повторно. | [codex_retry_notice.rs:71](../crud/src/codex_retry_notice.rs#L71) |
| `task_delivery.delivered_turn_id` | SQL выбирает следующий ID через `ORDER BY … LIMIT 1` в пределах turn fence; Rust требует `active_turn > after_turn`. | [SQL](../crud/src/repositories/compaction_task_output.rs#L828), [валидация](../crud/src/repositories/compaction_task_output.rs#L918) |
| `compaction_coverage.source_scope`, `compaction_coverage.source_id`, `compaction_coverage.source_version` | SQL ограничивает страницу по тройке; наблюдатель topology сравнивает тот же `SourceRef` через Rust Ord с последним ключом и множеством уже прочитанной coverage. | [страница](../crud/src/repositories/compaction.rs#L1921), [потребитель](../crud/src/repositories/compaction.rs#L1844) |

`unread_message_position.sort_key` читается по составному PK, затем строка передаётся в timeline block. В unread-запросе publication служит для проверки наличия записи и времени публикации; диапазон применяется к `block.sort_key`. Поэтому хранилище позиции наследует collation базы, а индекс сравнения блока остаётся на `C`. SQL MIN/MAX по thread_id при восстановлении projection сравниваются в Rust с каноническим ID по равенству. Две крайние строки нужны там для проверки единственности, а их относительный порядок не используется. SQL-only keyset с курсором, взятым из последней строки и возвращённым в тот же SQL, сам по себе не требует исключения.

Для PostgreSQL-порта CRUD в [list_threads_by_workspace](../crud/src/repositories/thread.rs#L724) потребуется добавить `id ASC` после `updated_at DESC` и перед LIMIT. Сейчас путь с полным доступом ограничивает результат только по времени.

### Проверка каталога и воспроизводимости

В новой изолированной базе `pioneer_v1_thread_collation` было 0 объектов в `public`. Кластер инициализирован с `--locale=en_US.UTF-8 --encoding=UTF8`, установлен pgvector 0.8.7. Команды:

```sh
T=/tmp/pioneer-pg-baseline.RSdWXF
PGBIN=/Users/alexander/Code/pioneer/pioneer-pg/classic/vendor/runtime/bin
url=postgresql://pioneer_review@127.0.0.1:55493/pioneer_v1_thread_collation
for step in up-1 up-2 reset reapply; do
  case "$step" in reset) action=reset;; *) action=up;; esac
  CARGO_NET_OFFLINE=true CARGO_TARGET_DIR="$T/target" cargo run --locked \
    --manifest-path "$T/new/Cargo.toml" -p pioneer-migration -- -u "$url" "$action" \
    >"$T/thread-collation-$step.log" 2>&1
  case "$step" in
    up-2|reapply) "$PGBIN/pg_dump" --schema-only --restrict-key=PioneerSchemaReview \
      "$url" >"$T/thread-collation-$step.sql";;
  esac
done
cmp "$T/thread-collation-up-2.sql" "$T/thread-collation-reapply.sql"
```

Первый и последний up применили 10 миграций, второй сообщил `No pending migrations`, reset откатил 10 шагов. Дампы совпали побайтно, SHA-256 `897c08463cd1bbc7ada82a5e9a500f38568047a2b308336f7cf338f6716e41e3`. `pg_attribute.attcollation` в соединении только для чтения подтвердил прежние 15 исключений плюс `thread.id` (16 всего) и 1616 текстовых колонок с `default`, включая `workspace.name`, `gateway_principal.display_name`, `gateway_principal.nickname` и `unread_message_position.sort_key`. Локаль базы `en_US.UTF-8`. Сверка использовала OID назначения колонок и подтвердила `PRIMARY KEY (id)` у `thread`. Фактический список находится в `thread-collation-catalog.log`. Проверка порядка потребителей была статической; приложение, DML и тесты не запускались.

## Конкурентная запись frozen layout и ссылок

### Публикация диапазонов и ревизия проверки

[StagedCopyCheck::step](../crud/src/repositories/compaction_frozen_storage.rs#L1070) постранично проверяет непрерывность и точное соответствие префикса базе, включая `source_manifest`. При замене `[0,10) → A` на `[0,10) → B` совпадение границ ничего не говорит о результате этой проверки. Представления читают физические данные по текущему `source_manifest`. [Frozen views](src/sql/m20261119_000010_database_rules/views.sql#L123).

Для сверки используется существующий `compaction_frozen_sweep_consumer.revision` по `(manifest_id, kind)`. Триггеры INSERT и DELETE span увеличивают ревизию владельца, UPDATE учитывает границы и источник и увеличивает ревизии старой и новой пар при переносе. Изменения заголовка и состояния layout тоже могут инвалидировать проверку. Строки ревизий сохраняются при удалении и повторном создании заголовка, счётчик не уменьшается. Обычный fanout меняет курсор и pending без сброса revision. [Триггеры span](src/sql/m20261119_000010_database_rules/history.sql#L3935), [защита ревизии](src/sql/m20261119_000010_database_rules/history.sql#L3328), [fanout](../crud/src/repositories/frozen_sweep_work.rs#L337).

В `compaction_frozen_layout` добавлен nullable `verified_span_revision`. При включении `active` триггер сравнивает переданное значение с текущей ревизией и очищает поле. При переходе header к `ready = 1` триггер проверяет метки активных layout. Проверка обращается к первичному ключу layout и уникальному ключу ревизий. У заголовка максимум две пары, поскольку `kind` ограничен значениями 0 и 1. `frozen_span_covers` удалена. Ни одна финальная проверка в схеме не читает строки span. Блокировку заголовка также берут изменения и удаление layout, чтобы проверка не пропустила смену layout или базы. [Объявление колонки](src/m20261119_000007_history_storage.rs#L740), [проверки ревизии](src/sql/m20261119_000010_database_rules/history.sql#L2238), [откат](src/sql/m20261119_000010_database_rules/drop_rules.sql#L282).

#### Контракт PostgreSQL-порта CRUD

1. Перед первым шагом `StagedCopyCheck` захватить состояние staged layout и его ревизию, а также ревизии обоих видов для candidate, поскольку `LayoutScan` проверяет оба. Снимать ревизии до чтения проверяемых страниц. Передавать результат вместе с ревизиями из [prepare](../crud/src/repositories/compaction_frozen_storage.rs#L303) и [maintenance](../crud/src/repositories/compaction_frozen_storage.rs#L691) в [publish](../crud/src/repositories/compaction_frozen_storage.rs#L322). Несовпадение ревизии при проверке или при публикации означает перезапуск проверки с новой парой состояние/ревизия. Помечать staged copy повреждённой можно после подтверждения ошибки на актуальной ревизии.
2. В финальной транзакции `publish`, перед нынешними чтениями header/layout и **до** `exact_span_insert`, заблокировать header цели и candidate по ID в одном порядке (`ORDER BY id FOR NO KEY UPDATE`). Следующими запросами перечитать зависимости, состояние копирования и ревизии. При несовпадении откатить транзакцию и повторить постраничную проверку. При совпадении добавить штатный хвост `[prefix,count)` из собственного manifest, если это нужно для готового header. После отдельного INSERT прочитать свою новую ревизию под той же блокировкой и передать её в `SET active = 1, verified_span_revision = …`. Без хвоста передать исходную сверенную ревизию. Точный повтор `ON CONFLICT DO NOTHING` не запускает AFTER INSERT и сохраняет ревизию. Префикс и хвост проверяются разными шагами. Подстановка свежей ревизии до сопоставления с проверенной позволила бы принять устаревший префикс.
3. Для [compaction_finish_frozen_history](../crud/src/repositories/compaction_frozen.rs#L257) после последнего ограниченного append и до первой страницы снять ревизии цели по обоим видам и всех зависимостей, используемых проверкой. Постранично проверить полное покрытие активных layout по счётчикам заголовка и совпадение `source_manifest` скопированного префикса с соответствующими диапазонами candidate. При сравнении префикса обрезать конец диапазона с обеих сторон по границе префикса: штатный append может продлить последний скопированный диапазон за эту границу. В короткой транзакции заблокировать заголовки цели и зависимостей по ID, перечитать состояние и сверить все снятые ревизии, включая пары цели без активного layout. При изменении ревизии или набора зависимостей откатить транзакцию и повторить постраничную проверку. Иначе записать проверенную ревизию в каждую активную строку layout и отдельным UPDATE выставить `ready = 1` с существующими условиями CAS. Запись метки не меняет sweep-ревизию.

#### Очерёдность транзакций, статический разбор

- **Span первым захватил заголовок.** При COMMIT замена A на B, вставка, удаление или перенос меняют ревизию в той же транзакции. Ожидающая публикация читает её после получения блокировки и откатывается до добавления хвоста. При ROLLBACK ревизия и диапазоны возвращаются к исходному состоянию, поэтому ранее проверенный результат остаётся применим при совпадении остальных условий.
- **Публикация первой захватила заголовок.** При COMMIT ожидающая мутация span перечитывает header и layout. Когда пара уже `active = 1` и `ready = 1`, срабатывает защита опубликованных диапазонов и сохраняется допуск точного повтора INSERT. У активной пары с `ready = 0` разрешённые изменения, включая append, создают новую ревизию для последующей проверки ready. При ROLLBACK мутация использует прежнее состояние пары, а возможный хвост из транзакции публикации откатывается.
- При переносе span блокируются оба заголовка по ID, проверяются точные пары OLD/NEW. При отсутствии layout запись span по-прежнему сериализуется на header. После удаления header дочерний каскад может пройти при отсутствующем родителе. Внешние ключи владельцев сохраняют `ON DELETE CASCADE`, ссылка на физический `source_manifest` сохраняет `NO ACTION`. [Правила span](src/sql/m20261119_000010_database_rules/history.sql#L2363), [FK](src/m20261119_000009_relations.rs#L1044).

### Допуск ссылок и контракт PostgreSQL-порта CRUD

По [root_predicate](../crud/src/repositories/frozen_storage_lifetime.rs#L813) проверены ссылки из `compaction_task_output` и четыре пути snapshot/binding. `compaction_task_output_available_*` и `compaction_operation_projection_available_*` берут `FOR SHARE` на доступном header. То же делают `*_verified_{insert,update}` для `task_run_conversation_snapshot`, `turn_runtime_snapshot`, `thread_cli_runtime_binding` и `turn_cli_runtime_binding`. `task_frozen_roots_{insert,update}` блокируют заголовки при повторном включении удержания снимков задачей. `checkpoint_published_proof_reset` блокирует заголовок привязанной проекции под блокировкой operation, затем проверяет доступность заново. Layout берёт более сильную блокировку владельца и кандидата перед проверкой доступности. [Проверки истории](src/sql/m20261119_000010_database_rules/history.sql#L2181), [проверки snapshot/binding](src/sql/m20261119_000010_database_rules/execution.sql#L539), [публикация checkpoint](src/sql/m20261119_000010_database_rules/history.sql#L2755), [удержание Task](src/sql/m20261119_000010_database_rules/history.sql#L3163).

- **Expiry первым.** Запись, которая нашла доступную прежнюю версию заголовка, ждёт конфликтующую блокировку. После COMMIT PostgreSQL под `READ COMMITTED` повторно применяет `ready/expired` к версии строки, возвращаемой блокирующим чтением. Ссылка на истёкший заголовок отклоняется. У путей с отдельно взятой блокировкой это делает следующий запрос. После ROLLBACK запись может пройти по прежнему доступному заголовку.
- **Ссылка первой.** Для полной гарантии порт [expire](../crud/src/repositories/frozen_storage_lifetime.rs#L831) должен заменить начальный `find_by_id` после `begin()` чтением строки по ID с `FOR NO KEY UPDATE`. После получения блокировки проверяются возвращённый header и затем отдельными запросами [root_predicate](../crud/src/repositories/frozen_storage_lifetime.rs#L852), активные проекции, незакрытые опубликованные checkpoint и переходные layout. Блокировка должна оставаться до коммита или отката, включая [UPDATE expired](../crud/src/repositories/frozen_storage_lifetime.rs#L1019). Тогда при COMMIT первого писателя expiry видит новую ссылку и прекращает очистку, если ссылка удерживает header по этим правилам. При ROLLBACK первого писателя expiry использует прежнее состояние и может продолжить при отсутствии удержания. Блокировка только на финальном UPDATE опаздывает относительно проверок.
- Порту также нужно согласовать переходы, которые меняют удержание без INSERT/UPDATE самой ссылки. В частности, активация и возобновление runner для существующей проекции должны брать блокировку header до повторной проверки доступности и изменения состояния. При включении Task и параллельной записи snapshot нужен согласованный порядок на задаче, чтобы одновременно добавляемый snapshot учитывался при проверке. Для неизвестных roots на уровне workspace требуется согласование на уровне workspace. В транзакциях с несколькими объектами порт должен соблюдать порядок блокировок и повторять транзакцию при deadlock. См. [runner activation](../crud/src/repositories/compaction_runner.rs#L854), [runner resume](../crud/src/repositories/compaction_runner.rs#L1224) и [unknown_predicate](../crud/src/repositories/frozen_storage_lifetime.rs#L799).

### Проверка PostgreSQL

На новой базе `pioneer_v1_layout_revision` (PostgreSQL 18.7, `en_US.UTF-8`) предварительное чтение каталога показало 0 отношений в `public` и доступный pgvector 0.8.7. Для сервера использован комплект `classic/pgvector-test/vendor/runtime`. Предварительная попытка с обычным `classic/vendor/runtime` остановилась на миграции памяти из-за отсутствия `vector`. После неё поднят новый кластер с pgvector. Временный Cargo workspace использовал исходники мигратора из worktree через symlink.

```sh
T=/tmp/pioneer-pg-baseline.RSdWXF
PGBIN=/Users/alexander/Code/pioneer/pioneer-pg/classic/vendor/runtime/bin
url=postgresql://pioneer_review@127.0.0.1:55495/pioneer_v1_layout_revision
for step in up-1 up-2 reset reapply; do
  case "$step" in reset) action=reset;; *) action=up;; esac
  CARGO_NET_OFFLINE=true CARGO_TARGET_DIR="$T/target" cargo run --locked \
    --manifest-path "$T/new/Cargo.toml" -p pioneer-migration -- -u "$url" "$action" \
    >"$T/layout-revision-$step.log" 2>&1
  case "$step" in
    up-2|reapply) "$PGBIN/pg_dump" --schema-only --restrict-key=PioneerSchemaReview \
      "$url" >"$T/layout-revision-$step.sql";;
  esac
done
cmp "$T/layout-revision-up-2.sql" "$T/layout-revision-reapply.sql"
"$T/py/bin/python" "$T/layout-revision-catalog.py"
```

| Команда | Фактический результат |
| --- | --- |
| `up` | 10 строк `has been applied`, последняя — `Migration 'm20261119_000010_database_rules' has been applied` |
| повторный `up` | `No pending migrations` |
| `reset` | 10 строк `has been rolled back`, последняя — `Migration 'm20261119_000001_identity' has been rolled back` |
| `up` после отката | 10 строк `has been applied`, последняя — `Migration 'm20261119_000010_database_rules' has been applied` |

Дампы совпали побайтно, SHA-256 `3c4acf136012139844c3519f89f7de51cd8e24f607b0fe1f7fbe5113eaffb98d`. В read-only соединении `pg_get_functiondef` и `pg_get_triggerdef` подтвердили совпадение 18 триггеров и их функций с исходниками и совпадение функции `frozen_layout_revision_matches`. Каталог подтвердил отсутствие `frozen_span_covers`, колонку `verified_span_revision bigint NULL`, `CHECK` для kind, индексы и три FK. Журналы и дампы находятся в `$T/layout-revision-*`. Порядок блокировок, COMMIT и ROLLBACK разобраны статически. Приложение, тесты, прикладной DML и конкурентные сценарии не запускались. Журналы предыдущей проверки блокировок expiry сохранены в `$T/frozen-races-*`.

## Следующий этап приложения и точки ревью

1. **Подключение и завершение транзакций.** `crates/gateway/src/database/connection.rs` и `crates/sqlite/src/{database,writer}.rs` открывают SQLite, держат writer lease и устанавливают PRAGMA. Нужны PostgreSQL-адаптер, выбор изоляции, привязка сигналов к коммиту и обработка deadlock/serialization retry. Проверить переходы с перекрёстными инвариантами, особенно proof sealing, frozen expiry, package scope и смену директории поиска. Упорядочивающие блокировки в схеме рассчитаны на `READ COMMITTED`. Снимки и границы захвата при других уровнях изоляции требуют ревью вызывающих транзакций.
2. **Удаления и авторизация.** SQLite Gateway использует `foreign_keys=OFF`. С PostgreSQL проверки и каскады выполняются автоматически. Сверить порядок явной очистки и FK RESTRICT/CASCADE в CRUD `thread.rs`, `identity.rs`, `membership.rs`, удалении workspace и frozen/history. Проверить порядок каскадных и пользовательских триггеров.
3. **SQLite SQL и version-probes.** `crates/crud/src/repositories/{agent_domain,task_completion_inbox,projection_receipt_cleanup,task_delivery_recovery}.rs` используют SQLite placeholders, `INDEXED BY`, функции дат и сравнения с типизированными флагами. `task_occurrence_producers.rs` и `task_occurrence_compatibility.rs` читают старый номер миграции и `sqlite_master`. Для 1.0.0 нужен прямой путь актуальных producers без 0.x introspection.
4. **Payload и история.** `crates/crud/src/repositories/{portable_payload,thread_history_read,compaction_preparation,checkpoint_proofs,frozen_storage_lifetime,frozen_sweep_work,turn}.rs` используют физический `rowid`, SQLite incremental BLOB и отдельные backing tables. Перевести обращения на стабильные ключи, `insertion_ordinal` и транзакционный watermark там, где курсор должен учитывать конкурентные коммиты. Удалить sqlite-zstd startup/maintenance из PostgreSQL-пути.
5. **Поиск и Entity.** `crates/crud/src/repositories/{agent_memory,thread_episodic_search,thread_episodic}.rs` требуют SQL для PostgreSQL FTS и pgvector, генерации сниппетов и сохранения порядка fusion/rank. `crates/crud/src/thread_episodic.rs` кодирует `Vec<f32>` в SQLite bytes, а на PostgreSQL должен связывать тип `vector`. Entity надо повторно сгенерировать из штатной PostgreSQL-схемы и проверить `timestamptz`, identity, read-only view projections, custom `NullableToolItemId` и маппинг vector.
6. **Начальные данные и повторная инициализация.** `crates/gateway/src/bootstrap.rs` уже пытается вставить default workspace; PostgreSQL-путь должен трактовать существующую запись как штатный результат. Сверить bootstrap маркеры исторической discovery в `crates/gateway/src/database/startup/{task_child_work_seed,html_preview_io,memory_storage_reset}.rs` с обновлёнными runtime producers.

Изменены `Cargo.lock`, `crates/migration/{Cargo.toml,README.md,POSTGRESQL_BASELINE.md}`, `src/{lib.rs,main.rs}`, десять модулей начальной схемы и SQL-файлы `src/sql/m20261119_000010_database_rules`. Исходные SQLite-модули удалены из крейта. Для отдельного ревью приоритетны [relations](src/m20261119_000009_relations.rs), [frozen/history DDL](src/sql/m20261119_000010_database_rules/history.sql), [payload rules](src/sql/m20261119_000010_database_rules/payloads.sql), [package rules](src/sql/m20261119_000010_database_rules/packages.sql) и [схема памяти](src/m20261119_000008_memory.rs).

Проверка охватывает штатное применение, повторный запуск, каталог и откат. Gateway, Desktop, Pioneer, полный workspace, CI и тесты не запускались. Прикладное поведение и конкурентные сценарии остаются предметом последующего прогона.
