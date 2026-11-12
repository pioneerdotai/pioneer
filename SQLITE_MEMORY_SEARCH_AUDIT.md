# Повторная сверка исходного задания, 2026-11-08

Это аудит исходников, diff и тестового кода, а не отчёт об исполненных тестах.
Предыдущая формулировка полной готовности была преждевременной: повторная
проверка нашла два дефекта, перечисленных ниже. Они исправлены в worktree.

Обозначения: **С** — сверено по текущему коду; **Р** — регрессия написана или
адаптирована, но не запущена; **О** — исполнение отложено по прямому запрету.
С не означает успешную компиляцию, прохождение теста или измеренную нагрузку.

## Найдено и исправлено при этой повторной сверке

1. В agent_memory repository список ON CONFLICT UPDATE ссылался на удалённые
   Entity Column::CapsuleId/CapsuleRef/FrameId/FrameUri/FrameVersion. Это был
   дефект компиляции. Список заменён на PayloadVersion. Дополнительно сверены
   все прямые Column-ссылки пяти изменённых Entity с их полями.
2. Discovery группы старых duplicate jobs выполнялось под writer-транзакцией,
   несмотря на ограничение удаления 64 строками. Discovery вынесено на scoped
   reader до writer admission. В транзакции остались перепроверка checkpoint,
   свежий выбор сохраняемого пользовательского исхода и удаление до 64 peers
   конкретного source через существующий index_item_id index. Устаревший
   discovery безопасен: пустая группа — no-op и повторное discovery, курсор
   источников не продвигается. Нового owner/очереди нет.
3. Удалён пустой комментарий о capsule store из корневого Cargo.toml.

## 1. Замена существующей системы

**С.** Переписаны существующие MemoryBackend/MemoryService callers, episodic
executor, refill и Thread Search. Старые memvid.rs, capsule Entity/repository,
file search/cache/rotation удалены. Новые SQLite файлы являются адаптерами и
частями существующих repositories. Сервисов, worker pools, job engine,
канонической истории или импортера рядом не создано. Отдельные домены agent
memory и conversation search сохранены. См. memory/src/service.rs, sqlite.rs,
thread_episodic.rs и gateway/src/thread_episodic.rs.

## 2. Ветка, worktree, исходные материалы

**С.** refactor/sqlite-memory-search, .worktrees/sqlite-memory-search, HEAD
`d1e0a10d0ee715ad51f0fe9f63cab8789ad35727`. Исходный worktree и другие ветки
не изменялись; reset/clean/force checkout/prune не применялись. Root и scoped
AGENTS прочитаны; research/report использовались как карта, код проверен
самостоятельно. Специальной обработки кавычек в продуктовой логике нет.

## 3. Запрет исполнения

**С/О.** Ни тесты, ни сборки/check/clippy/nextest/benchmark, ни приложение,
мигратор, codegen или CI не запускались. Cargo использовался только для fmt.
Production DB, капсулы, модели и процессы не менялись. Entity согласованы вручную
в разрешённом исключении; перечень находится в checklist и разделе 8 ниже.

## 4. Общий контракт и адаптер

**С.** thread_episodic.rs/embedding provider и MemoryBackend оперируют доменными
условиями, Vec<f32>, identity и рангами; не требуют BLOB/MATCH/frame/segment.
Provider calls, jobs, access policy, RRF, disclosure и пользовательские cursors
остаются в общих владельцах. memory/src/thread_episodic/sqlite.rs и CRUD
repositories реализуют SQLite storage/query/pushdown. Эквивалентный общий RRF
порядок вычисляется SQL для ограниченной передачи страниц, возвращаются ранги,
публичная шкала вычисляется общей функцией. Адаптер не открывает соединений и не
коммитит item/job самостоятельно. Для pgvector нужны адаптер, repositories и
схема; реализации/stub/plugin registry/query framework для него нет.

## 5. Обычный текст и ранжирование

**С/Р.** sqlite/search/literal.rs использует встроенный unicode61 через FTS5 API;
экранирует каждый literal token и соединяет OR. Пользовательские кавычки,
скобки, двоеточия, дефисы и AND/OR/NOT не становятся операторами. Нет intent/
regex/словари/boosts/LLM rewrite/reranker. Лимиты parser budget явные: 64 KiB,
512 terms; пустой набор — пустая quoted phrase без MATCH parser error.
Conversation search объединяет lexical/semantic ранги равными весами k=60,
не сырые BM25/distance. Lexical продолжает работать без provider; отказ даёт
неполноту/диагностику, explicit Semantic — типизированную ошибку. Внутренние
режимы и временной фильтр сохранены. Отдельная память агентов сохраняет прежнюю
lexical-only backend функцию; новых embedding product features ей не добавлено.

## 6. Векторы и текст

**С/Р.** Артефакты остаются в thread_episodic_embedding_artifacts.vector_bytes,
items ссылаются через embedding_artifact_id. Проверяются dimension, finite,
ненулевой вектор и identity; cosine корректен и для ненормализованных векторов.
Нет vec0/int8/file index/rebuild после каждой записи/Rust corpus ranking.
items.search_text — только поисковая проекция; agent_memory_payloads.content —
полный новый payload. FTS5 external content/rowid/triggers дают инкрементальные
обновления и rollback с исходной записью. Item/text/artifact link и завершение
job публикуются атомарно. Каноническая история не дублируется как новый owner и
не распаковывается целиком при поиске.

## 7. Все три использования Memvid

**С/Р.** A: memory/gateway thread_episodic, search.rs, thread_search_tools
используют новый контракт/SQLite, file fanout отсутствует.
B: существующий MemoryService использует SqliteMemoryBackend put/get/search/
delete с полным content; ownership, quality, sensitivity, quarantine, lifecycle
и recall hooks остаются. C: LocalEmbeddingRuntime использует существующие ort/
tokenizers, четыре модели и прежние имена файлов. По старому официальному
text_embed.rs сверены 512 right pad/truncation, CLS первой output, L2 и
последовательный batch; preparation/instructions/chunk pooling остаются у
существующего provider. Численная эквивалентность на установленных моделях — О.

## 8. Удаление, зависимости, схемы

**С.** memvid-core отсутствует в manifests/lock/imports, эксклюзивные transitive
packages удалены. sqlite-vec 0.1.6 и checksum взяты из реального registry archive;
все lock dependency edges разрешаются однозначно. Удалены file-only поля,
repositories, Entity, storage settings, native flags/score diagnostics;
protocol consumers и checked-in JSON согласованы. Сохранены thread directory,
affinity, канонические данные, бизнес- и audit metadata. Остаточные старые
строки — historical DDL, одноразовое retirement старых jobs, чтение старого audit
reason и стабильные checkpoint/lock names, не старый backend.

Entity вручную: изменены agent_memory, agent_memory_repair_job,
thread_episodic_items, thread_episodic_index_jobs; добавлена agent_memory_payloads;
удалены agent_memory_capsule/thread_episodic_capsules, обновлены lib/prelude.
Исторические миграции не редактировались; новая миграция зарегистрирована
последней. Проверка штатной генерацией и повторным schema export — О.

## 9. Модели, очереди и переиндексация

**С/Р.** Существующие providers/settings/resolver и refill supervisor остаются
владельцами. Подготовка и encoding вне DB capacity; scoped handles передаются
вниз. Publication проверяет source payload/committed status/turn deletion/
exclusions, actual attempt и serialized target/reset marker внутри общей
транзакции. Смена модели заменяет job identity и очищает проекцию bounded
квантами с durable cursor; поздний callback не публикует старую модель. Сохранены
retry deadlines/budgets, unknown commit recovery, cancellation и restart.
Основные точки: complete_index_attempt, complete_successful_index,
persist_successful_refill_item, reset_thread_episodic_projection.

## 10. Старые данные

**С/Р.** Импорта, dual read/write и fallback нет. DDL устанавливает схему и seed,
не выполняет исторический backfill. Native DROP COLUMN переписывает SQLite
metadata tables — стоимость установки не измерена. Agent reset использует
существующий startup maintenance, 64-row quanta, durable cursor, повторную
проверку отсутствия нового payload. FilesPending/Complete различаются.
Episodic refill восстанавливает из canonical history. Очистка файлов ограничена
известными memory roots, regular .mv2, depth/quantum/cancellation; symlinks,
custom historical URI/paths, models и history не обходятся. Такие custom paths
могут сохранить инертные старые файлы. Реальная очистка при разработке не шла.

## 11. Runtime, нагрузка, ресурсные границы

**С/Р.** Все операции идут через SqliteDatabase/CrudStore/repositories.
Регистрация static auto-extensions перед обоими существующими путями открытия
пулов; auto-extension покрывает replacements. Физический RO/query_only reader,
serialized writer и operation-scoped Interactive/Maintenance сохранены.
ACL/workspace/status/context/exclusions/embedding identity до rank/LIMIT;
canonical hash/secret/access снова проверяются перед disclosure. Batches,
ответы и retained preparation ограничены. Provider/FS/hash/encoding не добавлены
под writer; исправление duplicate discovery описано выше. Существующие WorkSignal,
unknown outcome и finite-pass/deadline владельцы сохранены, нового empty polling
нет. PostgreSQL/YSQL/ANN/distribution не реализованы. Throughput/idle CPU — О.

## 12. Поисковые контракты

**С/Р.** Source anchors/current titles, thread grouping, occurrence dedup, signed
neutral user cursor, bounded source validation, fragments/response byte limits
сохранены. Не загружаются все результаты ради страницы. Internal cursor — ordinal,
без SQLite/Memvid payload в пользовательском API. RRF scale обе ветки first=1,
одна first=0.5; default min_relevancy=0.25, ExactReference cutoff согласован.
Temporal mode сохраняет дату. Coverage отражает pending/failed/missing model,
provider failure и источник, отклонённый свежей проверкой. Delete/exclusion/
visibility/quarantine фильтры и fresh disclosure не отключены.

## 13. Процесс и самоаудит

**С.** Checklist ведётся в worktree, есть base/components/boundary/storage,
выполненные этапы, найденные ошибки и проверки. Проверены production callers,
Entity column references/exports, SQL bindings, публикация/reset/disclosure,
local inference и lock graph. Нет нового job engine/parallel storage path или
TODO/stub вместо обязательного поведения. Обнаруженные при повторной сверке
ошибки исправлены production-кодом. Утверждение «100% доказано» не делается:
компиляция и runtime намеренно не проверялись по разделу 3.

## 14. Тестовый код: матрица и основные владельцы

Все ниже — **Р/О**, не результаты запущенных тестов. Применён test-audit с
отдельными frozen-production фазами; production fixes делались после выхода
из этих фаз. В этой повторной сверке тесты не изменялись; существующий reset
regression покрывает >64 duplicate jobs, restart cursor и late callback.

| Требование | Основной код регрессий |
|---|---|
| insert/update/delete/rollback, полный agent payload | memory/tests/sqlite_backend.rs, sqlite_service.rs |
| atomic vector/FTS/job, отдельный reader | gateway thread_episodic::sqlite_projection_commits_text_vector_and_job_together_on_scoped_runtime |
| RO protection/Interactive/Maintenance | тот же physical-reader regression с runtime observers |
| human input без agent run | sql_search_pending через canonical UserMessage + действующий ingestor; existing ingestion tests |
| source enqueue/prepare/commit races | thread_episodic_executor_reconciles_source_changed_* |
| model/dimension/late result | sqlite_publication_rechecks_target_*, existing settings/model-reset tests |
| retries/restart/cancellation/poison/unknown outcome | existing episodic executor/runner/refill tests, episodic_model_reset_commits_cursor_* |
| ACL/exclusion/deletion/quarantine до лимита | thread_search_tools/tests.rs; sensitivity_and_quarantine_are_applied_before_the_candidate_limit |
| fresh disclosure | filtered_engine_pages_fill_results_and_disclosure_rechecks_source_and_acl |
| lexical/semantic/hybrid/cosine/RRF | sqlite_projection_commits_text_vector_and_job_together_on_scoped_runtime |
| no provider/provider failure | sqlite_common_retrieval_keeps_lexical_results_and_reports_embedding_failures; configured_provider_failure_* |
| quotes/operators/Unicode/punctuation/short/empty | sqlite_plain_text_queries_and_bounded_pages_use_actual_unicode_tokenizer |
| order/pagination/coverage/large cards | тот же query owner, sqlite_maximum_page_*, thread_search_tools/tests.rs |
| agent put/get/search/delete/policy/lifecycle | sqlite_backend.rs, sqlite_service.rs, existing memory_service.rs |
| local embedding preparation/batch/pooling | existing embedding provider tests; local.rs output/normalization tests |
| fresh/upgrade/down-up/reset/canonical preservation | crud/tests/sqlite_memory_search_schema.rs; startup reset regression |
| existing callers → common contract → SQLite | real Thread Search/CLI fixtures, real ingestion/claim/publication fixtures |

Удалены obsolete file/cache/capsule/capability/wrapper contracts, не продуктовые
assertions. Отдельного pgvector mock нет. Tests не запущены, assertions не
отключались. Runtime routing/cancellation/latency остаются предметом исполнения
после внешнего ревью, а не следствием чтения тестовых имён.

## 15. Передача

**С.** Изменения незакоммичены в заданном worktree/ветке, без push/PR/CI.
Повторно выполнены cargo fmt, cargo fmt --all --check и git diff --check.
JSON/TOML/lock статически проверены. Тесты, сборки, cargo check, миграции,
Entity codegen, приложение и CI НЕ запускались. Нужны внешнее ревью и отдельная
разрешённая фаза исполнения; производительность и runtime не объявляются
доказанными. Итоговый ответ должен явно включать найденные два дефекта и эту
границу уверенности.

## Исправления внешнего ревью (следующая итерация)

1. `agent_memory::search_payloads`: явный std::cmp::min, также однозначные
   числовые min в затронутом episodic repository. В остальных изменённых
   search/adapter участках подобного пересечения с ExprTrait нет.
2. semantic_artifacts MATERIALIZED содержит только DISTINCT artifact IDs после
   workspace/access/source/model/dimension/normalized/length фильтров. CASE
   вызывает cosine только при членстве ID в этом наборе (SQLite lazy CASE).
   semantic_scores материализует ID+distance, не BLOB. Одна distance на artifact,
   затем JOIN возвращает реальные occurrences; ranks/RRF/tie order сохранены.
3. Убран COUNT из refresh после каждого completion/recovery. Счётчик существующей
   directory поддерживается SQLite triggers по переходам searchable Active
   (status active + search_text non-NULL), включая insert/update/delete/reset и
   rollback. Неизменное состояние не даёт повторного инкремента; upsert metadata
   не перезаписывает authoritative count. Новая проекция начинается пустой, её
   directory count обнуляется заменой derived column в новой draft DDL миграции.
   Единственный COUNT — AFTER INSERT при настоящем создании/восстановлении
   отсутствующей directory, в serialized writer snapshot; не на conflict update
   каждой публикации. Этот редкий initialization всё ещё зависит от истории.
   Entity-схема по полям/типам не изменилась; historical DDL не менялись.
4. FTS5 snippet вокруг lexical match без спецсинтаксиса/подсветки. В обоих
   repositories сначала материализуется только ограниченная ranked page IDs/
   scores, затем извлекаются snippets выбранных строк; Unicode substr ограничивает
   120/профильный бюджет episodic и 240 символов agent memory. Semantic-only
   сохраняет начало текста. Adapter не обрезает второй раз уже bounded snippet.

Регрессии (не исполнены): позднее Unicode lexical совпадение с контекстом,
source hash и пределом 120; count при первой публикации, повторном completion,
rollback, пересоздании directory и exclusion. Reset regression теперь требует
точный 0 вместо сохранения фиктивного 99. Существующие behavioral проверки
разных dimension/workspace, shared artifacts/occurrences и страниц сохранены.
Тестовая фаза test-audit: два изменённых владельца, protected hashes без
изменений. Затем отдельная production/format phase. Никаких SQL-string тестов.
Тесты/сборки/cargo check/мигратор/codegen/CI не запускались.

## Внешнее ревью: полный payload, atomic publication, repair ownership

Изменения ограничены MemoryService/backend contract, SQLite adapter и текущими
CrudStore payload/publication/repair операциями; semantic SQL, directory count,
RRF, snippets, query syntax, embeddings, schema/Entity и model refill не менялись.

### 1. Restore/reindex не восстанавливает оригинал из превью

- Удалён backend_put_request_from_row: content_preview больше не становится
  content при repair. Reindex читает полный payload через существующий backend.
- Для SQLite FTS уже согласован triggers, поэтому повторного put/rebuild нет.
  Полное содержание и revision остаются неизменными.
- При отсутствующем payload применяется существующая диагностика missing
  payload/repair_needed; repair не объявляется успешным, оригинал не выдумывается.
- Перед clear repair_status отдельная короткая существующая CrudStore-транзакция
  повторно проверяет scope/workspace/provenance, payload_version, lifecycle,
  quarantine, timestamp/status и durable payload revision. Lifecycle event
  подготовлен вне writer и коммитится со сменой статуса. Изменённый/исчезнувший
  payload или owner не даёт ложного repair_status=ok.

### 2. SQLite MemoryService публикует payload и control атомарно

- Выбран вариант устранения окна, а не новый GC/recovery worker. Никаких новых
  repair jobs, очередей, сканов, таймеров, схемы или хранилища версий не добавлено.
- Минимальный engine-neutral prepare_put возвращает обычный content и revision
  guard для публикации. SQLite prepare не пишет payload. Старые backend put/get/
  search/delete остаются; стандартный backend по умолчанию использует свой put.
- MemoryService передаёт prepared content в существующий publish_agent_memory_record.
  Тот повторно проверяет mutable scope ownership, expected active owner и
  предыдущую revision внутри serialized writer; payload write/FTS triggers,
  control record, supersession/identity transfer и events — один commit.
- Вне транзакции читаются owner snapshot/scope и предыдущая payload revision;
  контент уже подготовлен. Всё изменяемое повторно проверяется перед записью.
  Конкурентное изменение вызывает отказ/rollback, не перезапись победителя.
- Отмена до commit оставляет старого владельца и полный content; при unknown
  commit outcome payload и control появляются вместе или вместе откатываются.
  In-place update тоже не меняет старый payload во время preparation.
- Cleanup после ошибки/неизвестного исхода использует hash-only orphan request:
  DELETE ещё раз проверяет отсутствие любого control owner под writer. Поздний
  commit не может потерять payload из-за предварительного stale reader snapshot.

### 3. Repair удаляет по подтверждённому scope

- Для orphan repair используется пустой scope key + настоящий kind/hash из job,
  не hash строки "repair". Отсутствие kind/hash — ошибка, а не успешный no-op.
- SQLite проверяет фактические scope kind/hash payload и control в той же
  транзакции, что DELETE. Несовпадение hash у orphan repair даёт ошибку; payload
  другого scope сохраняется. Отсутствующий payload допускает идемпотентный повтор.
- Hash-only orphan cleanup сохраняет любого опубликованного владельца.
  Cleanup по существующему control использует его подтверждённый scope; актуальное
  active/non-quarantined состояние повторно проверяется с DELETE. Restore,
  выигравший race, сохраняет payload. Обычный запрос с неверным scope не удаляет
  данные; delete активного non-quarantined control также защищён.

### Тестовая фаза (написано, не исполнено)

Основной владелец: memory/tests/sqlite_service.rs.
- Длинный Unicode full content переживает quarantine без cleanup, restore и
  reindex целиком, с прежней revision; поздний hash-only cleanup его сохраняет.
- После настоящего quarantine cleanup missing full payload не превращается
  в preview: repair остаётся pending/repair_needed, payload отсутствует.
- Отмена после preparation до atomic publication + restart: нет третьего
  неопубликованного payload/фиктивного обязательного job, старый owner/body
  сохранены, последующая обычная публикация работает. Отдельно in-place cancel.
- Repair без control: неверный hash и отсутствующий hash дают failed, настоящий
  hash удаляет целевой payload, чужой body сохранён, повтор безопасен. Проверяется
  реальное состояние payload, не только статус job.
- Существующий InMemory lifecycle regression сохраняет cleanup/restore checks,
  но теперь ожидает честную недоступность утраченного оригинала вместо preview.
- Missing-payload fault fixture удаляет payload через project DB path, поскольку
  обычный backend delete теперь защищает активного владельца.

Test-audit: отдельные frozen-production фазы, protected hash differences=0;
production corrections и formatter запускались после выхода из этих фаз.
Проверки не исполнялись. Новых SQL-string/implementation mirror тестов нет.

Самоаудит: проверены все callers publish/prepare/get/delete/repair, scope/hash,
CAS/revision, rollback/unknown outcome, quarantine/restore и отсутствие orphan
window. Общие контракты не содержат SQLite BLOB/FTS/Memvid деталей; новый worker,
job engine, архив и background polling не созданы. Штатный backend put как низкий
storage API по-прежнему может создавать payload без control; продуктовый путь
MemoryService SQLite использует prepare + атомарную publication, не этот путь.

cargo fmt --all --check и git diff --check выполнены. Тесты, сборки, cargo check,
clippy/nextest, codegen, мигратор, приложение и CI НЕ запускались. Production DB,
реальные модели/капсулы/процессы не изменялись. Изменения незакоммичены; runtime
корректность/численная эквивалентность/производительность исполнением не доказаны.


## Внешнее ревью: legacy storage cleanup и lock-only paths

Решение пользователя после cleanup-audit учтено: **memory сохраняется**.
Отчёт с предложением удалить сам memory не является текущим требованием.

### Владение и блокировки

Сверены Gateway connection WriterOwnership, startup/settings supervisor,
workspace refill entrypoints, process workspace ownership и executor callers.
DB WriterOwnership продолжает исключать другой поддерживаемый writable Gateway
на весь срок физических соединений; его sidecar и механизм не изменены.
В обычном Gateway это частично дублирует workspace file lock. Но refill
entrypoints принимают CrudStore без требования WriterOwnership, а supervisor и
workspace mutex не межпроцессные. Сохранён существующий workspace fs4 lock для
всего reset/prepare/provider/publish workflow: иначе два допущенных владельца
могут чередовать отдельные SQLite-транзакции разных refill generations.
Нового координатора/lock framework/worker нет.

Отдельный общий file lock не сохраняется: особое имя для legacy default теперь
тоже заменено workspace hash. Global settings owner координирует workspace
leases существующим supervisor, а не одним глобальным файловым mutex.
Lock-файл не равен занятой блокировке: current inode не удаляется и не заменяется,
OpenOptions без truncate, fs4 RAII освобождает ownership при return/error/cancel;
закрытие дескриптора процессом/ОС снимает lock. Контракт межпроцессный, не только
сериализация SQL. Execute/shutdown/source-model fencing не менялись.

### Конечные пути

`runtime_home/memory/locks/workspace-<existing workspace key hash>.lock` — плоский
каталог текущих refill locks, без workspace-id в пути и без подкаталогов workspace.
`runtime_home/memory/capsules` — исключительно фиксированный legacy cleanup root.
Параметры/поля затронутых lib/startup/message callers переименованы из
thread_episodic_storage_root в thread_episodic_lock_root. Runtime не создаёт
capsules; test-only isolated roots тоже больше не именуются capsules-*.
DB, models, history, memory и memory/locks не удаляются.

### Конечная cleanup-операция

Существующий startup maintenance owner работает под кооперативным DB
WriterOwnership, исключающим поддерживаемых старых writable владельцев instance.
Обход касается только фиксированного disposable Memvid дерева. Исторические URI,
custom roots и совместная работа со старым backend не поддерживаются.
Symlink memory parent отклоняется как недоказанное владение. Symlinks внутри
capsules и symlink самого capsules удаляются как ссылки, без обхода targets.
Все regular/service остатки и пустые подкаталоги удаляются postorder, включая
сам capsules. Отсутствие пути и повтор безопасны, частичная очистка restart-safe.

Перед удалением regular .lock делается nonblocking exclusive fs4 probe; занятый
или недоступный lock даёт ошибку и **не** отмечает завершение. Проба освобождается
до unlink: удерживаемый inode не удаляется. Unix open использует O_NOFOLLOW.
Без кооперативного DB ownership эту проверку нельзя трактовать как поддержку
конкурентного старого backend; такой режим явно вне задачи. Текущие locks вообще
не входят в cleanup tree.

Кванты по 64 entries, cancellation/checkpoint вне DB capacity. Стек дескрипторов
ограничен глубиной 128; превышение — явная ошибка, никогда не молчаливый skip или
complete. Финишный checkpoint вызывается только после удаления root.
Используется тот же projection_meta marker: version 1 complete означает лишь
прежнюю .mv2-очистку; version 2 complete — всё дерево удалено. Старый complete v1
не пропускает новую обязанность. После v2 EOF нет recurring filesystem polling.

### Схема

Повторно сверены новая draft DDL и актуальные Entity: две старые таблицы удаляются
со своими 14 indexes (включая autoindex), ещё 3 named indexes удаляются явно;
16 полей удалены, frame_version переименован в payload_version. Дополнительных
Memvid-only объектов не обнаружено. **Production DDL/Entity в этой итерации не
менялись**, новой миграции нет. Checkpoint version использует существующий столбец.
FTS tables/shadows/6 triggers, 4 directory count triggers, artifacts/vector_bytes,
items/jobs/exclusions/directory/recall, agent control/policy/quality/quarantine/
identity/candidates и delegation home/source_capsule_id сохранены.

### Написанные регрессии, не исполненные

- Existing startup cleanup owner: old .mv2/service/lock files, глубина >16,
  посторонние данные отдельно в models/history/memory; current locks сохраняют
  тот же удерживаемый inode; symlink target остаётся; cancel не завершает duty;
  old complete v1, held legacy lock, fresh facade restart, repeat, depth-budget
  failure и последующее продолжение без false complete.
- Existing refill test owner: hashed flat filename, exclusion второго владельца,
  release после cancellation, actual refill и model-target replacement не создают
  capsules и сохраняют current lock file.
- Existing schema owner: sqlite_master/PRAGMA для всех 17 legacy index names,
  всех полей четырёх таблиц, payload_version, новых объектов/shadows/triggers,
  отсутствия dangling schema references и сохранения canonical/delegation data.
  Upgrade fixture доказывает наличие всех 17 indexes до up; те же assertions
  используются fresh/upgrade/down-up. Не проверки текста Rust/SQL.

Test-audit frozen production: 4 test owners, protected hashes без изменений.
После выхода из фазы выполнены formatter/static checks и обновлены документы.
Semantic search/RRF/snippets/embeddings/atomic payload/repair API не переделывались.
Ограничения: глубина >128 откладывает cleanup с ошибкой; busy legacy lock требует
освобождения владельцем и следующего startup/retry, а не force unlock. Чужие
нестандартные paths не обходятся. Runtime/OS-specific execution ещё не проверено.
Тесты, сборки, cargo check, nextest, мигратор, Entity codegen, приложение и CI НЕ
запускались; production DB, instance files, модели и процессы не изменялись.
Изменения оставлены незакоммиченными, без push/PR/CI, для внешнего ревью.

## Test-only correction: pending reset after configuration reversion

Исправлен только существующий
`interrupted_model_cleanup_restarts_even_when_config_reverts_to_complete_target`
в cfg(test) модуле startup/thread_episodic_refill.rs.
Удалён вызов несуществующего delete_thread_episodic_capsules_for_workspace;
никакие новые SQL-данные вместо капсулы не удаляются.

Сценарий: исходный lexical refill действительно complete/current; для другой
embedding identity установлен durable pending reset checkpoint; прерывание до
первого reset quantum подтверждено неизменностью прежнего job. При возврате к
исходному target прежний complete marker остаётся, но pending reset требует
работы. Следующий реальный refill не skipped/resumed, завершает job, закрывает
pending reset, заменяет job identity; старый ID отсутствует, source occurrence
сохранён. Дополнительно проверяется committed канонический UserMessage с точным
исходным текстом и current итогового target.

Тест оставлен отдельным: соседние pending-preparation и model/backoff tests не
покрывают возврат конфигурации к прежнему complete target при pending reset.
В ближайшем startup-владельце статический поиск аналогичных removed capsule API
не выявил других вызовов; массового переименования/рефакторинга нет.

Test-audit: снимки байтов/хешей рабочих файлов до правки; разрешён только точный
span указанной тестовой функции и этот отчёт. Остальные тесты и весь production,
Cargo manifests/lock, миграции, Entity и конфигурация неизменны относительно
начала этой итерации. Проверены cargo fmt --all --check и git diff --check.
Тесты, компиляция, сборки, cargo check/nextest, мигратор, codegen, приложение и CI
НЕ запускались. Изменения незакоммичены в прежнем worktree/ветке для ревью.
