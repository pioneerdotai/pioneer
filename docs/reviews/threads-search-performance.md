# threads_search: передача на внешнее ревью

## Ревизии и изоляция

- Исходный commit и фактическая база `main`: `3f862da3f494f3e6048e211f426c24cd81d2aeac`.
- Ветка Pioneer: `fix/threads-search-performance`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-search-performance`.
- Итоговый commit реализации Pioneer: `153f2738039f190d0e9865f09857562c01ecfdd9`.
- Этот отчёт добавлен отдельным последующим commit передачи. Его точный SHA
  определяется `git log -1 --format=%H -- docs/reviews/threads-search-performance.md`
  и передан в финальном ответе; включить собственный SHA в содержимое commit невозможно.
- Fork Memvid: существующий upstream `https://github.com/pioneerdotai/memory`;
  база `v4.0.1`, `c499f5e71a86c204ab263a0ca9f0a00651226c44`.
- Ветка fork: `fix/reusable-read-only-reader`.
- Worktree fork:
  `/Users/alexander/Code/pioneer/memory-threads-search/.worktrees/reusable-read-only-reader`.
- Commit реализации fork: `6f19fb80c53677077898b485a7bb694fd789b149`.
- Опубликованный релиз fork: [v4.1.0](https://github.com/pioneerdotai/memory/releases/tag/v4.1.0),
  release commit `e3c2d9605d00a1166f45f43c57c1985d1979bc80`.
- `Cargo.toml` Pioneer выбирает версию через `tag = "v4.1.0"`, без `rev`.
  `Cargo.lock` содержит версию `4.1.0` и обычную фиксацию commit опубликованного
  тега. Features и другие зависимости не менялись. Lockfile изменён вручную,
  без dependency resolution или исполнения Cargo metadata/build.

Основной checkout и соседние worktree Pioneer не изменялись. Ветка и путь
перед созданием были свободны. Commits сделаны с `core.hooksPath=/dev/null`.
Merge, deploy, push Pioneer и публикация PR не выполнялись. Fork клонирован отдельно;
Cargo checkout читался как источник диагностики, но не редактировался.
После статической приёмки пользователь отдельно разрешил выпустить новую версию
fork и заменить привязку Pioneer к commit на версию. Ветка fork и тег `v4.1.0`
опубликованы; release не является подтверждением компиляции или прохождения тестов.

## Карта изменений

| Направление | Файлы | Изменение |
| --- | --- | --- |
| Reader reuse | `crates/memory/src/thread_episodic.rs` | Ограниченный кэш idle readers, владелец workspace/capsule, single-flight существующим capsule lock, инвалидация перед мутациями |
| Безопасный idle reader | fork: `src/memvid/read_only_reader.rs`, exports в `src/lib.rs`, `src/memvid/mod.rs` | Lazy open, повторная проверка snapshot под OS lock, освобождение lock между вызовами |
| Batch источников | `crates/crud/src/repositories/thread_episodic.rs`, `crates/crud/src/lib.rs` | Ограниченная ORM-выборка источников и канонических items |
| Проверка источников | `crates/gateway/src/thread_episodic/search.rs`, `crates/gateway/src/thread_episodic.rs` | Batch-предпроверка, общий существующий отбор канонического текста |
| Поисковый caller | `crates/gateway/src/message/thread_search_tools.rs` | Переиспользование метаданных и авторизации внутри retrieval-порции |
| Регрессии | `crates/memory/src/thread_episodic.rs`, `crates/gateway/src/message/thread_search_tools/tests.rs`, fork reader tests | Реальные временные файлы, CRUD и backend; тесты не запускались |
| Привязка fork | `Cargo.toml`, `Cargo.lock` | Опубликованная версия Memvid `v4.1.0` |

## Reader: владение, актуальность и ресурсы

Изучены `Memvid::open_read_only`, изменяемые search APIs, `FileLock`, lifecycle,
commit footer, lifecycle recovery, doctor и существующий snapshot fingerprint.
Последний хеширует весь файл: использовать его для каждой страницы означало
бы повторять полный файловый проход. Публичного API безопасного idle reuse
в исходном fork не было. Изменение оставлено внутри соответствующего backend
и минимального общего API fork; формат индекса не менялся.

Кэш Pioneer хранит до **8 idle readers на процесс**, независимо от размера
корпуса. При вытеснении освобождаются reader, файловые дескрипторы и поисковые
структуры. Следующий запрос к этому индексу открывает его заново. Активные
readers принадлежат уже допущенным blocking operations; отдельной очереди,
worker или scheduler нет. TTL, query/result cache и кэш готовых ответов отсутствуют.
Счётчики открытий существуют только в тестах, пользовательские данные не логируются.

Ключ включает нормализованный путь и владельца `(workspace_id, capsule_id)`.
Путь сам по себе не является доказательством актуальности. Fork при каждом
чтении захватывает shared OS lock через существующий `FileLock`, сравнивает
физическую идентичность открытого файла с опубликованным путём, длину, 4096-байтовый
header и commit footer. Footer содержит generation, TOC length и TOC hash.
При парковке проверяется, что footer соответствует действительно загруженной
reader generation и footer offset. Неоднозначный хвост не кэшируется: обычное
чтение остаётся возможным, последующий вызов заново открывает reader.

Не используются предположения «mtime не изменился» или «путь тот же».
Надёжность маркеров опирается на существующий протокол commit и OS locks;
произвольная запись в обход Memvid с сохранением всех commit markers не является
поддерживаемым писателем. Кэш не держит OS lock между вызовами. Search получает
изменяемый reader эксклюзивно. Новых `unsafe`, ручных `Send/Sync` и объявления
конкурентного поиска безопасным нет. Требования Send проверяются типами
`spawn_blocking`/`Mutex`, но компилятор в задании не запускался.

При переоткрытии сбрасывается проверенная embedding identity. При неизменном
snapshot её TOC-проверка переиспользуется вместе с reader. Ошибка открытия,
проверки, unlock или поиска не возвращает старый reader в кэш. Следующий вызов
пытается открыть файл нормально. Panic также не возвращает reader: он принадлежит
стеку blocking closure. Fork не паркует writable handle, если callback выполнил
мутацию либо заменил переданный ему `Memvid`.

### Писатели и замены, проверенные по callers

| Путь | Согласование |
| --- | --- |
| `index_item` / `index_item_with_workspace_ownership` | `run_blocking` → capsule lock → извлечение и закрытие idle reader → open/create → commit |
| `index_batch_with_workspace_ownership`, native batch и single fallback | Та же инвалидация перед всей группой, включая partial failure, checkpoints и Drop commits |
| `open_or_create_memvid` → `Memvid::open` recovery/repair | Выполняется только после инвалидации в mutation boundary |
| Refill/batch indexing | Использует существующий backend с workspace ownership; тот же capsule boundary |
| Refill/reset, удаление или удаление старого файла при replacement | `delete_capsule_file` → `remove_thread_episodic_capsule_file` → тот же boundary, инвалидация до remove |
| Создание следующего segment / файла по прежнему пути | Новый reader; прежний удалён либо несовпадение физического файла/commit signature |
| Внешний Memvid writer, doctor, atomic publication/rename | Idle OS lock отсутствует; следующее чтение проверяет inode и committed signature и переоткрывается |

Просмотрены пути `thread_episodic.rs` index executor и
`database/startup/thread_episodic_workspace_capsule_refill.rs` refill/reset/delete,
а также fork mutation/doctor/publication. Изменений в их алгоритмы индексации нет.
Legacy backend `search()` сохраняет прежнюю логику и консервативно инвалидирует
idle cache через старый helper. Автоматический recall не подключался и его
настройки не менялись.

## База и проверки кандидатов

До изменения каждый hit отдельно читал indexed item, exclusion, turn guard,
канонический item (дополнительно turn location и thread), thread title и проходил
авторизацию. Теперь предварительная порция до 128 IDs выполняет ORM-выборки:
indexed items, exclusions, threads, проекцию `(turn_id, thread_id)` без deleted
turns, затем только необходимые пары `(turn_id, item_id)` канонических rows.
Обычная непустая порция использует пять reads независимо от числа кандидатов.
Это статический подсчёт вызовов, не измерение SQL latency.

Декодирование canonical payload, source selection, hashing и secret filtering
выполняются после `.all().await`, без удержания транзакции, query stream или
ручного permit. SQL не перенесён в Gateway. Используются scoped handle
`CrudStore` и существующий SeaORM repository; scheduling class не повышается.

Проверяется фактическая принадлежность turn треду, а треда workspace: FK в SQLite
не считаются достаточной гарантией. Выборки не загружают историю turn/workspace.
Внутренние portions продолжаются до исчерпания входных hits; cap 128 ограничивает
только одну SQL-порцию и не исключает кандидатов. Gateway retrieval quantum
остаётся прежним — 100.

Метаданные title берутся из той же порции. Повторная предварительная авторизация
одного треда переиспользуется только внутри текущей retrieval-порции. Положительные
права не сохраняются глобально, между запросами или в окончательной проверке.

Сохранены Active/visibility/context/deleted/exclusion/committed проверки,
канонический выбор текста, hash, соответствие hit/source workspace/thread/turn/item
и source version. Старый текст индекса не заменяет изменившийся canonical source.
`current_thread_search_source`, `search_fragment_readable`,
`search_response_readable`, oversized-card continuation и обновление authority
перед возвратом остаются свежими. Изменение доступа, удаление, исключение или
смена версии после batch-предпроверки может запретить итоговую выдачу.

## Карта блокировок и отмены

| Замок | Кто берёт и что защищает |
| --- | --- |
| Workspace ownership | Ordinary indexing/admission, refill/reset/replacement; существующая сериализация состояния поколения и писателей |
| Capsule mutex | Поиск, одиночная/batch-запись и удаление одного нормализованного файла; удерживается до реального завершения blocking closure |
| Короткие registry/cache mutex | Только поиск lock entry либо передача владения reader; без открытия, поиска и закрытия индекса |
| Memvid OS lock | Shared во время чтения и верификации snapshot; exclusive у писателя/repair/doctor |

Порядок: workspace ownership, если нужен → capsule → короткий checkout кэша
(затем отпускается) → OS lock. Возврат в кэш выполняется уже после освобождения
OS lock. Другие файлы остаются независимыми. Протухшие weak entries capsule
registry удаляются при создании нового entry, активные замки сохраняются.

Устранены полная подготовка reader на каждом cache hit, повторный decode TOC для
embedding identity неизменного snapshot и удержание shared OS lock у idle reader.
Это сокращает подтверждённую по коду работу внутри capsule critical section;
величина уменьшения ожидания не измерялась. Сохранены сериализация одного
изменяемого reader, записи, repair, удаления и destructive replacement. Shared
read-only open сам по себе не использован как основание убрать capsule mutex.

Отмена queued async waiter не оставляет blocking operation. После старта
blocking closure capsule guard и clone workspace ownership находятся внутри неё.
Отмена async join waiter не освобождает их. Работа не стала новым detached worker:
это прежняя семантика существующего `spawn_blocking` с тем же владельцем работы.

## Незапущенные регрессии

- Backend cache: реальные lexical queries и continuation 2/2/1 без нового полного
  open, повторный запрос, bounded eviction и reopening, смена owner.
- Concurrent open: tokio barrier, существующий capsule mutex, реальный поиск,
  одно открытие; Drop cache и последующее exclusive open.
- Ошибка открытия, восстановление, rename/replacement, удаление и recreate без
  возврата прежнего snapshot.
- Существующая backend pagination-регрессия расширена append после чтения,
  mutation/recovery boundary, delete и recreate.
- Существующая cancellation-регрессия теперь держит настоящий reader и проверяет
  последующий настоящий exclusive writer. Отрицательная временная проверка
  заменена детерминированным `try_lock`; синхронизация каналами, без sleeps.
- Существующий Gateway test расширен до 141 источника и repository portions,
  проверкой другого workspace, exclusion и tombstone после предпроверки,
  canonical version change. Существующая проверка потери/возврата ACL сохранена.
- Существующая проверка нескольких файлов, dedup и позиционных страниц сохранена,
  oversized unicode card и продолжение после удаления не изменялись.
- Fork: счётчик у настоящего полного open, reuse, внешний exclusive writer и
  doctor при idle reader, missing/corrupt/replaced/deleted/recovered file.

Сценарии не исполнялись, успешность не заявляется. Низкоуровневые новые тесты fork
проверяют lifecycle API; Pioneer tests отдельно проверяют передачу invalidation
и pagination на backend/service/repository границах. Полный fault matrix всех
вариантов повреждённого WAL остаётся в существующем покрытии Memvid, здесь не
дублируется.

## Самоаудит и выполненные проверки

Промежуточно проверены четыре направления: reader ownership/freshness/eviction;
batch/filter/final recheck; lock ordering/cancellation; tests/callers/integration.
Первоначальный вариант с idle shared lock отклонён собственным ревью и заменён
API fork. Дополнительно защищена парковка от writable replacement в callback.
Финально перечитаны полный собственный diff обоих репозиториев и перечисленные
mutation/deletion callers. Новых corpus cutoff, токенных/временных лимитов,
query cache, фоновых механизмов, изменений API/schema `threads_search`, recall,
ранжирования, backend selection, UI, Tasks/Subagents или execution ledger нет.

Фактически выполнены только статические действия и разрешённые проверки:

1. Чтение AGENTS.md (root, CRUD; дополнительно Gateway database для caller audit),
   исходников, manifests, lockfile, поиск `rg`, Git status/worktree/base/diff/log.
2. `cargo fmt --all --check` Pioneer — показал изменения форматирования своего
   diff и постороннее замечание базы `crates/observability/src/lib.rs:828`.
3. `cargo fmt --all` Pioneer; его единственное постороннее изменение в
   observability восстановлено из HEAD. Этот файл в итоговый diff не входит.
4. `cargo fmt -p pioneer-crud -p pioneer-gateway -p pioneer-memory`, затем
   `cargo fmt -p pioneer-crud -p pioneer-gateway -p pioneer-memory --check` — успешно.
5. Fork: начат `cargo +stable fmt --all`, остановлен при rustup download до
   форматирования. Использован уже установленный toolchain:
   `cargo +1.99.0 fmt --all` и `cargo +1.99.0 fmt --all --check` — успешно.
6. `git diff --check` обоих worktree и проверка итогового committed diff
   относительно базы — без whitespace errors.
7. Commits с отключёнными hooks; финальный Git status обоих рабочих worktree
   и основного Pioneer checkout.

Тесты, cargo check, компиляция, сборки, приложение, Gateway, CLI-агенты,
миграции, benchmarks, нагрузки и генераторы **не запускались**. Ни production,
ни dev базы и индексы не открывались для экспериментов и не изменялись.
Компилируемость, прохождение тестов и фактическое ускорение не подтверждены.
Отдельные доли задержки неизвестны. Изменение устраняет перечисленную работу по
статическому анализу, без обещаний кратности или времени поиска. Следующий этап —
внешнее ревью обоих commits; разрешение на любые исполняемые проверки и
окончательная приёмка выполняются отдельно.

## Публикация версии после статического ревью

По отдельному поручению пользователя опубликован GitHub release `v4.1.0`.
Release commit меняет только версию пакета в manifest/lockfile fork и changelog;
исходники совпадают с прошедшим статическое ревью `6f19fb80`. Ветка fork и тег
отправлены без hooks; `main` обоих репозиториев не изменялся. Docker Release
workflow временно отключался перед push тега и затем возвращён в `active`.
GitHub не зарегистрировал workflow runs для release commit.

Pioneer использует опубликованный `tag = "v4.1.0"`; SHA в `Cargo.lock` фиксирует
содержимое этого тега обычным способом Cargo. Дополнительно выполнены
`git diff --check`, сверка remote tag с release commit, проверка отсутствия
изменений `src` между implementation/release commits fork,
`cargo +1.99.0 fmt --all --check` fork и
`cargo fmt -p pioneer-crud -p pioneer-gateway -p pioneer-memory --check` Pioneer.
Проверки успешны. Тесты, компиляция, сборки и измерения производительности
по-прежнему не запускались; публикация версии не подтверждает их успешность.
