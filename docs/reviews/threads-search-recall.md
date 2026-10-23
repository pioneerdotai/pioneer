# Поиск тредов и episodic recall — кандидат на внешнее ревью

Статус: реализация подготовлена локально для полного внешнего ревью другой моделью. Внутренние проверки исполнителей и родительский самоаудит не являются внешней приёмкой. Разрешение на тесты или сборки не выдавалось.

**Тесты, cargo check, компиляция, сборки, приложение, CLI-агенты и генераторы не запускались. Компилируемость и прохождение тестов не подтверждены.**

## Репозитории и ревизии

| Репозиторий | Рабочая ветка | Изолированное рабочее дерево | Исходный commit | Итоговый commit кода |
| --- | --- | --- | --- | --- |
| Pioneer | `feat/threads-search` | `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-search` | `6c1e6b2c49359c76e49624c0545bbafc4a3d310a` | `7b11f2a145ef692937e8b87a001c2c2c8e7a3af4` |
| Memvid, существующий форк | `feat/search-pagination` | `/Users/alexander/Code/pioneer/memory-search-pagination` | `fe0b3d9ac883c667587c262f7b372bf06eb605d5` | aec587174243b95875c63a91876e3b62498ff234 |

Pioneer commit в таблице содержит реализацию и незапущенные регрессии. Этот отчёт добавлен следующим отдельным documentation commit в той же ветке; его точный HEAD передаётся в финальном сообщении, чтобы избежать самоссылки SHA в содержимом commit. Memvid история содержит два локальных commits: `da735d06816032c718df419fedd9fe4c23ada633` и итоговый `aec587174243b95875c63a91876e3b62498ff234`.

При начале основной checkout Pioneer был чистым, `main` и локальная ссылка `origin/main` указывали на `6c1e6b2c49359c76e49624c0545bbafc4a3d310a`, уже после указанного в задании `fdc1b069df0fdca80690e759b596a2626f5619da`. Основной checkout Memvid был чистым на `main` = `b827f174eab60bcfe3deb8412f507e4990d4c341`; origin — `https://github.com/pioneerdotai/memory.git`. Работа Memvid начата с закреплённой в Pioneer версии 3.1.8, а не со старого main. Соседние ветки и worktree не очищались и не использовались как база поиска.

Pioneer `Cargo.toml` и `Cargo.lock` статически переведены с tag `v3.1.8` / `fe0b3d9ac883c667587c262f7b372bf06eb605d5` на одну и ту же Git-ревизию aec587174243b95875c63a91876e3b62498ff234. Версия пакета остаётся 3.1.8. Абсолютной path-зависимости, vendor-копии или второго форка нет.

**Новая ревизия Memvid не опубликована на GitHub.** Для последующей проверки внешним Git-потребителем потребуется отдельная разрешённая публикация этой ревизии. Локальная готовность к ревью не означает доступность Git-зависимости извне. Push, merge, release tags, deploy и CI не выполнялись.

## Карта требований и точек расширения

| Требование | Реализация |
| --- | --- |
| Скрытый `threads_search`, query/limit/cursor | `crates/protocol/src/thread_search.rs`, `schemas/threads_search_{params,response}.json`, `crates/gateway/src/message/thread_search_tools.rs` |
| Native и фиксированный CLI-каталог, общий dispatch | `message/artifact_tools.rs`, `turn_mcp/native_tools.rs`, `tools/src/domain.rs`, `authorization/catalog.rs` |
| Защищённый cursor, текущие права и frozen Agent grants | существующие `thread_read_cursor.rs`, `thread_read_authority`, `thread_source_readable`; повторное разрешение authority на каждом вызове |
| Полный каталог, без первых 16 файлов / 8 бесед | `CrudStore::list_thread_episodic_capsules_for_workspace_after`, одноимённый repository accessor, `search_thread_sources` |
| Общее получение страниц, отсутствие adaptive cutoff до source checks | `ThreadEpisodicMemvidBackend::search_segment_page`; общий gateway-путь инструмента и recall |
| Embedding один раз, та же совместимость | `thread_episodic/search.rs::prepare_thread_search_embedding`; существующий indexing provider resolver, checked query embedding, полная persisted identity в адаптере |
| Актуальные канонические источники | `current_thread_search_source`, `resolve_thread_search_source`; canonical committed item, hash, status, message deletion, exclusion |
| Полные названия, 100 фрагментов, 2 MiB | актуальный `Thread.name`, `search_response`, сериализованный byte budget, `pageFragment` для одной большой карточки |
| Дочитывание уже сохранённого результата | native durable lane + действительный `threads_tools_result_read` reference, повторная ACL/source проверка в `thread_tool_results.rs`; history не встраивает cached search display |
| Production recall | `bind_memory_bridge` → `MemoryBridgeProviders.episodic_provider` → memory hook package → `ThreadEpisodicMemoryRecallProvider` → `recall_thread_search` → общий поиск |
| Planner и реальные execution IDs | `memory/src/hooks/recall.rs`, `active_recall.rs`; host-owned full-input episodic section, execution thread ID с реальным turn ID |
| HNSW, vector/lexical/hybrid pagination, raw retrieval | Memvid `src/vec.rs`, `src/memvid/search/{api,mod,tantivy,fallback,helpers}.rs`, `src/memvid/ask.rs`, `src/memvid/acl.rs` |

Tasks/Subagents skills и policy blocks не менялись. Системный Threads skill не перерабатывался. History reader получил только классификацию нового observer tool для защиты сохранённого результата; его отдельная пагинация не менялась.

## Memvid: существующий API

Используется **`Memvid::search(SearchRequest)`**, а не `ask(context_only=true)`. В существующий `SearchRequest` добавлено optional `query_embedding: Option<Vec<f32>>`, с serde default `None`. Без него выполняется lexical retrieval; с подготовленным вектором — lexical и vector retrieval с общим RRF, затем выделение общей страницы. Существующий `SearchRequest.cursor` и `SearchResponse.next_cursor` продолжают результат по позиции. `no_sketch=true` исключает прежний необязательный sketch prefilter из полного поиска.

Существующий `vec_search_with_embedding_acl` расширен последним параметром `cursor: Option<&str>`. Все обнаруженные callers обновлены. `AskRequest.cursor` относится к общей выдаче после объединения, переупорядочивания и проверок; фиксированный векторный потолок 64 убран. `ask` сохраняет свои answer-oriented эвристики; raw `search` их не вызывает.

JSON-схемы Pioneer добавлены вручную по объявленным типам; генератор не запускался.

Новых публичных методов Memvid нет. Внутренние helpers реально используются lexical/vector/hybrid/ask путями. Формат `.mv2` и сохранённые схемы индексов не менялись, sidecar отсутствует. API для поколения не добавлялся; фиксация поколения не нужна.

HNSW требует отдельного внимания внешнего ревью: в hnsw 0.11.0 `search_layer` выполняет `dest.copy_from_slice(nearest[..found])`. Даже `min(limit, graph.len())` недостаточно для валидного графа с коротким достижимым пулом (например, одинаковые векторы). Поэтому ANN-обход выполняется с пустым destination, при котором обе длины всегда нулевые. Существующая метрика регистрирует действительно рассмотренные публичные feature references и расстояния в thread-local collector; результат ранжируется только среди этих ANN-кандидатов, без полного вычисления расстояний по корпусу. Адреса публичных feature references используются как непрозрачные ключи: приватные поля зависимости, арифметика указателей, unsafe и dereference адресов не используются. Транзиентный адресный map пропускается serde, сбрасывается при clone/decode; бинарное представление сохраняет прежние три поля. Search width зависит от limit, с существующим минимумом 50 и пределом размера графа; количество результатов определяется фактически рассмотренными кандидатами, а не предположением о заполнении буфера. Добавлена регрессия короткого пула, clone/decode и совпадения сохранённых байтов. Порог HNSW остаётся 1,000 векторов.

Vector retrieval расширяет внутреннее число кандидатов после scope/status/ACL фильтров, пока не набран запрошенный диапазон с lookahead или не исчерпан индекс. Tantivy расширяет окно документов после проверок авторитетных frame/source данных. Окончание служебного окна не выдаётся за окончание поиска. RRF при равенстве использует frame ID внутри одного файла.

В Pioneer добавлен один небольшой страничный метод существующего Memvid adapter trait — `search_segment_page(request, page_size, cursor, prepared_embedding)`. Старый wrapper возвращал уже ограниченный/ранжированный список без continuation; он не позволяет продолжить поиск после gateway-проверок текущего источника. Новый метод отдаёт raw engine page одного выбранного файла, переиспользуя открытие/блокировки, request construction и metadata parsing. Он не выполняет отдельный обход каталога, не выбирает провайдера и не применяет профили отсечения. Это деталь Memvid-адаптера, не обязательный контракт будущего backend.

Новый keyset accessor каталога читает существующую таблицу bounded порциями по 100 строк, без новой таблицы/индекса/очереди. Он нужен для освобождения SQLite reader между порциями; прежний `list_all...` материализовал весь каталог одним запросом. Scoped CrudStore/SqliteDatabase и scheduling class сохраняются, соединения напрямую не открываются.

## Область, ранжирование и страницы

На каждом вызове выполняется полный проход каталога пространства. Обрабатываются workspace capsules и доступные legacy thread capsules. Служебные порции каталога и engine-кандидатов — по 100, но вызов не останавливается из-за конца такой порции. Предварительного списка бесед нет. Для каждого файла накапливается необходимый префикс доступных уникальных источников; источник проходит текущую проверку перед попаданием в глобальное слияние. Глобально удерживается необходимый диапазон `offset + limit + 1`, а не все найденные тексты или векторы корпуса.

Учитываются права инициирующего пользователя **из persisted execution context**, admitted allowed actions и Agent role policy, ChildObserve, ThreadObserveWorkspace и обычные thread ACL. Principal/workspace не являются аргументами инструмента. Наличие workspace observation не отменяет приватные ACL; личная доступная беседа не исключается по отсутствию workspace visibility. Cursor привязан к query hash, principal/workspace, execution thread/turn и размеру страницы и сам не предоставляет доступ.

Перед включением источника проверяются workspace/thread/turn/item identity, активное состояние проекции, source context/visibility, explicit exclusion, отсутствие удаления сообщения, существование committed canonical item и совпадение source hash. Из отсутствующего или устаревшего источника сырой индексный текст не возвращается. Каноническая дедупликация — по `(threadId, turnId, itemId)`; frame ID разных файлов не смешиваются. Название берётся из актуальной записи беседы, полностью.

Режим выбирается автоматически. Подготовка embedding использует существующий indexing resolver и `embed_query_checked` один раз на вызов; prepared vector переиспользуется для всех файлов и внутренних страниц. В адаптере читается только committed TOC metadata через существующие публичные codecs и проверяется точное совпадение provider/model/dimension/normalization identity. Неизвестная identity не привязывается молча. Если embedding provider недоступен, выполняется lexical поиск с явным неполным semantic-покрытием. Если у файла отсутствует или несовместима vector projection, его lexical index всё равно просматривается, с явным признаком неполного vector-покрытия. Файловые ошибки и repair/missing статусы также отражаются в `coverageIncomplete`; это не используется для намеренного отсечения здорового корпуса.

Пагинация позиционная: 1–100, затем 101–200 и далее при limit=100. Внутри повторно ищется нужный префикс; наружу выдаётся только текущая страница. ANN может менять порядок при росте префикса, поэтому допустимы некоторое пересечение и пропуски из-за приблизительного порядка. Сессии с зафиксированной полной выдачей не создаются.

При достижении 2 MiB готовой JSON-страницы не поместившиеся источники остаются за `nextCursor`. Если одна карточка сама больше 2 MiB из-за полного названия, существующий exact-result pager отдаёт её JSON через `pageFragment.text`; части конкатенируются до `complete=true`, затем тот же защищённый cursor продолжает общую выдачу. Версия карточки и текущий источник проверяются повторно. Название не усекается.

## Реальный production recall

`MessageProcessor::bind_memory_bridge(self: &Arc<Self>)` создаёт существующий `ThreadEpisodicMemoryRecallProvider` с Weak-ссылкой на processor и сохраняет его в bridge. `install_memory_hook_runtime` передаёт его непосредственно в production memory package; optional `workspace_service=None` ветки больше нет.

Обычная episodic операция planner использует `recall_current_thread` как существующую entry point общего поиска. Она ищет весь разрешённый workspace, включая текущую беседу. В provenance сохраняется различие `CurrentThread` / `WorkspaceThread`, реальные turn/item/index item IDs и source kind. Отдельные related/workspace операции не рекламируются как дополнительные вызовы той же выдачи. Старый отдельный thread context hook в production не устанавливается, чтобы соседние hooks не повторяли поиск.

Native/CLI preflight может формироваться до разрешения capabilities. При `full_input_query=true` фактический memory hook берёт episodic section из host-local плана после разрешения capabilities, сохраняя durable section preflight и действующие policy/config запреты. Это не оставляет общий поиск в необязательной ветке, выключенной старым preflight решением. Исходный текст поступает без нового query cap. Runtime request содержит execution thread ID и реальный turn ID; effective conversation ID не подставляется вместо execution для проверки полномочий. Необязательный principal hint hook не является источником полномочий: initiating principal перечитывается из persisted execution.

Контекстный бюджет recall применяется после общего поиска. Полные ссылки сохраняются и рендерятся как history anchor; бюджет не определяет число файлов или бесед. Остальной memory loop не переписывался.

## Вызовы и чтение источника

```json
{"query":"где обсуждали настройку провайдеров","limit":100,"cursor":null}
```

```json
{"query":"где обсуждали настройку провайдеров","limit":100,"cursor":"<nextCursor>"}
```

Из `threads[].fragments[].source` используются настоящие поля:

```json
{"threadId":"<source.threadId>","limit":20,"anchor":{"turnId":"<source.turnId>","itemId":"<source.itemId>"}}
```

Последний объект — параметры существующего `threads_history_read`. `itemId` не выводится из Memvid frame ID. Сохранённая большая страница native возвращает `fullPageSaved/readTool/reference`; сначала `threads_tools_result_read` с точным reference и его byte cursor восстанавливает страницу до eof, затем используется поисковый `nextCursor`. При дочитывании повторяются текущие проверки всех упомянутых источников; после изменения прав, удаления или исключения выдача недоступна.

## Оставшиеся ограничения

- 1–100 найденных фрагментов на обычную страницу; группировка по беседам не меняет счёт. Сериализованный ответ инструмента не больше 2,097,152 байт.
- Используется существующий профиль engine snippet: 360 символов вокруг совпадений; у vector snippet сохраняется library minimum 80 символов. Полное сообщение доступно через history anchor.
- Внутренний каталог и кандидаты читаются порциями 100 без максимума числа порций, файлов или бесед.
- Oversized-card continuation и saved-result reader переиспользуют существующий pager с квантами до 32,768 байт / 4,096 токенов. Это продолжение точного текста, а не потеря остатка и не общий token cap страницы поиска.
- Native working-context excerpt имеет существующий бюджет 65,536 байт / 8,192 токена; перед его выдачей полная поисковая страница подтверждённо сохраняется. Глобальные лимиты других инструментов не менялись.
- Новый query-specific максимум отсутствует. У CLI сохраняется общий admission limit аргументов инструмента 131,072 байта, 8 concurrent calls и 16 queued calls; это прежняя общая политика, не новый лимит query.
- Существующие настройки memory active recall остаются конфигурируемыми: default top_k=5, max_prompt_chars=1,500, timeout=70,000ms, max_queries=3. Они применяются после/вокруг recall, не как границы корпуса. Ошибка/timeout не превращаются в частичную страницу с заявлением полного покрытия.
- При изменении прав/источника cursor не фиксирует прежний корпус; изменившуюся большую карточку нужно начать читать заново. Абсолютная стабильность ANN и поколения не обещается.
- Chroma, выбор provider в UI и миграция данных не реализованы. Caller-facing request/scope/page/source contract не требует `.mv2`, frame ID или file generation; эти поля остаются в Memvid adapter. Будущий backend может сам выделять префикс/диапазон и обеспечить lexical fusion, без предположения о native hybrid в обычной Chroma.

## Незапущенные регрессии

Все перечисленные проверки написаны и **не запускались**.

- Memvid `src/vec.rs`: HNSW >1,000 векторов, limit=100/200, запрос больше индекса, zero/empty, короткий достижимый пул одинаковых векторов, clone/decode и неизменность сериализации. ANN-порядок разных префиксов не сравнивается на точное равенство.
- Memvid `tests/search_pagination.rs`: страницы после scope/ACL отбрасывания 1,100 из 1,320 векторных кандидатов; hybrid 100/100/20; lexical scope/ACL, продолжение нескольких snippets одного документа; raw обычный и аналитический запрос; ask combined continuation, дедупликация и timestamp postfilters. Внутренний Tantivy тест помещает 120 авторитетно непригодных документов перед 13 подходящими и проверяет страницы 5/5/3 за первоначальным окном.
- Pioneer `memory/src/thread_episodic.rs`: реальные lexical страницы 3/3/1 при прежнем profile cap=1, segment_index=25; несовместимость provider/model/dimension/normalization; неизвестная persisted vector identity; один prepared vector для двух файлов. Существующая fake-embedder проверка теперь требует ровно один вызов embedding для нескольких файлов.
- `protocol/src/thread_search.rs`: blank query, limit=0/101, длинный query из 100,000 кириллических символов, запрет model-supplied principal, nullable title и typed oversized-card envelope.
- `gateway/message/thread_search_tools/tests.rs`: 18 файлов и 10 бесед; 7/7/4 страницы, каноническая дедупликация между файлами; 105 недоступных кандидатов перед разрешённым; личные беседы Member, чужой источник, текущая authority; реальные committed item IDs/history anchors; отзыв ACL, исключение и удаление. Полное Unicode-название больше 2 MiB восстанавливается по fragment continuation. Сохранённая выдача больше 256 KiB дочитывается точно и повторно авторизуется. Production-bound provider игнорирует подставной principal hint и сохраняет текущий источник.
- `gateway/message/tests/cli_task_tools.rs`: реальная admitted CLI fixture и установленный production recall hook находят другую доступную беседу даже при preflight episodic skip; реальная материализация Agent role intersection ограничивает поиск текущей беседой. Это проверка production binding/role policy, а не полный lifecycle отдельного child task. Replay/recovery фиксированного каталога без search не получает новые полномочия. Защищённый cursor разделяет две полные карточки с названиями по 1,250,000 байт, связывает query и повторно учитывает сужение Agent grants.
- `memory/hooks/recall.rs`: host-owned full-input episodic retrieval заменяет устаревший preflight skip, сохраняя policy no_use.
- `agent/src/chat/mod.rs`: большая Unicode-страница сохраняется полностью, reference появляется после durable ack, типизированный result-reader reference восстанавливает исходный payload.
- `tools/{domain,visibility,permissions}` и `gateway/turn_mcp/native_tools.rs`: скрытость до открытия threads, catalog/action/schema и регистрация native; существующие CLI catalog assertions дополнены новым инструментом.

## Самоаудит и разрешённые проверки

Начальный аудит: прочитаны root `AGENTS.md` Pioneer, Memvid `CLAUDE.md` / `CONTRIBUTING.md`, доступные skill инструкции. Проверены чистота основных checkout, ветки/worktree, фактические main/зависимость/lockfile и доступность pinned Memvid commit. Вложенных AGENTS в затронутых crates не обнаружено. Запрет пользователя имеет приоритет над локальными командами тестирования.

Промежуточный аудит Memvid: перечитаны существующие lexical/vector/ask пути, публичные cursor поля, installed hnsw 0.11.0 contract; изучены all callers и optional feature gates. Возвращены на доработку ложноположительный тест служебного окна и продолжение после ask postfilters. Проверены общий raw retrieval без analytical timeline, source filters/ACL до страницы, отсутствие fixed 64, отсутствие нового публичного метода/формата/sidecar.

Промежуточный аудит Pioneer: просмотрены catalog traversal, source hydration, provider config/identity, trusted authority, canonical dedup, защищённые cursor, byte budget и production provider. Найдены и исправлены необходимость явного native saved-reference envelope для большой страницы, отсутствие source reauthorization для cached search results, потеря execution thread ID в hooks и возможность старого preflight выключить host-owned episodic retrieval. Старые тестовые helpers, оставшиеся без production callers после удаления ask-подмены, убраны вместе с зависящими от них implementation-only проверками.

Финальный статический аудит: перечитан совокупный diff обоих репозиториев, включая новые файлы, регрессии, protocol/schema, все изменённые SearchRequest/векторные callers и provenance literals. Проверены native/CLI shared dispatch, неизменность frozen catalog при replay, durable result envelope/ack и повторное разрешение source authority. Повторный source gate применяется также непосредственно перед выдачей сохранённого результата. Нет нового индекса, sidecar, provider registry, ограничений числа файлов/бесед, query cap или выгрузки векторов в Pioneer. Cargo.toml/lock закрепляют одну локальную Git-ревизию. `cargo fmt ... -- --check` Pioneer, `cargo +1.98.1 fmt --check` Memvid и `git diff --check` завершились с exit code 0. Основные checkout остаются чистыми на исходных commits. Это статический самоаудит; компилируемость не подтверждена.

Внутренние attached tasks применялись по [pioneer-system-subagents SKILL.md](/Users/alexander/.codex/skills/pioneer-system-subagents/SKILL.md); предоставленный [pioneer-system-tasks SKILL.md](/Users/alexander/.codex/skills/pioneer-system-tasks/SKILL.md) прочитан. Они не изменялись. Результаты Memvid (`mpg6WFOd6zTwJHvdqrpSi`) и memory adapter (`k1NX5lUKPa84o3e5c4qDv`) перечитаны и приняты внутренним reviewer после доработок; это не внешнее ревью. Последние финальные тексты исполнителей регистрации (`Nc6760CkvdRgkEyRIJEyV`) и gateway regressions (`tozjvRs1Koyjt7X8og0h6`) прочитаны через их thread history, а изменённые исходники проверены непосредственно родителем. Task Service не сформировал для этих последних текстов review candidates. Задача регистрации отменена как более не нужная после прямого аудита; её последний результат не объявляется принятым кандидатом.

Инфраструктурный блокер закрытия последней подзадачи: `task_wait` продолжает возвращать gateway run `wWfiHnxIjPXP0N472tG3i` как running без candidate, хотя финальный текст уже получен; повторные `task_cancel`/`task_get` отклонены с `MCP turn is not active / session_binding_unavailable`. Закрытие через Task Service не подтверждено. Это не результат тестирования и не блокер подготовки локального кода. Обход через БД Task Service или запуск CLI-агента не выполнялся.

## Команды

Ниже приведены точные значимые команды изменения Git/форматирования/whitespace-проверок. Поиск и чтение исходников выполнялись через `pwd`, `cat`, `sed -n`, `rg`, `rg --files`, `wc -l`, `git status`, `git log`, `git show`, `git diff`, `git worktree list`, `git remote -v`, `git rev-parse` с указанными выше файлами/символами. Исходники редактировались apply_patch, Python текстовыми edit-скриптами и heredoc; код проекта не импортировался и не исполнялся. Этот раздел — журнал значимых операций, а не shell transcript всех read-only запросов.

```text
# Из основных репозиториев — только создание изолированных worktree:
git worktree add -b feat/threads-search /Users/alexander/Code/pioneer/pioneer/.worktrees/threads-search main
git worktree add -b feat/search-pagination /Users/alexander/Code/pioneer/memory-search-pagination fe0b3d9ac883c667587c262f7b372bf06eb605d5

# Pioneer worktree:
cargo fmt -p pioneer-protocol -p pioneer-tools
cargo fmt -p pioneer-protocol
cargo fmt -p pioneer-memory
cargo fmt -p pioneer-gateway -p pioneer-crud -p pioneer-memory -p pioneer-protocol -p pioneer-tools
cargo fmt -p pioneer-gateway -p pioneer-crud -p pioneer-memory -p pioneer-protocol -p pioneer-tools -p pioneer-agent
cargo fmt -p pioneer-gateway -- --check
rustfmt --edition 2024 crates/gateway/src/message/thread_search_tools/tests.rs crates/gateway/src/message/thread_read_tools/tests.rs
cargo fmt -p pioneer-gateway -p pioneer-crud -p pioneer-memory -p pioneer-protocol -p pioneer-tools -p pioneer-agent -- --check
git add Cargo.toml Cargo.lock crates schemas/threads_search_params.json schemas/threads_search_response.json
git diff --cached --check
git -c core.hooksPath=/dev/null commit -m "feat(threads): search indexed conversations and share workspace recall"
git add docs/reviews/threads-search-recall.md
git -c core.hooksPath=/dev/null commit -m "docs: prepare threads search and recall for external review"
git diff 6c1e6b2c49359c76e49624c0545bbafc4a3d310a HEAD --check
git diff --check

# Memvid worktree:
cargo fmt
cargo +1.98.1 fmt
cargo +1.98.1 fmt --check
git diff --check
git diff HEAD^ HEAD --check
git -c core.hooksPath=/dev/null commit -m "fix(search): paginate filtered lexical vector and hybrid retrieval"
git -c core.hooksPath=/dev/null commit -m "fix(search): fill ask pages after postfilters and safely collect ANN candidates"
git diff fe0b3d9 HEAD --check
```

Первый `cargo fmt` Memvid начал rustup download закреплённого toolchain 1.90.0 и был прерван; затем использован установленный formatter 1.98.1. Проект при этом не компилировался. Промежуточный format check мог находить различия во время редактирования; итоговые результаты указаны выше.

Кандидат передаётся на полное внешнее ревью кода. До отдельного разрешения после внешней приёмки запрет тестов/сборок сохраняется.
