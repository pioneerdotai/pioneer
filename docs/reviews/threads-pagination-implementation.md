# Threads pagination implementation review

Base: `afe4e51b5e69bbe6a2c90b148462f224dab54a00` (local main and original checkout HEAD).
Branch: `fix/threads-unbounded-pagination`.
Worktree: `.worktrees/threads-unbounded-pagination`.
Original main checkout clean. Existing worktrees inspected and left untouched.

## Pre-edit map

| Tool / source | Existing limit | Lower layer | Agreed change |
| --- | --- | --- | --- |
| threads_list / children | 28 KiB / 2048 tokens, 32 KiB / 4096 tokens final | metadata helper, handler, projection, native result excerpt | Pages sized to existing 2 MiB projection, intact continuation; keep 100 items and 200 candidate quantum |
| history | 28 KiB / 2048 tokens, 4096-byte fragments | history_events_fit, cursor reservation, handler, native excerpt | Whole records when possible; capacity-sized UTF-8 JSON fragments; keep 100 row quantum, snapshot and anchors |
| titles / previews | 256 title chars / 160 preview chars | thread_read_metadata | Full title with continuation for oversized metadata; retain preview cap with explicit flag |
| tools_result_read | 4096 tokens / 32768 bytes | three readers plus compaction::results::result_page | Caller budgets optional, no old clamps; preserve unrelated result_page callers |
| saved result ACL | 32 pages / 256 KiB / 16 wrapper levels | saved_thread_read_result_disclosable, saved_read_response | Separate exact canonical source ownership from incremental source ACL inspection; no full-result assembly |
| start_options | unpaged arrays, 128 identities / 256 profiles | workspace launch catalog, CRUD catalog, lineage query, grant validation, saved ceiling | Remove only these cardinality ceilings; page every available option; retain immutable snapshot and exact launch checks |
| identity_odels_list | unpaged nested executor/model arrays | model metadata and ceiling intersection, shared projection | Continuation through nested models, source changes explicit |
| delivery | 2 MiB view + smaller native excerpt | ToolResultView, chat result persistence | Retain 2 MiB guard; account for envelope and continuation before projection; bypass unrelated excerpt only for bounded thread pages |

No tests, builds, application, Gateway, CLI agents, generators or hooks will be run. Only source inspection, edits, Git and cargo fmt are authorized. Root AGENTS.md read; database scheduling and bounded queries retained. User's explicit prohibition overrides repository test instructions. Tasks/Subagents skills read; their sources and policy blocks remain untouched. Test-audit authoring guidance considered only for test edits; user explicitly requires production changes and forbids executing tests.

## Реализованное поведение

| Прежнее ограничение | Новое поведение |
| --- | --- |
| 2048 токенов / 28 KiB содержимого, 4096 токенов / 32 KiB полного ответа | Страницы рассчитываются по существующей проекции 2 * 1024 * 1024 байт. Считаются JSON escaping, envelope, EOF и публичные курсоры. Никакого малого токенного default вместо них не добавлено. |
| История: фрагмент 4096 байт | Помещающаяся запись возвращается целиком; большая запись — канонические JSON-фрагменты максимальной вместимости на границах UTF-8, с digest и byte offsets. |
| Title 256 символов | Полный title. Очень большие metadata-записи читаются фрагментами с тем же threadId; после объединения получается исходный JSON с полным title. |
| Необозначенное сокращение preview | До 160 Unicode-символов, `previewTruncated` явно сообщает сокращение сохранённого preview инструментом. `titleTruncated=false` сохранено для совместимости. |
| Saved-result ACL: 32 страницы / 256 KiB / 16 wrapper levels | Отдельный canonical ownership lookup по workspace/thread/turn/tool item, без десериализации output/storage; потоковый разбор только известных wrappers и source references; каждый источник повторно авторизуется. |
| Вложенные сохранённые reader results | Проверяются права каждого источника и точное соответствие сохранённого текста каноническому источнику с исходного offset. Подмена источника другим доступным результатом не разрешает старые private metadata. |
| Внутренний fragment выдавался как конец публичной страницы | Публичная result-страница заполняется ограниченными source reads до вместимости ответа. Внутренний quantum не становится потолком страницы или всего результата. |
| 128 identities / 256 profiles | Численные потолки удалены из создания grants, валидации, исходного CRUD-каталога, настройки CLI identities и refresh. Admission snapshot остаётся immutable и проверяется при восстановлении/выборе. |
| Обрезанный lineage launch targets | Все lineage rows доступны через внутренние keyset-порции по 200. Удалён запрос с общим пределом 128. |
| Непостраничные launch/model catalogs | `limit` (default 100), `cursor`, `nextCursor`, `eof`, `fragments`. Продолжение проходит все коллекции и каждый model внутри executor. Profiles, ранее скрывавшиеся презентационным фильтром default-модели, доступны на последующих страницах. |
| Второе сокращение уже сформированной страницы | Для шести перечисленных инструментов сохранение и native replay сохраняют страницу. Generic result excerpt и защитная проекция остальных инструментов сохранены. CLI получает ту же общую проекцию. |

`compaction::results::result_page` сохраняет прежние defaults для остальных потребителей. Новый узкий `result_page_in_budget` используется thread reader; он не применяет старые page clamps. Существующие excerpt budgets других инструментов не изменены. Предел 2 MiB называется ограничением проекции Pioneer, а не доказанным физическим пределом MCP/CLI.

## Продолжение, стабильность и память

История сохраняет newest-to-oldest представление, anchor `{turnId,itemId?}`, обе стороны, snapshot/revision fence, child/tool/source references и восстановление UTF-8 JSON. Size check использует реальные длины двух зашифрованных курсоров и EOF конкретной страницы. Окончание большого фрагмента проверяется отдельно, поскольку EOF убирает continuation framing.

List/children cursors содержат позицию, небольшой fragment offset/digest и fingerprint metadata/lineage workspace. Изменения явно требуют restart. Сам fingerprint вычисляется ограниченными запросами по 200; reader/transaction между запросами и вызовами не удерживается. DFS хранит только ancestor path в памяти и постоянного размера позицию/hash в курсоре; общего максимума страниц или глубины нет. Непросмотренные скрытые кандидаты не превращаются в EOF. Пустая nonterminal страница допустима.

Launch catalog cursor хранит fingerprint и координаты field/index/model/offset; каталог или ранее накопленные результаты в него не попадают. Fingerprint сериализуется прямо в digest, без отдельной полной JSON-копии. Страница собирается из отдельных элементов, без клонирования полного executor.models. Изменённый каталог требует restart. В models response постоянен `identityId`; `identity` присутствует на своём сегменте или читается через fragments. Executor metadata может повторяться на соседних страницах; models продолжаются без повторов/пропусков при неизменном fingerprint.

Для metadata/history/catalog fragments нужно соединять `text` в порядке byte offsets и затем декодировать JSON. `fragment.eof` относится к записи; верхний `eof` относится к операции. Result-reader cursor offset сохраняет прежние Unicode scalar semantics. Saved stdout/progress logs сохраняют порядок immutable ordinal chunks и EOF текущего сохранённого лога.

ACL inspection и сравнение cached reader page работают потоково. В памяти находятся текущие ограниченные страницы/источники, структурный путь JSON и один reference; весь сохранённый ответ не собирается. История загружает до 100 metadata records и одну обрабатываемую запись; полный список истории/результатов/дерево не загружается. Существующий immutable launch snapshot по-прежнему хранит все admitted identities/profiles: это прежний механизм authority, а не новый каталог полномочий.

CLI identity reconciliation теперь проверяет configured membership после bounded fetch, вместо SQL NOT IN со всем конфигурационным каталогом. Это предотвращает превращение числа SQL bind parameters в новый потолок identities. Retirement revalidates source ID/status in the update; writer scheduling/transaction boundaries не менялись.

## Оставшиеся ограничения

- Существующая проекция одного ответа: `2 * 1024 * 1024` байт; общий guard других инструментов сохранён.
- Публичные страницы: максимум 100 элементов там, где он был; launch catalogs используют такой же максимум и default 100 (identity header, model и элементы коллекций считаются сегментами страницы).
- Cursor: максимум 16384 байт; конфиденциальность и аутентификация ChaCha20-Poly1305, контекст доступа/параметры, текущие ACL и immutable execution grants.
- Preview: максимум 160 Unicode-символов с явным флагом.
- Внутренние history/list/lineage quanta: 100/200; canonical saved source read — существующие 16384 Unicode scalars, агрегируемые в публичную страницу; identity sync — существующие порции 256. Это не общие пределы данных/страниц.
- Optional caller `max_tokens`/`max_bytes` saved-result reader могут уменьшить страницу по явному запросу. Defaults 4096/32768 и их clamps удалены. Физически не помещающееся обрамление даёт явную ошибку.
- Skills и MCP servers в grants: прежние 256/256; остальные несогласованные permission/payload/resource/graph ограничения не удалялись. Существующий persisted output-log chunk frame limit 128 KiB также сохранён.
- Несогласованные лимиты запуска сохранены: route graph до 2048 edges и прежняя проверка глубины routes 8; work graph concurrency 4096, queue depth 2048, depth 64, fan-out 128, total nodes 4096. Они ограничивают исполнения/маршруты, а не количество страниц чтения или доступных identities/profiles.
- Никаких общих пределов числа identities, execution profiles, страниц или глубины обхода descendants не добавлено.

## Immutable authorization boundary

Новые исполнения получают полный текущий разрешённый snapshot вместо количественно обрезанного. Старый snapshot при восстановлении не пополняется текущим каталогом. `current_saved_child_launch_catalog` начинает с persisted ceiling; `intersect_child_launch_ceiling` сохраняет same-source и no-wider проверки. Поля и fingerprint wire grants v2 не менялись, v1 migration и проверка внешнего persisted fingerprint сохранены. Численные ограничения skills/MCP остаются.

Явный запуск по-прежнему проходит identity/profile compatibility, executor/provider/model/reasoning, current initiating principal, permission cap, workspace/capsule/route scope и commit-time immutable grant validation. Source lookup saved results доказывает точную принадлежность workspace/thread/turn/item, исключает удалённые turn и non-tool records; один result ID, чужая ссылка или доступ к соседнему результату не являются разрешением. Повторная проверка current source ACL и canonical revision stamp выполняется перед выдачей результата.

## Контракты и курсоры

- History cursor state: v3; старые list/history/children AEAD scopes несовместимы с `thread-read-v2` и требуют нового чтения.
- Saved-result tool cursor сохраняет объект `{version,offset}`, но `version` теперь opaque AEAD token, связывающий source version и offset с контекстом/точным источником/параметрами. Старые tool cursors отвергаются с restart; внутренний/direct canonical reader сохраняет raw source cursor.
- Launch/model catalogs: новые optional inputs limit/cursor; закрытые input schemas сохраняются. Новый cursor scope привязан к workspace/thread/turn/tool/identity/limit. Explicit stale/invalid catalog code `agent_catalog_cursor_invalid_or_stale_restart` проходит существующую sanitization без raw errors/IDs.
- Обновлены Rust wire types, schema registry, 8 JSON schema documents и непосредственные tool descriptions. Schemas отредактированы вручную: запрещённый генератор не запускался.
- Имя `threads_identity_odels_list`, visibility/opening policy, общая native/CLI логика сохранены. Tasks/Subagents skills и их policy blocks не изменялись.

## Регрессии: написаны/обновлены, НЕ запускались

- `thread_read_tools/tests.rs`: цельные сообщения 5000 байт и выше прежних token/byte budgets; большая input-запись >2 MiB с точным восстановлением и stale при edit; полный длинный title, preview flag; 205 list entries; большой отдельный title; stale при metadata edit; существующий DFS >100 descendants дополнен stale при reparent ещё не посещённого descendant.
- `thread_read_tools/anchor_tests.rs`: большой Unicode item >2 MiB в обоих направлениях; существующие anchor/tie/snapshot/ACL сценарии сохранены.
- Saved observer regression расширена: source >2 MiB, >32 старых внутренних порций, >256 KiB, 24 wrapper levels, 180-level retained storage без его десериализации для ACL, отказ чужого canonical tuple и stale private cache после замены source.
- `thread_result_sources`: escaped/вложенные wrappers, Unicode references, исключение пользовательского message JSON из ACL traversal.
- `thread_launch_pages`: 140 identities, 300 profiles, 205 destinations; mutation/context mismatch; 205 nested models, empty executor, большой model entry и точное восстановление.
- Protocol grants: round-trip 140 identities / 300 profiles и tampering rejection.
- CRUD identity sync: 260 runtime identities, paging и retirement через границу внутренней порции.
- CRUD persisted execution grant: восстановление полного authenticated snapshot без widening; отказ внешней identity.
- Gateway launch ceiling: все admitted choices сверх прежних потолков остаются, новые catalog entries исключаются; явный выбор profile #300 разрешён, чужой отклоняется.
- Tools projection: крупная страница и cursor сохраняются в общей CLI/native проекции; overflow 2 MiB отвергается расчётом.
- Native retained/replay: thread page не сокращается generic result excerpt, включая запрос восстановления с прежним малым token budget.

## Статический самоаудит и ограничения проверки

После логических блоков перечитаны diff и прямые callers: read handlers/metadata/history; shared result_page/response projection/native persistence/replay/MCP delivery; grant creation/load/migration/selection; CRUD identity/lineage; source authorization/cached readers; wire schemas и тестовые fixtures. Обнаруженные скрытые ограничения устранены в соответствующих нижних слоях. В частности, internal source quantum перестал быть публичным потолком, а generic native excerpt перестал повторно сокращать сформированную thread page.

Финально проверены сигнатуры и reexports, JSON serialization/restoration, cursor protection, отсутствие прежних численных caps в затронутых paths, bounded queries и release reader capacity, отсутствие зависимостей/vendor/forks/search/episodic/skills изменений. JSON syntax/local refs проверены отдельно. Это статическое чтение, не подтверждение компиляции или выполнения тестов.

Известные ограничения для внешнего аудита: fingerprint list/children требует полного bounded metadata scan workspace перед/после страницы (стоимость O(workspace size)); ACL inspection больших сохранённых observer результатов повторяется перед выдачей, а reader chains дополнительно сравниваются с canonical source. Производительность не измерялась. End-to-end native/CLI transport не запускался. Соответствие вручную обновлённых schemas фактическому выводу генератора требует отдельной разрешённой проверки. Компиляция, runtime SQL и тесты не проверены.

Разрешённые выполненные операции: чтение `cat`, `sed`, `rg`, `wc`, `ls`; локальные статические правки `python3`, `mkdir`; Git status/rev-parse/log/branch/worktree/diff/config/ls-files, создание worktree/ветки, stage/commit с отключёнными hooks. `cargo fmt --all` и `cargo fmt --all -- --check`; `git diff --check` / staged whitespace check; статический Python JSON parse/local-$ref check. Первый fmt check обнаруживал требуемое форматирование. Финальный `cargo fmt --all -- --check` завершился с exit code 0; `git diff --check` — exit code 0; статический JSON parse/local-$ref check 8 schemas — без ошибок.

Никакие тесты, cargo check/build/clippy/test/nextest/run, приложения, Gateway, CLI agents, компилирующие/исполняющие project generators, hooks, merge/push/deploy не выполнялись. Точный финальный commit и полный перечень изменённых файлов будут в handoff report, сформированном после commit как отдельный REVIEW_REPORT.md в этом worktree (вне implementation commit), чтобы не создавать самоссылку commit hash.
