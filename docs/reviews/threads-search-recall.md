# Thread search and episodic recall: review and publication record

## Memvid 4.0.0 release (2026-10-24)

The user authorized merging the Memvid PR and publishing the next major version of the fork. PR https://github.com/pioneerdotai/memory/pull/1 is merged into main at `85eed6a081413a047f219813f37ee4bc550b0379`. Release https://github.com/pioneerdotai/memory/releases/tag/v4.0.0 and its tag both point to that merge commit. The package version and its lockfile entry are `4.0.0`.

Pioneer uses `tag = "v4.0.0"` in Cargo.toml, restoring the project's version-tag convention. Cargo.lock names version `4.0.0` and resolves that tag to the merge commit; the manifest's `rev` selector is removed. No other dependency changed.

The existing monitor `G2dk4ksihOwuhHcY6NR6s` is retained for Pioneer PR #174. Its instructions preserve release tags and prohibit replacing the version with an unreleased revision. The Memvid Windows job on the earlier `e0f971d` head failed in file-lock scenarios; Linux and macOS jobs were cancelled by the matrix. Publishing the release does not establish GREEN. The main-branch CI and current Pioneer PR checks remain observable through GitHub.

Commands for this stage: Git status and diff reads, `git fetch origin main`, ancestry checks, `git ls-remote`, GitHub PR/release/tag/job-log queries, a static version and lockfile edit, commits and pushes with hooks disabled, `gh pr merge --merge --match-head-commit`, `gh release create --target`, and `git diff --check`. Local tests, compilation, builds, applications, CLI agents, and generators were not run. Earlier sections are historical records.

## Publication and CI follow-up (2026-10-24)

The user accepted the result and authorized publication. Both existing feature branches are now published:

- Pioneer: https://github.com/pioneerdotai/pioneer/pull/174 (`feat/threads-search`).
- Memvid: https://github.com/pioneerdotai/memory/pull/1 (`feat/search-pagination`).

Pioneer now pins the published Memvid revision `e0f971d667fa6f3b37eaeffa6a08a7a4d6af96a0` in both Cargo.toml and Cargo.lock. The first GitHub CI run found three lint errors in footer lifetime annotations and snapshot fingerprint reads. Commit `e0f971d667fa6f3b37eaeffa6a08a7a4d6af96a0` elides the lifetime, allocates the 64 KiB read buffer on the heap, and propagates an invalid-size error instead of calling `expect`. The new Lint job succeeded; the remaining CI jobs were still running at publication time.

One durable monitor, `G2dk4ksihOwuhHcY6NR6s`, follows both PRs every 15 minutes. It pauses future launches during repairs, pushes fixes to the same branches, checks conflicts with main, and stops permanently after all expected checks succeed on the current heads with no conflicts, or after the primary PR closes. Local builds and tests are forbidden during this follow-up; GitHub CI provides validation.

Publication and repair used `gh pr create`, `gh pr edit --body-file`, `gh pr view`, `gh run view`, the GitHub job-log API, `git fetch origin main`, ancestry checks, `git -c core.hooksPath=/dev/null commit`, and ordinary feature-branch pushes with hooks disabled. The Memvid repair also used `cargo fmt` and `git diff --check`. The dependency revision was edited statically without running Cargo lockfile generation. PR descriptions were edited with the humanizer skill. Earlier review, local-only publication statements, and validation results below describe their historical stages.

Historical review and validation record follows. Publication and the current CI policy are described above.

## Разрешённая проверка всей библиотеки Memvid — 24 октября 2026

Использованы только существующие изолированные worktree:

| Репозиторий | Ветка / worktree | Исходный HEAD этого этапа | Итог |
| --- | --- | --- | --- |
| Memvid | `feat/search-pagination`, `/Users/alexander/Code/pioneer/memory-search-pagination` | `d71c077a8c5144fb4eec99a86e7ca457443e8b68` | `6951277fe9a912fd1343880e2e51a4af21b748bb` |
| Pioneer | `feat/threads-search`, `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-search` | `c273b91dfe625a1c85d08874953fd77c85bcbc81` | dependency commit `c5191250fdd1893fb03afa40c62d6c976d8d9504`; этот отчёт — последующий docs commit |

Проверяемый source tree Memvid: `48f7f0303174d18c41523b00225337dc4e4cd849`, совпадает с tree итогового commit. Cargo.toml и Cargo.lock Pioneer статически согласованы с новым Git commit; другие зависимости не менялись.

В начале оба worktree были чистыми. Основные checkout и соседние worktree не менялись. Закреплённый Rust 1.90.0 восстановлен rustup при первом тестовом запуске. Cargo.lock Memvid не менялся; все тестовые команды использовали `--locked`.

### Найденные ошибки и исправления

1. **Потеря pending WAL на границе sentinel** (`src/io/wal.rs`). На полном корпусе обнаружено исчезновение 22 документов: после 133 insertions оставалось 111 frames. При остатке меньше 48 байт запись sentinel переносила write head в начало даже при pending records. Линейный scanner переставал видеть хвост, а следующий append обходил проверку необходимости WAL growth. Теперь head сохраняет позицию конца региона, пока записи не закоммичены; sentinel не оборачивается через pending tail. Повторное открытие и `records_after` сохраняют эту позицию. Header checkpoint position остаётся нормализованной; формат WAL / `.mv2` не менялся.
2. **Wildcard в Tantivy** (`src/search/tantivy/query.rs`). FST regex отвергал явные anchors из существующего parser regex, и hybrid `(need* OR tag:allowed) AND NOT need*` завершался `LexNotEnabled`. Для FST удаляются только внешние `^` / `$`: этот backend уже сопоставляет целый term. Авторитетный parser/evaluator и Boolean-семантика не менялись. Прежняя behavioral regression теперь проходит, а не пропускается.
3. **Optional inference code не компилировался с закреплённым ort rc.12** (`src/clip.rs`, `src/analysis/ner.rs`). Исправлены обращения к закрытым полям Session/Outlet на действующие методы; Tensor получает прежнюю shape и owned data без зависимости от совпадения версий ndarray. Ровно такой контракт уже использует `src/text_embed.rs`. Версии зависимостей и публичные API не менялись.
4. **Неверные fixtures**, без ослабления production-контрактов. В `tests/mutation.rs` writer явно закрывается перед повторным reader open; прежние тесты ждали недоступный собственный exclusive lock. Поисковые регрессии используют явные URI вместо возвращаемых WAL sequence numbers как frame IDs. Тест пересборки использует постоянный заголовок, чтобы старое слово не оставалось в законно сохраняемой metadata. Для snippet-count fixtures убраны inferred query words и непреднамеренное automatic chunking; zero-page AskRequest содержит обязательный `snippet_chars`. Корпуса 1,320/1,100, URI/ACL фильтры и ожидаемые размеры страниц сохранены. В search-only fixtures отключена не проверяемая ими triplet extraction; тесты instant indexing и triplets остаются в своих владельцах.

### Регрессии и самоаудит

Добавлен один тест WAL `pending_tail_survives_short_sentinel_space_and_reopen`: остаток 0/1/47 байт, видимость pending записи, reopen, отказ от перезаписи и успешное продолжение после checkpoint. Он дополняет существующие append/checkpoint/corruption сценарии. Существующий Tantivy postfilter test сохраняет 133 документа и страницы 5/5/3 с полным набором 13 ожидаемых URI. Доработаны существующие `tests/search_pagination.rs`, `tests/mutation.rs` и rebuild fixture; тесты не удалялись, новых ignore/skip/retry нет.

После каждого исправления проверены владельцы и callers. Фокусированный набор WAL/mutation: **20 passed, 0 failed**; все pagination regressions: **11 passed, 0 failed**. Перечитан полный diff всех девяти изменённых файлов, включая mixed production/test modules. Проверены WAL open/scan/checkpoint/growth callers, две inference модели и существующий text embedder, parser/FST compatibility. Новых публичных методов, формата, поисковых ограничений и изменений Pioneer runtime нет. `cargo fmt --check`, `git diff --check` и `git diff --cached --check` проходят.

### Полные прогоны

Завершены полные unit/integration suites через nextest и отдельные doctests. Все шесть итоговых команд завершились с exit code 0. Исходники после начала этих итоговых прогонов не менялись; commit содержит ровно проверенный source tree. Логи: `/tmp/memvid-default-final.log`, `/tmp/memvid-pioneer-features-final.log`, `/tmp/memvid-extended-features-final.log` и соответствующие `*-doctests*.log`.

| Конфигурация Memvid | Unit/integration | Doctests |
| --- | --- | --- |
| default (`lex,pdf_extract,simd`) | 502 passed / 0 failed / 0 skipped; exit 0 | 1 passed / 0 failed / 17 ignored |
| конфигурация зависимости Pioneer (`lex,temporal_track,simd,vec`, без defaults) | 527 passed / 0 failed / 1 ignored; exit 0 | 1 passed / 0 failed / 18 ignored |
| расширенный CPU набор | 597 passed / 0 failed / 5 ignored; exit 0 | 1 passed / 0 failed / 20 ignored |

Расширенный CPU набор: `vec,clip,tokenizers_esaxx_fast,mmap,pdf_oxide,pdfium,temporal_track,temporal_enrich,parallel_segments,logic_mesh,whisper,replay,encryption,symspell_cleanup,api_embed,hnsw_bench,jsonwebtoken,rustfft,accelerate` плюс default features. Это полные suites для трёх явно указанных конфигураций, не все возможные комбинации flags. Варианты CUDA, Metal и Extractous/GraalVM не запускались. Существующие ignored-тесты требуют model files, внешнего OpenAI API или отсутствующего `legacy_test.mv2`; ignored doctests — прежние примеры документации. Новых исключений не добавлено.

Отдельная существующая граница покрытия: все 14 тестов `tests/xlsx_structured.rs` зависят от не поставляемого в репозитории `/Users/olow/Desktop/memvid-org/arden.xlsx`. Файл отсутствует, и прежние guards возвращаются до assertions. Nextest считает их PASS; это не подтверждает выполнение содержательных XLSX-проверок. Fixture/guards не подменялись, данные не выдумывались.

### Команды этого этапа

Все следующие команды выполнялись только в Memvid worktree; перенаправления сохраняли полные логи в `/tmp/memvid-*.log`.

```text
cargo test --locked --no-fail-fast
cargo test --locked --lib both_full_branches_rebuild_once_and_publish_current_ids_and_vectors -- --nocapture
RUST_BACKTRACE=1 cargo test --locked --lib both_full_branches_rebuild_once_and_publish_current_ids_and_vectors -- --nocapture
cargo test --locked --lib postfilter_rejections_do_not_end_the_initial_document_window -- --nocapture
cargo nextest run --locked --test-threads 2 --no-fail-fast --retries 0 postfilter_rejections_do_not_end_the_initial_document_window
cargo nextest run --locked --test-threads 2 --no-fail-fast --retries 0 -E 'test(io::wal::tests) | test(postfilter_rejections_do_not_end_the_initial_document_window) | binary(mutation)'
cargo nextest run --locked --test-threads 2 --no-fail-fast --retries 0 -E 'test(hybrid_query_fields_filter_semantic_candidates_before_paging) | test(ask_pages_grow_past_duplicate_snippets_before_later_sources)'
cargo nextest run --locked --test-threads 2 --no-fail-fast --retries 0 --test search_pagination
cargo nextest run --locked --test-threads 2 --no-fail-fast --retries 0
cargo nextest run --locked --no-default-features --features lex,temporal_track,simd,vec --test-threads 2 --no-fail-fast --retries 0
cargo nextest run --locked --features vec,clip,tokenizers_esaxx_fast,mmap,pdf_oxide,pdfium,temporal_track,temporal_enrich,parallel_segments,logic_mesh,whisper,replay,encryption,symspell_cleanup,api_embed,hnsw_bench --test-threads 2 --no-fail-fast --retries 0
cargo nextest run --locked --features vec,clip,tokenizers_esaxx_fast,mmap,pdf_oxide,pdfium,temporal_track,temporal_enrich,parallel_segments,logic_mesh,whisper,replay,encryption,symspell_cleanup,api_embed,hnsw_bench,jsonwebtoken,rustfft,accelerate --test-threads 2 --no-fail-fast --retries 0
cargo test --locked --doc
cargo test --locked --no-default-features --features lex,temporal_track,simd,vec --doc
cargo test --locked --features vec,clip,tokenizers_esaxx_fast,mmap,pdf_oxide,pdfium,temporal_track,temporal_enrich,parallel_segments,logic_mesh,whisper,replay,encryption,symspell_cleanup,api_embed,hnsw_bench,jsonwebtoken,rustfft,accelerate --doc
cargo fmt
cargo fmt --check
git diff --check
git diff --cached --check
```

Первый default run и промежуточные full runs со старыми fixtures уже подтвердили ошибки и были остановлены после запуска заменяющих их полных suites на окончательном source tree. Они не объявляются успешными. Для остановки только собственных устаревших процессов выполнено `kill -TERM 92432 98789 98949 95962 99597 84352 89483 93201`; собраны их terminal результаты (143/100/100). Диагностика длительных процессов выполнена `sample 86435 1 -file /tmp/memvid-default-sample.txt` и `sample 92432 1 -file /tmp/memvid-search-pagination-sample.txt`; это наблюдение тестовых процессов, не запуск приложения. Остальные read-only команды: `git status/rev-parse/diff`, `rg`, `cat`, `sed`, `ps`, `tail`, `lsof`, `df`, `du`, `rustup toolchain list`, `command -v`, `xcrun --find metal`, `/usr/libexec/java_home -V`. Редактирование — apply_patch и Python text edits; код Pioneer не выполнялся.

Локальные commits созданы с отключёнными hooks:

```text
# Memvid worktree
git add src/analysis/ner.rs src/clip.rs src/io/wal.rs src/memvid/search/tantivy.rs src/memvid/tantivy_rebuild_tests.rs src/search/tantivy/query.rs src/tests_lex_flag.rs tests/mutation.rs tests/search_pagination.rs
git write-tree
git -c core.hooksPath=/dev/null commit -m "fix: preserve pending WAL tail and repair search test failures" -m "Use supported ort session and tensor APIs for optional inference features. Correct retrieval fixtures without weakening page, filter, or persistence assertions."
# Pioneer worktree (revision заменена Python text edit, без Cargo)
git add Cargo.toml Cargo.lock
git diff --cached --check
git -c core.hooksPath=/dev/null commit -m "chore(memory): pin Memvid WAL and search regression fixes"
git add docs/reviews/threads-search-recall.md
git diff --cached --check
git -c core.hooksPath=/dev/null commit -m "docs: record Memvid library test validation"
```

Итоговый самоаудит: перечитаны полный Memvid diff от `d71c077` и полный Pioneer diff от `c273b91`, включая зависимости и отчёт. Проверены соответствие tree, hooks-disabled commits и согласованность Git revision. После commit отчёта оба выделенных worktree должны быть чистыми; точный docs HEAD Pioneer получается `git rev-parse HEAD` и передаётся в финальном сообщении. Тесты Pioneer, Gateway, приложение, CLI-агенты, генераторы, cargo check/build/clippy/bench/run и CI не запускались. Компиляция выполнялась только как часть разрешённых тестовых команд Memvid.

Новая ревизия Memvid `6951277fe9a912fd1343880e2e51a4af21b748bb` остаётся локальной: push, merge, release, tags, deploy, CI и публикация не выполнялись. Проверка зависимости извне потребует отдельной публикации. Тесты и компиляция Pioneer не подтверждены; внешняя приёмка всей реализации остаётся отдельным этапом.

## Два оставшихся дефекта аудита — URI-prefilter и повторяющиеся предикаты

Перед изменениями фактические HEAD и ветки совпали с заданием; оба worktree были чистыми. Прочитаны Pioneer `AGENTS.md`, Memvid `CLAUDE.md` / `CONTRIBUTING.md`; вложенных AGENTS в затронутых каталогах нет. Требования локальных инструкций запускать проверки уступают прямому запрету пользователя. Основные checkout и соседние worktree не изменялись.

| Worktree / ветка | Исходный commit этого исправления | Итоговый commit кода |
| --- | --- | --- |
| `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-search`, `feat/threads-search` | `3995aae5bfe728efe6596cbf0e6526d6fecbbaec` | `a15214a5051bfc3ad827dd2273654717586d2f42` |
| `/Users/alexander/Code/pioneer/memory-search-pagination`, `feat/search-pagination` | `3adcf08d6c383e0b0183f07ce6c6c6567234bcfe` | `d71c077a8c5144fb4eec99a86e7ca457443e8b68` |

Этот отчёт фиксируется отдельным последующим documentation commit; точный итоговый HEAD Pioneer указан в финальной передаче, без самоссылки SHA в содержимом commit. `Cargo.toml` и `Cargo.lock` согласованно закрепляют `d71c077a8c5144fb4eec99a86e7ca457443e8b68`; другие зависимости не менялись. **Ревизия Memvid не опубликована.** Для внешнего получения зависимости потребуется отдельная разрешённая публикация; fetch/update/генерация lockfile не выполнялись.

### 1. Lexical URI до postfilter

Подтверждена полная цепочка: `try_tantivy_search` передавал `request.uri` в `TantivyEngine::search_documents`, а `QueryPlanner::build_root_query` превращал аргумент в точный `TermQuery`. Поэтому `MV2://WANTED/` отбрасывал `mv2://Wanted/parent#one` до авторитетного общего `uri_matches`.

Изменён только аргумент этого вызова: передаётся `None`, чтобы URI проверял существующий postfilter. `uri_filter` по-прежнему сохраняется для проверки frame metadata и отключает отдельный scope. Сохраняются ASCII case-insensitive prefix без `#`, exact с `#`, фильтрация до выделения страницы и расширение `doc_limit` после отбрасывания кандидатов. Query parser, публичный engine API, query planner остальных callers и формат индекса не менялись; переиндексация не нужна.

После этого исправления перечитан diff вызова и весь участок от расчёта `uri_filter/scope_filter` до завершения окна; подтверждено, что убран только несовместимый coarse filter и конец служебного окна не стал концом выдачи.

### 2. Одна переменная для одинакового текстового условия

Прежний `semantic_truth` независимо возвращал `(true,true)` каждому текстовому вхождению. Так обе стороны `needle AND NOT needle` могли считаться допустимыми одновременно.

Существующий evaluator дополнен общей частичной оценкой текстовых предикатов на том же `Expr`. Ключ word/phrase соответствует их существующему lowercased substring matching; wildcard использует ключ существующего regex. Все одинаковые предикаты используют одно значение. Поля по-прежнему проверяются существующим `FieldTerm::matches`.

Предикат, встречающийся только в одной полярности, сразу получает благоприятное значение; обычные положительные semantic-запросы не требуют перебора. Если предикат встречается и положительно, и под NOT, evaluator итеративно рассматривает false/true с отсечением заведомо ложных ветвей. Это проверка существования согласованного присваивания, а не требование буквального наличия слова в документе. У `(needle OR tag:allowed) AND NOT needle` без тега нет допустимого присваивания; с тегом допустимо needle=false. Итеративный перебор не добавляет глубину стека на каждый текстовый предикат.

После второго исправления статически разобраны обе ветви приведённого выражения, простое и составное противоречия, NOT/двойная полярность, одинаковые word/phrase в разном регистре и wildcard. Проверены все callers `semantic_truth`, `permits_semantic_match`, прежний `evaluate` и место проверки vector-кандидатов до ACL/page slicing. Нового публичного API, языка запросов и зависимости нет.

### Изменённые файлы и незапущенные регрессии

- Memvid `src/memvid/search/tantivy.rs`: снят только точный prefilter аргумента request URI.
- Memvid `src/search/mod.rs`: общие значения повторных текстовых предикатов и внутреннее итеративное разрешение смешанной полярности.
- Memvid `tests/search_pagination.rs`: расширены **существующие** два теста, без отдельного дублирующего fixture.
- Pioneer: только Git revision в `Cargo.toml` / `Cargo.lock` и этот отчёт. Gateway, recall, ANN и пагинация не переписывались.

`hybrid_uri_uses_lexical_prefix_case_and_scope_precedence` теперь содержит 80 более высоких BM25/более близких vector-кандидатов с неподходящими URI перед двумя допустимыми. Сохранены обе ветки lexical/semantic, prefix/case, exact `#`, конфликтующий scope и продолжение страниц. Длиннее текст допустимых документов делает их BM25 ниже коротких неподходящих, поэтому lexical должен пройти первоначальное окно.

`hybrid_query_fields_filter_semantic_candidates_before_paging` сохранён с прежней матрицей tag/date/AND/OR/NOT и страницами 2/2/1 после 80 неподходящих записей. Теперь он проверяет и прямой vector API, и hybrid. Добавлены исходное выражение из аудита, повтор в word/phrase с разным регистром, два взаимосвязанных текстовых предиката и wildcard. Пять допустимых дальних документов не содержат needle; прямой vector вызов проверяет semantic positive control без маскирования lexical-веткой. Самопротиворечия слова, фразы, wildcard и составного выражения возвращают пустой результат без продолжения.

### Финальный статический аудит и ограничения

Перечитан весь совокупный diff этого исправления от указанных исходных commits обоих репозиториев, включая расширенные тесты, связанные callers и revision/lockfile. В Memvid изменены три файла; в Pioneer — два файла зависимости и отчёт. Разбор/нормализация текста, поля и Boolean tree переиспользуются, формат `.mv2` не меняется. Сохранены zero-page error, recall full anchor с сокращением текста, ToolResultView/CLI byte budget и frozen catalog; их исходники не изменялись. Полный охват индексов, ACL, references, однократный embedding и lexical fallback не затронуты. Новых query/corpus caps нет; страницы остаются до 100 fragments и 2 MiB, ANN не фиксируется snapshot-сессией.

Для произвольного Boolean-выражения точная проверка может потребовать в худшем случае экспоненциальный перебор числа различных текстовых предикатов со смешанной полярностью. Это работа над выражением запроса, не ограничение корпуса; искусственный предел/таймаут не добавлен. Однополярные предикаты не ветвятся. Новых backend/framework/dependencies/очередей/индексов нет.

Работа выполнена родителем без новых attached или durable задач: при таком связанном объёме делегирование не требовалось. Предоставленные Tasks/Subagents инструкции не изменялись. Самоаудит не является внешней приёмкой.

### Фактически выполненные команды этого этапа

Read-only аудит использовал `git status --short`, `git rev-parse HEAD`, `git branch --show-current`, `git diff`, `git diff --stat`, `git diff --numstat`, `rg`, `rg --files`, `sed -n`, `cat`, `head`. Редактирование — apply_patch и Python-текстовые правки без импорта/исполнения кода проекта. Значимые команды форматирования и Git:

```text
# Memvid worktree:
cargo +1.98.1 fmt
cargo +1.98.1 fmt --check
git diff --check
git add src/memvid/search/tantivy.rs src/search/mod.rs tests/search_pagination.rs
git diff --cached --check
git -c core.hooksPath=/dev/null commit -m "fix(search): preserve URI postfilters and correlated text predicates"
git diff 3adcf08d6c383e0b0183f07ce6c6c6567234bcfe HEAD --check

# Pioneer worktree:
git diff -- Cargo.toml Cargo.lock
git diff --check
git add Cargo.toml Cargo.lock
git -c core.hooksPath=/dev/null commit -m "fix(memory): pin correlated search predicate and URI fixes"
git add docs/reviews/threads-search-recall.md
git diff --cached --check
git -c core.hooksPath=/dev/null commit -m "docs: record URI and Boolean audit corrections"
git diff 3995aae5bfe728efe6596cbf0e6526d6fecbbaec HEAD --check
```

Formatter/format check и whitespace-проверки завершились успешно. Оба worktree после commits чистые. Push, merge, release, deploy, fetch и CI не выполнялись.

**Тесты, cargo check, компиляция, сборки, приложение, CLI-агенты и генераторы не запускались. Компилируемость и прохождение тестов не подтверждены.** Кандидат передаётся на повторное внешнее ревью; запрет проверок сохраняется до отдельного разрешения после внешней приёмки кода.

## Исправления пяти дефектов внешнего аудита — 2026-10-24

Перед изменениями оба worktree были чистыми, фактические HEAD совпали с заданием. Ветки сохранены. Основные checkout и соседние worktree не изменялись.

| Репозиторий / worktree / ветка | Начало исправлений | Итоговый commit кода |
| --- | --- | --- |
| Pioneer `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-search`, `feat/threads-search` | `596969d8ea5f252c49e8b81c8539b4ecde329501` | `048fec0d2ffdbab04909a8d7db2a683b881a6ee5` |
| Memvid `/Users/alexander/Code/pioneer/memory-search-pagination`, `feat/search-pagination` | `aec587174243b95875c63a91876e3b62498ff234` | `3adcf08d6c383e0b0183f07ce6c6c6567234bcfe` |

Отчёт обновлён отдельным последующим documentation commit; его HEAD указан в финальной передаче. **Новая ревизия Memvid `3adcf08d6c383e0b0183f07ce6c6c6567234bcfe` не опубликована.** На этом этапе `Cargo.toml` и `Cargo.lock` согласованно закрепляли её без path/vendor и без Cargo-генерации lockfile. Для внешнего получения зависимости потребуется отдельная разрешённая публикация.

### Подтверждение и исправление

1. **Обязательные query filters.** В исходном `search_with_embedding` lexical ветка разбирала `SearchRequest.query`, а vector ветка проверяла только отдельные поля `SearchRequest`. В `vec_search_page` добавлен существующий `parse_query` и внутренний `ParsedQuery::permits_semantic_match`. Он переиспользует `FieldTerm::matches`, включая даты, и Boolean tree AND/OR/NOT. Текстовые термы имеют неопределённую истинность; они не становятся обязательным буквальным совпадением. Проверка полей выполняется до ACL/page slicing, существующее увеличение candidate prefix продолжает искать дальние допустимые записи. Новый язык запросов и публичный API не добавлены.
2. **URI.** Исходный vector код требовал регистрозависимое равенство `request.uri` и одновременно применял scope. Existing `uri_matches` перенесён без изменения поведения из Tantivy в общий внутренний helpers: ASCII case-insensitive prefix без `#`, ASCII case-insensitive exact с `#`. Обе ветки используют его; explicit `uri` отключает scope. Семантика отдельных query-parser полей остаётся прежней.
3. **Нулевая страница.** Исходные vector/hybrid/ask paths вычисляли `next_cursor` через `offset + 0`. На публичных границах `search`, vector page и `ask` введена одинаковая ошибка `InvalidQuery { reason: "top_k must be positive" }`. Положительные размеры не получили нового верхнего лимита, low-level `VecIndex::search(limit=0)` не изменён.
4. **Recall budget.** В исходном `episodic_prompt_line` текст занимал весь остаток, затем к нему добавлялась подпись с JSON reference и строка отбрасывалась целиком. Теперь сначала строится полная подпись; её число символов и newline между результатами вычитаются до усечения текста. IDs/anchor не режутся. Непоместившаяся подпись пропускается; предыдущие результаты сохраняются. `truncated` учитывает сокращение текста и пропуск непустого результата. Изменённая сигнатура приватного helper проверена по всем callers.
5. **CLI output projection.** Исходная страница проверялась как raw JSON, а `llm_view_for_policy` затем ограничивал сериализованный `ToolResultView::Json`, заменяя допустимый raw response на preview. Новый приватный `search_page_fits_output` измеряет существующий ToolResultView и через существующий `project_tool_result` фактический `llm_payload`, включая transport metadata. Этот расчёт используется до принятия карточки, при `pageFragment` и в handler. Не поместившиеся карточки остаются за тем же cursor; одна большая карточка читает точный JSON существующим pager. Глобальные output policy, `mcp_service` и native retained-result механизм не менялись.

### Регрессии — не запускались

- `tests/search_pagination.rs::hybrid_query_fields_filter_semantic_candidates_before_paging`: 80 ближайших недопустимых vectors и пять дальних допустимых, страницы 2/2/1; tag, date, отрицательные/составные поля, OR branches и текст внутри NOT. Случай `NOT (semantically OR tag:denied)` использует присутствующее во всех документах слово, чтобы lexical не маскировал semantic regression.
- `hybrid_uri_uses_lexical_prefix_case_and_scope_precedence`: одинаковые lexical/semantic URI области, дочерние URI, ASCII case, конфликтующий scope, exact `#`, продолжение страниц. Semantic query не встречается в тексте.
- `zero_pages_are_rejected_without_nonadvancing_cursors`: непустой индекс, cursor None/0/1, lexical/vector/hybrid и все режимы ask; ожидается явная ошибка.
- `episodic_prompt_budget_preserves_full_anchor_and_truncates_only_text`: contribution budget=400, snippet=360 Unicode-символов, полные production-подобные IDs и JSON anchor; непустой сокращённый текст, общий бюджет и остаток после предыдущего результата.
- `episodic_prompt_skips_unfittable_reference_without_losing_prior_context`: подпись сама не помещается, предшествующий контекст сохраняется, усечение отмечено.
- `admitted_cli_search_preserves_structured_pages_at_projection_byte_boundary`: реальный admitted CLI invocation через existing fixture/TurnMcpInvoker/MCP service/shared runtime projection. Калибровка ставит две raw карточки на границу 2 MiB минус один байт и подтверждает переполнение ToolResultView. Вызов возвращает полное название первой карточки, structured response без preview, `nextCursor`, следующие карточки по cursor и payload/text до 2 MiB. Сохранены query binding и повторная проверка Agent grants. Эта проверка заменяет прежний direct service test с двумя названиями по 1,250,000 байт.

### Статические самоаудиты и команды исправлений

После Memvid перечитаны parser/field matcher, candidate refill, URI priority, публичные zero guards, все три callers `vec_search_page`, feature cfg и полный diff, включая tests. Production patch находится в `f408e59cedfd0c07395d298e94c2b86ce9cee275`, уточнения Boolean-регрессии — в `e97c6099d42701f92f07ab917d27d56ac83e1ebd` и `3adcf08d6c383e0b0183f07ce6c6c6567234bcfe`.

После Pioneer проверены формирование contribution с остаточным бюджетом, классификатор успешного tool outcome, `ToolResultView::bounded_to_bytes`, projection, фактическая CLI доставка и реальный admitted fixture. Нет нового API/provider/index/session, query cap или ограничения корпуса. Полные названия, source authorization, immutable grants, frozen CLI catalog, повторное использование embedding и приблизительная позиционная ANN-пагинация сохранены. Финально перечитан совокупный diff **этого исправления** обоих репозиториев от указанных исходных HEAD, включая тесты, dependency pin и этот отчёт. Новых изменений первоначальной реализации вне пяти дефектов нет.

Для исправлений использованы attached tasks `GgI9VqujnThKdccWEz7hZ` (Memvid) и `7PtiRG0FT4lQ9hcpYl8d0` (recall). Родитель прочитал точные candidates через `task_result`, просмотрел исходники; Memvid-регрессия дважды уточнена. Recall candidate принят внутренним reviewer. Последний финальный текст Memvid сохранён в истории дочернего треда, но Task Service не сформировал candidate этого раунда. Родитель прочитал текст и сам перечитал весь diff, включая последнюю правку; дополнительная работа исполнителя больше не нужна. Подзадача Memvid отменена через Task Service (receipt `Rbd4c300a6c65b52c7a72`), без заявления о принятии отсутствующего candidate. Финальный `task_wait` подтвердил обе задачи терминальными, pending=0 и reviewRequired=0; вложенная ошибка отмены class=cancelled/code=conflict относится к task lifecycle. Самоаудит и внутреннее принятие recall не являются повторной внешней приёмкой.

Точные значимые команды этого этапа (read-only инспекция — `git status --short`, `git rev-parse HEAD`, `git branch --show-current`, `git log`, `git diff`, `rg`, `sed`, `cat`; текстовые edits через apply_patch/Python, без исполнения кода проекта):

```text
# Memvid:
cargo +1.98.1 fmt
cargo +1.98.1 fmt --check
git diff --check
git -c core.hooksPath=/dev/null commit -m "fix(search): enforce semantic query filters and consistent page boundaries"
git -c core.hooksPath=/dev/null commit -m "test(search): cover unknown text in Boolean filter branches"
git -c core.hooksPath=/dev/null commit -m "test(search): isolate semantic unknown negation regression"
git diff aec587174243b95875c63a91876e3b62498ff234 HEAD --check

# Pioneer:
cargo +1.98.1 fmt -p pioneer-memory
cargo +1.98.1 fmt -p pioneer-memory -- --check
cargo fmt -p pioneer-gateway
cargo fmt -p pioneer-gateway -p pioneer-memory -- --check
git diff --check
git add Cargo.toml Cargo.lock crates/gateway/src/message/thread_search_tools.rs crates/gateway/src/message/tests/cli_task_tools.rs crates/memory/src/hooks/active_recall.rs crates/memory/src/hooks/tests/episodic_recall.rs
git diff --cached --check
git -c core.hooksPath=/dev/null commit -m "fix(threads): preserve search pages and recall within output budgets"
git add docs/reviews/threads-search-recall.md
git -c core.hooksPath=/dev/null commit -m "docs: record five search audit fixes for external review"
git diff 596969d8ea5f252c49e8b81c8539b4ecde329501 HEAD --check
```

Форматирование и whitespace-проверки выполнены успешно. Верхние ограничения остаются прежними: до 100 fragments и 2 MiB представления страницы; обычные слова query не получают нового ограничения длины. Low-level продолжения oversized records и native retained result используют прежние quanta, описанные ниже. Абсолютная стабильность ANN не обещается. Глобальные бюджеты recall и остальных tools не повышались.

**Тесты, cargo check, компиляция, сборки, приложение, CLI-агенты и генераторы не запускались. Компилируемость и прохождение тестов не подтверждены.** Все новые и ранее написанные тесты остаются незапущенными до отдельного разрешения после внешней приёмки. Кандидат передаётся на повторное внешнее ревью.

## Исходный этап: репозитории и ревизии

| Репозиторий | Рабочая ветка | Изолированное рабочее дерево | Исходный commit | Итоговый commit кода |
| --- | --- | --- | --- | --- |
| Pioneer | `feat/threads-search` | `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-search` | `6c1e6b2c49359c76e49624c0545bbafc4a3d310a` | `7b11f2a145ef692937e8b87a001c2c2c8e7a3af4` |
| Memvid, существующий форк | `feat/search-pagination` | `/Users/alexander/Code/pioneer/memory-search-pagination` | `fe0b3d9ac883c667587c262f7b372bf06eb605d5` | aec587174243b95875c63a91876e3b62498ff234 |

Pioneer commit в таблице содержит реализацию и незапущенные регрессии. Этот отчёт добавлен следующим отдельным documentation commit в той же ветке; его точный HEAD передаётся в финальном сообщении, чтобы избежать самоссылки SHA в содержимом commit. На момент первого этапа Memvid история содержала два локальных commits: `da735d06816032c718df419fedd9fe4c23ada633` и итоговый `aec587174243b95875c63a91876e3b62498ff234`.

При начале основной checkout Pioneer был чистым, `main` и локальная ссылка `origin/main` указывали на `6c1e6b2c49359c76e49624c0545bbafc4a3d310a`, уже после указанного в задании `fdc1b069df0fdca80690e759b596a2626f5619da`. Основной checkout Memvid был чистым на `main` = `b827f174eab60bcfe3deb8412f507e4990d4c341`; origin — `https://github.com/pioneerdotai/memory.git`. Работа Memvid начата с закреплённой в Pioneer версии 3.1.8, а не со старого main. Соседние ветки и worktree не очищались и не использовались как база поиска.

На первом этапе Pioneer `Cargo.toml` и `Cargo.lock` статически переведены с tag `v3.1.8` / `fe0b3d9ac883c667587c262f7b372bf06eb605d5` на одну и ту же Git-ревизию aec587174243b95875c63a91876e3b62498ff234. Версия пакета остаётся 3.1.8. Абсолютной path-зависимости, vendor-копии или второго форка нет.

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

При достижении 2 MiB сериализованного ToolResultView или фактического CLI payload не поместившиеся источники остаются за `nextCursor`. Если одна карточка с обёрткой не помещается в 2 MiB из-за полного названия, существующий exact-result pager отдаёт её JSON через `pageFragment.text`; части конкатенируются до `complete=true`, затем тот же защищённый cursor продолжает общую выдачу. Версия карточки и текущий источник проверяются повторно. Название не усекается.

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

- 1–100 найденных фрагментов на обычную страницу; группировка по беседам не меняет счёт. Сериализованный ToolResultView и фактический CLI payload не больше 2,097,152 байт каждый; размер обёртки измеряется непосредственно, без фиксированного запаса.
- Используется существующий профиль engine snippet: 360 символов вокруг совпадений; у vector snippet сохраняется library minimum 80 символов. Полное сообщение доступно через history anchor.
- Внутренний каталог и кандидаты читаются порциями 100 без максимума числа порций, файлов или бесед.
- Oversized-card continuation и saved-result reader переиспользуют существующий pager с квантами до 32,768 байт / 4,096 токенов. Это продолжение точного текста, а не потеря остатка и не общий token cap страницы поиска.
- Native working-context excerpt имеет существующий бюджет 65,536 байт / 8,192 токена; перед его выдачей полная поисковая страница подтверждённо сохраняется. Глобальные лимиты других инструментов не менялись.
- Новый query-specific максимум отсутствует. У CLI сохраняется общий admission limit аргументов инструмента 131,072 байта, 8 concurrent calls и 16 queued calls; это прежняя общая политика, не новый лимит query.
- Существующие настройки memory active recall остаются конфигурируемыми: default top_k=5, max_prompt_chars=1,500, timeout=70,000ms, max_queries=3. Они применяются после/вокруг recall, не как границы корпуса. Ошибка/timeout не превращаются в частичную страницу с заявлением полного покрытия.
- При изменении прав/источника cursor не фиксирует прежний корпус; изменившуюся большую карточку нужно начать читать заново. Абсолютная стабильность ANN и поколения не обещается.
- Chroma, выбор provider в UI и миграция данных не реализованы. Caller-facing request/scope/page/source contract не требует `.mv2`, frame ID или file generation; эти поля остаются в Memvid adapter. Будущий backend может сам выделять префикс/диапазон и обеспечить lexical fusion, без предположения о native hybrid в обычной Chroma.

## Исторический перечень регрессий до разрешения тестов Memvid

На момент первичного внешнего ревью перечисленные проверки были написаны и не запускались. Актуальные результаты Memvid приведены в начале отчёта; тесты Pioneer по-прежнему не запускались.

- Memvid `src/vec.rs`: HNSW >1,000 векторов, limit=100/200, запрос больше индекса, zero/empty, короткий достижимый пул одинаковых векторов, clone/decode и неизменность сериализации. ANN-порядок разных префиксов не сравнивается на точное равенство.
- Memvid `tests/search_pagination.rs`: страницы после scope/ACL отбрасывания 1,100 из 1,320 векторных кандидатов; hybrid 100/100/20; lexical scope/ACL, продолжение нескольких snippets одного документа; raw обычный и аналитический запрос; ask combined continuation, дедупликация и timestamp postfilters. Внутренний Tantivy тест помещает 120 авторитетно непригодных документов перед 13 подходящими и проверяет страницы 5/5/3 за первоначальным окном.
- Pioneer `memory/src/thread_episodic.rs`: реальные lexical страницы 3/3/1 при прежнем profile cap=1, segment_index=25; несовместимость provider/model/dimension/normalization; неизвестная persisted vector identity; один prepared vector для двух файлов. Существующая fake-embedder проверка теперь требует ровно один вызов embedding для нескольких файлов.
- `protocol/src/thread_search.rs`: blank query, limit=0/101, длинный query из 100,000 кириллических символов, запрет model-supplied principal, nullable title и typed oversized-card envelope.
- `gateway/message/thread_search_tools/tests.rs`: 18 файлов и 10 бесед; 7/7/4 страницы, каноническая дедупликация между файлами; 105 недоступных кандидатов перед разрешённым; личные беседы Member, чужой источник, текущая authority; реальные committed item IDs/history anchors; отзыв ACL, исключение и удаление. Полное Unicode-название больше 2 MiB восстанавливается по fragment continuation. Сохранённая выдача больше 256 KiB дочитывается точно и повторно авторизуется. Production-bound provider игнорирует подставной principal hint и сохраняет текущий источник.
- `gateway/message/tests/cli_task_tools.rs`: реальная admitted CLI fixture и установленный production recall hook находят другую доступную беседу даже при preflight episodic skip; реальная материализация Agent role intersection ограничивает поиск текущей беседой. Это проверка production binding/role policy, а не полный lifecycle отдельного child task. Replay/recovery фиксированного каталога без search не получает новые полномочия. После аудита проверка границы заменена настоящим admitted CLI invocation: размер названия калибруется так, чтобы две исходные карточки занимали 2,097,151 байт, но превышали лимит после обёртки. Первая полная карточка доставляется структурированно, остальные читаются по cursor; сохраняются проверки query binding и сужения Agent grants.
- `memory/hooks/recall.rs`: host-owned full-input episodic retrieval заменяет устаревший preflight skip, сохраняя policy no_use.
- `agent/src/chat/mod.rs`: большая Unicode-страница сохраняется полностью, reference появляется после durable ack, типизированный result-reader reference восстанавливает исходный payload.
- `tools/{domain,visibility,permissions}` и `gateway/turn_mcp/native_tools.rs`: скрытость до открытия threads, catalog/action/schema и регистрация native; существующие CLI catalog assertions дополнены новым инструментом.

## Самоаудит и разрешённые проверки

Начальный аудит: прочитаны root `AGENTS.md` Pioneer, Memvid `CLAUDE.md` / `CONTRIBUTING.md`, доступные skill инструкции. Проверены чистота основных checkout, ветки/worktree, фактические main/зависимость/lockfile и доступность pinned Memvid commit. Вложенных AGENTS в затронутых crates не обнаружено. Запрет пользователя имеет приоритет над локальными командами тестирования.

Промежуточный аудит Memvid: перечитаны существующие lexical/vector/ask пути, публичные cursor поля, installed hnsw 0.11.0 contract; изучены all callers и optional feature gates. Возвращены на доработку ложноположительный тест служебного окна и продолжение после ask postfilters. Проверены общий raw retrieval без analytical timeline, source filters/ACL до страницы, отсутствие fixed 64, отсутствие нового публичного метода/формата/sidecar.

Промежуточный аудит Pioneer: просмотрены catalog traversal, source hydration, provider config/identity, trusted authority, canonical dedup, защищённые cursor, byte budget и production provider. Найдены и исправлены необходимость явного native saved-reference envelope для большой страницы, отсутствие source reauthorization для cached search results, потеря execution thread ID в hooks и возможность старого preflight выключить host-owned episodic retrieval. Старые тестовые helpers, оставшиеся без production callers после удаления ask-подмены, убраны вместе с зависящими от них implementation-only проверками.

Финальный статический аудит: перечитан совокупный diff обоих репозиториев, включая новые файлы, регрессии, protocol/schema, все изменённые SearchRequest/векторные callers и provenance literals. Проверены native/CLI shared dispatch, неизменность frozen catalog при replay, durable result envelope/ack и повторное разрешение source authority. Повторный source gate применяется также непосредственно перед выдачей сохранённого результата. Нет нового индекса, sidecar, provider registry, ограничений числа файлов/бесед, query cap или выгрузки векторов в Pioneer. Cargo.toml/lock закрепляют одну локальную Git-ревизию. `cargo fmt ... -- --check` Pioneer, `cargo +1.98.1 fmt --check` Memvid и `git diff --check` завершились с exit code 0. Основные checkout остаются чистыми на исходных commits. Это статический самоаудит; компилируемость не подтверждена.

Внутренние attached tasks применялись по [pioneer-system-subagents SKILL.md](/Users/alexander/.codex/skills/pioneer-system-subagents/SKILL.md); предоставленный [pioneer-system-tasks SKILL.md](/Users/alexander/.codex/skills/pioneer-system-tasks/SKILL.md) прочитан. Они не изменялись. Результаты Memvid (`mpg6WFOd6zTwJHvdqrpSi`) и memory adapter (`k1NX5lUKPa84o3e5c4qDv`) перечитаны и приняты внутренним reviewer после доработок; это не внешнее ревью. Последние финальные тексты исполнителей регистрации (`Nc6760CkvdRgkEyRIJEyV`) и gateway regressions (`tozjvRs1Koyjt7X8og0h6`) прочитаны через их thread history, а изменённые исходники проверены непосредственно родителем. Task Service не сформировал для этих последних текстов review candidates. Задача регистрации отменена как более не нужная после прямого аудита; её последний результат не объявляется принятым кандидатом.

Инфраструктурный блокер закрытия последней подзадачи устранён после восстановления runtime binding. Ранее `task_wait` возвращал gateway run `wWfiHnxIjPXP0N472tG3i` как running без candidate, а `task_cancel`/`task_get` отклонялись с `MCP turn is not active / session_binding_unavailable`. После восстановления выполнены `task_get`, `task_wait` и разрешённый `task_cancel(scope=task_only)`: дополнительная работа исполнителя больше не нужна, его финальные исходники уже проверены напрямую и включены в commit кода. Отмена подтверждена Task Service (receipt `R164feca0d377e023ff46`); отсутствующий последний candidate не объявляется принятым.

Итоговая сверка `task_list(parentTaskId=fEfVJFNzxAECTEu8oLi7a)` и `task_wait` всех четырёх run IDs подтвердила: Memvid и adapter — completed/succeeded; registration и gateway regressions — cancelled; `terminalCount=4`, `pendingCount=0`, `reviewRequiredCount=0`, `timedOut=false`. Терминальные ошибки отменённых задач имеют class=cancelled с вложенным code=conflict; это состояние управления задачами, а не ошибка сборки или теста. Все созданные attached-подзадачи разрешены. Обход через БД Task Service или запуск CLI-агента не выполнялся.

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
git -c core.hooksPath=/dev/null commit -m "docs: reconcile terminal search implementation tasks"
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

Историческая передача: кандидат передавался на полное внешнее ревью кода с запретом тестов/сборок. Последующее отдельное разрешение для Memvid и его результаты описаны в начале отчёта; запрет для Pioneer сохраняется.
