# Повторный статический аудит OpenCode V2 — R2

Дата: 2026-10-16. Результат: текущая реализация принята на уровне статического аудита. R1-01–R1-04 закрыты по коду; новых подтверждённых findings не обнаружено. Runtime-работоспособность и компиляция пока не подтверждены.

## Проверенная рабочая копия

- Backend worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/opencode-v2`, ветка `feature/opencode-v2`.
- HEAD и включённый локальный main: `f2d493c672aa58a184f800b4b695e289ce0f6b50`.
- Незакоммиченный diff к HEAD: 13 файлов, +3680/-2581 строк до добавления этого отчёта.
- Отдельно прочитан сохранённый `notes/opencode-v2-audit-r1.md`; он не является продуктовым изменением и не редактировался.
- App HEAD: `9d631c87e05485126c51ab77939802c131418a67`. В App по-прежнему только чужое изменение `bun.lock`; аудитор его не менял.
- Продуктовый код не изменён аудитом. Добавлен только этот отчёт.

## Покрытие

Повторно прочитаны все текущие изменения к HEAD, включая полный `opencode_session.rs` (5977 строк), оба JS-файла, все изменения Rust-тестов и полный отчёт реализации. Для каждого изменённого файла зафиксированы строки и SHA-256 ниже. Чтение тестов не означает их запуск.

Прослежены связанные неизменённые пути: manager/recovery acquisition, durable projector/command output policy, request answer delivery, общая установка и выбор skills, instruction projection, TokenUsage и usage journal. Контракты сверены с опубликованными API/OpenAPI V2 и официальным upstream snapshot `4617210822bdbb31e5749751afbfb4584ccedac8` в `/tmp/pioneer-opencode-v2-sources/upstream`. Проверены native Effect plugin/module loading, host runtime imports, registry update/reload, Tool.Error и decode-before-execute. Snapshot — источник доказательств, не version gate.

| Файл diff | Строк | SHA-256 проверенного содержимого |
| --- | ---: | --- |
| `crates/cli-agent-runtime/src/instructions.rs` | 137 | `96ce40edbf40e240e7b7f4f139eaba2dd091f54ae81710ea48637ae6f388f3ee` |
| `crates/cli-agent-runtime/src/reserved_args.rs` | 409 | `6836a344bf6d6fb56e71cc1b8a917b164955cec5ff8b6ee262b911e3a9b3444f` |
| `crates/gateway/src/cli_runtime/handlers.rs` | 14407 | `f9a4475f322f4141d0167769dae887dced60ea49987c95d55578cd23f3230bfb` |
| `crates/gateway/src/cli_runtime/mcp/readiness.rs` | 1596 | `bbcb5fadd1ae309324c337e9879bdc89cfbf9e8c0208132994b42f6cbabcd3db` |
| `crates/gateway/src/cli_runtime/opencode_catalog.js` | 67 | `dfc3b86a6a98f250d993ac24a48f57233b7fdafd0638a3f5eea546673495e29b` |
| `crates/gateway/src/cli_runtime/opencode_catalog.test.mjs` | 88 | `60b0a507625f84cac628a620779a3e8bc669d584357664cccd94e7cc46de21d4` |
| `crates/gateway/src/cli_runtime/opencode_mcp.rs` | 442 | `88a3fe852acc36f15e15f9492c2ad1c94b481c37b698c0a8ab6f02caf3c014bd` |
| `crates/gateway/src/cli_runtime/opencode_session.rs` | 5977 | `74d119e058199dddc5e7e678eea042fb5251f7a0c6407de82b66eaa8e1d64d88` |
| `crates/gateway/src/cli_runtime/projector.rs` | 2936 | `a4045cf16a23c0427dcbf9e68fe8943c11123aac8f5264471e4959d66b6ab9a3` |
| `crates/gateway/src/compaction.rs` | 1439 | `7f07adb21f0b0f72ed6c4a72c002134c170774c21896e4d1963846594b42b342` |
| `crates/gateway/src/compaction/service.rs` | 826 | `cae7274071c75f7e8d54c22ea886f84261079344c71d22c8a3af6dff2a8b9aa5` |
| `crates/gateway/src/message/tests.rs` | 82796 | `7a89d737ead860fbcda3aa6aa69851e7145ebf22a8cfa4d1744b8581e0b7380d` |
| `notes/opencode-v2-review.md` | 129 | `b9e2e9061391ac0ccea2704580e867d1577ac777a068e08e4f15df4677a24fcc` |

Дополнительно проверены следующие сквозные цепочки:

- Private `serve --stdio`: loopback/PID/auth, configuration/data paths, stdin ownership, bounded shutdown и cleanup.
- HTTP: location query, response wrappers, native IDs, pagination, request/error boundaries.
- Models: provider/model/variant, diagnostic version metadata, readiness и отсутствие semver gates.
- Turn: подготовка input, instructions/attachments, deferred MCP dispatch, admission, native ownership, text/reasoning/tools и terminal/usage.
- MCP: общий helper/bootstrap, frozen names/schemas, allocator, bridge/list/activation barriers, native correlation, catalog verification и recovery.
- Skills: общий install/receipt, canonical SKILL.md discovery, native prompt attachment/preparation и вспомогательные файлы.
- Interactions: permission scope, question/form IDs, labels→values, typed answers, websearch provider selection, cancel/Stop.
- History: volatile SSE, dedup/gaps, authoritative history/idle, load snapshot, reopening/replacement.
- Operations: fork/cutoff/marker, rename, compact, isolated summary, normalized usage до SummaryCompletion/journal.
- Shared boundaries: Codex/Claude branches, protocol/UI contracts, отсутствие изменений avatars/тем и отсутствие миграций V1.

## Приёмка исправлений R1

| Finding | Результат проверки кода | Подготовленные, но незапущенные регрессии |
| --- | --- | --- |
| R1-01 | Закрыт. `OpenCodeTurnActivity::assess` признаёт promoted текущий input + native running началом исполнения без assistant message. Parked input не присваивает чужой busy period. Native activity/history change/Stop/terminal/ownership/pending исключают timeout cleanup. Перед cleanup повторяются native reads и local assessment; решение и closed state фиксируются под runtime mutex. Deadline не увеличен, повторной отправки prompt нет. | `opencode_session.rs:5649`, `:5681` — running без assistant, parked, lost admission, changing history, progress, terminal, Stop и смена ownership. |
| R1-02 | Закрыт. Штатный session-scoped `websearch.provider` допускается без tool metadata при доказанном текущем execution. SSE сначала делает REST reconciliation. Передаются настоящие form/session/turn IDs, item ID отсутствует. Обычный question сохраняет tool correlation; чужие/global формы не присваиваются turn. Ответ и cancel используют native session-scoped routes. | `opencode_session.rs:5753`, `:5912` — question/provider/foreign/global, native values, cancel и Stop. |
| R1-03 | Закрыт. Effect registry transform оборачивает executor shell после native decode. Для background=true возвращается typed Tool.Error до вызова исходного executor. Catalog проверяет идентичность wrapper через WeakSet; native registry rebuild повторяет transform. Post-execution detectors удалены, отказ проектируется как failed tool и не убивает успешное последующее выполнение. | `opencode_catalog.test.mjs` — executor spy, foreground/Pioneer tasks, replaced wrapper/reload; `opencode_session.rs:5855` — recovery отказа. Native host double не заменяет проверку настоящего V2. |
| R1-04 | Закрыт. Единая нормализация включает cache/read/write в effective input и reasoning в output один раз. Native categories и missing/zero сохранены. Summary возвращает нормализованные totals; consumer не перезаписывает их native-only tuple. Journal сохраняет нормализованные totals и breakdown; usage failed terminal передаётся через существующий ObservedServiceUsage. Codex/Claude branches сохранены. | `opencode_session.rs:5873`, `message/tests.rs:82664` — 110/10, несколько steps, compaction, missing/zero, SummaryCompletion/journal, повторное сохранение и failed terminal. |

## Выполненные проверки

- `rustfmt --edition 2024 --config skip_children=true --check` для всех 10 изменённых Rust-файлов — успешно.
- `node --check` отдельно для `opencode_catalog.js` и `opencode_catalog.test.mjs` — успешно; это синтаксический разбор, тестовый runner не запускался.
- `git diff --check` — успешно.
- Проверены status/diff/HEAD/main и отсутствие продуктовых изменений App.
- Поиск старых OpenCode transport/API identifiers в адаптере: V1 fallback/compatibility/version gates не обнаружены; имя теста с semver подтверждает именно необязательность release metadata.

Сборки, cargo check, Rust/JS tests, модели, Pioneer/OpenCode server, установка бинарника и генераторы не запускались. Компиляция и runtime остаются непроверенными.

## Остающиеся границы проверки

Это приёмка исходников, а не утверждение о полной работоспособности на бинарнике. До runtime-проверки остаются практические риски сопряжения с настоящим host, порядком событий и отменой. Известные ограничения private-store authorization, native background/subagent policy, произвольных внешних/plugin forms и bounded recovery history сохранены и описаны в отчёте реализации; новых требований V1 compatibility не добавлено.

Следующий проверяемый этап после разрешения пользователя: компиляция и подготовленные регрессии, затем реальные сценарии text/tools/MCP/skills/questions/websearch, Stop/races/reconnect/restart, fork/compact/summary и обе темы UI. Переход к этому этапу в рамках данного аудита не выполнялся.

Промпт на исправления не составлен: новых подтверждённых дефектов для такого задания нет.
