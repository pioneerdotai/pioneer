# Статический аудит OpenCode V2 — R1

Дата: 2026-10-16. Решение: код пока не принят; требуются четыре исправления ниже.

## Проверенная ревизия и границы

- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/opencode-v2`.
- Ветка: `feature/opencode-v2`.
- HEAD и локальный main: `f2d493c672aa58a184f800b4b695e289ce0f6b50`.
- Адаптер изменён незакоммиченным diff: 11 файлов, +2973/-2596 строк до добавления этого отчёта.
- App HEAD: `9d631c87e05485126c51ab77939802c131418a67`. Единственное изменение App — чужой `bun.lock`; он не изменялся аудитом.
- Продуктовый код не изменялся. Добавлен только этот отчёт.
- Сборки, cargo check, тесты, модели, OpenCode CLI/server и Pioneer не запускались. Выполнены `git diff --check` и синтаксический `node --check` нового JS-файла; оба успешны.

Прочитана вся текущая реализация `opencode_session.rs` (строки 1–5554), все остальные изменения, новые/изменённые тесты и исходный отчёт. Проверены связанные неизменённые пути manager/recovery, durable projection, skills, instruction projection, usage и конфигурации. Проверка исходников не доказывает компиляцию или runtime-работоспособность.

Детали native V2 сверены с локальной копией официального upstream `/tmp/pioneer-opencode-v2-sources/upstream`, HEAD `4617210822bdbb31e5749751afbfb4584ccedac8`, remote `https://github.com/anomalyco/opencode.git`, и сохранённым OpenAPI. Повторное обращение к сайту через web-инструмент было недоступно (403); свежесть upstream сверх указанного snapshot не заявляется. SHA используется только как источник доказательств аудита, не как runtime version gate.

## Полное покрытие текущего diff

| Файл | Проверенная цепочка | Результат |
| --- | --- | --- |
| `crates/cli-agent-runtime/src/instructions.rs` | Новый transport, все ссылки и persisted instruction projection | Дополнительных findings нет |
| `crates/cli-agent-runtime/src/reserved_args.rs` | Reserved flags, native serve command/options, regression changes | Дополнительных findings нет |
| `crates/gateway/src/cli_runtime/handlers.rs` | Probe data home, преобразование durable answers, permission decisions | R1-02 связан с формами; см. также R1-04 для summary |
| `crates/gateway/src/cli_runtime/mcp/readiness.rs` | Fingerprints, отсутствие version gate, scope readiness | Дополнительных findings нет |
| `crates/gateway/src/cli_runtime/opencode_catalog.js` | Native promise plugin/RPC/tool-list contracts, namespaces, schemas | Каталог проверяется; механизм отказа shell требует R1-03 |
| `crates/gateway/src/cli_runtime/opencode_mcp.rs` | Frozen projection, allocator/name limits, event enrichment/correlation, новые тесты | Дополнительных findings нет |
| `crates/gateway/src/cli_runtime/opencode_session.rs` | Весь файл: process/API/SSE/models/turn/Stop/recovery/MCP/skills/forms/summary/fork/compact/tests | R1-01–R1-04 |
| `crates/gateway/src/cli_runtime/projector.rs` | Изменение комментария и повторная проверка commandExecution/status/output policy | Дополнительных findings нет |
| `crates/gateway/src/compaction/service.rs` | OpenCode private data home, constructor callsites, service lifecycle и usage journal | R1-04 |
| `crates/gateway/src/message/tests.rs` | Обновлённое ожидание transport | Дополнительных findings нет |
| `notes/opencode-v2-review.md` | Полный отчёт, матрица, claims и ограничения | Ограничения forms/background не устраняют R1-02/R1-03 |

Также прослежены: loopback/stdin ownership/auth и custom home; location/data wrappers и pagination; model refs/variants; deferred MCP activation и точный catalog; выбранные skills от общей установки до native prompt preparation; text/reasoning/tool projection и UTF-8 ingress; cancel/Stop; authoritative history/idle; fork/name/compact; summary cleanup; неизменность публичного UI/protocol. Отсутствие дополнительных findings в этих цепочках — результат статического просмотра, а не runtime-проверки.

## R1-01 — P1: startup timeout останавливает уже выполняющийся turn

Код: [refresh ownership](../crates/gateway/src/cli_runtime/opencode_session.rs#L4785), [snapshot/expiry](../crates/gateway/src/cli_runtime/opencode_session.rs#L4975), особенно строки 5028–5057.

`awaiting_execution` сбрасывается только после появления assistant/idle в истории. При этом `/api/session/active` уже может подтверждать выполнение текущего принятого input. `expired` не учитывает это доказательство: после dispatch deadline и при пустых permissions/forms код вызывает interrupt и закрывает owner даже при `live == true`.

В V2 execution claim/run coordinator появляется до первого provider output; assistant step может ещё отсутствовать при медленном соединении, инициализации или retry до начала вывода. Результат — ошибочный `PromptStartupTimeout` для живого turn. Настраиваемый HTTP request timeout становится ограничением ожидания первого assistant message (по умолчанию 120 секунд).

Есть и гонка: после вычисления `expired` выполняются HTTP reads permissions/forms, затем процесс закрывается **до** повторной проверки runtime state. Если за это время уже появилась assistant history/состояние прогресса, сервер всё равно уничтожается, хотя проверка после close может уже не выпустить timeout outcome.

Native evidence: `packages/core/src/session/execution.ts` (started/coordinator), `packages/core/src/session/runner/step.ts` (provider stream), `packages/core/src/session/runner/publish-llm-event.ts` (отложенный Step.Started), `packages/core/src/session/runner/retry.ts` (ожидания retry).

Минимальное исправление: учитывать подтверждённую активность именно текущего input при фиксации начала исполнения; перед принудительным закрытием повторно проверять актуальные native/local признаки и ownership. Не увеличивать произвольно таймаут и не убирать защиту от реально не запустившегося prompt.

Регрессии для последующего запуска: expired dispatch + current user history + native running без assistant; настоящий потерянный admission; прогресс/terminal, появившийся между проверками expiry и cleanup; Stop сохраняет приоритет.

## R1-02 — P2: штатная форма web search уничтожает turn

Код: [opencode_form_supported](../crates/gateway/src/cli_runtime/opencode_session.rs#L2351), [form mapper](../crates/gateway/src/cli_runtime/opencode_session.rs#L2372), [SSE rejection](../crates/gateway/src/cli_runtime/opencode_session.rs#L1628), [REST rejection](../crates/gateway/src/cli_runtime/opencode_session.rs#L5129).

Для любой формы обязательны `metadata.tool.messageID` и `metadata.tool.id`. Однако встроенный V2 websearch при первом выборе провайдера создаёт обычную string/options форму с корректным `sessionID` и `metadata.kind = "websearch.provider"`, без `metadata.tool`. Она представима существующим Pioneer question UI, но текущая проверка отменяет её, прерывает execution и закрывает сервер.

Native evidence: [официальный websearch tool](https://github.com/anomalyco/opencode/blob/4617210822bdbb31e5749751afbfb4584ccedac8/packages/core/src/tool/plugin/websearch.ts), строки 75–98 и 108–131 локального snapshot. Локальное доказательство: `/tmp/pioneer-opencode-v2-sources/upstream/packages/core/src/tool/plugin/websearch.ts`.

Минимальное исправление: поддержать штатные представимые session-scoped формы, необходимые уже разрешённым native tools, включая websearch.provider. Не выдумывать tool call ID; использовать допустимый отсутствующий `native_item_id` и доказанную привязку к управляемой сессии/активному turn. Сохранить строгую изоляцию чужих/global форм, Stop/cancel и явный отказ для действительно неподдерживаемых типов. Общую UI-подсистему форм не строить.

Регрессии: реальная native форма websearch.provider без tool metadata; обычный question с tool metadata; чужая/global форма; корректная доставка ответа и отмена.

## R1-03 — P2: background shell запрещается после возможного побочного эффекта

Код: [SSE detector](../crates/gateway/src/cli_runtime/opencode_session.rs#L1599), [history detector](../crates/gateway/src/cli_runtime/opencode_session.rs#L2339), [REST cleanup](../crates/gateway/src/cli_runtime/opencode_session.rs#L5110).

Запрет реализован через получение `session.tool.called` и последующие interrupt/close либо последующее чтение истории. У V2 это уведомление не является барьером исполнения: после его публикации runner сразу запускает native tool. Native shell создаёт процесс до возврата background result. Поэтому `shell(background=true)` может выполнить внешнее действие до того, как Pioneer объявит вызов неподдерживаемым. При разрыве SSE задержка до обнаружения ещё больше.

Native evidence: `packages/core/src/session/runner/step.ts`, строки 107–137; `packages/core/src/tool/plugin/shell.ts`, создание shell и ветка background. В существующем plugin surface доступен `tool.execute.before`; конкретную безопасную реализацию нужно сверить с контрактом hooks/wrappers.

Минимальное исправление: обеспечить отказ до выполнения native shell при background=true через поддерживаемый V2 механизм, без новой архитектуры. Согласовать live/recovery-детекторы с отказом до исполнения: отклонённый вызов не должен считаться реально запущенной фоновой задачей. System instruction и последующий kill не заменяют барьер до side effect. Foreground shell и Pioneer task tools должны сохранить работу.

Регрессия: при запрещённом вызове исполнитель native shell не вызывается; foreground доступен; отказ корректно проектируется и восстанавливается. Не требуется реализовывать поддержку native background jobs.

## R1-04 — P2: usage summary не соответствует TokenUsage Pioneer

Код: [OpenCode summary accumulation/return](../crates/gateway/src/cli_runtime/opencode_session.rs#L1098), [summary result overwrite](../crates/gateway/src/compaction/service.rs#L405), [apply_cli_usage](../crates/gateway/src/compaction/service.rs#L434), [контракт TokenUsage](../crates/provider/src/types.rs#L1036). Аналогичные native поля публикуются в turn usage в `opencode_session.rs:4865`.

V2 `tokens.input` содержит nonCachedInputTokens, а `tokens.output` — visibleOutputTokens. Cache read/write и reasoning идут отдельно. В Pioneer `TokenUsage.input_tokens` включает cache ровно один раз, `output_tokens` включает reasoning ровно один раз.

Сейчас summary возвращает exclusive native input/output как effective totals. `apply_cli_usage` для opencode-cli не складывает cache, удаляет uncached_input_tokens, не добавляет reasoning в output; затем service дополнительно перезаписывает totals исходным tuple. Usage journal и SummaryCompletion сохраняют заниженные значения.

Пример: native input=10, cache.read=80, cache.write=20, output=3, reasoning=7. Должны получиться effective input=110/output=10 с сохранением отдельных категорий; сейчас сохраняются input=10/output=3.

Native evidence: [официальная нормализация V2 usage](https://github.com/anomalyco/opencode/blob/4617210822bdbb31e5749751afbfb4584ccedac8/packages/core/src/session/usage.ts), строки 11–18 локального snapshot.

Минимальное исправление: явно нормализовать OpenCode V2 категории на границе Pioneer и согласовать summary tuple, journal и turn usage metadata. Сохранить raw/native категории, различие missing/zero, отсутствие двойного счёта; учесть относящееся к операции compaction usage, если оно присутствует. Семантику Codex/Claude не менять.

Регрессии: приведённый пример, нулевые и отсутствующие поля, несколько assistant steps, путь summary → apply_cli_usage → journal/SummaryCompletion.

## Промпт на исправление

Исправь четыре подтверждённых finding R1-01–R1-04 из этого отчёта в существующем OpenCode V2 адаптере Pioneer.

Рабочий репозиторий: `/Users/alexander/Code/pioneer/pioneer/.worktrees/opencode-v2`, ветка `feature/opencode-v2`. База аудита — `f2d493c672aa58a184f800b4b695e289ce0f6b50`, изменения адаптера незакоммичены. App не менялся; чужой `/Users/alexander/Code/pioneer/pioneer-app/bun.lock` не трогать.

0. Прочитай применимые AGENTS.md, этот отчёт и текущий diff. Проверь status/HEAD/main. Если main продвинулся, безопасно синхронизируй feature-ветку, сохранив весь незакоммиченный результат и чужие изменения. После синхронизации статически проверь совместимость и заново подтверди findings. Не выполняй reset/clean, push или merge в main.

1. Исправь R1-01: подтверждённый запуск текущего input не должен ожидать первого assistant message. Native running/retry и ownership должны предотвращать ложный startup timeout. Исключи закрытие owner по устаревшему решению после HTTP await. Сохрани реальные admission deadlines, Stop и recovery; не лечи проблему увеличением таймаута.

2. Исправь R1-02: корректно проведи через существующий question flow штатную session-scoped websearch.provider форму без tool metadata. Сохрани реальные native IDs, typed answers, cancel/Stop и изоляцию чужих/global запросов. Не выдумывай tool ID и не создавай универсальный form framework.

3. Исправь R1-03: запрети native shell(background=true) до вызова исполнителя, через поддерживаемый V2 механизм. Согласуй live/recovery projection с отказом до исполнения. Не реализовывай native background jobs, не блокируй foreground shell или Pioneer task tools, не полагайся только на инструкции модели либо последующий interrupt.

4. Исправь R1-04: нормализуй exclusive V2 token categories в effective Pioneer totals без двойного счёта; сохрани native breakdown и missing/zero. Согласуй summary result tuple, apply_cli_usage, usage journal и turn usage metadata. Учитывай только usage соответствующей операции; Codex/Claude не меняй.

Общие ограничения:

- OpenCode V1 никогда не публиковался. Итоговый адаптер поддерживает только V2. Не добавляй V1 fallback, legacy/compatibility layers, миграции/импорт экспериментальных данных или отдельного провайдера V2.
- Никаких minimum/exact version gates, allowlist релизов, обязательного semver или version metadata. Upstream SHA в отчёте — доказательство, не требование к бинарнику.
- Используй общие MCP/skills/recovery/durable/UI механизмы и минимальные локальные изменения. Сохрани frozen MCP projection/catalog/activation/correlation, skills installation/selection, instructions, attachments, reasoning, Stop/fork/compact и историю V2.
- Продуктовые изменения ограничь этими дефектами и необходимыми связанными исправлениями. Без новой архитектуры, чисток соседних подсистем и изменений БД/локальных данных.
- Добавь/обнови необходимые поведенческие регрессии штатными средствами проекта, без нового тестового фреймворка, но НЕ запускай их.

СТРОГО: до отдельной приёмки кода запрещены любые сборки и тесты, включая cargo check/build/test, Desktop/App build, unit/integration/e2e/smoke и проверочные вызовы моделей. Не запускай/не перезапускай Pioneer или OpenCode server, не обновляй бинарник OpenCode. Разрешены чтение, статический аудит, форматирование, diff check и разбор синтаксиса без сборки; генераторы допустимы только если не запускают запрещённые операции.

После исправлений выполни полный статический аудит всего итогового дерева изменений; не останавливайся на первой ошибке. Обнови `notes/opencode-v2-review.md`, приложи для каждого R1 finding ссылки на исправление и незапущенные регрессии. Укажи все изменённые файлы, выполненные статические проверки и оставшиеся ограничения. Заверши фразой: «Сборки, тесты и runtime-проверки не выполнялись; код подготовлен к повторной приёмке». Не заявляй runtime-работоспособность по статическому анализу.
