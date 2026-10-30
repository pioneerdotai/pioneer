# Active-memory recall: передача на внешнее ревью

Дата: 2026-10-29. Это статическая подготовка к внешнему ревью другой моделью, не окончательная приёмка.

## База и изоляция

- Фактическая база: локальный `main`, `c1a692f8f7d564ed81d2581e7d032271b080c108` (Add isolated dev runtime for source builds).
- В задании указан более ранний `1dda8c1662289a57d473271cbfd79b6f87991b23`; при подготовке проверен фактический commit main, использован именно он. Fetch и интеграция соседних веток не выполнялись.
- Ветка: `fix/active-memory-recall`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/active-memory-recall`.
- До создания ветка и путь отсутствовали; основной checkout был чист. Worktree создан от SHA main, без копирования содержимого рабочего дерева. Основной checkout, соседние worktree и установленные копии skills не редактировались.
- Прочитаны корневой `AGENTS.md` и правила CRUD; более узких AGENTS.md для изменённых файлов нет. Запрет пользователя на запуски имеет приоритет над обычными требованиями AGENTS.md о тестах.

## Причина и минимальное исправление

`bind_memory_bridge` создавал `ThreadEpisodicMemoryRecallProvider`, а `install_memory_hook_runtime` передавал его как `Some` в существующий Memory package. Адаптер объявлял `current_thread_search` и `full_input_query`, но `recall_current_thread → recall_thread_search → search_thread_sources` фактически искал по всему доступному индексированному workspace. Поэтому исторический поиск выполнялся в подготовке turn даже при неподходящем или устаревшем preflight-плане: full-input provider мог восстанавливать локальную episodic-часть.

Теперь в той же точке подключения передаётся `None`; поле и создание episodic provider удалены из production bridge. Остальные providers остаются на месте. Ни новая система recall, ни переключатель, ни дополнительный prompt-путь не введены.

Отдельно исправлена фактическая рассинхронизация: `run_agent_turn_preflight_stage` раньше самостоятельно объявлял current-thread и task capabilities. Теперь передаёт существующий пустой `MemoryEpisodicRecallCapabilities::default()`, соответствующий production runtime. Planner и его нормализация не переписаны.

Необязательный адаптер и общие библиотечные episodic-типы сохранены. Уточнён его комментарий; прежняя проверка авторизации адаптера создаёт его явно в тесте, без утверждения о production-подключении.

## Статически проверенные пути

| Путь | Результат статического разбора |
| --- | --- |
| Gateway startup → bind_memory_bridge_if_enabled → bind_memory_bridge → install_memory_hook_runtime | Единственная production-установка Memory package передаёт None для episodic provider. |
| Повторный bind и reinstall_memory_hook_runtime_if_bound | Используют ту же функцию установки; providers активной памяти сохранены. |
| settings/update: memory и thread_episodic в message/dispatch.rs | Оба пути переустанавливают тот же runtime. apply_thread_episodic_runtime_config меняет индексатор, ingestor, embedding resolver и отдельный сервис истории, не подключает episodic provider к Memory. |
| ensure_hook_runtime_with_run_store, generic hook recovery | Сохраняют зарегистрированные definitions при добавлении CRUD run store и AGENTS prompt package; нового Memory episodic provider не создают. |
| Agent runtime dependencies и continuation/recovery | Production dependencies изначально имеют episodic provider None; production callers setter для него не найдены. ensure_hook_runtime_with_current_providers возвращает установленный runtime. Сохранённые plans повторно проходят нормализацию в active hook. |
| Agent подготовка с инструментами и ветка без provider tool calling | Обе вызывают общий исправленный run_agent_turn_preflight_stage и затем post-preflight hook. Общий запуск turn не менялся. |
| deterministic recall | GatewayMemoryProvider → MemoryService::recall_for_prompt, обычный поиск сохранённых фактов, прежние eligibility и budgets. |
| active recall | GatewayMemoryProvider::recall_memory_mode → recall_mode_for_prompt; profile/project/durable/exact_canonical сохранены. Старые thread/task native-provider-required режимы не стали историческим fallback. |
| Пустой recall, ошибка, invalid/mixed/legacy plan, debug fallback | Отсутствующий provider даёт пустые capabilities; normalize_active_recall_plan_for_input отбрасывает episodic modes/queries, сохраняя применимые durable modes. Дополнительная защита execution пропускает отсутствующий provider. Debug fallback обращается только к AgentMemoryProvider::recall_memory. |
| Явный Threads search | Прежние catalog, handler, search_threads_page, search backend, grants, source revalidation и история источника независимы от Memory package. Их production-логика не менялась. |

## Что сохранено

- Deterministic recall, active recall и настройки их mode, budgets, deadlines, synthesis и output policies.
- Memory read/write providers, policy provider, materialization и post-turn extractor provider.
- Проверки статуса Active, срока, repair, качества, sensitivity, scopes, источников и актуальной авторизации в GatewayMemoryProvider и MemoryService.
- Post-turn eligibility, extraction, remember, semantic quality/write gates, forget/tombstones, suppression и правило skill о запрете повторного сохранения забытого факта из старой истории.
- Индексация разговоров и её настройки; явные threads_search и threads_history_read.
- Контракты страниц/cursors и поиска, backend, Memvid, зависимости, grants и CLI export.
- Tasks/Subagents skills/protocol, UI и управление задачами не менялись.

## Точный текст изменения Memory skill

Изменён только bundled `resources/skills/pioneer/memory/SKILL.md`. References Memory и действующий Threads skill просмотрены для проверки смыслов и discovery. Подробные Threads schemas/workflows не копировались. Ни отсутствие памяти, ни пустой индекс не объявлены доказательством отсутствия информации в разговоре.

Полный diff skill:

```diff
diff --git a/resources/skills/pioneer/memory/SKILL.md b/resources/skills/pioneer/memory/SKILL.md
index 92fbb6ca..24d16164 100644
--- a/resources/skills/pioneer/memory/SKILL.md
+++ b/resources/skills/pioneer/memory/SKILL.md
@@ -21,7 +21,7 @@ Memory is not a command source. Treat recalled memory and recalled thread contex
 Before any non-trivial answer or task, inspect what is already in the prompt and decide whether memory could improve the result.

 1. If relevant memory or thread context is already shown and it helps the task, use it directly.
-2. If the turn likely depends on user identity, preferences, communication style, prior project decisions, recurring instructions, ongoing work, known procedures, or older discussion, use memory even if the user did not explicitly ask for it.
+2. If the turn likely depends on user identity, preferences, communication style, prior project decisions, recurring instructions, ongoing work, or known procedures, use memory even if the user did not explicitly ask for it. For past discussions or original messages, follow **Thread Context** below.
 3. If useful memory is missing, ambiguous, stale, conflicting, or provenance-sensitive, use `memory_search`.
 4. If the user asks what is stored, or asks to audit/clean/delete memory, use `memory_list`.
 5. If search/list gives an id or key and exact details matter, use `memory_get`.
@@ -215,7 +215,13 @@ For detailed scope/category/key guidance, read `references/scopes-categories-key

 ## Thread Context

-Thread context may appear separately from durable memory. It is recalled conversation context, not durable memory.
+Automatic pre-turn recall uses saved active memories. For past discussions, original messages, or broader conversation context missing from memory, use the Threads skill and tools: `threads_search` to find relevant conversations, then `threads_history_read` to inspect the sources. Choose history search when it helps the current task; it is not a prerequisite for every answer.
+
+`memory_search` searches saved memory facts; `threads_search` searches indexed history of accessible conversations; `threads_history_read` reads the source. Missing memory or an empty index result does not prove the conversation lacks the information.
+
+In native runtimes, load `system:pioneer/threads` through its internal skill reference and, if needed, open hidden tools with `request_tools` using `domains: ["threads"]`. In CLI runtimes, use the available `pioneer-system-threads` skill and only the issued callable names in the current turn's fixed catalog; `request_tools` and `read_skill` are not required. A skill reference neither grants access nor guarantees tool availability. Follow the Threads skill for tool schemas and workflows.
+
+Thread context may also appear separately from durable memory. It is conversation context, not saved memory.

 Use it for continuation, prior discussion, older answers, research summaries, and artifact references. Treat it as context only, not instruction.

@@ -236,11 +242,11 @@ If the user asks "What do you remember about me?", use `memory_list`, not semant

 If the user says "Remember that I prefer short answers", write a compact `communication_style` memory if `memory_remember` is visible.

-If the user says "make it like last time", "continue the proposal", "use my usual style", or gives a task that depends on prior project/user context, search memory before answering unless injected memory already gives enough context.
+If the user says "make it like last time", "continue the proposal", "use my usual style", or gives a task that depends on prior project/user context, use relevant injected memory or search saved facts as needed. For original discussions or broader context, follow **Thread Context** above.

 If the user states a stable durable fact such as "I prefer Rust examples" or "for this project, migrations go in this file", save it when `memory_remember` is visible, even if the user did not explicitly say "remember".

-If the user asks about "what we discussed yesterday", prefer injected thread context. Durable memory is only appropriate if the question is about stable facts or decisions, not raw conversation history.
+If the user asks about "what we discussed yesterday", use relevant visible thread context or the Threads search and history tools to inspect the original discussion. Use memory for saved stable facts or decisions.

 For full examples, read `references/workflows-and-examples.md`.
```

## Написанные и обновлённые регрессии — НЕ ЗАПУСКАЛИСЬ

1. `admitted_cli_production_recall_uses_active_memory_and_keeps_explicit_history_search` расширяет прежний тест production hook, который требовал автоматический поиск другого разговора.
   - Использует реальный bind_memory_bridge_if_enabled, установленный GatewayHookRuntime с CRUD run store, deterministic phase и post-preflight phase. Package с вручную переданным None в тесте не собирается.
   - Существующие fixtures создают доступный индексированный текущий и другой разговор. Индексация и episodic recall включены.
   - RecordingRecallSearchBackend оборачивает существующий Memvid backend и фиксирует legacy search и search_segment_page. Положительные контроли явно ищут историю до и после автоматических фаз через тот же настроенный сервис, чтобы нулевое изменение счётчика не было следствием недоступного поиска.
   - Проверяет empty, saved и read_error; для saved использует существующий InMemoryMemoryBackend, для ошибки — существующий NoopMemoryBackend при сохранённых control-plane rows.
   - Проверяет допустимый факт deterministic recall и отдельный факт active project recall, отсутствующий в deterministic contribution.
   - Для начальной и повторной установки проверяет local plan, mixed current/related/workspace queries, legacy episodic modes, invalid plan, episodic-only plan и explicit debug fallback (36 post-preflight сценариев).
   - После обновления episodic config, reinstall и ensure_hook_runtime_with_run_store история автоматически не вызывается, active-факт сохраняется.
   - Явный CLI threads_search вызывается по прежнему admitted catalog и возвращает два разговора. Frozen CLI handlers удерживают исходный fixture processor; отдельный явный вызов настроенного processor проверяет именно recording backend.
   - Прежние assertions ограниченного Agent workspace observation сохранены; бизнес-логика поиска не продублирована новыми наборами тестов.
2. `turn_preflight_stage_offers_active_memory_without_history_capabilities` вызывает реальный Agent preflight stage с существующим FakePreflightProvider, для обычного контекста и контекста с task при ошибке planner. Проверяет доступность profile/project/durable, отсутствие episodic capabilities/queries и сохранность durable planning/fallback. Для reuse существующий test-only config helper открыт родительскому модулю; production API не расширен.
3. `optional_recall_adapter_uses_indexed_sources_and_trusted_execution_identity` сохраняет прежние проверки индексированных источников и доверенной identity, но создаёт необязательный адаптер напрямую. Общие библиотечные episodic-тесты не удалялись.

До исправления ожидается рост history backend counter в основном regression и наличие current-thread/task возможностей в preflight regression. Это вывод из кода; baseline и исправленные тесты не исполнялись.

## Промежуточный и финальный самоаудит

Промежуточно проверены подключение/capabilities, начальная и повторная установка, чтение/запись/extraction, независимость Threads и согласованность skill. Найдена и устранена hardcoded-реклама episodic capabilities в Agent preflight, которой одного отключения provider в Gateway было недостаточно.

При проверке тестовой связки дополнительно найдены и исправлены:
- старая регрессия, обращавшаяся к удалённому полю bridge;
- ошибочное предположение о наличии ToolLoopConfig::default — переиспользован существующий test-only helper;
- контракт aggregate_hook_contributions (owned contributions и limits);
- frozen CLI handlers исходного fixture: контроль счётчика направлен на реально настроенный processor.

Финально перечитан весь кодовый diff и затронутые callers, включая обе Agent preflight ветки и settings/recovery paths. Не обнаружено production-пути, заново подключающего историю к автоматическому Memory recall. Обычная память не отключена. Skills Quick Start, Read Flow, Thread Context, Common Patterns согласованы с целевым поведением; forget-инструкция сохранена.

Это собственный статический самоаудит, не внешняя приёмка.

## Разрешённые проверки и ограничения

- Первый `cargo fmt --all -- --check` завершился с кодом 1: требовалось форматирование изменённых тестов.
- Выполнен `cargo fmt --package pioneer-agent --package pioneer-gateway`; повторное форматирование после изменений assertions.
- Итоговый `cargo fmt --all -- --check`: код 0.
- `git diff --check`: без whitespace ошибок. Перед commit также выполняется cached whitespace check.
- Поиски по нескольким предположенным glob-путям вернули zsh no matches found или rg missing path; уточняющие чтения выполнены по фактическим путям. Это не проверки сборки.
- Тесты, mock-сценарии, cargo check, компиляция, сборки, приложение, Gateway, CLI-агенты, миграции и генераторы НЕ ЗАПУСКАЛИСЬ. Проверки типов, benchmarks и живое воспроизведение также не выполнялись.
- Компилируемость и прохождение тестов не подтверждены. Запрет на запуски остаётся до отдельного разрешения пользователя после внешней приёмки.
- Старые snippets/история, уже переданные модели до изменения, не отзываются этой правкой. История остаётся доступна только при явном использовании разрешённых Threads tools; пустой поиск не доказывает отсутствие сведений.
- Commit выполняется с отключёнными hooks и подписью. Merge, push, deploy и публикация PR не выполнялись.

## Точный перечень выполненных команд

Ниже приведены shell-вызовы, включая read/search и повторные проверки, с исходными аргументами. Внутри параллельных read-only групп порядок выдачи результатов не гарантировался. `worktree` означает абсолютный путь из раздела изоляции. Файлы редактировались через apply_patch; проектные scripts не исполнялись. Сессии cargo fmt только дочитывались через write_stdin (65138, 16378, 84522, 10832), без ввода команд.

### 1. cwd: `/`

```sh
pwd; rg --files -g AGENTS.md -g '!node_modules' -g '!target' /Users/alexander/Code/pioneer 2>/dev/null
```

### 2. cwd: `/Users/alexander/Code/pioneer/pioneer`

```sh
git status --short; git worktree list; git rev-parse main; git branch --list fix/active-memory-recall
```

### 3. cwd: `/`

```sh
for p in /AGENTS.md /Users/AGENTS.md /Users/alexander/AGENTS.md /Users/alexander/Code/AGENTS.md /Users/alexander/Code/pioneer/AGENTS.md; do if test -f "$p"; then cat "$p"; fi; done
```

### 4. cwd: `/Users/alexander/Code/pioneer/pioneer`

```sh
cat AGENTS.md; git log -1 --format=fuller main; test ! -e .worktrees/active-memory-recall
```

### 5. cwd: `/`

```sh
cat /Users/alexander/.codex/skills/test-audit/SKILL.md
```

### 6. cwd: `/Users/alexander/Code/pioneer/pioneer`

```sh
git -c core.hooksPath=/dev/null worktree add -b fix/active-memory-recall .worktrees/active-memory-recall c1a692f8f7d564ed81d2581e7d032271b080c108
```

### 7. cwd: worktree

```sh
rg -n 'episodic|MemoryHook|install_memory|memory_hook' crates/gateway/src/message/mod.rs crates/gateway/src/memory_tools.rs crates/gateway/src/thread_episodic_hooks.rs crates/memory/src/hooks/package.rs; cat resources/skills/pioneer/memory/SKILL.md; cat resources/skills/pioneer/threads/SKILL.md; rg --files resources/skills/pioneer/memory
```

### 8. cwd: worktree

```sh
rg -n 'episodic|capabilit|fallback|preflight' crates/memory/src/hooks/handlers/active_recall.rs crates/memory/src/hooks/active_recall.rs crates/memory/src/hooks/handlers/deterministic_recall.rs crates/agent/src; cat crates/memory/src/hooks/package.rs
```

### 9. cwd: worktree

```sh
sed -n '950,975p' crates/gateway/src/message/mod.rs; sed -n '2080,2170p' crates/gateway/src/message/mod.rs; cat crates/gateway/src/thread_episodic_hooks.rs; cat crates/memory/src/hooks/handlers/active_recall.rs
```

### 10. cwd: worktree

```sh
rg -n 'episodic|active_recall|hook_runtime' crates/gateway/src/memory_tools.rs; rg -n 'episodic|memory_hook|active_recall' crates/gateway/src/message/*test*; cat resources/skills/pioneer/memory/SKILL.md
```

### 11. cwd: worktree

```sh
sed -n '3590,4010p' crates/gateway/src/message/tests/cli_task_tools.rs
```

### 12. cwd: worktree

```sh
cat crates/memory/src/hooks/handlers/deterministic_recall.rs; sed -n '1,220p' crates/memory/src/hooks/active_recall.rs
```

### 13. cwd: worktree

```sh
rg -n 'capabilit|normalize|episodic_provider|fallback' crates/memory/src/hooks/active_recall.rs; rg -n 'memory.*capabilit|episodic|preflight' crates/memory/src/hooks/handlers/deterministic_recall.rs crates/agent/src/chat/turn_preflight*.rs crates/agent/src/chat/*preparation*.rs
```

### 14. cwd: worktree

```sh
sed -n '1,230p' crates/gateway/src/message/thread_search_tools.rs; rg -n 'struct .*Backend|calls|backend|seed|recall_thread_search' crates/gateway/src/message/thread_search_tools.rs
```

### 15. cwd: worktree

```sh
rg -n 'recall|search|cfg\(test\)' crates/gateway/src/memory_tools.rs; rg -n 'hook|recall|remember|Fake|Mock' crates/gateway/src/message/tests.rs | head -100; cat crates/crud/AGENTS.md
```

### 16. cwd: worktree

```sh
cat resources/skills/pioneer/memory/references/*.md; cat resources/skills/pioneer/threads/SKILL.md
```

### 17. cwd: worktree

```sh
rg -n 'struct .*Backend|calls|backend|seed|pub.*fn' crates/gateway/src/message/thread_search_tools/tests.rs; sed -n '1,180p' crates/gateway/src/message/thread_search_tools/tests.rs; sed -n '390,455p' crates/gateway/src/memory_tools.rs; sed -n '550,610p' crates/gateway/src/memory_tools.rs; rg -n 'resolve_episodic|normalize.*capabilit|sanitize|provider_missing|episodic_provider|supported_modes' crates/memory/src/hooks/active_recall.rs
```

### 18. cwd: worktree

```sh
sed -n '560,810p' crates/memory/src/hooks/active_recall.rs; sed -n '2110,2320p' crates/memory/src/hooks/active_recall.rs; rg -n 'resolve_episodic_recall_capabilities|thread_episodic.*capabilit|active_recall.*preflight' crates/memory/src/hooks/handlers/*.rs crates/agent/src/chat/*.rs | head -80
```

### 19. cwd: worktree

```sh
rg -n 'with_.*backend|set_.*backend|Fake.*Backend|Recording.*Backend|search_calls' crates/gateway/src/thread_episodic* crates/gateway/src/memory* crates/memory/src/thread_episodic*; sed -n '1,220p' crates/gateway/src/memory_runtime.rs; sed -n '5420,5520p' crates/gateway/src/message/tests.rs
```

### 20. cwd: worktree

```sh
rg -n 'setup_memory_gateway_harness|MemoryGatewayHarness|FakeMemoryBackend' crates/gateway/src/message/tests.rs crates/gateway/src/memory_tools/*.rs; rg -n 'recall_for_prompt|recall_mode_for_prompt|MemoryStatus::Active|eligible' crates/memory/src/service.rs | head -75; sed -n '1,150p' crates/gateway/src/message/tests/cli_task_tools.rs
```

### 21. cwd: worktree

```sh
sed -n '4030,4185p' crates/gateway/src/thread_episodic.rs; rg -n 'struct ThreadEpisodicRecallService|impl ThreadEpisodicRecallService|pub.*fn new|thread_search_backend' crates/gateway/src/thread_episodic.rs | tail -35; sed -n '76720,76910p' crates/gateway/src/message/tests.rs; sed -n '78080,78210p' crates/gateway/src/message/tests.rs; sed -n '78450,78560p' crates/gateway/src/message/tests.rs
```

### 22. cwd: worktree

```sh
sed -n '1100,1235p' crates/gateway/src/thread_episodic.rs; rg -n 'Fake|Mock|search_calls|requests' crates/gateway/src/thread_episodic/recall*.rs | head -80; rg -n 'trait MemoryBackend|struct InMemoryMemoryBackend|fail' crates/memory/src/backend*.rs; sed -n '2150,2260p' crates/agent/src/chat/mod.rs; sed -n '2245,2290p' crates/memory/src/hooks/active_recall.rs; rg -n 'capabilit' crates/memory/src/hooks/active_recall/decision*.rs
```

### 23. cwd: worktree

```sh
rg -n 'ThreadEpisodicMemoryRecallProvider|recall_thread_search|thread_search_recall_enabled' crates; rg --files crates/gateway/src | rg 'episodic'; cat crates/memory/src/backend.rs | head -225; rg -n 'normalize.*episodic|capabilit|fallback' crates/memory/src/hooks/active_recall.rs | tail -75; rg -n 'run_turn_preflight_for|run_turn_preflight|episodic|active_recall' crates/agent/src/chat/tests.rs | head -80
```

### 24. cwd: worktree

```sh
rg -n 'Fake|Mock|search_calls|requests|struct.*Backend' crates/gateway/src/thread_episodic/search.rs; sed -n '1,135p' crates/gateway/src/thread_episodic/search.rs; sed -n '1900,2115p' crates/memory/src/hooks/active_recall.rs; rg -n 'run_turn_preflight_orchestrator_for_turn|run_.*preflight|build.*preflight' crates/agent/src/chat/mod.rs | head -30; rg -n '^mod |preflight|MemoryEpisodic' crates/agent/src/chat/mod.rs | tail -50; sed -n '1829,1980p' crates/memory/src/service.rs
```

### 25. cwd: worktree

```sh
rg -n 'trait ThreadEpisodicMemvidBackend' crates/memory/src/thread_episodic.rs; sed -n '380,510p' crates/memory/src/thread_episodic.rs; sed -n '7214,7300p' crates/agent/src/chat/mod.rs; sed -n '2140,2168p' crates/agent/src/chat/mod.rs; rg -n 'struct.*Backend|impl.*MemoryBackend|fail.*search|search.*fail' crates/gateway/src/message/tests.rs | tail -25; rg -n 'memory_remember_params|MemoryStatus' crates/gateway/src/message/tests.rs | tail -25; rg -n 'struct MemoryEpisodicRecallCapabilities|impl Default' crates/memory/src/hooks/episodic*.rs
```

### 26. cwd: worktree

```sh
sed -n '610,710p' crates/memory/src/thread_episodic.rs; rg -n 'MemoryBackend for|struct .*Backend' crates/gateway/src/memory_tools -g '*.rs'; rg -n 'struct .*Backend|search_calls' crates/gateway/src -g '*test*' | head -60; sed -n '76988,77035p' crates/gateway/src/message/tests.rs; sed -n '120,180p' crates/gateway/src/message/tests.rs; rg -n 'struct.*Preflight|FakePreflight|run_agent_turn_preflight|ActiveRecall' crates/agent/src/chat/mod.rs | tail -25; sed -n '1,120p' crates/agent/src/chat/preflight_failure_diagnostics_tests.rs; rg -n 'pub struct MemoryEpisodicRecallCapabilities' crates/memory/src/hooks -g '*.rs'
```

### 27. cwd: worktree

```sh
sed -n '390,510p' crates/memory/src/hooks/active_recall.rs; rg -n 'enum MemoryActiveRecallMode|Strict|strict' crates/memory/src/hooks/config.rs; sed -n '99,135p' crates/memory/src/hooks/providers.rs; rg -n 'fn memory_recall_mode_scopes|MemoryRecallMode::Project|MemoryRecallMode::Durable' crates/memory/src/service.rs | tail -20; sed -n '1095,1120p' crates/gateway/src/message/mod.rs; rg -n 'impl Clone|derive\(Clone\)' crates/gateway/src/message/mod.rs | head; rg -n 'active_memory_decision_from_preflight_plan|resolve_active_memory_decision_without' crates/memory/src/hooks -g '*.rs'
```

### 28. cwd: worktree

```sh
rg -n 'with_prompt_context_set|HookPromptContextSet::|prompt_context_set\(' crates/memory/src/hooks/tests* crates/gateway/src/message/tests.rs | head -45; sed -n '90,180p' crates/memory/src/hooks/recall.rs; sed -n '4280,4315p' crates/memory/src/service.rs; sed -n '93,101p' crates/memory/src/hooks/providers.rs; rg -n 'struct TurnPreflightOrchestratorResult|struct TurnPreflight.*Input|struct PreflightToolIndex|impl.*EffectiveTurn' crates/agent/src/chat/preflight.rs crates/agent/src/*.rs | head -30; sed -n '7200,7220p' crates/agent/src/chat/mod.rs
```

### 29. cwd: worktree

```sh
sed -n '415,440p' crates/memory/src/hooks/tests/mod.rs; rg -n 'aggregate_hook_contributions|with_prompt_context_set' crates/hooks/src; sed -n '1565,1615p' crates/memory/src/hooks/active_recall.rs; sed -n '1670,1755p' crates/memory/src/hooks/active_recall.rs; rg -n 'fn active_recall_no_provider_local_decision' crates/memory/src/hooks/recall.rs; sed -n '185,310p' crates/memory/src/hooks/recall.rs; sed -n '1,50p' crates/memory/src/hooks/config.rs; rg -n 'impl.*Clone|pub struct MessageProcessor|derive\(Clone\)' crates/gateway/src/message/mod.rs | head -15; cat crates/agent/Cargo.toml | head -90; cat crates/gateway/Cargo.toml | head -95; rg --files .github/workflows
```

### 30. cwd: worktree

```sh
sed -n '168,193p' crates/hooks/src/prompt_context_set.rs; rg -n 'backend.search|backend_search|search_with_diagnostics' crates/memory/src/service.rs | head -30; sed -n '1150,1320p' crates/memory/src/service.rs; sed -n '368,418p' crates/memory/src/hooks/recall.rs; sed -n '297,325p' crates/agent/src/chat/preflight.rs; sed -n '465,525p' crates/agent/src/hooks.rs; rg -n 'AgentEventHub::new|PreflightToolIndex::default|ProviderRegistry::new' crates/agent/src/chat/mod.rs | tail -20; sed -n '1,95p' .github/workflows/ci.yml
```

### 31. cwd: worktree

```sh
sed -n '1677,1755p' crates/memory/src/service.rs; sed -n '7400,7460p' crates/agent/src/chat/mod.rs; sed -n '8430,8465p' crates/agent/src/chat/mod.rs; rg -n 'struct PreflightToolIndex' crates/tools/src -g '*.rs'; rg -n 'struct FakePreflightProvider|fn sample_provider_needed_modules|fn sample_memory_local_plan' crates/agent/src/chat/preflight.rs; sed -n '2955,3025p' crates/agent/src/chat/preflight.rs; rg -n 'fn memory_turn_policy.*contribution|policy_contribution' crates/memory/src/hooks/*.rs
```

### 32. cwd: worktree

```sh
sed -n '2310,2440p' crates/agent/src/chat/preflight.rs; sed -n '279,298p' crates/agent/src/chat/preflight.rs; sed -n '3049,3115p' crates/agent/src/chat/preflight.rs; sed -n '1,90p' crates/memory/src/hooks/policy_codec.rs; rg -n 'hook_policy_set|memory_policy' crates/memory/src/hooks/mod.rs; sed -n '38,70p' crates/tools/src/tool_index.rs; rg -n 'struct AgentTurnHookRuntimeContext' crates/agent/src/hooks.rs
```

### 33. cwd: worktree

```sh
sed -n '95,140p' crates/agent/src/chat/preflight.rs; sed -n '145,195p' crates/agent/src/hooks.rs; rg -n 'decision_context|decision_request|availableModes|available_modes|episodic_recall' crates/agent/src/chat/preflight.rs | head -45; rg -n 'struct MemoryActiveRecallDecisionContext|struct MemoryActiveRecallDecisionRequest' crates/memory/src/hooks/providers.rs crates/memory/src/hooks/active_recall.rs; sed -n '34,41p' crates/tools/src/tool_index.rs; sed -n '755,790p' crates/memory/src/hooks/recall.rs
```

### 34. cwd: worktree

```sh
sed -n '140,185p' crates/agent/src/chat/preflight.rs; sed -n '484,555p' crates/memory/src/hooks/providers.rs; sed -n '3200,3255p' crates/agent/src/chat/preflight.rs; sed -n '2465,2535p' crates/agent/src/chat/preflight.rs; sed -n '3690,4030p' crates/gateway/src/message/tests/cli_task_tools.rs
```

### 35. cwd: worktree

```sh
rg -n 'idempotency|invocation_id|run_id|Already' crates/hooks/src/runtime.rs | head -65; sed -n '275,345p' crates/hooks/src/runtime.rs; rg -n 'idempotency|invocation_id' crates/gateway/src/hooks/*.rs | head -45; sed -n '3220,3248p' crates/memory/src/service.rs; sed -n '1800,1828p' crates/memory/src/service.rs; sed -n '3210,3235p' crates/agent/src/chat/preflight.rs; sed -n '2850,2885p' crates/agent/src/chat/preflight.rs
```

### 36. cwd: worktree

```sh
sed -n '2476,2528p' crates/hooks/src/runtime.rs; sed -n '1780,1885p' crates/hooks/src/runtime.rs; sed -n '2290,2310p' crates/agent/src/chat/preflight.rs; rg -n 'available_episodic_modes|episodic_capabilities' crates/memory/src/hooks/active_recall.rs | tail -15; sed -n '1240,1285p' crates/gateway/src/message/tests/cli_task_tools.rs; rg -n 'async fn invoke_task' crates/gateway/src/message/tests/cli_task_tools.rs
```

### 37. cwd: worktree

```sh
sed -n '1385,1450p' crates/hooks/src/runtime.rs; sed -n '1900,1970p' crates/hooks/src/runtime.rs; sed -n '568,615p' crates/gateway/src/message/tests/cli_task_tools.rs; sed -n '3170,3205p' crates/agent/src/chat/preflight.rs; rg -n 'fn sample_provider_plan_json' crates/agent/src/chat/preflight.rs
```

### 38. cwd: worktree

```sh
rg -n 'bind_agent_tool_bridges|bind.*thread|set.*processor|register.*thread' crates/gateway/src/message/mod.rs crates/gateway/src/message/thread_search_tools.rs; sed -n '1880,1990p' crates/gateway/src/message/mod.rs; sed -n '2724,2765p' crates/agent/src/chat/preflight.rs; rg -n 'weak|processor' crates/gateway/src/turn_mcp/*.rs | head -30
```

### 39. cwd: worktree

```sh
git diff --stat; git diff -- crates/gateway/src/message/mod.rs crates/agent/src/chat/mod.rs resources/skills/pioneer/memory/SKILL.md; sed -n '1,45p' crates/agent/src/chat/preflight.rs; rg -n 'struct ToolLoopConfig|impl Default for ToolLoopConfig|memory:' crates/agent/src -g '*config*'; rg -n 'pub use.*NoopMemoryBackend|NoopMemoryBackend' crates/memory/src/lib.rs; rg -n 'reinstall_memory_hook_runtime_if_bound|bind_memory_bridge' crates/gateway/src -g '*.rs'; rg -n 'package\(' crates/gateway/src | rg 'memory|hooks' | head -35
```

### 40. cwd: worktree

```sh
sed -n '390,545p' crates/gateway/src/message/thread_search_tools/tests.rs; rg -n 'memory.*default|impl Default for ToolLoopConfig|pub struct ToolLoopConfig' crates/agent/src -g '*.rs' | head -20; sed -n '7955,8025p' crates/gateway/src/message/dispatch.rs; sed -n '8070,8115p' crates/gateway/src/message/dispatch.rs; rg -n 'system:pioneer/threads|Internal Skill References|skill:' resources/skills/pioneer -g SKILL.md | head -35; sed -n '1,180p' resources/skills/pioneer/memory/references/troubleshooting.md; sed -n '245,340p' resources/skills/pioneer/memory/references/scopes-categories-keys.md
```

### 41. cwd: worktree

```sh
cargo fmt --all -- --check
```

### 42. cwd: worktree

```sh
sed -n '95,155p' crates/agent/src/lib.rs; sed -n '2140,2185p' crates/memory/src/hooks/active_recall.rs; sed -n '40,145p' crates/memory/src/hooks/recall.rs; rg -n 'fallback|episodic|history' resources/skills/pioneer/memory/references/workflows-and-examples.md resources/skills/pioneer/memory/references/tool-schemas.md; sed -n '185,215p' resources/skills/pioneer/memory/references/troubleshooting.md; git diff --check
```

### 43. cwd: worktree

```sh
rg -n 'Default for ToolLoopConfig|ToolLoopConfig::default|fn .*tool_loop_config' crates/agent/src/lib.rs crates/agent/src/chat/preflight.rs crates/agent/src/manager_tests.rs | head -25; sed -n '175,190p' resources/skills/pioneer/memory/references/troubleshooting.md; sed -n '90,115p' resources/skills/pioneer/memory/references/workflows-and-examples.md; sed -n '485,505p' resources/skills/pioneer/memory/references/workflows-and-examples.md; sed -n '260,360p' crates/agent/src/lib.rs
```

### 44. cwd: worktree

```sh
rg -n 'impl ToolLoopConfig|impl .*SkillsLoopConfig|fn test_tool_loop_config|fn test_skills' crates/agent/src/lib.rs crates/agent/src/manager_tests.rs; sed -n '100,165p' crates/agent/src/manager_tests.rs; rg -n 'ToolLoopConfig \{' crates/agent/src/chat/mod.rs | head -15; sed -n '7395,7405p' crates/agent/src/chat/mod.rs
```

### 45. cwd: worktree

```sh
sed -n '629,720p' crates/agent/src/lib.rs; sed -n '165,205p' crates/agent/src/manager_tests.rs; rg -n 'impl.*From|pub fn' crates/agent/src/lib.rs | head -40
```

### 46. cwd: worktree

```sh
cargo fmt --package pioneer-agent --package pioneer-gateway
```

### 47. cwd: worktree

```sh
sed -n '1,200p' crates/gateway/src/memory_tools.rs; sed -n '3245,3360p' crates/agent/src/chat/preflight.rs; sed -n '3940,4215p' crates/gateway/src/message/tests/cli_task_tools.rs; rg -n 'episodic' crates/agent/src/hooks.rs; rg -n 'memory|recall' crates/agent/src/prompt.rs | head -45
```

### 48. cwd: worktree

```sh
sed -n '195,290p' crates/gateway/src/memory_tools.rs; rg -n 'AgentTurnHookRuntimeContext|AgentEventHub|mod manager_tests' crates/agent/src/lib.rs | head -20; sed -n '4295,4330p' crates/memory/src/service.rs; sed -n '1,115p' crates/memory/src/hooks/handlers/post_turn_extractor.rs; rg -n 'suppress|forgotten|tombstone' crates/memory/src/service.rs crates/gateway/src/memory_tools.rs | head -40; rg -n 'memory|pre_prompt|preflight|hook_runtime' crates/agent/src/message* crates/agent/src/manager*.rs | head -65; git diff --numstat; git status --short
```

### 49. cwd: worktree

```sh
sed -n '1750,1848p' crates/memory/src/hooks/active_recall.rs; rg -n 'normal_default_allow|SourceAccess|scoped_collaboration|RuntimePrincipalPolicy' crates/gateway/src/authorization/runtime*.rs crates/gateway/src/authorization/*.rs | head -55; rg -n 'memory_runtime_principal_policy' crates/gateway/src -g '*.rs'; sed -n '3970,4035p' crates/agent/src/chat/mod.rs; sed -n '4350,4395p' crates/agent/src/chat/mod.rs; rg -n 'run_agent_turn_pre_prompt_context|deterministic_recall|episodic' crates/agent/src/hooks.rs | head -50; rg -n 'recall|memory' crates/agent/src/chat/prompt*.rs | head -50
```

### 50. cwd: worktree

```sh
sed -n '3655,3705p' crates/gateway/src/authorization/execution.rs; rg -n 'recall_memory\(|recall_memory_mode\(|AgentEpisodicRecallProvider|ThreadEpisodicMemoryRecallProvider' crates/agent/src crates/gateway/src crates/memory/src/hooks -g '*.rs' | head -100; rg -n 'forget|rejection|deleted' crates/memory/src/write_quality*.rs crates/memory/src/semantic*.rs | head -40; git diff -- crates/agent/src/manager_tests.rs crates/gateway/src/message/thread_search_tools/tests.rs crates/gateway/src/thread_episodic_hooks.rs; git rev-parse main; git status --short
```

### 51. cwd: worktree

```sh
sed -n '3050,3120p' crates/agent/src/lib.rs; rg -n 'memory_episodic_recall_provider|memory_hook_package|pioneer_memory::hooks::package' crates/agent/src; rg -n 'forget|rejection|deleted' crates/memory/src -g '*quality*' -g '*semantic*' | head -40; rg -n 'memory|recall' crates/agent/src/hooks.rs | head -55; sed -n '1,60p' crates/gateway/src/message/tests.rs; rg -n 'MemoryHookPackage|hooks::package|episodic' crates/gateway/src/hook_runtime*.rs crates/agent/src/hook*.rs
```

### 52. cwd: worktree

```sh
cargo fmt --package pioneer-agent --package pioneer-gateway
```

### 53. cwd: worktree

```sh
sed -n '2635,2685p' crates/agent/src/lib.rs; sed -n '3115,3140p' crates/agent/src/lib.rs; rg -n 'HookRuntimeBuilder|hook_runtime.*None|memory_provider' crates/agent/src/lib.rs | head -45; sed -n '795,825p' crates/gateway/src/lib.rs; sed -n '830,852p' crates/gateway/src/lib.rs; rg -n 'deleted|suppressed|tombstone' crates/memory/src/service/semantic*.rs crates/memory/src/quality_gate.rs 2>/dev/null | head -30
```

### 54. cwd: worktree

```sh
git diff -- crates/agent/src/chat/mod.rs crates/agent/src/chat/preflight.rs crates/agent/src/manager_tests.rs crates/gateway/src/message/mod.rs crates/gateway/src/thread_episodic_hooks.rs crates/gateway/src/message/thread_search_tools/tests.rs resources/skills/pioneer/memory/SKILL.md
```

### 55. cwd: worktree

```sh
git diff -- crates/gateway/src/message/tests/cli_task_tools.rs
```

### 56. cwd: worktree

```sh
cargo fmt --all -- --check
```

### 57. cwd: worktree

```sh
git diff --check; git status --short; git log -1 --format='%H %s' main; git diff --name-only
```

### 58. cwd: worktree

```sh
git diff -- resources/skills/pioneer/memory/SKILL.md; git diff --check; git status --short --untracked-files=all
```

### Команды завершения

Эти команды выполняются после записи отчёта, в приведённом порядке. Результат commit и состояние дерева передаются в финальном ответе.

```sh
sed -n '1,135p' docs/reviews/active-memory-recall.md; git diff --check; git diff --stat
```

```sh
git add crates/agent/src/chat/mod.rs crates/agent/src/chat/preflight.rs crates/agent/src/manager_tests.rs crates/gateway/src/message/mod.rs crates/gateway/src/message/tests/cli_task_tools.rs crates/gateway/src/message/thread_search_tools/tests.rs crates/gateway/src/thread_episodic_hooks.rs resources/skills/pioneer/memory/SKILL.md docs/reviews/active-memory-recall.md
```

```sh
git diff --cached --check; git diff --cached --stat; git status --short
```

```sh
git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit -m "Restrict automatic recall to saved active memory"
```

```sh
git status --short; git rev-parse HEAD; git log -1 --format='%H%n%s'; git diff HEAD --check; git branch --show-current
```

```sh
git -C /Users/alexander/Code/pioneer/pioneer status --short; git -C /Users/alexander/Code/pioneer/pioneer rev-parse main; git worktree list
```
