# Промпт 3/4: Stop, foreground control и отмена потомков

Ты работаешь в проекте /Users/alexander/Code/pioneer/pioneer.

Продолжи уже подготовленную часть Threads domain execution. Код перенесён из готового кандидата; заново реализовывать задачу с нуля не нужно. Выполни самостоятельный статический самоаудит, необходимые минимальные исправления своей части и подготовь результат к внешнему ревью. Разделение не означает приёмку кода.

## Зафиксированная рабочая область

- Ветка: `fix/threads-turn-cancellation`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-turn-cancellation`.
- База собственной части: `13b22123b19026b1ab4701e72c884f3efe4a15b3`.
- Родительская ветка для будущего PR: `fix/threads-independent-root-launch`.
- Исходный кандидат разделения: `9d86bc3eb65cc58f0e67bf3022a545dfec0b14b6`.
- Совокупная база исходного кандидата: `bb85dcdf7080164c4dc6480d12410e5e38edade8`.
- Собственный интервал ревью: `git diff 13b22123b19026b1ab4701e72c884f3efe4a15b3..HEAD` плюс твои незакоммиченные изменения.

Перед работой прочитай применимые AGENTS.md, проверь ветку, HEAD и чистоту дерева. Зафиксируй фактический начальный commit. Если есть незакоммиченная работа, не удаляй и не перезаписывай её.

Части обрабатываются последовательно 1 → 2 → 3 → 4. Перенос принятых исправлений в следующие ветки выполняет родительский ревьюер. Не изменяй другие ветки/worktree и не выполняй самостоятельную интеграцию с main или исходным кандидатом. Если начало работы сопровождается обновлённой базой от ревьюера, используй именно её и отрази это в отчёте.

## Граница этой части

Проверь уже перенесённые изменения:
- foreground admission standalone root совпадает с обычным пользовательским сценарием; конфликт не оставляет phantom turn/action и возвращает agent_action_conflict;
- Stop до actor admission, во время initial registration и после activation завершается через существующие canonical owner/generation fences;
- mailbox/process ACK и canonical completion различаются; ordinary CancelTurn не ждёт durable publication до process ACK;
- после process ACK actor сохраняет обязанность durable registration/terminal publication; Task cancellation не теряет actor после прежнего ACK window;
- handoff сверяет actor/run/batch, workspace и durable owner/generation; registered flag описывает фазу, а не право бросить Stop;
- typed storage failure в guard до регистрации, в самой регистрации и после неё сохраняет обязанность уже переданного Stop, не ослабляя semantic denial;
- accepted-but-pending projection не считается ни permanent failure, ни завершённой отменой; completion подтверждается после projection, exact cleanup и требуемого sync;
- root graph fence не теряет unfinished direct responses уже cancelled executions; ошибка одного потомка не забывает siblings, повторный public Stop завершает оставшееся;
- настоящий Task остаётся под своим aggregate cancellation protocol;
- canonical terminal, resource state и running permit согласованы; повторная доставка не освобождает ресурсы нового поколения;
- guarded memory sync и generic resume не заменяют InProgress N+1 старым terminal N;
- отсутствие unloaded target Turn внутри loaded thread не считается memory conflict; настоящие conflicting/duplicate loaded states проверяются;
- optional cancellation_owner согласован в Rust, native/CLI constructors и checked-in JSON Schema; legacy события без поля читаются.

Основные файлы: agent/src/{lib,agent_loop,manager_tests}.rs; protocol/agent_event.rs и schema; cli_runtime/projector.rs; cancellation-фрагменты message/agent_runtime.rs; thread/mod.rs; resume bridge в message/turn_handlers.rs; generation-bound cancellation CRUD; cancel_agent_work_graph.

Здесь же находятся guarded synchronization primitives и общий resume bridge, потому что Stop уже использует их. Связанные recovery consumers и их собственная resume-регрессия тоже входят сюда; restart/outbox matrix добавляется частью 4. Не копируй эти primitives повторно.

Проверь native_cancellation_tests, recovered_native_turn_stop, constructor compatibility, fixture завершения предыдущего launch и recovery terminalization/resume регрессию. Сквозная independent-root first-dispatch/replay/Stop и foreground-регрессии входят в эту часть. Упражняй реальные production paths и обе стороны гонок. Тесты только читать/дополнять, не запускать.

Не добавляй новые control APIs, отдельные очереди отмены, новые таймауты или альтернативный Task protocol. Не расширяй cancellation proof на другое поколение или отозванные права.

## Строгие правила

1. Работай автономно, без вопросов и остановок ради обычных технических решений.
2. Изменяй только свою цепочку и необходимые для неё callers/tests. Исправление минимальное, без попутного рефакторинга, новых подсистем, UI, зависимостей, новых поисковых возможностей или иных расширений задачи.
3. Сохрани права initiating user, immutable grants, explicit allow/deny, workspace/thread scope, opaque routes, replay/idempotency и точные generation fences.
4. Не меняй skills, policy blocks или правила Tasks/Subagents. Не увеличивай таймауты, лимиты, квоты, число попыток или DB priority для обхода ошибки.
5. Работай только в указанном worktree. Основной checkout, исходная ветка и соседние worktree должны остаться нетронутыми. Не выполняй merge, push, deploy и не создавай PR самостоятельно.
6. Тесты при необходимости допиши или исправь, но НЕ ЗАПУСКАЙ. Запрещены cargo check/build/test/nextest/clippy/run, любые сборки, выполнение кода проекта, приложение, Gateway, тестовые CLI-агенты, миграции, EXPLAIN, benchmarks и генераторы.
7. Из Cargo-команд разрешён только cargo fmt, включая check. Разрешены чтение, поиск, обычные Git-операции, git diff --check и статическая обработка текста. Не обходи запрет через scripts, hooks, IDE или вспомогательные команды.
8. Самоаудит и даже внешняя приёмка сами по себе не разрешают запуск проверок: требуется отдельное разрешение пользователя после приёмки.
9. Не меняй dev/prod-базы, службы, конфиги и установленные skills. Коммить с отключёнными hooks.

## Самоаудит, покрытие и передача

- Проверяй законченные участки по ходу работы: контракт/вход → авторизация → действие/сохранение → runtime/control → результат/replay.
- Прочитай весь собственный итоговый diff и непосредственно затронутые callers. Проверь зависимые shared signatures; не считай будущие определения уже присутствующими.
- Следи за обеими сторонами гонок и точкой canonical acceptance. Строка ошибки, ACK или наличие snapshot не заменяют соответствующее подтверждение.
- Расширяй существующие содержательные регрессии; не дублируй сквозные тесты из следующих частей и не подменяй production path заранее готовым результатом helper.
- Не используй исходный кандидат или его самоотчёт как эталон корректности. При обнаружении дефекта проследи причину и исправь минимально в границе своей части.
- После исправлений повторно проверь затронутые цепочки и выполни только разрешённые formatting/whitespace-проверки.
- Обнови/создай `docs/reviews/threads-domain-split/threads-turn-cancellation.review.md`: начальный и итоговый commits, база, scope/files, найденные и исправленные проблемы, зависимости, написанные tests, самоаудит, реальные выполненные команды и оставшиеся ограничения.
- Зафиксируй результат в указанной ветке с отключёнными hooks. Итоговый commit передай отдельно от текста отчёта, если он не может ссылаться сам на себя.

В финале передай commit, ветку, абсолютный worktree, ссылку на отчёт, состояние дерева, краткое описание правок и ограничения. Явно укажи: тесты, cargo check, компиляция, сборки, приложение, Gateway, тестовые CLI-агенты и генераторы не запускались; компилируемость и прохождение тестов не подтверждены.

Это передача на внешнее ревью своей части. Общая приёмка будет после ревью всех четырёх частей и их совместной интеграции.
