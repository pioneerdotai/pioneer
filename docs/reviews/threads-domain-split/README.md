# Threads execution: разделение готового кандидата

Разделены существующие изменения. Реализация заново не переписывалась и не принимается этим переносом. Исходная ветка и worktree сохранены.

- Исходный кандидат: `9d86bc3eb65cc58f0e67bf3022a545dfec0b14b6`.
- Исходная база: `bb85dcdf7080164c4dc6480d12410e5e38edade8`.
- Исходный worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-domain-execution`.
- Основной checkout наблюдался чистым на `1e8231118ac6ef328c14241e0e5770de07eade65`; не изменялся этой работой.
- Полный HEAD последней ветки передаётся в финальном сообщении; файл не ссылается на собственный будущий commit.

## Последовательность

| № | Ветка | База собственной части | Промпт |
|---|---|---|---|
| 1 | `fix/threads-tool-access` | `bb85dcdf7080164c4dc6480d12410e5e38edade8` | [threads-tool-access.prompt.md](threads-tool-access.prompt.md) |
| 2 | `fix/threads-independent-root-launch` | `0361ef587bff0b33f2c38edf87d254e3c1769547` | [threads-independent-root-launch.prompt.md](threads-independent-root-launch.prompt.md) |
| 3 | `fix/threads-turn-cancellation` | `13b22123b19026b1ab4701e72c884f3efe4a15b3` | [threads-turn-cancellation.prompt.md](threads-turn-cancellation.prompt.md) |
| 4 | `fix/threads-dispatch-recovery` | `4ea81a4c67a9d24876e4333989dae1955b00b943` | [threads-dispatch-recovery.prompt.md](threads-dispatch-recovery.prompt.md) |

Worktree названы по назначению:

- `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-tool-access`.
- `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-independent-root-launch`.
- `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-turn-cancellation`.
- `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-dispatch-recovery`.

Зафиксированные HEAD первых трёх веток при разделении:

- 1: `0361ef587bff0b33f2c38edf87d254e3c1769547`.
- 2: `13b22123b19026b1ab4701e72c884f3efe4a15b3`.
- 3: `4ea81a4c67a9d24876e4333989dae1955b00b943`.

Отправлять промпты последовательно 1 → 2 → 3 → 4. После внешней приёмки исправлений предыдущей части родительский ревьюер переносит их в зависимые ветки и обновляет базу handoff. Одновременная независимая доработка всех четырёх веток сейчас не предполагается. Каждая будущая PR сравнивается со своей родительской веткой; первый PR — с main. Текущий main при разделении не подмешивался.

## Границы кода

1. **Access/catalog:** обычный CLI-каталог, workspace/source authorization, optional creation grants, option filtering, fresh/legacy Task admission ceiling и неизменность persisted Task admission. Общие test fixtures размещены здесь целиком.
2. **Root/start:** standalone-root provenance, graph/input/response binding, current-user admission, первый API/CLI dispatch, activation CAS и единая typed-proof preparation chain. Общие failure types и storage classifiers здесь нужны Stop и следующему delivery слою.
3. **Stop/control:** foreground admission вместе с завершением первого ещё не активированного turn; mailbox/process ACK и separate canonical completion; initial Stop, storage faults, pending projection, отмена графа/потомков, atomic terminal persistence, guarded memory sync/resume и оба связанных recovery caller. Включает schema/constructor updates и соответствующие регрессии.
4. **Delivery/reconciliation:** окончательные отказ/неопределённость, post-dispatch outcome confirmation, terminal replay/cleanup, restart, compaction, bounded selectors и две индексные миграции; outbox/CLI/restart/resume matrices. Исторический отчёт исходного кандидата сохранён без изменения только в этой части.

Общие файлы разделены по изменённым функциям и зависимостям, а не целиком:

| Общий файл | Распределение |
|---|---|
| `authorization/execution.rs` | creation grants/proofs — 1; independent-root proof/admission — 2 |
| `message/agent_action_tools.rs` | creation options — 1; root writer/typed preparation/initial dispatch — 2; foreground writer hunk — 3; rejection wrapper/terminal replay/outcome confirmation — 4 |
| `crud/lib.rs` | root graph/CAS и failure type export — 2; atomic terminal/cancellation foundation — 3; outbox exports — 4 |
| `repositories/agent_domain.rs` | root graph и failure type — 2; graph cancellation rediscovery — 3; selectors/bookkeeping/compaction — 4 |
| `message/agent_runtime.rs` | storage-classifier visibility — 2; cancellation, guarded sync и recovery consumers — 3 |
| `message/turn_handlers.rs` | CLI definitions — 1; typed preparation/CLI failure transport — 2; generic resume bridge — 3 |
| `task_tools/mod.rs` | ceiling/readmission — 1; typed internal materialization — 2 |
| `thread/mod.rs` | foreground conflict и terminal/resume memory foundation — 3 |
| Test modules | общие fixtures/permissions — 1; proof unit test — 2; Stop/foreground/recovery race — 3; delivery/restart/failure matrices — 4 |

В промежуточных частях 2–3 существующий dispatcher сохраняет прежнее имя. Тело взято из final inner с сохранением прежних terminal shortcuts до части 4; storage classifiers добавлены отдельно. В части 4 вводится итоговая wrapper/inner структура. Это единственная структурная адаптация границы функций; итоговый продукт должен побайтово совпасть с кандидатом.

## Проверки и ограничения

- Для каждого промежуточного дерева выполнен `cargo fmt --all -- --check`; форматирование не подтверждает типизацию.
- Прочитаны карты зависимостей двух read-only подзадач, учтены связанные API/callers и размещение tests. Статический поиск новых function references не выявил отсутствующих перенесённых definitions.
- Выполнены `git diff --check` и `git diff --cached --check` для фиксируемых изменений.
- Сверка Git blobs/modes выполнена: все 3952 исходных tracked paths совпадают с исходным кандидатом, потерь и изменений нет. Единственные добавления — четыре файла `*.prompt.md` и этот README в `docs/reviews/threads-domain-split/`.
- При финальной проверке в main наблюдались параллельные изменения browser skill. Эта работа их не создавала и не меняла; чистота подтверждена для исходного worktree и всех четырёх новых worktree.
- Исходный отчёт исполнителя не является результатом новой внешней приёмки. Полный аудит девяти последних исправлений и приёмка всех частей впереди.

Тесты, cargo check, компиляция, сборки, приложение, Gateway, тестовые CLI-агенты, миграции, EXPLAIN, benchmarks и генераторы не запускались. Компилируемость и прохождение тестов не подтверждены. DB/службы/установленные skills не изменялись. Commits выполнялись с отключёнными hooks; merge, push, deploy и создание PR не выполнялись.

Для координации использованы [Tasks](/Users/alexander/.codex/skills/pioneer-system-tasks/SKILL.md) и [Subagents](/Users/alexander/.codex/skills/pioneer-system-subagents/SKILL.md).
