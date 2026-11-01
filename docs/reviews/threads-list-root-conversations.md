# threads_list: самостоятельные разговоры — передача на внешнее ревью

## Ревизии и изоляция

- База: `3f862da3f494f3e6048e211f426c24cd81d2aeac`.
  Проверены чистое состояние основного checkout, `git worktree list`, отсутствие
  целевой ветки/пути; после `git fetch origin main` локальный main и origin/main совпали.
- Ветка: `fix/threads-list-root-conversations`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/threads-list-root-conversations`.
- Commit реализации, регрессий и bundled Threads: `c765f18c22d8fa246f4c0342d639afcd10910346`.
- Итоговый review commit: HEAD этой ветки, docs-only commit, содержащий данный
  отчёт; его полный SHA передан в финальном сообщении. Получить локально:
  `git rev-parse fix/threads-list-root-conversations`.
  Собственный SHA невозможно включить в содержимое того же commit.
- Commits создаются с `core.hooksPath=/dev/null`. Основной checkout и соседние
  worktree не редактировались; соседняя threads_search ветка не интегрировалась.

## Причина и правило

Gateway использовал workspace-кандидатов без фильтра вида разговора. ACL
разрешает читать некоторые внутренние истории, поэтому после авторизации они
становились отдельными карточками общего списка.

Теперь repository применяет до LIMIT:

- `sidebar_visibility = visible`;
- `access_class IN (private, workspace)`;
- `origin_kind NOT IN (task_run, taskRun, system)`.

Это существующая семантика обычных видимых разговоров: пользовательский список
в membership отбирает visible private/workspace, а пользовательский management
дополнительно исключает TaskRun/System (включая legacy spelling taskRun).
Collaborative, DirectMessage и legacy User сохраняются. Автор AgentExecution,
наличие lineage, parentThreadId, execution root/home capsule и архивность не
определяют принадлежность списку. Поэтому видимый самостоятельный разговор
агента остаётся кандидатом даже при наличии связи происхождения.

Это фильтр вида списка, а не новое разрешение чтения. Миграций, backfill и
изменений сохранённых признаков production/dev тредов нет.

## Repository, пагинация и повторная проверка

Используется существующий SeaORM Entity-путь через CrudStore и scoped connection.
У `list_thread_read_candidates_page` единственный production caller —
`thread_list_page`; остальные callers — CRUD-регрессии. Добавлена точечная
проверка принадлежности списку с тем же predicate и проекцией ключа без чтения
больших title/preview. Она применяется при обработке кандидата, включая
продолжение фрагмента, и в окончательной проверке общего handler только для
threads_list. Изменившийся кандидат пропускается с продвижением позиции;
изменившийся источник незавершённого фрагмента либо уже сформированного ответа
приводит к существующей безопасной ошибке, а не выдаче исключённой карточки.

Сохранены `created_at DESC, id DESC`, исходная строка created_at с полной
точностью, диапазон limit 1–100, quantum 200, byte budget и JSON-карточки.
Quantum не становится общим пределом; при недоступных ordinary-кандидатах
возможны пустые страницы с eof=false. Позиция продвигается по последнему
рассмотренному кандидату. Логика nextCursor/eof и metadata digest не менялась.

Только list_scope заменён с `list-created-desc-v1` на
`list-conversations-created-desc-v2`. Подписанные старые cursors, включая
незавершённую дочернюю карточку, отклоняются существующим механизмом
`read_cursor_incompatible`: требуется restart с исходными параметрами.
Cursors истории, поиска и потомков не менялись.

## Доступ к потомкам и общий handler

`thread_source_readable` и admitted execution grants не изменены. Сохранены
формирование childReference, выборочное открытие отдельной истории и direct/
recursive children traversal. Истории потомков автоматически не загружаются.
Общий ThreadReadHandler обслуживает native и admitted CLI; отдельного
CLI-фильтра нет. Обновлены описание инструмента и только bundled Threads
SKILL/tool-schemas/workflows. Tasks/Subagents, policy blocks, установленные
skills и схемы не редактировались; protocol payload не менялся.

## Написанные регрессии — не запускались

- CRUD `conversation_pages_include_archived_and_keep_exact_position_after_updates`:
  excluded rows перед LIMIT; nanosecond continuation, tie order, удаление
  completed candidate, новые записи и несвязанные обновления. Существующие
  assertions сохранены; fixture теперь соответствует новому виду списка.
- Gateway `list_conversation_boundary_excludes_internal_quanta_but_keeps_visible_agent_roots`:
  Collaborative/DirectMessage/User от AgentExecution с lineage; архивные
  visible roots; 205 внутренних кандидатов между самостоятельными разговорами,
  включая вложенные, hidden и TaskRun/System; private ACL и полнота всех страниц.
- Существующие ACL/creation-key регрессии дополнены restricted list assertion;
  205 недоступных кандидатов теперь ordinary private, чтобы сохранить проверку
  пустой непоследней страницы, точного порядка и отсутствия повторов.
- Существующие oversized title/fragment регрессии сохранены; добавлено изменение
  sidebar_visibility/origin_kind/access_class при незавершённой карточке.
- `list_rejects_previous_candidate_scope_even_for_pending_readable_child_fragments`:
  подписанные cursors предыдущего scope с обычной позицией и pending fragment.
- Существующая internal-child регрессия дополнена отсутствием в общем списке,
  наличием в children list и открытием ID из реального childReference истории.
- Существующий admitted facade regression для Codex/Claude/OpenCode проверяет
  наличие самостоятельного caller и отсутствие созданного скрытого ребёнка в
  общем списке; ребёнок перечисляется через children list и читается прежним
  history handler. Сохранены проверки replay, числа созданных тредов и сообщения.

Baseline/fixed исполнением не сравнивались: пользователь запретил запуск.

## Самоаудит и фактические проверки

Промежуточный аудит охватил canonical boundary, repository callers, cursor/eof,
фрагменты, старые scopes, ACL, native/CLI и bundled instructions. Исправлены
list-fixtures; shared child fixtures оставлены скрытыми. Недоступные ordinary
кандидаты сохранены отдельно от исключённых внутренних, чтобы не потерять
регрессию пустых непоследних страниц.

Финально перечитан весь собственный diff и затронутые цепочки: repository →
CrudStore → list page → metadata fragments → окончательная проверка handler;
также общая авторизация, children/history, protocol types и admitted CLI facade.
Повторная проверка использует минимальную проекцию; исправлена опечатка в skill.
Не найдено необходимости менять другие инструменты или persistence.

Фактически выполнены:

- чтение AGENTS.md (root и CRUD), исходников, manifests/CI, поиск callers;
- обычные Git status/worktree/branch/log/diff/fetch/commit операции;
- `cargo fmt --all --check`: обнаруживает существующий import-order diff в
  `crates/observability/src/lib.rs`; посторонний файл оставлен как в базе;
- `cargo fmt --all`: форматирование; его постороннее изменение observability
  отменено, затем применялось только форматирование затронутых crates;
- `cargo fmt -p pioneer-crud -p pioneer-gateway --check`: успешно;
- `git diff --check`: успешно. Перед commit также проверяется staged whitespace.

Тесты, cargo check, компиляция, сборки, приложение, Gateway, CLI-агенты,
миграции, генераторы, benchmarks и живое воспроизведение не запускались.
Mock-сценарии не выполнялись. Компилируемость и прохождение тестов не
подтверждены. Окончательная приёмка и разрешение на исполняющие проверки
выполняются отдельно после внешнего ревью.

## PR #257 CI follow-up

The first CI run (`41315685491`, head `18b00363ae03bd13cc9041a5dbee0204898c7c73`)
stopped at `rustfmt-check` in the Linux build job. Its log reports the existing
import-order difference in `crates/observability/src/lib.rs` noted above.
The Rust test shards were skipped because the build job failed at formatting.

After the request to fix CI and push again, moved the `SentryEventKind` import
to rustfmt's expected position in the test module. No assertions or runtime
behavior changed. Local validation: `cargo fmt --all --check` and
`git diff --check`. Tests and compilation were not run locally; the new push
lets GitHub Actions perform its normal checks without skip directives.
