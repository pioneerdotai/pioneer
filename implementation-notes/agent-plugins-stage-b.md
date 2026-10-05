# Agent Plugins — этап B, исправления ревью

**READY_FOR_STAGE_B_REVIEW**. B-01…B-03 подготовлены для повторного source review; это не принятие B и не разрешение на C/тестирование.

## Snapshot

- Branch: `feature/agent-plugins-simple`.
- Worktree и cwd всех checks ниже: `/Users/alexander/Code/pioneer/pioneer/.worktrees/agent-plugins-simple`.
- Base этой итерации: `0d3afb27e1ff17aa1572b4dd7f1d599801cccb4d`, clean при восстановлении; B имел CHANGES_REQUESTED.
- Принятый A: `6a137048716aef7fe1822e92417d5bd7424274ac`; исходный code HEAD B: `7e09cad56a48a6832350fa63684624a1c0116b94`.
- Final code HEAD: `a235fdeae16fb89cf170ecf9e528f048912d1b3b`.
- Следующий commit меняет только этот handoff. Delivery/final HEAD: `git log -1 --format=%H -- implementation-notes/agent-plugins-stage-b.md`.

## B-01…B-03

- **B-01:** перед первым archive link write проверяется весь bounded набор путей: symlink не может быть ancestor другого entry, включая link с ещё не созданным parent directory. Конфликт отвергается до создания links; порядок entries не влияет на результат. `symlink_metadata` обнаруживает также dangling overlaps. Regular writes по-прежнему предшествуют links; contained leaf links нормализуются Snapshot, escaping leaf assets остаются Denied. Standalone extractor по-прежнему отвергает links.
- **B-02:** normalizer использует прежние native catalog/effective policy и operational visibility, чтобы убрать disabled/unavailable Skills из input leaves и server root grants. Prepared snapshot сохраняет installed candidate identities для последующей проверки. Native resolver с реальной MCP availability выполняет trust/security/dependency checks и строит prompt/tools/read_skill. После durable native skill projection ready snapshot оставляет только Skill IDs из этого resolver event и MCP IDs с фактическими native bindings. В коротком writer transaction повторно проверяются исходный snapshot, все parent revisions/gates, ownership всех candidates и bindings оставшихся leaves; MCP projection header обязателен. Exclusions допускают allowed siblings и пустой execution; missing/foreign ownership, настоящий missing binding выбранного leaf и write failure остаются ошибками. Parent presentation/chip и поздние gates сохраняются. Card показывает native skill runtime status и disabled MCP.
- **B-03:** Plugins list/details и все install/idempotent ответы передают RequestContext в inventory projection. Из native Skills/MCP lists выделены общие disclosure predicates; native handlers и Plugins вызывают одну проверку AuthorizationService/operational visibility. SUPERUSER сохраняет complete inventory, включая failed children. MEMBER не получает keys/IDs/statuses скрытых children или failed uninstalled members. Authored package diagnostics/pointers и component diagnostics скрыты у участника; при необходимости возвращается нейтральное parent notice без пути. Details по parent ID использует ту же projection.

## Reuse и contracts

Genuine A `install_skill_source(PackageMember)` и `install_mcp_plan` не изменены; native installation/policy/audit/OAuth storage остаётся authoritative. Второго installer/OAuth/policy engine нет. В shared native list paths изменено только выделение существующих disclosure predicates, standalone semantics сохранены. Внутренний `CrudStore::ready_plugin_selection` теперь принимает server-derived resolved Skill IDs; caller — existing durable TurnSkillsResolved handler. `SkillsRuntimeContext.validation_policy` доступен внутри message module для native card resolution. Public DTO/schema, client/desktop/mobile contracts не менялись: generation и desktop/native rebuild не требовались и не выполнялись. Нет новых таблиц, jobs/generations/leases или UI/composer rewrite.

## Actual non-test checks

Все команды выполнялись в cwd из Snapshot; hooks отключены через `git -c core.hooksPath=/dev/null commit`.

| Command | Actual exit / evidence |
| --- | --- |
| `CARGO_INCREMENTAL=0 cargo check -p pioneer-gateway --lib` — первый запуск | 0; `/tmp/pioneer-stage-b-fix-gateway-1.log`, Finished dev profile, 2m27s |
| `CARGO_INCREMENTAL=0 cargo check -p pioneer-gateway --lib` — final production sources | 0; `/tmp/pioneer-stage-b-fix-gateway-final.log`, Finished dev profile, 1m47s |
| `rustfmt --edition 2024 --config skip_children=true --check` на всех 13 затронутых Rust files | 0, включая final regression sources |
| `git diff --check` | 0 |

Scoped formatting также завершилось с exit 0. Это compilation/format evidence, не проверка поведения. Test runners/targets, migrations, приложение, MCP/OAuth fixtures и functional/device scenarios агентом не запускались.

## Regression sources — NOT_RUN / NOT_COMPILED агентом

- `gateway/message/skills/upload.rs`: external ancestor + nested link/new parent directory в обоих порядках; отказ до links и отсутствие любых внешних writes. Прежние contained-link/Denied asset/legacy rejection/ordinary archive sources сохранены.
- `gateway/message/tests.rs`: normalization fixture теперь содержит настоящий contained SKILL; native disabled policy убирает execution, сохраняя один parent и structural candidates; raw owned/stale/foreign rejection сохранены.
- `agent/chat/skills.rs`: disabled и trust-excluded installed candidate + allowed sibling; blocked ID отсутствует в prompt/tools/read_skill, allowed остаётся; all-excluded пуст. Используется настоящий native resolver/runtime-plan builder без provider/process execution.
- `crud/plugin_ownership_tests.rs`: разрешённый sibling с actual native binding становится ready; disabled sibling закрыт поздним gate; all-excluded становится ready без leaves. Настоящий missing selected binding, injected DB write failure и foreign candidate отвергаются без ready publication.
- `gateway/message/tests.rs`: один parent для SUPERUSER/MEMBER, реальные native Skills/MCP lists и оба Plugins RPC; disabled Skills/MCP и failed member скрыты у MEMBER, allowed siblings доступны, authored diagnostics/paths/IDs не обходят фильтр. SUPERUSER видит полный inventory.

## External activity / unresolved / границы

Ранее сообщался внешний `cargo check ... --tests` в worktree: **UNKNOWN_EXTERNAL_ACTIVITY**, владелец и результат не установлены. В этой итерации при read-only process inspection такой runner не наблюдался. Внешние процессы не запускались/не останавливались агентом; их evidence не используется. NOT_COMPILED относится к собственным commands, не ко всему worktree за всё время.

Known B-specific compilation blockers нет; behavioral/disclosure/archive evidence остаётся NOT_RUN до разрешённого тестирования и source review. C (full lifecycle/OAuth controls/providers/continuations/repair/stop acknowledgement) и D (mobile/platform effects/native contracts) не начаты. Main, archived implementation и mobile branches/worktrees не изменялись; push/deployment отсутствуют.
