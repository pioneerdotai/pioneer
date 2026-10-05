# Agent Plugins — этап B

**READY_FOR_STAGE_B_REVIEW**. A принят координатором; B ожидает source review. Это не принятие B, behavioral evidence или разрешение на C/тестирование.

## Snapshot

- Branch: `feature/agent-plugins-simple`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/agent-plugins-simple`.
- Base HEAD: `6a137048716aef7fe1822e92417d5bd7424274ac`.
- Final implementation HEAD: `7e09cad56a48a6832350fa63684624a1c0116b94`.
- Code commits: `2c9dffc1` Gateway/contracts, `d0ff4647` shared client, `7e09cad5` desktop.
- Следующий commit содержит только handoff; delivery HEAD: `git log -1 --format=%H -- implementation-notes/agent-plugins-stage-b.md`.

## Реализовано

- Existing bounded Skills upload получил default-compatible `purpose=skill|plugin`. Desktop folder нормализуется в tar.gz с assets/modes; archive bytes отправляются выбранному Gateway. Native owner/expiry/cancel/limits сохранены; plugin RPC requests привязаны к connection. Локальный desktop path на Gateway не передаётся.
- `plugins/preview|list|details|install`, `plugins/changed`, existing Skills+MCP admission. Pure preview и install сверяют fingerprint. Parent reserve и consume finalized upload атомарны; повтор source upload возвращает тот же parent. Stable package/data, bounded pending reserved IDs, последовательная установка; sibling failures сохраняются как failed links, parent возвращает partial. Незавершённый parent закрыт для execution. Новых таблиц или executor нет; добавлены только upload purpose и nullable turn selection column.
- Genuine A `install_skill_source(PackageMember)` и `install_mcp_plan(portable_install_plan)` выполняют native storage/policy/audit/security/OAuth/reload. Child+ownership остаются в принятом атомарном native write. Второго installer/OAuth engine нет.
- Common native/client Skills/MCP catalogs сохраняют owned records с server-derived `plugin_owner`; standalone management/picker selectors скрывают их. Raw owned selection отвергается, implicit owned invocation принудительно выключена. Native restrictions сохраняются.
- Gateway сохраняет parent presentation и сам строит existing leaves по workspace/state/revision. Existing API preparation/composite authorization, compact catalog/read_skill, MCP projection/dispatch используются дальше. Turn snapshot prepared→ready проверяет parent/revision/ownership и persisted native bindings/projection. Поздние read_skill/dynamic handlers/MCP повторно проверяют gate; unlink child не превращает cached owned ID в standalone.
- Owned skill definition/policy остаются native; runtime asset context указывает на contained `package/skills/<member>`. Public envelope остаётся 64 capabilities; только внутренний Gateway→ThreadManager preparation принимает до 256 resolved leaves. Explicit root Agent launch получает только server-derived plugin grants после проверки client standalone grants.
- Shared parent capability/attachment содержит ID+expected revision; generic draft/history/edit/retry/voice representations сохраняют parent. Desktop Plugins management/card показывает component/runtime statuses, включая existing HTTP auth-required. Separate retained Plugins modal выбирает только parents; composer/history показывают по одному removable parent chip. Upload progress подписан на existing SkillsUpload publications через desktop registrar.

## Изменения contracts

Protocol: Plugins DTO/RPC/notification, optional owner metadata, Plugin capability/attachment, upload purpose. CRUD: reserve/settle/failure и turn selection gates на existing repositories. Agent TurnToolProvider: default late skill authorization hook; Gateway implementation проверяет ownership/snapshot. Native Gateway turn preparation дожидается durable skills confirmation перед provider/tools; standalone definitions, installers и public envelope limit сохранены. Native standalone extraction остаётся на прежней ветке; plugin branch нормализует contained links после всех archive writes.

## Checks — фактические exits

Все compilation commands агента — non-test targets; final runs используют `CARGO_INCREMENTAL=0`. Hooks отключены при commits.

| Check | Exit/result |
| --- | --- |
| `cargo check -p pioneer-crud --lib` (ранний) | 0 |
| `cargo check -p pioneer-gateway --lib` (final-5) | 0; final-2/3/4 также 0 |
| `cargo check -p pioneer-desktop --bin pioneer-app` (final-4) | 0; final-3 также 0; dependency future-incompat warning `block 0.1.6` |
| `cargo run -p pioneer-protocol --bin schema -- schemas` | 0 |
| `cargo run -p pioneer-client --features schema --bin schema -- schemas/client` | 0 |
| Scoped `rustfmt --edition 2024 --config skip_children=true --check` | 0 |
| `git diff --check` | 0 |
| Intermediate Gateway/client/desktop-plugin checks | 101: missing model initializer, exhaustive matches, RPC argument, GPUI width/event types; исправлены |
| Intermediate desktop `--lib` | 101: package имеет только production bin; заменено правильным target |
| Intermediate desktop bin / Gateway final-1 | 101: wrong activity scope / misplaced record reference; исправлены |
| Queued client/UI и уже ошибочный Gateway check-3 | 130: остановлены агентом при нехватке disk; удалён только generated incremental output этого worktree |

Genuine generation также обновила ранее stale base schemas (permissions/memory и existing client publications/file-view contracts). Их source behavior не менялся; hashes/artifacts вручную не подменялись. Миграции, приложение, MCP/OAuth fixtures и functional/device scenarios не запускались. Source tracing skill+assets → PackageMember → native definition/package asset context → compact/read_skill; stdio/HTTP → portable typed adapter → native MCP/OAuth/runtime — **не runtime test**.

## Regression sources — NOT_RUN / NOT_COMPILED агентом

- Protocol `plugins.rs`: client expansion/ownership rejection.
- CRUD `plugin_ownership_tests.rs`, `plugins.rs`: atomic owner/consume, concurrent reserve, sibling failure/native policy retention, snapshot bounds/identity, prepared gate and removed-child identity.
- Gateway `message/tests.rs`, `turn_handlers.rs`, `thread/mod.rs`: trusted parent expansion, raw child/stale revision/foreign workspace, root grant fabrication rejection, public 64 vs internal 256.
- Gateway `skills/upload.rs`, `skills/catalog.rs`: contained/escaping links, prevention of redirected writes, unchanged legacy rejection, implicit host constraint preserving native enabled policy.
- Agent `chat/skill_tools.rs`: late revocation blocks read and dynamic inner handlers.
- Client `plugins.rs`, `skills/catalog.rs`: one parent through draft/history serde, CLI chip retention for explicit rejection, unavailable parents, owned records retained in common catalog and hidden by selectors. Existing fixtures обновлены для DTO fields.

При работе замечен внешний процесс `cargo check --jobs 2 ... -p pioneer-gateway --tests` в этом worktree. Он не запускался/не останавливался агентом; владелец и результат не установлены, evidence не использовалось. Статус NOT_COMPILED выше относится к проверкам этого агента, а не утверждает отсутствие внешней compilation.

## Остаётся / границы B

CLI/ACP и detached Task Plugin execution явно unsupported; chip не удаляется молча и unmanaged provider plugins не подключаются. API provider без native tool calling также получает explicit refusal. Полные provider/continuation/recovery paths — C. Full enable/disable/config/OAuth/update/remove/retry/startup repair и остановка sessions перед file mutations — C; unfinished installing parents пока закрыты и не repaired автоматически. Mobile/native contracts/platform effects — D; mobile worktree не изменялся. Folder/archive delivery использует existing tar.gz format; plugin inventory ограничен bounded reader limit 1000. Behavioral/UI/conformance/OAuth evidence отсутствует до отдельно разрешённого тестирования. B-specific source compilation blockers не осталось; требуется coordinator review.
