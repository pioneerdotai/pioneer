# Agent Plugins — повторное ревью этапа A

**READY_FOR_STAGE_A_REVIEW**. Исправлены только A-01…A-04 из действующего proposal v2 / stage-a-review. Принятие A и запуск тестов ожидают координатора; B не начат.

## Snapshot

- Branch: `feature/agent-plugins-simple`.
- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/agent-plugins-simple`.
- Base HEAD (reviewed): `c26b590164684937f588c77acab2b3089754ecd7`.
- Final implementation HEAD: `5df59361227b2798d1a75a10767e8fb05c4974cb`.
- Следующий commit содержит только этот handoff. Его delivery HEAD определяется через `git log -1 --format=%H -- implementation-notes/agent-plugins-stage-a.md`; production/test sources совпадают с final implementation HEAD.
- Основной Rust worktree/main и архивная implementation branch не изменены. Mobile worktree остаётся чистым на `2867e763c0603b0c170bab61c57e7ad1d133d0c5`.

## Исправления и shared contracts

| Finding | Изменение |
|---|---|
| A-01 | Existing ownerless standalone `pplugin_*` разрешён для обычного update. Новые reserved names запрещены. Gateway проверяет фактическую ownership до записи secrets; правило reserved-name existence повторяется в existing native atomic DB write вместе с прежним ownership fence. Genuine owned updates с совпадающей ownership остаются разрешены; adoption standalone/foreign children запрещён. |
| A-02 | `./` executable path допускает пробелы как один literal token. Command не получает shell parsing или placeholder expansion. Bare command с whitespace и NUL по-прежнему запрещены; существующие filesystem/snapshot containment и file-kind checks сохранены. Native runtime использует прежний `Command::new` с отдельными args. |
| A-03 | Корректный SKILL остаётся обнаруженным при denied ancillary asset. `denied_package_path` сообщает отдельный path failure; invalid/escaped SKILL.md по-прежнему пропускается. Native folder copier и его запрет symlinks не менялись: невозможность copy остаётся installation failure, а не ложной conformance diagnostic. |
| A-04 | Existing MCP/OAuth paths различают standalone explicit Authorization override и portable fallback через server-derived origin. Portable headers больше не отключают provider/client/sign-in/status/identity paths. Managed HTTP client удаляет только portable configured Authorization и только при наличии managed client или generated auth token у конкретного request (POST/GET/DELETE). Anonymous fallback и прочие configured headers сохраняются. Standalone explicit Authorization сохраняет прежнее поведение. Второго OAuth engine нет. |

Новых таблиц, coordinator/jobs/generations/leases, UI/composer или lifecycle B–D не добавлено. DB boundaries/scheduling не изменены: новое name правило читает existing row внутри прежнего writer transaction; ownership revalidation остаётся там же.

## Фактические checks

| Check | Exit / результат |
|---|---|
| Coordinator `cargo check --locked --offline -p pioneer-gateway --lib`, review HEAD | **130, INTERRUPTED**, согласно stage-a-review-evidence.json; не PASSED. |
| Эта итерация: `CARGO_INCREMENTAL=0 cargo check --locked --offline -p pioneer-gateway -p pioneer-plugins -p pioneer-crud -p pioneer-mcp -p pioneer-mcp-oauth --lib` | Первый запуск **101**: новая ошибка E0308 (ключ `HashMap::remove` без borrow); исправлена. Повторный итоговый запуск **0**, 10m15s. |
| `rustfmt --edition 2024 --config skip_children=true --check` на 13 затронутых Rust sources | **0**. |
| `git diff --check` и `git diff --check main` | **0**. |

Два прежних Gateway dead-code warnings: `portable_install_plan` и `PackageMember` не используются до B. Commit hooks отключены для соблюдения запрета test runners. Test targets не компилировались. Приложение, MCP fixtures, OAuth providers, migrations, functional/device scenarios не запускались. Compilation не является behavioral/conformance evidence.

## Regression sources — NOT_RUN

- `crates/crud/src/plugin_ownership_tests.rs`: legacy reserved-name update, new reserved-name rejection, same-ID adoption rejection, genuine owned update и foreign-owner collision.
- `crates/plugins/tests/loading.rs`: literal spaced command/args/placeholder text; bare shell string, NUL и существующий escaping symlink; denied asset при valid SKILL и прежний native-copy rejection; invalid/escaped SKILL.md с valid sibling.
- `crates/mcp/src/oauth.rs`: anonymous fallback, generated request authorization priority и сохранение standalone branch.
- `crates/mcp-oauth/tests/oauth_lifecycle.rs`: existing OAuth explicit sign-in/client/status и generated header для portable installation; non-auth header и совместимый OAuth binding сохраняются. Existing standalone explicit-header case дополнен assertions о header/client/sign-in поведении. Fixture изменён только в test source для наблюдения отправленных MCP headers.

## Unresolved

Известных дополнительных source fixes по A-01…A-04 не выявлено; координатор должен подтвердить исправления. Все regression sources **NOT_RUN и NOT_COMPILED**; их behavior и type checking остаются непроверенными до разрешённого этапа. Следующие этапы и ранее описанные обязанности delivery/parent gating/lifecycle остаются за пределами этой итерации.
