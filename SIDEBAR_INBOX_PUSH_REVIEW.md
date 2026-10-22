# Sidebar / inbox addressed push — source review

## Isolation and allowed work

Rust worktree `/Users/alexander/Code/pioneer/pioneer/.worktrees/sidebar-inbox-push`, branch `feature/sidebar-inbox-push`, fixed base `adb446a542e4c0c24e2d011fe73b184c9af1fb29`.
Mobile worktree `/Users/alexander/Code/pioneer/pioneer/.worktrees/sidebar-inbox-push-app`, branch `feature/sidebar-inbox-push`, fixed base `32609e4cb5431f0efc3e0573db7c5354d474cc5a`.
Both primary checkouts had empty `git status --short` at start. Requested paths/branches were free. No fetch, rebase, merge, push, database opening, migrations or measurements. Bases are immutable for this change.

At final observation Rust primary main/HEAD had advanced externally to `a13feb3dcbf5688619b46a2dc620d3d51e754ddc`; its status remained clean. This worktree still uses the recorded `adb446a...` base, with no merge/rebase or transfer of external changes. Mobile primary main/HEAD remained the recorded base and clean.

Audit read: REPORT, SERVER_AUDIT, CLIENT_AUDIT, FINAL_HARNESS_REVIEW, HARNESS_REVIEW_RESOLUTION, summary.csv and archived activity/inbox/client probe source. Audit Rust base b7132117 differs from fixed current base primarily in CLI provider readiness/model selection and version/lockfile changes; addressed producer/repository/tree/inbox baseline paths remain present. Experimental SQL rewrites, COW/in-place reducers and build watcher opt-outs are not copied.

## Baseline map

| Boundary | Existing owner/source | Preserved responsibility |
| --- | --- | --- |
| Canonical user/assistant message commit | gateway message_turn, timeline_notifications, message_mutations, task_delivery | persisted semantic blocks, unread eligibility and hidden task children |
| Task lifecycle | gateway message/tasks, task_handlers | Starting/Running/terminal and root activity; progress excluded |
| Thread read | gateway timeline_handlers; CRUD thread_timeline_projection | monotonic prefix cursor and authoritative personal count |
| Inbox receipts | gateway task_completion_inbox; CRUD task_completion_inbox | read/read-all/viewed, canonical membership and access-start/activation boundaries |
| Authorization | gateway authorization registry/resolver/service; session manager | existing identity/session lease, action and resource policy |
| Transport | existing JSON-RPC WebSocket session outbound | bounded send, current session lifecycle, old hints |
| Tree state | client workspaces controller/directory/projection | demand, generation/read/touched guards, metadata restore/create, canonical folder aggregates |
| Inbox state | client tasks/notifications | request identity, loaded keyset prefix/frontier, actions and dedupe |
| Shells | desktop ClientCore publications; mobile FFI binding/core publications | existing badges/spinners, lifecycle demand and full bridge snapshots |
| Contracts | protocol + client/FFI schema exporters; mobile client generators | typed shared data, generated TypeScript/source hashes |

## Validation boundary

No tests or application compilation is authorized before external review. Self-review and parent acceptance of delegated source work are not external acceptance. Delegated CRUD, Gateway, static-review findings and test-only integration candidates were read and accepted for integration. The client assignment was superseded/cancelled after the parent directly completed and reviewed its final integration and recovery corrections; no unaccepted candidate is used as final evidence. No delegated work remains pending.

Actual formatting and generation (no acceptance implied):

- `cargo fmt -p pioneer-crud` (CRUD owner twice).
- `cargo fmt -p pioneer-protocol`, `cargo fmt -p pioneer-crud -p pioneer-protocol`; delegated owners also ran `cargo fmt -p pioneer-client` and `cargo fmt -p pioneer-gateway` during editing.
- Final formatting: `cargo fmt -p pioneer-client -p pioneer-crud -p pioneer-gateway -p pioneer-protocol`, then `cargo fmt -p pioneer-client -p pioneer-protocol` after the last recovery refinement, both exit 0. This formats source; it does not compile or run tests.
- Mobile dependency preparation: `bun install --frozen-lockfile --ignore-scripts`; exit 0, lifecycle scripts disabled, mobile lockfile unchanged.
- Mobile generator attempt 1: `PIONEER_RUST_ROOT=/Users/alexander/Code/pioneer/pioneer/.worktrees/sidebar-inbox-push bun run client:schema`; exit 1 while compiling pioneer-client schema dependency graph: exhaustive routing matches missing the newly introduced WorkspacePush variant in gateway/event_router.rs and runtime.rs. FFI exporter not reached. This is a generator failure, not a test/check run; fixed generator rerun will be recorded below.

Successful canonical generation (all commands explicitly use the isolated Rust root):

- Rust cwd: `PIONEER_RUST_ROOT=/Users/alexander/Code/pioneer/pioneer/.worktrees/sidebar-inbox-push cargo run --locked -p pioneer-protocol --bin schema -- schemas`: exit 0 three times, including after final protocol edits; compiled and ran pioneer-protocol's `schema` binary.
- Mobile cwd: `PIONEER_RUST_ROOT=/Users/alexander/Code/pioneer/pioneer/.worktrees/sidebar-inbox-push bun run client:schema`: attempts 2, 3 and 4 exit 0, final run after all source integration/recovery refinements; compiled and ran `pioneer-client --features schema --bin schema`, then `pioneer-client-ffi --features schema --bin schema`, and their required host library/dependency graph. No Gateway/desktop/iOS/Android target was built. No test target was compiled.
- Mobile cwd: `PIONEER_RUST_ROOT=/Users/alexander/Code/pioneer/pioneer/.worktrees/sidebar-inbox-push bun run client:types`: exit 0 three times; final run consumes the final schemas.
- Mobile cwd: `PIONEER_RUST_ROOT=/Users/alexander/Code/pioneer/pioneer/.worktrees/sidebar-inbox-push bun run client:contract`: exit 0, including after final source edits and generation, `Client source contract: matched`. This **writes** boundary-source.json using canonical cargo metadata/source hashes; it is not the deferred independent check-source/reproducibility check.

Canonical generation also exposes pre-existing schema staleness: ProviderRequestId's Rust regex already accepts `req_`, whereas checked-in base schemas omitted it. No Rust validation rule is changed by this task. Mechanical schema synchronizations are retained to avoid hand-editing generated files or weakening verification. This is an inherited generated-diff scope deviation from a strictly push-only patch. TypeScript types for this regex do not change. The final generated diff, including source hashes and new files, was read in full. Cargo.lock, bun.lock, manifests, native bindings/assets and localization are unchanged.

## Итоговый поток и границы

Опциональный `workspace/push` регистрирует demand на существующем соединении. Начальная metadata/list загрузка остаётся; первый пакет нового client-generated epoch — authoritative indicator/inbox snapshot с revision=1. Далее идут последовательные absolute patches. Legacy клиенты не регистрируют demand и сохраняют hints. Unsupported registration возвращает shared client к прежним owners.

Gateway хранит один scope на физическое соединение, а не на пользователя или тред. Scope привязан к полному AuthenticatedSessionPrincipal и workspace. Вычисления между сессиями намеренно не объединяются: эквивалентность restricted authorization scopes не выводится из principal_id. Это консервативная граница безопасности; экономия fanout/count между устройствами здесь не заявляется.

Фоновый worker — отдельная Tokio task с Maintenance CrudStore. Request ack сохраняет request scope. Source PushMutation захватывает поколение ДО операции и удерживает только источник вычисления, без DB capacity. При неизменном поколении worker повторно использует готовый thread/read count, inbox read remove+total или read-all replacement+total. Чужое изменение инвалидирует cache. Viewed ack содержит proofs, но не total: один последующий count нужен и не дублирует count в ack. Inbox receipts не сдвигают thread cursor.

Общий Rust reducer применяется под controller → inbox → registry locks, сначала проверяет весь пакет, затем создаёт один ClientCore transition с drafts затронутых domains. Для числового tree batch выполняется один прежний canonical project. Inbox-only пакет не проектирует дерево. Desktop и mobile получают прежние publications; FFI adapter и UI не переписаны. Metadata ThreadUpdated остаются отдельными существующими notifications: worker получает только persisted thin metadata/latest Turn marker, без timeline/items/HTML, и отправляет их под тем же generation/session enqueue gate. Поэтому одна publication на indicator batch не означает одну publication на все metadata notifications burst.

## Изменённые файлы

Пути ниже относительно соответствующего worktree; фигурные скобки перечисляют каждый изменённый source basename.

- Rust `crates/protocol/src/{scoped_push,constants,lib,notification,schema,task}.rs`.
- Rust `crates/crud/src/lib.rs`, `crates/crud/src/repositories/{authorization_scope,task_completion_inbox}.rs`, `crates/crud/tests/task_completion_inbox.rs`.
- Rust `crates/client/src/{core,runtime,schema}.rs`, `crates/client/src/gateway/event_router.rs`, `crates/client/src/tasks/notifications.rs`, `crates/client/src/workspaces/{controller,directory,mod,push}.rs`.
- Rust `crates/gateway/src/authorization/registry.rs`, `crates/gateway/src/session/mod.rs`.
- Rust `crates/gateway/src/message/{access_invalidation,agent_action_tools,dispatch,message_mutations,message_turn,mod,notifications,scoped_push,task_completion_inbox,task_delivery,tasks,tests,thread_handlers,timeline_handlers,timeline_notifications,turn_handlers}.rs`.
- Rust `crates/gateway/src/message/tests/{scoped_push,scoped_push_client}.rs`, этот `SIDEBAR_INBOX_PUSH_REVIEW.md`.
- Rust canonical generated: `schemas/{workspace_push_params,workspace_push_response,workspace_push_notification}.json` (новые), `schemas/gateway_notification.json`, `schemas/task_completion_inbox_{entry,entry_response,list_response}.json`; остальные затронутые `schemas/*.json` содержат только унаследованную синхронизацию ProviderRequestId regex, описанную выше. Полный точный список выдаёт `git diff --name-only adb446a542e4c0c24e2d011fe73b184c9af1fb29 -- schemas`.
- Mobile `modules/pioneer-client-nitro/boundary-source.json`.
- Mobile `src/client/generated/{gateway_ws_event,task_completion_inbox_entry,task_completion_inbox_entry_response,task_completion_inbox_list_response,task_inbox_publication,workspace_push_notification,workspace_push_params,workspace_push_response}.ts`.
- Mobile `src/client/schema/{gateway_ws_event,task_completion_inbox_entry,task_completion_inbox_entry_response,task_completion_inbox_list_response,task_inbox_publication,workspace_push_notification,workspace_push_params,workspace_push_response}.json`.
- Mobile inherited regex-only schema synchronization: `src/client/schema/{task_accept_response,task_cancel_response,task_revise_response,task_user_notification,task_user_notification_acknowledge_response,task_user_notification_list_response}.json`.

Desktop и mobile пользуются изменённым shell-neutral client через прежние adapters. Отдельные desktop/JS reducers не добавлены. Координационные заметки делегированных задач в итоговый diff не включаются.

## Producers → dirty keys → consumers

Все пути отмечают ключи после durable commit; progress/heartbeat сами по себе не считают индикаторы.

| Причина | Ключи | Consumer / recovery |
| --- | --- | --- |
| User Message; executable/native Turn start; agent-authored Message | thread unread + thin metadata | message_turn.rs:1052; turn_handlers.rs:10456; agent_action_tools.rs:3486,3923; scoped worker canonical unread repository |
| Edit/delete canonical user Message | thread unread + metadata | message_mutations.rs:535,635; прежняя семантика удаления |
| Completed canonical agent text/HTML, late completion | thread unread + metadata | timeline_notifications.rs:20; hidden Task execution исключён прежним lineage predicate |
| Видимая Task delivery в root | root thread unread + metadata | task_delivery.rs:1103,1183,1279; terminal run и delivered root — разные изменения |
| thread/read | unread одного thread только для principal | timeline_handlers.rs:162; source ack reuse, peers dirty; cursor monotonic отдельно |
| RunStarting/RunStarted/terminal, review/revision, lineage/link/recovery | activity root | tasks.rs:434; resolver.rs:981; все executor types, потомки и другие active runs сохраняются |
| Terminal run / фактически изменённая inbox row | exact run ID | tasks.rs:521; canonical membership+total, upsert/remove |
| Task-level metadata/root/access без точного run ID | inbox_reset | tasks.rs:525; authoritative loaded prefix в том же epoch; без tree расчёта |
| Inbox read | run ID/remove + total | task_completion_inbox.rs:189; source ack reuse; peers exact membership |
| Read all | inbox replacement | task_completion_inbox.rs:74,91; unchanged INSERT SELECT охватывает незагруженные страницы; source готовый response, peers inbox-only reset |
| Реальные viewed proofs | подтверждённые run IDs | task_completion_inbox.rs:304; receipts остаются независимы от thread read; один count в worker, без prefix reload после принятого push |
| Собственная отмена | committed RunCancelled / task terminal, прежние receipts | тот же Task event/inbox path; не меняет чужие сообщения |
| Folder/thread create/move/delete, AGENTS metadata/title, ACL change | structural metadata recovery / retire | thread_handlers.rs:2170, access_invalidation.rs:23; обычный numeric patch не создаёт unknown thread |
| Task wake bus lag | прежний durable fanout quantum | message/mod.rs:3670; bus — wake, committed events читаются из существующего durable source |

## Статические трассы порядка

| Сценарий | Трасса и результат |
| --- | --- |
| Событие во время SQL/auth/serialization | scoped_push.rs:435 захватывает generation и забирает sets; :513 проверяет dirty-again и возвращает старые+новые ключи для одного follow-up. Финальный :655 gate снова проверяет generation под mutex рядом с try_send. Повторный конфликт/overflow даёт content-free recovery, а не stale value с новой revision. |
| Поздний tree RPC | directory.rs:563 captures push/read baseline; :735 сохраняет уже принятые значения при смене stamp; touched-key merge продолжает защищать move/delete и bootstrap restore. |
| Поздний inbox RPC/ack | notifications.rs:392 compares captured stamp; negotiated mutation values приходят через ordered publisher. Просроченный response не восстанавливает removed row/старый total. |
| Snapshot одновременно с событием | server installs scope before registration reply; первый calculation идёт через те же before/after/final gates. Client push.rs:86 installs epoch до RPC, допускает snapshot до registration ack. Inbox recovery читает canonical prefix до точного старого cursor, закрывая list→register gap, включая >30 новых completions. |
| Дубль / перестановка / gap | client push.rs:326 принимает только revision1 snapshot, затем contiguous next revision; старые packets игнорируются, gap retire/recovery. Ранний indicator не переводит metadata load в Ready. Два подряд неуспешных bootstrap без принятого snapshot отключают rich delivery до новой connection/authorization identity; существующий worker отправляет unsubscribe и возвращается к legacy owners, без retry timer. |
| Собственный ack одновременно с publisher | PushMutation begin до операции инвалидирует старый calculation и ставит source hold; finish использует результат только при неизменном generation. Final gate не позволяет старому calculation обойти ack. Ошибка/drop снимает hold и ставит bounded recovery. |
| Revoke / identity renewal / workspace switch | access_invalidation.rs:23 retire до остальных awaits. session/mod.rs:500 проверяет полный expected principal/workspace/termination/expiry под connections read lock непосредственно перед enqueue. Watch завершает worker; stale final scope не отключает новое соединение. Client core.rs:3008 очищает negotiation перед protected stores. |
| Disconnect / reconnect / server restart | закрытие session watches завершает worker и удаляет только Arc-matched owner; новая connection/authorization identity создаёт новый epoch и snapshot. Durable replay store не добавлен. |
| Demand suspend/resume / foreground | прежние lifecycle owners retire/re-register; inbox resume с отсутствующим store запускает прежний Refresh owner до регистрации. Pagination временно перестаёт принимать старый epoch; новый snapshot закрывает пропущенные изменения. |
| Unknown ID / malformed payload | неизвестный thread не создаётся indicator patch; существующие bounded recovery owners получают refresh. protocol/notification.rs:183 сохраняет known-method push даже без params как Unknown. Client восстанавливает единственный текущий scope, не доверяя повреждённому workspace в JSON. |
| Inbox ACL/remove | outgoing removes фильтруются по bounded known-inbox IDs конкретного scope. Неизвестный недоступный run не раскрывается через remove. Upsert проходит canonical eligibility и существующий Gateway TaskRead resolver. |

## Ограничения состояния, SQL и lifetime

- Server: <=256 connection scopes; registration <=4096 thread IDs и <=4096 loaded run IDs, каждый ID <=128 bytes, epoch <=128 bytes. Pending dirty sets ограничены; inbox overflow превращается в inbox-only reset. Metadata coverage и known inbox sets ограничены 4096. Один calculation на scope, один dirty-again follow-up; нет бесконечного retry/backlog.
- На отправку одного прохода действует 1 MiB serialized budget, включая metadata frames. Большой inbox snapshot/replacement сокращается по exact keyset frontier. Overload существующего outbound lane закрывает соединение для reconnect snapshot; смена scope лишь завершает старого worker. Это не оценка общей RAM процесса: transient row fields сохраняют существующие DB bounds.
- Scope исчезает при unsubscribe/replacement/session close/revoke. Нет offline queues, per-thread actors, timers, новых таблиц/миграций/indexes/backfill, ANALYZE/optimize или нового transport.
- Activity: DISTINCT discovery <=512, scope subsets <=128, прежний parser/predicates и финальный TaskRead. Cursor берётся из discovery, включая filtered-empty pages. LIMIT proof: DISTINCT не содержит run-specific fields, joins к task/root/turn/thread/lineage-child/admission-task уникальны; максимум одна scope row/task. Parent Idle/Waiting не исключает active descendants.
- Inbox: run-first <=100 для обычного patch; list/recovery остаются workspace-first. Count и membership/metadata читаются в одном scoped read snapshot, один count за проход. Broad history get не становится unread membership. Сохраняются receipt/activation/access-start predicates и public error projection.
- DB read snapshot закрывается перед Gateway parsing/error projection/serialization/auth fanout/enqueue. Physically query_only reader pool и serialized writer не меняются. Metadata fetch берёт один persisted Thread и thin latest Turn marker, не timeline/history/HTML.
- Shared inbox сохраняет `(completed_key, run_id)`, dedupe, item revisions, pending actions/native-effect reconciliation и загруженный frontier. Ordinary incoming values удерживают ранее загруженный диапазон до явного cap4096; затем cursor указывает на последнюю оставшуюся row. Numeric tree projection остаётся прежней, с cycles и saturating sums; selection/expansion/scroll widgets не менялись.

## Регрессии в исходниках, без запуска/компиляции

- CRUD task_completion_inbox.rs: addressed membership против broad history/read/revoke; дробные timestamps; 513 tasks с двумя active runs, filtered-empty progress и scope LIMIT.
- Shared push.rs: registration snapshot до ack, одна batch transition, folder sums/activity через циклическую topology, неизменная navigation, late tree RPC, duplicate/gap/malformed (включая omitted params и неверный workspace) recovery; повторный провал bootstrap возвращает legacy snapshot authority.
- Shared notifications.rs: exact frontier/fractional ordering, upsert/remove+authoritative total, late mutation ack, read-all replace, resume через настоящий request worker, сохранение страницы при 30→31 completion.
- Gateway tests/scoped_push.rs: committed canonical completion и реальный thread/read → существующий outbound, contiguous revisions и тот же absolute count из source ack, без legacy whole-tree hint; неизвестный недоступный run не раскрывается через remove.
- Gateway tests/scoped_push_client.rs: реальный loopback WS и ClientCore production ingress, committed completion/read меняют unread 0→1→0 без нового thread/tree RPC. Только auth/me synthetic; domain data не подаются вручную.
- Session final enqueue regression: workspace switch, renewed principal и expiry исключают queued old payload. Existing folder move/delete/deep/cycle/saturation и inbox viewed/read-all/access tests сохранены, не перезапускались.

Итоговый diff относительно обеих фиксированных баз перечитан полностью, включая новые файлы, все тестовые добавления, schemas/TypeScript/source hashes и этот документ. Отдельно сверены producers после commit, SQL predicates/binding order/LIMIT, lock order/final enqueue, early snapshot/late RPC и lifetime. При перечитывании исправлены first-preview metadata race, unknown remove privacy, пропущенные producers, omitted-params recovery, сохранение inbox frontier и повторный неуспешный bootstrap. Это статические выводы; перечисленная матрица ещё требует functional execution, особенно dirty-during-SQL/overflow, HTML/late completion, read-all/viewed/own-cancel и restricted sessions.

## Отложенные обязательные проверки — НЕ выполнены

После полного внешнего source review и отдельного явного разрешения:

```sh
cargo check --locked -p pioneer-crud -p pioneer-gateway -p pioneer-client -p pioneer-client-ffi -p pioneer-desktop
cargo nextest run --locked -p pioneer-crud --test-threads 2 --no-fail-fast --retries 0
cargo nextest run --locked -p pioneer-client --features schema --test-threads 2 --no-fail-fast --retries 0
cargo nextest run --locked -p pioneer-client-ffi --features schema --test-threads 2 --no-fail-fast --retries 0
cargo nextest run --locked -p pioneer-gateway --lib --test-threads 2 --no-fail-fast --retries 0 scoped_push
cargo nextest run --locked -p pioneer-gateway --lib --test-threads 2 --no-fail-fast --retries 0 queued_derived_payload
cargo nextest run --locked --workspace --test-threads 2 --no-fail-fast --retries 0
cargo test --locked --workspace --doc
```

Mobile worktree, с тем же конкретным Rust root:

```sh
PIONEER_RUST_ROOT=/Users/alexander/Code/pioneer/pioneer/.worktrees/sidebar-inbox-push bun run client:check
PIONEER_RUST_ROOT=/Users/alexander/Code/pioneer/pioneer/.worktrees/sidebar-inbox-push node modules/pioneer-client-nitro/scripts/boundary-integrity.mjs check-source
PIONEER_RUST_ROOT=/Users/alexander/Code/pioneer/pioneer/.worktrees/sidebar-inbox-push bunx tsc --noEmit
PIONEER_RUST_ROOT=/Users/alexander/Code/pioneer/pioneer/.worktrees/sidebar-inbox-push bun run lint
```

Также отложены protocol schema reproduction, desktop/mobile native builds и functional UI validation: selection/expansion, collapsed folder badges/spinners, move/delete, virtualization/scroll, accessibility, workspace/principal/reconnect/foreground transitions. Native scripts `rust:build:ios`, `rust:build:android` и приложения не запускались. Их выполнение требует следующего разрешённого этапа, не следует из успешной генерации host schemas.

Отдельно требуется performance validation на согласованных fixtures: current/dense/backlog, per-principal/restricted-session fanout, writer/read admission/occupancy, CPU/bytes/peak+steady RAM Gateway и shared client, commit→visible p50/p95 desktop/mobile, reconnect/recovery и loaded-prefix bursts. Исполняемый production performance harness в этой задаче не создавался; команды и новые замеры не выдумываются. Цифры исходного аудита — ориентиры компонентов, не подтверждённый speedup этой реализации.

Полная компиляция, runtime и производительность новой реализации не подтверждены. Внутренний статический review не является внешней приёмкой.

Реализация подготовлена к внешнему ревью исходного кода. Тесты, cargo check, сборки приложений и замеры не запускались. Отложенные обязательные проверки перечислены в отчёте.
