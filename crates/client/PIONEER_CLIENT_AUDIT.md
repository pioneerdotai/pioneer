# pioneer-client API Audit

Date: 2026-06-10

Scope:

- `crates/client/src`
- consumers in `crates/desktop/src`
- consumers in `crates/client-ffi/src`

Goal:

Identify code that currently lives in `pioneer-client` but is only consumed by the
mobile/Nitro FFI boundary, and separate it from code that is genuinely shared by
desktop and mobile.

## Method

This audit used the current tree, not only git diff. The checks were:

1. inventory public `pioneer-client` symbols;
2. search each suspicious symbol in `crates/desktop/src` and `crates/client-ffi/src`;
3. manually inspect the relevant modules to avoid trusting raw symbol counts for
   common names such as `switch_workspace`;
4. classify each item as shared core, boundary-only, or mixed.

## Verification Pass

This document was re-checked against the current `desktop`, `client`, and
`client-ffi` tree on 2026-06-10 before using it as a refactoring guide.

The re-check included:

- desktop imports of `pioneer_client::*`;
- targeted usage counts for every symbol listed in the hard evidence matrix;
- manual inspection of gateway runtime/setup, workspace actions/bootstrap,
  workspace management wrappers, thread tree refresh/projection, and runtime
  event handling.

Important false positives were checked manually. For example, desktop has local
methods named `create_workspace` and `rename_workspace`, but it does not import
or call `pioneer_client::workspaces::management::{create_workspace,
rename_workspace}`.

## Executive Result

`pioneer-client` is currently mixed.

There is real shared core already used by desktop:

- gateway profile/runtime planning;
- workspace action planning and reductions;
- workspace bootstrap;
- thread tree refresh reductions and desktop sidebar planning;
- runtime websocket event filtering/reduction;
- notification/effect routing.

But there is also boundary/mobile-only API in `pioneer-client`:

- JSON/request DTOs that only `client-ffi` consumes;
- mobile-shaped event projection (`ClientEvent`);
- mobile-shaped thread tree query/level projection;
- workspace management wrapper functions that duplicate orchestration already
  handled by desktop through lower-level shared actions.

That means the current crate is not cleanly separated into:

- domain core shared by all shells;
- shell boundary API for Nitro/TS;
- desktop GPUI/application orchestration.

## Hard Evidence Matrix

These symbols are used by `client-ffi` and not by `desktop`.

| Area | Symbol / file | Current desktop usage | Current FFI usage | Verdict |
| --- | --- | ---: | ---: | --- |
| contracts | `contracts::ClientEvent` | no | yes | Boundary-only event shape |
| contracts | `contracts::ClientCommand` | no | no direct consumer found | Boundary contract, not core |
| contracts | `ClientGatewayConnectionEvent` | no | transitive via `ClientEvent` | Boundary event payload |
| contracts | `ClientErrorEvent` | no | transitive via `ClientEvent` | Boundary event payload |
| contracts | `ClientGatewayWsTimings` | no | transitive via connect request | FFI request payload |
| contracts | `ClientGatewayConnectRequest` | no | yes | FFI request DTO |
| contracts | `ClientGatewayConnectResult` | no | yes | FFI result DTO |
| runtime | `ClientRuntime::reduce_ws_events_to_client_events` | no | yes | Boundary projection |
| gateway setup | `RemoteGatewayValidationRequest` | no | yes | FFI request DTO |
| gateway setup | `PlanAddRemoteGatewayRequest` | no | yes | FFI request DTO |
| gateway setup | `AddRemoteGatewayPlan` | no | yes | FFI result DTO |
| gateway setup | `AddAndActivateRemoteGatewayRegistryPlan` | no | yes | FFI result DTO |
| gateway setup | `plan_add_remote_gateway_request` | no | yes | FFI wrapper |
| gateway setup | `plan_add_and_activate_remote_gateway_registry_request` | no | yes | FFI wrapper |
| gateway setup | `PlanActivateGatewayRequest` | no | yes | FFI request DTO |
| gateway setup | `plan_activate_gateway_registry_request` | no | yes | FFI wrapper |
| gateway setup | `PlanSetGatewayWorkspaceRequest` | no | yes | FFI request DTO |
| gateway setup | `plan_set_gateway_workspace_registry_request` | no | yes | FFI wrapper |
| gateway setup | `PlanUpdateRemoteGatewayRequest` | no | yes | FFI request DTO |
| gateway setup | `plan_update_remote_gateway_registry_request` | no | yes | FFI wrapper |
| gateway setup | `PlanDeleteRemoteGatewayRequest` | no | yes | FFI request DTO |
| gateway setup | `plan_delete_remote_gateway_registry_request` | no | yes | FFI wrapper |
| workspaces | `workspaces::management::*Request` | no | yes | FFI wrapper DTOs |
| workspaces | `workspaces::management::*Result` | no | yes | FFI wrapper results |
| workspaces | `workspaces::management::{switch,create,rename}_workspace` | no direct import/use | yes | FFI orchestration wrapper |
| threads | `ThreadTreeRefreshRequest` | no | yes | FFI request DTO |
| threads | `ClientThreadTreeSnapshot` | no | yes | Mobile-shaped projection |
| threads | `ClientThreadTreeQueryData` | no | yes | Mobile-shaped result |
| threads | `ThreadTreeLevelRequest` | no | yes | FFI request DTO |
| threads | `ClientThreadTreeLevel` | no | yes | Mobile-shaped projection |
| threads | `refresh_thread_tree` | no | yes | FFI orchestration wrapper |
| threads | `client_thread_tree_level` | no | yes | Mobile-shaped projection |

## Shared Core That Should Stay In `pioneer-client`

These are currently useful shared pieces, not mobile-only.

### Gateway

Keep in `pioneer-client`:

- `gateway::runtime` profile operations:
  - `activate_gateway`
  - `plan_add_remote_gateway_profile`
  - `plan_update_remote_gateway_profile`
  - `plan_delete_remote_gateway_profile`
  - `remote_delete_fallback_endpoint`
  - registry lookup and apply/rollback helpers
- `gateway::setup` base planner functions:
  - `validate_remote_gateway_address`
  - `validate_remote_gateway_connection`
  - `validate_remote_gateway_connection_with_timings`
  - `plan_add_remote_gateway`
  - `plan_activate_gateway_registry`
  - `plan_set_gateway_workspace_registry`
  - `plan_update_remote_gateway_registry`
  - `plan_delete_remote_gateway_registry`
- shared structs needed by those planners:
  - `AddRemoteGatewayInput`
  - `AddRemoteGatewayChange`
  - `AddRemoteGatewayApplyMode`
  - `UpdateRemoteGatewayRegistryInput`
  - `DeleteRemoteGatewayRegistryInput`
  - `ActivateGatewayRegistryPlan`
  - `SetGatewayWorkspaceRegistryPlan`
  - `UpdateRemoteGatewayRegistryPlan`
  - `DeleteRemoteGatewayRegistryPlan`
  - `RemoteGatewayValidation`
  - `GatewayAuthTokenUpdate`
  - `GatewayAuthTokenWrite`

Desktop already consumes this level in:

- `crates/desktop/src/gateway/connectivity.rs`
- `crates/desktop/src/gateway/runtime/mod.rs`
- `crates/desktop/src/app/flow/helpers.rs`

Do not move this layer into `client-ffi`.

### Workspaces

Keep in `pioneer-client`:

- `workspaces::actions`
  - `plan_workspace_create`
  - `plan_workspace_rename`
  - `plan_workspace_switch_from_ui`
  - `reduce_workspace_create_success`
  - `reduce_workspace_rename_success`
  - `reduce_workspace_switch_success`
  - workspace preference and bootstrap reductions
- `workspaces::bootstrap`
  - `WorkspaceBootstrapRequest`
  - `bootstrap_workspace_catalog`

Desktop already consumes this lower-level shared path in:

- `crates/desktop/src/app/workspaces/actions.rs`
- `crates/desktop/src/app/flow/workspace_bootstrap.rs`
- `crates/desktop/src/app/flow/workspace_switch.rs`

### Threads

Keep in `pioneer-client`:

- thread tree request params:
  - `thread_tree_params`
- refresh reductions:
  - `reduce_thread_tree_refresh_success`
  - `reduce_thread_tree_refresh_failure`
  - `ThreadTreeRefreshContext`
  - `ThreadTreeRefreshSuccessReduction`
  - `ThreadTreeRefreshFailureReduction`
- desktop/sidebar-neutral tree planning:
  - `SidebarTreeNodeKey`
  - `sidebar_tree_model_from_workspace_data`
  - folder/thread placement planning helpers

Desktop already consumes these from:

- `crates/desktop/src/app/flow/thread_list.rs`
- `crates/desktop/src/app/sidebar/actions.rs`
- `crates/desktop/src/app/sidebar/view.rs`
- `crates/desktop/src/app/root/queries.rs`

### Runtime / Notifications

Keep in `pioneer-client`:

- `ClientRuntime`
- websocket command/event queue
- `drain_applicable_ws_events`
- `reduce_ws_event`
- `reduce_gateway_ws_event`
- `reduce_gateway_notification`
- `ClientRuntimeWsEvent`
- `ClientRuntimeNotification`
- notification effect planning

Desktop already consumes this path in:

- `crates/desktop/src/app/flow/ws_events_pump.rs`
- `crates/desktop/src/app/flow/ws_events_notifications.rs`

## Boundary-Only Code That Should Not Stay In Core As-Is

### 1. `contracts::ClientEvent`

Current file:

- `crates/client/src/contracts/mod.rs`

Problem:

`ClientEvent` is not a desktop domain model. It is a Nitro/TS event envelope.
Desktop uses `ClientRuntimeWsEvent` and `ClientRuntimeNotification` directly.

Correct direction:

- keep `ClientRuntimeWsEvent` and lower-level reductions in `pioneer-client`;
- move `ClientEvent` and `reduce_ws_events_to_client_events` to `client-ffi`;
- or explicitly make desktop consume `ClientEvent` too, but that would be a
  large desktop architecture change and is not currently justified.

Recommended action:

Move `ClientEvent`, `ClientCommand`, `ClientErrorEvent`,
`ClientGatewayConnectionEvent`, `ClientGatewayConnectRequest`,
`ClientGatewayConnectResult`, and `ClientGatewayWsTimings` out of
`pioneer-client` into the FFI/boundary crate, unless a concrete desktop
migration is planned in the same patch.

### 2. Gateway request wrappers

Current file:

- `crates/client/src/gateway/setup.rs`

Mixed state:

- base planner logic is shared and desktop uses it;
- `*Request` DTOs and `*_request` functions are FFI wrappers around the base
  planner logic.
- `AddRemoteGatewayPlan` and `AddAndActivateRemoteGatewayRegistryPlan` are also
  FFI result DTOs. Desktop uses `AddRemoteGatewayChange` from the base planner
  instead.

Correct direction:

- keep base functions in `pioneer-client`;
- move request DTOs, FFI result DTOs, and `*_request` wrappers to `client-ffi`;
- schema export for these request DTOs should come from the FFI boundary, not
  from core shared domain code.

Do not delete the base gateway planners. They are the good part.

### 3. `workspaces::management`

Current file:

- `crates/client/src/workspaces/management.rs`

Problem:

This module wraps lower-level shared workspace actions into request/result
functions that perform transport calls. Desktop does not use this module; desktop
uses `workspaces::actions` directly and performs its own GPUI/application flow.

Correct direction:

- keep `workspaces::actions` and `workspaces::bootstrap` in `pioneer-client`;
- move `workspaces::management` to `client-ffi`, or remove it and let FFI call
  the lower-level shared actions explicitly;
- do not keep it in core unless desktop is intentionally switched to the same
  wrapper and the wrapper improves desktop code.

Current desktop does not need this module.

### 4. Mobile-shaped thread tree projection

Current file:

- `crates/client/src/threads/tree.rs`

Problem:

The file contains both:

- existing shared desktop tree/reduction code;
- new mobile/Nitro query data and level projection.

The following are currently FFI/mobile-only:

- `ThreadTreeRefreshRequest`
- `ClientThreadTreeSnapshot`
- `ClientThreadTreeQueryData`
- `ThreadTreeLevelRequest`
- `ClientThreadTreeLevel`
- `refresh_thread_tree`
- `client_thread_tree_level`

Correct direction:

Choose one of two paths:

1. If the projection is truly shell-neutral, rename it to neutral domain names
   and switch desktop sidebar/tree code to consume it where it improves desktop.
2. If it is just the mobile query shape, move it to `client-ffi`.

Do not leave it as `ClientThreadTree*` inside the shared core while desktop uses
a completely different path.

## Schema Export Problem

Current files:

- `crates/client/src/schema.rs`
- `crates/client/src/contracts/export.rs`

Problem:

Schema export currently exposes a mix of:

- genuine shared contracts;
- FFI/mobile-only DTOs;
- request wrapper DTOs that are not desktop core.

Correct direction:

- shared domain schemas can stay in `pioneer-client`;
- Nitro/TS boundary schemas should be generated from the boundary crate or from
  explicitly boundary-scoped modules;
- do not use `schema.rs` as a reason to keep mobile-only DTOs in the shared core.

## Why Not Make Desktop Use Every Wrapper

Desktop can and should use shared `pioneer-client` logic when that logic is a
domain planner, reducer, projection, or runtime primitive.

Desktop should not be forced to use FFI/Nitro wrapper APIs only to make a symbol
look shared.

The distinction is:

- shared core is the code that expresses application behavior independently of a
  shell: registry planning, workspace planning, reducers, websocket/runtime
  semantics, notification/effect planning, thread tree domain projection;
- boundary code is the code that adapts a shell boundary: JSON request/result
  DTOs, Nitro event envelopes, TS-friendly command/event shapes, and functions
  whose main job is to deserialize input, call a lower-level shared planner, and
  serialize a result.

If desktop uses a boundary wrapper like `ClientGatewayConnectRequest` or
`PlanAddRemoteGatewayRequest`, it does not automatically become cleaner. Desktop
is already in Rust and already has typed state, GPUI flow, secret persistence,
rollback handling, loading flags, and localized errors. Forcing it through a
Nitro/TS-shaped request/result envelope would usually add another layer while
hiding the actual domain function that desktop needs.

Therefore the cleanup rule is:

1. If an API improves desktop code and is shell-neutral, rename/generalize it and
   migrate desktop to it.
2. If an API is only a JSON/Nitro/TS boundary around shared lower-level logic,
   keep the lower-level logic in `pioneer-client` and move the wrapper to
   `client-ffi`.
3. If an API is currently mobile-shaped but could become a real shared
   projection, it needs an explicit desktop migration plan. Otherwise it should
   not stay in core under `Client*` names.

## Cleanup Plan

### Phase 1: Freeze new boundary code in `pioneer-client`

Rule:

No new `*Request`, `*Result`, `Client*` shell envelope, or Nitro-oriented schema
type should be added to `pioneer-client` unless desktop uses the same type in
the same change.

### Phase 2: Split gateway setup

Keep in `pioneer-client`:

- base validation/planner functions;
- domain input structs;
- registry plan result structs that are returned by base functions.

Move to `client-ffi`:

- `RemoteGatewayValidationRequest`
- `PlanAddRemoteGatewayRequest`
- `AddRemoteGatewayPlan`
- `AddAndActivateRemoteGatewayRegistryPlan`
- `PlanActivateGatewayRequest`
- `PlanSetGatewayWorkspaceRequest`
- `PlanUpdateRemoteGatewayRequest`
- `PlanDeleteRemoteGatewayRequest`
- all `*_request` wrapper functions.

Expected result:

Desktop keeps using the same clean shared planner functions. FFI still has its
JSON boundary, but the boundary is no longer presented as shared core.

### Phase 3: Split workspace management

Keep in `pioneer-client`:

- `workspaces::actions`
- `workspaces::bootstrap`

Move to `client-ffi`:

- `workspaces::management` request/result wrappers and transport orchestration.

Expected result:

Desktop and mobile both use the same planning/reduction logic, but each shell
owns its transport/application orchestration boundary.

### Phase 4: Resolve thread tree projection

Decision required:

- If desktop should use the new snapshot/level model, rename it to neutral names
  and migrate desktop sidebar/tree code to it.
- If desktop should keep its current sidebar model, move the mobile snapshot/level
  request API to `client-ffi`.

Minimum cleanup:

- remove `Client` prefix from any type that stays in `pioneer-client`;
- move `ThreadTreeRefreshRequest` and `ThreadTreeLevelRequest` to boundary code
  unless desktop also consumes them.

### Phase 5: Move event envelope to boundary

Keep in `pioneer-client`:

- `ClientRuntimeWsEvent`
- `ClientRuntimeNotification`
- lower-level websocket reductions.

Move to `client-ffi`:

- `ClientEvent`
- `ClientCommand`
- `ClientErrorEvent`
- `ClientGatewayConnectionEvent`
- `ClientGatewayWsTimings`
- `ClientGatewayConnectRequest`
- `ClientGatewayConnectResult`
- `reduce_ws_events_to_client_events`.

If `ClientCommand` has no mobile consumer at refactor time, delete it instead of
moving dead API.

Expected result:

Desktop remains on the shared runtime model it already uses. Mobile gets its
Nitro event envelope from FFI, not from the core crate.

### Phase 6: Split schema generation

After phases 2-5:

- `pioneer-client` schema export should contain only shared domain contracts;
- `client-ffi` or a boundary-specific schema module should export TS/Nitro
  request/result/event schemas.

## Non-Goals

This audit does not say that all FFI-only code is useless. Some of it is useful
for the mobile boundary. The issue is location and naming.

This audit also does not recommend forcing desktop to use every mobile wrapper.
That would make desktop worse. The correct shared layer is the domain
planner/reducer layer, not necessarily the JSON request wrapper layer.

## Bottom Line

The current `pioneer-client` has enough shared code to be worth keeping, but it
must be cleaned.

The correct direction is:

- `pioneer-client`: shell-neutral domain planning, reductions, runtime state,
  websocket/event semantics, registry/workspace/thread/timeline logic;
- `client-ffi`: Nitro/TS JSON request/result DTOs, event envelopes, schema for
  the mobile bridge;
- `desktop`: GPUI state application, rendering, dialogs, and desktop-specific
  persistence hooks, while consuming shared planners/reducers from
  `pioneer-client`.
