# AGENTS.md

## Crate Role

`pioneer-desktop` is the GPUI desktop shell for macOS, Windows, and Linux. It owns
desktop UI, desktop lifecycle, desktop persistence hooks, localization, menus,
themes, dialogs, window state, and OS integration.

Desktop should consume shared client behavior from `pioneer-client`, but it
should not consume mobile/FFI boundary wrappers just to make code look shared.

## What Belongs Here

Keep code in this crate when it needs desktop-specific context:

- GPUI views, components, windows, menus, theme/layout state, input handling, and
  rendering;
- desktop dialogs, file pickers, OS open/reveal actions, and platform-specific
  integrations guarded with `cfg` where needed;
- localized UI copy and user-facing desktop error presentation;
- desktop keystore/config/file-system persistence wiring;
- local gateway service install/start/recovery and desktop service management;
- applying shared `pioneer-client` reductions to GPUI state.

Do not describe desktop behavior as macOS-only unless the code is actually gated
to macOS. This app targets macOS, Windows, and Linux.

## Using `pioneer-client`

Before writing client behavior in desktop, check whether it is shell-neutral.

Use `pioneer-client` for:

- gateway registry/profile plans and connectivity validation;
- workspace create/rename/switch/bootstrap planning and reductions;
- thread tree refresh reductions, folder/thread placement planning, and
  sidebar-neutral tree logic;
- websocket runtime event filtering and notification/effect reductions;
- provider, skill, MCP, settings, timeline, artifact, composer, and agents-doc
  logic when it does not depend on GPUI.

When moving logic from desktop into `pioneer-client`, keep behavior equivalent and
leave only GPUI/application state application in desktop.

## What Not To Do

- Do not add a dependency on `pioneer-client-ffi`.
- Do not route desktop through Nitro/TS/C ABI request wrappers such as
  bridge-only `*Request`, `*Result`, or `ClientEvent` envelopes unless a change
  explicitly proves that the wrapper improves desktop code.
- Do not duplicate business rules that already exist in `pioneer-client`.
- Do not add mobile assumptions such as React Native lifecycle, MMKV,
  SecureStore, or Nitro event semantics.
- Do not move GPUI/localization/dialog/platform code into `pioneer-client`.

The right shared layer for desktop is the typed Rust domain planner/reducer
layer, not the FFI JSON boundary layer.

## Localization

All user-visible desktop text and errors must remain localized through the
desktop localization system. Do not hard-code new UI strings in flow code when
they should be translated.

## Testing

Use focused checks for desktop changes:

```bash
cargo test -p pioneer-desktop
cargo check -p pioneer-desktop
```

When shared client code changes as part of desktop work, also run:

```bash
cargo test -p pioneer-client
```

