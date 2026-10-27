# Shared Voice Input settings implementation map

Base: dcc63b7142bf681cba96de8267a15e8383e82b11; initially clean worktree on
feat/voice-transcription-modes. Source inspection and edits only; no checks run.

Integration boundary: supervisor.settings_snapshot() carries
an engine-only placeholder true; voice_session_start must merge the saved mode
before freezing it. `MessageProcessor::apply_persisted_voice_input_mode(&mut
settings)` is now available in message/mod.rs and callable from voice_handlers.rs.
It must be called under the existing settings start/update lock, propagating
failure before creating the pipeline. Static reinspection confirms the gateway
owner has now added that call in voice_session_start. I did not edit
voice_handlers.rs/supervisor.rs (owned separately).

- Config `GatewayVoiceConfig` supplies `transcribe_while_recording = true` when
  missing. The legacy `transcription_strategy` remains accepted; neither legacy
  enum value selected the actual incremental session pipeline at this base.
  Its conversion in voice/transcription.rs has no production caller. Only the
  new boolean selects the session behavior.
- `GatewaySettings.voice_input` persists a gateway-wide override in the existing
  TOML settings file. An optional boolean preserves inheritance from config;
  explicit false is serialized, reloads, and survives unrelated partial updates.
- Protocol settings snapshots carry a boolean (Rust Default and serde missing
  both true); updates carry Option<bool>, so omission never resets the mode.
- `apply_protocol_update` prepares a candidate, validates, then updates the
  owner. Separate persisted `changed` from engine `reconfigure`: only enabled,
  provider, model and retry affect the existing active-session busy guard and
  supervisor. A mode-only change never calls apply_desired.
- `message/dispatch.rs::update_gateway_settings_with_saver` keeps the existing
  permission/request context, settings lock, atomic save-before-effects and
  snapshot publication sequence. A saver error exits before mode publication.
- Session start reads the effective settings config (gateway pipeline owner is
  handled separately); the boolean must be captured there, outside engine
  generation. Existing sessions retain their captured mode.
- `SettingsIntent::VoiceTranscribeWhileRecording` and
  `VoiceInputSettingsAction::SetTranscribeWhileRecording` call the same typed
  shared planner. Settings runtime owns pending/error/epoch checks and publishes
  the authoritative snapshot; desktop calls typed dispatch, mobile uses existing
  FFI dispatch. FFI contains no new business rule or local default.
- Affected exported schemas include SettingsIntent/ClientIntent,
  VoiceInputSettingsAction/PlanRequest/Plan, GatewayVoiceInputSettings/Update,
  settings page/store/get/update/refresh contracts and their containing DTOs.
  Mobile snapshots are updated separately by the mobile owner; generation and
  boundary seals remain deferred and must not be forged.

## Intermediate static audit

1. Fixed the initial derived protocol Default (would incorrectly be false) with
   an explicit Default and matching serde default true.
2. Used Option<bool> in persisted overrides rather than a second default true
   value that would shadow an explicitly false config. Explicit overrides retain
   precedence; absence inherits config. No reset-to-inherited UI is introduced.
3. Separated persisted `changed` from `reconfigure`; preflight busy checking and
   post-save supervisor application both use `reconfigure || retry_install`.
4. Found supervisor notifications replace the entire client voice settings.
   Runtime events now merge the persisted mode while holding the settings-update
   lock, read the latest supervisor snapshot after acquiring that lock, and
   broadcast through the existing management notification method. A successful
   settings update explicitly broadcasts its authoritative Voice snapshot.
   No new polling loop or engine generation change is involved.
5. Checked existing shared notification-generation fences: newer Voice ingress
   supersedes delayed settings responses; gateway/authorization replacement
   discards stale events. Existing tests now carry false mode to cover it.
6. Rechecked all explicit literals with rg; fields in owned source/test files
   are updated. Gateway owner updated supervisor's internal engine snapshot
   placeholder and uses the persisted-mode merger at session start.

## Tests authored or extended, never executed

- Config roundtrip false; both legacy strategy values with missing mode and
  explicit false.
- Protocol missing snapshot bool defaults true; missing update remains None;
  false snapshot/update serialization and roundtrip.
- Gateway real settings writer/readback: false persists while disabled and
  without model; explicit config false inheritance; selection/model changes and
  partial updates retain mode; independent gateway file remains true.
- Existing busy handler test: mode changes in Recording/Finalizing/Transcribing
  preserve active session and supervisor generation, are received by another
  authenticated connection, and survive subsequent model selection.
- Save failure injection through real settings handler: no file mutation,
  generation change, or peer Voice-success notification.
- Existing throttled supervisor-notification test retains persisted false
  through Missing/Downloading/progress events.
- Shared planner exact mode-only update and no-op; selection does not reset mode.
- Typed SettingsIntent to actual runtime failed-save completion preserves
  authoritative false and exposes existing error on Voice page.
- Thin FFI JSON dispatch accepts the shared intent and marks only Voice pending.
- Existing shared ingress/current-generation/delayed-response tests use false.

## Formatting and deferred contracts

Ran only these executable formatting commands (besides read-only Git/text tools):

```
cargo fmt --package pioneer-config --package pioneer-protocol --package pioneer-client --package pioneer-client-ffi
cargo fmt --package pioneer-gateway -- --config skip_children=true crates/gateway/src/settings.rs crates/gateway/src/message/mod.rs crates/gateway/src/message/dispatch.rs crates/gateway/src/message/tests.rs
```

Tracked Rust schema snapshots also need the same manual changes or later
accepted regeneration; this task owner has not edited schemas outside its
assigned crate scope. Affected root snapshots:
gateway_settings_update, gateway_voice_input_settings_update,
gateway_notification, gateway_voice_input_settings,
gateway_settings_get_response, gateway_settings_update_params,
gateway_voice_input_status_changed_notification, gateway_settings_snapshot,
gateway_settings_update_response (all schemas/*.json).

Affected schemas/client snapshots:
voice_input_settings_action, gateway_ws_event,
cli_runtime_provider_settings_plan, settings_page_publication,
gateway_settings_update_plan, gateway_settings_get_response,
gateway_settings_state, gateway_settings_store,
voice_input_settings_plan_request, client_intent, voice_input_settings_plan,
gateway_settings_update_response, client_intent_dispatch_dto.

Boundary-source manifests, hashes, seals, prebuilt libraries and dependencies
were not modified. No compile/type/schema-generation success is claimed.
