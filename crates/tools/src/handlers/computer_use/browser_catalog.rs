// Browser schema expressions copied from trycua/cua at 688c5fd62a9f13dddc309970c948df494e7de33f.
// Source: libs/cua-driver/rust/crates/cua-driver-core/src/browser/tools.rs (MIT).
use serde_json::{Value, json};
fn schema_delivery_mode() -> Value {
    json!({
        "type": "string",
        "enum": ["background", "foreground"],
        "default": "background",
        "description": "background (default) refuses trusted input where it would activate the browser window, including Linux Chromium and macOS Chromium configurations whose trusted route cannot stay in the background. foreground accepts that activation, for a browser whose window nobody else is using (for example inside a sandbox)."
    })
}

fn schema_target_id() -> Value {
    json!({
        "type": "string",
        "description": "Opaque browser target id minted by get_browser_state \
            (session-scoped; never a CDP id)."
    })
}

fn schema_tab_id() -> Value {
    json!({
        "type": "string",
        "description": "Opaque tab id from get_browser_state (session-scoped)."
    })
}

fn schema_ref() -> Value {
    json!({
        "type": "string",
        "description": "Page element ref in the p<snapshot>:<index> namespace from \
            get_browser_state. Refs are invalidated by navigation and by newer \
            snapshots of the same tab."
    })
}

fn schema_session() -> Value {
    json!({"type":"string", "description":"For multi-call work, prefer a short public session label and repeat it on every call that\naccepts it. Omit it to use the authenticated transport's implicit lifecycle session. Browser targets, tabs, and refs belong to the resolved lifecycle session."})
}
pub(super) fn schemas() -> Vec<(&'static str, Value)> {
    vec![
        (
            "get_browser_state",
            json!({
                "type": "object",
                "properties": {
                    "pid": { "type": "integer", "description": "Native browser process id (bind mode)." },
                    "window_id": { "type": "integer", "description": "Native window id owned by pid (bind mode)." },
                    "target_id": schema_target_id(),
                    "tab_id": schema_tab_id(),
                    "session": schema_session(),
                    "snapshot_format": {
                        "type": "string",
                        "enum": ["dom_refs_v1", "semantic_v2"],
                        "description": "Versioned snapshot contract. dom_refs_v1 remains the compatibility default."
                    },
                    "scope_ref": {
                        "type": "string",
                        "description": "Current semantic/content ref whose subtree should be observed."
                    },
                    "query": {
                        "type": "string",
                        "description": "Read-only semantic match over role, accessible name, and visible text."
                    },
                    "continuation": {
                        "type": "string",
                        "description": "Opaque continuation minted by an earlier semantic_v2 response."
                    },
                    "include_screenshot": {
                        "type": "boolean",
                        "default": false,
                        "description": "Capture the exact tab viewport as PNG through CDP without selecting the tab or foregrounding its native window. The request refuses if capture cannot be completed."
                    },
                },
                "additionalProperties": true
            }),
        ),
        (
            "browser_prepare",
            json!({
                "type": "object",
                "properties": {
                    "pid": { "type": "integer", "description": "Browser process id to prepare. Required except for a driver-owned isolated_new/isolated_named launch with allow_launch=true." },
                    "window_id": { "type": "integer", "description": "Exact native window approval anchor; required for strategy.kind=existing_profile." },
                    "allow_launch": {
                        "type": "boolean",
                        "description": "Allow a separate driver-owned isolated Chromium process to be launched (default false)."
                    },
                    "profile": {
                        "type": "object",
                        "description": "Driver-owned isolated Chromium profile to launch with allow_launch=true. mode=isolated_new creates a fresh throwaway profile; mode=isolated_named reuses the named driver-owned profile. Never an existing user profile.",
                        "properties": {
                            "mode": { "type": "string", "enum": ["isolated_new", "isolated_named"] },
                            "name": { "type": "string", "description": "Required only for isolated_named; 1-64 path-safe ASCII characters." }
                        },
                        "required": ["mode"],
                        "additionalProperties": false
                    },
                    "strategy": {
                        "type": "object",
                        "description": "Attach to an already-running browser instead of launching one. Pass an object, {\"kind\":\"existing_profile\"}, not a string. kind=existing_profile attaches to the user's running profile at pid/window_id under the runtime's profile authorization.",
                        "properties": {
                            "kind": { "type": "string", "enum": ["existing_profile"] }
                        },
                        "required": ["kind"],
                        "additionalProperties": false
                    },
                    "delivery_mode": {
                        "type": "string",
                        "enum": ["background", "foreground"],
                        "default": "background",
                        "description": "Existing-profile attachment only. background (default) attaches in the background when the browser window is on another Space than the user's, and otherwise refuses with background_unavailable, before any visible effect, where attaching would bring the browser in front of the user's app. foreground accepts that, for a screen nobody else is using or when the user agreed; the app that was in front is brought back afterwards."
                    },
                    "session": schema_session(),
                },
                // Keep the top-level input schema a plain object. Bedrock rejects
                // anyOf/oneOf/allOf at this level, so the conditional pid/profile
                // contract is described above and enforced by invoke instead.
                "required": [],
                "additionalProperties": true
            }),
        ),
        (
            "browser_navigate",
            json!({
                "type": "object",
                "properties": {
                    "target_id": schema_target_id(),
                    "tab_id": schema_tab_id(),
                    "url": { "type": "string", "description": "Destination URL (http:, https:, or about:)." },
                    "session": schema_session(),
                },
                "required": ["target_id", "tab_id", "url"],
                "additionalProperties": true
            }),
        ),
        (
            "browser_click",
            json!({
                "type": "object",
                "properties": {
                    "target_id": schema_target_id(),
                    "tab_id": schema_tab_id(),
                    "session": schema_session(),
                    "ref": schema_ref(),
                    "x": { "type": "number", "description": "Viewport x (CSS px) — alternative to ref." },
                    "y": { "type": "number", "description": "Viewport y (CSS px) — alternative to ref." },
                    "delivery_mode": schema_delivery_mode(),
                    "input_route": {
                        "type": "string",
                        "enum": ["trusted", "dom_event"],
                        "description": "\"trusted\" (default): Input.dispatchMouseEvent. \
                            It refuses rather than foregrounding a standalone browser. \
                            \"dom_event\": synthetic full-background DOM click, only \
                            when explicitly requested. Dispatch does not prove the \
                            control activated; refresh page state and verify the \
                            expected postcondition."
                    },
                },
                "required": ["target_id", "tab_id"],
                "additionalProperties": true
            }),
        ),
        (
            "browser_type",
            json!({
                "type": "object",
                "properties": {
                    "target_id": schema_target_id(),
                    "tab_id": schema_tab_id(),
                    "session": schema_session(),
                    "text": { "type": "string", "description": "Text to type." },
                    "ref": schema_ref(),
                    "mode": {
                        "type": "string",
                        "enum": ["insert_text", "keystrokes"],
                        "description": "insert_text (default): bulk Input.insertText. \
                            keystrokes: per-character Input.dispatchKeyEvent."
                    },
                    "replace": {
                        "type": "boolean",
                        "description": "false (default): insert at the caret, appending \
                            to whatever the field already holds. true: select the \
                            element's whole content first so the text replaces it — \
                            with an empty text this clears the field. Replacement goes \
                            through the selection, so beforeinput/input still fire and \
                            framework state stays consistent."
                    },
                },
                "required": ["target_id", "tab_id", "ref", "text"],
                "additionalProperties": true
            }),
        ),
        (
            "browser_select",
            json!({
                "type": "object",
                "properties": {
                    "target_id": schema_target_id(),
                    "tab_id": schema_tab_id(),
                    "session": schema_session(),
                    "ref": schema_ref(),
                    "label": {
                        "type": "string",
                        "description": "Exact visible label of the option to select."
                    },
                    "value": {
                        "type": "string",
                        "description": "Exact value attribute of the option to select."
                    },
                },
                "required": ["target_id", "tab_id", "ref"],
                "additionalProperties": false
            }),
        ),
        (
            "browser_dialog",
            json!({
                "type": "object",
                "properties": {
                    "target_id": schema_target_id(),
                    "tab_id": schema_tab_id(),
                    "session": schema_session(),
                    "action": { "type": "string", "enum": ["inspect", "accept", "dismiss"], "description": "inspect returns the current dialog and its dialog_id; accept or dismiss resolves that exact dialog." },
                    "dialog_id": { "type": "string", "description": "Opaque current dialog generation returned by action=inspect." },
                    "prompt_text": { "type": "string", "description": "Sensitive response text, valid only when accepting a prompt dialog." },
                    "delivery_mode": {
                        "type": "string",
                        "enum": ["background", "foreground"],
                        "default": "background",
                        "description": "Requested foreground posture for accept/dismiss. Linux Chromium requires foreground; inspect is read-only."
                    }
                },
                "required": ["target_id", "tab_id", "action"],
                "additionalProperties": true
            }),
        ),
        (
            "browser_set_input_files",
            json!({
                "type": "object",
                "properties": {
                    "target_id": schema_target_id(),
                    "tab_id": schema_tab_id(),
                    "session": schema_session(),
                    "ref": schema_ref(),
                    "files": {
                        "type": "array", "minItems": 1, "maxItems": 32,
                        "description": "Absolute paths of 1 to 32 local regular files to assign to the input.",
                        "items": { "type": "string", "description": "Absolute path to one local regular file." }
                    }
                },
                "required": ["target_id", "tab_id", "ref", "files"],
                "additionalProperties": true
            }),
        ),
    ]
}
