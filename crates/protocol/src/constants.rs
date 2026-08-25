pub mod methods {
    pub const AUTH_ME: &str = "auth/me";
    pub const AUTHORIZATION_CAPABILITIES: &str = "authorization/capabilities";
    pub const AUTH_PROFILE_UPDATE: &str = "auth/profile/update";
    pub const AUTH_SESSION_LIST: &str = "auth/session/list";
    pub const AUTH_SESSION_REVOKE: &str = "auth/session/revoke";
    pub const AUTH_LOGOUT: &str = "auth/logout";
    pub const AUTH_DEVICE_CREATE: &str = "auth/device/create";
    pub const AUTH_REFRESH: &str = "auth/refresh";
    pub const AUTH_DEVICE_ACTIVATE: &str = "auth/device/activate";
    pub const INVITE_CREATE: &str = "invite/create";
    pub const INVITE_LIST: &str = "invite/list";
    pub const INVITE_REVOKE: &str = "invite/revoke";
    pub const INVITE_PREVIEW: &str = "invite/preview";
    pub const INVITE_ACCEPT: &str = "invite/accept";
    pub const MEMBER_LIST: &str = "member/list";
    pub const MEMBER_SUSPEND: &str = "member/suspend";
    pub const MEMBER_RESTORE: &str = "member/restore";
    pub const MEMBER_REMOVE: &str = "member/remove";
    pub const MEMBER_DEVICE_CREATE: &str = "member/device/create";
    pub const WORKSPACE_MEMBER_LIST: &str = "workspace/member/list";
    pub const WORKSPACE_MEMBER_ADD: &str = "workspace/member/add";
    pub const WORKSPACE_MEMBER_REMOVE: &str = "workspace/member/remove";
    pub const WORKSPACE_LIST: &str = "workspace/list";
    pub const WORKSPACE_CREATE: &str = "workspace/create";
    pub const WORKSPACE_DEFAULT: &str = "workspace/default";
    pub const WORKSPACE_SELECT: &str = "workspace/select";
    pub const WORKSPACE_UPDATE: &str = "workspace/update";
    pub const THREAD_START: &str = "thread/start";
    pub const THREAD_TREE: &str = "thread/tree";
    pub const THREAD_UPDATE: &str = "thread/update";
    pub const THREAD_MOVE: &str = "thread/move";
    pub const THREAD_PARTICIPANTS_LIST: &str = "thread/participants/list";
    pub const THREAD_PARTICIPANTS_ADD: &str = "thread/participants/add";
    pub const THREAD_PARTICIPANTS_REMOVE: &str = "thread/participants/remove";
    pub const THREAD_FOLDER_CREATE: &str = "thread/folder/create";
    pub const THREAD_FOLDER_MOVE: &str = "thread/folder/move";
    pub const THREAD_FOLDER_DELETE: &str = "thread/folder/delete";
    pub const THREAD_AGENTS_DOC_GET: &str = "thread/agents_doc/get";
    pub const THREAD_AGENTS_DOC_SAVE: &str = "thread/agents_doc/save";
    pub const THREAD_AGENTS_DOC_ARCHIVE: &str = "thread/agents_doc/archive";
    pub const THREAD_AGENTS_DOC_RESOLVE_FOR_THREAD: &str = "thread/agents_doc/resolve_for_thread";
    pub const THREAD_GET: &str = "thread/get";
    pub const THREAD_TIMELINE_PAGE: &str = "thread/timeline/page";
    pub const THREAD_PATCH_STEPS_PAGE: &str = "thread/patch_steps/page";
    pub const THREAD_FILE_PATCH_HISTORY_PAGE: &str = "thread/file_patch_history/page";
    pub const THREAD_READ: &str = "thread/read";
    pub const THREAD_UNSUBSCRIBE: &str = "thread/unsubscribe";
    pub const TURN_START: &str = "turn/start";
    pub const TURN_MESSAGE_EDIT: &str = "turn/message/edit";
    pub const TURN_MESSAGE_DELETE: &str = "turn/message/delete";
    pub const TURN_MESSAGE_REVISIONS_PAGE: &str = "turn/message/revisions/page";
    pub const TURN_CANCEL: &str = "turn/cancel";
    pub const TURN_RESUME: &str = "turn/resume";
    pub const TURN_GET: &str = "turn/get";
    pub const TURN_ITEMS_PAGE: &str = "turn/items/page";
    pub const TURN_PATCH_STEPS_PAGE: &str = "turn/patch_steps/page";
    pub const TURN_PATCH_RECORD_GET: &str = "turn/patch_record/get";
    pub const TURN_PATCH_DIFF_GET: &str = "turn/patch_diff/get";
    pub const TURN_WORK_PAGE: &str = "turn/work/page";
    pub const TURN_WORK_ITEMS_GET: &str = "turn/work/items/get";
    pub const TURN_PERMISSION_REQUEST_RESPOND: &str = "turn/permission/request/respond";
    pub const VOICE_STATUS: &str = "voice/status";
    pub const VOICE_SESSION_START: &str = "voice/session/start";
    pub const VOICE_SESSION_FINALIZE: &str = "voice/session/finalize";
    pub const VOICE_SESSION_CANCEL: &str = "voice/session/cancel";
    pub const PROVIDER_LIST: &str = "provider/list";
    pub const PROVIDER_MODELS_LIST: &str = "provider/models/list";
    pub const PROVIDER_EMBEDDING_MODELS_LIST: &str = "provider/embedding_models/list";
    pub const PROVIDER_TRANSCRIPTION_MODELS_LIST: &str = "provider/transcription_models/list";
    pub const PROVIDER_CONFIGURE: &str = "provider/configure";
    pub const PROVIDER_SET_API_KEY: &str = "provider/set_api_key";
    pub const PROVIDER_DELETE_API_KEY: &str = "provider/delete_api_key";
    pub const CLI_RUNTIME_LIST: &str = "cli_runtime/list";
    pub const CLI_RUNTIME_GET: &str = "cli_runtime/get";
    pub const CLI_RUNTIME_STATUS: &str = "cli_runtime/status";
    pub const CLI_RUNTIME_REFRESH: &str = "cli_runtime/refresh";
    pub const CLI_RUNTIME_LIST_MODELS: &str = "cli_runtime/list_models";
    pub const CLI_RUNTIME_THREAD_BINDING_GET: &str = "cli_runtime/thread_binding/get";
    pub const CLI_RUNTIME_THREAD_FORK: &str = "cli_runtime/thread/fork";
    pub const CLI_RUNTIME_THREAD_COMPACT: &str = "cli_runtime/thread/compact";
    pub const CLI_RUNTIME_TURN_STEER: &str = "cli_runtime/turn/steer";
    pub const CLI_RUNTIME_REVIEW_START: &str = "cli_runtime/review/start";
    pub const CLI_RUNTIME_LOGIN_START: &str = "cli_runtime/login/start";
    pub const CLI_RUNTIME_LOGIN_CANCEL: &str = "cli_runtime/login/cancel";
    pub const CLI_RUNTIME_PROXY_SET: &str = "cli_runtime/proxy/set";
    pub const CLI_RUNTIME_PROXY_DELETE: &str = "cli_runtime/proxy/delete";
    pub const CLI_RUNTIME_REQUEST_RESPOND: &str = "cli_runtime/request/respond";
    pub const SETTINGS_GET: &str = "settings/get";
    pub const SETTINGS_UPDATE: &str = "settings/update";
    pub const SKILLS_LIST: &str = "skills/list";
    pub const SKILLS_INSTALL: &str = "skills/install";
    pub const SKILLS_UPDATE: &str = "skills/update";
    pub const SKILLS_UNINSTALL: &str = "skills/uninstall";
    pub const SKILLS_PACK_INSTALL: &str = "skills/pack/install";
    pub const SKILLS_PACK_UPDATE: &str = "skills/pack/update";
    pub const SKILLS_PACK_UNINSTALL: &str = "skills/pack/uninstall";
    pub const SKILLS_HEALTH: &str = "skills/health";
    pub const SKILLS_UPLOAD_START: &str = "skills/upload/start";
    pub const SKILLS_UPLOAD_FINISH: &str = "skills/upload/finish";
    pub const SKILLS_UPLOAD_ABORT: &str = "skills/upload/abort";
    pub const SKILLS_POLICY_LIST: &str = "skills/policy/list";
    pub const SKILLS_POLICY_SET: &str = "skills/policy/set";
    pub const MCP_LIST: &str = "mcp/list";
    pub const MCP_INSTALL: &str = "mcp/install";
    pub const MCP_POLICY_SET: &str = "mcp/policy/set";
    pub const MCP_SERVER_RESTART: &str = "mcp/server/restart";
    pub const MCP_UNINSTALL: &str = "mcp/uninstall";
    pub const MCP_SERVER_DETAILS: &str = "mcp/server/details";
    pub const TASK_CREATE: &str = "task/create";
    pub const TASK_GET: &str = "task/get";
    pub const TASK_LIST: &str = "task/list";
    pub const TASK_TREE: &str = "task/tree";
    pub const TASK_EVENTS: &str = "task/events";
    pub const TASK_WAIT: &str = "task/wait";
    pub const TASK_ACCEPT: &str = "task/accept";
    pub const TASK_REVISE: &str = "task/revise";
    pub const TASK_CANCEL: &str = "task/cancel";
    pub const TASK_UPDATE: &str = "task/update";
    pub const TASK_RESCHEDULE: &str = "task/reschedule";
    pub const TASK_DETACH: &str = "task/detach";
    pub const TASK_PAUSE: &str = "task/pause";
    pub const TASK_RESUME: &str = "task/resume";
    pub const TASK_AGENDA: &str = "task/agenda";
    pub const TASK_DELIVERIES: &str = "task/deliveries";
    pub const TASK_USER_NOTIFICATION_LIST: &str = "task/user_notification/list";
    pub const TASK_USER_NOTIFICATION_ACKNOWLEDGE: &str = "task/user_notification/acknowledge";
    pub const AGENT_ROUTE_CREATE: &str = "agent/route/create";
    pub const AGENT_ROUTE_LIST: &str = "agent/route/list";
    pub const AGENT_ROUTE_REVOKE: &str = "agent/route/revoke";
    pub const MEMORY_SEARCH: &str = "memory/search";
    pub const MEMORY_GET: &str = "memory/get";
    pub const MEMORY_LIST: &str = "memory/list";
    pub const MEMORY_REMEMBER: &str = "memory/remember";
    pub const MEMORY_FORGET: &str = "memory/forget";
    pub const MEMORY_CANDIDATES_LIST: &str = "memory/candidates/list";
    pub const MEMORY_CANDIDATES_GET: &str = "memory/candidates/get";
    pub const MEMORY_CANDIDATES_DECIDE: &str = "memory/candidates/decide";
    pub const MEMORY_CANDIDATES_APPROVE: &str = "memory/candidates/approve";
    pub const MEMORY_CANDIDATES_REJECT: &str = "memory/candidates/reject";
    pub const MEMORY_CANDIDATES_EDIT_AND_APPROVE: &str = "memory/candidates/edit_and_approve";
    pub const MEMORY_CANDIDATES_MERGE: &str = "memory/candidates/merge";
    pub const MEMORY_CANDIDATES_SUPPRESS_SIMILAR: &str = "memory/candidates/suppress_similar";
    pub const ARTIFACT_CAPABILITIES: &str = "artifact/capabilities";
    pub const ARTIFACT_LIST: &str = "artifact/list";
    pub const ARTIFACT_LIST_FOR_THREAD: &str = "artifact/list/thread";
    pub const ARTIFACT_LIST_FOR_TURN: &str = "artifact/list/turn";
    pub const ARTIFACT_LIST_FOR_MESSAGE: &str = "artifact/list/message";
    pub const ARTIFACT_GET: &str = "artifact/get";
    pub const ARTIFACT_VIEW_GRANT_CREATE: &str = "artifact/view_grant/create";
    pub const ARTIFACT_DELETE: &str = "artifact/delete";
    pub const ARTIFACT_RESTORE: &str = "artifact/restore";
    pub const ARTIFACT_BIND: &str = "artifact/bind";
    pub const ARTIFACT_UPLOAD_START: &str = "artifact/upload/start";
    pub const ARTIFACT_UPLOAD_FINISH: &str = "artifact/upload/finish";
    pub const ARTIFACT_UPLOAD_ABORT: &str = "artifact/upload/abort";

    /// Authenticated JSON-RPC methods accepted by the normal Gateway
    /// transport. Restricted credential exchange methods are deliberately
    /// excluded and listed in `RESTRICTED_AUTH_METHODS`.
    pub const NORMAL_METHODS: &[&str] = &[
        AUTH_ME,
        AUTHORIZATION_CAPABILITIES,
        AUTH_PROFILE_UPDATE,
        AUTH_SESSION_LIST,
        AUTH_SESSION_REVOKE,
        AUTH_LOGOUT,
        AUTH_DEVICE_CREATE,
        INVITE_CREATE,
        INVITE_LIST,
        INVITE_REVOKE,
        MEMBER_LIST,
        MEMBER_SUSPEND,
        MEMBER_RESTORE,
        MEMBER_REMOVE,
        MEMBER_DEVICE_CREATE,
        WORKSPACE_MEMBER_LIST,
        WORKSPACE_MEMBER_ADD,
        WORKSPACE_MEMBER_REMOVE,
        WORKSPACE_LIST,
        WORKSPACE_CREATE,
        WORKSPACE_DEFAULT,
        WORKSPACE_SELECT,
        WORKSPACE_UPDATE,
        THREAD_START,
        THREAD_TREE,
        THREAD_UPDATE,
        THREAD_MOVE,
        THREAD_PARTICIPANTS_LIST,
        THREAD_PARTICIPANTS_ADD,
        THREAD_PARTICIPANTS_REMOVE,
        THREAD_FOLDER_CREATE,
        THREAD_FOLDER_MOVE,
        THREAD_FOLDER_DELETE,
        THREAD_AGENTS_DOC_GET,
        THREAD_AGENTS_DOC_SAVE,
        THREAD_AGENTS_DOC_ARCHIVE,
        THREAD_AGENTS_DOC_RESOLVE_FOR_THREAD,
        THREAD_GET,
        THREAD_TIMELINE_PAGE,
        THREAD_PATCH_STEPS_PAGE,
        THREAD_FILE_PATCH_HISTORY_PAGE,
        THREAD_READ,
        THREAD_UNSUBSCRIBE,
        TURN_START,
        TURN_MESSAGE_EDIT,
        TURN_MESSAGE_DELETE,
        TURN_MESSAGE_REVISIONS_PAGE,
        TURN_CANCEL,
        TURN_RESUME,
        TURN_GET,
        TURN_ITEMS_PAGE,
        TURN_PATCH_STEPS_PAGE,
        TURN_PATCH_RECORD_GET,
        TURN_PATCH_DIFF_GET,
        TURN_WORK_PAGE,
        TURN_WORK_ITEMS_GET,
        TURN_PERMISSION_REQUEST_RESPOND,
        VOICE_STATUS,
        VOICE_SESSION_START,
        VOICE_SESSION_FINALIZE,
        VOICE_SESSION_CANCEL,
        PROVIDER_LIST,
        PROVIDER_MODELS_LIST,
        PROVIDER_EMBEDDING_MODELS_LIST,
        PROVIDER_TRANSCRIPTION_MODELS_LIST,
        PROVIDER_CONFIGURE,
        PROVIDER_SET_API_KEY,
        PROVIDER_DELETE_API_KEY,
        CLI_RUNTIME_LIST,
        CLI_RUNTIME_GET,
        CLI_RUNTIME_STATUS,
        CLI_RUNTIME_REFRESH,
        CLI_RUNTIME_LIST_MODELS,
        CLI_RUNTIME_THREAD_BINDING_GET,
        CLI_RUNTIME_THREAD_FORK,
        CLI_RUNTIME_THREAD_COMPACT,
        CLI_RUNTIME_TURN_STEER,
        CLI_RUNTIME_REVIEW_START,
        CLI_RUNTIME_LOGIN_START,
        CLI_RUNTIME_LOGIN_CANCEL,
        CLI_RUNTIME_PROXY_SET,
        CLI_RUNTIME_PROXY_DELETE,
        CLI_RUNTIME_REQUEST_RESPOND,
        SETTINGS_GET,
        SETTINGS_UPDATE,
        SKILLS_LIST,
        SKILLS_INSTALL,
        SKILLS_UPDATE,
        SKILLS_UNINSTALL,
        SKILLS_PACK_INSTALL,
        SKILLS_PACK_UPDATE,
        SKILLS_PACK_UNINSTALL,
        SKILLS_HEALTH,
        SKILLS_UPLOAD_START,
        SKILLS_UPLOAD_FINISH,
        SKILLS_UPLOAD_ABORT,
        SKILLS_POLICY_LIST,
        SKILLS_POLICY_SET,
        MCP_LIST,
        MCP_INSTALL,
        MCP_POLICY_SET,
        MCP_SERVER_RESTART,
        MCP_UNINSTALL,
        MCP_SERVER_DETAILS,
        TASK_CREATE,
        TASK_GET,
        TASK_LIST,
        TASK_TREE,
        TASK_EVENTS,
        TASK_WAIT,
        TASK_ACCEPT,
        TASK_REVISE,
        TASK_CANCEL,
        TASK_RESCHEDULE,
        TASK_DETACH,
        TASK_PAUSE,
        TASK_RESUME,
        TASK_AGENDA,
        TASK_DELIVERIES,
        TASK_USER_NOTIFICATION_LIST,
        TASK_USER_NOTIFICATION_ACKNOWLEDGE,
        AGENT_ROUTE_CREATE,
        AGENT_ROUTE_LIST,
        AGENT_ROUTE_REVOKE,
        MEMORY_SEARCH,
        MEMORY_GET,
        MEMORY_LIST,
        MEMORY_REMEMBER,
        MEMORY_FORGET,
        MEMORY_CANDIDATES_LIST,
        MEMORY_CANDIDATES_GET,
        MEMORY_CANDIDATES_DECIDE,
        MEMORY_CANDIDATES_APPROVE,
        MEMORY_CANDIDATES_REJECT,
        MEMORY_CANDIDATES_EDIT_AND_APPROVE,
        MEMORY_CANDIDATES_MERGE,
        MEMORY_CANDIDATES_SUPPRESS_SIMILAR,
        ARTIFACT_CAPABILITIES,
        ARTIFACT_LIST,
        ARTIFACT_LIST_FOR_THREAD,
        ARTIFACT_LIST_FOR_TURN,
        ARTIFACT_LIST_FOR_MESSAGE,
        ARTIFACT_GET,
        ARTIFACT_VIEW_GRANT_CREATE,
        ARTIFACT_DELETE,
        ARTIFACT_RESTORE,
        ARTIFACT_BIND,
        ARTIFACT_UPLOAD_START,
        ARTIFACT_UPLOAD_FINISH,
        ARTIFACT_UPLOAD_ABORT,
    ];

    /// Methods accepted only by the pre-authenticated credential exchange
    /// transport. They must never be admitted through normal authorization.
    pub const RESTRICTED_AUTH_METHODS: &[&str] = &[
        AUTH_REFRESH,
        AUTH_DEVICE_ACTIVATE,
        INVITE_PREVIEW,
        INVITE_ACCEPT,
    ];
}

pub mod events {
    pub const ACCESS_CHANGED: &str = "access/changed";
    pub const AUTHORIZATION_PROJECTION_CHANGED: &str = "authorization/projection_changed";
    pub const INVITATION_CHANGED: &str = "invitation/changed";
    pub const MEMBER_CHANGED: &str = "member/changed";
    pub const WORKSPACE_MEMBERS_CHANGED: &str = "workspace/members_changed";
    pub const AUTH_SESSION_REVOKED: &str = "auth/session_revoked";
    pub const AUTH_ACCESS_EXPIRING: &str = "auth/access_expiring";
    pub const WORKSPACE_CHANGED: &str = "workspace/changed";
    pub const THREAD_STARTED: &str = "thread/started";
    pub const THREAD_UPDATED: &str = "thread/updated";
    pub const THREAD_PARTICIPANTS_CHANGED: &str = "thread/participants/changed";
    pub const THREAD_CLOSED: &str = "thread/closed";
    pub const THREAD_TREE_CHANGED: &str = "thread/tree/changed";
    pub const THREAD_AGENTS_DOC_CHANGED: &str = "thread/agents_doc/changed";
    pub const THREAD_TIMELINE_BLOCKS_CHANGED: &str = "thread/timeline/blocks/changed";
    pub const THREAD_READ_CURSOR_CHANGED: &str = "thread/read/changed";
    pub const TURN_STARTED: &str = "turn/started";
    pub const TURN_MESSAGE_EDITED: &str = "turn/message/edited";
    pub const TURN_MESSAGE_DELETED: &str = "turn/message/deleted";
    pub const TURN_COMPLETED: &str = "turn/completed";
    pub const TURN_FAILED: &str = "turn/failed";
    pub const TURN_BLOCKED: &str = "turn/blocked";
    pub const TURN_WORK_ITEMS_CHANGED: &str = "turn/work/items/changed";
    pub const TURN_WORK_STATE_CHANGED: &str = "turn/work/state/changed";
    pub const TURN_PERMISSION_REQUEST_OPENED: &str = "turn/permission/request/opened";
    pub const TURN_PERMISSION_REQUEST_RESOLVED: &str = "turn/permission/request/resolved";
    pub const TURN_PERMISSION_AUDIT: &str = "turn/permission/audit";
    pub const TURN_EXECUTION_WINDOW_STARTED: &str = "turn/execution_window/started";
    pub const TURN_EXECUTION_WINDOW_EXHAUSTED: &str = "turn/execution_window/exhausted";
    pub const TURN_EXECUTION_WINDOW_CHECKPOINTED: &str = "turn/execution_window/checkpointed";
    pub const TURN_EXECUTION_WINDOW_CONTINUED: &str = "turn/execution_window/continued";
    pub const TURN_EXECUTION_WINDOW_BLOCKED: &str = "turn/execution_window/blocked";
    pub const VOICE_CHUNK_ACK: &str = "voice/chunk/ack";
    pub const VOICE_SESSION_RESULT: &str = "voice/session/result";
    pub const ITEM_STARTED: &str = "item/started";
    pub const ITEM_AGENT_MESSAGE_DELTA: &str = "item/agent_message/delta";
    pub const ITEM_COMMAND_EXECUTION_OUTPUT_DELTA: &str = "item/command_execution/output_delta";
    pub const ITEM_FILE_CHANGE_OUTPUT_DELTA: &str = "item/file_change/output_delta";
    pub const ITEM_TOOL_PROGRESS: &str = "item/tool/progress";
    pub const ITEM_TIMEOUT_DETECTED: &str = "item/timeout_detected";
    pub const ITEM_RECOVERY_OPENED: &str = "item/recovery_opened";
    pub const ITEM_RECOVERY_ATTACHED: &str = "item/recovery_attached";
    pub const ITEM_RETRY_SCHEDULED: &str = "item/retry_scheduled";
    pub const ITEM_RETRY_ATTEMPT_STARTED: &str = "item/retry_attempt_started";
    pub const ITEM_RECOVERY_SUCCEEDED: &str = "item/recovery_succeeded";
    pub const ITEM_RECOVERY_EXHAUSTED: &str = "item/recovery_exhausted";
    pub const ITEM_TOOL_RETRY_SCHEDULED: &str = "item/tool/retry_scheduled";
    pub const ITEM_TOOL_RETRY_RESOLVED: &str = "item/tool/retry_resolved";
    pub const ITEM_TOOL_RETRY_EXHAUSTED: &str = "item/tool/retry_exhausted";
    pub const ITEM_COMPLETED: &str = "item/completed";
    pub const ITEM_UPDATED: &str = "item/updated";
    pub const TURN_TOOL_LOOP_BUDGET_EXCEEDED: &str = "turn/tool_loop/budget_exceeded";
    pub const CONTEXT_COMPRESSING: &str = "context/compressing";
    pub const CONTEXT_COMPRESSED: &str = "context/compressed";
    pub const SKILLS_CHANGED: &str = "skills/changed";
    pub const SKILLS_UPLOAD_CHUNK_ACK: &str = "skills/upload/chunk_ack";
    pub const MCP_CHANGED: &str = "mcp/changed";
    pub const MCP_SERVER_STATUS_CHANGED: &str = "mcp/server/status_changed";
    pub const MCP_SERVER_CATALOG_CHANGED: &str = "mcp/server/catalog_changed";
    pub const GATEWAY_REMOTE_ACCESS_STATUS_CHANGED: &str = "gateway/remote_access/status_changed";
    pub const GATEWAY_THREAD_EPISODIC_VECTOR_REFILL_STATUS_CHANGED: &str =
        "gateway/thread_episodic/vector_refill/status_changed";
    pub const GATEWAY_VOICE_INPUT_STATUS_CHANGED: &str = "gateway_voice_input_status_changed";
    pub const CLI_RUNTIME_STATUS_CHANGED: &str = "cli_runtime/status_changed";
    pub const CLI_RUNTIME_ACCOUNT_UPDATED: &str = "cli_runtime/account_updated";
    pub const CLI_RUNTIME_REQUEST_OPENED: &str = "cli_runtime/request_opened";
    pub const CLI_RUNTIME_REQUEST_RESOLVED: &str = "cli_runtime/request_resolved";
    pub const CLI_RUNTIME_APPS_CHANGED: &str = "cli_runtime/apps_changed";
    pub const TASK_CREATED: &str = "task/created";
    pub const TASK_SCHEDULED: &str = "task/scheduled";
    pub const TASK_QUEUED: &str = "task/queued";
    pub const TASK_RUN_CREATED: &str = "task/run/created";
    pub const TASK_RUN_STARTED: &str = "task/run/started";
    pub const TASK_PROGRESS: &str = "task/progress";
    pub const TASK_RUN_COMPLETED: &str = "task/run/completed";
    pub const TASK_RUN_FAILED: &str = "task/run/failed";
    pub const TASK_RUN_BLOCKED: &str = "task/run/blocked";
    pub const TASK_RUN_RETRY_SCHEDULED: &str = "task/run/retry_scheduled";
    pub const TASK_RUN_RETRY_EXHAUSTED: &str = "task/run/retry_exhausted";
    pub const TASK_RUN_CANCELLED: &str = "task/run/cancelled";
    pub const TASK_COMPLETED: &str = "task/completed";
    pub const TASK_FAILED: &str = "task/failed";
    pub const TASK_BLOCKED: &str = "task/blocked";
    pub const TASK_CANCELLED: &str = "task/cancelled";
    pub const TASK_DETACHED: &str = "task/detached";
    pub const TASK_UPDATED: &str = "task/updated";
    pub const TASK_RESCHEDULED: &str = "task/rescheduled";
    pub const TASK_TREE_CHANGED: &str = "task/tree/changed";
    pub const TASK_RECOVERED: &str = "task/recovered";
    pub const TASK_PAUSED: &str = "task/paused";
    pub const TASK_RESUMED: &str = "task/resumed";
    pub const TASK_RUN_THREAD_BINDING_CREATED: &str = "task/run/thread_binding/created";
    pub const TASK_RUN_TURN_STARTED: &str = "task/run/turn/started";
    pub const TASK_RUN_TURN_COMPLETED: &str = "task/run/turn/completed";
    pub const TASK_RUN_TURN_FAILED: &str = "task/run/turn/failed";
    pub const TASK_RUN_TURN_BLOCKED: &str = "task/run/turn/blocked";
    pub const TASK_RESULT_CANDIDATE_CREATED: &str = "task/result_candidate/created";
    pub const TASK_RESULT_REVIEW_EVENT_RECORDED: &str = "task/result_review_event/recorded";
    pub const TASK_RESULT_CANDIDATE_ACCEPTED: &str = "task/result_candidate/accepted";
    pub const TASK_RESULT_CANDIDATE_REJECTED: &str = "task/result_candidate/rejected";
    pub const TASK_RESULT_CANDIDATE_CANCELLED: &str = "task/result_candidate/cancelled";
    pub const TASK_REVISION_REQUESTED: &str = "task/revision/requested";
    pub const TASK_RUN_ENTERED_REVIEW: &str = "task/run/entered_review";
    pub const TASK_DELIVERY_QUEUED: &str = "task/delivery/queued";
    pub const TASK_DELIVERY_STARTED: &str = "task/delivery/started";
    pub const TASK_DELIVERY_DELIVERED: &str = "task/delivery/delivered";
    pub const TASK_DELIVERY_FAILED: &str = "task/delivery/failed";
    pub const TASK_DELIVERY_CANCELLED: &str = "task/delivery/cancelled";
    pub const TASK_USER_NOTIFICATION_DELIVERED: &str = "task/user_notification/delivered";
    pub const TASK_WRITE_LOCK_ACQUIRED: &str = "task/write_lock/acquired";
    pub const TASK_WRITE_LOCK_EXTENDED: &str = "task/write_lock/extended";
    pub const TASK_WRITE_LOCK_RELEASED: &str = "task/write_lock/released";
    pub const TASK_WRITE_LOCK_BLOCKED: &str = "task/write_lock/blocked";
    pub const TASK_WRITE_LOCK_EXPIRED: &str = "task/write_lock/expired";
    pub const MEMORY_CHANGED: &str = "memory/changed";
    pub const MEMORY_CANDIDATE_CREATED: &str = "memory/candidate_created";
    pub const MEMORY_FORGOTTEN: &str = "memory/forgotten";
    pub const ARTIFACT_CREATED: &str = "artifact/created";
    pub const ARTIFACT_UPDATED: &str = "artifact/updated";
    pub const ARTIFACT_DELETED: &str = "artifact/deleted";
    pub const THREAD_ARTIFACTS_CHANGED: &str = "thread/artifacts/changed";
    pub const ARTIFACT_PROJECTION_UPDATED: &str = "artifact/projection/updated";
    pub const ARTIFACT_UPLOAD_CHUNK_ACK: &str = "artifact/upload/chunk_ack";
    pub const ARTIFACT_UPLOAD_PROGRESS: &str = "artifact/upload/progress";
}

#[cfg(test)]
mod tests {
    use super::{events, methods};

    #[test]
    fn epic5_normal_and_restricted_method_sets_are_exact_and_disjoint() {
        let expected_normal = [
            "invite/create",
            "invite/list",
            "invite/revoke",
            "member/list",
            "member/suspend",
            "member/restore",
            "member/remove",
            "member/device/create",
            "workspace/member/list",
            "workspace/member/add",
            "workspace/member/remove",
        ];
        let expected_restricted = ["invite/preview", "invite/accept"];

        for method in expected_normal {
            assert!(
                methods::NORMAL_METHODS.contains(&method),
                "missing {method}"
            );
            assert!(!methods::RESTRICTED_AUTH_METHODS.contains(&method));
        }
        for method in expected_restricted {
            assert!(
                methods::RESTRICTED_AUTH_METHODS.contains(&method),
                "missing {method}"
            );
            assert!(!methods::NORMAL_METHODS.contains(&method));
        }
        let normal = methods::NORMAL_METHODS
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let restricted = methods::RESTRICTED_AUTH_METHODS
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(normal.len(), methods::NORMAL_METHODS.len());
        assert_eq!(restricted.len(), methods::RESTRICTED_AUTH_METHODS.len());
        assert!(normal.is_disjoint(&restricted));
    }

    #[test]
    fn epic5_event_set_is_exact() {
        assert_eq!(events::INVITATION_CHANGED, "invitation/changed");
        assert_eq!(events::MEMBER_CHANGED, "member/changed");
        assert_eq!(
            events::WORKSPACE_MEMBERS_CHANGED,
            "workspace/members_changed"
        );
    }
}
