use super::*;

#[derive(Clone)]
pub(super) struct MemoryHookTurnState {
    pub(super) context: MemoryTurnContext,
}

#[derive(Default)]
pub(super) struct MemoryHookTurnStateStore {
    states: Mutex<BTreeMap<String, MemoryHookTurnState>>,
}

impl MemoryHookTurnStateStore {
    pub(super) fn set_turn_context(&self, context: MemoryTurnContext) {
        if let Ok(mut states) = self.states.lock() {
            states.insert(
                memory_hook_state_key(
                    context.workspace_id.as_str(),
                    context.thread_id.as_str(),
                    context.turn_id.as_str(),
                ),
                MemoryHookTurnState { context },
            );
        }
    }

    pub(super) fn state(&self, request: &HookHandlerRequest) -> Option<MemoryHookTurnState> {
        let workspace_id = request.context.workspace_id.as_ref()?.as_str();
        let thread_id = request.context.thread_id.as_ref()?.as_str();
        let turn_id = request.context.turn_id.as_ref()?.as_str();
        self.states.lock().ok().and_then(|states| {
            states
                .get(&memory_hook_state_key(workspace_id, thread_id, turn_id))
                .cloned()
        })
    }
}

pub(super) fn memory_hook_state_key(workspace_id: &str, thread_id: &str, turn_id: &str) -> String {
    format!("{workspace_id}\n{thread_id}\n{turn_id}")
}
