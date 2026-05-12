use super::*;

pub(super) fn turn_pre_policy_input(
    request: &HookHandlerRequest,
) -> HookResult<&TurnPrePolicyHookInput> {
    match &request.input.payload {
        HookInputPayload::TurnPrePolicy(input) => Ok(input),
        _ => Err(memory_hook_error(
            "memory.invalid_input",
            "memory policy hook expected turn pre-policy input",
        )),
    }
}

pub(super) fn turn_pre_prompt_context_input(
    request: &HookHandlerRequest,
) -> HookResult<&TurnPrePromptContextHookInput> {
    match &request.input.payload {
        HookInputPayload::TurnPrePromptContext(input) => Ok(input),
        _ => Err(memory_hook_error(
            "memory.invalid_input",
            "memory deterministic recall hook expected turn pre-prompt-context input",
        )),
    }
}

pub(super) fn turn_pre_tool_materialization_allows_tools(request: &HookHandlerRequest) -> bool {
    match &request.input.payload {
        HookInputPayload::TurnPreToolMaterialization(input) => input.provider_tool_calling,
        _ => false,
    }
}

pub(super) fn turn_pre_prompt_compile_input(
    request: &HookHandlerRequest,
) -> HookResult<&TurnPrePromptCompileHookInput> {
    match &request.input.payload {
        HookInputPayload::TurnPrePromptCompile(input) => Ok(input),
        _ => Err(memory_hook_error(
            "memory.invalid_input",
            "memory prompt contract hook expected turn pre-prompt-compile input",
        )),
    }
}

pub(super) fn turn_post_turn_input(
    request: &HookHandlerRequest,
) -> HookResult<&TurnPostTurnHookInput> {
    match &request.input.payload {
        HookInputPayload::TurnPostTurn(input) => Ok(input),
        _ => Err(memory_hook_error(
            "memory.invalid_input",
            "memory post-turn extractor hook expected turn post-turn input",
        )),
    }
}

pub(super) fn required_context_id<'a>(
    value: Option<&'a str>,
    field: &'static str,
) -> HookResult<&'a str> {
    value.ok_or_else(|| {
        memory_hook_error(
            "memory.missing_context",
            format!("memory hook request missing {field}"),
        )
    })
}
