use super::*;
use crate::types::ChatMessage;
#[test]
fn function_parameters_use_json_schema_surface_without_dropping_constraints() {
    let schema = serde_json::json!({"type":"object","properties":{"nested":{"type":"object","properties":{"count":{"type":"integer","minimum":1}},"additionalProperties":false}},"additionalProperties":false});
    let mut request = ChatRequest {
        model: "gemini-2.5-flash".into(),
        messages: vec![ChatMessage::user("use tool")],
        temperature: None,
        max_tokens: Some(128),
        tools: None,
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: None,
        compiled_prompt: None,
    };
    request.tools = Some(vec![crate::ToolDefinition {
        name: "nested".into(),
        description: "nested fixture".into(),
        parameters: schema.clone(),
    }]);
    let wire =
        serde_json::to_value(GeminiProvider::build_request_result(&request).unwrap()).unwrap();
    let declaration = &wire["tools"][0]["functionDeclarations"][0];
    assert_eq!(declaration["parametersJsonSchema"], schema);
    assert!(declaration.get("parameters").is_none());
    request.tools.as_mut().unwrap()[0].parameters["properties"]["nested"]["oneOf"] =
        serde_json::json!([{ "type":"string"},{"type":"number"}]);
    assert!(GeminiProvider::build_request_result(&request).is_err());
}

#[test]
fn typed_error_content_is_preserved_in_function_response_after_restore() {
    let call = crate::ProviderToolCall {
        id: "call".into(),
        name: "read".into(),
        arguments: "{}".into(),
    };
    let mut result = ChatMessage::tool_result(&call.id, &call.name, "");
    result.tool_outcome = Some(crate::ToolResultOutcome::new(
        crate::ToolResultStatus::RecoverableError,
    ));
    let cold: ChatMessage = serde_json::from_value(serde_json::to_value(result).unwrap()).unwrap();
    let request = ChatRequest {
        model: "gemini-2.5-flash".into(),
        messages: vec![
            ChatMessage::user("read"),
            ChatMessage::assistant_tool_calls(None::<String>, vec![call]),
            cold,
        ],
        temperature: None,
        max_tokens: Some(128),
        tools: None,
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: None,
        compiled_prompt: None,
    };
    let wire =
        serde_json::to_value(GeminiProvider::build_request_result(&request).unwrap()).unwrap();
    assert_eq!(
        wire["contents"][2]["parts"][0]["functionResponse"]["response"]["content"],
        "Tool outcome: recoverable_error.\n"
    );
    assert!(
        wire["contents"][2]["parts"][0]["functionResponse"]
            .get("is_error")
            .is_none()
    );
}
