use futures_util::StreamExt;
use pioneer_provider::providers::EchoProvider;
use pioneer_provider::{
    AttachmentDataSource, ChatMessage, ChatRequest, MessageAttachment, MessageContentPart,
    ProviderRegistry,
};
use std::sync::Arc;

fn request() -> ChatRequest {
    ChatRequest {
        model: "text-only-fixture".into(),
        messages: vec![ChatMessage::user("hello")],
        temperature: None,
        max_tokens: Some(128),
        tools: None,
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: None,
        compiled_prompt: None,
    }
}

#[tokio::test]
async fn text_only_dependency_adapter_needs_no_media_contract_in_either_route() {
    // pioneer-provider is a normal dependency here, so its cfg(test) unknown
    // adapter allowance cannot hide a production text-only admission failure.
    let registry = ProviderRegistry::with_provider("echo", Arc::new(EchoProvider::new()));
    let provider = registry
        .get_or_create_for_workspace("fixture", "echo")
        .unwrap();
    for stream in [false, true] {
        let budget = provider.prepare_input_budget(request()).await.unwrap();
        assert!(budget.media.is_empty());
        if stream {
            let mut response = provider.stream_chat(budget.request).await.unwrap();
            let mut text = String::new();
            while let Some(chunk) = response.next().await {
                text.push_str(&chunk.unwrap().delta);
            }
            assert_eq!(text, "hello");
        } else {
            assert_eq!(provider.chat(budget.request).await.unwrap().text, "hello");
        }
    }
}

#[tokio::test]
async fn text_only_dependency_adapter_still_rejects_binary_input_before_generation() {
    use base64::Engine;
    let registry = ProviderRegistry::with_provider("echo", Arc::new(EchoProvider::new()));
    let provider = registry
        .get_or_create_for_workspace("fixture", "echo")
        .unwrap();
    let mut input = request();
    input.messages[0]
        .content_parts
        .push(MessageContentPart::image(MessageAttachment {
            mime_type: "image/png".into(),
            name: Some("pixel.png".into()),
            size_bytes: None,
            sha256: None,
            artifact: None,
            source: AttachmentDataSource::Bytes {
                base64_data: base64::engine::general_purpose::STANDARD
                    .encode(crate::media_test_fixtures::image(image::ImageFormat::Png)),
            },
        }));
    assert!(provider.prepare_input_budget(input.clone()).await.is_err());
    assert!(provider.chat(input.clone()).await.is_err());
    assert!(provider.stream_chat(input).await.is_err());
}
