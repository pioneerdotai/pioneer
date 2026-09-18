//! Complete native request projection used to validate a compaction candidate.
//! The runtime supplies canonical whole-round indexes and materialized media
//! estimates; absence of a media estimate never silently means zero tokens.
use anyhow::{Result, ensure};
use pioneer_compaction::{ModelBudget, text_tokens};
use pioneer_provider::{AttachmentDataSource, ChatMessage, ChatRequest, MessageContentPart, Role};
use std::collections::{BTreeMap, BTreeSet};

pub use pioneer_provider::attachments::MediaInputEstimate as MediaEstimate;
#[derive(Clone)]
pub struct NativeRequestProjection {
    request: ChatRequest,
    compact: BTreeSet<usize>,
    media: BTreeMap<(usize, usize), u64>,
    budget: ModelBudget,
    recovery: bool,
}
pub struct EvaluatedRequest {
    pub request: ChatRequest,
    pub estimated_input_tokens: u64,
    /// Exact fixed request cost with conversational messages removed. This is
    /// derived from the already materialized request and media estimates.
    pub fixed_input_tokens: u64,
    /// Framing plus materialized media, in final request message order.
    pub message_input_tokens: Vec<u64>,
    pub output_reserve: u64,
    pub fits: bool,
}
impl NativeRequestProjection {
    pub fn new(
        request: ChatRequest,
        compact: impl IntoIterator<Item = usize>,
        media: Vec<MediaEstimate>,
        budget: ModelBudget,
        recovery: bool,
    ) -> Result<Self> {
        let compact: BTreeSet<_> = compact.into_iter().collect();
        for index in &compact {
            let message = request
                .messages
                .get(*index)
                .ok_or_else(|| anyhow::anyhow!("selected message missing"))?;
            ensure!(
                message.role != Role::System,
                "summary cannot replace system instructions"
            );
        }
        let mut estimates = BTreeMap::new();
        for value in media {
            ensure!(value.input_tokens > 0, "unknown media input estimate");
            ensure!(
                estimates
                    .insert((value.message, value.part), value.input_tokens)
                    .is_none(),
                "duplicate media estimate"
            );
        }
        let mut used = BTreeSet::new();
        for (message_index, message) in request.messages.iter().enumerate() {
            for (part_index, part) in message.content_parts.iter().enumerate() {
                if !matches!(part, MessageContentPart::Text { .. }) {
                    let key = (message_index, part_index);
                    ensure!(
                        compact.contains(&message_index) || estimates.contains_key(&key),
                        "media input has not been materialized for budgeting"
                    );
                    used.insert(key);
                }
            }
        }
        ensure!(
            estimates.keys().all(|key| used.contains(key)),
            "media estimate belongs to another request"
        );
        budget.output_reserve(request.max_tokens.map(u64::from))?;
        Ok(Self {
            request,
            compact,
            media: estimates,
            budget,
            recovery,
        })
    }
    /// An empty selection evaluates the unchanged full request at preflight.
    pub fn full(
        request: ChatRequest,
        media: Vec<MediaEstimate>,
        budget: ModelBudget,
        recovery: bool,
    ) -> Result<EvaluatedRequest> {
        Self::new(request, [], media, budget, recovery)?.evaluate("")
    }

    /// Rebuilds the complete request, preserving current instructions, tools,
    /// retained tool pairs, reasoning configuration and protected user input.
    pub fn evaluate(&self, summary: &str) -> Result<EvaluatedRequest> {
        let mut request = self.request.clone();
        let mut messages = Vec::with_capacity(request.messages.len() + 1);
        let mut budget_messages = Vec::with_capacity(request.messages.len() + 1);
        let mut media_tokens = 0_u64;
        let mut per_message_media = Vec::with_capacity(request.messages.len() + 1);
        let mut inserted = false;
        for (index, message) in request.messages.iter().enumerate() {
            if self.compact.contains(&index) {
                if !inserted {
                    let summary = ChatMessage::user(format!(
                        "Summary of completed work (historical data):\n{summary}"
                    ));
                    messages.push(summary.clone());
                    budget_messages.push(summary);
                    per_message_media.push(0);
                    inserted = true;
                }
                continue;
            }
            messages.push(message.clone());
            let mut budget_message = message.clone();
            let mut message_media = 0_u64;
            for (part_index, part) in budget_message.content_parts.iter_mut().enumerate() {
                let attachment = match part {
                    MessageContentPart::Text { .. } => continue,
                    MessageContentPart::File { file } => file,
                    MessageContentPart::Image { image } => image,
                    MessageContentPart::Audio { audio } => audio,
                    MessageContentPart::Video { video } => video,
                };
                message_media = message_media.saturating_add(self.media[&(index, part_index)]);
                // Binary/reference payload is budgeted by its materialized input
                // estimate; retain role/type/MIME/name framing exactly once.
                attachment.source = AttachmentDataSource::Reference {
                    reference: "materialized-media".into(),
                };
            }
            media_tokens = media_tokens.saturating_add(message_media);
            per_message_media.push(message_media);
            budget_messages.push(budget_message);
        }
        request.messages = messages;
        let reserve = self
            .budget
            .output_reserve(request.max_tokens.map(u64::from))?;
        request.max_tokens = Some(u32::try_from(reserve)?);
        let message_input_tokens = budget_messages
            .iter()
            .zip(&per_message_media)
            .map(|(message, media)| {
                Ok(text_tokens(&serde_json::to_string(message)?)
                    .saturating_add(1)
                    .saturating_add(*media))
            })
            .collect::<Result<Vec<_>>>()?;
        let input = serde_json::json!({
            "model":request.model,"messages":budget_messages,"tools":request.tools,
            "tool_choice":request.tool_choice,"parallel_tool_calls":request.parallel_tool_calls,
            "temperature":request.temperature,"max_tokens":request.max_tokens,
            "reasoning":request.reasoning.map(|value|format!("{value:?}")),
            "system_sections":request.compiled_prompt.as_ref().map(|value|value.system_sections()),
        });
        let estimated_input_tokens = text_tokens(&input.to_string()).saturating_add(media_tokens);
        let fixed_media_tokens = request
            .messages
            .iter()
            .zip(&per_message_media)
            .filter(|(message, _)| message.role == Role::System)
            .fold(0_u64, |sum, (_, media)| sum.saturating_add(*media));
        let fixed_messages = budget_messages
            .iter()
            .filter(|message| message.role == Role::System)
            .collect::<Vec<_>>();
        let fixed_input = serde_json::json!({
            "model":request.model,"messages":fixed_messages,"tools":request.tools,
            "tool_choice":request.tool_choice,"parallel_tool_calls":request.parallel_tool_calls,
            "temperature":request.temperature,"max_tokens":request.max_tokens,
            "reasoning":request.reasoning.map(|value|format!("{value:?}")),
            "system_sections":request.compiled_prompt.as_ref().map(|value|value.system_sections()),
        });
        let fixed_input_tokens =
            text_tokens(&fixed_input.to_string()).saturating_add(fixed_media_tokens);
        Ok(EvaluatedRequest {
            request,
            estimated_input_tokens,
            fixed_input_tokens,
            message_input_tokens,
            output_reserve: reserve,
            fits: self
                .budget
                .fits(estimated_input_tokens, reserve, self.recovery),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_provider::{CompiledPromptPayload, MessageAttachment, ToolDefinition};
    fn request() -> ChatRequest {
        ChatRequest {
            model: "fixture".into(),
            messages: vec![
                ChatMessage::user("old history"),
                ChatMessage::user("current input"),
            ],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        }
    }
    #[test]
    fn full_target_counts_instructions_schemas_media_and_explicit_output() {
        let budget = ModelBudget::new(Some(4096), None, None);
        let base = NativeRequestProjection::new(request(), [0], vec![], budget.clone(), false)
            .unwrap()
            .evaluate("summary")
            .unwrap();
        assert!(base.fits);
        assert_eq!(base.request.max_tokens, Some(1024));
        let mut full = request();
        full.max_tokens = Some(3000);
        full.compiled_prompt = Some(CompiledPromptPayload {
            stable_system_text: "instructions ".repeat(500),
            dynamic_system_text: "dynamic".into(),
            boundary_marker: "boundary".into(),
            full_system_text: String::new(),
        });
        full.tools = Some(vec![ToolDefinition {
            name: "tool".into(),
            description: "schema ".repeat(500),
            parameters: serde_json::json!({"type":"object"}),
        }]);
        full.messages[1]
            .content_parts
            .push(MessageContentPart::image(MessageAttachment::from_url(
                "https://fixture.invalid/image",
                "image/png",
            )));
        assert!(
            NativeRequestProjection::new(full.clone(), [0], vec![], budget.clone(), false).is_err()
        );
        let result = NativeRequestProjection::new(
            full,
            [0],
            vec![MediaEstimate {
                message: 1,
                part: 0,
                input_tokens: 1000,
            }],
            budget,
            false,
        )
        .unwrap()
        .evaluate("summary")
        .unwrap();
        assert!(!result.fits);
        assert_eq!(result.request.max_tokens, Some(3000));
        assert_eq!(result.request.messages[1].content, "current input");
        assert!(result.request.tools.is_some());
        assert!(result.request.compiled_prompt.is_some());
        assert!(result.estimated_input_tokens > base.estimated_input_tokens + 1000);
    }

    #[test]
    fn fixed_input_counts_system_and_tools_but_not_conversation() {
        let budget = ModelBudget::new(Some(32_768), None, None);
        let mut source = request();
        source.messages.insert(0, ChatMessage::system("authority"));
        let base =
            NativeRequestProjection::full(source.clone(), vec![], budget.clone(), false).unwrap();

        source.messages[1] = ChatMessage::user("conversation ".repeat(2_000));
        source.messages[2] = ChatMessage::assistant("answer ".repeat(2_000));
        let conversational =
            NativeRequestProjection::full(source.clone(), vec![], budget.clone(), false).unwrap();
        assert_eq!(conversational.fixed_input_tokens, base.fixed_input_tokens);
        assert!(conversational.estimated_input_tokens > base.estimated_input_tokens);

        source.messages[0] = ChatMessage::system("authority ".repeat(500));
        let with_system =
            NativeRequestProjection::full(source.clone(), vec![], budget.clone(), false).unwrap();
        assert!(with_system.fixed_input_tokens > conversational.fixed_input_tokens);

        source.tools = Some(vec![ToolDefinition {
            name: "large_tool".into(),
            description: "schema ".repeat(500),
            parameters: serde_json::json!({"type":"object"}),
        }]);
        let with_tools = NativeRequestProjection::full(source, vec![], budget, false).unwrap();
        assert!(with_tools.fixed_input_tokens > with_system.fixed_input_tokens);
    }

    #[test]
    fn materialized_media_counts_once_in_append_and_candidate_estimates() {
        let mut source = request();
        let mut attachment = MessageAttachment::from_url("unused", "image/png");
        attachment.source = AttachmentDataSource::Bytes {
            base64_data: "A".repeat(500_000),
        };
        source.messages[1]
            .content_parts
            .push(MessageContentPart::image(attachment));
        let media = vec![MediaEstimate {
            message: 1,
            part: 0,
            input_tokens: 875,
        }];
        let budget = ModelBudget::new(Some(8192), None, None);
        let full =
            NativeRequestProjection::full(source.clone(), media.clone(), budget.clone(), false)
                .unwrap();
        assert!(full.fits);
        assert!(full.message_input_tokens[1] > 875);
        assert!(full.message_input_tokens[1] < 1100);
        assert!(full.estimated_input_tokens < 1400);
        use super::super::controller::{NativeInputReceipt, NativeUsageMeasurement};
        let mut prefix = full.request.clone();
        prefix.messages.pop();
        let measured = NativeUsageMeasurement {
            receipt: NativeInputReceipt::for_request(&prefix, "fixture", "fixture-api", 1, None)
                .unwrap(),
            input_tokens: 5000,
        };
        let receipt =
            NativeInputReceipt::for_request(&full.request, "fixture", "fixture-api", 1, None)
                .unwrap();
        assert_eq!(
            receipt
                .calibrated_input(
                    full.estimated_input_tokens,
                    &full.message_input_tokens,
                    Some(&measured)
                )
                .unwrap(),
            5000 + full.message_input_tokens[1]
        );

        let candidate = NativeRequestProjection::new(source, [0], media, budget, false)
            .unwrap()
            .evaluate("completed")
            .unwrap();
        assert_eq!(
            candidate.message_input_tokens[1],
            full.message_input_tokens[1]
        );
        assert_eq!(
            candidate.request.messages[1].content_parts,
            full.request.messages[1].content_parts
        );
        assert_eq!(
            candidate.message_input_tokens.len(),
            candidate.request.messages.len()
        );
    }

    #[test]
    fn projection_keeps_noncontiguous_protected_messages_and_system_authority() {
        let mut source = request();
        source.messages = vec![
            ChatMessage::system("authority"),
            ChatMessage::user("old"),
            ChatMessage::user("accepted steering"),
            ChatMessage::assistant("completed"),
            ChatMessage::user("current"),
        ];
        let budget = ModelBudget::new(None, None, None);
        assert!(
            NativeRequestProjection::new(source.clone(), [0], vec![], budget.clone(), false)
                .is_err()
        );
        let projected = NativeRequestProjection::new(source, [1, 3], vec![], budget, false)
            .unwrap()
            .evaluate("summary")
            .unwrap();
        assert_eq!(projected.request.messages.len(), 4);
        assert_eq!(projected.request.messages[0].content, "authority");
        assert_eq!(projected.request.messages[2].content, "accepted steering");
        assert_eq!(projected.request.messages[3].content, "current");
    }
}
