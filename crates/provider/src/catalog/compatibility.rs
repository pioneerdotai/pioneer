//! Provider wire-format and reasoning capabilities from the pinned Pi rules.
use super::{Candidate, Value, json, merge, rules, text};

pub(super) fn effort_map(options: &Value) -> Option<Value> {
    let values: Vec<&str> = options
        .as_array()?
        .iter()
        .filter(|o| o["type"] == "effort")
        .flat_map(|o| o["values"].as_array().into_iter().flatten())
        .filter_map(Value::as_str)
        .collect();
    let levels = ["minimal", "low", "medium", "high", "xhigh", "max"];
    if !values.contains(&"none") && !levels.iter().any(|l| values.contains(l)) {
        return None;
    }
    let mut map = json!({"off": if values.contains(&"none") {json!("none")} else {Value::Null}});
    for level in levels {
        map[level] = if values.contains(&level) {
            json!(level)
        } else {
            Value::Null
        };
    }
    Some(map)
}

pub(super) fn together(c: &mut Candidate) {
    c.compat(json!({"supportsStore":false,"supportsDeveloperRole":false,"supportsReasoningEffort":false,
        "maxTokensField":"max_tokens","supportsStrictMode":false,"supportsLongCacheRetention":false}));
    if c.model["reasoning"] != true {
        return;
    }
    if rules::TOGETHER_REASONING_EFFORT_MODELS.contains(&c.id()) {
        c.compat(json!({"supportsReasoningEffort":true,"thinkingFormat":"openai"}));
        c.thinking(json!({"off":null,"minimal":null}));
    } else if rules::TOGETHER_TOGGLE_REASONING_EFFORT_MODELS.contains(&c.id()) {
        c.compat(json!({"supportsReasoningEffort":true,"thinkingFormat":"together"}));
        c.thinking(json!({"minimal":null,"low":null,"medium":null,"high":"high","xhigh":null}));
    } else if rules::TOGETHER_REASONING_ONLY_MODELS.contains(&c.id()) {
        c.thinking(json!({"off":null,"minimal":null,"low":null,"medium":null}));
    } else {
        c.compat(json!({"thinkingFormat":"together"}));
        c.thinking(json!({"minimal":null,"low":null,"medium":null}));
    }
}

fn completions(c: &mut Candidate) {
    let p = c.provider();
    let u = text(&c.model["baseUrl"]);
    let id = c.id();
    let zai = matches!(p, "zai" | "zai-coding-cn")
        || u.contains("api.z.ai")
        || u.contains("open.bigmodel.cn");
    let together =
        p == "together" || u.contains("api.together.ai") || u.contains("api.together.xyz");
    let moonshot = matches!(p, "moonshotai" | "moonshotai-cn") || u.contains("api.moonshot.");
    let router = p == "openrouter" || u.contains("openrouter.ai");
    let workers = p == "cloudflare-workers-ai" || u.contains("api.cloudflare.com");
    let gateway = p == "cloudflare-ai-gateway" || u.contains("gateway.ai.cloudflare.com");
    let nvidia = p == "nvidia" || u.contains("integrate.api.nvidia.com");
    let ling = p == "ant-ling" || u.contains("api.ant-ling.com");
    let deepseek = p == "deepseek" || u.to_lowercase().contains("deepseek.com");
    let grok = p == "xai" || u.contains("api.x.ai");
    let nonstandard = nvidia
        || p == "cerebras"
        || u.contains("cerebras.ai")
        || grok
        || together
        || u.contains("chutes.ai")
        || deepseek
        || zai
        || moonshot
        || p == "opencode"
        || u.contains("opencode.ai")
        || workers
        || gateway
        || ling;
    let mut delta = json!({});
    for (key, value) in [
        ("supportsStore", !nonstandard),
        (
            "supportsDeveloperRole",
            router && (id.starts_with("anthropic/") || id.starts_with("openai/"))
                || (!nonstandard && !router),
        ),
        (
            "supportsReasoningEffort",
            !grok && !zai && !moonshot && !together && !gateway && !nvidia && !ling,
        ),
        (
            "supportsStrictMode",
            !moonshot && !together && !gateway && !nvidia,
        ),
        (
            "supportsLongCacheRetention",
            !(together || workers || gateway || nvidia || ling),
        ),
    ] {
        if !value {
            delta[key] = json!(false);
        }
    }
    if u.contains("chutes.ai")
        || deepseek
        || moonshot
        || gateway
        || together
        || nvidia
        || ling
        || zai
    {
        delta["maxTokensField"] = json!("max_tokens");
    }
    if deepseek {
        delta["requiresReasoningContentOnAssistantMessages"] = json!(true);
    }
    let format = if deepseek {
        "deepseek"
    } else if zai {
        "zai"
    } else if together && !rules::TOGETHER_REASONING_ONLY_MODELS.contains(&id) {
        "together"
    } else if ling {
        "ant-ling"
    } else if router {
        "openrouter"
    } else {
        "openai"
    };
    if format != "openai" {
        delta["thinkingFormat"] = json!(format);
    }
    if p == "openrouter" && id.trim_start_matches('~').starts_with("anthropic/") {
        delta["cacheControlFormat"] = json!("anthropic");
    }
    merge(&mut delta, c.model["compat"].clone());
    if delta.as_object().is_some_and(|o| !o.is_empty()) {
        c.model["compat"] = delta;
    } else {
        c.model.as_object_mut().unwrap().remove("compat");
    }
}

fn mid_convo(id: &str) -> bool {
    let lower = id.to_lowercase();
    let id = lower
        .trim_start_matches('~')
        .strip_prefix("anthropic/")
        .unwrap_or(&lower);
    [
        "claude-opus-5",
        "claude-fable-5-1",
        "claude-fable-5.1",
        "claude-mythos-5-1",
        "claude-mythos-5.1",
    ]
    .iter()
    .any(|prefix| {
        id.strip_prefix(prefix).is_some_and(|tail| {
            tail.is_empty()
                || tail
                    .strip_prefix('-')
                    .is_some_and(|date| date.len() == 8 && date.bytes().all(|b| b.is_ascii_digit()))
        })
    })
}
fn contains_any(id: &str, patterns: &[&str]) -> bool {
    patterns.iter().any(|p| id.contains(p))
}
fn gemini3(id: &str, family: &str) -> bool {
    id.match_indices("gemini-3").any(|(pos, _)| {
        let tail = &id[pos + 8..];
        let tail = if let Some(n) = tail.strip_prefix('.') {
            let n = n.trim_start_matches(|c: char| c.is_ascii_digit());
            if n.len() == tail.len() - 1 {
                return false;
            }
            n
        } else {
            tail
        };
        tail.starts_with(family)
    })
}

pub(super) fn apply(c: &mut Candidate) {
    if c.api() == "openai-completions" {
        completions(c);
    }
    let id = c.id().to_owned();
    let lower = id.to_lowercase();
    let p = c.provider().to_owned();
    let api = c.api().to_owned();
    if api == "anthropic-messages" {
        if rules::VERIFIED_ANTHROPIC_MID_CONVO_EFFORT_PROVIDERS.contains(&p.as_str())
            && mid_convo(&id)
        {
            c.compat(json!({"supportsMidConvoEffort":true}));
            c.thinking(json!({"off":null}));
        }
        if rules::EAGER_TOOL_INPUT_STREAMING_UNSUPPORTED_ANTHROPIC_MODELS
            .contains(&format!("{p}:{id}").as_str())
        {
            c.compat(json!({"supportsEagerToolInputStreaming":false}));
        }
        if p == "xiaomi" || p.starts_with("xiaomi-token-plan-") {
            c.compat(json!({"allowEmptySignature":true}));
        }
    }
    let accepts_effort = match api.as_str() {
        "anthropic-messages" => c.model["compat"]["forceAdaptiveThinking"] == true,
        "openai-responses" | "azure-openai-responses" | "openai-codex-responses" => true,
        "openai-completions" => {
            c.model["compat"]["supportsReasoningEffort"] != false
                && matches!(
                    c.model["compat"]["thinkingFormat"].as_str(),
                    None | Some("openai")
                )
        }
        _ => false,
    };
    if accepts_effort && let Some(map) = effort_map(&c.reasoning_options) {
        c.thinking(map);
    }
    let responses = matches!(api.as_str(), "openai-responses" | "azure-openai-responses");
    if responses && id.starts_with("gpt-5") {
        c.thinking(json!({"off":null}));
    }
    if rules::OPENAI_GRAMMAR_TOOL_APIS.contains(&api.as_str()) && id == "gpt-6-astra" {
        c.thinking(json!({"off":null,"minimal":null,"low":"low","medium":"medium","high":"high","xhigh":"xhigh","max":"max"}));
    }
    if p == "github-copilot" && id.starts_with("gpt-5") {
        c.thinking(json!({"minimal":"low"}));
    }
    if p == "openai"
        && api == "openai-responses"
        && rules::OPENAI_RESPONSES_NONE_REASONING_MODELS.contains(&id.as_str())
    {
        c.thinking(json!({"off":"none"}));
    }
    if p == "xai" && api == "openai-responses" && c.model["thinkingLevelMap"].is_null() {
        c.thinking(json!({"off":null,"minimal":null}));
    }
    if contains_any(
        &id,
        &[
            "gpt-5.2",
            "gpt-5.3",
            "gpt-5.4",
            "gpt-5.5",
            "gpt-5.6",
            "gpt-6-astra",
        ],
    ) {
        c.thinking(json!({"xhigh":"xhigh"}));
    }
    if contains_any(&id, &["gpt-5.6", "gpt-6-astra"])
        && (responses
            || matches!(
                api.as_str(),
                "openai-codex-responses" | "openai-completions"
            ))
    {
        c.thinking(json!({"max":"max"}));
    }
    if p == "openai" && id == "gpt-5.5" {
        c.thinking(json!({"minimal":null}));
    }
    if id.ends_with("gpt-5.5-pro") {
        c.thinking(json!({"off":null,"minimal":null,"low":null}));
    }
    if contains_any(&id, &["opus-4-6", "opus-4.6", "sonnet-4-6", "sonnet-4.6"]) {
        c.thinking(json!({"max":"max"}));
    }
    let newest = contains_any(
        &id,
        &[
            "opus-4-7", "opus-4.7", "opus-4-8", "opus-4.8", "opus-5", "opus.5", "sonnet-5",
            "sonnet.5",
        ],
    );
    if newest {
        c.thinking(json!({"xhigh":"xhigh","max":"max"}));
    }
    if id.contains("fable-5") {
        c.thinking(json!({"off":null,"xhigh":"xhigh","max":"max"}));
    }
    if api == "anthropic-messages" {
        if newest
            || contains_any(
                &lower,
                &[
                    "opus-4-6",
                    "opus-4.6",
                    "sonnet-4-6",
                    "sonnet-4.6",
                    "fable-5",
                    "mythos-5",
                ],
            )
        {
            c.compat(json!({"forceAdaptiveThinking":true}));
        }
        if contains_any(
            &lower,
            &[
                "opus-4-7", "opus-4.7", "opus-4-8", "opus-4.8", "opus-5", "opus.5",
            ],
        ) {
            c.compat(json!({"supportsTemperature":false}));
        }
    }
    if api == "openai-completions" && id.contains("deepseek-v4") {
        c.thinking(json!({"minimal":null,"low":null,"medium":null,"high":"high","max":"max"}));
        if p == "openrouter" {
            c.thinking(json!({"xhigh":"xhigh","max":null}));
        } else if matches!(p.as_str(), "deepseek" | "opencode" | "opencode-go")
            && id.contains("deepseek-v4-flash")
        {
            c.thinking(json!({"low":"low"}));
        }
    }
    if matches!(api.as_str(), "google-generative-ai" | "google-vertex") {
        if gemini3(&lower, "-pro") {
            c.thinking(json!({"off":null,"minimal":null,"low":"LOW","medium":null,"high":"HIGH"}));
        }
        if gemini3(&lower, "-flash")
            || matches!(
                lower.as_str(),
                "gemini-flash-latest" | "gemini-flash-lite-latest"
            )
        {
            c.thinking(json!({"off":null}));
        }
        if contains_any(&lower, &["gemma4", "gemma-4"]) {
            c.thinking(
                json!({"off":null,"minimal":"MINIMAL","low":null,"medium":null,"high":"HIGH"}),
            );
        }
    }
    if p == "groq" && id == "qwen/qwen3.6-27b" {
        c.thinking(json!({"minimal":null,"low":null,"medium":null,"high":"default"}));
    }
    if api == "openai-codex-responses" && c.model["thinkingLevelMap"]["xhigh"] == "xhigh" {
        c.thinking(json!({"minimal":"low"}));
    }
    if matches!(p.as_str(), "moonshotai" | "moonshotai-cn")
        && matches!(id.as_str(), "kimi-k2.7-code" | "kimi-k2.7-code-highspeed")
    {
        c.thinking(json!({"off":null}));
    }
    if p == "openrouter" && id.starts_with("inception/mercury-2") {
        c.thinking(json!({"off":null}));
    }
    if p == "openrouter" && id == "z-ai/glm-5.2" {
        c.thinking(json!({"xhigh":"xhigh"}));
    }
    if p == "fireworks" && id.contains("glm-5p2") {
        c.thinking(json!({"off":"none","minimal":null,"low":"high","medium":"high","max":"max"}));
    }
    if p == "opencode-go" && id == "glm-5.2" {
        c.thinking(
            json!({"off":null,"minimal":null,"low":null,"medium":null,"high":"high","max":"max"}),
        );
    }
    if p == "opencode-go" && id == "kimi-k2.6" {
        c.thinking(json!({"minimal":null,"low":null,"medium":null}));
    }
    if p == "opencode" && id == "grok-build-0.1" {
        c.thinking(json!({"off":null,"minimal":null,"low":null,"medium":null}));
    }
    if p == "ant-ling" && c.model["reasoning"] == true {
        c.thinking(json!({"off":null,"minimal":null,"low":null,"medium":null,"high":"high","xhigh":"xhigh"}));
    }
    if p == "github-copilot" {
        if matches!(
            id.as_str(),
            "claude-opus-4.7" | "claude-opus-4.8" | "claude-opus-5"
        ) {
            c.thinking(json!({"minimal":"low"}));
        }
        if id == "claude-sonnet-4.6" {
            c.thinking(json!({"minimal":"low","max":"max"}));
        }
    }
    if matches!(p.as_str(), "openai" | "cloudflare-ai-gateway") && api == "openai-responses" {
        c.compat(json!({"supportsStrictMode":true}));
    }
    if p == "anthropic" && api == "anthropic-messages" {
        c.compat(json!({"supportsStrictTools":true}));
    }
    let gpt_generation = id
        .strip_prefix("gpt-")
        .and_then(|s| s.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|s| s.parse::<u32>().ok());
    if rules::OPENAI_GRAMMAR_TOOL_PROVIDERS.contains(&p.as_str())
        && rules::OPENAI_GRAMMAR_TOOL_APIS.contains(&api.as_str())
        && gpt_generation.is_some_and(|v| v >= 5)
    {
        c.compat(json!({"supportsOpenAIGrammarTools":true}));
    }
    let direct = p == "openai" && api == "openai-responses";
    let codex = p == "openai-codex" && api == "openai-codex-responses";
    if (direct || codex) && rules::OPENAI_TOOL_SEARCH_MODEL_IDS.contains(&id.as_str()) {
        c.compat(json!({"supportsToolSearch":true}));
        if direct || rules::OPENAI_CODEX_ADDITIONAL_TOOLS_MODEL_IDS.contains(&id.as_str()) {
            c.compat(json!({"supportsAdditionalTools":true}));
        }
    }
    if direct && super::number(&c.model["cost"]["cacheWrite"]) > 0. {
        c.compat(json!({"supportsExplicitPromptCacheMode":true}));
    }
}

pub(super) fn fallbacks(models: &mut [Candidate]) {
    let costs: std::collections::BTreeMap<_, _> = models
        .iter()
        .filter(|m| m.provider() == "anthropic" && m.api() == "anthropic-messages")
        .map(|m| (m.id().to_owned(), m.model["cost"].clone()))
        .collect();
    for model in models {
        if model.provider() != "anthropic" || model.api() != "anthropic-messages" {
            continue;
        }
        let ids: &[&str] = match model.id() {
            "claude-fable-5" => &["claude-opus-4-8", "claude-opus-5"],
            "claude-opus-5" => &["claude-opus-4-8"],
            _ => continue,
        };
        let allowed: Vec<_> = ids
            .iter()
            .filter(|id| model.model["compat"]["supportsMidConvoEffort"] != true || mid_convo(id))
            .filter_map(|id| {
                costs
                    .get(*id)
                    .map(|cost| json!({"provider":"anthropic","model":id,"cost":cost}))
            })
            .collect();
        if !allowed.is_empty() {
            model.compat(json!({"allowedFallbackModels":allowed}));
        }
    }
}
