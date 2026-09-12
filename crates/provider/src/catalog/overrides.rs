//! Explicit reference corrections and models absent from upstream catalogs.
use super::{Candidate, LimitOrigin, OriginKind, Value, json, number, round, rules};

fn pricing(rates: [f64; 4]) -> Value {
    json!({"input":rates[0],"output":rates[1],"cacheRead":rates[2],"cacheWrite":rates[3]})
}
fn long_pricing(mut cost: Value) -> Value {
    cost["tiers"] = json!([{"inputTokensAbove":272000,"input":round(number(&cost["input"])*2.),
        "output":round(number(&cost["output"])*1.5),"cacheRead":round(number(&cost["cacheRead"])*2.),"cacheWrite":round(number(&cost["cacheWrite"])*2.)}]);
    cost
}
fn standard(id: &str) -> Option<Value> {
    match id {
        "gpt-5.6-terra" => Some(pricing([2., 12., 0.2, 2.5])),
        "gpt-5.6-luna" => Some(pricing([0.2, 1.2, 0.02, 0.25])),
        _ => None,
    }
}
#[allow(clippy::too_many_arguments)]
fn explicit(
    provider: &str,
    id: &str,
    name: &str,
    api: &str,
    url: &str,
    reasoning: bool,
    image: bool,
    cost: Value,
    context: u64,
    output: u64,
) -> Candidate {
    let origin = LimitOrigin {
        kind: OriginKind::Override,
        expression: "pinned Pi explicit model definition".into(),
    };
    Candidate {
        model: json!({"id":id,"name":name,"provider":provider,"api":api,"baseUrl":url,"reasoning":reasoning,
        "input":if image {vec!["text","image"]} else {vec!["text"]},"cost":cost,"contextWindow":context,"maxTokens":output}),
        context_origin: origin.clone(),
        output_origin: origin,
        reasoning_options: Value::Null,
        input_limit: None,
    }
}
fn add_missing(models: &mut Vec<Candidate>, model: Candidate) {
    if !models
        .iter()
        .any(|m| m.provider() == model.provider() && m.id() == model.id())
    {
        models.push(model);
    }
}

pub(super) fn apply(models: &mut Vec<Candidate>) -> anyhow::Result<()> {
    for m in models.iter_mut() {
        let id = m.id().to_owned();
        let p = m.provider().to_owned();
        if p == "github-copilot"
            && rules::GITHUB_COPILOT_EXTENDED_CONTEXT_MODELS.contains(&id.as_str())
        {
            m.override_limit("contextWindow", 1_000_000, "Copilot extended context");
        }
        if matches!(p.as_str(), "anthropic" | "opencode" | "opencode-go")
            && matches!(
                id.as_str(),
                "claude-opus-4-6" | "claude-sonnet-4-6" | "claude-opus-4.6" | "claude-sonnet-4.6"
            )
        {
            m.override_limit("contextWindow", 1_000_000, "Claude 4.6 context");
        }
        if matches!(p.as_str(), "opencode" | "opencode-go")
            && matches!(id.as_str(), "claude-sonnet-4-5" | "claude-sonnet-4")
        {
            m.override_limit("contextWindow", 200_000, "OpenCode Sonnet context");
        }
        if (matches!(p.as_str(), "opencode" | "opencode-go") && id == "gpt-5.4")
            || (p == "openai"
                && rules::OPENAI_SHORT_CONTEXT_CAPPED_MODEL_IDS.contains(&id.as_str()))
        {
            m.override_limit("contextWindow", 272_000, "OpenAI short context cap");
            m.override_limit("maxTokens", 128_000, "OpenAI output cap");
        }
        if p == "openai" && rules::OPENAI_LONG_CONTEXT_PRICING_MODEL_IDS.contains(&id.as_str()) {
            m.model["cost"] =
                long_pricing(standard(&id).unwrap_or_else(|| m.model["cost"].clone()));
        }
        if p == "cloudflare-ai-gateway"
            && let Some(cost) = standard(&id)
        {
            m.model["cost"] = long_pricing(cost);
        }
        if p == "openai" && id == "gpt-5-pro" {
            m.override_limit("maxTokens", 128_000, "GPT-5 Pro output");
        }
        if (p == "openrouter" && rules::OPENROUTER_KIMI_K3_MODEL_IDS.contains(&id.as_str()))
            || (p == "vercel-ai-gateway" && id == "moonshotai/kimi-k3")
        {
            m.override_limit("maxTokens", 131_072, "Kimi K3 output");
        }
        if p == "openrouter" && id == "moonshotai/kimi-k2.5" {
            m.model["cost"]["input"] = json!(0.41);
            m.model["cost"]["output"] = json!(2.06);
            m.model["cost"]["cacheRead"] = json!(0.07);
            m.override_limit("maxTokens", 4096, "OpenRouter Kimi K2.5 output");
        }
        if p == "openrouter" && id.starts_with("moonshotai/kimi-k2.6") {
            m.compat(json!({"supportsDeveloperRole":false,"requiresReasoningContentOnAssistantMessages":true}));
        }
        if p == "openrouter" && id == "z-ai/glm-5" {
            m.model["cost"]["input"] = json!(0.6);
            m.model["cost"]["output"] = json!(1.9);
            m.model["cost"]["cacheRead"] = json!(0.119);
        }
    }
    for (id, name, rates) in [
        ("gpt-6-astra", "GPT-6 Astra", [10., 50., 1., 12.5]),
        ("gpt-5.6-sol", "GPT-5.6 Sol", [5., 30., 0.5, 6.25]),
        ("gpt-5.6-terra", "GPT-5.6 Terra", [2., 12., 0.2, 2.5]),
        ("gpt-5.6-luna", "GPT-5.6 Luna", [0.2, 1.2, 0.02, 0.25]),
    ] {
        add_missing(
            models,
            explicit(
                "openai",
                id,
                name,
                "openai-responses",
                "https://api.openai.com/v1",
                true,
                true,
                long_pricing(pricing(rates)),
                272000,
                128000,
            ),
        );
    }
    add_missing(
        models,
        explicit(
            "openai",
            "gpt-5-chat-latest",
            "GPT-5 Chat Latest",
            "openai-responses",
            "https://api.openai.com/v1",
            false,
            true,
            pricing([1.25, 10., 0.125, 0.]),
            128000,
            16384,
        ),
    );
    for (id, name, rates, image) in [
        (
            "deepseek-v4-flash",
            "DeepSeek V4 Flash",
            [0.14, 0.28, 0.0028, 0.],
            false,
        ),
        (
            "deepseek-v4-flash-vision-exp",
            "DeepSeek V4 Flash Vision Exp",
            [0.14, 0.28, 0.0028, 0.],
            true,
        ),
        (
            "deepseek-v4-pro",
            "DeepSeek V4 Pro",
            [0.435, 0.87, 0.003625, 0.],
            false,
        ),
    ] {
        let mut m = explicit(
            "deepseek",
            id,
            name,
            "openai-completions",
            "https://api.deepseek.com",
            true,
            image,
            pricing(rates),
            1_000_000,
            384_000,
        );
        m.compat(
            json!({"requiresReasoningContentOnAssistantMessages":true,"thinkingFormat":"deepseek"}),
        );
        models.push(m);
    }
    for (id, name, reasoning, rates) in [
        (
            "Ling-2.6-flash",
            "Ling 2.6 Flash",
            false,
            [0.01, 0.02, 0., 0.],
        ),
        ("Ling-2.6-1T", "Ling 2.6 1T", false, [0.06, 0.25, 0., 0.]),
        ("Ring-2.6-1T", "Ring 2.6 1T", true, [0.06, 0.25, 0., 0.]),
    ] {
        let mut m = explicit(
            "ant-ling",
            id,
            name,
            "openai-completions",
            "https://api.ant-ling.com/v1",
            reasoning,
            false,
            pricing(rates),
            262144,
            65536,
        );
        m.compat(json!({"supportsStore":false,"supportsDeveloperRole":false,"supportsReasoningEffort":false,"maxTokensField":"max_tokens","supportsLongCacheRetention":false}));
        if reasoning {
            m.compat(json!({"thinkingFormat":"ant-ling"}));
        }
        models.push(m);
    }
    for m in models.iter_mut() {
        if m.api() == "openai-completions"
            && m.id().contains("deepseek-v4")
            && !rules::QWEN_TOKEN_PLAN_PROVIDER_IDS.contains(&m.provider())
        {
            if !matches!(m.provider(), "openrouter" | "opencode") {
                m.compat(json!({"thinkingFormat":"deepseek"}));
            }
            m.compat(json!({"requiresReasoningContentOnAssistantMessages":true}));
        }
    }
    models.retain(|m| {
        !matches!(m.provider(), "minimax" | "minimax-cn")
            || rules::MINIMAX_DIRECT_SUPPORTED_IDS.contains(&m.id())
    });
    for (id, name, rates, long, image, context) in [
        (
            "gpt-6-astra",
            "GPT-6 Astra",
            [10., 50., 1., 12.5],
            true,
            true,
            272000,
        ),
        (
            "gpt-5.3-codex-spark",
            "GPT-5.3 Codex Spark",
            [1.75, 14., 0.175, 0.],
            false,
            false,
            128000,
        ),
        (
            "gpt-5.4",
            "GPT-5.4",
            [2.5, 15., 0.25, 0.],
            true,
            true,
            272000,
        ),
        (
            "gpt-5.4-mini",
            "GPT-5.4 mini",
            [0.75, 4.5, 0.075, 0.],
            false,
            true,
            272000,
        ),
        ("gpt-5.5", "GPT-5.5", [5., 30., 0.5, 0.], true, true, 272000),
        (
            "gpt-5.6-luna",
            "GPT-5.6 Luna",
            [0.2, 1.2, 0.02, 0.25],
            true,
            true,
            272000,
        ),
        (
            "gpt-5.6-sol",
            "GPT-5.6 Sol",
            [5., 30., 0.5, 6.25],
            true,
            true,
            272000,
        ),
        (
            "gpt-5.6-terra",
            "GPT-5.6 Terra",
            [2., 12., 0.2, 2.5],
            true,
            true,
            272000,
        ),
    ] {
        let cost = pricing(rates);
        models.push(explicit(
            "openai-codex",
            id,
            name,
            "openai-codex-responses",
            "https://chatgpt.com/backend-api",
            true,
            image,
            if long { long_pricing(cost) } else { cost },
            context,
            128000,
        ));
    }
    add_missing(
        models,
        explicit(
            "mistral",
            "mistral-medium-3.5",
            "Mistral Medium 3.5",
            "mistral-conversations",
            "https://api.mistral.ai",
            true,
            true,
            pricing([1.5, 7.5, 0., 0.]),
            262144,
            262144,
        ),
    );
    add_missing(
        models,
        explicit(
            "openrouter",
            "auto",
            "Auto",
            "openai-completions",
            "https://openrouter.ai/api/v1",
            true,
            true,
            pricing([0.; 4]),
            2_000_000,
            30000,
        ),
    );
    add_missing(
        models,
        explicit(
            "openrouter",
            "openrouter/fusion",
            "OpenRouter: Fusion",
            "openai-completions",
            "https://openrouter.ai/api/v1",
            true,
            false,
            pricing([0.; 4]),
            1_000_000,
            30000,
        ),
    );
    let azure: Vec<_> = models
        .iter()
        .filter(|m| m.provider() == "openai" && m.api() == "openai-responses")
        .map(|m| {
            let mut m = m.clone();
            m.model["provider"] = json!("azure-openai-responses");
            m.model["api"] = json!("azure-openai-responses");
            m.model["baseUrl"] = json!("");
            m.reasoning_options = Value::Null;
            m.model["cost"].as_object_mut().unwrap().remove("tiers");
            if matches!(
                m.id(),
                "gpt-5.4" | "gpt-5.5" | "gpt-5.6-luna" | "gpt-5.6-sol" | "gpt-5.6-terra"
            ) {
                m.override_limit("contextWindow", 1_050_000, "Azure context override");
            }
            m
        })
        .collect();
    models.extend(azure);
    Ok(())
}
