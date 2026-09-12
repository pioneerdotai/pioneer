//! Source-specific transforms and provider routing. No I/O and no generated
//! catalog lookups: new source entries follow the same rules as saved fixtures.
use super::compatibility::{effort_map, together};
use super::rules::*;
use super::*;
use std::collections::BTreeSet;
const WORKERS: &str = "https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/ai/v1";
const CF_PREFIX: &str =
    "https://gateway.ai.cloudflare.com/v1/{CLOUDFLARE_ACCOUNT_ID}/{CLOUDFLARE_GATEWAY_ID}";
fn entries<'a>(data: &'a Value, provider: &str) -> impl Iterator<Item = (&'a String, &'a Value)> {
    data[provider]["models"]
        .as_object()
        .into_iter()
        .flat_map(|m| m.iter())
}
fn constrained_compat() -> Value {
    json!({"supportsStore":false,"supportsDeveloperRole":false,"supportsReasoningEffort":false,"maxTokensField":"max_tokens","supportsStrictMode":false,"supportsLongCacheRetention":false})
}

pub(super) fn models_dev(data: &Value, nvidia: &Value, strict: bool) -> Result<Vec<Candidate>> {
    let mut result = Vec::new();
    let specs = [
        (
            "amazon-bedrock",
            "amazon-bedrock",
            "bedrock-converse-stream",
            "https://bedrock-runtime.us-east-1.amazonaws.com",
        ),
        (
            "anthropic",
            "anthropic",
            "anthropic-messages",
            "https://api.anthropic.com",
        ),
        (
            "google",
            "google",
            "google-generative-ai",
            "https://generativelanguage.googleapis.com/v1beta",
        ),
        (
            "google-vertex",
            "google-vertex",
            "google-vertex",
            "https://{location}-aiplatform.googleapis.com",
        ),
        (
            "openai",
            "openai",
            "openai-responses",
            "https://api.openai.com/v1",
        ),
        (
            "groq",
            "groq",
            "openai-completions",
            "https://api.groq.com/openai/v1",
        ),
        (
            "cerebras",
            "cerebras",
            "openai-completions",
            "https://api.cerebras.ai/v1",
        ),
        (
            "cloudflare-workers-ai",
            "cloudflare-workers-ai",
            "openai-completions",
            WORKERS,
        ),
        ("xai", "xai", "openai-responses", "https://api.x.ai/v1"),
        (
            "mistral",
            "mistral",
            "mistral-conversations",
            "https://api.mistral.ai",
        ),
        (
            "huggingface",
            "huggingface",
            "openai-completions",
            "https://router.huggingface.co/v1",
        ),
        (
            "minimax",
            "minimax",
            "anthropic-messages",
            "https://api.minimax.io/anthropic",
        ),
        (
            "minimax-cn",
            "minimax-cn",
            "anthropic-messages",
            "https://api.minimaxi.com/anthropic",
        ),
        (
            "xiaomi",
            "xiaomi",
            "openai-completions",
            "https://api.xiaomimimo.com/v1",
        ),
        (
            "xiaomi-token-plan-cn",
            "xiaomi-token-plan-cn",
            "openai-completions",
            "https://token-plan-cn.xiaomimimo.com/v1",
        ),
        (
            "xiaomi-token-plan-ams",
            "xiaomi-token-plan-ams",
            "openai-completions",
            "https://token-plan-ams.xiaomimimo.com/v1",
        ),
        (
            "xiaomi-token-plan-sgp",
            "xiaomi-token-plan-sgp",
            "openai-completions",
            "https://token-plan-sgp.xiaomimimo.com/v1",
        ),
    ];
    for (source, provider, api, url) in specs {
        for (id, m) in entries(data, source) {
            if m["tool_call"] != true {
                continue;
            }
            if provider == "amazon-bedrock"
                && (BEDROCK_INFERENCE_PROFILE_ONLY_MODEL_IDS.contains(&id.as_str())
                    || id.starts_with("ai21.jamba")
                    || id.starts_with("mistral.mistral-7b-instruct-v0"))
            {
                continue;
            }
            if provider == "openai"
                && MODELS_DEV_OPENAI_UNSUPPORTED_MODEL_IDS.contains(&id.as_str())
            {
                continue;
            }
            if provider == "google-vertex"
                && (!id.starts_with("gemini-") || id == "gemini-3.1-flash-lite-preview")
            {
                continue;
            }
            if provider.starts_with("xiaomi") && m["status"] == "deprecated" {
                continue;
            }
            let alias = match id.as_str() {
                "gemini-flash-latest" => Some("gemini-3.5-flash"),
                "gemini-flash-lite-latest" => Some("gemini-3.1-flash-lite"),
                _ => None,
            };
            let effective = if matches!(provider, "google" | "google-vertex") {
                alias
                    .and_then(|a| data[source]["models"].get(a))
                    .unwrap_or(m)
            } else {
                m
            };
            let mut candidate = base(provider, id, api, url, effective, (4096, 4096));
            candidate.model["name"] =
                json!(m["name"].as_str().filter(|s| !s.is_empty()).unwrap_or(id));
            match provider {
                "amazon-bedrock"=>{if id.starts_with("eu."){candidate.model["baseUrl"]=json!("https://bedrock-runtime.eu-central-1.amazonaws.com");}
if m["structured_output"]==true {candidate.compat(json!({"supportsStrictMode":true}));}},
                "google-vertex"=>{candidate.model["cost"]["cacheWrite"]=json!(0);if id=="gemini-2.5-flash"{candidate.model["cost"]["cacheRead"]=json!(0.03);}},
                "cloudflare-workers-ai"=>candidate.compat(json!({"sendSessionAffinityHeaders":true})),
                "xai"=>candidate.compat(json!({"supportsLongCacheRetention":false})),
                "mistral"=>{if m["cost"]["cache_read"].is_null(){candidate.model["cost"]["cacheRead"]=json!(round(number(&m["cost"]["input"])*0.1));}},
                "huggingface"=>candidate.compat(json!({"supportsDeveloperRole":false})),
                p if p.starts_with("xiaomi")=>candidate.compat(json!({"requiresReasoningContentOnAssistantMessages":true,"thinkingFormat":"deepseek"})),
                _=>{}
            }
            result.push(candidate);
        }
    }
    let mut cloudflare_ids = BTreeSet::new();
    for (prefixed, m) in entries(data, "cloudflare-ai-gateway") {
        if m["tool_call"] != true {
            continue;
        }
        let Some((upstream, native)) = prefixed.split_once('/') else {
            continue;
        };
        let (id, api, path) = match upstream {
            "openai" => (native, "openai-responses", "openai"),
            "anthropic" => (native, "anthropic-messages", "anthropic"),
            "workers-ai" => (prefixed.as_str(), "openai-completions", "compat"),
            _ => continue,
        };
        let mut c = base(
            "cloudflare-ai-gateway",
            id,
            api,
            &format!("{CF_PREFIX}/{path}"),
            m,
            (4096, 4096),
        );
        if upstream != "openai" {
            c.compat(json!({"sendSessionAffinityHeaders":true}));
        }
        cloudflare_ids.insert(id.to_owned());
        result.push(c);
    }
    for (id, m) in entries(data, "cloudflare-workers-ai") {
        let id = format!("workers-ai/{id}");
        if m["tool_call"] != true || !cloudflare_ids.insert(id.clone()) {
            continue;
        }
        let mut c = base(
            "cloudflare-ai-gateway",
            &id,
            "openai-completions",
            &format!("{CF_PREFIX}/compat"),
            m,
            (4096, 4096),
        );
        c.compat(json!({"sendSessionAffinityHeaders":true}));
        result.push(c);
    }
    let mut live = BTreeMap::new();
    for m in nvidia["data"].as_array().into_iter().flatten() {
        let id = text(&m["id"]);
        live.insert(id.to_owned(), id);
        live.insert(id.to_lowercase().replace('_', "."), id);
    }
    for (id, m) in entries(data, "nvidia") {
        if m["tool_call"] != true
            || !has(&m["modalities"]["input"], "text")
            || !has(&m["modalities"]["output"], "text")
        {
            continue;
        }
        let Some(id) = live
            .get(id)
            .or_else(|| live.get(&id.to_lowercase().replace('_', ".")))
        else {
            continue;
        };
        if NVIDIA_NIM_UNSUPPORTED_MODELS.contains(id) {
            continue;
        }
        let mut c = base(
            "nvidia",
            id,
            "openai-completions",
            "https://integrate.api.nvidia.com/v1",
            m,
            (4096, 4096),
        );
        c.compat(constrained_compat());
        c.model["headers"] = json!({"NVCF-POLL-SECONDS":"3600"});
        result.push(c);
    }
    for (source, provider, url) in [
        (
            "zai-coding-plan",
            "zai",
            "https://api.z.ai/api/coding/paas/v4",
        ),
        (
            "zhipuai-coding-plan",
            "zai-coding-cn",
            "https://open.bigmodel.cn/api/coding/paas/v4",
        ),
    ] {
        for (id, m) in entries(data, source) {
            if m["tool_call"] != true {
                continue;
            }
            let mut c = base(provider, id, "openai-completions", url, m, (4096, 4096));
            c.model["cost"] = cost(data["zai"]["models"][id].get("cost").unwrap_or(&m["cost"]));
            c.compat(json!({"supportsDeveloperRole":false,"thinkingFormat":"zai"}));
            if let Some(mut map) = effort_map(&m["reasoning_options"]) {
                if matches!(id.as_str(), "glm-5.2" | "glm-5.2-highspeed") {
                    map["off"] = json!("none");
                }
                c.thinking(map);
                c.compat(json!({"supportsReasoningEffort":true}));
            }
            if !ZAI_TOOL_STREAM_UNSUPPORTED_MODELS.contains(&id.as_str()) {
                c.compat(json!({"zaiToolStream":true}));
            }
            result.push(c);
        }
    }
    let together_source = ["together", "togetherai", "together-ai"]
        .into_iter()
        .find(|p| !data[*p].is_null())
        .unwrap_or("together");
    for (id, m) in entries(data, together_source) {
        if m["tool_call"] != true || m["status"] == "deprecated" {
            continue;
        }
        let mut c = base(
            "together",
            id,
            "openai-completions",
            "https://api.together.ai/v1",
            m,
            (4096, 4096),
        );
        together(&mut c);
        result.push(c);
    }
    for (id, m) in entries(data, "baseten") {
        if m["status"] == "deprecated" {
            continue;
        }
        let glm = matches!(id.as_str(), "zai-org/GLM-5.2" | "zai-org/GLM-5.2-Fast");
        let options = m["reasoning_options"].as_array();
        let toggle = glm || options.is_some_and(|v| v.iter().any(|o| o["type"] == "toggle"));
        let effort = glm || options.is_some_and(|v| v.iter().any(|o| o["type"] == "effort"));
        let mut c = base(
            "baseten",
            id,
            "openai-completions",
            "https://inference.baseten.co/v1",
            m,
            (4096, 4096),
        );
        c.reasoning_options = Value::Null;
        c.compat(constrained_compat());
        c.compat(json!({"supportsUsageInStreaming":true,"supportsStrictMode":true}));
        if effort {
            c.compat(json!({"supportsReasoningEffort":true,"thinkingFormat":"openai"}));
        }
        if toggle {
            c.compat(json!({"thinkingFormat":"baseten","chatTemplateArgs":{"enable_thinking":{"$var":"thinking.enabled"}}}));
        }
        if glm {
            c.model["input"] = json!(["text"]);
            c.thinking(json!({"off":"none","minimal":null,"low":null,"medium":null,"high":"high","xhigh":null,"max":"max"}));
        } else if toggle {
            c.thinking(json!({"off":"off","minimal":null,"low":null,"medium":null,"high":"high","xhigh":null,"max":null}));
        } else if let Some(map) = effort_map(&m["reasoning_options"]) {
            c.thinking(map);
        }
        result.push(c);
    }
    for (id, m) in entries(data, "fireworks-ai") {
        if m["tool_call"] != true {
            continue;
        }
        let openai = id.contains("glm-") || id.contains("kimi-k3");
        let mut c = base(
            "fireworks",
            id,
            if openai {
                "openai-completions"
            } else {
                "anthropic-messages"
            },
            if openai {
                "https://api.fireworks.ai/inference/v1"
            } else {
                "https://api.fireworks.ai/inference"
            },
            m,
            (4096, 4096),
        );
        c.compat(json!({"sendSessionAffinityHeaders":true,"supportsLongCacheRetention":false}));
        if openai {
            c.compat(json!({"supportsStore":false,"supportsDeveloperRole":false}));
        } else {
            c.compat(json!({"supportsEagerToolInputStreaming":false,"supportsCacheControlOnTools":false}));
        }
        if id.contains("kimi-k3") {
            c.compat(json!({"requiresReasoningContentOnAssistantMessages":true,"thinkingFormat":"openai","deferredToolsMode":"kimi"}));
        }
        result.push(c);
    }
    for provider in ["opencode", "opencode-go"] {
        let url = if provider == "opencode" {
            "https://opencode.ai/zen"
        } else {
            "https://opencode.ai/zen/go"
        };
        for (id, m) in entries(data, provider) {
            if m["tool_call"] != true || m["status"] == "deprecated" {
                continue;
            }
            let api = match text(&m["provider"]["npm"]) {
                "@ai-sdk/openai" => "openai-responses",
                "@ai-sdk/anthropic" => "anthropic-messages",
                "@ai-sdk/google" => "google-generative-ai",
                _ => "openai-completions",
            };
            let mut c = base(
                provider,
                id,
                api,
                &if api == "anthropic-messages" {
                    url.into()
                } else {
                    format!("{url}/v1")
                },
                m,
                (4096, 4096),
            );
            if api == "openai-responses" {
                c.compat(json!({"sessionAffinityFormat":"openai-nosession"}));
            }
            if m["provider"]["npm"] == "@ai-sdk/alibaba" {
                c.compat(json!({"cacheControlFormat":"anthropic"}));
            }
            if provider == "opencode" && id == "grok-build-0.1" {
                c.compat(json!({"supportsReasoningEffort":false}));
            }
            if id == "kimi-k2.6" {
                c.compat(json!({"thinkingFormat":"deepseek","supportsReasoningEffort":false}));
            }
            if provider == "opencode-go"
                && matches!(
                    id.as_str(),
                    "minimax-m2.7" | "qwen3.5-plus" | "qwen3.6-plus"
                )
            {
                c.model["api"] = json!("openai-completions");
                c.model["baseUrl"] = json!(format!("{url}/v1"));
                if id.starts_with("qwen") {
                    c.compat(json!({"thinkingFormat":"qwen"}));
                }
            }
            if c.api() == "openai-completions" {
                c.compat(json!({"maxTokensField":"max_tokens"}));
                if OPENCODE_OPENAI_COMPLETIONS_LONG_CACHE_RETENTION_UNSUPPORTED_MODELS
                    .contains(&format!("{provider}:{id}").as_str())
                {
                    c.compat(json!({"supportsLongCacheRetention":false}));
                }
            }
            result.push(c);
        }
    }
    for (id, m) in entries(data, "github-copilot") {
        if m["tool_call"] != true || m["status"] == "deprecated" {
            continue;
        }
        let claude = ["haiku", "sonnet", "opus", "fable"].iter().any(|family| {
            ["4", "5"].iter().any(|v| {
                let prefix = format!("claude-{family}-{v}");
                id == &prefix
                    || id.starts_with(&format!("{prefix}."))
                    || id.starts_with(&format!("{prefix}-"))
            })
        });
        let api = if claude {
            "anthropic-messages"
        } else if ["grok-", "gpt-5", "oswe", "mai-"]
            .iter()
            .any(|prefix| id.starts_with(prefix))
        {
            "openai-responses"
        } else {
            "openai-completions"
        };
        let mut c = base(
            "github-copilot",
            id,
            api,
            "https://api.individual.githubcopilot.com",
            m,
            (128000, 8192),
        );
        c.model["headers"] = json!({"User-Agent":"GitHubCopilotChat/0.35.0","Editor-Version":"vscode/1.107.0","Editor-Plugin-Version":"copilot-chat/0.35.0","Copilot-Integration-Id":"vscode-chat"});
        let tiers: Vec<_> = m["cost"]["tiers"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|t| t["tier"]["type"] == "context" && !t["tier"]["size"].is_null())
            .map(|t| {
                let mut v = cost(t);
                v["inputTokensAbove"] = t["tier"]["size"].clone();
                v
            })
            .collect();
        if !tiers.is_empty() {
            c.model["cost"]["tiers"] = json!(tiers);
        }
        if api == "openai-completions" {
            c.compat(json!({"supportsStore":false,"supportsDeveloperRole":false,"supportsReasoningEffort":false}));
        }
        result.push(c);
    }
    for (id, m) in entries(data, "kimi-for-coding") {
        if m["tool_call"] != true {
            continue;
        }
        let alias = matches!(id.as_str(), "k2p5" | "k2p6" | "k2p7");
        if alias
            && data["kimi-for-coding"]["models"]
                .get("kimi-for-coding")
                .is_some()
        {
            continue;
        }
        let id = if alias { "kimi-for-coding" } else { id };
        let mut c = base(
            "kimi-coding",
            id,
            "anthropic-messages",
            "https://api.kimi.com/coding",
            m,
            (4096, 4096),
        );
        if alias {
            c.model["name"] = json!("Kimi For Coding");
        }
        c.compat(json!({"forceAdaptiveThinking":true}));
        if matches!(id, "k3" | "kimi-for-coding") {
            c.compat(json!({"allowEmptySignature":true}));
        }
        if id == "k3" {
            c.model["reasoning"] = json!(true);
        }
        let rates = match id {
            "k3" => [3., 15., 0.3, 0.],
            "kimi-for-coding" => [0.95, 4., 0.19, 0.],
            "kimi-for-coding-highspeed" => [1.9, 8., 0.38, 0.],
            "kimi-k2-thinking" => [0.6, 2.5, 0.15, 0.],
            _ => [0.; 4],
        };
        for (key, rate) in ["input", "output", "cacheRead", "cacheWrite"]
            .into_iter()
            .zip(rates)
        {
            if number(&c.model["cost"][key]) == 0. {
                c.model["cost"][key] = json!(rate);
            }
        }
        result.push(c);
    }
    for (provider, url) in [
        ("moonshotai", "https://api.moonshot.ai/v1"),
        ("moonshotai-cn", "https://api.moonshot.cn/v1"),
    ] {
        for (id, m) in entries(data, provider) {
            if m["tool_call"] != true {
                continue;
            }
            let mut c = base(provider, id, "openai-completions", url, m, (4096, 4096));
            c.compat(json!({"supportsStore":false,"supportsDeveloperRole":false,"supportsReasoningEffort":false,"maxTokensField":"max_tokens","supportsStrictMode":false,"thinkingFormat":"deepseek"}));
            if id == "kimi-k3" {
                c.model["reasoning"] = json!(true);
                c.compat(json!({"requiresReasoningContentOnAssistantMessages":true,"deferredToolsMode":"kimi","thinkingFormat":"openai","supportsReasoningEffort":true}));
                for (key, rate) in ["input", "output", "cacheRead", "cacheWrite"]
                    .into_iter()
                    .zip([3., 15., 0.3, 0.])
                {
                    if number(&c.model["cost"][key]) == 0. {
                        c.model["cost"][key] = json!(rate);
                    }
                }
            }
            result.push(c);
        }
    }
    for (source, provider, region, individual) in [
        (
            "alibaba-token-plan",
            "qwen-token-plan",
            "ap-southeast-1",
            false,
        ),
        (
            "alibaba-token-plan",
            "qwen-token-plan-individual",
            "ap-southeast-1",
            true,
        ),
        (
            "alibaba-token-plan-cn",
            "qwen-token-plan-cn",
            "cn-beijing",
            false,
        ),
    ] {
        let mut emitted = BTreeSet::new();
        for (id, m) in entries(data, source) {
            if m["tool_call"] != true
                || QWEN_TOKEN_PLAN_EXCLUDED_MODEL_IDS.contains(&id.as_str())
                || individual && !QWEN_TOKEN_PLAN_INDIVIDUAL_MODEL_IDS.contains(&id.as_str())
            {
                continue;
            }
            let map=effort_map(&m["reasoning_options"]).or_else(||QWEN_TOKEN_PLAN_REASONING_EFFORT_FALLBACK_MODEL_IDS.contains(&id.as_str()).then(||json!({"minimal":null,"low":null,"medium":null,"high":"high","xhigh":null,"max":"max"})));
            let mut c = base(
                provider,
                id,
                "openai-completions",
                &format!("https://token-plan.{region}.maas.aliyuncs.com/compatible-mode/v1"),
                m,
                (4096, 4096),
            );
            c.reasoning_options = Value::Null;
            c.compat(json!({"thinkingFormat":"qwen","supportsDeveloperRole":false,"supportsStore":false,"supportsReasoningEffort":map.is_some()}));
            if let Some(map) = map {
                c.thinking(map);
            }
            emitted.insert(id.as_str());
            result.push(c);
        }
        if individual && strict {
            ensure!(
                emitted
                    == QWEN_TOKEN_PLAN_INDIVIDUAL_MODEL_IDS
                        .iter()
                        .copied()
                        .collect(),
                "incomplete Qwen individual model allowlist"
            );
        }
    }
    Ok(result)
}

pub(super) fn openrouter(data: &Value) -> Vec<Candidate> {
    data["data"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|m| has(&m["supported_parameters"], "tools"))
        .map(|m| {
            let id = text(&m["id"]);
            let anthropic = id.starts_with("anthropic/") && !id.ends_with(":batch");
            let mut c = base(
                "openrouter",
                id,
                if anthropic {
                    "anthropic-messages"
                } else {
                    "openai-completions"
                },
                if anthropic {
                    "https://openrouter.ai/api"
                } else {
                    "https://openrouter.ai/api/v1"
                },
                &json!({}),
                (4096, 4096),
            );
            c.model["name"] = m["name"].clone();
            c.model["reasoning"] = json!(has(&m["supported_parameters"], "reasoning"));
            c.model["input"] = if text(&m["architecture"]["modality"]).contains("image") {
                json!(["text", "image"])
            } else {
                json!(["text"])
            };
            let context = if number(&m["top_provider"]["context_length"]) != 0. {
                &m["top_provider"]["context_length"]
            } else {
                &m["context_length"]
            };
            (c.model["contextWindow"], c.context_origin) =
                limit(context, 4096, "openrouter.context_length");
            (c.model["maxTokens"], c.output_origin) = limit(
                &m["top_provider"]["max_completion_tokens"],
                4096,
                "openrouter.max_completion_tokens",
            );
            for (key, source) in [
                ("input", "prompt"),
                ("output", "completion"),
                ("cacheRead", "input_cache_read"),
                ("cacheWrite", "input_cache_write"),
            ] {
                c.model["cost"][key] = json!(round(number(&m["pricing"][source]) * 1_000_000.));
            }
            let reasoning = &m["reasoning"];
            let mandatory = reasoning["mandatory"] == true;
            if let Some(mut map) =
                effort_map(&json!([{"type":"effort","values":reasoning["supported_efforts"]}]))
            {
                map["off"] = if mandatory {
                    Value::Null
                } else {
                    json!("none")
                };
                c.thinking(map);
            } else if mandatory {
                c.thinking(json!({"off":null}));
            }
            c
        })
        .collect()
}
pub(super) fn vercel(data: &Value) -> Vec<Candidate> {
    data["data"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|m| has(&m["tags"], "tool-use"))
        .map(|m| {
            let id = text(&m["id"]);
            let mut c = base(
                "vercel-ai-gateway",
                id,
                "anthropic-messages",
                "https://ai-gateway.vercel.sh",
                m,
                (4096, 4096),
            );
            c.model["reasoning"] = json!(has(&m["tags"], "reasoning"));
            c.model["input"] = if has(&m["tags"], "vision") {
                json!(["text", "image"])
            } else {
                json!(["text"])
            };
            (c.model["contextWindow"], c.context_origin) =
                limit(&m["context_window"], 4096, "vercel.context_window");
            (c.model["maxTokens"], c.output_origin) =
                limit(&m["max_tokens"], 4096, "vercel.max_tokens");
            for (key, source) in [
                ("input", "input"),
                ("output", "output"),
                ("cacheRead", "input_cache_read"),
                ("cacheWrite", "input_cache_write"),
            ] {
                c.model["cost"][key] = json!(round(number(&m["pricing"][source]) * 1_000_000.));
            }
            c
        })
        .collect()
}
