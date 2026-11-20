use base64::Engine;
use serde_json::{Value, json};

pub(crate) const RENDER_ORIGIN: &str = "http://pioneer-render.invalid";
const ASSET_BUDGET: usize = 64 * 1024 * 1024;

pub(crate) struct AssetSource {
    client: reqwest::Client,
    address: std::net::SocketAddr,
    credential: pioneer_protocol::AuthSecretString,
    entry: String,
    development: bool,
    remaining: usize,
    requests: usize,
}

impl AssetSource {
    pub(crate) fn new(
        address: std::net::SocketAddr,
        credential: pioneer_protocol::AuthSecretString,
        entry: String,
        development: bool,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(std::time::Duration::from_secs(10))
                .build()?,
            address,
            credential,
            entry,
            development,
            remaining: ASSET_BUDGET,
            requests: 0,
        })
    }

    pub(super) async fn reply(&mut self, id: &str, event: &Value) -> (&'static str, Value) {
        match self.load(event).await {
            Some((headers, data)) => (
                "Fetch.fulfillRequest",
                json!({"requestId":id,"responseCode":200,"responseHeaders":headers,
                    "body":base64::engine::general_purpose::STANDARD.encode(data)}),
            ),
            None => (
                "Fetch.failRequest",
                json!({"requestId":id,"errorReason":"BlockedByClient"}),
            ),
        }
    }

    async fn load(&mut self, event: &Value) -> Option<(Vec<Value>, Vec<u8>)> {
        let request = &event["request"];
        let path = request["url"].as_str()?.strip_prefix(RENDER_ORIGIN)?;
        if request["method"] != "GET" || self.requests >= 256 {
            return None;
        }
        let resource = event["resourceType"].as_str().unwrap_or_default();
        if self.development {
            if !dev_render_path(path, &self.entry, resource) {
                return None;
            }
        } else if !path.starts_with('/')
            || !path[1..]
                .split('/')
                .all(|part| valid_component(part, b"-_."))
            || (path != self.entry && !path.starts_with("/assets/"))
        {
            return None;
        }
        self.requests += 1;
        let mut response = self
            .client
            .get(format!("http://{}{path}", self.address))
            .header(
                "pioneer-app-execution-credential",
                self.credential.expose_secret(),
            )
            .send()
            .await
            .ok()?;
        if response.status() != reqwest::StatusCode::OK {
            return None;
        }
        let content_type = response
            .headers()
            .get("content-type")?
            .to_str()
            .ok()?
            .to_owned();
        if self.development && path != self.entry && !dev_render_mime(resource, &content_type) {
            return None;
        }
        let mut headers = vec![json!({"name":"content-type","value":content_type})];
        if path == self.entry {
            headers.push(json!({"name":"content-security-policy","value":
                "default-src 'self' data: blob:; connect-src 'none'; frame-src 'none'; object-src 'none'; form-action 'none'; base-uri 'none'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'"}));
        }
        let mut data = Vec::new();
        while let Some(chunk) = response.chunk().await.ok()? {
            self.remaining = self.remaining.checked_sub(chunk.len())?;
            data.extend_from_slice(&chunk);
        }
        Some((headers, data))
    }
}

fn valid_component(part: &str, extra: &[u8]) -> bool {
    !part.is_empty()
        && !matches!(part, "." | "..")
        && part
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || extra.contains(&byte))
}

fn dev_render_path(path: &str, entry: &str, resource: &str) -> bool {
    if path == entry {
        return resource == "Document";
    }
    if !matches!(
        resource,
        "Script" | "Stylesheet" | "Image" | "Font" | "Media"
    ) || path.len() > 4096
    {
        return false;
    }
    let pathname = path.split('?').next().unwrap_or_default();
    let Some(relative) = pathname.strip_prefix('/') else {
        return false;
    };
    relative.split('/').all(|part| {
        valid_component(part, b"-_.@:+~")
            && (!part.starts_with('.') || part == ".vite")
            && !part.eq_ignore_ascii_case("credential.json")
            && part != "@fs"
    }) && !matches!(relative.split('/').next(), Some("api" | "collaboration"))
}

fn dev_render_mime(resource: &str, value: &str) -> bool {
    let mime = value.split(';').next().unwrap_or_default().trim();
    match resource {
        "Script" => matches!(
            mime,
            "text/javascript"
                | "application/javascript"
                | "application/ecmascript"
                | "text/ecmascript"
        ),
        "Stylesheet" => mime == "text/css",
        "Image" => mime.starts_with("image/"),
        "Font" => mime.starts_with("font/") || mime.starts_with("application/font-"),
        "Media" => mime.starts_with("audio/") || mime.starts_with("video/"),
        _ => false,
    }
}
