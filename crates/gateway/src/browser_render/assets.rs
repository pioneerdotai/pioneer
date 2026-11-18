use base64::Engine;
use serde_json::{Value, json};

pub(crate) const RENDER_ORIGIN: &str = "http://pioneer-render.invalid";
const ASSET_BUDGET: usize = 64 * 1024 * 1024;

pub(crate) struct AssetSource {
    client: reqwest::Client,
    address: std::net::SocketAddr,
    credential: pioneer_protocol::AuthSecretString,
    entry: String,
    remaining: usize,
    requests: usize,
}

impl AssetSource {
    pub(crate) fn new(
        address: std::net::SocketAddr,
        credential: pioneer_protocol::AuthSecretString,
        entry: String,
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
        let component_ok = |p: &str| {
            !p.is_empty()
                && p != "."
                && p != ".."
                && p.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        };
        if request["method"] != "GET"
            || self.requests >= 256
            || !path.starts_with('/')
            || !path[1..].split('/').all(component_ok)
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
