#[macro_use]
extern crate rust_i18n;

i18n!("locales", fallback = "en");

mod app;
mod assets;
mod audio;
mod code_highlight;
mod components;
mod gateway;
mod menu;
mod settings;
mod state;
#[cfg(test)]
mod tests;
mod theme;
mod updater;
mod window;

use anyhow::Context as _;
use assets::PioneerAssetsSource;
use futures_util::{AsyncReadExt as _, FutureExt as _, future::BoxFuture};
use gpui::http_client::{self, HttpClient};
use gpui::*;
use gpui_component::Root;
use reqwest::header::HeaderValue;
use std::sync::Arc;
use tracing::error;

use pioneer_protocol::{InvitationPresentation, PioneerAppUrlScheme};

use app::PioneerDesktop;

#[derive(Clone)]
struct DesktopHttpClient {
    client: reqwest::blocking::Client,
    user_agent: HeaderValue,
}

impl DesktopHttpClient {
    fn new(user_agent: &str) -> anyhow::Result<Self> {
        let user_agent_header =
            HeaderValue::from_str(user_agent).context("invalid HTTP user-agent")?;

        let client = reqwest::blocking::Client::builder()
            .user_agent(user_agent)
            .build()
            .context("failed to build reqwest client")?;

        Ok(Self {
            client,
            user_agent: user_agent_header,
        })
    }
}

impl HttpClient for DesktopHttpClient {
    fn user_agent(&self) -> Option<&HeaderValue> {
        Some(&self.user_agent)
    }

    fn proxy(&self) -> Option<&http_client::Url> {
        None
    }

    fn send(
        &self,
        req: http_client::Request<http_client::AsyncBody>,
    ) -> BoxFuture<'static, anyhow::Result<http_client::Response<http_client::AsyncBody>>> {
        let client = self.client.clone();
        async move {
            let (parts, mut body) = req.into_parts();
            let mut request = client.request(parts.method, parts.uri.to_string());
            for (name, value) in &parts.headers {
                request = request.header(name, value);
            }

            let mut body_bytes = Vec::new();
            body.read_to_end(&mut body_bytes).await?;
            if !body_bytes.is_empty() {
                request = request.body(body_bytes);
            }

            let response = request.send()?;
            let status = response.status();
            let headers = response.headers().clone();
            let bytes = response.bytes()?;

            let mut builder = http_client::Response::builder().status(status);
            for (name, value) in &headers {
                builder = builder.header(name, value);
            }

            builder
                .body(http_client::AsyncBody::from(bytes.to_vec()))
                .map_err(|error| anyhow::anyhow!("failed to build HTTP response: {error}"))
        }
        .boxed()
    }
}

fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if let Some(version_probe) = version_probe_from_args(args.iter().cloned()) {
        print_version_probe(version_probe);
        return;
    }
    let initial_urls = invitation_urls_from_args(args);

    let sentry_guard =
        pioneer_observability::init_sentry(pioneer_observability::SentryTarget::Desktop);
    pioneer_observability::init_tracing(sentry_guard.is_some());
    init_locale();

    if let Err(error) = gateway::ensure_runtime_home_dir() {
        pioneer_observability::capture_anyhow(&error);
        error!(
            error = %format!("{error:#}"),
            message = %t!("logs.runtime.prepare_home_failed")
        );
        drop(sentry_guard);
        std::process::exit(1);
    }

    let http_client = DesktopHttpClient::new("pioneer-desktop")
        .expect("failed to initialize HTTP client for remote assets");

    let (url_sender, mut url_receiver) = tokio::sync::mpsc::unbounded_channel::<Vec<String>>();
    let app = gpui_platform::application()
        .with_assets(PioneerAssetsSource)
        .with_http_client(Arc::new(http_client));
    app.on_open_urls(move |urls| {
        let _ = url_sender.send(urls);
    });

    app.run(move |cx| {
        gpui_component::init(cx);
        theme::init(cx);
        menu::init_system_menus(cx);

        let initial_window_bounds = window::initial_window_bounds(cx);

        cx.spawn(async move |cx| {
            let window_options = WindowOptions {
                titlebar: Some(gpui_component::TitleBar::title_bar_options()),
                window_bounds: Some(initial_window_bounds),
                ..Default::default()
            };

            let mut desktop = None;
            let window_handle = cx
                .open_window(window_options, |window, cx| {
                    let view = cx.new(|cx| PioneerDesktop::new(window, cx));
                    desktop = Some(view.clone());
                    cx.new(|cx| Root::new(view, window, cx))
                })
                .context(t!("errors.window.open_failed").to_string())?;
            let desktop = desktop.context("desktop view was not created")?;

            for url in initial_urls {
                let desktop = desktop.clone();
                let _ = window_handle.update(cx, move |_, window, cx| {
                    let _ = desktop.update(cx, |view, cx| {
                        view.handle_invitation_url(url, window, cx);
                    });
                });
            }

            while let Some(urls) = url_receiver.recv().await {
                for url in urls {
                    let desktop = desktop.clone();
                    let _ = window_handle.update(cx, move |_, window, cx| {
                        let _ = desktop.update(cx, |view, cx| {
                            view.handle_invitation_url(url, window, cx);
                        });
                    });
                }
            }

            Ok::<_, anyhow::Error>(())
        })
        .detach();
    });
}

fn invitation_urls_from_args(args: impl IntoIterator<Item = String>) -> Vec<String> {
    let expected_scheme = PioneerAppUrlScheme::for_current_build();
    args.into_iter()
        .filter(|candidate| {
            InvitationPresentation::parse(candidate)
                .is_ok_and(|presentation| presentation.app_url_scheme() == expected_scheme)
        })
        .collect()
}

fn init_locale() {
    let locale = settings::resolve_app_locale();
    rust_i18n::set_locale(locale.as_str());
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VersionProbe {
    Plain,
    Json,
}

fn version_probe_from_args(args: impl IntoIterator<Item = String>) -> Option<VersionProbe> {
    let args = args.into_iter().collect::<Vec<_>>();
    match args.as_slice() {
        [flag] if is_version_flag(flag) => Some(VersionProbe::Plain),
        [flag, json] | [json, flag] if is_version_flag(flag) && json == "--json" => {
            Some(VersionProbe::Json)
        }
        _ => None,
    }
}

fn is_version_flag(flag: &str) -> bool {
    flag == "--version" || flag == "-V"
}

fn print_version_probe(probe: VersionProbe) {
    match probe {
        VersionProbe::Plain => println!("{}", env!("CARGO_PKG_VERSION")),
        VersionProbe::Json => println!(
            "{}",
            serde_json::json!({
                "schema_version": 1,
                "product": "pioneer-desktop",
                "binary": "pioneer-app",
                "version": env!("CARGO_PKG_VERSION"),
            })
        ),
    }
}
