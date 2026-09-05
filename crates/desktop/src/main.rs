#[macro_use]
extern crate rust_i18n;

i18n!("locales", fallback = "en");

mod app;
mod assets;
mod audio;
mod code_highlight;
mod components;
mod file_opener;
mod gateway;
mod menu;
mod qualification_diagnostics;
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
use gpui_kit::component::Root;
use gpui_kit::http_client::{self, HttpClient};
use gpui_kit::*;
use reqwest::header::HeaderValue;
use std::sync::Arc;
use std::time::Duration;
use tracing::error;

use pioneer_config::AppConfig;
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

    pioneer_observability::set_telemetry_enabled(false);
    let startup = pioneer_observability::DesktopStartupTrace::start();

    let config_stage = startup.stage(pioneer_observability::DesktopStartupStage::ConfigLoad);
    let startup_config = AppConfig::load().ok();
    if startup_config.is_some() {
        config_stage.succeed();
    }

    if let Some(config) = startup_config.as_ref() {
        match config.runtime_home_dir().and_then(|runtime_home| {
            updater::relaunch::claim_post_update_receipt(
                runtime_home.as_path(),
                updater::desktop_current_version(),
            )
            .map_err(Into::into)
        }) {
            Ok(Some(receipt)) => {
                startup.set_post_update_context(pioneer_observability::DesktopPostUpdateContext {
                    attempt_id: receipt.attempt_id,
                    from_version: receipt.from_version,
                    to_version: receipt.to_version,
                    platform: receipt.platform,
                    process_exit_wait: receipt.process_exit_wait,
                    apply_duration: receipt.apply_duration,
                    relaunch_duration: receipt.relaunch_duration,
                    total_duration: receipt.total_duration,
                    claimed_at: receipt.claimed_at,
                });
            }
            Ok(None) => {}
            Err(error) => eprintln!("failed to claim desktop update receipt: {error:#}"),
        }
    }

    let consent_stage = startup.stage(pioneer_observability::DesktopStartupStage::ConsentLoad);
    let telemetry_enabled = startup_config
        .as_ref()
        .map(desktop_telemetry_enabled)
        .unwrap_or(true);
    pioneer_observability::set_telemetry_enabled(telemetry_enabled);
    startup.bind_consent();
    consent_stage.succeed();

    if let Some(config) = startup_config.as_ref() {
        let observability_stage =
            startup.stage(pioneer_observability::DesktopStartupStage::ObservabilityInit);
        if pioneer_observability::init_otlp_observability_for(
            pioneer_observability::TelemetryTarget::Desktop,
            pioneer_observability::OtlpTelemetryConfig {
                metrics_endpoint: config.gateway.telemetry.otlp_metrics_endpoint.clone(),
                traces_endpoint: config.gateway.telemetry.otlp_traces_endpoint.clone(),
                export_interval: Duration::from_millis(config.gateway.telemetry.export_interval_ms),
                export_timeout: Duration::from_millis(config.gateway.telemetry.export_timeout_ms),
                deployment_environment: None,
                service_version: None,
            },
        )
        .is_ok()
        {
            observability_stage.succeed();
            startup.emit_post_update_handoff();
            startup.schedule_post_update_stall_checkpoint(Duration::from_secs(30));
        }
    }

    let sentry_guard =
        pioneer_observability::init_sentry(pioneer_observability::SentryTarget::Desktop);
    pioneer_observability::init_tracing(sentry_guard.is_some());
    let locale_stage = startup.stage(pioneer_observability::DesktopStartupStage::LocaleInitialize);
    init_locale();
    locale_stage.succeed();

    let runtime_home_stage =
        startup.stage(pioneer_observability::DesktopStartupStage::RuntimeHomePrepare);
    if let Err(error) = gateway::ensure_runtime_home_dir() {
        pioneer_observability::capture_anyhow(&error);
        error!(
            error = %format!("{error:#}"),
            message = %t!("logs.runtime.prepare_home_failed")
        );
        drop(sentry_guard);
        std::process::exit(1);
    }
    runtime_home_stage.succeed();

    let http_client_stage =
        startup.stage(pioneer_observability::DesktopStartupStage::HttpClientInitialize);
    let http_client = DesktopHttpClient::new("pioneer-desktop")
        .expect("failed to initialize HTTP client for remote assets");
    http_client_stage.succeed();

    let (url_sender, mut url_receiver) = tokio::sync::mpsc::unbounded_channel::<Vec<String>>();
    let ui_runtime_stage =
        startup.stage(pioneer_observability::DesktopStartupStage::UiRuntimeInitialize);
    let app = gpui_kit::application()
        .with_assets(PioneerAssetsSource)
        .with_http_client(Arc::new(http_client));
    ui_runtime_stage.succeed();
    app.on_open_urls(move |urls| {
        let _ = url_sender.send(urls);
    });

    let ui_event_loop_stage =
        startup.stage(pioneer_observability::DesktopStartupStage::UiEventLoopEnter);
    let startup_for_app = startup.clone();
    app.run(move |cx| {
        ui_event_loop_stage.succeed();
        let ui_components_stage = startup_for_app
            .stage(pioneer_observability::DesktopStartupStage::UiComponentsInitialize);
        gpui_kit::init(cx);
        theme::init(cx);
        menu::init_system_menus(cx);

        let initial_window_bounds = window::initial_window_bounds(cx);
        ui_components_stage.succeed();

        let startup = startup_for_app.clone();
        cx.spawn(async move |cx| {
            let window_stage =
                startup.stage(pioneer_observability::DesktopStartupStage::WindowOpen);
            let window_options = WindowOptions {
                titlebar: Some(gpui_kit::component::TitleBar::title_bar_options()),
                window_bounds: Some(initial_window_bounds),
                ..Default::default()
            };

            let mut desktop = None;
            let window_handle = cx
                .open_window(window_options, |window, cx| {
                    let view = cx.new(|cx| PioneerDesktop::new(window, cx, startup.clone()));
                    desktop = Some(view.clone());
                    cx.new(|cx| Root::new(view, window, cx))
                })
                .context(t!("errors.window.open_failed").to_string())?;
            window_stage.succeed();
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

    let _ = pioneer_observability::shutdown_observability(Duration::from_secs(3));
}

fn desktop_telemetry_enabled(config: &AppConfig) -> bool {
    let Ok(runtime_home) = config.runtime_home_dir() else {
        return config.gateway.telemetry.enabled;
    };
    let settings_path = runtime_home.join(config.gateway.settings_file_name.as_str());
    let Ok(contents) = std::fs::read_to_string(settings_path) else {
        return config.gateway.telemetry.enabled;
    };
    let Ok(settings) = contents.parse::<toml::Value>() else {
        return config.gateway.telemetry.enabled;
    };
    settings
        .get("general")
        .and_then(|general| general.get("telemetry_enabled"))
        .and_then(toml::Value::as_bool)
        .unwrap_or(config.gateway.telemetry.enabled)
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
