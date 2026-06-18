//! Shared Sentry and tracing setup for Pioneer runtime binaries.
//!
//! `PIONEER_SENTRY_DSN` is used by non-desktop binaries. `PIONEER_DESKTOP_SENTRY_DSN`
//! is used by the desktop app. Runtime environment variables take precedence over
//! build-time values with the same names.

use std::borrow::Cow;

use sentry::integrations::tracing::EventFilter;
use sentry::{ClientInitGuard, ClientOptions};
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// DSN for non-desktop runtime binaries.
pub const SENTRY_DSN_ENV: &str = "PIONEER_SENTRY_DSN";
/// DSN for the desktop app.
pub const DESKTOP_SENTRY_DSN_ENV: &str = "PIONEER_DESKTOP_SENTRY_DSN";
/// Optional Sentry environment value shared by all targets.
pub const SENTRY_ENVIRONMENT_ENV: &str = "PIONEER_SENTRY_ENVIRONMENT";

const BUILD_SENTRY_DSN: Option<&str> = option_env!("PIONEER_SENTRY_DSN");
const BUILD_DESKTOP_SENTRY_DSN: Option<&str> = option_env!("PIONEER_DESKTOP_SENTRY_DSN");

#[derive(Clone, Copy, Debug)]
pub enum SentryTarget {
    Shared,
    Desktop,
}

impl SentryTarget {
    fn dsn_env(self) -> &'static str {
        match self {
            Self::Shared => SENTRY_DSN_ENV,
            Self::Desktop => DESKTOP_SENTRY_DSN_ENV,
        }
    }

    fn build_dsn(self) -> Option<&'static str> {
        match self {
            Self::Shared => BUILD_SENTRY_DSN,
            Self::Desktop => BUILD_DESKTOP_SENTRY_DSN,
        }
    }

    fn tag(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::Desktop => "desktop",
        }
    }
}

#[must_use]
pub fn init_sentry(target: SentryTarget) -> Option<ClientInitGuard> {
    let dsn = configured_value(target.dsn_env(), target.build_dsn())?;
    let dsn = dsn.parse().ok()?;
    let environment = configured_value(SENTRY_ENVIRONMENT_ENV, None).map(Cow::Owned);

    let guard = sentry::init(ClientOptions {
        dsn: Some(dsn),
        release: Some(Cow::Owned(format!("pioneer@{}", env!("CARGO_PKG_VERSION")))),
        environment,
        ..Default::default()
    });

    sentry::configure_scope(|scope| {
        scope.set_tag("pioneer.target", target.tag());
    });

    Some(guard)
}

pub fn init_tracing(sentry_enabled: bool) {
    let filter = Targets::new()
        .with_default(LevelFilter::INFO)
        .with_target("rmcp::service", LevelFilter::WARN);

    let subscriber = tracing_subscriber::fmt()
        .with_target(false)
        .without_time()
        .with_max_level(LevelFilter::TRACE)
        .finish()
        .with(filter);

    if sentry_enabled {
        let _ = subscriber.with(sentry_tracing_layer()).try_init();
    } else {
        let _ = subscriber.try_init();
    }
}

pub fn capture_anyhow(error: &anyhow::Error) {
    sentry::integrations::anyhow::capture_anyhow(error);
}

fn sentry_tracing_layer<S>() -> impl tracing_subscriber::Layer<S>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    sentry::integrations::tracing::layer().event_filter(|metadata| match *metadata.level() {
        tracing::Level::ERROR => EventFilter::Event,
        tracing::Level::TRACE => EventFilter::Ignore,
        _ => EventFilter::Breadcrumb,
    })
}

fn configured_value(env_name: &str, build_value: Option<&str>) -> Option<String> {
    std::env::var(env_name)
        .ok()
        .as_deref()
        .and_then(non_empty)
        .map(str::to_owned)
        .or_else(|| build_value.and_then(non_empty).map(str::to_owned))
}

fn non_empty(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}
