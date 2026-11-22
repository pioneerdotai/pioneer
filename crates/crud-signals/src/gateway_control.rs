//! Wake keys and commit notifications shared by Gateway control producers.

pub const NATIVE_RETENTION_WAKE_KEY: &str = "native-retention";

/// Reasons carried by the existing coalesced control signal. Row identities
/// are advisory and bounded by WorkSignal; UNKNOWN/overflow means a full pass.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GatewayControlWake {
    Turn(String),
    Activity(String),
    Request(String),
    Attempt(String),
    CommandTurn(String),
    Native(String, String),
    Thread(String, String),
    Cli,
    Commands,
    Buffers,
    Timeouts,
    Supervision,
    Resources,
    Actions,
    Skills,
    Ledger,
    Terminalization,
    NativeRetention,
}

impl GatewayControlWake {
    pub fn key(&self) -> String {
        match self {
            Self::Turn(id) => format!("turn:{id}"),
            Self::Activity(id) => format!("activity:{id}"),
            Self::Request(id) => format!("request:{id}"),
            Self::Attempt(id) => format!("attempt:{id}"),
            Self::CommandTurn(id) => format!("command-turn:{id}"),
            Self::Native(runtime, native) => format!(
                "native:{}",
                serde_json::to_string(&(runtime, native)).expect("string pair")
            ),
            Self::Thread(runtime, thread) => format!(
                "thread:{}",
                serde_json::to_string(&(runtime, thread)).expect("string pair")
            ),
            Self::Cli => "cli".into(),
            Self::Commands => "commands".into(),
            Self::Buffers => "buffers".into(),
            Self::Timeouts => "timeouts".into(),
            Self::Supervision => "supervision".into(),
            Self::Resources => "resources".into(),
            Self::Actions => "actions".into(),
            Self::Skills => "skills".into(),
            Self::Ledger => "ledger".into(),
            Self::Terminalization => "terminalization".into(),
            Self::NativeRetention => NATIVE_RETENTION_WAKE_KEY.into(),
        }
    }

    pub fn from_key(key: &str) -> Option<Self> {
        if let Some(id) = key.strip_prefix("activity:") {
            return Some(Self::Activity(id.into()));
        }
        if let Some(id) = key.strip_prefix("turn:") {
            return Some(Self::Turn(id.into()));
        }
        if let Some(id) = key.strip_prefix("request:") {
            return Some(Self::Request(id.into()));
        }
        if let Some(id) = key.strip_prefix("attempt:") {
            return Some(Self::Attempt(id.into()));
        }
        if let Some(id) = key.strip_prefix("command-turn:") {
            return Some(Self::CommandTurn(id.into()));
        }
        if let Some(value) = key.strip_prefix("thread:") {
            let (runtime, thread) = serde_json::from_str(value).ok()?;
            return Some(Self::Thread(runtime, thread));
        }
        if let Some(value) = key.strip_prefix("native:") {
            let (runtime, native) = serde_json::from_str(value).ok()?;
            return Some(Self::Native(runtime, native));
        }
        Some(match key {
            "cli" => Self::Cli,
            "commands" => Self::Commands,
            "buffers" => Self::Buffers,
            "timeouts" => Self::Timeouts,
            "supervision" => Self::Supervision,
            "resources" => Self::Resources,
            "actions" => Self::Actions,
            "skills" => Self::Skills,
            "ledger" => Self::Ledger,
            "terminalization" => Self::Terminalization,
            NATIVE_RETENTION_WAKE_KEY => Self::NativeRetention,
            _ => return None,
        })
    }
}

pub fn control_work_change(
    database: &pioneer_postgres_pools::PostgresDatabase,
    domain: &'static str,
    work: &[GatewayControlWake],
) -> pioneer_postgres_pools::WorkChange {
    let change = database.work_signal(domain).changing();
    if matches!(domain, "gateway.control" | "gateway.cli") {
        change.with_keys(&work.iter().map(GatewayControlWake::key).collect::<Vec<_>>())
    } else if domain == "projection" {
        let turns = work
            .iter()
            .filter_map(|wake| match wake {
                GatewayControlWake::Turn(id)
                | GatewayControlWake::Activity(id)
                | GatewayControlWake::CommandTurn(id) => Some(format!("turn:{id}")),
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if turns.is_empty() {
            change
        } else {
            change.with_keys(&turns)
        }
    } else {
        change
    }
}
