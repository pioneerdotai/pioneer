//! View-scoped presentation requests; never a Gateway or general native RPC bridge.
use super::AppPlacement;
use tokio::sync::mpsc::{Receiver, channel};

pub(super) struct Request {
    id: String,
    command: Command,
}
#[derive(Clone, Copy)]
pub(super) enum Command {
    SetPlacement(AppPlacement),
    Close,
}
impl Request {
    pub(super) fn command(&self) -> Command {
        self.command
    }
    pub(super) fn response(&self, development: bool) -> Option<String> {
        let Command::SetPlacement(placement) = self.command else {
            return None;
        };
        let placement = match placement {
            AppPlacement::Bottom => "bottom",
            AppPlacement::Right => "right",
            AppPlacement::Expanded => "fullscreen",
        };
        let message =
            serde_json::json!({"type":"pioneer.view.result", "id":self.id, "placement":placement});
        Some(if development {
            format!("window.postMessage({message}, location.origin)")
        } else {
            format!("document.querySelector('iframe')?.contentWindow.postMessage({message}, '*')")
        })
    }
}

pub(super) fn install<'a>(
    builder: wry::WebViewBuilder<'a>,
    development: bool,
    url: &str,
) -> anyhow::Result<(wry::WebViewBuilder<'a>, Receiver<Request>)> {
    let token = format!("{:032x}", rand::random::<u128>());
    let origin = if development {
        serde_json::Value::String(url::Url::parse(url)?.origin().ascii_serialization())
    } else {
        serde_json::Value::Null
    };
    let script = include_str!("view_bridge.js")
        .replace("__PIONEER_ORIGIN__", &origin.to_string())
        .replace("__PIONEER_TOKEN__", &serde_json::to_string(&token)?);
    let (sender, receiver) = channel(32);
    let builder = builder
        .with_initialization_script(script)
        .with_ipc_handler(move |request| {
            if let Some(request) = parse(request.body(), &token) {
                // The native callback never blocks or mutates GPUI reentrantly.
                let _ = sender.try_send(request);
            }
        });
    Ok((builder, receiver))
}

fn parse(body: &str, token: &str) -> Option<Request> {
    if body.len() > 1024 {
        return None;
    }
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    let value = value.as_object()?;
    if value.get("token")?.as_str()? != token {
        return None;
    }
    let id = value.get("id")?.as_str()?;
    if id.is_empty() || id.len() > 64 || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return None;
    }
    let command = match value.get("type")?.as_str()? {
        "pioneer.view.close" if value.len() == 3 => Command::Close,
        "pioneer.view.setPlacement" if value.len() == 4 => {
            Command::SetPlacement(match value.get("placement")?.as_str()? {
                "bottom" => AppPlacement::Bottom,
                "right" => AppPlacement::Right,
                "fullscreen" => AppPlacement::Expanded,
                _ => return None,
            })
        }
        _ => return None,
    };
    Some(Request {
        id: id.to_owned(),
        command,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_requests_require_the_current_view_secret_and_only_allow_view_controls() {
        let valid = serde_json::json!({
            "type": "pioneer.view.setPlacement", "token": "current-view",
            "id": "request-1", "placement": "right"
        });
        let request = parse(&valid.to_string(), "current-view").unwrap();
        assert!(matches!(
            request.command(),
            Command::SetPlacement(AppPlacement::Right)
        ));
        assert!(parse(&valid.to_string(), "different-view").is_none());
        for (field, value) in [
            ("type", "host.pty.create"),
            ("token", "expired-view"),
            ("placement", "close"),
            ("id", "request');evil()"),
        ] {
            let mut invalid = valid.clone();
            invalid[field] = value.into();
            assert!(parse(&invalid.to_string(), "current-view").is_none());
        }
        let mut extra = valid.clone();
        extra["command"] = "shell".into();
        assert!(parse(&extra.to_string(), "current-view").is_none());
        assert!(parse(&"x".repeat(1025), "current-view").is_none());
        for (placement, expected) in [
            ("fullscreen", AppPlacement::Expanded),
            ("bottom", AppPlacement::Bottom),
        ] {
            let mut value = valid.clone();
            value["placement"] = placement.into();
            let request = parse(&value.to_string(), "current-view").unwrap();
            assert!(matches!(request.command(), Command::SetPlacement(p) if p == expected));
        }
        let mut close = serde_json::json!({
            "type": "pioneer.view.close", "token": "current-view", "id": "close-1"
        });
        let request = parse(&close.to_string(), "current-view").unwrap();
        assert!(matches!(request.command(), Command::Close));
        assert!(request.response(false).is_none());
        assert!(parse(&close.to_string(), "different-view").is_none());
        close["placement"] = "right".into();
        assert!(parse(&close.to_string(), "current-view").is_none());
    }
}
