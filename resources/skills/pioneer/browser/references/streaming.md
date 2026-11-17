# Live Streaming

<!-- Modified for Pioneer. -->

All shell examples use the task session selected in [SKILL.md](../SKILL.md).

Stream a session's viewport over WebSocket and drive it with remote input. This is what a remote preview or embedded dashboard connects to: the browser runs wherever the daemon runs (a sandbox, a container, a CI box), and the client renders frames and sends clicks back.

**Related**: [commands.md](commands.md) for full command reference, [SKILL.md](../SKILL.md) for quick start.

## Contents

- [Enabling the stream](#enabling-the-stream)
- [Connecting](#connecting)
- [Messages from the server](#messages-from-the-server)
- [Messages from the client](#messages-from-the-client)
- [Frame rate and staleness](#frame-rate-and-staleness)
- [Limitations](#limitations)

## Enabling the stream

Streaming is always available; the server binds an OS-assigned localhost port unless told otherwise.

```bash
agent-browser stream status --json     # Report enabled state, port, client count
agent-browser stream enable            # Create the server (--port to pin one)
agent-browser stream disable           # Tear it down
```

`AGENT_BROWSER_STREAM_PORT` pins the port for the whole daemon instead of passing `--port`.

Frame encoding is daemon-wide, read once at startup:

| Variable | Default | Notes |
|---|---|---|
| `AGENT_BROWSER_STREAM_QUALITY` | `80` | 0 to 100, clamped |
| `AGENT_BROWSER_STREAM_MAX_WIDTH` | the viewport | caps the frame, does not resize the page |
| `AGENT_BROWSER_STREAM_MAX_HEIGHT` | the viewport | same |

The live stream requests jpeg, since a `frame` message carries no format field. An explicit `screencast_start` reconfigures the same underlying screencast, so a client can still see the format change mid-stream; sniff the bytes rather than assuming. Upstream's illustrative measurements on a busy page at 1280x720 were ~54 KB per frame at quality 80, ~25 KB at quality 20, and ~9 KB at quality 20 with 640x360 frames. These are not measurements of the current host. An unusable value leaves the default.

Read the port from `stream status --json` rather than assuming one; the OS-assigned default changes per daemon.

## Connecting

Connect a WebSocket client to `ws://127.0.0.1:<port>`. Frame delivery starts automatically once a client attaches, so there is no subscribe message. Browser clients must load from `localhost`, `127.0.0.1`, `::1` or `file://`. Any other origin gets a 403 on the upgrade and needs a proxy.

## Messages from the server

Every message is JSON text with a `type` field.

- `frame`: a viewport image plus its metadata. Delivered latest-first (see below).

```json
{
  "type": "frame",
  "seq": 41,
  "data": "<base64-encoded-jpeg>",
  "metadata": {
    "deviceWidth": 1280, "deviceHeight": 720, "pageScaleFactor": 1,
    "offsetTop": 0, "scrollOffsetX": 0, "scrollOffsetY": 0,
    "timestamp": 1785038682238
  }
}
```

`seq` is a monotonic frame id, echoed back under ack pacing and stable across browser relaunches. `metadata.timestamp` is the capture time in epoch milliseconds, so `Date.now() - timestamp` is the age of the frame being drawn. The other message types:

- `status`: connection state, screencasting flag, viewport size, engine, recording flag. Sent once on connect and again on change.
- `tabs`: the current tab list, sent on connect when tabs are known and on change.
- `url`: on Chrome, full-document, History API, and fragment navigation in the active tab's main frame. Child-frame and background-tab navigation is ignored.
- `console`: console events.

Status, tabs, url, and console travel on an ordered channel: they are delivered in order and are never replaced by a newer message the way frames are. They are not unconditionally durable. A client that falls far enough behind can lag out of that channel and lose messages it never saw, so treat console output as a live feed, not an audit log.

## Messages from the client

```json
{"type": "input_mouse", "eventType": "mousePressed", "x": 40, "y": 40, "button": "left", "clickCount": 1}
{"type": "input_keyboard", "eventType": "keyDown", "key": "a", "text": "a"}
{"type": "input_touch", "eventType": "touchStart", "touchPoints": []}
{"type": "config", "maxFps": 10}
{"type": "config", "pacing": "ack"}
{"type": "ack", "seq": 41}
```

Input dispatches to the browser on a task of its own, separate from frame delivery, so a click is not queued behind a frame write. Events are sent to the browser without waiting for its reply, so a click stays responsive behind a burst of mouse moves. Ordering is preserved: press never overtakes move. Mouse, keyboard, and touch input also reset the daemon idle timer, so an actively driven preview is not shut down by the idle timeout.

`config` sets a per-client frame cap: 1 to 120, or `0` for uncapped (the default). It takes effect immediately, including when it loosens the cap. Each client's cap is its own; other connected clients are unaffected. A value above 120 is clamped to 120; a negative or non-numeric value is ignored, leaving the current cap in place. Neither rejects the connection.

Both settings can also be declared on the URL, which is the only way to have them cover the connection's opening frame: `ws://127.0.0.1:<port>/?pacing=ack&maxFps=10`. A `config` message sent after connecting still wins.

## Frame rate and staleness

The server holds only the newest frame per client and reads it at send time. A frame produced while an earlier one is still being written is skipped, not queued, so the application never builds a backlog.

Push pacing (the default) stops there, and the transport underneath is still ordered: frames already accepted by the socket are delivered in order, so a client that stalls drains whatever the kernel buffered before the writer blocked.

Ack pacing closes that gap. Send `{"type":"config","pacing":"ack"}` and the server keeps at most one frame in flight, waiting for `{"type":"ack","seq":N}` before sending the next. Every frame carries a monotonic `seq`; echo the one you finished rendering. Frames produced while an ack is outstanding replace each other and never reach the socket, so a client that stalls for ten seconds and resumes gets the current page, not ten seconds of history.

Under ack pacing one frame is in flight at a time, so the rate is one frame per transfer plus one acknowledgement round trip. Both the link's bandwidth and its latency bound it, and a link whose bandwidth-delay product exceeds a single frame goes underused. Ack pacing bounds one hop. With a proxy in the path, forward the renderer's acks; acks generated on receipt leave frames queued on the far side. Acks are cumulative, so acknowledging a newer id covers any older one. A client that opts in and then stops acking simply stops receiving frames; status, tabs, url, and console keep flowing.

The two settings compose: `pacing` bounds how much is in flight, `maxFps` bounds the rate. A constrained preview usually wants both.

## Limitations

- Localhost only. Exposing the stream beyond the machine is the embedder's job (tunnel, proxy, or port forward), and the origin allowlist applies to browser clients.
- Frames are images, not a video codec. Bandwidth scales with viewport size and page activity; cap the rate for constrained links.
- In push pacing the server cannot tell a slow renderer from a fast one beyond transport backpressure. Use ack pacing when that distinction matters.

## Observability dashboard

The dashboard is a separate local process, normally on port 4848, and survives
individual browser sessions. Start it when the user needs a live dashboard or
when that observation helps the task:

```bash
agent-browser dashboard start
agent-browser dashboard start --port 8080
agent-browser dashboard stop
```

These are alternative configurations, not a sequence to run on every task.
Do not stop a dashboard shared with other work as routine browser cleanup.
Local loopback origins over IPv4/IPv6 work directly without an access token.
Read the returned URL instead of assuming its configured port.

For an intentionally configured HTTPS reverse proxy or forwarded origin:

```bash
agent-browser dashboard start --allowed-origins https://dashboard.example.com
```

`AGENT_BROWSER_DASHBOARD_ALLOWED_ORIGINS` is the environment alternative.
Allowed origins must be exact valid HTTPS origins, optionally comma-separated;
custom ports must be 1–65535. Invalid options/ports/origins fail before startup.
Changing the allowed origins or port requires stopping/restarting that dashboard;
coordinate this if it is shared.

External access uses a private tokenized URL. Open it once to establish the
Secure host-bound same-site cookie; keep the URL and cookie out of published
reports and logs. The token starts in the URL fragment. A reverse proxy should
redact cookies from its logs. Requests without valid browser provenance are
rejected. Local `http://localhost:<port>` needs no token.

Stay on the dashboard origin: session/tab/stream traffic is proxied internally;
individual session ports do not need exposure. Exposing a dashboard or raw
remote-input stream is a separate network configuration action, not implied by
a request for an ordinary screenshot. Pioneer inline HTML does not itself grant
access to a host's localhost ports. Use only the runtime's admitted capabilities.
