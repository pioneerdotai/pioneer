# Execution Host API

The backend-only `/apps/host` endpoint uses JSON-RPC 2.0 over WebSocket. Each
connection authenticates its execution credential and validates current durable
App/workspace/execution state once before upgrade. Running requests and outgoing
PTY data consult the execution cancellation token under the owned resource lock;
they do not issue database queries. `apps.lifecycle` reconciliation, the runtime's
HostLease cleanup and committed workspace invalidation already own revocation.
Package publication fences disabled/non-installed Apps before notification I/O,
and stop fences admission before waiting on the runtime owner.
They remain the authority-changing paths, including cooperative database changes
observed by the existing lifecycle work signal. This is execution authority, not
a timed authorization cache. There is no new periodic database discovery.

There is no shared pacing timer between commands and output. The connection
handles one command or bounded output frame per turn, with fair selection between
ready socket input and output and round robin between PTYs. Slow socket sends
have cancellation and a finite deadline. Existing message-size, connection,
session, command/input queue and replay-ring limits remain bounded.

## Host shell and UTF-8

`host.pty.create` accepts an optional `command` argv. Omitting it asks
`portable-pty` for the Gateway account's login shell (`$SHELL`, then the account
record, with the library's final `/bin/sh` fallback). Default cwd is that
account's home; an explicit cwd still takes precedence. Terminal omits command
instead of hard-coding `/bin/sh -l`. Existing explicit-command clients remain
supported. User profile files are not edited or suppressed.

The child receives a small environment allowlist, including LANG, LANGUAGE and
standard LC_* settings, rather than Gateway credentials. An available UTF-8
LC_ALL is retained. Otherwise LC_ALL is removed if it would override UTF-8;
valid UTF-8 LANG/LC_CTYPE are preserved and missing/non-UTF-8 values are replaced
with an installed UTF-8 locale. Candidates are checked using private `newlocale`
objects: no process-global setlocale, locale enumeration subprocess or per-byte
probe is used. A host with no suitable locale fails explicitly. macOS/Linux PTYs
also enable IUTF8 for canonical multibyte erase without changing echo/raw mode.
Windows retains its existing unsupported host-PTY boundary; no Unix locale or
termios calls are compiled into that backend.

Terminal keeps at most one resize and one lease heartbeat in flight per PTY;
resize bursts retain only the latest requested geometry. Output subscriptions
are created once per actual WebSocket and ACKs are bound to that socket and
stream ID. Closing an old socket settles only its own RPCs/timers. Reconnect
cannot leave their requests occupying capacity or reject replacement-socket
requests. The 128-request hard limit remains unchanged.

## PTY output streams

- `host.pty.subscribe {session_id, stream_id, after}` installs a connection-local
  subscription after validating its absolute replay cursor. A new stream ID fences
  superseded frames. At most eight session subscriptions exist per connection.
- `host.pty.output` carries `session_id`, `stream_id`, `after`, `bytes`, `next`,
  `end`, `exit_status`, `failed` (or an `error` code). Frames contain at most
  16 KiB of bytes. Process exit is published only after the output reader reaches
  EOF; clients finish when their cursor also reaches `end`.
- `host.pty.ack {session_id, stream_id, after}` confirms a cumulative delivered
  prefix. It cannot advance beyond bytes sent; duplicate ACKs cannot add credit.
  ACKs for a superseded stream cannot credit its replacement.
- Each stream permits at most 64 KiB / 64 data frames outstanding. A full window
  waits for ACK rather than polling or adding pending frames. The PTY's existing
  256 KiB replay ring remains the source; a cursor overwritten during a stall
  fails explicitly rather than skipping bytes.
- `host.pty.keepalive {session_id}` retains an idle session's existing five-minute
  lease without reading output or durable state. ACK/input/output activity also
  touches the session. The Terminal client uses a ten-second lease heartbeat.
- Existing `host.pty.read` and `host.pty.changed` remain for installed clients that
  have not subscribed. Streaming connections do not pay that extra exchange.

## Ordered input window

Terminal submits at most eight writes concurrently, each assigned its sequence
before sending. Gateway enqueues them in order and retains the last 64 accepted
sequence/byte identities, bounded to 128 KiB (enough for the eight maximum-sized
outstanding writes). A reconnect retry with the same sequence and bytes confirms
that write without enqueueing it again, even after later sequences were accepted.
Conflicts or identities outside retained history fail closed. Browser ACKs still
advance only through the contiguous accepted prefix; an ambiguous failure closes
the session without a success ACK or blind replay. Submission ordering is not a
promise that the shell has already executed the input.

## Platform and validation boundary

The WebSocket, cancellation, flow control, input history and TCP_NODELAY paths are
shared across macOS, Linux and Windows. PTY creation retains its existing Unix-only
support; this change does not introduce a Windows host PTY implementation.

Source inspection and syntax/whitespace checks only. Builds, tests, packaging,
runtime checks and comparison with browser dev latency have not been run for this
change. Source edits do not replace an installed Terminal payload. Gateway and the
updated Terminal must be rebuilt for the new streaming/input-window path to run.
