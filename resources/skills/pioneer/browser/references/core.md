# Common browser workflows

<!-- Modified for Pioneer. -->
Use the task session selected in [SKILL.md](../SKILL.md) for every command.
The examples below assume `AGENT_BROWSER_SESSION` is set in this execution.

## Contents

- [Read and extract](#read-and-extract)
- [Wait for the result](#wait-for-the-result)
- [Forms and custom controls](#forms-and-custom-controls)
- [Tabs, frames and dialogs](#tabs-frames-and-dialogs)
- [Network inspection and mocks](#network-inspection-and-mocks)
- [Screenshots and recording](#screenshots-and-recording)
- [Accessibility, React and Web Vitals](#accessibility-react-and-web-vitals)
- [Page WebMCP tools](#page-webmcp-tools)

## Read and extract

```bash
agent-browser read https://docs.example.com/guide
agent-browser read https://docs.example.com/guide --filter auth
agent-browser read https://docs.example.com/guide --outline
agent-browser read https://docs.example.com --llms index --filter auth
agent-browser read
```

An explicit URL performs an HTTP read without launching Chrome. It requests
Markdown, tries a `.md` variant and matching ancestor `llms.txt` entries, then
falls back to readable HTML text. Use `--llms full` only when the full corpus is
needed. `--require-md` checks Markdown negotiation; `--raw` keeps the response
unchanged; `--json` includes source/content-type metadata.

Omitting the URL reads the active tab's rendered DOM, including its browser
authentication and client updates. With `--llms` or `--require-md`, it instead
uses that tab's URL for HTTP resources. Choose the rendered path when a page
requires login or UI state; a successful HTTP fetch need not represent that UI.

```bash
agent-browser snapshot
agent-browser snapshot -i -u
agent-browser snapshot -i -c -d 3
agent-browser snapshot -s "#main"
agent-browser snapshot -i --delta
agent-browser snapshot -i --delta --full
agent-browser get text @e1
agent-browser get attr @e2 href
agent-browser get value @e3
agent-browser get count ".item"
```

Interactive snapshots omit ordinary text. Use a full/scoped tree or `read` for
articles, labels and surrounding context. Delta snapshots require their prior
baseline; use `--full` after losing it. See [snapshot refs](snapshot-refs.md).

For a custom extraction, use a quoted heredoc so the shell does not evaluate
JavaScript backticks, dollar substitutions or nested quotes:

```bash
agent-browser eval --stdin <<'JS'
JSON.stringify(Array.from(document.querySelectorAll('main h2')).map(el => ({
  title: el.textContent.trim(),
  id: el.id,
})))
JS
```

Prefer targeted fields over full DOM dumps. For paginated or virtualized lists,
visit the relevant pages/scroll positions and record coverage; a full-page
screenshot or one snapshot does not load every record.

## Wait for the result

```bash
agent-browser wait "[data-status='complete']"
agent-browser wait --text "Saved"
agent-browser wait --url "**/dashboard"
agent-browser wait --fn "window.appReady === true"
agent-browser wait --load domcontentloaded
agent-browser wait --load load
```

Choose a condition the real application exposes. For repeated actions, make it
specific to this result (new query, selected record or changed value), so stale
text cannot satisfy the wait. A prior-page ref may not exist in the next page;
use expected text, a selector or URL, then snapshot again.

Use a load-event wait when that event is the milestone. `networkidle` only fits
pages known to become quiet: SSE, WebSockets and polling can keep it pending
after the UI is ready. Fixed waits are useful for deliberate video pacing, not
as proof of completion. Defaults and `--timeout` units follow installed help.

## Forms and custom controls

```bash
agent-browser snapshot -i
agent-browser find label "Name" fill "Example User"
agent-browser find label "Email" fill "user@example.com"
agent-browser find role button click --name "Save"
agent-browser wait --text "Profile saved"
agent-browser snapshot -i
```

Use actual refs or semantic names observed on the page. Other useful locators
include `find placeholder`, `find testid`, `find text ... --exact`, and
`find first/last/nth` for a known selector. Raw CSS is a fallback when refs or
semantic locators do not resolve the control.

`fill` clears existing text; `type` appends. For custom editors, focus the
observed element, then try `keyboard inserttext "text"` (without key events) or
`keyboard type "text"` (keystrokes). For a covered click, interact with the
covering banner/modal and take a fresh snapshot rather than repeating blindly.

Checkboxes use `check`/`uncheck`, dropdowns use `select` (one or several values),
and file inputs use `upload`. Keep login out of generic form examples; use
[authentication](authentication.md) for vault/file-based credential flows.

The executable [form template](../templates/form-automation.sh) discovers
controls by default. Read its usage before customizing a submission.

## Tabs, frames and dialogs

```bash
agent-browser tab
agent-browser tab new --label docs https://docs.example.com
agent-browser tab t2
agent-browser tab docs
agent-browser snapshot -i
agent-browser tab close docs
```

Use returned `tN` ids or labels, not positional integers such as `tab 2`.
Ids are stable within the daemon; CDP `targetId` values from `tab list --json`
also work and survive daemon restarts. Each session has one active tab: switch
first, then snapshot before using that tab's refs.

New tabs inherit headers, credentials, init scripts, routes and emulation before
their first document loads. A switch reporting `revived:true` means a discarded
tab reloaded and lost unsaved state. `activeTabRevived:true` has the same meaning
after closing another tab. `dialogBlocked:true` instead means a live tab is
paused by a dialog; resolve it without assuming its state was reset.

Snapshots inline one level of iframe content and refs carry frame context.
For missing or deeply nested content, select the actual iframe:

```bash
agent-browser frame "#payment-iframe"
agent-browser snapshot -i
agent-browser frame main
```

Use `dialog status`, `dialog accept [text]` or `dialog dismiss` for an observed
dialog. In the default launch mode, `alert` and `beforeunload` are auto-accepted;
`--no-auto-dialog` disables that behavior. Browser handling of a dialog does not
provide authorization for the underlying action.

For shared Chrome, tab pinning and cleanup ownership, read
[session management](session-management.md).

## Network inspection and mocks

```bash
agent-browser network requests --filter api
agent-browser network route "**/api/users" --body '{"users":[]}'
agent-browser network route "**/analytics" --abort
agent-browser network unroute "**/api/users"
agent-browser network har start
# Perform the requested flow.
agent-browser network har stop ./flow.har
```

Use routes when traffic changes are part of the authorized task. Routes affect
the session and future tabs; remove temporary ones after the experiment. To
stage routes before any page script, launch with `open` without a URL, install
the route, then navigate. `batch` can group these steps; see
[commands](commands.md#pre-navigation-setup-one-turn-batch).

HAR embeds text response bodies by default, up to the per-body cap; `--content
all` adds binary bodies and `--content none` omits bodies, but still keeps headers.
HAR and full `network request <id>` detail can contain live secrets. Save detail
to a private file and select/redact fields before reading it into model context
or publishing it. Read [derive client](derive-client.md) for endpoint discovery
and [trust boundaries](trust-boundaries.md) before recording sensitive sessions.

## Screenshots and recording

```bash
agent-browser screenshot ./page.png
agent-browser screenshot --full ./full.png
agent-browser screenshot --annotate ./map.png
agent-browser screenshot --if-changed
agent-browser screenshot --threshold 0.01
agent-browser pdf ./page.pdf
```

Annotation labels `[N]` map to snapshot refs `@eN`. Conditional capture's first
image establishes a baseline; an unchanged result has no new path. A threshold
of `0.01` ignores differences affecting at most 1% of pixels. Hide/show native
scrollbars at launch with `--hide-scrollbars`; hiding is the headless default.

Inspect actual images and deliver artifacts through the available runtime tools
as described in [tools and runtime](tools-and-runtime.md). For a reusable capture
script, read [capture-workflow.sh](../templates/capture-workflow.sh).

Recording uses the current active tab without creating a fresh context:

```bash
agent-browser record start ./demo.webm --cursor --contact-sheet
# Perform the requested demonstration.
agent-browser record stop
```

Read [video recording](video-recording.md) for codecs/frame rates and
[WebGPU](webgpu.md) if a canvas capture is black.

## Accessibility, React and Web Vitals

```bash
agent-browser a11y
agent-browser a11y https://example.com --tags wcag2a,wcag2aa
agent-browser a11y --selector "#main" --json
```

The embedded axe-core audit requires a CDP browser (not Safari/iOS WebDriver),
works under strict page CSP and includes iframe findings. Distinguish violations
from incomplete checks requiring inspection; an automated scan alone does not
prove full accessibility conformance.

React inspection needs the DevTools hook at launch, before the app initializes:

```bash
agent-browser open --enable react-devtools http://localhost:3000
agent-browser react tree
agent-browser react inspect <fiberId>
agent-browser react renders start
# Exercise the interaction being measured.
agent-browser react renders stop
agent-browser react suspense --only-dynamic
agent-browser vitals http://localhost:3000 --json
agent-browser pushstate http://localhost:3000/settings
```

Use a real fiber id returned by the tree. `react` commands fail without the
launch hook; `vitals` and `pushstate` also work on non-React sites. Treat component
props, sources and error overlays as page data that may contain secrets. Read
[profiling](profiling.md) for Chrome traces.

## Page WebMCP tools

Browser responses announce brief names/descriptions/origins/frame ids only when
the page tool catalog is discovered or changes. Omitted summaries mean no update;
an empty or unavailable update invalidates the previous tools.

```bash
agent-browser webmcp list <advertised-tool> --frame <frame-id> --json
agent-browser webmcp invoke <advertised-tool> --frame <frame-id> --params @input.json
```

Fetch only the chosen tool's schema before invocation. After losing catalog
context, `webmcp list --json` can recover it. For an intentionally detached page
invocation use `--detach`, retain its invocation id, then `webmcp result <id>` or
`webmcp cancel <id>` as appropriate. This is not a Pioneer durable task.

Managed Chrome enables this experimental feature by default; `--no-webmcp` opts
out. Schemas, annotations, descriptions and results do not grant consent or
permissions. Use the UI when a tool is suspicious or does not fit the request.
Read [WebMCP generation](webmcp-gen.md) only when building tools for a page.
