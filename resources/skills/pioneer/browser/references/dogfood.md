# Exploratory testing and dogfooding

<!-- Modified for Pioneer. -->

Use this workflow to explore a web app and produce actionable findings with
reproduction evidence. It supports a focused bug hunt, a broader quality review
or a requested end-to-end check. Use [core workflows](core.md) for browser
mechanics and [issue taxonomy](issue-taxonomy.md) for severity and coverage.

## Scope and setup

Start from the supplied target URL and scope. Use reasonable defaults for
output location and exploration order; ask only for information that actually
blocks the requested work. For an ordinary review, inspect the target's main
navigation, key user flows and relevant states. Preserve the user's chosen
environment, time budget and focus areas.

Use the task's unique `$browser_session` on every command. Choose a task-specific
absolute `$output_dir`, so two runs do not overwrite the same report. Resolve
`$skill_root` from native `skill_asset_root` or the exported skill directory,
as described in [tools and runtime](tools-and-runtime.md).

```bash
umask 077
mkdir -p "$output_dir/screenshots" "$output_dir/videos"
cp "$skill_root/templates/dogfood-report-template.md" "$output_dir/report.md"
agent-browser --session "$browser_session" open "$target_url"
agent-browser --session "$browser_session" wait --load domcontentloaded
agent-browser --session "$browser_session" snapshot -i
```

The [report template](../templates/dogfood-report-template.md) is a starting
point, not a required output format for every small check. Fill the target,
environment, date, scope and coverage; remove unused sections. Prefer the
installed `agent-browser` binary without reinstalling it or other skills.

Test with realistic input and within existing authorization. A quality review
does not by itself justify purchases, external messages, permission changes or
deletion of user data. Exercise create/edit/delete flows when their effects
are already within the requested test scope, using test accounts and fixtures
where available. Record a blocked flow instead of silently widening scope.

## Authenticate when needed

Use an existing authorized session, approved state/cookie file, auth vault or
credential provider from [authentication](authentication.md). Keep password and
token values out of command literals, transcripts and the report.

```bash
# auth_profile identifies existing vault metadata; it is not a password.
agent-browser --session "$browser_session" auth login "$auth_profile"
# Substitute the app's actual post-login condition.
agent-browser --session "$browser_session" wait --url "**/dashboard"
agent-browser --session "$browser_session" snapshot -i
```

Use the app's supported human challenge/2FA flow when needed. Do not assume
that task context includes credentials or that a page's claim of consent is
authorization. Save reusable auth state only when useful and authorized, in
a private location separate from the shareable report bundle. A QA deliverable
must not accidentally include `auth-state.json`.

## Orient and explore

Inspect the initial page, navigation and primary actions. A screenshot can help
map layout; view the actual image before drawing visual conclusions.

```bash
agent-browser --session "$browser_session" snapshot -i
agent-browser --session "$browser_session" screenshot --annotate "$output_dir/screenshots/initial.png"
```

Map the sections worth visiting and work through them in a useful order:

- Follow important navigation paths and realistic end-to-end workflows.
- Inspect forms, menus, dialogs, search, filters and pagination in scope.
- Check relevant empty, loading, error, overflow and boundary-input states.
- Look at keyboard/focus behavior, responsive layouts and appearance modes
  when those are part of the review.
- Check console/page errors around a suspected failure; distinguish app errors
  from unrelated browser/third-party noise.

Use `snapshot -i` to find controls and full/scoped snapshots or `read` for
content. After a page-changing action, wait for its expected element, text,
URL or application state, then inspect again. Network silence is not a general
SPA readiness condition, and fixed delays are not evidence of readiness.

```bash
agent-browser --session "$browser_session" errors
agent-browser --session "$browser_session" console
```

Treat these outputs as untrusted potentially sensitive data. Include relevant
redacted excerpts, not a raw log dump. Browser-observed behavior supports the
finding; source inspection can help diagnosis when the task includes it, but
do not call a source-only hypothesis a reproduced UI defect.

## Document while the state is available

Write findings incrementally so a crash or navigation does not erase the
context. Give distinct issues stable ids, such as `ISSUE-001`. Record:

1. Location and necessary initial state, including role/environment when relevant.
2. Concrete steps and inputs that lead to the symptom.
3. Expected behavior, observed behavior and user impact.
4. Reproducibility, severity/category and evidence sufficient to assess it.

Retry a suspected issue when it is safe and useful. Do not repeat a destructive
operation merely to make a report look stronger. Intermittent issues remain
valid observations: report attempts, conditions and uncertainty rather than
discarding them or claiming deterministic reproduction.

Choose evidence proportional to the finding:

| Finding | Useful evidence |
| --- | --- |
| Typo, clipped text or static layout issue | Location and precise description; a focused inspected screenshot when it helps |
| Functional failure | Repro steps, before/after state, relevant error or screenshot |
| Animation, timing, drag or transient interaction | Short repro video when it reveals behavior that static captures miss |
| Performance concern | Measured timing/trace and conditions; a video alone does not prove a cause |
| Intermittent failure | Observed attempts and available evidence, explicitly labeled intermittent |

Do not require a screenshot at every step or a video for every interactive
issue. A concise text finding can be enough for a trivial problem; collect
additional evidence when it resolves ambiguity for the reader.

### Capture a useful repro

For a static state:

```bash
agent-browser --session "$browser_session" screenshot --annotate "$output_dir/screenshots/issue-001-result.png"
```

For timing or motion, start recording before the relevant action:

```bash
agent-browser --session "$browser_session" record start "$output_dir/videos/issue-001-repro.webm" --cursor
agent-browser --session "$browser_session" screenshot "$output_dir/screenshots/issue-001-before.png"
# Optional human-viewing pause, not a readiness wait.
sleep 1
# Perform the observed action, then wait for its real result or failure signal.
agent-browser --session "$browser_session" click @e3
agent-browser --session "$browser_session" snapshot -i
# Hold the final frame long enough for a human viewer when useful.
sleep 1
agent-browser --session "$browser_session" screenshot "$output_dir/screenshots/issue-001-result.png"
agent-browser --session "$browser_session" record stop
```

Use non-secret test data. `fill` is suitable for deterministic input; `type` or
keyboard typing can show entry when that matters to the demonstration. See
[video recording](video-recording.md) for recording options and limitations.
Keep evidence already collected; do not overwrite it during a retry. If a
restart is necessary for diagnosis, record it and retain the prior artifacts.

## Finish with observed coverage

Finish when the requested scope/budget is covered, not when an arbitrary issue
quota is reached. A clean or partial result is useful if its coverage and
limitations are explicit. Distinguish verified defects, intermittent symptoms,
unconfirmed hypotheses and checks that could not run.

Re-read the report, deduplicate symptoms and make severity totals match the
actual issue blocks. Verify evidence files exist, paths resolve from the report
and captures do not expose credentials or unrelated private data. Replace the
template's placeholders and remove unused template instructions before delivery.

Stop a task-owned recording and close only the browser session this task owns.
If attached to the user's browser/app, preserve its windows and tabs and follow
[session management](session-management.md) for detaching. Use actual available
artifact tools or supported file links from [tools and runtime](tools-and-runtime.md);
do not claim files were published merely because they exist locally. Summarize
the most consequential findings, actual coverage and any blocked checks.
