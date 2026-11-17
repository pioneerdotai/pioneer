# AWS Bedrock AgentCore cloud browser

<!-- Modified for Pioneer. -->

Use this guide when the requested browser is hosted by AWS Bedrock AgentCore.
The provider owns the remote browser session; normal snapshot/ref interaction
continues through agent-browser. Use a configured service connector if it
already supports the needed operation, and stay within the selected AWS account,
region and resource scope.

Every command below carries the task-selected unique `$browser_session`.
Native and CLI tool bindings are described in [tools and runtime](tools-and-runtime.md).
Configure cloud credentials outside model-visible commands; never print access
keys, session tokens or credential-export output.

## Credentials and configuration

Upstream 0.39.4 resolves credentials from:

1. Existing `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, with
   `AWS_SESSION_TOKEN` when needed.
2. AWS CLI `aws configure export-credentials`, supporting configured profiles,
   SSO and IAM-role-based credentials.

An already working credential chain needs no reconfiguration. Use `AWS_PROFILE`
to select the intended existing profile. If SSO needs renewal, use its supported
interactive flow; do not ask the user to paste keys into chat. Do not create
IAM users, broaden policies or replace credentials as a side effect of browsing.

| Variable | Purpose | Upstream default |
| --- | --- | --- |
| `AGENTCORE_REGION` | AWS region | `us-east-1` |
| `AGENTCORE_BROWSER_ID` | Browser resource identifier | `aws.browser.v1` |
| `AGENTCORE_PROFILE_ID` | Deliberately reusable remote cookies/localStorage profile | None |
| `AGENTCORE_SESSION_TIMEOUT` | Remote session timeout in seconds | `3600` |
| `AWS_PROFILE` | Existing AWS CLI profile | `default` |
| `AGENT_BROWSER_PROVIDER` | Provider selection instead of `-p agentcore` | Host configuration |

Use the actual authorized region/browser resource rather than assuming the
defaults fit the user's account. Supply needed environment configuration in
each execution process; a shell export in a prior tool call may not persist.

## Core workflow

```bash
agent-browser --session "$browser_session" -p agentcore open "$target_url"
agent-browser --session "$browser_session" -p agentcore snapshot -i
# Choose refs from the current snapshot and wait for the expected result.
agent-browser --session "$browser_session" -p agentcore click @e1
agent-browser --session "$browser_session" -p agentcore wait --text "Expected result"
agent-browser --session "$browser_session" -p agentcore snapshot -i
agent-browser --session "$browser_session" -p agentcore screenshot "$output_dir/page.png"
```

Interaction follows [core workflows](core.md). View the actual screenshot
before making visual claims, and deliver only intended artifacts. Remote
execution does not make auth state, recordings or private page content public.

For an explicit existing profile and region, launch with that configuration:

```bash
AWS_PROFILE="$aws_profile" AGENTCORE_REGION=eu-west-1 \
  agent-browser --session "$browser_session" -p agentcore open "$target_url"
```

Use `AGENT_BROWSER_PROVIDER=agentcore` in each process when preferred, instead
of `-p agentcore`. Do not accidentally issue a later `open` in a local provider
after a restart because the intended environment was not carried forward.

## Persistent browser profiles

`AGENTCORE_PROFILE_ID` is a reusable remote auth/storage container; the
agent-browser session name isolates this task's active browser. They serve
different purposes. Reuse a remote profile deliberately for the requested
account/role; a shared profile is not independent-task isolation.

```bash
AGENTCORE_PROFILE_ID="$agentcore_profile_id" \
  agent-browser --session "$browser_session" -p agentcore open "$login_url"
agent-browser --session "$browser_session" -p agentcore snapshot -i
```

Complete login using the supported private mechanism in
[authentication](authentication.md), without putting passwords into `fill`
command literals. Wait for the app's actual authenticated state before relying
on the profile. On a later authorized task, select that same remote profile
with the later task's own unique session and verify that the intended account
is still signed in:

```bash
AGENTCORE_PROFILE_ID="$agentcore_profile_id" \
  agent-browser --session "$browser_session" -p agentcore open "$protected_url"
agent-browser --session "$browser_session" -p agentcore snapshot -i
```

Do not mix roles or unrelated users in one profile. Retention and cleanup of
remote profiles should follow the requested reuse policy; closing an active
session and deleting persistent auth are separate operations.

## Live view and session lifetime

AgentCore can print a Live View URL on session creation for the AWS Console.
Use the actual returned URL only with its intended viewer/account. Do not
fabricate a session URL, paste private access links into a public report or
claim it was published as an artifact.

The upstream session timeout is one hour. Set `AGENTCORE_SESSION_TIMEOUT` when
the authorized workflow needs another lifetime; avoid increasing it indefinitely
to hide stalled work. When finished, close this task-owned remote session:

```bash
agent-browser --session "$browser_session" -p agentcore close
```

Use the session-specific close even after an error; do not close other tasks'
remote sessions. Keep deliberate persistent state separate from the lifecycle
of this allocated browser.

## Diagnose failures

| Symptom | Useful next check |
| --- | --- |
| Failed to run AWS CLI | Verify the executable/path or the existing environment credential chain; do not dump exported credentials |
| Expired SSO credentials | Refresh the selected existing profile through its supported login flow |
| Access denied or resource missing | Verify account, region, browser id and permitted IAM scope before proposing any access change |
| Session expired | Reopen within the task's scope, select the intended profile and verify restored auth |
| CLI/provider syntax differs | Compare installed version and narrow help, as described in tools and runtime |

Report the actual unavailable capability or access scope. Do not treat a
provider error as permission to create new resources or widen policies. For
user-requested future/recurring checks, use `system:pioneer/tasks` when its
skill and tools are available; a long remote session is not a durable schedule.
