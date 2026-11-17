# Proxy support

<!-- Modified for Pioneer. -->
Use a named task session for all examples.

## Contents

- [Select a proxy](#select-a-proxy)
- [Authentication and bypass](#authentication-and-bypass)
- [Regional and corporate workflows](#regional-and-corporate-workflows)
- [Trust an inspection CA](#trust-an-inspection-ca)
- [Troubleshooting](#troubleshooting)

## Select a proxy

Use a proxy when required by the requested network environment or testing
scenario, not as a universal prerequisite:

```bash
agent-browser --proxy http://proxy.example.com:8080 open https://example.com
```

The environment alternatives are `HTTP_PROXY`, `HTTPS_PROXY` and
`ALL_PROXY` (for example a SOCKS5 endpoint). Configure the intended transport
and keep it consistent for the task. Launch configuration is applied to the
browser session; consult installed help before changing it mid-session.

Do not include proxy passwords in command literals or printed URLs. Use the
host's supplied secret environment/configuration if authentication is required.
An environment populated outside model-authored commands avoids exposing its
contents to the transcript; `export ...="user:password@..."` in a tool call does
not provide that property.

## Authentication and bypass

Chrome supports `--proxy-bypass` and `NO_PROXY` for selected destinations:

```bash
agent-browser --proxy http://proxy.example.com:8080 --proxy-bypass "localhost,*.internal.example.com" open https://example.com
```

Choose bypass destinations from the actual environment contract. Do not
silently route restricted hosts directly as a performance fix.

The experimental Obscura engine rejects proxy bypass settings, including
`proxyBypass`, `AGENT_BROWSER_PROXY_BYPASS`, `NO_PROXY` and `no_proxy`,
even without a proxy. An explicit `--engine obscura` uses the invocation's
resolved settings. Remove the bypass settings for that intended launch or use
Chrome when bypass is required; do not loop retrying the same unsupported setup.

## Regional and corporate workflows

For authorized geo checks, use a task-specific session suffix per region and
record the actual proxy, viewport and account setup without credentials.
Capture the same state under each region. Do not infer location from a proxy
hostname alone.

Corporate environments may route external traffic through an inspection proxy
while keeping localhost/internal targets direct. Use the supplied proxy and
bypass policy. If the task needs an apparent-IP check, use an endpoint explicitly
allowed for that purpose; such a check contacts another service.

For bulk collection, respect the user's scope and the site's supported access
limits. A proxy is transport configuration, not authorization for extra data
or a reason to retry a rate-limited request indefinitely.

## Trust an inspection CA

Prefer adding the actual supplied CA over bypassing all verification:

```bash
agent-browser --ca-cert /path/to/proxy-ca.crt open https://example.com
agent-browser --ca-cert /path/to/proxy-ca.crt read https://example.com
agent-browser --use-system-ca install
```

These are different scopes:

- CLI HTTP requests (`read`, `install`, `upgrade`, `doctor`) work on all
  platforms with `--ca-cert` or `--use-system-ca`. Default roots are Mozilla's.
  `SSL_CERT_FILE` is a fallback CA file; an unusable file is ignored with a warning.
- Locally launched Chromium on Linux supports `--ca-cert` through a private
  NSS database. It requires `certutil`, provided by `install --with-deps`
  (`libnss3-tools` on Debian/Ubuntu, `nss-tools` on RPM systems).
- This browser-specific trust path does not cover macOS/Windows, profiles,
  existing CDP/auto-connect, providers, Lightpanda or remote CDP transport.

Neither CA selection disables hostname, validity or unrelated authority checks.
Linux browser CA changes relaunch Chromium without restarting its daemon,
potentially losing unsaved page state. Identical certificate content reuses the
browser; `--no-ca-cert` clears the retained CA. Omitted flags keep it for later
session commands.

For `read`, an explicit trust selection remains effective across daemon
restarts until close or an explicit clear. `--use-system-ca false` clears that
setting; read-only HTTP operations do not relaunch Chrome because trust changed.

Use `--ignore-https-errors` only when broad certificate-error bypass is
explicitly part of the intended test. A missing CA is not by itself a reason to
weaken every connection.

## Troubleshooting

- Connection refused: verify the supplied proxy endpoint and protocol; avoid
  echoing its credentials in diagnostics.
- Authentication failure: check how the approved host mechanism supplies the
  credentials, not a guessed password in a retry command.
- TLS failure: separate CLI requests from browser navigation, then check whether
  that trust mechanism is supported on this platform/connection type.
- Slow traffic: inspect the intended proxy path and legitimate bypass policy;
  preserve required egress restrictions.
- New launch flags seem ignored: inspect `session info --json` and installed
  help before restarting only the task-owned session.

For broader browser launch problems, see
[setup and troubleshooting](setup-and-troubleshooting.md).
