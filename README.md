<p align="center">
  <img src="assets/pioneer.png" alt="Pioneer" width="50">
</p>

<h1 align="center">Pioneer — Personal AI Assistant</h1>

<p align="center">
  <strong>You own the assistant. You own the data. You choose where the gateway runs.</strong>
</p>

<p align="center">
  <a href="https://github.com/pioneerdotai/pioneer/releases"><img src="https://img.shields.io/github/v/release/pioneerdotai/pioneer?include_prereleases&label=release" alt="Release"></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License"></a>
</p>

<p align="center">
  <a href="https://docs.getpioneer.dev">Docs</a>
  ·
  <a href="https://docs.getpioneer.dev/getting-started/installation">Installation</a>
  ·
  <a href="https://docs.getpioneer.dev/getting-started/quickstart">Quick start</a>
  ·
  <a href="https://docs.getpioneer.dev/architecture/overview">Architecture</a>
  ·
  <a href="https://docs.getpioneer.dev/protocol/introduction">Protocol reference</a>
  ·
  <a href="https://github.com/pioneerdotai/pioneer/releases">Releases</a>
</p>

<p align="center">
  <img src="assets/screenshots/pioneer-main-dark.png" alt="Pioneer">
  <img src="assets/screenshots/pioneer-main-light.png" alt="Pioneer">
</p>

**Pioneer** is a local-first AI workspace for running an assistant on your own machine or on infrastructure you control. It combines a persistent gateway, a native desktop app, a JSON-RPC protocol, provider adapters, durable threads, agent memory, task automation, MCP servers, skills, and real local tools.

The gateway is the core of Pioneer. It owns state, configuration, storage, model access, task execution, tool execution, MCP runtime, skills, and thread history. The desktop app is the primary client for connecting to one or more gateways, whether the gateway is running on the same computer or on a remote server.

> **Early-stage warning**
>
> Pioneer is in an extremely early stage of development. Expect rough edges, breaking changes, and incomplete flows. Use it carefully, and test it in a safe environment before trusting it with important work or exposing a gateway outside your machine.
>
> Tool runs are not sandboxed yet. Tools currently execute as the OS user running the gateway.

## Highlights

<table>
  <colgroup>
    <col width="30%">
    <col width="70%">
  </colgroup>
  <tbody>
    <tr>
      <td><strong>Gateway-centered design</strong></td>
      <td>All important work happens in the gateway: workspaces, threads, turns, tools, MCP, skills, tasks, provider settings, auth, and durable storage.<br><br>Relevant docs: <a href="https://docs.getpioneer.dev/getting-started/concepts">User Guide</a> · <a href="https://docs.getpioneer.dev/architecture/gateway">Architecture</a></td>
    </tr>
    <tr>
      <td><strong>Local or remote deployment</strong></td>
      <td>Run a gateway on your personal computer for a local assistant, or host gateways on separate servers for work, study, home, or other isolated environments.<br><br>Relevant docs: <a href="https://docs.getpioneer.dev/getting-started/installation">User Guide</a> · <a href="https://docs.getpioneer.dev/architecture/overview">Architecture</a></td>
    </tr>
    <tr>
      <td><strong>One desktop, many gateways</strong></td>
      <td>Connect the desktop app to any number of gateways and switch between them from one native client.<br><br>Relevant docs: <a href="https://docs.getpioneer.dev/desktop/overview">User Guide</a> · <a href="https://docs.getpioneer.dev/architecture/gateway">Architecture</a></td>
    </tr>
    <tr>
      <td><strong>Workspace management</strong></td>
      <td>Create, switch, and rename workspaces inside a gateway; each workspace keeps its own threads, provider keys, MCP servers, skills, tasks, and artifacts.<br><br>Relevant docs: <a href="https://docs.getpioneer.dev/desktop/workspace">User Guide</a></td>
    </tr>
    <tr>
      <td><strong>Multi-agent workflows</strong></td>
      <td>The gateway can automatically fan work out to subagents with their own prompts, roles, models, context policies, tool policies, result contracts, and child threads. The parent agent reviews each subagent result, accepts it, or asks the same subagent to revise the work with concrete feedback.<br><br>Relevant docs: <a href="https://docs.getpioneer.dev/tasks/overview">User Guide</a> · <a href="https://docs.getpioneer.dev/architecture/tasks">Architecture</a></td>
    </tr>
    <tr>
      <td><strong>Durable agent memory</strong></td>
      <td>Agent mode can recall and write stable facts, preferences, recurring instructions, project decisions, and communication style through prompt policy, memory tools, proactive post-turn extraction, service-owned dedupe, and memvid-backed search capsules. It is configurable, quality-gated, and does not claim full transcript recall.<br><br>Relevant docs: <a href="https://docs.getpioneer.dev/desktop/memory">User Guide</a> · <a href="https://docs.getpioneer.dev/architecture/memory">Architecture</a></td>
    </tr>
    <tr>
      <td><strong>Typed hook runtime</strong></td>
      <td>Lifecycle hooks attach policy, context, prompt sections, tool bundles, diagnostics, and post-turn work without turning the agent loop into a domain-specific container.<br><br>Relevant docs: <a href="https://docs.getpioneer.dev/architecture/hooks">Architecture</a></td>
    </tr>
    <tr>
      <td><strong>Bring your own model</strong></td>
      <td>Built-in providers are available for OpenAI, Anthropic, OpenRouter, Gemini, Azure OpenAI, Bedrock, Ollama, Copilot, Claude Code, and many OpenAI-compatible endpoints.<br><br>Relevant docs: <a href="https://docs.getpioneer.dev/providers/overview">User Guide</a> · <a href="https://docs.getpioneer.dev/architecture/providers">Architecture</a></td>
    </tr>
    <tr>
      <td><strong>Keystore-backed secrets</strong></td>
      <td>Workspace-scoped provider API keys, MCP env/header secrets, superuser JWT signing material, and desktop gateway bearer tokens are stored in <code>keystore.db</code> instead of ordinary TOML or domain tables.<br><br>Relevant docs: <a href="https://docs.getpioneer.dev/architecture/secrets">Architecture</a></td>
    </tr>
    <tr>
      <td><strong>Real tools</strong></td>
      <td>Shell sessions, file reads and edits, patch application, grep, web search/fetch, URL downloads, computer use, MCP tool proxying, and dynamic skill tools are available to agents through the gateway.<br><br>Relevant docs: <a href="https://docs.getpioneer.dev/architecture/tools">Architecture</a></td>
    </tr>
    <tr>
      <td><strong>MCP servers</strong></td>
      <td>Install and manage servers compatible with <a href="https://modelcontextprotocol.io/docs/getting-started/intro">Model Context Protocol</a> per gateway and per workspace, track their health and catalog, and expose their tools to agents through the gateway.<br><br>Relevant docs: <a href="https://docs.getpioneer.dev/mcp/overview">User Guide</a> · <a href="https://docs.getpioneer.dev/architecture/mcp">Architecture</a></td>
    </tr>
    <tr>
      <td><strong>Skills</strong></td>
      <td>Skills are compatible with the <a href="https://agentskills.io/home">Agent Skills specification</a>, with installation, validation, trust gates, dependency preflight, gateway/workspace policy, upload flow, and health diagnostics.<br><br>Relevant docs: <a href="https://docs.getpioneer.dev/skills/overview">User Guide</a> · <a href="https://docs.getpioneer.dev/architecture/skills">Architecture</a></td>
    </tr>
    <tr>
      <td><strong>Tasks</strong></td>
      <td>Scheduled and on-demand task execution is available with dependencies, retries, delivery state, progress events, write locks, and task trees.<br><br>Relevant docs: <a href="https://docs.getpioneer.dev/tasks/overview">User Guide</a> · <a href="https://docs.getpioneer.dev/architecture/tasks">Architecture</a></td>
    </tr>
    <tr>
      <td><strong>Thread modes</strong></td>
      <td>Use Chat mode for direct conversations and Agent mode when the thread should plan, use tools, and work through multi-step tasks.<br><br>Relevant docs: <a href="https://docs.getpioneer.dev/getting-started/highlights">User Guide</a> · <a href="https://docs.getpioneer.dev/architecture/agent-loop">Architecture</a></td>
    </tr>
    <tr>
      <td><strong>Thread tree AGENTS.md</strong></td>
      <td>Define persistent instructions at the workspace root or any thread folder; child threads inherit the nearest active file and Pioneer injects it into the prompt through the hook runtime.<br><br>Relevant docs: <a href="https://docs.getpioneer.dev/desktop/agents-md">User Guide</a> · <a href="https://docs.getpioneer.dev/architecture/agents-md">Architecture</a></td>
    </tr>
    <tr>
      <td><strong>Protocol-first architecture</strong></td>
      <td><code>pioneer-protocol</code> defines the public JSON-RPC surface and generated schemas under <code>schemas/</code>.<br><br>Relevant docs: <a href="https://docs.getpioneer.dev/architecture/protocol">Architecture</a></td>
    </tr>
    <tr>
      <td><strong>Explicit workspace-scoped artifacts</strong></td>
      <td>User uploads and agent-created result files are stored by the gateway, linked to workspace/thread/turn/message lineage, previewed when possible, and downloadable from local or remote gateways.<br><br>Relevant docs: <a href="https://docs.getpioneer.dev/desktop/artifacts">User Guide</a> · <a href="https://docs.getpioneer.dev/architecture/artifacts">Architecture</a></td>
    </tr>
    <tr>
      <td><strong>Cross-platform packaging</strong></td>
      <td>Gateway builds are available for macOS, Linux, and Windows; desktop packaging targets DMG, AppImage, and MSI.<br><br>Relevant docs: <a href="https://docs.getpioneer.dev/getting-started/installation">User Guide</a></td>
    </tr>
    <tr>
      <td><strong>Multi-language desktop</strong></td>
      <td>Desktop UI locales are available for English, German, Spanish, French, Hindi, Japanese, Russian, and Chinese.<br><br>Relevant docs: <a href="https://docs.getpioneer.dev/desktop/overview">User Guide</a></td>
    </tr>
  </tbody>
</table>

## 100% Rust

Pioneer is built in Rust all the way through the product: gateway, CLI, desktop app, protocol, tools, tasks, MCP, skills, and provider integrations.

That keeps the core memory-safe, fast, and small. The desktop app is native GPUI, not Electron or a web app wrapped in a window.

## Gateway and Desktop

Pioneer is split into two parts:

- **Gateway** - the main runtime and control plane. It runs as a service, stores the data, talks to model providers, executes tools, manages MCP servers and skills, schedules tasks, and exposes the JSON-RPC WebSocket API.
- **Desktop app** - the primary native client. It connects to gateways, starts and manages a local gateway when needed, and gives you the UI for workspaces, conversations, provider setup, MCP, skills, settings, and thread history.
- **Protocol clients** - any client can be built on top of the Pioneer JSON-RPC protocol. Native mobile apps for iOS and Android are planned next.

For a single-machine setup, install the desktop app and let it start the local gateway for you. On macOS, that means downloading the `.dmg`, moving Pioneer to Applications, launching it, and pressing `Start local gateway` when prompted.

For a multi-environment setup, install gateways wherever the work should live: a laptop, a workstation, a home server, or a remote machine. Then connect to each gateway from the same desktop app. You can keep separate gateways for work, study, home, experiments, or clients without mixing their state, settings, tools, and histories.

## Gateway Install

Use the gateway bootstrap scripts when you want to install or update the gateway directly, for example on a remote server or in a headless environment. Gateway installation uses a single user mode: the service runs as the current OS user.

macOS and Linux:

```bash
curl -fsSL https://getpioneer.dev/install.sh | bash
```

Install the native computer-use gateway variant without installing the desktop app:

```bash
curl -fsSL https://getpioneer.dev/install.sh | bash -s -- --computer-use
```

Windows PowerShell:

```powershell
iwr -useb https://getpioneer.dev/install.ps1 | iex
```

Install the native computer-use gateway variant without installing the desktop app:

```powershell
$env:PIONEER_INSTALL_COMPUTER_USE="1"; iwr -useb https://getpioneer.dev/install.ps1 | iex
```

Windows CMD:

```cmd
curl -fsSL https://getpioneer.dev/install.cmd -o install.cmd && install.cmd && del install.cmd
```

The bootstrap scripts download a release asset, verify checksums, and then run the native installer path through `pioneer install --source local`. The installer registers a user-level gateway service and exposes the `pioneer` command.

On Linux, the gateway is installed as a `systemd --user` service. For server and headless installs, that service must be allowed to run without an active login session. The installer validates/enables systemd lingering for the current user; if the OS denies that operation, run this once on the server and then rerun the installer:

```bash
sudo loginctl enable-linger "$USER"
```

On macOS, the gateway is installed as a per-user LaunchAgent. On Windows, it is installed as a current-user Scheduled Task triggered at logon. Those modes run as the current user and auto-start after user login; they are not boot-time LaunchDaemon/Windows Service installs before login.

Install scripts support:

```bash
--channel stable|beta|canary
--version x.y.z
--computer-use
--headless
--no-start
--force-start
```

Channel/version and gateway variant selection depend on matching release assets being published for the target platform.

After first install, open a new shell session so the updated user `PATH` is picked up. If automatic PATH profile update is skipped, gateway install/start can still succeed and the service remains reachable.

Optional manual Unix PATH setup:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

## Gateway Network Bind

By default the production gateway listens on `0.0.0.0:17878`, so a server install can be reached from other machines when the host firewall allows it:

```toml
[gateway]
listen_addr = "0.0.0.0:17878"
```

To restrict a local install to this machine only, override the persistent user config:

```toml
[gateway]
listen_addr = "127.0.0.1:17878"
```

Config file locations:

- Linux: `~/.config/pioneer/config.toml`
- macOS: `~/Library/Application Support/pioneer/config.toml`
- Windows: `%APPDATA%\pioneer\config.toml`

After changing the bind address, restart the gateway service. For external access, allow TCP `17878` through the host firewall.

## Desktop Install

If you only want to use Pioneer on your local computer, start here. Install the desktop app, launch it, and let it install/start the local gateway when needed:

- macOS: install via `.dmg` and move the app to Applications.
- Windows: install via `.msi` or `.exe`.
- Linux: install via `.AppImage`.

The desktop app can also connect to remote gateways. Use it as one control surface for any number of Pioneer gateways: local, work, study, home, and server-hosted environments.

The desktop app does not execute installer shell scripts. For local gateway setup it uses bundled `pioneer-bootstrap` plus local assets/checksums and runs the native `pioneer install` flow.

## CLI Quick Reference

Useful commands after installation:

```bash
pioneer status                  # check service status and gateway reachability
pioneer start                   # start the gateway service
pioneer issue-superuser-token   # print a JWT for privileged clients
pioneer secrets status          # inspect keystore status without printing secret values
pioneer secrets garbage-collection --dry-run
pioneer secrets rotate-jwt-token superuser
pioneer update                  # update using the configured release source
pioneer stop                    # stop and unregister the gateway service
pioneer version
pioneer help
```

The CLI installer can resolve a local bundle or release assets:

```bash
pioneer install --source local --asset <path> --checksums <path>
pioneer install --source release --channel stable
```

The same options are available through `pioneer update`. Release-based install/update requires a published gateway asset and matching `SHA256SUMS` for the current OS, architecture, and gateway variant. A headless gateway updates from the standard asset name; a computer-use gateway updates from the `-computer-use` asset name.

## Security Note

Pioneer tools can execute commands, read and write files, use the network, and control the desktop when enabled. Treat the gateway as a privileged local service.

There is currently no separate sandbox for tool runs. All tool execution happens with the permissions of the user account that runs the gateway service.

Before binding the gateway to a non-local interface, make sure access is intentional, the host firewall is configured, and clients use a token issued by the gateway.

## From Source

This repository is a Rust workspace and pins the stable toolchain in `rust-toolchain.toml`.

```bash
git clone https://github.com/pioneerdotai/pioneer.git
cd pioneer

cargo build --workspace
cargo run -p pioneer-gateway
cargo run -p pioneer-desktop --bin pioneer-app
cargo run -p pioneer-cli -- help
```

Development builds load `config/local.toml`, use `~/.pioneer.dev`, expose `pioneer-dev`, and default to port `18778`:

```bash
cargo run -p pioneer-cli --features dev --bin pioneer-dev -- status
./scripts/reset-pioneer-dev-env.sh
```

## Release Signing

Tagged desktop release builds enforce signing/notarization where configured.

macOS secrets:

- `MACOS_CERTIFICATE_P12_BASE64`
- `MACOS_CERTIFICATE_PASSWORD`
- `MACOS_DESKTOP_SIGN_IDENTITY`
- `MACOS_DMG_SIGN_IDENTITY` (optional, defaults to desktop identity)
- `APPLE_NOTARIZATION_KEY_ID`
- `APPLE_NOTARIZATION_ISSUER_ID`
- `APPLE_NOTARIZATION_KEY` or `APPLE_NOTARIZATION_KEY_BASE64`

Windows signing secrets:

- `WINDOWS_SIGNING_CERT_BASE64`
- `WINDOWS_SIGNING_CERT_PASSWORD`
- `WINDOWS_SIGNING_TIMESTAMP_URL` (optional)
- `WINDOWS_SIGNING_FILE_DIGEST` (optional)
- `WINDOWS_SIGNING_TIMESTAMP_DIGEST` (optional)
- `WINDOWS_SIGNING_SUBJECT_NAME` (optional alternative to cert file)

If Windows signing secrets are absent, Windows artifacts are built unsigned and the release still succeeds.

## License

Pioneer is released under the [MIT License](./LICENSE).
