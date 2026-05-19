<p align="center">
  <img src="assets/pioneer.png" alt="Pioneer" width="50">
</p>

<h1 align="center">Pioneer — Personal AI Assistant</h1>

<p align="center">
  <strong>You own the assistant. You own the data. You choose where the gateway runs.</strong>
</p>

<p align="center">
  <a href="https://github.com/pioneerdotai/pioneer/actions/workflows/ci.yml"><img src="https://github.com/pioneerdotai/pioneer/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/pioneerdotai/pioneer/releases"><img src="https://img.shields.io/github/v/release/pioneerdotai/pioneer?include_prereleases&label=release" alt="Release"></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License"></a>
  <img src="https://img.shields.io/badge/rust-edition%202024-orange.svg" alt="Rust edition 2024">
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

---

**Pioneer** is a local-first AI workspace for running an assistant on your own machine or on infrastructure you control. It combines a persistent gateway, a native desktop app, a JSON-RPC protocol, provider adapters, durable threads, agent memory, task automation, MCP servers, skills, and real local tools.

The gateway is the core of Pioneer. It owns state, configuration, storage, model access, task execution, tool execution, MCP runtime, skills, and thread history. The desktop app is the primary client for connecting to one or more gateways, whether the gateway is running on the same computer or on a remote server.

> **Early-stage warning**
>
> Pioneer is in an extremely early stage of development. Expect rough edges, breaking changes, and incomplete flows. Use it carefully, and test it in a safe environment before trusting it with important work or exposing a gateway outside your machine.
>
> Tool runs are not sandboxed yet. Tools currently execute as the OS user running the gateway.

## Highlights

- **Gateway-centered design** - all important work happens in the gateway: workspaces, threads, turns, tools, MCP, skills, tasks, provider settings, auth, and durable storage.
- **Local or remote deployment** - run a gateway on your personal computer for a local assistant, or host gateways on separate servers for work, study, home, or other isolated environments.
- **One desktop, many gateways** - connect the desktop app to any number of gateways and switch between them from one native client.
- **Workspace management** - create, switch, and rename workspaces inside a gateway; each workspace keeps its own threads, provider keys, MCP servers, skills, tasks, and artifacts.
- **Multi-agent workflows** - the gateway can automatically fan work out to subagents with their own prompts, roles, models, context policies, tool policies, result contracts, and child threads.
- **Durable agent memory** - agent mode can recall and write stable facts, preferences, recurring instructions, project decisions, and communication style through prompt policy, memory tools, proactive post-turn extraction, service-owned dedupe, and memvid-backed search capsules. It is configurable, quality-gated, and does not claim full transcript recall.
- **Typed hook runtime** - lifecycle hooks attach policy, context, prompt sections, tool bundles, diagnostics, and post-turn work without turning the agent loop into a domain-specific container.
- **Bring your own model** - built-in providers for OpenAI, Anthropic, OpenRouter, Gemini, Azure OpenAI, Bedrock, Ollama, Copilot, Claude Code, Gemini CLI, Kilo CLI, and many OpenAI-compatible endpoints.
- **Keystore-backed secrets** - workspace-scoped provider API keys, MCP env/header secrets, superuser JWT signing material, and desktop gateway bearer tokens are stored in `keystore.db` instead of ordinary TOML or domain tables.
- **Real tools** - shell sessions, file reads and edits, patch application, grep, web search/fetch, URL downloads, computer use, MCP tool proxying, and dynamic skill tools.
- **MCP servers** - install and manage servers compatible with [Model Context Protocol](https://modelcontextprotocol.io/docs/getting-started/intro) per gateway and per workspace, track their health and catalog, and expose their tools to agents through the gateway.
- **Skills** - compatible with the [Agent Skills specification](https://agentskills.io/home), with installation, validation, trust gates, dependency preflight, gateway/workspace policy, upload flow, and health diagnostics.
- **Tasks** - scheduled and on-demand task execution with dependencies, retries, delivery state, progress events, write locks, and task trees.
- **Thread modes** - use Chat mode for direct conversations and Agent mode when the thread should plan, use tools, and work through multi-step tasks.
- **Thread tree AGENTS.md** - define persistent instructions at the workspace root or any thread folder; child threads inherit the nearest active file and Pioneer injects it into the prompt through the hook runtime.
- **Protocol-first architecture** - `pioneer-protocol` defines the public JSON-RPC surface and generated schemas under `schemas/`.
- **Explicit workspace-scoped artifacts** - user uploads and agent-created result files are stored by the gateway, linked to workspace/thread/turn/message lineage, previewed when possible, and downloadable from local or remote gateways.
- **Cross-platform packaging** - gateway builds for macOS, Linux, and Windows; desktop packaging targets DMG, AppImage, and MSI.
- **Multi-language desktop** - desktop UI locales are available for English, German, Spanish, French, Hindi, Japanese, Russian, and Chinese.

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
