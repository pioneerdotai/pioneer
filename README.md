# Pioneer

<p align="center">
  <img src="assets/pioneer.png" alt="Pioneer" width="96">
</p>

<p align="center">
  <a href="https://github.com/pioneerdotai/pioneer/actions/workflows/ci.yml"><img src="https://github.com/pioneerdotai/pioneer/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License"></a>
</p>

**Pioneer** is a local-first AI workspace for running an assistant on your own machine or on infrastructure you control. It combines a persistent gateway, a native desktop app, a JSON-RPC protocol, provider adapters, durable threads, task automation, MCP servers, and real local tools.

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
- **Thread modes** - use Chat mode for direct conversations and Agent mode when the thread should plan, use tools, and work through multi-step tasks.
- **Multi-agent workflows** - the gateway can automatically fan work out to subagents with their own prompts, roles, models, context policies, tool policies, result contracts, and child threads.
- **Bring your own model** - built-in providers for OpenAI, Anthropic, OpenRouter, Gemini, Azure OpenAI, Bedrock, Ollama, Copilot, Claude Code, Gemini CLI, Kilo CLI, and many OpenAI-compatible endpoints.
- **Real tools** - shell sessions, file reads and edits, patch application, grep, web search/fetch, URL downloads, computer use, MCP tool proxying, and dynamic skill tools.
- **MCP servers** - install and manage servers compatible with [Model Context Protocol](https://modelcontextprotocol.io/docs/getting-started/intro) per gateway and per workspace, track their health and catalog, and expose their tools to agents through the gateway.
- **Skills** - compatible with the [Agent Skills specification](https://agentskills.io/home), with installation, validation, trust gates, dependency preflight, gateway/workspace policy, upload flow, and health diagnostics.
- **Tasks** - scheduled and on-demand task execution with dependencies, retries, delivery state, progress events, write locks, and task trees.
- **Protocol-first architecture** - `pioneer-protocol` defines the public JSON-RPC surface and generated schemas under `schemas/`.
- **Cross-platform packaging** - gateway builds for macOS, Linux, and Windows; desktop packaging targets DMG, AppImage, and MSI.
- **Multi-language desktop** - desktop UI locales are available for English, German, Spanish, French, Hindi, Japanese, Russian, and Chinese.

## Rust Native

Pioneer is built in Rust all the way through the product: gateway, CLI, desktop app, protocol, tools, tasks, MCP, skills, and provider integrations.

That keeps the core memory-safe, fast, and small. The desktop app is native GPUI, not Electron or a web app wrapped in a window.

## Gateway and Desktop

Pioneer is split into two parts:

- **Gateway** - the main runtime and control plane. It runs as a service, stores the data, talks to model providers, executes tools, manages MCP servers and skills, schedules tasks, and exposes the JSON-RPC WebSocket API.
- **Desktop app** - the primary native client. It connects to gateways, starts and manages a local gateway when needed, and gives you the UI for conversations, provider setup, MCP, skills, settings, and thread history.
- **Protocol clients** - any client can be built on top of the Pioneer JSON-RPC protocol. Native mobile apps for iOS and Android are planned next.

For a single-machine setup, install the desktop app and let it start the local gateway for you. On macOS, that means downloading the `.dmg`, moving Pioneer to Applications, launching it, and pressing `Start local gateway` when prompted.

For a multi-environment setup, install gateways wherever the work should live: a laptop, a workstation, a home server, or a remote machine. Then connect to each gateway from the same desktop app. You can keep separate gateways for work, study, home, experiments, or clients without mixing their state, settings, tools, and histories.

## Multi-Agent Workflows

Pioneer is designed for more than one long-running chat thread. The gateway can coordinate subagents automatically as part of the task system: a parent task can create an agent spec, start a child thread, run that subagent with its own model and instructions, and link the result back to the parent task.

Each subagent can be scoped independently:

- **Role and identity** - give the agent a role and nickname so its work is understandable in task history.
- **Model choice** - choose the model and provider per agent, instead of forcing every subtask through the same model.
- **Context policy** - inherit parent context, pass only recent turns, use a summary, start empty, or provide custom context.
- **Tool policy** - allow or deny tools, choose read-only or write-capable modes, restrict paths, and control network access.
- **Result contract** - ask for text, Markdown, JSON, or artifacts with required outputs.
- **Depth and lineage** - nested agent work is tracked through child threads and task lineage, so delegated work can be audited and recovered.

This makes the gateway useful as a coordinator: one agent can break work into subtasks, specialized subagents can handle pieces in isolated threads, and the desktop app can still show the full task tree from one place. Users can shape this with goals and policies, but the orchestration is handled by Pioneer.

## MCP Servers

The gateway is also the MCP runtime. It installs MCP servers, stores their configuration and secrets, validates definitions, starts and restarts runtimes, tracks health, and keeps a catalog of exposed tools, resources, resource templates, and prompts.

MCP is scoped by gateway and workspace, so different gateways and workspaces can have different external capabilities. A work gateway can connect to work systems, a home gateway can connect to personal automations, and experimental gateways can run separate MCP servers without leaking tools or secrets across environments.

Agents do not talk to MCP servers directly. They call gateway tools, and the gateway proxies MCP tool calls through its runtime, policy, audit, and redaction layers.

## Gateway Install

Use the gateway bootstrap scripts when you want to install or update the gateway directly, for example on a remote server or in a headless environment. Gateway installation uses a single user mode: the service runs as the current OS user.

macOS and Linux:

```bash
curl -fsSL https://pioneer.ai/install.sh | bash
```

Windows PowerShell:

```powershell
iwr -useb https://pioneer.ai/install.ps1 | iex
```

Windows CMD:

```cmd
curl -fsSL https://pioneer.ai/install.cmd -o install.cmd && install.cmd && del install.cmd
```

The bootstrap scripts download a release asset, verify checksums, and then run the native installer path through `pioneer install --source local`. The installer registers a user-level gateway service and exposes the `pioneer` command.

Install scripts support:

```bash
--channel stable|beta|canary
--version x.y.z
--no-start
--force-start
```

Channel/version selection depends on matching release assets being published for the target platform.

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

The same options are available through `pioneer update`. Release-based install/update requires a published gateway asset and matching `SHA256SUMS` for the current OS and architecture.

## Installer Notes

- Native install/update flow is centralized in the CLI: stop service, atomically replace binary, optionally restart, run health check, rollback on failure.
- Production install path is user-local: `~/.local/share/pioneer/managed` on Linux, `~/.local/share/Pioneer/managed` on macOS, `%LOCALAPPDATA%\Pioneer\managed` on Windows.
- Development install path uses `config/local.toml`: `~/.local/share/pioneer-dev/managed-dev` on Linux, `~/.local/share/PioneerDev/managed-dev` on macOS, `%LOCALAPPDATA%\PioneerDev\managed-dev` on Windows.
- Production links `pioneer`; development links `pioneer-dev` (`~/.local/bin/pioneer-dev` on Unix, user `Path` on Windows).
- Production service name: `com.pioneer.gateway`.
- Development service name: `com.pioneer.gateway.dev`.
- Production listens on `0.0.0.0:17878` by default.
- Development listens on `0.0.0.0:18778` by default.

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

## Repository Map

| Path | Purpose |
| --- | --- |
| `crates/gateway` | Long-running gateway service, transport, auth, bootstrap, sessions, workspaces, threads, MCP, skills, tasks, and message dispatch. |
| `crates/desktop` | Native GPUI desktop app (`pioneer-app`). |
| `crates/cli` | `pioneer` and `pioneer-dev` binaries, installer, updater, service manager, status, and token issuance. |
| `crates/agent` | Agent turn execution and tool orchestration. |
| `crates/protocol` | JSON-RPC request, response, notification, thread, turn, task, MCP, skill, provider, and workspace types. |
| `crates/provider` | LLM provider abstraction, adapters, model listing, streaming, tool calls, and attachment handling. |
| `crates/tools` | Built-in tool specs, runtime, routing, output policy, recovery, and handlers. |
| `crates/mcp` | MCP client runtime, catalog, validation, policies, secrets, and redaction. |
| `crates/skills` | Skill installation, validation, provenance, runtime tool registration, trust, and dependency checks. |
| `crates/tasks` | Task scheduler, executor, triggers, delivery, reconciliation, notifications, and event projection. |
| `crates/crud`, `crates/entity`, `crates/migration`, `crates/sqlite` | Persistence layer backed by SQLite/libSQL and SeaORM. |
| `crates/config` | Layered configuration loader and runtime path conventions. |
| `crates/promt` | Prompt bundle compilation, source budgeting, sanitization, rendering, diagnostics, and snapshots. |
| `schemas` | Generated JSON Schemas for the public protocol. |
| `scripts` | Packaging helpers, schema export, and development environment reset. |

## Checks

The CI workflow runs the core checks below:

```bash
cargo fmt --all --check
cargo check --workspace
cargo test -p pioneer-cli
cargo test -p pioneer-desktop gateway::
```

## License

Pioneer is released under the [MIT License](./LICENSE).
