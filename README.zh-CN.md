<p align="center">
  <img src="assets/pioneer.png" alt="Pioneer" width="50">
</p>

<h1 align="center">Pioneer — 个人 AI 助手</h1>

<p align="center">
  <strong>掌控你的助手。掌控你的数据。自由决定网关运行位置。</strong>
</p>

<p align="center">
  <a href="https://github.com/pioneerdotai/pioneer/releases"><img src="https://img.shields.io/github/v/release/pioneerdotai/pioneer?include_prereleases&label=release" alt="Release"></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License"></a>
</p>

<p align="center">
  <a href="https://docs.getpioneer.dev">文档</a>
  ·
  <a href="https://docs.getpioneer.dev/getting-started/installation">安装指南</a>
  ·
  <a href="https://docs.getpioneer.dev/getting-started/quickstart">快速上手</a>
  ·
  <a href="https://docs.getpioneer.dev/architecture/overview">架构设计</a>
  ·
  <a href="https://docs.getpioneer.dev/protocol/introduction">协议参考</a>
  ·
  <a href="https://github.com/pioneerdotai/pioneer/releases">版本发布</a>
</p>

<p align="center">
  <a href="README.md">English</a>
  ·
  <a href="README.zh-CN.md">简体中文</a>
</p>

<p align="center">
  <img src="assets/screenshots/pioneer-main-dark.png" alt="Pioneer">
  <img src="assets/screenshots/pioneer-main-light.png" alt="Pioneer">
</p>

**Pioneer** 是一个本地优先（Local-First）的 AI 工作空间，专为在你的个人电脑或自控基础设施上运行专属智能助手而设计。它整合了持久化网关、原生桌面应用、JSON-RPC 协议、模型提供商适配器、持久化会话线程、智能体记忆、任务自动化、MCP 服务器、技能扩展（Skills）以及真实的本地系统工具。

网关（Gateway）是 Pioneer 的核心。它统一管理系统状态、全局配置、数据存储、模型接入、任务执行、工具调度、MCP 运行时、技能管理以及会话历史。桌面应用则是连接网关的主要客户端，无论网关是运行在当前电脑上还是远程服务器上，均可通过桌面应用连接并管理一个或多个网关。

> **早期版本提示**
>
> Pioneer 目前仍处于极早期的活跃开发阶段。可能存在不完善之处、破坏性变更或尚未完备的工作流。请谨慎使用，在将其用于重要工作或将网关暴露到机器外部之前，请务必在安全环境中充分测试。
>
> 工具调用目前尚未沙箱化。当前所有工具均以运行网关服务的操作系统用户权限执行。

## 特性亮点

<table>
  <colgroup>
    <col width="30%">
    <col width="70%">
  </colgroup>
  <tbody>
    <tr>
      <td><strong>网关中心化设计</strong></td>
      <td>所有核心业务均在网关中运行：工作区、会话线程、执行轮次、工具调用、MCP、技能、任务自动化、模型提供商设置、鉴权认证以及持久化存储。<br><br>相关文档：<a href="https://docs.getpioneer.dev/getting-started/concepts">用户指南</a> · <a href="https://docs.getpioneer.dev/architecture/gateway">架构文档</a></td>
    </tr>
    <tr>
      <td><strong>本地或远程部署</strong></td>
      <td>可以在个人电脑上运行网关作为本地助手，也可将网关部署在独立服务器上，以便在工作、学习、家庭或其它隔离环境中独立使用。<br><br>相关文档：<a href="https://docs.getpioneer.dev/getting-started/installation">用户指南</a> · <a href="https://docs.getpioneer.dev/architecture/overview">架构文档</a></td>
    </tr>
    <tr>
      <td><strong>单一桌面，多网关连接</strong></td>
      <td>桌面端应用可连接任意数量的网关，在一个原生客户端中随时自由切换。<br><br>相关文档：<a href="https://docs.getpioneer.dev/desktop/overview">用户指南</a> · <a href="https://docs.getpioneer.dev/architecture/gateway">架构文档</a></td>
    </tr>
    <tr>
      <td><strong>工作区管理</strong></td>
      <td>在网关内创建、切换和重命名工作区；每个工作区独立维护自己的会话线程、模型密钥、MCP 服务器、技能、任务和生成产物。<br><br>相关文档：<a href="https://docs.getpioneer.dev/desktop/workspace">用户指南</a></td>
    </tr>
    <tr>
      <td><strong>多智能体协作工作流</strong></td>
      <td>网关可自动将工作分发给具备独立提示词、角色、模型、上下文策略、工具权限、结果契约和子会话的子智能体（Subagents）。主智能体负责审阅子智能体的执行成果，决定采纳或提出具体反馈要求其修订。<br><br>相关文档：<a href="https://docs.getpioneer.dev/tasks/overview">用户指南</a> · <a href="https://docs.getpioneer.dev/architecture/tasks">架构文档</a></td>
    </tr>
    <tr>
      <td><strong>持久化智能体记忆</strong></td>
      <td>Agent 模式能够通过提示词策略、记忆工具、轮次后主动提取、服务级去重以及基于 memvid 的检索胶囊，记录和回忆稳定事实、用户偏好、周期性指令、项目决策和沟通风格。记忆系统支持灵活配置与质量门禁（Quality Gate），且专注于提炼高价值记忆而非无差别保留全部对话原文。<br><br>相关文档：<a href="https://docs.getpioneer.dev/desktop/memory">用户指南</a> · <a href="https://docs.getpioneer.dev/architecture/memory">架构文档</a></td>
    </tr>
    <tr>
      <td><strong>强类型钩子运行时</strong></td>
      <td>生命周期钩子（Hooks）可在不将 Agent 执行循环耦合为特定领域容器的前提下，注入策略、上下文、提示词分段、工具包、诊断信息和轮次后处理。<br><br>相关文档：<a href="https://docs.getpioneer.dev/architecture/hooks">架构文档</a></td>
    </tr>
    <tr>
      <td><strong>自由接入各类模型</strong></td>
      <td>内置支持 OpenAI、Anthropic、OpenRouter、Gemini、Azure OpenAI、Bedrock、Ollama、Copilot、Claude Code 以及多种兼容 OpenAI 规范的模型端点。<br><br>相关文档：<a href="https://docs.getpioneer.dev/providers/overview">用户指南</a> · <a href="https://docs.getpioneer.dev/architecture/providers">架构文档</a></td>
    </tr>
    <tr>
      <td><strong>密钥库安全存储</strong></td>
      <td>工作区范围的模型 API Key、MCP 环境变量/请求头机密、用途分离的网关认证密钥以及桌面设备会话刷新凭据，均存储在 <code>keystore.db</code> 中，而非明文存放在普通 TOML 或业务数据表中。<br><br>相关文档：<a href="https://docs.getpioneer.dev/architecture/secrets">架构文档</a></td>
    </tr>
    <tr>
      <td><strong>真实系统工具</strong></td>
      <td>智能体可通过网关调用 Shell 终端会话、文件读取与编辑、Patch 补丁应用、正则搜索（Grep）、网页搜索/内容抓取、URL 下载、屏幕控制（Computer Use）、MCP 工具代理以及动态技能工具。<br><br>相关文档：<a href="https://docs.getpioneer.dev/architecture/tools">架构文档</a></td>
    </tr>
    <tr>
      <td><strong>MCP 服务器支持</strong></td>
      <td>在网关和工作区级别安装和管理兼容 <a href="https://modelcontextprotocol.io/docs/getting-started/intro">Model Context Protocol</a> 的服务器，监控其健康状况与工具目录，并将工具暴露给智能体调用。<br><br>相关文档：<a href="https://docs.getpioneer.dev/mcp/overview">用户指南</a> · <a href="https://docs.getpioneer.dev/architecture/mcp">架构文档</a></td>
    </tr>
    <tr>
      <td><strong>技能扩展 (Skills)</strong></td>
      <td>技能与 <a href="https://agentskills.io/home">Agent Skills 规范</a> 完全兼容，支持安装、校验、信任门禁、依赖预检、网关/工作区策略、上传流程及健康诊断。<br><br>相关文档：<a href="https://docs.getpioneer.dev/skills/overview">用户指南</a> · <a href="https://docs.getpioneer.dev/architecture/skills">架构文档</a></td>
    </tr>
    <tr>
      <td><strong>任务自动化</strong></td>
      <td>支持定时计划与按需触发的任务执行引擎，具备依赖编排、自动重试、交付状态、进度事件、写入锁以及任务树结构。<br><br>相关文档：<a href="https://docs.getpioneer.dev/tasks/overview">用户指南</a> · <a href="https://docs.getpioneer.dev/architecture/tasks">架构文档</a></td>
    </tr>
    <tr>
      <td><strong>会话线程模式</strong></td>
      <td>支持用于直接对话的 Chat 模式，以及用于规划、调用工具和执行复杂多步骤任务的 Agent 模式。<br><br>相关文档：<a href="https://docs.getpioneer.dev/getting-started/highlights">用户指南</a> · <a href="https://docs.getpioneer.dev/architecture/agent-loop">架构文档</a></td>
    </tr>
    <tr>
      <td><strong>树状 AGENTS.md 指令继承</strong></td>
      <td>可在工作区根目录或任意线程目录中定义持久化指令文件；子线程会自动继承距离最近的有效文件，并通过钩子运行时将其注入提示词中。<br><br>相关文档：<a href="https://docs.getpioneer.dev/desktop/agents-md">用户指南</a> · <a href="https://docs.getpioneer.dev/architecture/agents-md">架构文档</a></td>
    </tr>
    <tr>
      <td><strong>协议优先架构</strong></td>
      <td><code>pioneer-protocol</code> 定义了公开的 JSON-RPC 协议面，并在 <code>schemas/</code> 目录下生成完整的强类型 Schema。<br><br>相关文档：<a href="https://docs.getpioneer.dev/architecture/protocol">架构文档</a></td>
    </tr>
    <tr>
      <td><strong>显式工作区产物管理</strong></td>
      <td>用户上传及智能体生成的成果文件均由网关集中存储，并与工作区/线程/轮次/消息建立清晰的血缘追踪，支持文件在线预览以及从本地或远程网关下载。<br><br>相关文档：<a href="https://docs.getpioneer.dev/desktop/artifacts">用户指南</a> · <a href="https://docs.getpioneer.dev/architecture/artifacts">架构文档</a></td>
    </tr>
    <tr>
      <td><strong>跨平台打包支持</strong></td>
      <td>网关构建支持 macOS、Linux 和 Windows；桌面客户端打包覆盖 DMG、AppImage 和 MSI。<br><br>相关文档：<a href="https://docs.getpioneer.dev/getting-started/installation">用户指南</a></td>
    </tr>
    <tr>
      <td><strong>多语言桌面界面</strong></td>
      <td>桌面端界面目前已支持英语、德语、西班牙语、法语、印地语、日语、俄语和中文界面切换。<br><br>相关文档：<a href="https://docs.getpioneer.dev/desktop/overview">用户指南</a></td>
    </tr>
  </tbody>
</table>

## 100% 纯 Rust 实现

Pioneer 在产品的各个层面上均采用 Rust 编写：网关、CLI、桌面端应用、通信协议、工具、任务系统、MCP、技能以及模型提供商集成。

这使得核心系统具备内存安全、高性能和轻量资源占用的优势。桌面端采用原生 **GPUI** 框架，而非 Electron 或套壳网页应用。

## 网关与桌面端架构

Pioneer 架构上拆分为两部分：

- **网关 (Gateway)** —— 核心运行时与控制平面。它作为常驻服务运行，负责存储数据、与大模型提供商通信、执行系统工具、管理 MCP 服务器与技能、调度自动化任务，并对外暴露 JSON-RPC WebSocket API。
- **桌面端应用 (Desktop App)** —— 主要的原生客户端。用于连接网关，在需要时可在本地启动并管理本地网关，并为工作区、对话、模型配置、MCP、技能、系统设置以及会话历史提供图形界面。
- **协议客户端 (Protocol Clients)** —— 任何客户端均可基于 Pioneer JSON-RPC 协议进行构建。iOS 和 Android 的原生移动应用已在后续规划中。

对于单机个人使用，直接安装桌面端应用即可，桌面端会在需要时自动为你启动本地网关。在 macOS 上，只需下载 `.dmg` 安装包，将 Pioneer 移动到“应用程序”文件夹，启动它，并在提示时点击 `Start local gateway`（启动本地网关）。

对于多环境使用，可以在任务实际运行的任意位置部署网关：笔记本电脑、工作站、家庭服务器或远程云主机。随后从同一个桌面端应用连接所有网关。你可以为工作、学习、家庭、实验或客户独立保留专属网关，而不会混淆它们的状态、配置、工具和历史记录。

## 网关安装

如果你希望直接安装或更新网关（例如在远程服务器或无头 headless 环境中），可以使用网关引导脚本。网关安装使用单用户模式：服务将以当前操作系统用户身份运行。

macOS 与 Linux：

```bash
curl -fsSL https://getpioneer.dev/install.sh | bash
```

仅安装支持计算机屏幕控制（computer-use）的原生网关变体（不安装桌面端）：

```bash
curl -fsSL https://getpioneer.dev/install.sh | bash -s -- --computer-use
```

Windows PowerShell：

```powershell
iwr -useb https://getpioneer.dev/install.ps1 | iex
```

仅安装支持计算机屏幕控制（computer-use）的原生网关变体（不安装桌面端）：

```powershell
$env:PIONEER_INSTALL_COMPUTER_USE="1"; iwr -useb https://getpioneer.dev/install.ps1 | iex
```

Windows CMD：

```cmd
curl -fsSL https://getpioneer.dev/install.cmd -o install.cmd && install.cmd && del install.cmd
```

引导脚本会自动下载发布资产、验证校验和（checksums），并通过 `pioneer install --source local` 执行原生安装流程。安装程序会注册用户级网关服务并配置 `pioneer` 命令行工具。

在 Linux 上，网关被安装为 `systemd --user` 用户级服务。对于服务器和无头安装，必须允许该服务在没有活跃登录会话的情况下持续运行。安装程序会尝试为当前用户检查并启用 systemd 延迟驻留（lingering）；如果操作系统拒绝了该操作，请在服务器上执行一次以下命令，然后重新运行安装程序：

```bash
sudo loginctl enable-linger "$USER"
```

在 macOS 上，网关被安装为当前用户的 LaunchAgent。在 Windows 上，网关被安装为在登录时触发的当前用户计划任务（Scheduled Task）。这些模式以当前用户身份运行并在用户登录后自动启动；它们不是在登录前启动的系统级 LaunchDaemon 或 Windows 系统服务。

安装脚本支持以下参数：

```bash
--channel stable|beta|canary
--version x.y.z
--computer-use
--headless
--no-start
--force-start
```

版本通道、指定版本以及网关变体的选择，取决于目标平台是否发布了对应的 release 构建产物。

首次安装后，请打开新的终端会话以便加载更新后的 `PATH` 环境变量。即使跳过了自动 PATH 更新，网关的安装和启动依然能成功，服务仍可正常访问。

可选的手动 Unix PATH 配置：

```bash
export PATH="$HOME/.local/bin:$PATH"
```

## 网关网络监听与绑定

默认情况下，生产版网关监听 `0.0.0.0:17878`，因此在主机防火墙放行的情况下，可以在其他机器上访问服务器安装的网关：

```toml
[gateway]
listen_addr = "0.0.0.0:17878"
```

若需限制为仅限当前机器本地访问，可修改用户持久化配置文件：

```toml
[gateway]
listen_addr = "127.0.0.1:17878"
```

配置文件路径：

- Linux: `~/.config/pioneer/config.toml`
- macOS: `~/Library/Application Support/pioneer/config.toml`
- Windows: `%APPDATA%\pioneer\config.toml`

修改监听地址后，请重启网关服务。若需外部访问，请确保在主机防火墙中放行 TCP `17878` 端口。

## 桌面端安装

如果你只打算在本地电脑上使用 Pioneer，从这里开始即可。安装桌面应用，启动它，并在需要时让其自动安装和启动本地网关：

- macOS：通过 `.dmg` 安装并将应用拖移至“应用程序”文件夹。
- Windows：通过 `.msi` 或 `.exe` 安装。
- Linux：通过 `.AppImage` 安装运行。

桌面端同样可以连接远程网关。你可以将其作为管理所有 Pioneer 网关（本地、工作、学习、家庭及各类服务器环境）的统一控制面板。

桌面应用不会执行安装脚本 shell。对于本地网关配置，它使用内置的 `pioneer-bootstrap` 及本地构建资产与校验和，直接运行原生 `pioneer install` 流程。

## CLI 常用命令参考

安装后的常用命令：

```bash
pioneer status                  # 检查服务状态与网关可达性
pioneer start                   # 启动网关服务
pioneer device create           # 创建待激活的设备会话并输出一次性激活码
pioneer secrets status          # 查看密钥库状态（不会输出机密明文）
pioneer secrets garbage-collection --dry-run
pioneer update                  # 使用已配置的发布源进行更新
pioneer stop                    # 停止并注销网关服务
pioneer version
pioneer help
```

CLI 安装程序可解析本地构建包或远端发布资产：

```bash
pioneer install --source local --asset <path> --checksums <path>
pioneer install --source release --channel stable
```

`pioneer update` 也支持相同的选项。基于 release 的安装与更新需要当前操作系统、架构及网关变体已发布对应构建资产并具备匹配的 `SHA256SUMS`。无头网关通过标准资产名称更新；支持 computer-use 的网关通过 `-computer-use` 资产名称更新。

## 安全注意事项

启用后，Pioneer 工具可执行系统命令、读写文件、访问网络并控制桌面。请将网关视为具有特权的本地服务对待。

目前工具执行**尚未提供独立沙箱**。所有工具调用均以运行网关服务的操作系统用户账户权限执行。

在将网关绑定到非本地网络接口之前，请确保该访问符合预期、已正确配置主机防火墙，并且每个客户端都需激活由网关生成的待激活设备会话。

## 源码编译构建

本仓库是一个 Rust 工作区（workspace），并在 `rust-toolchain.toml` 中锁定了稳定版工具链。

```bash
git clone https://github.com/pioneerdotai/pioneer.git
cd pioneer

cargo build --workspace
cargo run -p pioneer-gateway
cargo run -p pioneer-desktop --bin pioneer-app
cargo run -p pioneer-cli -- help
```

开发构建会加载 `config/local.toml`，使用 `~/.pioneer.dev` 目录，暴露 `pioneer-dev` 命令，并默认监听端口 `18778`：

```bash
cargo run -p pioneer-cli --features dev --bin pioneer-dev -- status
./scripts/reset-pioneer-dev-env.sh
```

## 发布包签名

协同的 Gateway/Desktop/Pioneer App 边缘发布与 registry-v3 恢复检查清单记录在文档 [Gateway Edge Breaking Release](docs/gateway-edge-release.md) 中。

配置了签名信息的环境下，带标签的桌面端发布构建将强制执行签名与公证（notarization）。

macOS 签名机密：

- `MACOS_CERTIFICATE_P12_BASE64`
- `MACOS_CERTIFICATE_PASSWORD`
- `MACOS_DESKTOP_SIGN_IDENTITY`
- `MACOS_DMG_SIGN_IDENTITY`（可选，默认使用桌面端身份）
- `APPLE_NOTARIZATION_KEY_ID`
- `APPLE_NOTARIZATION_ISSUER_ID`
- `APPLE_NOTARIZATION_KEY` 或 `APPLE_NOTARIZATION_KEY_BASE64`

Windows 签名机密：

- `WINDOWS_SIGNING_CERT_BASE64`
- `WINDOWS_SIGNING_CERT_PASSWORD`
- `WINDOWS_SIGNING_TIMESTAMP_URL`（可选）
- `WINDOWS_SIGNING_FILE_DIGEST`（可选）
- `WINDOWS_SIGNING_TIMESTAMP_DIGEST`（可选）
- `WINDOWS_SIGNING_SUBJECT_NAME`（证书文件的可选替代方案）

若未提供 Windows 签名机密，生成的 Windows 构件将不包含签名，发布流程仍会正常完成。

## 开源协议

Pioneer 基于 [MIT License](./LICENSE) 开源发布。
