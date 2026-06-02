---
name: browser
slug: browser
owner: pioneer
description: "Headless web browsing: navigation, page inspection, interaction, data extraction, screenshots, PDFs, and browser session management."
version: "0.1.0"
compatibility: Requires the agent-browser CLI plus Node.js, npm, and npx for installation and browser dependencies.
allowed-tools: Bash(agent-browser:*)
user-invocable: true
disable-model-invocation: false
---

# Browser Automation with agent-browser

## Installation

For normal browser work, run the requested `agent-browser ...` command directly. Do not run setup checks before every browser action.

Run setup only when provisioning the skill, when `agent-browser` is missing, or when browser dependencies need repair. If Node.js and npm are already installed, do not reinstall or replace them. Use the OS-specific Node.js install block only when `node` or `npm` is missing, then install `agent-browser`.

### macOS and Linux

```bash
# Download and install nvm:
if ! command -v node >/dev/null 2>&1 || ! command -v npm >/dev/null 2>&1; then
  curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.4/install.sh | bash

  # In lieu of restarting the shell:
  \. "$HOME/.nvm/nvm.sh"

  # Download and install Node.js:
  nvm install 24
fi

# Verify the Node.js version:
node -v # Should print the active Node.js version.

# Verify npm version:
npm -v # Should print the active npm version.
```

### Windows

```powershell
# Download and install Chocolatey:
if (-not (Get-Command node -ErrorAction SilentlyContinue) -or -not (Get-Command npm -ErrorAction SilentlyContinue)) {
  if (-not (Get-Command choco -ErrorAction SilentlyContinue)) {
    powershell -c "irm https://community.chocolatey.org/install.ps1|iex"
  }

  # Download and install Node.js:
  choco install nodejs --version="24.16.0" -y
}

# Verify the Node.js version:
node -v # Should print the active Node.js version.

# Verify npm version:
npm -v # Should print the active npm version.
```

### Install agent-browser

```bash
npm install -g agent-browser
agent-browser install
agent-browser install --with-deps
```

### From Source

```bash
git clone https://github.com/vercel-labs/agent-browser
cd agent-browser
pnpm install
pnpm build
agent-browser install
```

## Quick start

```bash
agent-browser open <url>        # Navigate to page
agent-browser snapshot -i       # Get interactive elements with refs
agent-browser click @e1         # Click element by ref
agent-browser fill @e2 "text"   # Fill input by ref
agent-browser close             # Close browser
```

## Core workflow

1. Navigate: `agent-browser open <url>`
2. Snapshot: `agent-browser snapshot -i` (returns elements with refs like `@e1`, `@e2`)
3. Interact using refs from the snapshot
4. Re-snapshot after navigation or significant DOM changes

## Commands

### Navigation

```bash
agent-browser open <url>      # Navigate to URL
agent-browser back            # Go back
agent-browser forward         # Go forward
agent-browser reload          # Reload page
agent-browser close           # Close browser
```

### Snapshot (page analysis)

```bash
agent-browser snapshot            # Full accessibility tree
agent-browser snapshot -i         # Interactive elements only (recommended)
agent-browser snapshot -c         # Compact output
agent-browser snapshot -d 3       # Limit depth to 3
agent-browser snapshot -s "#main" # Scope to CSS selector
```

### Interactions (use @refs from snapshot)

```bash
agent-browser click @e1           # Click
agent-browser dblclick @e1        # Double-click
agent-browser focus @e1           # Focus element
agent-browser fill @e2 "text"     # Clear and type
agent-browser type @e2 "text"     # Type without clearing
agent-browser press Enter         # Press key
agent-browser press Control+a     # Key combination
agent-browser keydown Shift       # Hold key down
agent-browser keyup Shift         # Release key
agent-browser hover @e1           # Hover
agent-browser check @e1           # Check checkbox
agent-browser uncheck @e1         # Uncheck checkbox
agent-browser select @e1 "value"  # Select dropdown
agent-browser scroll down 500     # Scroll page
agent-browser scrollintoview @e1  # Scroll element into view
agent-browser drag @e1 @e2        # Drag and drop
agent-browser upload @e1 file.pdf # Upload files
```

### Get information

```bash
agent-browser get text @e1        # Get element text
agent-browser get html @e1        # Get innerHTML
agent-browser get value @e1       # Get input value
agent-browser get attr @e1 href   # Get attribute
agent-browser get title           # Get page title
agent-browser get url             # Get current URL
agent-browser get count ".item"   # Count matching elements
agent-browser get box @e1         # Get bounding box
```

### Check state

```bash
agent-browser is visible @e1      # Check if visible
agent-browser is enabled @e1      # Check if enabled
agent-browser is checked @e1      # Check if checked
```

### Screenshots & PDF

```bash
agent-browser screenshot          # Screenshot to stdout
agent-browser screenshot path.png # Save to file
agent-browser screenshot --full   # Full page
agent-browser pdf output.pdf      # Save as PDF
```

### Video recording

```bash
agent-browser record start ./demo.webm    # Start recording (uses current URL + state)
agent-browser click @e1                   # Perform actions
agent-browser record stop                 # Stop and save video
agent-browser record restart ./take2.webm # Stop current + start new recording
```

Recording creates a fresh context but preserves cookies/storage from your session. If no URL is provided, it automatically returns to your current page. For smooth demos, explore first, then start recording.

### Wait

```bash
agent-browser wait @e1                     # Wait for element
agent-browser wait 2000                    # Wait milliseconds
agent-browser wait --text "Success"        # Wait for text
agent-browser wait --url "/dashboard"      # Wait for URL pattern
agent-browser wait --load networkidle      # Wait for network idle
agent-browser wait --fn "window.ready"     # Wait for JS condition
```

### Mouse control

```bash
agent-browser mouse move 100 200      # Move mouse
agent-browser mouse down left         # Press button
agent-browser mouse up left           # Release button
agent-browser mouse wheel 100         # Scroll wheel
```

### Semantic locators (alternative to refs)

```bash
agent-browser find role button click --name "Submit"
agent-browser find text "Sign In" click
agent-browser find label "Email" fill "user@test.com"
agent-browser find first ".item" click
agent-browser find nth 2 "a" text
```

### Browser settings

```bash
agent-browser set viewport 1920 1080      # Set viewport size
agent-browser set device "iPhone 14"      # Emulate device
agent-browser set geo 37.7749 -122.4194   # Set geolocation
agent-browser set offline on              # Toggle offline mode
agent-browser set headers '{"X-Key":"v"}' # Extra HTTP headers
agent-browser set credentials user pass   # HTTP basic auth
agent-browser set media dark              # Emulate color scheme
```

### Cookies & Storage

```bash
agent-browser cookies                     # Get all cookies
agent-browser cookies set name value      # Set cookie
agent-browser cookies clear               # Clear cookies
agent-browser storage local               # Get all localStorage
agent-browser storage local key           # Get specific key
agent-browser storage local set k v       # Set value
agent-browser storage local clear         # Clear all
```

### Network

```bash
agent-browser network route <url>              # Intercept requests
agent-browser network route <url> --abort      # Block requests
agent-browser network route <url> --body '{}'  # Mock response
agent-browser network unroute [url]            # Remove routes
agent-browser network requests                 # View tracked requests
agent-browser network requests --filter api    # Filter requests
```

### Tabs & Windows

```bash
agent-browser tab                 # List tabs
agent-browser tab new [url]       # New tab
agent-browser tab 2               # Switch to tab
agent-browser tab close           # Close tab
agent-browser window new          # New window
```

### Frames

```bash
agent-browser frame "#iframe"     # Switch to iframe
agent-browser frame main          # Back to main frame
```

### Dialogs

```bash
agent-browser dialog accept [text]  # Accept dialog
agent-browser dialog dismiss        # Dismiss dialog
```

### JavaScript

```bash
agent-browser eval "document.title"   # Run JavaScript
```

### State management

```bash
agent-browser state save auth.json    # Save session state
agent-browser state load auth.json    # Load saved state
```

## Example: Form submission

```bash
agent-browser open https://example.com/form
agent-browser snapshot -i
# Output shows: textbox "Email" [ref=e1], textbox "Password" [ref=e2], button "Submit" [ref=e3]

agent-browser fill @e1 "user@example.com"
agent-browser fill @e2 "password123"
agent-browser click @e3
agent-browser wait --load networkidle
agent-browser snapshot -i  # Check result
```

## Example: Authentication with saved state

```bash
# Login once
agent-browser open https://app.example.com/login
agent-browser snapshot -i
agent-browser fill @e1 "username"
agent-browser fill @e2 "password"
agent-browser click @e3
agent-browser wait --url "/dashboard"
agent-browser state save auth.json

# Later sessions: load saved state
agent-browser state load auth.json
agent-browser open https://app.example.com/dashboard
```

## Sessions (parallel browsers)

```bash
agent-browser --session test1 open site-a.com
agent-browser --session test2 open site-b.com
agent-browser session list
```

## JSON output (for parsing)

Add `--json` for machine-readable output:

```bash
agent-browser snapshot -i --json
agent-browser get text @e1 --json
```

## Debugging

```bash
agent-browser open example.com --headed              # Show browser window
agent-browser console                                # View console messages
agent-browser console --clear                        # Clear console
agent-browser errors                                 # View page errors
agent-browser errors --clear                         # Clear errors
agent-browser highlight @e1                          # Highlight element
agent-browser trace start                            # Start recording trace
agent-browser trace stop trace.zip                   # Stop and save trace
agent-browser record start ./debug.webm              # Record from current page
agent-browser record stop                            # Save recording
agent-browser --cdp 9222 snapshot                    # Connect via CDP
```

## Troubleshooting

- If the command is not found on Linux ARM64, use the full path in the bin folder.
- If an element is not found, use snapshot to find the correct ref.
- If the page is not loaded, add a wait command after navigation.
- Use --headed to see the browser window for debugging.

### Linux Chrome shared-library failure

On Linux, if `agent-browser open ...` fails with Chrome exit code `127` and stderr like `error while loading shared libraries: <library>: cannot open shared object file`, Chrome for Testing is installed but required system libraries are missing.

First run the built-in dependency installer:

```bash
agent-browser install --with-deps
agent-browser doctor
```

If dependency installation fails, use the package-manager commands and missing-library diagnostics printed by `agent-browser install --with-deps` on that host.

After installing dependencies, retry without screenshots:

```bash
agent-browser open https://example.com
agent-browser get title
agent-browser close
```

If Chrome then fails with `No usable sandbox` or `Permission denied (13)`, fix AppArmor next.

### AppArmor Chrome sandbox failure on Linux

On AppArmor-restricted Linux hosts, Chrome for Testing can fail with `Permission denied (13)` when launching its sandbox. Ubuntu 24.04 commonly exposes this because `kernel.apparmor_restrict_unprivileged_userns=1` is enabled and Chrome for Testing may live in a non-standard path.

Do not leave `--no-sandbox` as the permanent fix. Add a narrow AppArmor profile for the Chrome binaries installed by `agent-browser`, load it directly with `apparmor_parser`, then remove any temporary `--no-sandbox` workaround once the browser launches.

Create and load the AppArmor profile. Use a path glob so the profile survives Chrome for Testing updates installed under new `chrome-*` directories.

```bash
sudo tee /etc/apparmor.d/agent-browser-chrome >/dev/null <<'EOF'
abi <abi/4.0>,
include <tunables/global>

profile agent-browser-chrome @{HOME}/.agent-browser/browsers/chrome-*/chrome flags=(unconfined) {
  userns,
  include if exists <local/agent-browser-chrome>
}
EOF

sudo systemctl start apparmor 2>/dev/null || true
sudo apparmor_parser -r /etc/apparmor.d/agent-browser-chrome
```

Validate with a real launch:

```bash
agent-browser close --all || true
agent-browser doctor
agent-browser open https://example.com
agent-browser get title
agent-browser close
```

## Options

- --session <name> uses an isolated session.
- --json provides JSON output.
- --full takes a full page screenshot.
- --headed shows the browser window.
- --timeout sets the command timeout in milliseconds.
- --cdp <port> connects via Chrome DevTools Protocol.

## Notes

- Refs are stable per page load but change on navigation.
- Always snapshot after navigation to get new refs.
- Use fill instead of type for input fields to ensure existing text is cleared.

## Reporting Issues

- agent-browser CLI issues: Open an issue at https://github.com/vercel-labs/agent-browser
