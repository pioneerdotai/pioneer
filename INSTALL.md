# Pioneer Install Guide

## Gateway-only (Remote Gateway)

Gateway install uses a single user mode (current OS user).

### macOS / Linux

```bash
curl -fsSL https://pioneer.ai/install.sh | bash
```

### Windows (PowerShell)

```powershell
iwr -useb https://pioneer.ai/install.ps1 | iex
```

### Windows (CMD)

```cmd
curl -fsSL https://pioneer.ai/install.cmd -o install.cmd && install.cmd && del install.cmd
```

## Desktop App Install

Desktop app install is separate from gateway scripts.

- macOS: install via `.dmg` and move app to Applications.
- Windows: install via `.msi` or `.exe` installer.
- Linux: install via `.AppImage`.

Then launch desktop and press `Start local gateway` when needed.

## Notes

- `install.sh`, `install.ps1`, and `install.cmd` are for gateway-only installation/update.
- Scripts support `--channel`, `--version`, `--no-start`, and `--force-start`.
- Scripts are bootstrap-only: they download/verify assets and then run native `pioneer install --source local`.
- Native installer flow is centralized in CLI (`pioneer install` / `pioneer update`) with:
  stop service -> atomic binary replace -> optional restart -> health check -> rollback on fail.
- Install path is user-local (`~/.local/share/pioneer/managed` on Linux/macOS, `%LOCALAPPDATA%\\Pioneer\\managed` on Windows).
- `pioneer` command is linked to the current user context (`~/.local/bin/pioneer` on Unix, user `Path` on Windows).
- After first install, open a new shell session so the updated user `PATH` is applied.
- If PATH profile update is skipped, gateway install/start still succeeds and service remains reachable.
- Optional manual PATH setup (Unix): `export PATH="$HOME/.local/bin:$PATH"` (or add it to your shell profile).
- CLI sources:
  - local bundle: `--source local --asset <path> --checksums <path>`
  - GitHub release: `--source release [--channel stable|beta|canary] [--version vX.Y.Z]`
- Desktop `Start local gateway` does not execute installer shell scripts.
- Desktop uses bundled `pioneer-bootstrap` + local assets/checksums and runs native `pioneer install`.

## Gateway DB (Turso/libSQL + SeaORM)

- Runtime database config is in `config/default.toml` under `[gateway.database]`.
- Gateway opens a SQLite-compatible URL (`sqlite://...`) and applies migrations on startup (when `run_migrations_on_startup = true`).
- Migration crate: `crates/migration`
- Entity crate: `crates/entity`

### Developer workflow

```bash
# Install CLI once (if missing)
cargo install sea-orm-cli

# 1) Set DATABASE_URL (example path from local config/runtime_home)
export DATABASE_URL="sqlite://$HOME/.pioneer.local/gateway.db?mode=rwc"

# 2) Apply migrations
cargo run -p pioneer-migration -- up

# 3) Regenerate entities from schema
sea-orm-cli generate entity \
  --lib \
  --with-serde both \
  --output-dir crates/entity/src \
  --entity-format dense \
  --ignore-tables seaql_migrations
```

## Release Signing (Desktop)

- Tag builds (`v*`) in `desktop-packages.yml` enforce signing/notarization.
- macOS secrets:
  - `MACOS_CERTIFICATE_P12_BASE64`
  - `MACOS_CERTIFICATE_PASSWORD`
  - `MACOS_DESKTOP_SIGN_IDENTITY`
  - `MACOS_DMG_SIGN_IDENTITY` (optional, defaults to desktop identity)
  - `APPLE_NOTARIZATION_KEY_ID`
  - `APPLE_NOTARIZATION_ISSUER_ID`
  - `APPLE_NOTARIZATION_KEY` or `APPLE_NOTARIZATION_KEY_BASE64`
- Windows secrets (certificate-based signing):
  - `WINDOWS_SIGNING_CERT_BASE64`
  - `WINDOWS_SIGNING_CERT_PASSWORD`
  - optional: `WINDOWS_SIGNING_TIMESTAMP_URL`, `WINDOWS_SIGNING_FILE_DIGEST`, `WINDOWS_SIGNING_TIMESTAMP_DIGEST`
  - optional alternative to cert file: `WINDOWS_SIGNING_SUBJECT_NAME`
  - if these secrets are absent, Windows artifacts are built unsigned (release still succeeds)
