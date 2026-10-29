#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  check-package-contents.sh macos-app-zip <Pioneer-arch.app.zip> <rust-target>
  check-package-contents.sh linux-appdir <Pioneer.AppDir> <rust-target>
  check-package-contents.sh windows-stage <stage-dir> <rust-target>
USAGE
}

require_path() {
  local path="$1"
  if [[ ! -e "$path" ]]; then
    echo "missing required package content: $path" >&2
    exit 1
  fi
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "required command not found: $cmd" >&2
    exit 1
  fi
}

require_zip_entry() {
  local zip_path="$1"
  local pattern="$2"
  local entries
  # Capture the listing before matching: `unzip | grep -q` races grep's early
  # exit against unzip's writes, and the resulting SIGPIPE fails the pipeline
  # under `set -o pipefail` even when the entry exists.
  entries="$(unzip -Z1 "$zip_path")" || {
    echo "failed to list zip entries: $zip_path" >&2
    exit 1
  }
  if ! grep -E -q "$pattern" <<<"$entries"; then
    echo "missing required zip entry matching: $pattern" >&2
    exit 1
  fi
}

mode="${1:-}"
target="${2:-}"
rust_target="${3:-}"
if [[ -z "$mode" || -z "$target" || -z "$rust_target" ]]; then
  usage
  exit 1
fi

case "$mode" in
  macos-app-zip)
    require_cmd unzip
    require_path "$target"
    require_zip_entry "$target" '^Pioneer\.app/Contents/MacOS/pioneer-app$'
    require_zip_entry "$target" '^Pioneer\.app/Contents/MacOS/pioneer-app-updater$'
    require_zip_entry "$target" '^Pioneer\.app/Contents/Resources/gateway/pioneer-bootstrap$'
    require_zip_entry "$target" '^Pioneer\.app/Contents/Resources/gateway/pioneer-gateway-macos-.*-computer-use-package\.zip$'
    require_zip_entry "$target" '^Pioneer\.app/Contents/Resources/gateway/SHA256SUMS$'
    gateway_asset="$(mktemp -t pioneer-gateway)"
    trap 'rm -f "$gateway_asset"' EXIT
    gateway_entry="$(unzip -Z1 "$target" | grep -E '^Pioneer\.app/Contents/Resources/gateway/pioneer-gateway-macos-.*-computer-use-package\.zip$')"
    unzip -p "$target" "$gateway_entry" > "$gateway_asset"
    require_zip_entry "$gateway_asset" '^cua/Pioneer Gateway\.app/Contents/MacOS/pioneer$'
    require_zip_entry "$gateway_asset" '^cua/Pioneer Gateway\.app/Contents/Helpers/cua-driver$'
    require_zip_entry "$gateway_asset" '^cua/manifest\.json$'
    ;;
  linux-appdir)
    require_path "$target/usr/bin/pioneer-app"
    require_path "$target/usr/bin/pioneer-app-updater"
    require_path "$target/usr/bin/gateway/pioneer-bootstrap"
    require_path "$target/usr/bin/gateway/SHA256SUMS"
    if ! compgen -G "$target/usr/bin/gateway/pioneer-gateway-linux-*-computer-use-package.zip" >/dev/null; then
      echo "missing bundled Linux gateway asset in $target/usr/bin/gateway" >&2
      exit 1
    fi
    for asset in "$target"/usr/bin/gateway/pioneer-gateway-linux-*-computer-use-package.zip; do
      require_zip_entry "$asset" '^cua/cua-driver$'
      require_zip_entry "$asset" '^cua/manifest\.json$'
    done
    ;;
  windows-stage)
    if command -v cygpath >/dev/null 2>&1; then target="$(cygpath -u "$target")"; fi
    require_path "$target/pioneer-app.exe"
    require_path "$target/pioneer-app-updater.exe"
    require_path "$target/gateway/pioneer-bootstrap.exe"
    require_path "$target/gateway/SHA256SUMS"
    if ! compgen -G "$target/gateway/pioneer-gateway-windows-*-computer-use-package.zip" >/dev/null; then
      echo "missing bundled Windows gateway asset in $target/gateway" >&2
      exit 1
    fi
    for asset in "$target"/gateway/pioneer-gateway-windows-*-computer-use-package.zip; do
      require_zip_entry "$asset" '^cua/cua-driver\.exe$'
      require_zip_entry "$asset" '^cua/cua-driver-uia\.exe$'
      require_zip_entry "$asset" '^cua/manifest\.json$'
    done
    ;;
  *)
    usage
    exit 1
    ;;
esac

# Presence checks above and approved producer identity are separate contracts.
case "$mode" in
  macos-app-zip)
    python3 scripts/packaging/desktop-inputs.py macos-app-zip --target "$rust_target" --destination "$target"
    ;;
  linux-appdir)
    python3 scripts/packaging/desktop-inputs.py verify --target "$rust_target" --destination "$target/usr/bin/gateway"
    ;;
  windows-stage)
    python scripts/packaging/desktop-inputs.py verify --target "$rust_target" --destination "$target/gateway"
    ;;
esac
