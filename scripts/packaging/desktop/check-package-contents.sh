#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  check-package-contents.sh macos-app-zip <Pioneer-arch.app.zip>
  check-package-contents.sh linux-appdir <Pioneer.AppDir>
  check-package-contents.sh windows-stage <stage-dir>
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
  if ! unzip -Z1 "$zip_path" | grep -E -q "$pattern"; then
    echo "missing required zip entry matching: $pattern" >&2
    exit 1
  fi
}

mode="${1:-}"
target="${2:-}"
if [[ -z "$mode" || -z "$target" ]]; then
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
    require_zip_entry "$target" '^Pioneer\.app/Contents/Resources/gateway/pioneer-gateway-macos-.*\.(gz|zip)$'
    require_zip_entry "$target" '^Pioneer\.app/Contents/Resources/gateway/SHA256SUMS$'
    ;;
  linux-appdir)
    require_path "$target/usr/bin/pioneer-app"
    require_path "$target/usr/bin/pioneer-app-updater"
    require_path "$target/usr/bin/gateway/pioneer-bootstrap"
    require_path "$target/usr/bin/gateway/SHA256SUMS"
    if ! compgen -G "$target/usr/bin/gateway/pioneer-gateway-linux-*.gz" >/dev/null; then
      echo "missing bundled Linux gateway asset in $target/usr/bin/gateway" >&2
      exit 1
    fi
    ;;
  windows-stage)
    require_path "$target/pioneer-app.exe"
    require_path "$target/pioneer-app-updater.exe"
    require_path "$target/gateway/pioneer-bootstrap.exe"
    require_path "$target/gateway/SHA256SUMS"
    if ! compgen -G "$target/gateway/pioneer-gateway-windows-*.zip" >/dev/null; then
      echo "missing bundled Windows gateway asset in $target/gateway" >&2
      exit 1
    fi
    ;;
  *)
    usage
    exit 1
    ;;
esac
