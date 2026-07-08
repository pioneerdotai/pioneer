#!/usr/bin/env bash
set -euo pipefail

sha256_file() {
  local path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" | awk '{print $1}'
  else
    shasum -a 256 "$path" | awk '{print $1}'
  fi
}

usage() {
  cat <<'USAGE'
Usage:
  package-linux-appimage.sh --target <rust-target> [--out-dir <dir>] [--appimagetool <path>]

Examples:
  package-linux-appimage.sh --target x86_64-unknown-linux-gnu
  package-linux-appimage.sh --target aarch64-unknown-linux-gnu --appimagetool ./appimagetool
USAGE
}

TARGET=""
OUT_DIR=""
APPIMAGETOOL_BIN="${APPIMAGETOOL_BIN:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      shift
      [[ $# -gt 0 ]] || {
        echo "missing value for --target" >&2
        exit 1
      }
      TARGET="$1"
      ;;
    --out-dir)
      shift
      [[ $# -gt 0 ]] || {
        echo "missing value for --out-dir" >&2
        exit 1
      }
      OUT_DIR="$1"
      ;;
    --appimagetool)
      shift
      [[ $# -gt 0 ]] || {
        echo "missing value for --appimagetool" >&2
        exit 1
      }
      APPIMAGETOOL_BIN="$1"
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 1
      ;;
  esac
  shift
done

if [[ -z "$TARGET" ]]; then
  usage
  exit 1
fi

case "$TARGET" in
  x86_64-unknown-linux-gnu) ARCH="x86_64" ;;
  aarch64-unknown-linux-gnu) ARCH="aarch64" ;;
  *)
    echo "unsupported Linux target: $TARGET" >&2
    exit 1
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$REPO_ROOT"

if [[ -z "$OUT_DIR" ]]; then
  OUT_DIR="$REPO_ROOT/dist"
fi
mkdir -p "$OUT_DIR"

if [[ -z "$APPIMAGETOOL_BIN" ]]; then
  APPIMAGETOOL_BIN="appimagetool"
fi

if [[ ! -x "$APPIMAGETOOL_BIN" ]] && ! command -v "$APPIMAGETOOL_BIN" >/dev/null 2>&1; then
  echo "appimagetool not found: $APPIMAGETOOL_BIN" >&2
  exit 1
fi

cargo build --release -p pioneer-desktop --target "$TARGET"
cargo build --release -p pioneer-app-updater --target "$TARGET"
cargo build --release -p pioneer-cli --features computer-use --target "$TARGET"

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

APPDIR="$WORK_DIR/Pioneer.AppDir"
mkdir -p "$APPDIR/usr/bin" "$APPDIR/usr/share/applications" "$APPDIR/usr/share/icons/hicolor/256x256/apps"

LINUX_ICON_SOURCE="$REPO_ROOT/crates/desktop/assets/app-icon-256.png"
if [[ ! -f "$LINUX_ICON_SOURCE" ]]; then
  echo "missing Linux app icon: $LINUX_ICON_SOURCE" >&2
  exit 1
fi

cp "target/$TARGET/release/pioneer-app" "$APPDIR/usr/bin/pioneer-app"
chmod 0755 "$APPDIR/usr/bin/pioneer-app"
cp "target/$TARGET/release/pioneer-app-updater" "$APPDIR/usr/bin/pioneer-app-updater"
chmod 0755 "$APPDIR/usr/bin/pioneer-app-updater"

if [[ ! -x "$APPDIR/usr/bin/pioneer-app-updater" ]]; then
  echo "missing packaged desktop updater helper: $APPDIR/usr/bin/pioneer-app-updater" >&2
  exit 1
fi

GATEWAY_BUNDLE_DIR="$APPDIR/usr/bin/gateway"
mkdir -p "$GATEWAY_BUNDLE_DIR"

GATEWAY_ASSET_NAME="pioneer-gateway-linux-${ARCH}.gz"
GATEWAY_ASSET_PATH="$GATEWAY_BUNDLE_DIR/$GATEWAY_ASSET_NAME"
gzip -f --stdout --best "target/$TARGET/release/pioneer" > "$GATEWAY_ASSET_PATH"

GATEWAY_SHA256="$(sha256_file "$GATEWAY_ASSET_PATH")"
printf 'sha256:%s %s\n' "$GATEWAY_SHA256" "$GATEWAY_ASSET_NAME" > "$GATEWAY_BUNDLE_DIR/SHA256SUMS"

cp "target/$TARGET/release/pioneer" "$GATEWAY_BUNDLE_DIR/pioneer-bootstrap"
chmod 0755 "$GATEWAY_BUNDLE_DIR/pioneer-bootstrap"

cat > "$APPDIR/AppRun" <<'APP_RUN'
#!/bin/sh
HERE="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
exec "$HERE/usr/bin/pioneer-app" "$@"
APP_RUN
chmod 0755 "$APPDIR/AppRun"

cat > "$APPDIR/pioneer.desktop" <<'DESKTOP'
[Desktop Entry]
Type=Application
Name=Pioneer
Exec=pioneer-app
Icon=pioneer
Categories=Utility;
Terminal=false
DESKTOP
cp "$APPDIR/pioneer.desktop" "$APPDIR/usr/share/applications/pioneer.desktop"

cp "$LINUX_ICON_SOURCE" "$APPDIR/pioneer.png"
cp "$APPDIR/pioneer.png" "$APPDIR/usr/share/icons/hicolor/256x256/apps/pioneer.png"
ln -s "pioneer.png" "$APPDIR/.DirIcon"
scripts/packaging/desktop/check-package-contents.sh linux-appdir "$APPDIR"

APPIMAGE_NAME="pioneer-linux-${ARCH}.AppImage"
APPIMAGE_PATH="$OUT_DIR/$APPIMAGE_NAME"

if [[ -x "$APPIMAGETOOL_BIN" ]]; then
  ARCH="$ARCH" "$APPIMAGETOOL_BIN" "$APPDIR" "$APPIMAGE_PATH"
else
  ARCH="$ARCH" "$(command -v "$APPIMAGETOOL_BIN")" "$APPDIR" "$APPIMAGE_PATH"
fi

chmod 0755 "$APPIMAGE_PATH"

echo "Created: $APPIMAGE_PATH"
