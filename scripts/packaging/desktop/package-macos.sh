#!/usr/bin/env bash
set -euo pipefail

bool_is_true() {
  local value="${1:-}"
  value="$(echo "$value" | tr '[:upper:]' '[:lower:]')"
  [[ "$value" == "1" || "$value" == "true" || "$value" == "yes" ]]
}

workspace_version() {
  awk '
    /^\[workspace\.package\]$/ { in_section = 1; next }
    in_section && /^\[/ { in_section = 0 }
    in_section && /^version = "/ {
      line = $0
      sub(/^version = "/, "", line)
      sub(/".*$/, "", line)
      if (line != "") {
        print line
        exit
      }
    }
  ' Cargo.toml
}

normalize_semver_triplet() {
  local raw="$1"
  local normalized="${raw#v}"
  normalized="${normalized#V}"

  if [[ "$normalized" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)([-+].*)?$ ]]; then
    printf '%s.%s.%s\n' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}" "${BASH_REMATCH[3]}"
    return 0
  fi

  return 1
}

resolve_release_version() {
  local candidate workspace
  candidate="${PIONEER_DESKTOP_VERSION:-${GITHUB_REF_NAME:-}}"

  if [[ -n "$candidate" ]] && normalize_semver_triplet "$candidate"; then
    return 0
  fi

  workspace="$(workspace_version || true)"
  if [[ -n "$workspace" ]] && normalize_semver_triplet "$workspace"; then
    return 0
  fi

  echo "failed to resolve release version from PIONEER_DESKTOP_VERSION/GITHUB_REF_NAME/Cargo.toml" >&2
  return 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "required command is missing: $1" >&2
    exit 1
  }
}

apply_applications_link_icon() {
  local alias_path="$1"
  local work_dir="$2"
  local icon_source="/System/Library/CoreServices/CoreTypes.bundle/Contents/Resources/ApplicationsFolderIcon.icns"
  local icon_work="$work_dir/.ApplicationsFolderIcon.icns"
  local icon_rsrc="$work_dir/.ApplicationsFolderIcon.rsrc"

  [[ -f "$icon_source" ]] || return 0
  command -v sips >/dev/null 2>&1 || return 0
  command -v DeRez >/dev/null 2>&1 || return 0
  command -v Rez >/dev/null 2>&1 || return 0
  command -v SetFile >/dev/null 2>&1 || return 0

  # Finder can render DMG aliases as blank placeholders unless the alias carries an icon resource.
  {
    cp "$icon_source" "$icon_work" &&
      sips -i "$icon_work" >/dev/null 2>&1 &&
      DeRez -only icns "$icon_work" > "$icon_rsrc" 2>/dev/null &&
      Rez -append "$icon_rsrc" -o "$alias_path" >/dev/null 2>&1 &&
      SetFile -a C "$alias_path" >/dev/null 2>&1
  } || true

  rm -f "$icon_work" "$icon_rsrc"
}

create_applications_link() {
  local stage_dir="$1"
  local alias_path="$stage_dir/Applications"

  rm -rf "$alias_path"

  if command -v osascript >/dev/null 2>&1; then
    if PIONEER_DMG_STAGE_DIR="$stage_dir" osascript <<'OSA' >/dev/null 2>&1; then
set stageDir to system attribute "PIONEER_DMG_STAGE_DIR"
tell application "Finder"
  set applicationsFolder to POSIX file "/Applications" as alias
  set destinationFolder to POSIX file stageDir as alias
  make new alias file to applicationsFolder at destinationFolder with properties {name:"Applications"}
end tell
OSA
      if [[ -e "$alias_path" ]]; then
        apply_applications_link_icon "$alias_path" "$stage_dir"
        return 0
      fi
    fi
  fi

  ln -s /Applications "$alias_path"
}

configure_dmg_finder_layout() {
  local mount_dir="$1"
  local background_file="$2"

  command -v osascript >/dev/null 2>&1 || return 0

  PIONEER_DMG_MOUNT_DIR="$mount_dir" \
    PIONEER_DMG_BACKGROUND_FILE="$background_file" \
    PIONEER_DMG_APP_ITEM="${APP_NAME}.app" \
    osascript <<'OSA' >/dev/null 2>&1 || true
set mountDir to system attribute "PIONEER_DMG_MOUNT_DIR"
set backgroundFilePath to system attribute "PIONEER_DMG_BACKGROUND_FILE"
set appItemName to system attribute "PIONEER_DMG_APP_ITEM"

tell application "Finder"
  set mountedFolder to POSIX file mountDir as alias
  set backgroundFile to POSIX file backgroundFilePath as alias

  open mountedFolder
  delay 1

  set dmgWindow to container window of mountedFolder
  set current view of dmgWindow to icon view
  set toolbar visible of dmgWindow to false
  set statusbar visible of dmgWindow to false
  set bounds of dmgWindow to {200, 120, 700, 420}

  set viewOptions to icon view options of dmgWindow
  set arrangement of viewOptions to not arranged
  set icon size of viewOptions to 80
  set background picture of viewOptions to backgroundFile

  set position of item appItemName of mountedFolder to {155, 174}
  set position of item "Applications" of mountedFolder to {355, 174}

  update mountedFolder without registering applications
  delay 1
  close dmgWindow
end tell
OSA
}

detach_dmg() {
  local mount_dir="$1"
  local attempt

  for attempt in 1 2 3 4 5; do
    if hdiutil detach "$mount_dir" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  hdiutil detach -force "$mount_dir"
}

sha256_file() {
  local path="$1"
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$path" | awk '{print $1}'
  else
    sha256sum "$path" | awk '{print $1}'
  fi
}

ensure_macos_signing_prerequisites() {
  if bool_is_true "${MACOS_SIGNING_REQUIRED:-false}"; then
    [[ -n "${MACOS_DESKTOP_SIGN_IDENTITY:-}" ]] || {
      echo "MACOS_DESKTOP_SIGN_IDENTITY is required when MACOS_SIGNING_REQUIRED=true" >&2
      exit 1
    }
    [[ -n "${APPLE_NOTARIZATION_KEY_ID:-}" ]] || {
      echo "APPLE_NOTARIZATION_KEY_ID is required when MACOS_SIGNING_REQUIRED=true" >&2
      exit 1
    }
    [[ -n "${APPLE_NOTARIZATION_ISSUER_ID:-}" ]] || {
      echo "APPLE_NOTARIZATION_ISSUER_ID is required when MACOS_SIGNING_REQUIRED=true" >&2
      exit 1
    }
    if [[ -z "${APPLE_NOTARIZATION_KEY:-}" && -z "${APPLE_NOTARIZATION_KEY_BASE64:-}" ]]; then
      echo "APPLE_NOTARIZATION_KEY or APPLE_NOTARIZATION_KEY_BASE64 is required when MACOS_SIGNING_REQUIRED=true" >&2
      exit 1
    fi
  fi
}

write_notarization_key() {
  local destination="$1"

  if [[ -n "${APPLE_NOTARIZATION_KEY_BASE64:-}" ]]; then
    python3 - <<'PY' "$destination"
import base64
import os
import sys

destination = sys.argv[1]
raw = os.environ.get("APPLE_NOTARIZATION_KEY_BASE64", "")
normalized = "".join(raw.split()).rstrip("%")
if not normalized:
    raise SystemExit("APPLE_NOTARIZATION_KEY_BASE64 is empty after normalization")
try:
    decoded = base64.b64decode(normalized, validate=True)
except Exception as exc:
    raise SystemExit(f"APPLE_NOTARIZATION_KEY_BASE64 is invalid base64: {exc}")

with open(destination, "wb") as handle:
    handle.write(decoded)
PY
    return 0
  fi

  if [[ -n "${APPLE_NOTARIZATION_KEY:-}" ]]; then
    printf '%s' "$APPLE_NOTARIZATION_KEY" > "$destination"
    return 0
  fi

  return 1
}

usage() {
  cat <<'USAGE'
Usage:
  package-macos.sh --target <rust-target> [--out-dir <dir>]

Examples:
  package-macos.sh --target aarch64-apple-darwin
  package-macos.sh --target x86_64-apple-darwin --out-dir dist
USAGE
}

TARGET=""
OUT_DIR=""

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
  aarch64-apple-darwin) ARCH="aarch64" ;;
  x86_64-apple-darwin) ARCH="x86_64" ;;
  *)
    echo "unsupported macOS target: $TARGET" >&2
    exit 1
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$REPO_ROOT"
APP_VERSION="$(resolve_release_version)"

require_cmd cargo
require_cmd hdiutil
require_cmd gzip
ensure_macos_signing_prerequisites

if [[ -z "$OUT_DIR" ]]; then
  OUT_DIR="$REPO_ROOT/dist"
fi
mkdir -p "$OUT_DIR"

cargo build --release -p pioneer-desktop --target "$TARGET"
cargo build --release -p pioneer-cli --target "$TARGET"

WORK_DIR="$(mktemp -d)"
DMG_MOUNT_DIR=""
cleanup_work_dir() {
  if [[ -n "${DMG_MOUNT_DIR:-}" ]]; then
    hdiutil detach "$DMG_MOUNT_DIR" >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK_DIR"
}
trap cleanup_work_dir EXIT

APP_NAME="Pioneer"
APP_DIR="$WORK_DIR/${APP_NAME}.app"
CONTENTS_DIR="$APP_DIR/Contents"
MACOS_DIR="$CONTENTS_DIR/MacOS"
RESOURCES_DIR="$CONTENTS_DIR/Resources"
DESKTOP_EXECUTABLE_NAME="pioneer-app"
MACOS_ICON_NAME="Pioneer.icns"
MACOS_ICON_SOURCE="$REPO_ROOT/crates/desktop/assets/app-icon.icns"
DMG_BACKGROUND_SOURCE="$REPO_ROOT/assets/dmg-backgrond@2x.png"
DMG_BACKGROUND_NAME="background@2x.png"
MACOS_DMG_SIGN_IDENTITY="${MACOS_DMG_SIGN_IDENTITY:-${MACOS_DESKTOP_SIGN_IDENTITY:-}}"
mkdir -p "$MACOS_DIR" "$RESOURCES_DIR"

if [[ ! -f "$MACOS_ICON_SOURCE" ]]; then
  echo "missing macOS app icon: $MACOS_ICON_SOURCE" >&2
  exit 1
fi
if [[ ! -f "$DMG_BACKGROUND_SOURCE" ]]; then
  echo "missing macOS DMG background: $DMG_BACKGROUND_SOURCE" >&2
  exit 1
fi
cp "$MACOS_ICON_SOURCE" "$RESOURCES_DIR/$MACOS_ICON_NAME"

cp "target/$TARGET/release/pioneer-app" "$MACOS_DIR/${DESKTOP_EXECUTABLE_NAME}"
chmod 0755 "$MACOS_DIR/${DESKTOP_EXECUTABLE_NAME}"

GATEWAY_BUNDLE_DIR="$RESOURCES_DIR/gateway"
mkdir -p "$GATEWAY_BUNDLE_DIR"

GATEWAY_ASSET_NAME="pioneer-gateway-macos-${ARCH}.gz"
GATEWAY_ASSET_PATH="$GATEWAY_BUNDLE_DIR/$GATEWAY_ASSET_NAME"
GATEWAY_BINARY_RAW="$WORK_DIR/pioneer-gateway-${ARCH}"
cp "target/$TARGET/release/pioneer" "$GATEWAY_BINARY_RAW"
chmod 0755 "$GATEWAY_BINARY_RAW"

if [[ -n "${MACOS_DESKTOP_SIGN_IDENTITY:-}" ]]; then
  require_cmd codesign
  codesign --force --timestamp --options runtime --sign "$MACOS_DESKTOP_SIGN_IDENTITY" "$GATEWAY_BINARY_RAW"
  codesign --verify --strict "$GATEWAY_BINARY_RAW"
fi

cp "$GATEWAY_BINARY_RAW" "$GATEWAY_BUNDLE_DIR/pioneer-bootstrap"
chmod 0755 "$GATEWAY_BUNDLE_DIR/pioneer-bootstrap"

gzip -f --stdout --best "$GATEWAY_BINARY_RAW" > "$GATEWAY_ASSET_PATH"

GATEWAY_SHA256="$(sha256_file "$GATEWAY_ASSET_PATH")"
printf 'sha256:%s %s\n' "$GATEWAY_SHA256" "$GATEWAY_ASSET_NAME" > "$GATEWAY_BUNDLE_DIR/SHA256SUMS"

cat > "$CONTENTS_DIR/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key>
  <string>${APP_NAME}</string>
  <key>CFBundleDisplayName</key>
  <string>${APP_NAME}</string>
  <key>CFBundleIdentifier</key>
  <string>ai.pioneer.macos</string>
  <key>CFBundleVersion</key>
  <string>${APP_VERSION}</string>
  <key>CFBundleShortVersionString</key>
  <string>${APP_VERSION}</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>CFBundleExecutable</key>
  <string>${DESKTOP_EXECUTABLE_NAME}</string>
  <key>CFBundleIconFile</key>
  <string>${MACOS_ICON_NAME}</string>
  <key>LSMinimumSystemVersion</key>
  <string>12.0</string>
</dict>
</plist>
PLIST

echo "APPL????" > "$CONTENTS_DIR/PkgInfo"

if [[ -n "${MACOS_DESKTOP_SIGN_IDENTITY:-}" ]]; then
  require_cmd codesign
  codesign --deep --force --timestamp --options runtime --sign "$MACOS_DESKTOP_SIGN_IDENTITY" "$APP_DIR"
  codesign --verify --deep --strict "$APP_DIR"
fi

DMG_NAME="Pioneer-${ARCH}.dmg"
DMG_PATH="$OUT_DIR/$DMG_NAME"
DMG_RW_PATH="$WORK_DIR/Pioneer-${ARCH}.rw.dmg"
DMG_MOUNT_DIR="$WORK_DIR/dmg-mount"
DMG_APP_SIZE_MB="$(du -sm "$APP_DIR" | awk '{print $1}')"
DMG_SIZE_MB=$((DMG_APP_SIZE_MB + 128))
mkdir -p "$DMG_MOUNT_DIR"

hdiutil create \
  -volname "$APP_NAME" \
  -fs HFS+ \
  -size "${DMG_SIZE_MB}m" \
  "$DMG_RW_PATH"

hdiutil attach \
  -readwrite \
  -noverify \
  -noautoopen \
  -mountpoint "$DMG_MOUNT_DIR" \
  "$DMG_RW_PATH"

cp -R "$APP_DIR" "$DMG_MOUNT_DIR/${APP_NAME}.app"
create_applications_link "$DMG_MOUNT_DIR"
mkdir -p "$DMG_MOUNT_DIR/.background"
cp "$DMG_BACKGROUND_SOURCE" "$DMG_MOUNT_DIR/.background/$DMG_BACKGROUND_NAME"
chflags hidden "$DMG_MOUNT_DIR/.background" || true
configure_dmg_finder_layout "$DMG_MOUNT_DIR" "$DMG_MOUNT_DIR/.background/$DMG_BACKGROUND_NAME"
sync
detach_dmg "$DMG_MOUNT_DIR"
DMG_MOUNT_DIR=""

rm -f "$DMG_PATH"
hdiutil convert "$DMG_RW_PATH" \
  -ov \
  -format UDZO \
  -imagekey zlib-level=9 \
  -o "$DMG_PATH"

if [[ -n "$MACOS_DMG_SIGN_IDENTITY" ]]; then
  require_cmd codesign
  codesign --force --timestamp --sign "$MACOS_DMG_SIGN_IDENTITY" "$DMG_PATH"
  codesign --verify --strict "$DMG_PATH"
fi

if [[ -n "${APPLE_NOTARIZATION_KEY_ID:-}" ]] && [[ -n "${APPLE_NOTARIZATION_ISSUER_ID:-}" ]]; then
  require_cmd xcrun
  NOTARY_KEY_FILE="$WORK_DIR/notary-api-key.p8"
  if ! write_notarization_key "$NOTARY_KEY_FILE"; then
    echo "notarization key is missing, skipping notarization"
  else
    NOTARY_SUBMIT_JSON="$WORK_DIR/notary-submit.json"
    xcrun notarytool submit "$DMG_PATH" \
      --key "$NOTARY_KEY_FILE" \
      --key-id "$APPLE_NOTARIZATION_KEY_ID" \
      --issuer "$APPLE_NOTARIZATION_ISSUER_ID" \
      --wait \
      --output-format json > "$NOTARY_SUBMIT_JSON"

    read -r NOTARY_SUBMISSION_ID NOTARY_STATUS < <(
      python3 - <<'PY' "$NOTARY_SUBMIT_JSON"
import json
import sys

payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
submission_id = (payload.get("id") or "").strip()
status = (payload.get("status") or "").strip()
print(f"{submission_id} {status}")
PY
    )

    if [[ "$NOTARY_STATUS" != "Accepted" ]]; then
      echo "notarization failed with status: ${NOTARY_STATUS:-unknown}" >&2
      if [[ -n "${NOTARY_SUBMISSION_ID:-}" ]]; then
        echo "notary submission id: $NOTARY_SUBMISSION_ID" >&2
        xcrun notarytool log "$NOTARY_SUBMISSION_ID" \
          --key "$NOTARY_KEY_FILE" \
          --key-id "$APPLE_NOTARIZATION_KEY_ID" \
          --issuer "$APPLE_NOTARIZATION_ISSUER_ID" || true
      fi
      exit 1
    fi

    xcrun stapler staple "$DMG_PATH"
  fi
fi

echo "Created: $DMG_PATH"
