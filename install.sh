#!/usr/bin/env bash
set -euo pipefail

CHANNEL="stable"
VERSION=""
NO_START=0
FORCE_START=0
COMPUTER_USE=0
WORK_DIR=""

REPO="${PIONEER_RELEASE_REPO:-pioneerdotai/pioneer}"
API_BASE="${PIONEER_RELEASE_API_BASE:-https://api.github.com/repos/${REPO}/releases}"
DOWNLOAD_BASE="${PIONEER_RELEASE_DOWNLOAD_BASE:-https://github.com/${REPO}/releases/download}"

usage() {
  cat <<'USAGE'
Usage:
  install.sh [--channel stable|beta|canary] [--version x.y.z] [--computer-use] [--no-start] [--force-start]

Options:
  --channel <name>   Release channel (default: stable)
  --version <value>  Install explicit version (for example: 0.2.1 or v0.2.1)
  --computer-use     Install the native computer-use gateway variant
  --headless         Install the headless gateway variant (default)
  --no-start         Do not start service after install/update
  --force-start      Start service even if an existing install is currently stopped
  --help             Show this help

Environment:
  PIONEER_RELEASE_REPO          GitHub repo in owner/name format
  PIONEER_RELEASE_API_BASE      Releases API base URL
  PIONEER_RELEASE_DOWNLOAD_BASE Release download base URL
  PIONEER_INSTALL_COMPUTER_USE  Set to 1/true/yes to install the computer-use variant
  PIONEER_LOCAL_ASSET_FILE      Local gateway archive path (skip network download)
  PIONEER_LOCAL_CHECKSUMS_FILE  Local SHA256SUMS path (skip network download)
USAGE
}

log() {
  printf '[pioneer-install] %s\n' "$*"
}

fail() {
  printf '[pioneer-install] error: %s\n' "$*" >&2
  exit 1
}

cleanup() {
  if [[ -n "${WORK_DIR:-}" ]]; then
    rm -rf "$WORK_DIR"
  fi
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "required command is missing: $1"
}

read_env_flags() {
  case "${PIONEER_INSTALL_COMPUTER_USE:-}" in
    1|true|TRUE|yes|YES|on|ON) COMPUTER_USE=1 ;;
    ""|0|false|FALSE|no|NO|off|OFF) ;;
    *) fail "invalid PIONEER_INSTALL_COMPUTER_USE value; expected 1/true/yes or 0/false/no" ;;
  esac
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --channel)
        shift
        [[ $# -gt 0 ]] || fail "--channel requires a value"
        CHANNEL="$1"
        ;;
      --version)
        shift
        [[ $# -gt 0 ]] || fail "--version requires a value"
        VERSION="$1"
        ;;
      --computer-use)
        COMPUTER_USE=1
        ;;
      --headless)
        COMPUTER_USE=0
        ;;
      --no-start)
        NO_START=1
        ;;
      --force-start)
        FORCE_START=1
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        fail "unknown argument: $1"
        ;;
    esac
    shift
  done

  case "$CHANNEL" in
    stable|beta|canary) ;;
    *) fail "invalid --channel '$CHANNEL'; expected stable|beta|canary" ;;
  esac
}

detect_platform() {
  local os arch uname_os uname_arch
  uname_os="$(uname -s)"
  uname_arch="$(uname -m)"

  case "$uname_os" in
    Darwin) os="macos" ;;
    Linux) os="linux" ;;
    *) fail "unsupported OS: $uname_os" ;;
  esac

  case "$uname_arch" in
    x86_64|amd64) arch="x86_64" ;;
    aarch64|arm64) arch="aarch64" ;;
    *) fail "unsupported architecture: $uname_arch" ;;
  esac

  printf '%s %s\n' "$os" "$arch"
}

normalize_tag() {
  local value="$1"
  value="${value#v}"
  printf 'v%s\n' "$value"
}

resolve_release_tag() {
  if [[ -n "$VERSION" ]]; then
    normalize_tag "$VERSION"
    return
  fi

  if [[ "$CHANNEL" == "stable" ]]; then
    curl -fsSL "${API_BASE}/latest" | python3 -c 'import json,sys
payload=json.load(sys.stdin)
tag=payload.get("tag_name","").strip()
if not tag:
    raise SystemExit("latest release does not include tag_name")
print(tag)'
    return
  fi

  curl -fsSL "${API_BASE}?per_page=100" | python3 -c 'import json,sys; channel=sys.argv[1]; needle=f"-{channel}"; 
for release in json.load(sys.stdin):
    tag=str(release.get("tag_name",""))
    if needle in tag:
        print(tag)
        raise SystemExit(0)
raise SystemExit(f"failed to find release for channel {channel!r}")' "$CHANNEL"
}

download_release_asset() {
  local tag="$1"
  local asset="$2"
  local output="$3"

  local url="${DOWNLOAD_BASE}/${tag}/${asset}"
  log "downloading ${asset} from ${tag}"
  curl -fL --retry 3 --retry-delay 1 -o "$output" "$url"
}

expected_sha_for_asset() {
  local checksums_file="$1"
  local asset="$2"

  local expected
  expected="$(awk -v name="$asset" '$2 == name {print $1; exit}' "$checksums_file" | sed 's/^sha256://')"
  if [[ -z "$expected" ]]; then
    fail "failed to find checksum for ${asset} in ${checksums_file}"
  fi

  printf '%s\n' "$expected"
}

sha256_file() {
  local path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" | awk '{print $1}'
  else
    shasum -a 256 "$path" | awk '{print $1}'
  fi
}

run_bootstrap_installer() {
  local asset_file="$1"
  local checksums_file="$2"
  local work_dir="$3"

  local installer_bin="${work_dir}/pioneer-installer"
  gunzip -c "$asset_file" > "$installer_bin"
  chmod 0755 "$installer_bin"

  local args=(
    install
    --source local
    --asset "$asset_file"
    --checksums "$checksums_file"
    --managed-by script
  )
  if [[ "$NO_START" -eq 1 ]]; then
    args+=(--no-start)
  fi
  if [[ "$FORCE_START" -eq 1 ]]; then
    args+=(--force-start)
  fi

  "$installer_bin" "${args[@]}"
}

main() {
  need_cmd curl
  need_cmd python3
  need_cmd gunzip

  read_env_flags
  parse_args "$@"
  read -r os arch < <(detect_platform)

  WORK_DIR="$(mktemp -d)"
  trap cleanup EXIT

  local asset_name tag asset_file checksums_file
  if [[ -n "${PIONEER_LOCAL_ASSET_FILE:-}" || -n "${PIONEER_LOCAL_CHECKSUMS_FILE:-}" ]]; then
    [[ -n "${PIONEER_LOCAL_ASSET_FILE:-}" ]] || fail "PIONEER_LOCAL_ASSET_FILE is required when using local assets"
    [[ -n "${PIONEER_LOCAL_CHECKSUMS_FILE:-}" ]] || fail "PIONEER_LOCAL_CHECKSUMS_FILE is required when using local assets"
    asset_file="${PIONEER_LOCAL_ASSET_FILE}"
    checksums_file="${PIONEER_LOCAL_CHECKSUMS_FILE}"
    [[ -f "$asset_file" ]] || fail "local asset file does not exist: $asset_file"
    [[ -f "$checksums_file" ]] || fail "local checksums file does not exist: $checksums_file"
    asset_name="$(basename "$asset_file")"
    tag="local-bundle"
  else
    local asset_suffix=""
    if [[ "$COMPUTER_USE" -eq 1 ]]; then
      asset_suffix="-computer-use"
    fi
    asset_name="pioneer-gateway-${os}-${arch}${asset_suffix}.gz"
    tag="$(resolve_release_tag)"
    [[ -n "$tag" ]] || fail "resolved release tag is empty"
    asset_file="${WORK_DIR}/${asset_name}"
    checksums_file="${WORK_DIR}/SHA256SUMS"
    download_release_asset "$tag" "$asset_name" "$asset_file"
    download_release_asset "$tag" "SHA256SUMS" "$checksums_file"
  fi

  local expected actual
  expected="$(expected_sha_for_asset "$checksums_file" "$asset_name")"
  actual="$(sha256_file "$asset_file")"
  [[ "$expected" == "$actual" ]] || fail "checksum mismatch for ${asset_name}"

  run_bootstrap_installer "$asset_file" "$checksums_file" "$WORK_DIR"
}

main "$@"
