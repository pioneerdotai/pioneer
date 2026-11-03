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
  install.sh [--channel stable|beta|canary] [--version x.y.z] [--headless|--computer-use] [--no-start] [--force-start]

Options:
  --channel <name>   Release channel (default: stable)
  --version <value>  Install explicit version (for example: 0.2.1 or v0.2.1)
  --headless         Install the server gateway without computer use (default)
  --computer-use     Install the Gateway with its complete CUA driver package
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
    # Restore owner access only while disposing operation-owned staging; an
    # archive may legitimately preserve read-only directories or files.
    chmod -R u+rwX "$WORK_DIR" 2>/dev/null || true
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

run_package_installer() {
  local asset_file="$1"
  local checksums_file="$2"
  local work_dir="$3"
  local os="$4"
  local arch="$5"

  local executable="pioneer"
  local gateway_bundle=""
  if [[ "$os" == "macos" && "$COMPUTER_USE" -eq 1 ]]; then
    executable="cua/Pioneer Gateway.app/Contents/MacOS/pioneer"
    gateway_bundle="${work_dir}/payload/cua/Pioneer Gateway.app"
  fi
  local installer_bin="${work_dir}/payload/${executable}"
  PIONEER_ASSET_FILE="$asset_file" PIONEER_INSTALLER_BIN="$installer_bin" \
    PIONEER_EXTRACT_DIR="${work_dir}/payload" PIONEER_TARGET_OS="$os" PIONEER_TARGET_ARCH="$arch" python3 <<'PY'
import gzip
import os
import pathlib
import stat
import zipfile

asset = pathlib.Path(os.environ["PIONEER_ASSET_FILE"])
installer = pathlib.Path(os.environ["PIONEER_INSTALLER_BIN"])
destination = pathlib.Path(os.environ["PIONEER_EXTRACT_DIR"])
destination.mkdir()
max_file = 512 * 1024 * 1024
if asset.suffix == ".zip":
    with zipfile.ZipFile(asset) as archive:
        seen, files, directories, total = set(), [], [], 0
        devices = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10))}
        for entry in archive.infolist():
            name = entry.orig_filename[:-1] if entry.is_dir() else entry.orig_filename
            member = pathlib.PurePosixPath(name)
            mode = entry.external_attr >> 16
            kind = stat.S_IFMT(mode)
            if (not name or member.is_absolute() or member.as_posix() != name
                    or any(part in (".", "..") or part.endswith((".", " "))
                           or part.split(".")[0].upper() in devices for part in member.parts)
                    or any(char in name for char in ("\\", ":"))
                    or any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in name)
                    or kind not in (0, stat.S_IFREG, stat.S_IFDIR)
                    or (kind != 0 and kind != (stat.S_IFDIR if entry.is_dir() else stat.S_IFREG))
                    or mode & 0o7022
                    or name.casefold() in seen):
                raise SystemExit(f"Unsafe or duplicate archive member: {entry.filename!r}")
            seen.add(name.casefold())
            total += entry.file_size
            if (entry.file_size < 0 or entry.file_size > max_file
                    or total > 2 * 1024 * 1024 * 1024 or len(seen) > 10000
                    or (entry.is_dir() and entry.file_size != 0)):
                raise SystemExit("Gateway archive exceeds extraction budget")
            if not entry.is_dir():
                files.append((entry, member, mode))
            else:
                directories.append((member, mode))
        # Reject parent/file collisions before creating anything from the archive.
        filenames = {member.as_posix().casefold() for _, member, _ in files}
        paths = [member for _, member, _ in files] + [member for member, _ in directories]
        if any(parent.as_posix().casefold() in filenames
               for member in paths for parent in member.parents if parent.parts):
            raise SystemExit("Archive member conflicts with a parent file")
        for member, _ in directories:
            (destination / member).mkdir(parents=True, exist_ok=True)
        for entry, member, mode in files:
            output = destination / member
            output.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(entry) as source, output.open("xb") as target:
                written = 0
                while chunk := source.read(min(1024 * 1024, entry.file_size - written + 1)):
                    written += len(chunk)
                    if written > entry.file_size or written > max_file:
                        raise SystemExit("Archive member exceeds declared size")
                    target.write(chunk)
            if written != entry.file_size:
                raise SystemExit("Archive member size mismatch")
            output.chmod(mode & 0o777)
        for member, mode in sorted(directories, key=lambda row: len(row[0].parts), reverse=True):
            (destination / member).chmod(mode & 0o777)
elif asset.suffix == ".gz":
    with gzip.open(asset, "rb") as source, installer.open("xb") as target:
        total = 0
        while chunk := source.read(1024 * 1024):
            total += len(chunk)
            if total > max_file:
                raise SystemExit("Gateway executable exceeds extraction budget")
            target.write(chunk)
else:
    raise SystemExit("Gateway installation requires a .gz server archive or complete ZIP archive")
if not installer.is_file():
    raise SystemExit(f"{asset} does not contain {installer.relative_to(destination)}")
# Check the selected native target before executing any extracted code.
with installer.open("rb") as source:
    header = source.read(4096)
arm = os.environ["PIONEER_TARGET_ARCH"] == "aarch64"
if os.environ["PIONEER_TARGET_OS"] == "linux":
    valid = (len(header) >= 20 and header[:6] == b"\x7fELF\x02\x01"
             and int.from_bytes(header[18:20], "little") == (183 if arm else 62))
else:
    order = "little" if header[:4] == b"\xcf\xfa\xed\xfe" else "big"
    valid = (len(header) >= 8 and header[:4] in (b"\xcf\xfa\xed\xfe", b"\xfe\xed\xfa\xcf")
             and int.from_bytes(header[4:8], order) == (0x100000C if arm else 0x1000007))
if not valid:
    raise SystemExit("Gateway executable does not match this OS and architecture")
PY
  if [[ -n "$gateway_bundle" ]]; then
    need_cmd codesign
    codesign --verify --deep --strict "$gateway_bundle"
  elif [[ "$asset_file" == *.gz ]]; then
    # Gzip carries no Unix file mode; ZIP permissions were preserved above.
    chmod 0755 "$installer_bin"
  fi
  [[ -x "$installer_bin" ]] || fail "Gateway installer executable is not executable: $installer_bin"

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
    mkdir "${WORK_DIR}/inputs"
    cp "$asset_file" "${WORK_DIR}/inputs/${asset_name}"
    cp "$checksums_file" "${WORK_DIR}/inputs/SHA256SUMS"
    asset_file="${WORK_DIR}/inputs/${asset_name}"
    checksums_file="${WORK_DIR}/inputs/SHA256SUMS"
    tag="local-bundle"
  else
    local asset_suffix="" asset_ext="gz"
    if [[ "$COMPUTER_USE" -eq 1 ]]; then
      asset_suffix="-computer-use-package"
      asset_ext="zip"
    elif [[ "$os" == "macos" && "$arch" == "x86_64" ]]; then
      asset_ext="zip"
    fi
    asset_name="pioneer-gateway-${os}-${arch}${asset_suffix}.${asset_ext}"
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

  run_package_installer "$asset_file" "$checksums_file" "$WORK_DIR" "$os" "$arch"
}

main "$@"
