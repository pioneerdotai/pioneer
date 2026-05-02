#!/usr/bin/env bash
set -euo pipefail

# Pioneer dev-only reset for macOS.
# This intentionally does not touch production artifacts:
#   command/service/home: pioneer, com.pioneer.gateway, ~/.pioneer
#   desktop app/data: /Applications/Pioneer.app, ~/Library/Application Support/Pioneer
#   prod port: 17878
#
# Dev settings accounted for:
#   home_directory = ".pioneer.dev"
#   install.macos_root_directory_name = "PioneerDev"
#   install.managed_directory_name = "managed-dev"
#   install.command_name = "pioneer-dev"
#   gateway.service_name = "com.pioneer.gateway.dev"
#   gateway.legacy_service_names = ["com.pioneer.gateway.local"]
#   gateway.listen_addr = "0.0.0.0:18778"

DEV_SERVICE_NAME="com.pioneer.gateway.dev"
DEV_LEGACY_SERVICE_NAMES=("com.pioneer.gateway.local")
DEV_HOME_DIR="$HOME/.pioneer.dev"
ROOT_DEV_HOME_DIR="/var/root/.pioneer.dev"
DEV_PORT="18778"
DEV_COMMAND="pioneer-dev"

DEV_USER_PLIST="$HOME/Library/LaunchAgents/${DEV_SERVICE_NAME}.plist"
DEV_SYSTEM_PLIST="/Library/LaunchDaemons/${DEV_SERVICE_NAME}.plist"

DEV_USER_LOG_DIR="$HOME/Library/Logs/Pioneer/${DEV_SERVICE_NAME}"
DEV_SYSTEM_LOG_DIR="/Library/Logs/Pioneer/${DEV_SERVICE_NAME}"

DEV_WRAPPER="$HOME/.local/bin/${DEV_COMMAND}"

DEV_INSTALL_ROOTS=(
  "$HOME/Library/Application Support/PioneerDev/managed-dev"
  "$HOME/.local/share/PioneerDev/managed-dev"
  "$HOME/.local/share/pioneer-dev/managed-dev"
)

log() {
  printf '[pioneer-reset] %s\n' "$*"
}

user_launchctl_domain() {
  printf 'gui/%s' "$(id -u)"
}

stop_and_disable_user_service() {
  local service_name="$1"
  local plist_path="$2"
  local domain
  local target
  domain="$(user_launchctl_domain)"
  target="${domain}/${service_name}"

  log "stop/disable user launchd service: ${service_name}"
  launchctl bootout "$domain" "$plist_path" >/dev/null 2>&1 || true
  launchctl bootout "$target" >/dev/null 2>&1 || true
  launchctl disable "$target" >/dev/null 2>&1 || true
  launchctl remove "$service_name" >/dev/null 2>&1 || true
}

stop_and_disable_system_service_if_present() {
  local service_name="$1"
  local plist_path="$2"
  if [[ ! -e "$plist_path" && ! -L "$plist_path" ]]; then
    return
  fi

  log "stop/disable legacy system launchd dev service: ${service_name}"
  sudo launchctl bootout "system" "$plist_path" >/dev/null 2>&1 || true
  sudo launchctl bootout "system/${service_name}" >/dev/null 2>&1 || true
  sudo launchctl disable "system/${service_name}" >/dev/null 2>&1 || true
  sudo launchctl remove "${service_name}" >/dev/null 2>&1 || true
}

kill_port_listener_if_any() {
  local port="$1"
  local pids
  pids="$(lsof -tiTCP:"${port}" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -n "${pids}" ]]; then
    log "kill listeners on TCP:${port} (${pids//$'\n'/, })"
    while IFS= read -r pid; do
      [[ -n "$pid" ]] || continue
      kill -TERM "$pid" >/dev/null 2>&1 || true
    done <<< "$pids"
    sleep 1
    pids="$(lsof -tiTCP:"${port}" -sTCP:LISTEN 2>/dev/null || true)"
    if [[ -n "${pids}" ]]; then
      while IFS= read -r pid; do
        [[ -n "$pid" ]] || continue
        kill -KILL "$pid" >/dev/null 2>&1 || true
      done <<< "$pids"
    fi
  fi
}

remove_path() {
  local path="$1"
  if [[ -e "$path" || -L "$path" ]]; then
    log "remove: $path"
    rm -rf "$path"
  fi
}

remove_path_sudo() {
  local path="$1"
  if sudo test -e "$path" || sudo test -L "$path"; then
    log "remove (sudo): $path"
    sudo rm -rf "$path"
  fi
}

log "1/6 stop dev launchd services"
stop_and_disable_user_service "$DEV_SERVICE_NAME" "$DEV_USER_PLIST"
stop_and_disable_system_service_if_present "$DEV_SERVICE_NAME" "$DEV_SYSTEM_PLIST"
for legacy_service_name in "${DEV_LEGACY_SERVICE_NAMES[@]}"; do
  stop_and_disable_user_service "$legacy_service_name" "$HOME/Library/LaunchAgents/${legacy_service_name}.plist"
done

log "2/6 remove dev launchd plist files"
remove_path "$DEV_USER_PLIST"
remove_path_sudo "$DEV_SYSTEM_PLIST"
for legacy_service_name in "${DEV_LEGACY_SERVICE_NAMES[@]}"; do
  remove_path "$HOME/Library/LaunchAgents/${legacy_service_name}.plist"
done

log "3/6 remove dev installed binary roots + logs"
for install_root in "${DEV_INSTALL_ROOTS[@]}"; do
  remove_path "$install_root"
done
remove_path "$DEV_USER_LOG_DIR"
remove_path_sudo "$DEV_SYSTEM_LOG_DIR"

log "4/6 remove dev runtime state"
remove_path "$DEV_HOME_DIR"
remove_path_sudo "$ROOT_DEV_HOME_DIR"

log "5/6 remove dev command wrapper"
remove_path "$DEV_WRAPPER"

log "6/6 kill leftover listener on dev port"
kill_port_listener_if_any "$DEV_PORT"

hash -r 2>/dev/null || true

log "done"
log "verify:"
log "  which -a ${DEV_COMMAND} || true"
log "  launchctl print $(user_launchctl_domain)/${DEV_SERVICE_NAME} || true"
log "  lsof -nP -iTCP:${DEV_PORT} -sTCP:LISTEN || true"
