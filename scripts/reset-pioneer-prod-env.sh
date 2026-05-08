#!/usr/bin/env bash
set -euo pipefail

# Pioneer production reset for macOS.
# This intentionally does not touch dev artifacts:
#   command/service/home: pioneer-dev, com.pioneer.gateway.dev, ~/.pioneer.dev
#   desktop app/data: ~/Library/Application Support/PioneerDev
#   dev port: 18778
#
# Production settings accounted for:
#   home_directory = ".pioneer"
#   install.macos_root_directory_name = "Pioneer"
#   install.managed_directory_name = "managed"
#   install.command_name = "pioneer"
#   gateway.service_name = "com.pioneer.gateway"
#   gateway.listen_addr = "0.0.0.0:17878"

PROD_SERVICE_NAME="com.pioneer.gateway"
PROD_HOME_DIR="$HOME/.pioneer"
ROOT_PROD_HOME_DIR="/var/root/.pioneer"
PROD_PORT="17878"
PROD_COMMAND="pioneer"

PROD_USER_PLIST="$HOME/Library/LaunchAgents/${PROD_SERVICE_NAME}.plist"
PROD_SYSTEM_PLIST="/Library/LaunchDaemons/${PROD_SERVICE_NAME}.plist"

PROD_USER_LOG_DIR="$HOME/Library/Logs/Pioneer/${PROD_SERVICE_NAME}"
PROD_SYSTEM_LOG_DIR="/Library/Logs/Pioneer/${PROD_SERVICE_NAME}"

PROD_WRAPPER="$HOME/.local/bin/${PROD_COMMAND}"

PROD_APP_PATHS=(
  "/Applications/Pioneer.app"
  "$HOME/Applications/Pioneer.app"
)

PROD_APP_DATA_PATHS=(
  "$HOME/Library/Application Support/Pioneer"
  "$HOME/Library/Caches/Pioneer"
  "$HOME/Library/Saved Application State/ai.pioneer.macos.savedState"
  "$HOME/Library/Preferences/ai.pioneer.macos.plist"
)

PROD_INSTALL_ROOTS=(
  "$HOME/Library/Application Support/Pioneer/managed"
  "$HOME/.local/share/Pioneer/managed"
  "$HOME/.local/share/pioneer/managed"
)

log() {
  printf '[pioneer-reset-prod] %s\n' "$*"
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

  log "stop/disable legacy system launchd prod service: ${service_name}"
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

log "1/8 stop prod launchd services"
stop_and_disable_user_service "$PROD_SERVICE_NAME" "$PROD_USER_PLIST"
stop_and_disable_system_service_if_present "$PROD_SERVICE_NAME" "$PROD_SYSTEM_PLIST"

log "2/8 remove prod launchd plist files"
remove_path "$PROD_USER_PLIST"
remove_path_sudo "$PROD_SYSTEM_PLIST"

log "3/8 remove prod installed binary roots"
for install_root in "${PROD_INSTALL_ROOTS[@]}"; do
  remove_path "$install_root"
done

log "4/8 remove prod desktop app + desktop data"
for app_path in "${PROD_APP_PATHS[@]}"; do
  remove_path_sudo "$app_path"
done
for app_data_path in "${PROD_APP_DATA_PATHS[@]}"; do
  remove_path "$app_data_path"
done

log "5/8 remove prod logs"
remove_path "$PROD_USER_LOG_DIR"
remove_path_sudo "$PROD_SYSTEM_LOG_DIR"

log "6/8 remove prod runtime state"
remove_path "$PROD_HOME_DIR"
remove_path_sudo "$ROOT_PROD_HOME_DIR"

log "7/8 remove prod command wrapper"
remove_path "$PROD_WRAPPER"

log "8/8 kill leftover listener on prod port"
kill_port_listener_if_any "$PROD_PORT"

hash -r 2>/dev/null || true

log "done"
log "verify:"
log "  which -a ${PROD_COMMAND} || true"
log "  launchctl print $(user_launchctl_domain)/${PROD_SERVICE_NAME} || true"
log "  lsof -nP -iTCP:${PROD_PORT} -sTCP:LISTEN || true"
