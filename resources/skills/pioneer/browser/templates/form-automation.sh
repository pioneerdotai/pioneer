#!/bin/bash
# Modified for Pioneer.
# Requires jq 1.6+ to validate structured CLI replies; no policy is changed.
# Usage: ./form-automation.sh <form-url> [output-root] (default: .)
# Discovery only by default: save form-structure.txt and form.png in a new
# private form-XXXXXX directory. Customize the marked block for authorized actions.
# AGENT_BROWSER_SESSION: reuse a task-specific caller session; leave it open.
# Otherwise a unique session is created and closed on success/error/signals.
# BROWSER_STATE_FILE: optional approved input state JSON.
# BROWSER_WAIT_TEXT / BROWSER_WAIT_URL / BROWSER_WAIT_FN: at most one readiness
# condition; default waits for a visible enabled form control.
# Artifacts may contain private data; inspect/redact before sharing.

set -euo pipefail
umask 077

if ! command -v jq >/dev/null 2>&1; then
    printf 'jq 1.6+ is required to validate browser command completion.\n' >&2
    exit 2
fi

if [[ $# -lt 1 || $# -gt 2 ]]; then
    printf 'Usage: %s <url> [output-root]\n' "$0" >&2
    exit 2
fi
target_url=$1
output_root=${2:-.}
state_file=${BROWSER_STATE_FILE:-}
wait_flag=
wait_value=
wait_count=0
if [[ -n ${BROWSER_WAIT_TEXT:-} ]]; then
    wait_flag=--text; wait_value=$BROWSER_WAIT_TEXT; wait_count=$((wait_count + 1))
fi
if [[ -n ${BROWSER_WAIT_URL:-} ]]; then
    wait_flag=--url; wait_value=$BROWSER_WAIT_URL; wait_count=$((wait_count + 1))
fi
if [[ -n ${BROWSER_WAIT_FN:-} ]]; then
    wait_flag=--fn; wait_value=$BROWSER_WAIT_FN; wait_count=$((wait_count + 1))
fi
if [[ $wait_count -gt 1 ]]; then
    printf 'Set only one of BROWSER_WAIT_TEXT, BROWSER_WAIT_URL, BROWSER_WAIT_FN.\n' >&2
    exit 2
fi
if [[ -n $state_file && ( ! -f $state_file || ! -r $state_file ) ]]; then
    printf 'BROWSER_STATE_FILE must be a readable approved state file.\n' >&2
    exit 2
fi
case ${AGENT_BROWSER_SESSION:-} in
    default|*[!a-zA-Z0-9_-]*)
        printf 'AGENT_BROWSER_SESSION must be a task-specific name using letters, digits, _ or -.\n' >&2
        exit 2 ;;
esac

mkdir -p -- "$output_root"
output_root=$(cd -- "$output_root" && pwd -P)
output_dir=$(mktemp -d "$output_root/form-XXXXXX")
session=${AGENT_BROWSER_SESSION:-}
owns_session=0
browser_started=0
cleanup_attempted=0
if [[ -z $session ]]; then
    session="pioneer-${output_dir##*/}-$$"
    owns_session=1
fi

cleanup() {
    local status=$?
    trap - EXIT INT TERM
    if ! close_owned_session; then
        if [[ $status -eq 0 ]]; then status=1; fi
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Pass /dev/null for status-only commands; save only requested page artifacts.
# Read the structured result: exit 0 can mean an action is awaiting confirmation.
# Print only step names; arguments, raw errors and response data stay private.
browser() {
    local destination=$1
    local status=0
    local reply
    shift
    local step=$1
    case "$1 ${2:-}" in
        "state load"|"state save"|"auth login"|"get title"|"get url"|"get text")
            step="$1 $2" ;;
    esac
    reply=$(mktemp "$output_dir/.response-XXXXXX")
    browser_started=1
    agent-browser --session "$session" --json "$@" >"$reply" 2>/dev/null || status=$?
    if [[ $status -eq 0 ]]; then
        jq -rs --arg step "$step" --arg destination "$destination" '
            def pending:
                if type != "object" then false
                elif .confirmation_required == true then true
                else .result.data? | pending end;
            (if length == 1 then .[0] else "" | halt_error(1) end) |
            if type != "object" or .success != true then "" | halt_error(1)
            elif (.data | pending) then "" | halt_error(42)
            elif $destination == "/dev/null" then empty
            else
                .data |
                (if $step == "snapshot" then
                    .snapshot | if type == "object" and .kind == "full" then .tree else . end
                 elif $step == "get title" then .title
                 elif $step == "get url" then .url
                 elif $step == "get text" then .text
                 else null end) |
                if type == "string" then . else "" | halt_error(1) end
            end
        ' "$reply" >"$destination" 2>/dev/null || status=$?
        if [[ $status -eq 42 ]]; then
            mv -- "$reply" "$output_dir/pending-${step// /-}.json"
            if [[ $destination != /dev/null ]]; then rm -f -- "$destination"; fi
            printf 'Browser step "%s" requires existing CLI confirmation; workflow stopped. Private response: %s\n' \
                "$step" "$output_dir/pending-${step// /-}.json" >&2
            return 3
        fi
    fi
    rm -f -- "$reply"
    if [[ $status -ne 0 ]]; then
        if [[ $destination != /dev/null ]]; then rm -f -- "$destination"; fi
        printf 'Browser step "%s" failed (exit %s); raw output suppressed.\n' "$step" "$status" >&2
        return "$status"
    fi
}

close_owned_session() {
    if [[ $owns_session -eq 1 && $browser_started -eq 1 && $cleanup_attempted -eq 0 ]]; then
        cleanup_attempted=1
        browser /dev/null close
    fi
}

if [[ -n $state_file ]]; then browser /dev/null state load "$state_file"; fi
browser /dev/null open "$target_url"
browser /dev/null wait --load domcontentloaded
if [[ $wait_count -eq 1 ]]; then
    browser /dev/null wait "$wait_flag" "$wait_value"
else
    browser /dev/null wait --fn "Array.from(document.querySelectorAll('input:not([type=hidden]), textarea, select, button')).some((el) => { const style = getComputedStyle(el); return !el.disabled && !el.readOnly && style.display !== 'none' && style.visibility !== 'hidden' && el.getClientRects().length > 0; })"
fi
browser "$output_dir/form-structure.txt" snapshot -i
browser /dev/null screenshot "$output_dir/form.png"

# CUSTOMIZE: use observed labels or fresh snapshot refs for authorized actions.
# Keep commands in this wrapper so they all target the same named session.
# browser /dev/null find label "Name" fill "$FORM_NAME"
# browser /dev/null find role button click --name "Submit"
# browser /dev/null wait --text "Saved"
# browser "$output_dir/result-structure.txt" snapshot -i
# browser /dev/null screenshot "$output_dir/result.png"

close_owned_session
printf 'Private form artifacts: %s\n' "$output_dir"
