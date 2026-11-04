#!/bin/sh
# An app's side of embedding the Tetral CLI, as cli/docs/app-integration.md
# describes it: registering the machine, configuring and starting the CLI,
# reading its local API and stopping it. It stands in for the app's backend
# in the registration and for the user's answer to enabling drives.
#
#   sh app-integration.sh register | start | wait-online | status | stop
#
# It reads TETRAL_HOME, TETRAL_APP, TETRAL_BIN, TETRAL_BASE_URL,
# TETRAL_API_KEY and TETRAL_ENABLE_DRIVES (section 14 of the guide) and
# needs curl and jq. Each step's JSON object goes to stdout, one per line; a
# failure also writes "app-integration.sh: <code>" to stderr and exits 1.
set -u

# No program this script runs inherits the API key.
api_key=${TETRAL_API_KEY-}
unset TETRAL_API_KEY

nl='
'

# Writes the failing step's object, then its code to stderr, and exits 1.
fail() {
    [ -n "$1" ] && printf '%s\n' "$1"
    echo "app-integration.sh: $2" >&2
    exit 1
}

# Fails with the script's own error object.
fail_own() {
    fail "$(jq -nc --arg code "$1" --arg message "$2" '{error: {code: $code, message: $message}}')" "$1"
}

# Checks curl and jq, then that each named variable is set and valid.
require() {
    for program in curl jq; do
        if ! command -v "$program" >/dev/null 2>&1; then
            fail "{\"error\":{\"code\":\"missing_prerequisite\",\"message\":\"$program is not on PATH\"}}" missing_prerequisite
        fi
    done
    for name in "$@"; do
        case $name in
        TETRAL_HOME)
            case ${TETRAL_HOME-} in
            /*) ;;
            *) fail_own invalid_arguments "TETRAL_HOME must be an absolute path" ;;
            esac
            ;;
        TETRAL_APP)
            [ -n "${TETRAL_APP-}" ] || fail_own invalid_arguments "TETRAL_APP must be set"
            ;;
        TETRAL_BIN)
            case ${TETRAL_BIN-} in
            /*) [ -x "$TETRAL_BIN" ] || fail_own invalid_arguments "TETRAL_BIN must name an executable" ;;
            *) fail_own invalid_arguments "TETRAL_BIN must be an absolute path" ;;
            esac
            ;;
        TETRAL_BASE_URL)
            case ${TETRAL_BASE_URL-} in
            http://?* | https://?*) ;;
            *) fail_own invalid_arguments "TETRAL_BASE_URL must be an http or https URL" ;;
            esac
            ;;
        TETRAL_API_KEY)
            [ -n "$api_key" ] || fail_own invalid_arguments "TETRAL_API_KEY must be set"
            ;;
        TETRAL_ENABLE_DRIVES)
            case ${TETRAL_ENABLE_DRIVES-} in
            '' | 0 | 1) ;;
            *) fail_own invalid_arguments "TETRAL_ENABLE_DRIVES must be 1, 0 or unset" ;;
            esac
            ;;
        esac
    done
}

macos() {
    [ "$(uname -s)" = Darwin ]
}

# The App Server's socket for TETRAL_HOME (section 9 of the guide); fails
# when the root does not exist.
socket() {
    root=$(CDPATH='' cd -P -- "$TETRAL_HOME" 2>/dev/null && pwd -P) || return 1
    if macos; then
        printf '/private/tmp/tetral-sock-%s/%s.sock' "$(id -u)" \
            "$(printf '%s' "$root" | shasum -a 256 | cut -c1-16)"
    else
        printf '/tmp/tetral-sock-%s/%s.sock' "$(id -u)" \
            "$(printf '%s' "$root" | sha256sum | cut -c1-16)"
    fi
}

# One local API request: sets `answer_status` and `answer_body`; fails when
# it cannot connect.
local_api() {
    sock=$(socket) || return 1
    answer=$(curl -s --unix-socket "$sock" -X "$1" -w "\\n%{http_code}" "http://localhost$2") || return 1
    answer_status=${answer##*"$nl"}
    answer_body=${answer%"$nl"*}
}

# Writes a tetral command's object; on a failure, fails with its code.
tetral_result() {
    object=$(printf '%s' "$2" | jq -c . 2>/dev/null)
    if [ "$1" -ne 0 ]; then
        code=$(printf '%s' "$object" | jq -r '.error.code // empty' 2>/dev/null)
        fail "$object" "${code:-tetral exit status $1}"
    fi
    printf '%s\n' "$object"
}

# `tetral drives enable`, passing on a SIGINT or SIGTERM meanwhile.
drives_enable() {
    output=$(mktemp) || exit 1
    "$TETRAL_BIN" drives enable --app "$TETRAL_APP" >"$output" &
    child=$!
    trap 'kill -INT "$child" 2>/dev/null' INT
    trap 'kill -TERM "$child" 2>/dev/null' TERM
    wait "$child"
    status=$?
    while kill -0 "$child" 2>/dev/null; do
        wait "$child"
        status=$?
    done
    trap - INT TERM
    result=$(cat "$output")
    rm -f "$output"
    tetral_result "$status" "$result"
}

register() {
    require TETRAL_HOME TETRAL_APP TETRAL_BIN TETRAL_BASE_URL TETRAL_API_KEY TETRAL_ENABLE_DRIVES
    if [ -e "$TETRAL_HOME/config.toml" ]; then
        fail_own already_configured "$TETRAL_HOME/config.toml exists"
    fi
    key=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
    body=$(jq -nc --arg name "$(uname -n)" '{name: $name, credential: {}}')
    response=$(printf 'X-Api-Key: %s\n' "$api_key" | curl -s -H @- \
        -H "Idempotency-Key: $key" -H 'Content-Type: application/json' \
        --data-binary "$body" -w "\\n%{http_code}" "${TETRAL_BASE_URL%/}/v1/instances")
    sent=$?
    [ "$sent" -eq 0 ] || fail_own request_failed "curl: exit status $sent"
    status=${response##*"$nl"}
    response=${response%"$nl"*}
    if [ "$status" != 201 ]; then
        if printf '%s' "$response" | jq -e '.type == "error" and (.error.type | type == "string")' >/dev/null 2>&1; then
            fail "$(printf '%s' "$response" | jq -c .)" "$(printf '%s' "$response" | jq -r .error.type)"
        fi
        fail_own request_failed "HTTP $status"
    fi
    printf '%s' "$response" | jq -c .instance
    result=$(printf '%s' "$response" | jq -c --arg url "$TETRAL_BASE_URL" '{
        server_url: $url,
        instance_id: .instance.id,
        credential_id: .credential.id,
        connection_key: .credential.connection_key,
        expires_at: .credential.expires_at
    }' | "$TETRAL_BIN" configure --app "$TETRAL_APP" --from-stdin)
    tetral_result $? "$result"
    if macos && [ "${TETRAL_ENABLE_DRIVES-}" = 1 ]; then
        drives_enable
    fi
}

start() {
    require TETRAL_HOME TETRAL_APP TETRAL_BIN
    exec "$TETRAL_BIN" server run --app "$TETRAL_APP"
}

wait_online() {
    require TETRAL_HOME
    answered=0
    reads=0
    while [ "$reads" -lt 120 ]; do
        [ "$reads" -eq 0 ] || sleep 1
        reads=$((reads + 1))
        local_api GET /v1/status || continue
        answered=1
        [ "$answer_status" = 200 ] || continue
        case $(printf '%s' "$answer_body" | jq -r .connection_state) in
        online)
            printf '%s' "$answer_body" | jq -c .
            return
            ;;
        blocked)
            fail "$(printf '%s' "$answer_body" | jq -c .)" "$(printf '%s' "$answer_body" | jq -r .last_error.code)"
            ;;
        esac
    done
    if [ "$answered" -eq 0 ]; then
        fail_own server_not_running "no App Server answered on its socket"
    fi
    fail_own not_online "the connection was not online after 120 seconds"
}

# Reads one local API endpoint and writes its object.
read_endpoint() {
    local_api GET "$1" || fail_own server_not_running "no App Server answered on its socket"
    [ "$answer_status" = 200 ] || fail_own request_failed "HTTP $answer_status"
    printf '%s' "$answer_body" | jq -c .
}

status() {
    require TETRAL_HOME
    read_endpoint /v1/status
    read_endpoint /v1/sessions
}

stop() {
    require TETRAL_HOME
    local_api POST /v1/shutdown || fail_own server_not_running "no App Server answered on its socket"
    [ "$answer_status" = 202 ] || fail_own request_failed "HTTP $answer_status"
    checks=0
    while [ -e "$sock" ]; do
        [ "$checks" -lt 10 ] || fail_own stop_timeout "$sock is still there 10 seconds after the shutdown"
        sleep 1
        checks=$((checks + 1))
    done
    echo '{}'
}

case ${1-} in
register) [ "$#" -eq 1 ] && register ;;
start) [ "$#" -eq 1 ] && start ;;
wait-online) [ "$#" -eq 1 ] && wait_online ;;
status) [ "$#" -eq 1 ] && status ;;
stop) [ "$#" -eq 1 ] && stop ;;
*) false ;;
esac || {
    command -v jq >/dev/null 2>&1 ||
        fail '{"error":{"code":"missing_prerequisite","message":"jq is not on PATH"}}' missing_prerequisite
    fail_own invalid_arguments "usage: app-integration.sh register | start | wait-online | status | stop"
}
