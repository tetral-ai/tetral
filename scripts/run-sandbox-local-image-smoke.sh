#!/usr/bin/env bash
# Builds the repository's Sandbox image and exercises its local runtime contract.
set -euo pipefail

engine_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_context="${TETRAL_SANDBOX_SMOKE_BUILD_CONTEXT:-$engine_root}"
image_tag="tetral-sandbox-local-smoke:${$}"

cleanup() {
  docker image rm --force "$image_tag" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker build --tag "$image_tag" \
  --file "$build_context/Dockerfile.sandbox" "$build_context"

docker run --rm --entrypoint /bin/bash "$image_tag" -ceu '
  test "$(id -u)" -ne 0
  test "$(id -un)" = daytona
  test "$HOME" = /home/daytona
  test "$(getent passwd "$(id -u)" | cut -d: -f7)" = /bin/bash
  test -x /usr/local/bin/sandbox

  mkdir -p /workspace/smoke-repo

  file_payload_root=/tmp/tetral-runtime/tool-payloads/smoke_file
  install -d -m 0700 "$file_payload_root"
  printf "%s\n" '\''{"schema_version":1,"tool":"write","tool_use_event_id":"smoke_file","workspace_root":"/workspace","roots":[{"path":"/workspace","mode":"read_write"}],"limits":{"visible_bytes":262144,"visible_lines":2000},"input":{"path":"smoke-repo/smoke.txt","content":"local sandbox smoke\n"}}'\'' > "$file_payload_root/payload.json"
  chmod 0600 "$file_payload_root/payload.json"
  if ! file_result="$(/usr/local/bin/sandbox write --payload "$file_payload_root/payload.json")"; then
    printf "local Sandbox file operation failed: %s\n" "$file_result" >&2
    exit 1
  fi
  if ! grep -Fq '\''"status":"success"'\'' <<<"$file_result"; then
    printf "local Sandbox file operation was not successful: %s\n" "$file_result" >&2
    exit 1
  fi
  test "$(cat /workspace/smoke-repo/smoke.txt)" = "local sandbox smoke"

  git_payload_root=/tmp/tetral-runtime/tool-payloads/smoke_git
  install -d -m 0700 "$git_payload_root"
  printf "%s\n" '\''{"schema_version":1,"tool":"exec","tool_use_event_id":"smoke_git","workspace_root":"/workspace","roots":[{"path":"/workspace","mode":"read_write"}],"limits":{"visible_bytes":262144,"visible_lines":2000},"input":{"cmd":"git init -q smoke-repo && git -C smoke-repo config user.name \"Tetral Smoke\" && git -C smoke-repo config user.email smoke@tetral.invalid && git -C smoke-repo add smoke.txt && git -C smoke-repo status --short","cwd":"/workspace","on_wait_expiry":"kill","wait_ms":5000,"task_lifetime_ms":30000}}'\'' > "$git_payload_root/payload.json"
  chmod 0600 "$git_payload_root/payload.json"
  if ! git_result="$(/usr/local/bin/sandbox exec --payload "$git_payload_root/payload.json")"; then
    printf "local Sandbox git operation failed: %s\n" "$git_result" >&2
    exit 1
  fi
  if ! grep -Fq '\''"status":"success"'\'' <<<"$git_result" || ! grep -Fq '\''A  smoke.txt'\'' <<<"$git_result"; then
    printf "local Sandbox git operation was not successful: %s\n" "$git_result" >&2
    exit 1
  fi

  printf "local-sandbox-image-smoke: helper file and git operations passed for %s\n" "$(id -un)"
'
