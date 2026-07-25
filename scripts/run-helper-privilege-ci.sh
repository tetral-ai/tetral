#!/usr/bin/env bash
# Produces the root-run sandbox-helper privilege-continuity proof used by CI.
set -euo pipefail

verify_output() {
  local output
  output="$(cat)"
  if grep -Fq -- '--- SKIP:' <<<"$output"; then
    echo "sandbox-helper privilege proof skipped" >&2
    return 1
  fi
  if ! grep -Fq -- '--- PASS: TestSupervisorKeepsDetachedTaskAuthorizationAfterPrivilegeDrop' <<<"$output"; then
    echo "sandbox-helper privilege proof did not report PASS" >&2
    return 1
  fi
}

if [[ "${1:-}" == "--verify-output" ]]; then
  verify_output
  exit
fi

engine_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

set +e
output="$(docker run --rm \
  --user 0:0 \
  --env GOFLAGS=-buildvcs=false \
  --volume "$engine_root:/workspace/engine" \
  --workdir /workspace/engine \
  golang:1.25.12 \
  sh -ceu 'test "$(id -u)" -eq 0; go test ./internal/sandbox/helper -run "^TestSupervisorKeepsDetachedTaskAuthorizationAfterPrivilegeDrop$" -count=1 -v' 2>&1)"
status=$?
set -e
printf '%s\n' "$output"
if [[ "$status" -ne 0 ]]; then
  exit "$status"
fi
verify_output <<<"$output"
