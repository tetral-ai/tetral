#!/usr/bin/env bash
# Runs a Bun dependency audit while distinguishing registry outages from findings.

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <package-directory>" >&2
  exit 2
fi

engine_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
package_dir="$engine_root/$1"
if [[ ! -f "$package_dir/package.json" ]]; then
  echo "bun audit package directory is invalid: $1" >&2
  exit 2
fi

attempt=1
max_attempts=3
retry_delay_seconds="${TETRAL_BUN_AUDIT_RETRY_DELAY_SECONDS:-5}"
output="$(mktemp)"
trap 'rm -f "$output"' EXIT

while (( attempt <= max_attempts )); do
  set +e
  (cd "$package_dir" && timeout 5m bun audit) >"$output" 2>&1
  status=$?
  set -e

  cat "$output"
  if (( status == 0 )); then
    exit 0
  fi

  transient=false
  if (( status == 124 )); then
    transient=true
  elif grep -Eqi \
    'audit request failed \(status (408|425|429|500|502|503|504)\)|(ConnectionClosed|ConnectionRefused|ConnectionReset|TimedOut|Timeout).*audit request failed|audit request failed.*(connection|timed out|timeout)' \
    "$output"; then
    transient=true
  fi

  if [[ "$transient" != true ]] || (( attempt == max_attempts )); then
    exit "$status"
  fi

  echo "bun audit transport failure; retrying ($attempt/$max_attempts)" >&2
  sleep "$((retry_delay_seconds * attempt))"
  attempt=$((attempt + 1))
done
