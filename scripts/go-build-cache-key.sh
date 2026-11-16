#!/usr/bin/env bash
# Computes the key of the Go build cache entry of the checked-out commit and
# its fallback prefixes, for every job that reads or writes the entry, and
# writes them to the step's outputs. Run it from the root of the checkout.
#
# The go.sum part is the value GitHub's hashFiles gives for the two files:
# the SHA-256 of their SHA-256 digests, in this order.

set -euo pipefail

if [[ ! -f go.mod || -z "${GITHUB_OUTPUT:-}" ]]; then
  echo "run from the repository root of a workflow step" >&2
  exit 2
fi

digests() {
  local file hex
  for file in "$@"; do
    hex="$(sha256sum "$file" | cut -c 1-64)"
    printf '%b' "$(printf '%s' "$hex" | sed 's/../\\x&/g')"
  done
}

go_sum_hash="$(digests go.sum integration/envoy-gateway-secret-helper/go.sum | sha256sum | cut -c 1-64)"
toolchain="go-race-v1-Linux-$(go env GOVERSION)-"
compiler="${toolchain}cc$(gcc --version | head -n 1 | sha256sum | cut -c 1-12)-"
modules="${compiler}${go_sum_hash}-"
key="${modules}$(git rev-parse HEAD)"
echo "Go build cache key: $key"
{
  echo "key=$key"
  echo "modules-prefix=$modules"
  echo "compiler-prefix=$compiler"
  echo "toolchain-prefix=$toolchain"
} >> "$GITHUB_OUTPUT"
