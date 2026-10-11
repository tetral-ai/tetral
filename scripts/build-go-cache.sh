#!/usr/bin/env bash
# Compiles what Go Race jobs compile, so the Go build cache entry saved
# afterwards serves their builds. Run it from the root of the checkout to
# build. Each command matches the flags, environment and directory of the
# build it warms; a difference in any of them makes that build miss.
#
# It executes no repository or test binary and stores no test result:
# -exec /bin/true never starts a test binary, so go test writes no test log
# and caches no result, and -count=1 disables result caching as well.

set -euo pipefail

if [[ ! -f go.mod || ! -f deploy/dependencies.lock.json ]]; then
  echo "run from the repository root" >&2
  exit 2
fi

# Every package's race test build and vet run.
go test -race -count=1 -run '^$' -exec /bin/true ./...

# The pinned SecretType helper, built as the runner's egctl dependency builds
# it (internal/testinfra/edge_dependencies.go).
helper_directory="$(jq -er '.envoy_gateway.secret_helper.module_directory' deploy/dependencies.lock.json)"
helper_toolchain="$(jq -er '.envoy_gateway.secret_helper.go_toolchain' deploy/dependencies.lock.json)"
(cd "$helper_directory" && GOTOOLCHAIN="$helper_toolchain" go build -mod=readonly -p 2 -o /dev/null .)

# The protocol generator the evidence action installs.
go install github.com/bufbuild/buf/cmd/buf@v1.70.0

# The repository runner every evidence job starts.
go build -o /dev/null ./internal/testinfra/cmd/tetral-test

# Process fixtures integration tests build (replica_web_connector_test.go,
# transporttest/proxy.go).
go build -race -o /dev/null ./services/web-connector/cmd/web-connector
CGO_ENABLED=0 go build -trimpath -o /dev/null ./integration/transporttest/cmd/backend
