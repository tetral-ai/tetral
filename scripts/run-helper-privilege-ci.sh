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
	for test_name in \
		TestBuiltHelperForegroundExecUsesRuntimeIdentityAndGitConfiguration \
		TestBuiltHelperDetachedExecUsesRuntimeIdentityAndGitConfiguration \
		TestBuiltHelperFileToolUsesRuntimeIdentity \
		TestRuntimeAdapterReadUsesRuntimeProcessIdentityAndEnvironment; do
		if ! grep -Fq -- "--- PASS: ${test_name}" <<<"$output"; then
			echo "sandbox-helper runtime identity proof ${test_name} did not report PASS" >&2
			return 1
		fi
	done
}

if [[ "${1:-}" == "--verify-output" ]]; then
  verify_output
  exit
fi

engine_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
proof_dir="$(mktemp -d)"
image_tag="tetral-helper-identity-proof:${$}"
cleanup() {
  docker image rm --force "$image_tag" >/dev/null 2>&1 || true
  rm -rf -- "$proof_dir"
}
trap cleanup EXIT

docker build --tag "$image_tag" --file "$engine_root/Dockerfile.sandbox" "$engine_root"
if [[ "$(docker image inspect --format '{{.Config.User}}' "$image_tag")" != "daytona" ]]; then
  echo "Sandbox image does not select the daytona runtime account" >&2
  exit 1
fi

set +e
# Tetral's public GHCR mirror avoids anonymous third-party registry limits on shared runner IPs.
output="$({
  docker run --rm \
    --user 0:0 \
    --env GOFLAGS=-buildvcs=false \
    --volume "$engine_root:/engine" \
    --volume "$proof_dir:/proof" \
    --workdir /engine \
    ghcr.io/tetral-ai/mirror/golang:1.25.12 \
    sh -ceu '
      test "$(id -u)" -eq 0
      go test ./internal/sandbox/helper -run "^TestSupervisorKeepsDetachedTaskAuthorizationAfterPrivilegeDrop$" -count=1 -v
      CGO_ENABLED=0 go test -c -o /proof/cli.test ./internal/sandbox/helper/internal/cli
    '
  docker run --rm \
    --user 0:0 \
    --env TETRAL_RUN_ROOT_IDENTITY_TESTS=1 \
    --env TETRAL_TEST_HELPER_BINARY=/usr/local/bin/sandbox \
    --volume "$proof_dir:/proof:ro" \
    --entrypoint /proof/cli.test \
    "$image_tag" \
    -test.run '^Test(BuiltHelper(Foreground|Detached)ExecUsesRuntimeIdentityAndGitConfiguration|BuiltHelperFileToolUsesRuntimeIdentity|RuntimeAdapterReadUsesRuntimeProcessIdentityAndEnvironment)$' \
    -test.count=1 \
    -test.v
} 2>&1)"
status=$?
set -e
printf '%s\n' "$output"
if [[ "$status" -ne 0 ]]; then
  exit "$status"
fi
verify_output <<<"$output"
