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
		TestRunReadUsesRuntimeProcessIdentityAndEnvironment; do
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

set +e
# Tetral's public GHCR mirror avoids anonymous third-party registry limits on shared runner IPs.
output="$(docker run --rm \
  --user 0:0 \
  --env GOFLAGS=-buildvcs=false \
  --volume "$engine_root:/engine" \
  --workdir /engine \
  ghcr.io/tetral-ai/mirror/golang:1.25.12 \
  sh -ceu '
    test "$(id -u)" -eq 0
    printf "%s\n" "daytona:x:1000:1000:Tetral runtime:/home/daytona:/bin/sh" >> /etc/passwd
    printf "%s\n" "daytona:x:1000:" >> /etc/group
    go test ./internal/sandbox/helper -run "^TestSupervisorKeepsDetachedTaskAuthorizationAfterPrivilegeDrop$" -count=1 -v
    # Main stderr redirection creates this exact disposable-container path; the
    # next proof requires a production-shaped runtime root created from zero.
    rm -rf -- /tmp/tetral-runtime
    go build -o /tmp/tetral-runtime-identity-helper ./internal/sandbox/helper/cmd/sandbox
    TETRAL_RUN_ROOT_IDENTITY_TESTS=1 \
      TETRAL_TEST_HELPER_BINARY=/tmp/tetral-runtime-identity-helper \
      go test ./internal/sandbox/helper/internal/cli \
        -run "^Test(BuiltHelper(Foreground|Detached)ExecUsesRuntimeIdentityAndGitConfiguration|RunReadUsesRuntimeProcessIdentityAndEnvironment)$" \
        -count=1 -v
  ' 2>&1)"
status=$?
set -e
printf '%s\n' "$output"
if [[ "$status" -ne 0 ]]; then
  exit "$status"
fi
verify_output <<<"$output"
