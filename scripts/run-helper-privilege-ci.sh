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
runtime_user="$(sed -nE 's/^RUN useradd --create-home --shell [^[:space:]]+ ([^[:space:]]+).*/\1/p' "$engine_root/Dockerfile.sandbox")"
runtime_shell="$(sed -nE 's/^RUN useradd --create-home --shell ([^[:space:]]+) .*/\1/p' "$engine_root/Dockerfile.sandbox")"
runtime_home="/home/$runtime_user"
runtime_uid="$(awk '$1 == "UID" && $2 == "=" { print $3 }' "$engine_root/internal/sandbox/runtimeidentity/identity.go")"
runtime_gid="$(awk '$1 == "GID" && $2 == "=" { print $3 }' "$engine_root/internal/sandbox/runtimeidentity/identity.go")"
for identity_value in "$runtime_user" "$runtime_home" "$runtime_shell" "$runtime_uid" "$runtime_gid"; do
  if [[ -z "$identity_value" ]]; then
    echo "sandbox runtime identity contract is incomplete" >&2
    exit 1
  fi
done

set +e
# Tetral's public GHCR mirror avoids anonymous third-party registry limits on shared runner IPs.
output="$(docker run --rm \
  --user 0:0 \
  --env GOFLAGS=-buildvcs=false \
  --env TETRAL_TEST_RUNTIME_USER="$runtime_user" \
  --env TETRAL_TEST_RUNTIME_HOME="$runtime_home" \
  --env TETRAL_TEST_RUNTIME_SHELL="$runtime_shell" \
  --env TETRAL_TEST_RUNTIME_UID="$runtime_uid" \
  --env TETRAL_TEST_RUNTIME_GID="$runtime_gid" \
  --volume "$engine_root:/engine" \
  --workdir /engine \
  ghcr.io/tetral-ai/mirror/golang:1.25.12 \
  sh -ceu '
    test "$(id -u)" -eq 0
    test -z "$(getent passwd "$TETRAL_TEST_RUNTIME_UID" || true)"
    test -z "$(getent group "$TETRAL_TEST_RUNTIME_GID" || true)"
    printf "%s:x:%s:%s:Tetral runtime:%s:%s\n" \
      "$TETRAL_TEST_RUNTIME_USER" "$TETRAL_TEST_RUNTIME_UID" "$TETRAL_TEST_RUNTIME_GID" \
      "$TETRAL_TEST_RUNTIME_HOME" "$TETRAL_TEST_RUNTIME_SHELL" >> /etc/passwd
    printf "%s:x:%s:\n" "$TETRAL_TEST_RUNTIME_USER" "$TETRAL_TEST_RUNTIME_GID" >> /etc/group
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
