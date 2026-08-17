#!/usr/bin/env bash
# Produces the root-run sandbox-helper privilege-continuity proof used by CI.
set -euo pipefail

sandbox_runtime_base_reference="ghcr.io/tetral-ai/mirror/ubuntu:24.04@sha256:ee4d23cbccd3aa96de85ab491b0404aecdda1d458931e9ef1aaba733a1128ba9"

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
  if ! grep -Eq -- '^sandbox-runtime-image: user=daytona uid=[1-9][0-9]* gid=[1-9][0-9]* home=/home/daytona shell=/bin/bash env_home=/home/daytona$' <<<"$output"; then
    echo "sandbox-helper proof did not report the real runtime image identity" >&2
    return 1
  fi
  if ! grep -Eq -- 'daytona-adapter-image: id=sha256:[0-9a-f]{64} user=daytona uid=[1-9][0-9]* gid=[1-9][0-9]* home=/home/daytona shell=/bin/bash' <<<"$output"; then
    echo "sandbox-helper proof did not join Daytona adapters to the final image identity" >&2
    return 1
  fi
  local selected_digest="${sandbox_runtime_base_reference##*@}"
  if ! grep -Fxq -- "sandbox-runtime-base: release=$sandbox_runtime_base_reference proof=$sandbox_runtime_base_reference selected=$selected_digest id=ubuntu version=24.04" <<<"$output"; then
    echo "sandbox-helper proof did not report the release runtime base" >&2
    return 1
  fi
	for test_name in \
		TestBuiltHelperForegroundExecUsesRuntimeIdentityAndGitConfiguration \
		TestBuiltHelperDetachedExecUsesRuntimeIdentityAndGitConfiguration \
		TestBuiltHelperFileToolUsesRuntimeIdentity \
		TestBuiltHelperReadUsesRuntimeProcessIdentityAndEnvironment \
		TestDaytonaProductionAdaptersUseFinalImageIdentity; do
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
build_context="${TETRAL_HELPER_PROOF_BUILD_CONTEXT:-$engine_root}"
runtime_base_image="${TETRAL_HELPER_PROOF_RUNTIME_BASE_IMAGE:-$sandbox_runtime_base_reference}"
proof_dir="$(mktemp -d)"
image_tag="tetral-helper-identity-proof:${$}"
cleanup() {
  docker image rm --force "$image_tag" >/dev/null 2>&1 || true
  rm -rf -- "$proof_dir"
}
trap cleanup EXIT

release_runtime_base="$(awk 'toupper($1) == "ARG" && $2 ~ /^SANDBOX_RUNTIME_BASE_IMAGE=/ { sub(/^[^=]*=/, "", $2); print $2 }' "$build_context/Dockerfile.sandbox")"
if [[ "$runtime_base_image" != "$release_runtime_base" ]]; then
  echo "Sandbox proof runtime base does not match the release Dockerfile: release=$release_runtime_base proof=$runtime_base_image" >&2
  exit 1
fi
selected_digest="${runtime_base_image##*@}"
if [[ ! "$selected_digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "Sandbox proof runtime base is not digest-pinned: $runtime_base_image" >&2
  exit 1
fi
runtime_base_identity="$(docker run --rm --entrypoint /bin/sh "$runtime_base_image" -ceu '
  . /etc/os-release
  printf "id=%s version=%s" "$ID" "$VERSION_ID"
')"
if [[ "$runtime_base_identity" != "id=ubuntu version=24.04" ]]; then
  echo "Sandbox proof runtime base is not Ubuntu 24.04: $runtime_base_identity" >&2
  exit 1
fi
runtime_repo_digests="$(docker image inspect --format '{{join .RepoDigests " "}}' "$runtime_base_image")"
if [[ " $runtime_repo_digests " != *"@$selected_digest "* ]]; then
  echo "Sandbox proof pulled runtime base does not expose selected digest $selected_digest: $runtime_repo_digests" >&2
  exit 1
fi
runtime_base_proof="sandbox-runtime-base: release=$release_runtime_base proof=$runtime_base_image selected=$selected_digest $runtime_base_identity"

docker build --tag "$image_tag" \
  --build-arg SANDBOX_HELPER_BASE_IMAGE=ghcr.io/tetral-ai/mirror/golang:1.25.12 \
  --build-arg SANDBOX_RUNTIME_BASE_IMAGE="$runtime_base_image" \
  --file "$build_context/Dockerfile.sandbox" "$build_context"
if [[ "$(docker image inspect --format '{{.Config.User}}' "$image_tag")" != "daytona" ]]; then
  echo "Sandbox image does not select the daytona runtime account" >&2
  exit 1
fi

image_identity="$(docker run --rm --entrypoint /bin/bash "$image_tag" -ceu '
  passwd_entry="$(getent passwd "$(id -u)")"
  IFS=: read -r account_name _ account_uid account_gid _ account_home account_shell <<<"$passwd_entry"
  test "$account_name" = "$(id -un)"
  test "$account_uid" = "$(id -u)"
  test "$account_gid" = "$(id -g)"
  printf "sandbox-runtime-image: user=%s uid=%s gid=%s home=%s shell=%s env_home=%s\n" \
    "$(id -un)" "$(id -u)" "$(id -g)" "$account_home" "$account_shell" "$HOME"
')"
if [[ ! "$image_identity" =~ ^sandbox-runtime-image:\ user=daytona\ uid=([1-9][0-9]*)\ gid=([1-9][0-9]*)\ home=/home/daytona\ shell=/bin/bash\ env_home=/home/daytona$ ]]; then
  echo "Sandbox image runtime process identity is invalid: $image_identity" >&2
  exit 1
fi
runtime_uid="${BASH_REMATCH[1]}"
runtime_gid="${BASH_REMATCH[2]}"

set +e
# Tetral's public GHCR mirror avoids anonymous third-party registry limits on shared runner IPs.
output="$({
  printf '%s\n' "$runtime_base_proof"
  printf '%s\n' "$image_identity"
  docker run --rm \
    --user 0:0 \
    --env GOFLAGS=-buildvcs=false \
    --volume "$build_context:/engine" \
    --volume "$proof_dir:/proof" \
    --workdir /engine \
    ghcr.io/tetral-ai/mirror/golang:1.25.12 \
    sh -ceu '
      test "$(id -u)" -eq 0
      go test ./internal/sandbox/helper -run "^TestSupervisorKeepsDetachedTaskAuthorizationAfterPrivilegeDrop$" -count=1 -v
      CGO_ENABLED=0 go test -c -o /proof/cli.test ./internal/sandbox/helper/internal/cli
      CGO_ENABLED=0 go test -c -o /proof/driver.test ./internal/sandbox/driver
    '
  docker run --rm \
    --user 0:0 \
    --env TETRAL_RUN_ROOT_IDENTITY_TESTS=1 \
    --env TETRAL_TEST_HELPER_BINARY=/usr/local/bin/sandbox \
    --env TETRAL_TEST_RUNTIME_UID="$runtime_uid" \
    --env TETRAL_TEST_RUNTIME_GID="$runtime_gid" \
    --volume "$proof_dir:/proof:ro" \
    --entrypoint /proof/cli.test \
    "$image_tag" \
    -test.run '^Test(BuiltHelper(Foreground|Detached)ExecUsesRuntimeIdentityAndGitConfiguration|BuiltHelper(FileToolUsesRuntimeIdentity|ReadUsesRuntimeProcessIdentityAndEnvironment))$' \
    -test.count=1 \
    -test.v
  docker run --rm \
    --user 0:0 \
    --env TETRAL_TEST_DAYTONA_IMAGE_CONTRACT=1 \
    --env TETRAL_TEST_EXPECTED_IMAGE_ID="$(docker image inspect --format '{{.Id}}' "$image_tag")" \
    --entrypoint /proof/driver.test \
    --volume "$proof_dir:/proof:ro" \
    "$image_tag" \
    -test.run '^TestDaytonaProductionAdaptersUseFinalImageIdentity$' \
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
