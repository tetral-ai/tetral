#!/usr/bin/env bash
# Shared CLI cache identity, paths and compile-only recipe. All Cargo commands
# run in cli/ so rustup selects its pinned toolchain.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root/cli"

paths() {
  case "$1" in
    common)
      printf '%s\n' '~/.cargo/registry/index' '~/.cargo/registry/cache' '~/.cargo/registry/src' '~/.cargo/git/db' '~/.cargo/git/checkouts' 'cli/target/debug' 'cli/target/test-seams' '!cli/target/debug/incremental' '!cli/target/test-seams/debug/incremental'
      ;;
    digest) printf '%s\n' 'cli/target/test-seams-bwrap-digest/debug' '!cli/target/test-seams-bwrap-digest/debug/incremental' ;;
    release) printf '%s\n' 'cli/target/release-seams/release' '!cli/target/release-seams/release/incremental' ;;
    *) echo "unknown cache component: $1" >&2; return 2 ;;
  esac
}
free_bytes() { df -PB1 . | awk 'NR==2 {print $4}'; }
size_bytes() {
  local component="$1" path total=0 bytes
  while IFS= read -r path; do
    [[ "$path" == '!'* ]] && continue
    path="${path/#\~/$HOME}"
    [[ "$path" == /* ]] || path="$root/$path"
    if [[ -e "$path" ]]; then
      bytes="$(du -sb --exclude=incremental "$path" | cut -f1)"
      total=$((total + bytes))
    fi
  done < <(paths "$component")
  echo "$total"
}
output() { printf '%s=%s\n' "$1" "$2" >> "${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"; }
prepare() {
  local profile="$1" evidence="$2" variant=none required_gib=14 free allowed=false identity recipes component prefix
  mkdir -p "$evidence"
  case "$profile" in
    functional) variant=digest; required_gib=18 ;;
    release) variant=release; required_gib=16 ;;
    task-capacity) ;;
    all) required_gib=20 ;;
    *) echo "unknown cache profile: $profile" >&2; return 2 ;;
  esac
  # Local extracted sizes: common about 8 GiB, digest 4 GiB, release 2 GiB.
  # Leave another 6 GiB for archive extraction and source rebuilds; a miss or
  # insufficient space falls back to the ordinary cold build.
  free="$(free_bytes || true)"
  if [[ "$free" =~ ^[0-9]+$ ]] && (( free >= required_gib * 1024 * 1024 * 1024 )); then allowed=true; fi
  recipes="$(git -C "$root" ls-files -z -- cli/Cargo.lock 'cli/**/Cargo.toml' cli/Cargo.toml 'cli/**/build.rs' 'cli/.cargo/*' cli/rust-toolchain.toml | (cd "$root" && xargs -0 sha256sum) | sha256sum | cut -d' ' -f1)"
  identity="$( { printf '%s\n' "${RUNNER_OS:-$(uname -s)}" "${RUNNER_ARCH:-$(uname -m)}" "${ImageOS:-local}" "${ImageVersion:-local}"; rustc -vV; cargo -V; go version; cc --version; printf '%s\n' "$recipes"; sha256sum ../scripts/cli-build-cache.sh; } | sha256sum | cut -d' ' -f1)"
  : > "$evidence/cache-keys.tsv"
  for component in common digest release; do
    prefix="cli-build-v1-${component}-${identity}-"
    if [[ "$component" == digest ]]; then prefix="${prefix}$(sha256sum /usr/bin/bwrap | cut -d' ' -f1)-"; fi
    printf '%s\t%s\n' "$component" "${prefix}$(git -C "$root" rev-parse HEAD)" >> "$evidence/cache-keys.tsv"
    output "$component-prefix" "$prefix"
    output "$component-key" "${prefix}$(git -C "$root" rev-parse HEAD)"
    { echo "$component-paths<<CLI_CACHE_PATHS"; paths "$component"; echo CLI_CACHE_PATHS; } >> "$GITHUB_OUTPUT"
  done
  output variant "$variant"
  output restore-allowed "$allowed"
  output started "$(date +%s)"
  printf 'profile\t%s\nvariant\t%s\nfree_bytes\t%s\nrequired_bytes\t%s\nrestore_allowed\t%s\n' "$profile" "$variant" "$free" "$((required_gib * 1024 * 1024 * 1024))" "$allowed" > "$evidence/cache-prepare.tsv"
}
phase() {
  local name="$1" start status=0
  shift
  start="$(date +%s)"
  "$@" 2>&1 | tee "$evidence/build-${label}-${name}.log" || status=$?
  printf '%s\t%s\t%s\t%s\n' "$label" "$name" "$(( $(date +%s) - start ))" "$status" >> "$evidence/build-phases.tsv"
  return "$status"
}
build() {
  local profile="$1" label="$2" evidence="$3"
  mkdir -p "$evidence"
  # Ambient digest flags must never change the ordinary or release fixtures.
  unset CODEX_BWRAP_SHA256
  case "$profile" in
    all|common|functional)
      phase ordinary cargo build --locked
      phase tests cargo test --locked --no-run
      phase seams cargo build --locked -p tetral --features test-seams --target-dir target/test-seams
      ;;
    release|task-capacity)
      local target=tool_admission
      [[ "$profile" != release ]] || target=release_seams
      phase tests cargo test --locked -p tetral-tests --test "$target" --no-run
      phase seams cargo build --locked -p tetral --features test-seams --target-dir target/test-seams
      ;;
    digest|release-only) ;;
    *) echo "unknown build profile: $profile" >&2; return 2 ;;
  esac
  case "$profile" in
    all|functional|digest)
      phase digest env CODEX_BWRAP_SHA256="$(sha256sum /usr/bin/bwrap | cut -d' ' -f1)" cargo build --locked -p tetral --features test-seams --target-dir target/test-seams-bwrap-digest
      ;;
  esac
  case "$profile" in
    all|release|release-only)
      phase release cargo build --locked --release -p tetral --target-dir target/release-seams
      ;;
  esac
}
record() {
  local component="$1" evidence="$2" state=miss elapsed=""
  mkdir -p "$evidence"
  case "${RESTORE_OUTCOME:-skipped}" in
    skipped) state=skipped ;;
    failure|cancelled) state="${RESTORE_OUTCOME}" ;;
    *)
      if [[ "${CACHE_HIT:-}" == true ]]; then state=exact; elif [[ -n "${MATCHED_KEY:-}" ]]; then state=prefix; fi
      ;;
  esac
  if [[ "${RESTORE_STARTED:-}" =~ ^[0-9]+$ ]]; then elapsed=$(( $(date +%s) - RESTORE_STARTED )); fi
  printf 'component\t%s\nstate\t%s\nrequested_key\t%s\nmatched_key\t%s\nduration_seconds\t%s\nextracted_bytes\t%s\nfree_bytes\t%s\n' "$component" "$state" "${REQUESTED_KEY:-}" "${MATCHED_KEY:-}" "$elapsed" "$(size_bytes "$component")" "$(free_bytes || true)" > "$evidence/cache-${component}.tsv"
  [[ -z "${GITHUB_OUTPUT:-}" ]] || output started "$(date +%s)"
  cat "$evidence/cache-${component}.tsv"
}
lookups() {
  local evidence="$1" component upper hit key matched
  for component in common digest release; do
    upper="${component^^}"
    hit="${upper}_HIT"; key="${upper}_KEY"
    matched=""
    [[ "${!hit:-}" != true ]] || matched="${!key:-}"
    CACHE_HIT="${!hit:-}" REQUESTED_KEY="${!key:-}" MATCHED_KEY="$matched" RESTORE_OUTCOME=success record "$component" "$evidence/lookups"
  done
}
case "${1:-}" in
  lookups) lookups "${2:?evidence directory}" ;;
  prepare) prepare "${2:?profile}" "${3:?evidence directory}" ;;
  build) build "${2:?profile}" "${3:?label}" "${4:?evidence directory}" ;;
  record) record "${2:?component}" "${3:?evidence directory}" ;;
  paths) paths "${2:?component}" ;;
  *) echo 'usage: cli-build-cache.sh {prepare PROFILE EVIDENCE|build PROFILE LABEL EVIDENCE|record COMPONENT EVIDENCE|paths COMPONENT}' >&2; exit 2 ;;
esac
