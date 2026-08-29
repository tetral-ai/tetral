#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: release-oci-record.sh fetch KIND REFERENCE OUTPUT | publish KIND INPUT REFERENCE" >&2
  exit 2
}

command="${1:-}"
kind="${2:-}"
remote_args=()
copy_from_args=()
copy_to_args=()
if [[ "${TETRAL_RELEASE_PLAIN_HTTP:-false}" == true ]]; then
  remote_args+=(--plain-http)
  copy_from_args+=(--from-plain-http)
  copy_to_args+=(--to-plain-http)
fi
case "$command" in
  fetch)
    reference="${3:-}"
    output="${4:-}"
    [[ -n "$kind" && -n "$reference" && -n "$output" ]] || usage
    layout="$(mktemp -d)"
    trap 'rm -rf "$layout"' EXIT
    oras cp "${copy_from_args[@]}" --to-oci-layout "$reference" "$layout:record" >/dev/null
    go run ./internal/release/cmd/tetral-release validate-layout \
      --kind "$kind" --root "$layout" --output-layer "$output"
    ;;
  publish)
    input="${3:-}"
    reference="${4:-}"
    [[ -n "$kind" && -n "$input" && -n "$reference" ]] || usage
    layout="$(mktemp -d)"
    trap 'rm -rf "$layout"' EXIT
    result="$(go run ./internal/release/cmd/tetral-release artifact --kind "$kind" --input "$input" --output "$layout")"
    digest="$(jq -r .manifest_digest <<<"$result")"
    if existing="$(oras manifest fetch "${remote_args[@]}" --format go-template --template '{{ .digest }}' "$reference" 2>/dev/null)"; then
      [[ "$existing" == "$digest" ]] || {
        echo "$reference already identifies a conflicting artifact" >&2
        exit 1
      }
    else
      oras cp --from-oci-layout "${copy_to_args[@]}" "$layout@$digest" "$reference" >/dev/null
    fi
    printf '%s\n' "$digest"
    ;;
  *) usage ;;
esac
