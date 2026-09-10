#!/usr/bin/env bash
set -euo pipefail

version="${1:?artifact version is required}"
output="${2:?facts output is required}"
: "${RELEASE_METADATA_REPOSITORY:?metadata repository is required}"
: "${GITHUB_REPOSITORY:?GitHub repository is required}"

# Promotion keeps these canonical files for Release attachments. Standalone
# state queries own a temporary directory instead.
work="${3:-}"
if [[ -z "$work" ]]; then
  work="$(mktemp -d)"
  trap 'rm -rf "$work"' EXIT
fi
facts='{}'

read_record() {
  local kind="$1" tag="$2" output_name="$3" digest
  if ! digest="$(oras manifest fetch --format go-template --template '{{ .digest }}' "$RELEASE_METADATA_REPOSITORY:$tag" 2>/dev/null)"; then
    return 1
  fi
  ./scripts/release-oci-record.sh fetch "$kind" "$RELEASE_METADATA_REPOSITORY@$digest" "$work/$output_name.json" >/dev/null
  printf '%s\n' "$digest"
}

if reservation_digest="$(read_record reservation "reservation-$version" reservation)"; then
  facts="$(jq -cn --slurpfile value "$work/reservation.json" '{reservation:$value[0]}')"
fi

if candidate_digest="$(read_record candidate "candidate-$version" candidate)"; then
  facts="$(jq -c --slurpfile value "$work/candidate.json" --arg digest "$candidate_digest" '. + {candidate:$value[0],candidate_digest:$digest}' <<<"$facts")"
fi

if rehearsal_digest="$(read_record rehearsal "rehearsal-$version" rehearsal)"; then
  facts="$(jq -c --slurpfile value "$work/rehearsal.json" --arg digest "$rehearsal_digest" '. + {rehearsal:$value[0],rehearsal_digest:$digest}' <<<"$facts")"
fi
if authorization_digest="$(read_record authorization "authorization-$version" authorization)"; then
  facts="$(jq -c --slurpfile value "$work/authorization.json" --arg digest "$authorization_digest" '. + {authorization:$value[0],authorization_digest:$digest}' <<<"$facts")"
fi
if disposition_digest="$(read_record disposition "disposition-$version" disposition)"; then
  facts="$(jq -c --slurpfile value "$work/disposition.json" '. + {disposition:$value[0]}' <<<"$facts")"
fi

./scripts/release-final-state.sh "$version" "$work/final.json"
jq -c --slurpfile final "$work/final.json" '. + {final:$final[0]}' <<<"$facts" > "$output"
