#!/usr/bin/env bash
set -euo pipefail

version="${1:?artifact version is required}"
output="${2:?facts output is required}"
: "${RELEASE_METADATA_REPOSITORY:?metadata repository is required}"
: "${GITHUB_REPOSITORY:?GitHub repository is required}"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
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

images='{}'
for image in tetral gateway agent-runtime sandbox; do
  if digest="$(oras manifest fetch --format go-template --template '{{ .digest }}' "ghcr.io/tetral-ai/$image:$version" 2>/dev/null)"; then
    images="$(jq -c --arg name "$image" --arg digest "$digest" '. + {($name):$digest}' <<<"$images")"
  fi
done
chart=''
chart_package=''
if chart="$(oras manifest fetch --format go-template --template '{{ .digest }}' "ghcr.io/tetral-ai/charts/tetral:$version" 2>/dev/null)"; then
  oras manifest fetch "ghcr.io/tetral-ai/charts/tetral@$chart" > "$work/chart.json"
  jq -e '
    .schemaVersion == 2 and
    .mediaType == "application/vnd.oci.image.manifest.v1+json" and
    .config.mediaType == "application/vnd.cncf.helm.config.v1+json" and
    ([.layers[] | select(.mediaType=="application/vnd.cncf.helm.chart.content.v1.tar+gzip")] | length) == 1 and
    (.layers | length) == 1
  ' "$work/chart.json" >/dev/null
  chart_package="$(jq -r '.layers[] | select(.mediaType=="application/vnd.cncf.helm.chart.content.v1.tar+gzip") | .digest' "$work/chart.json")"
fi
tag_commit=''
git_version="v$version"
if tag_object="$(gh api "repos/$GITHUB_REPOSITORY/git/ref/tags/$git_version" 2>/dev/null)"; then
  tag_commit="$(jq -r .object.sha <<<"$tag_object")"
  if test "$(jq -r .object.type <<<"$tag_object")" = tag; then
    tag_commit="$(gh api "repos/$GITHUB_REPOSITORY/git/tags/$tag_commit" --jq .object.sha)"
  fi
fi
assets='{}'
if gh release view "$git_version" --repo "$GITHUB_REPOSITORY" >/dev/null 2>&1; then
  mapfile -t release_assets < <(gh release view "$git_version" --repo "$GITHUB_REPOSITORY" --json assets --jq '.assets[].name' | sort)
  for asset in "${release_assets[@]}"; do
    case "$asset" in
      candidate.json|evidence.json|authorization.json)
        gh release download "$git_version" --repo "$GITHUB_REPOSITORY" --pattern "$asset" --dir "$work"
        digest="sha256:$(sha256sum "$work/$asset" | awk '{print $1}')"
        ;;
      *) digest=unexpected ;;
    esac
    assets="$(jq -c --arg name "$asset" --arg digest "$digest" '. + {($name):$digest}' <<<"$assets")"
  done
fi

jq -c --argjson images "$images" --arg chart "$chart" --arg chart_package "$chart_package" --arg tag "$tag_commit" --argjson assets "$assets" \
  '. + {final:{images:$images,chart_manifest:$chart,chart_package_digest:$chart_package,git_tag_commit:$tag,github_release_assets:$assets}}' <<<"$facts" > "$output"
