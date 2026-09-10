#!/usr/bin/env bash
set -euo pipefail

# Read the published targets. Immutable release records are owned by release-state.sh.
version="${1:?artifact version is required}"
output="${2:?output is required}"
: "${GITHUB_REPOSITORY:?GitHub repository is required}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

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
  # OCI permits omitting the top-level mediaType, as Helm push does. When
  # present it must match; Helm config and the single chart layer remain required.
  jq -e '
    .schemaVersion == 2 and
    ((has("mediaType") | not) or .mediaType == "application/vnd.oci.image.manifest.v1+json") and
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
publication=null
if release=$(gh release view "$git_version" --repo "$GITHUB_REPOSITORY" --json isDraft,isPrerelease,assets 2>/dev/null); then
  publication=$(jq -c '{draft:.isDraft,prerelease:.isPrerelease}' <<<"$release")
  mapfile -t release_assets < <(jq -r '.assets[].name' <<<"$release" | sort)
  for asset in "${release_assets[@]}"; do
    case "$asset" in
      candidate.json|evidence.json|authorization.json)
        digest="sha256:$(gh release download "$git_version" --repo "$GITHUB_REPOSITORY" --pattern "$asset" --output - | sha256sum | awk '{print $1}')"
        ;;
      *) digest=unexpected ;;
    esac
    assets="$(jq -c --arg name "$asset" --arg digest "$digest" '. + {($name):$digest}' <<<"$assets")"
  done
fi

jq -cn --argjson images "$images" --arg chart "$chart" --arg chart_package "$chart_package" --arg tag "$tag_commit" --argjson assets "$assets" --argjson publication "$publication" \
  '{images:$images,chart_manifest:$chart,chart_package_digest:$chart_package,git_tag_commit:$tag,github_release_assets:$assets,github_release:$publication}' > "$output"
