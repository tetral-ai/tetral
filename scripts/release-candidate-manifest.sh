#!/usr/bin/env bash
# Writes candidate.json in the working directory from images.json and the
# chart, values and render digests found there. The chart's manifest digest
# stays zero until the chart is published.
set -euo pipefail

: "${VERSION:?git version is required}"
: "${ARTIFACT_VERSION:?artifact version is required}"
: "${SOURCE_COMMIT:?source commit is required}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
release() { (cd "$root" && go run ./internal/release/cmd/tetral-release "$@"); }

bases="$(release verify-bases --repository-root "$root" | jq .bases)"
database="$(release database-identity)"
images="$(jq 'with_entries(.value={repository:("ghcr.io/tetral-ai/"+.key),top_level_digest:.value.top_level_digest,top_level_media_type:.value.top_level_media_type,linux_amd64_child_digest:.value.child_digest,linux_amd64_child_media_type:.value.child_media_type,platform:{os:"linux",architecture:"amd64"}})' images.json)"
jq -n --arg version "$VERSION" --arg artifact "$ARTIFACT_VERSION" --argjson sequence "${ARTIFACT_VERSION##*.}" --arg source "$SOURCE_COMMIT" --argjson images "$images" --arg package "$(cat chart.digest)" --arg values "$(cat values.digest)" --arg render "$(cat render.digest)" --arg render_command "helm template tetral dist/tetral-$ARTIFACT_VERSION.tgz -f release-values.json" --argjson bases "$bases" --arg now "$(date -u +%FT%TZ)" \
  --argjson database "$database" \
  '{schema:"tetral.release-candidate/v1",version:{git:$version,artifact:$artifact,sequence:$sequence},source_commit:$source,platform:"linux/amd64",images:$images,chart:{candidate_manifest_digest:"sha256:0000000000000000000000000000000000000000000000000000000000000000",package_sha256:$package,render_sha256:$render,values_sha256:$values,render_command:$render_command},database_schema_version:$database.version,database_schema_checksum:$database.checksum,base_images:$bases,created_at:$now}' > candidate.json
