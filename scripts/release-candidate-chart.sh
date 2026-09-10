#!/usr/bin/env bash
set -euo pipefail

version="${1:?artifact version is required}"
destination="${2:?package directory is required}"
: "${RELEASE_METADATA_REPOSITORY:?metadata repository is required}"
mkdir -p "$destination"

# The chart can outlive an interrupted Candidate finalization. Reuse its exact
# bytes: packaging the same sources again can encode different tar timestamps.
reference="$RELEASE_METADATA_REPOSITORY:helm-candidate-$version"
if digest="$(oras manifest fetch --format go-template --template '{{ .digest }}' "$reference" 2>/dev/null)"; then
  ./scripts/release-oci-record.sh fetch helm-candidate "$RELEASE_METADATA_REPOSITORY@$digest" "$destination/tetral-$version.tgz" >/dev/null
else
  helm package deploy/helm/tetral --version "$version" --app-version "$version" --destination "$destination"
fi
